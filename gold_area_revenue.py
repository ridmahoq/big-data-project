from connect_db import CassandraConnection
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
import logging
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

session = CassandraConnection()

def insert_gold(records):
    insert_stmt = session.prepare("""
        INSERT INTO gold_area_1 (
            trip_start_date, pickup_community_area,
            total_fare, total_tips, total_trip_total,
            avg_trip_total, trip_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    
    try:
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        for record in records:
            batch.add(insert_stmt, record)

        session.execute(batch)
        
    except Exception as e:
        logger.error(f"Batch Insert Error: {str(e)}")

def gold_optimized():
    session = CassandraConnection()
    
    try:
        # get min,max dates
        date_range = session.execute("""
            SELECT MIN(trip_start_date) as min_date, MAX(trip_start_date) as max_date 
            FROM silver
        """).one()
        
        start_date = datetime.strptime(str(date_range.min_date), "%Y-%m-%d").date()
        end_date = datetime.strptime(str(date_range.max_date), "%Y-%m-%d").date()
        logger.info(f"Processing dates from {start_date} to {end_date}")

        total_processed = 0
        current_date = start_date
        
        while current_date <= end_date:
            try:
                areas = session.execute("""
                    SELECT DISTINCT pickup_community_area, trip_start_date
                    FROM silver
                    WHERE trip_start_date = %s;
                """, (current_date,))
                
                records_to_insert = []
                for area_row in areas:
                    area = area_row.pickup_community_area
                    
                    result = session.execute("""
                        SELECT SUM(fare) as total_fare, SUM(tips) as total_tips, SUM(trip_total) as total_trip_total, 
                               AVG(trip_total) as avg_trip_total, COUNT(*) as trip_count
                        FROM silver
                        WHERE pickup_community_area = %s AND trip_start_date = %s
                    """, (area, current_date)).one()
                    
                    records_to_insert.append((
                        current_date, area,
                        float(result.total_fare) if result.total_fare else 0.0,
                        float(result.total_tips) if result.total_tips else 0.0,
                        float(result.total_trip_total) if result.total_trip_total else 0.0,
                        float(result.avg_trip_total) if result.avg_trip_total else 0.0,
                        int(result.trip_count) if result.trip_count else 0
                    ))

                batch_size = 50 
                with ThreadPoolExecutor(max_workers=5) as executor:
                    for i in range(0, len(records_to_insert), batch_size):
                        executor.submit(insert_gold, records_to_insert[i:i + batch_size])
                
                total_processed += len(records_to_insert)
                logger.info(f"Processed {current_date}: {len(records_to_insert)} areas")

            except Exception as e:
                logger.error(f"Error processing {current_date}: {str(e)}")
            
            current_date += timedelta(days=1)

        logger.info(f"Total processed: {total_processed}")
    
    finally:
        session.shutdown()

if __name__ == "__main__":
    gold_optimized()
