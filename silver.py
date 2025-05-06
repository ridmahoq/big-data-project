from connect_db import CassandraConnection
from cassandra.query import BatchStatement
from datetime import datetime, time
from cassandra import ConsistencyLevel
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def silver_table():
    session = CassandraConnection()
    
    try:
        # bronze table row count
        bronze_count = session.execute("SELECT COUNT(*) FROM bronze").one()[0]
        logger.info(f"Bronze table row count {bronze_count}")

        insert_stmt = session.prepare("""
        INSERT INTO silver (
            trip_id, trip_start_date, trip_start_time, 
            trip_end_date, trip_end_time, trip_seconds,
            trip_miles, pickup_community_area, dropoff_community_area,
            fare, tips, tolls, extras, trip_total, payment_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        # read from bronze table
        rows = session.execute("SELECT * FROM bronze")
        
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        batch_size = 50
        processed_count = 0
        inserted_count = 0
        skipped_count = 0
        
        for row in rows:
            processed_count += 1
            try:
                # skip rows with missing essential columns
                if not row.trip_id or not row.trip_start_timestamp:
                    skipped_count += 1
                    continue
                # skip rows where pickup community area is blank or not an integer
                try:
                    pickup_area = int(row.pickup_community_area)
                except (ValueError, TypeError):
                    skipped_count += 1
                    continue
                
                # keep only valid trip start and end timestamps (end time should be larger than start time)
                start_dt = row.trip_start_timestamp
                end_dt = row.trip_end_timestamp
                if end_dt and start_dt and end_dt < start_dt:
                    skipped_count += 1
                    continue
                
                trip_seconds = int(row.trip_seconds) if row.trip_seconds and int(row.trip_seconds) > 0 else None
                trip_miles = float(row.trip_miles) if row.trip_miles and float(row.trip_miles) > 0 else None
                fare = float(row.fare) if row.fare and float(row.fare) >= 0 else None
                tips = float(row.tips) if row.tips and float(row.tips) >= 0 else None
                tolls = float(row.tolls) if row.tolls and float(row.tolls) >= 0 else None
                extras = float(row.extras) if row.extras and float(row.extras) >= 0 else None
                trip_total = float(row.trip_total) if row.trip_total and float(row.trip_total) >= 0 else None
                
                batch.add(insert_stmt, (
                    str(row.trip_id).strip(),
                    start_dt.date(),
                    start_dt.time(),
                    end_dt.date() if end_dt else None,
                    end_dt.time() if end_dt else None,
                    trip_seconds,
                    trip_miles,
                    pickup_area,
                    int(row.dropoff_community_area) if row.dropoff_community_area else None,
                    fare,
                    tips,
                    tolls,
                    extras,
                    trip_total,
                    str(row.payment_type).strip()
                ))
                
                # execute
                if processed_count % batch_size == 0:
                    session.execute(batch)
                    inserted_count += batch_size
                    batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
                    logger.info(f"processed {processed_count} rows, inserted {inserted_count} rows")

            except Exception as e:
                logger.error(f"error processing {row.trip_id}: {str(e)}")
                skipped_count += 1
                continue

        # insert remaining records 
        if batch:
            remaining = processed_count % batch_size
            session.execute(batch)
            inserted_count += remaining
            logger.info(f"inserted {remaining} records")

        logger.info(f"""
        total processed: {processed_count} rows
        inserted: {inserted_count} rows
        skipped {skipped_count} rows
        """)

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise
    finally:
        CassandraConnection.shutdown()

if __name__ == "__main__":
    silver_table()

