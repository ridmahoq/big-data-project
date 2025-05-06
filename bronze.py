from cassandra.query import BatchStatement
from connect_db import CassandraConnection
from cassandra import ConsistencyLevel
import pandas as pd
from datetime import datetime
import logging
import math

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data():
    try:
        session = CassandraConnection()
        
        file_path = "taxi.csv"
        logger.info(f"reading: {file_path}")
        df = pd.read_csv(file_path)

        df["Trip Start Timestamp"] = pd.to_datetime(df["Trip Start Timestamp"], errors='coerce')
        df["Trip End Timestamp"] = pd.to_datetime(df["Trip End Timestamp"], errors='coerce')

        insert_stmt = session.prepare("""
            INSERT INTO bronze (
                trip_id, taxi_id, trip_start_timestamp, trip_end_timestamp, trip_seconds,
                trip_miles, pickup_census_tract, dropoff_census_tract, pickup_community_area,
                dropoff_community_area, fare, tips, tolls, extras, trip_total, payment_type,
                company, pickup_latitude, pickup_longitude, pickup_location,
                dropoff_latitude, dropoff_longitude, dropoff_location
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        batch_size = 50
        success_count = 0
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            batch_stmt = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            
            for _, row in batch.iterrows():
                try:
                    def safe_int(x):
                        return int(x) if not pd.isna(x) and not math.isnan(x) else None

                    def safe_float(x):
                        return float(x) if not pd.isna(x) and not math.isnan(x) else None
                    
                    batch_stmt.add(insert_stmt, (
                        str(row['Trip ID']),
                        str(row['Taxi ID']),
                        row['Trip Start Timestamp'].to_pydatetime(),
                        row['Trip End Timestamp'].to_pydatetime(),
                        safe_int(row['Trip Seconds']),
                        safe_float(row['Trip Miles']),
                        str(row['Pickup Census Tract']) if pd.notna(row['Pickup Census Tract']) else None,
                        str(row['Dropoff Census Tract']) if pd.notna(row['Dropoff Census Tract']) else None,
                        safe_int(row['Pickup Community Area']),
                        safe_int(row['Dropoff Community Area']),
                        safe_float(row['Fare']),
                        safe_float(row['Tips']),
                        safe_float(row['Tolls']),
                        safe_float(row['Extras']),
                        safe_float(row['Trip Total']),
                        str(row['Payment Type']),
                        str(row['Company']),
                        safe_float(row['Pickup Centroid Latitude']),
                        safe_float(row['Pickup Centroid Longitude']),
                        str(row['Pickup Centroid Location']),
                        safe_float(row['Dropoff Centroid Latitude']),
                        safe_float(row['Dropoff Centroid Longitude']),
                        str(row['Dropoff Centroid  Location'])
                    ))
                except Exception as e:
                    logger.error(f"row error {i}: {str(e)}")
                    continue
            
            try:
                session.execute(batch_stmt)
                success_count += len(batch)
                logger.info(f"inserted batch {i//batch_size + 1}, total: {success_count}")
            except Exception as e:
                logger.error(f"error inserting batch {i//batch_size + 1}: {str(e)}")

        logger.info(f"{success_count}/{len(df)} records into cassandra")

    except Exception as e:
        logger.error(f"fatal error: {str(e)}")
    finally:
        CassandraConnection.shutdown()

if __name__ == "__main__":
    load_data()