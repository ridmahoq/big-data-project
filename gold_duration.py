from connect_db import CassandraConnection
from cassandra.query import SimpleStatement
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def classify_duration(seconds):
    if seconds is None:
        return None
    if seconds <= 600:
        return 'short'
    elif seconds <= 1800:
        return 'medium'
    else:
        return 'long'

def make_buckets():
    session = CassandraConnection()
    
    try:
        rows = session.execute("SELECT trip_seconds FROM silver")
        bucket_counts = {'short': 0, 'medium': 0, 'long': 0}

        for row in rows:
            label = classify_duration(row.trip_seconds)
            if label:
                bucket_counts[label] += 1

        logger.info(f"Trip duration counts: {bucket_counts}")

        for label, count in bucket_counts.items():
            session.execute(
                "INSERT INTO gold_duration (duration_label, trip_count) VALUES (%s, %s)",
                (label, count)
            )

        logger.info("Inserted into gold table")

    finally:
        session.shutdown()

if __name__ == "__main__":
    make_buckets()
