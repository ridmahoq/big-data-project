from connect_db import CassandraConnection
from cassandra.query import BatchStatement
from collections import defaultdict

session = CassandraConnection()

query = """
SELECT trip_start_time, fare, tips, trip_miles 
FROM silver;
"""
rows = session.execute(query)

aggregated_data = defaultdict(lambda: {'trip_count': 0, 'total_fare': 0, 'total_tips': 0, 'total_trip_miles': 0})

def assign_time_category(hour):
    if 0 <= hour < 3:
        return "Midnight - 3AM"
    elif 3 <= hour < 6:
        return "Pre-Dawn"
    elif 6 <= hour < 9:
        return "Morning Rush"
    elif 9 <= hour < 12:
        return "Late Morning"
    elif 12 <= hour < 15:
        return "Afternoon"
    elif 15 <= hour < 18:
        return "Evening Rush"
    elif 18 <= hour < 21:
        return "Night Commuters"
    else:
        return "Late Night Movers"

for row in rows:
    trip_hour = row.trip_start_time.hour  # Extract the hour
    time_category = assign_time_category(trip_hour)  # Assign to a broad category

    aggregated_data[(time_category, trip_hour)]['trip_count'] += 1
    aggregated_data[(time_category, trip_hour)]['total_fare'] += row.fare if row.fare is not None else 0
    aggregated_data[(time_category, trip_hour)]['total_tips'] += row.tips if row.tips is not None else 0
    aggregated_data[(time_category, trip_hour)]['total_trip_miles'] += row.trip_miles if row.trip_miles is not None else 0

# Compute Averages
for (time_category, trip_hour), values in aggregated_data.items():
    values['avg_fare'] = values['total_fare'] / values['trip_count']
    values['avg_tips'] = values['total_tips'] / values['trip_count']
    values['avg_trip_miles'] = values['total_trip_miles'] / values['trip_count']

insert_query = session.prepare("""
INSERT INTO gold_time_category (trip_hour, time_category, trip_count, avg_fare, avg_tips, avg_trip_miles) 
VALUES (?, ?, ?, ?, ?, ?);
""")

batch = BatchStatement()
for (time_category, trip_hour), values in aggregated_data.items():
    batch.add(insert_query, (trip_hour, time_category, values['trip_count'], values['avg_fare'], values['avg_tips'], values['avg_trip_miles']))

session.execute(batch)
print("Data inserted.")

session.shutdown()
