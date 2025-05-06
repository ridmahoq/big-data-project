from connect_db import CassandraConnection
from cassandra.query import BatchStatement
from collections import defaultdict

session = CassandraConnection()

query = """
SELECT payment_type, fare, tips, trip_total 
FROM silver;
"""

rows = session.execute(query)

aggregated_data = defaultdict(lambda: {'total_fare': 0, 'total_tips': 0, 'avg_trip_total': [], 'trip_count': 0})

for row in rows:
    payment_type = row.payment_type
    aggregated_data[payment_type]['total_fare'] += row.fare if row.fare is not None else 0
    aggregated_data[payment_type]['total_tips'] += row.tips if row.tips is not None else 0
    aggregated_data[payment_type]['avg_trip_total'].append(row.trip_total if row.trip_total is not None else 0)
    aggregated_data[payment_type]['trip_count'] += 1

for payment_type, values in aggregated_data.items():
    values['avg_trip_total'] = sum(values['avg_trip_total']) / len(values['avg_trip_total'])

insert_query = session.prepare("""
INSERT INTO gold_payment (payment_type, total_fare, total_tips, avg_trip_total, trip_count) 
VALUES (?, ?, ?, ?, ?);
""")

batch = BatchStatement()
for payment_type, values in aggregated_data.items():
    batch.add(insert_query, (payment_type, values['total_fare'], values['total_tips'], values['avg_trip_total'], values['trip_count']))

session.execute(batch)
print("Data inserted.")

# result = session.execute("SELECT * FROM gold_payment;")
# for row in result:
#     print(row)

session.shutdown()
