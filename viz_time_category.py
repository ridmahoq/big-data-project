import matplotlib.pyplot as plt
import seaborn as sns
from connect_db import CassandraConnection

session = CassandraConnection()

query = "SELECT trip_hour, trip_count FROM gold_time_category;"
rows = session.execute(query)

plt.figure(figsize=(10, 5))
for row in rows:
    plt.scatter(row.trip_hour, row.trip_count, color="blue")  
plt.plot([r.trip_hour for r in rows], [r.trip_count for r in rows], linestyle="-", linewidth=2, color="blue")
plt.title("Trips Per Hour")
plt.xlabel("Trip Hour")
plt.ylabel("Trip Count")
plt.xticks(range(0, 24))
plt.grid()
plt.savefig("plots/trips_per_hour.png")
plt.show()

query = "SELECT time_category, avg_fare FROM gold_time_category;"
rows = session.execute(query)

time_categories = []
avg_fares = []
for row in rows:
    time_categories.append(row.time_category)
    avg_fares.append(row.avg_fare)

plt.figure(figsize=(8, 4))
sns.barplot(x=time_categories, y=avg_fares, palette="coolwarm", ci="sd")  # standard deviation
plt.title("Average Fare Per Time Category")
plt.xlabel("Time Category")
plt.ylabel("Avg Fare ($)")
plt.xticks(rotation=25)
plt.grid()
plt.savefig("plots/avg_fare_per_time_category.png")
plt.show()

session.shutdown()
