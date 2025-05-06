import seaborn as sns
import matplotlib.pyplot as plt
from connect_db import CassandraConnection
from datetime import datetime
from collections import defaultdict

sns.set_style("whitegrid")
plt.figure(figsize=(15, 8))

session = CassandraConnection()
all_data = session.execute("SELECT trip_start_date, pickup_community_area, total_trip_total FROM gold_area_1")

monthly_totals = defaultdict(lambda: defaultdict(float))
for record in all_data:
    date_str = str(record.trip_start_date)
    python_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    month_key = f"{python_date.year}-{python_date.month:02d}"
    monthly_totals[record.pickup_community_area][month_key] += record.total_trip_total

high_volume_areas = [area for area in monthly_totals if sum(monthly_totals[area].values()) >= 65000]
all_months = sorted({month for area in high_volume_areas for month in monthly_totals[area].keys()})
x_labels = [f"{datetime.strptime(m, '%Y-%m').strftime('%m/%y')}" for m in all_months]

palette = sns.color_palette("tab10", len(high_volume_areas))
lines = []
for i, area in enumerate(high_volume_areas):
    y_values = [monthly_totals[area].get(month, 0) for month in all_months]
    line = sns.lineplot(x=range(len(all_months)), y=y_values, 
                       marker='o', label=f'Area {area}',
                       color=palette[i], linewidth=2.5)
    lines.append(line.lines[-1]) 

# focus on hover
def on_hover(event):
    if event.inaxes == plt.gca():
        for line in lines:
            if line.contains(event)[0]:
                plt.setp(lines, alpha=0.2) 
                plt.setp(line, alpha=1, linewidth=3)  
                break
        else:
            plt.setp(lines, alpha=1, linewidth=2.5) 
        plt.draw()

plt.gcf().canvas.mpl_connect('motion_notify_event', on_hover)
plt.title("Monthly Trip Totals - Hover to focus", pad=20)
plt.xlabel('Month (MM/YY)')
plt.ylabel('Total Trip Amount ($)')
plt.xticks(range(len(all_months)), x_labels, rotation=45)
plt.legend()
plt.tight_layout()
plt.savefig("plots/monthly_trip_total.png")
plt.show()
CassandraConnection.shutdown()