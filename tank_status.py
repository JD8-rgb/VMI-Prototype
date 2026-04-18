import json
import os
from alerts import get_all_alerts
from time_utils import format_run_hour
import email_hooks

def _load_data():
    path = "data.json" if os.path.exists("data.json") else "defaults.json"
    with open(path) as f:
        return json.load(f)

data = _load_data()

tanks = data["tanks"]
trucks = data["scheduled_trucks"]
current_hour = data["current_run_hour"]

print("=" * 60)
print("TANK STATUS REPORT")
print(f"Current: {format_run_hour(data, current_hour)}")
print("=" * 60)

for tank_name, tank_info in tanks.items():
    product = tank_info["product"]
    level = tank_info["current_level_lbs"]
    capacity = tank_info["max_capacity_lbs"]
    status = tank_info["status"]
    percent_full = (level / capacity) * 100

    print(f"\n{tank_name} ({product})")
    print(f"  Level:    {level:,} / {capacity:,} lbs ({percent_full:.1f}% full)")
    print(f"  Status:   {status}")

print("\n" + "=" * 60)
print("SCHEDULED TRUCKS")
print("=" * 60)

if not trucks:
    print("(none)")
else:
    sorted_trucks = sorted(trucks, key=lambda t: t["arrival_run_hour"])
    for truck in sorted_trucks:
        arrival = format_run_hour(data, truck["arrival_run_hour"])
        print(f"  {truck['sap_order']} | {truck['product']} | "
              f"{truck['quantity_lbs']:,} lbs")
        print(f"    arrives: {arrival}")

print("\n" + "=" * 60)
print("RUN SCHEDULE")
print("=" * 60)

run_schedule = data["run_schedule"]
if not run_schedule:
    print("(none)")
else:
    for window in run_schedule:
        duration = window["end_hour"] - window["start_hour"]
        label = f" — {window['label']}" if window["label"] else ""
        start = format_run_hour(data, window["start_hour"])
        end = format_run_hour(data, window["end_hour"])
        print(f"  {start}")
        print(f"  -> {end} ({duration} hrs){label}")

print("\n" + "=" * 60)
print("ALERTS")
print("=" * 60)

alerts = get_all_alerts(data)
if not alerts:
    print("All clear.")
else:
    for alert in alerts:
        print(f"  {alert['text']}")

data = email_hooks.send_alert_emails_if_new(data)
with open("data.json", "w") as f:
    json.dump(data, f, indent=2)

print("=" * 60)