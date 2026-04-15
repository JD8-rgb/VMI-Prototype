"""
mark_schedule_received.py
-------------------------
Manually mark the run schedule as received for a given week.
Use this when Anna sends the schedule by phone, Teams, or any channel
other than email (where read_schedule.py would pick it up automatically).

Usage
-----
    python mark_schedule_received.py              # defaults to next Monday
    python mark_schedule_received.py 2026-05-11   # specify any Monday date
"""

import json
import sys
from datetime import datetime, timedelta

DATA_PATH = "data.json"

# Determine the target Monday
if len(sys.argv) >= 2:
    date_str = sys.argv[1].strip()
    try:
        target = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        print(f"Error: could not parse '{date_str}'. Use YYYY-MM-DD format.")
        sys.exit(1)
    if target.weekday() != 0:
        print(f"Warning: {target} is a {target.strftime('%A')}, not a Monday. Using it anyway.")
else:
    today = datetime.now()
    days_ahead = (7 - today.weekday()) % 7 or 7
    target = (today + timedelta(days=days_ahead)).date()

with open(DATA_PATH) as f:
    data = json.load(f)

previous = data.get("schedule_received_for_week")
data["schedule_received_for_week"] = target.isoformat()

with open(DATA_PATH, "w") as f:
    json.dump(data, f, indent=2)

print(f"Schedule marked as received for week of {target.isoformat()}.")
if previous:
    print(f"(Previously: {previous})")
