"""
Add a run window to the schedule.

Usage: python schedule_run.py <start> <end> [label]
start and end can be run-hours or datetimes ("YYYY-MM-DD HH:MM").
"""

import json
import sys
from time_utils import parse_time_input, format_run_hour

if len(sys.argv) < 3:
    print('Usage: python schedule_run.py <start> <end> [label]')
    print('  start, end: run-hour number OR "YYYY-MM-DD HH:MM"')
    print('Examples:')
    print('  python schedule_run.py 168 184 "Day 4"')
    print('  python schedule_run.py "2026-04-21 06:00" "2026-04-21 22:00" "Day 4"')
    sys.exit(1)

with open("data.json", "r") as f:
    data = json.load(f)

try:
    start = parse_time_input(data, sys.argv[1])
    end = parse_time_input(data, sys.argv[2])
except ValueError as e:
    print(f"Error: {e}")
    sys.exit(1)

label = sys.argv[3] if len(sys.argv) >= 4 else ""

if end <= start:
    print(f"Error: end ({end}) must be greater than start ({start}).")
    sys.exit(1)

for window in data["run_schedule"]:
    if start < window["end_hour"] and end > window["start_hour"]:
        print(f"Error: overlaps existing window "
              f"({window['start_hour']} -> {window['end_hour']}).")
        sys.exit(1)

new_window = {
    "start_hour": start,
    "end_hour": end,
    "label": label
}

data["run_schedule"].append(new_window)
data["run_schedule"].sort(key=lambda w: w["start_hour"])

with open("data.json", "w") as f:
    json.dump(data, f, indent=2)

duration = end - start
print(f"Added run window ({duration} hrs) {label}")
print(f"  start: {format_run_hour(data, start)}")
print(f"  end:   {format_run_hour(data, end)}")
print(f"Total windows in schedule: {len(data['run_schedule'])}")