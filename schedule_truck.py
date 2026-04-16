"""
Schedule a truck delivery.

Usage: python schedule_truck.py <sap_order> <product> <arrival>
arrival can be a run-hour (e.g. 168) or a datetime (e.g. "2026-04-21 08:00").
"""

import json
import os
import sys
from time_utils import parse_time_input, format_run_hour

def _load_data():
    path = "data.json" if os.path.exists("data.json") else "defaults.json"
    with open(path) as f:
        return json.load(f)

if len(sys.argv) < 4:
    print('Usage: python schedule_truck.py <sap_order> <product> <arrival>')
    print('  arrival: run-hour number OR "YYYY-MM-DD HH:MM"')
    print('Examples:')
    print('  python schedule_truck.py SAP12345 "Product U" 168')
    print('  python schedule_truck.py SAP12345 "Product U" "2026-04-21 08:00"')
    sys.exit(1)

sap_order = sys.argv[1]
product = sys.argv[2]

data = _load_data()

try:
    arrival = parse_time_input(data, sys.argv[3])
except ValueError as e:
    print(f"Error: {e}")
    sys.exit(1)

if product not in data["truck_quantities"]:
    valid = ", ".join(data["truck_quantities"].keys())
    print(f"Error: Unknown product '{product}'. Valid products: {valid}")
    sys.exit(1)

for truck in data["scheduled_trucks"]:
    if truck["sap_order"] == sap_order:
        print(f"Error: SAP order {sap_order} is already scheduled.")
        sys.exit(1)

quantity = data["truck_quantities"][product]

new_truck = {
    "sap_order": sap_order,
    "product": product,
    "quantity_lbs": quantity,
    "arrival_run_hour": arrival
}

data["scheduled_trucks"].append(new_truck)

with open("data.json", "w") as f:
    json.dump(data, f, indent=2)

print(f"Scheduled: {sap_order} | {product} | {quantity:,} lbs")
print(f"  arrives: {format_run_hour(data, arrival)}")
print(f"Total scheduled trucks: {len(data['scheduled_trucks'])}")