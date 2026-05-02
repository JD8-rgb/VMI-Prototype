"""
Advance the simulation clock by N run-hours, processing:
- Truck deliveries at their arrival_run_hour
- Run-schedule windows (consumption only happens inside scheduled windows)

Consumption and delivery delegate to alerts.simulate_consume /
alerts.simulate_delivery_no_alert so CLI behavior matches the Streamlit
app, projection chart, and planner. Previously this script had its own
copies that consumed from the persisted draw/status field, which made
CLI runs disagree with the live simulation after the draw-status fix.
"""

import json
import os
import sys
from time_utils import parse_time_input, format_run_hour
from alerts import simulate_consume, simulate_delivery_no_alert
import email_hooks

def _load_data():
    path = "data.json" if os.path.exists("data.json") else "defaults.json"
    with open(path) as f:
        return json.load(f)

if len(sys.argv) < 2:
    print("Usage: python advance_time.py <hours_or_target_datetime>")
    print("Examples:")
    print("  python advance_time.py 8")
    print('  python advance_time.py "2026-04-22 17:00"')
    sys.exit(1)

data = _load_data()

tanks = data["tanks"]
rates = data["consumption_rates"]
trucks = data["scheduled_trucks"]
run_schedule = data["run_schedule"]
start_hour = data["current_run_hour"]

# Parse the argument: if it's a small number (< current_run_hour), treat as
# a duration to add. Otherwise, treat as a target run-hour or datetime.
arg = sys.argv[1]
try:
    parsed = parse_time_input(data, arg)
except ValueError as e:
    print(f"Error: {e}")
    sys.exit(1)

if parsed < start_hour:
    hours = parsed
    end_hour = start_hour + hours
else:
    end_hour = parsed
    hours = end_hour - start_hour

if hours <= 0:
    print(f"Error: target {format_run_hour(data, end_hour)} is not in the future.")
    sys.exit(1)

print(f"Advancing {hours} hours")
print(f"  from: {format_run_hour(data, start_hour)}")
print(f"  to:   {format_run_hour(data, end_hour)}\n")


def consume_segment(seg_hours):
    """Drain each product's per-hour rate × seg_hours via shared logic."""
    if seg_hours <= 0:
        return
    for product, rate_info in rates.items():
        demand = rate_info["lbs_per_hour"] * seg_hours
        # Snapshot tank levels so we can print a per-tank delta after
        # simulate_consume has done its (possibly multi-tank) draining.
        before = {n: i["current_level_lbs"] for n, i in tanks.items()
                  if i["product"] == product}
        print(f"  {product}: {demand:,.1f} lbs demand")
        simulate_consume(tanks, product, demand)
        for name, prev in before.items():
            now = tanks[name]["current_level_lbs"]
            if abs(now - prev) > 0.01:
                print(f"    {name}: {prev:,.1f} -> {now:,.1f}  "
                      f"(consumed {prev - now:,.1f} lbs, status={tanks[name]['status']})")


def deliver_truck(truck):
    """Pour truck into the lowest-level tank via shared logic."""
    product = truck["product"]
    quantity = truck["quantity_lbs"]
    sap = truck["sap_order"]
    print(f"  >> DELIVERY: {sap} | {product} | {quantity:,} lbs")
    before = {n: i["current_level_lbs"] for n, i in tanks.items()
              if i["product"] == product}
    simulate_delivery_no_alert(tanks, truck)
    for name, prev in before.items():
        now = tanks[name]["current_level_lbs"]
        if abs(now - prev) > 0.01:
            print(f"     {name}: +{now - prev:,.1f} lbs -> {now:,.1f}  "
                  f"(status={tanks[name]['status']})")


# --- Build event queue: trucks + run-window edges, in [start_hour, end_hour] ---
events = []

for truck in trucks:
    ah = truck["arrival_run_hour"]
    if start_hour < ah <= end_hour:
        events.append((ah, "delivery", truck))

for window in run_schedule:
    if window["end_hour"] > start_hour and window["start_hour"] < end_hour:
        ws = max(window["start_hour"], start_hour)
        we = min(window["end_hour"], end_hour)
        if ws > start_hour:
            events.append((ws, "run_start", None))
        if we < end_hour:
            events.append((we, "run_end", None))

def event_priority(e):
    order = {"run_end": 0, "run_start": 1, "delivery": 2}
    return (e[0], order[e[1]])

events.sort(key=event_priority)

# Determine initial burning state
burning = False
for window in run_schedule:
    if window["start_hour"] <= start_hour < window["end_hour"]:
        burning = True
        break

clock = start_hour
delivered_sap_orders = []

print(f"(Plant is {'RUNNING' if burning else 'idle'} at start)")

for event in events:
    ev_time, ev_type, payload = event
    seg = ev_time - clock
    if seg > 0:
        if burning:
            print(f"Segment: {format_run_hour(data, clock)} -> {format_run_hour(data, ev_time)} ({seg} hrs, RUNNING)")
            consume_segment(seg)
        else:
            print(f"Segment: {format_run_hour(data, clock)} -> {format_run_hour(data, ev_time)} ({seg} hrs, idle)")
        clock = ev_time

    if ev_type == "run_start":
        burning = True
        print(f"  ** Plant started running at {format_run_hour(data, ev_time)}")
    elif ev_type == "run_end":
        burning = False
        print(f"  ** Plant stopped running at {format_run_hour(data, ev_time)}")
    elif ev_type == "delivery":
        deliver_truck(payload)
        delivered_sap_orders.append(payload["sap_order"])
    print()

if clock < end_hour:
    seg = end_hour - clock
    if burning:
        print(f"Segment: {format_run_hour(data, clock)} -> {format_run_hour(data, end_hour)} ({seg} hrs, RUNNING)")
        consume_segment(seg)
    else:
        print(f"Segment: {format_run_hour(data, clock)} -> {format_run_hour(data, end_hour)} ({seg} hrs, idle)")
    clock = end_hour

data["scheduled_trucks"] = [t for t in trucks if t["sap_order"] not in delivered_sap_orders]
data["current_run_hour"] = end_hour

data = email_hooks.send_alert_emails_if_new(data)

with open("data.json", "w") as f:
    json.dump(data, f, indent=2)

print(f"\nClock now at {format_run_hour(data, end_hour)}.")
print(f"Delivered {len(delivered_sap_orders)} trucks. {len(data['scheduled_trucks'])} remaining.")
print("Data saved.")