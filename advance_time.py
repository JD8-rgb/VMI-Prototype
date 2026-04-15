"""
Advance the simulation clock by N run-hours, processing:
- Truck deliveries at their arrival_run_hour
- Run-schedule windows (consumption only happens inside scheduled windows)
"""

import json
import sys
from time_utils import parse_time_input, format_run_hour
import email_hooks

if len(sys.argv) < 2:
    print("Usage: python advance_time.py <hours_or_target_datetime>")
    print("Examples:")
    print("  python advance_time.py 8")
    print('  python advance_time.py "2026-04-22 17:00"')
    sys.exit(1)

with open("data.json", "r") as f:
    data = json.load(f)

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


def find_draw_tank(product):
    for name, info in tanks.items():
        if info["product"] == product and info["status"] == "draw":
            return name
    return None


def find_standby_tank(product):
    for name, info in tanks.items():
        if info["product"] == product and info["status"] == "standby":
            return name
    return None


def find_lowest_tank(product):
    candidates = [(name, info["current_level_lbs"])
                  for name, info in tanks.items()
                  if info["product"] == product]
    if not candidates:
        return None
    candidates.sort(key=lambda pair: pair[1])
    return candidates[0][0]


def find_other_tank(product, exclude_name):
    for name, info in tanks.items():
        if info["product"] == product and name != exclude_name:
            return name
    return None


def consume_product(product, lbs_to_consume):
    remaining = lbs_to_consume
    while remaining > 0:
        draw_name = find_draw_tank(product)
        if draw_name is None:
            print(f"  !! No draw tank for {product}. {remaining:,.1f} lbs unmet.")
            return
        draw_tank = tanks[draw_name]
        current = draw_tank["current_level_lbs"]
        heel = draw_tank["heel_lbs"]
        drawable = current - heel

        if drawable <= 0:
            print(f"  {draw_name} at/below heel ({current:,.1f}). Switching.")
            standby_name = find_standby_tank(product)
            if standby_name is None:
                new_level = current - remaining
                draw_tank["current_level_lbs"] = round(new_level, 1)
                print(f"  !! No standby for {product}. {draw_name} -> {new_level:,.1f} (NEGATIVE).")
                return
            draw_tank["status"] = "standby"
            tanks[standby_name]["status"] = "draw"
            print(f"  Switched draw: {draw_name} -> {standby_name}")
            continue

        if remaining <= drawable:
            new_level = current - remaining
            draw_tank["current_level_lbs"] = round(new_level, 1)
            print(f"  {draw_name}: consumed {remaining:,.1f} lbs ({current:,.1f} -> {new_level:,.1f})")
            remaining = 0
        else:
            draw_tank["current_level_lbs"] = round(heel, 1)
            print(f"  {draw_name}: consumed {drawable:,.1f} lbs ({current:,.1f} -> {heel:,.1f}) — reached heel")
            remaining -= drawable
            standby_name = find_standby_tank(product)
            if standby_name is None:
                print(f"  !! No standby for {product}. {remaining:,.1f} lbs unmet.")
                return
            draw_tank["status"] = "standby"
            tanks[standby_name]["status"] = "draw"
            print(f"  Switched draw: {draw_name} -> {standby_name}")


def consume_segment(seg_hours):
    if seg_hours <= 0:
        return
    for product, rate_info in rates.items():
        demand = rate_info["lbs_per_hour"] * seg_hours
        print(f"  {product}: {demand:,.1f} lbs demand")
        consume_product(product, demand)


def deliver_truck(truck):
    product = truck["product"]
    quantity = truck["quantity_lbs"]
    sap = truck["sap_order"]
    print(f"  >> DELIVERY: {sap} | {product} | {quantity:,} lbs")

    target_name = find_lowest_tank(product)
    if target_name is None:
        print(f"  !! No tank for {product}. Aborted.")
        return

    target = tanks[target_name]
    original_status = target["status"]
    target["status"] = "receiving"
    space = target["max_capacity_lbs"] - target["current_level_lbs"]
    to_pour = min(quantity, space)
    target["current_level_lbs"] = round(target["current_level_lbs"] + to_pour, 1)
    print(f"     {target_name}: +{to_pour:,.1f} lbs -> {target['current_level_lbs']:,.1f}")
    overflow = quantity - to_pour
    target["status"] = original_status

    if overflow > 0:
        other_name = find_other_tank(product, target_name)
        if other_name is None:
            print(f"  !! Overflow {overflow:,.1f} lbs and no other tank. LOST.")
            return
        other = tanks[other_name]
        other_original = other["status"]
        other["status"] = "receiving"
        other_space = other["max_capacity_lbs"] - other["current_level_lbs"]
        other_pour = min(overflow, other_space)
        other["current_level_lbs"] = round(other["current_level_lbs"] + other_pour, 1)
        print(f"     {other_name} (overflow): +{other_pour:,.1f} lbs -> {other['current_level_lbs']:,.1f}")
        other["status"] = other_original
        still_over = overflow - other_pour
        if still_over > 0:
            print(f"  !! {still_over:,.1f} lbs could not fit in either tank. LOST.")


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