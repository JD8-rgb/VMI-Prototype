"""
Auto-plan truck orders for the upcoming week.

Rules:
- Trigger: combined product level drops below target.
- Target scales linearly between 15,000 lbs (28 run-hrs/wk) and
  30,000 lbs (118 run-hrs/wk).
- Truck arrival must be: Mon-Fri, 06:00-14:00, during an active run window,
  >= 48h from current_run_hour, AND inside the target week.
- Truck placed at the LATEST valid slot at or before the moment the
  combined level would breach the target.
- Iterative: each placed truck changes the projection, so re-simulate.
- Only addresses breaches that occur within the target week. Earlier
  breaches are the responsibility of the alerts system, not the planner.
"""

import json
import sys
import copy
import re
from datetime import datetime, timedelta
from time_utils import (
    get_epoch, run_hour_to_dt, dt_to_run_hour, format_run_hour,
)
import email_hooks
from alerts import (
    simulate_consume, simulate_delivery_no_alert,
    is_running_at, get_combined_level_from_tanks,
)

LEAD_TIME_HOURS = 48
DELIVERY_WINDOW_START = 6   # 06:00
DELIVERY_WINDOW_END = 14    # 14:00
TARGET_LOW_RUN_HOURS = 28
TARGET_HIGH_RUN_HOURS = 118
TARGET_LOW_LBS = 15000
TARGET_HIGH_LBS = 27000
MAX_ITERATIONS = 50


def get_target_for_week(week_run_hours):
    if week_run_hours <= TARGET_LOW_RUN_HOURS:
        return TARGET_LOW_LBS
    if week_run_hours >= TARGET_HIGH_RUN_HOURS:
        return TARGET_HIGH_LBS
    span_hours = TARGET_HIGH_RUN_HOURS - TARGET_LOW_RUN_HOURS
    span_lbs = TARGET_HIGH_LBS - TARGET_LOW_LBS
    fraction = (week_run_hours - TARGET_LOW_RUN_HOURS) / span_hours
    return TARGET_LOW_LBS + fraction * span_lbs


def get_target_week_bounds(data):
    """Return (start, end) run-hours for the next Mon-Sun week."""
    current = data["current_run_hour"]
    now_dt = run_hour_to_dt(data, current)
    days_until_monday = (7 - now_dt.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    next_monday = (now_dt + timedelta(days=days_until_monday)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    next_sunday_end = next_monday + timedelta(days=7)
    return (
        dt_to_run_hour(data, next_monday),
        dt_to_run_hour(data, next_sunday_end),
    )


def get_run_hours_in_window(data, start, end):
    total = 0
    for window in data["run_schedule"]:
        ws = max(window["start_hour"], start)
        we = min(window["end_hour"], end)
        if we > ws:
            total += we - ws
    return total


def is_valid_delivery_moment(data, run_hour, week_start):
    """All delivery constraints, including 'must be inside target week'."""
    current = data["current_run_hour"]
    if run_hour < current + LEAD_TIME_HOURS:
        return False
    if run_hour < week_start:
        return False
    dt = run_hour_to_dt(data, run_hour)
    if dt.weekday() >= 5:  # Sat=5, Sun=6
        return False
    if dt.hour < DELIVERY_WINDOW_START or dt.hour >= DELIVERY_WINDOW_END:
        return False
    if not is_running_at(data, run_hour):
        return False
    return True


def find_latest_valid_slot(data, latest_hour, earliest_hour, week_start):
    """Walk backward from latest_hour to earliest_hour. Return latest valid slot."""
    hour = int(latest_hour)
    floor = max(int(earliest_hour), int(week_start))
    while hour >= floor:
        if is_valid_delivery_moment(data, float(hour), week_start):
            return float(hour)
        hour -= 1
    return None


def find_earliest_valid_slot(data, from_hour, to_hour):
    """
    Walk forward from from_hour to to_hour.
    Return the first valid Mon-Fri 06:00-14:00 delivery slot inside a run window.
    Used as a fallback when the breach occurs before the first run window of the
    target week (e.g. tanks empty entering Monday midnight, first window 6 AM).
    """
    current = data["current_run_hour"]
    hour = max(int(from_hour), int(current + LEAD_TIME_HOURS) + 1)
    while hour <= int(to_hour):
        if is_valid_delivery_moment(data, float(hour), from_hour):
            return float(hour)
        hour += 1
    return None


def find_first_breach_in_target_week(
    data, product, target, week_start, week_end, extra_trucks, breach_floor=None
):
    """
    Walk hour by hour from current to week_end, simulating consumption and
    deliveries. Return the first hour AT OR AFTER max(week_start, breach_floor)
    where combined level drops below target.  Return None if no such breach.

    breach_floor: ignore breaches before this run-hour (used after placing a
    recovery truck at an early slot so the loop doesn't re-detect the same
    pre-run-window depletion in subsequent iterations).
    """
    tanks = copy.deepcopy(data["tanks"])
    rates = data["consumption_rates"]
    current = data["current_run_hour"]
    check_from = max(week_start, breach_floor) if breach_floor is not None else week_start

    all_trucks = list(data["scheduled_trucks"]) + list(extra_trucks)
    pending = sorted(
        [t for t in all_trucks
         if t["product"] == product
         and current < t["arrival_run_hour"] <= week_end],
        key=lambda t: t["arrival_run_hour"]
    )
    truck_idx = 0

    hour = current
    while hour < week_end:
        next_hour = hour + 1
        if is_running_at(data, hour):
            simulate_consume(tanks, product, rates[product]["lbs_per_hour"])
        while truck_idx < len(pending) and pending[truck_idx]["arrival_run_hour"] <= next_hour:
            simulate_delivery_no_alert(tanks, pending[truck_idx])
            truck_idx += 1
        level = get_combined_level_from_tanks(tanks, product)
        if next_hour >= check_from and level < target:
            return next_hour
        hour = next_hour
    return None


def plan_for_product(data, product, target, week_start, week_end, extra_trucks):
    new_trucks = []
    current = data["current_run_hour"]
    breach_floor = None   # advances past early-week pre-window depletions

    for iteration in range(MAX_ITERATIONS):
        breach_hour = find_first_breach_in_target_week(
            data, product, target, week_start, week_end,
            extra_trucks + new_trucks,
            breach_floor=breach_floor,
        )
        if breach_hour is None:
            return new_trucks

        earliest = current + LEAD_TIME_HOURS
        slot = find_latest_valid_slot(data, breach_hour, earliest, week_start)
        if slot is None:
            # Breach falls before the first scheduled run window of the target week
            # (e.g. tanks empty entering Monday midnight, first window 6 AM).
            # Fall back to earliest valid slot, then advance the breach floor past
            # this slot so the next iteration doesn't re-detect the same early breach.
            slot = find_earliest_valid_slot(data, week_start, week_end)
            if slot is None:
                print(f"  !! {product}: No valid Mon-Fri 06:00-14:00 delivery slot "
                      f"found in target week. Apply a run schedule first.")
                return new_trucks
            breach_floor = slot + 1   # <-- advance floor past recovery slot
            print(f"  {product}: Level depleted at week start — "
                  f"placing at earliest valid slot {format_run_hour(data, slot)}")

        quantity = data["truck_quantities"][product]
        new_truck = {
            "sap_order": None,
            "product": product,
            "quantity_lbs": quantity,
            "arrival_run_hour": slot,
            "_planned_reason": (
                f"combined level would drop below {target:,.0f} lbs "
                f"at {format_run_hour(data, breach_hour)}"
            ),
        }
        new_trucks.append(new_truck)
        print(f"  Placed {product} truck at {format_run_hour(data, slot)}")
        print(f"    reason: {new_truck['_planned_reason']}")

    print(f"  !! Hit max iterations ({MAX_ITERATIONS}) for {product}.")
    return new_trucks


def main():
    with open("data.json", "r") as f:
        data = json.load(f)

    week_start, week_end = get_target_week_bounds(data)
    week_run_hours = get_run_hours_in_window(data, week_start, week_end)
    target = get_target_for_week(week_run_hours)

    print("=" * 60)
    print("ORDER PLANNER")
    print("=" * 60)
    print(f"Current: {format_run_hour(data, data['current_run_hour'])}")
    print(f"Planning week: {format_run_hour(data, week_start)}")
    print(f"           to: {format_run_hour(data, week_end)}")
    print(f"Scheduled run hours in target week: {week_run_hours:.1f}")
    print(f"Reorder target (combined): {target:,.0f} lbs")
    print()

    if week_run_hours == 0:
        print("No run hours scheduled for the target week. Nothing to plan.")
        return

    all_new_trucks = []
    for product in data["consumption_rates"].keys():
        print(f"--- Planning {product} ---")
        new = plan_for_product(
            data, product, target, week_start, week_end, all_new_trucks
        )
        all_new_trucks.extend(new)
        if not new:
            print(f"  No new {product} trucks needed.")
        print()

    if not all_new_trucks:
        print("No new trucks needed for the target week.")
        return

    print(f"Planner proposes {len(all_new_trucks)} new truck(s).")
    sap_start_str = input("Enter starting SAP order number (e.g. SAP20001): ").strip()
    if not sap_start_str:
        print("Cancelled.")
        return

    m = re.search(r"(\d+)$", sap_start_str)
    if not m:
        print(f"Error: '{sap_start_str}' has no trailing number to increment.")
        return
    prefix = sap_start_str[:m.start()]
    start_num = int(m.group(1))
    width = len(m.group(1))

    all_new_trucks.sort(key=lambda t: t["arrival_run_hour"])

    for i, truck in enumerate(all_new_trucks):
        truck["sap_order"] = f"{prefix}{str(start_num + i).zfill(width)}"
        truck.pop("_planned_reason", None)
        data["scheduled_trucks"].append(truck)
        print(f"  Added: {truck['sap_order']} | {truck['product']} | "
              f"{truck['quantity_lbs']:,} lbs | "
              f"{format_run_hour(data, truck['arrival_run_hour'])}")

    with open("data.json", "w") as f:
        json.dump(data, f, indent=2)

    print()
    print(f"Added {len(all_new_trucks)} truck(s). Data saved.")
    email_hooks.send_cs_load_entry(data, all_new_trucks)


if __name__ == "__main__":
    main()