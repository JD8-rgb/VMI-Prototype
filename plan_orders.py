"""
Auto-plan truck orders for the upcoming week.

Rules:
- Trigger: combined product level drops below target.
- Target scales linearly between 15,000 lbs (28 run-hrs/wk) and
  27,000 lbs (118 run-hrs/wk).
- Truck arrival must be one of the three allowed delivery slots:
    06:00, 08:00, or 14:00  (Mon-Fri, during an active run window,
    >= 48 h from current_run_hour, inside the target week).
- Default preferred slot is 08:00.
- No two trucks (any product) may arrive in the same slot.
- A slot is skipped if the delivery would overfill — it is better
  to let levels drop (even below safety stock) than to overfill.
- Truck placed at the LATEST valid slot at or before the breach.
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
    find_lowest_in, find_other_in,
)

LEAD_TIME_HOURS      = 48
DELIVERY_SLOTS       = [6, 8, 14]   # allowed delivery hours: 06:00, 08:00, 14:00
TARGET_LOW_RUN_HOURS  = 28
TARGET_HIGH_RUN_HOURS = 118
TARGET_LOW_LBS        = 15000
TARGET_HIGH_LBS       = 27000
MAX_ITERATIONS        = 50


# ---------------------------------------------------------------------------
# Target calculation
# ---------------------------------------------------------------------------

def get_target_for_week(week_run_hours):
    if week_run_hours <= TARGET_LOW_RUN_HOURS:
        return TARGET_LOW_LBS
    if week_run_hours >= TARGET_HIGH_RUN_HOURS:
        return TARGET_HIGH_LBS
    span_hours = TARGET_HIGH_RUN_HOURS - TARGET_LOW_RUN_HOURS
    span_lbs   = TARGET_HIGH_LBS - TARGET_LOW_LBS
    fraction   = (week_run_hours - TARGET_LOW_RUN_HOURS) / span_hours
    return TARGET_LOW_LBS + fraction * span_lbs


def get_target_week_bounds(data):
    """Return (start, end) run-hours for the next Mon-Sun week."""
    current  = data["current_run_hour"]
    now_dt   = run_hour_to_dt(data, current)
    days_until_monday = (7 - now_dt.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    next_monday     = (now_dt + timedelta(days=days_until_monday)).replace(
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


# ---------------------------------------------------------------------------
# Slot generation
# ---------------------------------------------------------------------------

def _all_slot_run_hours(data, range_start, range_end):
    """
    Return all run-hours for the three allowed delivery slots (06:00, 08:00,
    14:00) on Mon-Fri days within [range_start, range_end]. Ascending order.
    """
    slots    = []
    start_dt = run_hour_to_dt(data, range_start).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_dt   = run_hour_to_dt(data, range_end)
    day      = start_dt
    while day < end_dt:
        if day.weekday() < 5:              # Mon–Fri only
            for h in DELIVERY_SLOTS:
                slot_dt = day.replace(hour=h)
                rh      = dt_to_run_hour(data, slot_dt)
                if range_start <= rh <= range_end:
                    slots.append(rh)
        day += timedelta(days=1)
    return sorted(slots)


# ---------------------------------------------------------------------------
# Slot validation
# ---------------------------------------------------------------------------

def is_valid_delivery_slot(data, run_hour, week_start):
    """
    Return True if run_hour meets all base delivery constraints:
      - >= 48 h lead time
      - >= week_start
      - Mon–Fri
      - Hour is one of DELIVERY_SLOTS (06, 08, 14)
      - Falls inside an active run window
    Does NOT check for conflicts or overfill — those are checked separately.
    """
    current = data["current_run_hour"]
    if run_hour < current + LEAD_TIME_HOURS:
        return False
    if run_hour < week_start:
        return False
    dt = run_hour_to_dt(data, run_hour)
    if dt.weekday() >= 5:                  # Sat=5, Sun=6
        return False
    if dt.hour not in DELIVERY_SLOTS:
        return False
    if not is_running_at(data, run_hour):
        return False
    return True


# ---------------------------------------------------------------------------
# Overfill check
# ---------------------------------------------------------------------------

def _project_tanks_to_hour(data, product, target_hour, product_trucks):
    """
    Copy tanks, then simulate consumption + deliveries for product up to
    target_hour. Returns the advanced tanks dict.
    """
    tanks   = copy.deepcopy(data["tanks"])
    rates   = data["consumption_rates"]
    current = data["current_run_hour"]

    pending = sorted(
        [t for t in product_trucks
         if t["product"] == product
         and current < t["arrival_run_hour"] <= target_hour],
        key=lambda t: t["arrival_run_hour"],
    )
    truck_idx = 0
    hour      = current
    while hour < target_hour:
        next_hour = hour + 1
        if is_running_at(data, hour):
            simulate_consume(tanks, product, rates[product]["lbs_per_hour"])
        while (truck_idx < len(pending)
               and pending[truck_idx]["arrival_run_hour"] <= next_hour):
            simulate_delivery_no_alert(tanks, pending[truck_idx])
            truck_idx += 1
        hour = next_hour
    return tanks


def _would_overfill(data, product, slot_rh, product_trucks):
    """
    Return True if delivering a standard truck of product at slot_rh would
    overfill, given already-planned product_trucks. Mirrors the overfill
    logic in alerts.simulate_delivery.

    The planner treats overfill as a hard constraint: it is preferable to
    let levels drop (even below safety stock) rather than to overfill a tank.
    """
    tanks    = _project_tanks_to_hour(data, product, slot_rh, product_trucks)
    quantity = data["truck_quantities"][product]

    target_name = find_lowest_in(tanks, product)
    if target_name is None:
        return False

    target      = tanks[target_name]
    target_space = target["max_capacity_lbs"] - target["current_level_lbs"]
    single_tank_usable = (
        target["max_capacity_lbs"] - target.get("heel_lbs", 0)
    )
    expected_overflow = quantity > single_tank_usable

    other_name  = find_other_in(tanks, product, target_name)
    other_space = (
        (tanks[other_name]["max_capacity_lbs"] - tanks[other_name]["current_level_lbs"])
        if other_name else 0
    )
    total_space = target_space + other_space

    if expected_overflow:
        return total_space < quantity   # truck must fit across both tanks
    else:
        return target_space < quantity  # truck must fit in one tank


# ---------------------------------------------------------------------------
# Slot finders
# ---------------------------------------------------------------------------

def find_latest_valid_slot(
    data, product, latest_hour, earliest_hour, week_start, week_end, all_trucks
):
    """
    Return the latest allowed slot at or before latest_hour that:
      - passes is_valid_delivery_slot
      - is not already booked by any product (conflict avoidance)
      - would not overfill the product's tanks

    Returns None if no such slot exists.
    """
    booked        = {t["arrival_run_hour"] for t in all_trucks}
    product_trucks = [t for t in all_trucks if t["product"] == product]

    candidates = [
        rh for rh in _all_slot_run_hours(data, week_start, week_end)
        if is_valid_delivery_slot(data, rh, week_start)
        and rh <= latest_hour
        and rh >= earliest_hour
    ]

    for slot in reversed(candidates):        # latest first
        if slot in booked:
            continue
        if _would_overfill(data, product, slot, product_trucks):
            continue
        return slot
    return None


def find_earliest_valid_slot(data, product, from_hour, to_hour, all_trucks):
    """
    Return the earliest allowed slot at or after from_hour that:
      - passes is_valid_delivery_slot
      - is not already booked by any product
      - would not overfill

    Used as a fallback when the breach precedes the first run window.
    Returns None if no such slot exists.
    """
    booked        = {t["arrival_run_hour"] for t in all_trucks}
    product_trucks = [t for t in all_trucks if t["product"] == product]

    candidates = [
        rh for rh in _all_slot_run_hours(data, from_hour, to_hour)
        if is_valid_delivery_slot(data, rh, from_hour)
    ]

    for slot in sorted(candidates):          # earliest first
        if slot in booked:
            continue
        if _would_overfill(data, product, slot, product_trucks):
            continue
        return slot
    return None


# ---------------------------------------------------------------------------
# Breach finder (unchanged)
# ---------------------------------------------------------------------------

def find_first_breach_in_target_week(
    data, product, target, week_start, week_end, extra_trucks, breach_floor=None
):
    """
    Walk hour by hour from current to week_end, simulating consumption and
    deliveries. Return the first hour AT OR AFTER max(week_start, breach_floor)
    where combined level drops below target.  Return None if no such breach.
    """
    tanks   = copy.deepcopy(data["tanks"])
    rates   = data["consumption_rates"]
    current = data["current_run_hour"]
    check_from = max(week_start, breach_floor) if breach_floor is not None else week_start

    all_trucks = list(data["scheduled_trucks"]) + list(extra_trucks)
    pending    = sorted(
        [t for t in all_trucks
         if t["product"] == product
         and current < t["arrival_run_hour"] <= week_end],
        key=lambda t: t["arrival_run_hour"],
    )
    truck_idx = 0

    hour = current
    while hour < week_end:
        next_hour = hour + 1
        if is_running_at(data, hour):
            simulate_consume(tanks, product, rates[product]["lbs_per_hour"])
        while (truck_idx < len(pending)
               and pending[truck_idx]["arrival_run_hour"] <= next_hour):
            simulate_delivery_no_alert(tanks, pending[truck_idx])
            truck_idx += 1
        level = get_combined_level_from_tanks(tanks, product)
        if next_hour >= check_from and level < target:
            return next_hour
        hour = next_hour
    return None


# ---------------------------------------------------------------------------
# Main planner
# ---------------------------------------------------------------------------

def plan_for_product(data, product, target, week_start, week_end, extra_trucks):
    new_trucks  = []
    current     = data["current_run_hour"]
    breach_floor = None

    for iteration in range(MAX_ITERATIONS):
        breach_hour = find_first_breach_in_target_week(
            data, product, target, week_start, week_end,
            extra_trucks + new_trucks,
            breach_floor=breach_floor,
        )
        if breach_hour is None:
            return new_trucks

        earliest  = current + LEAD_TIME_HOURS
        all_trucks = (
            list(data["scheduled_trucks"])
            + list(extra_trucks)
            + list(new_trucks)
        )

        slot = find_latest_valid_slot(
            data, product, breach_hour, earliest, week_start, week_end, all_trucks
        )

        if slot is None:
            # Breach falls before the first scheduled run window, or all valid
            # slots are booked / would overfill — try earliest available slot.
            slot = find_earliest_valid_slot(
                data, product, week_start, week_end, all_trucks
            )
            if slot is None:
                print(
                    f"  !! {product}: No valid delivery slot found in target week "
                    f"(all slots may be booked or would overfill). "
                    f"Letting alerts handle the shortfall."
                )
                return new_trucks
            breach_floor = slot + 1
            print(
                f"  {product}: Level depleted at week start — "
                f"placing at earliest valid slot {format_run_hour(data, slot)}"
            )

        quantity  = data["truck_quantities"][product]
        new_truck = {
            "sap_order":      None,
            "product":        product,
            "quantity_lbs":   quantity,
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


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _load_data():
    import os
    path = "data.json" if os.path.exists("data.json") else "defaults.json"
    with open(path) as f:
        return json.load(f)


def main():
    data = _load_data()

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
    prefix    = sap_start_str[:m.start()]
    start_num = int(m.group(1))
    width     = len(m.group(1))

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
