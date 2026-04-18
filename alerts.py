"""
Alert logic for the VMI prototype.

Walks forward from current_run_hour by 1-hour steps, simulating consumption
(only during scheduled run windows), tank switching at heel, and truck
deliveries. At each step, checks:

1. SAFETY STOCK: combined level for any product below SAFETY_STOCK_LBS.
2. OVERFILL: when a truck arrives, will it fit?
   - Product where truck size > single tank usable capacity (e.g. Product M):
     alert if total space across BOTH tanks < truck quantity.
   - Product where truck size <= single tank usable capacity (e.g. Product U):
     alert if the lowest tank's space < truck quantity.

Also retains the lead-time warning: usable + inbound vs. demand from
next LEAD_TIME_HOURS of scheduled run time.
"""

import copy
from time_utils import format_run_hour

LEAD_TIME_HOURS = 48
LATE_TRUCK_HOURS = 3    # alert if truck is this many hours past its arrival time
SAFETY_STOCK_LBS = 10000
PROJECTION_WINDOW_HOURS = 168
PLANT_STATE_MISMATCH_HOURS = 3   # alert if plant state is off-schedule this many hrs


# ---------------------------------------------------------------------------
# Alert dict helper
#
# Every alert in the system is a dict with the shape below. Each field is
# non-optional — call sites pass None for anything that doesn't apply —
# so downstream consumers can rely on key presence.
#
#   text      (str)   : human-readable alert body, same prefix convention
#                      as before ("RED FLAG: ...", "WARNING: ...", "LATE TRUCK: ...").
#   type      (str)   : "safety_stock" | "overfill" | "lead_time" | "late_truck"
#                       | "schedule_parse" | "schedule_deadline" | "plant_state"
#   severity  (str)   : "red_flag" | "warning"
#   direction (str)   : "too_low" | "too_full" | "other"
#   product   (str | None) : e.g. "Product U"; None if the alert isn't product-scoped.
#   tank      (str | None) : e.g. "U-Tank1"; None for product-scoped alerts.
#   level_lbs (float | None): tank/combined fill level snapshot at emission time,
#                       or None if not applicable (schedule/plant-state alerts).
#
# The alert log in data.json copies each of these fields verbatim plus a
# logged_at_iso timestamp and the dedup hash — see email_hooks.send_alert_emails_if_new.
# ---------------------------------------------------------------------------

def _alert(text, type, severity, direction,
           product=None, tank=None, level_lbs=None):
    return {
        "text":      text,
        "type":      type,
        "severity":  severity,
        "direction": direction,
        "product":   product,
        "tank":      tank,
        "level_lbs": level_lbs,
    }


def get_lbs_per_hour(data, product):
    return data["consumption_rates"][product]["lbs_per_hour"]


def get_combined_usable(data, product):
    return sum(
        info["current_level_lbs"] - info["heel_lbs"]
        for info in data["tanks"].values()
        if info["product"] == product
    )


def get_inbound_total(data, product):
    return sum(
        t["quantity_lbs"]
        for t in data["scheduled_trucks"]
        if t["product"] == product
    )


def get_scheduled_run_hours_in_window(data, start, end):
    total = 0
    for window in data["run_schedule"]:
        ws = max(window["start_hour"], start)
        we = min(window["end_hour"], end)
        if we > ws:
            total += we - ws
    return total


def check_lead_time(data, product):
    rate = get_lbs_per_hour(data, product)
    usable = get_combined_usable(data, product)
    inbound = get_inbound_total(data, product)
    total_supply = usable + inbound
    current = data["current_run_hour"]
    scheduled_hours = get_scheduled_run_hours_in_window(
        data, current, current + LEAD_TIME_HOURS
    )
    if scheduled_hours == 0:
        return None
    demand = scheduled_hours * rate
    if total_supply < demand:
        text = (f"WARNING: {product} supply {total_supply:,.0f} lbs "
                f"(usable {usable:,.0f} + inbound {inbound:,.0f}) "
                f"won't cover next {LEAD_TIME_HOURS}h of scheduled run time "
                f"({scheduled_hours:.0f} run-hrs = {demand:,.0f} lbs). "
                f"Order another truck.")
        return _alert(text, type="lead_time", severity="warning",
                      direction="too_low", product=product,
                      level_lbs=float(total_supply))
    return None


def is_running_at(data, hour):
    """True if the plant is scheduled to be running at this hour."""
    for window in data["run_schedule"]:
        if window["start_hour"] <= hour < window["end_hour"]:
            return True
    return False


def get_combined_level_from_tanks(tanks, product):
    return sum(
        info["current_level_lbs"]
        for info in tanks.values()
        if info["product"] == product
    )


def find_draw_in(tanks, product):
    for name, info in tanks.items():
        if info["product"] == product and info["status"] == "draw":
            return name
    return None


def find_standby_in(tanks, product):
    for name, info in tanks.items():
        if info["product"] == product and info["status"] == "standby":
            return name
    return None


def find_lowest_in(tanks, product):
    candidates = [(name, info["current_level_lbs"])
                  for name, info in tanks.items()
                  if info["product"] == product]
    if not candidates:
        return None
    candidates.sort(key=lambda pair: pair[1])
    return candidates[0][0]


def find_other_in(tanks, product, exclude):
    for name, info in tanks.items():
        if info["product"] == product and name != exclude:
            return name
    return None


def simulate_consume(tanks, product, lbs):
    """Consume from draw tank, switch at heel. Mutates the tanks dict."""
    remaining = lbs
    while remaining > 0:
        draw_name = find_draw_in(tanks, product)
        if draw_name is None:
            return
        draw_tank = tanks[draw_name]
        drawable = draw_tank["current_level_lbs"] - draw_tank["heel_lbs"]
        if drawable <= 0:
            standby_name = find_standby_in(tanks, product)
            if standby_name is None:
                draw_tank["current_level_lbs"] -= remaining
                return
            standby_tank = tanks[standby_name]
            if standby_tank["current_level_lbs"] - standby_tank["heel_lbs"] <= 0:
                # Both tanks at or below heel — nothing left to draw
                return
            draw_tank["status"] = "standby"
            tanks[standby_name]["status"] = "draw"
            continue
        if remaining <= drawable:
            draw_tank["current_level_lbs"] -= remaining
            remaining = 0
        else:
            draw_tank["current_level_lbs"] = draw_tank["heel_lbs"]
            remaining -= drawable
            standby_name = find_standby_in(tanks, product)
            if standby_name is None:
                return
            draw_tank["status"] = "standby"
            tanks[standby_name]["status"] = "draw"


def simulate_delivery(tanks, truck, data=None):
    """
    Pour a truck into the lowest tank, overflow to the other.
    Returns an alert dict (see `_alert`) if overfill conditions are violated,
    else None. Pass data to show human-readable arrival times instead of raw
    run-hours.
    """
    product = truck["product"]
    quantity = truck["quantity_lbs"]
    sap = truck["sap_order"]
    arrival_label = (
        format_run_hour(data, truck["arrival_run_hour"])
        if data else f"run-hour {truck['arrival_run_hour']:.0f}"
    )

    product_tanks = [(name, info) for name, info in tanks.items()
                     if info["product"] == product]
    if not product_tanks:
        return None
    sample = product_tanks[0][1]
    single_tank_usable = sample["max_capacity_lbs"] - sample["heel_lbs"]
    expected_overflow = quantity > single_tank_usable

    target_name = find_lowest_in(tanks, product)
    target = tanks[target_name]
    target_space = target["max_capacity_lbs"] - target["current_level_lbs"]
    other_name = find_other_in(tanks, product, target_name)
    other = tanks[other_name] if other_name else None
    other_space = (other["max_capacity_lbs"] - other["current_level_lbs"]) if other else 0
    total_space = target_space + other_space

    alert = None

    if expected_overflow:
        # Product spans both tanks (e.g. Product M, 37k lbs > single-tank usable)
        # Alert only if the truck won't fit across BOTH tanks combined.
        if total_space < quantity:
            text = (f"RED FLAG: {sap} ({product}, {quantity:,} lbs) at {arrival_label} — "
                    f"projected combined tank space is {total_space:,.0f} lbs "
                    f"({target_name} + {other_name or 'no other tank'}). "
                    f"Truck cannot fit across both tanks. Reschedule or delay.")
            alert = _alert(text, type="overfill", severity="red_flag",
                           direction="too_full", product=product,
                           tank=target_name,
                           level_lbs=float(target["current_level_lbs"]))
    else:
        # Product must fit in a single tank (e.g. Product U, 33k lbs ≤ single-tank usable)
        # Alert if the lowest tank doesn't have enough room.
        if target_space < quantity:
            text = (f"RED FLAG: {sap} ({product}, {quantity:,} lbs) at {arrival_label} — "
                    f"projected space in {target_name} is {target_space:,.0f} lbs. "
                    f"Delivery must fit in one tank. Arriving too early — reschedule later.")
            alert = _alert(text, type="overfill", severity="red_flag",
                           direction="too_full", product=product,
                           tank=target_name,
                           level_lbs=float(target["current_level_lbs"]))

    pour_into_target = min(quantity, target_space)
    target["current_level_lbs"] += pour_into_target
    overflow = quantity - pour_into_target
    if overflow > 0 and other:
        pour_into_other = min(overflow, other_space)
        other["current_level_lbs"] += pour_into_other

    return alert


def simulate_delivery_no_alert(tanks, truck):
    """Same as simulate_delivery but without the overfill check. For planner use."""
    product = truck["product"]
    quantity = truck["quantity_lbs"]
    target_name = find_lowest_in(tanks, product)
    if target_name is None:
        return
    target = tanks[target_name]
    target_space = target["max_capacity_lbs"] - target["current_level_lbs"]
    pour_into_target = min(quantity, target_space)
    target["current_level_lbs"] += pour_into_target
    overflow = quantity - pour_into_target
    if overflow > 0:
        other_name = find_other_in(tanks, product, target_name)
        if other_name:
            other = tanks[other_name]
            other_space = other["max_capacity_lbs"] - other["current_level_lbs"]
            pour_into_other = min(overflow, other_space)
            other["current_level_lbs"] += pour_into_other


def run_projection(data):
    """
    Walk forward 1 hour at a time. At each step:
      - if running, consume per-product
      - check safety stock for each product
      - if a truck arrives this hour, deliver it and check overfill

    Returns a list of alert dicts (see `_alert`). Duplicate safety-stock
    alerts for the same product are suppressed within a single projection.
    """
    tanks = copy.deepcopy(data["tanks"])
    rates = data["consumption_rates"]
    products = list(rates.keys())
    current = data["current_run_hour"]
    end = current + PROJECTION_WINDOW_HOURS

    pending = sorted(
        [t for t in data["scheduled_trucks"]
         if current < t["arrival_run_hour"] <= end],
        key=lambda t: t["arrival_run_hour"]
    )
    truck_idx = 0

    alerts = []
    seen_safety = set()

    hour = current
    while hour < end:
        next_hour = hour + 1

        if is_running_at(data, hour):
            for product in products:
                simulate_consume(tanks, product, rates[product]["lbs_per_hour"])

        while truck_idx < len(pending) and pending[truck_idx]["arrival_run_hour"] <= next_hour:
            truck = pending[truck_idx]
            alert = simulate_delivery(tanks, truck, data=data)
            if alert:
                alerts.append(alert)
            truck_idx += 1

        for product in products:
            level = get_combined_level_from_tanks(tanks, product)
            if level < SAFETY_STOCK_LBS and product not in seen_safety:
                text = (
                    f"RED FLAG: {product} projected to drop to {level:,.0f} lbs "
                    f"at {format_run_hour(data, next_hour)} — below {SAFETY_STOCK_LBS:,} lb "
                    f"safety stock. Add trucks or check the schedule."
                )
                alerts.append(_alert(
                    text, type="safety_stock", severity="red_flag",
                    direction="too_low", product=product,
                    level_lbs=float(level),
                ))
                seen_safety.add(product)

        hour = next_hour

    return alerts


def check_late_trucks(data):
    """Return alert dicts for any truck more than LATE_TRUCK_HOURS past its arrival time."""
    current = data["current_run_hour"]
    alerts = []
    for truck in data["scheduled_trucks"]:
        overdue = current - truck["arrival_run_hour"]
        if overdue > LATE_TRUCK_HOURS:
            text = (
                f"LATE TRUCK: {truck['sap_order']} ({truck['product']}, "
                f"{truck['quantity_lbs']:,} lbs) was due "
                f"{format_run_hour(data, truck['arrival_run_hour'])} — "
                f"{overdue:.0f} hrs overdue. Please verify delivery."
            )
            alerts.append(_alert(
                text, type="late_truck", severity="warning",
                direction="too_low", product=truck.get("product"),
            ))
    return alerts


def check_schedule_alerts(data):
    """
    Three schedule-related alerts:
    1. Low-confidence parse: an email was found but couldn't be reliably parsed.
    2. Friday 11 AM–2:59 PM: reminder sent, still waiting for schedule.
    3. Friday 3 PM+: no schedule received yet for next week (replaces #2).
    """
    from datetime import datetime, timedelta
    alerts = []

    # ── Low-confidence parse ──────────────────────────────────────────────────
    issue = data.get("schedule_parse_issue")
    if issue:
        alerts.append(_alert(
            "WARNING: Schedule email received but could not be parsed — "
            "enter the schedule manually using the Schedule Parser.",
            type="schedule_parse", severity="warning", direction="other",
        ))

    # ── Friday schedule deadline alerts ──────────────────────────────────────
    epoch   = datetime.fromisoformat(data["simulation_epoch"])
    sim_now = epoch + timedelta(hours=data["current_run_hour"])
    if sim_now.weekday() == 4 and sim_now.hour >= 11:   # Friday, 11 AM or later
        days_ahead = (7 - sim_now.weekday()) % 7 or 7
        next_mon   = (sim_now + timedelta(days=days_ahead)).date().isoformat()
        next_mon_display = next_mon[5:].replace("-", "/").lstrip("0")  # "04/27" → "4/27"
        if data.get("schedule_received_for_week") != next_mon:
            if sim_now.hour >= 15:
                # 3 PM or later — critical: missed the deadline
                alerts.append(_alert(
                    f"RED FLAG: No schedule received for week of {next_mon_display} "
                    f"by Friday 3 PM — reminder email sent to customer contact.",
                    type="schedule_deadline", severity="red_flag", direction="other",
                ))
            else:
                # 11 AM–2:59 PM — initial reminder sent, still waiting
                alerts.append(_alert(
                    f"WARNING: No schedule received for week of {next_mon_display} — "
                    f"reminder email sent to customer contact at 11 AM.",
                    type="schedule_deadline", severity="warning", direction="other",
                ))

    return alerts


def check_plant_state_mismatch(data):
    """
    Compare actual plant running state (from real-time telemetry) against the
    scheduled state. Fires a RED alert if the two diverge for more than
    PLANT_STATE_MISMATCH_HOURS — e.g. the plant is running when the schedule
    says it's down, or down when the schedule says it's running.

    In the production tool this reads live telemetry from the plant historian.
    The simulation assumes perfect schedule adherence, so this check only
    fires when data["plant_state_override"] has been populated for testing.

    Override format:
        data["plant_state_override"] = {
            "actual":     "running" | "down",
            "since_hour": float   # run-hour when this state began
        }
    """
    override = data.get("plant_state_override")
    if not override:
        return []
    actual = override.get("actual")
    since  = override.get("since_hour", 0)
    current = data["current_run_hour"]
    duration = current - since
    if duration < PLANT_STATE_MISMATCH_HOURS:
        return []
    scheduled_state = "running" if is_running_at(data, current) else "down"
    if actual == scheduled_state:
        return []
    return [_alert(
        f"RED FLAG: Plant state mismatch — actual plant state is '{actual}' "
        f"for {duration:.0f}+ hrs but schedule says '{scheduled_state}'. "
        f"Verify plant status and/or update the schedule.",
        type="plant_state", severity="red_flag", direction="other",
    )]


def get_all_alerts(data):
    """
    Aggregate every active alert. Returns a list of alert dicts (see `_alert`).
    Consumers read ``a["text"]`` for the human-readable body; the other fields
    power the persistent alert log in data.json.
    """
    alerts = []
    for product in data["consumption_rates"].keys():
        lead_alert = check_lead_time(data, product)
        if lead_alert:
            alerts.append(lead_alert)
    alerts.extend(run_projection(data))
    alerts.extend(check_late_trucks(data))
    alerts.extend(check_schedule_alerts(data))
    alerts.extend(check_plant_state_mismatch(data))
    return alerts