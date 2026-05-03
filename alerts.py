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
from config import DEFAULT_CONFIG, PlantConfig
from state import PlantState

# Back-compat re-exports — deprecated in favor of passing a PlantConfig
# instance to algorithm functions. Existing callers that import these
# constants directly continue to work unchanged.
LEAD_TIME_HOURS            = DEFAULT_CONFIG.lead_time_hours
LATE_TRUCK_HOURS           = DEFAULT_CONFIG.late_truck_hours
SAFETY_STOCK_LBS           = DEFAULT_CONFIG.safety_stock_lbs
PROJECTION_WINDOW_HOURS    = DEFAULT_CONFIG.projection_window_hours
PLANT_STATE_MISMATCH_HOURS = DEFAULT_CONFIG.plant_state_mismatch_hours


def _as_state(data_or_state) -> PlantState:
    """Polymorphic entry shim for the dict→dataclass migration.

    Every public function in this module accepts either the legacy
    `data` dict (loaded from data.json) OR a `PlantState` dataclass.
    Internal code uses attribute access throughout. New code SHOULD
    pass PlantState; old code (app.py, advance_time.py, the CLI
    scripts) keeps working without changes.

    The conversion is shallow-cheap: PlantState.from_dict walks the
    top-level keys once. For hot loops, callers that already have a
    PlantState should pass it directly to avoid the per-call walk.
    """
    if isinstance(data_or_state, PlantState):
        return data_or_state
    return PlantState.from_dict(data_or_state)


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
    state = _as_state(data)
    return state.consumption_rates[product].lbs_per_hour


def get_combined_usable(data, product):
    """Total drawable lbs across all tanks for a product.

    Each tank contributes max(0, current_level_lbs - heel_lbs). Without
    the clamp, an empty tank with a non-zero heel (e.g. 0 lbs vs 1,000
    heel) would contribute -1,000 and falsely shrink reported usable
    inventory.
    """
    return _as_state(data).combined_usable_lbs(product)


def get_inbound_total(data, product, within_hours=None):
    """Total scheduled inbound lbs for a product.

    within_hours : if provided, only count trucks arriving in the next
                   `within_hours` (relative to current_run_hour). This
                   matters for lead-time checks: a truck arriving 5 days
                   from now should not mask a shortage in the next 48h.
                   None = legacy behavior (count everything).
    """
    state = _as_state(data)
    trucks = [t for t in state.scheduled_trucks if t.product == product]
    if within_hours is not None:
        current = state.current_run_hour
        horizon = current + within_hours
        trucks = [t for t in trucks if current < t.arrival_run_hour <= horizon]
    return sum(t.quantity_lbs for t in trucks)


def get_scheduled_run_hours_in_window(data, start, end):
    state = _as_state(data)
    total = 0
    for window in state.run_schedule:
        ws = max(window.start_hour, start)
        we = min(window.end_hour, end)
        if we > ws:
            total += we - ws
    return total


def check_lead_time(data, product, cfg: PlantConfig = DEFAULT_CONFIG):
    state = _as_state(data)
    rate = state.consumption_rates[product].lbs_per_hour
    usable = state.combined_usable_lbs(product)
    # Only count trucks arriving inside the lead-time window. A truck
    # scheduled days past lead_time_hours does NOT cover near-term demand,
    # so summing all inbound was masking real shortages.
    inbound = get_inbound_total(state, product, within_hours=cfg.lead_time_hours)
    total_supply = usable + inbound
    current = state.current_run_hour
    scheduled_hours = get_scheduled_run_hours_in_window(
        state, current, current + cfg.lead_time_hours
    )
    if scheduled_hours == 0:
        return None
    demand = scheduled_hours * rate
    if total_supply < demand:
        text = (f"WARNING: {product} supply {total_supply:,.0f} lbs "
                f"(usable {usable:,.0f} + inbound {inbound:,.0f}) "
                f"won't cover next {cfg.lead_time_hours:g}h of scheduled run time "
                f"({scheduled_hours:.0f} run-hrs = {demand:,.0f} lbs). "
                f"Order another truck.")
        return _alert(text, type="lead_time", severity="warning",
                      direction="too_low", product=product,
                      level_lbs=float(total_supply))
    return None


def is_running_at(data, hour, cfg: PlantConfig = DEFAULT_CONFIG):
    """True if the plant is scheduled to be running at this hour.

    Returns False when the hour falls on a date listed in
    cfg.plant_holidays, even if a run window covers it. The schedule
    representation in run_schedule is independent of calendar dates,
    so without this check a plant that "runs Mon-Fri 6-22" would
    silently include Christmas Day inside its windows."""
    state = _as_state(data)
    if cfg.plant_holidays:
        # Only do the date conversion when there are holidays — avoids
        # the per-call dt-conversion cost on the demo path.
        from time_utils import run_hour_to_dt
        iso = run_hour_to_dt(state, hour).date().isoformat()
        if iso in cfg.plant_holidays:
            return False
    for window in state.run_schedule:
        if window.start_hour <= hour < window.end_hour:
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


def find_others_in(tanks, product, exclude):
    """All tanks for `product` except `exclude`, sorted ascending by
    current level. Lowest-first ordering means overflow pouring fills
    the least-full tank before the more-full one — matches operator
    behavior and keeps levels balanced.

    Returns [] when there's only one tank for the product (the 1-tank
    customer topology). Algorithm callers must handle the empty-list
    case the same way they handled `find_other_in` returning None.
    """
    candidates = [(name, info) for name, info in tanks.items()
                  if info["product"] == product and name != exclude]
    candidates.sort(key=lambda pair: pair[1]["current_level_lbs"])
    return [name for name, _ in candidates]


def find_other_in(tanks, product, exclude):
    """Back-compat single-other lookup for the 2-tank-per-product
    topology. Prefer `find_others_in` for new code so 1- and 3+-tank
    customers work without changes."""
    others = find_others_in(tanks, product, exclude)
    return others[0] if others else None


def simulate_consume(tanks, product, lbs):
    """Consume from the lowest above-heel tank for this product.

    Mirrors operator behavior: always drain the tank with less inventory
    first, switch to the other when it hits heel. Status flags are
    recomputed each step so the displayed draw/standby always reflects
    where the plant is actually pulling from — never persisted state from
    a stale prior tick.
    """
    remaining = lbs
    while remaining > 0:
        product_tanks = [(name, info) for name, info in tanks.items()
                         if info["product"] == product]
        if not product_tanks:
            return
        drawable_tanks = [(n, i) for n, i in product_tanks
                          if i["current_level_lbs"] > i.get("heel_lbs", 0)]
        if not drawable_tanks:
            # All tanks at or below heel — nothing left to draw
            return
        draw_name, draw_tank = min(
            drawable_tanks, key=lambda p: p[1]["current_level_lbs"]
        )
        # Reflect reality: the lowest above-heel tank is the draw, others standby
        for _n, i in product_tanks:
            i["status"] = "standby"
        draw_tank["status"] = "draw"

        heel = draw_tank.get("heel_lbs", 0)
        drawable = draw_tank["current_level_lbs"] - heel
        if remaining <= drawable:
            draw_tank["current_level_lbs"] -= remaining
            remaining = 0
        else:
            draw_tank["current_level_lbs"] = heel
            remaining -= drawable
            # Loop iterates and picks the next-lowest above-heel tank.


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
    # Defensive .get on heel_lbs to match plan_orders._would_overfill,
    # which uses the same default. A tank dict missing the heel_lbs key
    # is treated as zero-heel rather than crashing — keeps the overfill
    # check parity between the planner and the alert path.
    single_tank_usable = sample["max_capacity_lbs"] - sample.get("heel_lbs", 0)
    expected_overflow = quantity > single_tank_usable

    target_name = find_lowest_in(tanks, product)
    target = tanks[target_name]
    target_space = target["max_capacity_lbs"] - target["current_level_lbs"]
    other_names = find_others_in(tanks, product, target_name)
    other_spaces = [
        tanks[n]["max_capacity_lbs"] - tanks[n]["current_level_lbs"]
        for n in other_names
    ]
    total_space = target_space + sum(other_spaces)

    alert = None

    if expected_overflow:
        # Product needs more than one tank (e.g. Product M, 37k lbs > single-tank usable)
        # Alert only if the truck won't fit across the target + every other tank for
        # this product combined. Generalizes to any number of tanks (1, 2, 3+).
        if total_space < quantity:
            others_label = (" + ".join(other_names) if other_names
                            else "no other tank")
            text = (f"RED FLAG: {sap} ({product}, {quantity:,} lbs) at {arrival_label} — "
                    f"projected combined tank space is {total_space:,.0f} lbs "
                    f"({target_name} + {others_label}). "
                    f"Truck cannot fit across all tanks. Reschedule or delay.")
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
    # Cascade overflow into other tanks lowest-current-level first. With
    # 1 tank the loop is a no-op; with 2 it matches the prior behavior
    # exactly; with 3+ it fills tanks in order until the truck is empty.
    for name, space in zip(other_names, other_spaces):
        if overflow <= 0:
            break
        pour = min(overflow, space)
        tanks[name]["current_level_lbs"] += pour
        overflow -= pour

    _refresh_draw_status(tanks, product)
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
    # Cascade overflow through every other tank (lowest-first) so 1- and
    # 3+-tank topologies work without changes.
    for other_name in find_others_in(tanks, product, target_name):
        if overflow <= 0:
            break
        other = tanks[other_name]
        other_space = other["max_capacity_lbs"] - other["current_level_lbs"]
        pour = min(overflow, other_space)
        other["current_level_lbs"] += pour
        overflow -= pour
    _refresh_draw_status(tanks, product)


def _refresh_draw_status(tanks, product):
    """Recompute draw/standby flags so display matches reality.

    The lowest above-heel tank is the draw; everything else for that
    product is standby. If no tank has drawable inventory the draw flag
    falls onto the lowest tank (so the UI still has *something* labeled).
    """
    product_tanks = [(n, i) for n, i in tanks.items()
                     if i["product"] == product]
    if not product_tanks:
        return
    drawable = [(n, i) for n, i in product_tanks
                if i["current_level_lbs"] > i.get("heel_lbs", 0)]
    pool = drawable if drawable else product_tanks
    draw_name, _ = min(pool, key=lambda p: p[1]["current_level_lbs"])
    for n, i in product_tanks:
        i["status"] = "draw" if n == draw_name else "standby"


def run_projection(data, cfg: PlantConfig = DEFAULT_CONFIG):
    """
    Walk forward 1 hour at a time. At each step:
      - if running, consume per-product
      - check safety stock for each product
      - if a truck arrives this hour, deliver it and check overfill

    Returns a list of alert dicts (see `_alert`). Duplicate safety-stock
    alerts for the same product are suppressed within a single projection.

    Caller can pass a `data` dict (legacy) or a `PlantState`.
    """
    state = _as_state(data)
    # Convert state.tanks (Dict[str, TankState]) → dict-of-dicts here
    # because simulate_consume/simulate_delivery still operate on the
    # legacy shape. That migration is its own pass (the mutation
    # functions need the deeper "return new tanks" refactor).
    tanks = copy.deepcopy(state.to_dict()["tanks"])
    products = list(state.consumption_rates.keys())
    current = state.current_run_hour
    end = current + cfg.projection_window_hours

    pending = sorted(
        [t for t in state.scheduled_trucks
         if current < t.arrival_run_hour <= end],
        key=lambda t: t.arrival_run_hour,
    )
    truck_idx = 0

    alerts = []
    seen_safety = set()

    hour = current
    while hour < end:
        next_hour = hour + 1

        if is_running_at(state, hour, cfg=cfg):
            for product in products:
                simulate_consume(tanks, product, state.consumption_rates[product].lbs_per_hour)

        while truck_idx < len(pending) and pending[truck_idx].arrival_run_hour <= next_hour:
            truck = pending[truck_idx]
            alert = simulate_delivery(tanks, truck.to_dict(), data=state)
            if alert:
                alerts.append(alert)
            truck_idx += 1

        for product in products:
            level = get_combined_level_from_tanks(tanks, product)
            if level < cfg.safety_stock_lbs and product not in seen_safety:
                text = (
                    f"RED FLAG: {product} projected to drop to {level:,.0f} lbs "
                    f"at {format_run_hour(state, next_hour)} — below {cfg.safety_stock_lbs:,.0f} lb "
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


def check_late_trucks(data, cfg: PlantConfig = DEFAULT_CONFIG):
    """Return alert dicts for any truck >= cfg.late_truck_hours past its arrival time."""
    state = _as_state(data)
    current = state.current_run_hour
    alerts = []
    for truck in state.scheduled_trucks:
        overdue = current - truck.arrival_run_hour
        # Inclusive (>=) so the documented "3+ hours" threshold actually
        # fires at exactly 3.0 hours. Was `>` which silently waited until
        # 3.0001 hours.
        if overdue >= cfg.late_truck_hours:
            text = (
                f"LATE TRUCK: {truck.sap_order} ({truck.product}, "
                f"{truck.quantity_lbs:,} lbs) was due "
                f"{format_run_hour(state, truck.arrival_run_hour)} — "
                f"{overdue:.0f} hrs overdue. Please verify delivery."
            )
            alerts.append(_alert(
                text, type="late_truck", severity="warning",
                direction="too_low", product=truck.product,
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
    state = _as_state(data)
    alerts = []

    # ── Low-confidence parse ──────────────────────────────────────────────────
    if state.schedule_parse_issue:
        alerts.append(_alert(
            "WARNING: Schedule email received but could not be parsed — "
            "enter the schedule manually using the Schedule Parser.",
            type="schedule_parse", severity="warning", direction="other",
        ))

    # ── Friday schedule deadline alerts ──────────────────────────────────────
    epoch   = datetime.fromisoformat(state.simulation_epoch)
    sim_now = epoch + timedelta(hours=state.current_run_hour)
    if sim_now.weekday() == 4 and sim_now.hour >= 11:   # Friday, 11 AM or later
        days_ahead = (7 - sim_now.weekday()) % 7 or 7
        next_mon   = (sim_now + timedelta(days=days_ahead)).date().isoformat()
        next_mon_display = next_mon[5:].replace("-", "/").lstrip("0")  # "04/27" → "4/27"
        if state.schedule_received_for_week != next_mon:
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


def check_plant_state_mismatch(data, cfg: PlantConfig = DEFAULT_CONFIG):
    """
    Compare actual plant running state (from real-time telemetry) against the
    scheduled state. Fires a RED alert if the two diverge for more than
    cfg.plant_state_mismatch_hours — e.g. the plant is running when the
    schedule says it's down, or down when the schedule says it's running.

    In the production tool this reads live telemetry from the plant historian.
    The simulation assumes perfect schedule adherence, so this check only
    fires when data["plant_state_override"] has been populated for testing.

    Override format:
        data["plant_state_override"] = {
            "actual":     "running" | "down",
            "since_hour": float   # run-hour when this state began
        }
    """
    state = _as_state(data)
    override = state.plant_state_override
    if not override:
        return []
    actual = override.get("actual")
    since  = override.get("since_hour", 0)
    current = state.current_run_hour
    duration = current - since
    if duration < cfg.plant_state_mismatch_hours:
        return []
    scheduled_state = "running" if is_running_at(state, current, cfg=cfg) else "down"
    if actual == scheduled_state:
        return []
    return [_alert(
        f"RED FLAG: Plant state mismatch — actual plant state is '{actual}' "
        f"for {duration:.0f}+ hrs but schedule says '{scheduled_state}'. "
        f"Verify plant status and/or update the schedule.",
        type="plant_state", severity="red_flag", direction="other",
    )]


def check_vmi_off(data, cfg: PlantConfig = DEFAULT_CONFIG):
    """RED weekly alert when the operator has turned VMI automation off.

    Fires every Friday at or after 09:00 sim time, until the operator
    flips the toggle back on. The point is "you turned this off and
    forgot" — a one-shot alert is too easy to miss, so this re-fires
    weekly. The dedup is week-scoped via state.alerted_hashes (the
    alert text includes the target week so the hash differs by week).

    When VMI automation is enabled (the default), returns [].
    """
    state = _as_state(data)
    if state.vmi_automation_enabled:
        return []

    from datetime import datetime, timedelta
    epoch   = datetime.fromisoformat(state.simulation_epoch)
    sim_now = epoch + timedelta(hours=state.current_run_hour)
    # Friday weekday() == 4, hour-of-day >= 9
    if sim_now.weekday() != 4 or sim_now.hour < 9:
        return []

    # Include the target week in the alert text so weekly re-firings
    # produce different hashes (alerted_hashes dedup is per-text).
    days_ahead = (7 - sim_now.weekday()) % 7 or 7
    next_mon   = (sim_now + timedelta(days=days_ahead)).date().isoformat()
    return [_alert(
        f"RED FLAG: VMI automation is OFF for week of {next_mon} — "
        f"no truck orders will be auto-placed this week. "
        f"Re-enable in the VMI Controls panel if this is unintended.",
        type="vmi_off", severity="red_flag", direction="other",
    )]


def get_all_alerts(data, cfg: PlantConfig = DEFAULT_CONFIG):
    """
    Aggregate every active alert. Returns a list of alert dicts (see `_alert`).
    Consumers read ``a["text"]`` for the human-readable body; the other fields
    power the persistent alert log in data.json.

    `cfg` is threaded through every downstream check so per-customer
    overrides apply consistently to the entire alert evaluation.

    Caller can pass a `data` dict (legacy) or a `PlantState` (new). The
    state object is computed once at entry and reused across every
    downstream call to avoid the per-check conversion cost.
    """
    state = _as_state(data)
    alerts = []
    for product in state.consumption_rates.keys():
        lead_alert = check_lead_time(state, product, cfg=cfg)
        if lead_alert:
            alerts.append(lead_alert)
    alerts.extend(run_projection(state, cfg=cfg))
    alerts.extend(check_late_trucks(state, cfg=cfg))
    alerts.extend(check_schedule_alerts(state))
    alerts.extend(check_plant_state_mismatch(state, cfg=cfg))
    alerts.extend(check_vmi_off(state, cfg=cfg))
    return alerts