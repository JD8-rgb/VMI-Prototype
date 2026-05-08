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
import logging
import sys
import copy
import re
from datetime import datetime, timedelta
from time_utils import (
    get_epoch, run_hour_to_dt, dt_to_run_hour, format_run_hour,
)
import email_hooks

logger = logging.getLogger(__name__)
from alerts import (
    simulate_consume, simulate_delivery_no_alert,
    is_running_at, get_combined_level_from_tanks,
    find_lowest_in, find_others_in,
)
from config import DEFAULT_CONFIG, PlantConfig
# Use the public `as_state` alias (state.py); the underscore-prefixed
# `_as_state` triggers an opaque ImportError on Streamlit Cloud for
# cross-module imports.
from state import PlantState, as_state as _as_state

# Back-compat re-exports — deprecated in favor of passing PlantConfig.
LEAD_TIME_HOURS       = DEFAULT_CONFIG.lead_time_hours
DELIVERY_SLOTS        = list(DEFAULT_CONFIG.delivery_slots)
TARGET_LOW_RUN_HOURS  = DEFAULT_CONFIG.target_low_run_hours
TARGET_HIGH_RUN_HOURS = DEFAULT_CONFIG.target_high_run_hours
TARGET_LOW_LBS        = DEFAULT_CONFIG.target_low_lbs
TARGET_HIGH_LBS       = DEFAULT_CONFIG.target_high_lbs
MAX_ITERATIONS        = 50


# ---------------------------------------------------------------------------
# Target calculation
# ---------------------------------------------------------------------------

def get_target_for_week(week_run_hours, cfg: PlantConfig = DEFAULT_CONFIG,
                          state=None):
    """Reorder target for a week given its scheduled run hours.

    Consults the operator's `target_overrides` first (set via the
    Streamlit "VMI Controls" panel and persisted in PlantState). When
    overrides are present, the target curve uses cfg's run-hour x-axis
    but the operator's lbs y-values; otherwise falls back to the cfg
    curve. The override values are clamped to cfg.tunable_* bounds so
    a stale override that no longer fits the customer's window won't
    produce nonsense.
    """
    # Pull override from either dict or PlantState shape (polymorphic
    # to match the rest of the codebase). None when no override active.
    overrides = None
    if state is not None:
        if hasattr(state, "target_overrides"):
            overrides = state.target_overrides
        elif isinstance(state, dict):
            overrides = state.get("target_overrides")

    if overrides and "low" in overrides and "high" in overrides:
        low_lbs  = max(cfg.tunable_low_min,
                        min(cfg.tunable_low_max, float(overrides["low"])))
        high_lbs = max(cfg.tunable_high_min,
                        min(cfg.tunable_high_max, float(overrides["high"])))
        # Same interpolation as PlantConfig.target_for_week, but with
        # the operator's lbs values substituted.
        if week_run_hours <= cfg.target_low_run_hours:
            return low_lbs
        if week_run_hours >= cfg.target_high_run_hours:
            return high_lbs
        span_hours = cfg.target_high_run_hours - cfg.target_low_run_hours
        span_lbs   = high_lbs - low_lbs
        fraction   = (week_run_hours - cfg.target_low_run_hours) / span_hours
        return low_lbs + fraction * span_lbs

    return cfg.target_for_week(week_run_hours)


def get_target_week_bounds(data):
    """Return (start, end) run-hours for the next Mon-Sun week."""
    state = _as_state(data)
    current  = state.current_run_hour
    now_dt   = run_hour_to_dt(state, current)
    days_until_monday = (7 - now_dt.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    next_monday     = (now_dt + timedelta(days=days_until_monday)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    next_sunday_end = next_monday + timedelta(days=7)
    return (
        dt_to_run_hour(state, next_monday),
        dt_to_run_hour(state, next_sunday_end),
    )


def get_run_hours_in_window(data, start, end):
    state = _as_state(data)
    total = 0
    for window in state.run_schedule:
        ws = max(window.start_hour, start)
        we = min(window.end_hour, end)
        if we > ws:
            total += we - ws
    return total


# ---------------------------------------------------------------------------
# Slot generation
# ---------------------------------------------------------------------------

def _all_slot_run_hours(data, range_start, range_end, cfg: PlantConfig = DEFAULT_CONFIG):
    """
    Return all run-hours for the configured delivery slots on Mon-Fri days
    within [range_start, range_end], skipping cfg.plant_holidays.
    Ascending order.
    """
    slots    = []
    start_dt = run_hour_to_dt(data, range_start).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_dt   = run_hour_to_dt(data, range_end)
    holidays = set(cfg.plant_holidays)
    day      = start_dt
    while day < end_dt:
        if day.weekday() < 5 and day.date().isoformat() not in holidays:
            for h in cfg.delivery_slots:
                slot_dt = day.replace(hour=h)
                rh      = dt_to_run_hour(data, slot_dt)
                if range_start <= rh <= range_end:
                    slots.append(rh)
        day += timedelta(days=1)
    return sorted(slots)


# ---------------------------------------------------------------------------
# Slot validation
# ---------------------------------------------------------------------------

def is_valid_delivery_slot(data, run_hour, week_start, cfg: PlantConfig = DEFAULT_CONFIG):
    """
    Return True if run_hour meets all base delivery constraints:
      - >= cfg.lead_time_hours
      - >= week_start
      - Mon–Fri
      - Hour is one of cfg.delivery_slots
      - Falls inside an active run window
    Does NOT check for conflicts or overfill — those are checked separately.
    """
    state = _as_state(data)
    current = state.current_run_hour
    if run_hour < current + cfg.lead_time_hours:
        return False
    if run_hour < week_start:
        return False
    dt = run_hour_to_dt(state, run_hour)
    if dt.weekday() >= 5:                  # Sat=5, Sun=6
        return False
    if dt.hour not in cfg.delivery_slots:
        return False
    if not is_running_at(state, run_hour, cfg=cfg):
        return False
    return True


# ---------------------------------------------------------------------------
# Overfill check
# ---------------------------------------------------------------------------

def _project_tanks_to_hour(data, product, target_hour, product_trucks,
                            cfg: PlantConfig = DEFAULT_CONFIG):
    """
    Copy tanks, then simulate consumption + deliveries for product up to
    target_hour. Returns the advanced tanks dict (still dict-of-dicts
    shape because the simulator hasn't migrated yet).

    `product_trucks` may be a list of Truck dataclasses OR raw dicts; we
    normalize per-truck below since this is called with a mix during
    Streamlit's planning loop.
    """
    state = _as_state(data)
    tanks   = copy.deepcopy(state.to_dict()["tanks"])
    rates   = state.consumption_rates
    current = state.current_run_hour

    def _truck_arrival(t):
        return t.arrival_run_hour if hasattr(t, "arrival_run_hour") else t["arrival_run_hour"]

    def _truck_product(t):
        return t.product if hasattr(t, "product") else t["product"]

    def _truck_to_dict(t):
        return t.to_dict() if hasattr(t, "to_dict") else t

    pending = sorted(
        [t for t in product_trucks
         if _truck_product(t) == product
         and current < _truck_arrival(t) <= target_hour],
        key=_truck_arrival,
    )
    truck_idx = 0
    hour      = current
    while hour < target_hour:
        next_hour = hour + 1
        if is_running_at(state, hour, cfg=cfg):
            simulate_consume(tanks, product, rates[product].lbs_per_hour)
        while (truck_idx < len(pending)
               and _truck_arrival(pending[truck_idx]) <= next_hour):
            simulate_delivery_no_alert(tanks, _truck_to_dict(pending[truck_idx]))
            truck_idx += 1
        hour = next_hour
    return tanks


def _would_overfill(data, product, slot_rh, product_trucks,
                     cfg: PlantConfig = DEFAULT_CONFIG):
    """
    Return True if delivering a standard truck of product at slot_rh would
    overfill, given already-planned product_trucks. Mirrors the overfill
    logic in alerts.simulate_delivery.

    The planner treats overfill as a hard constraint: it is preferable to
    let levels drop (even below safety stock) rather than to overfill a tank.
    """
    state    = _as_state(data)
    tanks    = _project_tanks_to_hour(state, product, slot_rh, product_trucks, cfg=cfg)
    quantity = state.truck_quantities[product]

    target_name = find_lowest_in(tanks, product)
    if target_name is None:
        return False

    target      = tanks[target_name]
    target_space = target["max_capacity_lbs"] - target["current_level_lbs"]
    single_tank_usable = (
        target["max_capacity_lbs"] - target.get("heel_lbs", 0)
    )
    expected_overflow = quantity > single_tank_usable

    other_names  = find_others_in(tanks, product, target_name)
    other_spaces = [
        tanks[n]["max_capacity_lbs"] - tanks[n]["current_level_lbs"]
        for n in other_names
    ]
    total_space = target_space + sum(other_spaces)

    if expected_overflow:
        return total_space < quantity   # truck must fit across all tanks
    else:
        return target_space < quantity  # truck must fit in one tank


# ---------------------------------------------------------------------------
# Slot finders
# ---------------------------------------------------------------------------

def _truck_arrival_rh(t):
    return t.arrival_run_hour if hasattr(t, "arrival_run_hour") else t["arrival_run_hour"]


def _truck_product_name(t):
    return t.product if hasattr(t, "product") else t["product"]


def find_latest_valid_slot(
    data, product, latest_hour, earliest_hour, week_start, week_end, all_trucks,
    cfg: PlantConfig = DEFAULT_CONFIG,
):
    """
    Return the latest allowed slot at or before latest_hour that:
      - passes is_valid_delivery_slot
      - is not already booked by any product (conflict avoidance)
      - would not overfill the product's tanks

    Returns None if no such slot exists. `all_trucks` may contain
    dataclasses or raw dicts; both are handled.
    """
    state = _as_state(data)
    booked        = {_truck_arrival_rh(t) for t in all_trucks}
    product_trucks = [t for t in all_trucks if _truck_product_name(t) == product]

    candidates = [
        rh for rh in _all_slot_run_hours(state, week_start, week_end, cfg=cfg)
        if is_valid_delivery_slot(state, rh, week_start, cfg=cfg)
        and rh <= latest_hour
        and rh >= earliest_hour
    ]

    for slot in reversed(candidates):        # latest first
        if slot in booked:
            continue
        if _would_overfill(state, product, slot, product_trucks, cfg=cfg):
            continue
        return slot
    return None


def find_earliest_valid_slot(data, product, from_hour, to_hour, all_trucks,
                              cfg: PlantConfig = DEFAULT_CONFIG):
    """
    Return the earliest allowed slot at or after from_hour that:
      - passes is_valid_delivery_slot
      - is not already booked by any product
      - would not overfill

    Used as a fallback when the breach precedes the first run window.
    Returns None if no such slot exists. `all_trucks` may contain
    dataclasses or raw dicts; both are handled.
    """
    state = _as_state(data)
    booked        = {_truck_arrival_rh(t) for t in all_trucks}
    product_trucks = [t for t in all_trucks if _truck_product_name(t) == product]

    candidates = [
        rh for rh in _all_slot_run_hours(state, from_hour, to_hour, cfg=cfg)
        if is_valid_delivery_slot(state, rh, from_hour, cfg=cfg)
    ]

    for slot in sorted(candidates):          # earliest first
        if slot in booked:
            continue
        if _would_overfill(state, product, slot, product_trucks, cfg=cfg):
            continue
        return slot
    return None


# ---------------------------------------------------------------------------
# Breach finder (unchanged)
# ---------------------------------------------------------------------------

def find_first_breach_in_target_week(
    data, product, target, week_start, week_end, extra_trucks, breach_floor=None,
    cfg: PlantConfig = DEFAULT_CONFIG,
):
    """
    Walk hour by hour from current to week_end, simulating consumption and
    deliveries. Return the first hour AT OR AFTER max(week_start, breach_floor)
    where combined level drops below target.  Return None if no such breach.

    `extra_trucks` may be a list of Truck dataclasses OR raw dicts; we
    normalize per-truck below.
    """
    state   = _as_state(data)
    tanks   = copy.deepcopy(state.to_dict()["tanks"])
    rates   = state.consumption_rates
    current = state.current_run_hour
    check_from = max(week_start, breach_floor) if breach_floor is not None else week_start

    def _truck_arrival(t):
        return t.arrival_run_hour if hasattr(t, "arrival_run_hour") else t["arrival_run_hour"]

    def _truck_product(t):
        return t.product if hasattr(t, "product") else t["product"]

    def _truck_to_dict(t):
        return t.to_dict() if hasattr(t, "to_dict") else t

    all_trucks = list(state.scheduled_trucks) + list(extra_trucks)
    pending    = sorted(
        [t for t in all_trucks
         if _truck_product(t) == product
         and current < _truck_arrival(t) <= week_end],
        key=_truck_arrival,
    )
    truck_idx = 0

    hour = current
    while hour < week_end:
        next_hour = hour + 1
        if is_running_at(state, hour, cfg=cfg):
            simulate_consume(tanks, product, rates[product].lbs_per_hour)
        while (truck_idx < len(pending)
               and _truck_arrival(pending[truck_idx]) <= next_hour):
            simulate_delivery_no_alert(tanks, _truck_to_dict(pending[truck_idx]))
            truck_idx += 1
        level = get_combined_level_from_tanks(tanks, product)
        if next_hour >= check_from and level < target:
            return next_hour
        hour = next_hour
    return None


# ---------------------------------------------------------------------------
# Main planner
# ---------------------------------------------------------------------------

def _truck_as_dict(t):
    """Normalize Truck dataclass or raw dict to dict shape (used by slot
    finders which still operate on dict-of-dicts trucks)."""
    return t.to_dict() if hasattr(t, "to_dict") else dict(t)


def plan_for_product(data, product, target, week_start, week_end, extra_trucks,
                     cfg: PlantConfig = DEFAULT_CONFIG):
    state       = _as_state(data)
    # When the operator has disabled VMI automation, the planner stops
    # proposing trucks. The Friday 09:00 RED alert (alerts.check_vmi_off)
    # reminds them weekly. Operator can still manually add trucks via
    # the schedule_truck CLI / Streamlit form.
    if not state.vmi_automation_enabled:
        logger.info("VMI automation disabled — no trucks proposed for %s.",
                     product)
        return []
    new_trucks: list[dict] = []
    current     = state.current_run_hour
    breach_floor: float | None = None

    for iteration in range(MAX_ITERATIONS):
        breach_hour = find_first_breach_in_target_week(
            state, product, target, week_start, week_end,
            extra_trucks + new_trucks,
            breach_floor=breach_floor,
            cfg=cfg,
        )
        if breach_hour is None:
            return new_trucks

        earliest  = current + cfg.lead_time_hours
        all_trucks = (
            [_truck_as_dict(t) for t in state.scheduled_trucks]
            + [_truck_as_dict(t) for t in extra_trucks]
            + [_truck_as_dict(t) for t in new_trucks]
        )

        slot = find_latest_valid_slot(
            state, product, breach_hour, earliest, week_start, week_end, all_trucks,
            cfg=cfg,
        )

        if slot is None:
            # Breach falls before the first scheduled run window, or all valid
            # slots are booked / would overfill — try earliest available slot.
            slot = find_earliest_valid_slot(
                state, product, week_start, week_end, all_trucks, cfg=cfg,
            )
            if slot is None:
                logger.warning(
                    "%s: No valid delivery slot found in target week "
                    "(all slots may be booked or would overfill). "
                    "Letting alerts handle the shortfall.",
                    product,
                )
                return new_trucks
            breach_floor = slot + 1
            logger.info(
                "%s: Level depleted at week start — placing at earliest "
                "valid slot %s",
                product, format_run_hour(state, slot),
            )

        quantity  = state.truck_quantities[product]
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
        logger.info("Placed %s truck at %s",
                     product, format_run_hour(data, slot))
        logger.info("  reason: %s", new_truck["_planned_reason"])

    logger.warning("Hit max iterations (%d) for %s.", MAX_ITERATIONS, product)
    return new_trucks


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _load_data():
    import os
    path = "data.json" if os.path.exists("data.json") else "defaults.json"
    with open(path) as f:
        return json.load(f)


def _parse_args(argv=None):
    """CLI argument parser for plan_orders.main.

    --sap-start lets cron / scheduled-task callers run the planner
    headlessly. When omitted, main() still prompts via input() for
    backward compatibility with the old interactive flow.

    --customer points at a per-customer config bundle in customers/.
    When given, the planner loads that customer's PlantConfig +
    state instead of the default data.json + DEFAULT_CONFIG. Writes
    are NOT persisted back to the customer file by this CLI; this
    flag is a read-only "plan against this customer" mode.
    """
    import argparse
    p = argparse.ArgumentParser(
        prog="plan_orders.py",
        description="Auto-plan truck orders for the upcoming week.",
    )
    p.add_argument(
        "--sap-start",
        dest="sap_start",
        default=None,
        help=("Starting SAP order number string (e.g. SAP20001). When "
              "given, the planner runs without prompting — required "
              "for cron / unattended automation."),
    )
    p.add_argument(
        "--customer",
        dest="customer",
        default=None,
        help=("Plan against a specific customer's PlantConfig + state "
              "from customers/<id>.json instead of data.json. "
              "Read-only — proposed trucks are not saved back to the "
              "customer file."),
    )
    return p.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)
    # Surface algorithm-module logger output (planner progress, parser
    # diagnostics) on stdout so CLI users still see what they used to
    # see before the print()→logger migration. Don't reconfigure if a
    # handler is already attached (e.g. when run from a test harness).
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.customer is not None:
        from customers import load_customer
        cfg, data = load_customer(args.customer)
        print(f"(Loaded customer: {args.customer})")
    else:
        cfg = DEFAULT_CONFIG
        data = _load_data()

    week_start, week_end = get_target_week_bounds(data)
    week_run_hours = get_run_hours_in_window(data, week_start, week_end)
    target = get_target_for_week(week_run_hours, cfg=cfg, state=data)

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
            data, product, target, week_start, week_end, all_new_trucks,
            cfg=cfg,
        )
        all_new_trucks.extend(new)
        if not new:
            print(f"  No new {product} trucks needed.")
        print()

    if not all_new_trucks:
        print("No new trucks needed for the target week.")
        return

    print(f"Planner proposes {len(all_new_trucks)} new truck(s).")
    if args.customer is not None:
        # --customer is a read-only mode; don't write back to data.json
        # or the customer file. Just print the plan and exit so the
        # caller can decide whether to commit.
        print()
        print("Read-only mode (--customer). Proposed trucks:")
        all_new_trucks.sort(key=lambda t: t["arrival_run_hour"])
        for t in all_new_trucks:
            print(f"  {t['product']:25s}  qty={t['quantity_lbs']:>6,}  "
                   f"arrival={format_run_hour(data, t['arrival_run_hour'])}")
        return
    if args.sap_start is not None:
        sap_start_str = args.sap_start.strip()
        print(f"Using --sap-start={sap_start_str}")
    else:
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

    from data_io import save_data
    save_data(data)

    print()
    print(f"Added {len(all_new_trucks)} truck(s). Data saved.")
    email_hooks.send_cs_load_entry(data, all_new_trucks)


if __name__ == "__main__":
    main()
