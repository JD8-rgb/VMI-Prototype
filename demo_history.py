"""demo_history.py
-----------------
Backfill realistic-looking past level_history without touching
current_run_hour. Powers the "Generate demo history" button.

Why backfill instead of advancing forward: rolling the sim clock
forward N weeks means the operator has to scrub back to "today" before
they can demo anything else. Backfill keeps the clock where it is and
just fills in plausible history behind it.

Realism: the historical schedule alternates between two patterns
(per Acme's typical week) so the chart doesn't look like a single
repeating sawtooth. Truck deliveries are inserted automatically when
combined product level drops below a reorder threshold.

The synthetic past does NOT smoothly connect to current tank levels —
they're a different timeline. The chart shows past trends, the tank
cards show current truth.
"""

from __future__ import annotations

import copy
from datetime import datetime, timedelta
from typing import Any, Dict, List

from alerts import simulate_consume, simulate_delivery_no_alert
from level_history import LEVEL_HISTORY_MAX_ENTRIES


# Customer's typical "long-shift" week: Mon 6am → Tue 4pm continuous
# (34h), Wed 6am-4pm (10h), Thu 6am → Fri 4pm continuous (34h).
# Tuples are (weekday_index, start_hour_of_day, end_hour_offset_from_dow_midnight).
_PATTERN_LONG_SHIFTS = [
    (0,  6, 24 + 16),   # Mon 6am → Tue 4pm
    (2,  6, 16),        # Wed 6am-4pm
    (3,  6, 24 + 16),   # Thu 6am → Fri 4pm
]
# Standard 5-day week — for variety
_PATTERN_STANDARD = [(d, 6, 22) for d in range(5)]

# Trigger a synthetic delivery when combined product level falls below
# this. ~22k = "one tank near full, second tank pulling close to heel"
# in the typical 30k/tank topology — matches when a real operator
# would call the order in. Lower thresholds (e.g. 12k) wait until both
# tanks are nearly empty, producing flat-line chart segments at heel.
_REORDER_THRESHOLD_LBS = 22_000

# Each truck arrives 24h after the trigger event — gives the chart a
# visible "low → delivery → bounce" arc.
_DELIVERY_LEAD_HOURS = 24


def generate_demo_history(data: Dict[str, Any], weeks: int) -> int:
    """Backfill `weeks` of synthetic past into data['level_history'].

    Returns the number of snapshot entries added. Mutates `data` in
    place: writes `level_history`. Does NOT modify `current_run_hour`,
    `run_schedule`, `scheduled_trucks`, or `tanks`.
    """
    if weeks <= 0:
        return 0

    current_rh = float(data.get("current_run_hour", 0.0))
    # Snap start to a Monday-midnight boundary so the (dow, sh, eh)
    # pattern lands on the right weekdays. Without this, opening the
    # demo on (say) Wed at 12pm would put start_rh at "Wed noon, N
    # weeks ago", and "Mon 6am-Tue 4pm" would render on Wed evening.
    # Epoch is always Mon midnight (per _reanchor_to_now), so
    # run_hour % 168 == 0 ↔ Mon midnight.
    days_into_week = current_rh % 168.0
    monday_anchor  = current_rh - days_into_week
    start_rh       = monday_anchor - weeks * 168.0

    # ── 1. Build a synthetic schedule for the past horizon ───────────────
    # Alternate patterns week-by-week so the chart doesn't look like
    # one repeating shape. Cover one extra week past `weeks` so the
    # current (partial) week — between the last snap-aligned Monday and
    # `current_rh` — also gets run windows; otherwise the chart shows
    # several days of flat lines at the right edge.
    synthetic_windows: List[Dict[str, float]] = []
    for w in range(weeks + 1):
        base = start_rh + w * 168.0
        pattern = _PATTERN_LONG_SHIFTS if w % 2 == 0 else _PATTERN_STANDARD
        for (dow, sh, eh) in pattern:
            synthetic_windows.append({
                "start_hour": base + dow * 24 + sh,
                "end_hour":   base + dow * 24 + eh,
            })

    def _is_running_at(h: float) -> bool:
        return any(w["start_hour"] <= h < w["end_hour"]
                   for w in synthetic_windows)

    # ── 2. Initial tank levels at the start of the past horizon ──────────
    # 70% full so the early chart has clear runway before the first
    # truck event. Independent of current state — this is a fresh
    # synthetic timeline.
    past_tanks = copy.deepcopy(data.get("tanks", {}))
    for name, info in past_tanks.items():
        info["current_level_lbs"] = info.get("max_capacity_lbs", 0) * 0.70

    # ── 3. Walk forward hourly, tracking pending trucks + snapshots ──────
    epoch_iso = data.get("simulation_epoch")
    epoch     = datetime.fromisoformat(epoch_iso) if epoch_iso else datetime.now()
    rates     = data.get("consumption_rates", {})
    truck_qty = data.get("truck_quantities", {})

    pending: List[Dict[str, Any]] = []   # {arrival_rh, product, quantity_lbs}
    snapshots: List[Dict[str, Any]] = []

    h = start_rh
    while h < current_rh:
        # 3a. Deliver any pending trucks that have arrived
        arrivals = [t for t in pending if t["arrival_rh"] <= h]
        for t in arrivals:
            simulate_delivery_no_alert(past_tanks, t)
            pending.remove(t)

        # 3b. Consume during run windows
        if _is_running_at(h):
            for product, rate_info in rates.items():
                lph = (rate_info["lbs_per_hour"]
                       if isinstance(rate_info, dict)
                       else getattr(rate_info, "lbs_per_hour", 0))
                if lph:
                    simulate_consume(past_tanks, product, float(lph))

        # 3c. Auto-trigger a delivery if any product is running low and
        #     no truck is already in-flight for it. Synthetic stand-in
        #     for what the planner would have done.
        for product in rates.keys():
            prefix = "U-" if product == "Product U" else "M-"
            combined = sum(
                t.get("current_level_lbs", 0)
                for n, t in past_tanks.items()
                if n.startswith(prefix)
            )
            already = any(t["product"] == product for t in pending)
            if combined < _REORDER_THRESHOLD_LBS and not already:
                pending.append({
                    "arrival_rh":   h + _DELIVERY_LEAD_HOURS,
                    "product":      product,
                    "quantity_lbs": int(truck_qty.get(product, 33_000)),
                })

        # 3d. Snapshot
        snapshots.append({
            "run_hour": float(h),
            "iso":      (epoch + timedelta(hours=h)).isoformat(),
            "tanks":    {n: float(t.get("current_level_lbs", 0))
                          for n, t in past_tanks.items()},
        })
        h += 1.0

    # ── 4. Merge into level_history ─────────────────────────────────────
    # Replace any existing entries within the backfill window so a
    # second click doesn't double-stack. Keep entries strictly after
    # current_rh (shouldn't exist, but be defensive).
    existing = data.get("level_history") or []
    kept = [e for e in existing
            if float(e.get("run_hour", 0)) >= current_rh - 0.01]
    merged = snapshots + kept
    if len(merged) > LEVEL_HISTORY_MAX_ENTRIES:
        merged = merged[-LEVEL_HISTORY_MAX_ENTRIES:]
    data["level_history"] = merged
    return len(snapshots)
