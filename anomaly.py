"""
anomaly.py
----------
Soft warnings on parsed schedules + planner output that look unusual
versus the customer's history.

Unlike alerts.py (which fires on threshold breaches that BLOCK or
escalate), anomaly checks fire as YELLOW warnings that the operator
can confirm or override. The schedule still applies; the planner
still proposes; nothing is blocked. The point is to surface "this
looks weird, did you mean it?"

All checks are PURE FUNCTIONS that take a PlantState (or dict) and
return a list of `_alert(...)` dicts with type="anomaly" and
severity="warning".

Six checks today:

  check_run_hours_unusual         Weekly run hours outside historical
                                  ±2σ window. Catches stale forwards.
  check_day_shape_unusual          A weekday that the customer doesn't
                                  normally use (e.g. Sun in a Mon-Fri
                                  shop). Catches misreads / typos.
  check_holiday_in_run_window      Parsed run window covers a date
                                  listed in cfg.plant_holidays.
                                  Operator likely forgot the holiday.
  check_truck_cadence_unusual      Planner proposed many more (or
                                  many fewer) trucks than the
                                  customer historically receives.
  check_schedule_arrival_unusual   Schedule arrived at an hour-of-week
                                  the customer doesn't typically use.
                                  Catches accidental stale forward.
  check_projected_ending_unusual   Predicted end-of-week levels
                                  outside the operator's tunable
                                  window — flags drift before it
                                  becomes an alert.

Anomaly checks are wired into get_all_alerts in alerts.py so they
flow through the same email distribution.
"""

from __future__ import annotations

import statistics
from datetime import datetime, timedelta
from typing import Any, Dict, List

from alerts import _alert, _as_state
from config import DEFAULT_CONFIG, PlantConfig


# ── Helpers ──────────────────────────────────────────────────────────────────

def _weekly_run_hours_history(state) -> List[float]:
    """Total run hours per ISO-week from the current run_schedule.
    Returns a list of weekly totals (ascending by week-of)."""
    by_week: Dict[str, float] = {}
    from time_utils import run_hour_to_dt
    for w in state.run_schedule:
        start_dt = run_hour_to_dt(state, w.start_hour)
        # Bucket by Monday of that week
        monday = (start_dt - timedelta(days=start_dt.weekday())).date().isoformat()
        by_week.setdefault(monday, 0.0)
        by_week[monday] += float(w.end_hour - w.start_hour)
    return [hrs for _, hrs in sorted(by_week.items())]


def _zscore_outlier(value: float, samples: List[float], sigma: float = 2.0
                     ) -> bool:
    """True when `value` is more than `sigma` standard deviations from
    the mean of `samples`. Needs at least 3 samples to compute σ;
    returns False otherwise (not enough history to flag confidently)."""
    if len(samples) < 3:
        return False
    mean = statistics.mean(samples)
    stdev = statistics.pstdev(samples)
    if stdev == 0:
        return value != mean
    return abs(value - mean) > sigma * stdev


# ── Check 1: weekly run hours unusual ────────────────────────────────────────

def check_run_hours_unusual(data, cfg: PlantConfig = DEFAULT_CONFIG
                              ) -> List[Dict[str, Any]]:
    """If the most recent week's total run hours is more than 2σ from
    the historical weekly mean, warn. Catches stale forwards (operator
    re-sent last month's schedule by accident) or parser misreads
    (extracted 24h windows when meant 12h)."""
    state = _as_state(data)
    weekly = _weekly_run_hours_history(state)
    if len(weekly) < 4:
        return []   # need 3+ weeks of history + 1 current to compare
    current_week = weekly[-1]
    history = weekly[:-1]
    if not _zscore_outlier(current_week, history):
        return []
    mean_h = statistics.mean(history)
    return [_alert(
        f"WARNING: This week has {current_week:.0f} run-hours, but the "
        f"customer's historical mean is {mean_h:.0f} hrs. >2σ outlier — "
        f"verify the schedule isn't a stale forward or parser misread.",
        type="anomaly", severity="warning", direction="other",
    )]


# ── Check 2: day shape unusual ───────────────────────────────────────────────

_DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def check_day_shape_unusual(data, cfg: PlantConfig = DEFAULT_CONFIG
                              ) -> List[Dict[str, Any]]:
    """If the current week's run schedule includes weekdays the
    customer's history doesn't normally include, warn. Catches a
    Mon-Fri shop suddenly reporting a Sunday window."""
    state = _as_state(data)
    from time_utils import run_hour_to_dt
    # Bucket windows by week → set of weekdays used per week
    by_week: Dict[str, set] = {}
    for w in state.run_schedule:
        start_dt = run_hour_to_dt(state, w.start_hour)
        monday = (start_dt - timedelta(days=start_dt.weekday())).date().isoformat()
        by_week.setdefault(monday, set()).add(start_dt.weekday())
    weeks_sorted = sorted(by_week.items())
    if len(weeks_sorted) < 4:
        return []
    current_week_days = weeks_sorted[-1][1]
    historical_days: set = set()
    for _, days in weeks_sorted[:-1]:
        historical_days |= days
    novel = current_week_days - historical_days
    if not novel:
        return []
    novel_names = ", ".join(_DAY_NAMES[d] for d in sorted(novel))
    return [_alert(
        f"WARNING: This week's schedule includes {novel_names}, "
        f"which this customer hasn't used in any of the prior weeks. "
        f"Confirm the operator intended this.",
        type="anomaly", severity="warning", direction="other",
    )]


# ── Check 3: holiday in run window ───────────────────────────────────────────

def check_holiday_in_run_window(data, cfg: PlantConfig = DEFAULT_CONFIG
                                  ) -> List[Dict[str, Any]]:
    """If a parsed run window covers a date listed in
    cfg.plant_holidays, warn. The operator likely forgot the
    holiday when sending the schedule, OR the schedule was sent
    before the holiday calendar was updated."""
    state = _as_state(data)
    if not cfg.plant_holidays:
        return []
    from time_utils import run_hour_to_dt
    holidays = set(cfg.plant_holidays)
    flagged_dates: set = set()
    for w in state.run_schedule:
        start_dt = run_hour_to_dt(state, w.start_hour)
        end_dt   = run_hour_to_dt(state, w.end_hour)
        # Walk every date the window touches
        d = start_dt.date()
        while d <= end_dt.date():
            iso = d.isoformat()
            if iso in holidays:
                flagged_dates.add(iso)
            d = d + timedelta(days=1)
    if not flagged_dates:
        return []
    flagged = ", ".join(sorted(flagged_dates))
    return [_alert(
        f"WARNING: The current run schedule covers plant holiday(s): "
        f"{flagged}. The plant won't actually run on those dates "
        f"(holiday gating in is_running_at), but the operator may have "
        f"forgotten the holiday when sending the schedule.",
        type="anomaly", severity="warning", direction="other",
    )]


# ── Check 4: truck cadence unusual ───────────────────────────────────────────

# Reasonable bounds derived from the demo's typical 5-trucks-per-week.
# Customers with very different cadences should override these.
_DEFAULT_TRUCK_CADENCE_LOW  = 1
_DEFAULT_TRUCK_CADENCE_HIGH = 12


def check_truck_cadence_unusual(data, cfg: PlantConfig = DEFAULT_CONFIG,
                                  proposed_count: int = None,
                                  ) -> List[Dict[str, Any]]:
    """If the planner proposed an unusually high or low number of
    trucks for the upcoming week, warn. Default thresholds:
    [1, 12]. A customer that normally needs 5 trucks/week and now
    needs 12 likely has a wrong consumption rate or a runaway
    schedule.

    Pass `proposed_count` from the planner's output. If omitted,
    the function uses len(scheduled_trucks) as a proxy."""
    state = _as_state(data)
    if proposed_count is None:
        proposed_count = len(state.scheduled_trucks)
    if _DEFAULT_TRUCK_CADENCE_LOW <= proposed_count <= _DEFAULT_TRUCK_CADENCE_HIGH:
        return []
    if proposed_count == 0:
        return []   # zero trucks is "no schedule yet" — handled elsewhere
    direction = "high" if proposed_count > _DEFAULT_TRUCK_CADENCE_HIGH else "low"
    return [_alert(
        f"WARNING: Truck cadence is unusually {direction} "
        f"({proposed_count} trucks scheduled this week). Typical range "
        f"is {_DEFAULT_TRUCK_CADENCE_LOW}-{_DEFAULT_TRUCK_CADENCE_HIGH}. "
        f"Verify consumption rates and run-schedule before committing.",
        type="anomaly", severity="warning", direction="other",
    )]


# ── Check 5: schedule arrival timing unusual ────────────────────────────────

def check_schedule_arrival_unusual(data, cfg: PlantConfig = DEFAULT_CONFIG
                                     ) -> List[Dict[str, Any]]:
    """If the most recent schedule arrived on a weekday/hour the
    customer doesn't typically use, warn. Catches stale forwards
    (an old email re-fetched, or operator forwarding last week's
    schedule). Uses a lightweight `schedule_arrival_history` event
    log on data; full time-series storage is a separate TODO.

    Schedule arrival is recorded by read_schedule.fetch_and_apply
    when a HIGH-confidence parse lands."""
    state = _as_state(data)
    history = state._extra.get("schedule_arrival_history") or []
    if len(history) < 4:
        return []
    # Use the last 8 arrivals as the historical sample
    sample = history[-9:-1]
    latest_iso = history[-1]
    try:
        latest_dt = datetime.fromisoformat(latest_iso)
    except ValueError:
        return []
    sample_dts = []
    for iso in sample:
        try:
            sample_dts.append(datetime.fromisoformat(iso))
        except ValueError:
            pass
    if len(sample_dts) < 3:
        return []
    sample_weekdays = {dt.weekday() for dt in sample_dts}
    if latest_dt.weekday() not in sample_weekdays:
        return [_alert(
            f"WARNING: This week's schedule arrived on "
            f"{_DAY_NAMES[latest_dt.weekday()]}, but the customer's last "
            f"{len(sample_dts)} schedules arrived on "
            f"{', '.join(sorted({_DAY_NAMES[w] for w in sample_weekdays}))}. "
            f"Verify this is a fresh schedule, not a stale forward.",
            type="anomaly", severity="warning", direction="other",
        )]
    return []


# ── Check 6: projected ending levels outside override window ────────────────

def check_projected_ending_unusual(data, cfg: PlantConfig = DEFAULT_CONFIG
                                     ) -> List[Dict[str, Any]]:
    """If the projected combined level at the end of the projection
    window falls outside the operator's tunable_low_min .. tunable_high_max
    band, warn. Catches drift toward overfill / underfill BEFORE the
    safety_stock or overfill alert fires.

    Uses run_projection's combined-level snapshot at end-of-window —
    no full time-series needed."""
    from projection import compute_level_history
    state = _as_state(data)
    hist = compute_level_history(state, hours=int(cfg.projection_window_hours),
                                    cfg=cfg)
    findings = []
    for product in state.consumption_rates.keys():
        ending = sum(
            hist["tanks"][tname][-1]
            for tname, tinfo in hist["tanks"].items()
            if state.tanks[tname].product == product
        )
        # Use cfg's reasonable band (tunable_low_min..tunable_high_max)
        # as the "normal" window. Outside → warn.
        if ending < cfg.tunable_low_min:
            findings.append(_alert(
                f"WARNING: {product} projected ending level "
                f"({ending:,.0f} lbs) is below the customer's tunable "
                f"low floor ({int(cfg.tunable_low_min):,} lbs). "
                f"Drift toward safety-stock breach — check the planner.",
                type="anomaly", severity="warning",
                direction="too_low", product=product,
                level_lbs=float(ending),
            ))
        elif ending > cfg.tunable_high_max:
            findings.append(_alert(
                f"WARNING: {product} projected ending level "
                f"({ending:,.0f} lbs) is above the customer's tunable "
                f"high ceiling ({int(cfg.tunable_high_max):,} lbs). "
                f"Drift toward overfill — check planner / target sliders.",
                type="anomaly", severity="warning",
                direction="too_full", product=product,
                level_lbs=float(ending),
            ))
    return findings


# ── Aggregator ───────────────────────────────────────────────────────────────

def get_all_anomalies(data, cfg: PlantConfig = DEFAULT_CONFIG,
                        proposed_truck_count: int = None) -> List[Dict[str, Any]]:
    """Run every anomaly check and return the combined list.

    Wired into alerts.get_all_alerts so anomalies flow through the
    standard email distribution path. Caller may pass
    proposed_truck_count from the planner for check 4; otherwise
    that check uses len(scheduled_trucks) as a proxy."""
    state = _as_state(data)
    out: List[Dict[str, Any]] = []
    out += check_run_hours_unusual(state, cfg=cfg)
    out += check_day_shape_unusual(state, cfg=cfg)
    out += check_holiday_in_run_window(state, cfg=cfg)
    out += check_truck_cadence_unusual(state, cfg=cfg,
                                          proposed_count=proposed_truck_count)
    out += check_schedule_arrival_unusual(state, cfg=cfg)
    out += check_projected_ending_unusual(state, cfg=cfg)
    return out
