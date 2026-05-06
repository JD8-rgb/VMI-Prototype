"""
forecast.py — next-week consumption + truck forecast.

Engine abstraction so the technical team can swap implementations
without touching algorithm code.

Default engine: WeightedSeasonalForecaster (4-week lookback, weights
40/30/20/10 for most-recent → oldest, week-level outlier filter,
holiday gating). Configurable per-customer in PlantConfig.

Production swap target: ProphetForecaster (Stan-backed, decomposes
trend + weekly + holidays). Same input/output contract.

Key interface:

    forecaster = WeightedSeasonalForecaster(cfg=cfg)
    fc = forecaster.forecast(state, target_week_start_run_hour, hours=168)
    # → ForecastResult with per-product weekly_run_hours, hourly_consumption,
    #   suggested truck count, sufficiency notes, and a per-day breakdown
    #   so the UI can show "Mon: 16h, 9333 lbs forecast / Tue: ..." etc.

Used by:
  - Streamlit predictive panel (auto on advance, manual refresh button)
  - Future: planner pre-application of next-week's likely shape
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from config import DEFAULT_CONFIG, PlantConfig
from state import PlantState, as_state

# Single source of truth lives at `state.as_state` (public, no underscore
# — sidesteps the Streamlit Cloud import bug that previously broke
# `from alerts import _as_state` on cold-start). Local underscore alias
# kept for back-compat with the existing in-module call sites.
_as_state = as_state


# ── Tunables (default values; PlantConfig will mirror these in Phase 8b) ────

DEFAULT_LOOKBACK_WEEKS  = 4
DEFAULT_WEIGHTS         = (0.4, 0.3, 0.2, 0.1)   # most-recent → oldest
DEFAULT_OUTLIER_RATIO   = 0.30                    # < 30% of median = down week


# ── Result type ──────────────────────────────────────────────────────────────

@dataclass
class ProductForecast:
    """Forecast output for one product over the target week."""
    product:           str
    weekly_run_hours:  float                            # sum across the week
    weekly_lbs:        float                            # consumption in lbs
    suggested_trucks:  int
    truck_size_lbs:    int
    by_weekday:        Dict[int, Dict[str, float]] = field(default_factory=dict)
    """{weekday: {"run_hours": float, "lbs": float}} for the chart."""
    notes:             List[str]                   = field(default_factory=list)


@dataclass
class ForecastResult:
    """Aggregate forecast across every product in the customer's state."""
    products:           List[ProductForecast] = field(default_factory=list)
    lookback_weeks:     int                   = 0
    weeks_used:         int                   = 0   # after outlier filter
    weights_applied:    Tuple[float, ...]     = ()
    target_week_start_run_hour: float         = 0.0
    notes:              List[str]             = field(default_factory=list)
    engine_name:        str                   = ""


# ── The engine interface (so technical team can swap to Prophet) ────────────

class ForecastEngine:
    """Base class — every engine ships forecast() with the same shape."""
    name = "base"

    def forecast(self, state, target_week_start_run_hour: float,
                  hours: int = 168) -> ForecastResult:
        raise NotImplementedError


# ── Default: WeightedSeasonalForecaster ─────────────────────────────────────

class WeightedSeasonalForecaster(ForecastEngine):
    """Per-weekday weighted average over the last N weeks of
    level_history, with week-level outlier exclusion + holiday gating.

    NOT to be confused with strict "seasonal naive" (textbook: just
    one-period lookback). This averages multiple weeks with recency
    weighting and filters down weeks.
    """
    name = "weighted_seasonal"

    def __init__(self, cfg: PlantConfig = DEFAULT_CONFIG,
                  lookback_weeks: int = DEFAULT_LOOKBACK_WEEKS,
                  weights: Tuple[float, ...] = DEFAULT_WEIGHTS,
                  outlier_ratio: float = DEFAULT_OUTLIER_RATIO):
        self.cfg = cfg
        self.lookback_weeks = lookback_weeks
        self.weights = weights
        self.outlier_ratio = outlier_ratio

    def forecast(self, state, target_week_start_run_hour: float,
                  hours: int = 168) -> ForecastResult:
        state = _as_state(state)
        result = ForecastResult(
            lookback_weeks=self.lookback_weeks,
            weights_applied=self.weights,
            target_week_start_run_hour=target_week_start_run_hour,
            engine_name=self.name,
        )

        # Step 1: parse the historical run_schedule into per-week,
        # per-weekday run-hour totals. Pass target_week_start so future
        # weeks (e.g. a just-applied next-week schedule) don't pollute
        # the lookback — only weeks strictly before the target are kept.
        weekly_by_dow = _bucket_run_schedule_by_week(
            state, target_week_start_run_hour=target_week_start_run_hour
        )

        # Insufficient data → fall back to cfg.consumption_rates static
        # baseline. Operator sees a "no history yet, using defaults"
        # note instead of a degenerate empty forecast.
        if len(weekly_by_dow) < 2:
            return self._fallback_baseline(state, target_week_start_run_hour,
                                              hours, result,
                                              "Insufficient history (< 2 weeks).")

        # Step 2: take the most-recent N weeks, drop down-week outliers
        recent_weeks = sorted(weekly_by_dow.keys())[-self.lookback_weeks:]
        week_totals = {w: sum(weekly_by_dow[w].values()) for w in recent_weeks}
        median_total = statistics.median(week_totals.values())
        threshold = median_total * self.outlier_ratio if median_total > 0 else 0
        kept_weeks = [w for w in recent_weeks
                       if week_totals[w] >= threshold]
        if not kept_weeks:
            return self._fallback_baseline(state, target_week_start_run_hour,
                                              hours, result,
                                              "Every week in lookback was a "
                                              "down-week outlier.")

        result.weeks_used = len(kept_weeks)
        excluded_count = len(recent_weeks) - len(kept_weeks)
        if excluded_count:
            result.notes.append(
                f"Excluded {excluded_count} down-week outlier(s) "
                f"(< {int(self.outlier_ratio * 100)}% of median total)."
            )

        # Step 3: weighted per-weekday average across kept weeks.
        # Normalize the weight slice to the actual number of kept weeks
        # (so we don't divide by a sum that includes weights for excluded
        # weeks).
        weights_for_kept = list(self.weights[:len(kept_weeks)])
        if not weights_for_kept:
            weights_for_kept = [1.0]
        # If we have fewer weeks than weight terms, normalize what's there.
        weight_sum = sum(weights_for_kept) or 1.0
        # Pair weights with kept weeks: most-recent kept week gets
        # weights[0]; older weeks get later weights.
        weighted_kept_weeks = list(reversed(kept_weeks))[:len(weights_for_kept)]
        # Reverse back so we walk oldest → most-recent in the loop
        weighted_kept_weeks = list(reversed(weighted_kept_weeks))
        # Match weight indices to recency (most-recent first)
        weight_pairs = list(zip(reversed(weighted_kept_weeks),
                                 weights_for_kept))

        per_weekday_weighted_run_hours: Dict[int, float] = {d: 0.0 for d in range(7)}
        for week, w in weight_pairs:
            for dow in range(7):
                per_weekday_weighted_run_hours[dow] += (
                    weekly_by_dow[week].get(dow, 0.0) * (w / weight_sum)
                )

        # Step 4: holiday gating on the predicted week.
        cfg_holidays = set(self.cfg.plant_holidays or ())
        from time_utils import run_hour_to_dt
        for dow in range(7):
            day_start_dt = run_hour_to_dt(state, target_week_start_run_hour
                                                + dow * 24)
            iso = day_start_dt.date().isoformat()
            if iso in cfg_holidays:
                per_weekday_weighted_run_hours[dow] = 0.0
                result.notes.append(
                    f"{_DAY_NAMES[dow]} is a holiday ({iso}) — zeroed in forecast."
                )

        # Step 5: convert to per-product consumption + truck counts
        for product, rate in state.consumption_rates.items():
            lbs_per_hour = float(rate.lbs_per_hour)
            truck_size = int(state.truck_quantities.get(product, 0)) or 1
            by_weekday = {}
            weekly_hours = 0.0
            weekly_lbs   = 0.0
            for dow in range(7):
                hours_d = per_weekday_weighted_run_hours[dow]
                lbs_d   = hours_d * lbs_per_hour
                by_weekday[dow] = {"run_hours": hours_d, "lbs": lbs_d}
                weekly_hours += hours_d
                weekly_lbs   += lbs_d
            # Trucks needed = ceil(weekly_lbs / truck_size). Below 0.1
            # truck → zero (don't propose a fractional truck).
            import math
            suggested = (
                math.ceil(weekly_lbs / truck_size)
                if weekly_lbs >= 0.1 * truck_size else 0
            )
            result.products.append(ProductForecast(
                product=product,
                weekly_run_hours=weekly_hours,
                weekly_lbs=weekly_lbs,
                suggested_trucks=suggested,
                truck_size_lbs=truck_size,
                by_weekday=by_weekday,
                notes=[],
            ))

        return result

    def _fallback_baseline(self, state, target_week_start_run_hour, hours,
                              result: ForecastResult, reason: str
                              ) -> ForecastResult:
        """Static-baseline fallback when not enough history. Uses
        cfg.consumption_rates × cfg.target_high_run_hours as a
        conservative "this is what an average heavy week looks like"
        proxy. Marked as fallback in result.notes."""
        result.notes.append(
            f"Forecast falling back to static baseline: {reason} "
            f"Once advance_time records >= 2 weeks of history, the "
            f"weighted seasonal estimate kicks in."
        )
        baseline_hours_per_day = (
            float(self.cfg.target_high_run_hours) / 5.0   # spread across Mon-Fri
        )
        for product, rate in state.consumption_rates.items():
            lbs_per_hour = float(rate.lbs_per_hour)
            truck_size = int(state.truck_quantities.get(product, 0)) or 1
            by_weekday = {}
            for dow in range(7):
                if dow < 5:   # Mon-Fri
                    hours_d = baseline_hours_per_day
                else:
                    hours_d = 0.0
                lbs_d = hours_d * lbs_per_hour
                by_weekday[dow] = {"run_hours": hours_d, "lbs": lbs_d}
            weekly_hours = sum(d["run_hours"] for d in by_weekday.values())
            weekly_lbs   = sum(d["lbs"]       for d in by_weekday.values())
            import math
            suggested = (
                math.ceil(weekly_lbs / truck_size)
                if weekly_lbs >= 0.1 * truck_size else 0
            )
            result.products.append(ProductForecast(
                product=product,
                weekly_run_hours=weekly_hours,
                weekly_lbs=weekly_lbs,
                suggested_trucks=suggested,
                truck_size_lbs=truck_size,
                by_weekday=by_weekday,
                notes=["Static baseline (insufficient history)."],
            ))
        return result


# ── Helpers ──────────────────────────────────────────────────────────────────

_DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _bucket_run_schedule_by_week(state, target_week_start_run_hour: float | None = None
                                  ) -> Dict[str, Dict[int, float]]:
    """Bucket the customer's run_schedule into per-week, per-weekday
    run-hour totals.

    Returns: {week_monday_iso: {weekday: total_hours_for_that_day}}

    Two windows on the same day in the same week stack additively
    (some operators send fragmented schedules).

    target_week_start_run_hour : if provided, the target week itself is
        excluded from the bucketed history. Without this exclusion,
        applying next week's schedule would pollute the seasonal
        lookback — the just-applied future week's run-hours would sort
        as the most-recent kept week and dominate the weighted average,
        so next week's forecast would echo whatever the operator just
        applied. Only the target week is excluded; weeks before AND
        weeks beyond the target stay (a future-future window doesn't
        affect next-week's prediction the same way the target-week
        window does).
    """
    from time_utils import run_hour_to_dt, distribute_window_across_days
    target_monday_iso = None
    if target_week_start_run_hour is not None:
        target_dt = run_hour_to_dt(state, target_week_start_run_hour)
        target_monday_iso = (
            target_dt - timedelta(days=target_dt.weekday())
        ).date().isoformat()
    out: Dict[str, Dict[int, float]] = {}
    # Multi-day and overnight-spanning windows must be split across the
    # calendar days they actually cover; otherwise the seasonal model
    # over-credits the start weekday and under-credits the rest.
    for window in state.run_schedule:
        for monday, weekday, hours in distribute_window_across_days(state, window):
            if target_monday_iso is not None and monday == target_monday_iso:
                continue
            bucket = out.setdefault(monday, {d: 0.0 for d in range(7)})
            bucket[weekday] += hours
    return out


# ── Convenience top-level forecast() ────────────────────────────────────────

def forecast(state, target_week_start_run_hour: float,
              cfg: PlantConfig = DEFAULT_CONFIG) -> ForecastResult:
    """Convenience: instantiate the default engine and forecast.

    The technical team's Prophet swap goes here later — same signature,
    different engine selected by cfg / env-var. Demo path always uses
    WeightedSeasonalForecaster."""
    engine = WeightedSeasonalForecaster(cfg=cfg)
    return engine.forecast(state, target_week_start_run_hour)


# ── Augmented schedule for the integrated 12-day projection ─────────────────

def _compute_forecast_cutoff(state, current_run_hour: float) -> float:
    """Forecast starts at Monday 06:00 of the week immediately AFTER the
    latest scheduled week.

    Anchor week = the week containing the END of the latest future-going
    window. End-of-window (not start) is the right boundary for windows
    that span multiple weeks — e.g. a single Mon→Sat window covering
    two weeks should be considered to "scheduled" both weeks. Forecast
    cutoff = Monday 06:00 of (anchor_week + 7 days). If no future windows
    exist, anchor = current sim-time so forecast starts Mon 06:00 of
    next calendar week.

    This guarantees forecast NEVER overlaps any week the operator has
    touched: scheduled weeks own their own destiny (with gaps as
    intentional down-time), forecast only fills the un-scheduled future.
    """
    from time_utils import run_hour_to_dt, dt_to_run_hour
    future_ends = [w.end_hour for w in state.run_schedule
                   if w.end_hour > current_run_hour]
    if future_ends:
        anchor_dt = run_hour_to_dt(state, max(future_ends))
    else:
        anchor_dt = run_hour_to_dt(state, current_run_hour)

    anchor_week_mon = (anchor_dt - timedelta(days=anchor_dt.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    forecast_start_dt = anchor_week_mon + timedelta(days=7, hours=6)
    return dt_to_run_hour(state, forecast_start_dt)


def build_augmented_data(data: Dict[str, Any], cfg: PlantConfig = DEFAULT_CONFIG,
                          hours: int = 288) -> Tuple[Dict[str, Any], float]:
    """Extend data["run_schedule"] with forecast-derived run windows for
    the period beyond the operator's parsed schedule.

    Returns:
        (augmented_data_dict, cutoff_run_hour)

    The cutoff is the end of the last parsed run window after current
    sim time. Up to that point, the projection chart draws SOLID lines
    (we know what's happening from the parsed schedule). Beyond it,
    DOTTED lines show the forecaster's best guess.

    If there are no parsed run windows after current sim time, cutoff =
    current_run_hour and the entire chart is dotted.

    For each forecast day in [cutoff, current+hours], emit one window
    starting at the customer's typical 06:00 weekday start, lasting
    `forecasted run hours for that weekday`. Holiday / weekend days
    with zero predicted hours produce no window. End times can exceed
    24h for continuous-shift customers (matches the 24/5 template).
    """
    state = _as_state(data)
    current = float(state.current_run_hour)
    end_hour = current + hours

    # Cutoff = Monday 06:00 of the week AFTER the latest scheduled week.
    # See _compute_forecast_cutoff() for the rule. The chart draws SOLID
    # up to this cutoff (operator's domain — entered windows or down-time
    # within a scheduled week) and DOTTED beyond it (forecast).
    cutoff = _compute_forecast_cutoff(state, current)

    # If the parsed schedule already covers the full chart horizon,
    # no augmentation needed.
    if cutoff >= end_hour:
        return (dict(data) if not isinstance(data, dict)
                 else {**data}), cutoff

    # Build forecast for the FIRST week beyond cutoff. The forecaster
    # produces a per-weekday-of-week pattern; we re-use the same
    # pattern for any subsequent week within the chart horizon.
    fc = forecast(state, target_week_start_run_hour=cutoff, cfg=cfg)

    # Pull per-weekday run-hours from the first product (all products
    # share the same plant schedule, so per-weekday hours are uniform).
    if fc.products:
        per_dow_hours = {
            dow: fc.products[0].by_weekday.get(dow, {}).get("run_hours", 0.0)
            for dow in range(7)
        }
    else:
        per_dow_hours = {dow: 0.0 for dow in range(7)}

    # Walk day-by-day from cutoff to end, emitting forecast windows.
    # Use run_hour_to_dt for sim-time math (handles arbitrary epoch).
    from time_utils import run_hour_to_dt, dt_to_run_hour
    forecast_windows: List[Dict[str, Any]] = []
    cutoff_dt = run_hour_to_dt(state, cutoff)
    end_dt    = run_hour_to_dt(state, end_hour)
    # Start at cutoff's date, but only skip to the next day if cutoff is
    # PAST today's 06:00 window-start. When cutoff == Mon 06:00 (the new
    # default), this keeps day_dt at Monday so the first forecast window
    # begins exactly at Mon 06:00 instead of being pushed to Tuesday.
    day_dt = cutoff_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    _today_window_start = day_dt.replace(hour=6)
    if cutoff_dt > _today_window_start:
        day_dt = day_dt + timedelta(days=1)

    while day_dt < end_dt:
        dow = day_dt.weekday()
        predicted_h = float(per_dow_hours.get(dow, 0.0))
        if predicted_h > 0:
            # Window starts at 06:00 of the predicted day, ends at
            # 06:00 + predicted_h. End can exceed 24:00 for continuous
            # shifts (e.g. 18h Mon → continues into Tue 00:00).
            start_dt = day_dt.replace(hour=6)
            end_dt_w = start_dt + timedelta(hours=predicted_h)
            forecast_windows.append({
                "start_hour": dt_to_run_hour(state, start_dt),
                "end_hour":   dt_to_run_hour(state, end_dt_w),
                "label":      f"forecast-{day_dt.date().isoformat()}",
            })
        day_dt = day_dt + timedelta(days=1)

    # Build augmented data dict — original run_schedule + forecast windows
    if isinstance(data, dict):
        augmented = {**data}
        augmented["run_schedule"] = list(data.get("run_schedule", [])) + forecast_windows
    else:
        augmented = state.to_dict()
        augmented["run_schedule"] = augmented.get("run_schedule", []) + forecast_windows

    # ── Prospective trucks for the forecast period ──────────────────────
    # The point of the projection chart is to let the scheduler eyeball
    # next week's likely truck cadence — how many, when. We piggyback
    # on the forecaster's `suggested_trucks` count and distribute that
    # count across the customer's allowed delivery slots, starting at
    # the Monday after the cutoff. The chart renders these as DOTTED
    # amber vlines so they read as "forecast, not real".
    #
    # IMPORTANT: pass the AUGMENTED state to the truck generator —
    # _generate_forecast_trucks needs to see the forecast run windows
    # we just appended above, otherwise its forward-walking simulator
    # has no "running" hours past the parsed week and no tank ever
    # drops far enough to trigger a reorder.
    forecast_trucks = _generate_forecast_trucks(
        _as_state(augmented), fc, cutoff, end_hour, cfg
    )
    if forecast_trucks:
        augmented["scheduled_trucks"] = (
            list(augmented.get("scheduled_trucks", [])) + forecast_trucks
        )

    return augmented, cutoff


def _generate_forecast_trucks(state, fc, cutoff: float, end_hour: float,
                                cfg: PlantConfig) -> List[Dict[str, Any]]:
    """Plan prospective trucks for each forecast week using the SAME
    logic as the operator's "Plan Next Week" button.

    For every Mon-Sun week that falls within [cutoff, end_hour], call
    plan_orders.plan_for_product() with that week's bounds and target
    inventory. Tag the returned trucks with FORECAST-XXX SAP IDs and
    feed them back into the working state so subsequent weeks' planner
    calls see them and don't double-stack.

    This replaces the previous hour-by-hour simulator that triggered on
    a per-product `combined < reorder_threshold` check. The simulator
    waited for combined-level to drop before scheduling, which meant a
    nearly-empty tank at cutoff (with the other tank still full) didn't
    trip the trigger until consumption brought combined down — leaving
    the empty tank at heel for days. The planner-driven path uses the
    target inventory model + breach detection instead, so a Monday
    truck lands at the start of a forecast week if the operator would
    have ordered one then.
    """
    from copy import deepcopy
    from plan_orders import (
        plan_for_product, get_target_for_week, get_run_hours_in_window,
    )

    if cutoff >= end_hour or not getattr(fc, "products", None):
        return []

    # Build a working DICT (planner accepts dict or state). deepcopy so
    # the augmented run_schedule + tanks aren't mutated for the caller.
    working = deepcopy(state.to_dict())
    working["scheduled_trucks"] = list(working.get("scheduled_trucks", []))
    # Defensive fallback: if a product is missing from truck_quantities,
    # plan_for_product would KeyError. Backfill with a 33k default to
    # match the simulator's old behavior.
    working.setdefault("truck_quantities", {})
    for product_fc in fc.products:
        prod = getattr(product_fc, "product", None) or str(product_fc)
        working["truck_quantities"].setdefault(prod, 33_000)

    forecast_trucks: List[Dict[str, Any]] = []
    forecast_idx = 0
    week_step = 168.0   # 7 days

    week_start = cutoff
    while week_start < end_hour:
        week_end = min(week_start + week_step, end_hour)

        # Skip weeks with no run windows — nothing to plan against.
        rh_in_window = get_run_hours_in_window(working, week_start, week_end)
        if rh_in_window <= 0:
            week_start += week_step
            continue

        target_lbs = get_target_for_week(rh_in_window, cfg=cfg, state=working)

        for product_fc in fc.products:
            product = getattr(product_fc, "product", None) or str(product_fc)
            try:
                new_trucks = plan_for_product(
                    working, product, target_lbs,
                    week_start, week_end,
                    extra_trucks=[],   # working["scheduled_trucks"] holds prior weeks
                    cfg=cfg,
                )
            except Exception:
                # Planner can fail on malformed config; log + skip rather
                # than take down the whole projection chart.
                import logging
                logging.getLogger(__name__).exception(
                    "plan_for_product failed for %s in forecast week "
                    "[%.1f, %.1f]; skipping that product/week.",
                    product, week_start, week_end,
                )
                continue

            for t in new_trucks:
                arrival = float(t.get("arrival_run_hour", 0.0))
                # Defensive: only return trucks whose arrival lands in
                # the forecast portion [cutoff, end_hour). The planner
                # respects week_start/week_end so this is belt-and-
                # braces. The cutoff bound is INCLUSIVE because cutoff
                # is Mon 06:00 of the forecast week (often a configured
                # delivery slot) — a truck arriving exactly at cutoff
                # is a legitimate first-Monday-morning forecast truck.
                if arrival < cutoff or arrival >= end_hour:
                    continue
                tagged = {
                    "sap_order":        f"FORECAST-{forecast_idx:03d}",
                    "product":          t["product"],
                    "quantity_lbs":     t["quantity_lbs"],
                    "arrival_run_hour": arrival,
                }
                forecast_trucks.append(tagged)
                # Feed forward so the next week's planner sees this
                # truck and doesn't double-stack.
                working["scheduled_trucks"].append(tagged)
                forecast_idx += 1

        week_start += week_step

    return forecast_trucks
