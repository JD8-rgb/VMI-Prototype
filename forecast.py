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
from state import PlantState


def _as_state(data_or_state):
    """Polymorphic dict→PlantState shim, inlined here.

    Was `from alerts import _as_state` originally, but Streamlit Cloud
    raised an opaque ImportError on the underscore-prefixed import even
    though the symbol exists in alerts.py. Inlining removes the
    cross-module dependency at import time. Behavior is identical to
    alerts._as_state (which keeps its own copy for callers there).

    KNOWN FRAGILITY: two implementations of the same shim can drift.
    To fix: make `_as_state` a public function in state.py (rename to
    `as_state`) so both alerts.py and forecast.py import from the same
    source without triggering the Streamlit Cloud underscore-import bug.
    """
    if isinstance(data_or_state, PlantState):
        return data_or_state
    return PlantState.from_dict(data_or_state)


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
    from time_utils import run_hour_to_dt
    target_monday_iso = None
    if target_week_start_run_hour is not None:
        target_dt = run_hour_to_dt(state, target_week_start_run_hour)
        target_monday_iso = (
            target_dt - timedelta(days=target_dt.weekday())
        ).date().isoformat()
    out: Dict[str, Dict[int, float]] = {}
    for window in state.run_schedule:
        start_dt = run_hour_to_dt(state, window.start_hour)
        monday = (start_dt - timedelta(days=start_dt.weekday())).date().isoformat()
        if target_monday_iso is not None and monday == target_monday_iso:
            continue
        bucket = out.setdefault(monday, {d: 0.0 for d in range(7)})
        bucket[start_dt.weekday()] += float(window.end_hour - window.start_hour)
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
    """Walk the projection forward and schedule prospective trucks the
    way the live planner would: only when a tank actually needs one
    to stay above safety stock. The forecaster's `suggested_trucks`
    count is informational; what we ACTUALLY place is whatever the
    forecast schedule + consumption rates demand.

    Mirrors the demo_history backfill pattern (consume per-hour during
    run windows, deliver pending trucks, trigger an order when
    combined product level drops below the reorder threshold) but
    runs forward from current_run_hour through end_hour. Trucks
    landing strictly between cutoff and end_hour are returned with
    a 'FORECAST-' SAP prefix; trucks before cutoff are NOT returned
    (those are real, already in state.scheduled_trucks).

    Trucks arrive at the next valid delivery slot (cfg.delivery_slots
    on the wall-clock day) at least `cfg.lead_time_hours` from the
    trigger event. This matches what the real planner produces, so
    the projection's prospective trucks visually agree with what the
    operator would see if they ran the planner for the forecast week.
    """
    from copy import deepcopy
    from alerts import simulate_consume, simulate_delivery_no_alert
    from time_utils import run_hour_to_dt, dt_to_run_hour

    if cutoff >= end_hour or not fc.products:
        return []

    # The augmented state's full run_schedule (parsed + forecast
    # windows) is what we walk against — defines when consumption
    # happens during the forecast period.
    schedule = state.run_schedule

    def _is_running_at(h: float) -> bool:
        # Mirror alerts.is_running_at semantics inline so we don't
        # circular-import. Honors holiday gating via cfg.
        if cfg.plant_holidays:
            iso = run_hour_to_dt(state, h).date().isoformat()
            if iso in cfg.plant_holidays:
                return False
        for w in schedule:
            if w.start_hour <= h < w.end_hour:
                return True
        return False

    # Forecast trucks are illustrative, not orders the operator must
    # place today. Use a shorter "intent" lead time (half the real
    # lead) so prospective trucks appear in the chart at the moment
    # the planner would have wanted them — not 48h after the trigger
    # when delivery might already be past the chart's right edge.
    FORECAST_LEAD_HOURS = max(8.0, float(cfg.lead_time_hours) / 2.0)

    sim_tanks = deepcopy(state.to_dict().get("tanks", {}))
    truck_qty_map = dict(state.truck_quantities or {})
    rates = state.consumption_rates or {}
    products = list(rates.keys())
    slots = sorted(set(int(s) for s in (cfg.delivery_slots or (8,))))

    # Per-product reorder threshold, sized so combined level stays
    # above safety stock through the entire (trigger → next-slot →
    # delivery) gap. Two sources of consumption during that gap:
    #   1. FORECAST_LEAD_HOURS of intent lead.
    #   2. Up to ~24h of slot-snap penalty (e.g. trigger fires Fri 15h
    #      but next valid slot is Mon 06h → ~63h of weekend stall).
    # The earlier static 15k trigger fired AT 15k, then the plant
    # drained another ~28k before delivery, dropping combined level
    # to roughly 0 (heel) — well below the 10k safety floor.
    GAP_SAFETY_HOURS = FORECAST_LEAD_HOURS + 24.0
    reorder_thresholds = {
        product: float(cfg.safety_stock_lbs)
                  + GAP_SAFETY_HOURS * float(rates[product].lbs_per_hour)
        for product in products
    }

    # Real trucks already on the books — we walk them so consumption
    # math stays accurate up to and past the cutoff.
    pending_real = sorted(
        [{"product": t.product,
           "quantity_lbs": t.quantity_lbs,
           "arrival_rh": t.arrival_run_hour}
          for t in state.scheduled_trucks
          if state.current_run_hour <= t.arrival_run_hour < end_hour],
        key=lambda r: r["arrival_rh"],
    )
    pending_forecast: List[Dict[str, Any]] = []

    def _next_delivery_slot(after_rh: float):
        """Smallest run_hour >= after_rh that lands on a configured
        delivery slot (e.g. 06:00 / 08:00 / 14:00) on a non-holiday.

        Returns None if no non-holiday slot is available within the
        next 14 days (e.g. customer config has 14+ consecutive holidays
        — pathological but possible with bad data). Callers must skip
        the truck when this returns None — falling back to `after_rh`
        would land the truck ON a holiday."""
        dt = run_hour_to_dt(state, after_rh)
        for day_offset in range(0, 14):
            check_dt = (dt + timedelta(days=day_offset)).replace(
                minute=0, second=0, microsecond=0
            )
            if cfg.plant_holidays and \
               check_dt.date().isoformat() in cfg.plant_holidays:
                continue
            for slot in slots:
                slot_dt = check_dt.replace(hour=slot)
                slot_rh = dt_to_run_hour(state, slot_dt)
                if slot_rh >= after_rh:
                    return float(slot_rh)
        return None

    forecast_trucks: List[Dict[str, Any]] = []
    forecast_idx = 0

    h = float(state.current_run_hour)
    while h < end_hour:
        # 1. Deliver any trucks that arrive at this hour (real or fcst)
        for arr_list in (pending_real, pending_forecast):
            ready = [t for t in arr_list if t["arrival_rh"] <= h]
            for t in ready:
                simulate_delivery_no_alert(sim_tanks, t)
                arr_list.remove(t)

        # 2. Consume during run windows
        if _is_running_at(h):
            for product in products:
                rate = rates[product].lbs_per_hour
                if rate:
                    simulate_consume(sim_tanks, product, float(rate))

        # 3. After consuming, check whether any product needs reordering.
        for product in products:
            # Combined-by-product uses the tank's `product` field, not
            # tank-name prefix matching. Earlier code did
            # `prefix = "U-" if product == "Product U" else "M-"`,
            # which silently evaluated to 0 for any non-Acme customer
            # (Product Acid / Base / Catalyst → no tank starts with
            # "A-" or "B-" → combined always 0 → trigger fires every
            # hour → 36+ stacked forecast trucks). Membership-based
            # match works for any product/tank topology.
            combined = sum(
                tk.get("current_level_lbs", 0)
                for tk in sim_tanks.values()
                if tk.get("product") == product
            )
            already_in_flight = any(
                t["product"] == product
                for t in pending_real + pending_forecast
            )
            if combined < reorder_thresholds[product] and not already_in_flight:
                arrival_rh = _next_delivery_slot(
                    h + FORECAST_LEAD_HOURS
                )
                # _next_delivery_slot returns None when every day in
                # the next 14 is a holiday — skip the truck rather
                # than landing it ON a holiday via the old fallback.
                if arrival_rh is None:
                    continue
                # Drop trucks that would arrive past the chart's right
                # edge — they'd be invisible (Plotly clips vlines past
                # the data range) and the operator gets nothing useful
                # from a "ghost" delivery.
                if arrival_rh >= end_hour:
                    continue
                qty = int(truck_qty_map.get(product, 33_000))
                truck = {
                    "product": product,
                    "quantity_lbs": qty,
                    "arrival_rh": arrival_rh,
                }
                pending_forecast.append(truck)
                # Only record as a "forecast truck" if it arrives in the
                # forecast period; otherwise it's effectively a real
                # planner suggestion for the parsed period (don't double-
                # count with state.scheduled_trucks).
                if arrival_rh > cutoff:
                    forecast_trucks.append({
                        "sap_order":        f"FORECAST-{forecast_idx:03d}",
                        "product":          product,
                        "quantity_lbs":     qty,
                        "arrival_run_hour": float(arrival_rh),
                    })
                    forecast_idx += 1

        h += 1.0

    return forecast_trucks
