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

from alerts import _as_state
from config import DEFAULT_CONFIG, PlantConfig


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
        # per-weekday run-hour totals.
        weekly_by_dow = _bucket_run_schedule_by_week(state)

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


def _bucket_run_schedule_by_week(state) -> Dict[str, Dict[int, float]]:
    """Bucket the customer's run_schedule into per-week, per-weekday
    run-hour totals.

    Returns: {week_monday_iso: {weekday: total_hours_for_that_day}}

    Two windows on the same day in the same week stack additively
    (some operators send fragmented schedules)."""
    from time_utils import run_hour_to_dt
    out: Dict[str, Dict[int, float]] = {}
    for window in state.run_schedule:
        start_dt = run_hour_to_dt(state, window.start_hour)
        # Bucket by Monday of that week
        monday = (start_dt - timedelta(days=start_dt.weekday())).date().isoformat()
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
