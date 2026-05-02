"""
config.py
---------
Single PlantConfig dataclass for all plant-specific business constants.

The algorithm modules (alerts.py, plan_orders.py, read_schedule.py)
historically declared these as module-level globals, which made
multi-customer / multi-plant support impossible without forking the
codebase or monkey-patching.

Now: every algorithm function accepts an optional `cfg: PlantConfig`
parameter that defaults to DEFAULT_CONFIG. Existing callers see no
change. The enterprise team can swap in per-customer configs without
touching algorithm code:

    from config import PlantConfig
    customer_a_cfg = PlantConfig(
        lead_time_hours=72,
        delivery_slots=(7, 9, 15),
        ...
    )
    alerts.check_lead_time(state, "Product U", cfg=customer_a_cfg)

Backward compatibility: the algorithm modules also re-export the old
module-level constants (LEAD_TIME_HOURS, etc.), wired to
DEFAULT_CONFIG fields, so any code still doing `from alerts import
LEAD_TIME_HOURS` continues to work. Those re-exports are deprecated
but kept indefinitely — removing them is its own pass.
"""

from dataclasses import dataclass, field
from typing import Tuple


@dataclass(frozen=True)
class PlantConfig:
    # ── Alerts ────────────────────────────────────────────────────────────
    safety_stock_lbs:           float = 10_000
    """Combined product level below this = safety-stock alert."""

    lead_time_hours:            float = 48
    """Truck must be planned at least this far in advance, AND lead-time
    alert horizon for inbound coverage check."""

    late_truck_hours:           float = 3
    """Alert if a scheduled truck is this many hours past its arrival."""

    projection_window_hours:    float = 168
    """How far run_projection looks ahead (default = 7 days)."""

    plant_state_mismatch_hours: float = 3
    """Alert if plant running/idle state contradicts the schedule for
    longer than this (e.g. running when scheduled idle)."""

    # ── Parser ────────────────────────────────────────────────────────────
    plant_max_hours: float = 118
    """Parser sanity cap. Any single window or total weekly run that
    exceeds this is treated as quoted-history leakage and forced low."""

    # ── Planner ───────────────────────────────────────────────────────────
    delivery_slots: Tuple[int, ...] = (6, 8, 14)
    """Allowed delivery arrival hours (24-hour clock)."""

    # ── Reorder target curve ──────────────────────────────────────────────
    # Linear interpolation between (low_run_hours, low_lbs) and
    # (high_run_hours, high_lbs). Below low → low_lbs flat. Above high →
    # high_lbs flat.
    target_low_run_hours:  float = 28
    target_high_run_hours: float = 118
    target_low_lbs:        float = 15_000
    target_high_lbs:       float = 27_000

    def target_for_week(self, week_run_hours: float) -> float:
        """Linear-interpolated reorder target given scheduled weekly run hours."""
        if week_run_hours <= self.target_low_run_hours:
            return self.target_low_lbs
        if week_run_hours >= self.target_high_run_hours:
            return self.target_high_lbs
        span_hours = self.target_high_run_hours - self.target_low_run_hours
        span_lbs   = self.target_high_lbs       - self.target_low_lbs
        fraction   = (week_run_hours - self.target_low_run_hours) / span_hours
        return self.target_low_lbs + fraction * span_lbs


# The default config matches the historic module-globals exactly, so
# existing callers (which don't pass `cfg=`) see no behavior change.
DEFAULT_CONFIG = PlantConfig()
