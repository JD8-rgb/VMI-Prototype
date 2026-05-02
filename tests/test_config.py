"""PlantConfig invariants — the curve that drives reorder targets.

If this drifts the planner will quietly under- or over-order across
every customer simultaneously."""

from __future__ import annotations

import pytest

from config import PlantConfig, DEFAULT_CONFIG


def test_target_for_week_below_low_clamps():
    cfg = DEFAULT_CONFIG
    # Way below low_run_hours floor → should clamp to low_lbs
    assert cfg.target_for_week(0) == cfg.target_low_lbs
    assert cfg.target_for_week(cfg.target_low_run_hours) == cfg.target_low_lbs


def test_target_for_week_above_high_clamps():
    cfg = DEFAULT_CONFIG
    assert cfg.target_for_week(cfg.target_high_run_hours) == cfg.target_high_lbs
    assert cfg.target_for_week(cfg.target_high_run_hours + 50) == cfg.target_high_lbs


def test_target_for_week_midpoint_interpolates():
    cfg = DEFAULT_CONFIG
    # Midpoint between low (28h, 15000lbs) and high (118h, 27000lbs) is
    # 73 run-hours → expect 21000 lbs.
    midpoint_hours = (cfg.target_low_run_hours + cfg.target_high_run_hours) / 2
    midpoint_lbs   = (cfg.target_low_lbs       + cfg.target_high_lbs)       / 2
    assert cfg.target_for_week(midpoint_hours) == pytest.approx(midpoint_lbs)


def test_target_for_week_monotone_non_decreasing():
    cfg = DEFAULT_CONFIG
    last = -1.0
    for hrs in range(0, 200, 5):
        v = cfg.target_for_week(hrs)
        assert v >= last, f"target curve regressed at {hrs}h: {v} < {last}"
        last = v


def test_dataclass_is_frozen():
    """PlantConfig is supposed to be immutable so it can be safely shared
    across threads / passed by reference without surprise mutation."""
    cfg = DEFAULT_CONFIG
    with pytest.raises(Exception):
        cfg.lead_time_hours = 999  # type: ignore[misc]


def test_per_customer_overrides_compose():
    """Confirm a customer-specific override doesn't bleed into DEFAULT_CONFIG."""
    customer = PlantConfig(lead_time_hours=72, late_truck_hours=6)
    assert customer.lead_time_hours == 72
    assert customer.late_truck_hours == 6
    # DEFAULT_CONFIG unchanged
    assert DEFAULT_CONFIG.lead_time_hours == 48
