"""Operator-tunable target overrides (PlantState.target_overrides).

The Streamlit "VMI Controls" panel writes these via Apply; Reset clears
them. The values must:
  - persist week-to-week through PlantState round-trip
  - be clamped to PlantConfig.tunable_* bounds at read time so a stale
    override that no longer fits the customer's window can't ship
    nonsense to the planner
  - fall back cleanly to cfg.target_for_week when absent
"""

from __future__ import annotations

import copy

import pytest

from config import PlantConfig, DEFAULT_CONFIG
from plan_orders import get_target_for_week
from state import PlantState


# ── Tunable bounds validation ────────────────────────────────────────────────

def test_tunable_low_min_must_be_less_than_max():
    with pytest.raises(ValueError):
        PlantConfig(tunable_low_min=15_000, tunable_low_max=15_000)


def test_tunable_high_min_must_be_less_than_max():
    with pytest.raises(ValueError):
        PlantConfig(tunable_high_min=27_000, tunable_high_max=27_000)


def test_tunable_low_max_must_not_exceed_high_min():
    """Sliders shouldn't be able to express low > high even at extremes."""
    with pytest.raises(ValueError):
        PlantConfig(tunable_low_max=22_000, tunable_high_min=20_000)


def test_default_tunable_bounds_form_a_valid_window():
    cfg = DEFAULT_CONFIG
    assert cfg.tunable_low_min < cfg.tunable_low_max
    assert cfg.tunable_high_min < cfg.tunable_high_max
    assert cfg.tunable_low_max <= cfg.tunable_high_min


# ── State round-trip ─────────────────────────────────────────────────────────

def test_target_overrides_round_trip(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    d["target_overrides"] = {"low": 12_000, "high": 24_000}
    rt = PlantState.from_dict(d).to_dict()
    assert rt["target_overrides"] == {"low": 12_000, "high": 24_000}


def test_no_target_overrides_round_trips_as_none(defaults_dict):
    rt = PlantState.from_dict(defaults_dict).to_dict()
    assert rt["target_overrides"] is None


def test_vmi_automation_enabled_defaults_true(defaults_dict):
    state = PlantState.from_dict(defaults_dict)
    assert state.vmi_automation_enabled is True
    rt = state.to_dict()
    assert rt["vmi_automation_enabled"] is True


def test_vmi_automation_enabled_round_trip(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    d["vmi_automation_enabled"] = False
    state = PlantState.from_dict(d)
    assert state.vmi_automation_enabled is False
    assert state.to_dict()["vmi_automation_enabled"] is False


# ── get_target_for_week with overrides ───────────────────────────────────────

def test_no_state_falls_back_to_cfg_curve():
    cfg = DEFAULT_CONFIG
    # 73 run-hours is the midpoint between low=28 and high=118 →
    # midpoint of 15k and 27k = 21k
    assert get_target_for_week(73, cfg=cfg) == pytest.approx(21_000)


def test_override_replaces_cfg_target_lbs():
    """With an override of low=12k, high=24k, the midpoint (73h) should
    return 18k (midpoint of 12k and 24k) instead of the cfg's 21k."""
    cfg = DEFAULT_CONFIG
    state = PlantState.from_dict({
        "schema_version": 1, "simulation_epoch": "2026-04-13T00:00:00",
        "current_run_hour": 0.0, "tanks": {},
        "consumption_rates": {}, "truck_quantities": {},
        "scheduled_trucks": [], "run_schedule": [],
        "target_overrides": {"low": 12_000, "high": 24_000},
    })
    assert get_target_for_week(73, cfg=cfg, state=state) == pytest.approx(18_000)


def test_override_clamped_to_tunable_bounds():
    """A stale override outside the customer's tunable window must be
    silently clamped at read time. (The Apply button enforces bounds
    too, but a hand-edited customers/<id>.json could carry junk.)"""
    cfg = PlantConfig(tunable_low_min=11_000, tunable_low_max=15_000,
                       tunable_high_min=20_000, tunable_high_max=27_000)
    state = PlantState.from_dict({
        "schema_version": 1, "simulation_epoch": "2026-04-13T00:00:00",
        "current_run_hour": 0.0, "tanks": {},
        "consumption_rates": {}, "truck_quantities": {},
        "scheduled_trucks": [], "run_schedule": [],
        # Both values outside the bounds
        "target_overrides": {"low": 5_000, "high": 99_999},
    })
    # Below low_run_hours: clamped to low (which clamps to 11k floor)
    assert get_target_for_week(0, cfg=cfg, state=state) == 11_000
    # Above high_run_hours: clamped to high (which clamps to 27k ceiling)
    assert get_target_for_week(200, cfg=cfg, state=state) == 27_000


def test_override_dict_state_works_too():
    """Polymorphism check: dict-shape state honors overrides identically."""
    cfg = DEFAULT_CONFIG
    d = {"target_overrides": {"low": 12_000, "high": 24_000}}
    assert get_target_for_week(73, cfg=cfg, state=d) == pytest.approx(18_000)


def test_partial_override_ignored():
    """Override with only one of low/high is treated as missing —
    the operator must set both via the Apply button, never one alone."""
    cfg = DEFAULT_CONFIG
    state = PlantState.from_dict({
        "schema_version": 1, "simulation_epoch": "2026-04-13T00:00:00",
        "current_run_hour": 0.0, "tanks": {},
        "consumption_rates": {}, "truck_quantities": {},
        "scheduled_trucks": [], "run_schedule": [],
        "target_overrides": {"low": 12_000},   # missing "high"
    })
    # Falls through to cfg curve (21k at midpoint)
    assert get_target_for_week(73, cfg=cfg, state=state) == pytest.approx(21_000)
