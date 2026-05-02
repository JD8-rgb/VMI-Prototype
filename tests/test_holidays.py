"""Plant calendar / holiday support (HANDOFF.md P1#8).

When a date appears in cfg.plant_holidays, the plant is treated as
not-running for that entire date even if a run-schedule window covers
it. The planner's slot enumerator skips delivery slots on those dates,
the breach detector doesn't accumulate consumption, and the projection
holds tank levels flat across the holiday."""

from __future__ import annotations

import copy

import pytest

from alerts import is_running_at, run_projection
from config import PlantConfig, DEFAULT_CONFIG
from plan_orders import _all_slot_run_hours, get_target_week_bounds
from projection import compute_level_history


def test_default_no_holidays_unchanged_behavior(defaults_dict):
    """Empty plant_holidays must be byte-identical to old behavior."""
    cfg = DEFAULT_CONFIG
    assert cfg.plant_holidays == ()
    # Mon hour 10 falls in run window 6-22
    assert is_running_at(defaults_dict, 10.0, cfg=cfg) is True


def test_holiday_overrides_run_window(defaults_dict):
    """Mon 2026-04-13 is the simulation epoch and inside run window
    Mon 6-22. Marking it a holiday must make is_running_at return False."""
    cfg = PlantConfig(plant_holidays=("2026-04-13",))
    # Hour 10 → 2026-04-13 10:00 → holiday → not running
    assert is_running_at(defaults_dict, 10.0, cfg=cfg) is False
    # Hour 30 → 2026-04-14 (Tue, not a holiday) → still running
    assert is_running_at(defaults_dict, 30.0, cfg=cfg) is True


def test_planner_skips_holiday_slots(defaults_dict):
    """_all_slot_run_hours must not return any slot whose date is a holiday."""
    d = copy.deepcopy(defaults_dict)
    week_start, week_end = get_target_week_bounds(d)
    # Pick the Tuesday of the target week as a holiday (week_start is Mon
    # 00:00 of next week → Tue is week_start + 24h, all hours that day).
    from time_utils import run_hour_to_dt
    tue_iso = run_hour_to_dt(d, week_start + 24).date().isoformat()
    cfg = PlantConfig(plant_holidays=(tue_iso,))
    slots = _all_slot_run_hours(d, week_start, week_end, cfg=cfg)
    for slot in slots:
        slot_iso = run_hour_to_dt(d, slot).date().isoformat()
        assert slot_iso != tue_iso, (
            f"slot {slot} → {slot_iso} should have been skipped (holiday)")


def test_projection_levels_flat_across_holiday(defaults_dict):
    """Tank levels must NOT decrease across a holiday hour, even though
    the schedule window covers it."""
    cfg = PlantConfig(plant_holidays=("2026-04-13",))  # Mon
    out = compute_level_history(defaults_dict, hours=24, cfg=cfg)
    # U-Tank2 starts at 20000. Without holiday it drops; with holiday all
    # levels at every projected hour should equal the starting level.
    levels = out["tanks"]["U-Tank2"]
    assert all(lv == levels[0] for lv in levels), (
        "U-Tank2 changed during a holiday day — consumption applied")


def test_run_projection_emits_no_safety_when_holiday_blocks_consumption(defaults_dict):
    """Empty schedule + holiday everywhere = no consumption ever, no
    safety_stock alerts."""
    d = copy.deepcopy(defaults_dict)
    cfg = PlantConfig(plant_holidays=tuple(
        f"2026-04-{day:02d}" for day in range(13, 27)
    ))
    alerts = run_projection(d, cfg=cfg)
    safety = [a for a in alerts if a["type"] == "safety_stock"]
    assert safety == []


def test_holidays_round_trip_through_state(defaults_dict):
    """plant_holidays lives on PlantConfig, not PlantState. Confirm
    it doesn't end up persisted through the data file."""
    from state import PlantState
    state = PlantState.from_dict(defaults_dict)
    rt = state.to_dict()
    assert "plant_holidays" not in rt
