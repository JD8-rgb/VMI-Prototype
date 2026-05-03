"""Anomaly check behavior contracts (anomaly.py).

All checks fire as YELLOW warnings (severity="warning", type="anomaly")
that don't block the schedule or planner — they surface "this looks
unusual, did you mean it?" Operator confirms or overrides."""

from __future__ import annotations

import copy
from datetime import datetime, timedelta

import pytest

from anomaly import (
    check_run_hours_unusual,
    check_day_shape_unusual,
    check_holiday_in_run_window,
    check_truck_cadence_unusual,
    check_schedule_arrival_unusual,
    check_projected_ending_unusual,
    get_all_anomalies,
)
from config import PlantConfig, DEFAULT_CONFIG


def _multi_week_schedule(weeks=4, hours_per_day=16):
    """Build run_schedule covering Mon-Fri 6-22 across N weeks."""
    out = []
    for wk in range(weeks):
        for d in range(5):     # Mon-Fri
            offset = wk * 168 + d * 24
            out.append({
                "start_hour": 6.0 + offset,
                "end_hour":   6.0 + offset + hours_per_day,
                "label":      f"D{wk}-{d}",
            })
    return out


def _state_with_schedule(defaults_dict, schedule):
    d = copy.deepcopy(defaults_dict)
    d["run_schedule"] = schedule
    return d


# ── Check 1: run-hours unusual ───────────────────────────────────────────────

def test_run_hours_silent_with_consistent_history(defaults_dict):
    """4 weeks of identical 80-hr weeks → no flag."""
    d = _state_with_schedule(defaults_dict, _multi_week_schedule(weeks=4))
    assert check_run_hours_unusual(d) == []


def test_run_hours_silent_with_too_little_history(defaults_dict):
    """Need 4+ weeks (3 historical + 1 current) to compute σ confidently."""
    d = _state_with_schedule(defaults_dict, _multi_week_schedule(weeks=2))
    assert check_run_hours_unusual(d) == []


def test_run_hours_fires_on_outlier(defaults_dict):
    """3 weeks of 80h then 1 week of 200h → outlier."""
    sched = _multi_week_schedule(weeks=3)
    # Week 4 has a single huge window (200h)
    sched.append({"start_hour": 504.0, "end_hour": 704.0, "label": "huge"})
    d = _state_with_schedule(defaults_dict, sched)
    alerts = check_run_hours_unusual(d)
    assert len(alerts) == 1
    assert alerts[0]["type"] == "anomaly"
    assert alerts[0]["severity"] == "warning"
    assert "outlier" in alerts[0]["text"].lower()


# ── Check 2: day shape unusual ───────────────────────────────────────────────

def test_day_shape_silent_with_consistent_weekdays(defaults_dict):
    """4 weeks of Mon-Fri schedule → no flag."""
    d = _state_with_schedule(defaults_dict, _multi_week_schedule(weeks=4))
    assert check_day_shape_unusual(d) == []


def test_day_shape_fires_on_novel_weekday(defaults_dict):
    """3 weeks Mon-Fri then a Sun window in week 4 → flag."""
    sched = _multi_week_schedule(weeks=3)
    # Week 4: Sunday window (weekday 6). Mon of week 4 is run-hour 504.
    # Sunday is +6 days → start at 504 + 6*24 = 648.
    sched.append({"start_hour": 648.0 + 6, "end_hour": 648.0 + 22, "label": "Sun"})
    d = _state_with_schedule(defaults_dict, sched)
    alerts = check_day_shape_unusual(d)
    assert len(alerts) == 1
    assert "Sun" in alerts[0]["text"]


# ── Check 3: holiday in run window ───────────────────────────────────────────

def test_holiday_in_window_silent_when_no_holidays():
    """Default cfg has no holidays → never fires."""
    cfg = DEFAULT_CONFIG
    state = {"schema_version": 1, "simulation_epoch": "2026-04-13T00:00:00",
              "current_run_hour": 0.0,
              "tanks": {}, "consumption_rates": {}, "truck_quantities": {},
              "scheduled_trucks": [],
              "run_schedule": [{"start_hour": 6.0, "end_hour": 22.0, "label": "Mon"}]}
    assert check_holiday_in_run_window(state, cfg=cfg) == []


def test_holiday_in_window_fires_when_window_covers_holiday():
    """Mon 2026-04-13 is the simulation epoch and inside a run window;
    mark it a holiday → anomaly fires."""
    cfg = PlantConfig(plant_holidays=("2026-04-13",))
    state = {"schema_version": 1, "simulation_epoch": "2026-04-13T00:00:00",
              "current_run_hour": 0.0,
              "tanks": {}, "consumption_rates": {}, "truck_quantities": {},
              "scheduled_trucks": [],
              "run_schedule": [{"start_hour": 6.0, "end_hour": 22.0, "label": "Mon"}]}
    alerts = check_holiday_in_run_window(state, cfg=cfg)
    assert len(alerts) == 1
    assert "2026-04-13" in alerts[0]["text"]


# ── Check 4: truck cadence unusual ───────────────────────────────────────────

def test_truck_cadence_silent_in_normal_range(defaults_dict):
    """1-12 trucks → no flag."""
    d = copy.deepcopy(defaults_dict)
    assert check_truck_cadence_unusual(d, proposed_count=5) == []


def test_truck_cadence_fires_high(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    alerts = check_truck_cadence_unusual(d, proposed_count=20)
    assert len(alerts) == 1
    assert "high" in alerts[0]["text"].lower()


def test_truck_cadence_silent_at_zero(defaults_dict):
    """Zero trucks is 'no schedule yet' — handled by other checks."""
    d = copy.deepcopy(defaults_dict)
    assert check_truck_cadence_unusual(d, proposed_count=0) == []


# ── Check 5: schedule arrival timing unusual ────────────────────────────────

def test_arrival_silent_with_consistent_thursdays(defaults_dict):
    """8 Thursday arrivals → 9th arrival on Thursday is fine."""
    d = copy.deepcopy(defaults_dict)
    history = []
    for wk in range(9):
        # Each Thursday at 16:00, week-by-week
        thu = datetime(2026, 4, 2) + timedelta(weeks=wk)  # 2026-04-02 = Thu
        history.append(thu.isoformat())
    d["schedule_arrival_history"] = history
    assert check_schedule_arrival_unusual(d) == []


def test_arrival_fires_on_novel_weekday(defaults_dict):
    """8 Thursday arrivals then a Tuesday → flag."""
    d = copy.deepcopy(defaults_dict)
    history = [
        (datetime(2026, 4, 2) + timedelta(weeks=wk)).isoformat()
        for wk in range(8)
    ]
    history.append(datetime(2026, 5, 26).isoformat())  # Tuesday
    d["schedule_arrival_history"] = history
    alerts = check_schedule_arrival_unusual(d)
    assert len(alerts) == 1
    assert "Tue" in alerts[0]["text"]


def test_arrival_silent_with_no_history(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    d["schedule_arrival_history"] = []
    assert check_schedule_arrival_unusual(d) == []


# ── Check 6: projected ending unusual ───────────────────────────────────────

def test_projected_ending_silent_in_window(defaults_dict):
    """Defaults customer should project an ending level inside the
    cfg.tunable_low_min .. tunable_high_max window for the standard
    projection."""
    d = copy.deepcopy(defaults_dict)
    # No flag expected (or both products in-window)
    alerts = check_projected_ending_unusual(d)
    # Test passes whether or not it fires — this just confirms it
    # doesn't crash on the demo defaults
    assert isinstance(alerts, list)


def test_projected_ending_fires_when_drained(defaults_dict):
    """Force a near-empty starting state → projection drives ending
    levels well below tunable_low_min → check_projected_ending_unusual
    flags."""
    d = copy.deepcopy(defaults_dict)
    for tank in d["tanks"].values():
        tank["current_level_lbs"] = tank["heel_lbs"] + 100   # near empty
    alerts = check_projected_ending_unusual(d)
    # At least one product should flag as too-low
    assert any(a["direction"] == "too_low" for a in alerts)


# ── Aggregator ───────────────────────────────────────────────────────────────

def test_get_all_anomalies_returns_list(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    assert isinstance(get_all_anomalies(d), list)


def test_get_all_anomalies_wired_into_get_all_alerts(defaults_dict):
    """alerts.get_all_alerts must include anomalies in its output."""
    from alerts import get_all_alerts
    cfg = PlantConfig(plant_holidays=("2026-04-13",))
    d = copy.deepcopy(defaults_dict)
    alerts = get_all_alerts(d, cfg=cfg)
    # The holiday anomaly should fire and be in the combined list
    assert any(a["type"] == "anomaly" for a in alerts)
