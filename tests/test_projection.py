"""Behavior contracts for projection.compute_level_history."""

from __future__ import annotations

import copy

from projection import compute_level_history, PROJECTION_HOURS
from state import PlantState


def test_projection_returns_required_keys(defaults_dict, as_shape):
    out = compute_level_history(as_shape(defaults_dict), hours=24)
    for key in ("run_hours", "datetimes", "tanks", "truck_events", "run_windows"):
        assert key in out, f"missing key: {key}"


def test_projection_step_count_matches_hours(defaults_dict, as_shape):
    """N+1 sample points for an N-hour projection (initial + each step)."""
    hours = 48
    out = compute_level_history(as_shape(defaults_dict), hours=hours)
    assert len(out["run_hours"]) == hours + 1
    assert len(out["datetimes"]) == hours + 1
    for tank, history in out["tanks"].items():
        assert len(history) == hours + 1, f"tank {tank} length mismatch"


def test_projection_initial_levels_match_input(defaults_dict, as_shape):
    """First sample at run_hour=current matches the live tank levels."""
    out = compute_level_history(as_shape(defaults_dict), hours=24)
    for tank, history in out["tanks"].items():
        starting = defaults_dict["tanks"][tank]["current_level_lbs"]
        assert history[0] == starting


def test_projection_decreases_during_run_window(defaults_dict, as_shape):
    """Mon 6-22 is a run window. Tank levels must monotonically
    decrease across consecutive sample points within that window for
    the draw tank (until it switches)."""
    out = compute_level_history(as_shape(defaults_dict), hours=24)
    # U-Tank2 starts at 20000 (draw). Consumption at 583.3/hr means
    # by hour 22 it's well below 20000.
    rh = out["run_hours"]
    levels = out["tanks"]["U-Tank2"]
    # Find sample at hour 6 and hour 22
    i_start = rh.index(6.0) if 6.0 in rh else None
    i_end   = rh.index(22.0) if 22.0 in rh else None
    assert i_start is not None and i_end is not None
    assert levels[i_end] < levels[i_start]


def test_projection_truck_events_recorded(defaults_dict, as_shape):
    d = copy.deepcopy(defaults_dict)
    d["scheduled_trucks"] = [{
        "sap_order": "SAP90001",
        "product": "Product U",
        "quantity_lbs": 33000,
        "arrival_run_hour": 8.0,
    }]
    out = compute_level_history(as_shape(d), hours=24)
    assert len(out["truck_events"]) == 1
    ev = out["truck_events"][0]
    assert ev["sap"] == "SAP90001"
    assert ev["product"] == "Product U"
    assert ev["qty"] == 33000
    assert ev["run_hour"] == 8.0


def test_projection_no_consumption_outside_run_windows(defaults_dict, as_shape):
    """Mon 22:00 → Tue 6:00 is idle. Levels must NOT drop in that span."""
    out = compute_level_history(as_shape(defaults_dict), hours=48)
    rh = out["run_hours"]
    levels = out["tanks"]["U-Tank2"]
    i_22 = rh.index(22.0)
    i_30 = rh.index(30.0)
    # Strictly equal — no consumption applied during idle hours
    for i in range(i_22, i_30 + 1):
        assert levels[i] == levels[i_22]


def test_projection_polymorphism_parity(defaults_dict):
    """Same input, two shapes, identical history."""
    d = copy.deepcopy(defaults_dict)
    via_dict  = compute_level_history(d, hours=24)
    via_state = compute_level_history(PlantState.from_dict(d), hours=24)
    assert via_dict["run_hours"] == via_state["run_hours"]
    assert via_dict["tanks"]    == via_state["tanks"]
    assert via_dict["truck_events"] == via_state["truck_events"]


def test_projection_clips_run_windows(defaults_dict, as_shape):
    """run_windows in the output should be clipped to [current, current+hours]."""
    out = compute_level_history(as_shape(defaults_dict), hours=24)
    for w in out["run_windows"]:
        assert 0 <= w["start_hour"] < w["end_hour"] <= 24
