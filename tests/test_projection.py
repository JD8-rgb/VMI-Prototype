"""Behavior contracts for projection.compute_level_history."""

from __future__ import annotations

import copy
from datetime import datetime, timedelta

from projection import compute_level_history, PROJECTION_HOURS
from state import PlantState


# ── Shared helper ─────────────────────────────────────────────────────────────

def _minimal_state(epoch_iso, run_hours_schedule=None):
    """Minimal valid state dict for projection tests with a custom epoch."""
    d = {
        "simulation_epoch": epoch_iso,
        "current_run_hour": 0.0,
        "tanks": {
            "T1": {"product": "P", "current_level_lbs": 20000,
                    "max_capacity_lbs": 35000, "heel_lbs": 1000, "status": "draw"},
        },
        "consumption_rates": {"P": {"lbs_per_hour": 100.0}},
        "truck_quantities": {"P": 30000},
        "scheduled_trucks": [],
        "run_schedule": run_hours_schedule or [],
    }
    return d


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


# ── Year / leap-year boundary tests ──────────────────────────────────────────


def test_projection_spans_year_boundary():
    """Projection starting Dec 28 and covering into Jan 3 next year must
    not raise and must return the correct number of sample points."""
    # Dec 28, 2026 → 6 days to Jan 3, 2027. A single 72-hour run window
    # spans the New Year boundary (Dec 30 06:00 → Jan 2 06:00 = 72 h).
    epoch = datetime(2026, 12, 28)
    d = _minimal_state(
        "2026-12-28T00:00:00",
        run_hours_schedule=[{"start_hour": 48.0, "end_hour": 120.0, "label": "NYE"}],
    )
    hours = 168  # 1 week
    out = compute_level_history(d, hours=hours)
    assert len(out["run_hours"]) == hours + 1

    # Derive datetimes from run_hours + epoch (avoids parsing the label format)
    dts = [epoch + timedelta(hours=rh) for rh in out["run_hours"]]
    years = {dt.year for dt in dts}
    assert 2026 in years and 2027 in years, (
        "Projection should span both 2026 and 2027"
    )
    # Tank levels decrease during the run window (hours 48-120 of the schedule)
    levels = out["tanks"]["T1"]
    assert levels[50] < levels[48], "Level should drop during run window"


def test_projection_leap_year_feb28_to_feb29():
    """Projection epoch Feb 26 2028 spanning into Mar 1 (through leap day
    Feb 29) must not raise and must correctly cross the month boundary."""
    # 2028 is a leap year. epoch=Feb 26, run window 24-96h covers Feb 28 → Mar 1
    epoch = datetime(2028, 2, 26)
    d = _minimal_state(
        "2028-02-26T00:00:00",
        run_hours_schedule=[{"start_hour": 24.0, "end_hour": 96.0, "label": "Feb28-Mar1"}],
    )
    hours = 120  # 5 days
    out = compute_level_history(d, hours=hours)
    assert len(out["run_hours"]) == hours + 1

    dts = [epoch + timedelta(hours=rh) for rh in out["run_hours"]]
    months = {dt.month for dt in dts}
    # Should include Feb (2) and March (3)
    assert 2 in months and 3 in months, (
        "Projection should cross from February into March"
    )
    # Confirm Feb 29 appears in the datetimes (leap day)
    feb29_exists = any(dt.month == 2 and dt.day == 29 for dt in dts)
    assert feb29_exists, "Projection must include Feb 29 (leap day) in 2028"
    # Tank level must drop during the run window
    levels = out["tanks"]["T1"]
    assert levels[50] < levels[24], "Level should drop during run window"
