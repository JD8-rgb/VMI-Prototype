"""Time-series storage for tank levels (level_history ring buffer).

The ring buffer is the foundation for the dashboard tank-levels-over-
time chart and any future bias-detection / suggest-target logic.
Bounded retention keeps data.json from growing unbounded.
"""

from __future__ import annotations

import copy
import json

import pytest

from data_io import CURRENT_SCHEMA_VERSION, _migrate
from level_history import (
    LEVEL_HISTORY_MAX_ENTRIES,
    record_level_snapshot,
    downsample_for_chart,
    inventory_age_hours,
)
from state import PlantState


# ── Schema migration ─────────────────────────────────────────────────────────

def test_current_schema_version_is_two():
    """Phase I bumped to v2."""
    assert CURRENT_SCHEMA_VERSION == 2


def test_v1_file_migrates_to_v2_with_empty_level_history():
    """Older v1 files (recorded before Phase I) must migrate to v2 with
    level_history initialized to an empty list."""
    legacy = {
        "schema_version": 1,
        "simulation_epoch": "2026-04-13T00:00:00",
        "current_run_hour": 0.0,
        "tanks": {}, "consumption_rates": {}, "truck_quantities": {},
    }
    out = _migrate(legacy)
    assert out["schema_version"] == 2
    assert out["level_history"] == []


def test_v0_file_migrates_through_to_v2():
    """Pre-versioned files walk both migrators (v0→v1→v2)."""
    legacy = {"simulation_epoch": "2026-04-13T00:00:00",
                "current_run_hour": 0.0, "tanks": {}}
    out = _migrate(legacy)
    assert out["schema_version"] == 2
    assert "level_history" in out


# ── PlantState round-trip ────────────────────────────────────────────────────

def test_level_history_round_trips(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    d["level_history"] = [
        {"run_hour": 0.0, "iso": "2026-04-13T00:00:00",
          "tanks": {"U-Tank1": 0, "U-Tank2": 20000}},
        {"run_hour": 1.0, "iso": "2026-04-13T01:00:00",
          "tanks": {"U-Tank1": 0, "U-Tank2": 19416}},
    ]
    rt = PlantState.from_dict(d).to_dict()
    assert rt["level_history"] == d["level_history"]


def test_level_history_defaults_empty(defaults_dict):
    """A state without level_history loads as an empty list, not None."""
    d = copy.deepcopy(defaults_dict)
    d.pop("level_history", None)
    state = PlantState.from_dict(d)
    assert state.level_history == []


# ── record_level_snapshot ────────────────────────────────────────────────────

def _data_with_two_tanks():
    return {
        "tanks": {
            "T1": {"product": "X", "current_level_lbs": 10000},
            "T2": {"product": "X", "current_level_lbs": 20000},
        },
        "level_history": [],
    }


def test_record_appends_snapshot():
    d = _data_with_two_tanks()
    record_level_snapshot(d, run_hour=0.0)
    assert len(d["level_history"]) == 1
    snap = d["level_history"][0]
    assert snap["run_hour"] == 0.0
    assert snap["tanks"] == {"T1": 10000.0, "T2": 20000.0}
    assert "iso" in snap


def test_record_idempotent_at_same_run_hour():
    """Calling record_level_snapshot twice at the same run_hour
    replaces the entry instead of duplicating — protects against
    operator double-clicks or a re-run of the same advance."""
    d = _data_with_two_tanks()
    record_level_snapshot(d, run_hour=0.0)
    d["tanks"]["T1"]["current_level_lbs"] = 9000   # mutate between records
    record_level_snapshot(d, run_hour=0.0)
    assert len(d["level_history"]) == 1
    # Latest snapshot reflects the mutation
    assert d["level_history"][0]["tanks"]["T1"] == 9000.0


def test_record_truncates_to_max_entries():
    """When the ring buffer fills, the oldest entries are discarded."""
    d = _data_with_two_tanks()
    # Append more than the limit
    for h in range(LEVEL_HISTORY_MAX_ENTRIES + 50):
        record_level_snapshot(d, run_hour=float(h))
    assert len(d["level_history"]) == LEVEL_HISTORY_MAX_ENTRIES
    # The OLDEST entries are gone; the newest survive
    assert d["level_history"][0]["run_hour"] == 50.0
    assert d["level_history"][-1]["run_hour"] == LEVEL_HISTORY_MAX_ENTRIES + 49.0


def test_record_handles_missing_history_field():
    """A data dict with no level_history key gets one initialized."""
    d = {"tanks": {}}
    record_level_snapshot(d, run_hour=0.0)
    assert "level_history" in d
    assert len(d["level_history"]) == 1


def test_record_handles_none_history_field():
    """A data dict with level_history=None gets it replaced with a list."""
    d = {"tanks": {}, "level_history": None}
    record_level_snapshot(d, run_hour=0.0)
    assert d["level_history"] is not None
    assert len(d["level_history"]) == 1


def test_snapshot_captures_all_tanks(defaults_dict):
    """Every tank in the data dict shows up in the snapshot."""
    d = copy.deepcopy(defaults_dict)
    d["level_history"] = []
    record_level_snapshot(d, run_hour=0.0)
    snap_tanks = d["level_history"][0]["tanks"]
    assert set(snap_tanks.keys()) == set(d["tanks"].keys())


# ── downsample_for_chart ─────────────────────────────────────────────────────

def test_downsample_passes_through_short_history():
    history = [{"run_hour": float(i)} for i in range(10)]
    out = downsample_for_chart(history, max_points=720)
    assert out == history


def test_downsample_caps_long_history():
    history = [{"run_hour": float(i)} for i in range(2000)]
    out = downsample_for_chart(history, max_points=200)
    assert len(out) <= 201   # last entry always appended
    # First and last preserved
    assert out[0]["run_hour"] == 0.0
    assert out[-1]["run_hour"] == 1999.0


def test_downsample_empty_returns_empty():
    assert downsample_for_chart([]) == []


def test_downsample_zero_max_points_passes_through():
    """Edge case: max_points=0 means 'no decimation' rather than 'drop
    everything', because dropping everything would silently break the
    chart."""
    history = [{"run_hour": 1.0}, {"run_hour": 2.0}]
    assert downsample_for_chart(history, max_points=0) == history


# ── inventory_age_hours ──────────────────────────────────────────────────────


def _make_history(run_hours_and_levels):
    """Build a history list: [(run_hour, level_lbs), ...]."""
    return [
        {"run_hour": float(rh), "iso": "2026-01-01T00:00:00",
         "tanks": {"T1": float(lvl)}}
        for rh, lvl in run_hours_and_levels
    ]


def test_age_empty_history():
    """No history → (0.0, True) — no data, treat as capped."""
    age, capped = inventory_age_hours("T1", [], current_run_hour=100.0)
    assert age == 0.0
    assert capped is True


def test_age_dipped_recently():
    """Tank dipped below threshold at run_hour=50; current=100 → age=50h."""
    history = _make_history([
        (10, 5000),   # above threshold
        (50, 1500),   # below 2000 lbs — most recent dip
        (80, 8000),   # back above
        (100, 9000),
    ])
    age, capped = inventory_age_hours("T1", history, current_run_hour=100.0)
    assert age == pytest.approx(50.0)
    assert capped is False


def test_age_never_dipped():
    """Tank always above threshold in history → capped=True, age = time
    since oldest entry."""
    history = _make_history([
        (10, 5000),
        (50, 8000),
        (100, 9000),
    ])
    age, capped = inventory_age_hours("T1", history, current_run_hour=100.0)
    assert age == pytest.approx(90.0)   # 100 - 10
    assert capped is True


def test_age_dipped_at_start():
    """Tank dipped below threshold only in the very first history entry."""
    history = _make_history([
        (0,   500),    # below 2000
        (24,  5000),
        (100, 8000),
    ])
    age, capped = inventory_age_hours("T1", history, current_run_hour=100.0)
    assert age == pytest.approx(100.0)
    assert capped is False


def test_age_currently_below_threshold():
    """If the MOST RECENT entry is itself below threshold, age is ~0."""
    history = _make_history([
        (80, 8000),
        (99, 1000),   # below threshold
    ])
    age, capped = inventory_age_hours("T1", history, current_run_hour=100.0)
    assert age == pytest.approx(1.0)
    assert capped is False


def test_age_tank_absent_from_history():
    """Tank name not in any history entry → (0.0, True)."""
    history = _make_history([(10, 5000), (50, 8000)])
    age, capped = inventory_age_hours("MISSING", history, current_run_hour=100.0)
    assert age == 0.0
    assert capped is True


def test_age_custom_threshold():
    """threshold_lbs is configurable."""
    history = _make_history([
        (10, 4500),   # below custom threshold of 5000
        (50, 7000),
    ])
    age, capped = inventory_age_hours(
        "T1", history, current_run_hour=100.0, threshold_lbs=5000.0
    )
    assert age == pytest.approx(90.0)   # 100 - 10
    assert capped is False
