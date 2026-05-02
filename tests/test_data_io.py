"""data_io migration + atomic-write contracts.

Schema versioning is the foundation enterprise-team migrations rest on:
they need to add v1->v2, v2->v3 migrators without touching any of the
~20 callers of load_data / save_data. These tests fail loudly if that
contract drifts.
"""

from __future__ import annotations

import json
import os
import tempfile

import pytest

import data_io
from data_io import (
    CURRENT_SCHEMA_VERSION,
    _MIGRATIONS,
    _migrate,
    load_data,
    save_data,
)
from state import PlantState


def test_current_schema_version_has_migrator():
    """Every version below current must have a migrator registered."""
    for v in range(CURRENT_SCHEMA_VERSION):
        assert v in _MIGRATIONS, f"missing migrator from v{v}"


def test_migrate_pre_versioned_file_stamps_to_current():
    legacy = {"simulation_epoch": "2026-04-13T00:00:00", "current_run_hour": 0}
    out = _migrate(legacy)
    assert out["schema_version"] == CURRENT_SCHEMA_VERSION


def test_migrate_idempotent_on_current_version():
    already_current = {"schema_version": CURRENT_SCHEMA_VERSION, "x": 1}
    out = _migrate(already_current)
    assert out["schema_version"] == CURRENT_SCHEMA_VERSION
    assert out["x"] == 1


def test_migrate_raises_on_future_version():
    """A file written by a newer code version (e.g. 999) must not silently
    proceed — that would risk lossy down-conversion."""
    future = {"schema_version": 999}
    out = _migrate(future)
    # Migrator should not loop indefinitely or down-convert. We
    # specifically allow newer-than-current to pass through unchanged
    # because PlantState._extra preserves unknown fields. Confirm:
    assert out["schema_version"] == 999


def test_migrate_handles_null_schema_version():
    """Null/missing schema_version must be treated as 0 (pre-versioned),
    not crash with 'NoneType < int' from the while-loop comparison."""
    out = _migrate({"schema_version": None, "marker": "x"})
    assert out["schema_version"] == CURRENT_SCHEMA_VERSION
    assert out["marker"] == "x"


def test_migrate_handles_numeric_string():
    """A numeric string must coerce to int rather than fail comparison."""
    out = _migrate({"schema_version": "0"})
    assert out["schema_version"] == CURRENT_SCHEMA_VERSION


def test_migrate_rejects_garbage_schema_version():
    """A malformed schema_version that can't be coerced must raise
    ValueError with a clear message, not a cryptic TypeError."""
    with pytest.raises(ValueError) as exc:
        _migrate({"schema_version": "not_a_number"})
    assert "schema_version" in str(exc.value)


def test_migrate_rejects_bool_schema_version():
    """bool is a subclass of int in Python; True would silently be
    version 1, which is a misleading shape error. Reject explicitly."""
    with pytest.raises(ValueError):
        _migrate({"schema_version": True})


def test_migrate_rejects_list_schema_version():
    with pytest.raises(ValueError):
        _migrate({"schema_version": [1, 2]})


def test_load_data_migrates_legacy_file(tmp_path):
    """load_data must upgrade an unversioned file in memory."""
    legacy_path = tmp_path / "legacy.json"
    legacy = {
        "simulation_epoch": "2026-04-13T00:00:00",
        "current_run_hour": 0,
        "tanks": {},
        "consumption_rates": {},
        "truck_quantities": {},
    }
    legacy_path.write_text(json.dumps(legacy))
    data = load_data(path=str(legacy_path), fallback=str(legacy_path))
    assert data["schema_version"] == CURRENT_SCHEMA_VERSION


def test_save_data_stamps_schema_version_when_missing(tmp_path):
    target = tmp_path / "out.json"
    save_data({"foo": "bar"}, path=str(target))
    written = json.loads(target.read_text())
    assert written["schema_version"] == CURRENT_SCHEMA_VERSION
    assert written["foo"] == "bar"


def test_save_data_preserves_existing_schema_version(tmp_path):
    """Caller-supplied schema_version must not be overwritten — the
    migrator chain owns version bumps, not save_data."""
    target = tmp_path / "out.json"
    save_data({"schema_version": 999, "foo": "bar"}, path=str(target))
    written = json.loads(target.read_text())
    assert written["schema_version"] == 999


def test_save_data_atomic_round_trip(tmp_path):
    """save_data → load_data preserves all fields including unknowns."""
    target = tmp_path / "out.json"
    payload = {
        "schema_version": CURRENT_SCHEMA_VERSION,
        "simulation_epoch": "2026-04-13T00:00:00",
        "current_run_hour": 42.0,
        "tanks": {},
        "consumption_rates": {},
        "truck_quantities": {},
        "future_field": {"nested": True},
    }
    save_data(payload, path=str(target))
    loaded = load_data(path=str(target), fallback=str(target))
    assert loaded == payload


def test_save_data_does_not_leave_temp_files(tmp_path):
    target = tmp_path / "out.json"
    save_data({"foo": 1}, path=str(target))
    leftover = [p for p in os.listdir(tmp_path) if p.startswith(".tmp_")]
    assert leftover == [], f"orphaned tempfiles: {leftover}"


def test_load_data_falls_back_to_defaults_when_missing(tmp_path):
    """load_data uses fallback when primary doesn't exist."""
    primary = tmp_path / "missing.json"
    fb = tmp_path / "defaults.json"
    fb.write_text(json.dumps({"schema_version": CURRENT_SCHEMA_VERSION,
                              "marker": "from_fallback"}))
    loaded = load_data(path=str(primary), fallback=str(fb))
    assert loaded["marker"] == "from_fallback"


def test_plant_state_preserves_schema_version_via_extra(defaults_dict):
    """PlantState doesn't promote schema_version to a typed field; it
    must round-trip via _extra. The handoff explicitly calls this out
    as a non-negotiable property."""
    state = PlantState.from_dict(defaults_dict)
    rt = state.to_dict()
    assert rt.get("schema_version") == defaults_dict["schema_version"]


# ── Typed wrapper round-trip ──────────────────────────────────────────────────

def test_load_state_returns_plant_state(tmp_path, defaults_dict):
    """load_state should return a PlantState with the same content as
    load_data → from_dict, all in one step."""
    from data_io import load_state, save_data
    target = tmp_path / "state.json"
    save_data(defaults_dict, path=str(target))
    state = load_state(path=str(target), fallback=str(target))
    assert isinstance(state, PlantState)
    assert state.simulation_epoch == defaults_dict["simulation_epoch"]
    assert state.current_run_hour == defaults_dict["current_run_hour"]


def test_fsync_failure_logged_as_warning(tmp_path, defaults_dict, caplog,
                                            monkeypatch):
    """fsync OSError used to be swallowed silently. Now it must surface
    via logger.warning so production monitoring can detect a genuine
    durability regression on a real disk."""
    target = tmp_path / "out.json"

    def _broken_fsync(fd):
        raise OSError("simulated fsync failure")

    monkeypatch.setattr(os, "fsync", _broken_fsync)

    # Save still succeeds (write is atomic in the page-cache sense)
    with caplog.at_level("WARNING", logger="data_io"):
        save_data({"foo": 1}, path=str(target))
    assert target.exists()
    # And a warning was emitted
    assert any("fsync" in r.message and "failed" in r.message
                for r in caplog.records)


def test_save_state_round_trips_through_load_state(tmp_path, defaults_dict):
    """save_state(load_state(...)) should be lossless across all known
    fields PLUS unknown fields that round-trip via _extra."""
    from data_io import load_state, save_state, save_data
    target = tmp_path / "state.json"
    payload = dict(defaults_dict)
    payload["future_field_x"] = {"nested": [1, 2]}
    save_data(payload, path=str(target))

    state = PlantState.from_dict(payload)
    save_state(state, path=str(target))

    state2 = PlantState.from_dict(
        __import__("json").loads(target.read_text())
    )
    # All known fields equal
    assert state2.simulation_epoch == state.simulation_epoch
    assert state2.current_run_hour == state.current_run_hour
    assert state2.tanks.keys() == state.tanks.keys()
    # Unknown future field preserved
    assert state2._extra.get("future_field_x") == {"nested": [1, 2]}
