"""apply_schedule_to_data behavior contracts.

This is the function that takes parsed parser entries and writes them
into data["run_schedule"]. Critical for the demo flow — bugs here
mean the operator's hand-applied schedule doesn't take effect, or the
prior schedule isn't cleared properly.

Coverage:
  - replace mode (default) drops the target week's existing windows
    and writes the parsed entries; marks schedule_received_for_week
  - merge mode is additive — keeps existing windows for days NOT in
    parsed entries; does NOT mark the week as received
  - empty entries is a no-op safety net (don't blow away the schedule
    just because the parser had a bad day)
  - dry_run preserves data["run_schedule"] unchanged

All tests pin now_dt to the simulation epoch so target-week bounds
are deterministic regardless of real wall clock.
"""

from __future__ import annotations

import copy
import logging
from datetime import datetime

import pytest

from read_schedule import apply_schedule_to_data


# Pin "now" to the simulation epoch so target week is run-hour 168-336
# (week of Mon 2026-04-20).
NOW = datetime(2026, 4, 13, 0, 0, 0)


@pytest.fixture(autouse=True)
def silence_logger():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


def test_replace_mode_writes_target_week_entries(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    # Target week starts at run-hour 168 (next Mon = 2026-04-20)
    entries = [(0, 6, 16), (1, 6, 16), (2, 6, 16)]  # Mon, Tue, Wed 6-16
    d, removed, new = apply_schedule_to_data(d, entries, now_dt=NOW)
    assert removed == 0   # defaults has no target-week windows
    assert len(new) == 3
    # New windows are in the target week (run-hour 168-336)
    for w in new:
        assert 168 <= w["start_hour"] < 336
    # schedule_received_for_week marked
    assert d["schedule_received_for_week"] == "2026-04-20"


def test_replace_mode_drops_existing_target_week_windows(defaults_dict):
    """Pre-existing target-week windows must be replaced, not duplicated."""
    d = copy.deepcopy(defaults_dict)
    # Pre-seed two windows in the target week
    d["run_schedule"].append({"start_hour": 174.0, "end_hour": 190.0,
                                "label": "Mon-stale"})
    d["run_schedule"].append({"start_hour": 198.0, "end_hour": 214.0,
                                "label": "Tue-stale"})
    # Apply a new schedule covering only Wed
    new_entries = [(2, 6, 16)]
    d, removed, new = apply_schedule_to_data(d, new_entries, now_dt=NOW)
    assert removed == 2
    assert len(new) == 1
    # No "stale" labels remain
    labels = [w["label"] for w in d["run_schedule"]]
    assert "Mon-stale" not in labels
    assert "Tue-stale" not in labels


def test_merge_mode_keeps_other_days(defaults_dict):
    """Pre-existing target-week windows for days NOT in entries must
    survive a merge."""
    d = copy.deepcopy(defaults_dict)
    d["run_schedule"].append({"start_hour": 174.0, "end_hour": 190.0,
                                "label": "Mon-existing"})
    d["run_schedule"].append({"start_hour": 198.0, "end_hour": 214.0,
                                "label": "Tue-existing"})
    # Apply only Wed
    d, removed, new = apply_schedule_to_data(
        d, [(2, 6, 16)], mode="merge", now_dt=NOW)
    assert removed == 0
    labels = [w["label"] for w in d["run_schedule"]]
    assert "Mon-existing" in labels
    assert "Tue-existing" in labels
    # The new Wed window is added
    assert any("Wed" in (w.get("label") or "") for w in new)


def test_merge_mode_replaces_overlapping_day(defaults_dict):
    """If merge entries cover Mon, the existing Mon target-week window
    is replaced with the new Mon one."""
    d = copy.deepcopy(defaults_dict)
    d["run_schedule"].append({"start_hour": 174.0, "end_hour": 190.0,
                                "label": "Mon-existing"})
    d, removed, new = apply_schedule_to_data(
        d, [(0, 8, 18)], mode="merge", now_dt=NOW)
    assert removed == 1
    labels = [w["label"] for w in d["run_schedule"]]
    assert "Mon-existing" not in labels


def test_merge_mode_does_not_mark_received(defaults_dict):
    """merge is additive / partial — the schedule_received_for_week
    flag stays unset so the missing-schedule reminder still fires."""
    d = copy.deepcopy(defaults_dict)
    d["schedule_received_for_week"] = None
    apply_schedule_to_data(d, [(0, 6, 16)], mode="merge", now_dt=NOW)
    assert d["schedule_received_for_week"] is None


def test_empty_entries_is_noop(defaults_dict):
    """Empty parsed entries must NOT clear the existing schedule
    (parser bad day shouldn't blow away an operator's prior schedule)."""
    d = copy.deepcopy(defaults_dict)
    before_count = len(d["run_schedule"])
    d_after, removed, new = apply_schedule_to_data(d, [], now_dt=NOW)
    assert removed == 0
    assert new == []
    assert len(d_after["run_schedule"]) == before_count
    assert d_after["schedule_received_for_week"] is None


def test_dry_run_does_not_mutate_run_schedule(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    before = list(d["run_schedule"])
    d_after, removed, new = apply_schedule_to_data(
        d, [(0, 6, 16), (1, 6, 16)], dry_run=True, now_dt=NOW
    )
    # Removed is the count that WOULD be removed (zero in defaults)
    # but the actual run_schedule list is unchanged
    assert d_after["run_schedule"] == before
    # New windows reported but not committed
    assert len(new) == 2
