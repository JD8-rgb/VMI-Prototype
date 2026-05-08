"""
Tests for `read_schedule.apply_run_window_overrides`.

The helper backs the "Edit run windows" dialog — a mid-week schedule
override that replaces every run_schedule window in a scope range with
operator-edited entries, while preserving windows outside the scope.

Distinct from `apply_schedule_to_data` (the parse-and-apply path):
- never sets `schedule_received_for_week`
- accepts empty edited_entries (full-clear of scope)
- scope is bounded by absolute run-hour, not weekday
"""
import copy

from read_schedule import apply_run_window_overrides


def _state_with_schedule(schedule):
    """Minimal data dict for the helper. simulation_epoch is a Monday at
    00:00 so run-hour 0 = Mon 00:00 of week 0; scope spans 0-336 covers
    weeks 0-1, anything ≥ 336 is week 2+."""
    return {
        "simulation_epoch": "2026-04-13T00:00:00",
        "current_run_hour": 0.0,
        "run_schedule": list(schedule),
    }


def test_replace_one_day_with_a_longer_shift():
    """Pre-existing Mon-Fri 6-16 windows, operator changes Friday's
    end-time to 22 (longer shift). Every other day stays the same."""
    sched = [
        {"start_hour": d * 24 + 6, "end_hour": d * 24 + 16,
         "label": ["Mon","Tue","Wed","Thu","Fri"][d]}
        for d in range(5)
    ]
    data = _state_with_schedule(sched)

    edited = [
        (0,  6, 16),  # Mon unchanged
        (1,  6, 16),  # Tue unchanged
        (2,  6, 16),  # Wed unchanged
        (3,  6, 16),  # Thu unchanged
        (4,  6, 22),  # Fri 6-22 (was 6-16)
    ]
    _data, removed, added = apply_run_window_overrides(
        data, edited, scope_start_rh=0.0, scope_end_rh=14*24,
    )
    assert removed == 5
    assert added   == 5
    assert len(data["run_schedule"]) == 5
    fri = next(w for w in data["run_schedule"] if w["label"] == "Fri")
    assert fri["start_hour"] == 4*24 + 6
    assert fri["end_hour"]   == 4*24 + 22


def test_delete_a_day_reduces_count():
    """'No run Thursday this week' — operator deletes the Thursday row."""
    sched = [
        {"start_hour": d * 24 + 6, "end_hour": d * 24 + 16,
         "label": ["Mon","Tue","Wed","Thu","Fri"][d]}
        for d in range(5)
    ]
    data = _state_with_schedule(sched)

    edited = [
        (0, 6, 16), (1, 6, 16), (2, 6, 16),
        # Thu (3) deliberately omitted
        (4, 6, 16),
    ]
    _data, removed, added = apply_run_window_overrides(
        data, edited, scope_start_rh=0.0, scope_end_rh=14*24,
    )
    assert removed == 5
    assert added   == 4
    assert len(data["run_schedule"]) == 4
    assert not any(w["label"] == "Thu"
                   and 0 <= w["start_hour"] < 14*24
                   for w in data["run_schedule"])


def test_add_a_new_window_increases_count():
    """Customer added a Saturday 8-12 shift mid-week."""
    sched = [
        {"start_hour": d * 24 + 6, "end_hour": d * 24 + 16, "label": "x"}
        for d in range(5)
    ]
    data = _state_with_schedule(sched)

    edited = [
        (0, 6, 16), (1, 6, 16), (2, 6, 16), (3, 6, 16), (4, 6, 16),
        (5, 8, 12),   # NEW Saturday
    ]
    _data, removed, added = apply_run_window_overrides(
        data, edited, scope_start_rh=0.0, scope_end_rh=14*24,
    )
    assert added == 6
    sat = next(w for w in data["run_schedule"] if w["label"] == "Sat")
    assert sat["start_hour"] == 5*24 + 8
    assert sat["end_hour"]   == 5*24 + 12


def test_empty_edited_entries_clears_scope_only():
    """Operator empties every row in the dialog (plant down for the
    next two weeks) — every window IN scope is removed; windows OUTSIDE
    scope (e.g. last week's history, week-3 lookahead) are preserved."""
    sched = [
        {"start_hour": -7*24, "end_hour": -7*24 + 8, "label": "last-week"},  # week-1 window
        {"start_hour": 0,     "end_hour": 16,        "label": "this-week"},   # in scope
        {"start_hour": 7*24,  "end_hour": 7*24 + 16, "label": "next-week"},   # in scope
        {"start_hour": 21*24, "end_hour": 21*24 + 8, "label": "week-3"},      # past scope
    ]
    data = _state_with_schedule(sched)

    _data, removed, added = apply_run_window_overrides(
        data, [], scope_start_rh=0.0, scope_end_rh=14*24,
    )
    assert removed == 2
    assert added   == 0
    labels = {w["label"] for w in data["run_schedule"]}
    assert labels == {"last-week", "week-3"}


def test_spanning_window_into_scope_is_removed_when_scope_cleared():
    """Audit P1: a window whose start_hour is BEFORE scope_start but
    whose end_hour is AFTER scope_start touches the editable scope
    and must be removed when the operator clears the editor.

    Pre-fix: the filter was `not (scope_start <= start_hour < scope_end)`,
    which kept any window starting before scope. A Sun 22:00 → Mon
    06:00 overnight opened on Mon morning would survive a 'clear all'
    Apply, leaving the Mon-portion silently active.

    Post-fix: overlap test removes any window that touches the scope."""
    sched = [
        # Spans into scope from 2 hours before scope_start (rh=-2 → rh=10)
        {"start_hour": -2.0, "end_hour": 10.0, "label": "Sun-spans-in"},
        {"start_hour": 24.0, "end_hour": 40.0, "label": "Tue-in-scope"},
        # Fully outside scope (entirely before)
        {"start_hour": -200.0, "end_hour": -150.0, "label": "way-back"},
    ]
    data = _state_with_schedule(sched)

    # Operator clears all rows → empty edited_entries
    _data, removed, added = apply_run_window_overrides(
        data, [], scope_start_rh=0.0, scope_end_rh=14*24,
    )
    # Both the spanning AND the in-scope window are removed.
    # The way-back window survives.
    assert removed == 2
    assert added   == 0
    assert {w["label"] for w in data["run_schedule"]} == {"way-back"}


def test_label_uses_calendar_weekday_not_scope_day():
    """Audit P2a: the new windows' labels must reflect the calendar
    weekday of their actual start datetime, not `scope_day % 7`. Under
    the old code, scope_day=0 always rendered as 'Mon' regardless of
    what calendar day scope_start_rh actually fell on."""
    # Epoch is 2026-04-13 (Monday). If we call with scope_start_rh that
    # points at Wednesday (rh=48 = Wed midnight), scope_day=0 should
    # produce label='Wed', not 'Mon'.
    data = _state_with_schedule([])

    edited = [(0, 6, 16)]   # scope_day=0 at hour 6 to 16
    _data, _r, _a = apply_run_window_overrides(
        data, edited,
        scope_start_rh=48.0,           # Wed midnight (epoch is Monday)
        scope_end_rh=48.0 + 14*24,
    )
    assert len(data["run_schedule"]) == 1
    assert data["run_schedule"][0]["label"] == "Wed"


def test_windows_outside_scope_are_never_touched():
    """Editing this+next week must leave week-3 and prior weeks intact."""
    sched = [
        {"start_hour": -7*24,        "end_hour": -7*24 + 8,
         "label": "last-Mon-history"},
        {"start_hour": 24*0 + 6,     "end_hour": 24*0 + 16, "label": "Mon"},
        {"start_hour": 24*4 + 6,     "end_hour": 24*4 + 16, "label": "Fri"},
        {"start_hour": 24*21 + 6,    "end_hour": 24*21 + 16,
         "label": "wk3-Mon"},
    ]
    data = _state_with_schedule(sched)

    edited = [(0, 6, 22)]   # only Monday this week, longer shift
    _data, removed, added = apply_run_window_overrides(
        data, edited, scope_start_rh=0.0, scope_end_rh=14*24,
    )
    # In-scope: Mon + Fri (2 windows) replaced with the 1 edited row
    assert removed == 2
    assert added   == 1

    # Out-of-scope windows survived
    surviving_labels = {w["label"] for w in data["run_schedule"]}
    assert "last-Mon-history" in surviving_labels
    assert "wk3-Mon" in surviving_labels

    # Edited Monday is the new long shift
    new_mon = next(w for w in data["run_schedule"]
                   if w["label"] == "Mon" and 0 <= w["start_hour"] < 14*24)
    assert new_mon["end_hour"] - new_mon["start_hour"] == 16
