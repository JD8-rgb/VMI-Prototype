"""time_utils edge-case coverage.

parse_time_input is used by every CLI script that takes a "when" argument
(advance_time, schedule_run, schedule_truck). The implausible-magnitude
check is what protects against pasting "20260415" (a date typo without
separators) and getting a 2,300-year clock advance instead of an error.
"""

from __future__ import annotations

import pytest

from time_utils import (
    DISPLAY_FORMAT,
    dt_to_run_hour,
    format_run_hour,
    get_epoch,
    parse_time_input,
    run_hour_to_dt,
)


# ── Round-trip ────────────────────────────────────────────────────────────────

def test_run_hour_zero_equals_epoch(defaults_dict):
    epoch = get_epoch(defaults_dict)
    assert run_hour_to_dt(defaults_dict, 0) == epoch


def test_run_hour_round_trip(defaults_dict):
    for rh in (0.0, 1.5, 24.0, 168.0, 720.5):
        dt = run_hour_to_dt(defaults_dict, rh)
        back = dt_to_run_hour(defaults_dict, dt)
        assert back == pytest.approx(rh)


def test_format_run_hour_matches_display_format(defaults_dict):
    s = format_run_hour(defaults_dict, 0)
    # epoch is Mon 2026-04-13 00:00 → "Mon 2026-04-13 00:00"
    assert s == "Mon 2026-04-13 00:00"


# ── parse_time_input ──────────────────────────────────────────────────────────

def test_parse_run_hour_integer(defaults_dict):
    assert parse_time_input(defaults_dict, "168") == 168.0


def test_parse_run_hour_float(defaults_dict):
    assert parse_time_input(defaults_dict, "168.5") == 168.5


def test_parse_run_hour_strips_whitespace(defaults_dict):
    assert parse_time_input(defaults_dict, "  72  ") == 72.0


def test_parse_iso_datetime_t_separator(defaults_dict):
    # Mon 2026-04-13 00:00 + 168h = Mon 2026-04-20 00:00
    rh = parse_time_input(defaults_dict, "2026-04-20T00:00")
    assert rh == 168.0


def test_parse_iso_datetime_space_separator(defaults_dict):
    # Same date with the more user-friendly space separator
    rh = parse_time_input(defaults_dict, "2026-04-20 00:00")
    assert rh == 168.0


def test_parse_rejects_implausibly_large_run_hour(defaults_dict):
    """A date typed without separators ('20260415') succeeds as float()
    but means 2,300+ years in the future. Must reject."""
    with pytest.raises(ValueError) as exc:
        parse_time_input(defaults_dict, "20260415")
    assert "run-hours" in str(exc.value)
    assert "Did you mean a date" in str(exc.value)


def test_parse_rejects_negative_implausible_magnitude(defaults_dict):
    """Same magnitude check on the negative side."""
    with pytest.raises(ValueError):
        parse_time_input(defaults_dict, "-99999")


def test_parse_accepts_negative_small_magnitude(defaults_dict):
    """A small negative is interpreted as a target run-hour (which the
    caller will diagnose) — magnitude check shouldn't fire on plausible
    historical hours."""
    assert parse_time_input(defaults_dict, "-12") == -12.0


def test_parse_rejects_garbage(defaults_dict):
    with pytest.raises(ValueError):
        parse_time_input(defaults_dict, "next thursday")


def test_parse_helpful_error_for_garbage(defaults_dict):
    """The error must include format hints so CLI users self-heal."""
    with pytest.raises(ValueError) as exc:
        parse_time_input(defaults_dict, "blah")
    msg = str(exc.value)
    assert "168" in msg or "YYYY-MM-DD" in msg, (
        "error should hint at supported formats")


# ── State + dict polymorphism ────────────────────────────────────────────────

def test_time_utils_polymorphism(defaults_dict):
    """time_utils helpers must accept either a dict or a PlantState."""
    from state import PlantState
    state = PlantState.from_dict(defaults_dict)
    # All four helpers should work with either
    assert get_epoch(defaults_dict) == get_epoch(state)
    assert run_hour_to_dt(defaults_dict, 24) == run_hour_to_dt(state, 24)
    dt = run_hour_to_dt(defaults_dict, 24)
    assert dt_to_run_hour(defaults_dict, dt) == dt_to_run_hour(state, dt)
    assert format_run_hour(defaults_dict, 24) == format_run_hour(state, 24)
