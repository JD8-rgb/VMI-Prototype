"""Unit tests for forecast._compute_forecast_cutoff().

Pin the rule: forecast starts at Monday 06:00 of the week immediately
AFTER the latest scheduled week. Anchor is the END of the latest
future-going window (handles multi-week windows). When no future
windows exist, anchor = current sim-time, so forecast starts on
Monday 06:00 of next calendar week.
"""
from __future__ import annotations

from copy import deepcopy

from alerts import _as_state
from forecast import _compute_forecast_cutoff


def _make_state(defaults_dict, **overrides):
    """Fixture: epoch = Mon 2026-05-04 00:00, current_run_hour = 0,
    no scheduled trucks. Override any field via kwargs."""
    d = deepcopy(defaults_dict)
    d['simulation_epoch'] = '2026-05-04T00:00:00'
    d['current_run_hour'] = 0.0
    d['scheduled_trucks'] = []
    d.update(overrides)
    return _as_state(d)


def test_no_future_windows_uses_now_anchor(defaults_dict):
    """Empty run_schedule → anchor = now (Mon 2026-05-04 00:00).
    Forecast cutoff = Mon 2026-05-11 06:00 = run_hour 174."""
    state = _make_state(defaults_dict, run_schedule=[])
    cutoff = _compute_forecast_cutoff(state, current_run_hour=0.0)
    assert cutoff == 174.0


def test_single_week_window_anchors_to_that_week(defaults_dict):
    """Mon-Fri current-week schedule (window ending Fri at hour 94).
    Anchor week's Monday = May 4. Forecast = Mon May 11 06:00 = 174."""
    state = _make_state(
        defaults_dict,
        run_schedule=[
            {'start_hour':  6.0, 'end_hour':  22.0, 'label': 'Mon'},
            {'start_hour': 30.0, 'end_hour':  46.0, 'label': 'Tue'},
            {'start_hour': 54.0, 'end_hour':  70.0, 'label': 'Wed'},
            {'start_hour': 78.0, 'end_hour':  94.0, 'label': 'Thu'},
            {'start_hour':102.0, 'end_hour': 118.0, 'label': 'Fri'},
        ],
    )
    cutoff = _compute_forecast_cutoff(state, current_run_hour=0.0)
    assert cutoff == 174.0


def test_multi_week_window_anchors_to_last_week(defaults_dict):
    """Single window spanning May 4 → May 16 (12 days, two weeks).
    Anchor = end of window = Sat May 16 → week of May 11. Forecast
    = Mon May 18 06:00 = run_hour 342."""
    state = _make_state(
        defaults_dict,
        run_schedule=[{'start_hour': 0.0, 'end_hour': 288.0, 'label': '12d'}],
    )
    cutoff = _compute_forecast_cutoff(state, current_run_hour=0.0)
    assert cutoff == 342.0


def test_only_past_windows_treated_as_empty(defaults_dict):
    """Window with end_hour <= current_run_hour is filtered out.
    With current_run_hour = 200 (Tue May 12 08:00), the past Monday
    window doesn't count. Anchor = now. Forecast = next week's Mon
    06:00. May 12 → next Mon = May 18 → cutoff = 342."""
    state = _make_state(
        defaults_dict,
        current_run_hour=200.0,
        run_schedule=[{'start_hour': 6.0, 'end_hour': 22.0, 'label': 'past'}],
    )
    cutoff = _compute_forecast_cutoff(state, current_run_hour=200.0)
    assert cutoff == 342.0


def test_window_spanning_week_boundary_uses_end(defaults_dict):
    """Sun 22:00 → Mon 06:00 wrap. start_hour = Sun May 10 22:00
    = 144 + 22 = 166. end_hour = Mon May 11 06:00 = 168 + 6 = 174.
    Anchor by END → end_dt is Mon May 11 → week-of-May-11. Forecast
    = May 11 + 7d + 6h = May 18 06:00 = 174 + 168 = 342.
    (Verifies anchor uses end_hour, not start_hour — the start
    falls in the May 4-10 week, which would yield 174 if used.)"""
    state = _make_state(
        defaults_dict,
        run_schedule=[{'start_hour': 166.0, 'end_hour': 174.0, 'label': 'wrap'}],
    )
    cutoff = _compute_forecast_cutoff(state, current_run_hour=0.0)
    assert cutoff == 342.0
