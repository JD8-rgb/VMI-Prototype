"""
Audit Round 5: tests for `time_utils.distribute_window_across_days`.

The helper splits a run-window across every calendar day it touches,
yielding (week_monday_iso, weekday_int, hours) triples. It's the single
source of truth used by:
- forecast.py:_bucket_run_schedule_by_week (per-weekday seasonal model)
- anomaly.py:_weekly_run_hours_history    (per-week totals)

Before the fix, both call sites attributed a window's full duration to
its START weekday only — overnight and multi-day windows were skewed.
"""
from datetime import datetime
from time_utils import distribute_window_across_days
from state import RunWindow


# Simulation epoch chosen so run_hour 0 lands on Monday 00:00 of a known
# ISO week, making the math obvious in test asserts.
class _StubState:
    """Minimal state shape for time_utils — only `simulation_epoch` matters."""
    def __init__(self, epoch_iso: str = "2026-04-13T00:00:00"):
        # 2026-04-13 is a Monday. run_hour 0 = Mon 00:00.
        self.simulation_epoch = epoch_iso


def _state():
    return _StubState()


def test_single_day_window_yields_one_slice():
    """Mon 06:00 → Mon 16:00 (10 h) attributes 10 h to Mon."""
    s = _state()
    # run_hour 6 = Mon 06:00; run_hour 16 = Mon 16:00.
    w = RunWindow(start_hour=6, end_hour=16)
    slices = list(distribute_window_across_days(s, w))
    assert slices == [("2026-04-13", 0, 10.0)]


def test_overnight_window_splits_at_midnight():
    """Mon 22:00 → Tue 06:00 → Mon gets 2 h, Tue gets 6 h."""
    s = _state()
    w = RunWindow(start_hour=22, end_hour=30)   # Mon 22:00 → Tue 06:00
    slices = list(distribute_window_across_days(s, w))
    assert slices == [
        ("2026-04-13", 0, 2.0),   # Mon, 2 h
        ("2026-04-13", 1, 6.0),   # Tue, 6 h
    ]


def test_multi_day_window_distributes_three_slices():
    """Mon 06:00 → Wed 06:00 (48 h) → Mon=18, Tue=24, Wed=6."""
    s = _state()
    w = RunWindow(start_hour=6, end_hour=54)    # Mon 06:00 → Wed 06:00
    slices = list(distribute_window_across_days(s, w))
    assert slices == [
        ("2026-04-13", 0, 18.0),  # Mon: 06:00 → 24:00 = 18 h
        ("2026-04-13", 1, 24.0),  # Tue: full day
        ("2026-04-13", 2,  6.0),  # Wed: 00:00 → 06:00 = 6 h
    ]
    # Total preserved
    assert sum(h for _, _, h in slices) == 48.0


def test_week_spanning_window_uses_two_distinct_mondays():
    """Sun 22:00 (week N-1) → Mon 06:00 (week N) → split into the
    two ISO weeks correctly."""
    s = _state()
    # The Sunday before our 2026-04-13 epoch Monday is 2026-04-12 (week
    # of 2026-04-06). run_hour for Sun 22:00 = -2.0 (before epoch).
    # Use a window that starts in the SECOND ISO-week containing a
    # Sunday, e.g. Sun 2026-04-19 22:00 → Mon 2026-04-20 06:00.
    # Sun 2026-04-19 22:00 = run_hour 24*6 + 22 = 166
    # Mon 2026-04-20 06:00 = run_hour 24*7 + 6 = 174
    w = RunWindow(start_hour=166, end_hour=174)
    slices = list(distribute_window_across_days(s, w))
    assert slices == [
        ("2026-04-13", 6, 2.0),   # Sun of week-of-04-13, 22:00→24:00 = 2 h
        ("2026-04-20", 0, 6.0),   # Mon of week-of-04-20, 00:00→06:00 = 6 h
    ]


def test_empty_window_yields_nothing():
    """start_hour == end_hour yields no slices."""
    s = _state()
    w = RunWindow(start_hour=10, end_hour=10)
    assert list(distribute_window_across_days(s, w)) == []


def test_total_hours_preserved_for_arbitrary_window():
    """For any window, sum of yielded hours == window duration."""
    s = _state()
    cases = [
        (0, 1),       # 1 h
        (5, 23.5),    # mid-day window
        (23, 25),     # straddles midnight
        (47, 73),     # multi-day spanning
        (167, 169),   # week-boundary
    ]
    for start, end in cases:
        w = RunWindow(start_hour=start, end_hour=end)
        slices = list(distribute_window_across_days(s, w))
        total = sum(h for _, _, h in slices)
        assert abs(total - (end - start)) < 1e-9, (
            f"window ({start}, {end}): total {total} != duration {end - start}"
        )


# ── Regression tests on the call sites ──────────────────────────────────────


def test_seasonal_forecast_bucketing_distributes_overnight_window():
    """forecast._bucket_run_schedule_by_week previously credited the
    entire 8h of a Mon 22:00 → Tue 06:00 window to Mon. After the fix
    it should split Mon=2, Tue=6."""
    from forecast import _bucket_run_schedule_by_week

    class _S:
        simulation_epoch = "2026-04-13T00:00:00"
        run_schedule = [RunWindow(start_hour=22, end_hour=30)]   # Mon 22→Tue 06

    out = _bucket_run_schedule_by_week(_S(), target_week_start_run_hour=None)
    assert out == {
        "2026-04-13": {0: 2.0, 1: 6.0, 2: 0.0, 3: 0.0,
                        4: 0.0, 5: 0.0, 6: 0.0},
    }


def test_weekly_run_hours_history_splits_at_week_boundary():
    """anomaly._weekly_run_hours_history previously credited a Sun→Mon
    window entirely to the previous week. After the fix the two-hour
    Sunday tail and the six-hour Monday head land in their own weeks."""
    from anomaly import _weekly_run_hours_history

    class _S:
        simulation_epoch = "2026-04-13T00:00:00"
        run_schedule = [RunWindow(start_hour=166, end_hour=174)]   # Sun 22→Mon 06

    weekly = _weekly_run_hours_history(_S())
    # Two distinct weeks, each with its own slice. Order is ascending by week.
    assert weekly == [2.0, 6.0]
