"""Unit tests for app._entries_overlap_pairs().

Pin the absolute-hours-from-week-start overlap detection introduced to
catch cross-day overlaps the old per-weekday check missed (e.g.
Mon-Tue + Tue-Thu collide on Tuesday but have different `weekday`
fields).
"""
from __future__ import annotations

from app import _entries_overlap_pairs


def test_no_overlaps_returns_empty():
    """Three non-overlapping single-day windows → no pairs reported."""
    entries = [
        (0, 6, 16),   # Mon 06-16
        (1, 6, 16),   # Tue 06-16
        (2, 6, 16),   # Wed 06-16
    ]
    assert _entries_overlap_pairs(entries) == []


def test_simple_same_day_overlap():
    """Two Mon windows that overlap on Mon 14:00."""
    entries = [
        (0, 6, 16),   # Mon 06-16
        (0, 14, 22),  # Mon 14-22
    ]
    assert _entries_overlap_pairs(entries) == [(1, 2)]


def test_multi_day_cross_overlap():
    """The user's reported case: Mon-Tue + Tue-Thu collide on Tue 06:00–16:00.
    The old per-weekday check missed this because the windows have
    different `weekday` fields. Absolute-hours check: [6, 40] and
    [30, 88] overlap on [30, 40]."""
    entries = [
        (0, 6, 40),   # Mon 06:00 → Tue 16:00
        (1, 6, 64),   # Tue 06:00 → Thu 16:00
    ]
    assert _entries_overlap_pairs(entries) == [(1, 2)]


def test_touching_not_overlapping():
    """Windows that share an endpoint but don't overlap: Mon 06-16 +
    Mon 16-22 → end of one == start of next, no overlap."""
    entries = [
        (0, 6, 16),
        (0, 16, 22),
    ]
    assert _entries_overlap_pairs(entries) == []


def test_three_way_overlap_returns_all_pairs():
    """Three windows that all overlap on Mon 10:00-12:00."""
    entries = [
        (0, 6, 16),    # Mon 06-16
        (0, 8, 14),    # Mon 08-14
        (0, 10, 12),   # Mon 10-12
    ]
    assert _entries_overlap_pairs(entries) == [(1, 2), (1, 3), (2, 3)]


def test_full_week_continuous_window_with_itself():
    """Mon 06:00 → Mon 04:00 (next week) wraps → entry (0, 6, 172).
    A single window can't overlap with itself."""
    entries = [(0, 6, 172)]
    assert _entries_overlap_pairs(entries) == []


def test_empty_input_returns_empty():
    """No entries → no pairs. Defensive check."""
    assert _entries_overlap_pairs([]) == []
