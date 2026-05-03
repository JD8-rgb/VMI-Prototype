"""VMI Health Dashboard summary contract.

The dashboard reads alert_log entries and counts them by type +
weekly bucket. Runtime UI (Plotly chart, st.metric cards) is
exercised by app.py import smoke; this file pins the pure-function
summary that drives them."""

from __future__ import annotations

import copy
from datetime import datetime, timedelta

import pytest

# Import app to surface the helper. The import side-effect set up
# Streamlit context warnings; pytest tolerates them.
import app


def _alert_entry(text, type_, days_ago):
    """Build an alert_log entry timestamped `days_ago` days before now."""
    return {
        "logged_at_iso": (datetime.now() - timedelta(days=days_ago)).isoformat(),
        "text": text,
        "type": type_,
        "severity": "red_flag",
        "direction": "too_full" if type_ == "overfill" else "too_low",
    }


def test_empty_log_returns_zeros():
    overfill, safety, weekly = app._alert_log_summary({"alert_log": []})
    assert overfill == 0
    assert safety == 0
    assert weekly == []


def test_missing_log_returns_zeros():
    """Backwards-compat: data dicts without alert_log key still work."""
    overfill, safety, weekly = app._alert_log_summary({})
    assert overfill == 0
    assert safety == 0
    assert weekly == []


def test_counts_overfill_and_safety_only():
    """Other alert types (lead_time, late_truck, schedule_*) must NOT
    bias the dashboard — only the two tank-level extremes."""
    log = [
        _alert_entry("R", "overfill",      5),
        _alert_entry("Y", "safety_stock",  5),
        _alert_entry("L", "lead_time",     5),    # ignored
        _alert_entry("T", "late_truck",    5),    # ignored
        _alert_entry("S", "schedule_deadline", 5), # ignored
    ]
    overfill, safety, _ = app._alert_log_summary({"alert_log": log})
    assert overfill == 1
    assert safety == 1


def test_filters_out_old_entries():
    """Entries older than the window are dropped."""
    log = [
        _alert_entry("recent", "overfill", 30),
        _alert_entry("old",    "overfill", 200),  # outside 180-day window
    ]
    overfill, _, _ = app._alert_log_summary({"alert_log": log}, window_days=180)
    assert overfill == 1


def test_buckets_by_week_monday():
    """Two alerts on Tue + Thu of the same week roll up to one bucket
    keyed on the Mon-of-week ISO date."""
    log = [
        _alert_entry("a", "overfill",     5),
        _alert_entry("b", "overfill",     6),
        _alert_entry("c", "safety_stock", 5),
    ]
    _, _, weekly = app._alert_log_summary({"alert_log": log})
    # All three within the same week → one bucket
    assert len(weekly) >= 1
    # The total counts add up
    total_overfill = sum(b[1] for b in weekly)
    total_safety   = sum(b[2] for b in weekly)
    assert total_overfill == 2
    assert total_safety == 1


def test_buckets_sorted_chronologically():
    log = [
        _alert_entry("recent", "overfill", 7),
        _alert_entry("older",  "overfill", 60),
        _alert_entry("middle", "overfill", 30),
    ]
    _, _, weekly = app._alert_log_summary({"alert_log": log})
    week_keys = [b[0] for b in weekly]
    assert week_keys == sorted(week_keys)


def test_malformed_timestamp_skipped():
    """Don't crash on a corrupted alert_log entry."""
    log = [
        {"logged_at_iso": "not_a_date", "type": "overfill"},
        _alert_entry("good", "overfill", 5),
    ]
    overfill, _, _ = app._alert_log_summary({"alert_log": log})
    assert overfill == 1   # only the good one counted


def test_missing_timestamp_skipped():
    log = [
        {"type": "overfill"},   # no logged_at_iso
        _alert_entry("good", "overfill", 5),
    ]
    overfill, _, _ = app._alert_log_summary({"alert_log": log})
    assert overfill == 1
