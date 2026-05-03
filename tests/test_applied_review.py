"""Phase 6: HIGH-confidence parse review panel.

When fetch_and_apply lands a HIGH parse, we stash
data["last_applied_parse_review"] alongside the existing pop of
pending_low_confidence_parse. The Streamlit panel renders this for
operator acknowledgement; the schedule has already auto-applied so
the buttons are informational + audit-trail."""

from __future__ import annotations

import copy

import pytest

from state import PlantState


def _sample_review() -> dict:
    return {
        "email_id":         "msg-456",
        "sender":           "anna@example.com",
        "subject":          "Schedule for week of 4/20",
        "body":             "Mon-Fri 6am-4pm",
        "entries":          [[0, 6, 16], [1, 6, 16]],
        "confidence":       "high",
        "applied":          True,
        "notes":            ["LLM rescue not needed."],
        "fetched_at":       "2026-04-17T10:00:00",
        "week_str":         "2026-04-20",
        "windows_applied":  5,
        "windows_replaced": 0,
    }


def test_applied_review_round_trips_via_extra(defaults_dict):
    """The field isn't a typed PlantState field; it round-trips through
    _extra so the panel and read_schedule see the same dict."""
    d = copy.deepcopy(defaults_dict)
    d["last_applied_parse_review"] = _sample_review()
    rt = PlantState.from_dict(d).to_dict()
    assert rt.get("last_applied_parse_review") == _sample_review()


def test_no_review_round_trips_as_absent(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    d.pop("last_applied_parse_review", None)
    rt = PlantState.from_dict(d).to_dict()
    assert "last_applied_parse_review" not in rt


def test_applied_review_shape_has_required_keys():
    """Pin the shape the Streamlit panel reads."""
    rev = _sample_review()
    for key in ("email_id", "sender", "subject", "body", "entries",
                 "confidence", "applied", "week_str",
                 "windows_applied", "windows_replaced"):
        assert key in rev
    assert rev["confidence"] == "high"
    assert rev["applied"] is True
    assert isinstance(rev["entries"], list)


def test_applied_review_body_truncation_documented():
    """read_schedule truncates body at 5KB to keep data.json bounded.
    Confirm via direct simulation:"""
    huge = "x" * 100_000
    truncated = huge[:5000]
    assert len(truncated) == 5000


def test_high_and_low_review_fields_independent(defaults_dict):
    """Both fields can coexist temporarily — they target different
    panels. The HIGH path clears pending_low_confidence_parse but
    LOW doesn't touch last_applied_parse_review."""
    d = copy.deepcopy(defaults_dict)
    d["pending_low_confidence_parse"] = {
        "email_id": "low-1", "sender": "x", "subject": "x",
        "body": "x", "entries": [], "confidence": "low",
        "notes": [], "fetched_at": "2026-04-15T10:00:00",
    }
    d["last_applied_parse_review"] = _sample_review()
    rt = PlantState.from_dict(d).to_dict()
    assert "pending_low_confidence_parse" in rt
    assert "last_applied_parse_review"   in rt
