"""Operator audit-log contract.

Every Streamlit operator action records a row via audit_log.record().
These tests pin the helper's append / truncation / fail-safe semantics
+ PlantState round-trip preservation."""

from __future__ import annotations

import copy

import pytest

import audit_log as _audit
from audit_log import (
    AUDIT_LOG_MAX_ENTRIES,
    record,
    recent,
    A_VMI_TOGGLE,
    A_TARGET_APPLY,
    A_RESET,
    A_ADVANCE,
)
from state import PlantState


# ── record() basic shape ─────────────────────────────────────────────────────

def test_record_appends_entry():
    d = {}
    record(d, A_VMI_TOGGLE, details={"enabled": False})
    assert len(d["audit_log"]) == 1
    e = d["audit_log"][0]
    assert e["action"] == "vmi_toggle"
    assert e["user"] == "operator"
    assert e["details"] == {"enabled": False}
    assert "iso" in e


def test_record_initializes_missing_audit_log_field():
    d = {"some_other_field": 1}
    record(d, A_RESET)
    assert "audit_log" in d
    assert len(d["audit_log"]) == 1


def test_record_handles_none_audit_log_field():
    """Defensive: a corrupted state with audit_log=None should be
    silently re-initialized rather than crash."""
    d = {"audit_log": None}
    record(d, A_RESET)
    assert isinstance(d["audit_log"], list)
    assert len(d["audit_log"]) == 1


def test_record_handles_non_list_audit_log_field():
    """If somehow the audit_log got corrupted to a dict / string, we
    re-init instead of raising."""
    d = {"audit_log": "garbage"}
    record(d, A_RESET)
    assert isinstance(d["audit_log"], list)


def test_record_preserves_existing_entries():
    d = {"audit_log": [{"iso": "old", "action": "older_action",
                          "user": "x", "details": {}}]}
    record(d, A_VMI_TOGGLE, details={"enabled": True})
    assert len(d["audit_log"]) == 2
    assert d["audit_log"][0]["action"] == "older_action"
    assert d["audit_log"][1]["action"] == "vmi_toggle"


def test_record_default_user_is_operator():
    d = {}
    record(d, A_RESET)
    assert d["audit_log"][0]["user"] == "operator"


def test_record_custom_user():
    """Single-user prototype today; the user field exists for the
    technical team's RBAC pass."""
    d = {}
    record(d, A_RESET, user="alice@example.com")
    assert d["audit_log"][0]["user"] == "alice@example.com"


def test_record_truncates_to_max_entries():
    d = {"audit_log": []}
    for i in range(AUDIT_LOG_MAX_ENTRIES + 50):
        record(d, A_ADVANCE, details={"i": i})
    assert len(d["audit_log"]) == AUDIT_LOG_MAX_ENTRIES
    # Oldest dropped, newest kept
    assert d["audit_log"][0]["details"]["i"] == 50
    assert d["audit_log"][-1]["details"]["i"] == AUDIT_LOG_MAX_ENTRIES + 49


def test_record_empty_details_normalize_to_empty_dict():
    d = {}
    record(d, A_RESET)
    assert d["audit_log"][0]["details"] == {}


def test_record_details_are_copied_not_aliased():
    """Mutating the caller's `details` after the fact must not change
    the recorded entry."""
    payload = {"low": 12000, "high": 24000}
    d = {}
    record(d, A_TARGET_APPLY, details=payload)
    payload["low"] = 99999
    assert d["audit_log"][0]["details"]["low"] == 12000


# ── recent() ─────────────────────────────────────────────────────────────────

def test_recent_returns_last_n():
    d = {"audit_log": []}
    for i in range(10):
        record(d, A_ADVANCE, details={"i": i})
    last5 = recent(d, n=5)
    assert len(last5) == 5
    assert [e["details"]["i"] for e in last5] == [5, 6, 7, 8, 9]


def test_recent_returns_empty_when_no_log():
    assert recent({}) == []


def test_recent_handles_corrupted_log():
    assert recent({"audit_log": "garbage"}) == []


def test_recent_handles_missing_log_key():
    assert recent({"other_field": 1}) == []


# ── PlantState round-trip ────────────────────────────────────────────────────

def test_audit_log_round_trips(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    d["audit_log"] = [
        {"iso": "2026-04-15T10:00:00", "action": "advance_clock",
          "user": "operator", "details": {"hours": 8.0}},
    ]
    rt = PlantState.from_dict(d).to_dict()
    assert rt["audit_log"] == d["audit_log"]


def test_audit_log_defaults_empty_via_plant_state(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    d.pop("audit_log", None)
    state = PlantState.from_dict(d)
    assert state.audit_log == []
    rt = state.to_dict()
    assert rt["audit_log"] == []


def test_action_constants_match_string_values():
    """Pin the action key string values — changing them silently would
    invalidate every prior audit_log entry."""
    assert A_VMI_TOGGLE == "vmi_toggle"
    assert A_TARGET_APPLY == "target_override_apply"
    assert A_RESET == "reset"
    assert A_ADVANCE == "advance_clock"
