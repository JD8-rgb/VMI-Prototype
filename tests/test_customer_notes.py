"""Customer notes scratchpad — round-trip + persistence."""

from __future__ import annotations

import copy

import pytest

from state import PlantState


def test_customer_notes_default_empty(defaults_dict):
    state = PlantState.from_dict(defaults_dict)
    assert state.customer_notes == ""


def test_customer_notes_round_trip(defaults_dict):
    d = copy.deepcopy(defaults_dict)
    d["customer_notes"] = "Anna out 4/22-4/26, expect manual schedules"
    rt = PlantState.from_dict(d).to_dict()
    assert rt["customer_notes"] == d["customer_notes"]


def test_customer_notes_unicode_round_trip(defaults_dict):
    """Notes should preserve Unicode (operator might paste accented
    characters, emoji, etc.)."""
    d = copy.deepcopy(defaults_dict)
    d["customer_notes"] = "Acmé Plástics — switching to ☀️ weekend shifts in May"
    rt = PlantState.from_dict(d).to_dict()
    assert rt["customer_notes"] == d["customer_notes"]


def test_customer_notes_long_text_preserved(defaults_dict):
    """No truncation — operator-authored content is sacred."""
    d = copy.deepcopy(defaults_dict)
    d["customer_notes"] = "x" * 10_000
    rt = PlantState.from_dict(d).to_dict()
    assert len(rt["customer_notes"]) == 10_000


def test_customer_notes_none_coerces_to_empty(defaults_dict):
    """Defensive: if a hand-edited file has notes=null instead of '', the
    loader normalizes."""
    d = copy.deepcopy(defaults_dict)
    d["customer_notes"] = None
    state = PlantState.from_dict(d)
    assert state.customer_notes == ""


def test_customer_notes_int_coerces_to_string(defaults_dict):
    """Defensive: any non-string is stringified rather than crash."""
    d = copy.deepcopy(defaults_dict)
    d["customer_notes"] = 42
    state = PlantState.from_dict(d)
    assert state.customer_notes == "42"
