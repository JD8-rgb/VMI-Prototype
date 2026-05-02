"""Friday reminder per-week dedup contract.

The Streamlit hourly trigger loop (app.py:236-260) advances the sim
clock and calls send_friday_reminder_if_needed once per advanced
hour. Without per-week dedup the operator gets a duplicate email at
both Fri 11 AM and Fri 3 PM, which we explicitly fixed."""

from __future__ import annotations

import copy
from datetime import datetime
import logging

import pytest

import email_hooks


@pytest.fixture(autouse=True)
def silence_logger():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


class _FakeOutlook:
    sent: list = []

    def __init__(self, config):
        pass

    def send_mail(self, recipients, subject, body):
        type(self).sent.append({"to": recipients, "subject": subject})


@pytest.fixture
def fake_outlook(monkeypatch):
    _FakeOutlook.sent = []
    monkeypatch.setattr(email_hooks, "OutlookClient", _FakeOutlook)
    monkeypatch.setattr(email_hooks, "load_config",
                         lambda: {"anna_email": "anna@example.com"})
    return _FakeOutlook


def test_first_friday_call_sends(fake_outlook, defaults_dict):
    d = copy.deepcopy(defaults_dict)
    fri_11am = datetime(2026, 4, 17, 11, 0)   # Friday
    email_hooks.send_friday_reminder_if_needed(d, now_dt=fri_11am)
    assert len(fake_outlook.sent) == 1


def test_second_friday_call_same_week_does_not_send(fake_outlook, defaults_dict):
    """Reproduction of the original bug: advancing from Fri 11 AM to
    Fri 3 PM in Streamlit calls this twice. The second call must
    NOT send a duplicate."""
    d = copy.deepcopy(defaults_dict)
    fri_11am = datetime(2026, 4, 17, 11, 0)
    fri_3pm  = datetime(2026, 4, 17, 15, 0)

    email_hooks.send_friday_reminder_if_needed(d, now_dt=fri_11am)
    email_hooks.send_friday_reminder_if_needed(d, now_dt=fri_3pm)

    assert len(fake_outlook.sent) == 1, (
        "duplicate reminder email sent within same week")


def test_next_week_reminder_fires_again(fake_outlook, defaults_dict):
    """When a NEW week's reminder is genuinely due, the dedup must
    NOT block it — the field is week-scoped."""
    d = copy.deepcopy(defaults_dict)
    fri_w1 = datetime(2026, 4, 17, 11, 0)
    fri_w2 = datetime(2026, 4, 24, 11, 0)

    email_hooks.send_friday_reminder_if_needed(d, now_dt=fri_w1)
    assert len(fake_outlook.sent) == 1

    email_hooks.send_friday_reminder_if_needed(d, now_dt=fri_w2)
    assert len(fake_outlook.sent) == 2, (
        "next week's reminder must fire — dedup is week-scoped")


def test_send_failure_does_not_record_dedup(monkeypatch, defaults_dict):
    """If SMTP fails, the dedup field must NOT be stamped — next call
    should retry."""
    monkeypatch.setattr(email_hooks, "load_config",
                         lambda: {"anna_email": "anna@example.com"})

    class _Broken:
        def __init__(self, config): pass
        def send_mail(self, *a, **k): raise RuntimeError("smtp fail")

    monkeypatch.setattr(email_hooks, "OutlookClient", _Broken)
    d = copy.deepcopy(defaults_dict)
    fri = datetime(2026, 4, 17, 11, 0)
    email_hooks.send_friday_reminder_if_needed(d, now_dt=fri)
    assert "last_reminder_sent_for_week" not in d


def test_received_takes_precedence_over_dedup(fake_outlook, defaults_dict):
    """If the schedule already arrived, the older 'already received'
    short-circuit fires before the dedup, so no email goes out."""
    d = copy.deepcopy(defaults_dict)
    fri = datetime(2026, 4, 17, 11, 0)
    # Mark next week's schedule as received
    d["schedule_received_for_week"] = "2026-04-20"
    email_hooks.send_friday_reminder_if_needed(d, now_dt=fri)
    assert fake_outlook.sent == []


def test_dedup_field_round_trips_through_plant_state(defaults_dict):
    """last_reminder_sent_for_week is an unknown field to PlantState;
    it must round-trip via _extra so the dedup persists across saves."""
    from state import PlantState
    d = copy.deepcopy(defaults_dict)
    d["last_reminder_sent_for_week"] = "2026-04-20"
    rt = PlantState.from_dict(d).to_dict()
    assert rt.get("last_reminder_sent_for_week") == "2026-04-20"
