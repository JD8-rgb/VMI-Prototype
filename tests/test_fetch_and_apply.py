"""Smoke coverage for read_schedule.fetch_and_apply_schedule early-return paths.

The full "happy path" requires a real IMAP fetch + parser flow; that's
covered by manual end-to-end testing during the demo. These tests
mock OutlookClient and load_config to exercise the early-return paths
that protect the rest of the pipeline from missing-config / empty-
inbox / no-relevant-email states.
"""

from __future__ import annotations

import copy
from datetime import datetime
import logging

import pytest

import read_schedule
from read_schedule import fetch_and_apply_schedule


@pytest.fixture(autouse=True)
def silence_logger():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


class _StubOutlookClient:
    """Returns whatever inbox the test sets up via fake_inbox fixture."""
    inbox: list = []

    def __init__(self, config): pass

    def search_inbox(self, sender=None, top=50):
        return list(type(self).inbox)

    def send_mail(self, *args, **kwargs):
        pass


@pytest.fixture
def stub_outlook(monkeypatch):
    _StubOutlookClient.inbox = []
    monkeypatch.setattr(read_schedule, "OutlookClient", _StubOutlookClient)
    return _StubOutlookClient


def test_returns_not_found_when_no_config(monkeypatch, defaults_dict):
    monkeypatch.setattr(read_schedule, "load_config", lambda: {})
    out = fetch_and_apply_schedule(copy.deepcopy(defaults_dict))
    assert out == "not_found"


def test_returns_not_found_when_inbox_empty(monkeypatch, defaults_dict, stub_outlook):
    monkeypatch.setattr(read_schedule, "load_config",
                         lambda: {"anna_email": "anna@example.com",
                                  "distribution_group": "ops@example.com"})
    stub_outlook.inbox = []
    out = fetch_and_apply_schedule(copy.deepcopy(defaults_dict))
    assert out == "not_found"


def test_returns_not_found_when_only_self_generated_in_inbox(
    monkeypatch, defaults_dict, stub_outlook
):
    """VMI-system-generated emails (alerts, reminders, load-entry PDFs)
    must be filtered out so the parser doesn't see its own outbound
    traffic as new schedules."""
    monkeypatch.setattr(read_schedule, "load_config",
                         lambda: {"anna_email": "anna@example.com",
                                  "distribution_group": "ops@example.com",
                                  "email_address": "vmi-bot@example.com"})
    # Only emails authored by the VMI bot itself
    stub_outlook.inbox = [
        {"sender": "vmi-bot@example.com",
          "subject": "VMI Alert (1 new)",
          "body": "RED FLAG: Product U projected to drop",
          "date": "2026-04-15T10:00:00"},
        {"sender": "vmi-bot@example.com",
          "subject": "Schedule reminder",
          "body": "Friday reminder",
          "date": "2026-04-15T11:00:00"},
    ]
    out = fetch_and_apply_schedule(copy.deepcopy(defaults_dict))
    assert out == "not_found"


def test_returns_not_found_when_inbox_has_no_schedule_shaped_emails(
    monkeypatch, defaults_dict, stub_outlook
):
    """Random emails without day or time tokens must be filtered out
    by the schedule-shape detector before the parser runs."""
    monkeypatch.setattr(read_schedule, "load_config",
                         lambda: {"anna_email": "anna@example.com",
                                  "distribution_group": "ops@example.com"})
    stub_outlook.inbox = [
        {"sender": "marketing@example.com",
          "subject": "Newsletter",
          "body": "This is just a regular newsletter, nothing schedule-related.",
          "date": "2026-04-15T10:00:00"},
    ]
    out = fetch_and_apply_schedule(copy.deepcopy(defaults_dict))
    assert out == "not_found"
