"""Severity-based escalation routing for alerts (Phase 4).

WARNING (yellow) → smaller list (scheduler + backup)
RED FLAG (red)   → bigger list (scheduler + backup + manager
                     + scheduling team + shipping team + operations)

Backwards compat: a config without escalation fields falls back to
the legacy single distribution_group so unconfigured deployments
behave exactly as before."""

from __future__ import annotations

import copy
import logging

import pytest

import email_hooks
from email_hooks import _escalation_recipients, send_alert_emails_if_new


@pytest.fixture(autouse=True)
def silence_logger():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


# ── _escalation_recipients pure-function ────────────────────────────────────

def test_legacy_config_falls_back_to_distribution_group():
    """A config with only distribution_group (no escalation fields)
    must behave like before: one list for both severities."""
    cfg = {"distribution_group": "ops@example.com"}
    assert _escalation_recipients(cfg, "warning")  == ["ops@example.com"]
    assert _escalation_recipients(cfg, "red_flag") == ["ops@example.com"]


def test_warning_routes_to_scheduler_and_backup():
    cfg = {
        "scheduler_email":          "anna@example.com",
        "scheduler_backup_email":   "ben@example.com",
        "scheduler_manager_email":  "boss@example.com",
        "scheduling_team_distribution": "team@example.com",
        "shipping_team_distribution":   "shipping@example.com",
        "operations_email":             "ops@example.com",
    }
    out = _escalation_recipients(cfg, "warning")
    assert out == ["anna@example.com", "ben@example.com"]


def test_red_flag_routes_to_full_chain():
    cfg = {
        "scheduler_email":          "anna@example.com",
        "scheduler_backup_email":   "ben@example.com",
        "scheduler_manager_email":  "boss@example.com",
        "scheduling_team_distribution": "team@example.com",
        "shipping_team_distribution":   "shipping@example.com",
        "operations_email":             "ops@example.com",
    }
    out = _escalation_recipients(cfg, "red_flag")
    assert out == [
        "anna@example.com", "ben@example.com",
        "boss@example.com", "team@example.com",
        "shipping@example.com", "ops@example.com",
    ]


def test_partial_escalation_drops_missing_addresses():
    """Missing fields are dropped — empty list / partial OK."""
    cfg = {"scheduler_email": "anna@example.com"}   # only scheduler set
    out = _escalation_recipients(cfg, "red_flag")
    assert out == ["anna@example.com"]


def test_dedups_same_address_in_multiple_fields():
    """If the same address appears in multiple fields, return it once."""
    cfg = {
        "scheduler_email":              "anna@example.com",
        "scheduler_backup_email":       "anna@example.com",
        "operations_email":             "anna@example.com",
    }
    out = _escalation_recipients(cfg, "red_flag")
    assert out == ["anna@example.com"]


def test_unknown_severity_treated_as_warning():
    """Defensive: an alert with severity="info" or similar should not
    escalate to the full red-flag chain by accident."""
    cfg = {
        "scheduler_email":         "anna@example.com",
        "scheduler_backup_email":  "ben@example.com",
        "operations_email":        "ops@example.com",
    }
    # Anything that isn't "red_flag" → warning routing
    out = _escalation_recipients(cfg, "info")
    assert "ops@example.com" not in out


def test_empty_config_returns_empty():
    assert _escalation_recipients({}, "red_flag") == []
    assert _escalation_recipients(None, "red_flag") == []


# ── send_alert_emails_if_new with escalation ────────────────────────────────

class _CapturingClient:
    """Capture send_mail calls without hitting SMTP."""
    sent: list = []

    def __init__(self, config):
        pass

    def send_mail(self, recipients, subject, body):
        type(self).sent.append({
            "to": recipients, "subject": subject, "body": body,
        })


@pytest.fixture
def capture_client(monkeypatch):
    _CapturingClient.sent = []
    monkeypatch.setattr(email_hooks, "OutlookClient", _CapturingClient)
    return _CapturingClient


@pytest.fixture
def fake_alerts(monkeypatch):
    state = {"alerts": []}
    def _set(alerts): state["alerts"] = alerts
    monkeypatch.setattr(email_hooks, "get_all_alerts",
                          lambda data, cfg=None: state["alerts"])
    return _set


def _alert_dict(text, severity="red_flag"):
    return {
        "text": text, "type": "safety_stock", "severity": severity,
        "direction": "too_low", "product": "Product U",
        "tank": None, "level_lbs": 0,
    }


def test_warning_only_sends_one_email_to_short_list(
    monkeypatch, capture_client, fake_alerts, defaults_dict,
):
    monkeypatch.setattr(email_hooks, "load_config", lambda: {
        "scheduler_email":         "anna@example.com",
        "scheduler_backup_email":  "ben@example.com",
        "operations_email":        "ops@example.com",
    })
    fake_alerts([_alert_dict("LEAD-TIME WARNING", severity="warning")])
    d = copy.deepcopy(defaults_dict)
    send_alert_emails_if_new(d)
    assert len(capture_client.sent) == 1
    sent = capture_client.sent[0]
    assert "anna@example.com" in sent["to"]
    assert "ben@example.com" in sent["to"]
    assert "ops@example.com" not in sent["to"]
    assert "WARNING" in sent["subject"]


def test_red_flag_sends_one_email_to_full_chain(
    monkeypatch, capture_client, fake_alerts, defaults_dict,
):
    monkeypatch.setattr(email_hooks, "load_config", lambda: {
        "scheduler_email":         "anna@example.com",
        "scheduler_backup_email":  "ben@example.com",
        "scheduler_manager_email": "boss@example.com",
        "scheduling_team_distribution": "team@example.com",
        "shipping_team_distribution":   "shipping@example.com",
        "operations_email":             "ops@example.com",
    })
    fake_alerts([_alert_dict("RED FLAG: drained", severity="red_flag")])
    d = copy.deepcopy(defaults_dict)
    send_alert_emails_if_new(d)
    assert len(capture_client.sent) == 1
    sent = capture_client.sent[0]
    for addr in ("anna@example.com", "ben@example.com", "boss@example.com",
                  "team@example.com", "shipping@example.com",
                  "ops@example.com"):
        assert addr in sent["to"]
    assert "RED FLAG" in sent["subject"]


def test_mixed_severities_send_two_emails(
    monkeypatch, capture_client, fake_alerts, defaults_dict,
):
    """One batch with a warning AND a red flag → two emails, each to
    the right list."""
    monkeypatch.setattr(email_hooks, "load_config", lambda: {
        "scheduler_email":         "anna@example.com",
        "scheduler_backup_email":  "ben@example.com",
        "operations_email":        "ops@example.com",
    })
    fake_alerts([
        _alert_dict("WARNING: lead time", severity="warning"),
        _alert_dict("RED FLAG: drained",  severity="red_flag"),
    ])
    d = copy.deepcopy(defaults_dict)
    send_alert_emails_if_new(d)
    assert len(capture_client.sent) == 2
    severities = sorted(s["subject"].split()[-1] for s in capture_client.sent)
    assert "WARNING" in severities
    assert "FLAG" in severities


def test_legacy_config_still_works(
    monkeypatch, capture_client, fake_alerts, defaults_dict,
):
    """A config with only distribution_group (no escalation fields)
    sends a single email to that list, exactly like before."""
    monkeypatch.setattr(email_hooks, "load_config", lambda: {
        "distribution_group": "ops@example.com",
    })
    fake_alerts([
        _alert_dict("WARNING: lead time", severity="warning"),
        _alert_dict("RED FLAG: drained",  severity="red_flag"),
    ])
    d = copy.deepcopy(defaults_dict)
    send_alert_emails_if_new(d)
    # Two emails total (one per severity), both to the legacy list
    assert len(capture_client.sent) == 2
    for s in capture_client.sent:
        assert "ops@example.com" in s["to"]
