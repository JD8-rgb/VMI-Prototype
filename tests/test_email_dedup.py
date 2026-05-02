"""Alert dedup / log-append contract for email_hooks.send_alert_emails_if_new.

The dedup logic is the gate that keeps the same alert from emailing
every CLI tick. If it drifts, the technical team will get woken up by
50,000 duplicate Slack pages overnight. These tests fail loud if any
of these properties regress.

Network is fully mocked — the OutlookClient.send_mail call site is
captured but never reaches an SMTP server.
"""

from __future__ import annotations

import copy

import pytest

import email_hooks
from email_hooks import alert_hash, send_alert_emails_if_new


# ── alert_hash ───────────────────────────────────────────────────────────────

def test_alert_hash_is_deterministic():
    a = alert_hash("RED FLAG: Product U projected to drop")
    b = alert_hash("RED FLAG: Product U projected to drop")
    assert a == b


def test_alert_hash_strips_surrounding_whitespace():
    """Trailing whitespace shouldn't break dedup — same alert text
    with a stray newline must hash identically."""
    assert alert_hash("alert") == alert_hash("alert\n")
    assert alert_hash("alert") == alert_hash("  alert  ")


def test_alert_hash_distinguishes_different_alerts():
    assert alert_hash("alert 1") != alert_hash("alert 2")


# ── Dedup behavior ───────────────────────────────────────────────────────────

class _FakeOutlookClient:
    """Captures send_mail calls without hitting the network."""
    sent: list = []

    def __init__(self, config):
        self.config = config

    def send_mail(self, recipients, subject, body):
        type(self).sent.append({"to": recipients, "subject": subject, "body": body})


@pytest.fixture
def fake_outlook(monkeypatch):
    _FakeOutlookClient.sent = []
    monkeypatch.setattr(email_hooks, "OutlookClient", _FakeOutlookClient)
    monkeypatch.setattr(email_hooks, "load_config",
                         lambda: {"distribution_group": "ops@example.com"})
    return _FakeOutlookClient


@pytest.fixture
def fake_alerts(monkeypatch):
    """Replace get_all_alerts so we control what alerts the dedup
    logic sees on each tick."""
    state = {"alerts": []}

    def _set(alerts):
        state["alerts"] = alerts

    def _get(data, cfg=None):
        return state["alerts"]

    monkeypatch.setattr(email_hooks, "get_all_alerts", _get)
    return _set


def _alert_dict(text, **overrides):
    base = {
        "text": text, "type": "safety_stock", "severity": "red_flag",
        "direction": "too_low", "product": "Product U",
        "tank": None, "level_lbs": 0,
    }
    base.update(overrides)
    return base


def test_first_alert_sends_and_records_hash(defaults_dict, fake_outlook, fake_alerts):
    fake_alerts([_alert_dict("RED FLAG: First alert")])
    d = copy.deepcopy(defaults_dict)
    out = send_alert_emails_if_new(d)
    assert len(fake_outlook.sent) == 1
    assert "First alert" in fake_outlook.sent[0]["body"]
    # Hash recorded
    assert alert_hash("RED FLAG: First alert") in out["alerted_hashes"]


def test_repeat_alert_does_not_send_twice(defaults_dict, fake_outlook, fake_alerts):
    fake_alerts([_alert_dict("RED FLAG: Repeat alert")])
    d = copy.deepcopy(defaults_dict)
    d = send_alert_emails_if_new(d)
    assert len(fake_outlook.sent) == 1
    # Tick 2 — same alerts, same data
    d = send_alert_emails_if_new(d)
    assert len(fake_outlook.sent) == 1, "duplicate email sent for same alert"


def test_cleared_alert_pruned_from_hashes(defaults_dict, fake_outlook, fake_alerts):
    """Alert fires, gets emailed, hash recorded. Next tick alert no
    longer firing — its hash must be pruned so when it re-fires later
    we treat it as fresh."""
    fake_alerts([_alert_dict("RED FLAG: Transient")])
    d = copy.deepcopy(defaults_dict)
    d = send_alert_emails_if_new(d)
    h = alert_hash("RED FLAG: Transient")
    assert h in d["alerted_hashes"]

    # Alert clears
    fake_alerts([])
    d = send_alert_emails_if_new(d)
    assert h not in d["alerted_hashes"]

    # Same alert re-fires later — must be treated as new
    fake_alerts([_alert_dict("RED FLAG: Transient")])
    d = send_alert_emails_if_new(d)
    assert len(fake_outlook.sent) == 2, "re-fire after clear should send"


def test_new_alerts_appended_to_log(defaults_dict, fake_outlook, fake_alerts):
    fake_alerts([_alert_dict("WARN: First", severity="warning")])
    d = copy.deepcopy(defaults_dict)
    d = send_alert_emails_if_new(d)
    assert any(entry.get("text", "").endswith("First")
               for entry in d.get("alert_log", []))


def test_alert_log_persists_across_ticks(defaults_dict, fake_outlook, fake_alerts):
    """Earlier alerts remain in the log even after they clear."""
    d = copy.deepcopy(defaults_dict)
    fake_alerts([_alert_dict("WARN: Early")])
    d = send_alert_emails_if_new(d)
    fake_alerts([])
    d = send_alert_emails_if_new(d)
    fake_alerts([_alert_dict("WARN: Late")])
    d = send_alert_emails_if_new(d)
    log_texts = [e["text"] for e in d["alert_log"]]
    assert any("Early" in t for t in log_texts)
    assert any("Late" in t for t in log_texts)


def test_send_failure_does_not_record_hash(defaults_dict, monkeypatch, fake_alerts):
    """If the SMTP send raises, the hash must NOT be recorded — the
    next tick should retry the email."""
    fake_alerts([_alert_dict("RED FLAG: Will fail to send")])

    class _BrokenClient:
        def __init__(self, config): pass
        def send_mail(self, *args, **kwargs):
            raise RuntimeError("simulated SMTP failure")

    monkeypatch.setattr(email_hooks, "OutlookClient", _BrokenClient)
    monkeypatch.setattr(email_hooks, "load_config",
                         lambda: {"distribution_group": "ops@example.com"})

    d = copy.deepcopy(defaults_dict)
    out = send_alert_emails_if_new(d)
    h = alert_hash("RED FLAG: Will fail to send")
    assert h not in out["alerted_hashes"], (
        "send failure must not record the hash; otherwise next tick "
        "won't retry the email"
    )


def test_no_recipient_logs_but_skips_send(defaults_dict, monkeypatch, fake_alerts):
    """If config has no distribution_group, alert is logged but no
    email attempt is made."""
    sent_calls = []

    class _Client:
        def __init__(self, config): pass
        def send_mail(self, *args, **kwargs):
            sent_calls.append(args)

    monkeypatch.setattr(email_hooks, "OutlookClient", _Client)
    monkeypatch.setattr(email_hooks, "load_config",
                         lambda: {"distribution_group": ""})  # empty

    fake_alerts([_alert_dict("RED FLAG: No recipient")])
    d = copy.deepcopy(defaults_dict)
    d = send_alert_emails_if_new(d)
    assert sent_calls == []
    # But the alert still made it to the log
    assert any("No recipient" in e["text"] for e in d["alert_log"])
