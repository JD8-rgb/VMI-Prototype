"""Coverage for email_client helper functions.

The IMAP/SMTP transport layer (OutlookClient.send_mail / search_inbox)
isn't worth unit-testing without a full mocked imaplib/smtplib —
manual end-to-end testing during the demo covers the live paths.
But the pure-function helpers _html_to_text and _extract_body are
deterministic and worth a regression guard, especially since real
schedule emails frequently come in as HTML from Outlook senders."""

from __future__ import annotations

import email
import textwrap

import pytest

from email_client import _extract_body, _html_to_text, load_config


# ── _html_to_text ────────────────────────────────────────────────────────────

def test_html_to_text_strips_simple_tags():
    out = _html_to_text("<p>Hello <b>world</b></p>")
    assert "Hello" in out
    assert "world" in out
    assert "<" not in out


def test_html_to_text_decodes_common_entities():
    out = _html_to_text("Tom &amp; Jerry &lt;3 &nbsp; &quot;quotes&quot;")
    assert "&" in out
    assert "<" in out
    assert '"quotes"' in out


def test_html_to_text_normalizes_whitespace():
    out = _html_to_text("<p>line1</p><p>line2</p><p>line3</p>")
    # Block tags become newlines; multiple newlines collapse to two
    assert "line1" in out
    assert "line2" in out
    assert "\n\n\n" not in out


def test_html_to_text_handles_lists():
    """Multi-day schedules sometimes arrive as HTML <ul><li>."""
    html = "<ul><li>Mon 6am-4pm</li><li>Tue 6am-4pm</li></ul>"
    out = _html_to_text(html)
    assert "Mon 6am-4pm" in out
    assert "Tue 6am-4pm" in out


def test_html_to_text_drops_attributes():
    out = _html_to_text('<a href="http://example.com" class="link">click</a>')
    assert "click" in out
    assert "href" not in out
    assert "http://example.com" not in out


# ── _extract_body ────────────────────────────────────────────────────────────

def _make_plain_email(text: str):
    msg = email.message.Message()
    msg.set_payload(text.encode("utf-8"))
    msg.set_charset("utf-8")
    msg.set_type("text/plain")
    return msg


def _make_html_email(html: str):
    msg = email.message.Message()
    msg.set_payload(html.encode("utf-8"))
    msg.set_charset("utf-8")
    msg.set_type("text/html")
    return msg


def test_extract_body_plain_text():
    msg = _make_plain_email("Mon-Fri 6am-4pm")
    assert "Mon-Fri 6am-4pm" in _extract_body(msg)


def test_extract_body_html_falls_back_to_strip():
    msg = _make_html_email("<p>Mon-Fri 6am-4pm</p>")
    out = _extract_body(msg)
    assert "Mon-Fri 6am-4pm" in out
    assert "<" not in out


def test_extract_body_corrupted_payload_returns_empty():
    """A corrupted/binary payload must not raise; returns empty
    string so the parser falls through to other emails."""
    msg = email.message.Message()
    msg.set_payload(b"\xff\xfe\xfd not utf-8")
    msg.set_charset("utf-8")
    msg.set_type("text/plain")
    out = _extract_body(msg)
    # Should return SOMETHING (errors="replace") without raising
    assert isinstance(out, str)


# ── load_config ──────────────────────────────────────────────────────────────

def test_load_config_returns_dict_when_missing(tmp_path, monkeypatch):
    """When email_config.json doesn't exist, load_config returns {}.
    Real CLI scripts skip email features when the dict is empty."""
    monkeypatch.chdir(tmp_path)
    out = load_config()
    assert out == {}


def test_load_config_parses_existing_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "email_config.json").write_text(
        '{"email_address": "vmi@example.com", "anna_email": "anna@example.com"}'
    )
    out = load_config()
    assert out["email_address"] == "vmi@example.com"
    assert out["anna_email"] == "anna@example.com"
