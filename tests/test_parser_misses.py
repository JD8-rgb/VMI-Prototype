"""parser_misses log + triage CLI contract tests.

The log file is the engineering feedback channel — every low-confidence
parse in production becomes one line, and engineering periodically
walks the file to promote real misses to must_pass test cases.

These tests cover:
  - append_miss writes JSON line in the right shape
  - read_all returns the entries
  - read_all skips malformed lines (single bad line shouldn't poison
    the whole file)
  - clear wipes the log
  - missing log file is treated as empty (no exception)
  - triage CLI argparse surface
  - triage CLI --count, --list, --clear modes
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import parser_misses
from parser_misses import append_miss, read_all, clear


@pytest.fixture
def tmp_log(tmp_path, monkeypatch):
    """Redirect the log path to a tmp file for the test."""
    p = tmp_path / "parser_misses_log.jsonl"
    monkeypatch.setattr(parser_misses, "PARSER_MISSES_LOG_PATH", str(p))
    return p


# ── append_miss ──────────────────────────────────────────────────────────────

def test_append_writes_json_line(tmp_log):
    append_miss(
        email_id="m1", sender="anna@example.com",
        subject="Schedule", body="Mon-Fri 6am-4pm",
        entries=[(0, 6, 16)], confidence="low",
        notes=["only 1 day"],
    )
    assert tmp_log.exists()
    lines = tmp_log.read_text().splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["email_id"] == "m1"
    assert entry["sender"] == "anna@example.com"
    assert entry["entries"] == [[0, 6, 16]]
    assert entry["confidence"] == "low"


def test_append_multiple_entries(tmp_log):
    for i in range(3):
        append_miss(
            email_id=f"m{i}", sender="x", subject="x", body="x",
            entries=[], confidence="low", notes=[],
        )
    lines = tmp_log.read_text().splitlines()
    assert len(lines) == 3


def test_append_truncates_body(tmp_log):
    """5KB cap so the log doesn't blow up on giant emails."""
    huge = "x" * 100_000
    append_miss(email_id="m", sender="s", subject="s", body=huge,
                 entries=[], confidence="low", notes=[])
    entry = json.loads(tmp_log.read_text().strip())
    assert len(entry["body"]) == 5000


def test_append_does_not_raise_on_unwritable_path(tmp_path, monkeypatch, caplog):
    """A non-writable log path must not break the parser pipeline.
    Writes a warning, returns silently."""
    bad = tmp_path / "no_such_dir" / "log.jsonl"
    monkeypatch.setattr(parser_misses, "PARSER_MISSES_LOG_PATH", str(bad))
    # Should not raise
    append_miss(email_id="m", sender="s", subject="s", body="b",
                 entries=[], confidence="low", notes=[])


# ── read_all ─────────────────────────────────────────────────────────────────

def test_read_all_returns_empty_when_missing(tmp_log):
    """No log file → empty list (no exception)."""
    assert read_all() == []


def test_read_all_round_trips_appended_entries(tmp_log):
    for i in range(3):
        append_miss(email_id=f"m{i}", sender="s", subject="s", body="b",
                     entries=[], confidence="low", notes=[])
    out = read_all()
    assert len(out) == 3
    assert [e["email_id"] for e in out] == ["m0", "m1", "m2"]


def test_read_all_skips_malformed_lines(tmp_log):
    """A single corrupted line shouldn't poison the whole read."""
    tmp_log.write_text(
        '{"email_id": "good_1", "sender": "a"}\n'
        'NOT_JSON_GARBAGE\n'
        '{"email_id": "good_2", "sender": "b"}\n'
    )
    out = read_all()
    assert len(out) == 2
    assert {e["email_id"] for e in out} == {"good_1", "good_2"}


def test_read_all_skips_blank_lines(tmp_log):
    tmp_log.write_text(
        '{"email_id": "g1"}\n\n\n{"email_id": "g2"}\n'
    )
    out = read_all()
    assert len(out) == 2


# ── clear ────────────────────────────────────────────────────────────────────

def test_clear_wipes_log(tmp_log):
    """With only MISS entries, clear() removes the file entirely."""
    append_miss(email_id="m", sender="s", subject="s", body="b",
                 entries=[], confidence="low", notes=[])
    assert tmp_log.exists()
    clear()
    assert not tmp_log.exists()


def test_clear_preserves_correction_and_validation(tmp_log):
    """clear() drops MISS entries only — CORRECTION and VALIDATION
    records are training signal for parser_learning.py and must
    survive. Pre-fix, clear() deleted the whole file (destroying the
    per-customer few-shot examples the LLM rescue prompt enrichment
    depends on)."""
    from parser_misses import append_correction, append_validation
    # Two misses, one correction (joined to m1 by email_id), one validation
    append_miss(email_id="m1", sender="s", subject="s", body="vague",
                 entries=[], confidence="low", notes=[])
    append_miss(email_id="m2", sender="s", subject="s", body="also vague",
                 entries=[], confidence="low", notes=[])
    append_correction(email_id="m1", corrected_entries=[(0, 6, 16)])
    append_validation(email_id="v1")
    assert len(read_all()) == 4
    clear()
    after = read_all()
    kinds = sorted(r.get("kind") for r in after)
    assert kinds == ["correction", "validation"], (
        f"expected correction + validation to survive; got kinds={kinds}"
    )


def test_clear_silent_when_log_missing(tmp_log):
    """No-op when there's nothing to clear."""
    clear()   # must not raise


# ── triage CLI ───────────────────────────────────────────────────────────────

PROJ_ROOT = Path(__file__).resolve().parent.parent


def _run_cli(*args, log_file: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(PROJ_ROOT / "triage_parser_misses.py"),
          "--path", str(log_file), *args],
        capture_output=True, text=True, timeout=10,
    )


def test_cli_count_returns_zero_when_empty(tmp_path):
    log_file = tmp_path / "log.jsonl"
    result = _run_cli("--count", log_file=log_file)
    assert result.returncode == 0
    assert result.stdout.strip() == "0"


def test_cli_count_returns_n_after_writes(tmp_path):
    log_file = tmp_path / "log.jsonl"
    log_file.write_text(
        '{"email_id":"a","sender":"a","subject":"a","body":"x","entries":[],'
        '"confidence":"low","notes":[]}\n'
        '{"email_id":"b","sender":"b","subject":"b","body":"x","entries":[],'
        '"confidence":"low","notes":[]}\n'
    )
    result = _run_cli("--count", log_file=log_file)
    assert result.stdout.strip() == "2"


def test_cli_list_renders_entries(tmp_path):
    log_file = tmp_path / "log.jsonl"
    log_file.write_text(
        '{"email_id":"good_email","sender":"anna","subject":"sched",'
        '"body":"Mon-Fri 6am-4pm","entries":[[0,6,16]],"confidence":"low",'
        '"notes":["test note"]}\n'
    )
    result = _run_cli("--list", log_file=log_file)
    assert result.returncode == 0
    assert "anna" in result.stdout
    assert "good_email" in result.stdout
    assert "Mon-Fri 6am-4pm" in result.stdout
    assert "test note" in result.stdout


def test_cli_help_exits_cleanly(tmp_path):
    log_file = tmp_path / "log.jsonl"
    result = _run_cli("--help", log_file=log_file)
    assert result.returncode == 0
    assert "triage" in result.stdout.lower()
