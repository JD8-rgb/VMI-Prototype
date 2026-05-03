"""Per-customer parser learning loop (Phase 7).

Once a customer accumulates >= LEARNING_TRIGGER_MISS_COUNT MISS
entries with corrections in parser_misses_log.jsonl, the LLM rescue
prompt for that customer gets enriched with up to
LEARNING_FEW_SHOT_K example pairs.
"""

from __future__ import annotations

import json

import pytest

import parser_misses
from parser_misses import append_miss, append_correction, append_validation
from parser_learning import (
    LEARNING_TRIGGER_MISS_COUNT,
    LEARNING_FEW_SHOT_K,
    count_misses,
    should_enrich,
    build_few_shot_examples,
    build_few_shot_prefix,
)


@pytest.fixture
def tmp_log(tmp_path, monkeypatch):
    p = tmp_path / "parser_misses_log.jsonl"
    monkeypatch.setattr(parser_misses, "PARSER_MISSES_LOG_PATH", str(p))
    return p


def _add_misses(n, customer_id=None):
    for i in range(n):
        append_miss(
            email_id=f"miss-{i}",
            sender="anna@example.com",
            subject=f"Schedule {i}",
            body=f"Mon-Fri 6am-{4 + i}pm",
            entries=[(0, 6, 16 + i)],
            confidence="low",
            notes=[f"only one day extracted in miss {i}"],
            customer_id=customer_id,
        )


# ── count_misses + should_enrich ─────────────────────────────────────────────

def test_count_zero_when_log_empty(tmp_log):
    assert count_misses([]) == 0
    assert should_enrich([]) is False


def test_count_includes_only_miss_kind(tmp_log):
    _add_misses(3)
    append_correction(email_id="miss-0",
                       corrected_entries=[(0, 6, 16)])
    append_validation(email_id="some-other")
    log = parser_misses.read_all()
    assert count_misses(log) == 3   # 3 miss + 1 correction + 1 validation


def test_should_enrich_at_threshold(tmp_log):
    _add_misses(LEARNING_TRIGGER_MISS_COUNT)
    log = parser_misses.read_all()
    assert should_enrich(log) is True


def test_should_not_enrich_below_threshold(tmp_log):
    _add_misses(LEARNING_TRIGGER_MISS_COUNT - 1)
    log = parser_misses.read_all()
    assert should_enrich(log) is False


def test_should_enrich_filters_by_customer(tmp_log):
    """A customer with 10 misses triggers; another customer with 0
    does NOT pull from the first customer's bucket."""
    _add_misses(10, customer_id="acme")
    _add_misses(2,  customer_id="other")
    log = parser_misses.read_all()
    assert should_enrich(log, customer_id="acme")  is True
    assert should_enrich(log, customer_id="other") is False


# ── build_few_shot_examples ──────────────────────────────────────────────────

def test_build_examples_empty_when_no_corrections(tmp_log):
    """Misses without corrections produce nothing — we need ground
    truth, not just the parser's bad guess."""
    _add_misses(15)
    log = parser_misses.read_all()
    assert build_few_shot_examples(log) == []


def test_build_examples_joins_miss_to_correction(tmp_log):
    _add_misses(3)
    append_correction(email_id="miss-0",
                       corrected_entries=[(0, 6, 16), (1, 6, 16)])
    append_correction(email_id="miss-1",
                       corrected_entries=[(2, 6, 16)])
    log = parser_misses.read_all()
    examples = build_few_shot_examples(log)
    assert len(examples) == 2
    # Each example has the body, parser output, and operator confirmation
    for ex in examples:
        assert "body" in ex
        assert "parser_entries" in ex
        assert "corrected_entries" in ex


def test_build_examples_caps_at_k(tmp_log):
    _add_misses(20)
    for i in range(20):
        append_correction(email_id=f"miss-{i}",
                           corrected_entries=[(0, 6, 16)])
    log = parser_misses.read_all()
    examples = build_few_shot_examples(log, k=LEARNING_FEW_SHOT_K)
    assert len(examples) == LEARNING_FEW_SHOT_K


def test_build_examples_returns_recent_first(tmp_log):
    _add_misses(5)
    for i in range(5):
        append_correction(email_id=f"miss-{i}",
                           corrected_entries=[(i, 6, 16)])
    log = parser_misses.read_all()
    examples = build_few_shot_examples(log, k=3)
    # Most-recent (corrected_entries with weekday 4) first
    assert examples[0]["corrected_entries"][0][0] == 4


# ── build_few_shot_prefix ────────────────────────────────────────────────────

def test_prefix_none_when_below_threshold(tmp_log):
    _add_misses(3)
    log = parser_misses.read_all()
    assert build_few_shot_prefix(log) is None


def test_prefix_none_when_no_corrections(tmp_log):
    _add_misses(LEARNING_TRIGGER_MISS_COUNT)
    log = parser_misses.read_all()
    # Threshold met but no corrections → no examples → no prefix
    assert build_few_shot_prefix(log) is None


def test_prefix_renders_when_threshold_and_corrections(tmp_log):
    _add_misses(LEARNING_TRIGGER_MISS_COUNT)
    for i in range(LEARNING_TRIGGER_MISS_COUNT):
        append_correction(email_id=f"miss-{i}",
                           corrected_entries=[(i % 5, 6, 16)])
    log = parser_misses.read_all()
    prefix = build_few_shot_prefix(log)
    assert prefix is not None
    assert "Past corrections from this customer:" in prefix
    assert "Example 1" in prefix
    assert "Operator confirmed:" in prefix
    assert "Now parse this email:" in prefix


def test_prefix_truncates_long_bodies(tmp_log):
    """A 5KB body in the log shouldn't blow up the prompt size."""
    append_miss(email_id="big",
                 sender="x", subject="x",
                 body="x" * 5000,
                 entries=[(0, 6, 16)],
                 confidence="low", notes=[])
    for i in range(LEARNING_TRIGGER_MISS_COUNT):
        append_miss(email_id=f"m-{i}", sender="x", subject="x",
                     body="short body", entries=[(0, 6, 16)],
                     confidence="low", notes=[])
    append_correction(email_id="big",
                       corrected_entries=[(0, 6, 16)])
    log = parser_misses.read_all()
    prefix = build_few_shot_prefix(log, k=1)
    assert prefix is not None
    assert "[…truncated]" in prefix


# ── append_correction / append_validation ───────────────────────────────────

def test_append_correction_writes_kind(tmp_log):
    append_correction(email_id="m1", corrected_entries=[(0, 6, 16)])
    log = parser_misses.read_all()
    assert log[-1]["kind"] == "correction"
    assert log[-1]["email_id"] == "m1"


def test_append_validation_writes_kind(tmp_log):
    append_validation(email_id="m2")
    log = parser_misses.read_all()
    assert log[-1]["kind"] == "validation"
    assert log[-1]["email_id"] == "m2"


def test_append_does_not_raise_on_unwritable_path(tmp_path, monkeypatch):
    """Same fail-safe as append_miss: a non-writable log path doesn't
    break the parser pipeline."""
    bad = tmp_path / "no_such_dir" / "log.jsonl"
    monkeypatch.setattr(parser_misses, "PARSER_MISSES_LOG_PATH", str(bad))
    # Should not raise
    append_correction(email_id="m", corrected_entries=[(0, 6, 16)])
    append_validation(email_id="m")
