"""parser_misses.py — production-misses log helper.

Whenever fetch_and_apply_schedule lands a LOW-confidence parse, we
append the input + best-guess output to parser_misses_log.jsonl.

The point: build a corpus of real production phrasings the parser
struggles with, so engineering can periodically promote the genuinely-
parseable ones to must_pass test cases (closing the regression-test
feedback loop) and document the rest as known limits.

Format (one entry per line, JSON):
    {
        "logged_at":  ISO datetime string of when the miss was logged,
        "customer_id": optional customer id (None for default tenant),
        "email_id":   inbound email id (so we can dedup later),
        "sender":     email sender,
        "subject":    email subject,
        "body":       email body (truncated to 5KB),
        "entries":    parser's best-guess [(weekday, start, end), ...],
        "confidence": "low",
        "notes":      list of parser-note strings
    }

The log file is gitignored (data.json policy applies — operational
state, not code). Triage via `python triage_parser_misses.py`.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Default location: project root. Customizable per deployment.
PARSER_MISSES_LOG_PATH = os.environ.get(
    "PARSER_MISSES_LOG_PATH",
    "parser_misses_log.jsonl",
)


def append_miss(
    *,
    email_id: Optional[str],
    sender: str,
    subject: str,
    body: str,
    entries: List,
    confidence: str,
    notes: List[str],
    customer_id: Optional[str] = None,
    log_path: Optional[str] = None,
) -> None:
    """Append a parser-miss entry as one JSON line.

    Failure to write is logged as a warning but never raises — the
    parser pipeline must keep running even if the audit log is
    misconfigured (e.g. read-only filesystem).
    """
    path = log_path or PARSER_MISSES_LOG_PATH
    entry: Dict[str, Any] = {
        "kind":        "miss",
        "logged_at":   datetime.now().isoformat(),
        "customer_id": customer_id,
        "email_id":    email_id,
        "sender":      sender,
        "subject":     subject,
        "body":        (body or "")[:5000],
        "entries":     [list(e) for e in (entries or [])],
        "confidence":  confidence,
        "notes":       list(notes or []),
    }
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.warning("parser_misses: append failed (%s) — %s", path, e)


def read_all(log_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """Read every entry from the log. Returns empty list if missing.

    Each entry is parsed independently — a corrupted line is skipped
    with a warning rather than raising, so the triage CLI can chew
    through a partially-broken file."""
    path = log_path or PARSER_MISSES_LOG_PATH
    if not os.path.exists(path):
        return []
    out: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning(
                        "parser_misses: skipping malformed line %d in %s: %s",
                        lineno, path, e,
                    )
    except OSError as e:
        logger.warning("parser_misses: read failed (%s) — %s", path, e)
    return out


def clear(log_path: Optional[str] = None) -> None:
    """Clear MISS entries from the log, PRESERVING CORRECTION and
    VALIDATION records. Used by `--clear` in the triage CLI when an
    engineer has finished promoting / discarding every entry.

    Why selective: the parser_misses_log.jsonl stream carries three
    `kind` values (see parser_learning.py for the full taxonomy):
      - MISS         — low-confidence parse the operator cleaned up
      - CORRECTION   — operator's authoritative final entries
      - VALIDATION   — operator acknowledged a HIGH parse unchanged

    The triage CLI's job is to walk MISS entries one at a time and
    either PROMOTE (add must-pass case) or DISCARD them. After triage,
    the MISS backlog should be empty — but the CORRECTION + VALIDATION
    records are training signal for the per-customer learning loop and
    must survive.

    Previously this function deleted the whole log, wiping all three
    kinds. That destroyed the customer-specific few-shot examples the
    rescue prompt enrichment depends on.
    """
    path = log_path or PARSER_MISSES_LOG_PATH
    if not os.path.exists(path):
        return
    try:
        # Read everything, drop MISS, rewrite. Records without an
        # explicit `kind` field were written by older code (pre-fix);
        # treat them as MISS — that was the only kind written without
        # an explicit field.
        records = read_all(path)
        preserved = [r for r in records
                     if r.get("kind") not in (None, "miss")]
        if not preserved:
            # Nothing to keep — remove the file entirely.
            os.remove(path)
            return
        # Write the preserved records back. Append-mode is wrong here;
        # we need to TRUNCATE then rewrite.
        with open(path, "w", encoding="utf-8") as f:
            for r in preserved:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.warning("parser_misses: clear failed (%s) — %s", path, e)


def append_correction(
    *,
    email_id: Optional[str],
    corrected_entries: list,
    customer_id: Optional[str] = None,
    log_path: Optional[str] = None,
) -> None:
    """Append a CORRECTION entry: the operator's authoritative final
    entries for an earlier MISS. The learning loop joins miss to
    correction by email_id so the LLM rescue prompt gets ground-truth
    pairs for that customer.

    {
        "kind":              "correction",
        "logged_at":         ISO timestamp,
        "customer_id":       optional,
        "email_id":          links to the prior MISS entry,
        "corrected_entries": [(weekday, start, end), ...]
    }
    """
    path = log_path or PARSER_MISSES_LOG_PATH
    entry = {
        "kind":              "correction",
        "logged_at":         datetime.now().isoformat(),
        "customer_id":       customer_id,
        "email_id":          email_id,
        "corrected_entries": [list(e) for e in (corrected_entries or [])],
    }
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.warning("parser_misses: correction append failed (%s) — %s",
                         path, e)


def append_validation(
    *,
    email_id: Optional[str],
    customer_id: Optional[str] = None,
    log_path: Optional[str] = None,
) -> None:
    """Append a VALIDATION entry: the operator acknowledged a
    HIGH-confidence parse without edits — strong positive signal
    that the parser handled this customer's phrasing correctly.
    Useful for the learning loop as positive few-shot examples
    ("here's what RIGHT looks like for this customer").

    {
        "kind":         "validation",
        "logged_at":    ISO timestamp,
        "customer_id":  optional,
        "email_id":     refers to the email the parser handled
    }
    """
    path = log_path or PARSER_MISSES_LOG_PATH
    entry = {
        "kind":        "validation",
        "logged_at":   datetime.now().isoformat(),
        "customer_id": customer_id,
        "email_id":    email_id,
    }
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.warning("parser_misses: validation append failed (%s) — %s",
                         path, e)
