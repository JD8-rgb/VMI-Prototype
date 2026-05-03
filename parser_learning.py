"""
parser_learning.py — per-customer LLM rescue prompt enrichment.

When a customer accumulates >= LEARNING_TRIGGER_MISS_COUNT MISS entries
in parser_misses_log.jsonl, the LLM rescue prompt for that customer
gets enriched with up to LEARNING_FEW_SHOT_K example pairs
(input → corrected entries). The model gets nudged toward that
customer's specific phrasings, especially the ones the regex parser
struggled with.

Two flavors of training signal in the log:
  MISS         (kind="miss", default — parser_misses.append_miss)
                   Low-confidence parse the operator had to clean up.
  CORRECTION   (kind="correction", appended on operator confirm)
                   The operator's authoritative final entries for a
                   prior miss. Joined to MISS by email_id.
  VALIDATION   (kind="validation", appended on HIGH-confidence ack)
                   Operator acknowledged a HIGH parse without edits —
                   positive example.

The learning loop builds few-shot examples preferring MISS+CORRECTION
pairs (where we know what RIGHT looks like for that customer) and
falling back to VALIDATIONs (positive baseline) if not enough pairs
exist yet.

Trigger threshold (10) is intentionally low so demo customers can
hit it within a few weeks of operator triage.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from parser_misses import read_all


# ── Configuration ────────────────────────────────────────────────────────────

LEARNING_TRIGGER_MISS_COUNT = 10
"""Minimum MISS entries before we enrich the rescue prompt for this
customer. Below this, return None (use the static default prompt)."""

LEARNING_FEW_SHOT_K = 5
"""Maximum few-shot examples to inject into the prompt. More examples
inflate the token cost without dramatically improving accuracy past
~5; tune if needed for a specific customer."""


# ── Helpers ──────────────────────────────────────────────────────────────────

def _filter_by_customer(entries: List[Dict[str, Any]],
                          customer_id: Optional[str]) -> List[Dict[str, Any]]:
    """Return only the entries matching customer_id. None matches None
    (default tenant)."""
    return [e for e in entries
             if e.get("customer_id") == customer_id]


def count_misses(log: List[Dict[str, Any]],
                   customer_id: Optional[str] = None) -> int:
    """Number of MISS entries for the given customer.
    Used to gate enrichment — see LEARNING_TRIGGER_MISS_COUNT."""
    relevant = _filter_by_customer(log, customer_id)
    return sum(1 for e in relevant if e.get("kind", "miss") == "miss")


def should_enrich(log: List[Dict[str, Any]],
                    customer_id: Optional[str] = None,
                    threshold: int = LEARNING_TRIGGER_MISS_COUNT) -> bool:
    """True when we have enough miss data for this customer to bother
    building a few-shot prefix."""
    return count_misses(log, customer_id) >= threshold


def _join_miss_to_correction(misses: List[Dict[str, Any]],
                                corrections: List[Dict[str, Any]]
                                ) -> List[Dict[str, Any]]:
    """For each miss with a matching correction (joined by email_id),
    produce {body, parser_entries, corrected_entries}. Misses without
    a correction are dropped."""
    by_email = {c.get("email_id"): c for c in corrections
                 if c.get("email_id") is not None}
    pairs = []
    for m in misses:
        email_id = m.get("email_id")
        corr = by_email.get(email_id) if email_id else None
        if corr is None:
            continue
        pairs.append({
            "body":              m.get("body", ""),
            "parser_entries":    m.get("entries", []),
            "corrected_entries": corr.get("corrected_entries", []),
        })
    return pairs


def build_few_shot_examples(
    log: Optional[List[Dict[str, Any]]] = None,
    customer_id: Optional[str] = None,
    k: int = LEARNING_FEW_SHOT_K,
) -> List[Dict[str, Any]]:
    """Return up to k few-shot examples for the LLM rescue prompt.

    Each example dict has shape:
        {
            "body":              email body the operator received,
            "parser_entries":    what the regex parser produced (low-conf),
            "corrected_entries": what the operator confirmed as correct
        }

    Most-recent examples are returned first (FIFO of the last k pairs).
    Returns [] when not enough miss+correction pairs exist."""
    if log is None:
        log = read_all()
    relevant = _filter_by_customer(log, customer_id)
    misses      = [e for e in relevant if e.get("kind", "miss") == "miss"]
    corrections = [e for e in relevant if e.get("kind") == "correction"]
    pairs = _join_miss_to_correction(misses, corrections)
    # Most-recent first; truncate to k
    return list(reversed(pairs))[:k]


def build_few_shot_prefix(
    log: Optional[List[Dict[str, Any]]] = None,
    customer_id: Optional[str] = None,
    k: int = LEARNING_FEW_SHOT_K,
) -> Optional[str]:
    """Render the few-shot examples as a prompt prefix string suitable
    to prepend to the LLM rescue user message.

    Returns None when:
      - count_misses < LEARNING_TRIGGER_MISS_COUNT (don't enrich yet)
      - no miss+correction pairs exist (corrections never landed)

    When enrichment is active, the prefix looks like:

        Past corrections from this customer:

        Example 1 — Body:
        \"\"\"<body>\"\"\"
        Parser produced: [(0, 6, 16), ...]
        Operator confirmed: [(0, 6, 16), ...]

        Example 2 — Body:
        ...

        Now parse this email:

    The model sees the customer's specific corrections as authority
    for "this is what RIGHT looks like for this customer."
    """
    if log is None:
        log = read_all()
    if not should_enrich(log, customer_id):
        return None
    examples = build_few_shot_examples(log, customer_id, k=k)
    if not examples:
        return None

    parts = ["Past corrections from this customer:\n"]
    for i, ex in enumerate(examples, start=1):
        body = (ex.get("body") or "").strip()
        if len(body) > 600:
            body = body[:600] + " […truncated]"
        parts.append(f"Example {i} — Body:")
        parts.append(f'"""\n{body}\n"""')
        parts.append(f"Parser produced: {ex.get('parser_entries', [])}")
        parts.append(f"Operator confirmed: {ex.get('corrected_entries', [])}")
        parts.append("")
    parts.append("Now parse this email:\n")
    return "\n".join(parts)
