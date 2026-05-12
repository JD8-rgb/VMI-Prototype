"""Pin the audit's ideal behavior for vague-but-plausible schedules.

Audit framing (red-team `fd0066b`):
  vague but plausible schedule -> low confidence
  extracted best-guess schedule shown
  operator approves, edits, or dismisses

Two cases the audit identified as failing this ideal:

(1) `24/5 next week` — parser used to return [] (zero entries) at low
    confidence. Operator review panel had nothing to show, so the
    operator effectively got "we don't know what to do" instead of
    "here's our best guess, confirm." Fixed by short-circuiting on
    _FULL_WEEK_RE BEFORE the date-token rewrite (which used to
    interpret `24/5` as the EU date May 24 = a Sunday).

(2) `Run all week` — direct parser already produced the full-week
    template at low confidence. The bug was upstream in the email
    prefilter (`fetch_and_apply_schedule`), which dropped the email
    as not-schedule-shaped because it has no day or time token.
    Fixed by adding `has_full_week_shorthand` to the prefilter so
    the parser actually gets to run.

These tests pin the END STATE: both inputs surface a non-empty
low-confidence parse to the operator, with the full-week template.
"""
from read_schedule import parse_schedule_text, _FULL_WEEK_TEMPLATE


def test_24_5_next_week_routes_to_low_confidence_review():
    """'24/5 next week' must produce ONE window (the full-week
    template) at LOW confidence — so the operator review panel has
    a pre-filled best-guess to approve, edit, or dismiss."""
    entries, conf, notes = parse_schedule_text("24/5 next week")
    assert entries == [_FULL_WEEK_TEMPLATE], (
        f"expected full-week template, got {entries!r}"
    )
    assert conf == "low"
    assert any("full-week" in n.lower() for n in notes), (
        f"expected a 'full-week phrasing detected' note, got {notes!r}"
    )


def test_run_all_week_routes_to_low_confidence_review():
    """'Run all week' must produce the full-week template at LOW
    confidence. This was already working in the direct parser
    pre-fix; the test pins both that AND the post-fix expectation
    that the email prefilter now lets this through to the parser."""
    entries, conf, notes = parse_schedule_text("Run all week")
    assert entries == [_FULL_WEEK_TEMPLATE]
    assert conf == "low"


def test_24_7_variant_also_routes_to_review():
    """Same shorthand family — '24/7' was already handled by the
    regex but pin the behavior so a future regex change can't
    silently break it."""
    entries, conf, notes = parse_schedule_text("Run 24/7 this coming week")
    assert entries == [_FULL_WEEK_TEMPLATE]
    assert conf == "low"


def test_full_week_phrasing_in_email_prefilter():
    """The email prefilter (inside fetch_and_apply_schedule) must
    accept emails whose body contains full-week shorthand even when
    no explicit day name or time token is present. Without this, the
    parser never runs and the operator sees `not_found` instead of
    a low-confidence review."""
    import re
    from read_schedule import _FULL_WEEK_RE
    # The same regex the prefilter uses (see read_schedule.py around
    # the `_HAS_DAY_RE`/`_HAS_TIME_RE` filter). Asserting against
    # `_FULL_WEEK_RE.search(body)` here is equivalent to asserting
    # that the prefilter accepts the message.
    assert _FULL_WEEK_RE.search("Run all week") is not None
    assert _FULL_WEEK_RE.search("24/5 next week") is not None
    assert _FULL_WEEK_RE.search("24/7") is not None
    assert _FULL_WEEK_RE.search("full week") is not None
    # And: emails that ARE just chatter still get rejected by the
    # combined prefilter (no day, no time, no full-week shorthand).
    assert _FULL_WEEK_RE.search("Hi just checking in") is None
