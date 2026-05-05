"""Regression tests for real-world email shapes the parser must handle.

The Sprint 4 customer-feedback bug: a routine schedule email with a
Gmail reply quote ("On Sun, May 3, 2026 at 9:16 PM <...> wrote:")
trip-wired the parser's confidence demoters — "Sun" got read as a
day mention with no time range, "9:16" got read as half-hour time —
and the regex's HIGH-confidence output got demoted to LOW, then
overridden by an LLM rescue that produced wrong entries.

Fix landed in read_schedule._strip_quoted_history (cut at the FIRST
forward/reply separator, not the second). These tests lock that fix
in by sweeping every common email-body shape we're likely to see and
asserting the parser stays HIGH-confidence with the right entries.

Each scenario is a (subject, body, expected_entries) triple. We
parse with no API key (regex-only path), assert confidence is HIGH,
and assert the entries match exactly. Failure indicates the parser
got tripped by chrome (signatures, quote chains, header blocks,
greetings) that it should have ignored.

End-of-window encoding refresher (entry tuples = (weekday, start_h, end_h)):
  - start_h is 0..23 (hours within the start day)
  - end_h is offset from start-day midnight; can exceed 24 for
    multi-day continuous shifts. e.g. Mon 06:00 → Sat 04:00 = (0, 6, 124)
"""
from __future__ import annotations

import pytest

from read_schedule import parse_schedule


# Canonical 3-window schedule used across most fixtures so tests
# don't drift over time. Maps the user's reported failure case:
#   "MOn 0600 to Tues 1600  (multi-day, 34h)
#    Wed 0600 to 1600        (10h, single day)
#    Thurs 0600 to Fri 0400  (multi-day, 22h)"
EXPECTED_3W = [(0, 6, 40), (2, 6, 16), (3, 6, 28)]
SCHEDULE_BODY_3W = (
    "MOn 0600 to Tues 1600\n"
    "Wed 0600 to 1600\n"
    "Thurs 0600 to Fri 0400"
)


def _assert_high_with(entries, body):
    parsed, conf, notes, _method = parse_schedule(body, api_key=None)
    assert conf == "high", (
        f"Expected HIGH confidence, got {conf!r}.\n"
        f"Body:\n{body}\n\nNotes:\n" + "\n".join(f"  - {n.strip()}" for n in notes)
    )
    assert parsed == entries, (
        f"Entries mismatch.\n"
        f"  Expected: {entries}\n"
        f"  Got:      {parsed}\n"
        f"Body:\n{body}"
    )


# ── Gmail reply-quote shapes ────────────────────────────────────────────────


def test_gmail_single_reply_quote_below():
    """The exact bug reported: schedule body, then a Gmail reply quote
    line ("On Sun, May 3, 2026 at 9:16 PM <...> wrote:") below.
    Parser must strip the quote header before parsing."""
    body = SCHEDULE_BODY_3W + (
        "\n\nOn Sun, May 3, 2026 at 9:16 PM <vmiprototype@gmail.com> wrote:"
    )
    _assert_high_with(EXPECTED_3W, body)


def test_gmail_reply_quote_with_quoted_block():
    """Reply quote header + an actual quoted block (lines starting with
    '>'). Both must be stripped — quoted block contains a stale schedule
    that should not be parsed."""
    body = SCHEDULE_BODY_3W + """

On Mon, Apr 27, 2026 at 9:16 PM <vmiprototype@gmail.com> wrote:
> Last week's schedule:
> Mon 0700 to 1500
> Tue 0700 to 1500
> Thanks!"""
    _assert_high_with(EXPECTED_3W, body)


def test_gmail_reply_with_short_greeting_above():
    """Real customer email: greeting + schedule + sign-off + reply quote."""
    body = (
        "Hi team,\n\n"
        + SCHEDULE_BODY_3W
        + "\n\nThanks,\nJonathan\n\n"
        + "On Sun, May 3, 2026 at 9:16 PM <vmiprototype@gmail.com> wrote:\n"
        "> Schedule for next week please."
    )
    _assert_high_with(EXPECTED_3W, body)


# ── Outlook forwarded-message shapes ────────────────────────────────────────


def test_outlook_original_message_separator():
    """Outlook's '-----Original Message-----' divider must be stripped."""
    body = SCHEDULE_BODY_3W + """

-----Original Message-----
From: scheduler@plant.com
Sent: Friday, April 17, 2026 3:30 PM
To: vmi@distributor.com
Subject: Re: Schedule

Last week we ran Mon 0700 to 1500.
"""
    _assert_high_with(EXPECTED_3W, body)


def test_outlook_underscore_divider():
    """The 20+ underscore divider Outlook uses on some replies."""
    body = SCHEDULE_BODY_3W + (
        "\n\n____________________________________________\n"
        "From: scheduler@plant.com\n"
        "Sent: Friday April 17 3:30 PM\n"
        "Subject: Re: Schedule\n\n"
        "Mon 0700 to 1500\n"
    )
    _assert_high_with(EXPECTED_3W, body)


def test_outlook_inline_header_block_at_top():
    """Some Outlook clients inline From:/Sent:/To:/Subject: headers
    at the TOP of a forwarded body. _strip_header_block handles that."""
    body = (
        "From: scheduler@plant.com\n"
        "Sent: Sunday, May 3, 2026 9:16 PM\n"
        "To: vmi@distributor.com\n"
        "Subject: Schedule for the week of 5/11\n\n"
        + SCHEDULE_BODY_3W
    )
    _assert_high_with(EXPECTED_3W, body)


# ── Greetings + sign-offs ───────────────────────────────────────────────────


def test_simple_greeting_and_signoff():
    body = (
        "Hi Sarah,\n\n"
        "Here's the schedule for next week:\n\n"
        + SCHEDULE_BODY_3W
        + "\n\nThanks,\nJonathan\nPlant Scheduler\n555-123-4567"
    )
    _assert_high_with(EXPECTED_3W, body)


def test_signoff_with_dashes():
    body = SCHEDULE_BODY_3W + "\n\n--\nJonathan Davidson\nPlant Scheduler\n"
    _assert_high_with(EXPECTED_3W, body)


def test_best_regards_signoff():
    body = SCHEDULE_BODY_3W + "\n\nBest regards,\nJonathan"
    _assert_high_with(EXPECTED_3W, body)


# ── Time-format variations within the same body ────────────────────────────


def test_mixed_24h_and_ampm_formats():
    """Customers mix formats inside the same email."""
    body = (
        "Mon 6am to Tue 4pm\n"
        "Wed 0600 to 1600\n"
        "Thursday 06:00 to Friday 04:00"
    )
    _assert_high_with(EXPECTED_3W, body)


def test_short_day_abbreviations():
    """Tues / Thurs — non-standard but common ("Wednes" / "Saturd"
    are not, and the parser is not expected to handle those)."""
    body = (
        "Mon 0600 to Tues 1600\n"
        "Wed 0600 to 1600\n"
        "Thurs 0600 to Fri 0400"
    )
    _assert_high_with(EXPECTED_3W, body)


# ── Date-token decoration that should NOT trip demoters ────────────────────


def test_date_in_subject_line_in_body():
    """Customer pastes the subject ('Schedule for week of 5/11') into
    the body. Date tokens shouldn't leak into the parser's day-mention
    counter or trip the half-hour detector."""
    body = (
        "Schedule for week of 5/11/2026:\n\n"
        + SCHEDULE_BODY_3W
    )
    _assert_high_with(EXPECTED_3W, body)


def test_date_range_header_above_schedule():
    body = "Week of May 11 - May 17, 2026:\n\n" + SCHEDULE_BODY_3W
    _assert_high_with(EXPECTED_3W, body)


# ── Surrounding chatter that must NOT pollute the parser ───────────────────


def test_extra_prose_with_no_day_mentions():
    """Mild chatter above/below the schedule — no extra day names —
    must not demote confidence."""
    body = (
        "Hi team,\n"
        "FYI the schedule for next week is below — let me know if "
        "anything changes.\n\n"
        + SCHEDULE_BODY_3W
        + "\n\nLet me know if you need anything.\nThanks,\nJonathan"
    )
    _assert_high_with(EXPECTED_3W, body)


def test_prose_with_extra_day_mention_demotes_to_low():
    """When prose mentions a day NOT covered in the schedule
    (e.g. 'cleaning crew on Saturday'), the parser cannot tell
    whether that's schedule-relevant. Correct behaviour: extract
    the 3 windows we can identify but flag LOW confidence so the
    operator confirms before applying. This is the ambiguity-
    routing safety net — not a bug."""
    body = (
        SCHEDULE_BODY_3W
        + "\n\nWe'll have the cleaning crew in on Saturday.\n"
        "Thanks,\nJonathan"
    )
    parsed, conf, notes, _method = parse_schedule(body, api_key=None)
    assert parsed == EXPECTED_3W, (
        "Schedule windows should still be extracted correctly even "
        "when confidence is demoted by an unrelated day mention.\n"
        f"  Expected: {EXPECTED_3W}\n  Got:      {parsed}"
    )
    assert conf == "low", (
        f"Saturday mention without a time range should demote "
        f"confidence so the operator can confirm. Got conf={conf!r}."
    )


# ── Pure schedule (control case — no chrome to strip) ──────────────────────


def test_bare_schedule_no_chrome():
    """Confirm the canonical body alone produces HIGH confidence —
    so the chrome-stripping tests are isolating chrome handling, not
    masking a different issue."""
    _assert_high_with(EXPECTED_3W, SCHEDULE_BODY_3W)
