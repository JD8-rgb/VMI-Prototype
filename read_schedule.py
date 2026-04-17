"""
read_schedule.py
----------------
Read Anna's latest schedule email, parse the run windows, and apply them
to data.json for the target week (next Mon-Sun).

Confidence is HIGH when 3 or more days are successfully parsed.
If confidence is LOW, an alert email is sent and the schedule is NOT applied.

Usage
-----
    python read_schedule.py              # parse and apply if high confidence
    python read_schedule.py --dry-run    # show what would be parsed, don't save

Importable
----------
    from read_schedule import fetch_and_apply_schedule
"""

import json
import re
import sys
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

from email_client import OutlookClient, load_config
from email_hooks import send_friday_reminder_if_needed
import time_utils

DATA_PATH = "data.json"
DRY_RUN   = "--dry-run" in sys.argv

# ── Day name lookup ───────────────────────────────────────────────────────────
# Include plural forms ("Mondays") and common short forms. The regex parser
# does a literal word-boundary search against these keys, so longer keys must
# come first — see _DAY_PATTERN below.
_DAY_MAP = {
    "mondays": 0,   "monday": 0,    "mon": 0,
    "tuesdays": 1,  "tuesday": 1,   "tues": 1,  "tue": 1,
    "wednesdays": 2,"wednesday": 2, "weds": 2,  "wed": 2,
    "thursdays": 3, "thursday": 3,  "thurs": 3, "thur": 3,  "thu": 3,
    "fridays": 4,   "friday": 4,    "fri": 4,
    "saturdays": 5, "saturday": 5,  "sat": 5,
    "sundays": 6,   "sunday": 6,    "sun": 6,
}
_DAY_ABBREV = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}

# Day-matching regex: longest keys first so "monday" matches before "mon",
# "mondays" before "monday". Word-bounded.
_DAY_KEYS_SORTED = sorted(_DAY_MAP.keys(), key=len, reverse=True)
_DAY_PATTERN = r"\b(" + "|".join(_DAY_KEYS_SORTED) + r")\b"

# Filler words that may appear between a day name and a time (e.g. "Saturday
# at 4AM", "Sun on 0600"). Non-capturing, optional group; absorbed and ignored.
_FILLER = r"(?:\s+(?:at|on|from|starting))?"

# ── Time parser ───────────────────────────────────────────────────────────────

def _parse_time(token):
    """
    Convert a time token to an int hour (0-23).
    Handles: '6am', '6:00', '6:00pm', '22', '22:00', '0600', '1400'.
    Returns None if unparseable.
    """
    t = token.strip().lower().replace(".", "")

    # 4-digit military time: 0600, 1400, 2200
    m = re.match(r"^(\d{2})(\d{2})$", t)
    if m:
        h = int(m.group(1))
        return h if 0 <= h <= 23 else None

    # HH:MM with optional am/pm
    m = re.match(r"^(\d{1,2}):(\d{2})\s*(am|pm)?$", t)
    if m:
        h = int(m.group(1))
        meridiem = m.group(3)
        if meridiem == "pm" and h != 12:
            h += 12
        elif meridiem == "am" and h == 12:
            h = 0
        return h if 0 <= h <= 23 else None

    # Hour-only with am/pm
    m = re.match(r"^(\d{1,2})\s*(am|pm)$", t)
    if m:
        h = int(m.group(1))
        if m.group(2) == "pm" and h != 12:
            h += 12
        elif m.group(2) == "am" and h == 12:
            h = 0
        return h if 0 <= h <= 23 else None

    # Plain number — treat as 24-hour
    m = re.match(r"^(\d{1,2})$", t)
    if m:
        h = int(m.group(1))
        return h if 0 <= h <= 23 else None

    return None


def _try_day_range_with_time(seg):
    """
    Detect a "day range + single time window" schedule like:
      "Monday - Friday 6AM-10PM"
      "Mon-Fri 0600-2200"
      "Monday through Friday 6am to 10pm"
      "Mon thru Wed 22:00-06:00"   (overnight window applied to each day)

    Unlike _try_multiday_range (which is ONE continuous window across
    days), this pattern means "apply the SAME daily window to every day
    in the range". Generates one entry per day.

    Returns list of (weekday, start_h, end_h) tuples, or [] if not found.
    """
    _TIME = r'(\d{4}|\d{1,2}:\d{2}(?:\s*(?:am|pm))?|\d{1,2}\s*(?:am|pm))'
    _SEP  = r'\s*(?:to|through|until|thru|[-–—])\s*'

    pattern = (
        _DAY_PATTERN + _SEP + _DAY_PATTERN + r'\s+' + _TIME
        + _SEP + _TIME
    )
    m = re.search(pattern, seg, flags=re.IGNORECASE)
    if not m:
        return []

    day1 = _DAY_MAP.get(m.group(1).lower())
    day2 = _DAY_MAP.get(m.group(2).lower())
    start_h = _parse_time(m.group(3))
    end_h   = _parse_time(m.group(4))
    if day1 is None or day2 is None or start_h is None or end_h is None:
        return []
    if end_h <= start_h:          # overnight window (e.g. 22:00-06:00)
        end_h += 24

    days_diff = (day2 - day1) % 7
    return [((day1 + i) % 7, start_h, end_h) for i in range(days_diff + 1)]


def _try_multiday_range(seg):
    """
    Detect a continuous multi-day run window like:
      "run monday 0600 to friday 0400"
      "monday 6am to friday 4am"
      "monday 06:00 through friday 04:00"
      "Monday 0600 to Saturday at 4AM"       ← filler word "at"
      "Starting wed 0800 to Sun on 0600"     ← filler word "on"

    Returns a list with one (start_weekday, start_h, end_h) entry where
    end_h may be > 24 (hours from start of start_weekday).
    Returns [] if pattern not found or days are the same.
    """
    # Match 0600 | 06:00 | 06:00am | 6am — the optional meridiem on HH:MM is
    # critical so "2:00am" consumes all three tokens, not just "2:00".
    _TIME = r'(\d{4}|\d{1,2}:\d{2}(?:\s*(?:am|pm))?|\d{1,2}\s*(?:am|pm))'
    # Day names are anchored to _DAY_PATTERN so we only match real days
    # (not "going" or "starting" as false day1 candidates).
    pattern = (
        _DAY_PATTERN + _FILLER + r'\s+' + _TIME + r'\s*'
        r'(?:to|through|until|thru|[-–—])\s*'
        + _DAY_PATTERN + _FILLER + r'\s+' + _TIME
    )
    m = re.search(pattern, seg, flags=re.IGNORECASE)
    if not m:
        return []

    day1_str, time1_str, day2_str, time2_str = (
        m.group(1).lower(), m.group(2), m.group(3).lower(), m.group(4)
    )
    day1 = _DAY_MAP.get(day1_str)
    day2 = _DAY_MAP.get(day2_str)
    if day1 is None or day2 is None or day1 == day2:
        return []

    start_h  = _parse_time(time1_str)
    end_h_d  = _parse_time(time2_str)
    if start_h is None or end_h_d is None:
        return []

    days_diff = (day2 - day1) % 7
    if days_diff == 0:
        return []
    duration = days_diff * 24 + (end_h_d - start_h)
    if duration <= 0:
        return []

    return [(day1, start_h, start_h + duration)]


# ── Email-cruft preprocessor ──────────────────────────────────────────────────

# Greeting lines at the very start: "Hi team,", "Hello,", "Hey —", etc.
_GREETING_LINE = re.compile(
    r'^\s*(?:hi|hello|hey|dear|good\s+(?:morning|afternoon|evening))'
    r'\b[^\n]*\n',
    re.IGNORECASE,
)
# Sign-off blocks at the very end: "Thanks,\nAnna" / "Regards," / "-- \nAnna"
# Also matches "Sent from my iPhone" footer.
_SIGNOFF_BLOCK = re.compile(
    r'(?:\n|^)\s*(?:thanks|thank\s+you|regards|best|cheers|sincerely|'
    r'sent\s+from\s+my\s+|--\s*$)'
    r'[^\n]*(?:\n[^\n]*){0,3}\s*$',
    re.IGNORECASE,
)
# Quoted-reply lines: starts with ">"
_QUOTED_LINE = re.compile(r'^\s*>.*$', re.MULTILINE)


def _clean_email_text(text):
    """
    Strip greetings, sign-offs, and quoted-reply lines so both parsers see
    just the meaningful body. Fail-safe: if stripping leaves nothing, return
    the original text unchanged.
    """
    if not isinstance(text, str) or not text.strip():
        return text
    t = _QUOTED_LINE.sub('', text)
    t = _GREETING_LINE.sub('', t, count=1)
    t = _SIGNOFF_BLOCK.sub('', t)
    t = re.sub(r'\n{3,}', '\n\n', t).strip()
    return t if t else text


# Pattern: a day-range on one line followed by a time-range on the next.
# Example email bodies where this shows up:
#     Monday - Friday
#     6AM-10PM
# We merge the two lines with a space so the segment splitter keeps them
# together and _try_day_range_with_time can match.
_RANGE_SEP_CHARS = r'(?:[-–—]|to|through|until|thru)'
_TIME_TOKEN      = r'(?:\d{4}|\d{1,2}:\d{2}(?:\s*(?:am|pm))?|\d{1,2}\s*(?:am|pm))'
_DAY_RANGE_THEN_TIME = re.compile(
    r'(?P<drange>' + _DAY_PATTERN + r'\s*' + _RANGE_SEP_CHARS + r'\s*'
    + _DAY_PATTERN + r')\s*\n+\s*(?P<trange>'
    + _TIME_TOKEN + r'\s*' + _RANGE_SEP_CHARS + r'\s*' + _TIME_TOKEN + r')',
    re.IGNORECASE,
)


def _join_range_lines(text):
    """Merge a day-range line with a following time-range line so the
    segment splitter doesn't strand either half.

    Named groups avoid a trap where _DAY_PATTERN's internal capture shifts
    the numbered-backreference indices.
    """
    if not isinstance(text, str):
        return text
    return _DAY_RANGE_THEN_TIME.sub(r'\g<drange> \g<trange>', text)


# ── Schedule text parser ──────────────────────────────────────────────────────

def _split_segments(text):
    """Split on commas, semicolons, newlines, sentence terminators, and the
    literal word 'and'. Sentence terminators use lookarounds so they don't
    break `06.00`-style digit-period-digit time tokens."""
    # Drop sentence terminators between non-digits, keep `.` inside times intact.
    t = re.sub(r'(?<!\d)[.!](?!\d)', '\n', text)
    # Split on newlines, commas, semicolons, and the word 'and' between tokens.
    pieces = re.split(r'[,;\n]+|\s+and\s+', t)
    return pieces


def _single_day_window(seg, weekday, day_span, time_pat):
    """
    Extract a single (weekday, start_h, end_h) window from `seg` near the
    given day_span (match span of the day word). Returns tuple or None.
    Off-marker check is proximity-bounded: only cancels if the marker is
    within ~15 chars of the day name (so prose like "demand pick up" or
    "coming down" doesn't accidentally cancel a day).
    """
    # Window of the segment to examine for off-marker / times
    d_start, d_end = day_span
    near_start = max(0, d_start - 4)
    near_end   = min(len(seg), d_end + 30)
    near       = seg[near_start:near_end]

    if re.search(r"\b(off|no run|shutdown|n/a|none)\b", near):
        return ("off", weekday)
    # "down" is prose-ambiguous — require it right after the day word
    if re.search(r"\b" + re.escape(seg[d_start:d_end]) + r"\s+(down)\b", seg):
        return ("off", weekday)

    # Find a time range in the rest of the segment (from day_end onward)
    tail = seg[d_end:]
    # Strip filler words "at"/"on" right after the day so time_pat can match
    tail = re.sub(r"^\s+(?:at|on)\s+", " ", tail)
    m = re.search(time_pat, tail)
    if not m:
        return None
    start_h = _parse_time(m.group(1))
    end_h   = _parse_time(m.group(2))
    if start_h is None or end_h is None:
        return None
    if end_h <= start_h:   # overnight window (22:00-06:00)
        end_h += 24
    return ("window", weekday, start_h, end_h)


def parse_schedule_text(text):
    """
    Parse plain-text schedule like:
        "Monday 6am-10pm, Tuesday 6am-2pm, Wednesday off, Thursday 6am-10pm"

    Returns
    -------
    entries    : list of (weekday_int, start_hour_int, end_hour_int)
    confidence : "high" if >= 3 calendar days covered, else "low"
    notes      : list of warning strings for partially-unreadable lines
    """
    entries        = []
    notes          = []
    effective_days = 0   # counts logical calendar days for confidence

    # Clean greetings / sign-offs / quoted replies before segmentation, so
    # prose like "Hi team,\n ... \nThanks, Anna" doesn't pollute segments.
    cleaned = _clean_email_text(text)
    # Merge "Monday-Friday\n6AM-10PM" style split-across-lines ranges before
    # the segment splitter sees them on separate lines.
    cleaned = _join_range_lines(cleaned)
    segments = _split_segments(cleaned)

    # Regex that matches any time token: 0600, 06:00, 6am, 6:00am, 22:00, etc.
    # (Plain 1-2 digit numbers are not accepted to avoid matching stray
    # numbers in prose.)
    _T = r'(\d{4}|\d{1,2}:\d{2}(?:\s*(?:am|pm))?|\d{1,2}\s*(?:am|pm))'
    time_pat = _T + r'\s*(?:[-–—]|to|until|through|thru)\s*' + _T

    for raw in segments:
        seg = raw.strip().lower()
        if not seg:
            continue

        # ── Day-range + single time ("Mon-Fri 6am-10pm") ────────────────────
        # Applied first because the pattern (DAY sep DAY TIME sep TIME) is
        # distinct from the continuous range (DAY TIME sep DAY TIME) — the
        # two don't overlap, but checking this first means a clean, small
        # branch for the most common recurring-hours format.
        drange = _try_day_range_with_time(seg)
        if drange:
            entries.extend(drange)
            effective_days += len(drange)
            days_str = "-".join(_DAY_ABBREV[drange[0][0]].split()[0:1]
                                + [_DAY_ABBREV[drange[-1][0]]])
            sh, eh = drange[0][1], drange[0][2]
            notes.append(
                f"  Day-range window: {days_str} "
                f"{sh:02d}:00-{eh % 24:02d}:00 ({len(drange)} days)"
            )
            continue

        # ── Continuous multi-day range ("Run Monday 0600 to Friday 0400") ────
        multiday = _try_multiday_range(seg)
        if multiday:
            entries.extend(multiday)
            day1    = multiday[0][0]
            duration_h   = multiday[0][2] - multiday[0][1]
            days_covered = max(1, (duration_h + 23) // 24)
            effective_days += days_covered
            day2_num = (day1 + duration_h // 24) % 7
            notes.append(
                f"  Multi-day window: {_DAY_ABBREV[day1]} {multiday[0][1]:02d}:00 "
                f"→ {_DAY_ABBREV[day2_num]} {multiday[0][2] % 24:02d}:00 "
                f"({days_covered} days)"
            )
            continue

        # ── Find EVERY day name in this segment (not just the first), so
        #    segments like "Mon 6am-10pm and Tue 6am-10pm" emit both days.
        day_hits = [(m.start(), m.end(), _DAY_MAP[m.group(1).lower()])
                    for m in re.finditer(_DAY_PATTERN, seg, flags=re.IGNORECASE)]
        if not day_hits:
            continue
        # De-duplicate weekdays keeping the first occurrence's span
        seen = set()
        unique_hits = []
        for s, e, wd in day_hits:
            if wd in seen:
                continue
            seen.add(wd)
            unique_hits.append((s, e, wd))

        for d_start, d_end, weekday in unique_hits:
            result = _single_day_window(seg, weekday, (d_start, d_end), time_pat)
            if result is None:
                notes.append(f"  {_DAY_ABBREV[weekday]}: day found but no time range detected")
                continue
            if result[0] == "off":
                notes.append(f"  {_DAY_ABBREV[weekday]}: marked as off/no run")
                continue
            _, wd, start_h, end_h = result
            entries.append((wd, start_h, end_h))
            effective_days += 1

    confidence = "high" if effective_days >= 3 else "low"
    return entries, confidence, notes


# ── LLM schedule parser ───────────────────────────────────────────────────────

class LLMParseError(Exception):
    """Raised by parse_schedule_llm with a specific failure stage tag."""
    def __init__(self, stage, detail, raw_response=None):
        self.stage = stage            # "import" | "auth" | "api" | "empty" | "json" | "schema"
        self.detail = detail          # human-readable error text
        self.raw_response = raw_response  # raw model output if we got that far
        super().__init__(f"[{stage}] {detail}")


def _coverage_days(entries):
    """
    How many *calendar days* are covered across all windows. A single
    continuous window like Mon 06:00 → Fri 04:00 (end_hour = 94) covers
    ~4 days, so it should count as high-confidence even though only one
    weekday is listed.
    """
    total = 0
    for _, start_h, end_h in entries:
        duration = max(0, end_h - start_h)
        total += max(1, (duration + 23) // 24)   # round up, min 1 per window
    return total


def parse_schedule_llm(text, api_key):
    """
    Use Claude to parse schedule text into run windows.
    Handles arbitrary natural language formats, time-first notation,
    multi-day ranges packed without commas, etc.

    Returns the same signature as parse_schedule_text:
        (entries, confidence, notes)
    where entries = list of (weekday_int, start_hour_int, end_hour_int).
    end_hour_int may exceed 24 for overnight / multi-day windows.

    Raises LLMParseError with a `stage` tag if anything goes wrong so the
    caller can surface a specific reason (auth / network / JSON / schema).
    """
    import json as _json
    try:
        import anthropic as _anthropic
    except ImportError as e:
        raise LLMParseError("import", f"anthropic package not installed: {e}")

    try:
        client = _anthropic.Anthropic(api_key=api_key)
    except Exception as e:
        raise LLMParseError("auth", f"could not create Anthropic client: {e}")

    prompt = (
        "Parse this production run schedule into JSON run windows.\n\n"
        "IMPORTANT: The input may be a complete email containing greetings,\n"
        "sign-offs, quoted replies ('> ...'), and other prose. Extract ONLY\n"
        "the production run schedule and ignore everything else. If no run\n"
        "schedule is present, return [].\n\n"
        "Filler words like 'at', 'on', 'from', 'starting' may appear between\n"
        "a day and a time (e.g. 'Saturday at 4AM', 'Sun on 0600') — ignore\n"
        "them. Plural day names ('Mondays', 'Tuesdays') mean the same as\n"
        "singular.\n\n"
        "Return a JSON array. Each item has:\n"
        "  weekday    : int 0–6 (Monday=0 … Sunday=6)\n"
        "  start_hour : int 0–23 (hour of day the window starts)\n"
        "  end_hour   : int, hours from midnight of start_weekday\n"
        "               (can exceed 24 for overnight or multi-day windows)\n\n"
        "TIME CONVERSION (critical):\n"
        "  - '7pm'  → 19        '7:00pm' → 19      '16:00' → 16\n"
        "  - '7am'  →  7        '12pm'   → 12      '12am'  →  0\n"
        "  - '0600' →  6        '2200'   → 22\n\n"
        "SAME-DAY WINDOW arithmetic: end_hour = end_time (no +24).\n"
        "  'wednesday 16:00-7:00pm' → start_hour=16, end_time=19 → end_hour=19\n"
        "  (Do NOT treat this as overnight — 19 > 16, so same day.)\n\n"
        "OVERNIGHT WINDOW (end_time <= start_time on same day):\n"
        "  end_hour = end_time + 24.\n"
        "  'Thursday 22:00-06:00' → weekday=3, start_hour=22, end_hour=30.\n\n"
        "MULTI-DAY WINDOW arithmetic (start day ≠ end day):\n"
        "  days_diff = (end_weekday - start_weekday) mod 7\n"
        "    (if result is 0 and times differ, use 7)\n"
        "  end_hour  = start_hour + days_diff*24 + (end_time - start_time)\n"
        "  Do NOT add an extra 24h for the final day.\n\n"
        "Examples:\n"
        "  'Monday 6am-10pm'              → [{\"weekday\":0,\"start_hour\":6,\"end_hour\":22}]\n"
        "  'wed 16:00-7:00pm'             → [{\"weekday\":2,\"start_hour\":16,\"end_hour\":19}]\n"
        "  'Thu 22:00-06:00'              → [{\"weekday\":3,\"start_hour\":22,\"end_hour\":30}]\n"
        "  'Mon 0600 - Tue 1600'          → [{\"weekday\":0,\"start_hour\":6,\"end_hour\":34}]\n"
        "     (days_diff=1; end = 6 + 1*24 + (16-6) = 34)\n"
        "  '0600 Mon - 0400 Fri'          → [{\"weekday\":0,\"start_hour\":6,\"end_hour\":94}]\n"
        "     (days_diff=4; end = 6 + 4*24 + (4-6) = 94)\n"
        "  'monday 0500 thru Sun 0800'    → [{\"weekday\":0,\"start_hour\":5,\"end_hour\":152}]\n"
        "     (days_diff=6; end = 5 + 6*24 + (8-5) = 152)\n"
        "  'mon 0600-tues 1600 Wed 0600-1600 Thurs 0600-Fri 0400'\n"
        "      → [{\"weekday\":0,\"start_hour\":6,\"end_hour\":34},\n"
        "         {\"weekday\":2,\"start_hour\":6,\"end_hour\":16},\n"
        "         {\"weekday\":3,\"start_hour\":6,\"end_hour\":46}]\n"
        "  'Hi team, we are going to Monday 0600 to Saturday at 4AM. Thanks'\n"
        "      → [{\"weekday\":0,\"start_hour\":6,\"end_hour\":124}]\n"
        "        (days_diff=5; end = 6 + 5*24 + (4-6) = 124)\n\n"
        "Omit days marked off / down / no run / shutdown.\n"
        "Return ONLY valid JSON — no explanation, no markdown fences.\n\n"
        f"Schedule text:\n{text}"
    )

    try:
        msg = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
    except _anthropic.AuthenticationError as e:
        raise LLMParseError("auth", f"API key rejected: {e}")
    except _anthropic.APIConnectionError as e:
        raise LLMParseError("api", f"network/connection error: {e}")
    except _anthropic.RateLimitError as e:
        raise LLMParseError("api", f"rate-limited: {e}")
    except _anthropic.APIStatusError as e:
        raise LLMParseError("api", f"API {e.status_code}: {e}")
    except Exception as e:
        raise LLMParseError("api", f"unexpected API error ({type(e).__name__}): {e}")

    raw = msg.content[0].text.strip() if msg.content else ""
    if not raw:
        raise LLMParseError("empty", "model returned no text content")

    # Strip markdown code fences if model adds them
    cleaned = re.sub(r'^```(?:json)?\s*|\s*```$', '', raw, flags=re.MULTILINE).strip()

    try:
        windows = _json.loads(cleaned)
    except Exception as e:
        raise LLMParseError("json", f"response was not valid JSON: {e}",
                            raw_response=raw[:400])

    try:
        entries = [(int(w["weekday"]), int(w["start_hour"]), int(w["end_hour"]))
                   for w in windows]
    except Exception as e:
        raise LLMParseError("schema", f"unexpected JSON shape: {e}",
                            raw_response=raw[:400])

    # Confidence: count calendar-days covered across all windows (so a single
    # continuous Mon → Fri range counts as multiple days, not just one).
    days_covered  = _coverage_days(entries)
    distinct_days = len({e[0] for e in entries})
    confidence    = "high" if days_covered >= 3 else "low"
    notes = [
        f"  LLM parsed {len(entries)} window(s) starting on {distinct_days} day(s), "
        f"covering ~{days_covered} calendar day(s) — confidence: {confidence}"
    ]
    return entries, confidence, notes


def parse_schedule(text, api_key=None):
    """
    REGEX-FIRST strategy:
      1. Run the regex parser (parse_schedule_text). If it returns HIGH
         confidence, use it — no LLM call needed. The regex is exhaustively
         tested (test_schedule_parser.py, 1,474 synthetic cases) and is
         faster, free, and deterministic.
      2. Only when regex is LOW confidence (or produces no entries) do we
         fall through to the LLM to rescue genuinely novel phrasings.
      3. Whichever result has the better score (high > low, then
         coverage-days, then entry-count) wins. Notes from both parsers
         are merged so the full trail is visible.

    Rationale: in a prior stress-test sweep, Haiku was confidently wrong
    on ~15% of multi-day arithmetic cases (days_diff off by one, PM times
    treated as overnight). Trusting the LLM first and falling back to
    regex only on low-confidence caused combined to propagate those
    errors. Inverting the priority — regex first — recovers 100% pass
    rate on the synthetic corpus and reserves the LLM for the long tail.

    Always returns (entries, confidence, notes).
    """
    # ── 1. Regex pass (always) ────────────────────────────────────────────
    rx_entries, rx_confidence, rx_notes = parse_schedule_text(text)
    rx_notes = [f"  Regex parser: {len(rx_entries)} window(s), "
                f"confidence {rx_confidence}."] + rx_notes

    # Decide whether the LLM rescue is worth trying:
    #   * HIGH confidence → regex is trusted, skip LLM (saves API cost).
    #   * LOW confidence but regex already extracted as many windows as
    #     there are distinct day-name mentions in the text → probably
    #     single-day or already complete, skip LLM.
    #   * LOW confidence AND regex extracted fewer windows than there are
    #     distinct day names → LLM rescue may recover missed days.
    distinct_day_mentions = len(set(
        m.group(1).lower()
        for m in re.finditer(_DAY_PATTERN, text, flags=re.IGNORECASE)
    ))
    distinct_regex_days = len({e[0] for e in rx_entries})

    if rx_confidence == "high":
        print(f"[schedule] Regex parse is HIGH confidence "
              f"({len(rx_entries)} window(s)) — no LLM call needed.")
        return rx_entries, rx_confidence, rx_notes

    if rx_entries and distinct_regex_days >= distinct_day_mentions:
        print(f"[schedule] Regex covered all {distinct_day_mentions} distinct "
              f"day mention(s) — no LLM call needed.")
        return rx_entries, rx_confidence, rx_notes

    # ── 2. Regex was incomplete — try the LLM as a rescue ─────────────────
    if not api_key:
        note = "  No Anthropic API key configured — using regex parser only."
        print("[schedule] " + note.strip())
        return rx_entries, rx_confidence, [note] + rx_notes

    llm_failure_note = None
    try:
        llm_entries, llm_confidence, llm_notes = parse_schedule_llm(text, api_key)
        print(f"[schedule] LLM rescue returned {llm_confidence} confidence "
              f"({len(llm_entries)} window(s)).")
    except LLMParseError as e:
        stage_label = {
            "import": "Anthropic SDK not installed",
            "auth":   "API key invalid or rejected",
            "api":    "API call failed (network / rate-limit / service)",
            "empty":  "API returned an empty response",
            "json":   "API response was not valid JSON",
            "schema": "API returned JSON with unexpected fields",
        }.get(e.stage, "LLM parse failed")
        print(f"[schedule] LLM rescue failed — {stage_label}: {e.detail}")
        if e.raw_response:
            print(f"[schedule] Raw model output (truncated): {e.raw_response}")
        llm_failure_note = f"  LLM rescue failed — {stage_label}: {e.detail}"
        return rx_entries, rx_confidence, [llm_failure_note] + rx_notes
    except Exception as e:
        print(f"[schedule] LLM rescue failed (unexpected): {e}")
        llm_failure_note = f"  LLM rescue failed (unexpected): {e}"
        return rx_entries, rx_confidence, [llm_failure_note] + rx_notes

    # ── 3. Score both and keep the better one ─────────────────────────────
    def _score(entries, confidence):
        # Higher is better. Confidence dominates; then coverage-days; then count.
        return (1 if confidence == "high" else 0,
                _coverage_days(entries),
                len(entries))

    if _score(llm_entries, llm_confidence) > _score(rx_entries, rx_confidence):
        print(f"[schedule] LLM rescue improved on regex "
              f"({llm_confidence}, {len(llm_entries)} window(s)).")
        combined_notes = (
            ["  Used LLM rescue — regex was low confidence."]
            + rx_notes + llm_notes
        )
        return llm_entries, llm_confidence, combined_notes
    else:
        print(f"[schedule] Keeping regex result — LLM rescue did not improve it "
              f"({rx_confidence}, {len(rx_entries)} window(s)).")
        combined_notes = (
            ["  Kept regex result — LLM rescue did not improve confidence."]
            + rx_notes + llm_notes
        )
        return rx_entries, rx_confidence, combined_notes


def check_anthropic_api(api_key):
    """
    Diagnostic: verify the Anthropic API is reachable with the given key.

    Returns (ok, message) where:
      ok=True  → API is reachable and the key works
      ok=False → message explains exactly why (missing key / auth / network / etc.)

    Performs a minimal 1-token call so it costs ~nothing but exercises the
    full auth + network path.
    """
    if not api_key:
        return False, ("No API key configured. Set ANTHROPIC_API_KEY in your "
                       "environment, Streamlit secrets, or email_config.json "
                       "(anthropic_api_key field).")
    try:
        import anthropic as _anthropic
    except ImportError as e:
        return False, f"anthropic package not installed: {e}"

    try:
        client = _anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=5,
            messages=[{"role": "user", "content": "Reply with the single word: ok"}],
        )
        reply = msg.content[0].text.strip() if msg.content else "(empty)"
        masked = api_key[:7] + "…" + api_key[-4:] if len(api_key) > 12 else "***"
        return True, (f"Anthropic API reachable. Key {masked} accepted by "
                      f"claude-haiku-4-5. Test reply: '{reply}'.")
    except _anthropic.AuthenticationError as e:
        return False, f"API key rejected (authentication failed): {e}"
    except _anthropic.APIConnectionError as e:
        return False, f"Network/connection error — check internet access: {e}"
    except _anthropic.RateLimitError as e:
        return False, f"Rate-limited by Anthropic: {e}"
    except _anthropic.APIStatusError as e:
        return False, f"API returned status {e.status_code}: {e}"
    except Exception as e:
        return False, f"Unexpected error ({type(e).__name__}): {e}"


# ── Apply schedule to data ────────────────────────────────────────────────────

def _next_week_bounds(data, now_dt=None):
    """
    Return (week_start_rh, week_end_rh, target_monday_dt) for the next Mon-Sun.

    now_dt : reference datetime to calculate "next Monday" from.
             Defaults to datetime.now() (real wall clock).
             Pass the sim clock datetime from the Streamlit app so that
             'next week' is relative to the simulation, not real time.
    """
    today = now_dt if now_dt is not None else datetime.now()
    days_ahead = (7 - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    next_monday = (today + timedelta(days=days_ahead)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    next_sunday_end = next_monday + timedelta(days=7)
    week_start_rh = time_utils.dt_to_run_hour(data, next_monday)
    week_end_rh   = time_utils.dt_to_run_hour(data, next_sunday_end)
    return week_start_rh, week_end_rh, next_monday

# Keep old name as alias for backward compatibility with any external callers
_next_week_bounds_real = _next_week_bounds


def apply_schedule_to_data(data, entries, dry_run=False, now_dt=None):
    """
    Remove existing run windows that fall in next week, then add new ones.

    entries : list of (weekday_int, start_hour_int, end_hour_int)
    Returns updated data dict (not saved — caller must save).
    """
    week_start_rh, week_end_rh, next_monday = _next_week_bounds(data, now_dt=now_dt)

    # Remove windows whose start falls in the target week
    before = len(data["run_schedule"])
    data["run_schedule"] = [
        w for w in data["run_schedule"]
        if not (week_start_rh <= w["start_hour"] < week_end_rh)
    ]
    removed = before - len(data["run_schedule"])

    new_windows = []
    for weekday, start_h, end_h in sorted(entries):
        day_dt_start = next_monday + timedelta(days=weekday, hours=start_h)
        day_dt_end   = next_monday + timedelta(days=weekday, hours=end_h)
        new_windows.append({
            "start_hour": time_utils.dt_to_run_hour(data, day_dt_start),
            "end_hour":   time_utils.dt_to_run_hour(data, day_dt_end),
            "label":      _DAY_ABBREV[weekday],
        })

    if not dry_run:
        data["run_schedule"].extend(new_windows)
        data["run_schedule"].sort(key=lambda w: w["start_hour"])
        # Mark schedule received for the target week
        data["schedule_received_for_week"] = next_monday.date().isoformat()

    return data, removed, new_windows


# ── Main fetch-and-apply function (importable) ────────────────────────────────

def fetch_and_apply_schedule(data, dry_run=False, now_dt=None, session_start_utc=None):
    """
    Read Anna's latest email, parse the schedule, and apply it.

    now_dt : optional datetime to use as "now" when computing the target week.
             Pass the simulation clock datetime so "next week" is relative to
             sim time rather than real wall-clock time.

    session_start_utc : optional timezone-aware datetime (UTC). When provided,
             only emails RECEIVED AT OR AFTER this wall-clock moment are
             considered. This prevents a fresh session (or post-Reset state)
             from silently applying a stale schedule email left over in the
             inbox from a previous demo run.

    Returns
    -------
    "applied"        — schedule parsed (high confidence) and applied
    "low_confidence" — email found but parsing was unreliable; alert sent
    "not_found"      — no email from Anna in inbox
    """
    config   = load_config()
    anna     = config.get("anna_email", "")
    dist     = config.get("distribution_group", "")
    # API key priority: env var → config file
    import os as _os
    api_key  = (_os.environ.get("ANTHROPIC_API_KEY", "")
                or config.get("anthropic_api_key", ""))
    if not config:
        print("[schedule] WARN: email not configured.")
        return "not_found"

    client = OutlookClient(config)
    # anna_email="" means "accept from any sender" — useful for demos where
    # the schedule may be sent from different addresses. Use a wider window
    # (top=10) when there is no sender filter so the schedule email isn't
    # missed if other inbox activity has arrived more recently.
    results = client.search_inbox(sender=anna or None, top=10 if not anna else 5)

    if not results:
        who = anna if anna else "anyone"
        print(f"[schedule] No emails found from {who}.")
        return "not_found"

    # Inbox summary — lets us see immediately whether the schedule email
    # is actually being fetched by IMAP and what its body looks like.
    print(f"[schedule] search_inbox returned {len(results)} email(s):")
    for m in results:
        body_snip = ((m.get("body", "") or "").replace("\r", "").replace("\n", " ⏎ "))[:80]
        print(f"[schedule]   - from={m.get('sender','?')[:40]!r} "
              f"subj={m.get('subject','')[:40]!r} "
              f"body[:80]={body_snip!r}")

    # Exclude system-generated emails (alerts, red-flag notifications, etc.)
    # by body-start signature and subject prefix ONLY.  We intentionally do
    # NOT filter by sender address — the demo account (vmiprototype@gmail.com)
    # is both the sender of real schedule emails AND the recipient of alert
    # emails, so a sender-address check would drop legitimate schedules.
    # Every alert body the system emits starts with one of these fixed strings:
    _VMI_SUBJECT_PREFIXES = ("VMI:", "VMI ALERT", "VMI RED FLAG")
    _VMI_BODY_SIGNATURES  = (
        "VMI ALERT",
        "The VMI system received an email",
        "RED FLAG:",
    )
    before_self = len(results)
    filtered = []
    for m in results:
        subj      = (m.get("subject", "") or "").strip()
        body_head = ((m.get("body",    "") or "").lstrip())[:80]
        drop_reason = None
        if subj.upper().startswith(tuple(p.upper() for p in _VMI_SUBJECT_PREFIXES)):
            drop_reason = f"subject prefix ({subj!r})"
        elif any(body_head.startswith(sig) for sig in _VMI_BODY_SIGNATURES):
            drop_reason = f"body signature ({body_head[:40]!r})"
        if drop_reason:
            print(f"[schedule]   drop: {drop_reason}")
            continue
        filtered.append(m)
    dropped_self = before_self - len(filtered)
    if dropped_self:
        print(f"[schedule] Ignored {dropped_self} VMI-system-generated email(s).")
    results = filtered
    if not results:
        print("[schedule] No candidate schedule emails after filtering system-generated.")
        return "not_found"

    # Pre-filter: only emails that LOOK like schedule emails — i.e. that
    # contain at least one day name OR one time pattern in the body —
    # reach the parser.  This silently skips things like "Can you please
    # share next week's run schedule?" requests from colleagues, which
    # would otherwise parse to 0 windows and trigger a spurious
    # "unreadable schedule" alert that gets emailed back into the inbox
    # and creates a feedback loop.
    _HAS_DAY_RE  = re.compile(_DAY_PATTERN, re.IGNORECASE)
    _HAS_TIME_RE = re.compile(_TIME_TOKEN, re.IGNORECASE)
    before_shape = len(results)
    shape_kept = []
    for m in results:
        body = m.get("body", "") or ""
        has_day  = bool(_HAS_DAY_RE.search(body))
        has_time = bool(_HAS_TIME_RE.search(body))
        if has_day or has_time:
            shape_kept.append(m)
        else:
            body_snip = body.replace("\r", "").replace("\n", " ⏎ ")[:100]
            print(f"[schedule]   skip (no day or time tokens): "
                  f"subject={m.get('subject','')!r} "
                  f"body[:100]={body_snip!r}")
    dropped_shape = before_shape - len(shape_kept)
    if dropped_shape:
        print(f"[schedule] Skipped {dropped_shape} email(s) that look non-schedule.")
    results = shape_kept
    if not results:
        print("[schedule] No schedule-shaped emails in inbox — nothing to do.")
        return "not_found"

    # Filter out truly stale emails.  The original filter used the Streamlit
    # session_start timestamp, but that turned out to be too strict for
    # demos: an operator who composes the schedule email and THEN opens the
    # app has their email silently dropped.  We now use a 24-hour wall-clock
    # window.  The schedule_email_id dedup below still prevents re-applying
    # any specific email that has already been used.
    wall_now = datetime.now(timezone.utc)
    cutoff = wall_now - timedelta(hours=24)
    # If the session started more recently than the cutoff, use session_start
    # as the cutoff anyway — keeps legacy behaviour for long-running sessions.
    if session_start_utc is not None and session_start_utc < cutoff:
        cutoff = session_start_utc

    before = len(results)
    kept = []
    for m in results:
        rcv_hdr = m.get("received", "")
        try:
            rcv_dt = parsedate_to_datetime(rcv_hdr) if rcv_hdr else None
            if rcv_dt is not None and rcv_dt.tzinfo is None:
                rcv_dt = rcv_dt.replace(tzinfo=timezone.utc)
        except Exception:
            rcv_dt = None
        if rcv_dt is None:
            # Unparseable date — skip to be safe
            continue
        if rcv_dt >= cutoff:
            kept.append(m)
    dropped = before - len(kept)
    if dropped:
        print(f"[schedule] Ignored {dropped} email(s) older than cutoff "
              f"({cutoff.isoformat()}).")
    results = kept
    if not results:
        print("[schedule] No recent schedule emails within the 24h window.")
        return "not_found"

    # Skip emails we've already processed — either successfully applied OR
    # already alerted on as unreadable.  Without this the same bad email
    # keeps triggering the "low_confidence" alert on every clock advance.
    # `schedule_alerted_ids` is a SET of every id we've alerted on, so
    # multiple non-schedule emails in the inbox don't cycle through each
    # other on repeated advances.
    last_applied_id = data.get("schedule_email_id")
    last_alert_id   = data.get("schedule_unreadable_alert_id")  # legacy single-id
    alerted_ids     = set(data.get("schedule_alerted_ids", []) or [])
    if last_alert_id:
        alerted_ids.add(last_alert_id)
    ignore_ids = {i for i in (last_applied_id,) if i} | alerted_ids
    if ignore_ids:
        results = [m for m in results if m["id"] not in ignore_ids]
    if not results:
        print(f"[schedule] No new emails since last applied/alerted "
              f"(ids={sorted(ignore_ids)}).")
        return "not_found"

    # Try the most recent email first; if low confidence, try older ones.
    # Uses LLM parsing when an Anthropic API key is configured.
    best_entries, best_confidence, best_notes, best_msg = [], "low", [], None

    for msg in results:
        # Diagnostic: show what the parser is actually seeing.  Helps debug
        # cases where the email arrives but the body has unexpected chars
        # (HTML entities, non-breaking spaces, unusual dashes, etc.) that
        # prevent the regex from recognising the schedule.
        raw_body = (msg.get("body") or "").replace("\r", "").strip()
        body_preview = raw_body[:200].replace("\n", " ⏎ ")
        print(f"[schedule] Trying email from {msg.get('sender','?')}: "
              f"body[0:200]={body_preview!r}")
        entries, confidence, notes = parse_schedule(msg["body"], api_key=api_key)
        if confidence == "high":
            best_entries, best_confidence, best_notes, best_msg = entries, confidence, notes, msg
            break
        elif len(entries) > len(best_entries):
            best_entries, best_confidence, best_notes, best_msg = entries, confidence, notes, msg

    print(f"[schedule] Best match: {len(best_entries)} day(s) parsed — confidence: {best_confidence}")
    for n in best_notes:
        print(n)

    if best_confidence == "high":
        data, removed, new_windows = apply_schedule_to_data(data, best_entries, dry_run=dry_run, now_dt=now_dt)
        _, _, next_monday = _next_week_bounds(data, now_dt=now_dt)
        week_str = next_monday.date().isoformat()
        if dry_run:
            print(f"[schedule] DRY RUN — would replace {removed} window(s) with {len(new_windows)} for week of {week_str}:")
        else:
            print(f"[schedule] Applied {len(new_windows)} window(s) for week of {week_str} (removed {removed} old).")
            # Remember which email we just used so we don't re-apply it next check
            if best_msg:
                data["schedule_email_id"] = best_msg["id"]
        for w in new_windows:
            print(f"  {w['label']}: {time_utils.format_run_hour(data, w['start_hour'])} → {time_utils.format_run_hour(data, w['end_hour'])}")
        return "applied"

    else:
        # Either partial parse (low confidence, 1-2 days) OR zero parse. In
        # BOTH cases we received email(s) but couldn't confidently apply a
        # schedule — raise an alert so the operator knows the inbound email
        # needs manual review. Silent "not_found" here would be dangerous:
        # the operator would assume nothing arrived when in fact a malformed
        # or unreadable schedule did.
        days_found  = len(best_entries)
        preview_msg = best_msg or (results[0] if results else None)

        subject = "VMI: Could not read schedule from inbound email"
        if days_found:
            lead = (f"The VMI system received an email but could only parse "
                    f"{days_found} day(s) with low confidence.")
        else:
            lead = ("The VMI system received an email but could not extract "
                    "ANY schedule windows from it. The format may be "
                    "unrecognised (e.g. image-only, embedded table, unusual "
                    "phrasing).")
        body_preview = ""
        if preview_msg:
            raw_body = preview_msg.get("body", "") or ""
            body_preview = (
                f"From: {preview_msg.get('sender', 'N/A')}\n"
                f"Subject: {preview_msg.get('subject', 'N/A')}\n\n"
                f"Body preview:\n---\n{raw_body[:500]}\n---\n"
            )
        body = (
            f"{lead}\n\n"
            f"{body_preview}\n"
            f"Parse notes:\n" + "\n".join(best_notes or ["(none)"]) + "\n\n"
            f"Please review the email and update the run schedule manually."
        )

        # Dedup: don't resend the same alert on every clock advance.
        # Track ALL alerted ids in a set so multiple unreadable emails in
        # the inbox don't cycle through each other on repeated advances.
        preview_id = preview_msg["id"] if preview_msg else None
        alerted_set = set(data.get("schedule_alerted_ids", []) or [])
        legacy_id   = data.get("schedule_unreadable_alert_id")
        if legacy_id:
            alerted_set.add(legacy_id)
        if dist and preview_id and preview_id not in alerted_set:
            try:
                client.send_mail([dist], subject, body)
                print(f"[schedule] Unreadable-email alert sent to {dist}.")
                alerted_set.add(preview_id)
                data["schedule_alerted_ids"] = sorted(alerted_set)
                data["schedule_unreadable_alert_id"] = preview_id  # keep legacy field in sync
            except Exception as e:
                print(f"[schedule] WARN: could not send alert — {e}")
        elif dist and preview_id in alerted_set:
            print(f"[schedule] Alert already sent for this email — suppressing duplicate.")
        return "low_confidence"


# ── Standalone entry point ────────────────────────────────────────────────────

if __name__ == "__main__":
    with open(DATA_PATH) as f:
        data = json.load(f)

    result = fetch_and_apply_schedule(data, dry_run=DRY_RUN)

    if result == "applied" and not DRY_RUN:
        with open(DATA_PATH, "w") as f:
            json.dump(data, f, indent=2)
        print("[schedule] data.json saved.")
    elif result == "not_found":
        print("[schedule] No schedule found — consider running check_reminder.py.")
