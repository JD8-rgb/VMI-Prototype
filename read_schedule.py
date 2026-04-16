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
from datetime import datetime, timedelta

from email_client import OutlookClient, load_config
from email_hooks import send_friday_reminder_if_needed
import time_utils

DATA_PATH = "data.json"
DRY_RUN   = "--dry-run" in sys.argv

# ── Day name lookup ───────────────────────────────────────────────────────────
_DAY_MAP = {
    "monday": 0,    "mon": 0,
    "tuesday": 1,   "tue": 1,   "tues": 1,
    "wednesday": 2, "wed": 2,
    "thursday": 3,  "thu": 3,   "thur": 3,  "thurs": 3,
    "friday": 4,    "fri": 4,
    "saturday": 5,  "sat": 5,
    "sunday": 6,    "sun": 6,
}
_DAY_ABBREV = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}

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


def _try_multiday_range(seg):
    """
    Detect a continuous multi-day run window like:
      "run monday 0600 to friday 0400"
      "monday 6am to friday 4am"
      "monday 06:00 through friday 04:00"

    Returns a list with one (start_weekday, start_h, end_h) entry where
    end_h may be > 24 (hours from start of start_weekday).
    Returns [] if pattern not found or days are the same.
    """
    _TIME = r'(\d{4}|\d{1,2}:\d{2}|\d{1,2}\s*(?:am|pm))'
    pattern = (
        r'(?:run|from)?\s*'
        r'(\w+)\s+' + _TIME + r'\s*'
        r'(?:to|through|until|-|–|thru)\s*'
        r'(\w+)\s+' + _TIME
    )
    m = re.search(pattern, seg)
    if not m:
        return []

    day1_str, time1_str, day2_str, time2_str = (
        m.group(1), m.group(2), m.group(3), m.group(4)
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


# ── Schedule text parser ──────────────────────────────────────────────────────

def parse_schedule_text(text):
    """
    Parse plain-text schedule like:
        "Monday 6am-10pm, Tuesday 6am-2pm, Wednesday off, Thursday 6am-10pm"

    Returns
    -------
    entries    : list of (weekday_int, start_hour_int, end_hour_int)
    confidence : "high" if >= 3 days parsed, else "low"
    notes      : list of warning strings for partially-unreadable lines
    """
    entries        = []
    notes          = []
    effective_days = 0   # counts logical calendar days for confidence

    # Split on commas, semicolons, and newlines
    segments = re.split(r"[,;\n]+", text)

    # Regex that matches any time token: 0600, 06:00, 6am, 6:00am, 22, etc.
    _T = r'(\d{4}|\d{1,2}:\d{2}(?:\s*(?:am|pm))?|\d{1,2}\s*(?:am|pm))'
    time_pat = _T + r'\s*(?:[-–]|to|until|through)\s*' + _T

    for raw in segments:
        seg = raw.strip().lower()
        if not seg:
            continue

        # ── Try multi-day range first ("Run Monday 0600 to Friday 0400") ─────
        multiday = _try_multiday_range(seg)
        if multiday:
            entries.extend(multiday)
            day1    = multiday[0][0]
            duration_h  = multiday[0][2] - multiday[0][1]
            days_covered = max(1, (duration_h + 23) // 24)   # round up
            effective_days += days_covered
            day2_num = (day1 + duration_h // 24) % 7
            notes.append(
                f"  Multi-day window: {_DAY_ABBREV[day1]} {multiday[0][1]:02d}:00 "
                f"→ {_DAY_ABBREV[day2_num]} {multiday[0][2] % 24:02d}:00 "
                f"({days_covered} days)"
            )
            continue

        # ── Single-day parsing ────────────────────────────────────────────────
        # Skip lines that contain no day name
        day_match = None
        for day_word, day_int in _DAY_MAP.items():
            if re.search(r"\b" + day_word + r"\b", seg):
                day_match = (day_word, day_int)
                break
        if day_match is None:
            continue

        day_word, weekday = day_match

        # "off", "no run", "shutdown", "down", "n/a" — skip this day
        if re.search(r"\b(off|no run|shutdown|down|n/a|none)\b", seg):
            notes.append(f"  {_DAY_ABBREV[weekday]}: marked as off/no run")
            continue

        # Extract times: two time-like tokens separated by - / to / until / –
        m = re.search(time_pat, seg)
        if m:
            start_h = _parse_time(m.group(1))
            end_h   = _parse_time(m.group(2))
            if start_h is not None and end_h is not None:
                if end_h <= start_h:          # overnight window (e.g. 22:00–06:00)
                    end_h += 24
                    notes.append(f"  {_DAY_ABBREV[weekday]}: overnight window detected — "
                                 f"end adjusted to {end_h}h")
                entries.append((weekday, start_h, end_h))
                effective_days += 1
            else:
                notes.append(f"  {_DAY_ABBREV[weekday]}: found times but could not parse ('{m.group(0)}')")
        else:
            notes.append(f"  {_DAY_ABBREV[weekday]}: day found but no time range detected in: '{raw.strip()}'")

    confidence = "high" if effective_days >= 3 else "low"
    return entries, confidence, notes


# ── LLM schedule parser ───────────────────────────────────────────────────────

def parse_schedule_llm(text, api_key):
    """
    Use Claude to parse schedule text into run windows.
    Handles arbitrary natural language formats, time-first notation,
    multi-day ranges packed without commas, etc.

    Returns the same signature as parse_schedule_text:
        (entries, confidence, notes)
    where entries = list of (weekday_int, start_hour_int, end_hour_int).
    end_hour_int may exceed 24 for overnight / multi-day windows.
    """
    import anthropic as _anthropic
    import json as _json

    client = _anthropic.Anthropic(api_key=api_key)
    prompt = (
        "Parse this production run schedule into JSON run windows.\n\n"
        "Return a JSON array. Each item has:\n"
        "  weekday    : int 0–6 (Monday=0 … Sunday=6)\n"
        "  start_hour : int 0–23 (hour of day the window starts)\n"
        "  end_hour   : int, hours from midnight of start_weekday "
        "(can exceed 24 for overnight or multi-day windows)\n\n"
        "Examples:\n"
        "  'Monday 6am-10pm'              → [{\"weekday\":0,\"start_hour\":6,\"end_hour\":22}]\n"
        "  'Mon 0600 - Tue 1600'          → [{\"weekday\":0,\"start_hour\":6,\"end_hour\":34}]\n"
        "  '0600 Mon - 0400 Fri'          → [{\"weekday\":0,\"start_hour\":6,\"end_hour\":94}]\n"
        "  'mon 0600-tues 1600 Wed 0600-1600 Thurs 0600-Fri 0400'\n"
        "      → [{\"weekday\":0,\"start_hour\":6,\"end_hour\":34},\n"
        "         {\"weekday\":2,\"start_hour\":6,\"end_hour\":16},\n"
        "         {\"weekday\":3,\"start_hour\":6,\"end_hour\":46}]\n\n"
        "Omit days marked off / down / no run / shutdown.\n"
        "Return ONLY valid JSON — no explanation, no markdown fences.\n\n"
        f"Schedule text:\n{text}"
    )
    msg = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    # Strip markdown code fences if model adds them
    raw = re.sub(r'^```(?:json)?\s*|\s*```$', '', raw, flags=re.MULTILINE).strip()
    windows = _json.loads(raw)
    entries = [(int(w["weekday"]), int(w["start_hour"]), int(w["end_hour"])) for w in windows]
    distinct_days = len({e[0] for e in entries})   # unique weekday ints
    confidence    = "high" if distinct_days >= 3 else "low"
    notes = [
        f"  LLM parsed {len(entries)} window(s) across {distinct_days} day(s) — "
        f"confidence: {confidence}"
    ]
    return entries, confidence, notes


def parse_schedule(text, api_key=None):
    """
    Try LLM parsing first (if api_key provided), fall back to regex.
    Always returns (entries, confidence, notes).
    """
    if api_key:
        try:
            return parse_schedule_llm(text, api_key)
        except Exception as e:
            print(f"[schedule] LLM parse failed — using regex fallback: {e}")
    return parse_schedule_text(text)


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

def fetch_and_apply_schedule(data, dry_run=False, now_dt=None):
    """
    Read Anna's latest email, parse the schedule, and apply it.

    now_dt : optional datetime to use as "now" when computing the target week.
             Pass the simulation clock datetime so "next week" is relative to
             sim time rather than real wall-clock time.

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
    if not config or not anna:
        print("[schedule] WARN: anna_email not configured.")
        return "not_found"

    client = OutlookClient(config)
    results = client.search_inbox(sender=anna, top=5)

    if not results:
        print(f"[schedule] No emails found from {anna}.")
        return "not_found"

    # Skip the last-applied email so re-checking the inbox after a bulk
    # time advance doesn't re-use a schedule email that already belongs to
    # a previous week.
    last_id = data.get("schedule_email_id")
    if last_id:
        results = [m for m in results if m["id"] != last_id]
    if not results:
        print(f"[schedule] No new emails since last schedule application (id={last_id}).")
        return "not_found"

    # Try the most recent email first; if low confidence, try older ones.
    # Uses LLM parsing when an Anthropic API key is configured.
    best_entries, best_confidence, best_notes, best_msg = [], "low", [], None

    for msg in results:
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

    elif best_entries:
        # Low confidence but at least 1 day parsed — send alert
        subject = "VMI: Could not read schedule from Anna's email"
        body = (
            f"The VMI system found an email from {anna} but could not reliably "
            f"parse the run schedule from it (only {len(best_entries)} day(s) detected).\n\n"
            f"Subject: {best_msg['subject'] if best_msg else 'N/A'}\n"
            f"From: {best_msg['sender'] if best_msg else 'N/A'}\n\n"
            f"Parse notes:\n" + "\n".join(best_notes or ["(none)"]) + "\n\n"
            f"Please update the run schedule manually."
        )
        if dist:
            try:
                client.send_mail([dist], subject, body)
                print(f"[schedule] Low-confidence alert sent to {dist}.")
            except Exception as e:
                print(f"[schedule] WARN: could not send alert — {e}")
        return "low_confidence"

    else:
        # 0 days parsed — emails found but no schedule content detected (e.g. system emails, non-schedule messages)
        print(f"[schedule] Emails found but no schedule content detected — treating as not found.")
        return "not_found"


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
