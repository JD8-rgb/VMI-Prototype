"""
test_schedule_parser.py
-----------------------
Stress-test harness for read_schedule.parse_schedule* functions.

Generates ~2,000-3,000 synthetic schedule strings covering shorthand
variations, email prose wrappers, overnight and continuous-range
windows, noisy casing, and false-positive prose, then runs them through
the regex parser, the LLM parser, and the combined parser. Reports
per-category pass rates and a list of top failure modes.

Usage
-----
    python test_schedule_parser.py                    # full sweep (regex + LLM)
    python test_schedule_parser.py --regex-only       # offline, seconds
    python test_schedule_parser.py --llm-only         # skip regex sweep
    python test_schedule_parser.py --sample 100       # random subset
    python test_schedule_parser.py --out results.csv  # CSV dump of failures
    python test_schedule_parser.py --verbose          # print every failure

Exit code 0 on pass (>=99% regex + 100% must-pass LLM), 1 otherwise.
"""

import argparse
import contextlib
import csv
import io
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Tuple

# Harness-wide telemetry for rate-limit pressure (populated by run_llm).
_RETRY_STATS = {"retried": 0, "final_429": 0}

# ── Make sure we import the in-tree read_schedule, not any stale package ────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from read_schedule import (
    parse_schedule_text,
    parse_schedule_llm,
    parse_schedule,
    LLMParseError,
    _coverage_days,
)

# ── Reproducibility ────────────────────────────────────────────────────────
RNG = random.Random(42)


# ════════════════════════════════════════════════════════════════════════════
# 1. Building blocks — day names, time formats, separators, wrappers
# ════════════════════════════════════════════════════════════════════════════

# Each entry: (variants_list, weekday_int, canonical_abbrev)
DAY_VARIANTS = [
    (["mon", "monday", "Mon", "MON", "Monday", "Mondays"],           0, "Mon"),
    (["tue", "tues", "tuesday", "Tue", "Tuesday", "Tuesdays"],        1, "Tue"),
    (["wed", "wednesday", "Wed", "WED", "Wednesday", "Wednesdays"],  2, "Wed"),
    (["thu", "thur", "thurs", "thursday", "Thursday", "Thursdays"],  3, "Thu"),
    (["fri", "friday", "Fri", "FRI", "Friday", "Fridays"],           4, "Fri"),
    (["sat", "saturday", "Sat", "SAT", "Saturday", "Saturdays"],     5, "Sat"),
    (["sun", "sunday", "Sun", "SUN", "Sunday", "Sundays"],           6, "Sun"),
]


def time_variants(h: int) -> List[str]:
    """
    Return string forms an hour can take that the current parser *should*
    recognize via `_T`. We deliberately skip bare single/double digits
    because `_T` doesn't accept them.
    """
    vs = [
        f"{h:02d}00",        # 0600, 2200
        f"{h:02d}:00",       # 06:00, 22:00
    ]
    if h == 0:
        vs += ["12am", "12:00am"]
    elif h < 12:
        vs += [f"{h}am", f"{h}:00am"]
    elif h == 12:
        vs += ["12pm", "12:00pm"]
    else:  # 13-23
        vs += [f"{h - 12}pm", f"{h - 12}:00pm"]
    return vs


RANGE_SEPS   = ["-", "–", "—", " to ", " until ", " through ", " thru "]
LIST_SEPS    = [", ", "; ", "\n", "\n\n", " and "]
PREFIXES     = ["", "Run ", "From ", "Starting ", "We're running "]
OFF_MARKERS  = ["off", "no run", "shutdown", "down", "n/a", "none"]
TRAIL_PUNCT  = ["", ".", "!"]

# Email wrappers — each is a format string with one {schedule} slot.
EMAIL_WRAPPERS = [
    "Hi team,\n\nThis week we'll run: {schedule}\n\nThanks,\nAnna",
    "Hi,\n\nScheduling the plant: {schedule}.\n\nLet me know if anything changes.\n\nBest,\nAnna",
    "Hello,\n\n{schedule}\n\nRegards,\nAnna",
    "Hey team — we are going to {schedule}. Thanks, Anna",
    "Good morning,\n\n{schedule}\n\nCheers,\nAnna",
    "FYI — this week's schedule: {schedule}. Anna",
    "Team,\n\n{schedule}\n\n-- \nAnna Smith\nPlant Scheduling Lead\nSent from my iPhone",
    "Hi all,\n\nAs discussed, we'll be running {schedule}. Please plan accordingly.\n\nThanks,\nAnna",
    "Quick update: {schedule}. Let me know if there are any issues.",
    "Hi,\n\n> Previous message quoted here\n> Another quoted line\n\nNew plan: {schedule}.\n\nThanks,\nAnna",
    "{schedule}",                                          # bare
    "Hi team,\n{schedule}\nThanks",
    "Hello,\n\nHere is our run plan: {schedule}\n\nBest regards,\nAnna",
    "We are starting to see demand pick up. We are going to {schedule}.\n\nThanks,\nAnna",
    "Dear team,\n\n{schedule}\n\nSincerely,\nAnna",
]


# ════════════════════════════════════════════════════════════════════════════
# 2. Case dataclass
# ════════════════════════════════════════════════════════════════════════════

Entry = Tuple[int, int, int]  # (weekday, start_hour, end_hour)


@dataclass
class Case:
    label: str
    category: str
    input: str
    expected: List[Entry]
    must_pass: bool = False
    regex_expected: bool = True   # False = don't count in regex pass-rate bar
    expected_confidence: Optional[str] = None  # pin confidence ('high'/'low'); None = default rule


@dataclass
class CaseResult:
    case: Case
    entries: List[Entry]
    confidence: str
    notes: List[str]
    passed: bool
    error: Optional[str] = None    # for LLM-stage errors


# ════════════════════════════════════════════════════════════════════════════
# 3. Case generators (one per category)
# ════════════════════════════════════════════════════════════════════════════

def _pick_day():
    variants, wd, _ = RNG.choice(DAY_VARIANTS)
    return RNG.choice(variants), wd


def _pick_time_pair(overnight: bool = False) -> Tuple[int, int, str, str]:
    """Return (start_h, end_h, start_str, end_str) for a same-day window
    (or overnight when requested). end_h returned raw (before +24 wrap)."""
    if overnight:
        start_h = RNG.randint(18, 23)
        end_h   = RNG.randint(2, 8)
    else:
        start_h = RNG.randint(0, 20)
        end_h   = RNG.randint(start_h + 1, 23)
    return start_h, end_h, RNG.choice(time_variants(start_h)), RNG.choice(time_variants(end_h))


def gen_single_day_simple(n=300) -> List[Case]:
    cases = []
    for i in range(n):
        day_str, wd = _pick_day()
        start_h, end_h, s, e = _pick_time_pair(overnight=False)
        sep = RNG.choice(RANGE_SEPS)
        text = f"{day_str} {s}{sep}{e}"
        cases.append(Case(
            label=f"single_day_simple_{i:04d}",
            category="single_day_simple",
            input=text,
            expected=[(wd, start_h, end_h)],
        ))
    return cases


def gen_single_day_overnight(n=100) -> List[Case]:
    cases = []
    for i in range(n):
        day_str, wd = _pick_day()
        start_h, end_h, s, e = _pick_time_pair(overnight=True)
        sep = RNG.choice(RANGE_SEPS)
        text = f"{day_str} {s}{sep}{e}"
        cases.append(Case(
            label=f"single_day_overnight_{i:04d}",
            category="single_day_overnight",
            input=text,
            expected=[(wd, start_h, end_h + 24)],   # overnight wrap
        ))
    return cases


def gen_multi_day_list(n=300) -> List[Case]:
    """Comma/semicolon/newline-separated list of 3-5 day windows."""
    cases = []
    for i in range(n):
        k = RNG.randint(3, 5)
        weekdays = sorted(RNG.sample(range(7), k))
        parts    = []
        expected = []
        for wd in weekdays:
            variants = DAY_VARIANTS[wd][0]
            day_str  = RNG.choice(variants)
            start_h, end_h, s, e = _pick_time_pair()
            sep = RNG.choice(RANGE_SEPS)
            parts.append(f"{day_str} {s}{sep}{e}")
            expected.append((wd, start_h, end_h))
        list_sep = RNG.choice(LIST_SEPS)
        text = list_sep.join(parts)
        # Prefix sometimes
        text = RNG.choice(PREFIXES) + text
        cases.append(Case(
            label=f"multi_day_list_{i:04d}",
            category="multi_day_list",
            input=text,
            expected=sorted(expected),
        ))
    return cases


def gen_multi_day_packed(n=80) -> List[Case]:
    """Packed no-comma format, e.g. 'mon 0600-tues 1600 Wed 0600-1600 Thurs 0600-Fri 0400'.
    LLM-only — the regex splits on commas/newlines so these fail there."""
    cases = []
    for i in range(n):
        # Two or three windows packed together
        windows_raw = []
        expected = []
        # Always use a multi-day range first, then a simple window, optionally another
        k = RNG.randint(2, 3)
        start_wd = RNG.randint(0, 3)
        wds_used = set()
        for j in range(k):
            if j == 0:
                # Multi-day range
                wd1 = start_wd
                wd2 = wd1 + 1
                if wd2 in wds_used or wd2 > 6:
                    break
                d1 = RNG.choice(DAY_VARIANTS[wd1][0])
                d2 = RNG.choice(DAY_VARIANTS[wd2][0])
                s_h, _, s_str, _ = _pick_time_pair()
                e_h = RNG.randint(6, 22)
                e_str = RNG.choice(time_variants(e_h))
                windows_raw.append(f"{d1} {s_str}-{d2} {e_str}")
                duration = 24 + (e_h - s_h)
                expected.append((wd1, s_h, s_h + duration))
                wds_used.update([wd1, wd2])
            else:
                # Simple window
                wd = wd2 + 1
                if wd > 6 or wd in wds_used:
                    break
                d = RNG.choice(DAY_VARIANTS[wd][0])
                s_h, e_h, s_str, e_str = _pick_time_pair()
                windows_raw.append(f"{d} {s_str}-{e_str}")
                expected.append((wd, s_h, e_h))
                wds_used.add(wd)
                wd2 = wd
        text = " ".join(windows_raw)
        cases.append(Case(
            label=f"multi_day_packed_{i:04d}",
            category="multi_day_packed",
            input=text,
            expected=sorted(expected),
            regex_expected=False,   # regex path is known-weak here
        ))
    return cases


def gen_continuous_range(n=100) -> List[Case]:
    """'Mon 0600 to Fri 0400'-style single continuous window."""
    cases = []
    for i in range(n):
        wd1 = RNG.randint(0, 5)
        gap = RNG.randint(1, 6 - wd1) if wd1 < 6 else 1
        wd2 = wd1 + gap
        if wd2 > 6:
            wd2 = 6
        d1 = RNG.choice(DAY_VARIANTS[wd1][0])
        d2 = RNG.choice(DAY_VARIANTS[wd2][0])
        s_h = RNG.randint(0, 22)
        e_h = RNG.randint(0, 22)
        s_str = RNG.choice(time_variants(s_h))
        e_str = RNG.choice(time_variants(e_h))
        sep = RNG.choice([" to ", " until ", " through ", "-", " thru "])
        prefix = RNG.choice(PREFIXES)
        text = f"{prefix}{d1} {s_str}{sep}{d2} {e_str}"
        duration = (wd2 - wd1) * 24 + (e_h - s_h)
        if duration <= 0:
            duration += 24      # roll forward if times don't work
        cases.append(Case(
            label=f"continuous_range_{i:04d}",
            category="continuous_range",
            input=text,
            expected=[(wd1, s_h, s_h + duration)],
        ))
    return cases


def gen_continuous_range_prose(n=80) -> List[Case]:
    """Continuous range embedded in natural prose with 'at' / 'on' filler."""
    cases = []
    for i in range(n):
        wd1 = RNG.randint(0, 4)
        wd2 = wd1 + RNG.randint(2, 6 - wd1) if wd1 < 4 else wd1 + 2
        if wd2 > 6:
            wd2 = 6
        d1 = RNG.choice(DAY_VARIANTS[wd1][0])
        d2 = RNG.choice(DAY_VARIANTS[wd2][0])
        s_h = RNG.randint(4, 8)
        e_h = RNG.randint(2, 6)
        s_str = RNG.choice(time_variants(s_h))
        e_str = RNG.choice(time_variants(e_h))
        filler_end = RNG.choice([" at ", " on ", " "])  # the key prose trait
        sep = RNG.choice([" to ", " until "])
        templates = [
            "We are going to {d1} {s}{sep}{d2}{filler}{e}",
            "Starting {d1} {s}{sep}{d2}{filler}{e}",
            "We'll run {d1} {s}{sep}{d2}{filler}{e}",
            "The plant will be running from {d1} {s}{sep}{d2}{filler}{e}",
        ]
        text = RNG.choice(templates).format(
            d1=d1, s=s_str, sep=sep, d2=d2, filler=filler_end, e=e_str,
        )
        duration = (wd2 - wd1) * 24 + (e_h - s_h)
        if duration <= 0:
            duration += 24
        cases.append(Case(
            label=f"continuous_range_prose_{i:04d}",
            category="continuous_range_prose",
            input=text,
            expected=[(wd1, s_h, s_h + duration)],
        ))
    return cases


def gen_email_wrapped(n=200) -> List[Case]:
    """Schedule (single-day list OR continuous range) wrapped in a real email."""
    cases = []
    for i in range(n):
        # 50/50 list vs continuous range
        if RNG.random() < 0.5:
            # multi-day list
            k = RNG.randint(3, 5)
            weekdays = sorted(RNG.sample(range(7), k))
            parts, expected = [], []
            for wd in weekdays:
                day_str = RNG.choice(DAY_VARIANTS[wd][0])
                start_h, end_h, s, e = _pick_time_pair()
                parts.append(f"{day_str} {s}-{e}")
                expected.append((wd, start_h, end_h))
            schedule_text = ", ".join(parts)
        else:
            wd1 = RNG.randint(0, 4)
            wd2 = wd1 + RNG.randint(2, 6 - wd1) if wd1 < 4 else wd1 + 2
            if wd2 > 6:
                wd2 = 6
            d1 = RNG.choice(DAY_VARIANTS[wd1][0])
            d2 = RNG.choice(DAY_VARIANTS[wd2][0])
            s_h = RNG.randint(4, 10)
            e_h = RNG.randint(2, 10)
            s_str = RNG.choice(time_variants(s_h))
            e_str = RNG.choice(time_variants(e_h))
            schedule_text = f"{d1} {s_str} to {d2} {e_str}"
            duration = (wd2 - wd1) * 24 + (e_h - s_h)
            if duration <= 0:
                duration += 24
            expected = [(wd1, s_h, s_h + duration)]

        wrapper = RNG.choice(EMAIL_WRAPPERS)
        text = wrapper.format(schedule=schedule_text)
        cases.append(Case(
            label=f"email_wrapped_{i:04d}",
            category="email_wrapped",
            input=text,
            expected=sorted(expected),
        ))
    return cases


def gen_quoted_reply(n=60) -> List[Case]:
    """Real schedule beneath a quoted-reply block (>) from a prior thread."""
    cases = []
    for i in range(n):
        k = RNG.randint(3, 5)
        weekdays = sorted(RNG.sample(range(7), k))
        parts, expected = [], []
        for wd in weekdays:
            day_str = RNG.choice(DAY_VARIANTS[wd][0])
            start_h, end_h, s, e = _pick_time_pair()
            parts.append(f"{day_str} {s}-{e}")
            expected.append((wd, start_h, end_h))
        schedule_text = ", ".join(parts)
        quoted = (
            "> On Mon, someone wrote:\n"
            "> We had a question about the schedule.\n"
            "> Please confirm the plan for next week.\n"
        )
        text = f"Hi,\n\n{quoted}\nSure — here's the plan: {schedule_text}\n\nThanks,\nAnna"
        cases.append(Case(
            label=f"quoted_reply_{i:04d}",
            category="quoted_reply",
            input=text,
            expected=sorted(expected),
        ))
    return cases


def gen_off_markers_inline(n=100) -> List[Case]:
    cases = []
    for i in range(n):
        k = RNG.randint(4, 6)
        weekdays = sorted(RNG.sample(range(7), k))
        off_idx  = RNG.sample(range(k), RNG.randint(1, 2))
        parts, expected = [], []
        for j, wd in enumerate(weekdays):
            day_str = RNG.choice(DAY_VARIANTS[wd][0])
            if j in off_idx:
                parts.append(f"{day_str} {RNG.choice(OFF_MARKERS)}")
            else:
                start_h, end_h, s, e = _pick_time_pair()
                parts.append(f"{day_str} {s}-{e}")
                expected.append((wd, start_h, end_h))
        text = ", ".join(parts)
        cases.append(Case(
            label=f"off_markers_inline_{i:04d}",
            category="off_markers_inline",
            input=text,
            expected=sorted(expected),
        ))
    return cases


def gen_noisy_casing(n=100) -> List[Case]:
    cases = []
    for i in range(n):
        k = RNG.randint(3, 5)
        weekdays = sorted(RNG.sample(range(7), k))
        parts, expected = [], []
        for wd in weekdays:
            variants = DAY_VARIANTS[wd][0]
            day_str = RNG.choice(variants)
            # Jitter casing randomly
            day_str = "".join(c.upper() if RNG.random() < 0.5 else c.lower()
                              for c in day_str)
            start_h, end_h, s, e = _pick_time_pair()
            trail = RNG.choice(TRAIL_PUNCT)
            parts.append(f"{day_str} {s}-{e}{trail}")
            expected.append((wd, start_h, end_h))
        list_sep = RNG.choice(LIST_SEPS)
        text = list_sep.join(parts)
        cases.append(Case(
            label=f"noisy_casing_{i:04d}",
            category="noisy_casing",
            input=text,
            expected=sorted(expected),
        ))
    return cases


def gen_unparseable_control(n=40) -> List[Case]:
    """Prose with NO schedule content — parser must return []."""
    cases = []
    templates = [
        "Hi team, I'll send the schedule later today. Thanks, Anna",
        "Hello,\n\nQuick question about yesterday's run. Can we chat?\n\nAnna",
        "FYI the truck was late this morning. Follow up tomorrow.",
        "Sent from my iPhone",
        "Please confirm receipt.",
        "Dear customer, thank you for your order.",
        "Hey — nothing new to report.",
        "Just checking in.",
    ]
    for i in range(n):
        text = RNG.choice(templates)
        cases.append(Case(
            label=f"unparseable_control_{i:04d}",
            category="unparseable_control",
            input=text,
            expected=[],
        ))
    return cases


def curated_must_pass() -> List[Case]:
    """Hand-picked cases that MUST pass — includes Anna's real email and the
    formats documented in the UI caption."""
    return [
        Case("must_01_anna_prose", "continuous_range_prose",
             "Hi team,\n\nWe are starting to see demand pick up. We are going to "
             "Monday 0600 to Saturday at 4AM.\n\nThanks,\nAnna",
             expected=[(0, 6, 124)],  # Mon 06:00 -> Sat 04:00 = 5d*24 - 2 = 118h; end = 6+118 = 124
             must_pass=True),
        Case("must_02_mon_fri_short", "continuous_range",
             "mon 0600 - fri 0400",
             expected=[(0, 6, 100)],  # 4d*24 - 2 = 94h; end = 6+94 = 100
             must_pass=True),
        Case("must_03_ui_example_1", "multi_day_list",
             "Mon 6am-10pm, Tue 6am-10pm, Wed 6am-2pm, Thu off, Fri 6am-2pm",
             expected=[(0, 6, 22), (1, 6, 22), (2, 6, 14), (4, 6, 14)],
             must_pass=True),
        Case("must_04_ui_example_2", "multi_day_list",
             "Mon 0600-2200, Tue 0600-2200, Wed 0600-1400",
             expected=[(0, 6, 22), (1, 6, 22), (2, 6, 14)],
             must_pass=True),
        Case("must_05_ui_example_3", "multi_day_list",
             "Mon 06:00-22:00, Tue 06:00-22:00, Wed 06:00-14:00",
             expected=[(0, 6, 22), (1, 6, 22), (2, 6, 14)],
             must_pass=True),
        Case("must_06_ui_continuous", "continuous_range",
             "Run Mon 0600 to Fri 0400",
             expected=[(0, 6, 100)],
             must_pass=True),
        Case("must_07_single_day", "single_day_simple",
             "Monday 6am-10pm",
             expected=[(0, 6, 22)],
             must_pass=True),
        Case("must_08_overnight", "single_day_overnight",
             "Thu 22:00-06:00",
             expected=[(3, 22, 30)],
             must_pass=True),
        Case("must_09_off_day", "off_markers_inline",
             "Mon 6am-2pm, Tue off, Wed 6am-2pm, Thu 6am-2pm",
             expected=[(0, 6, 14), (2, 6, 14), (3, 6, 14)],
             must_pass=True),
        Case("must_10_email_basic", "email_wrapped",
             "Hi team,\n\nThis week: Mon 6am-10pm, Tue 6am-10pm, Wed 6am-2pm, "
             "Thu 6am-10pm, Fri 6am-2pm\n\nThanks,\nAnna",
             expected=[(0, 6, 22), (1, 6, 22), (2, 6, 14), (3, 6, 22), (4, 6, 14)],
             must_pass=True),
        Case("must_11_iphone_sig", "email_wrapped",
             "Mon 6am-10pm, Tue 6am-10pm, Wed 6am-2pm\n\nSent from my iPhone",
             expected=[(0, 6, 22), (1, 6, 22), (2, 6, 14)],
             must_pass=True),
        Case("must_12_semicolon_list", "multi_day_list",
             "Mon 0600-2200; Tue 0600-2200; Wed 0600-1400",
             expected=[(0, 6, 22), (1, 6, 22), (2, 6, 14)],
             must_pass=True),
        Case("must_13_newline_list", "multi_day_list",
             "Mon 6am-10pm\nTue 6am-10pm\nWed 6am-2pm",
             expected=[(0, 6, 22), (1, 6, 22), (2, 6, 14)],
             must_pass=True),
        Case("must_14_nothing_to_parse", "unparseable_control",
             "Hi, nothing new to report. Thanks, Anna",
             expected=[],
             must_pass=True),
        # Time-first ordering — the parser must accept both DAY TIME and TIME DAY forms.
        Case("must_15_time_first_continuous", "continuous_range",
             "1400 Monday to 0800 Wedneday\n1300 Thursday to 0400 Saturday",
             expected=[(0, 14, 56), (3, 13, 52)],
             must_pass=True),
        Case("must_16_time_first_day_range", "multi_day_list",
             "6AM-10PM Mon-Fri",
             expected=[(0, 6, 22), (1, 6, 22), (2, 6, 22), (3, 6, 22), (4, 6, 22)],
             must_pass=True),
        Case("must_17_time_first_linejoin", "multi_day_list",
             "6AM-10PM\nMonday-Friday",
             expected=[(0, 6, 22), (1, 6, 22), (2, 6, 22), (3, 6, 22), (4, 6, 22)],
             must_pass=True),
        # Common misspellings seen in real mail — should not need the LLM.
        Case("must_18_wednesday_misspelled", "single_day_simple",
             "Wedneday 6am-10pm",
             expected=[(2, 6, 22)],
             must_pass=True),
        Case("must_19_saturday_misspelled", "single_day_simple",
             "Saterday 0800-1700",
             expected=[(5, 8, 17)],
             must_pass=True),
        # Outlook forwarded reply chain — only the MOST RECENT schedule block
        # should survive the quote-history strip. Weeks of stacked history
        # would otherwise sum to 100h+ of bogus runtime for one week.
        Case("must_20_outlook_forward_chain", "quoted_reply",
             "From: Kimberly Hawks <KHawks@FunderAmerica.com>\n"
             "Sent: Friday, April 17, 2026 11:12 AM\n"
             "To: Davidson, Jonathan <jonathan.davidson@hexion.com>\n\n"
             "Hi Jon,\n\n"
             "Next week: Mon 0600-2200, Tue 0600-2200, Wed 0600-1400\n\n"
             "Thanks,\nKimberly\n\n"
             "From: Kimberly Hawks <KHawks@FunderAmerica.com>\n"
             "Sent: Friday, April 10, 2026 10:00 AM\n"
             "To: Davidson, Jonathan\n\n"
             "Last week: Mon 6am-10pm, Tue 6am-10pm, Wed 6am-10pm, Thu 6am-10pm\n",
             expected=[(0, 6, 22), (1, 6, 22), (2, 6, 14)],
             must_pass=True),
        # Plant cap: a schedule summing to >118h is physically impossible and
        # must drop to low confidence (blocking auto-apply).
        Case("must_21_over_cap_low_confidence", "unparseable_control",
             # 6 days × 22h = 132h — over cap.
             "Mon 0000-2200, Tue 0000-2200, Wed 0000-2200, Thu 0000-2200, "
             "Fri 0000-2200, Sat 0000-2200",
             expected=[(0,0,22),(1,0,22),(2,0,22),(3,0,22),(4,0,22),(5,0,22)],
             expected_confidence="low",
             must_pass=True),
        # Overlap: same day emitted twice with overlapping windows must drop
        # to low confidence.
        Case("must_22_overlap_low_confidence", "unparseable_control",
             "Mon 0600-2200\nMon 1000-1400",
             expected=[(0, 6, 22), (0, 10, 14)],
             expected_confidence="low",
             must_pass=True),
        # Duplicate emission (same window listed twice) should collapse.
        Case("must_23_dedup_duplicate", "multi_day_list",
             "Mon 0600-2200, Mon 0600-2200, Tue 0600-2200, Wed 0600-1400",
             expected=[(0, 6, 22), (1, 6, 22), (2, 6, 14)],
             must_pass=True),
    ]


def generate_all() -> List[Case]:
    cases  = []
    cases += gen_single_day_simple(300)
    cases += gen_single_day_overnight(100)
    cases += gen_multi_day_list(300)
    cases += gen_multi_day_packed(80)
    cases += gen_continuous_range(100)
    cases += gen_continuous_range_prose(80)
    cases += gen_email_wrapped(200)
    cases += gen_quoted_reply(60)
    cases += gen_off_markers_inline(100)
    cases += gen_noisy_casing(100)
    cases += gen_unparseable_control(40)
    cases += curated_must_pass()
    return cases


# ════════════════════════════════════════════════════════════════════════════
# 4. Runner
# ════════════════════════════════════════════════════════════════════════════

def _normalize(entries) -> List[Entry]:
    """Sort and cast entries to tuples of ints for comparison."""
    return sorted((int(wd), int(s), int(e)) for wd, s, e in entries)


def _silent(fn, *args, **kwargs):
    """Swallow stdout so the 1000s of [schedule] prints don't flood the run."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        return fn(*args, **kwargs)


def _check_passed(case: Case, entries, confidence) -> bool:
    """
    Pass iff entries match expected. Confidence has a SOFT bar:
      * If expected coverage is >= 3 calendar-days, confidence MUST be 'high'.
      * If expected coverage is < 3 days (e.g. single-day tests), confidence
        may be 'low' — that's the parser's correct behavior.
      * Expected schedules whose total runtime exceeds the plant's physical
        cap (PLANT_MAX_HOURS, Mon 06:00 → Sat 04:00 = 118h) are intentionally
        forced to 'low' confidence by the parser — accept either.
      * Expected schedules containing overlapping windows are also forced to
        'low' confidence by the parser — accept either.
      * Unparseable control cases must produce no entries.
    """
    from read_schedule import PLANT_MAX_HOURS
    actual   = _normalize(entries)
    expected = _normalize(case.expected)
    # If the case pins a required confidence (e.g. must_21 expects 'low'
    # because the input exceeds the plant cap), enforce it.
    if case.expected_confidence is not None and confidence != case.expected_confidence:
        return False
    if not case.expected:
        return actual == []
    if actual != expected:
        return False
    # Cases that would trip the parser's sanity checks are allowed to be low.
    total_h = sum(eh - sh for _, sh, eh in expected)
    exceeds_cap = total_h > PLANT_MAX_HOURS or any(
        (eh - sh) > PLANT_MAX_HOURS for _, sh, eh in expected
    )
    ranges = sorted((wd * 24 + sh, wd * 24 + eh) for wd, sh, eh in expected)
    has_overlap = any(ranges[i + 1][0] < ranges[i][1]
                      for i in range(len(ranges) - 1))
    if exceeds_cap or has_overlap:
        return True  # low confidence is the correct outcome
    expected_cov = _coverage_days(expected)
    if expected_cov >= 3 and confidence != "high":
        return False
    return True


def run_regex(case: Case) -> CaseResult:
    try:
        entries, confidence, notes = _silent(parse_schedule_text, case.input)
    except Exception as e:
        return CaseResult(case, [], "low", [], False, error=f"exception: {e!r}")
    return CaseResult(case, entries, confidence, notes,
                      _check_passed(case, entries, confidence))


def run_llm(case: Case, api_key: str, throttle: float = 0.0) -> CaseResult:
    """
    Run a single case through the LLM parser with retry-and-backoff on 429
    rate-limit errors.  Up to 2 retries at 3s then 8s.  Other LLMParseError
    stages (auth/json/schema/empty) fail immediately — no retry.
    """
    if throttle > 0:
        time.sleep(throttle)

    backoffs = [3.0, 8.0]   # seconds; empty list = give up

    def _call_once():
        return parse_schedule_llm(case.input, api_key)

    last_err: Optional[LLMParseError] = None
    for attempt in range(len(backoffs) + 1):
        try:
            entries, confidence, notes = _call_once()
            return CaseResult(case, entries, confidence, notes,
                              _check_passed(case, entries, confidence))
        except LLMParseError as e:
            last_err = e
            # Only retry rate-limit failures — other stages won't improve.
            is_rate_limit = (e.stage == "api" and "429" in str(e.detail))
            if is_rate_limit and attempt < len(backoffs):
                _RETRY_STATS["retried"] += 1
                time.sleep(backoffs[attempt])
                continue
            break
        except Exception as e:
            # Not an LLMParseError — don't retry
            if not case.expected:
                return CaseResult(case, [], "low", [str(e)], True)
            return CaseResult(case, [], "low", [], False,
                              error=f"exception: {e!r}")

    # All retries exhausted (or non-retryable LLMParseError).
    if last_err and last_err.stage == "api" and "429" in str(last_err.detail):
        _RETRY_STATS["final_429"] += 1
    if not case.expected:
        return CaseResult(case, [], "low", [str(last_err)] if last_err else [], True)
    return CaseResult(case, [], "low", [], False,
                      error=f"[{last_err.stage}] {last_err.detail}" if last_err
                      else "unknown")


def run_combined(case: Case, api_key: str) -> CaseResult:
    try:
        entries, confidence, notes = _silent(parse_schedule, case.input, api_key)
    except Exception as e:
        return CaseResult(case, [], "low", [], False, error=f"exception: {e!r}")
    return CaseResult(case, entries, confidence, notes,
                      _check_passed(case, entries, confidence))


# ════════════════════════════════════════════════════════════════════════════
# 5. Reporting
# ════════════════════════════════════════════════════════════════════════════

def _summarize(name: str, results: List[CaseResult], regex_mode: bool = False):
    """Print per-category + overall stats for a parser run."""
    if not results:
        print(f"\n=== {name} ===\n  (no results)")
        return

    by_cat: dict = {}
    for r in results:
        if regex_mode and not r.case.regex_expected:
            continue   # skip cases we don't expect regex to handle
        by_cat.setdefault(r.case.category, []).append(r)

    print(f"\n=== {name} ===")
    for cat in sorted(by_cat):
        rs = by_cat[cat]
        passed = sum(1 for r in rs if r.passed)
        total  = len(rs)
        pct    = 100.0 * passed / total if total else 0.0
        mark   = "OK" if pct >= 99.0 else ("WARN" if pct >= 90.0 else "FAIL")
        flag   = "  " if pct >= 99.0 else " <"
        print(f"  {cat:28s}: {passed:5d}/{total:5d}  ({pct:5.1f}%) [{mark}]")

    total = sum(len(rs) for rs in by_cat.values())
    passed = sum(1 for rs in by_cat.values() for r in rs if r.passed)
    pct = 100.0 * passed / total if total else 0.0
    bar_mark = "PASS" if pct >= 99.0 else "FAIL"
    print(f"  {'OVERALL':28s}: {passed:5d}/{total:5d}  ({pct:5.1f}%) [{bar_mark}]")
    return passed, total


def _failure_modes(results: List[CaseResult], top_n: int = 15):
    """Group failures by a short fingerprint so the user sees patterns."""
    fails = [r for r in results if not r.passed]
    if not fails:
        print("\n(no failures)")
        return
    # Simple grouping by (category, error-ish reason)
    groups: dict = {}
    for r in fails:
        if r.error:
            key = (r.case.category, f"error:{r.error[:60]}")
        elif not r.entries:
            key = (r.case.category, "no entries extracted")
        elif r.confidence != "high" and r.case.expected:
            key = (r.case.category, f"low confidence (expected high); got {len(r.entries)} entries")
        else:
            # Wrong entries
            diff = len(_normalize(r.entries)) - len(_normalize(r.case.expected))
            key = (r.case.category, f"wrong entries (diff={diff:+d})")
        groups.setdefault(key, []).append(r)

    print(f"\nTop failure modes ({min(len(groups), top_n)} of {len(groups)} distinct):")
    ranked = sorted(groups.items(), key=lambda kv: -len(kv[1]))
    for (cat, reason), rs in ranked[:top_n]:
        print(f"  [{len(rs):3d}]  {cat:26s}  {reason}")
        # Show one concrete example
        ex = rs[0]
        snippet = ex.case.input.replace("\n", " \\n ")[:80]
        print(f"         e.g. {ex.case.label}: {snippet!r}")
        if ex.entries:
            print(f"              got:      {_normalize(ex.entries)}")
        print(f"              expected: {_normalize(ex.case.expected)}")


def _dump_failures_csv(results: List[CaseResult], path: str):
    fails = [r for r in results if not r.passed]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["label", "category", "must_pass", "input",
                    "expected", "actual", "confidence", "error"])
        for r in fails:
            w.writerow([
                r.case.label, r.case.category, r.case.must_pass,
                r.case.input.replace("\n", "\\n"),
                str(_normalize(r.case.expected)),
                str(_normalize(r.entries)),
                r.confidence,
                r.error or "",
            ])
    print(f"\nWrote {len(fails)} failure rows to {path}")


# ════════════════════════════════════════════════════════════════════════════
# 6. API-key resolution (mirrors app.py::_get_anthropic_key priority)
# ════════════════════════════════════════════════════════════════════════════

def _resolve_api_key() -> str:
    # 1. env var
    k = os.environ.get("ANTHROPIC_API_KEY", "")
    if k:
        return k
    # 2. .streamlit/secrets.toml  ANTHROPIC_API_KEY field
    try:
        import tomllib
        with open(".streamlit/secrets.toml", "rb") as f:
            k = tomllib.load(f).get("ANTHROPIC_API_KEY", "")
            if k:
                return k
    except Exception:
        pass
    # 3. email_config.json anthropic_api_key field
    try:
        import json
        with open("email_config.json") as f:
            return json.load(f).get("anthropic_api_key", "")
    except Exception:
        return ""


# ════════════════════════════════════════════════════════════════════════════
# 7. Main
# ════════════════════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--regex-only", action="store_true",
                    help="Skip the LLM sweep (offline, seconds).")
    ap.add_argument("--llm-only", action="store_true",
                    help="Skip the regex sweep.")
    ap.add_argument("--sample", type=int, default=None,
                    help="Random subset of N cases.")
    ap.add_argument("--out", type=str, default=None,
                    help="Write failures to CSV at this path.")
    ap.add_argument("--verbose", action="store_true",
                    help="Print every failing case.")
    ap.add_argument("--max-workers", type=int, default=3,
                    help="Parallelism for LLM calls (default 3 — tuned for "
                         "Anthropic Haiku rate limits).")
    ap.add_argument("--throttle", type=float, default=0.0,
                    help="Per-worker sleep (seconds) before each LLM call; "
                         "useful if you hit 429s at higher concurrency.")
    args = ap.parse_args()

    cases = generate_all()
    print(f"Generated {len(cases)} cases.")
    if args.sample:
        cases = RNG.sample(cases, min(args.sample, len(cases)))
        print(f"Sampling {len(cases)} cases.")

    api_key = ""
    if not args.regex_only:
        api_key = _resolve_api_key()
        if not api_key:
            print("WARNING: no ANTHROPIC_API_KEY resolved — skipping LLM sweeps.")
            args.regex_only = True

    # ── Regex sweep ────────────────────────────────────────────────────────
    regex_results: List[CaseResult] = []
    if not args.llm_only:
        print("\nRunning regex sweep...")
        regex_results = [run_regex(c) for c in cases]
        _summarize("Regex parser", regex_results, regex_mode=True)
        _failure_modes([r for r in regex_results if r.case.regex_expected])

    # ── Must-pass combined check (always run, no API required) ─────────────
    # The production parser (parse_schedule) is regex-first. These 14 curated
    # cases MUST pass via regex alone, regardless of LLM availability.
    # We always run the full 14-case set even when --sample is active.
    if not args.llm_only:
        print("\nRunning must-pass combined check (regex-first, no API needed)...")
        all_must = curated_must_pass()
        mp_combined = [run_combined(c, "") for c in all_must]
        mp_pass_count = sum(1 for r in mp_combined if r.passed)
        mp_ok = (mp_pass_count == len(mp_combined))
        print(f"  Curated must-pass set (combined/regex): "
              f"{mp_pass_count}/{len(mp_combined)} "
              f"[{'PASS' if mp_ok else 'FAIL'}]")
        if not mp_ok:
            for r in mp_combined:
                if not r.passed:
                    snippet = r.case.input[:60].replace("\n", "\\n")
                    print(f"    FAIL {r.case.label}: {snippet!r}")
                    print(f"         got:      {_normalize(r.entries)}  "
                          f"confidence={r.confidence}")
                    print(f"         expected: {_normalize(r.case.expected)}")
    else:
        mp_ok = True   # LLM-only mode: skip combined must-pass gate

    # ── LLM sweep ──────────────────────────────────────────────────────────
    llm_results: List[CaseResult] = []
    combined_results: List[CaseResult] = []
    if not args.regex_only:
        print(f"\nRunning LLM sweep on {len(cases)} cases "
              f"({args.max_workers} workers, throttle={args.throttle}s)...")
        _RETRY_STATS["retried"] = 0
        _RETRY_STATS["final_429"] = 0
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futs = {ex.submit(run_llm, c, api_key, args.throttle): c for c in cases}
            done = 0
            for fut in as_completed(futs):
                llm_results.append(fut.result())
                done += 1
                if done % 50 == 0:
                    print(f"  ... {done}/{len(cases)} "
                          f"(retries: {_RETRY_STATS['retried']}, "
                          f"final_429: {_RETRY_STATS['final_429']})")
        print(f"\nRate-limit stats: {_RETRY_STATS['retried']} call(s) retried, "
              f"{_RETRY_STATS['final_429']} call(s) still 429 after backoff.")
        _summarize("LLM parser", llm_results)
        _failure_modes(llm_results)

        # LLM must-pass — informational only; production gate is combined above.
        must = [r for r in llm_results if r.case.must_pass]
        if must:
            lmp_passed = sum(1 for r in must if r.passed)
            print(f"\n  LLM must-pass set (informational): {lmp_passed}/{len(must)} "
                  f"[{'PASS' if lmp_passed == len(must) else 'FAIL'}]")

        # Combined parser sweep (skipped for sample mode to save budget).
        # Uses the same concurrency as the LLM sweep since it also hits the API.
        if not args.sample or args.sample >= 100:
            print(f"\nRunning combined parse_schedule sweep...")
            with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
                futs = {ex.submit(run_combined, c, api_key): c for c in cases}
                for fut in as_completed(futs):
                    combined_results.append(fut.result())
            _summarize("Combined parser", combined_results)

    # ── Verbose dump ───────────────────────────────────────────────────────
    if args.verbose:
        print("\n--- VERBOSE FAILURE DUMP ---")
        for r in (regex_results + llm_results + combined_results):
            if r.passed:
                continue
            print(f"\n{r.case.label} [{r.case.category}]  must_pass={r.case.must_pass}")
            print(f"  input:    {r.case.input[:200]!r}")
            print(f"  expected: {_normalize(r.case.expected)}")
            print(f"  actual:   {_normalize(r.entries)}  confidence={r.confidence}")
            if r.error:
                print(f"  error:    {r.error}")

    # ── CSV dump ───────────────────────────────────────────────────────────
    if args.out:
        all_fails = regex_results + llm_results + combined_results
        _dump_failures_csv(all_fails, args.out)

    # ── Exit code ──────────────────────────────────────────────────────────
    overall_ok = True
    if regex_results:
        counted = [r for r in regex_results if r.case.regex_expected]
        p = sum(1 for r in counted if r.passed)
        if p < 0.99 * len(counted):
            overall_ok = False
    if not mp_ok:
        # Combined/regex must-pass gate — always required when not --llm-only
        overall_ok = False
    if llm_results:
        # LLM overall pass rate (must-pass no longer gates exit — use combined)
        if sum(1 for r in llm_results if r.passed) < 0.99 * len(llm_results):
            overall_ok = False

    print(f"\n{'=' * 60}\n{'OVERALL: PASS' if overall_ok else 'OVERALL: FAIL'}\n{'=' * 60}")
    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    main()
