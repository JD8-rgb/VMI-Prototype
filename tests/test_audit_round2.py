"""Regression tests for Audit Round 2 findings.

Each test pins a real bug from the external audit so the fix can't
silently regress. See plan: forecast cutoff at next un-scheduled
Monday → planner-driven forecast trucks → 4 audit findings + 1
styling tweak.
"""
from __future__ import annotations

from read_schedule import (
    parse_schedule_text,
    _PRODUCT_ALIAS_RE,
    _DAY_OFF_RE,
    _validate_llm_windows,
    _windows_overlap_any,
)


# ── Fix 1 — _PRODUCT_ALIAS_RE catches 4-letter chemical-name prefixes ──────


def test_acid_feed_alias_now_matches():
    """Pre-fix: `[A-Z]{1,3}[0-9]?` rejected 4-letter 'Acid' even though
    the comment listed 'Acid feed' as a target case. Post-fix:
    `[A-Z]{1,12}[0-9]?` accepts it."""
    text = "Acid feed Mon-Wed 6am-4pm"
    assert _PRODUCT_ALIAS_RE.search(text) is not None


def test_acid_feed_two_products_forces_low():
    """End-to-end: per-product schedule via alias (Acid + Base) forces
    LOW so the operator splits manually rather than getting a silent
    flatten."""
    text = "Acid feed Mon-Wed 6am-4pm; Base feed Thu-Fri 6am-4pm"
    entries, conf, notes = parse_schedule_text(text)
    assert conf == "low"
    assert any(
        "Product-specific" in n or "alias" in n.lower() for n in notes
    ), f"Expected per-product LOW note, got: {notes}"


def test_short_alias_still_matches():
    """Sanity: existing 1-3 char codes still match (no regression)."""
    assert _PRODUCT_ALIAS_RE.search("U feed Mon 6am-4pm") is not None
    assert _PRODUCT_ALIAS_RE.search("M1 resin Tue 6-16") is not None


# ── Fix 2 — _DAY_OFF_RE single source of truth, marker-first phrasing ──────


def test_day_off_re_day_first_form():
    """Day-first: 'Thursday off' captures Thu via group(1)."""
    m = _DAY_OFF_RE.search("Mon-Fri 6am-4pm; Thursday off")
    assert m is not None
    assert (m.group(1) or m.group(2)).lower().startswith("thu")


def test_day_off_re_marker_first_form():
    """Marker-first: 'no run Thursday' captures Thu via group(2)."""
    m = _DAY_OFF_RE.search("Mon-Fri 6am-4pm; no run Thursday")
    assert m is not None
    assert (m.group(1) or m.group(2)).lower().startswith("thu")


def test_day_off_re_plant_down_form():
    """Marker-first variant: 'plant down Wednesday'."""
    m = _DAY_OFF_RE.search("plant down Wednesday")
    assert m is not None
    assert (m.group(1) or m.group(2)).lower().startswith("wed")


def test_marker_first_offday_removes_thursday_in_parser():
    """Integration check that marker-first off-days actually drop the
    weekday from regex entries. Without the unified _DAY_OFF_RE, this
    test would still pass via the parser-internal copy — but with the
    LLM-gate copy fixed too, an API-keyed run also won't re-add Thu."""
    entries, conf, notes = parse_schedule_text(
        "Mon-Fri 6am-4pm; no run Thursday"
    )
    weekdays = {wd for wd, _, _ in entries}
    assert 3 not in weekdays, (
        f"Thursday should be removed by marker-first off-day; "
        f"got entries={entries}"
    )


# ── Fix 3 — LLM window structural validation ──────────────────────────────


def test_validate_drops_out_of_range_weekday():
    windows = [(0, 6, 16), (15, 6, 16), (2, 6, 16)]   # weekday=15 is bogus
    kept, errors = _validate_llm_windows(windows)
    assert (15, 6, 16) not in kept
    assert (0, 6, 16) in kept
    assert (2, 6, 16) in kept
    assert any("weekday 15" in e for e in errors)


def test_validate_drops_negative_or_huge_hours():
    windows = [
        (0, -1, 16),    # start_hour < 0
        (1, 6, 5),      # end <= start
        (2, 25, 30),    # start_hour > 23
        (3, 6, 22),     # OK
    ]
    kept, errors = _validate_llm_windows(windows)
    assert kept == [(3, 6, 22)]
    assert len(errors) == 3


def test_validate_drops_overlong_duration_when_cfg_provided():
    """If cfg.plant_max_hours is set (e.g. 118), reject a 168h window."""
    class _Cfg:
        plant_max_hours = 118.0
    windows = [(0, 6, 174)]   # 168 hours, exceeds 118
    kept, errors = _validate_llm_windows(windows, cfg=_Cfg())
    assert kept == []
    assert any("plant_max_hours" in e for e in errors)


def test_validate_rejects_overlapping_set_entirely():
    """Overlapping windows can't be partially trusted — drop the set."""
    windows = [(0, 6, 40), (1, 6, 64)]   # Mon-Tue + Tue-Thu overlap on Tue
    kept, errors = _validate_llm_windows(windows)
    assert kept == []
    assert any("overlap" in e for e in errors)


def test_validate_keeps_valid_set():
    """Healthy LLM output passes through unchanged."""
    windows = [(0, 6, 22), (1, 6, 22), (2, 6, 14)]
    kept, errors = _validate_llm_windows(windows)
    assert kept == windows
    assert errors == []


def test_windows_overlap_any_detects_cross_day():
    """The helper itself: Mon→Tue and Tue→Thu overlap on Tuesday."""
    assert _windows_overlap_any([(0, 6, 40), (1, 6, 64)]) is True
    assert _windows_overlap_any([(0, 6, 22), (1, 6, 22)]) is False
