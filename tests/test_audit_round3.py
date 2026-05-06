"""
Audit Round 3 regression tests.

Covers two interacting defects in the LLM-rescue gate that were both
visible in `read_schedule.parse_schedule()`'s validation-of-LLM-result
block:

  Defect A — under-expansion of source-text day mentions. "Mon-Fri
  06:00-16:00 CDT" only NAMES Mon and Fri; without expanding the
  range, the gate would reject Tue/Wed/Thu from a correct LLM rescue
  as "invented" days.

  Defect B — off-day mentions leaking into the "valid" mention set.
  "Mon-Wed 6am-4pm; no run Thursday" puts Thu in the raw mentioned
  weekdays, so an LLM hallucination of a Thursday window slipped
  through the invented-day check.

The fix added `_expand_text_day_ranges()` and a new off-day-intrusion
guard. Both tests mock the regex/LLM parsers to isolate the gate logic.
"""
import read_schedule as rs


def test_llm_gate_accepts_ranged_days_when_regex_failed(monkeypatch):
    """Range expansion: 'Mon-Fri ...' should be treated as mentioning
    {Mon..Fri}, not just {Mon, Fri}, so a correct 5-window LLM rescue
    is accepted instead of rejected as 'invented' on Tue/Wed/Thu."""
    text = "Run schedule: Mon-Fri 06:00-16:00 CDT next week."

    fake_llm_entries = [(d, 6, 16) for d in range(5)]   # Mon..Fri

    def fake_parse_text(t, now_dt=None):
        # Force regex to be empty/low so the LLM-rescue path runs
        return [], "low", ["forced-low for test"]

    def fake_parse_llm(t, api_key, few_shot_prefix=None, cfg=None):
        return fake_llm_entries, "high", ["LLM 95%"], fake_llm_entries

    monkeypatch.setattr(rs, "parse_schedule_text", fake_parse_text)
    monkeypatch.setattr(rs, "parse_schedule_llm",  fake_parse_llm)

    entries, conf, notes, method = rs.parse_schedule(text, api_key="test-key")

    assert method == "llm", f"expected LLM rescue accepted, got method={method}"
    assert conf == "high"
    assert {wd for wd, _, _ in entries} == {0, 1, 2, 3, 4}
    # Sanity: no "invented day" rejection note
    assert not any("invented" in n.lower() for n in notes)


def test_llm_gate_rejects_off_day_intrusion(monkeypatch):
    """Off-day guard: when source explicitly says 'no run Thursday',
    an LLM result that covers Thursday must be rejected — even though
    Thursday is textually mentioned (it's mentioned IN the off-day
    phrase). Falls back to the regex result."""
    text = "Mon-Wed 6am-4pm; no run Thursday"

    fake_llm_entries = [(0, 6, 16), (1, 6, 16), (2, 6, 16), (3, 6, 16)]
    # Thu (weekday=3) is the hallucination

    def fake_parse_text(t, now_dt=None):
        return [(0, 6, 16)], "low", []

    def fake_parse_llm(t, api_key, few_shot_prefix=None, cfg=None):
        return fake_llm_entries, "high", ["LLM 95%"], fake_llm_entries

    monkeypatch.setattr(rs, "parse_schedule_text", fake_parse_text)
    monkeypatch.setattr(rs, "parse_schedule_llm",  fake_parse_llm)

    entries, conf, notes, method = rs.parse_schedule(text, api_key="test-key")

    assert method == "regex", f"expected fallback to regex, got method={method}"
    # Result is the regex's [(Mon, 6, 16)], not the LLM's 4 windows
    assert entries == [(0, 6, 16)]
    # Note explains why the LLM was rejected
    assert any("off-day" in n.lower() for n in notes), (
        f"expected an off-day rejection note in {notes!r}"
    )


def test_expand_text_day_ranges_full_names():
    """Helper sanity: full-name DAY-DAY ranges expand inclusively."""
    assert rs._expand_text_day_ranges("Run Mon-Fri this week") == {0, 1, 2, 3, 4}
    assert rs._expand_text_day_ranges("Tuesday through Thursday") == {1, 2, 3}
    assert rs._expand_text_day_ranges("Wed-Wed") == {2}        # single-day range
    assert rs._expand_text_day_ranges("Sat-Mon") == {5, 6, 0}  # week wrap


def test_expand_text_day_ranges_letter_form():
    """Helper sanity: M-F single-letter ranges expand the same way."""
    assert rs._expand_text_day_ranges("Run M-F 6-4") == {0, 1, 2, 3, 4}
    assert rs._expand_text_day_ranges("T-Th 8am-4pm") == {1, 2, 3}


def test_expand_text_day_ranges_no_match():
    """Helper sanity: returns empty set when no range present."""
    assert rs._expand_text_day_ranges("Run Monday 6am-4pm") == set()
    assert rs._expand_text_day_ranges("") == set()
