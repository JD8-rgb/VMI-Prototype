"""Regression tests for the May-2026 review's parser/forecast bugs.

Each test names the fail-open class it prevents:
  - split-shift-dropped-silently
  - product-specific-alias-flattened
  - hardcoded-U-/M--prefix in forecast tank-membership math
"""
from __future__ import annotations

from copy import deepcopy

import pytest

from forecast import build_augmented_data
from read_schedule import parse_schedule


# ── Split-shift fail-open (review section 1) ────────────────────────────────


def test_split_shift_with_and_forces_low():
    """'Mon 6am-10am and 2pm-6pm; Tue 6am-4pm; Wed 6am-4pm' previously
    returned HIGH confidence with only the Mon-morning shift extracted
    — fail open. Must now return LOW with a split-shift note."""
    text = "Mon 6am-10am and 2pm-6pm; Tue 6am-4pm; Wed 6am-4pm"
    _entries, conf, notes, _method = parse_schedule(text, api_key=None)
    assert conf == "low", (
        f"Split shift with 'and' must demote to LOW (was HIGH in review). "
        f"Got conf={conf!r}, notes={notes}"
    )
    assert any("split shift" in n.lower() for n in notes), (
        "Expected a 'split shift' note explaining the demotion."
    )


def test_split_shift_with_ampersand_forces_low():
    """'Mon 6-10 & 2-6' uses '&' as the connector instead of 'and'."""
    _, conf, notes, _method = parse_schedule("Mon 6-10 & 2-6", api_key=None)
    assert conf == "low"
    assert any("split shift" in n.lower() for n in notes)


def test_split_shift_with_plus_word_forces_low():
    """'Mon 0600-1000 plus 1400-1800' uses 'plus' as the connector."""
    _, conf, notes, _method = parse_schedule("Mon 0600-1000 plus 1400-1800",
                                     api_key=None)
    assert conf == "low"
    assert any("split shift" in n.lower() for n in notes)


def test_two_separate_days_not_treated_as_split_shift():
    """'Monday 6am-4pm and Tuesday 6am-4pm and Wednesday 6am-4pm' is
    THREE distinct day windows, not a split shift — must stay HIGH.
    Guard against over-eager regex matching the day-connector 'and'."""
    text = ("Monday 6am-4pm and Tuesday 6am-4pm "
            "and Wednesday 6am-4pm")
    _, conf, _, _method = parse_schedule(text, api_key=None)
    assert conf == "high", (
        "Three distinct days connected by 'and' must NOT trigger "
        "split-shift detection."
    )


# ── Product-alias fail-open (review section 2) ──────────────────────────────


def test_product_alias_resin_forces_low():
    """'U resin Mon-Wed 6am-4pm; M resin Thu-Fri 6am-4pm' previously
    flattened both products' schedules into one Mon-Fri plant
    schedule with HIGH confidence. Must demote to LOW so the operator
    knows the data model has one shared plant schedule."""
    text = "U resin Mon-Wed 6am-4pm; M resin Thu-Fri 6am-4pm"
    _entries, conf, notes, _method = parse_schedule(text, api_key=None)
    assert conf == "low"
    assert any("alias" in n.lower() or "product-specific" in n.lower()
               for n in notes), (
        "Expected a product-alias note explaining the demotion."
    )


def test_product_alias_grade_letter_forces_low():
    """'grade A Mon-Wed 6am-4pm; grade B Thu-Fri 6am-4pm' is the same
    fail-open class — different operator vocabulary."""
    text = "grade A Mon-Wed 6am-4pm; grade B Thu-Fri 6am-4pm"
    _, conf, _, _method = parse_schedule(text, api_key=None)
    assert conf == "low"


def test_product_alias_material_code_forces_low():
    """'P-127 Mon 6am-4pm; P-128 Tue 6am-4pm' — material codes
    instead of product names."""
    text = "P-127 Mon 6am-4pm; P-128 Tue 6am-4pm"
    _, conf, _, _method = parse_schedule(text, api_key=None)
    assert conf == "low"


def test_plain_schedule_without_product_alias_stays_high():
    """Sanity: a clean Mon-Fri schedule with no product/material
    qualifier must still parse HIGH."""
    _, conf, _, _method = parse_schedule("Mon-Fri 6am-4pm", api_key=None)
    assert conf == "high"


# ── forecast tank membership uses product field, not name prefix ────────────


def test_forecast_uses_product_membership_not_tank_name_prefix(defaults_dict):
    """Hardcoded 'U-'/'M-' prefix in _generate_forecast_trucks meant
    any non-Acme customer (Product Acid → 'A-Tank1', Product Base →
    'B-Tank1', etc.) saw combined=0 → trigger fired every hour →
    36 stacked forecast trucks with same arrival_run_hour. Must now
    work with arbitrary tank names + product fields."""
    d = deepcopy(defaults_dict)
    d['simulation_epoch'] = '2026-05-04T00:00:00'
    d['current_run_hour'] = 0.0
    d['scheduled_trucks'] = []
    # Rename tanks so they don't start with U-/M- and re-key the
    # tanks dict — the `product` field stays the same.
    new_tanks = {}
    for name, info in d['tanks'].items():
        if name.startswith('U-'):
            new_tanks[name.replace('U-', 'Alpha-')] = info
        else:
            new_tanks[name.replace('M-', 'Bravo-')] = info
    d['tanks'] = new_tanks
    d['run_schedule'] = [{'start_hour': 6, 'end_hour': 22, 'label': 'Mon'}]

    aug, _cutoff = build_augmented_data(d, hours=288)
    fcst = [t for t in aug.get('scheduled_trucks', [])
            if str(t.get('sap_order', '')).startswith('FORECAST-')]

    # Pre-fix: with U-/M- prefix matching, both products evaluated to
    # combined=0 → trigger spammed every hour, producing many trucks
    # all stacked at the same arrival_run_hour. Post-fix: trucks are
    # paced by actual consumption + threshold logic.
    times_per_product = {}
    for t in fcst:
        times_per_product.setdefault(t['product'], []).append(
            t['arrival_run_hour'])
    for prod, times in times_per_product.items():
        # Within a single product, no two trucks at the same hour.
        # (The "already_in_flight" check should prevent stacking.)
        assert len(times) == len(set(times)), (
            f"{prod}: forecast trucks duplicated at same arrival hour: "
            f"{sorted(times)}"
        )


# ── Same-time-range fail-open (Playwright red-team, May 2026) ───────────────


def test_same_start_end_time_forces_low():
    """'Tue 6am-6am' silently became a 24-hour shift. Numerically the
    parser's most defensible reading, but just as likely an operator
    typo. Must demote to LOW with a same-time note so the operator
    confirms whether a 24h run is intended."""
    text = "Mon 6am-2pm; Tue 6am-6am"
    _entries, conf, notes, _method = parse_schedule(text, api_key=None)
    assert conf == "low", (
        f"Same start/end time must demote to LOW. Got conf={conf!r}, "
        f"notes={notes}"
    )
    assert any("same start/end" in n.lower() for n in notes), (
        "Expected a 'same start/end' note explaining the demotion."
    )


def test_same_start_end_military_forces_low():
    """'Tue 0600-0600' — same time in 4-digit military format."""
    _, conf, notes, _method = parse_schedule("Mon 6am-4pm; Tue 0600-0600",
                                     api_key=None)
    assert conf == "low"
    assert any("same start/end" in n.lower() for n in notes)


def test_same_start_end_colon_forces_low():
    """'Tue 06:00-06:00' — same time in HH:MM format."""
    _, conf, notes, _method = parse_schedule("Mon 6am-4pm; Tue 06:00-06:00",
                                     api_key=None)
    assert conf == "low"
    assert any("same start/end" in n.lower() for n in notes)


def test_overnight_window_does_not_trigger_same_time():
    """Sanity: 'Mon 10pm-6am' is a legitimate cross-midnight 8h
    window, NOT a same-time typo — must stay HIGH-eligible."""
    text = "Mon 10pm-6am; Tue 6am-4pm; Wed 6am-4pm"
    _, conf, notes, _method = parse_schedule(text, api_key=None)
    assert not any("same start/end" in n.lower() for n in notes), (
        "Overnight windows must not trip the same-time guard."
    )
    assert conf == "high"


# ── Forecast lookback: target week excluded ─────────────────────────────────


def test_forecast_excludes_target_week_from_lookback(defaults_dict):
    """Applying next week's schedule (windows whose Monday equals the
    target week's Monday) must NOT pollute the seasonal lookback. The
    target week is the prediction target — it can't also be a history
    sample without making the forecast echo whatever was just applied."""
    from forecast import _bucket_run_schedule_by_week
    from state import PlantState
    from time_utils import dt_to_run_hour
    from datetime import datetime

    d = deepcopy(defaults_dict)
    d['simulation_epoch'] = '2026-05-04T00:00:00'   # Mon 5/4
    d['current_run_hour'] = 0.0
    state = PlantState.from_dict(d)
    # Window whose Monday matches the target week (5/11) — simulating
    # the operator just applied next week's schedule.
    target_week_start = dt_to_run_hour(state, datetime(2026, 5, 11))
    from state import RunWindow
    state.run_schedule = [
        RunWindow(start_hour=-168 + 6, end_hour=-168 + 22, label="prev-Mon"),
        RunWindow(start_hour=6, end_hour=22, label="this-Mon"),
        # A window in the TARGET week (5/11) — must be filtered out:
        RunWindow(start_hour=target_week_start + 6,
                   end_hour=target_week_start + 22, label="target-Mon"),
    ]
    bucketed = _bucket_run_schedule_by_week(
        state, target_week_start_run_hour=target_week_start
    )
    target_iso = "2026-05-11"
    assert target_iso not in bucketed, (
        f"Target week ({target_iso}) leaked into lookback: {list(bucketed.keys())}"
    )
    # Other weeks should still be present
    assert "2026-04-27" in bucketed
    assert "2026-05-04" in bucketed


def test_forecast_keeps_historical_weeks_when_no_target(defaults_dict):
    """Without a target_week_start_run_hour, the bucketer keeps
    everything (legacy callers)."""
    from forecast import _bucket_run_schedule_by_week
    from state import PlantState, RunWindow

    d = deepcopy(defaults_dict)
    d['simulation_epoch'] = '2026-05-04T00:00:00'
    d['current_run_hour'] = 0.0
    state = PlantState.from_dict(d)
    state.run_schedule = [
        RunWindow(start_hour=6, end_hour=22, label="this"),
        RunWindow(start_hour=174, end_hour=190, label="next"),
    ]
    bucketed = _bucket_run_schedule_by_week(state)
    assert len(bucketed) == 2


# ── Truck NL: dynamic product matching ──────────────────────────────────────


def test_truck_nl_uses_customer_products():
    """The natural-language truck parser must accept whatever products
    the customer has configured — not hardcoded 'Product U' / 'Product M'.
    Error message also must list the configured products dynamically."""
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
    from app import _parse_nl   # noqa: E402

    # Customer with custom products: "Acid" and "Base"
    data = {
        "truck_quantities": {"Acid": 25000, "Base": 30000},
        "current_run_hour": 0.0,
        "simulation_epoch": "2026-05-04T00:00:00",
    }
    # _parse_nl returns (product, arrival_run_hour, friendly_time)
    product, _, _ = _parse_nl("Acid monday 0800", data)
    assert product == "Acid"
    product, _, _ = _parse_nl("Base wed 1400", data)
    assert product == "Base"


def test_truck_nl_error_lists_configured_products():
    """Error message for unmatched product must reference the actual
    configured products, not the hardcoded 'U / M'."""
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
    from app import _parse_nl   # noqa: E402

    data = {
        "truck_quantities": {"Acid": 25000, "Base": 30000},
        "current_run_hour": 0.0,
        "simulation_epoch": "2026-05-04T00:00:00",
    }
    with pytest.raises(ValueError) as exc:
        _parse_nl("Z monday 0800", data)
    msg = str(exc.value)
    assert "Acid" in msg and "Base" in msg, (
        f"Error must list configured products. Got: {msg!r}"
    )
    assert "'Product U'" not in msg, (
        f"Error must not hardcode legacy U/M names. Got: {msg!r}"
    )


# ── Test API: binary message (no key fingerprint or model leak) ─────────────


def test_check_anthropic_api_no_key_returns_unreachable():
    """No key → must return ('API unreachable.', no fingerprint)."""
    from read_schedule import check_anthropic_api
    ok, msg = check_anthropic_api(None)
    assert ok is False
    assert msg == "API unreachable."


def test_check_anthropic_api_messages_are_binary():
    """Diagnostic messages must be exactly 'API reachable.' or
    'API unreachable.' — never leak masked key, model name, or reply.
    The previous code rendered 'sk-ant-…XXXX accepted by claude-haiku-4-5'
    on a public Streamlit deployment (info-disclosure)."""
    from read_schedule import check_anthropic_api
    ok, msg = check_anthropic_api("")
    assert msg in ("API reachable.", "API unreachable.")
    # Defensive: even an explicit empty string mustn't leak any extra info
    forbidden = ("sk-ant", "haiku", "opus", "sonnet", "key", "Reply",
                  "reachable.", "Anthropic")
    # The literal phrase "API reachable." / "API unreachable." is allowed
    # via the membership check above; assert no other identifying content
    # appears beyond those two exact strings.
    assert msg.startswith("API "), msg
