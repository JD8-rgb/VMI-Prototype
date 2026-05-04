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
    _entries, conf, notes = parse_schedule(text, api_key=None)
    assert conf == "low", (
        f"Split shift with 'and' must demote to LOW (was HIGH in review). "
        f"Got conf={conf!r}, notes={notes}"
    )
    assert any("split shift" in n.lower() for n in notes), (
        "Expected a 'split shift' note explaining the demotion."
    )


def test_split_shift_with_ampersand_forces_low():
    """'Mon 6-10 & 2-6' uses '&' as the connector instead of 'and'."""
    _, conf, notes = parse_schedule("Mon 6-10 & 2-6", api_key=None)
    assert conf == "low"
    assert any("split shift" in n.lower() for n in notes)


def test_split_shift_with_plus_word_forces_low():
    """'Mon 0600-1000 plus 1400-1800' uses 'plus' as the connector."""
    _, conf, notes = parse_schedule("Mon 0600-1000 plus 1400-1800",
                                     api_key=None)
    assert conf == "low"
    assert any("split shift" in n.lower() for n in notes)


def test_two_separate_days_not_treated_as_split_shift():
    """'Monday 6am-4pm and Tuesday 6am-4pm and Wednesday 6am-4pm' is
    THREE distinct day windows, not a split shift — must stay HIGH.
    Guard against over-eager regex matching the day-connector 'and'."""
    text = ("Monday 6am-4pm and Tuesday 6am-4pm "
            "and Wednesday 6am-4pm")
    _, conf, _ = parse_schedule(text, api_key=None)
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
    _entries, conf, notes = parse_schedule(text, api_key=None)
    assert conf == "low"
    assert any("alias" in n.lower() or "product-specific" in n.lower()
               for n in notes), (
        "Expected a product-alias note explaining the demotion."
    )


def test_product_alias_grade_letter_forces_low():
    """'grade A Mon-Wed 6am-4pm; grade B Thu-Fri 6am-4pm' is the same
    fail-open class — different operator vocabulary."""
    text = "grade A Mon-Wed 6am-4pm; grade B Thu-Fri 6am-4pm"
    _, conf, _ = parse_schedule(text, api_key=None)
    assert conf == "low"


def test_product_alias_material_code_forces_low():
    """'P-127 Mon 6am-4pm; P-128 Tue 6am-4pm' — material codes
    instead of product names."""
    text = "P-127 Mon 6am-4pm; P-128 Tue 6am-4pm"
    _, conf, _ = parse_schedule(text, api_key=None)
    assert conf == "low"


def test_plain_schedule_without_product_alias_stays_high():
    """Sanity: a clean Mon-Fri schedule with no product/material
    qualifier must still parse HIGH."""
    _, conf, _ = parse_schedule("Mon-Fri 6am-4pm", api_key=None)
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
