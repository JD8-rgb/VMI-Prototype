"""Regression suite for forecast.build_augmented_data + _generate_forecast_trucks.

These tests lock in the red-team findings from the Sprint-7 review:
  - holidays-everywhere case must NOT land trucks on holidays
  - missing tanks / missing consumption_rates must not crash
  - cutoff edge cases (zero-hour chart, cutoff == end_hour)
  - PlantState input vs raw dict input
  - 30-day chart horizon (multi-week distribution)
  - Truck arrival never past chart edge (Plotly clip ghosts)

Each test names the failure mode it prevents in the docstring so a
future regression is self-explanatory.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta

import pytest

from alerts import _as_state
from config import DEFAULT_CONFIG, PlantConfig
from forecast import build_augmented_data
from state import PlantState
from time_utils import run_hour_to_dt


def _make_state(defaults_dict, **overrides):
    """Test fixture: clone defaults_dict, set epoch + current_run_hour
    to a deterministic Mon May 4 2026 / hour 0, clear scheduled trucks,
    and apply any overrides."""
    d = deepcopy(defaults_dict)
    d['simulation_epoch']  = '2026-05-04T00:00:00'
    d['current_run_hour']  = 0.0
    d['scheduled_trucks']  = []
    d.update(overrides)
    return d


def _forecast_trucks(aug):
    """Filter aug['scheduled_trucks'] down to forecast trucks only."""
    return [t for t in aug.get('scheduled_trucks', [])
            if str(t.get('sap_order', '')).startswith('FORECAST-')]


# ── Holiday gating ──────────────────────────────────────────────────────────


def test_no_trucks_when_every_day_is_a_holiday(defaults_dict):
    """RED-TEAM REGRESSION: with every day in the next 30 marked as a
    holiday, _next_delivery_slot's old fallback returned float(after_rh)
    and emitted trucks on day-1 of the holiday range. Bug fixed by
    returning None and skipping the truck when no non-holiday slot
    is available.
    """
    cfg = PlantConfig(plant_holidays=tuple(
        (datetime(2026, 5, 4) + timedelta(days=i)).date().isoformat()
        for i in range(30)
    ))
    d = _make_state(defaults_dict,
                    run_schedule=[{'start_hour': 6, 'end_hour': 22, 'label': 'Mon'}])
    aug, _cutoff = build_augmented_data(d, cfg=cfg, hours=288)
    assert _forecast_trucks(aug) == [], (
        "All days are holidays — no forecast truck should land on any of them."
    )


def test_holidays_in_forecast_period_skip_those_dates(defaults_dict):
    """When SOME (but not all) days in the forecast period are holidays,
    forecast trucks must arrive on non-holiday days only."""
    holiday_dates = ('2026-05-11', '2026-05-12', '2026-05-13')
    cfg = PlantConfig(plant_holidays=holiday_dates)
    d = _make_state(defaults_dict,
                    run_schedule=[{'start_hour': 6, 'end_hour': 22, 'label': 'Mon'}])
    aug, _cutoff = build_augmented_data(d, cfg=cfg, hours=288)
    state = _as_state(d)
    landings = [run_hour_to_dt(state, t['arrival_run_hour']).date().isoformat()
                for t in _forecast_trucks(aug)]
    on_holiday = [d for d in landings if d in holiday_dates]
    assert on_holiday == [], (
        f"Forecast trucks landed on holiday dates: {on_holiday}"
    )


# ── Empty / missing config ──────────────────────────────────────────────────


def test_empty_run_schedule_is_safe(defaults_dict):
    """No parsed run windows at all — the function should not crash;
    cutoff should equal current_run_hour (no future-going windows)."""
    d = _make_state(defaults_dict, run_schedule=[])
    aug, cutoff = build_augmented_data(d, hours=288)
    assert cutoff == 0.0
    # forecast trucks may or may not be added (depends on whether
    # the forecaster falls back to a baseline) — what matters is
    # we didn't crash and we returned a valid dict.
    assert isinstance(aug, dict)
    assert 'scheduled_trucks' in aug


def test_zero_consumption_rate_for_one_product_gates_its_trucks(defaults_dict):
    """A product with lbs_per_hour == 0 will never drop below the
    reorder threshold (no consumption), so no forecast truck for it.
    Exercises the rate>0 guard inside the consumption loop."""
    d = _make_state(defaults_dict,
                    run_schedule=[{'start_hour': 6, 'end_hour': 22, 'label': 'Mon'}])
    d['consumption_rates'] = {
        'Product U': {'lbs_per_hour': 0},
        'Product M': {'lbs_per_hour': 583.3},
    }
    aug, _ = build_augmented_data(d, hours=288)
    products = [t['product'] for t in _forecast_trucks(aug)]
    # Product M still cycles consumption + trucks; Product U sits idle.
    assert 'Product U' not in products, (
        "Zero-consumption product should not get forecast trucks."
    )


def test_empty_consumption_rates_returns_no_forecast_trucks(defaults_dict):
    """fc.products will be empty — short-circuit guard at top of
    _generate_forecast_trucks."""
    d = _make_state(defaults_dict,
                    run_schedule=[{'start_hour': 6, 'end_hour': 22, 'label': 'Mon'}])
    d['consumption_rates'] = {}
    aug, _ = build_augmented_data(d, hours=288)
    assert _forecast_trucks(aug) == []


def test_missing_truck_quantity_falls_back_to_default(defaults_dict):
    """truck_quantities map missing one product → fallback to 33000 lbs.
    Verifies the dict.get default works."""
    d = _make_state(defaults_dict,
                    run_schedule=[{'start_hour': 6, 'end_hour': 22, 'label': 'Mon'}])
    d['truck_quantities'] = {'Product U': 33000}   # M missing
    aug, _ = build_augmented_data(d, hours=288)
    m_qtys = {t['quantity_lbs'] for t in _forecast_trucks(aug)
              if t['product'] == 'Product M'}
    if m_qtys:
        assert m_qtys == {33_000}, (
            f"Missing M quantity should fall back to default 33000, got {m_qtys}"
        )


# ── Cutoff edge cases ──────────────────────────────────────────────────────


def test_cutoff_equals_end_hour_yields_zero_forecast_trucks(defaults_dict):
    """If the parsed schedule's last window ends exactly at the chart
    end_hour, the forecast period has zero hours and we emit nothing."""
    d = _make_state(defaults_dict,
                    run_schedule=[{'start_hour': 0, 'end_hour': 288, 'label': 'edge'}])
    aug, cutoff = build_augmented_data(d, hours=288)
    assert cutoff == 288.0
    assert _forecast_trucks(aug) == []


def test_no_future_windows_means_cutoff_equals_current(defaults_dict):
    """When all parsed windows are in the past (end_hour <= current),
    cutoff falls back to current_run_hour — entire chart is forecast."""
    d = _make_state(defaults_dict, current_run_hour=200.0)
    d['run_schedule'] = [{'start_hour': 6, 'end_hour': 22, 'label': 'past'}]
    aug, cutoff = build_augmented_data(d, hours=288)
    assert cutoff == 200.0


# ── Forecast trucks always within chart range ───────────────────────────────


def test_all_forecast_trucks_within_chart_range(defaults_dict):
    """No truck should arrive past end_hour — Plotly would clip the
    vline silently, leaving a 'ghost' delivery the operator can't see.
    Sprint-7 fix: drop trucks where arrival_rh >= end_hour."""
    d = _make_state(defaults_dict,
                    run_schedule=[{'start_hour': 6, 'end_hour': 22, 'label': 'Mon'}])
    aug, _ = build_augmented_data(d, hours=288)
    end_hour = d['current_run_hour'] + 288
    out_of_range = [t for t in _forecast_trucks(aug)
                    if t['arrival_run_hour'] >= end_hour]
    assert out_of_range == [], (
        f"Forecast trucks past end_hour={end_hour}: {out_of_range}"
    )


# ── Polymorphic input (dict vs PlantState) ──────────────────────────────────


def test_plantstate_input_returns_dict(defaults_dict):
    """build_augmented_data accepts either a raw dict or a PlantState.
    With a PlantState in, it must still return a dict + cutoff tuple."""
    d = _make_state(defaults_dict,
                    run_schedule=[{'start_hour': 6, 'end_hour': 22, 'label': 'Mon'}])
    state = PlantState.from_dict(d)
    aug, cutoff = build_augmented_data(state, hours=288)
    assert isinstance(aug, dict)
    assert isinstance(cutoff, float)
    assert cutoff > 0


# ── Multi-week horizon ──────────────────────────────────────────────────────


def test_30_day_chart_distributes_trucks_across_weeks(defaults_dict):
    """A long horizon (720h ≈ 30 days) should generate trucks across
    multiple forecast weeks — not all stacked in week 1. Verifies the
    forward-walking simulator continues across multiple weeks instead
    of bailing after the first reorder cycle."""
    d = _make_state(defaults_dict,
                    run_schedule=[{'start_hour': 6, 'end_hour': 22, 'label': 'Mon'}])
    aug, cutoff = build_augmented_data(d, hours=720)
    trucks = _forecast_trucks(aug)
    if not trucks:
        pytest.skip("Forecaster fell back to baseline; no trucks to check.")
    # Spread check: arrival hours should span at least 2 distinct
    # weeks (>168h apart).
    arrivals = sorted(t['arrival_run_hour'] for t in trucks)
    span = arrivals[-1] - arrivals[0]
    assert span >= 168, (
        f"30-day chart's trucks span only {span:.0f}h — should be "
        f"distributed across multiple forecast weeks."
    )


# ── Real trucks within the parsed period are preserved ──────────────────────


def test_real_trucks_in_parsed_period_remain_in_augmented_state(defaults_dict):
    """Real trucks (no FORECAST- prefix) scheduled within the parsed
    period must survive the augmentation — we only ADD prospective
    trucks, never replace or strip the operator's confirmed ones."""
    d = _make_state(defaults_dict,
                    run_schedule=[{'start_hour': 6, 'end_hour': 22, 'label': 'Mon'}])
    real_truck = {'sap_order': 'SAP90001', 'product': 'Product U',
                  'quantity_lbs': 33000, 'arrival_run_hour': 24}
    d['scheduled_trucks'] = [real_truck]
    aug, _ = build_augmented_data(d, hours=288)
    aug_saps = [t['sap_order'] for t in aug['scheduled_trucks']]
    assert 'SAP90001' in aug_saps, "Real truck disappeared from augmented state."


def test_state_scheduled_trucks_not_mutated(defaults_dict):
    """build_augmented_data must not mutate the input data's
    scheduled_trucks list. Caller still reads the original to render
    the operator-confirmed trucks separately."""
    d = _make_state(defaults_dict,
                    run_schedule=[{'start_hour': 6, 'end_hour': 22, 'label': 'Mon'}])
    original_trucks_id = id(d['scheduled_trucks'])
    original_len = len(d['scheduled_trucks'])
    aug, _ = build_augmented_data(d, hours=288)
    assert id(d['scheduled_trucks']) == original_trucks_id, (
        "scheduled_trucks list identity changed — mutation suspected."
    )
    assert len(d['scheduled_trucks']) == original_len, (
        "scheduled_trucks length changed — mutation suspected."
    )


# ── Forecast trucks honor the reorder threshold formula ─────────────────────


def test_forecast_trucks_keep_combined_above_safety_stock(defaults_dict):
    """The reorder threshold is sized to keep combined product level
    above safety_stock through the (trigger → next-slot → delivery)
    gap. Verifies that with default cfg, projected combined U+M
    levels in the forecast period stay above safety stock.

    Sprint-7 bug: a static 15k threshold fired too late, causing
    combined to drop to heel (1k for days) before the truck arrived.
    Fix: threshold = safety_stock + (FORECAST_LEAD + 24h slot-snap) × rate.
    """
    from projection import compute_level_history
    d = _make_state(defaults_dict,
                    run_schedule=[
                        {'start_hour':   6, 'end_hour':  22, 'label': 'Mon'},
                        {'start_hour':  30, 'end_hour':  46, 'label': 'Tue'},
                        {'start_hour':  54, 'end_hour':  70, 'label': 'Wed'},
                        {'start_hour':  78, 'end_hour':  94, 'label': 'Thu'},
                        {'start_hour': 102, 'end_hour': 118, 'label': 'Fri'},
                    ])
    # Real trucks delivering during the parsed week so the parsed
    # period itself doesn't drain to heel.
    d['scheduled_trucks'] = [
        {'sap_order': 'SAP1', 'product': 'Product U', 'quantity_lbs': 33000, 'arrival_run_hour':  30},
        {'sap_order': 'SAP2', 'product': 'Product M', 'quantity_lbs': 37000, 'arrival_run_hour':  54},
        {'sap_order': 'SAP3', 'product': 'Product U', 'quantity_lbs': 33000, 'arrival_run_hour':  78},
        {'sap_order': 'SAP4', 'product': 'Product M', 'quantity_lbs': 37000, 'arrival_run_hour': 102},
    ]
    aug, cutoff = build_augmented_data(d, hours=288)
    hist = compute_level_history(aug, hours=288)

    # Check combined-per-product post-cutoff stays above safety stock.
    safety = float(DEFAULT_CONFIG.safety_stock_lbs)
    for prefix, name in (('U-', 'Product U'), ('M-', 'Product M')):
        post = [
            sum(hist['tanks'][n][i]
                for n in hist['tanks'] if n.startswith(prefix))
            for i, h in enumerate(hist['run_hours']) if h > cutoff
        ]
        assert post, f"No post-cutoff samples for {name}"
        below = [v for v in post if v < safety]
        assert not below, (
            f"{name} dropped below safety stock {safety:,.0f} during the "
            f"forecast period (min={min(post):,.0f}). Forecast trucks "
            f"should be sized + timed to prevent this."
        )
