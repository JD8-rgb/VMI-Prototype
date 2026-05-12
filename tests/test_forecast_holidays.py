"""Audit P0.6: forecast fallback honors cfg.plant_holidays.

The weighted-seasonal path always respected holidays. The static
`_fallback_baseline` (used when there's not enough history) used to
spread heavy-week hours evenly across Mon-Fri regardless of holidays —
which produced bogus run-hour predictions on holiday weeks.

These tests pin the fixed behavior: target-week dates that fall on a
holiday get zeroed out, and the remaining weekdays absorb the load.
"""
from datetime import datetime, timedelta

from config import PlantConfig
from forecast import forecast


def _state_without_history(epoch_iso: str = "2026-05-04T00:00:00"):
    """Minimal state with NO level_history and NO run_schedule so the
    weighted seasonal path bails out and the fallback fires."""
    from state import PlantState, TankState, ProductRate
    return PlantState(
        simulation_epoch=epoch_iso,
        current_run_hour=0.0,
        tanks={
            "T1": TankState(
                product="P1",
                current_level_lbs=20000.0,
                max_capacity_lbs=35000.0,
                heel_lbs=1000.0,
                status="draw",
            ),
        },
        consumption_rates={"P1": ProductRate(lbs_per_hour=500.0)},
        truck_quantities={"P1": 30000.0},
        scheduled_trucks=[],
        run_schedule=[],
    )


def test_fallback_holiday_weekday_gets_zero_hours():
    """Mark target-week Tuesday as a holiday; with no history forcing
    the static fallback, Tuesday's forecast should be 0 hours and the
    remaining 4 weekdays should absorb the heavy-week target."""
    state = _state_without_history()
    # Target week starts Mon May 11 → Tuesday is May 12.
    target_week_start = 7 * 24.0     # Mon at week 2
    holiday_date = (
        datetime(2026, 5, 4) + timedelta(days=7 + 1)   # Tue May 12
    ).date().isoformat()
    cfg = PlantConfig(plant_holidays=(holiday_date,))

    result = forecast(state, target_week_start_run_hour=target_week_start, cfg=cfg)

    assert len(result.products) == 1
    by_wd = result.products[0].by_weekday
    # Tuesday (weekday=1) is zeroed out
    assert by_wd[1]["run_hours"] == 0.0
    assert by_wd[1]["lbs"] == 0.0
    # The other 4 weekdays (Mon, Wed, Thu, Fri) absorb the load —
    # each gets target_high_run_hours / 4 (since 4 non-holiday weekdays).
    expected_per_day = float(cfg.target_high_run_hours) / 4.0
    for wd in (0, 2, 3, 4):
        assert by_wd[wd]["run_hours"] == expected_per_day, (
            f"weekday={wd}: expected {expected_per_day} h, got {by_wd[wd]['run_hours']}"
        )
    # Weekend stays zero
    assert by_wd[5]["run_hours"] == 0.0
    assert by_wd[6]["run_hours"] == 0.0


def test_fallback_no_holidays_spreads_across_five_weekdays():
    """No holidays in target week → original behavior preserved:
    target_high_run_hours evenly across Mon-Fri."""
    state = _state_without_history()
    cfg = PlantConfig(plant_holidays=())   # no holidays

    result = forecast(state, target_week_start_run_hour=7 * 24.0, cfg=cfg)
    by_wd = result.products[0].by_weekday
    expected = float(cfg.target_high_run_hours) / 5.0
    for wd in range(5):
        assert by_wd[wd]["run_hours"] == expected, f"weekday {wd}"
    for wd in (5, 6):
        assert by_wd[wd]["run_hours"] == 0.0


def test_fallback_all_weekdays_holiday_zeros_week():
    """Pathological edge case: all 5 weekdays are holidays. Forecast
    zeros out — no division-by-zero, no negative hours."""
    state = _state_without_history()
    target_week_start = 7 * 24.0
    target_monday = datetime(2026, 5, 4) + timedelta(days=7)
    all_holidays = tuple(
        (target_monday + timedelta(days=d)).date().isoformat()
        for d in range(5)
    )
    cfg = PlantConfig(plant_holidays=all_holidays)

    result = forecast(state, target_week_start_run_hour=target_week_start, cfg=cfg)
    by_wd = result.products[0].by_weekday
    for wd in range(7):
        assert by_wd[wd]["run_hours"] == 0.0
        assert by_wd[wd]["lbs"] == 0.0
    assert result.products[0].weekly_run_hours == 0.0
    assert result.products[0].weekly_lbs == 0.0
