"""PlantConfig invariants — the curve that drives reorder targets.

If this drifts the planner will quietly under- or over-order across
every customer simultaneously."""

from __future__ import annotations

import pytest

from config import PlantConfig, DEFAULT_CONFIG


def test_target_for_week_below_low_clamps():
    cfg = DEFAULT_CONFIG
    # Way below low_run_hours floor → should clamp to low_lbs
    assert cfg.target_for_week(0) == cfg.target_low_lbs
    assert cfg.target_for_week(cfg.target_low_run_hours) == cfg.target_low_lbs


def test_target_for_week_above_high_clamps():
    cfg = DEFAULT_CONFIG
    assert cfg.target_for_week(cfg.target_high_run_hours) == cfg.target_high_lbs
    assert cfg.target_for_week(cfg.target_high_run_hours + 50) == cfg.target_high_lbs


def test_target_for_week_midpoint_interpolates():
    cfg = DEFAULT_CONFIG
    # Midpoint between low (28h, 15000lbs) and high (118h, 27000lbs) is
    # 73 run-hours → expect 21000 lbs.
    midpoint_hours = (cfg.target_low_run_hours + cfg.target_high_run_hours) / 2
    midpoint_lbs   = (cfg.target_low_lbs       + cfg.target_high_lbs)       / 2
    assert cfg.target_for_week(midpoint_hours) == pytest.approx(midpoint_lbs)


def test_target_for_week_monotone_non_decreasing():
    cfg = DEFAULT_CONFIG
    last = -1.0
    for hrs in range(0, 200, 5):
        v = cfg.target_for_week(hrs)
        assert v >= last, f"target curve regressed at {hrs}h: {v} < {last}"
        last = v


def test_dataclass_is_frozen():
    """PlantConfig is supposed to be immutable so it can be safely shared
    across threads / passed by reference without surprise mutation."""
    cfg = DEFAULT_CONFIG
    with pytest.raises(Exception):
        cfg.lead_time_hours = 999  # type: ignore[misc]


def test_per_customer_overrides_compose():
    """Confirm a customer-specific override doesn't bleed into DEFAULT_CONFIG."""
    customer = PlantConfig(lead_time_hours=72, late_truck_hours=6)
    assert customer.lead_time_hours == 72
    assert customer.late_truck_hours == 6
    # DEFAULT_CONFIG unchanged
    assert DEFAULT_CONFIG.lead_time_hours == 48


# ── __post_init__ validation ──────────────────────────────────────────────────

def test_sap_order_format_must_contain_n_placeholder():
    """A format string without {n} would issue identical SAPs for every
    truck. PlantConfig must reject this at construction."""
    with pytest.raises(ValueError) as exc:
        PlantConfig(sap_order_format="SAP-CONSTANT")
    assert "{n}" in str(exc.value) or "placeholder" in str(exc.value)


def test_sap_order_format_garbage_rejected():
    """Malformed format strings (unbalanced braces, etc.) must surface
    a clear error."""
    with pytest.raises(ValueError):
        PlantConfig(sap_order_format="SAP{{n:05d}")  # extra brace


def test_target_curve_inverted_range_rejected():
    """target_low_run_hours >= target_high_run_hours would divide-by-zero
    (or produce nonsense interpolation). Reject at construction."""
    with pytest.raises(ValueError):
        PlantConfig(target_low_run_hours=100, target_high_run_hours=50)
    with pytest.raises(ValueError):
        PlantConfig(target_low_run_hours=100, target_high_run_hours=100)


def test_valid_custom_format_accepted():
    """Sanity: legitimate overrides still construct cleanly."""
    cfg = PlantConfig(sap_order_format="ORD-{n:08d}", sap_order_seed=1000)
    assert cfg.sap_order_format.format(n=42) == "ORD-00000042"


# ── Strictly-positive scalar validation ──────────────────────────────────────

@pytest.mark.parametrize("field", [
    "safety_stock_lbs",
    "lead_time_hours",
    "late_truck_hours",
    "projection_window_hours",
    "plant_state_mismatch_hours",
    "plant_max_hours",
])
def test_strictly_positive_fields_reject_zero(field):
    with pytest.raises(ValueError) as exc:
        PlantConfig(**{field: 0})
    assert field in str(exc.value)


@pytest.mark.parametrize("field", [
    "safety_stock_lbs",
    "lead_time_hours",
    "late_truck_hours",
])
def test_strictly_positive_fields_reject_negative(field):
    with pytest.raises(ValueError) as exc:
        PlantConfig(**{field: -10})
    assert field in str(exc.value)


# ── Reorder target curve y-axis monotonicity ─────────────────────────────────

def test_inverted_target_lbs_rejected():
    """target_low_lbs > target_high_lbs would produce a downward
    interpolation curve — heavier-run weeks need less inventory, which
    is nonsense."""
    with pytest.raises(ValueError) as exc:
        PlantConfig(target_low_lbs=30_000, target_high_lbs=15_000)
    assert "target_low_lbs" in str(exc.value)


def test_equal_target_lbs_accepted():
    """A flat curve (low_lbs == high_lbs) is valid — represents a
    customer with a single fixed reorder target regardless of week
    utilization."""
    cfg = PlantConfig(target_low_lbs=20_000, target_high_lbs=20_000)
    assert cfg.target_for_week(50) == 20_000


# ── delivery_slots ────────────────────────────────────────────────────────────

def test_empty_delivery_slots_rejected():
    """An empty slot list means the planner can never propose a
    truck — silent customer-stranding bug. Reject loud."""
    with pytest.raises(ValueError) as exc:
        PlantConfig(delivery_slots=())
    assert "delivery_slots" in str(exc.value)


@pytest.mark.parametrize("bad_hour", [-1, 24, 99])
def test_out_of_range_delivery_slot_rejected(bad_hour):
    with pytest.raises(ValueError) as exc:
        PlantConfig(delivery_slots=(6, bad_hour))
    assert "delivery_slots" in str(exc.value)


# ── sap_order_seed ───────────────────────────────────────────────────────────

def test_negative_sap_seed_rejected():
    """A negative seed produces SAPs like 'SAP-0001' which collide
    with the suffix-digit regex used by _next_sap."""
    with pytest.raises(ValueError) as exc:
        PlantConfig(sap_order_seed=-1)
    assert "sap_order_seed" in str(exc.value)


def test_zero_sap_seed_accepted():
    """Zero is a legitimate starting point for some ERPs."""
    cfg = PlantConfig(sap_order_seed=0)
    assert cfg.sap_order_seed == 0
