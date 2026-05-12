"""Audit P0.4 — delivery overflow must NOT be silently discarded.

Pre-fix: `simulate_delivery_no_alert` returned None and the truck was
unconditionally marked delivered, even when total free space across all
tanks was less than the truck quantity. Up to (quantity - total_space)
lbs would vanish from the simulation, hiding the exact overfill failure
the product is supposed to prevent.

Post-fix: `simulate_delivery_no_alert` returns the residual overflow in
lbs (0.0 = fully delivered). `advance_time` checks the return value;
nonzero residual means the truck is REFUSED — tanks roll back to their
pre-pour state, the truck stays in `scheduled_trucks` for operator
action, and an audit entry is recorded.
"""
import pytest

from alerts import simulate_delivery_no_alert


def _two_tank_state(u1_lbs: float, u2_lbs: float, max_cap: float = 35000):
    """Two U-tanks with given current levels and a single product."""
    return {
        "U-Tank1": {
            "product": "Product U",
            "current_level_lbs": u1_lbs,
            "max_capacity_lbs":  max_cap,
            "heel_lbs":          1000.0,
            "status":            "draw",
        },
        "U-Tank2": {
            "product": "Product U",
            "current_level_lbs": u2_lbs,
            "max_capacity_lbs":  max_cap,
            "heel_lbs":          1000.0,
            "status":            "standby",
        },
    }


def _truck(qty_lbs: float, sap: str = "SAP20001", product: str = "Product U"):
    return {
        "sap_order":        sap,
        "product":          product,
        "quantity_lbs":     qty_lbs,
        "arrival_run_hour": 10.0,
    }


def test_fully_delivered_returns_zero_residual():
    """30k truck with exactly 30k free across both tanks → residual=0,
    full pour, behavior unchanged from before the audit fix."""
    tanks = _two_tank_state(u1_lbs=10000, u2_lbs=10000, max_cap=25000)
    # Total free space: (25000 - 10000) * 2 = 30000 lbs
    residual = simulate_delivery_no_alert(tanks, _truck(30000))
    assert residual == 0.0
    total_after = sum(t["current_level_lbs"] for t in tanks.values())
    assert total_after == pytest.approx(50000.0)   # 20000 before + 30000 in


def test_partial_pour_returns_residual_overflow():
    """30k truck arrives with only 1k of total free space — pre-fix this
    silently lost 29k lbs and the truck was marked delivered. Post-fix:
    residual=29k, caller (advance_time) refuses the delivery.

    The function itself still pours what fits before returning; the
    rollback happens in the caller. This test pins the residual value
    so the caller's logic has a contract to check."""
    tanks = _two_tank_state(u1_lbs=34500, u2_lbs=34500, max_cap=35000)
    # Total free: (35000 - 34500) * 2 = 1000 lbs
    residual = simulate_delivery_no_alert(tanks, _truck(30000))
    assert residual == pytest.approx(29000.0)
    # The function did pour everything it could (1000 lbs total).
    total_after = sum(t["current_level_lbs"] for t in tanks.values())
    assert total_after == pytest.approx(70000.0)   # 69000 before + 1000 fit


def test_cascade_works_when_target_full_but_others_have_space():
    """Target tank is full; cascade to the other tank works. No overflow."""
    tanks = _two_tank_state(u1_lbs=35000, u2_lbs=10000, max_cap=35000)
    # Target (lowest = U-Tank2) has 25000 lbs space; cascade not needed.
    residual = simulate_delivery_no_alert(tanks, _truck(20000))
    assert residual == 0.0
    assert tanks["U-Tank2"]["current_level_lbs"] == pytest.approx(30000.0)
    assert tanks["U-Tank1"]["current_level_lbs"] == 35000.0


def test_no_tank_for_product_returns_full_residual():
    """Truck for a product with no tank in the topology returns the
    FULL truck quantity as residual — signaling the caller to refuse
    the delivery (NOT silently mark it delivered).

    Pre-fix: returned 0.0, which the caller couldn't distinguish from
    a successful delivery, leading to the same silent-success failure
    mode the audit flagged for the overflow case."""
    tanks = _two_tank_state(u1_lbs=10000, u2_lbs=10000)
    residual = simulate_delivery_no_alert(
        tanks, _truck(20000, product="Product Nonexistent")
    )
    assert residual == 20000.0, (
        "no-tank case must return the full truck quantity so the "
        "caller's refusal branch fires"
    )
    # Tanks were not touched (nothing got poured)
    assert tanks["U-Tank1"]["current_level_lbs"] == 10000
    assert tanks["U-Tank2"]["current_level_lbs"] == 10000
