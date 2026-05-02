"""Per-customer SAP order formatting (HANDOFF.md Q5).

The default format string "SAP{n:05d}" reproduces the demo's
SAP90001-style numbers exactly. Customers with a different ERP numbering
scheme override sap_order_format / sap_order_seed in their PlantConfig
without touching algorithm code.
"""

from __future__ import annotations

from config import PlantConfig, DEFAULT_CONFIG


def test_default_format_matches_demo_string():
    cfg = DEFAULT_CONFIG
    assert cfg.sap_order_format.format(n=90_001) == "SAP90001"
    assert cfg.sap_order_format.format(n=90_002) == "SAP90002"


def test_default_seed_is_demo_starting_number():
    assert DEFAULT_CONFIG.sap_order_seed == 90_001


def test_custom_ten_digit_numeric_format():
    """A real-SAP customer wants "0000090001"-style 10-digit numerics."""
    cfg = PlantConfig(sap_order_format="{n:010d}", sap_order_seed=1)
    assert cfg.sap_order_format.format(n=1) == "0000000001"
    assert cfg.sap_order_format.format(n=42) == "0000000042"


def test_custom_prefix_format():
    """Different ERPs use different prefixes; format string accommodates."""
    cfg = PlantConfig(sap_order_format="ORD-{n:06d}", sap_order_seed=1000)
    assert cfg.sap_order_format.format(n=1000) == "ORD-001000"


def test_app_next_sap_uses_cfg(defaults_dict):
    """app._next_sap must thread the cfg's format/seed when given."""
    import app

    customer_cfg = PlantConfig(sap_order_format="ORD-{n:06d}",
                                sap_order_seed=500)
    fresh = dict(defaults_dict)
    fresh["sap_history"] = []
    fresh["scheduled_trucks"] = []
    assert app._next_sap(fresh, cfg=customer_cfg) == "ORD-000500"


def test_app_next_sap_increments_from_history(defaults_dict):
    """When history exists, seed is ignored; max+1 is the next number."""
    import app
    customer_cfg = PlantConfig(sap_order_format="X-{n:04d}", sap_order_seed=1)
    fresh = dict(defaults_dict)
    fresh["sap_history"] = ["X-0099", "X-0042"]
    fresh["scheduled_trucks"] = []
    assert app._next_sap(fresh, cfg=customer_cfg) == "X-0100"


def test_app_next_sap_default_cfg_matches_old_behavior(defaults_dict):
    """No cfg argument → defaults to DEFAULT_CONFIG; demo path unchanged."""
    import app
    fresh = dict(defaults_dict)
    fresh["sap_history"] = []
    fresh["scheduled_trucks"] = []
    # With empty history, seed=90001 → "SAP90001"
    assert app._next_sap(fresh) == "SAP90001"
