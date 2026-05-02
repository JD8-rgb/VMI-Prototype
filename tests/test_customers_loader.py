"""customers.load_customer hardening + list_customers."""

from __future__ import annotations

import json
import os

import pytest

from customers import load_customer, list_customers, _validate_id, CUSTOMERS_DIR


# ── ID validation ────────────────────────────────────────────────────────────

def test_valid_ids_accepted():
    for ok in ("example_customer", "tenant1", "ACME-corp", "a", "x_1-2"):
        _validate_id(ok)  # no exception


@pytest.mark.parametrize("bad", [
    "",
    "a/b",
    "../etc/passwd",
    "..",
    ".",
    "name with space",
    "name.json",
    "name?query=1",
    "x" * 65,           # too long
    "tenant\x00",
])
def test_invalid_ids_rejected(bad):
    with pytest.raises(ValueError):
        _validate_id(bad)


def test_load_customer_rejects_path_traversal():
    """Even if the attacker can craft a malicious file, the loader must
    reject the id before opening anything."""
    with pytest.raises(ValueError):
        load_customer("../etc/passwd")
    with pytest.raises(ValueError):
        load_customer("a/b")


# ── list_customers ────────────────────────────────────────────────────────────

def test_list_customers_includes_example():
    customers = list_customers()
    assert "example_customer" in customers


def test_list_customers_excludes_non_json():
    """README.md and __init__.py must not appear in the list."""
    customers = list_customers()
    assert "README" not in customers
    assert "__init__" not in customers


def test_list_customers_sorted():
    customers = list_customers()
    assert customers == sorted(customers)


# ── Unknown config field fails fast ──────────────────────────────────────────

def test_unknown_config_field_raises(tmp_path, monkeypatch):
    """A typo in config_overrides must fail loud, not silently drop."""
    bad = tmp_path / "typo_customer.json"
    bad.write_text(json.dumps({
        "config_overrides": {"this_field_does_not_exist": 42},
        "state": {"schema_version": 1, "tanks": {}},
    }))
    # Point CUSTOMERS_DIR at tmp_path for this test
    import customers as customers_mod
    monkeypatch.setattr(customers_mod, "CUSTOMERS_DIR", str(tmp_path))
    with pytest.raises(TypeError):
        load_customer("typo_customer")


def test_missing_state_block_returns_empty_state(tmp_path, monkeypatch):
    """A bare config_overrides file (no state) should still load — the
    technical team may use customer files purely as cfg overrides
    layered on top of a separate state source (e.g. a database)."""
    minimal = tmp_path / "cfg_only.json"
    minimal.write_text(json.dumps({
        "config_overrides": {"lead_time_hours": 96}
    }))
    import customers as customers_mod
    monkeypatch.setattr(customers_mod, "CUSTOMERS_DIR", str(tmp_path))
    cfg, state = load_customer("cfg_only")
    assert cfg.lead_time_hours == 96
    # state was empty → migrator stamped the schema version
    assert state.get("schema_version") is not None


def test_load_customer_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        load_customer("nonexistent_customer_xyz")
