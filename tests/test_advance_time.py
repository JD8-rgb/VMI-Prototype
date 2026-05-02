"""advance_time CLI argparse + early-return tests.

The full simulation walk is exercised by the e2e demo flow tests via
its own _advance_one_hour helper. This file just covers the argument-
parsing surface and the read-only --customer mode contract."""

from __future__ import annotations

import json

import pytest

import advance_time


def test_default_target_required():
    with pytest.raises(SystemExit):
        advance_time._parse_args([])


def test_target_positional():
    args = advance_time._parse_args(["8"])
    assert args.target == "8"
    assert args.customer is None


def test_customer_flag():
    args = advance_time._parse_args(["--customer", "example_customer", "8"])
    assert args.customer == "example_customer"
    assert args.target == "8"


def test_customer_flag_eq():
    args = advance_time._parse_args(["--customer=example_customer", "8"])
    assert args.customer == "example_customer"


def test_main_rejects_invalid_target(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    # Use defaults.json fallback (no data.json in tmp_path)
    monkeypatch.chdir(advance_time.__file__.rsplit("/", 1)[0])
    rc = advance_time.main(["not_a_number_or_date"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "Error" in out


def test_main_rejects_target_in_past(monkeypatch, capsys, tmp_path):
    """Target that's not in the future must error out instead of
    silently going backwards."""
    # Build a customer file in tmp_path with current_run_hour=100 so we
    # can pass target=50 (past) and confirm the error
    monkeypatch.chdir(tmp_path)
    state = {
        "schema_version": 1, "simulation_epoch": "2026-04-13T00:00:00",
        "current_run_hour": 100.0,
        "tanks": {}, "consumption_rates": {}, "truck_quantities": {},
        "scheduled_trucks": [], "run_schedule": [],
    }
    customers_dir = tmp_path / "customers"
    customers_dir.mkdir()
    (customers_dir / "past.json").write_text(json.dumps({
        "config_overrides": {}, "state": state,
    }))
    import customers as customers_mod
    monkeypatch.setattr(customers_mod, "CUSTOMERS_DIR", str(customers_dir))
    rc = advance_time.main(["--customer", "past", "0"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "not in the future" in out


def test_customer_mode_does_not_save(monkeypatch, tmp_path, capsys):
    """In --customer mode, save_data must NOT be called even after a
    successful walk."""
    monkeypatch.chdir(tmp_path)
    state = {
        "schema_version": 1, "simulation_epoch": "2026-04-13T00:00:00",
        "current_run_hour": 0.0,
        "tanks": {"T1": {"product": "X", "current_level_lbs": 10000,
                          "max_capacity_lbs": 30000, "heel_lbs": 1000,
                          "status": "draw"}},
        "consumption_rates": {"X": {"lbs_per_hour": 100.0}},
        "truck_quantities": {"X": 25000},
        "scheduled_trucks": [], "run_schedule": [],
    }
    customers_dir = tmp_path / "customers"
    customers_dir.mkdir()
    (customers_dir / "ro.json").write_text(json.dumps({
        "config_overrides": {}, "state": state,
    }))
    import customers as customers_mod
    monkeypatch.setattr(customers_mod, "CUSTOMERS_DIR", str(customers_dir))

    save_calls = []
    import data_io
    monkeypatch.setattr(data_io, "save_data",
                          lambda *a, **k: save_calls.append((a, k)))
    import email_hooks
    monkeypatch.setattr(email_hooks, "send_alert_emails_if_new",
                          lambda data: data)

    rc = advance_time.main(["--customer", "ro", "1"])
    assert rc == 0
    assert save_calls == [], "save_data must NOT be called in --customer mode"
    out = capsys.readouterr().out
    assert "Read-only" in out
