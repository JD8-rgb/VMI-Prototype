"""CLI argument parsing for plan_orders.main (HANDOFF.md Q13).

The flag exists so cron and scheduled-task callers can run the planner
without an interactive prompt. These tests cover the parser surface
only — the full main() flow needs a populated data.json and is exercised
manually."""

from __future__ import annotations

import pytest

from plan_orders import _parse_args


def test_default_no_sap_start():
    args = _parse_args([])
    assert args.sap_start is None


def test_sap_start_eq_form():
    args = _parse_args(["--sap-start=SAP20001"])
    assert args.sap_start == "SAP20001"


def test_sap_start_space_form():
    args = _parse_args(["--sap-start", "SAP20001"])
    assert args.sap_start == "SAP20001"


def test_unknown_flag_raises():
    with pytest.raises(SystemExit):
        _parse_args(["--bogus"])


def test_help_exits_cleanly():
    with pytest.raises(SystemExit) as exc:
        _parse_args(["--help"])
    assert exc.value.code == 0


def test_customer_flag():
    args = _parse_args(["--customer", "example_customer"])
    assert args.customer == "example_customer"


def test_customer_flag_eq_form():
    args = _parse_args(["--customer=example_customer"])
    assert args.customer == "example_customer"


def test_customer_default_none():
    assert _parse_args([]).customer is None


def test_customer_and_sap_start_combine():
    """Both flags can coexist for read-only customer planning runs."""
    args = _parse_args(["--customer=example_customer", "--sap-start=ORD-0001"])
    assert args.customer == "example_customer"
    assert args.sap_start == "ORD-0001"
