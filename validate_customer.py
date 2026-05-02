"""
validate_customer.py — lint customer files in customers/.

Catches misconfigurations BEFORE they hit a runtime CLI, so the
technical team can wire this into CI:

    python validate_customer.py                  # all customers
    python validate_customer.py example_customer # one customer
    python validate_customer.py --strict         # treat warnings as errors

Exit code 0 on pass, 1 on any error (and 1 on warnings under --strict).

What it checks
--------------
1. The file loads cleanly (JSON parse, schema migrate, PlantConfig
   construction with __post_init__ validation).
2. Every product in `consumption_rates` is also present in
   `truck_quantities` and has at least one tank in `tanks`.
3. Every tank's `product` matches a product in `consumption_rates`.
4. `current_level_lbs >= 0` and `<= max_capacity_lbs` per tank
   (warning, not error — operator could have valid out-of-bounds
   data temporarily).
5. `heel_lbs <= max_capacity_lbs` per tank (error if violated —
   means the tank can never hold drawable inventory).
6. Every truck in `scheduled_trucks` references a known product.
7. Holiday strings parse as ISO dates (warning if any don't).
8. delivery_slots fall within 0..23 (error if not).
9. lead_time_hours, late_truck_hours, safety_stock_lbs > 0 (error).

The technical team can extend this with their own rules.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

from config import PlantConfig
from customers import CUSTOMERS_DIR, list_customers, load_customer


@dataclass
class Finding:
    severity: str   # "error" | "warning"
    customer: str
    field: str
    message: str

    def render(self) -> str:
        marker = "  ✗" if self.severity == "error" else "  !"
        return f"{marker} {self.customer}: [{self.field}] {self.message}"


def _check_consumption_truck_tank_consistency(
    customer: str, state: Dict[str, Any]
) -> List[Finding]:
    out: List[Finding] = []
    rates = state.get("consumption_rates", {})
    quantities = state.get("truck_quantities", {})
    tanks = state.get("tanks", {})

    products_in_rates     = set(rates.keys())
    products_in_quantity  = set(quantities.keys())
    products_in_tanks     = {t["product"] for t in tanks.values()}

    for product in products_in_rates:
        if product not in products_in_quantity:
            out.append(Finding("error", customer, "truck_quantities",
                f"product {product!r} has a consumption_rate but no "
                f"truck_quantity — planner can't propose trucks"))
        if product not in products_in_tanks:
            out.append(Finding("error", customer, "tanks",
                f"product {product!r} has a consumption_rate but no "
                f"tank — algorithms have nowhere to consume from"))

    for product in products_in_quantity - products_in_rates:
        out.append(Finding("warning", customer, "truck_quantities",
            f"product {product!r} has a truck_quantity but no "
            f"consumption_rate — orphan entry"))

    for product in products_in_tanks - products_in_rates:
        out.append(Finding("warning", customer, "tanks",
            f"product {product!r} has tanks but no consumption_rate — "
            f"orphan tanks won't drain"))

    return out


def _check_tank_levels(customer: str, state: Dict[str, Any]) -> List[Finding]:
    out: List[Finding] = []
    for name, tank in state.get("tanks", {}).items():
        level = tank.get("current_level_lbs", 0)
        cap   = tank.get("max_capacity_lbs", 0)
        heel  = tank.get("heel_lbs", 0)
        if cap <= 0:
            out.append(Finding("error", customer, f"tanks.{name}",
                f"max_capacity_lbs must be > 0, got {cap}"))
        if heel < 0:
            out.append(Finding("error", customer, f"tanks.{name}",
                f"heel_lbs must be >= 0, got {heel}"))
        if heel > cap:
            out.append(Finding("error", customer, f"tanks.{name}",
                f"heel_lbs ({heel}) > max_capacity_lbs ({cap}) — "
                f"tank can never hold drawable inventory"))
        if level < 0:
            out.append(Finding("warning", customer, f"tanks.{name}",
                f"current_level_lbs is negative ({level})"))
        if level > cap:
            out.append(Finding("warning", customer, f"tanks.{name}",
                f"current_level_lbs ({level}) > max_capacity_lbs ({cap})"))
    return out


def _check_truck_products(customer: str, state: Dict[str, Any]) -> List[Finding]:
    out: List[Finding] = []
    known_products = set(state.get("consumption_rates", {}).keys())
    for i, truck in enumerate(state.get("scheduled_trucks", [])):
        p = truck.get("product")
        if p not in known_products:
            out.append(Finding("error", customer, f"scheduled_trucks[{i}]",
                f"references unknown product {p!r} (known: "
                f"{sorted(known_products)})"))
    return out


def _check_cfg(customer: str, cfg: PlantConfig) -> List[Finding]:
    out: List[Finding] = []
    for h in cfg.delivery_slots:
        if not (0 <= h <= 23):
            out.append(Finding("error", customer, "delivery_slots",
                f"hour {h} not in 0..23"))
    for iso in cfg.plant_holidays:
        try:
            datetime.fromisoformat(iso)
        except ValueError:
            out.append(Finding("warning", customer, "plant_holidays",
                f"{iso!r} is not a valid ISO date"))
    if cfg.lead_time_hours <= 0:
        out.append(Finding("error", customer, "lead_time_hours",
            f"must be > 0, got {cfg.lead_time_hours}"))
    if cfg.late_truck_hours <= 0:
        out.append(Finding("error", customer, "late_truck_hours",
            f"must be > 0, got {cfg.late_truck_hours}"))
    if cfg.safety_stock_lbs <= 0:
        out.append(Finding("error", customer, "safety_stock_lbs",
            f"must be > 0, got {cfg.safety_stock_lbs}"))
    return out


def validate_customer(customer_id: str) -> List[Finding]:
    """Validate one customer file. Returns a list of Findings (possibly empty)."""
    try:
        cfg, state = load_customer(customer_id)
    except FileNotFoundError:
        return [Finding("error", customer_id, "<file>",
                         f"customers/{customer_id}.json not found")]
    except (ValueError, TypeError) as e:
        return [Finding("error", customer_id, "<load>", str(e))]

    findings: List[Finding] = []
    findings += _check_consumption_truck_tank_consistency(customer_id, state)
    findings += _check_tank_levels(customer_id, state)
    findings += _check_truck_products(customer_id, state)
    findings += _check_cfg(customer_id, cfg)
    return findings


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="validate_customer.py", description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("customer_ids", nargs="*",
                    help="One or more customer ids; default = every file in customers/")
    p.add_argument("--strict", action="store_true",
                    help="Treat warnings as errors")
    args = p.parse_args(argv)

    ids = args.customer_ids or list_customers()
    if not ids:
        print("No customer files found in customers/.")
        return 0

    total_errors = 0
    total_warnings = 0
    for customer_id in ids:
        findings = validate_customer(customer_id)
        errors   = [f for f in findings if f.severity == "error"]
        warnings = [f for f in findings if f.severity == "warning"]
        if findings:
            print(f"\n{customer_id}:")
            for f in findings:
                print(f.render())
        else:
            print(f"\n{customer_id}: OK")
        total_errors += len(errors)
        total_warnings += len(warnings)

    print()
    print("=" * 60)
    print(f"Checked {len(ids)} customer(s): "
          f"{total_errors} error(s), {total_warnings} warning(s)")
    print("=" * 60)

    if total_errors > 0:
        return 1
    if args.strict and total_warnings > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
