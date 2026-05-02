"""Smoke coverage for pdf_generator.build_load_entry_pdf.

The PDF builder runs after the planner commits trucks. If it crashes
the customer-success email never gets sent and the demo flow breaks
silently. The unit tests don't need to validate the PDF content
byte-for-byte — they just need to confirm the builder produces a valid
PDF prefix without crashing on the inputs the planner actually emits.
"""

from __future__ import annotations

import pytest

from pdf_generator import build_load_entry_pdf


def _truck(sap, product, qty, run_hour):
    return {
        "sap_order":        sap,
        "product":          product,
        "quantity_lbs":     qty,
        "arrival_run_hour": run_hour,
    }


def test_build_pdf_with_one_truck(defaults_dict):
    pdf = build_load_entry_pdf(
        [_truck("SAP90001", "Product U", 33000, 8.0)],
        defaults_dict,
    )
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-")  # valid PDF magic header


def test_build_pdf_with_multiple_products(defaults_dict):
    pdf = build_load_entry_pdf(
        [
            _truck("SAP90001", "Product U", 33000, 8.0),
            _truck("SAP90002", "Product M", 37000, 14.0),
            _truck("SAP90003", "Product U", 33000, 174.0),
        ],
        defaults_dict,
    )
    assert pdf.startswith(b"%PDF-")
    assert len(pdf) > 1000   # non-trivial PDF body


def test_build_pdf_handles_three_product_customer_data():
    """Verify the builder doesn't crash on the example_customer's SAP
    format and product naming."""
    from customers import load_customer
    cfg, state = load_customer("example_customer")
    pdf = build_load_entry_pdf(
        [
            _truck("ORD-00100001", "Product Acid",     28000, 174.0),
            _truck("ORD-00100002", "Product Base",     45000, 174.0),
            _truck("ORD-00100003", "Product Catalyst", 22000, 198.0),
        ],
        state,
    )
    assert pdf.startswith(b"%PDF-")


def test_build_pdf_with_empty_truck_list_does_not_crash(defaults_dict):
    """email_hooks short-circuits before calling this when the list
    is empty, but defensive coverage is cheap."""
    pdf = build_load_entry_pdf([], defaults_dict)
    assert pdf.startswith(b"%PDF-")
