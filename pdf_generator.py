"""
pdf_generator.py
----------------
Build a CS load-entry PDF for a list of planned trucks.
Returns PDF as bytes (no file written to disk).

Requires: reportlab  (pip install reportlab)
"""

from datetime import datetime, timedelta
from io import BytesIO
from zoneinfo import ZoneInfo

_APP_TZ = ZoneInfo("America/New_York")

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

import time_utils

# Column header labels and widths (must be same length)
_HEADERS = ["SAP Order", "Date", "Time", "Product", "Qty (lbs)"]
_COL_WIDTHS = [1.1 * inch, 1.7 * inch, 0.7 * inch, 1.5 * inch, 1.1 * inch]

# Navy header colour
_HEADER_BG = colors.HexColor("#1F4E79")
_ALT_ROW_BG = colors.HexColor("#EBF3FB")   # light blue for alternating rows


def build_load_entry_pdf(trucks, data):
    """
    Build a CS load-entry PDF.

    Parameters
    ----------
    trucks : list of dicts
             Each dict must have: sap_order, product, quantity_lbs, arrival_run_hour
    data   : full data dict (used for run_hour -> datetime conversion)

    Returns
    -------
    bytes  -- the PDF content, ready to attach to an email
    """
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=1.0 * inch,
        bottomMargin=0.75 * inch,
    )

    styles = getSampleStyleSheet()
    story = []

    # --- Title -----------------------------------------------------------
    sorted_trucks = sorted(trucks, key=lambda t: t["arrival_run_hour"])

    if sorted_trucks:
        first_dt = time_utils.run_hour_to_dt(data, sorted_trucks[0]["arrival_run_hour"])
        # Monday of the delivery week
        week_monday = (first_dt - timedelta(days=first_dt.weekday())).strftime("%Y-%m-%d")
    else:
        week_monday = "N/A"

    generated_at = datetime.now(_APP_TZ).strftime("%Y-%m-%d %H:%M")

    story.append(Paragraph(f"<b>CS Load Entry \u2014 Week of {week_monday}</b>", styles["Title"]))
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph(f"Generated: {generated_at}", styles["Normal"]))
    story.append(Spacer(1, 0.25 * inch))

    # --- Table rows -------------------------------------------------------
    table_data = [_HEADERS]

    totals = {}   # { product: {"trucks": int, "lbs": int} }

    for truck in sorted_trucks:
        dt = time_utils.run_hour_to_dt(data, truck["arrival_run_hour"])
        product = truck["product"]
        qty = truck["quantity_lbs"]

        table_data.append([
            truck["sap_order"],
            dt.strftime("%a %Y-%m-%d"),
            dt.strftime("%H:%M"),
            product,
            f"{qty:,}",
        ])

        entry = totals.setdefault(product, {"trucks": 0, "lbs": 0})
        entry["trucks"] += 1
        entry["lbs"] += qty

    n_data = len(sorted_trucks)   # number of data rows (not counting header)

    # Blank spacer row then one summary line per product
    table_data.append(["", "", "", "", ""])
    for product in sorted(totals):
        t = totals[product]
        label = f"{t['trucks']} truck{'s' if t['trucks'] != 1 else ''}"
        table_data.append([label, "", "", f"{product} total:", f"{t['lbs']:,}"])

    # --- Table style ------------------------------------------------------
    # Alternating shading for data rows (rows 1 .. n_data, 0-indexed)
    alt_cmds = [
        ("BACKGROUND", (0, row), (-1, row), _ALT_ROW_BG)
        for row in range(2, n_data + 1, 2)   # every other data row
    ]

    style = TableStyle(
        [
            # Header row
            ("BACKGROUND",  (0, 0), (-1, 0), _HEADER_BG),
            ("TEXTCOLOR",   (0, 0), (-1, 0), colors.white),
            ("FONTNAME",    (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",    (0, 0), (-1, 0), 10),
            ("ALIGN",       (0, 0), (-1, 0), "CENTER"),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
            # Data rows
            ("FONTNAME",    (0, 1), (-1, n_data), "Helvetica"),
            ("FONTSIZE",    (0, 1), (-1, n_data), 9),
            ("TOPPADDING",  (0, 1), (-1, n_data), 5),
            ("BOTTOMPADDING", (0, 1), (-1, n_data), 5),
            # Right-align the Qty column
            ("ALIGN",       (4, 1), (4, -1), "RIGHT"),
            # Grid around data section
            ("GRID",        (0, 0), (-1, n_data), 0.5, colors.grey),
            ("LINEBELOW",   (0, 0), (-1, 0), 1.5, _HEADER_BG),
            # Summary rows (bold, no grid, slight top padding)
            ("FONTNAME",    (0, n_data + 2), (-1, -1), "Helvetica-Bold"),
            ("FONTSIZE",    (0, n_data + 2), (-1, -1), 9),
            ("ALIGN",       (3, n_data + 2), (3, -1), "RIGHT"),
            ("TOPPADDING",  (0, n_data + 1), (-1, -1), 4),
        ]
        + alt_cmds
    )

    table = Table(table_data, colWidths=_COL_WIDTHS, repeatRows=1)
    table.setStyle(style)
    story.append(table)

    doc.build(story)
    return buf.getvalue()
