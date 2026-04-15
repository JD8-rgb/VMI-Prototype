"""
build_product_sheet.py
----------------------
Generate the one-page product sheet PDF that the Streamlit app serves from
the "Product Sheet" button at the top of the page.

Run:
    python build_product_sheet.py

Writes to:
    assets/product_sheet.pdf

This PDF describes the *production* version of the VMI Automation tool —
hourly telemetry ingest, Microsoft Graph inbox automation, LLM schedule
parsing, SAP order verification, EDI order placement — not the Streamlit
simulation. Impact benefits at the bottom are tweakable via the IMPACT_STATS
list.

Requires: reportlab (already in requirements.txt)
"""

from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    KeepInFrame,
)


# ── Palette ───────────────────────────────────────────────────────────────────

PRIMARY   = colors.HexColor("#0F1629")  # near-black navy — headers, section titles
ACCENT    = colors.HexColor("#00C7A9")  # teal — rules, icons, callouts
LIGHT_BG  = colors.HexColor("#F7F9FC")  # card backgrounds
NEUTRAL   = colors.HexColor("#2D3748")  # body text
SUBTLE    = colors.HexColor("#718096")  # subtitle / footer grey
ALERT_RED = colors.HexColor("#E53E3E")
ALERT_AMB = colors.HexColor("#ED8936")
WHITE     = colors.white


# ── Paragraph styles ──────────────────────────────────────────────────────────

TITLE_STYLE = ParagraphStyle(
    "title", fontName="Helvetica-Bold", fontSize=22,
    textColor=PRIMARY, leading=26, spaceAfter=2,
)

SUBTITLE_STYLE = ParagraphStyle(
    "subtitle", fontName="Helvetica", fontSize=10,
    textColor=SUBTLE, leading=12, spaceAfter=4,
)

SECTION_HEADER_STYLE = ParagraphStyle(
    "section_header", fontName="Helvetica-Bold", fontSize=11,
    textColor=WHITE, leading=13, leftIndent=6,
)

BODY_STYLE = ParagraphStyle(
    "body", fontName="Helvetica", fontSize=9,
    textColor=NEUTRAL, leading=12,
)

BODY_BOLD_STYLE = ParagraphStyle(
    "body_bold", fontName="Helvetica-Bold", fontSize=9,
    textColor=NEUTRAL, leading=12,
)

BULLET_STYLE = ParagraphStyle(
    "bullet", fontName="Helvetica", fontSize=9,
    textColor=NEUTRAL, leading=13, leftIndent=0,
)

STAT_NUMBER_STYLE = ParagraphStyle(
    "stat_number", fontName="Helvetica-Bold", fontSize=18,
    textColor=PRIMARY, leading=20, alignment=TA_CENTER,
)

STAT_LABEL_STYLE = ParagraphStyle(
    "stat_label", fontName="Helvetica", fontSize=8,
    textColor=SUBTLE, leading=10, alignment=TA_CENTER,
)

PILL_TITLE_STYLE = ParagraphStyle(
    "pill_title", fontName="Helvetica-Bold", fontSize=10,
    textColor=WHITE, leading=12, alignment=TA_CENTER,
)

PILL_LABEL_STYLE = ParagraphStyle(
    "pill_label", fontName="Helvetica", fontSize=8,
    textColor=WHITE, leading=10, alignment=TA_CENTER,
)

FOOTER_STYLE = ParagraphStyle(
    "footer", fontName="Helvetica-Oblique", fontSize=7,
    textColor=SUBTLE, leading=9, alignment=TA_CENTER,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def section_header(title):
    """Navy bar with white bold title and a teal left accent stripe."""
    tbl = Table(
        [[Paragraph(title, SECTION_HEADER_STYLE)]],
        colWidths=[7.5 * inch],
        rowHeights=[0.22 * inch],
    )
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), PRIMARY),
        ("LINEBEFORE", (0, 0), (0, -1), 3, ACCENT),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return tbl


def two_col_section_header(left_title, right_title, left_w, right_w):
    """Two side-by-side section bars for the WHAT IT DOES / [image area] row."""
    tbl = Table(
        [[Paragraph(left_title, SECTION_HEADER_STYLE),
          Paragraph(right_title, SECTION_HEADER_STYLE)]],
        colWidths=[left_w, right_w],
        rowHeights=[0.22 * inch],
    )
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), PRIMARY),
        ("LINEBEFORE", (0, 0), (0, -1), 3, ACCENT),
        ("LINEBEFORE", (1, 0), (1, -1), 3, ACCENT),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return tbl


def bullet_list(items):
    """Return a flowable list of Paragraphs with teal bullet dots."""
    return [
        Paragraph(f'<font color="#00C7A9">●</font>&nbsp;&nbsp;{item}', BULLET_STYLE)
        for item in items
    ]


def numbered_list(items):
    """Return a flowable list of Paragraphs with teal numbers."""
    return [
        Paragraph(
            f'<font color="#00C7A9"><b>{idx}.</b></font>&nbsp;&nbsp;{item}',
            BULLET_STYLE,
        )
        for idx, item in enumerate(items, start=1)
    ]


def alert_list(items):
    """
    items = list of (severity, text) where severity is 'red' or 'amber'.
    Returns Paragraphs with a colored dot followed by the alert label.
    """
    out = []
    for sev, text in items:
        color = "#E53E3E" if sev == "red" else "#ED8936"
        out.append(Paragraph(
            f'<font color="{color}">●</font>&nbsp;&nbsp;{text}',
            BULLET_STYLE,
        ))
    return out


def stat_tile(number, label, width):
    """A single impact stat tile — bold headline on top, small label below."""
    inner = Table(
        [[Paragraph(number, STAT_NUMBER_STYLE)],
         [Paragraph(label, STAT_LABEL_STYLE)]],
        colWidths=[width],
        rowHeights=[0.32 * inch, 0.22 * inch],
    )
    inner.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
        ("BOX", (0, 0), (-1, -1), 0.75, ACCENT),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return inner


def pill_tile(title, label, width):
    """A single AI/integration pill — white bold title + subtitle on navy bg."""
    inner = Table(
        [[Paragraph(title, PILL_TITLE_STYLE)],
         [Paragraph(label, PILL_LABEL_STYLE)]],
        colWidths=[width],
        rowHeights=[0.22 * inch, 0.22 * inch],
    )
    inner.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), PRIMARY),
        ("LINEBEFORE", (0, 0), (0, -1), 3, ACCENT),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return inner


# ── Content definitions ──────────────────────────────────────────────────────

WHY_IT_MATTERS = (
    "The supply chain team spends significant time every week managing one "
    "specific customer with a poor VMI profile: late Friday schedule emails, "
    "week-to-week volatility, frequent unplanned downtime, relatively small "
    "tanks, and shelf-life limits. This account has already absorbed real "
    "cost from returned trucks that wouldn't fit and aged-material returns — "
    "and real risk from multiple near run-outs."
)

WHAT_IT_DOES = [
    "Ingests live tank telemetry every hour",
    "Parses schedule emails with an LLM",
    "Projects 7-day levels and auto-places EDI orders",
    "Fires live alerts before problems happen",
]

WORKFLOW = [
    "Hourly telemetry update triggers a fresh projection",
    "Microsoft Graph checks the inbox for new schedules",
    "LLM parses the schedule email into run windows",
    "Planner projects demand against dynamic targets",
    "Load-entry PDF built and order placed via EDI",
]

ALERTS = [
    ("red",   "Safety-stock breach projected"),
    ("red",   "Overfill on arriving truck"),
    ("red",   "Plant running off-schedule (3+ hrs)"),
    ("amber", "Lead-time shortfall"),
    ("amber", "Low-confidence schedule parse"),
    ("amber", "No schedule received by Fri 3 PM"),
    ("amber", "Late truck (3+ hrs overdue)"),
]

DYNAMIC_TARGETS = (
    "Reorder targets scale with projected weekly run hours. Light weeks "
    "(≤ 28 run hrs) target <b>15,000 lbs</b>; heavy weeks (≥ 118 run hrs) "
    "target <b>27,000 lbs</b>; intermediate weeks interpolate linearly. "
    "This reduces shelf-life exposure in slow weeks and run-out risk when "
    "the plant ramps up."
)

# Tech stack — 4 pills
AI_STACK = [
    ("Python",          "Core platform"),
    ("LLM",             "Schedule parsing"),
    ("Microsoft Graph", "Email automation"),
    ("SAP + EDI",       "Verify &amp; order"),
]

# Impact tiles — verb-led benefits.
IMPACT_STATS = [
    ("Eliminate", "a repetitive manual task"),
    ("Reduce",    "runout &amp; overfill risk"),
    ("Simplify",  "coverage within the team"),
]


# ── Build the PDF ─────────────────────────────────────────────────────────────

def build():
    out_path = Path("assets/product_sheet.pdf")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=letter,
        leftMargin=0.5 * inch,
        rightMargin=0.5 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
        title="VMI Automation — Product Sheet",
        author="VMI Automation",
    )

    # Usable content width = 8.5 - 1 = 7.5 inches
    content_width = 7.5 * inch
    left_col_w = 3.75 * inch
    right_col_w = 3.75 * inch

    story = []

    # ── Header ───────────────────────────────────────────────────────────────
    story.append(Paragraph("VMI AUTOMATION", TITLE_STYLE))
    story.append(Paragraph(
        "Autonomous tank monitoring, schedule parsing &amp; order placement",
        SUBTITLE_STYLE,
    ))

    # Teal accent rule
    rule = Table([[""]], colWidths=[content_width], rowHeights=[2.5])
    rule.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), ACCENT),
    ]))
    story.append(rule)
    story.append(Spacer(1, 0.12 * inch))

    # ── Why it matters ───────────────────────────────────────────────────────
    story.append(section_header("WHY IT MATTERS"))
    story.append(Spacer(1, 0.04 * inch))
    why_tbl = Table(
        [[Paragraph(WHY_IT_MATTERS, BODY_STYLE)]],
        colWidths=[content_width],
    )
    why_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(why_tbl)
    story.append(Spacer(1, 0.1 * inch))

    # ── What it does (full width) ────────────────────────────────────────────
    story.append(section_header("WHAT IT DOES"))
    story.append(Spacer(1, 0.04 * inch))
    what_tbl = Table(
        [[bullet_list(WHAT_IT_DOES)]],
        colWidths=[content_width],
    )
    what_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(what_tbl)
    story.append(Spacer(1, 0.1 * inch))

    # ── Workflow | Live Alerts (two columns) ─────────────────────────────────
    story.append(two_col_section_header(
        "WORKFLOW", "LIVE ALERTS", left_col_w, right_col_w,
    ))
    story.append(Spacer(1, 0.04 * inch))

    left_b = numbered_list(WORKFLOW)
    right_b = alert_list(ALERTS)

    row_b = Table(
        [[left_b, right_b]],
        colWidths=[left_col_w, right_col_w],
    )
    row_b.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
    ]))
    story.append(row_b)
    story.append(Spacer(1, 0.1 * inch))

    # ── Dynamic target levels ────────────────────────────────────────────────
    story.append(section_header("DYNAMIC TARGET LEVELS"))
    story.append(Spacer(1, 0.04 * inch))
    targets_tbl = Table(
        [[Paragraph(DYNAMIC_TARGETS, BODY_STYLE)]],
        colWidths=[content_width],
    )
    targets_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), LIGHT_BG),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(targets_tbl)
    story.append(Spacer(1, 0.1 * inch))

    # ── Tech stack ───────────────────────────────────────────────────────────
    story.append(section_header("TECH &amp; INTEGRATION STACK"))
    story.append(Spacer(1, 0.06 * inch))

    pill_outer_w = content_width / len(AI_STACK)
    pill_inner_w = pill_outer_w - 0.15 * inch  # account for cell padding
    pills = [pill_tile(t, l, pill_inner_w) for t, l in AI_STACK]
    pill_row = Table(
        [pills],
        colWidths=[pill_outer_w] * len(AI_STACK),
    )
    pill_row.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    story.append(pill_row)
    story.append(Spacer(1, 0.04 * inch))

    # SAP clarification line
    story.append(Paragraph(
        "<font color='#718096'><i>SAP is used to verify upcoming orders; "
        "orders are placed via EDI.</i></font>",
        ParagraphStyle("note", fontName="Helvetica-Oblique", fontSize=8,
                       textColor=SUBTLE, alignment=TA_CENTER, leading=10),
    ))
    story.append(Spacer(1, 0.1 * inch))

    # ── Impact ───────────────────────────────────────────────────────────────
    story.append(section_header("IMPACT"))
    story.append(Spacer(1, 0.06 * inch))

    tile_outer_w = content_width / len(IMPACT_STATS)
    tile_inner_w = tile_outer_w - 0.2 * inch
    tiles = [stat_tile(num, lbl, tile_inner_w) for num, lbl in IMPACT_STATS]
    tile_row = Table(
        [tiles],
        colWidths=[tile_outer_w] * len(IMPACT_STATS),
    )
    tile_row.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    story.append(tile_row)
    story.append(Spacer(1, 0.14 * inch))

    # ── Footer ───────────────────────────────────────────────────────────────
    story.append(Paragraph(
        "v1.0 &nbsp;·&nbsp; VMI Automation &nbsp;·&nbsp; "
        "built in Python with LLM, Microsoft Graph, SAP and EDI integrations",
        FOOTER_STYLE,
    ))

    # Wrap in KeepInFrame so the whole sheet shrinks to fit one page.
    frame_w = content_width
    frame_h = 10.0 * inch  # usable height after margins
    framed = KeepInFrame(frame_w, frame_h, content=story, mode="shrink")

    doc.build([framed])
    print(f"[product_sheet] wrote {out_path} ({out_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    build()
