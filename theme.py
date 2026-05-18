"""
theme.py — VMI design system.

Single source of truth for design tokens (colors / typography / spacing /
radii / shadows / motion) and component CSS classes used across app.py.
Injected once at app startup via inject_theme().

Reflects the Claude Design handoff (`design-system-handoff/`):
  - Single-blue identity: `#1E40AF` for brand, primary actions, h3 anchor,
    sim-pill edge, run-window chart shading.
  - 3px blue left-border on every h3 (the signature gesture).
  - Inter for UI, JetBrains Mono for numerics with tabular figures.
  - No drop shadows on cards; one exception is the blue-glow primary
    button hover.
  - Light only.
  - Emoji is the icon set (🚨 📈 🔮 📅 🤖 📊 🎛️ 🚛 🏭 🎬 🧪 ↻ ▶ 💻).

Reversibility: deleting this module + the one inject_theme() call in
app.py reverts every visual change. No algorithm code touched.
"""

from __future__ import annotations


# ── Design tokens (single source of truth) ───────────────────────────────────

TOKENS = {
    # ── Surfaces — Microsoft Fluent palette (bg-app kept at the operator's
    # explicit choice from the prior contrast pass)
    "bg-app":         "#D5DCE3",
    "bg-app-warm":    "#F7F3EC",
    "bg-card":        "#FFFFFF",
    "bg-subtle":      "#F3F2F1",
    "border":         "#E1DFDD",
    "border-strong":  "#C8C6C4",
    # Section-header band (dark navy / white text) for above-list headers.
    # Defined as a token; applied via the `.vmi-section-header` utility
    # class (Phase B+ wires it into Alerts / Trucks / Parse Review).
    "bg-section":     "#1F2A44",
    "fg-section":     "#FFFFFF",

    # ── Text — Microsoft neutralPrimary / secondary
    "text-primary":   "#323130",
    "text-headline":  "#323130",
    "text-body":      "#323130",
    "text-secondary": "#605E5C",
    "text-meta":      "#605E5C",
    "text-muted":     "#8A8886",

    # ── Brand + action — Microsoft Action Blue
    "accent":         "#0078D4",
    "accent-hover":   "#106EBE",
    "accent-bg":      "#DEECF9",
    "accent-fg":      "#0078D4",
    "action":         "#0078D4",
    "action-hover":   "#106EBE",
    "action-shadow":  "rgba(0, 120, 212, 0.25)",

    # ── Semantic — success (Microsoft success green)
    "success":     "#107C10",
    "success-bg":  "#DFF6DD",
    "success-fg":  "#0B5A0B",
    # ── Semantic — warning (Fluent yellow; FG kept dark for legibility
    # over the pale #FFF4CE tint — Microsoft's own recommended pairing)
    "warning":     "#FFB900",
    "warning-bg":  "#FFF4CE",
    "warning-fg":  "#323130",
    # ── Semantic — danger (Microsoft red)
    "danger":      "#D13438",
    "danger-bg":   "#FDE7E9",
    "danger-fg":   "#A4262C",
    # ── Semantic — info (re-uses the Fluent action blue)
    "info":        "#0078D4",
    "info-bg":     "#DEECF9",
    "info-fg":     "#004578",

    # ── Tank-status chip palette — re-aligned to Fluent so chip colors
    # match the rest of the chrome
    "draw-bg":      "#DEECF9",  # Fluent info tint
    "draw-fg":      "#0078D4",
    "standby-bg":   "#F3F2F1",  # Fluent neutral
    "standby-fg":   "#605E5C",
    "receiving-bg": "#DFF6DD",  # Fluent success tint
    "receiving-fg": "#0B5A0B",

    # ── Tank-fill semantic (chart + SVG — chart-internal use only,
    # not part of the Fluent UI chrome, kept for tank-level rendering)
    "fill-critical":  "#F43F5E",   # < 20%
    "fill-low":       "#F59E0B",   # < 50%
    "fill-healthy":   "#0EA5E9",   # ≥ 50%
    "fill-receiving": "#22C55E",

    # ── Chart palette (Plotly traces — chart-only, no chrome use)
}


# Customer-agnostic chart palette. Indexed by tank-position-in-customer
# rather than by hard-coded tank name, so customers with topologies other
# than Acme's 4-tank Product M / Product U setup get consistent colors
# without theme.py needing to know their tank names.
#
# The previous CHART_COLORS dict (keyed by Acme's "U-Tank1" / "M-Tank2"
# strings) was unused — Plotly was already falling through to its default
# palette in practice. This palette is available for any chart that
# wants a brand-aligned tank-color mapping; callers compute
# `CHART_PALETTE[tank_index % len(CHART_PALETTE)]`.
CHART_PALETTE = [
    "#1E3A8A",  # navy
    "#0F766E",  # deep teal
    "#7C2D12",  # auburn
    "#581C87",  # purple
    "#60A5FA",  # light blue
    "#5EEAD4",  # light teal
    "#FB923C",  # orange
    "#A78BFA",  # light purple
]


# ── CSS string ───────────────────────────────────────────────────────────────

def _build_css() -> str:
    """Render the design tokens + component classes as a single CSS string."""
    var_block = "\n".join(
        f"    --vmi-{k}: {v};" for k, v in TOKENS.items()
    )
    return f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

:root {{
{var_block}
    --vmi-font-ui:    'Segoe UI Variable', 'Segoe UI', 'Inter', -apple-system, BlinkMacSystemFont, system-ui, sans-serif;
    --vmi-font-mono:  'Cascadia Mono', 'Cascadia Code', Consolas, 'JetBrains Mono', Menlo, monospace;
    --vmi-ease:       cubic-bezier(0.4, 0, 0.2, 1);
}}

/* ── Base typography ─────────────────────────────────────────────────────── */
html, body, [class*="css"], .stApp {{
    font-family: var(--vmi-font-ui);
    color: var(--vmi-text-primary);
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}}
.stApp {{ background-color: var(--vmi-bg-app); }}

/* Headings — tightened tracking + better weight */
.stApp h1 {{
    font-weight: 700;
    letter-spacing: -0.5px;
    color: var(--vmi-text-headline);
    line-height: 1.1;
}}
.stApp h2 {{
    font-weight: 600;
    letter-spacing: -0.01em;
    color: var(--vmi-text-headline);
}}

/* h3 — section subheaders. The 3px blue left-border is the brand's
   signature marker for "this is a section start". */
.stApp h3 {{
    font-size: 1.125rem;
    font-weight: 600;
    color: var(--vmi-text-body);
    border-left: 3px solid var(--vmi-action);
    padding-left: 0.55rem;
    margin: 0.25rem 0 0.5rem 0;
}}

/* Captions */
.stApp small, [data-testid="stCaptionContainer"] {{
    color: var(--vmi-text-meta);
    font-size: 0.8125rem;
}}

/* ── Buttons — single-blue primary + soft hover glow ───────────────────── */
/* Selectors cover regular buttons, download buttons (`st.download_button`,
   which Streamlit wraps in `.stDownloadButton`), and link buttons
   (`st.link_button`, in `.stLinkButton`). Without explicitly targeting
   `.stDownloadButton`, primary download buttons render with Streamlit's
   default red — that was the cause of the "red Product Sheet button"
   bug after the inline CSS block in app.py was removed. */
.stApp .stButton > button,
.stApp .stDownloadButton > button,
.stApp .stLinkButton > a > button,
.stApp a[data-testid="stLinkButton"] > button {{
    font-family: var(--vmi-font-ui);
    font-weight: 600;
    font-size: 0.875rem;
    padding: 8px 16px;
    border-radius: 6px;
    border: 1.5px solid var(--vmi-border-strong);
    background: var(--vmi-bg-card);
    color: var(--vmi-text-body);
    transition: all 120ms var(--vmi-ease);
}}
.stApp .stButton > button:hover,
.stApp .stDownloadButton > button:hover,
.stApp .stLinkButton > a > button:hover,
.stApp a[data-testid="stLinkButton"] > button:hover {{
    border-color: var(--vmi-action);
    color: var(--vmi-action);
    transform: none;
}}
.stApp .stButton > button[kind="primary"],
.stApp .stDownloadButton > button[kind="primary"] {{
    background: var(--vmi-action);
    border-color: var(--vmi-action);
    color: #FFFFFF;
}}
.stApp .stButton > button[kind="primary"]:hover,
.stApp .stDownloadButton > button[kind="primary"]:hover {{
    background: var(--vmi-action-hover);
    border-color: var(--vmi-action-hover);
    color: #FFFFFF;
    box-shadow: 0 2px 8px var(--vmi-action-shadow);
}}
.stApp .stButton > button[disabled],
.stApp .stButton > button[disabled]:hover,
.stApp .stDownloadButton > button[disabled],
.stApp .stDownloadButton > button[disabled]:hover {{
    opacity: 0.55;
    cursor: not-allowed;
    transform: none;
    box-shadow: none;
}}

/* ── Inputs ─────────────────────────────────────────────────────────────── */
.stApp .stTextInput input,
.stApp .stTextArea textarea,
.stApp .stNumberInput input,
.stApp .stSelectbox > div > div,
.stApp .stMultiSelect > div > div {{
    border-radius: 6px;
    border-color: var(--vmi-border);
    font-family: var(--vmi-font-ui);
}}
.stApp .stTextInput input:focus,
.stApp .stTextArea textarea:focus,
.stApp .stNumberInput input:focus {{
    border-color: var(--vmi-action);
    box-shadow: 0 0 0 3px rgba(30, 64, 175, 0.18);
}}

/* ── Slider thumb to brand blue ─────────────────────────────────────────── */
.stApp .stSlider [data-baseweb="slider"] [role="slider"] {{
    background: var(--vmi-action);
    border-color: var(--vmi-action);
}}

/* ── Data editor table — cleaner edges ──────────────────────────────────── */
.stApp [data-testid="stDataFrameContainer"],
.stApp [data-testid="stDataEditorContainer"] {{
    border-radius: 8px;
    border: 1px solid var(--vmi-border);
}}

/* ── Metrics — proper KPI cards with mono numerics ─────────────────────── */
.stApp [data-testid="stMetric"] {{
    background: var(--vmi-bg-card);
    border: 1px solid var(--vmi-border);
    border-radius: 12px;
    padding: 14px 16px;
}}
.stApp [data-testid="stMetricLabel"] {{
    font-size: 0.72rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--vmi-text-meta);
}}
.stApp [data-testid="stMetricValue"] {{
    font-family: var(--vmi-font-mono);
    font-feature-settings: "tnum" 1;
    font-weight: 700;
    font-size: 1.4rem;
    line-height: 1.25;
    color: var(--vmi-text-headline);
    /* Cap + ellipsis so date-style metric values ("Mon 2026-05-04")
       don't overflow narrow columns like the auto-planner's 3-up. */
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}}

/* ── Container with border (st.container(border=True)) ─────────────────── */
/* Uses a deliberately stronger border than `--vmi-border` (the soft
   `#E2E8F0` line used by tank cards). With the darker `#D5DCE3` page
   bg, a 1px / `#E2E8F0` outline barely registers around the larger
   chart cards — the operator can't tell where the chart container
   ends. 1.5px / `#94A3B8` gives a clearly readable frame around the
   projection / level-history charts and any other
   `st.container(border=True)` blocks (LOW-confidence review, applied
   parse review, etc.). */
.stApp [data-testid="stVerticalBlockBorderWrapper"] {{
    background: var(--vmi-bg-card);
    border-radius: 12px !important;
    border: 1.5px solid #94A3B8 !important;
    padding: 1rem 1.25rem !important;
}}

/* ── Sim-time pill (top of page) ────────────────────────────────────────── */
.vmi-simtime {{
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: var(--vmi-bg-card);
    border: 1px solid var(--vmi-border);
    border-left: 3px solid var(--vmi-action);
    border-radius: 6px;
    padding: 6px 12px;
    font-size: 0.85rem;
}}
.vmi-simtime .lbl {{
    font-size: 0.66rem;
    font-weight: 600;
    color: var(--vmi-text-meta);
    letter-spacing: 0.08em;
    text-transform: uppercase;
}}
.vmi-simtime .val {{
    font-family: var(--vmi-font-mono);
    font-feature-settings: "tnum" 1;
    font-weight: 600;
    color: var(--vmi-text-headline);
}}

/* ── Eyebrow label — uppercase subhead before a value ──────────────────── */
.vmi-eyebrow {{
    font-family: var(--vmi-font-ui);
    font-size: 0.72rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--vmi-text-meta);
}}

/* ── Status chips ──────────────────────────────────────────────────────── */
.vmi-chip {{
    display: inline-block;
    padding: 3px 10px;
    border-radius: 999px;
    font-size: 0.6875rem;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    line-height: 1.6;
}}
.vmi-chip-draw      {{ background: var(--vmi-draw-bg); color: var(--vmi-draw-fg); }}
.vmi-chip-standby   {{ background: var(--vmi-standby-bg); color: var(--vmi-standby-fg); }}
.vmi-chip-receiving {{ background: var(--vmi-receiving-bg); color: var(--vmi-receiving-fg); }}
.vmi-chip-success   {{ background: var(--vmi-success-bg); color: var(--vmi-success-fg); }}
.vmi-chip-warning   {{ background: var(--vmi-warning-bg); color: var(--vmi-warning-fg); }}
.vmi-chip-danger    {{ background: var(--vmi-danger-bg); color: var(--vmi-danger-fg); }}
.vmi-chip-info      {{ background: var(--vmi-info-bg); color: var(--vmi-info-fg); }}

/* ── Numeric callouts (KPI tile values, inline figures) ────────────────── */
.vmi-num {{
    font-family: var(--vmi-font-mono);
    font-feature-settings: "tnum" 1;
    font-weight: 600;
}}
.vmi-num-lg {{ font-size: 1.75rem; line-height: 2rem; font-weight: 700; }}
.vmi-num-md {{ font-size: 1.25rem; line-height: 1.5rem; }}
.vmi-num-sm {{ font-size: 1.05rem; }}

/* ── Tank card ─────────────────────────────────────────────────────────── */
.vmi-tank-card {{
    background: var(--vmi-bg-card);
    border: 1px solid var(--vmi-border);
    border-radius: 12px;
    padding: 14px 16px;
    transition: border-color 0.15s var(--vmi-ease);
}}
.vmi-tank-card:hover {{ border-color: var(--vmi-border-strong); }}
.vmi-tank-card .name {{
    font-size: 0.88rem;
    font-weight: 600;
    color: var(--vmi-text-primary);
}}
.vmi-tank-card .product {{
    font-size: 0.72rem;
    color: var(--vmi-text-muted);
    margin-top: -2px;
}}

/* ── Animated tank-fill SVG ────────────────────────────────────────────── */
.vmi-tank-svg {{ display: block; }}
.vmi-tank-fluid {{
    transition: y 600ms var(--vmi-ease), height 600ms var(--vmi-ease);
}}

/* ── Alert banner — 4px left-border in semantic color ──────────────────── */
.vmi-banner {{
    border-radius: 8px;
    padding: 10px 14px;
    margin-bottom: 0.5rem;
    border: none;
    border-left: 4px solid;
    display: flex;
    align-items: flex-start;
    gap: 10px;
    font-size: 0.85rem;
    line-height: 1.45;
}}
.vmi-banner-success {{
    background: var(--vmi-success-bg);
    color: var(--vmi-success-fg);
    border-left-color: var(--vmi-success);
}}
.vmi-banner-warning {{
    background: var(--vmi-warning-bg);
    color: var(--vmi-warning-fg);
    border-left-color: var(--vmi-warning);
}}
.vmi-banner-danger {{
    background: var(--vmi-danger-bg);
    color: var(--vmi-danger-fg);
    border-left-color: var(--vmi-danger);
}}
.vmi-banner-info {{
    background: var(--vmi-info-bg);
    color: var(--vmi-info-fg);
    border-left-color: var(--vmi-info);
}}

/* ── Section header band — dark navy strip for above-list section
   headers (Alerts / Scheduled Trucks / Parse Review / Auto-Planner).
   Apply with:
       st.markdown('<div class="vmi-section-header">ALERTS (3)</div>',
                    unsafe_allow_html=True)
   Defined as part of the Fluent foundation; component wiring happens
   in follow-on phases. */
.vmi-section-header {{
    background: var(--vmi-bg-section);
    color: var(--vmi-fg-section);
    font-weight: 600;
    font-size: 0.78rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 0.4rem 0.75rem;
    border-radius: 4px;
    margin: 0.6rem 0 0.4rem 0;
}}

/* ── Monospaced ID utility — for SAP truck IDs, row IDs, run-hour
   anchors, anywhere an immutable identifier renders. Phase C wires
   into the scheduled-trucks dataframe via column_config + a cell
   selector. */
.vmi-id {{
    font-family: var(--vmi-font-mono);
    font-feature-settings: "tnum" 1;
    font-weight: 500;
}}

/* ── Small uppercase section sublabel (e.g. "Tank Levels (lbs)").
   Migrated from the deleted inline-CSS block in app.py so the single
   call site at app.py:2006 keeps rendering with the same look. */
.vmi-label {{
    font-family: var(--vmi-font-ui);
    font-size: 0.72rem;
    font-weight: 600;
    color: var(--vmi-text-meta);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 0.35rem;
}}

/* ── Operational-triage dataframe styling — gridlines off, single
   1px row separator in `--vmi-border`, no zebra. Affects the
   scheduled-trucks dataframe (app.py:~3368), the run-windows
   expander (app.py:~2095), and the HIGH-conf parse-review table
   (app.py:~2521). */
.stApp [data-testid="stDataFrameContainer"] table {{
    border-collapse: collapse;
}}
.stApp [data-testid="stDataFrameContainer"] tbody tr {{
    border-bottom: 1px solid var(--vmi-border);
}}
.stApp [data-testid="stDataFrameContainer"] thead th,
.stApp [data-testid="stDataFrameContainer"] tbody td {{
    border-left: none !important;
    border-right: none !important;
}}

/* ─────────────────────────────────────────────────────────────────────────
   Phase B — Linear-style dense alert rows.
   ─────────────────────────────────────────────────────────────────────────
   Replaces the multi-line `.vmi-banner` block layout with a single-row
   pattern:
       [ 2px severity bar │ glyph │ text … │ age │ ▸ ]
   Used by the Alerts list (app.py:~1873) and re-used by the
   Auto-Planner output rows (Phase E). */
.vmi-alert-row {{
    display: flex;
    align-items: center;
    gap: 0.6rem;
    background: var(--vmi-bg-card);
    border: 1px solid var(--vmi-border);
    border-left: 2px solid var(--vmi-text-muted);
    border-radius: 4px;
    padding: 0.45rem 0.75rem;
    margin-bottom: 0.35rem;
    font-size: 0.875rem;
    line-height: 1.3;
    min-height: 36px;
}}
.vmi-alert-row.danger    {{ border-left-color: var(--vmi-danger);  background: var(--vmi-danger-bg); }}
.vmi-alert-row.warning   {{ border-left-color: var(--vmi-warning); background: var(--vmi-warning-bg); }}
.vmi-alert-row.success   {{ border-left-color: var(--vmi-success); background: var(--vmi-success-bg); }}
.vmi-alert-row.info      {{ border-left-color: var(--vmi-info);    background: var(--vmi-info-bg); }}
.vmi-alert-row .glyph {{
    flex: 0 0 1.1em;
    font-size: 1.05em;
    line-height: 1;
}}
.vmi-alert-row .severity-label {{
    flex: 0 0 auto;
    font-size: 0.66rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding-right: 0.4rem;
    border-right: 1px solid rgba(0, 0, 0, 0.08);
}}
.vmi-alert-row .body {{
    flex: 1 1 auto;
    color: var(--vmi-text-body);
    font-weight: 500;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}}
.vmi-alert-row .meta {{
    flex: 0 0 auto;
    font-family: var(--vmi-font-mono);
    font-feature-settings: "tnum" 1;
    font-size: 0.78rem;
    color: var(--vmi-text-secondary);
}}

/* All-clear / empty-state row — same dense pattern as alerts, success tint. */
.vmi-empty-row {{
    display: flex;
    align-items: center;
    gap: 0.5rem;
    background: var(--vmi-success-bg);
    border: 1px solid var(--vmi-border);
    border-left: 2px solid var(--vmi-success);
    border-radius: 4px;
    padding: 0.5rem 0.75rem;
    color: var(--vmi-success-fg);
    font-size: 0.875rem;
    font-weight: 500;
    min-height: 36px;
}}

/* ─────────────────────────────────────────────────────────────────────────
   Phase D — Customer roster (sidebar) Fluent dense rows.
   ─────────────────────────────────────────────────────────────────────────
   Replaces the disabled-button + caption-below pattern with a single
   row: severity bar │ name │ alert-count badge. The disabled `st.button`
   underneath is kept for future re-activation of interactive switching
   (see _render_sidebar_customer_row docstring) but is visually
   hidden when this row renders above it. */
.vmi-customer-row {{
    display: flex;
    align-items: center;
    gap: 0.55rem;
    background: var(--vmi-bg-card);
    border: 1px solid var(--vmi-border);
    border-left: 2px solid var(--vmi-text-muted);
    border-radius: 4px;
    padding: 0.5rem 0.65rem;
    margin-bottom: 0.4rem;
    font-size: 0.875rem;
    line-height: 1.25;
    min-height: 36px;
}}
.vmi-customer-row.red     {{ border-left-color: var(--vmi-danger);  background: var(--vmi-danger-bg); }}
.vmi-customer-row.yellow  {{ border-left-color: var(--vmi-warning); background: var(--vmi-warning-bg); }}
.vmi-customer-row.green   {{ border-left-color: var(--vmi-success); }}
.vmi-customer-row.active  {{
    border-left-color: var(--vmi-action);
    background: var(--vmi-accent-bg);
}}
.vmi-customer-row .name {{
    flex: 1 1 auto;
    color: var(--vmi-text-body);
    font-weight: 600;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}}
.vmi-customer-row.active .name {{
    color: var(--vmi-action);
}}
.vmi-customer-row .badge {{
    flex: 0 0 auto;
    font-family: var(--vmi-font-mono);
    font-feature-settings: "tnum" 1;
    font-size: 0.72rem;
    font-weight: 700;
    color: var(--vmi-text-meta);
    background: var(--vmi-bg-subtle);
    border: 1px solid var(--vmi-border);
    border-radius: 999px;
    padding: 1px 8px;
    letter-spacing: 0.02em;
}}
.vmi-customer-row.red .badge    {{ color: var(--vmi-danger-fg);  background: #FFFFFF; border-color: var(--vmi-danger); }}
.vmi-customer-row.yellow .badge {{ color: var(--vmi-warning-fg); background: #FFFFFF; border-color: var(--vmi-warning); }}

/* Sidebar header tightening — matches the new dense roster rows. */
.vmi-sidebar-title {{
    font-size: 0.95rem;
    font-weight: 700;
    color: var(--vmi-text-headline);
    padding: 0.4rem 0 0.5rem 0;
    letter-spacing: -0.2px;
    text-transform: none;
}}

/* ─────────────────────────────────────────────────────────────────────────
   Phase G — list-summary caption rows (filter chip + counts).
   ─────────────────────────────────────────────────────────────────────────
   Quiet one-liner above list sections: "🔍 4 active · 1 critical · 3 warning".
   Reinforces the "operational triage queue" frame without competing
   visually with the rows below. */
.vmi-list-summary {{
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-family: var(--vmi-font-ui);
    font-size: 0.78rem;
    color: var(--vmi-text-secondary);
    margin: -0.1rem 0 0.4rem 0;
    letter-spacing: 0.01em;
}}
.vmi-list-summary .pip {{
    font-family: var(--vmi-font-mono);
    font-feature-settings: "tnum" 1;
    font-weight: 600;
    color: var(--vmi-text-body);
}}

/* ─────────────────────────────────────────────────────────────────────────
   Phase F — Email activity row tightening.
   ─────────────────────────────────────────────────────────────────────────
   Pulls the bespoke email-log HTML into a single set of utility classes
   so the rendering loop stops re-emitting style= on every entry. The
   tag pill now reads the Fluent tints via the same `kind` modifiers as
   alerts; the timestamp/recipient line uses the mono font for
   tabular figures. */
.vmi-email-row {{
    background: var(--vmi-bg-card);
    border: 1px solid var(--vmi-border);
    border-radius: 4px;
    padding: 0.5rem 0.75rem;
    margin-bottom: 0.4rem;
    font-family: var(--vmi-font-ui);
}}
.vmi-email-row .hdr {{
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
}}
.vmi-email-row .tag {{
    font-size: 0.66rem;
    font-weight: 700;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    padding: 1px 7px;
    border-radius: 3px;
    color: #FFFFFF;
}}
.vmi-email-row .tag.alert     {{ background: var(--vmi-danger);  }}
.vmi-email-row .tag.reminder  {{ background: var(--vmi-warning); color: #323130; }}
.vmi-email-row .tag.applied   {{ background: var(--vmi-success); }}
.vmi-email-row .tag.cs        {{ background: var(--vmi-bg-section); }}
.vmi-email-row .tag.test      {{ background: var(--vmi-text-secondary); }}
.vmi-email-row .tag.generic   {{ background: var(--vmi-text-secondary); }}
.vmi-email-row .subject {{
    font-weight: 600;
    font-size: 0.9rem;
    color: var(--vmi-text-headline);
}}
.vmi-email-row .status {{
    font-size: 0.7rem;
    font-weight: 600;
    padding: 1px 7px;
    border-radius: 999px;
}}
.vmi-email-row .status.sent     {{ color: var(--vmi-success-fg); background: var(--vmi-success-bg); }}
.vmi-email-row .status.notsent  {{ color: var(--vmi-warning-fg); background: var(--vmi-warning-bg); }}
.vmi-email-row .status.logged   {{ color: var(--vmi-text-secondary); background: var(--vmi-bg-subtle); }}
.vmi-email-row .meta {{
    margin-top: 0.25rem;
    font-size: 0.74rem;
    color: var(--vmi-text-meta);
    font-family: var(--vmi-font-mono);
    font-feature-settings: "tnum" 1;
}}
.vmi-email-row .meta .lbl {{
    color: var(--vmi-text-secondary);
    font-family: var(--vmi-font-ui);
    margin-right: 0.2rem;
}}

/* ── Hide Streamlit chrome we don't need ────────────────────────────────── */
#MainMenu {{ visibility: hidden; }}
header[data-testid="stHeader"] {{ background: transparent; }}
footer {{ visibility: hidden; }}

/* ── Tighten default vertical spacing between blocks ──────────────────── */
/* Page max-width tuning history:
     1280 (design-kit default)  — operators complained the side-by-side
                                  projection charts rendered daily ticks
                                  too narrowly to read.
     1480 (first bump)          — fixed the chart ticks, but on wider
                                  displays (≥ ~2200px) the centered block
                                  left a large empty strip between the
                                  sidebar and the main content.
     2200 (current)             — closes that gap on the operator's wide
                                  display while keeping a soft ceiling so
                                  very-wide 4K monitors don't stretch the
                                  chart lines uncomfortably. */
.stApp .block-container {{ padding-top: 1.5rem; max-width: 2200px; }}
.stApp [data-testid="stVerticalBlock"] {{ gap: 0.6rem; }}

/* ── Mobile: avoid overlap with Streamlit Cloud's sticky Fork button + */
/*           GitHub avatar pinned top-right at viewport edge.           */
/* The cloud chrome is ~44px tall on small viewports; we push our       */
/* content down so the brand header + first card aren't hidden under it.*/
@media (max-width: 480px) {{
    .stApp .block-container {{ padding-top: 3.5rem; }}
}}
</style>
"""


def inject_theme(st_module) -> None:
    """Render the theme CSS into the Streamlit page. Call once near the
    top of app.py, after st.set_page_config().

    Idempotent — re-rendering on every Streamlit rerun is fine; the
    browser de-dupes <style> tags.
    """
    st_module.markdown(_build_css(), unsafe_allow_html=True)


# ── Helper for HTML chips inside Streamlit panels ───────────────────────────

def chip_html(label: str, kind: str = "standby") -> str:
    """Return an inline HTML <span> for a status chip.
    Use with st.markdown(html, unsafe_allow_html=True).
    Valid kinds: draw / standby / receiving / success / warning / danger / info."""
    safe_kind = kind if kind in (
        "draw", "standby", "receiving", "success", "warning", "danger", "info"
    ) else "standby"
    safe_label = (str(label).replace("&", "&amp;")
                              .replace("<", "&lt;")
                              .replace(">", "&gt;"))
    return (f'<span class="vmi-chip vmi-chip-{safe_kind}">'
            f'{safe_label}</span>')


# ── Helper for the sim-time pill in the brand header ───────────────────────

def simtime_pill_html(value: str) -> str:
    """Return an inline HTML pill for the brand-header sim-time display.
    Use with st.markdown(html, unsafe_allow_html=True)."""
    safe = (str(value).replace("&", "&amp;")
                       .replace("<", "&lt;")
                       .replace(">", "&gt;"))
    return (
        '<span class="vmi-simtime">'
        '<span class="lbl">Sim time</span>'
        f'<span class="val">{safe}</span>'
        '</span>'
    )
