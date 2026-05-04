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
    # ── Surfaces
    "bg-app":         "#F8FAFC",
    "bg-app-warm":    "#F7F3EC",
    "bg-card":        "#FFFFFF",
    "bg-subtle":      "#F1F5F9",
    "border":         "#E2E8F0",
    "border-strong":  "#CBD5E1",

    # ── Text
    "text-primary":   "#0F172A",
    "text-headline":  "#0F1629",
    "text-body":      "#1E2A45",
    "text-secondary": "#475569",
    "text-meta":      "#64748B",
    "text-muted":     "#94A3B8",

    # ── Brand + action — single-blue identity
    "accent":         "#1E40AF",
    "accent-hover":   "#1E3A8A",
    "accent-bg":      "#DBEAFE",
    "accent-fg":      "#1E40AF",
    "action":         "#1E40AF",
    "action-hover":   "#1E3A8A",
    "action-shadow":  "rgba(30, 64, 175, 0.25)",

    # ── Semantic — success
    "success":     "#15803D",
    "success-bg":  "#DCFCE7",
    "success-fg":  "#166534",
    # ── Semantic — warning
    "warning":     "#B45309",
    "warning-bg":  "#FEF3C7",
    "warning-fg":  "#92400E",
    # ── Semantic — danger
    "danger":      "#B91C1C",
    "danger-bg":   "#FEE2E2",
    "danger-fg":   "#991B1B",
    # ── Semantic — info (uses brand blue)
    "info":        "#0EA5E9",
    "info-bg":     "#F0F9FF",
    "info-fg":     "#155E75",

    # ── Tank-status chip palette
    "draw-bg":      "#DBEAFE",  # blue
    "draw-fg":      "#1E40AF",
    "standby-bg":   "#F1F5F9",  # neutral
    "standby-fg":   "#475569",
    "receiving-bg": "#DCFCE7",  # green
    "receiving-fg": "#166534",

    # ── Tank-fill semantic (chart + SVG)
    "fill-critical":  "#F43F5E",   # < 20%
    "fill-low":       "#F59E0B",   # < 50%
    "fill-healthy":   "#0EA5E9",   # ≥ 50%
    "fill-receiving": "#22C55E",

    # ── Chart palette (Plotly traces — chart-only, no chrome use)
    "chart-u-tank1":  "#1E3A8A",   # navy
    "chart-u-tank2":  "#60A5FA",   # light blue
    "chart-m-tank1":  "#0F766E",   # deep teal
    "chart-m-tank2":  "#5EEAD4",   # light teal
}


# Public constants used by app.py to keep Plotly trace colors aligned with
# the design tokens without importing the whole TOKENS dict.
CHART_COLORS = {
    "U-Tank1": TOKENS["chart-u-tank1"],
    "U-Tank2": TOKENS["chart-u-tank2"],
    "M-Tank1": TOKENS["chart-m-tank1"],
    "M-Tank2": TOKENS["chart-m-tank2"],
}


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
    --vmi-font-ui:    'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    --vmi-font-mono:  'JetBrains Mono', Menlo, Consolas, monospace;
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
.stApp .stButton > button {{
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
.stApp .stButton > button:hover {{
    border-color: var(--vmi-action);
    color: var(--vmi-action);
    transform: none;
}}
.stApp .stButton > button[kind="primary"] {{
    background: var(--vmi-action);
    border-color: var(--vmi-action);
    color: #FFFFFF;
}}
.stApp .stButton > button[kind="primary"]:hover {{
    background: var(--vmi-action-hover);
    border-color: var(--vmi-action-hover);
    color: #FFFFFF;
    box-shadow: 0 2px 8px var(--vmi-action-shadow);
}}
.stApp .stButton > button[disabled],
.stApp .stButton > button[disabled]:hover {{
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
    color: var(--vmi-text-headline);
}}

/* ── Container with border (st.container(border=True)) ─────────────────── */
.stApp [data-testid="stVerticalBlockBorderWrapper"] {{
    background: var(--vmi-bg-card);
    border-radius: 12px !important;
    border: 1px solid var(--vmi-border) !important;
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
    background: #F0FDF4;
    color: #14532D;
    border-left-color: #22C55E;
}}
.vmi-banner-warning {{
    background: #FFFBEB;
    color: #92400E;
    border-left-color: #F59E0B;
}}
.vmi-banner-danger {{
    background: #FFF1F2;
    color: #9F1239;
    border-left-color: #F43F5E;
}}
.vmi-banner-info {{
    background: #F0F9FF;
    color: #155E75;
    border-left-color: var(--vmi-action);
}}

/* ── Hide Streamlit chrome we don't need ────────────────────────────────── */
#MainMenu {{ visibility: hidden; }}
header[data-testid="stHeader"] {{ background: transparent; }}
footer {{ visibility: hidden; }}

/* ── Tighten default vertical spacing between blocks ──────────────────── */
.stApp .block-container {{ padding-top: 1.5rem; max-width: 1280px; }}
.stApp [data-testid="stVerticalBlock"] {{ gap: 0.6rem; }}
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
