"""
theme.py — VMI design system.

One-stop shop for design tokens (colors / typography / spacing) and
component CSS classes used across app.py. Injected once at app startup
via inject_theme(). Pure presentation — no behavior.

Why a custom theme rather than Streamlit defaults:
  - Streamlit's defaults look like a prototype. The product needs to
    feel like a finished tool the operator trusts to wake them up at
    3am about an actual problem.
  - Semantic tokens (success / warning / danger) keep alert / status /
    chip colors consistent everywhere.
  - JetBrains Mono for numerics + Inter for UI gives the dashboard
    the tabular-numbers look operators recognize from monitoring tools.

Reversibility: deleting this module + the one inject_theme() call in
app.py reverts every visual change. No algorithm code touched.
"""

from __future__ import annotations


# ── Design tokens (single source of truth) ───────────────────────────────────

TOKENS = {
    # Surfaces
    "bg-app":     "#F8FAFC",
    "bg-card":    "#FFFFFF",
    "bg-subtle":  "#F1F5F9",
    "border":     "#E2E8F0",
    "border-strong": "#CBD5E1",
    # Text
    "text-primary":   "#0F172A",
    "text-secondary": "#475569",
    "text-muted":     "#94A3B8",
    # Brand
    "accent":         "#1E40AF",
    "accent-hover":   "#1E3A8A",
    "accent-bg":      "#DBEAFE",
    "accent-fg":      "#1E40AF",
    # Semantic — success
    "success":     "#15803D",
    "success-bg":  "#DCFCE7",
    "success-fg":  "#166534",
    # Semantic — warning
    "warning":     "#B45309",
    "warning-bg":  "#FEF3C7",
    "warning-fg":  "#92400E",
    # Semantic — danger
    "danger":      "#B91C1C",
    "danger-bg":   "#FEE2E2",
    "danger-fg":   "#991B1B",
    # Tank-status chip palette
    "draw-bg":     "#DBEAFE",  # blue
    "draw-fg":     "#1E40AF",
    "standby-bg":  "#F1F5F9",  # neutral
    "standby-fg":  "#475569",
    "receiving-bg": "#DCFCE7",  # green
    "receiving-fg": "#166534",
}


# ── CSS string ───────────────────────────────────────────────────────────────

def _build_css() -> str:
    """Render the design tokens + component classes as a single CSS string."""
    var_block = "\n".join(
        f"    --vmi-{k}: {v};" for k, v in TOKENS.items()
    )
    return f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

:root {{
{var_block}
}}

/* ── Base typography ─────────────────────────────────────────────────────── */
html, body, [class*="css"], .stApp {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    color: var(--vmi-text-primary);
}}
.stApp {{ background-color: var(--vmi-bg-app); }}

/* Headings — tightened tracking + better weight */
.stApp h1 {{ font-weight: 700; letter-spacing: -0.02em; color: var(--vmi-text-primary); }}
.stApp h2 {{ font-weight: 600; letter-spacing: -0.01em; color: var(--vmi-text-primary); }}
.stApp h3 {{ font-weight: 600; color: var(--vmi-text-primary); }}

/* Section subheaders (st.subheader). Streamlit renders these as h3. */
.stApp h3 {{ font-size: 1.125rem; margin-top: 0.25rem; margin-bottom: 0.5rem; }}

/* Captions */
.stApp small, [data-testid="stCaptionContainer"] {{
    color: var(--vmi-text-muted);
    font-size: 0.8125rem;
}}

/* ── Buttons — refined accent ───────────────────────────────────────────── */
.stApp .stButton > button {{
    border-radius: 8px;
    font-weight: 500;
    border: 1px solid var(--vmi-border);
    transition: all 0.12s ease;
}}
.stApp .stButton > button:hover {{
    border-color: var(--vmi-border-strong);
    transform: translateY(-1px);
}}
.stApp .stButton > button[kind="primary"] {{
    background: var(--vmi-accent);
    border-color: var(--vmi-accent);
    color: #FFFFFF;
}}
.stApp .stButton > button[kind="primary"]:hover {{
    background: var(--vmi-accent-hover);
    border-color: var(--vmi-accent-hover);
}}

/* ── Inputs ─────────────────────────────────────────────────────────────── */
.stApp .stTextInput input,
.stApp .stTextArea textarea,
.stApp .stNumberInput input,
.stApp .stSelectbox > div > div,
.stApp .stMultiSelect > div > div {{
    border-radius: 8px;
    border-color: var(--vmi-border);
}}

/* ── Data editor table — cleaner edges ──────────────────────────────────── */
.stApp [data-testid="stDataFrameContainer"],
.stApp [data-testid="stDataEditorContainer"] {{
    border-radius: 8px;
    border: 1px solid var(--vmi-border);
}}

/* ── Metrics — turn into proper KPI cards ──────────────────────────────── */
.stApp [data-testid="stMetric"] {{
    background: var(--vmi-bg-card);
    border: 1px solid var(--vmi-border);
    border-radius: 12px;
    padding: 16px 18px;
}}
.stApp [data-testid="stMetricLabel"] {{
    font-size: 0.75rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--vmi-text-secondary);
}}
.stApp [data-testid="stMetricValue"] {{
    font-family: 'JetBrains Mono', monospace;
    font-weight: 600;
    color: var(--vmi-text-primary);
}}

/* ── Container with border (st.container(border=True)) ─────────────────── */
.stApp [data-testid="stVerticalBlockBorderWrapper"] {{
    background: var(--vmi-bg-card);
    border-radius: 12px !important;
    border: 1px solid var(--vmi-border) !important;
    padding: 1rem 1.25rem !important;
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

/* ── Numeric callout (big number + small label, used in KPI tiles) ─────── */
.vmi-num {{
    font-family: 'JetBrains Mono', monospace;
    font-feature-settings: "tnum" 1;     /* tabular numbers */
    font-weight: 600;
}}
.vmi-num-lg {{ font-size: 1.75rem; line-height: 2rem; }}
.vmi-num-md {{ font-size: 1.25rem; line-height: 1.5rem; }}

/* ── Tank card ─────────────────────────────────────────────────────────── */
.vmi-tank-card {{
    background: var(--vmi-bg-card);
    border: 1px solid var(--vmi-border);
    border-radius: 12px;
    padding: 14px 16px;
    transition: border-color 0.15s ease;
}}
.vmi-tank-card:hover {{ border-color: var(--vmi-border-strong); }}
.vmi-tank-card .name {{
    font-size: 0.875rem;
    font-weight: 600;
    color: var(--vmi-text-primary);
}}
.vmi-tank-card .product {{
    font-size: 0.75rem;
    color: var(--vmi-text-muted);
    margin-top: -2px;
}}

/* ── Animated tank-fill SVG (Phase 9 placeholder hooks; real anim there) ─ */
.vmi-tank-svg {{ display: block; }}
.vmi-tank-fluid {{ transition: y 0.6s cubic-bezier(0.4, 0, 0.2, 1),
                              height 0.6s cubic-bezier(0.4, 0, 0.2, 1); }}

/* ── Alert banner (used by alerts subheader) ───────────────────────────── */
.vmi-banner {{
    border-radius: 12px;
    padding: 12px 16px;
    margin-bottom: 0.5rem;
    border: 1px solid;
    display: flex;
    align-items: center;
    gap: 10px;
}}
.vmi-banner-success {{
    background: var(--vmi-success-bg);
    border-color: var(--vmi-success);
    color: var(--vmi-success-fg);
}}
.vmi-banner-warning {{
    background: var(--vmi-warning-bg);
    border-color: var(--vmi-warning);
    color: var(--vmi-warning-fg);
}}
.vmi-banner-danger {{
    background: var(--vmi-danger-bg);
    border-color: var(--vmi-danger);
    color: var(--vmi-danger-fg);
}}

/* ── Hide Streamlit chrome we don't need ────────────────────────────────── */
#MainMenu {{ visibility: hidden; }}
header[data-testid="stHeader"] {{ background: transparent; }}
footer {{ visibility: hidden; }}

/* ── Tighten default vertical spacing between blocks ──────────────────── */
.stApp .block-container {{ padding-top: 1.5rem; }}
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
    Valid kinds: draw / standby / receiving / success / warning / danger."""
    safe_kind = kind if kind in (
        "draw", "standby", "receiving", "success", "warning", "danger"
    ) else "standby"
    # html-escape via .replace() — Streamlit renders unsafe markup
    safe_label = (str(label).replace("&", "&amp;")
                              .replace("<", "&lt;")
                              .replace(">", "&gt;"))
    return (f'<span class="vmi-chip vmi-chip-{safe_kind}">'
            f'{safe_label}</span>')
