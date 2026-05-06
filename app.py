"""
app.py  —  VMI Prototype  —  Streamlit web demo
Run: streamlit run app.py
"""

import base64
import contextlib
import copy
import io
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import streamlit as st
import plotly.graph_objects as go

from alerts import (
    get_all_alerts, simulate_consume, simulate_delivery_no_alert,
    is_running_at, SAFETY_STOCK_LBS,
    LEAD_TIME_HOURS, LATE_TRUCK_HOURS, PROJECTION_WINDOW_HOURS,
    PLANT_STATE_MISMATCH_HOURS,
)
from config import DEFAULT_CONFIG as _DEFAULT_CFG
from plan_orders import (
    plan_for_product, get_target_week_bounds,
    get_target_for_week, get_run_hours_in_window,
    TARGET_LOW_LBS, TARGET_HIGH_LBS,
    TARGET_LOW_RUN_HOURS, TARGET_HIGH_RUN_HOURS,
)
from read_schedule import (
    parse_schedule_text, parse_schedule, apply_schedule_to_data,
    check_anthropic_api,
)
from pdf_generator import build_load_entry_pdf
from projection import compute_level_history
from time_utils import run_hour_to_dt, dt_to_run_hour, format_run_hour
from email_client import OutlookClient, load_config

# Defensive import: inventory_age_hours powers the per-tank "age" chip.
# Hoisted to module top (was previously a late import inside _tank_info)
# so a deployment-environment ImportError no longer takes down the
# whole dashboard — the chip silently disappears instead.
try:
    from level_history import inventory_age_hours as _inv_age
except ImportError:
    _inv_age = None

DEFAULTS_PATH = Path("defaults.json")
CONFIG_PATH   = Path("email_config.json")
APP_TIMEZONE  = "America/New_York"   # used for sim-clock anchor and display

# How many hours forward the dashboard projection chart and the What-If
# preview chart render. 288 = 12 days, matching the dashboard heading
# ("📈 12-Day Projection") and the README. Distinct from
# PROJECTION_WINDOW_HOURS (= cfg.projection_window_hours, default 168 / 7
# days), which is the SAFETY-STOCK alert horizon — that's how far the
# alert engine looks ahead, not how far the chart renders.
PROJECTION_CHART_HOURS = 288


def _get_anthropic_key():
    """
    Resolve the Anthropic API key from (in priority order):
      1. Streamlit secrets  (st.secrets["ANTHROPIC_API_KEY"])  — used on Streamlit Cloud
      2. Environment variable ANTHROPIC_API_KEY                — set locally or in .env
      3. email_config.json  anthropic_api_key field            — local fallback
    Returns "" if not found anywhere.
    """
    import os
    try:
        return st.secrets["ANTHROPIC_API_KEY"]
    except Exception:
        pass
    val = os.environ.get("ANTHROPIC_API_KEY", "")
    if val:
        return val
    cfg = load_config()
    return cfg.get("anthropic_api_key", "") if cfg else ""

_PALETTE = [
    "#1E3A8A",  # navy
    "#60A5FA",  # light blue
    "#0F766E",  # deep teal
    "#5EEAD4",  # light teal
    "#7C3AED",  # violet
    "#A78BFA",  # light violet
    "#B45309",  # amber
    "#FCD34D",  # yellow
]

def _tank_color(tank_name: str, all_tanks: list) -> str:
    """Deterministic color per tank from the shared palette."""
    try:
        return _PALETTE[all_tanks.index(tank_name) % len(_PALETTE)]
    except ValueError:
        return "#888888"

# Header links
GITHUB_URL         = "https://github.com/JD8-rgb/vmi-prototype"
PRODUCT_SHEET_PATH = Path("assets/product_sheet.pdf")


def _load_product_sheet():
    """
    Return the pre-built product sheet PDF bytes, or None if missing.
    Intentionally NOT cached — the file is ~5 KB and we want regenerated
    PDFs to show up immediately on the next rerun without a server restart.
    """
    try:
        return PRODUCT_SHEET_PATH.read_bytes()
    except FileNotFoundError:
        return None


st.set_page_config(page_title="VMI Automation", layout="wide", initial_sidebar_state="collapsed")

# Inject the design system once at startup. Pure CSS; reverts cleanly
# by removing this import + call.
from theme import inject_theme as _inject_theme, chip_html as _chip_html
_inject_theme(st)

# Operator-action audit log helper. Single-line import keeps the
# recording call sites short.
import audit_log as _audit


# ── Shared helpers: render parser entries as Day+time strings ───────────────
#
# Three places in the UI display (weekday_int, start_h_in_day, end_h_offset)
# tuples to operators:
#   1. Schedule Parser inline editor (LOW + HIGH)
#   2. Pending low-confidence parse review (editor)
#   3. Applied parse review (read-only — the HIGH-confidence "what just
#      auto-applied" panel)
#
# Operators can't read the raw end_h offset (e.g. 46 = "Fri 22:00 from
# Mon midnight") — they need "Mon 06:00 → Tue 16:00". These helpers
# convert in both directions and live at module-top so all three
# panels stay consistent.

_PARSER_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_PARSER_DAY_TOKENS = {
    "monday": 0, "mon": 0,
    "tuesday": 1, "tue": 1, "tues": 1,
    "wednesday": 2, "wed": 2,
    "thursday": 3, "thu": 3, "thur": 3, "thurs": 3,
    "friday": 4, "fri": 4,
    "saturday": 5, "sat": 5,
    "sunday": 6, "sun": 6,
}


def _entry_to_strs(entry):
    """(weekday, start_h_in_day, end_h_offset) → ('Mon 06:00', 'Tue 16:00').

    `end_h_offset` is hours-from-start-day-midnight, so eh=24 means
    "next day 00:00", eh=124 means "5 days + 4h past start". divmod
    collapses multiples-of-24 to next-day-00:00 cleanly so we never
    print 'Mon 24:00' (which the parser later rejects)."""
    wd_s, sh, eh = int(entry[0]), int(entry[1]), int(entry[2])
    day_offset, h_e = divmod(eh, 24)
    wd_e = (wd_s + day_offset) % 7
    return f"{_PARSER_DAYS[wd_s]} {sh:02d}:00", f"{_PARSER_DAYS[wd_e]} {h_e:02d}:00"


def _entry_to_window_str(entry):
    """(weekday, start, end) → 'Mon 06:00 → Tue 16:00'.

    Single-string presentation for read-only display of a parsed
    window. Used by the applied-parse-review panel."""
    s, e = _entry_to_strs(entry)
    return f"{s} → {e}"


def _parse_day_time(s):
    """'Mon 6am' / 'Thu 4pm' / 'Sat 04:00' → (weekday, hour_of_day) | None.

    Reuses the existing time-token parser from read_schedule for
    the time half. Tolerant of trailing punctuation on the day
    name (',' '.' ';')."""
    from read_schedule import _parse_time as _parse_time_token
    if not s or not isinstance(s, str):
        return None
    parts = s.strip().lower().split(maxsplit=1)
    if len(parts) != 2:
        return None
    wd = _PARSER_DAY_TOKENS.get(parts[0].rstrip(",.;"))
    if wd is None:
        return None
    h = _parse_time_token(parts[1])
    if h is None:
        return None
    return (wd, h)


def _parsed_strs_to_entry(start_str, end_str):
    """Inverse of _entry_to_strs. Returns (weekday, start_h, end_h)
    or None if either side fails to parse OR is degenerate."""
    start = _parse_day_time(start_str)
    end   = _parse_day_time(end_str)
    if start is None or end is None:
        return None
    wd_s, h_s = start
    wd_e, h_e = end
    # Same day-of-week + same hour is a degenerate "Mon 6am to Mon 6am"
    # row — almost certainly an operator typo or unfilled cell, not a
    # legitimate 7-day continuous shift. Drop it. (For a real full-week
    # shift the operator types e.g. "Mon 6am" → "Mon 5am", which has
    # h_e < h_s and wraps correctly via the next branch.)
    if wd_s == wd_e and h_s == h_e:
        return None
    day_offset = (wd_e - wd_s) % 7
    if day_offset == 0 and h_e < h_s:
        day_offset = 7   # legitimate full-week wrap (e.g. continuous shop)
    end_in_day_offset = day_offset * 24 + h_e
    if end_in_day_offset <= h_s:
        return None
    return (wd_s, h_s, end_in_day_offset)


def _entries_overlap_pairs(entries):
    """Detect overlapping (weekday, start_h, end_h) windows by converting
    each to absolute hours from week start (Mon 00:00). This handles
    multi-day windows correctly — e.g. (0, 6, 40) [Mon-Tue] and
    (1, 6, 64) [Tue-Thu] map to abs ranges [6, 40] and [30, 88], which
    overlap on [30, 40]. Returns 1-based (i, j) row index pairs.

    The simple per-weekday check used previously missed these cross-day
    overlaps because the windows had different `weekday` fields even
    though their actual hour ranges intersected.
    """
    abs_ranges = [(int(wd) * 24 + int(sh), int(wd) * 24 + int(eh))
                  for wd, sh, eh in entries]
    pairs = []
    for i in range(len(abs_ranges)):
        for j in range(i + 1, len(abs_ranges)):
            s_i, e_i = abs_ranges[i]
            s_j, e_j = abs_ranges[j]
            if s_i < e_j and s_j < e_i:
                pairs.append((i + 1, j + 1))
    return pairs


def _has_fractional_minutes(s) -> bool:
    """True if `s` is a Day+time string whose time component carries
    a non-zero minute value (e.g. "Mon 6:30am", "Tue 14:45").

    Used by the parser editor + LC pending-review editor to warn the
    operator when their typed input gets silently rounded to integer
    hours by read_schedule._parse_time. Without this hint the operator
    could type "Mon 6:30am" and never notice the schedule actually
    applied as "Mon 06:00".

    Pattern requires a digit BEFORE the colon (so it's a time, not
    a label like "weekend:30") and a non-zero minute (00 means the
    operator was being explicit about "on the hour").
    """
    if not isinstance(s, str):
        return False
    # Trailing (?!\d) — not followed by another digit — so that "6:30am"
    # matches (am is a word char, blocks \b but not \d) while "12:455"
    # doesn't (the run of digits keeps going).
    return bool(re.search(r'\b\d{1,2}:([1-5]\d|0[1-9])(?!\d)', s))


# ── Session state / defaults ──────────────────────────────────────────────────

def _defaults():
    """
    Load defaults.json and anchor the epoch to the most recent Monday,
    setting current_run_hour to reflect the actual elapsed time since then.
    This means Reset always puts the sim clock at 'right now'.
    """
    with open(DEFAULTS_PATH) as f:
        tmpl = json.load(f)
    now      = datetime.now(ZoneInfo(APP_TIMEZONE)).replace(tzinfo=None)
    # Most recent Monday midnight (Eastern)
    anchor   = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    current_rh = (now - anchor).total_seconds() / 3600.0
    tmpl["simulation_epoch"]  = anchor.strftime("%Y-%m-%dT%H:%M:%S")
    tmpl["current_run_hour"]  = round(current_rh, 1)
    return tmpl


# Bridge Streamlit session state to the data.json file used by CLI
# scripts. Without this, the web app and CLI scripts each hold their
# own copy of the schedule/tank state and silently disagree.
#
#   - On session start (`data` not yet in session_state): if data.json
#     exists, load it; otherwise fall back to the re-anchored template.
#   - On every rerun: if data.json's mtime advanced beyond what the
#     session has seen (i.e. a CLI script mutated it externally), drop
#     the in-memory copy and reload.
#   - At the bottom of every rerun (handled near the end of this file),
#     persist the current state back to data.json so CLI scripts see
#     Streamlit-side changes.
import os as _os_state
from data_io import load_data as _load_data_state, save_data as _save_data_state

_DATA_FILE   = "data.json"
_disk_mtime  = _os_state.path.getmtime(_DATA_FILE) if _os_state.path.exists(_DATA_FILE) else None


def _save_active_state(data) -> None:
    """Persist mutations to disk — but only for the live Acme demo.

    Stage 2 of the multi-customer roster: when a non-Acme customer is
    selected via the sidebar, mutations to st.session_state.data stay
    in-memory for the duration of that selection. Switching back to
    Acme reloads its persisted state from data.json; switching to
    another customer reloads it fresh from customers/<id>.json.

    Trade-off: simulating actions on a non-Acme customer (advance
    clock, add trucks, etc.) does not survive a customer-switch or
    a page reload. This keeps the demo customer files clean and
    prevents accidental overwrites of the curated example state.
    """
    if st.session_state.get("current_customer", "acme") == "acme":
        _save_data_state(data, _DATA_FILE)   # underlying, not the wrapper


def _switch_customer(new_customer_id: str) -> None:
    """Sidebar customer-row click handler.

    1. If currently on Acme, flush its in-flight mutations to data.json
       so we don't lose them.
    2. Load the target customer's state AND its PlantConfig into
       st.session_state. For Acme: re-read data.json + DEFAULT_CONFIG.
       For others: customers.load_customer() returns (cfg, state_dict).
    3. Re-anchor the simulation clock to wall-clock now, so a customer
       JSON committed weeks ago doesn't display last-month timestamps.
    4. Update st.session_state.current_customer + clear session caches
       that key off "the active customer."

    The caller is responsible for st.rerun() after this returns.
    """
    current = st.session_state.get("current_customer", "acme")
    if current == "acme" and "data" in st.session_state:
        # Flush Acme's pending mutations before swapping. Call the
        # underlying save directly — _save_active_state would no-op
        # in some edge cases here (we want to ensure Acme's state
        # always lands on disk before we swap it out).
        try:
            _save_data_state(st.session_state.data, _DATA_FILE)
        except Exception:
            pass

    if new_customer_id == "acme":
        st.session_state.data = _initial_state()    # already reanchors
        st.session_state.cfg  = _DEFAULT_CFG        # Acme uses the defaults
    else:
        from customers import load_customer
        _cfg, _state_dict = load_customer(new_customer_id)
        # Reanchor the new customer's simulation clock to wall-clock
        # now — without this, switching to a customer whose JSON was
        # committed weeks ago shows weeks-ago timestamps and misaligned
        # schedule windows.
        _state_dict = _reanchor_to_now(_state_dict)
        st.session_state.data = _state_dict
        st.session_state.cfg  = _cfg

    st.session_state.current_customer = new_customer_id
    # Refresh the disk-mtime watchdog snapshot — we just wrote data.json
    # (if leaving Acme) and any spurious reload after this point would
    # clobber the swap we just made.
    if _os_state.path.exists(_DATA_FILE):
        st.session_state._disk_mtime_seen = _os_state.path.getmtime(_DATA_FILE)
    # Reset session-scoped caches that key off "the active customer".
    # `_edited_entries` is the LOW-confidence editor's draft state; if
    # the user edits Acme's schedule rows then switches before clicking
    # Apply, those edits must NOT bleed into the new customer's editor.
    # `parse_result` is the cached schedule-parse output. Both reset.
    for _k in ("_demo_data", "_demo_parsed",
                "_edited_entries", "parse_result"):
        st.session_state.pop(_k, None)

def _reanchor_to_now(state):
    """Re-anchor simulation_epoch + current_run_hour so the displayed
    sim clock matches wall-clock "now" on every fresh app open.

    Without this, opening the demo on Tuesday after it was last saved
    last week shows last week's Monday + last week's elapsed hours —
    confusing for first-time viewers ("why does it say it's Friday?").

    We only touch the two clock fields. Run-schedule windows are stored
    as offsets from the epoch, so they "follow" the re-anchor and end
    up pointing at this-week's Mon-Fri (which is what the operator
    almost always wants for a demo). scheduled_trucks / level_history
    work the same way.
    """
    now = datetime.now(ZoneInfo(APP_TIMEZONE)).replace(tzinfo=None)
    anchor = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    state["simulation_epoch"] = anchor.strftime("%Y-%m-%dT%H:%M:%S")
    state["current_run_hour"] = round((now - anchor).total_seconds() / 3600.0, 1)
    return state


def _initial_state():
    """Pick up data.json if present; otherwise re-anchored defaults template.
    Always re-anchors the sim clock to wall-clock now on session start."""
    if _os_state.path.exists(_DATA_FILE):
        try:
            return _reanchor_to_now(_load_data_state(_DATA_FILE))
        except Exception as _load_err:
            st.warning(f"⚠️ Could not load data.json ({_load_err}) — starting from defaults.")
    return _defaults()

if "data" not in st.session_state:
    st.session_state.data = _initial_state()
    st.session_state._disk_mtime_seen = _disk_mtime
elif (st.session_state.get("current_customer", "acme") == "acme"
        and _disk_mtime is not None
        and _disk_mtime != st.session_state.get("_disk_mtime_seen")):
    # data.json changed under us (CLI mutation, manual edit, etc.).
    # Reload so the dashboard reflects the on-disk truth. Only fires
    # when the active customer IS Acme — for other customers, data.json
    # holds Acme's persisted state, not theirs, so a mtime change
    # there must NOT trigger a reload of the active customer's view.
    try:
        st.session_state.data = _load_data_state(_DATA_FILE)
        st.session_state._disk_mtime_seen = _disk_mtime
        st.toast("data.json changed externally — reloaded.", icon="🔄")
    except Exception as _reload_err:
        st.warning(f"data.json changed externally but reload failed: {_reload_err}")

if "planned_trucks" not in st.session_state: st.session_state.planned_trucks = []
if "plan_reasoning" not in st.session_state: st.session_state.plan_reasoning = []
if "plan_log"       not in st.session_state: st.session_state.plan_log       = []
if "pdf_bytes"      not in st.session_state: st.session_state.pdf_bytes      = None
if "email_log"      not in st.session_state: st.session_state.email_log      = []
if "parse_result"   not in st.session_state: st.session_state.parse_result   = None
if "advance_log"    not in st.session_state: st.session_state.advance_log    = ""
if "what_if_rate"   not in st.session_state: st.session_state.what_if_rate   = 583.3
if "what_if_safety" not in st.session_state: st.session_state.what_if_safety = 10000.0
# Wall-clock moment this Streamlit session started (UTC, timezone-aware).
# Used to filter the inbox so fresh sessions don't pick up stale schedule
# emails left over from previous demo runs.
if "session_start_real_utc" not in st.session_state:
    from datetime import timezone as _tz_utc
    st.session_state.session_start_real_utc = datetime.now(_tz_utc.utc)

# View state. Today the only "view" is the dashboard; the customer
# roster lives in the sidebar (always visible).
if "view" not in st.session_state: st.session_state.view = "dashboard"

# Active customer for the dashboard. Default = "acme" (the live demo).
# Sidebar row click → _switch_customer() updates this and reloads
# st.session_state.data + st.session_state.cfg from the appropriate
# source file.
if "current_customer" not in st.session_state:
    st.session_state.current_customer = "acme"
# Per-customer PlantConfig. Updated alongside `data` by
# _switch_customer. Algorithm calls in the dashboard must always pass
# `cfg=st.session_state.cfg` so a non-Acme customer's overrides
# (lead_time_hours, safety_stock_lbs, plant_holidays, delivery_slots,
# etc.) are honored instead of silently falling through to DEFAULT_CONFIG.
if "cfg" not in st.session_state:
    st.session_state.cfg = _DEFAULT_CFG

data = st.session_state.data

# ── First-install bootstrap ───────────────────────────────────────────────────
# A new install lands with empty level_history → 12-day projection chart is
# blank, which is a confusing first impression. If the operator hasn't seen
# the guided tour yet AND there's no history, backfill 4 weeks of synthetic
# past so the chart is meaningful immediately. Idempotent: skipped on every
# subsequent load because level_history is now populated.
#
# Restricted to the live Acme customer: the demo tour and synthetic-history
# bootstrap are part of Acme's first-run onboarding only. Switching to a
# non-Acme customer must NOT re-trigger the modal or backfill its curated
# state with Acme-style synthetic history.
if (st.session_state.get("current_customer", "acme") == "acme"
        and not data.get("level_history")
        and not data.get("first_run_tour_complete", False)):
    try:
        from demo_history import generate_demo_history
        generate_demo_history(data, weeks=4)
        _save_active_state(data)
    except Exception as _bootstrap_err:
        import sys
        print(f"[first-install-bootstrap] {_bootstrap_err}", file=sys.stderr)


# ── Customer roster (sidebar) ─────────────────────────────────────────────────
#
# Always-visible left rail listing every customer with their name +
# status signal + alert counts. Acme is the live demo (backed by
# session_state.data). Other customers come from `customers/<id>.json`
# via `customers.load_customer()`.
#
# Stage 1 — sidebar is purely visual: rows aren't clickable, the active
# customer is fixed at Acme. Stage 2 will wire row-click → state-swap.

def _roster_alert_count(d):
    """Count of currently-firing alerts on a state dict — drives the
    sidebar status indicator. Returns (red, yellow) counts. Resilient
    to alert-engine errors so a transient bug in one customer doesn't
    blank out the whole sidebar."""
    try:
        from alerts import get_all_alerts as _gaa
        alerts = _gaa(d)
        red    = sum(1 for a in alerts if a.get("severity") == "red_flag")
        yellow = sum(1 for a in alerts if a.get("severity") == "warning")
        return red, yellow
    except Exception as _e:
        import sys; print(f"[roster_alert_count] {_e}", file=sys.stderr)
        return 0, 0


def _customer_display_name(customer_id: str) -> str:
    """Convert 'example_customer' → 'Example Customer' for sidebar display."""
    return customer_id.replace("_", " ").title()


def _render_sidebar_roster():
    """Render the always-visible, interactive customer list in the
    left sidebar.

    Each row is a full-width Streamlit button styled by row state:
      • Active row (== current_customer): primary type, disabled.
      • Inactive row: secondary type, click swaps the active customer.

    Caption below each button shows the alert counts ("2 red · 1
    yellow" or "no alerts"). Status emoji (🔴/🟡/🟢) prefixes the name.
    """
    current = st.session_state.get("current_customer", "acme")

    with st.sidebar:
        st.markdown(
            """<div style="font-size:1rem;font-weight:700;color:#0F172A;
                          padding:0.5rem 0 0.75rem;letter-spacing:-0.2px;">
                Customers
            </div>""",
            unsafe_allow_html=True,
        )

        # Build the customer list: Acme first, then everything else
        # from the customers/ directory in alphabetical order.
        try:
            from customers import list_customers
            _other_ids = [c for c in list_customers() if c != "acme"]
        except Exception as _e:
            import sys; print(f"[sidebar_roster] {_e}", file=sys.stderr)
            _other_ids = []
        _all_ids = ["acme"] + _other_ids

        for _cid in _all_ids:
            _is_active = (_cid == current)
            _display   = "Acme Plastics" if _cid == "acme" \
                          else _customer_display_name(_cid)
            _r, _y = _alerts_for_sidebar_customer(_cid)
            _render_sidebar_customer_row(_cid, _display, _r, _y, _is_active)


def _alerts_for_sidebar_customer(customer_id: str):
    """Return (red, yellow) alert counts for the sidebar row.

    For the active customer, use the in-memory state (so counts reflect
    live mutations). For inactive customers, load fresh from the
    backing file. Errors return (0, 0) so a single broken customer
    doesn't blank the whole sidebar."""
    current = st.session_state.get("current_customer", "acme")
    if customer_id == current:
        return _roster_alert_count(data)
    try:
        if customer_id == "acme":
            _state = _load_data_state(_DATA_FILE)
        else:
            from customers import load_customer
            _, _state = load_customer(customer_id)
        return _roster_alert_count(_state)
    except Exception as _e:
        import sys; print(f"[sidebar_alerts] {customer_id}: {_e}", file=sys.stderr)
        return 0, 0


def _render_sidebar_customer_row(customer_id: str, name: str,
                                  red: int, yellow: int,
                                  active: bool) -> None:
    """One clickable sidebar row. Active row is disabled (you can't
    click the customer you're already on). Inactive rows trigger
    _switch_customer() + rerun on click."""
    if red > 0:
        _signal = "🔴"
    elif yellow > 0:
        _signal = "🟡"
    else:
        _signal = "🟢"

    if red == 0 and yellow == 0:
        _count = "no alerts"
    else:
        _parts = []
        if red:    _parts.append(f"{red} red")
        if yellow: _parts.append(f"{yellow} yellow")
        _count = " · ".join(_parts)

    _label = f"{_signal}  {name}"
    _btn_type = "primary" if active else "secondary"
    _btn_help = ("Currently selected" if active
                  else f"Switch to {name}")

    if st.sidebar.button(
        _label,
        key=f"customer_select_{customer_id}",
        use_container_width=True,
        type=_btn_type,
        disabled=active,
        help=_btn_help,
    ):
        _switch_customer(customer_id)
        st.rerun()

    # Alert-count caption under the button
    st.sidebar.markdown(
        f"""<div style="color:#94A3B8;font-size:0.78rem;
                    margin:-0.4rem 0 0.7rem 0.65rem;">
            {_count}
        </div>""",
        unsafe_allow_html=True,
    )


_render_sidebar_roster()


# ── Advance simulation ────────────────────────────────────────────────────────

def _advance(data, hours, session_start_utc=None):
    """Advance data in-place by hours. Returns (log_str, email_events).

    session_start_utc : wall-clock UTC datetime of when this Streamlit
        session started. Used to filter stale schedule emails from earlier
        demo runs so they don't get auto-applied.
    """
    log   = []
    tanks = data["tanks"]
    rates = data["consumption_rates"]
    start = data["current_run_hour"]
    end   = start + hours

    events = []
    for t in data["scheduled_trucks"]:
        if start < t["arrival_run_hour"] <= end:
            events.append((t["arrival_run_hour"], "d", t))
    for w in data["run_schedule"]:
        if w["end_hour"] > start and w["start_hour"] < end:
            ws = max(w["start_hour"], start)
            we = min(w["end_hour"], end)
            if ws > start: events.append((ws, "s", None))
            if we < end:   events.append((we, "e", None))
    events.sort(key=lambda e: (e[0], {"e": 0, "s": 1, "d": 2}[e[1]]))

    burning = is_running_at(data, start)
    clock   = start
    done    = []

    from level_history import record_level_snapshot as _rls_app

    def _consume(seg):
        if seg > 0 and burning:
            for p, r in rates.items():
                simulate_consume(tanks, p, r["lbs_per_hour"] * seg)

    def _deliver(t):
        # Delegate to the shared simulator so app, advance_time.py,
        # projection, and planner all behave identically. Previously this
        # was a parallel copy that could (and did) drift.
        simulate_delivery_no_alert(tanks, t)
        log.append(f"  Delivered {t['sap_order']} — {t['product']} {t['quantity_lbs']:,} lbs")
        done.append(t["sap_order"])

    # Anchor snapshot at start so level_history has at least one
    # entry from this advance.
    _rls_app(data, clock)

    for ev_time, ev_type, payload in events:
        _consume(ev_time - clock)
        clock = ev_time
        _rls_app(data, clock)
        if   ev_type == "s": burning = True;  log.append(f"Plant running at {format_run_hour(data, ev_time)}")
        elif ev_type == "e": burning = False; log.append(f"Plant stopped at {format_run_hour(data, ev_time)}")
        elif ev_type == "d": _deliver(payload)
    _consume(end - clock)

    data["scheduled_trucks"] = [t for t in data["scheduled_trucks"] if t["sap_order"] not in done]
    data["current_run_hour"] = end
    # Final snapshot at end of advance.
    _rls_app(data, end)
    if done:
        log.append(f"{len(done)} truck(s) delivered and removed.")
    log.append(f"Clock now: {format_run_hour(data, end)}")

    # ── Email triggers ────────────────────────────────────────────────────────
    epoch        = datetime.fromisoformat(data["simulation_epoch"])
    old_dt       = epoch + timedelta(hours=start)
    new_dt       = epoch + timedelta(hours=end)
    email_events = []

    # Fire schedule reminder at Friday 11 AM and 3 PM sim time
    try:
        from email_hooks import send_friday_reminder_if_needed
        cfg = load_config()
        contact = cfg.get("anna_email", "") if cfg else ""
        check = old_dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        while check <= new_dt:
            if check.weekday() == 4 and check.hour in (11, 15):
                already = data.get("schedule_received_for_week")
                # calculate next Monday from this sim-Friday
                days_ahead = (7 - check.weekday()) % 7 or 7
                next_mon = (check + timedelta(days=days_ahead)).date().isoformat()
                if already != next_mon:
                    send_friday_reminder_if_needed(data, now_dt=check)
                    log.append(f"[Email] Schedule reminder sent at {check.strftime('%a %H:%M')}.")
                    email_events.append({
                        "sim_time": check.strftime("%a %Y-%m-%d %H:%M"),
                        "type":    "Schedule Reminder",
                        "to":      contact or "anna_email not configured",
                        "subject": "Schedule request",
                        "body":    "Hi,\n\nCan you please share next week's run schedule?\n\nThank you.",
                    })
                else:
                    log.append(f"[Email] Reminder check {check.strftime('%a %H:%M')} — schedule already received.")
            check += timedelta(hours=1)
    except Exception as e:
        log.append(f"[Email] Reminder error: {e}")

    # Send alert emails for any new alerts
    try:
        from email_hooks import send_alert_emails_if_new, alert_hash as _ah
        from alerts import get_all_alerts as _gaa
        prev_hashes = set(data.get("alerted_hashes", []))
        # Compute what WOULD be new BEFORE sending (so log is independent of send success)
        cur_alerts = _gaa(data)                            # list[dict]
        cur_map    = {_ah(a["text"]): a for a in cur_alerts}
        new_to_log = {h: cur_map[h] for h in cur_map if h not in prev_hashes}
        send_alert_emails_if_new(data)   # mutates data["alerted_hashes"] and data["alert_log"]
        if new_to_log:
            cfg  = load_config()
            dist = cfg.get("distribution_group", "") if cfg else ""
            preview = "\n\n".join(a["text"] for a in list(new_to_log.values())[:5])
            email_events.append({
                "sim_time": format_run_hour(data, end),
                "type":    f"Alert ({len(new_to_log)} new)",
                "to":      dist or "distribution_group not configured",
                "subject": f"VMI Alert ({len(new_to_log)} new)",
                "body":    "VMI ALERT\n" + "="*40 + "\n\n" + preview,
            })
    except Exception as e:
        log.append(f"[Email] Alert email error: {e}")

    # ── Autonomous: check inbox → apply schedule → plan → commit ─────────────────
    try:
        import io as _io, re as _re
        from read_schedule import fetch_and_apply_schedule as _fetch_sched
        from plan_orders import (
            plan_for_product, get_target_week_bounds,
            get_target_for_week, get_run_hours_in_window,
        )
        from email_hooks import send_cs_load_entry as _send_cs

        sim_now = new_dt
        # Determine the target week relative to sim time
        days_ahead = (7 - sim_now.weekday()) % 7 or 7
        next_mon_iso = (sim_now + timedelta(days=days_ahead)).date().isoformat()

        # Always check inbox — fetch_and_apply_schedule skips the last-used
        # email ID, so it's safe to call even on repeated advances.  It
        # returns "not_found" (silently) when no new email has arrived.
        sched_result = "not_found"   # default — overwritten if call succeeds
        captured = _io.StringIO()
        import sys as _sys
        _old_stdout = _sys.stdout
        _sys.stdout = captured
        try:
            sched_result = _fetch_sched(
                data, now_dt=sim_now, session_start_utc=session_start_utc
            )
        finally:
            _sys.stdout = _old_stdout
        fetch_log = captured.getvalue().strip()

        # Clear parse issue whenever no low-confidence email is present
        if sched_result != "low_confidence":
            data["schedule_parse_issue"] = None

        if sched_result == "applied":
            log.append(f"[Auto] Schedule email found — applied for week of {next_mon_iso}.")
            if fetch_log:
                for line in fetch_log.splitlines():
                    log.append(f"  {line}")

            # Build the new-windows summary for the email body
            # Only include windows that fall within the target week.
            sched_body_lines = ["Schedule applied:"]
            from time_utils import run_hour_to_dt as _rh2dt
            week_s, week_e = get_target_week_bounds(data)
            for w in sorted(data["run_schedule"], key=lambda x: x["start_hour"]):
                if not (week_s <= w["start_hour"] < week_e):
                    continue
                ws_dt = _rh2dt(data, w["start_hour"])
                we_dt = _rh2dt(data, w["end_hour"])
                sched_body_lines.append(
                    f"  {w.get('label', ws_dt.strftime('%a'))}: "
                    f"{ws_dt.strftime('%H:%M')} – {we_dt.strftime('%H:%M')}"
                )
            email_events.append({
                "sim_time": format_run_hour(data, end),
                "type":    "Schedule Applied",
                "to":      "— (system)",
                "subject": f"Schedule auto-applied for week of {next_mon_iso}",
                "body":    "\n".join(sched_body_lines),
                "status":  "applied",
            })

            # Auto-plan
            week_start, week_end = get_target_week_bounds(data)
            week_rh = get_run_hours_in_window(data, week_start, week_end)
            if week_rh > 0:
                target = get_target_for_week(week_rh, state=data)
                all_new = []
                for product in data["consumption_rates"]:
                    plan_cap = _io.StringIO()
                    _sys.stdout = plan_cap
                    try:
                        new = plan_for_product(
                            data, product, target, week_start, week_end, all_new,
                            cfg=st.session_state.cfg,
                        )
                    finally:
                        _sys.stdout = _old_stdout
                    for line in plan_cap.getvalue().strip().splitlines():
                        log.append(f"  [Planner] {line}")
                    all_new.extend(new)

                if all_new:
                    # Use the shared _next_sap which consults both
                    # scheduled_trucks AND the persistent sap_history,
                    # so delivered trucks' numbers can never be reused.
                    issued = set(data.get("sap_history", []))
                    issued.update(t["sap_order"] for t in data["scheduled_trucks"]
                                  if t.get("sap_order"))
                    nums = [
                        int(_re.search(r"\d+$", s).group())
                        for s in issued if _re.search(r"\d+$", s)
                    ]
                    next_n = max(nums) + 1 if nums else 20001
                    all_new.sort(key=lambda t: t["arrival_run_hour"])
                    for i, t in enumerate(all_new):
                        t["sap_order"] = f"SAP{next_n + i}"
                        t.pop("_planned_reason", None)
                        data["scheduled_trucks"].append(t)
                        _record_sap(data, t["sap_order"])
                    log.append(
                        f"[Auto] Committed {len(all_new)} truck order(s): "
                        + ", ".join(t["sap_order"] for t in all_new)
                    )

                    # CS load-entry email
                    cs_status = "queued"
                    try:
                        _send_cs(data, all_new)
                        cs_status = "sent"
                    except Exception as _cs_err:
                        cs_status = f"not sent ({_cs_err})"
                    cfg2 = load_config()
                    cs_addr = cfg2.get("cs_email", "") if cfg2 else ""
                    email_events.append({
                        "sim_time": format_run_hour(data, end),
                        "type":    "CS Load Entry",
                        "to":      cs_addr or "cs_email not configured",
                        "subject": f"Load Entry — {len(all_new)} auto-planned truck(s) "
                                   f"(week of {next_mon_iso})",
                        "body":    "\n".join(
                            f"{t['sap_order']} | {t['product']} | "
                            f"{t['quantity_lbs']:,} lbs | "
                            f"{format_run_hour(data, t['arrival_run_hour'])}"
                            for t in all_new
                        ),
                        "status":  cs_status,
                    })
                else:
                    log.append("[Auto] Planner: levels sufficient — no new trucks needed.")
            else:
                log.append("[Auto] No run hours scheduled for target week — skipping planner.")

        elif sched_result == "low_confidence":
            log.append("[Auto] Schedule email found but confidence too low to apply — manual review needed.")
            # Persist the issue so the Alerts section shows a warning
            from read_schedule import parse_schedule_text as _pst
            # re-capture day count from the fetch log if available
            import re as _re2
            m = _re2.search(r"(\d+) day", fetch_log)
            days_found = int(m.group(1)) if m else "?"
            data["schedule_parse_issue"] = {"days_found": days_found}
            if fetch_log:
                for line in fetch_log.splitlines():
                    log.append(f"  {line}")

        elif sched_result == "not_found" and fetch_log:
            # Surface diagnostics when something was checked but not applied —
            # helps during demos to confirm the inbox was reached and why the
            # email wasn't used (session filter, empty body, no schedule text…)
            for line in fetch_log.splitlines():
                log.append(f"  {line}")

    except Exception as e:
        log.append(f"[Auto] Schedule/plan error: {e}")

    return "\n".join(log), email_events


# ── Chart helpers ─────────────────────────────────────────────────────────────

def _short_label(dt_str):
    parts = dt_str.split()
    try:
        d = datetime.strptime(parts[1], "%Y-%m-%d")
        return f"{parts[0]}<br>{d.strftime('%b %d')}"
    except Exception:
        return parts[0]


def _chart(hist, product, safety=None, cutoff_run_hour=None):
    """Render per-product projection chart.

    cutoff_run_hour: if provided, x-values <= cutoff are drawn solid
    (operator-parsed schedule period) and x-values > cutoff are drawn
    dotted (forecast period). The two segments share a legend entry per
    tank. None = all-solid (back-compat for non-augmented callers)."""
    if safety is None:
        safety = SAFETY_STOCK_LBS
    # Filter tanks by product membership (hist["tank_product"][name] ==
    # product), not by tank-name prefix. The old `"U-"/"M-"` shortcut
    # only worked for the Acme demo's tank naming.
    tank_product_map = hist.get("tank_product") or {}
    tnks = [n for n in hist["tanks"] if tank_product_map.get(n) == product]
    x_vals = hist["run_hours"]          # numeric floats — safe for add_vline/vrect

    tick_idxs = list(range(0, len(x_vals), 24))
    if (len(x_vals) - 1) not in tick_idxs:
        tick_idxs.append(len(x_vals) - 1)
    tick_vals = [x_vals[i] for i in tick_idxs]
    tick_text = [_short_label(hist["datetimes"][i]) for i in tick_idxs]

    # Find split index for solid/dotted segments. split_idx is the
    # first x_vals index strictly greater than cutoff.
    #   None  → cutoff past chart end, entire chart solid
    #   0     → cutoff before chart start, entire chart dotted
    #   else  → solid up to split_idx (inclusive seam), dotted after
    split_idx = None
    if cutoff_run_hour is not None and x_vals:
        for i, x in enumerate(x_vals):
            if x > cutoff_run_hour:
                split_idx = i
                break

    fig = go.Figure()
    # Run windows — barely-there brand-blue tint per design system
    # (single-blue identity; no teal in the chrome).
    for w in hist["run_windows"]:
        fig.add_vrect(x0=w["start_hour"], x1=w["end_hour"],
                      fillcolor="rgba(30,64,175,0.06)", line_width=0)
    for name in tnks:
        color = _tank_color(name, tnks)
        y_vals = hist["tanks"][name]
        dts    = hist["datetimes"]
        if split_idx is None:
            # Single solid trace — no cutoff or cutoff past chart end
            fig.add_trace(go.Scatter(
                x=x_vals, y=y_vals, name=name,
                line=dict(color=color, width=2.5),
                customdata=dts,
                hovertemplate=f"<b>{name}</b><br>%{{customdata}}<br>%{{y:,.0f}} lbs<extra></extra>",
            ))
        elif split_idx == 0:
            # Cutoff before chart start (no future parsed windows) → all dotted
            fig.add_trace(go.Scatter(
                x=x_vals, y=y_vals, name=name,
                line=dict(color=color, width=2.5, dash="3 3"),
                customdata=dts,
                hovertemplate=f"<b>{name}</b> (forecast)<br>%{{customdata}}<br>%{{y:,.0f}} lbs<extra></extra>",
            ))
        else:
            # Solid up to split_idx, dotted from split_idx-1 onward.
            # The 1-point overlap at the seam keeps the line continuous.
            fig.add_trace(go.Scatter(
                x=x_vals[:split_idx], y=y_vals[:split_idx], name=name,
                line=dict(color=color, width=2.5),
                legendgroup=name,
                customdata=dts[:split_idx],
                hovertemplate=f"<b>{name}</b><br>%{{customdata}}<br>%{{y:,.0f}} lbs<extra></extra>",
            ))
            fig.add_trace(go.Scatter(
                x=x_vals[split_idx-1:], y=y_vals[split_idx-1:], name=name,
                line=dict(color=color, width=2.5, dash="3 3"),
                legendgroup=name, showlegend=False,
                customdata=dts[split_idx-1:],
                hovertemplate=f"<b>{name}</b> (forecast)<br>%{{customdata}}<br>%{{y:,.0f}} lbs<extra></extra>",
            ))
    # Vertical "forecast begins" marker at the cutoff
    if split_idx is not None and 0 < split_idx < len(x_vals):
        fig.add_vline(
            x=cutoff_run_hour,
            line_dash="dash", line_color="#94A3B8", line_width=1.0,
            annotation_text="forecast →",
            annotation_position="top right",
            annotation_font=dict(size=10, color="#64748B", family="Inter"),
        )
    # Safety-stock floor — dashed rose line per design spec
    fig.add_hline(
        y=safety, line_dash="dash", line_color="#F43F5E", line_width=1.2,
        annotation_text="Safety stock", annotation_position="bottom right",
        annotation_font=dict(size=10, color="#9F1239", family="Inter"),
    )
    for ev in hist["truck_events"]:
        if ev["product"] != product:
            continue
        # Forecast trucks (run_hour > cutoff OR sap starts with FORECAST-)
        # render as dotted, slightly fainter, with "(fcst)" annotation
        # so the scheduler can clearly distinguish prospective from
        # actual deliveries. Real trucks keep the dashed amber treatment.
        is_forecast = (
            (cutoff_run_hour is not None and ev["run_hour"] > cutoff_run_hour)
            or str(ev.get("sap", "")).startswith("FORECAST-")
        )
        if is_forecast:
            fig.add_vline(
                x=ev["run_hour"],
                line_dash="3 3", line_color="#F59E0B", line_width=1.0,
                opacity=0.7,
                annotation_text=f"+{ev['qty'] // 1000}k (fcst)",
                annotation_position="top left",
                annotation_font=dict(size=10, color="#B45309",
                                       family="Inter"),
            )
        else:
            fig.add_vline(
                x=ev["run_hour"],
                line_dash="dash", line_color="#F59E0B", line_width=1.2,
                annotation_text=f"{ev['sap']} +{ev['qty'] // 1000}k",
                annotation_position="top left",
                annotation_font=dict(size=10, color="#92400E",
                                       family="Inter"),
            )
    fig.update_layout(
        title=dict(
            text=product,
            font=dict(size=11, family="Inter", color="#1E2A45"),
            x=0.01, xanchor="left",
        ),
        height=560,
        margin=dict(l=5, r=5, t=34, b=44),
        font=dict(family="Inter", color="#1E2A45", size=11),
        yaxis=dict(
            range=[0, 37000], tickformat=",", title="lbs", dtick=10000,
            gridcolor="#E2E8F0", gridwidth=1, zeroline=False,
            title_font=dict(size=11, color="#64748B"),
            tickfont=dict(size=10, color="#64748B"),
        ),
        xaxis=dict(
            tickmode="array", tickvals=tick_vals, ticktext=tick_text, tickangle=-30,
            showgrid=False, zeroline=False,
            tickfont=dict(size=10, color="#64748B"),
        ),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
            font=dict(size=10, family="Inter", color="#1E2A45"),
            bgcolor="rgba(0,0,0,0)",
        ),
        plot_bgcolor="white", paper_bgcolor="white",
        hoverlabel=dict(font_family="Inter", font_size=11, bgcolor="#FFFFFF",
                        bordercolor="#E2E8F0"),
    )
    return fig


def _tank_info(col, name, info, level_history=(), current_run_hour=0.0):
    """Animated tank-fill card. SVG silhouette of a cylinder with the
    fluid level animated between renders (CSS transition on the
    fluid-rect's y / height). Color semantically encodes fill level:
    red < 20%, amber < 50%, green/teal otherwise. A subtle wave
    (animated SVG sinusoid) on top of the fluid sells the "real
    liquid" feel.

    level_history / current_run_hour: when provided, a small age chip
    is shown below the level — hours since the tank last dipped below
    2 000 lbs, indicating how long material has been in the tank.
    """
    pct      = info["current_level_lbs"] / info["max_capacity_lbs"]
    pct_clip = max(0.0, min(1.0, pct))
    if pct < 0.2:
        fluid_color, dot_color = "#F43F5E", "#F43F5E"   # critical → red
        fluid_dark             = "#BE123C"
    elif pct < 0.5:
        fluid_color, dot_color = "#F59E0B", "#F59E0B"   # low → amber
        fluid_dark             = "#B45309"
    else:
        fluid_color, dot_color = "#0EA5E9", "#22C55E"   # healthy → blue
        fluid_dark             = "#0369A1"
    is_draw = info["status"] == "draw"
    chip_kind = "draw" if is_draw else "standby"

    # ── Inventory age chip ─────────────────────────────────────────────
    # How long has material been sitting in this tank? Resets whenever
    # the tank dips below 2 000 lbs (proxy for near-empty = fresh fill).
    _age_html = ""
    if level_history and _inv_age is not None:
        _age_h, _capped = _inv_age(name, level_history, current_run_hour)
        if _capped and _age_h < 1:
            _age_str = "—"        # no history → nothing meaningful to show
        else:
            _days = int(_age_h) // 24
            _hrs  = int(_age_h) % 24
            if _days >= 1:
                _age_str = f"{_days}d+" if _capped else f"{_days}d"
            else:
                _age_str = f"{_hrs}h+" if _capped else f"{_hrs}h"
        if _age_str != "—":
            _age_html = (
                f'<span style="font-size:0.68rem;color:var(--vmi-text-secondary);'
                f'font-weight:500;margin-top:0.15rem;display:inline-block;" '
                f'title="Material age in tank (resets when level &lt; 2 000 lbs)">'
                f'⏱ {_age_str}</span>'
            )

    # SVG geometry: 60×80 viewBox, tank silhouette inset 4px on each
    # side. Fluid fills from the bottom; height encodes fill level.
    SVG_W, SVG_H = 60, 80
    INSET_X, INSET_TOP, INSET_BOT = 6, 8, 6
    tank_left   = INSET_X
    tank_top    = INSET_TOP
    tank_w      = SVG_W - 2 * INSET_X
    tank_h      = SVG_H - INSET_TOP - INSET_BOT
    fluid_h     = tank_h * pct_clip
    fluid_y     = tank_top + (tank_h - fluid_h)

    # Unique IDs per tank so multiple inline SVGs don't collide on
    # gradient / clip-path defs. Strip every non-[A-Za-z0-9_] char
    # — apostrophes, slashes, dots, parentheses in tank names would
    # break the SVG `clip-path="url(#clip_NAME)"` reference and the
    # animation would silently fail. Prefix with "t_" so a name
    # starting with a digit (e.g. "1-North") still produces a valid
    # SVG id (which must start with a letter or _).
    safe_id = "t_" + re.sub(r'[^A-Za-z0-9_]', '_', name)

    # Static surface wave — adds depth without the constant left/right
    # translateX animation that previously made the tank cards "shift"
    # in peripheral vision (operator complaint). Wave only renders
    # when fluid_h > 4 (otherwise it'd clip).
    wave_svg = ""
    if fluid_h > 4:
        # Same dedent rule as the parent SVG — single-line so Streamlit
        # markdown doesn't render this as a code block.
        wave_svg = (
            f'<path d="M {tank_left} {fluid_y} '
            f'Q {tank_left + tank_w * 0.25} {fluid_y - 2}, '
            f'{tank_left + tank_w * 0.5} {fluid_y} '
            f'T {tank_left + tank_w} {fluid_y} '
            f'L {tank_left + tank_w} {tank_top + tank_h} '
            f'L {tank_left} {tank_top + tank_h} Z" '
            f'fill="{fluid_dark}" opacity="0.4" '
            f'clip-path="url(#clip_{safe_id})"/>'
        )

    # SVG kept on a single line — Streamlit's markdown rendering
    # treats lines indented 4+ spaces as a code block, which would
    # render the SVG markup as TEXT below the tank card. Building
    # the SVG without leading whitespace dodges that interaction.
    svg = (
        f'<svg viewBox="0 0 {SVG_W} {SVG_H}" class="vmi-tank-svg" '
        f'width="56" height="74" xmlns="http://www.w3.org/2000/svg">'
        f'<defs><clipPath id="clip_{safe_id}">'
        f'<rect x="{tank_left}" y="{tank_top}" rx="3" ry="3" '
        f'width="{tank_w}" height="{tank_h}"/></clipPath></defs>'
        f'<rect x="{tank_left}" y="{tank_top}" rx="3" ry="3" '
        f'width="{tank_w}" height="{tank_h}" fill="#F8FAFC" '
        f'stroke="#CBD5E1" stroke-width="1.5"/>'
        f'<rect class="vmi-tank-fluid" x="{tank_left}" y="{fluid_y}" '
        f'width="{tank_w}" height="{fluid_h}" fill="{fluid_color}" '
        f'opacity="0.85" clip-path="url(#clip_{safe_id})"/>'
        f'{wave_svg}'
        f'<ellipse cx="{tank_left + tank_w / 2}" cy="{tank_top}" '
        f'rx="{tank_w / 2}" ry="2.5" fill="#E2E8F0" stroke="#CBD5E1" '
        f'stroke-width="1"/>'
        f'</svg>'
    )

    col.markdown(f"""
    <div class="vmi-tank-card" style="margin-bottom:0.4rem;">
        <div style="display:flex;gap:0.75rem;align-items:flex-start;">
            <div style="flex:0 0 56px;">{svg}</div>
            <div style="flex:1 1 auto;min-width:0;">
                <div style="display:flex;align-items:center;
                            justify-content:space-between;
                            gap:0.4rem;">
                    <div style="display:flex;align-items:center;gap:0.4rem;
                                min-width:0;">
                        <span style="display:inline-block;width:8px;
                                     height:8px;border-radius:50%;
                                     background:{dot_color};
                                     flex:0 0 8px;"></span>
                        <span style="font-weight:600;color:var(--vmi-text-primary);
                                     font-size:0.88rem;
                                     overflow:hidden;text-overflow:ellipsis;
                                     white-space:nowrap;">{name}</span>
                    </div>
                    {_chip_html('DRAW' if is_draw else 'STANDBY', chip_kind)}
                </div>
                <div style="margin-top:0.35rem;color:var(--vmi-text-secondary);
                            font-size:0.78rem;">
                    <span class="vmi-num" style="color:var(--vmi-text-primary);
                                                  font-size:1.05rem;
                                                  font-weight:600;">
                        {info['current_level_lbs']:,.0f}
                    </span>
                    <span style="color:var(--vmi-text-secondary);font-size:0.72rem;">
                        / {info['max_capacity_lbs']:,} lbs · {pct*100:.0f}%
                    </span>
                </div>
                {_age_html}
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ── Natural-language truck parser ─────────────────────────────────────────────

_DAY_NL = {
    "monday": 0, "mon": 0,
    "tuesday": 1, "tue": 1, "tues": 1,
    "wednesday": 2, "wed": 2,
    "thursday": 3, "thu": 3, "thur": 3, "thurs": 3,
    "friday": 4, "fri": 4,
    "saturday": 5, "sat": 5,
    "sunday": 6, "sun": 6,
}


def _product_aliases(product_name):
    """Tokens an operator might type to refer to `product_name`."""
    aliases = [product_name.lower()]
    # "Product U" → also accept "U"
    m = re.match(r"product\s+(\S+)", product_name, re.IGNORECASE)
    if m:
        aliases.append(m.group(1).lower())
    return aliases


def _parse_nl(text, data):
    tl = text.lower().strip()
    products = list(data.get("truck_quantities", {}).keys())
    product = None
    for p in products:
        for alias in _product_aliases(p):
            if re.search(r"\b" + re.escape(alias) + r"\b", tl):
                product = p
                break
        if product:
            break
    if product is None:
        if products:
            quoted = ", ".join(f"'{p}'" for p in products)
            raise ValueError(f"Specify a product: {quoted}.")
        raise ValueError("No products configured for this customer.")
    day_num = None
    for word, num in _DAY_NL.items():
        if re.search(r"\b" + word + r"\b", tl):
            day_num = num
            break
    if day_num is None:
        raise ValueError("Specify a day: Monday, Tuesday, Wednesday, Thursday, or Friday.")
    time_hour = None
    m = re.search(r"\b(\d{1,2}):(\d{2})\b", text)
    if m:
        time_hour = int(m.group(1))
    if time_hour is None:
        m = re.search(r"\b([01]\d|2[0-3])([0-5]\d)\b", text)
        if m:
            time_hour = int(m.group(1))
    if time_hour is None:
        m = re.search(r"\b(\d{1,2})\s*(am|pm)\b", tl)
        if m:
            h, ap = int(m.group(1)), m.group(2)
            if ap == "pm" and h != 12: h += 12
            elif ap == "am" and h == 12: h = 0
            time_hour = h
    if time_hour is None:
        raise ValueError("Specify a time: 0800, 08:00, or 8am.")
    epoch  = datetime.fromisoformat(data["simulation_epoch"])
    min_dt = epoch + timedelta(hours=data["current_run_hour"] + 48)
    days_to = (day_num - min_dt.weekday()) % 7
    cand    = (min_dt + timedelta(days=days_to)).replace(
        hour=time_hour, minute=0, second=0, microsecond=0
    )
    if cand < min_dt:
        cand += timedelta(weeks=1)
    arr_rh = (cand - epoch).total_seconds() / 3600.0
    return product, arr_rh, cand.strftime("%a %Y-%m-%d %H:%M")


def _next_sap(data, cfg=None):
    """Return the next SAP order number, monotonic across all time.

    Looks at BOTH currently-scheduled trucks AND the persistent
    `sap_history` list (every SAP number ever issued, never pruned).
    Without the history check, delivered/pruned trucks free up their
    SAP numbers and the next planner cycle re-issues the same string —
    which collides with the prior real-life delivered order.

    The output format and seed integer come from PlantConfig
    (cfg.sap_order_format, cfg.sap_order_seed) so customers using a
    non-default ERP numbering scheme don't need to fork app.py.
    """
    if cfg is None:
        from config import DEFAULT_CONFIG
        cfg = DEFAULT_CONFIG
    issued = set(data.get("sap_history", []))
    issued.update(t["sap_order"] for t in data["scheduled_trucks"]
                  if t.get("sap_order"))
    nums = [int(re.search(r"\d+$", s).group())
            for s in issued if re.search(r"\d+$", s)]
    next_n = max(nums) + 1 if nums else cfg.sap_order_seed
    return cfg.sap_order_format.format(n=next_n)


def _record_sap(data, sap_order):
    """Append `sap_order` to data['sap_history'] (deduped, kept sorted)."""
    if not sap_order:
        return
    hist = set(data.get("sap_history", []))
    hist.add(sap_order)
    data["sap_history"] = sorted(hist)


# ═════════════════════════════════════════════════════════════════════════════
# PAGE
# ═════════════════════════════════════════════════════════════════════════════

st.markdown("""
<style>
/* ── Google Font ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* ── Base ── */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* ── App background ── */
.stApp {
    background-color: #F7F9FC;
}

/* ── Main content area ── */
section.main > div {
    padding-top: 1.2rem;
}

/* ── Headings ── */
h1 { color: #0F1629 !important; font-weight: 700 !important; letter-spacing: -0.5px; }
h2 { color: #0F1629 !important; font-weight: 600 !important; }
h3 {
    color: #1E2A45 !important;
    font-weight: 600 !important;
    border-left: 3px solid #00C7A9;
    padding-left: 0.55rem;
    margin-top: 0.2rem !important;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background-color: #FFFFFF;
    border-right: 1px solid #E2E8F0;
}

/* ── Primary buttons → teal ── */
button[kind="primary"], .stDownloadButton > button[kind="primary"] {
    background-color: #00C7A9 !important;
    border: none !important;
    color: #0F1629 !important;
    font-weight: 600 !important;
    font-family: 'Inter', sans-serif !important;
    border-radius: 6px !important;
    letter-spacing: 0.01em;
}
button[kind="primary"]:hover, .stDownloadButton > button[kind="primary"]:hover {
    background-color: #00B09A !important;
    box-shadow: 0 2px 8px rgba(0,199,169,0.35) !important;
}

/* ── Secondary / default buttons ── */
button[kind="secondary"] {
    background-color: #FFFFFF !important;
    border: 1.5px solid #CBD5E1 !important;
    color: #1E2A45 !important;
    font-weight: 500 !important;
    font-family: 'Inter', sans-serif !important;
    border-radius: 6px !important;
}
button[kind="secondary"]:hover {
    border-color: #00C7A9 !important;
    color: #00C7A9 !important;
}

/* ── Link buttons ── */
a[data-testid="stLinkButton"] > button {
    background-color: #FFFFFF !important;
    border: 1.5px solid #CBD5E1 !important;
    color: #1E2A45 !important;
    font-weight: 500 !important;
    font-family: 'Inter', sans-serif !important;
    border-radius: 6px !important;
}
a[data-testid="stLinkButton"] > button:hover {
    border-color: #00C7A9 !important;
    color: #00C7A9 !important;
}

/* ── Expanders ── */
details {
    background-color: #FFFFFF;
    border: 1px solid #E2E8F0 !important;
    border-radius: 8px !important;
    margin-bottom: 0.6rem;
}
details > summary {
    font-weight: 600;
    color: #1E2A45;
    padding: 0.6rem 0.8rem;
}

/* ── Inputs and selects ── */
input[type="number"], input[type="text"], textarea, .stSelectbox > div {
    border-radius: 6px !important;
    font-family: 'Inter', sans-serif !important;
}

/* ── Dataframes / tables ── */
.stDataFrame {
    border-radius: 8px !important;
    overflow: hidden;
    border: 1px solid #E2E8F0 !important;
}
.stDataFrame thead tr th {
    background-color: #F1F5F9 !important;
    color: #0F1629 !important;
    font-weight: 600 !important;
    font-size: 0.82rem !important;
    letter-spacing: 0.04em;
    text-transform: uppercase;
}

/* ── Metrics ── */
[data-testid="stMetric"] {
    background-color: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-radius: 8px;
    padding: 0.75rem 1rem;
}
[data-testid="stMetric"] label {
    color: #64748B !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}
[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #0F1629 !important;
    font-weight: 700 !important;
}

/* ── Alert / info boxes ── */
div[data-testid="stAlert"] {
    border-radius: 8px !important;
    font-family: 'Inter', sans-serif !important;
}

/* ── Success boxes ── */
div[data-testid="stAlert"][kind="success"] {
    background-color: #F0FDF4 !important;
    border-left: 4px solid #22C55E !important;
    color: #14532D !important;
}

/* ── Warning boxes ── */
div[data-testid="stAlert"][kind="warning"] {
    background-color: #FFFBEB !important;
    border-left: 4px solid #F59E0B !important;
    color: #92400E !important;
}

/* ── Error boxes ── */
div[data-testid="stAlert"][kind="error"] {
    background-color: #FFF1F2 !important;
    border-left: 4px solid #F43F5E !important;
    color: #9F1239 !important;
}

/* ── Info boxes ── */
div[data-testid="stAlert"][kind="info"] {
    background-color: #F0F9FF !important;
    border-left: 4px solid #00C7A9 !important;
    color: #155E75 !important;
}

/* ── Caption / helper text ── */
.stCaption, small {
    color: #64748B !important;
    font-size: 0.82rem !important;
}

/* ── Horizontal rule ── */
hr {
    border: none;
    border-top: 1px solid #E2E8F0;
    margin: 1rem 0;
}

/* ── Code blocks ── */
code {
    background-color: #F1F5F9 !important;
    color: #0F1629 !important;
    border-radius: 4px !important;
    font-size: 0.85em !important;
    padding: 0.1em 0.35em !important;
}

/* ── Divider between major sections ── */
.section-divider {
    border: none;
    border-top: 2px solid #E2E8F0;
    margin: 1.5rem 0 1rem 0;
}

/* ── Subtle card container ── */
.vmi-card {
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-radius: 10px;
    padding: 1rem 1.25rem;
    margin-bottom: 0.75rem;
}

/* ── Small uppercase section label (used for inline sub-section headers) ── */
.vmi-label {
    font-family: 'Inter', sans-serif;
    font-size: 0.72rem;
    font-weight: 600;
    color: #64748B;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 0.35rem;
    margin-top: 0.1rem;
}

/* ── Sim time pill ── */
.vmi-simtime {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-left: 3px solid #00C7A9;
    border-radius: 6px;
    padding: 4px 10px;
    font-family: 'Inter', sans-serif;
    font-size: 0.85rem;
    color: #0F1629;
    margin-bottom: 0.4rem;
}
.vmi-simtime .lbl {
    font-size: 0.66rem;
    font-weight: 600;
    color: #64748B;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}
.vmi-simtime .val {
    font-family: 'JetBrains Mono', 'Menlo', 'Consolas', monospace;
    font-weight: 600;
    color: #0F1629;
}
</style>
""", unsafe_allow_html=True)


# ── First-run guided demo ─────────────────────────────────────────────────────
# Three-step modal walking a new user through risk → schedule parsed →
# trucks ordered, using LIVE data on a deep copy so real state isn't
# mutated unless the user clicks "Apply to my dashboard". Triggered once
# per install via the `first_run_tour_complete` flag in data.json.

def _demo_advance_clock_to_friday(_ddata):
    """Bump _ddata['current_run_hour'] to the next Friday 11:00 sim-time
    WITHOUT running the side-effecting _advance() function (which would
    poll the inbox, fire reminders, etc.). Pure clock arithmetic."""
    _now_dt = run_hour_to_dt(_ddata, _ddata["current_run_hour"])
    days_until = (4 - _now_dt.weekday()) % 7   # 4 = Friday
    if days_until == 0 and _now_dt.hour >= 11:
        days_until = 7
    _target = _now_dt.replace(hour=11, minute=0, second=0, microsecond=0) \
              + timedelta(days=days_until)
    _delta_h = (_target - _now_dt).total_seconds() / 3600.0
    _ddata["current_run_hour"] = float(_ddata["current_run_hour"]) + _delta_h


def _demo_render_risk():
    st.markdown("**Step 1 of 3 — ⚠️ Risk Detected**")
    st.markdown(
        "Your tank levels are dropping. The system has already flagged it and "
        "alerted the larger distribution group by email — scheduler, backup, "
        "operations, and shipping."
    )
    st.divider()

    # Current alerts on the LIVE data (these are real, not simulated)
    try:
        _live_alerts = get_all_alerts(st.session_state.data, cfg=st.session_state.cfg)
    except Exception:
        _live_alerts = []
    if _live_alerts:
        for alert in _live_alerts[:3]:
            _sev = (alert.get("severity") or "").upper()
            _txt = alert.get("text") or alert.get("message") or str(alert)
            st.error(f"**{_sev}** — {_txt}")
    else:
        # No active alerts — show current product totals as a fallback
        _prod_levels = {}
        for tname, tinfo in st.session_state.data.get("tanks", {}).items():
            prod = tinfo.get("product", "")
            _prod_levels.setdefault(prod, 0.0)
            _prod_levels[prod] += float(tinfo.get("current_level_lbs", 0.0))
        _cols = st.columns(max(len(_prod_levels), 1))
        for _c, (prod, lvl) in zip(_cols, _prod_levels.items()):
            with _c:
                _delta = lvl - SAFETY_STOCK_LBS
                st.metric(prod, f"{lvl:,.0f} lbs",
                           delta=f"{_delta:+,.0f} vs safety stock",
                           delta_color="inverse")

    st.caption(
        "The dashboard watches inventory 24/7 and projects forward. When a "
        "product is on track to dip below safety stock, you see it here "
        "before it becomes a problem."
    )
    st.divider()
    _b1, _spacer, _b2 = st.columns([2, 4, 3])
    with _b1:
        if st.button("Skip — just explore", use_container_width=True,
                      key="demo_skip_step0"):
            _demo_finalize(apply=False)
    with _b2:
        if st.button("Next: Schedule arrives →", type="primary",
                      use_container_width=True, key="demo_next_step0"):
            st.session_state._demo_step = 1
            st.rerun()


def _demo_render_parse():
    _ddata = st.session_state._demo_data
    # Bump the demo copy's clock to Friday 11:00 (narrative device only)
    if not st.session_state.get("_demo_advanced"):
        try:
            _demo_advance_clock_to_friday(_ddata)
        except Exception:
            pass
        st.session_state._demo_advanced = True

    # Parse a sample operator email (regex handles HIGH-confidence text)
    if "_demo_parsed" not in st.session_state:
        SIM_TEXT = (
            "Monday 6am-10pm, Tuesday 6am-10pm,\n"
            "Wednesday 6am-2pm, Thursday off,\n"
            "Friday 6am-2pm"
        )
        try:
            _result = parse_schedule(SIM_TEXT)
            if len(_result) == 4:
                entries, conf, notes, method = _result
            else:
                entries, conf, notes = _result
                method = "regex"
        except Exception:
            entries, conf, notes = parse_schedule_text(SIM_TEXT)
            method = "regex"
        st.session_state._demo_parsed = (SIM_TEXT, entries, conf, notes, method)

    SIM_TEXT, entries, conf, notes, method = st.session_state._demo_parsed

    st.markdown("**Step 2 of 3 — 📧 Schedule Received & Parsed**")
    st.markdown(
        "It's now Friday morning. The system sent the weekly reminder; the "
        "customer replied with next week's schedule. Here's the email body:"
    )
    st.code(SIM_TEXT, language=None)
    st.success(f"✓ Parsed with **{conf.upper()}** confidence (method: `{method}`)")

    if entries:
        _DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        _rows = [
            {"Day": _DAY_NAMES[wd] if 0 <= wd < 7 else str(wd),
             "Start": f"{int(sh):02d}:00",
             "End":   f"{int(eh):02d}:00"}
            for wd, sh, eh in entries
        ]
        st.dataframe(_rows, hide_index=True, use_container_width=True)

    st.divider()
    _b1, _spacer, _b2 = st.columns([2, 4, 3])
    with _b1:
        if st.button("← Back", use_container_width=True, key="demo_back_step1"):
            st.session_state._demo_step = 0
            st.rerun()
    with _b2:
        if st.button("Next: Truck recommendation →", type="primary",
                      use_container_width=True, key="demo_next_step1"):
            st.session_state._demo_step = 2
            st.rerun()


def _demo_render_plan():
    _ddata = st.session_state._demo_data

    if "_demo_planned" not in st.session_state:
        _, entries, _, _, _ = st.session_state._demo_parsed
        try:
            _now_dt = run_hour_to_dt(_ddata, _ddata["current_run_hour"])
            _ddata, _, _ = apply_schedule_to_data(
                _ddata, entries, now_dt=_now_dt, mode="replace",
            )
            ws, we = get_target_week_bounds(_ddata)
            wrh = get_run_hours_in_window(_ddata, ws, we)
            tgt = get_target_for_week(wrh, state=_ddata)
            _planned = []
            for prod in _ddata.get("consumption_rates", {}):
                _planned.extend(plan_for_product(
                    _ddata, prod, tgt, ws, we, _planned,
                    cfg=st.session_state.cfg,
                ))
            _ddata["scheduled_trucks"] = (
                list(_ddata.get("scheduled_trucks", [])) + _planned
            )
            st.session_state._demo_data = _ddata
            st.session_state._demo_planned = _planned
        except Exception as e:
            st.error(f"Could not run planner on demo data: {e}")
            st.session_state._demo_planned = []

    planned = st.session_state._demo_planned

    st.markdown("**Step 3 of 3 — 🚛 Trucks Recommended**")
    if planned:
        st.markdown(
            "The planner ran against the parsed schedule and current tank "
            "levels. Here's what it would order to keep you above safety "
            "stock. The schedule parsed with HIGH confidence, so these "
            "trucks would be entered automatically."
        )
        for truck in planned:
            try:
                _arr_dt = run_hour_to_dt(_ddata, truck.get("arrival_run_hour", 0))
                _arr_str = _arr_dt.strftime("%a %b %d, %H:%M")
            except Exception:
                _arr_str = f"run-hour {truck.get('arrival_run_hour', 0):.1f}"
            st.info(
                f"**{truck.get('product', '?')}** — "
                f"{int(truck.get('quantity_lbs', 0)):,} lbs — "
                f"arriving **{_arr_str}**"
            )
    else:
        st.success(
            "Levels are sufficient — no trucks needed for the target week. "
            "(The system orders only what's required.)"
        )

    st.caption(
        "Click **Apply to my dashboard** to commit this parsed schedule and "
        "the recommended trucks to your real state, or **Skip** to explore "
        "from your current state."
    )
    st.divider()
    _b1, _b2, _b3 = st.columns([2, 3, 3])
    with _b1:
        if st.button("← Back", use_container_width=True, key="demo_back_step2"):
            st.session_state._demo_step = 1
            st.rerun()
    with _b2:
        if st.button("Skip — just explore",
                      use_container_width=True, key="demo_skip_step2"):
            _demo_finalize(apply=False)
    with _b3:
        if st.button("✓ Apply to my dashboard", type="primary",
                      use_container_width=True, key="demo_apply_step2"):
            _demo_finalize(apply=True)


def _demo_finalize(*, apply: bool):
    """Mark tour complete, optionally swap demo copy in, save, clean up."""
    if apply and "_demo_data" in st.session_state:
        st.session_state.data = st.session_state._demo_data
    st.session_state.data["first_run_tour_complete"] = True
    try:
        _save_active_state(st.session_state.data)
    except Exception as e:
        import sys
        print(f"[demo_finalize save] {e}", file=sys.stderr)
    for k in ("_demo_step", "_demo_data", "_demo_advanced",
              "_demo_parsed", "_demo_planned"):
        st.session_state.pop(k, None)
    st.rerun()


@st.dialog("🏭 VMI Command Center — Quick Tour", width="large")
def _demo_tour():
    if "_demo_step" not in st.session_state:
        st.session_state._demo_step = 0
    if "_demo_data" not in st.session_state:
        st.session_state._demo_data = copy.deepcopy(st.session_state.data)
    _step = st.session_state._demo_step
    if _step == 0:
        _demo_render_risk()
    elif _step == 1:
        _demo_render_parse()
    else:
        _demo_render_plan()


# Trigger the tour on first install. The decorator opens the modal when
# the function is called; it pauses the rest of the page render until
# dismissed via Apply/Skip (which set first_run_tour_complete and rerun).
# Guarded by st.runtime.exists() so `import app` in tests doesn't try to
# open a dialog without a script context (which raises StreamlitAPIException).
def _streamlit_runtime_active():
    try:
        import streamlit.runtime as _rt
        return _rt.exists()
    except Exception:
        return False

if (st.session_state.get("current_customer", "acme") == "acme"
        and not data.get("first_run_tour_complete", False)
        and _streamlit_runtime_active()):
    _demo_tour()


# Header — title row with Codebase tucked top-right, then centered Product Sheet CTA below
_h_left, _h_right = st.columns([6, 1])
with _h_left:
    st.markdown("""
    <div style="padding:0.25rem 0 0;">
        <div style="font-size:1.6rem;font-weight:700;color:#0F1629;
                    font-family:'Inter',sans-serif;letter-spacing:-0.5px;
                    line-height:1.1;">
            🏭 &nbsp;VMI Automation
        </div>
        <div style="margin-top:0.2rem;font-size:0.85rem;color:#64748B;
                    font-family:'Inter',sans-serif;">
            Vendor-Managed Inventory — tank simulation, auto-planning, schedule parsing, alert emails
        </div>
    </div>
    """, unsafe_allow_html=True)
with _h_right:
    # Sim-time pill (design system signature) + Codebase link below.
    from theme import simtime_pill_html as _simtime_pill_html
    _sim_str = format_run_hour(data, data["current_run_hour"])
    st.markdown(
        f'<div style="text-align:right;margin-bottom:0.4rem;">'
        f'{_simtime_pill_html(_sim_str)}</div>',
        unsafe_allow_html=True,
    )
    st.link_button("💻 Codebase", GITHUB_URL, use_container_width=True)

# Centered Product Sheet button (~18% page width = 2.5× the previous 1/14)
_ps_l, _ps_c, _ps_r = st.columns([4, 2, 4])
with _ps_c:
    _pdf_bytes = _load_product_sheet()
    if _pdf_bytes:
        st.download_button(
            "📄 Product Sheet",
            data=_pdf_bytes,
            file_name="VMI_Automation.pdf",
            mime="application/pdf",
            use_container_width=True,
            type="primary",
        )
    else:
        st.button(
            "📄 Product Sheet",
            disabled=True,
            use_container_width=True,
            help="Asset missing — re-add assets/product_sheet.pdf.",
        )

st.markdown(
    '<div style="border-bottom:1px solid #E2E8F0;margin:0.6rem 0 1rem 0;"></div>',
    unsafe_allow_html=True,
)

with st.expander("ℹ️ Workflow guide"):
    st.markdown(f"""
**Typical demo flow:**

1. **Roll forward to Thursday or Friday** using *Advance Clock*. This simulates time passing with consumption during scheduled run windows.
2. **Set tank levels** (top-left) to a realistic mid-week inventory, then click *Apply Tank Levels*.
3. **Enter next week's run schedule** — two ways:
   - **Email (realistic):** Send the schedule to **vmiprototype@gmail.com**, then **advance at least 1 hour** — the system checks the inbox, parses the windows with AI, applies the schedule, and places orders automatically. No other steps needed.
   - **Schedule Parser (manual/testing):** Paste the schedule text, click *Parse* → *Apply to Schedule*, then use *Plan Next Week* to place orders.
4. **Auto-plan trucks** — if using the manual parser, click *Plan Next Week* after applying the schedule. The planner projects when each product breaches its reorder target and proposes deliveries with reasons. Click *Commit Trucks* to confirm (SAP numbers auto-assigned). A CS load-entry PDF is emailed automatically.
5. **Alerts** fire automatically as the projection detects problems. An email goes to the distribution group on first occurrence.
6. **Schedule reminder** — rolling the clock past **Friday 11 AM** (sim time) without a schedule on file automatically emails the customer contact. A second reminder fires at **3 PM**. No manual steps needed — just advance the clock.
7. **CS load-entry email** — committed trucks generate a PDF emailed to CS, also shown at the bottom of this page.

**Key rules:**
- Truck deliveries are snapped to **06:00, 08:00, or 14:00** (Mon–Fri, inside a run window, ≥ 48 h ahead). No two trucks may arrive in the same slot. Overfill is never allowed — the planner skips a slot rather than overfill.
- Apply next week's schedule *before* the week starts (Thursday or Friday is ideal).
- Reorder target scales from **{TARGET_LOW_LBS:,} lbs** (light week, {TARGET_LOW_RUN_HOURS} run hrs)
  to **{TARGET_HIGH_LBS:,} lbs** (heavy week, {TARGET_HIGH_RUN_HOURS} run hrs).
""")

# ── Controls ──────────────────────────────────────────────────────────────────

cl, cr = st.columns([3, 2])

with cl:
    st.markdown('<div class="vmi-label">Tank Levels (lbs)</div>', unsafe_allow_html=True)
    row1 = st.columns(2)
    row2 = st.columns(2)
    tank_names = list(data["tanks"].keys())
    tank_vals  = {}
    for col, name in zip(row1, [tank_names[0], tank_names[2]]):
        tank_vals[name] = col.number_input(
            name, min_value=0, max_value=35000,
            value=int(data["tanks"][name]["current_level_lbs"]), step=500,
            key=f"ti_{name}",
        )
    for col, name in zip(row2, [tank_names[1], tank_names[3]]):
        tank_vals[name] = col.number_input(
            name, min_value=0, max_value=35000,
            value=int(data["tanks"][name]["current_level_lbs"]), step=500,
            key=f"ti_{name}",
        )
    if st.button("Apply Tank Levels", use_container_width=True):
        _prev_levels = {n: data["tanks"][n]["current_level_lbs"]
                         for n in tank_vals}
        for name, val in tank_vals.items():
            data["tanks"][name]["current_level_lbs"] = float(val)
        _audit.record(data, _audit.A_TANK_LEVELS_APPLY,
                       details={"prev": _prev_levels,
                                 "new": {n: float(v) for n, v in tank_vals.items()}})
        st.success("Updated.")
        st.rerun()

with cr:
    now_label = format_run_hour(data, data["current_run_hour"])
    st.markdown(
        f'<div class="vmi-simtime">'
        f'<span class="lbl">Sim time</span>'
        f'<span class="val">{now_label}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
    adv_col, go_col, rst_col = st.columns([2, 1, 1])
    adv_hrs = adv_col.number_input("hrs", min_value=1, max_value=720, value=8, step=1,
                                    label_visibility="collapsed")
    if go_col.button("▶ Advance", type="primary", use_container_width=True):
        log, evts = _advance(
            data, float(adv_hrs),
            session_start_utc=st.session_state.session_start_real_utc,
        )
        st.session_state.advance_log = log
        st.session_state.email_log.extend(evts)
        _audit.record(st.session_state.data, _audit.A_ADVANCE,
                       details={"hours": float(adv_hrs)})
        st.rerun()
    if rst_col.button("🔄 Reset", use_container_width=True):
        from datetime import timezone as _tz_utc
        st.session_state.data                               = _defaults()
        st.session_state.data["run_schedule"]               = []
        st.session_state.data["schedule_received_for_week"] = None
        st.session_state.data["schedule_parse_issue"]       = None
        # Per the operator-controls spec: Reset clears the level
        # history, alert log, target overrides, and turns automation
        # back ON.
        st.session_state.data["level_history"]              = []
        st.session_state.data["alert_log"]                  = []
        st.session_state.data["target_overrides"]           = None
        st.session_state.data["vmi_automation_enabled"]     = True
        # Reset the session-start timestamp so any emails already in the inbox
        # are treated as "before the session" and ignored from now on.
        st.session_state.session_start_real_utc             = datetime.now(_tz_utc.utc)
        st.session_state.planned_trucks = []
        st.session_state.plan_reasoning = []
        st.session_state.plan_log       = []
        st.session_state.pdf_bytes      = None
        st.session_state.parse_result   = None
        st.session_state.advance_log    = ""
        st.session_state.email_log      = []
        # Audit log AFTER the reset so the entry survives in the
        # fresh data dict (the audit_log lives in data; reset wipes
        # data so we record on the new instance).
        _audit.record(st.session_state.data, _audit.A_RESET,
                       details={"wiped": ["run_schedule", "alert_log",
                                            "level_history",
                                            "target_overrides"]})
        st.rerun()
    if st.session_state.advance_log:
        with st.expander("Last advance log", expanded=False):
            st.text(st.session_state.advance_log)
    with st.expander("Upcoming run windows"):
        future = [w for w in data["run_schedule"] if w["end_hour"] > data["current_run_hour"]]
        if future:
            st.dataframe(
                [{"Label": w.get("label", ""), "Start": format_run_hour(data, w["start_hour"]),
                  "End":   format_run_hour(data, w["end_hour"]),
                  "Hrs":   f"{w['end_hour'] - w['start_hour']:.0f}"}
                 for w in sorted(future, key=lambda w: w["start_hour"])],
                use_container_width=True, hide_index=True, height=160,
            )
        else:
            st.caption("No future run windows — apply a schedule.")

st.divider()

# ── Alerts ────────────────────────────────────────────────────────────────────

alerts = get_all_alerts(data, cfg=st.session_state.cfg)
n_alerts = len(alerts)
st.subheader(f"🚨 Alerts {'(' + str(n_alerts) + ' active)' if n_alerts else ''}")
if not alerts:
    st.markdown("""
    <div style="background:#F0FDF4;border-left:4px solid #22C55E;border-radius:8px;
                padding:0.75rem 1rem;color:#14532D;font-family:'Inter',sans-serif;
                font-size:0.92rem;font-weight:500;">
        ✅ &nbsp; All clear — no active alerts.
    </div>""", unsafe_allow_html=True)
else:
    for a in alerts:
        # Alerts are structured dicts (see alerts._alert). Severity keys
        # the styling via the .vmi-banner class (4px left-border per
        # design system); the text field strips legacy prefixes.
        is_red = a.get("severity") == "red_flag"
        kind   = "danger" if is_red else "warning"
        label  = "🔴 &nbsp; CRITICAL" if is_red else "🟡 &nbsp; WARNING"
        accent = "#F43F5E" if is_red else "#F59E0B"
        raw    = a.get("text", "")
        text   = (raw.replace("RED FLAG: ", "")
                     .replace("YELLOW FLAG: ", "")
                     .replace("WARNING: ", ""))
        st.markdown(
            f'<div class="vmi-banner vmi-banner-{kind}">'
            f'<div>'
            f'<span style="font-size:0.7rem;font-weight:600;color:{accent};'
            f'letter-spacing:0.05em;text-transform:uppercase;">{label}</span>'
            f'<div style="font-size:0.875rem;font-weight:400;margin-top:0.2rem;">'
            f'{text}</div>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

st.divider()

# ── Trendline Charts with inline tank status ──────────────────────────────────
#
# 12-day projection comes BEFORE the next-week forecast panel. Operators
# read this top-down; the projection chart is the primary "what's about
# to happen" visual, with a per-day forecast table beneath as a smaller
# sanity-check artifact.

from forecast import forecast as _forecast, build_augmented_data as _build_augmented_data
from plan_orders import get_target_week_bounds as _gtwb

st.subheader("📈 12-Day Projection")
# Build a forecast-augmented schedule so the chart can extend past the
# operator-parsed window with a dotted-line forecast period. cutoff
# = end of last parsed run window (or now if no future windows). Solid
# line up to cutoff; dotted line beyond.
_augmented_data, _projection_cutoff = _build_augmented_data(
    data, hours=PROJECTION_CHART_HOURS, cfg=st.session_state.cfg
)
hist = compute_level_history(
    _augmented_data, hours=PROJECTION_CHART_HOURS, cfg=st.session_state.cfg
)

# Build product → [tank_name, ...] mapping from data["tanks"] so the
# chart section works for any customer config, not just the Acme demo.
_lh = data.get("level_history", [])
_rh = data.get("current_run_hour", 0.0)
_prod_tanks: dict = {}
for _tname, _tinfo in data.get("tanks", {}).items():
    _prod_tanks.setdefault(_tinfo.get("product", ""), []).append(_tname)

_prod_cols = st.columns(max(len(_prod_tanks), 1))
for _col, (_prod_name, _tank_names) in zip(_prod_cols, _prod_tanks.items()):
    with _col:
        _safe_key = _prod_name.lower().replace(" ", "_").replace("-", "_")
        st.plotly_chart(
            _chart(hist, _prod_name, cutoff_run_hour=_projection_cutoff),
            use_container_width=True,
            key=f"ch_{_safe_key}",
        )
        _tcols = st.columns(max(len(_tank_names), 1))
        for _tc, _tn in zip(_tcols, _tank_names):
            _tank_info(_tc, _tn, data["tanks"][_tn], _lh, _rh)

st.divider()

# ── Next-Week Forecast (Phase 8) ─────────────────────────────────────────────
#
# Weighted seasonal forecast for the upcoming week, rendered as a
# single-line caption ("Next week (forecast): Mon 16h · Tue 16h · …").
# The 12-day projection above is the primary visual; this just prints
# the shape so it doesn't take up vertical space.

_fc_week_start, _fc_week_end = _gtwb(data)
_fc_result = _forecast(
    data, target_week_start_run_hour=_fc_week_start, cfg=st.session_state.cfg
)

_DAY_NAMES_FC = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_fc_parts = []
if _fc_result.products:
    _p0 = _fc_result.products[0]
    for _dow in range(7):
        _hrs = float(_p0.by_weekday.get(_dow, {}).get("run_hours", 0))
        if _hrs > 0:
            _fc_parts.append(f"{_DAY_NAMES_FC[_dow]} {_hrs:.0f}h")

_fc_line_col, _fc_btn_col = st.columns([6, 1])
with _fc_line_col:
    if _fc_parts:
        st.markdown(
            f"🔮 **Next week (forecast):** {' · '.join(_fc_parts)}"
        )
    else:
        st.caption("🔮 Next week (forecast): no predicted run hours.")
with _fc_btn_col:
    if st.button("↻", key="fc_refresh_btn",
                  help="Recompute the weighted seasonal forecast.",
                  use_container_width=True):
        st.rerun()

# Forecast notes (fallback warnings, holiday exclusions, etc.)
if _fc_result.notes:
    with st.expander("Forecast notes & method", expanded=False):
        st.caption(
            f"Engine: **{_fc_result.engine_name}** · "
            f"lookback: {_fc_result.lookback_weeks} weeks "
            f"({_fc_result.weeks_used} kept after outlier filter)"
        )
        for _n in _fc_result.notes:
            st.markdown(f"- {_n}")

st.divider()

# ── Pending low-confidence parse (Phase J) ───────────────────────────────────
#
# When fetch_and_apply_schedule lands a low-confidence parse, it stashes
# the email + best-guess entries into data["pending_low_confidence_parse"]
# AND sends an email alert (existing behavior). This panel surfaces the
# same record in-page so the operator can confirm-and-apply with one click,
# instead of going to the Schedule Parser and re-pasting the email.

_pending_lc     = data.get("pending_low_confidence_parse")
_applied_review_for_mutex = data.get("last_applied_parse_review")
# Mutex: when both LOW + HIGH panels would render, hide the LOW one
# with a note pointing the operator at the fresh HIGH parse below.
# A HIGH parse landing means the schedule is already applied and
# correct; the older LOW pending is now stale.
if _pending_lc and _applied_review_for_mutex:
    with st.container(border=True):
        st.caption(
            "ℹ️ A fresh HIGH-confidence parse arrived since this "
            "LOW-confidence record was created. Review the green panel "
            "below first; this LOW record will auto-clear when you "
            "Acknowledge / Dismiss it, OR you can dismiss it directly:"
        )
        if st.button("✕ Dismiss stale low-confidence record",
                      key="lc_stale_dismiss",
                      use_container_width=True):
            _email_id = _pending_lc.get("email_id")
            st.session_state.data.pop("pending_low_confidence_parse", None)
            _audit.record(st.session_state.data, _audit.A_LC_PARSE_DISMISS,
                            details={"email_id": _email_id,
                                      "reason": "superseded_by_high_parse"})
            _save_active_state(st.session_state.data)
            st.rerun()
    _pending_lc = None   # suppress full LOW panel below
if _pending_lc:
    with st.container(border=True):
        st.subheader("⚠️ Low-confidence schedule needs review")
        st.markdown(
            f"**From:** `{_pending_lc.get('sender', '?')}` · "
            f"**Subject:** {_pending_lc.get('subject', '(none)')}"
        )
        _lc_left, _lc_right = st.columns([1, 1])
        with _lc_left:
            st.markdown("**Original email body:**")
            st.text_area(
                "Email body",
                value=_pending_lc.get("body", ""),
                height=200,
                disabled=True,
                key="lc_body_view",
                label_visibility="collapsed",
            )
        with _lc_right:
            _lc_method = _pending_lc.get("method", "regex")
            if _lc_method == "llm_hint":
                st.markdown(
                    f":orange[**⚠️ LLM hint** — confidence was below the "
                    f"95% threshold. Review every window carefully before "
                    f"applying.] (confidence: "
                    f"`{_pending_lc.get('confidence', '?')}`)"
                )
            else:
                st.markdown(f"**Parser's best guess** "
                              f"(confidence: `{_pending_lc.get('confidence', '?')}`):")
            _lc_entries = _pending_lc.get("entries") or []
            # Reuse the shared parser-editor format: text columns
            # accepting "Mon 6am" / "Tue 16:00" / "Sat 04:00".
            _LC_EDITOR_COL_CFG = {
                "Start": st.column_config.TextColumn(
                    "Start", required=True,
                    help="Day + time the window starts. "
                         "e.g. 'Mon 6am', 'Mon 06:00', 'Mon 0600'."),
                "End": st.column_config.TextColumn(
                    "End", required=True,
                    help="Day + time the window ends. Can roll "
                         "into a later day for continuous shifts. "
                         "e.g. 'Tue 4pm', 'Sat 04:00', 'Fri 16:00'."),
            }
            if _lc_entries:
                _lc_rows = []
                for e in _lc_entries:
                    s_str, e_str = _entry_to_strs(e)
                    _lc_rows.append({"Start": s_str, "End": e_str})
                _lc_edited = st.data_editor(
                    _lc_rows,
                    use_container_width=True,
                    hide_index=True,
                    num_rows="dynamic",
                    column_config=_LC_EDITOR_COL_CFG,
                    key="lc_editor",
                )
            else:
                st.info("Parser extracted zero entries. Add rows below "
                         "or use the Schedule Parser to manually paste "
                         "the schedule.")
                _lc_edited = st.data_editor(
                    [{"Start": "Mon 06:00", "End": "Mon 16:00"}],
                    use_container_width=True,
                    hide_index=True,
                    num_rows="dynamic",
                    column_config=_LC_EDITOR_COL_CFG,
                    key="lc_editor_empty",
                )
            # Half-hour rounding warning — same hint as the main parser
            # editor, so operators can't silently lose 30 minutes here.
            _lc_half = [
                (r.get("Start"), r.get("End"))
                for r in _lc_edited
                if (_has_fractional_minutes(r.get("Start"))
                    or _has_fractional_minutes(r.get("End")))
            ]
            if _lc_half:
                _bad = ", ".join(f"{s} → {e}" for s, e in _lc_half[:3])
                st.caption(
                    f"⚠️ Half-hour values rounded down to integer hours: "
                    f"{_bad}{' …' if len(_lc_half) > 3 else ''}."
                )
            if _pending_lc.get("notes"):
                with st.expander("Parser notes",
                                   expanded=(_pending_lc.get("confidence")
                                              != "high")):
                    for _n in _pending_lc["notes"]:
                        st.markdown(f"- {_n.strip()}")

        # Three-button row: confirm-and-apply, dismiss, or do nothing
        _lc_b1, _lc_b2, _lc_b3 = st.columns([2, 2, 5])
        with _lc_b1:
            if st.button("✓ Confirm & apply",
                          type="primary",
                          use_container_width=True,
                          key="lc_confirm_btn",
                          help="Apply the (possibly edited) entries as a "
                               "low-confidence merge. Other days' existing "
                               "windows survive; the week is NOT marked "
                               "received so reminders keep firing."):
                # Convert text editor rows back to entry tuples.
                # Reuses the shared _parsed_strs_to_entry helper so this
                # panel + the main parser editor stay in lockstep.
                _final_entries = []
                for _row in _lc_edited:
                    _ent = _parsed_strs_to_entry(_row.get("Start"),
                                                  _row.get("End"))
                    if _ent is not None:
                        _final_entries.append(_ent)

                # Refuse to apply if any windows overlap. Validation
                # runs ONLY on the click (not on every rerender) because
                # data_editor doesn't commit values reliably during typing.
                _overlaps = _entries_overlap_pairs(_final_entries)
                if _overlaps:
                    _pair_strs = ", ".join(f"rows {a}+{b}" for a, b in _overlaps)
                    st.error(
                        f"⛔ Did not apply — overlapping windows detected "
                        f"({_pair_strs}). Edit the rows so each time range "
                        f"is distinct, then click Apply again."
                    )
                    st.stop()

                if _final_entries:
                    sim_now = run_hour_to_dt(data, data["current_run_hour"])
                    data, _r, _a = apply_schedule_to_data(
                        data, _final_entries, now_dt=sim_now, mode="merge"
                    )
                    st.session_state.data = data
                    # Clear the pending record on confirm
                    _email_id_for_audit = (_pending_lc or {}).get("email_id")
                    st.session_state.data.pop("pending_low_confidence_parse",
                                                None)
                    _audit.record(
                        st.session_state.data, _audit.A_LC_PARSE_CONFIRM,
                        details={"email_id": _email_id_for_audit,
                                  "entries": _final_entries,
                                  "edited": _final_entries != [
                                      tuple(e) for e in
                                      (_pending_lc or {}).get("entries", [])
                                  ]},
                    )
                    # Phase 7 — feed the correction back into the
                    # parser-learning loop. Once the customer has
                    # >= 10 misses with corrections, the LLM rescue
                    # prompt gets enriched with these examples.
                    try:
                        from parser_misses import append_correction \
                            as _append_correction
                        _append_correction(
                            email_id=_email_id_for_audit,
                            corrected_entries=_final_entries,
                        )
                    except Exception:
                        pass
                    _save_active_state(st.session_state.data)
                    st.success(
                        f"Confirmed: {_a and len(_a)} day(s) applied as merge "
                        f"({_r} old window(s) replaced). Week NOT marked "
                        f"received — reminders will keep firing."
                    )
                    st.rerun()
                else:
                    st.warning("No valid entries to apply. Edit the table or "
                                "use Dismiss.")
        with _lc_b2:
            if st.button("✕ Dismiss",
                          use_container_width=True,
                          key="lc_dismiss_btn",
                          help="Discard this pending parse without applying. "
                               "Use the Schedule Parser below to manually "
                               "paste a corrected schedule."):
                _email_id_for_audit = (_pending_lc or {}).get("email_id")
                st.session_state.data.pop("pending_low_confidence_parse",
                                            None)
                _audit.record(st.session_state.data,
                                _audit.A_LC_PARSE_DISMISS,
                                details={"email_id": _email_id_for_audit})
                _save_active_state(st.session_state.data)
                st.rerun()
        with _lc_b3:
            st.caption(
                "💡 You can edit the table inline before confirming. "
                "Confirm uses **merge mode** so other days' existing windows "
                "are preserved."
            )

    st.divider()

# ── Last applied HIGH-confidence parse review (Phase 6) ──────────────────────
#
# When the IMAP fetch lands a HIGH-confidence parse, the schedule
# auto-applies AND we stash the email + parsed entries for operator
# review. Acknowledgement is optional; the schedule has already gone
# through. This gives the operator visibility into what was applied
# without having to dig through email or run the parser manually.

_applied_review = data.get("last_applied_parse_review")
if _applied_review:
    _ar_via_llm = _applied_review.get("parse_method") == "llm"
    with st.container(border=True):
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:0.6rem;'
            f'margin-bottom:0.4rem;">'
            f'{_chip_html("✓ APPLIED — HIGH CONFIDENCE", "success")}'
            + (f'{_chip_html("⚠️ VIA LLM PARSER", "warning")}' if _ar_via_llm else "")
            + f'<span style="color:#475569;font-size:0.85rem;">'
            f'{_applied_review.get("windows_applied", 0)} window(s) for '
            f'week of {_applied_review.get("week_str", "?")} '
            f'({_applied_review.get("windows_replaced", 0)} old replaced)'
            f'</span></div>',
            unsafe_allow_html=True,
        )
        if _ar_via_llm:
            st.warning(
                "⚠️ This schedule was applied via the **LLM parser** "
                "(the regex parser couldn't extract a complete result). "
                "LLM parses are more prone to errors — please verify the "
                "windows below before acknowledging.",
                icon=None,
            )
        st.markdown(
            f"**From:** `{_applied_review.get('sender', '?')}` · "
            f"**Subject:** {_applied_review.get('subject', '(none)')}"
        )
        _ar_left, _ar_right = st.columns([1, 1])
        with _ar_left:
            st.markdown("**Original email body:**")
            st.text_area(
                "Email body (read-only)",
                value=_applied_review.get("body", ""),
                height=150,
                disabled=True,
                key="ar_body_view",
                label_visibility="collapsed",
            )
        with _ar_right:
            st.markdown("**Parsed run windows:**")
            _ar_entries = _applied_review.get("entries") or []
            _ar_rows = [
                {"Window": _entry_to_window_str(e),
                  "Hours": int(e[2]) - int(e[1])}
                for e in _ar_entries
            ]
            st.dataframe(_ar_rows, use_container_width=True, hide_index=True)
            if _applied_review.get("notes"):
                with st.expander("Parser notes (HIGH confidence rationale)"):
                    for _n in _applied_review["notes"]:
                        st.markdown(f"- {_n.strip()}")

        _ar_b1, _ar_b2, _ar_b3 = st.columns([2, 2, 5])
        with _ar_b1:
            if st.button("✓ Acknowledge",
                          type="primary",
                          use_container_width=True,
                          key="ar_ack_btn",
                          help="Confirm you've reviewed the auto-applied "
                               "schedule. Clears the panel."):
                _email_id_for_audit = _applied_review.get("email_id")
                st.session_state.data.pop("last_applied_parse_review", None)
                st.session_state.data.pop("last_parse_method", None)
                _audit.record(st.session_state.data, "applied_parse_ack",
                                details={"email_id": _email_id_for_audit})
                # Phase 7 — operator-acknowledged HIGH parses are
                # positive signal for the learning loop ("here's what
                # RIGHT looks like for this customer").
                try:
                    from parser_misses import append_validation \
                        as _append_validation
                    _append_validation(email_id=_email_id_for_audit)
                except Exception:
                    pass
                _save_active_state(st.session_state.data)
                st.rerun()
        with _ar_b2:
            if st.button("✕ Dismiss",
                          use_container_width=True,
                          key="ar_dismiss_btn",
                          help="Hide the panel without acknowledging. "
                               "Same effect as Acknowledge for the schedule "
                               "(it's already applied) — different audit "
                               "intent."):
                _email_id_for_audit = _applied_review.get("email_id")
                st.session_state.data.pop("last_applied_parse_review", None)
                _audit.record(st.session_state.data,
                                "applied_parse_dismiss",
                                details={"email_id": _email_id_for_audit})
                _save_active_state(st.session_state.data)
                st.rerun()
        with _ar_b3:
            st.caption(
                "💡 Schedule already applied. Acknowledge / Dismiss "
                "just clears the panel — both are recorded in the "
                "operator audit log for compliance."
            )

    st.divider()

# ── Schedule Parser | Auto-Planner (side by side) ────────────────────────────

sp_col, ap_col = st.columns([2, 3])

# ── Left: Schedule Parser ────────────────────────────────────────────────────
with sp_col:
    st.subheader("📅 Schedule Parser")

    # Demo accelerators — clicking either button stages a sample
    # schedule text into the editor and immediately runs the parser
    # so the operator can step through both confidence paths without
    # typing anything. Useful for showing the LOW-confidence review
    # flow (front and center for demos).
    SIM_HIGH_TEXT = (
        "Monday 6am-10pm, Tuesday 6am-10pm,\n"
        "Wednesday 6am-2pm, Thursday off,\n"
        "Friday 6am-2pm"
    )
    SIM_LOW_TEXT = "Run all week"
    sim_hi_btn, sim_lo_btn = st.columns(2)
    _sim_now_for_seed = run_hour_to_dt(data, data["current_run_hour"])
    if sim_hi_btn.button("🧪 Simulate HIGH parse", use_container_width=True,
                          help="Stage a clean Mon-Fri example and parse it."):
        st.session_state["sched_text"] = SIM_HIGH_TEXT
        entries, confidence, notes, parse_method = parse_schedule(
            SIM_HIGH_TEXT, api_key=_get_anthropic_key(),
            now_dt=_sim_now_for_seed,
        )
        st.session_state.parse_result = (entries, confidence, notes, parse_method)
        st.rerun()
    if sim_lo_btn.button("🧪 Simulate LOW parse", use_container_width=True,
                          help="Stage an ambiguous 'run all week' example "
                               "and parse it (LOW-confidence fallback)."):
        st.session_state["sched_text"] = SIM_LOW_TEXT
        entries, confidence, notes, parse_method = parse_schedule(
            SIM_LOW_TEXT, api_key=_get_anthropic_key(),
            now_dt=_sim_now_for_seed,
        )
        st.session_state.parse_result = (entries, confidence, notes, parse_method)
        st.rerun()

    sched_text = st.text_area(
        "Paste schedule",
        placeholder=(
            "Monday 6am-10pm, Tuesday 6am-10pm,\n"
            "Wednesday 6am-2pm, Thursday off,\n"
            "Friday 6am-2pm\n"
            "— or —\n"
            "Run Monday 0600 to Friday 0400"
        ),
        height=150, key="sched_text", label_visibility="collapsed",
    )
    parse_btn, test_api_btn = st.columns(2)
    if parse_btn.button("🔍 Parse", use_container_width=True):
        if sched_text.strip():
            # Anchor date-token resolution (e.g. "4/20" → "Sat") to the sim
            # clock so the tester honours simulation time rather than wall
            # clock, matching `apply_schedule_to_data` below.
            sim_now = run_hour_to_dt(data, data["current_run_hour"])
            entries, confidence, notes, parse_method = parse_schedule(
                sched_text, api_key=_get_anthropic_key(), now_dt=sim_now
            )
            st.session_state.parse_result = (entries, confidence, notes, parse_method)
        else:
            st.warning("Paste a schedule first.")
    if test_api_btn.button("🧪 Test API", use_container_width=True,
                           help="Send a minimal request to the Anthropic API to "
                                "confirm the key works and the service is reachable."):
        with st.spinner("Pinging Anthropic API…"):
            ok, msg = check_anthropic_api(_get_anthropic_key())
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    st.caption(
        "**Formats:** `Mon 6am-10pm` · `Mon 0600-2200` · `Mon 06:00-22:00`  \n"
        "`Run Mon 0600 to Fri 0400` (continuous)  \n"
        "`off` / `no run` to skip a day. Separate with commas or line breaks."
    )

    if st.session_state.parse_result:
        _pr = st.session_state.parse_result
        entries, confidence, notes = _pr[0], _pr[1], _pr[2]
        parse_method = _pr[3] if len(_pr) > 3 else "regex"
        if confidence == "high":
            pill_bg, pill_fg, pill_label = "#DCFCE7", "#166534", "HIGH CONFIDENCE"
        else:
            pill_bg, pill_fg, pill_label = "#FFE4E6", "#9F1239", "LOW CONFIDENCE"
        # Surface partial-day rejections (e.g. "Wed: day found but no time
        # range detected") at the headline level. Without this, the warning
        # is buried inside the collapsed parse-details accordion and the
        # "X window(s) parsed" headline gives no hint that input rows were
        # silently dropped.
        skip_phrases = ("no time range detected", "could not parse",
                         "day found but")
        skipped_notes = [
            n.strip() for n in (notes or [])
            if any(p in n.lower() for p in skip_phrases)
        ]
        st.markdown(
            f"""
            <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.4rem;
                        font-family:'Inter',sans-serif;">
                <span style="background:{pill_bg};color:{pill_fg};font-size:0.7rem;
                             font-weight:600;letter-spacing:0.05em;padding:3px 9px;
                             border-radius:999px;">{pill_label}</span>
                <span style="color:#64748B;font-size:0.85rem;">
                    {len(entries)} window(s) parsed
                </span>
                {('<span style="background:#FEF3C7;color:#92400E;font-size:0.7rem;'
                  'font-weight:600;letter-spacing:0.05em;padding:3px 9px;'
                  f'border-radius:999px;">⚠️ {len(skipped_notes)} line(s) skipped</span>')
                  if skipped_notes else ''}
            </div>
            """,
            unsafe_allow_html=True,
        )
        if skipped_notes:
            st.warning(
                "Some lines were skipped because they had a day but no readable "
                "time range. Review them before applying:\n\n"
                + "\n".join(f"- {n}" for n in skipped_notes)
            )
        if entries:
            DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            # Editable table: operator types Start / End as
            # day-and-time strings ("Mon 6am", "Thu 4pm", "Sat 04:00").
            # No more raw 0-47 hour math. On Apply, strings are
            # parsed back to the (weekday, start_h, end_h) tuple
            # shape the rest of the pipeline uses, where end_h is
            # an offset from start-day midnight (so "Mon 6am →
            # Sat 4am" → (0, 6, 124)). Reuses the module-top helpers
            # _entry_to_strs / _parsed_strs_to_entry.
            edit_rows = []
            for e in entries:
                s_str, e_str = _entry_to_strs(e)
                edit_rows.append({"Start": s_str, "End": e_str})
            edited = st.data_editor(
                edit_rows,
                use_container_width=True,
                hide_index=True,
                key="parser_entries_editor",
                num_rows="dynamic",   # operator can add / remove rows
                column_config={
                    "Start": st.column_config.TextColumn(
                        "Start", required=True,
                        help="Day + time the window starts. "
                             "e.g. 'Mon 6am', 'Mon 06:00', 'Mon 0600'."),
                    "End":   st.column_config.TextColumn(
                        "End", required=True,
                        help="Day + time the window ends. Can roll "
                             "over to a later day for continuous shifts. "
                             "e.g. 'Thu 4pm', 'Sat 04:00', 'Fri 16:00'."),
                },
            )
            edited_entries = []
            _half_hour_typed = []
            for row in edited:
                _s, _e = row.get("Start"), row.get("End")
                _ent = _parsed_strs_to_entry(_s, _e)
                if _ent is not None:
                    edited_entries.append(_ent)
                    # Surface a hint when the operator typed a non-zero
                    # minute that read_schedule._parse_time will truncate.
                    if _has_fractional_minutes(_s) or _has_fractional_minutes(_e):
                        _half_hour_typed.append((_s, _e))
            st.session_state._edited_entries = edited_entries
            if edited_entries != [(e[0], e[1], e[2]) for e in entries]:
                st.caption(
                    f"✏️ Edited: {len(edited_entries)} window(s) "
                    f"(parser originally extracted {len(entries)})"
                )
            if _half_hour_typed:
                _bad = ", ".join(f"{s} → {e}" for s, e in _half_hour_typed[:3])
                st.caption(
                    f"⚠️ Half-hour values rounded down to integer hours: "
                    f"{_bad}{' …' if len(_half_hour_typed) > 3 else ''}. "
                    f"The plant runs in whole-hour blocks; if you need "
                    f"finer granularity, contact engineering."
                )

        # Show parse notes — critical when confidence is low so the user knows
        # WHY (e.g. "LLM parse failed — API key rejected" or "Thu: day found
        # but no time range detected"). Also helpful on high confidence as a
        # sanity-check trail ("LLM parsed 1 window covering ~4 calendar days").
        if notes:
            with st.expander(
                "Parse details" + (" — review why confidence is low"
                                   if confidence != "high" else ""),
                expanded=(confidence != "high"),
            ):
                for n in notes:
                    st.markdown(f"- {n.strip()}")

        # Single, neutral label — covers both "confirm what the parser
        # got right" and "apply the corrections I just made". Confidence
        # still drives apply_mode (HIGH = full replace + mark week
        # received; LOW = additive merge), but the button text no
        # longer requires the operator to interpret "Anyway".
        btn_lbl = "✅ Apply Schedule"
        _apply_entries_preview = st.session_state.get("_edited_entries", entries) or entries
        _no_windows = not _apply_entries_preview
        if _no_windows:
            st.caption(
                "⚠️ 0 windows parsed — Apply is disabled. "
                "Edit the rows above to add at least one window, "
                "or paste a different schedule."
            )
        if st.button(btn_lbl, use_container_width=True, disabled=_no_windows):
            sim_now = run_hour_to_dt(data, data["current_run_hour"])
            # HIGH confidence → full replace + mark week received.
            # LOW confidence "Apply Anyway" → additive merge: only the
            # parsed days overwrite, other days' existing windows stay,
            # and the week is NOT marked received (so the missing-schedule
            # reminder keeps firing). Prevents a partial parse from
            # silently wiping a complete week.
            apply_mode = "replace" if confidence == "high" else "merge"
            # Use the operator-edited entries if the inline editor was
            # touched; falls back to the original parser output if not.
            apply_entries = st.session_state.get(
                "_edited_entries", entries) or entries

            # Refuse to apply if any windows overlap (cross-day too).
            _overlaps = _entries_overlap_pairs(apply_entries)
            if _overlaps:
                _pair_strs = ", ".join(f"rows {a}+{b}" for a, b in _overlaps)
                st.error(
                    f"⛔ Did not apply — overlapping windows detected "
                    f"({_pair_strs}). Edit the rows so each time range "
                    f"is distinct, then click Apply again."
                )
                st.stop()

            data, removed, added = apply_schedule_to_data(
                data, apply_entries, now_dt=sim_now, mode=apply_mode
            )
            st.session_state.pop("_edited_entries", None)
            st.session_state.parse_result = None
            if apply_mode == "merge":
                st.warning(
                    f"Low-confidence merge: {len(added)} day(s) updated, "
                    f"{removed} old window(s) replaced. Other days kept. "
                    f"Schedule NOT marked received — please re-send a "
                    f"complete schedule when possible."
                )
            else:
                st.success(
                    f"Applied: {removed} old window(s) removed, "
                    f"{len(added)} new added."
                )
            st.rerun()

# ── Right: Auto-Planner ───────────────────────────────────────────────────────
with ap_col:
    st.subheader("🤖 Auto-Planner")
    week_start, week_end = get_target_week_bounds(data)
    week_rh    = get_run_hours_in_window(data, week_start, week_end)
    target_lbs = get_target_for_week(week_rh, state=data)

    ic1, ic2, ic3 = st.columns(3)
    # Compact date so the metric value doesn't overflow the narrow
    # auto-planner column. "Mon 5/4" instead of "Mon 2026-05-04".
    _ws_dt = run_hour_to_dt(data, week_start)
    ic1.metric("Plan week starts",
                f"{_ws_dt.strftime('%a')} {_ws_dt.month}/{_ws_dt.day}")
    ic2.metric("Scheduled run hrs", f"{week_rh:.0f} h")
    ic3.metric("Reorder target", f"{target_lbs:,.0f} lbs")

    if week_rh == 0:
        st.warning("No run hours scheduled for the target week — apply a schedule first, then plan.")
    else:
        if st.button("🔍 Plan Next Week", type="primary"):
            all_new   = []
            reasoning = []
            plan_log  = []
            for product in data["consumption_rates"]:
                captured = io.StringIO()
                with contextlib.redirect_stdout(captured):
                    new = plan_for_product(
                        data, product, target_lbs, week_start, week_end, all_new,
                        cfg=st.session_state.cfg,
                    )
                all_new.extend(new)
                out = captured.getvalue().strip()
                if out:
                    plan_log.append(out)
                for t in new:
                    reasoning.append({
                        "product":          t["product"],
                        "arrival_run_hour": t["arrival_run_hour"],
                        "qty":              t["quantity_lbs"],
                        "reason":           t.get("_planned_reason", ""),
                    })
            st.session_state.planned_trucks = all_new
            st.session_state.plan_reasoning = reasoning
            st.session_state.plan_log       = plan_log
            _audit.record(data, _audit.A_PLAN,
                           details={"trucks_proposed": len(all_new),
                                     "products": list(data["consumption_rates"].keys())})
            if not all_new:
                if plan_log:
                    # Planner hit a constraint — show what it found
                    for msg in plan_log:
                        st.warning(msg)
                else:
                    st.success("Levels are sufficient — no trucks needed for the target week.")

    if st.session_state.planned_trucks:
        st.markdown(f"**{len(st.session_state.planned_trucks)} truck(s) proposed:**")
        for item in st.session_state.plan_reasoning:
            st.info(
                f"🚛 **{item['product']}**  ·  {format_run_hour(data, item['arrival_run_hour'])}"
                f"  ·  {item['qty']:,} lbs  \n_{item['reason']}_"
            )
        if st.button("✅ Commit Trucks  (SAP numbers auto-assigned)", type="primary", key="commit_btn"):
            # Check both scheduled trucks AND sap_history so delivered
            # trucks' numbers are never reused.
            issued = set(data.get("sap_history", []))
            issued.update(t["sap_order"] for t in data["scheduled_trucks"]
                          if t.get("sap_order"))
            nums = [int(re.search(r"\d+$", s).group())
                    for s in issued if re.search(r"\d+$", s)]
            next_n  = max(nums) + 1 if nums else 20001
            sorted_t = sorted(st.session_state.planned_trucks, key=lambda t: t["arrival_run_hour"])
            for i, t in enumerate(sorted_t):
                t["sap_order"] = f"SAP{next_n + i}"
                t.pop("_planned_reason", None)
                data["scheduled_trucks"].append(t)
                _record_sap(data, t["sap_order"])
            try:
                st.session_state.pdf_bytes = build_load_entry_pdf(sorted_t, data)
            except Exception as e:
                st.warning(f"PDF generation failed: {e}")
            # Log the CS load-entry email
            cfg = load_config()
            cs  = cfg.get("cs_email", "") if cfg else ""
            from time_utils import run_hour_to_dt as _rh_dt
            first_dt = _rh_dt(data, sorted_t[0]["arrival_run_hour"])
            week_lbl = (first_dt - timedelta(days=first_dt.weekday())).strftime("%Y-%m-%d")
            body_lines = [f"Load Entry — Week of {week_lbl}", ""]
            for t2 in sorted_t:
                body_lines.append(
                    f"{t2['sap_order']}  |  {t2['product']}  |  "
                    f"{t2['quantity_lbs']:,} lbs  |  "
                    f"{format_run_hour(data, t2['arrival_run_hour'])}"
                )
            cs_send_status = "queued"
            try:
                import email_hooks as _eh
                _eh.send_cs_load_entry(data, sorted_t)
                cs_send_status = "sent"
            except Exception as _cs_err:
                cs_send_status = f"not sent ({_cs_err})"
            st.session_state.email_log.append({
                "sim_time": format_run_hour(data, data["current_run_hour"]),
                "type":    "CS Load Entry",
                "to":      cs or "cs_email not configured",
                "subject": f"Load Entry — Week of {week_lbl}",
                "body":    "\n".join(body_lines),
                "status":  cs_send_status,
            })
            st.session_state.planned_trucks = []
            st.session_state.plan_reasoning = []
            st.session_state.plan_log       = []
            _audit.record(data, _audit.A_TRUCK_COMMIT,
                           details={"saps": [t["sap_order"] for t in sorted_t],
                                     "count": len(sorted_t),
                                     "cs_email_status": cs_send_status})
            st.success(
                f"Added {len(sorted_t)} truck(s) — "
                f"SAP{next_n} through SAP{next_n + len(sorted_t) - 1}."
            )
            st.rerun()

st.divider()

# ── VMI Health Dashboard (6-month alert history) ──────────────────────────────
#
# Reads alert_log (already populated on every alert fire) and counts
# overfill ("too high") vs safety_stock ("too low") events over the
# last 180 days. Operator uses this to decide whether to raise or
# lower the target sliders above. Counts accumulate over time —
# resetting data clears them.

st.subheader("📊 VMI Health Dashboard")

from datetime import datetime as _dt_dash, timedelta as _td_dash

_DASH_WINDOW_DAYS = 180   # 6 months

def _alert_log_summary(data, window_days=_DASH_WINDOW_DAYS):
    """Bucket alert_log entries by week + count by overfill /
    safety_stock. Returns (overfill_total, safety_total, weekly_buckets)
    where weekly_buckets is a list of (week_start_iso, overfill_n,
    safety_n) sorted ascending."""
    log = data.get("alert_log", []) or []
    cutoff = _dt_dash.now() - _td_dash(days=window_days)
    overfill_total = 0
    safety_total   = 0
    by_week: dict = {}
    for entry in log:
        ts = entry.get("logged_at_iso")
        if not ts:
            continue
        try:
            dt = _dt_dash.fromisoformat(ts)
        except ValueError:
            continue
        if dt < cutoff:
            continue
        # Bucket to Monday of that week (date), ISO string for key
        monday = (dt - _td_dash(days=dt.weekday())).date().isoformat()
        bucket = by_week.setdefault(monday, {"overfill": 0, "safety": 0})
        atype = entry.get("type")
        if atype == "overfill":
            overfill_total += 1
            bucket["overfill"] += 1
        elif atype == "safety_stock":
            safety_total += 1
            bucket["safety"] += 1
    weekly_buckets = sorted(
        [(wk, b["overfill"], b["safety"]) for wk, b in by_week.items()]
    )
    return overfill_total, safety_total, weekly_buckets


_overfill_n, _safety_n, _weekly = _alert_log_summary(data)
# Current cycle: include alerts that are firing right now but haven't yet
# been persisted to alert_log (e.g. between fires of the email-sender that
# does the logging, or after a fresh demo-history backfill that didn't
# replay alert evaluation). Without this, a dashboard showing "0 alerts in
# 180d" while 2 critical alerts are visible above is contradictory and
# erodes trust. Dedupe by alert_hash against existing alert_log entries
# in the window so currently-active alerts that ALREADY logged don't
# get double-counted.
from email_hooks import alert_hash as _alert_hash
def _within_window(iso):
    if not iso:
        return False
    try:
        return _dt_dash.fromisoformat(iso) >= (
            _dt_dash.now() - _td_dash(days=_DASH_WINDOW_DAYS)
        )
    except ValueError:
        return False
_logged_hashes_in_window = {
    e.get("hash") for e in (data.get("alert_log") or [])
    if e.get("hash") and _within_window(e.get("logged_at_iso"))
}
_current_alerts = get_all_alerts(data, cfg=st.session_state.cfg)
for _a in _current_alerts:
    if _alert_hash(_a.get("text", "")) in _logged_hashes_in_window:
        continue
    if _a.get("type") == "overfill":
        _overfill_n += 1
    elif _a.get("type") == "safety_stock":
        _safety_n += 1
_total_alerts_window = _overfill_n + _safety_n

# Top row: three big-number cards
_d1, _d2, _d3 = st.columns(3)
with _d1:
    st.metric(
        label=f"🔴 Overfill alerts (last {_DASH_WINDOW_DAYS}d)",
        value=_overfill_n,
        help="Times an arriving truck would have exceeded available "
             "tank capacity. High count → consider lowering the high "
             "target slider.",
    )
with _d2:
    st.metric(
        label=f"🟡 Safety-stock alerts (last {_DASH_WINDOW_DAYS}d)",
        value=_safety_n,
        help="Times projected combined level dropped below safety "
             "stock. High count → consider raising the low target slider.",
    )
with _d3:
    if _total_alerts_window == 0:
        _bias_text = "No alerts in window — running cleanly. 🎉"
        _bias_color = "normal"
    else:
        _overfill_pct = _overfill_n / _total_alerts_window * 100
        if _overfill_pct >= 65:
            _bias_text = f"Overfill bias ({_overfill_pct:.0f}%) — running too high"
            _bias_color = "inverse"
        elif _overfill_pct <= 35:
            _bias_text = f"Safety-stock bias ({100 - _overfill_pct:.0f}%) — running too low"
            _bias_color = "inverse"
        else:
            _bias_text = f"Balanced ({_overfill_pct:.0f}% overfill / {100 - _overfill_pct:.0f}% safety)"
            _bias_color = "normal"
    st.metric(
        label="Alert bias",
        value=_bias_text,
        help="Bias indicator: which side the alerts are clustering "
             "on. Use this to decide whether to nudge the target "
             "sliders above.",
    )

# Weekly stacked bar chart of alert counts removed — operator request:
# the three KPI cards above carry the same signal in less space.

# ── Tank-level history chart (powered by level_history ring buffer) ──────────

st.markdown("**Tank levels over time**")
_history = data.get("level_history", []) or []

# Quick-fill button: programmatically advances the sim clock by N
# weeks, recording snapshots at every tick. Honest demo data — not
# synthetic.
_qf_col1, _qf_col2 = st.columns([3, 1])
with _qf_col1:
    st.caption(
        f"{len(_history)} snapshot(s) recorded. "
        "Use *Advance* above to add more, or use *Generate demo history* "
        "to fast-forward through several weeks of real simulation."
    )
with _qf_col2:
    _qf_weeks = st.number_input(
        "Weeks", min_value=1, max_value=26, value=4, step=1,
        key="qf_weeks", label_visibility="collapsed",
    )
    if st.button("🎬 Generate demo history",
                  use_container_width=True,
                  key="qf_btn",
                  help="Backfill the level-history chart with N weeks "
                       "of synthetic past — alternating long-shift and "
                       "standard weeks, with auto-inserted truck "
                       "deliveries. Does NOT advance the sim clock."):
        from demo_history import generate_demo_history
        added = generate_demo_history(
            st.session_state.data, int(_qf_weeks),
        )
        _audit.record(st.session_state.data, _audit.A_QUICK_FILL,
                       details={"weeks": int(_qf_weeks),
                                "snapshots_added": added,
                                "mode": "backfill"})
        _save_active_state(st.session_state.data)
        st.toast(f"Backfilled {added} snapshots ({int(_qf_weeks)} weeks).",
                  icon="🎬")
        st.rerun()

if _history:
    import plotly.graph_objects as _go_lvl
    from level_history import downsample_for_chart as _ds_lvl
    _decimated = _ds_lvl(_history, max_points=720)
    _xs = [entry["iso"] for entry in _decimated]
    _tank_names = sorted(_decimated[0].get("tanks", {}).keys())

    # Combine the per-tank traces into ONE line per product.
    # Operator complaint: 4 overlapping tank lines were impossible to
    # read. Now: one line per product (read dynamically from
    # data["tanks"][name]["product"] — works for any customer
    # topology, not just Acme's hardcoded U-/M- prefix).
    _live_tanks = data.get("tanks", {}) or {}
    _tanks_by_product: dict[str, list[str]] = {}
    for _tn in _tank_names:
        _prod = (_live_tanks.get(_tn) or {}).get("product")
        if _prod:
            _tanks_by_product.setdefault(_prod, []).append(_tn)

    def _totals_for(tank_list):
        return [
            sum(entry["tanks"].get(t, 0) for t in tank_list)
            for entry in _decimated
        ]
    def _capacity_for(tank_list):
        return sum(float((_live_tanks.get(t) or {}).get("max_capacity_lbs", 0))
                   for t in tank_list)

    _trace_palette = ["#1E3A8A", "#0F766E", "#7C3AED", "#B45309", "#0891B2",
                       "#BE185D"]

    _all_totals: list[float] = []
    _max_cap = 0.0
    _fig_lvl = _go_lvl.Figure()
    for _i, (_product, _tlist) in enumerate(sorted(_tanks_by_product.items())):
        _totals = _totals_for(_tlist)
        _all_totals.extend(_totals)
        _max_cap = max(_max_cap, _capacity_for(_tlist))
        _color = _trace_palette[_i % len(_trace_palette)]
        _fig_lvl.add_trace(_go_lvl.Scatter(
            x=_xs, y=_totals, mode="lines", name=f"{_product} (combined)",
            line=dict(color=_color, width=2.5),
            hovertemplate=f"<b>{_product}</b><br>%{{x}}<br>%{{y:,.0f}} lbs<extra></extra>",
        ))
    _y_max = max(_max_cap, max(_all_totals + [0])) * 1.05
    # Safety-stock floor — same dashed-rose treatment as the
    # 12-day projection chart, so the two charts read as a family.
    _fig_lvl.add_hline(
        y=SAFETY_STOCK_LBS, line_dash="dash",
        line_color="#F43F5E", line_width=1.2,
        annotation_text="Safety stock",
        annotation_position="bottom right",
        annotation_font=dict(size=10, color="#9F1239", family="Inter"),
    )
    _fig_lvl.update_layout(
        height=420,
        margin=dict(l=20, r=20, t=10, b=40),
        legend=dict(orientation="h", y=-0.15),
        xaxis_title="Sim time",
        yaxis_title="lbs (combined per product)",
        hovermode="x unified",
        yaxis=dict(range=[0, _y_max], tickformat=",", dtick=10000,
                    gridcolor="#E2E8F0"),
        xaxis=dict(
            tickformat="%a %m/%d",
            dtick=86400000.0,
            tickangle=-30,
        ),
    )
    st.plotly_chart(_fig_lvl, use_container_width=True)
else:
    st.caption("No level history yet. Click *Generate demo history* "
                "or use *Advance* above to start populating the chart.")


# ── VMI Controls (operator overrides) ─────────────────────────────────────────
#
# Bounded reorder-target sliders + automation on/off toggle. Both
# persist week-to-week via PlantState fields (target_overrides and
# vmi_automation_enabled). Reset → back to cfg defaults / ON.

from config import DEFAULT_CONFIG as _CFG

st.subheader("🎛️ VMI Controls")

_vmi_on = data.get("vmi_automation_enabled", True)
_overrides = data.get("target_overrides")

# Top row: status header + on/off toggle
_status_col, _toggle_col = st.columns([3, 1])
with _status_col:
    if _vmi_on:
        st.markdown("**Automation:** :green[ON] — planner will propose trucks "
                    "and the schedule auto-applies when received.")
    else:
        st.markdown("**Automation:** :red[OFF] — planner is suppressed. "
                    "A RED alert fires every Friday 9 AM until you turn "
                    "this back on.")
with _toggle_col:
    new_vmi = st.toggle("Automation enabled",
                          value=_vmi_on,
                          key="vmi_toggle",
                          help="When OFF, the planner stops proposing "
                               "trucks and a weekly Friday RED alert is "
                               "sent to the distribution list.")
    if new_vmi != _vmi_on:
        st.session_state.data["vmi_automation_enabled"] = new_vmi
        _audit.record(st.session_state.data, _audit.A_VMI_TOGGLE,
                       details={"enabled": bool(new_vmi)})
        _save_active_state(st.session_state.data)
        st.rerun()

# Bottom row: target sliders
st.markdown(
    "**Reorder targets** — operator-tunable. Set within the customer's "
    "allowed window; click *Apply* to lock in (persists week-to-week). "
    "*Reset* clears the override and reverts to the customer's default curve."
)
_eff_low  = (_overrides or {}).get("low",  _CFG.target_low_lbs)
_eff_high = (_overrides or {}).get("high", _CFG.target_high_lbs)

_slider_col_low, _slider_col_high = st.columns(2)
with _slider_col_low:
    _new_low = st.slider(
        f"Low target (lbs) — light-week floor",
        min_value=int(_CFG.tunable_low_min),
        max_value=int(_CFG.tunable_low_max),
        value=int(_eff_low),
        step=500,
        key="vmi_low_slider",
    )
with _slider_col_high:
    _new_high = st.slider(
        f"High target (lbs) — heavy-week ceiling",
        min_value=int(_CFG.tunable_high_min),
        max_value=int(_CFG.tunable_high_max),
        value=int(_eff_high),
        step=500,
        key="vmi_high_slider",
    )

_apply_col, _reset_col, _info_col = st.columns([1, 1, 3])
with _apply_col:
    if st.button("✓ Apply", use_container_width=True, key="vmi_apply"):
        st.session_state.data["target_overrides"] = {
            "low":  float(_new_low),
            "high": float(_new_high),
        }
        _audit.record(st.session_state.data, _audit.A_TARGET_APPLY,
                       details={"low": float(_new_low),
                                "high": float(_new_high)})
        _save_active_state(st.session_state.data)
        st.rerun()
with _reset_col:
    if st.button("↺ Reset",
                  use_container_width=True,
                  key="vmi_reset",
                  disabled=(_overrides is None),
                  help="Clear the operator override and revert to the "
                       "customer's default target curve."):
        st.session_state.data["target_overrides"] = None
        _audit.record(st.session_state.data, _audit.A_TARGET_RESET,
                       details={"prior": _overrides or {}})
        _save_active_state(st.session_state.data)
        st.rerun()
with _info_col:
    if _overrides is not None:
        st.caption(f"📌 Override active: low={int(_overrides['low']):,} lbs, "
                    f"high={int(_overrides['high']):,} lbs (persists week-to-week)")
    else:
        st.caption(f"Default curve: low={int(_CFG.target_low_lbs):,} lbs, "
                    f"high={int(_CFG.target_high_lbs):,} lbs")

# ── Customer notes (free-text scratchpad) ─────────────────────────────────────
st.markdown(
    "**Customer notes** — free-text context that doesn't fit any "
    "structured field. Persists across resets via PlantState."
)
_existing_notes = data.get("customer_notes", "") or ""
_notes_text = st.text_area(
    "Customer notes",
    value=_existing_notes,
    height=90,
    max_chars=4000,
    key="customer_notes_input",
    label_visibility="collapsed",
    placeholder=("e.g. 'Anna out 4/22-4/26, expect manual schedules' or "
                  "'switching to weekend shifts in May' or "
                  "'plant is undergoing minor maintenance Thu morning'"),
)
_save_notes_col, _notes_caption_col = st.columns([1, 5])
with _save_notes_col:
    if st.button("💾 Save notes",
                  use_container_width=True,
                  key="customer_notes_save",
                  disabled=(_notes_text == _existing_notes),
                  help="Save the notes to PlantState."):
        st.session_state.data["customer_notes"] = _notes_text
        _audit.record(st.session_state.data, "customer_notes_save",
                       details={"length": len(_notes_text)})
        _save_active_state(st.session_state.data)
        st.rerun()
with _notes_caption_col:
    if _notes_text == _existing_notes:
        if _existing_notes:
            st.caption(f"✓ Saved ({len(_existing_notes)} chars)")
        else:
            st.caption("No notes saved.")
    else:
        st.caption(f"⚡ Unsaved changes ({len(_notes_text)} chars). "
                    "Click Save to persist.")

st.divider()

# ── Upcoming Trucks + Add ─────────────────────────────────────────────────────

st.subheader("🚛 Trucks")
if data["scheduled_trucks"]:
    st.dataframe(
        [{"SAP": t["sap_order"] or "—", "Product": t["product"],
          "Qty (lbs)": f"{t['quantity_lbs']:,}",
          "Arrival": format_run_hour(data, t["arrival_run_hour"])}
         for t in sorted(data["scheduled_trucks"], key=lambda t: t["arrival_run_hour"])],
        use_container_width=True, hide_index=True,
    )
else:
    st.caption("No trucks scheduled.")

tab_nl, tab_form = st.tabs(["💬 Natural Language", "📝 Form"])

with tab_nl:
    st.caption("`M monday 0800`  ·  `product U tuesday 10am`  ·  `product M wednesday 14:00`")
    nl_text = st.text_input("Describe the delivery:", key="nl_input",
                             placeholder="M monday 0800")
    if st.button("Add Truck", key="nl_add"):
        if not nl_text.strip():
            st.warning("Enter a description.")
        else:
            try:
                product, arr_rh, display = _parse_nl(nl_text, data)
                qty = data["truck_quantities"][product]
                sap = _next_sap(data)
                data["scheduled_trucks"].append({
                    "sap_order": sap, "product": product,
                    "quantity_lbs": qty, "arrival_run_hour": arr_rh,
                })
                _record_sap(data, sap)
                _audit.record(data, _audit.A_ADD_TRUCK,
                               details={"sap": sap, "product": product,
                                         "qty_lbs": qty,
                                         "arrival_run_hour": arr_rh,
                                         "via": "nl"})
                st.success(f"Added {sap}: {product} — {qty:,} lbs arriving {display}")
                st.rerun()
            except ValueError as e:
                st.error(f"Could not parse: {e}")

with tab_form:
    with st.form("add_truck_form"):
        f1, f2 = st.columns(2)
        prod_in = f1.selectbox("Product", options=list(data["truck_quantities"].keys()))
        # Default qty tracks the selected product (was hardcoded to Product U,
        # which silently substituted the wrong size for non-Acme customers).
        # Two-level fallback: selected product → first product → 33000 floor.
        default_qty = int(data["truck_quantities"].get(
            prod_in,
            next(iter(data["truck_quantities"].values()), 33000),
        ))
        qty_in  = f2.number_input("Qty (lbs)", min_value=1000, max_value=70000,
                                   value=default_qty, step=500)
        now_dt  = run_hour_to_dt(data, data["current_run_hour"])
        d1, d2  = st.columns(2)
        arr_date = d1.date_input("Arrival date", value=(now_dt + timedelta(hours=48)).date())
        arr_time = d2.time_input("Arrival time", value=datetime.strptime("08:00", "%H:%M").time())
        if st.form_submit_button("Add Truck"):
            epoch  = datetime.fromisoformat(data["simulation_epoch"])
            arr_dt = datetime.combine(arr_date, arr_time)
            arr_rh = (arr_dt - epoch).total_seconds() / 3600.0
            if arr_rh < data["current_run_hour"] + 48:
                st.error("Arrival must be at least 48 h from current time.")
            else:
                sap = _next_sap(data)
                data["scheduled_trucks"].append({
                    "sap_order": sap, "product": prod_in,
                    "quantity_lbs": int(qty_in), "arrival_run_hour": arr_rh,
                })
                _record_sap(data, sap)
                _audit.record(data, _audit.A_ADD_TRUCK,
                               details={"sap": sap, "product": prod_in,
                                         "qty_lbs": int(qty_in),
                                         "arrival_run_hour": arr_rh,
                                         "via": "form"})
                st.success(f"Added {sap}.")
                st.rerun()

st.divider()

# ── What-If (expander) ────────────────────────────────────────────────────────

with st.expander("🎛️ What-If Scenarios"):
    st.caption(
        "Mutate hypothetical inputs and preview the projection. The main "
        "dashboard is unaffected — nothing here is saved. Use this to "
        "preview a decision (cancel a day, add an extra truck, raise "
        "the consumption rate) BEFORE committing it for real."
    )

    # ── Row 1: classic levers — rate + safety threshold ───────────────────
    wi1, wi2 = st.columns(2)
    wi_rate   = wi1.slider("Consumption rate (lbs/hr per product)", 100, 1000,
                            int(st.session_state.what_if_rate), step=10)
    wi_safety = wi2.slider("Safety stock threshold (lbs)", 0, 20000,
                            int(st.session_state.what_if_safety), step=500)
    st.session_state.what_if_rate   = float(wi_rate)
    st.session_state.what_if_safety = float(wi_safety)

    # ── Row 2: schedule mutations ─────────────────────────────────────────
    wi3, wi4 = st.columns(2)
    with wi3:
        wi_skip = st.multiselect(
            "Skip these weekdays (cancel runs)",
            options=["Mon", "Tue", "Wed", "Thu", "Fri"],
            default=[],
            help="Drop the run windows for the selected weekdays from the "
                 "projection. Models 'what if Wednesday is cancelled.'",
        )
    with wi4:
        wi_extra_runs = st.checkbox(
            "Add weekend runs (Sat + Sun 6am-4pm)",
            value=False,
            help="Append Sat + Sun 6-16 windows to the schedule. Models "
                 "'what if we run weekends to catch up.'",
        )

    # ── Row 3: hypothetical target sliders ────────────────────────────────
    wi5, wi6 = st.columns(2)
    with wi5:
        wi_low = st.slider(
            "Hypothetical low target (lbs)",
            min_value=int(_CFG.tunable_low_min),
            max_value=int(_CFG.tunable_low_max),
            value=int((_overrides or {}).get("low", _CFG.target_low_lbs)),
            step=500,
            key="wi_low_slider",
            help="Preview a target-curve adjustment without committing it.",
        )
    with wi6:
        wi_high = st.slider(
            "Hypothetical high target (lbs)",
            min_value=int(_CFG.tunable_high_min),
            max_value=int(_CFG.tunable_high_max),
            value=int((_overrides or {}).get("high", _CFG.target_high_lbs)),
            step=500,
            key="wi_high_slider",
            help="Preview a target-curve adjustment without committing it.",
        )

    # ── Row 4: extra hypothetical truck ───────────────────────────────────
    wi7, wi8, wi9 = st.columns(3)
    with wi7:
        _wi_truck_products = list(data.get("consumption_rates", {}).keys()) or ["—"]
        wi_truck_product = st.selectbox(
            "Add extra truck — product",
            options=["(none)"] + _wi_truck_products,
            index=0,
            key="wi_truck_product",
        )
    with wi8:
        wi_truck_arrival_h = st.number_input(
            "Arrival run-hour",
            min_value=0, max_value=720, value=24, step=1,
            key="wi_truck_arrival",
            help="Run-hour at which the hypothetical truck arrives.",
        )
    with wi9:
        wi_truck_qty = st.number_input(
            "Quantity (lbs)",
            min_value=0, max_value=80_000, value=33_000, step=1000,
            key="wi_truck_qty",
        )

    # ── Build the what-if data dict ───────────────────────────────────────
    wi_data = copy.deepcopy(data)
    for p in wi_data["consumption_rates"]:
        wi_data["consumption_rates"][p]["lbs_per_hour"] = float(wi_rate)

    # Apply skip-days mutation
    if wi_skip:
        from time_utils import run_hour_to_dt as _rh2dt_wi
        _DAY_INDEX = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4}
        _skip_set = {_DAY_INDEX[d] for d in wi_skip}
        wi_data["run_schedule"] = [
            w for w in wi_data["run_schedule"]
            if _rh2dt_wi(wi_data, w["start_hour"]).weekday() not in _skip_set
        ]

    # Apply weekend-runs mutation
    if wi_extra_runs:
        from time_utils import run_hour_to_dt as _rh2dt_wi2
        # Find next weekend (Sat = wd 5)
        _now_wi = _rh2dt_wi2(wi_data, wi_data["current_run_hour"])
        _days_to_sat = (5 - _now_wi.weekday()) % 7
        _sat_dt = _now_wi.replace(hour=0, minute=0, second=0,
                                    microsecond=0) + timedelta(days=_days_to_sat)
        from time_utils import dt_to_run_hour as _dt2rh_wi
        for _label, _offset in [("Sat-WI", 0), ("Sun-WI", 1)]:
            _start_dt = _sat_dt + timedelta(days=_offset, hours=6)
            _end_dt   = _sat_dt + timedelta(days=_offset, hours=16)
            wi_data["run_schedule"].append({
                "start_hour": _dt2rh_wi(wi_data, _start_dt),
                "end_hour":   _dt2rh_wi(wi_data, _end_dt),
                "label":      _label,
            })

    # Apply extra-truck mutation
    if wi_truck_product != "(none)" and wi_truck_qty > 0:
        wi_data.setdefault("scheduled_trucks", []).append({
            "sap_order":        "WHATIF",
            "product":          wi_truck_product,
            "quantity_lbs":     int(wi_truck_qty),
            "arrival_run_hour": float(wi_data["current_run_hour"] + wi_truck_arrival_h),
        })

    # Apply hypothetical target overrides for the projection chart
    wi_data["target_overrides"] = {"low": float(wi_low), "high": float(wi_high)}

    wi_hist = compute_level_history(
        wi_data, hours=PROJECTION_CHART_HOURS, cfg=st.session_state.cfg
    )

    # ── Render: one chart per product so 3+-product customers work ────────
    _wi_products = list(wi_data.get("consumption_rates", {}).keys())
    if _wi_products:
        _wi_chart_cols = st.columns(min(len(_wi_products), 3))
        for _i, _wi_p in enumerate(_wi_products):
            with _wi_chart_cols[_i % len(_wi_chart_cols)]:
                st.plotly_chart(
                    _chart(wi_hist, _wi_p, safety=wi_safety),
                    use_container_width=True,
                    key=f"wi_chart_{_i}",
                )

# ── Operator activity (expander) ─────────────────────────────────────────────

with st.expander("📜 Recent operator activity"):
    st.caption(
        "Append-only audit trail of every operator action — "
        "who did what when, with the action's payload. Compliance "
        "source-of-truth, retained for the last 1000 entries."
    )
    _activity = _audit.recent(data, n=50)
    if not _activity:
        st.caption("(no operator actions recorded yet)")
    else:
        # Newest first for review
        _act_rows = []
        for _e in reversed(_activity):
            _det = _e.get("details") or {}
            _det_str = ", ".join(f"{k}={v!r}" for k, v in _det.items())
            _act_rows.append({
                "When":    _e.get("iso", "?")[:19].replace("T", " "),
                "User":    _e.get("user", "?"),
                "Action":  _e.get("action", "?"),
                "Details": _det_str,
            })
        st.dataframe(_act_rows, use_container_width=True, hide_index=True)

# ── Email Configuration (expander) ───────────────────────────────────────────

with st.expander("✉️ Email Configuration"):
    cfg = {}
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
    with st.form("email_form"):
        e_all = st.text_input(
            "📬 Enter one email to receive all demo emails",
            value=cfg.get("all_in_one_email", ""),
            placeholder="you@example.com",
            help="Every alert, load-entry PDF, schedule reminder, and test email will be sent here.",
        )
        st.info("📅 Send run schedules to **vmiprototype@gmail.com** — the system reads, parses, and applies automatically.")
        ec1, ec2 = st.columns(2)
        e_contact = ec1.text_input("Customer contact (schedule reminders)",
                                    value=cfg.get("anna_email", ""))
        e_dist    = ec2.text_input("Distribution group (alert emails)",
                                    value=cfg.get("distribution_group", ""))
        e_cs      = st.text_input("CS email (load-entry PDFs)", value=cfg.get("cs_email", ""))
        sb_col, tb_col = st.columns(2)
        save_btn = sb_col.form_submit_button("💾 Save", use_container_width=True)
        test_btn = tb_col.form_submit_button("📧 Test Email", use_container_width=True)
    if save_btn:
        # Preserve any existing credential fields already in the config file
        new_cfg = {
            **{k: cfg.get(k, "") for k in ("email_address", "app_password",
                                            "smtp_server", "smtp_port",
                                            "imap_server", "imap_port")},
            "smtp_server": cfg.get("smtp_server", "smtp.gmail.com"),
            "smtp_port":   cfg.get("smtp_port", 587),
            "imap_server": cfg.get("imap_server", "imap.gmail.com"),
            "imap_port":   cfg.get("imap_port", 993),
            "anna_email":         e_contact,
            "distribution_group": e_dist,
            "cs_email":           e_cs,
            "all_in_one_email":   e_all,
            "anthropic_api_key":  cfg.get("anthropic_api_key", ""),
        }
        with open(CONFIG_PATH, "w") as f:
            json.dump(new_cfg, f, indent=2)
        st.success("Config saved.")
    if test_btn:
        target = e_all or e_dist or e_contact or e_cs
        if not target:
            st.error("No email address configured.")
        else:
            send_status = "queued"
            try:
                OutlookClient(load_config()).send_mail(
                    [target], "VMI Prototype — Test Email",
                    "Test email from the VMI Prototype demo. Email integration is working.",
                )
                send_status = "sent"
                st.success(f"Test email sent to {target}.")
            except Exception as e:
                send_status = "not sent (no SMTP in demo mode)"
                st.warning("Email logged — no SMTP server configured in demo mode.")
            # Always log, regardless of send outcome
            st.session_state.email_log.append({
                "sim_time": format_run_hour(st.session_state.data,
                                            st.session_state.data["current_run_hour"]),
                "type":    "Test Email",
                "to":      target,
                "subject": "VMI Prototype — Test Email",
                "body":    "Test email from the VMI Prototype demo. Email integration is working.",
                "status":  send_status,
            })

# ── Alert Rules Reference (expander) ─────────────────────────────────────────

with st.expander("📋 Alert Rules Reference"):
    st.markdown(f"""
**🔴 / 🟡 Alerts** — block a planner step or demand operator action.

| Alert | Triggers when | Threshold |
|---|---|---|
| **Safety Stock** | Projected combined product level drops below threshold within the next {PROJECTION_WINDOW_HOURS} h | **{SAFETY_STOCK_LBS:,} lbs** combined per product |
| **Overfill (multi-tank product)** | Delivery projected to exceed combined capacity across the product's tanks | Truck qty > projected combined space across all tanks holding that product |
| **Overfill (single-tank product)** | Delivery projected to exceed the assigned tank's available space | Truck qty > projected space in the lowest tank holding that product |
| **Plant State Mismatch** | Plant is running when the schedule says it's down, or down when the schedule says it's running | **> {PLANT_STATE_MISMATCH_HOURS} hours** off-schedule (reads live telemetry in production) |
| **Lead-Time Warning** | On-hand usable + scheduled inbound < demand for the next **{LEAD_TIME_HOURS} scheduled run hours** | — |
| **Late Truck** | A scheduled truck has not arrived | **> {LATE_TRUCK_HOURS} hours** past scheduled arrival |
| **Reminder Sent** (yellow) | Friday 11 AM sim time reached with no schedule on file for next week | Reminder email sent to customer contact at 11 AM; clears when schedule is received or the 3 PM alert replaces it |
| **No Schedule** (red) | Friday 3 PM sim time reached with no schedule on file for next week | RED alert sent to the distribution group; fires until `schedule_received_for_week` is set |
| **Low Confidence Parse** | A schedule email was found but fewer than 3 days could be parsed | Clears automatically when a high-confidence schedule is applied |
| **VMI Automation Off** (red) | `vmi_automation_enabled` is OFF at Friday 9 AM sim time | Recurs every Friday until automation is re-enabled |

**🟡 Anomalies** — soft "did you mean it?" warnings. Don't block; surface as a YELLOW chip so the operator can confirm or override.

| Anomaly | Triggers when | Threshold |
|---|---|---|
| **Run hours unusual** | Most recent week's total run-hours is a >2σ outlier vs. last several weeks | Needs 3+ historical weeks; catches stale forwards / parser misreads (e.g. 24h instead of 12h) |
| **Day shape unusual** | This week's run schedule covers a weekday the customer hasn't used in recent history | Catches "Sun shift" inserted by mistake into a Mon-Fri customer |
| **Holiday in run window** | A parsed run window covers a date in `cfg.plant_holidays` | Operator likely forgot the holiday; the planner will gate it out via `is_running_at`, but worth confirming |
| **Truck cadence unusual** | Scheduled trucks for the upcoming week is outside `predicted ± {{cfg.truck_cadence_band_pct × 100}}%` (min ±{{cfg.truck_cadence_min_band}}) | Forecaster output is the baseline (4-week 40/30/20/10 weighted, holiday-gated); prevents single-source drift |
| **Schedule arrival unusual** | This week's schedule email arrived on a weekday the customer hasn't used in the last 8 schedules | Catches stale-forward / re-fetch of an old email |

**Reorder target** scales with run activity:
- Light week ({TARGET_LOW_RUN_HOURS} run hrs/wk or less): **{TARGET_LOW_LBS:,} lbs**
- Heavy week ({TARGET_HIGH_RUN_HOURS} run hrs/wk or more): **{TARGET_HIGH_LBS:,} lbs**
- Intermediate weeks: linear interpolation between the two

The operator can override the low/high targets via the **VMI Controls** sliders below. The override persists week-to-week until cleared.
""")

# ── Alert History (expander) ─────────────────────────────────────────────────
# Persistent record of every distinct alert event. Written by
# email_hooks.send_alert_emails_if_new on first appearance of each hash; the
# same condition re-firing later (after it clears and returns) logs a new row.
# This is the read-out surface for manually tuning targets.

_alert_log = data.get("alert_log", [])
with st.expander(
    f"📋 Alert History ({len(_alert_log)} logged)",
    expanded=False,
):
    if not _alert_log:
        st.caption(
            "No alerts have fired yet. When a tank drops below safety stock, "
            "a delivery overfills, or a schedule deadline is missed, the event "
            "will be recorded here for later review."
        )
    else:
        # Newest first. Filters are view controls only — no data is mutated.
        products_seen = sorted({e.get("product") for e in _alert_log if e.get("product")})
        fc1, fc2, fc3 = st.columns(3)
        dir_filter  = fc1.selectbox("Direction", ["all", "too_low", "too_full", "other"],
                                    key="alertlog_dir")
        prod_filter = fc2.selectbox("Product",   ["all"] + products_seen,
                                    key="alertlog_product")
        sev_filter  = fc3.selectbox("Severity",  ["all", "red_flag", "warning"],
                                    key="alertlog_sev")

        rows = list(reversed(_alert_log))
        if dir_filter  != "all":
            rows = [r for r in rows if r.get("direction") == dir_filter]
        if prod_filter != "all":
            rows = [r for r in rows if r.get("product")   == prod_filter]
        if sev_filter  != "all":
            rows = [r for r in rows if r.get("severity")  == sev_filter]

        # Tight projection of fields — full entries remain in data.json for
        # anyone who wants to dig deeper.
        view = [{
            "time":      r.get("logged_at_iso") or f"run-hour {r.get('logged_at_run_hour', 0):.0f}",
            "severity":  r.get("severity"),
            "direction": r.get("direction"),
            "type":      r.get("type"),
            "product":   r.get("product") or "—",
            "tank":      r.get("tank") or "—",
            "level_lbs": r.get("level_lbs"),
            "text":      r.get("text"),
        } for r in rows]

        st.dataframe(view, hide_index=True, use_container_width=True)
        st.caption(f"{len(view)} of {len(_alert_log)} entries shown.")

# ── Recent Email Activity (expander) ─────────────────────────────────────────

with st.expander(
    f"📨 Recent Email Activity ({len(st.session_state.email_log)} sent this session)",
    expanded=False,
):
    if not st.session_state.email_log:
        st.caption("No emails sent yet this session. Advance the clock past a Friday 11 AM or 3 PM, trigger an alert, or run the planner to see activity here.")
    else:
        for entry in reversed(st.session_state.email_log):
            etype = entry.get("type", "Email")
            # Tag color by type, themed to the new palette
            if etype.startswith("Alert"):
                tag_color = "#F43F5E"   # rose
            else:
                tag_color = {
                    "Schedule Reminder": "#F59E0B",   # amber
                    "Schedule Applied":  "#22C55E",   # green
                    "CS Load Entry":     "#1E3A8A",   # navy
                    "Test Email":        "#64748B",   # slate
                }.get(etype, "#64748B")
            # Status badge
            status = entry.get("status", "")
            if status == "sent":
                status_html = (
                    ' <span style="color:#15803D;font-size:0.72em;font-weight:600;'
                    'background:#DCFCE7;padding:1px 6px;border-radius:999px;">✓ sent</span>'
                )
            elif status and "not sent" in status:
                status_html = (
                    ' <span style="color:#92400E;font-size:0.72em;font-weight:600;'
                    'background:#FEF3C7;padding:1px 6px;border-radius:999px;">⚠ not sent</span>'
                )
            else:
                status_html = (
                    ' <span style="color:#475569;font-size:0.72em;font-weight:600;'
                    'background:#F1F5F9;padding:1px 6px;border-radius:999px;">• logged</span>'
                )
            st.markdown(
                f'''
                <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:8px;
                            padding:0.6rem 0.85rem;margin-bottom:0.5rem;
                            font-family:'Inter',sans-serif;">
                    <div style="display:flex;align-items:center;gap:0.5rem;flex-wrap:wrap;">
                        <span style="background:{tag_color};color:#fff;padding:2px 8px;
                                     border-radius:4px;font-size:0.7em;font-weight:600;
                                     letter-spacing:0.04em;">{etype}</span>
                        <strong style="color:#0F1629;font-size:0.92rem;">{entry.get("subject","")}</strong>
                        {status_html}
                    </div>
                    <div style="margin-top:0.3rem;font-size:0.78rem;color:#64748B;">
                        <span style="color:#475569;">To:</span> {entry.get("to","")}
                        &nbsp;·&nbsp;
                        <span style="color:#475569;">Sim time:</span> {entry.get("sim_time","")}
                    </div>
                </div>
                ''',
                unsafe_allow_html=True,
            )
            if entry.get("body"):
                with st.expander("Show body", expanded=False):
                    st.text(entry["body"][:600] + ("…" if len(entry.get("body","")) > 600 else ""))

# ── PDF Preview ───────────────────────────────────────────────────────────────

if st.session_state.pdf_bytes:
    st.subheader("📄 CS Load Entry PDF")
    # Inline preview removed: Chrome (and most modern browsers) block
    # data:application/pdf URLs inside iframes via CSP, leaving the
    # operator with a gray "broken-image" panel. The download button
    # works reliably across all browsers — make it the primary action
    # and explain why there's no inline preview.
    st.markdown(
        "Your CS load-entry PDF is ready. Click below to open it in "
        "your browser's PDF viewer."
    )
    st.download_button(
        "⬇️ Download CS Load Entry PDF",
        data=st.session_state.pdf_bytes,
        file_name="cs_load_entry.pdf",
        mime="application/pdf",
        type="primary",
        use_container_width=True,
    )
    st.caption(
        "Inline preview is disabled — modern browsers block embedded "
        "data-URL PDFs by default. Download to view."
    )


# ── Persist Streamlit-side mutations to data.json ─────────────────────────────
# Streamlit reruns the entire script on every interaction. Persist the
# current state at the end of each rerun so CLI scripts and a restarted
# session see the same truth. Update _disk_mtime_seen so the reload
# detector at the top of the next rerun doesn't ping-pong on our own
# write. Save errors are surfaced as a warning rather than crashing the
# UI — the operator can still see the dashboard even if disk is full.
try:
    _save_active_state(st.session_state.data)
    if _os_state.path.exists(_DATA_FILE):
        st.session_state._disk_mtime_seen = _os_state.path.getmtime(_DATA_FILE)
except Exception as _persist_err:
    st.warning(f"Could not persist data.json: {_persist_err}")
