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
    get_all_alerts, simulate_consume, is_running_at,
    find_lowest_in, find_other_in, SAFETY_STOCK_LBS,
    LEAD_TIME_HOURS, LATE_TRUCK_HOURS, PROJECTION_WINDOW_HOURS,
    PLANT_STATE_MISMATCH_HOURS,
)
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

DEFAULTS_PATH = Path("defaults.json")
CONFIG_PATH   = Path("email_config.json")
APP_TIMEZONE  = "America/New_York"   # used for sim-clock anchor and display


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

COLORS = {
    "U-Tank1": "#1E3A8A",   # navy
    "U-Tank2": "#60A5FA",   # light blue
    "M-Tank1": "#0F766E",   # deep teal
    "M-Tank2": "#5EEAD4",   # light teal
}

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


if "data"           not in st.session_state: st.session_state.data           = _defaults()
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

data = st.session_state.data


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

    def _consume(seg):
        if seg > 0 and burning:
            for p, r in rates.items():
                simulate_consume(tanks, p, r["lbs_per_hour"] * seg)

    def _deliver(t):
        tgt = find_lowest_in(tanks, t["product"])
        if not tgt:
            return
        tank  = tanks[tgt]
        space = tank["max_capacity_lbs"] - tank["current_level_lbs"]
        pour  = min(t["quantity_lbs"], space)
        tank["current_level_lbs"] = round(tank["current_level_lbs"] + pour, 1)
        ov = t["quantity_lbs"] - pour
        if ov > 0:
            other = find_other_in(tanks, t["product"], tgt)
            if other:
                ot = tanks[other]
                ot["current_level_lbs"] = round(
                    ot["current_level_lbs"] + min(ov, ot["max_capacity_lbs"] - ot["current_level_lbs"]), 1
                )
        log.append(f"  Delivered {t['sap_order']} — {t['product']} {t['quantity_lbs']:,} lbs")
        done.append(t["sap_order"])

    for ev_time, ev_type, payload in events:
        _consume(ev_time - clock)
        clock = ev_time
        if   ev_type == "s": burning = True;  log.append(f"Plant running at {format_run_hour(data, ev_time)}")
        elif ev_type == "e": burning = False; log.append(f"Plant stopped at {format_run_hour(data, ev_time)}")
        elif ev_type == "d": _deliver(payload)
    _consume(end - clock)

    data["scheduled_trucks"] = [t for t in data["scheduled_trucks"] if t["sap_order"] not in done]
    data["current_run_hour"] = end
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
                target = get_target_for_week(week_rh)
                all_new = []
                for product in data["consumption_rates"]:
                    plan_cap = _io.StringIO()
                    _sys.stdout = plan_cap
                    try:
                        new = plan_for_product(
                            data, product, target, week_start, week_end, all_new
                        )
                    finally:
                        _sys.stdout = _old_stdout
                    for line in plan_cap.getvalue().strip().splitlines():
                        log.append(f"  [Planner] {line}")
                    all_new.extend(new)

                if all_new:
                    existing = [
                        t["sap_order"] for t in data["scheduled_trucks"]
                        if t.get("sap_order")
                    ]
                    nums = [
                        int(_re.search(r"\d+$", s).group())
                        for s in existing if _re.search(r"\d+$", s)
                    ]
                    next_n = max(nums) + 1 if nums else 20001
                    all_new.sort(key=lambda t: t["arrival_run_hour"])
                    for i, t in enumerate(all_new):
                        t["sap_order"] = f"SAP{next_n + i}"
                        t.pop("_planned_reason", None)
                        data["scheduled_trucks"].append(t)
                    log.append(
                        f"[Auto] Committed {len(all_new)} truck order(s): "
                        + ", ".join(t["sap_order"] for t in all_new)
                    )

                    # CS load-entry email
                    cs_status = "queued"
                    try:
                        _send_cs(data, all_new)
                        cs_status = "sent"
                    except Exception:
                        cs_status = "not sent (no SMTP in demo mode)"
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


def _chart(hist, product, safety=None):
    if safety is None:
        safety = SAFETY_STOCK_LBS
    prefix = "U-" if product == "Product U" else "M-"
    tnks   = [n for n in hist["tanks"] if n.startswith(prefix)]
    x_vals = hist["run_hours"]          # numeric floats — safe for add_vline/vrect

    tick_idxs = list(range(0, len(x_vals), 24))
    if (len(x_vals) - 1) not in tick_idxs:
        tick_idxs.append(len(x_vals) - 1)
    tick_vals = [x_vals[i] for i in tick_idxs]
    tick_text = [_short_label(hist["datetimes"][i]) for i in tick_idxs]

    fig = go.Figure()
    # Run windows — subtle warm tint so they read as "active" without competing
    for w in hist["run_windows"]:
        fig.add_vrect(x0=w["start_hour"], x1=w["end_hour"],
                      fillcolor="rgba(0,199,169,0.07)", line_width=0)
    for name in tnks:
        fig.add_trace(go.Scatter(
            x=x_vals, y=hist["tanks"][name], name=name,
            line=dict(color=COLORS.get(name, "#888"), width=2.5),
            customdata=hist["datetimes"],
            hovertemplate=f"<b>{name}</b><br>%{{customdata}}<br>%{{y:,.0f}} lbs<extra></extra>",
        ))
    # Safety stock — softer rose, smaller annotation
    fig.add_hline(
        y=safety, line_dash="dot", line_color="#F43F5E", line_width=1.2,
        annotation_text="Safety stock", annotation_position="bottom right",
        annotation_font=dict(size=10, color="#9F1239", family="Inter"),
    )
    for ev in hist["truck_events"]:
        if ev["product"] == product:
            fig.add_vline(
                x=ev["run_hour"],
                line_dash="dash", line_color="#F59E0B", line_width=1.2,
                annotation_text=f"{ev['sap']} +{ev['qty'] // 1000}k",
                annotation_position="top left",
                annotation_font=dict(size=10, color="#92400E", family="Inter"),
            )
    fig.update_layout(
        title=dict(
            text=product,
            font=dict(size=14, family="Inter", color="#1E2A45"),
            x=0.01, xanchor="left",
        ),
        height=280,
        margin=dict(l=5, r=5, t=34, b=44),
        font=dict(family="Inter", color="#1E2A45", size=11),
        yaxis=dict(
            range=[0, 37000], tickformat=",", title="lbs",
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


def _tank_info(col, name, info):
    pct      = info["current_level_lbs"] / info["max_capacity_lbs"]
    pct_clip = max(0.0, min(1.0, pct))
    if pct < 0.2:
        bar_color, dot_color = "#F43F5E", "#F43F5E"   # critical → red
    elif pct < 0.5:
        bar_color, dot_color = "#F59E0B", "#F59E0B"   # low → amber
    else:
        bar_color, dot_color = "#00C7A9", "#22C55E"   # healthy → teal/green
    is_draw = info["status"] == "draw"
    badge_bg = "#0F1629" if is_draw else "#F1F5F9"
    badge_fg = "#FFFFFF" if is_draw else "#64748B"
    badge_lbl = "DRAW" if is_draw else "STANDBY"
    col.markdown(f"""
    <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:8px;
                padding:0.55rem 0.75rem;margin-bottom:0.4rem;
                font-family:'Inter',sans-serif;">
        <div style="display:flex;align-items:center;justify-content:space-between;">
            <div style="display:flex;align-items:center;gap:0.4rem;">
                <span style="display:inline-block;width:8px;height:8px;border-radius:50%;
                             background:{dot_color};"></span>
                <span style="font-weight:600;color:#0F1629;font-size:0.88rem;">{name}</span>
            </div>
            <span style="background:{badge_bg};color:{badge_fg};font-size:0.62rem;
                         font-weight:600;letter-spacing:0.06em;padding:2px 7px;
                         border-radius:4px;">{badge_lbl}</span>
        </div>
        <div style="margin-top:0.35rem;color:#64748B;font-size:0.78rem;">
            <span style="color:#0F1629;font-weight:600;">{info['current_level_lbs']:,.0f}</span>
            &nbsp;/&nbsp; {info['max_capacity_lbs']:,} lbs
            &nbsp;·&nbsp; {pct*100:.0f}%
        </div>
        <div style="margin-top:0.3rem;height:5px;background:#F1F5F9;border-radius:3px;
                    overflow:hidden;">
            <div style="height:100%;width:{pct_clip*100:.1f}%;background:{bar_color};
                        border-radius:3px;"></div>
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


def _parse_nl(text, data):
    tl = text.lower().strip()
    if re.search(r"\bproduct\s+u\b", tl) or re.search(r"\bu\b", tl):
        product = "Product U"
    elif re.search(r"\bproduct\s+m\b", tl) or re.search(r"\bm\b", tl):
        product = "Product M"
    else:
        raise ValueError("Specify 'Product U' or 'Product M' (or just U / M).")
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


def _next_sap(data):
    existing = [t["sap_order"] for t in data["scheduled_trucks"] if t.get("sap_order")]
    nums = [int(re.search(r"\d+$", s).group()) for s in existing if re.search(r"\d+$", s)]
    return f"SAP{max(nums) + 1 if nums else 90001}"


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
            help="Run `python build_product_sheet.py` to generate.",
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
        for name, val in tank_vals.items():
            data["tanks"][name]["current_level_lbs"] = float(val)
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
        st.rerun()
    if rst_col.button("🔄 Reset", use_container_width=True):
        from datetime import timezone as _tz_utc
        st.session_state.data                               = _defaults()
        st.session_state.data["run_schedule"]               = []
        st.session_state.data["schedule_received_for_week"] = None
        st.session_state.data["schedule_parse_issue"]       = None
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

alerts = get_all_alerts(data)
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
        # Alerts are structured dicts (see alerts._alert). Severity keys the
        # styling; the text field strips the legacy prefix for clean display.
        is_red = a.get("severity") == "red_flag"
        label  = "🔴 &nbsp; CRITICAL" if is_red else "🟡 &nbsp; WARNING"
        raw    = a.get("text", "")
        text   = (raw.replace("RED FLAG: ", "")
                     .replace("YELLOW FLAG: ", "")
                     .replace("WARNING: ", ""))
        bg     = "#FFF1F2" if is_red else "#FFFBEB"
        border = "#F43F5E" if is_red else "#F59E0B"
        lcolor = "#9F1239" if is_red else "#92400E"
        bcolor = "#FECDD3" if is_red else "#FDE68A"
        st.markdown(f"""
        <div style="background:{bg};border:1px solid {bcolor};border-left:4px solid {border};
                    border-radius:8px;padding:0.65rem 1rem;margin-bottom:0.5rem;
                    font-family:'Inter',sans-serif;">
            <span style="font-size:0.72rem;font-weight:600;color:{border};
                         letter-spacing:0.04em;text-transform:uppercase;">{label}</span>
            <div style="color:{lcolor};font-size:0.9rem;font-weight:400;margin-top:0.2rem;">{text}</div>
        </div>""", unsafe_allow_html=True)

st.divider()

# ── Trendline Charts with inline tank status ──────────────────────────────────

st.subheader("📈 10-Day Projection")
hist = compute_level_history(data, hours=240)
c1, c2 = st.columns(2)

with c1:
    st.plotly_chart(_chart(hist, "Product U"), use_container_width=True, key="ch_u")
    t1, t2 = st.columns(2)
    _tank_info(t1, "U-Tank1", data["tanks"]["U-Tank1"])
    _tank_info(t2, "U-Tank2", data["tanks"]["U-Tank2"])

with c2:
    st.plotly_chart(_chart(hist, "Product M"), use_container_width=True, key="ch_m")
    t1, t2 = st.columns(2)
    _tank_info(t1, "M-Tank1", data["tanks"]["M-Tank1"])
    _tank_info(t2, "M-Tank2", data["tanks"]["M-Tank2"])

st.divider()

# ── Schedule Parser | Auto-Planner (side by side) ────────────────────────────

sp_col, ap_col = st.columns([2, 3])

# ── Left: Schedule Parser ────────────────────────────────────────────────────
with sp_col:
    st.subheader("📅 Schedule Parser")
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
            entries, confidence, notes = parse_schedule(
                sched_text, api_key=_get_anthropic_key(), now_dt=sim_now
            )
            st.session_state.parse_result = (entries, confidence, notes)
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
        entries, confidence, notes = st.session_state.parse_result
        if confidence == "high":
            pill_bg, pill_fg, pill_label = "#DCFCE7", "#166534", "HIGH CONFIDENCE"
        else:
            pill_bg, pill_fg, pill_label = "#FFE4E6", "#9F1239", "LOW CONFIDENCE"
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
            </div>
            """,
            unsafe_allow_html=True,
        )
        if entries:
            DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            rows = []
            for e in entries:
                total_h = e[2] - e[1]
                end_day_n = (e[0] + total_h // 24) % 7 if total_h > 24 else e[0]
                rows.append({
                    "Day": DAYS[e[0]],
                    "Start": f"{e[1]:02d}:00",
                    "End day": DAYS[end_day_n],
                    "End": f"{e[2] % 24:02d}:00",
                    "Hrs": f"{total_h:.0f}",
                })
            st.dataframe(rows, use_container_width=True, hide_index=True)

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

        btn_lbl = "✅ Apply to Schedule" if confidence == "high" else "⚠️ Apply Anyway"
        if st.button(btn_lbl, use_container_width=True):
            sim_now = run_hour_to_dt(data, data["current_run_hour"])
            data, removed, added = apply_schedule_to_data(data, entries, now_dt=sim_now)
            st.session_state.parse_result = None
            st.success(f"Applied: {removed} old window(s) removed, {len(added)} new added.")
            st.rerun()

# ── Right: Auto-Planner ───────────────────────────────────────────────────────
with ap_col:
    st.subheader("🤖 Auto-Planner")
    week_start, week_end = get_target_week_bounds(data)
    week_rh    = get_run_hours_in_window(data, week_start, week_end)
    target_lbs = get_target_for_week(week_rh)

    ic1, ic2, ic3 = st.columns(3)
    ic1.metric("Plan week starts", format_run_hour(data, week_start).split()[0] + " " +
               format_run_hour(data, week_start).split()[1])
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
                    new = plan_for_product(data, product, target_lbs, week_start, week_end, all_new)
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
            existing = [t["sap_order"] for t in data["scheduled_trucks"] if t.get("sap_order")]
            nums = [int(re.search(r"\d+$", s).group()) for s in existing if re.search(r"\d+$", s)]
            next_n  = max(nums) + 1 if nums else 20001
            sorted_t = sorted(st.session_state.planned_trucks, key=lambda t: t["arrival_run_hour"])
            for i, t in enumerate(sorted_t):
                t["sap_order"] = f"SAP{next_n + i}"
                t.pop("_planned_reason", None)
                data["scheduled_trucks"].append(t)
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
            except Exception:
                cs_send_status = "not sent (no SMTP in demo mode)"
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
            st.success(
                f"Added {len(sorted_t)} truck(s) — "
                f"SAP{next_n} through SAP{next_n + len(sorted_t) - 1}."
            )
            st.rerun()

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
                st.success(f"Added {sap}: {product} — {qty:,} lbs arriving {display}")
                st.rerun()
            except ValueError as e:
                st.error(f"Could not parse: {e}")

with tab_form:
    with st.form("add_truck_form"):
        f1, f2 = st.columns(2)
        prod_in = f1.selectbox("Product", options=list(data["truck_quantities"].keys()))
        qty_in  = f2.number_input("Qty (lbs)", min_value=1000, max_value=70000,
                                   value=data["truck_quantities"].get("Product U", 33000), step=500)
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
                st.success(f"Added {sap}.")
                st.rerun()

st.divider()

# ── What-If (expander) ────────────────────────────────────────────────────────

with st.expander("🎛️ What-If Scenarios"):
    st.caption("Explore parameter changes — main dashboard is unaffected.")
    wi1, wi2 = st.columns(2)
    wi_rate   = wi1.slider("Consumption rate (lbs/hr per product)", 100, 1000,
                            int(st.session_state.what_if_rate), step=10)
    wi_safety = wi2.slider("Safety stock threshold (lbs)", 0, 20000,
                            int(st.session_state.what_if_safety), step=500)
    st.session_state.what_if_rate   = float(wi_rate)
    st.session_state.what_if_safety = float(wi_safety)
    wi_data = copy.deepcopy(data)
    for p in wi_data["consumption_rates"]:
        wi_data["consumption_rates"][p]["lbs_per_hour"] = float(wi_rate)
    wi_hist = compute_level_history(wi_data, hours=240)
    wc1, wc2 = st.columns(2)
    wc1.plotly_chart(_chart(wi_hist, "Product U", safety=wi_safety),
                     use_container_width=True, key="wi_u")
    wc2.plotly_chart(_chart(wi_hist, "Product M", safety=wi_safety),
                     use_container_width=True, key="wi_m")

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
| Alert | Triggers when | Threshold |
|---|---|---|
| **Safety Stock** | Projected combined product level drops below threshold within the next {PROJECTION_WINDOW_HOURS} h | **{SAFETY_STOCK_LBS:,} lbs** combined per product |
| **Overfill — Product M** | Delivery projected to exceed combined capacity of both M tanks | Truck qty > projected space across M-Tank1 + M-Tank2 (Product M spans both tanks) |
| **Overfill — Product U** | Delivery projected to exceed the lowest U tank's available space | Truck qty > projected space in lowest U tank (Product U must fit in one tank) |
| **Plant State Mismatch** | Plant is running when the schedule says it's down, or down when the schedule says it's running | **> {PLANT_STATE_MISMATCH_HOURS} hours** off-schedule (reads live telemetry in production) |
| **Lead-Time Warning** | On-hand usable + scheduled inbound < demand for the next **{LEAD_TIME_HOURS} scheduled run hours** | — |
| **Late Truck** | A scheduled truck has not arrived | **> {LATE_TRUCK_HOURS} hours** past scheduled arrival |
| **Reminder Sent** (yellow) | Friday 11 AM sim time reached with no schedule on file for next week | Shows from 11 AM until schedule is received or the 3 PM alert replaces it |
| **No Schedule** (red) | Friday 3 PM sim time reached with no schedule on file for next week | Replaces the 11 AM alert; fires until `schedule_received_for_week` is set |
| **Low Confidence Parse** | A schedule email was found but fewer than 3 days could be parsed | Clears automatically when a high-confidence schedule is applied |

**Reorder target** scales with run activity:
- Light week ({TARGET_LOW_RUN_HOURS} run hrs/wk or less): **{TARGET_LOW_LBS:,} lbs**
- Heavy week ({TARGET_HIGH_RUN_HOURS} run hrs/wk or more): **{TARGET_HIGH_LBS:,} lbs**
- Intermediate weeks: linear interpolation between the two
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
    b64 = base64.b64encode(st.session_state.pdf_bytes).decode()
    st.components.v1.html(
        f'<iframe src="data:application/pdf;base64,{b64}" '
        f'width="100%" height="480px" '
        f'style="border:1px solid #E2E8F0; border-radius:8px;"></iframe>',
        height=500,
    )
    st.download_button("⬇️ Download PDF", data=st.session_state.pdf_bytes,
                       file_name="cs_load_entry.pdf", mime="application/pdf")
