"""
email_hooks.py
--------------
Orchestration layer that connects simulation events to email actions.
All functions are safe to call from any CLI script — failures print a
warning and never raise into the calling script.

Four entry points
-----------------
send_alert_emails_if_new(data)   -> dict  (updated data; caller must save)
send_cs_load_entry(data, trucks) -> None
send_friday_reminder_if_needed(data) -> None
alert_hash(alert_str)            -> str
"""

import hashlib

import logging
logger = logging.getLogger(__name__)

from datetime import datetime, timedelta

from alerts import get_all_alerts
from email_client import OutlookClient, load_config
from pdf_generator import build_load_entry_pdf
import time_utils


# ---------------------------------------------------------------------------
# Hash helper
# ---------------------------------------------------------------------------

def alert_hash(alert_str):
    """Return a stable SHA-1 hex digest for an alert string."""
    return hashlib.sha1(alert_str.strip().encode("utf-8")).hexdigest()


def _to(config, *addresses):
    """
    Build a recipient list from the given addresses, appending all_in_one_email
    if configured so every email is BCC'd to one trial inbox.
    """
    all_in_one = config.get("all_in_one_email", "").strip()
    recipients = [a for a in addresses if a]
    if all_in_one and all_in_one not in recipients:
        recipients.append(all_in_one)
    return recipients


# ── Severity-based escalation routing (Phase 4) ──────────────────────────────
#
# WARNING (yellow) alerts go to a smaller list:
#     scheduler_email + scheduler_backup_email
#
# RED FLAG (red) alerts go to a bigger list:
#     scheduler + backup + scheduler_manager + scheduling_team_distribution
#     + shipping_team_distribution + operations_email
#
# All optional. Missing fields are dropped from the recipient list.
# If NO escalation fields are configured, we fall back to the legacy
# distribution_group so unconfigured deployments behave exactly as
# before.

def _escalation_recipients(config, severity: str):
    """Return the recipient list for a given alert severity.

    severity : "red_flag" | "warning" | other (treated as warning)

    Backwards compat: if no escalation fields exist, returns
    [distribution_group] (the legacy single-list behavior).
    """
    if not config:
        return []
    has_escalation = any(
        config.get(k, "").strip() for k in (
            "scheduler_email", "scheduler_backup_email",
            "scheduler_manager_email", "scheduling_team_distribution",
            "shipping_team_distribution", "operations_email",
        )
    )
    if not has_escalation:
        # Legacy single-list fallback
        legacy = config.get("distribution_group", "").strip()
        return [legacy] if legacy else []

    # RED FLAG (red): full escalation chain.
    # Only the exact "red_flag" severity escalates; anything else
    # (warning, info, future severities) stays on the short list.
    # Defensive: prevents a typo / unrecognized severity from
    # accidentally paging the whole org.
    if severity == "red_flag":
        keys = (
            "scheduler_email",
            "scheduler_backup_email",
            "scheduler_manager_email",
            "scheduling_team_distribution",
            "shipping_team_distribution",
            "operations_email",
        )
    else:
        keys = ("scheduler_email", "scheduler_backup_email")
    out = []
    for k in keys:
        addr = (config.get(k) or "").strip()
        if addr and addr not in out:
            out.append(addr)
    return out


# ---------------------------------------------------------------------------
# Alert emails
# ---------------------------------------------------------------------------

def send_alert_emails_if_new(data):
    """
    Compare the current alert list against previously-emailed hashes stored
    in data["alerted_hashes"].  Send one email for any new alerts, then
    update the hash list.

    Also appends each new-hash alert to ``data["alert_log"]`` — the persistent
    history used by the Alert History panel. Logging happens BEFORE the email
    attempt so a send failure doesn't lose the detection record.

    - New hashes are only persisted after a successful send (so failures retry).
    - Stale hashes (alerts that cleared) are always pruned.

    Returns the updated data dict.  The caller is responsible for saving it.
    """
    current_alerts = get_all_alerts(data)                           # list[dict]
    current = {alert_hash(a["text"]): a for a in current_alerts}    # hash -> dict
    prev    = set(data.get("alerted_hashes", []))

    new_hashes  = [h for h in current if h not in prev]
    new_alerts  = [current[h] for h in new_hashes]                  # list[dict]

    # ── Append to persistent alert log (BEFORE email attempt) ───────────────
    # First-appearance-only: the dedup against `alerted_hashes` means a
    # condition that keeps firing across many ticks only logs once, until it
    # clears and later re-fires. That's exactly the "event" granularity we
    # want for review.
    if new_alerts:
        log = data.setdefault("alert_log", [])
        run_hour  = data.get("current_run_hour", 0)
        try:
            logged_at = time_utils.run_hour_to_dt(data, run_hour).isoformat()
        except Exception:
            logged_at = None
        for h, a in zip(new_hashes, new_alerts):
            log.append({
                "logged_at_run_hour": run_hour,
                "logged_at_iso":      logged_at,
                "hash":               h,
                "type":               a.get("type"),
                "severity":           a.get("severity"),
                "direction":          a.get("direction"),
                "product":            a.get("product"),
                "tank":               a.get("tank"),
                "level_lbs":          a.get("level_lbs"),
                "text":               a.get("text"),
            })

    if new_alerts:
        config = load_config()
        # Split new alerts by severity → two recipient lists.
        red_alerts     = [a for a in new_alerts
                          if a.get("severity") == "red_flag"]
        warning_alerts = [a for a in new_alerts
                          if a.get("severity") != "red_flag"]
        any_send_failed = False
        any_send_succeeded = False
        if config:
            client = OutlookClient(config)
            for severity_key, alerts_for_send in (
                ("red_flag", red_alerts),
                ("warning",  warning_alerts),
            ):
                if not alerts_for_send:
                    continue
                recipients = _escalation_recipients(config, severity_key)
                if not recipients:
                    continue
                try:
                    body = (
                        f"VMI ALERT — {severity_key.upper()}\n"
                        + "=" * 40 + "\n\n"
                        + "\n\n".join(a["text"] for a in alerts_for_send)
                        + "\n\n-- VMI Prototype"
                    )
                    subject = (
                        f"VMI Alert — {len(alerts_for_send)} "
                        f"{severity_key.replace('_', ' ').upper()}"
                    )
                    client.send_mail(_to(config, *recipients),
                                       subject, body)
                    any_send_succeeded = True
                    logger.info(
                        f"{len(alerts_for_send)} {severity_key} alert(s) "
                        f"sent to {len(recipients)} recipient(s)."
                    )
                except Exception as e:
                    logger.warning(
                        f"alert email failed ({severity_key}) — {e}"
                    )
                    any_send_failed = True
        if any_send_succeeded and not any_send_failed:
            # All sends OK: mark every current hash as sent
            data["alerted_hashes"] = list(current.keys())
        elif any_send_failed:
            # Some send failed: prune stale but DON'T add unsent hashes
            # (so the next tick retries them)
            data["alerted_hashes"] = list(prev & set(current.keys()))
        else:
            # No config / no recipient → behave like legacy "no email
            # sent": still prune stale hashes
            data["alerted_hashes"] = list(current.keys())
    else:
        # Nothing new — just prune hashes that are no longer active
        data["alerted_hashes"] = list(current.keys())

    return data


# ---------------------------------------------------------------------------
# CS load-entry email with PDF attachment
# ---------------------------------------------------------------------------

def send_cs_load_entry(data, new_trucks):
    """
    Build a PDF of new_trucks and email it to cs_email.
    Called from plan_orders.py after trucks are committed.
    """
    if not new_trucks:
        return

    config = load_config()
    cs     = config.get("cs_email", "")
    if not config or not cs:
        logger.warning("cs_email not configured — skipping CS email.")
        return

    try:
        pdf_bytes = build_load_entry_pdf(new_trucks, data)

        # Derive week-of label for subject line
        first = min(new_trucks, key=lambda t: t["arrival_run_hour"])
        dt    = time_utils.run_hour_to_dt(data, first["arrival_run_hour"])
        week_monday = (dt - timedelta(days=dt.weekday())).strftime("%Y-%m-%d")

        subject = f"Load Entry — Week of {week_monday}"
        body = (
            f"Hi,\n\n"
            f"Please find the attached load entry for the week of {week_monday}.\n\n"
            f"Total loads: {len(new_trucks)}\n\n"
            f"Thank you."
        )

        OutlookClient(config).send_mail(
            _to(config, cs),
            subject,
            body,
            attachments=[("loads.pdf", pdf_bytes)],
        )
        logger.info(f"CS load-entry email sent to {cs}.")

    except Exception as e:
        logger.warning(f"CS load-entry email failed — {e}")


# ---------------------------------------------------------------------------
# Friday reminder
# ---------------------------------------------------------------------------

def send_friday_reminder_if_needed(data, now_dt=None):
    """
    Send 'Can you share next week's schedule?' if the schedule
    for next week hasn't been marked as received yet.

    now_dt: optional datetime to use instead of datetime.now().
            Pass the sim clock datetime when calling from the Streamlit app
            so the reminder fires relative to sim time, not wall-clock time.
            Leave as None for Windows Task Scheduler (real-time) use.

    Per-week dedup: a `last_reminder_sent_for_week` key in `data` records
    the target_monday for the most recent reminder we sent. Without this,
    advancing the Streamlit sim clock from Fri 11 AM through Fri 3 PM
    would call this function twice (once per hourly trigger), and Anna
    would receive two identical emails. The dedup is week-scoped so when
    a new week's reminder is genuinely due, it fires.

    The dedup record is updated only AFTER a successful send_mail — a
    transient SMTP failure must not block the next attempt.
    """
    config  = load_config()
    contact = config.get("anna_email", "")
    if not config or not contact:
        logger.warning("anna_email not configured — skipping reminder.")
        return

    today = now_dt if now_dt is not None else datetime.now()
    # From Friday, next Monday is 3 days away.  weekday(): Mon=0 … Fri=4 … Sun=6
    days_ahead = (7 - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7          # today IS Monday — target next Monday
    target_monday = (today + timedelta(days=days_ahead)).date().isoformat()

    received = data.get("schedule_received_for_week")
    if received == target_monday:
        logger.info(f"Schedule already received for week of {target_monday} — no reminder sent.")
        return

    # Per-week dedup against repeated calls within the same Friday
    # window (Streamlit's hourly trigger loop calls this once per
    # advanced sim hour).
    last_sent = data.get("last_reminder_sent_for_week")
    if last_sent == target_monday:
        logger.info(f"Reminder already sent this week for {target_monday} — skipping duplicate.")
        return

    try:
        OutlookClient(config).send_mail(
            _to(config, contact),
            "Schedule request",
            "Hi,\n\nCan you please share next week's run schedule?\n\nThank you.",
        )
        # Record AFTER successful send so a transient SMTP failure
        # doesn't permanently block this week's reminder.
        data["last_reminder_sent_for_week"] = target_monday
        logger.info(f"Reminder sent to {contact} for week of {target_monday}.")
    except Exception as e:
        logger.warning(f"reminder email failed — {e}")
