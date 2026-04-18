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
        dist   = config.get("distribution_group", "")
        if config and dist:
            try:
                body = (
                    "VMI ALERT\n"
                    + "=" * 40 + "\n\n"
                    + "\n\n".join(a["text"] for a in new_alerts)
                    + "\n\n-- VMI Prototype"
                )
                OutlookClient(config).send_mail(
                    _to(config, dist),
                    f"VMI Alert ({len(new_alerts)} new)",
                    body,
                )
                # Send succeeded: mark all current hashes as sent, prune stale
                data["alerted_hashes"] = list(current.keys())
                print(f"[email] {len(new_alerts)} alert(s) sent to {dist}.")
            except Exception as e:
                print(f"[email] WARN: alert email failed — {e}")
                # Send failed: prune stale but do NOT add unsent new hashes
                data["alerted_hashes"] = list(prev & set(current.keys()))
        else:
            # No config / no recipient — still prune stale hashes
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
        print("[email] WARN: cs_email not configured — skipping CS email.")
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
        print(f"[email] CS load-entry email sent to {cs}.")

    except Exception as e:
        print(f"[email] WARN: CS load-entry email failed — {e}")


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
    """
    config  = load_config()
    contact = config.get("anna_email", "")
    if not config or not contact:
        print("[email] WARN: anna_email not configured — skipping reminder.")
        return

    today = now_dt if now_dt is not None else datetime.now()
    # From Friday, next Monday is 3 days away.  weekday(): Mon=0 … Fri=4 … Sun=6
    days_ahead = (7 - today.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7          # today IS Monday — target next Monday
    target_monday = (today + timedelta(days=days_ahead)).date().isoformat()

    received = data.get("schedule_received_for_week")
    if received == target_monday:
        print(f"[email] Schedule already received for week of {target_monday} — no reminder sent.")
        return

    try:
        OutlookClient(config).send_mail(
            _to(config, contact),
            "Schedule request",
            "Hi,\n\nCan you please share next week's run schedule?\n\nThank you.",
        )
        print(f"[email] Reminder sent to {contact} for week of {target_monday}.")
    except Exception as e:
        print(f"[email] WARN: reminder email failed — {e}")
