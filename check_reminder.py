"""
check_reminder.py
-----------------
Friday schedule reminder and alert logic.  Run by Windows Task Scheduler.

11:00 AM job (no flags):
    1. Try to read schedule from Anna's inbox.
    2. High confidence found → apply to data.json, mark received. Done.
    3. Not found or low confidence → send reminder to Anna.

 3:00 PM job (--alert flag):
    If schedule still not marked as received for next week → send alert to
    the distribution group ("No schedule received from Anna by 3PM Friday").

Other flags
-----------
    --force   Override the "only run on Fridays" check (for testing any day).

Windows Task Scheduler setup
-----------------------------
Job 1 — Friday 11:00 AM:
    Program : C:\\...\\venv\\Scripts\\python.exe
    Arguments: check_reminder.py --force     ← remove --force once live
    Start in : C:\\Users\\jonat\\Documents\\vmi-prototype

Job 2 — Friday 3:00 PM:
    Program : C:\\...\\venv\\Scripts\\python.exe
    Arguments: check_reminder.py --alert --force
    Start in : C:\\Users\\jonat\\Documents\\vmi-prototype
"""

import json
import sys
from datetime import datetime, timedelta

from email_client import OutlookClient, load_config
from read_schedule import fetch_and_apply_schedule

DATA_PATH = "data.json"
FORCE     = "--force" in sys.argv
ALERT_RUN = "--alert" in sys.argv   # True = 3PM job

today    = datetime.now()
is_friday = (today.weekday() == 4)

if not is_friday and not FORCE:
    print(f"Today is {today.strftime('%A')} — this script only runs on Fridays.")
    print("Use --force to override.")
    sys.exit(0)

with open(DATA_PATH) as f:
    data = json.load(f)

# Compute next Monday ISO date (same logic used by read_schedule)
days_ahead   = (7 - today.weekday()) % 7 or 7
target_monday = (today + timedelta(days=days_ahead)).date().isoformat()

# ── 3PM alert job ─────────────────────────────────────────────────────────────
if ALERT_RUN:
    received = data.get("schedule_received_for_week")
    if received == target_monday:
        print(f"[reminder] Schedule already received for week of {target_monday}. No alert needed.")
        sys.exit(0)

    config = load_config()
    dist   = config.get("distribution_group", "")
    if config and dist:
        try:
            OutlookClient(config).send_mail(
                [dist],
                "VMI: No schedule received from Anna by 3PM Friday",
                f"The run schedule for the week of {target_monday} has not been "
                f"received from Anna as of 3:00 PM today ({today.strftime('%A %Y-%m-%d')}).\n\n"
                f"Please follow up with Anna or enter the schedule manually.",
            )
            print(f"[reminder] 3PM alert sent to {dist}.")
        except Exception as e:
            print(f"[reminder] WARN: could not send 3PM alert — {e}")
    sys.exit(0)

# ── 11AM reminder job ─────────────────────────────────────────────────────────
# First try to read and apply the schedule from Anna's inbox
result = fetch_and_apply_schedule(data)

if result == "applied":
    # Schedule was found and applied — save and done
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, indent=2)
    print("[reminder] Schedule applied from Anna's email. No reminder needed.")
    sys.exit(0)

# Schedule not found or low confidence → send reminder to Anna
from email_hooks import send_friday_reminder_if_needed
send_friday_reminder_if_needed(data)
