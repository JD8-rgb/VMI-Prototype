"""
email_client.py
---------------
Send and read Outlook emails via SMTP and IMAP.
Uses only Python standard library (smtplib, imaplib, email).
Credentials are stored in email_config.json.
"""

import email as _email
import imaplib
import json
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

CONFIG_PATH = "email_config.json"


def load_config():
    """Load email_config.json and return as dict. Returns {} with a warning if missing."""
    try:
        with open(CONFIG_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[email] WARN: {CONFIG_PATH} not found — email features disabled.")
        return {}


class OutlookClient:
    def __init__(self, config=None):
        if config is None:
            config = load_config()
        self.config       = config
        self.address      = config.get("email_address", "")
        self.password     = config.get("app_password", "")
        self.smtp_server  = config.get("smtp_server", "smtp-mail.outlook.com")
        self.smtp_port    = config.get("smtp_port", 587)
        self.imap_server  = config.get("imap_server", "outlook.office365.com")
        self.imap_port    = config.get("imap_port", 993)

    # ------------------------------------------------------------------
    # Send
    # ------------------------------------------------------------------

    def send_mail(self, to, subject, body, attachments=None):
        """
        Send an email via SMTP.

        Parameters
        ----------
        to          : str or list[str]  -- recipient address(es)
        subject     : str
        body        : str               -- plain-text body
        attachments : list of (filename, bytes) tuples, optional
                      e.g. [("loads.pdf", pdf_bytes)]
        """
        if not self.address:
            print("[email] WARN: no email_address configured — skipping send.")
            return

        if isinstance(to, str):
            to = [to]

        msg = MIMEMultipart()
        msg["From"]    = self.address
        msg["To"]      = ", ".join(to)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        if attachments:
            for filename, data in attachments:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(data)
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f'attachment; filename="{filename}"',
                )
                msg.attach(part)

        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(self.address, self.password)
            server.sendmail(self.address, to, msg.as_string())

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_inbox(self, top=25):
        """
        Return the most recent `top` messages from the inbox, newest first.
        Each item is a dict: id, subject, sender, received, body.
        """
        return self._fetch_messages(search_criteria="ALL", top=top)

    def search_inbox(self, sender=None, subject_contains=None, top=10):
        """
        Search the inbox by sender address and/or subject keyword.

        Parameters
        ----------
        sender           : str, optional -- filter by sender address (partial, case-insensitive)
        subject_contains : str, optional -- filter by subject keyword (case-insensitive)
        top              : int           -- max results to return (newest first)
        """
        # Build the most specific IMAP search we can to reduce data fetched
        if sender:
            criteria = f'FROM "{sender}"'
        elif subject_contains:
            criteria = f'SUBJECT "{subject_contains}"'
        else:
            criteria = "ALL"

        messages = self._fetch_messages(search_criteria=criteria, top=top * 3)

        # Apply Python-side filtering for combined / partial-match cases
        results = []
        for msg in messages:
            if len(results) >= top:
                break
            if sender and sender.lower() not in msg["sender"].lower():
                continue
            if subject_contains and subject_contains.lower() not in msg["subject"].lower():
                continue
            results.append(msg)

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_messages(self, search_criteria="ALL", top=25):
        """Connect via IMAP, search, and return list of message dicts (newest first)."""
        results = []
        try:
            with imaplib.IMAP4_SSL(self.imap_server, self.imap_port) as mail:
                mail.login(self.address, self.password)
                mail.select("INBOX")

                _, data = mail.search(None, search_criteria)
                ids = data[0].split()

                # Take the last `top` message IDs (highest = most recent in IMAP)
                ids = ids[-top:]
                ids.reverse()  # newest first

                for msg_id in ids:
                    _, raw_data = mail.fetch(msg_id, "(RFC822)")
                    if not raw_data or raw_data[0] is None:
                        continue
                    raw = raw_data[0][1]
                    msg = _email.message_from_bytes(raw)

                    body_text = _extract_body(msg)

                    results.append({
                        "id":       msg_id.decode(),
                        "subject":  msg.get("Subject", ""),
                        "sender":   msg.get("From", ""),
                        "received": msg.get("Date", ""),
                        "body":     body_text,
                    })
        except Exception as e:
            print(f"[email] WARN: IMAP read failed — {e}")

        return results


def _extract_body(msg):
    """Pull the plain-text body out of an email.message.Message object."""
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain":
                try:
                    return part.get_payload(decode=True).decode("utf-8", errors="replace")
                except Exception:
                    return ""
        return ""
    else:
        try:
            return msg.get_payload(decode=True).decode("utf-8", errors="replace")
        except Exception:
            return ""
