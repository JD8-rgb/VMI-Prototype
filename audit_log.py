"""
audit_log.py — operator-action audit trail.

Every operator action (toggle VMI on/off, apply target override,
reset, dismiss low-confidence parse, confirm parse, advance the
clock, etc.) gets recorded as a row in data["audit_log"]:

    {
        "iso":     ISO timestamp of the action,
        "action":  short action key (e.g. "vmi_toggle_off",
                                          "target_override_apply"),
        "user":    operator identifier (default "operator" — the
                   prototype is single-user, but the field is here so
                   the technical team can wire it through RBAC),
        "details": dict of action-specific payload (slider values,
                   dismissed-parse email_id, etc.)
    }

Compliance value: this is the source of truth for "who changed what
when, and why" reviews. Pairs with alert_log (system events) to give
a complete audit story.

Bounded retention: last 1000 entries. Entries older than that fall
off the front. For real production scale, the technical team will
move this to PostgreSQL where the bound goes away (see
MIGRATION_GUIDE.md § 3).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional


AUDIT_LOG_MAX_ENTRIES = 1000


def _coerce_serializable(value: Any) -> Any:
    """Best-effort coerce a value to something JSON can dump.

    The audit_log is persisted via data_io.save_data → json.dump. A
    non-serializable details payload (a function, a custom class, a
    dataclass instance) would raise TypeError at save time, breaking
    the entire data.json write. Defensive: stringify anything we
    can't recognize so audit recording never silently kills the save
    loop.

    Recognized as-is: None, bool, int, float, str.
    Recognized recursively: list, tuple, dict (keys stringified).
    Everything else: repr() and stringify, with a marker so reviewers
    know the value was coerced.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_coerce_serializable(v) for v in value]
    if isinstance(value, dict):
        # Keys stringified — JSON requires string keys
        return {str(k): _coerce_serializable(v) for k, v in value.items()}
    # Fallback: repr it. Marker prefix so audit reviewers can see the
    # value wasn't a primitive.
    return f"<unserializable:{type(value).__name__}:{value!r}>"


def record(
    data: Dict[str, Any],
    action: str,
    *,
    user: str = "operator",
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """Append one audit entry to data["audit_log"], truncating to
    AUDIT_LOG_MAX_ENTRIES from the front.

    Idempotent on data without the field — initializes to []. Failures
    are non-fatal (no I/O happens here; the underlying save is the
    Streamlit save path).

    Defensive: details is recursively coerced via _coerce_serializable
    so a non-JSON-friendly value (function, dataclass instance, etc.)
    doesn't break data_io.save_data downstream. Coerced values are
    repr'd and prefixed `<unserializable:...>` so auditors can see
    something happened.
    """
    log = data.get("audit_log")
    if not isinstance(log, list):
        log = []
    safe_details = (
        _coerce_serializable(dict(details)) if details else {}
    )
    entry = {
        "iso":     datetime.now().isoformat(),
        "action":  str(action),
        "user":    str(user),
        "details": safe_details,
    }
    log.append(entry)
    if len(log) > AUDIT_LOG_MAX_ENTRIES:
        log = log[-AUDIT_LOG_MAX_ENTRIES:]
    data["audit_log"] = log


def recent(data: Dict[str, Any], n: int = 50) -> list:
    """Return the last `n` audit entries, newest last (reverse-chrono
    is left to the caller via .reverse() or .[::-1]).
    """
    log = data.get("audit_log") or []
    if not isinstance(log, list):
        return []
    return list(log[-n:])


# ── Convenience action keys (string constants) ──────────────────────────────
# Centralizing these prevents typos across recording sites and gives
# the technical team a single grep target when wiring RBAC.

A_VMI_TOGGLE         = "vmi_toggle"
A_TARGET_APPLY       = "target_override_apply"
A_TARGET_RESET       = "target_override_reset"
A_RESET              = "reset"
A_ADVANCE            = "advance_clock"
A_QUICK_FILL         = "quick_fill_history"
A_LC_PARSE_CONFIRM   = "lc_parse_confirm"
A_LC_PARSE_DISMISS   = "lc_parse_dismiss"
A_PLAN               = "plan_orders"
A_TRUCK_COMMIT       = "truck_commit"
A_SCHEDULE_APPLY     = "schedule_apply"
