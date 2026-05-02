"""
data_io.py
----------
Shared helpers for reading and writing data.json (the prototype's only
state file). Centralizes:

  - Atomic writes via temp-file + os.replace, so a crash / SIGINT / OS
    power loss mid-write cannot leave the file truncated and unparseable.
  - UTF-8 explicit encoding on both read and write, so Windows cp1252
    locale doesn't silently mojibake non-ASCII content.
  - Defaults fallback (defaults.json) when data.json is absent — fresh
    clones / reset environments work out of the box.
  - Schema migration: every load runs incremental migrators so older
    data files are upgraded to CURRENT_SCHEMA_VERSION before any
    algorithm sees them. New schema versions add a function to the
    _MIGRATIONS table; call sites stay unchanged.

All scripts that mutate state should write through `save_data(data)`
rather than calling `json.dump` directly.
"""

import json
import os
import tempfile
from typing import Callable, Dict

DATA_PATH    = "data.json"
DEFAULTS_PATH = "defaults.json"

# ── Schema versioning ─────────────────────────────────────────────────────────
#
# Bump CURRENT_SCHEMA_VERSION whenever the on-disk shape of data.json
# changes in a way that older code can't safely read. Add the migrator
# from the previous version to _MIGRATIONS. The migrators run in order
# from whatever version is on disk up to CURRENT_SCHEMA_VERSION.
#
# Files written before this field was introduced have no `schema_version`
# key; they are treated as version 0 and run through every migrator.
#
# Migrators MUST mutate the dict in place and only perform structural
# changes — no side effects (no logging, no file writes, no email).

CURRENT_SCHEMA_VERSION = 1


def _migrate_v0_to_v1(data: dict) -> None:
    """Pre-versioned files → v1. No structural change; this version
    bump exists so future migrators have a known baseline to migrate
    from."""
    # Intentionally empty.
    return


_MIGRATIONS: Dict[int, Callable[[dict], None]] = {
    0: _migrate_v0_to_v1,
}


def _migrate(data: dict) -> dict:
    """Run any pending schema migrations on `data` in place. Returns the
    same dict for chainability. Idempotent: a file already at
    CURRENT_SCHEMA_VERSION is returned unchanged."""
    version = data.get("schema_version", 0)
    while version < CURRENT_SCHEMA_VERSION:
        migrator = _MIGRATIONS.get(version)
        if migrator is None:
            raise RuntimeError(
                f"data_io: no migrator registered for schema_version "
                f"{version} → {version + 1}. Update _MIGRATIONS."
            )
        migrator(data)
        version += 1
        data["schema_version"] = version
    return data


def load_data(path=DATA_PATH, fallback=DEFAULTS_PATH):
    """Load JSON state, run schema migrations, return the upgraded dict.
    Falls back to `fallback` if `path` is absent."""
    src = path if os.path.exists(path) else fallback
    with open(src, encoding="utf-8") as f:
        data = json.load(f)
    return _migrate(data)


def save_data(data, path=DATA_PATH):
    """
    Atomically write `data` (a dict) as JSON to `path`.

    Stamps schema_version=CURRENT_SCHEMA_VERSION if missing, so files
    written through this helper are always self-describing. Existing
    schema_version values are preserved (caller controls the bump path
    via the migrator chain).

    Writes to a tempfile in the same directory, fsyncs, then os.replace's
    onto the target. Either the new content lands intact or the previous
    file is unchanged — never half-written. This is the only safe way to
    persist state when a script may be interrupted by SIGINT / OS sleep /
    power loss / scheduled-task timeout.
    """
    data.setdefault("schema_version", CURRENT_SCHEMA_VERSION)
    target_dir = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(
        prefix=".tmp_", suffix=".json", dir=target_dir
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                # fsync isn't supported on every filesystem (some Windows
                # network drives); the os.replace below is still atomic
                # in the page-cache sense.
                pass
        os.replace(tmp, path)
    except Exception:
        # Clean up the tempfile if anything went wrong before replace.
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


# ── Typed-state convenience wrappers ──────────────────────────────────────────
#
# These return / accept the PlantState dataclass from state.py instead of
# raw dicts. New code should prefer these. Existing dict-based code paths
# continue to work via load_data / save_data.

def load_state(path=DATA_PATH, fallback=DEFAULTS_PATH):
    """Load JSON state and parse into a PlantState dataclass."""
    from state import PlantState
    return PlantState.from_dict(load_data(path, fallback))


def save_state(state, path=DATA_PATH):
    """Atomically write a PlantState dataclass back to JSON."""
    save_data(state.to_dict(), path)
