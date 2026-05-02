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

All scripts that mutate state should write through `save_data(data)`
rather than calling `json.dump` directly.
"""

import json
import os
import tempfile

DATA_PATH    = "data.json"
DEFAULTS_PATH = "defaults.json"


def load_data(path=DATA_PATH, fallback=DEFAULTS_PATH):
    """Load JSON state. Falls back to defaults if `path` is absent."""
    src = path if os.path.exists(path) else fallback
    with open(src, encoding="utf-8") as f:
        return json.load(f)


def save_data(data, path=DATA_PATH):
    """
    Atomically write `data` (a dict) as JSON to `path`.

    Writes to a tempfile in the same directory, fsyncs, then os.replace's
    onto the target. Either the new content lands intact or the previous
    file is unchanged — never half-written. This is the only safe way to
    persist state when a script may be interrupted by SIGINT / OS sleep /
    power loss / scheduled-task timeout.
    """
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
