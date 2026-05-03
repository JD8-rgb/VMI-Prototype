"""
triage_parser_misses.py — engineering triage for production parser misses.

Walks parser_misses_log.jsonl one entry at a time. For each:

  [P] PROMOTE  — the parser SHOULD have parsed this. Engineer copies a
                  must_pass case template into test_schedule_parser.py
                  and the regex/LLM-rescue corpus grows.
  [D] DISCARD  — the input is genuinely ambiguous or non-schedule
                  (image-only email, table embedded in PDF, etc.).
                  Drop without action.
  [S] SKIP     — leave for later; entry stays in the log.
  [Q] QUIT     — stop triaging.

Once the engineer is done, `--clear` can wipe the log:

    python triage_parser_misses.py --clear

Usage:
    python triage_parser_misses.py            # interactive triage
    python triage_parser_misses.py --list     # non-interactive listing
    python triage_parser_misses.py --count    # just print the count
    python triage_parser_misses.py --clear    # wipe the log
    python triage_parser_misses.py --path X   # use a different log file
"""

from __future__ import annotations

import argparse
import sys
from typing import List

from parser_misses import PARSER_MISSES_LOG_PATH, read_all, clear


_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _format_entry(idx: int, total: int, entry: dict) -> str:
    """Pretty one-screen render of a single miss entry."""
    lines = []
    lines.append("=" * 70)
    lines.append(f"  [{idx + 1} / {total}]   logged at {entry.get('logged_at', '?')}")
    lines.append("=" * 70)
    lines.append(f"  Customer:   {entry.get('customer_id') or '(default)'}")
    lines.append(f"  Email id:   {entry.get('email_id') or '(none)'}")
    lines.append(f"  From:       {entry.get('sender', '?')}")
    lines.append(f"  Subject:    {entry.get('subject', '(no subject)')}")
    lines.append(f"  Confidence: {entry.get('confidence', '?')}")
    lines.append("")
    lines.append("  Email body:")
    lines.append("  " + "-" * 60)
    body = (entry.get("body") or "").rstrip()
    for body_line in body.splitlines() or ["(empty)"]:
        lines.append(f"  | {body_line}")
    lines.append("  " + "-" * 60)
    lines.append("")
    lines.append("  Parser's best-guess entries:")
    entries = entry.get("entries") or []
    if not entries:
        lines.append("    (none — parser extracted zero windows)")
    else:
        for e in entries:
            wd, s, end = int(e[0]), int(e[1]), int(e[2])
            day = _DAYS[wd] if 0 <= wd < 7 else f"?{wd}"
            lines.append(f"    {day} {s:02d}:00 → {end:02d}:00 "
                          f"({end - s}h)")
    lines.append("")
    notes = entry.get("notes") or []
    if notes:
        lines.append("  Parser notes:")
        for n in notes:
            for nl in str(n).rstrip().splitlines():
                lines.append(f"    - {nl}")
    lines.append("")
    return "\n".join(lines)


def _interactive_triage(entries: List[dict]) -> None:
    """Walk every entry, prompting the engineer per item."""
    if not entries:
        print("No parser misses logged. Nothing to triage.")
        return
    decisions = {"promote": 0, "discard": 0, "skip": 0}
    print(f"\n{len(entries)} parser miss(es) to triage. "
          f"Use [P]romote / [D]iscard / [S]kip / [Q]uit.\n")
    for idx, entry in enumerate(entries):
        print(_format_entry(idx, len(entries), entry))
        while True:
            choice = input("  [P/D/S/Q] > ").strip().lower()
            if choice in ("p", "promote"):
                decisions["promote"] += 1
                print(
                    "  → Marked PROMOTE. Add a must_pass case to "
                    "test_schedule_parser.py:\n"
                    f"    Case(\"must_NN_PRODUCTION_<short_label>\", "
                    f"\"<category>\",\n"
                    f"         {entry.get('body', '')[:80]!r},\n"
                    f"         expected=[...],   # fix to expected output\n"
                    f"         expected_confidence=\"<low|high>\",\n"
                    f"         must_pass=True),\n"
                )
                break
            elif choice in ("d", "discard"):
                decisions["discard"] += 1
                print("  → Discarded.\n")
                break
            elif choice in ("s", "skip"):
                decisions["skip"] += 1
                print("  → Skipped (stays in log).\n")
                break
            elif choice in ("q", "quit"):
                print(f"\nQuit. Triage so far: {decisions}")
                return
            else:
                print("  Please enter P, D, S, or Q.")
    print(f"\nTriage complete: {decisions}")
    print("Note: this CLI does NOT modify parser_misses_log.jsonl. To wipe "
          "the log after triage, run `python triage_parser_misses.py --clear`.")


def _list_entries(entries: List[dict]) -> None:
    """Non-interactive listing."""
    if not entries:
        print("No parser misses logged.")
        return
    for idx, entry in enumerate(entries):
        print(_format_entry(idx, len(entries), entry))


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="triage_parser_misses.py",
                                  description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--list",  action="store_true",
                    help="Print every entry without prompting.")
    p.add_argument("--count", action="store_true",
                    help="Just print the entry count and exit.")
    p.add_argument("--clear", action="store_true",
                    help="Wipe the log file. Use after triage is complete.")
    p.add_argument("--path",  default=None,
                    help=f"Override log file path (default: {PARSER_MISSES_LOG_PATH})")
    args = p.parse_args(argv)

    entries = read_all(log_path=args.path)

    if args.count:
        print(len(entries))
        return 0
    if args.clear:
        if not entries:
            print("Log is already empty.")
            return 0
        confirm = input(f"Clear {len(entries)} entry(ies) from "
                          f"{args.path or PARSER_MISSES_LOG_PATH}? [y/N] ")
        if confirm.strip().lower() in ("y", "yes"):
            clear(log_path=args.path)
            print("Cleared.")
            return 0
        print("Cancelled.")
        return 0
    if args.list:
        _list_entries(entries)
        return 0
    _interactive_triage(entries)
    return 0


if __name__ == "__main__":
    sys.exit(main())
