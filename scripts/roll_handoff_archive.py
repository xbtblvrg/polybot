#!/usr/bin/env python3
"""Roll old HANDOFF entries into the monthly append-only archive.

Flow stage: SELF-DEV. This is the daily-scorecard housekeeping step ordered by
Fable: entries older than two full UTC days move from docs/agents/HANDOFF.md to
the current month's HANDOFF_ARCHIVE file. Recorded operator decisions remain
canonical in AUTONOMOUS_FLOW.md.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_text  # noqa: E402


DEFAULT_HANDOFF = ROOT / "docs/agents/HANDOFF.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--handoff", default=str(DEFAULT_HANDOFF))
    parser.add_argument("--archive", default="", help="Archive path; default is docs/agents/HANDOFF_ARCHIVE_<YYYY-MM>.md")
    parser.add_argument("--today", default="", help="UTC date YYYY-MM-DD for deterministic tests; default is today.")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _today(value: str) -> date:
    if value:
        return date.fromisoformat(value)
    return datetime.now(tz=UTC).date()


def _entry_date(heading: str) -> date | None:
    match = re.search(r"\b(\d{4}-\d{2}-\d{2})", heading)
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def _entries(text: str) -> tuple[str, list[dict[str, Any]]]:
    matches = list(re.finditer(r"^## .+$", text, flags=re.MULTILINE))
    if not matches:
        return text, []
    preamble = text[: matches[0].start()]
    entries = []
    for idx, match in enumerate(matches):
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        chunk = text[match.start() : end].strip()
        heading = match.group(0).strip()
        entries.append({"heading": heading, "date": _entry_date(heading), "text": chunk})
    return preamble.rstrip() + "\n\n", entries


def _default_archive_path(handoff: Path, today: date) -> Path:
    return handoff.with_name(f"HANDOFF_ARCHIVE_{today:%Y-%m}.md")


def roll_handoff(
    *,
    handoff_path: Path,
    archive_path: Path | None,
    today: date,
    dry_run: bool = False,
) -> dict[str, Any]:
    text = handoff_path.read_text(encoding="utf-8", errors="replace") if handoff_path.exists() else ""
    preamble, entries = _entries(text)
    cutoff = today - timedelta(days=2)
    archive_path = archive_path or _default_archive_path(handoff_path, today)
    move = [entry for entry in entries if entry["date"] is not None and entry["date"] <= cutoff]
    keep = [entry for entry in entries if entry not in move]
    summary = {
        "handoff": str(handoff_path),
        "archive": str(archive_path),
        "today_utc": today.isoformat(),
        "cutoff_date_inclusive": cutoff.isoformat(),
        "entries_seen": len(entries),
        "entries_moved": len(move),
        "entries_kept": len(keep),
        "dry_run": bool(dry_run),
    }
    if dry_run or not move:
        return summary

    archive_header = f"# HANDOFF archive ({today:%Y-%m})\n\n"
    archive_existing = archive_path.read_text(encoding="utf-8", errors="replace") if archive_path.exists() else archive_header
    archive_text = archive_existing.rstrip() + "\n\n" + "\n\n".join(entry["text"] for entry in move).rstrip() + "\n"
    live_text = preamble.rstrip() + "\n\n" + "\n\n".join(entry["text"] for entry in keep).rstrip() + "\n"
    atomic_write_text(archive_path, archive_text)
    atomic_write_text(handoff_path, live_text)
    return summary


def main() -> int:
    args = parse_args()
    handoff = Path(args.handoff)
    if not handoff.is_absolute():
        handoff = ROOT / handoff
    archive = Path(args.archive) if args.archive else None
    if archive is not None and not archive.is_absolute():
        archive = ROOT / archive
    summary = roll_handoff(handoff_path=handoff, archive_path=archive, today=_today(args.today), dry_run=bool(args.dry_run))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
