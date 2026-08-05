#!/usr/bin/env python3
"""Continuously capture books and refresh the paper-only f418 spread shadow."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    while True:
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/capture_clob_book_snapshots.py"),
                "--output",
                "data/research/f418_spread_elasticity_books.jsonl",
                "--polygon-jsonl",
                "data/research/polygon_orderfilled_ws_shadow_resident.jsonl",
                "--polygon-source",
                "polygon_ws",
                "--polygon-max-age-s",
                "900",
                "--max-assets",
                "20",
                "--duration-s",
                "30",
                "--interval-s",
                "1",
                "--asset-refresh-s",
                "5",
                "--timeout-s",
                "1",
                "--clob-retries",
                "0",
            ],
            cwd=ROOT,
            check=False,
        )
        subprocess.run(
            [sys.executable, str(ROOT / "scripts/report_f418_spread_elasticity_shadow.py")],
            cwd=ROOT,
            check=False,
        )


if __name__ == "__main__":
    raise SystemExit(main())
