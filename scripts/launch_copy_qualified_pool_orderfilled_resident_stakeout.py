#!/usr/bin/env python3
"""Install the pre-F2 qualified-pool Polygon stakeout as a paper-only service."""

from __future__ import annotations

import os
import plistlib
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LABEL = "com.belavarga.polymarket.copy-qualified-pool-orderfilled-stakeout"
BOOK_LABEL = "com.belavarga.polymarket.early-01a-decision-book-capture"


def main() -> int:
    plist = ROOT / f"data/research/runtime_plists/{LABEL}.plist"
    plist.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "Label": LABEL,
        "ProgramArguments": [
            sys.executable,
            str(ROOT / "scripts/run_active_member_orderfilled_hot_source_shadow.py"),
            "--qualified-pool-only",
            "--state",
            "data/research/copy_qualified_pool_orderfilled_resident_stakeout_state.json",
            "--accumulator-state",
            "data/research/copy_qualified_pool_orderfilled_resident_stakeout_accumulator.json",
            "--max-accumulator-events",
            "20000",
            "--sleep-s",
            "5",
            "--no-forward-book-capture",
        ],
        "WorkingDirectory": str(ROOT),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "StandardOutPath": str(
            ROOT / "data/research/runtime_logs/copy_qualified_pool_orderfilled_stakeout.out.log"
        ),
        "StandardErrorPath": str(
            ROOT / "data/research/runtime_logs/copy_qualified_pool_orderfilled_stakeout.err.log"
        ),
        "EnvironmentVariables": {
            "PATH": os.environ.get("PATH", "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin")
        },
    }
    plist.write_bytes(plistlib.dumps(payload, sort_keys=True))
    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", domain, str(plist)], check=False, capture_output=True)
    subprocess.run(["launchctl", "bootstrap", domain, str(plist)], check=True)
    subprocess.run(["launchctl", "kickstart", "-k", f"{domain}/{LABEL}"], check=True)
    book_plist = ROOT / f"data/research/runtime_plists/{BOOK_LABEL}.plist"
    book_payload = {
        **payload,
        "Label": BOOK_LABEL,
        "ProgramArguments": [
            sys.executable,
            str(ROOT / "scripts/run_active_member_orderfilled_hot_source_shadow.py"),
            "--qualified-pool-only",
            "--forward-book-only",
            "--state",
            "data/research/early_01a_decision_time_book_observer_state.json",
            "--accumulator-state",
            "data/research/early_01a_decision_time_book_observer_accumulator.json",
            "--max-accumulator-events",
            "20000",
            "--sleep-s",
            "0.25",
        ],
        "StandardOutPath": str(ROOT / "data/research/runtime_logs/early_01a_decision_book_capture.out.log"),
        "StandardErrorPath": str(ROOT / "data/research/runtime_logs/early_01a_decision_book_capture.err.log"),
    }
    book_plist.write_bytes(plistlib.dumps(book_payload, sort_keys=True))
    subprocess.run(["launchctl", "bootout", domain, str(book_plist)], check=False, capture_output=True)
    subprocess.run(["launchctl", "bootstrap", domain, str(book_plist)], check=True)
    subprocess.run(["launchctl", "kickstart", "-k", f"{domain}/{BOOK_LABEL}"], check=True)
    print(plist)
    print(book_plist)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
