#!/usr/bin/env python3
"""Install and start the isolated fd05 climb-backup paper loop."""

from __future__ import annotations

import os
import plistlib
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LABEL = "com.belavarga.polymarket.climb-backup-fd05-paper"


def main() -> int:
    launch_agents = Path.home() / "Library" / "LaunchAgents"
    launch_agents.mkdir(parents=True, exist_ok=True)
    plist_path = launch_agents / f"{LABEL}.plist"
    payload = {
        "Label": LABEL,
        "ProgramArguments": [
            sys.executable,
            str(ROOT / "scripts" / "run_climb_backup_fd05_paper_loop.py"),
            "--interval-s",
            "90",
        ],
        "WorkingDirectory": str(ROOT),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "StandardOutPath": str(
            ROOT / "data" / "research" / "climb_backup_fd05_paper.launchd.out"
        ),
        "StandardErrorPath": str(
            ROOT / "data" / "research" / "climb_backup_fd05_paper.launchd.err"
        ),
    }
    with plist_path.open("wb") as handle:
        plistlib.dump(payload, handle)
    domain = f"gui/{os.getuid()}"
    subprocess.run(
        ["launchctl", "bootout", domain, str(plist_path)],
        check=False,
        capture_output=True,
    )
    subprocess.run(["launchctl", "bootstrap", domain, str(plist_path)], check=True)
    subprocess.run(
        ["launchctl", "kickstart", "-k", f"{domain}/{LABEL}"],
        check=True,
    )
    print(plist_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
