#!/usr/bin/env python3
"""Install the active-member OrderFilled hot-source shadow as a launchd service."""

from __future__ import annotations

import os
import plistlib
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LABEL = "com.belavarga.polymarket.active-member-orderfilled-hot-source-shadow"


def main() -> int:
    plist = ROOT / f"data/research/runtime_plists/{LABEL}.plist"
    plist.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "Label": LABEL,
        "ProgramArguments": [
            sys.executable,
            str(ROOT / "scripts/run_active_member_orderfilled_hot_source_shadow.py"),
            "--no-forward-book-capture",
        ],
        "WorkingDirectory": str(ROOT),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "StandardOutPath": str(ROOT / "data/research/runtime_logs/active_member_orderfilled_hot_source.out.log"),
        "StandardErrorPath": str(ROOT / "data/research/runtime_logs/active_member_orderfilled_hot_source.err.log"),
        "EnvironmentVariables": {
            "PATH": os.environ.get("PATH", "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin")
        },
    }
    plist.write_bytes(plistlib.dumps(payload, sort_keys=True))
    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", domain, str(plist)], check=False, capture_output=True)
    subprocess.run(["launchctl", "bootstrap", domain, str(plist)], check=True)
    subprocess.run(["launchctl", "kickstart", "-k", f"{domain}/{LABEL}"], check=True)
    print(plist)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
