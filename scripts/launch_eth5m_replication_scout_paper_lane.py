#!/usr/bin/env python3
"""Install and start the persistent ETH-5m replication paper observer."""

from __future__ import annotations

import os
import plistlib
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LABEL = "com.belavarga.polymarket.eth5m-replication-scout-paper"


def main() -> int:
    plist_path = ROOT / "data/research/runtime_plists" / f"{LABEL}.plist"
    stdout = ROOT / "data/research/runtime_logs/eth5m_replication_scout_paper.out.log"
    stderr = ROOT / "data/research/runtime_logs/eth5m_replication_scout_paper.err.log"
    for path in (plist_path, stdout, stderr):
        path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "Label": LABEL,
        "ProgramArguments": [sys.executable, str(ROOT / "scripts/run_eth5m_replication_scout_paper_lane.py")],
        "WorkingDirectory": str(ROOT),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "StandardOutPath": str(stdout),
        "StandardErrorPath": str(stderr),
        "EnvironmentVariables": {"PATH": os.environ.get("PATH", "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin")},
    }
    plist_path.write_bytes(plistlib.dumps(payload, sort_keys=True))
    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", domain, str(plist_path)], check=False, capture_output=True)
    subprocess.run(["launchctl", "bootstrap", domain, str(plist_path)], check=True)
    subprocess.run(["launchctl", "kickstart", "-k", f"{domain}/{LABEL}"], check=True)
    print(plist_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
