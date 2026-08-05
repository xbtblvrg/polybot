#!/usr/bin/env python3
"""Install and start the fee-aware long-horizon paper collector in launchd."""

from __future__ import annotations

import argparse
import os
import plistlib
import subprocess
import sys
from pathlib import Path
from typing import Any

import certifi


ROOT = Path(__file__).resolve().parents[1]
LABEL = "com.belavarga.polymarket.fee-aware-long-horizon-copy-paper"


def build_launchd_payload(*, python: str, stdout: str, stderr: str) -> dict[str, Any]:
    return {
        "Label": LABEL,
        "ProgramArguments": [python, str(ROOT / "scripts/run_fee_aware_long_horizon_copy_paper_lane.py")],
        "WorkingDirectory": str(ROOT),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "StandardOutPath": stdout,
        "StandardErrorPath": stderr,
        "EnvironmentVariables": {
            "PATH": os.environ.get("PATH", "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"),
            "SSL_CERT_FILE": certifi.where(),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--plist", default=f"data/research/runtime_plists/{LABEL}.plist")
    parser.add_argument("--stdout", default="data/research/runtime_logs/fee_aware_long_horizon_copy_paper.out.log")
    parser.add_argument("--stderr", default="data/research/runtime_logs/fee_aware_long_horizon_copy_paper.err.log")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plist_path = ROOT / args.plist
    stdout = str(ROOT / args.stdout)
    stderr = str(ROOT / args.stderr)
    for path in (plist_path, Path(stdout), Path(stderr)):
        path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_launchd_payload(python=str(args.python), stdout=stdout, stderr=stderr)
    plist_path.write_bytes(plistlib.dumps(payload, sort_keys=True))
    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", domain, str(plist_path)], check=False, capture_output=True)
    subprocess.run(["launchctl", "bootstrap", domain, str(plist_path)], check=True)
    subprocess.run(["launchctl", "kickstart", "-k", f"{domain}/{LABEL}"], check=True)
    print(str(plist_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
