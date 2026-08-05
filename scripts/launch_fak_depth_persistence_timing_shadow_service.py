#!/usr/bin/env python3
"""Install the persistent paper-only FAK depth-persistence timing shadow."""

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
LABEL = "com.belavarga.polymarket.fak-depth-persistence-timing-shadow"


def build_launchd_payload(*, python: str, stdout: str, stderr: str) -> dict[str, Any]:
    return {"Label": LABEL, "ProgramArguments": [python, str(ROOT / "scripts/run_fak_depth_persistence_timing_shadow_service.py")],
            "WorkingDirectory": str(ROOT), "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 10,
            "StandardOutPath": stdout, "StandardErrorPath": stderr,
            "EnvironmentVariables": {"PATH": os.environ.get("PATH", "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"),
                                     "SSL_CERT_FILE": certifi.where()}}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--plist", default=f"data/research/runtime_plists/{LABEL}.plist")
    parser.add_argument("--stdout", default="data/research/runtime_logs/fak_depth_persistence_timing_shadow.out.log")
    parser.add_argument("--stderr", default="data/research/runtime_logs/fak_depth_persistence_timing_shadow.err.log")
    args = parser.parse_args()
    plist, stdout, stderr = ROOT / args.plist, str(ROOT / args.stdout), str(ROOT / args.stderr)
    for path in (plist, Path(stdout), Path(stderr)):
        path.parent.mkdir(parents=True, exist_ok=True)
    plist.write_bytes(plistlib.dumps(build_launchd_payload(python=str(args.python), stdout=stdout, stderr=stderr), sort_keys=True))
    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", domain, str(plist)], check=False, capture_output=True)
    subprocess.run(["launchctl", "bootstrap", domain, str(plist)], check=True)
    subprocess.run(["launchctl", "kickstart", "-k", f"{domain}/{LABEL}"], check=True)
    print(plist)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
