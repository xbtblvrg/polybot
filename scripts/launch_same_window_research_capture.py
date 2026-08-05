#!/usr/bin/env python3
"""Install a non-restarting launchd job for one same-window capture."""

from __future__ import annotations

import argparse
import json
import os
import plistlib
import subprocess
import sys
from pathlib import Path
from typing import Any

import certifi

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--duration-s", type=float, default=7500.0)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--output-dir", default="data/research/same_window_capture")
    parser.add_argument("--bootstrap", action="store_true")
    return parser.parse_args()


def build_launchd_payload(*, run_id: str, duration_s: float, python: str, output_dir: str) -> dict[str, Any]:
    run_dir = ROOT / output_dir / run_id
    return {
        "Label": f"com.polymarket.same-window.{run_id}",
        "ProgramArguments": [
            python,
            str(ROOT / "scripts/run_same_window_research_capture.py"),
            "--run-id",
            run_id,
            "--duration-s",
            str(float(duration_s)),
            "--output-dir",
            output_dir,
        ],
        "WorkingDirectory": str(ROOT),
        "RunAtLoad": True,
        "KeepAlive": {"SuccessfulExit": False},
        "EnvironmentVariables": {
            "PYTHONUNBUFFERED": "1",
            "SSL_CERT_FILE": certifi.where(),
        },
        "StandardOutPath": str(run_dir / "launchd.out.log"),
        "StandardErrorPath": str(run_dir / "launchd.err.log"),
    }


def main() -> int:
    args = parse_args()
    run_id = str(args.run_id).strip()
    if not run_id or any(char not in "0123456789TZ" for char in run_id):
        raise SystemExit("run-id must be a compact UTC timestamp such as 20260719T162300Z")
    run_dir = ROOT / args.output_dir / run_id
    if run_dir.exists() and any(run_dir.iterdir()):
        raise SystemExit(f"refusing to reuse non-empty capture run-id: {run_id}")
    run_dir.mkdir(parents=True, exist_ok=True)
    plist_path = run_dir / "launchd.plist"
    payload = build_launchd_payload(
        run_id=run_id,
        duration_s=args.duration_s,
        python=str(Path(args.python).resolve()),
        output_dir=args.output_dir,
    )
    with plist_path.open("wb") as handle:
        plistlib.dump(payload, handle, sort_keys=True)
    result = {"status": "PLIST_WRITTEN", "label": payload["Label"], "plist": str(plist_path)}
    if args.bootstrap:
        domain = f"gui/{os.getuid()}"
        completed = subprocess.run(
            ["launchctl", "bootstrap", domain, str(plist_path)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        result.update(
            {
                "status": "BOOTSTRAPPED" if completed.returncode == 0 else "BOOTSTRAP_FAILED",
                "returncode": completed.returncode,
                "stderr": completed.stderr.strip(),
            }
        )
        print(json.dumps(result, sort_keys=True))
        return completed.returncode
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
