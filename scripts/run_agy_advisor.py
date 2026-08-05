#!/usr/bin/env python3
"""Run AGY in advisory modes with mode headers and secret pre-send checks."""

from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable


DEFAULT_LOG_DIR = Path("data/research/ask_fable_provider_logs")
SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"(?i)\b(?:CLOB_)?API[_-]?(?:KEY|SECRET|PASSPHRASE)\s*[:=]\s*['\"]?[A-Za-z0-9_./+=-]{12,}"),
    re.compile(r"(?i)\b(?:PRIVATE[_-]?KEY|POLYMARKET_[A-Z0-9_]*(?:KEY|SECRET|TOKEN))\s*[:=]\s*['\"]?[A-Za-z0-9_./+=-]{12,}"),
    re.compile(r"(?i)\b(?:password|secret|token)\s*[:=]\s*['\"]?[A-Za-z0-9_./+=-]{24,}"),
)

MODE_CONFIG = {
    "outsider-auditor": {
        "title": "OUTSIDER AUDITOR",
        "budget": "72h framework-audit cadence",
        "brief": "Read goal-first, then rulebook second. Find rules or habits that slow daily profitable BTC-5m trading.",
    },
    "prosecutor": {
        "title": "PROSECUTOR",
        "budget": "per promotion packet",
        "brief": "Adversarially falsify the promotion packet before capital. Findings are advisory; Fable rules.",
    },
    "archive-miner": {
        "title": "ARCHIVE MINER",
        "budget": "monthly or on-demand",
        "brief": "Mine long HANDOFF/archive/ledger spans for forgotten lessons, ruling drift, and post-analysis patterns.",
    },
    "env-sweeper": {
        "title": "ENV SWEEPER",
        "budget": "weekly environment-watch sweep",
        "brief": "Sweep external Polymarket/UMA/fee/API/ToS changes and report material changes with sources.",
    },
    "tiebreak": {
        "title": "TIEBREAK ADVISOR",
        "budget": "on Fable-codex factual disagreement",
        "brief": "Give a third independent evidence read. Advisory only; evidence still decides and Fable rules.",
    },
}


class SecretScanError(RuntimeError):
    pass


def utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def scan_text(label: str, text: str) -> None:
    for pattern in SECRET_PATTERNS:
        match = pattern.search(text)
        if match:
            raise SecretScanError(f"secret-like content in {label}: pattern={pattern.pattern!r}")


def scan_paths(paths: Iterable[Path]) -> None:
    for path in paths:
        if path.name == ".env" or ".env" in path.parts:
            raise SecretScanError(f"refusing to send env-like path: {path}")
        if not path.exists():
            raise FileNotFoundError(path)
        if path.is_dir():
            raise IsADirectoryError(path)
        with path.open("r", encoding="utf-8", errors="ignore") as fh:
            for line_no, line in enumerate(fh, start=1):
                try:
                    scan_text(f"{path}:{line_no}", line)
                except SecretScanError as exc:
                    raise SecretScanError(str(exc)) from exc


def build_prompt(mode: str, task: str, artifacts: list[Path]) -> str:
    config = MODE_CONFIG[mode]
    artifact_lines = "\n".join(f"- {path}" for path in artifacts) if artifacts else "- none"
    return "\n".join(
        [
            f"MODE: ADVISOR/{mode}",
            "AUTHORITY: advisory only; Fable rules; no live/config mutation authority.",
            "NO_SECRETS: do not request, print, or infer .env content, private keys, API keys, or credentials.",
            f"ROLE: {config['title']}",
            f"BUDGET: {config['budget']}",
            f"BRIEF: {config['brief']}",
            "",
            "TASK:",
            task.strip(),
            "",
            "ARTIFACTS TO READ (after the pre-send scan):",
            artifact_lines,
            "",
            "OUTPUT: concise findings, evidence, and recommended next action. Declare that findings are advisory.",
        ]
    )


def run_agy(prompt: str, *, agy_bin: str, print_timeout: str, log_dir: Path, mode: str) -> int:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{utc_stamp()}_agy_ADVISOR_{mode}_rcpending.log"
    proc = subprocess.run(
        [
            agy_bin,
            "--add-dir",
            str(Path.cwd()),
            "--dangerously-skip-permissions",
            "--print-timeout",
            print_timeout,
            "-p",
            prompt,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    final_path = log_dir / log_path.name.replace("rcpending", f"rc{proc.returncode}")
    final_path.write_text((proc.stdout or "") + (proc.stderr or ""), encoding="utf-8")
    print(str(final_path))
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)
    return proc.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=sorted(MODE_CONFIG))
    parser.add_argument("--task", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Path AGY may read after secret scan")
    parser.add_argument("--agy-bin", default="agy")
    parser.add_argument("--print-timeout", default="10m0s")
    parser.add_argument("--log-dir", default=str(DEFAULT_LOG_DIR))
    parser.add_argument("--dry-run", action="store_true", help="Build and scan prompt, then print it without calling agy")
    args = parser.parse_args()

    artifacts = [Path(value) for value in args.artifact]
    task = str(args.task)
    scan_text("task", task)
    scan_paths(artifacts)
    prompt = build_prompt(args.mode, task, artifacts)
    scan_text("prompt", prompt)

    if args.dry_run:
        print(prompt)
        return 0
    return run_agy(
        prompt,
        agy_bin=str(args.agy_bin),
        print_timeout=str(args.print_timeout),
        log_dir=Path(args.log_dir),
        mode=str(args.mode),
    )


if __name__ == "__main__":
    raise SystemExit(main())
