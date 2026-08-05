#!/usr/bin/env python3
"""Refresh self-feed reconciliation overlay evidence for scorecard truth."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_BENCHMARK = "data/research/wallet_copy_self_feed_duckdb_benchmark_latest.json"
DEFAULT_RETRACE = "data/research/wallet_copy_self_feed_full_ledger_retrace_latest.json"
DEFAULT_CLASSIFICATION = "data/research/wallet_copy_recon_window_cash_ledger_latest.json"
DEFAULT_OUTPUT = "data/research/self_feed_overlay_refresh_latest.json"


RunFunc = Callable[[Sequence[str], Path], subprocess.CompletedProcess[str]]


def _rooted(root: Path, path: str | Path) -> Path:
    target = Path(path)
    return target if target.is_absolute() else root / target


def _num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _overlay_summary(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    packet = payload.get("classification_packet") if isinstance(payload.get("classification_packet"), dict) else {}
    overlay = packet.get("resolved_pnl_overlay") if isinstance(packet.get("resolved_pnl_overlay"), dict) else {}
    recommendation = packet.get("recommendation") if isinstance(packet.get("recommendation"), dict) else {}
    return {
        "generated_at": payload.get("generated_at"),
        "status": payload.get("status"),
        "overlay_delta_usd": _num(overlay.get("overlay_delta_usd")),
        "raw_missing_pnl_upper_bound_usd": _num(overlay.get("raw_missing_pnl_upper_bound_usd") or overlay.get("pnl_usd")),
        "double_count_excluded_usd": _num(overlay.get("double_count_excluded_usd")),
        "reconciled_actual_estimate_usd": _num(overlay.get("reconciled_actual_estimate_usd")),
        "recommendation_mode": recommendation.get("mode"),
        "ledger_rewrite": recommendation.get("ledger_rewrite"),
    }


def _residual_summary(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    equation = payload.get("reconciliation_equation") if isinstance(payload.get("reconciliation_equation"), dict) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return {
        "generated_at": payload.get("generated_at"),
        "status": equation.get("status") or payload.get("status"),
        "actual_delta_usd": _num(equation.get("actual_delta_usd")),
        "account_value_residual_usd": _num(equation.get("account_value_residual_usd")),
        "unexplained_usd": _num(equation.get("unexplained_usd")),
        "candidate_total": summary.get("candidate_total"),
        "class_counts": summary.get("class_counts"),
    }


def _run_command(cmd: Sequence[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(cmd), cwd=cwd, text=True, capture_output=True, check=False)


def _command_record(proc: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    return {
        "cmd": list(proc.args) if isinstance(proc.args, (list, tuple)) else [str(proc.args)],
        "returncode": proc.returncode,
        "stdout_tail": (proc.stdout or "")[-2000:],
        "stderr_tail": (proc.stderr or "")[-2000:],
    }


def build_report(args: argparse.Namespace, *, root: Path = ROOT, run_func: RunFunc = _run_command) -> dict[str, Any]:
    benchmark_path = _rooted(root, args.benchmark)
    retrace_path = _rooted(root, args.retrace)
    classification_path = _rooted(root, args.classification)
    output_path = _rooted(root, args.output)

    before_benchmark = load_json(benchmark_path, {})
    before_retrace = load_json(retrace_path, {})
    commands: list[dict[str, Any]] = []
    commands_to_run: tuple[Sequence[str], ...] = (
        (sys.executable, "scripts/reconcile_self_wallet_feed.py"),
        (
            sys.executable,
            "scripts/build_wallet_copy_data_layer.py",
            "--input-glob",
            "data/research/wallet_copy_self_trades.jsonl",
            "--max-files",
            "1",
            "--force",
        ),
        (sys.executable, "scripts/classify_self_feed_ledger_gaps.py"),
        (sys.executable, "scripts/retrace_self_feed_full_ledger.py"),
        (sys.executable, "scripts/benchmark_self_feed_duckdb_scan.py"),
    )
    for cmd in commands_to_run:
        proc = run_func(cmd, root)
        commands.append(_command_record(proc))

    after_benchmark = load_json(benchmark_path, {})
    after_retrace = load_json(retrace_path, {})
    classification = load_json(classification_path, {})

    after_status = after_benchmark.get("status") if isinstance(after_benchmark, dict) else None
    benchmark_written = isinstance(after_benchmark, dict) and bool(after_benchmark)
    if not benchmark_written:
        status = "ERROR_NO_BENCHMARK_ARTIFACT"
    elif after_status == "PASS":
        status = "PASS"
    else:
        status = "REFRESHED_WITH_MISMATCH"

    before_overlay = _overlay_summary(before_benchmark)
    after_overlay = _overlay_summary(after_benchmark)
    before_residual = _residual_summary(before_retrace)
    after_residual = _residual_summary(after_retrace)
    report = {
        "schema_version": 1,
        "generated_at": utc_now_iso(),
        "status": status,
        "paper_only": True,
        "live_path_mutated": False,
        "benchmark": args.benchmark,
        "classification": args.classification,
        "full_retrace": args.retrace,
        "before_overlay": before_overlay,
        "after_overlay": after_overlay,
        "before_residual": before_residual,
        "after_residual": after_residual,
        "before_after_delta": {
            "overlay_delta_usd": _num(
                (after_overlay.get("overlay_delta_usd") or 0.0) - (before_overlay.get("overlay_delta_usd") or 0.0)
            ),
            "account_value_residual_usd": _num(
                (after_residual.get("account_value_residual_usd") or 0.0)
                - (before_residual.get("account_value_residual_usd") or 0.0)
            ),
        },
        "classification_summary": (
            classification.get("summary") if isinstance(classification.get("summary"), dict) else {}
        )
        if isinstance(classification, dict)
        else {},
        "commands": commands,
        "mismatch_is_reportable": status == "REFRESHED_WITH_MISMATCH",
        "next_action": (
            "use refreshed overlay evidence only when benchmark status is PASS; investigate parity mismatch before "
            "scorecard overlay consumption"
            if status == "REFRESHED_WITH_MISMATCH"
            else "daily self-feed overlay evidence is current"
        ),
    }
    atomic_write_json(output_path, report)
    return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", default=DEFAULT_BENCHMARK)
    parser.add_argument("--retrace", default=DEFAULT_RETRACE)
    parser.add_argument("--classification", default=DEFAULT_CLASSIFICATION)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    report = build_report(parse_args(argv))
    print(
        {
            "status": report["status"],
            "before_overlay_delta_usd": report["before_overlay"].get("overlay_delta_usd"),
            "after_overlay_delta_usd": report["after_overlay"].get("overlay_delta_usd"),
            "after_residual_usd": report["after_residual"].get("account_value_residual_usd"),
        }
    )
    return 1 if report["status"] == "ERROR_NO_BENCHMARK_ARTIFACT" else 0


if __name__ == "__main__":
    raise SystemExit(main())
