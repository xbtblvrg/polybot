#!/usr/bin/env python3
"""Onboard operator-provided wallets into BTC-5m wallet-copy research.

The command is paper/research only. It registers relevant wallets, then builds a
repeatable command plan for history ingest, paper replay, cross-wallet
analysis, ML dataset export, profit admission, and paper live-tracking.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.onboarding import (  # noqa: E402
    WalletOnboardingConfig,
    build_onboarding_plan,
    default_operator_wallet_artifact_paths,
    parse_or_resolve_wallet_candidate,
    register_operator_wallets,
    scoped_operator_wallet_registry_payload,
)
from src.wallet_copy.store import atomic_write_json  # noqa: E402


ACCEPTED_NON_GREEN_EVIDENCE_RETURNCODES = {
    "paper_live_tracker": {
        2: "paper_live_tracker_copy_efficiency_or_copyability_watch_evidence",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", action="append", required=True, help="0x address, name=0x address, or profile URL")
    parser.add_argument("--registry", default="configs/wallet_copy/wallets.json")
    parser.add_argument("--scoped-registry", default=None)
    parser.add_argument("--history-state", default=None)
    parser.add_argument("--wallet-event-log", default=None)
    parser.add_argument("--paper-state", default=None)
    parser.add_argument("--paper-event-log", default=None)
    parser.add_argument("--research-state", default=None)
    parser.add_argument("--inventory-paper-state", default=None)
    parser.add_argument("--inventory-paper-event-log", default=None)
    parser.add_argument("--ml-dataset", default=None)
    parser.add_argument("--profit-state", default=None)
    parser.add_argument("--live-tracker-state", default=None)
    parser.add_argument("--live-tracker-event-log", default=None)
    parser.add_argument("--live-tracker-paper-state", default=None)
    parser.add_argument("--live-tracker-paper-event-log", default=None)
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--output", default="data/research/operator_wallet_onboarding_state.json")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--wallet-fraction", type=float, default=0.05)
    parser.add_argument("--max-order-usd", type=float, default=2.0)
    parser.add_argument("--slippage-bps", type=float, default=250.0)
    parser.add_argument("--max-unresolved-ratio", type=float, default=0.5)
    parser.add_argument("--data-api-timeout-s", type=float, default=2.0)
    parser.add_argument("--profile-resolve-timeout-s", type=float, default=8.0)
    parser.add_argument("--live-tracker-iterations", type=int, default=1)
    parser.add_argument("--live-tracker-poll-interval-s", type=float, default=1.0)
    parser.add_argument("--live-tracker-max-runtime-s", type=float, default=0.0)
    parser.add_argument("--tag", action="append", default=[])
    parser.add_argument("--notes", default="")
    parser.add_argument("--skip-live-tracker", action="store_true")
    parser.add_argument("--plan-only", action="store_true", help="write registry and plan, but do not run the pipeline commands")
    return parser.parse_args()


def _run_command(argv: tuple[str, ...]) -> dict[str, Any]:
    result = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True, check=False)
    return {
        "argv": list(argv),
        "returncode": result.returncode,
        "stdout_tail": result.stdout[-4000:],
        "stderr_tail": result.stderr[-4000:],
    }


def _accepted_non_green_evidence_reason(command_name: str, returncode: int | None) -> str | None:
    if returncode is None:
        return None
    return ACCEPTED_NON_GREEN_EVIDENCE_RETURNCODES.get(command_name, {}).get(int(returncode))


def _command_result(command: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    returncode = int(result.get("returncode") or 0)
    evidence_reason = _accepted_non_green_evidence_reason(str(command["name"]), returncode)
    return {
        "name": command["name"],
        "purpose": command["purpose"],
        "ok": returncode == 0 or evidence_reason is not None,
        "accepted_non_green_evidence": evidence_reason is not None,
        "accepted_non_green_reason": evidence_reason,
        "evidence_status": "WATCH" if evidence_reason else ("PASS" if returncode == 0 else "FAIL"),
        **result,
    }


def _structural_failures(command_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in command_results
        if int(row.get("returncode") or 0) != 0 and not bool(row.get("accepted_non_green_evidence"))
    ]


def _classify_status(*, plan_only: bool, command_results: list[dict[str, Any]]) -> str:
    if plan_only:
        return "PLAN_WRITTEN"
    if not command_results:
        return "BLOCKED"
    if _structural_failures(command_results):
        return "BLOCKED"
    if any(bool(row.get("accepted_non_green_evidence")) for row in command_results):
        return "WATCH"
    return "PASS" if all(int(row.get("returncode") or 0) == 0 for row in command_results) else "BLOCKED"


def main() -> int:
    args = parse_args()
    candidates = [
        parse_or_resolve_wallet_candidate(
            value,
            notes=args.notes,
            tags=tuple(str(tag) for tag in args.tag if tag),
            profile_resolve_timeout_s=args.profile_resolve_timeout_s,
        )
        for value in args.wallet
    ]
    scoped_paths = default_operator_wallet_artifact_paths(candidates)
    registry_payload = register_operator_wallets(candidates, registry_path=args.registry)
    config = WalletOnboardingConfig(
        registry_path=args.registry,
        scoped_registry_path=args.scoped_registry or scoped_paths["scoped_registry_path"],
        history_state=args.history_state or scoped_paths["history_state"],
        wallet_event_log=args.wallet_event_log or scoped_paths["wallet_event_log"],
        paper_state=args.paper_state or scoped_paths["paper_state"],
        paper_event_log=args.paper_event_log or scoped_paths["paper_event_log"],
        research_state=args.research_state or scoped_paths["research_state"],
        inventory_paper_state=args.inventory_paper_state or scoped_paths["inventory_paper_state"],
        inventory_paper_event_log=args.inventory_paper_event_log or scoped_paths["inventory_paper_event_log"],
        ml_dataset=args.ml_dataset or scoped_paths["ml_dataset"],
        profit_state=args.profit_state or scoped_paths["profit_state"],
        live_tracker_state=args.live_tracker_state or scoped_paths["live_tracker_state"],
        live_tracker_event_log=args.live_tracker_event_log or scoped_paths["live_tracker_event_log"],
        live_tracker_paper_state=args.live_tracker_paper_state or scoped_paths["live_tracker_paper_state"],
        live_tracker_paper_event_log=args.live_tracker_paper_event_log or scoped_paths["live_tracker_paper_event_log"],
        resolutions=args.resolutions,
        pages=args.pages,
        limit=args.limit,
        wallet_fraction=args.wallet_fraction,
        max_order_usd=args.max_order_usd,
        slippage_bps=args.slippage_bps,
        max_unresolved_ratio=args.max_unresolved_ratio,
        data_api_timeout_s=args.data_api_timeout_s,
        live_tracker_iterations=args.live_tracker_iterations,
        live_tracker_poll_interval_s=args.live_tracker_poll_interval_s,
        live_tracker_max_runtime_s=args.live_tracker_max_runtime_s,
        run_live_tracker=not args.skip_live_tracker,
    )
    scoped_registry = scoped_operator_wallet_registry_payload(candidates)
    atomic_write_json(config.scoped_registry_path, scoped_registry)
    plan = build_onboarding_plan(candidates, config=config)
    command_results: list[dict[str, Any]] = []
    if not args.plan_only:
        for command in plan["commands"]:
            result = _run_command(tuple(str(item) for item in command["argv"]))
            command_results.append(_command_result(command, result))
            if _structural_failures([command_results[-1]]):
                break
    status = _classify_status(plan_only=bool(args.plan_only), command_results=command_results)
    accepted_non_green = [
        {
            "name": row["name"],
            "returncode": row["returncode"],
            "reason": row.get("accepted_non_green_reason"),
        }
        for row in command_results
        if bool(row.get("accepted_non_green_evidence"))
    ]
    structural_failures = [
        {"name": row["name"], "returncode": row["returncode"], "evidence_status": row.get("evidence_status")}
        for row in _structural_failures(command_results)
    ]
    payload = {
        **plan,
        "registry": {
            "path": args.registry,
            "wallets": len(registry_payload.get("wallets") or []) if isinstance(registry_payload, dict) else None,
        },
        "scoped_registry": {
            "path": config.scoped_registry_path,
            "wallets": len(scoped_registry.get("wallets") or []),
        },
        "plan_only": bool(args.plan_only),
        "command_results": command_results,
        "accepted_non_green_evidence_commands": accepted_non_green,
        "structural_failures": structural_failures,
        "status": status,
    }
    atomic_write_json(args.output, payload)
    print(
        json.dumps(
            {
                "output": args.output,
                "status": payload["status"],
                "wallets": payload["candidate_wallets"],
                "commands": [
                    {
                        "name": row["name"],
                        "returncode": row["returncode"],
                        "evidence_status": row.get("evidence_status"),
                    }
                    for row in command_results
                ],
                "accepted_non_green_evidence_commands": accepted_non_green,
                "structural_failures": structural_failures,
                "plan_only": args.plan_only,
                "paper_only": True,
                "live_orders_allowed": False,
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )
    return 0 if payload["status"] in {"PASS", "PLAN_WRITTEN", "WATCH"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
