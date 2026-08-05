#!/usr/bin/env python3
"""Build the fail-closed next-method supply packet for a dry live rotation."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


DEFAULT_ROUTER = "data/research/copy_source_identity_reconciliation_router_latest.json"
DEFAULT_DEADMAN = "data/research/order_flow_deadman_state.json"
DEFAULT_CROSS_EXCHANGE = (
    "data/research/btc5m_cross_exchange_probability_edge_paper_lane_state.json"
)
DEFAULT_OUTPUT = "data/research/live_method_supply_packet_latest.json"
SOURCE_WALLET = "0x32de91fa203321fa7735e7854f2b1c844e71ce9d"


def _load(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _write(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(f"{target.suffix}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(target)


def build_packet(
    *,
    router: dict[str, Any],
    deadman: dict[str, Any],
    cross_exchange: dict[str, Any],
) -> dict[str, Any]:
    current = router.get("current_cohort") if isinstance(router.get("current_cohort"), dict) else {}
    enabled = {
        str(wallet).lower() for wallet in router.get("enabled_wallets") or [] if str(wallet)
    }
    per_wallet = (
        current.get("per_wallet_counts")
        if isinstance(current.get("per_wallet_counts"), dict)
        else {}
    )
    source_rows = sum(
        int(count or 0)
        for count in (per_wallet.get(SOURCE_WALLET) or {}).values()
    )
    source_silent = bool(
        enabled == {SOURCE_WALLET}
        and source_rows == 0
        and not current.get("ambiguous_proxy_aliases")
        and int(current.get("identity_market_outcome_parity_violations") or 0) == 0
    )

    policy_choke = (
        deadman.get("policy_choke")
        if isinstance(deadman.get("policy_choke"), dict)
        else {}
    )
    actuator = (
        policy_choke.get("actuator")
        if isinstance(policy_choke.get("actuator"), dict)
        else {}
    )
    candidate_evidence = (
        actuator.get("candidate_evidence")
        if isinstance(actuator.get("candidate_evidence"), dict)
        else {}
    )
    sweep_dry = int(candidate_evidence.get("eligible_count") or 0) == 0

    gate = (
        cross_exchange.get("promotion_gate")
        if isinstance(cross_exchange.get("promotion_gate"), dict)
        else {}
    )
    checks = gate.get("checks") if isinstance(gate.get("checks"), dict) else {}
    required_checks = (
        "copyintent_parity",
        "distinct_windows_gte_10",
        "positive_post_fee_chronological_holdout",
        "positive_post_fee_train",
        "prospective_executable_book_post_fee_pnl_positive",
        "resolved_signals_gte_200",
        "single_guard_only",
    )
    all_gate_checks_pass = bool(required_checks) and all(
        checks.get(name) is True for name in required_checks
    )
    candidate = {
        "lane_id": cross_exchange.get("lane_id"),
        "experiment_id": (cross_exchange.get("frozen_model") or {}).get("experiment_id"),
        "model_checksum": (cross_exchange.get("frozen_model") or {}).get("checksum"),
        "paper_only": cross_exchange.get("paper_only") is True,
        "live_orders_allowed": cross_exchange.get("live_orders_allowed") is True,
        "promotion_checks": {name: checks.get(name) is True for name in required_checks},
        "all_gate_checks_pass": all_gate_checks_pass,
        "walk_forward_train": (cross_exchange.get("walk_forward") or {}).get("train"),
        "chronological_holdout": (cross_exchange.get("walk_forward") or {}).get(
            "chronological_holdout"
        ),
        "prospective_executable_book": cross_exchange.get("prospective_executable_book"),
    }
    activation_ready = bool(
        source_silent
        and sweep_dry
        and all_gate_checks_pass
        and candidate["paper_only"]
        and not candidate["live_orders_allowed"]
    )
    return {
        "schema_version": 1,
        "kind": "live_method_supply_packet",
        "flow_stage": "LIVE/ROTATE/PROMOTE",
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_acquisition": {
            "wallet": SOURCE_WALLET,
            "status": "SOURCE_SILENT_32DE" if source_silent else "SOURCE_EVIDENCE_PRESENT_OR_DIRTY",
            "enabled_wallets": sorted(enabled),
            "current_rows": source_rows,
            "proxy_aliases": current.get("proxy_source_aliases") or {},
            "ambiguous_aliases": current.get("ambiguous_proxy_aliases") or {},
            "identity_market_outcome_parity_violations": int(
                current.get("identity_market_outcome_parity_violations") or 0
            ),
        },
        "unchanged_bar_sweep": {
            "status": candidate_evidence.get("status"),
            "candidate_count": int(candidate_evidence.get("candidate_count") or 0),
            "eligible_count": int(candidate_evidence.get("eligible_count") or 0),
            "refusal_counts": candidate_evidence.get("refusal_counts") or {},
            "quality_bars_unchanged": actuator.get("quality_bars_unchanged") is True,
        },
        "candidate": candidate,
        "activation": {
            "status": (
                "PROMOTION_PACKET_READY_FOR_FABLE"
                if activation_ready
                else "ZERO_LIVE_PACKET_EVIDENCE_GATE_CLOSED"
            ),
            "live_mutation_allowed": False,
            "requires_fable_activation": True,
            "single_submitter": "scripts/run_wallet_copy_live_guard.py",
            "copyintent_parity_required": True,
        },
        "excluded_methods": {
            "c50d": "mechanical negative rotation",
            "a689": "proven-negative/excluded",
            "f418": "mechanically demoted",
            "927f": "unready",
            "e5": "negative live block",
            "btc5m_structural_scalp": "negative prospective paper evidence",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--router", default=DEFAULT_ROUTER)
    parser.add_argument("--deadman", default=DEFAULT_DEADMAN)
    parser.add_argument("--cross-exchange", default=DEFAULT_CROSS_EXCHANGE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    packet = build_packet(
        router=_load(args.router),
        deadman=_load(args.deadman),
        cross_exchange=_load(args.cross_exchange),
    )
    _write(args.output, packet)
    print(json.dumps(packet, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
