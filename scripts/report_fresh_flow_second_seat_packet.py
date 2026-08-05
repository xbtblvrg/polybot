#!/usr/bin/env python3
"""Build the Fable-facing fresh-flow second-seat admission packet.

The packet is evidence-only: it never edits guard config or live state. Fable's
latest hard filter is clearance-ready plus fresh remote BTC-5m flow with
policy-compatible in-band BUY rows; paper PnL and reject ratios are tie-breakers.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_QUEUE = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_PROBE = "data/research/queue_remote_dataapi_fresh_flow_probe_latest.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_fresh_flow_second_seat_packet_latest.json"
DEFAULT_EXCLUDED_WALLETS = (
    "0x927f7694b5d215d366d13e3c602546e2359ed215",
    "0x11c058db73b3c3c5322da3caf5e94c41486e34b0",
    "0xa727aa7c18821d191023561b6e410949215d91b5",
)
DEFAULT_FALLBACK_WALLET = "0x19729634ac5ffcd658f0847b9e8cf7c026a95821"
DEFAULT_FALLBACK_DEADLINE = "2026-07-10T21:00:00Z"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", default=DEFAULT_QUEUE)
    parser.add_argument("--probe", default=DEFAULT_PROBE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--top-limit", type=int, default=4)
    parser.add_argument("--audit-limit", type=int, default=12)
    parser.add_argument("--exclude-wallet", action="append", default=list(DEFAULT_EXCLUDED_WALLETS))
    parser.add_argument("--fallback-wallet", default=DEFAULT_FALLBACK_WALLET)
    parser.add_argument("--fallback-deadline", default=DEFAULT_FALLBACK_DEADLINE)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _metric(row: dict[str, Any], key: str, default: Any = None) -> Any:
    clearance = row.get("clearance") if isinstance(row.get("clearance"), dict) else {}
    replay = row.get("replay") if isinstance(row.get("replay"), dict) else {}
    if key in clearance:
        return clearance.get(key)
    if key in replay:
        return replay.get(key)
    return row.get(key, default)


def _fresh(row: dict[str, Any]) -> dict[str, Any]:
    return row.get("fresh_flow_rank") if isinstance(row.get("fresh_flow_rank"), dict) else {}


def _packet_row(row: dict[str, Any], *, excluded_wallets: set[str]) -> dict[str, Any]:
    wallet = _norm_wallet(row.get("wallet"))
    fresh = _fresh(row)
    remote_buys = int(fresh.get("remote_dataapi_btc5m_buys_24h") or 0)
    remote_policy = int(fresh.get("remote_dataapi_policy_compatible_inband_buy_rows_24h") or 0)
    excluded = wallet in excluded_wallets
    clearance_ready = bool(row.get("clearance_ready"))
    hard_pass = clearance_ready and not excluded and remote_buys > 0 and remote_policy > 0
    return {
        "queue_rank": row.get("queue_rank"),
        "wallet": wallet,
        "candidate_id": row.get("name") or _metric(row, "candidate_id", ""),
        "clearance_ready": clearance_ready,
        "excluded": excluded,
        "hard_pass": hard_pass,
        "hard_filter": {
            "clearance_ready": clearance_ready,
            "not_excluded": not excluded,
            "remote_dataapi_btc5m_buys_24h_gt_0": remote_buys > 0,
            "remote_dataapi_policy_compatible_inband_buy_rows_24h_gt_0": remote_policy > 0,
        },
        "paper_pnl_usd": num(_metric(row, "paper_pnl_usd"), None),
        "resolved_orders": int(_metric(row, "resolved_orders", 0) or 0),
        "copyable_buy_events": int(_metric(row, "copyable_buy_events", 0) or 0),
        "candidate_clob_backed_orders": int(_metric(row, "candidate_clob_backed_orders", 0) or 0),
        "raw_reject_ratio": _metric(row, "raw_reject_ratio"),
        "attributable_reject_ratio": _metric(row, "attributable_reject_ratio"),
        "unresolved_filled_order_count": int(_metric(row, "unresolved_filled_order_count", 0) or 0),
        "fresh_flow_rank_source": fresh.get("rank_source"),
        "remote_dataapi_btc5m_buys_24h": remote_buys,
        "remote_dataapi_policy_compatible_inband_buy_rows_24h": remote_policy,
        "remote_dataapi_latest_trade_age_h": fresh.get("remote_dataapi_latest_trade_age_h"),
        "remote_rows_saturated": bool(fresh.get("remote_rows_saturated")),
        "policy_compatible_fresh_buy_rows_le_30s": int(fresh.get("policy_compatible_fresh_buy_rows_le_30s") or 0),
        "p1_reject_reasons": fresh.get("p1_reject_reasons") or [],
        "next_action": row.get("next_action") or "",
    }


def build_packet(
    *,
    queue: dict[str, Any],
    probe: dict[str, Any],
    excluded_wallets: set[str],
    fallback_wallet: str,
    fallback_deadline: str,
    top_limit: int,
    audit_limit: int,
) -> dict[str, Any]:
    ranked_rows = [row for row in queue.get("ranked_members") or [] if isinstance(row, dict)]
    packet_rows = [_packet_row(row, excluded_wallets=excluded_wallets) for row in ranked_rows]
    hard_passers = [row for row in packet_rows if row.get("hard_pass")]
    top_hard_passers = hard_passers[: max(1, int(top_limit))]
    fallback = next((row for row in packet_rows if row.get("wallet") == fallback_wallet), None)
    recommendation = (
        "FABLE_ROTATION_DECISION_READY_HARD_PASSERS_PRESENT"
        if hard_passers
        else f"NO_ADMISSION_EXECUTE_{fallback_wallet[-4:]}_HALF_SIZE_FALLBACK_AT_{fallback_deadline}_IF_NO_HARD_PASS_BEFORE_THEN"
    )
    return {
        "schema_version": 1,
        "kind": "wallet_copy_fresh_flow_second_seat_packet",
        "flow_stage": "ROTATE/LEARN",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "source_direction": "2026-07-10T00:25Z fable fresh-flow-first second-seat rescan",
        "hard_filter": (
            "clearance_ready and remote_dataapi_btc5m_buys_24h>0 and "
            "remote_dataapi_policy_compatible_inband_buy_rows_24h>0 and wallet not excluded"
        ),
        "excluded_wallets": sorted(excluded_wallets),
        "queue": {
            "path": str(DEFAULT_QUEUE),
            "generated_at": queue.get("generated_at"),
            "summary": queue.get("summary") if isinstance(queue.get("summary"), dict) else {},
        },
        "probe": {
            "path": str(DEFAULT_PROBE),
            "generated_at": probe.get("generated_at"),
            "summary": probe.get("summary") if isinstance(probe.get("summary"), dict) else {},
        },
        "summary": {
            "ranked_rows_scanned": len(packet_rows),
            "hard_pass_count": len(hard_passers),
            "clearance_ready_with_remote_policy_fresh": sum(
                1
                for row in packet_rows
                if row.get("clearance_ready")
                and int(row.get("remote_dataapi_policy_compatible_inband_buy_rows_24h") or 0) > 0
            ),
            "clearance_ready_with_policy_compatible_fresh": sum(
                1
                for row in packet_rows
                if row.get("clearance_ready")
                and int(row.get("policy_compatible_fresh_buy_rows_le_30s") or 0) > 0
            ),
            "fallback_wallet": fallback_wallet,
            "fallback_deadline": fallback_deadline,
            "recommendation": recommendation,
        },
        "top_hard_passers": top_hard_passers,
        "fallback": fallback,
        "top_ranked_rows_for_audit": packet_rows[: max(1, int(audit_limit))],
    }


def main() -> int:
    args = parse_args()
    queue = load_json(args.queue, default={})
    probe = load_json(args.probe, default={})
    excluded_wallets = {_norm_wallet(item) for item in args.exclude_wallet or []}
    excluded_wallets.discard("")
    packet = build_packet(
        queue=queue if isinstance(queue, dict) else {},
        probe=probe if isinstance(probe, dict) else {},
        excluded_wallets=excluded_wallets,
        fallback_wallet=_norm_wallet(args.fallback_wallet),
        fallback_deadline=str(args.fallback_deadline),
        top_limit=int(args.top_limit),
        audit_limit=int(args.audit_limit),
    )
    atomic_write_json(args.output, packet)
    print(json.dumps(packet["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
