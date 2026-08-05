#!/usr/bin/env python3
"""Prepare the same top-10 watch candidates with their full-pool replay orders."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_CANDIDATES = "data/research/wallet_copy_top10_watch_clearance_candidates.json"
DEFAULT_FULL_POOL_REPLAY = "data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_top10_watch_clearance_replay.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", default=DEFAULT_CANDIDATES)
    parser.add_argument("--full-pool-replay", default=DEFAULT_FULL_POOL_REPLAY)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _candidate_index(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in payload.get("candidates") or []:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if wallet:
            out[wallet] = row
    return out


def prepare_replay_payload(
    *,
    candidates_payload: dict[str, Any],
    full_pool_replay_payload: dict[str, Any],
) -> dict[str, Any]:
    full_index = _candidate_index(full_pool_replay_payload)
    rows: list[dict[str, Any]] = []
    missing_wallets: list[str] = []
    for seed in candidates_payload.get("candidates") or []:
        if not isinstance(seed, dict):
            continue
        wallet = _norm_wallet(seed.get("wallet") or seed.get("source_wallet"))
        if not wallet:
            continue
        full = full_index.get(wallet)
        if not isinstance(full, dict):
            missing_wallets.append(wallet)
            rows.append(dict(seed))
            continue
        replay = full.get("paper_replay") if isinstance(full.get("paper_replay"), dict) else {}
        rows.append(
            {
                **full,
                "candidate_id": seed.get("candidate_id") or full.get("candidate_id") or "",
                "wallet": wallet,
                "source_queue_rank": seed.get("source_queue_rank"),
                "status": seed.get("status") or "TOP10_POSITIVE_REPLAY_WATCH_CLEARANCE",
                "flow_stage": "PROMOTE/LEARN",
                "paper_replay_seed": seed.get("paper_replay_seed") if isinstance(seed.get("paper_replay_seed"), dict) else {},
                "paper_replay": replay,
            }
        )
    replay_counts = [
        int(((row.get("paper_replay") if isinstance(row.get("paper_replay"), dict) else {}) or {}).get("paper_orders") or 0)
        for row in rows
    ]
    resolved_counts = [
        int(((row.get("paper_replay") if isinstance(row.get("paper_replay"), dict) else {}) or {}).get("resolved_orders") or 0)
        for row in rows
    ]
    return {
        "schema_version": 1,
        "kind": "wallet_copy_top10_watch_clearance_discover_candidates",
        "flow_stage": "PROMOTE/LEARN",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "source": {
            "candidate_seed": DEFAULT_CANDIDATES,
            "full_pool_replay": DEFAULT_FULL_POOL_REPLAY,
            "method": "same_top10_wallets_with_full_pool_stored_replay_orders",
        },
        "candidate_count": len(rows),
        "missing_full_pool_replay_wallets": missing_wallets,
        "replay_summary": {
            "flow_stage": "PROMOTE/LEARN",
            "candidate_count": len(rows),
            "complete_replays": sum(1 for count in replay_counts if count > 0),
            "total_paper_orders": sum(replay_counts),
            "candidates_with_resolved_orders": sum(1 for count in resolved_counts if count > 0),
            "total_resolved_orders": sum(resolved_counts),
        },
        "candidates": rows,
    }


def main() -> int:
    args = parse_args()
    payload = prepare_replay_payload(
        candidates_payload=load_json(args.candidates, default={}),
        full_pool_replay_payload=load_json(args.full_pool_replay, default={}),
    )
    payload["source"]["candidate_seed"] = args.candidates
    payload["source"]["full_pool_replay"] = args.full_pool_replay
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["replay_summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
