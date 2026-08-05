#!/usr/bin/env python3
"""Batch source-active liveness reports for cohort live-ready wallets."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_cohort_alive_admission_packets import _norm_wallet  # noqa: E402
from scripts.report_source_active_windows import (  # noqa: E402
    DEFAULT_RTDS_JSONL,
    _event_outcome,
    _event_price,
    _event_received_ts,
    _event_side,
    _event_size,
    _event_ts,
    _event_tx,
    _event_wallet,
    _iso_from_ts,
    _jsonl_rows,
    _window_start_from_slug,
    _window_summary,
)
from src.wallet_copy.models import num, parse_ts, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_COHORT = ROOT / "data/research/wallet_market_cohort_replay_latest.json"
DEFAULT_OUTPUT_DIR = ROOT / "data/research"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort-replay", default=str(DEFAULT_COHORT))
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS_JSONL)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--latest-name", default="source_active_liveness_batch_latest.json")
    parser.add_argument(
        "--ordering-latest-name",
        default="source_active_liveness_replay_ordering_latest.json",
    )
    parser.add_argument("--batch-id", default="")
    parser.add_argument("--wallet-offset", type=int, default=0)
    parser.add_argument("--wallet-limit", type=int, default=50)
    parser.add_argument("--since", default="")
    parser.add_argument("--tail-bytes", type=int, default=384_000_000)
    parser.add_argument("--min-offset-s", type=float, default=0.0)
    parser.add_argument("--max-offset-s", type=float, default=300.0)
    parser.add_argument("--max-price", type=float, default=0.50)
    parser.add_argument("--required-windows", type=int, default=1)
    return parser.parse_args()


def _cohort_wallets(payload: dict[str, Any]) -> list[str]:
    rows = payload.get("live_ready_picks") if isinstance(payload.get("live_ready_picks"), list) else []
    wallets: list[str] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if wallet and wallet not in seen:
            seen.add(wallet)
            wallets.append(wallet)
    return wallets


def _report_for_wallet(
    *,
    wallet: str,
    rows: list[dict[str, Any]],
    rtds_jsonl: str | Path,
    file_size: int,
    tail_bytes: int,
    since_ts: float,
    min_offset_s: float,
    max_offset_s: float,
    max_price: float | None,
    required_windows: int,
    generated_at: str,
) -> dict[str, Any]:
    band_by_window: dict[str, list[dict[str, Any]]] = defaultdict(list)
    all_rows: list[dict[str, Any]] = []
    for row in rows:
        slug = str(row.get("market_slug") or "")
        window_start_s = _window_start_from_slug(slug)
        if window_start_s is None:
            continue
        event_ts = _event_ts(row)
        if event_ts < since_ts:
            continue
        if _event_side(row) != "BUY":
            continue
        offset_s = event_ts - window_start_s
        price = _event_price(row)
        received_ts = _event_received_ts(row) or event_ts
        item = {
            "event_iso": _iso_from_ts(event_ts),
            "event_ts": round(event_ts, 6),
            "market_slug": slug,
            "offset_s": round(offset_s, 6),
            "outcome": _event_outcome(row),
            "price": price,
            "received_iso": _iso_from_ts(received_ts),
            "received_ts": round(received_ts, 6),
            "size": _event_size(row),
            "transaction_hash": _event_tx(row),
            "window_start_s": window_start_s,
        }
        all_rows.append(item)
        if min_offset_s <= offset_s <= max_offset_s:
            band_by_window[slug].append(item)
    band_rows = [item for window_rows in band_by_window.values() for item in window_rows]
    policy_rows = [
        item
        for item in band_rows
        if max_price is None or num(item.get("price"), default=999.0) <= float(max_price)
    ]
    policy_windows = {str(item.get("market_slug") or "") for item in policy_rows}
    source_active_windows = len(band_by_window)
    policy_eligible_windows = len(policy_windows)
    return {
        "kind": "source_active_windows_report_v1",
        "flow_stage": "LIVE/PROMOTE",
        "generated_at": generated_at,
        "source_wallet": wallet,
        "rtds_jsonl": str(rtds_jsonl),
        "file_size_bytes": file_size,
        "scan_tail_bytes": tail_bytes,
        "since_ts": since_ts,
        "since_iso": _iso_from_ts(since_ts),
        "band": {
            "min_offset_s": min_offset_s,
            "max_offset_s": max_offset_s,
            "max_price": max_price,
        },
        "summary": {
            "btc5m_buy_rows_since": len(all_rows),
            "btc5m_buy_windows_since": len({str(item.get("market_slug") or "") for item in all_rows}),
            "source_active_rows": len(band_rows),
            "source_active_windows": source_active_windows,
            "policy_eligible_rows": len(policy_rows),
            "policy_eligible_windows": policy_eligible_windows,
            "required_source_active_windows": required_windows,
            "source_active_tally_status": "PASS" if source_active_windows >= required_windows else "PENDING",
            "policy_eligible_tally_status": "PASS" if policy_eligible_windows >= required_windows else "PENDING",
        },
        "windows": [
            _window_summary(slug, window_rows, max_price)
            for slug, window_rows in sorted(
                band_by_window.items(), key=lambda item: num(item[1][0].get("window_start_s"))
            )
        ],
        "latest_rows": all_rows[-20:],
    }


def build_batch(args: argparse.Namespace) -> dict[str, Any]:
    generated_at = utc_now_iso()
    batch_id = str(args.batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%MZ"))
    cohort_payload = load_json(args.cohort_replay, default={})
    all_wallets = _cohort_wallets(cohort_payload)
    start = max(0, int(args.wallet_offset))
    limit = max(0, int(args.wallet_limit))
    wallets = all_wallets[start : start + limit] if limit else all_wallets[start:]
    selected = set(wallets)
    now = datetime.now(timezone.utc)
    since_ts = parse_ts(args.since) if args.since else (now - timedelta(hours=24)).timestamp()
    if since_ts is None:
        raise SystemExit(f"invalid --since timestamp: {args.since!r}")
    rows_by_wallet: dict[str, list[dict[str, Any]]] = {wallet: [] for wallet in wallets}
    rtds_path = Path(args.rtds_jsonl)
    file_size = rtds_path.stat().st_size if rtds_path.exists() else 0
    for row in _jsonl_rows(rtds_path, tail_bytes=int(args.tail_bytes)):
        if str(row.get("event") or "") != "rtds_trade_event":
            continue
        wallet = _event_wallet(row)
        if wallet in selected:
            rows_by_wallet[wallet].append(row)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ordering_name = Path(str(args.ordering_latest_name or "")).name
    if not ordering_name:
        raise SystemExit("--ordering-latest-name must name a file")
    ordering_path = output_dir / ordering_name
    ordering_sha256 = hashlib.sha256("\n".join(all_wallets).encode("utf-8")).hexdigest()
    atomic_write_json(
        ordering_path,
        {
            "schema_version": 1,
            "kind": "source_active_liveness_replay_ordering_snapshot",
            "flow_stage": "DISCOVER/LEARN/PROMOTE/SELF-DEV",
            "generated_at": generated_at,
            "source_replay": str(args.cohort_replay),
            "source_replay_generated_at": cohort_payload.get("generated_at") if isinstance(cohort_payload, dict) else None,
            "wallet_count": len(all_wallets),
            "wallet_order_sha256": ordering_sha256,
            "live_ready_picks": [{"wallet": wallet} for wallet in all_wallets],
            "rule": "immutable ordering input for identity-rebased liveness pass; commit with batch/cohort evidence",
        },
    )
    reports = []
    for wallet in wallets:
        report = _report_for_wallet(
            wallet=wallet,
            rows=rows_by_wallet.get(wallet, []),
            rtds_jsonl=args.rtds_jsonl,
            file_size=file_size,
            tail_bytes=int(args.tail_bytes),
            since_ts=float(since_ts),
            min_offset_s=float(args.min_offset_s),
            max_offset_s=float(args.max_offset_s),
            max_price=args.max_price,
            required_windows=int(args.required_windows),
            generated_at=generated_at,
        )
        output = output_dir / f"source_active_liveness_{wallet[2:12]}_{batch_id}.json"
        atomic_write_json(output, report)
        reports.append(
            {
                "wallet": wallet,
                "output": str(output),
                **report["summary"],
            }
        )
    summary = {
        "schema_version": 1,
        "kind": "source_active_liveness_batch",
        "flow_stage": "PROMOTE/LEARN",
        "generated_at": generated_at,
        "batch_id": batch_id,
        "wallet_offset": start,
        "wallet_limit": limit,
        "wallet_count": len(wallets),
        "cohort_live_ready_wallets": len(all_wallets),
        "source_active_pass": sum(1 for row in reports if row.get("source_active_tally_status") == "PASS"),
        "policy_eligible_pass": sum(1 for row in reports if row.get("policy_eligible_tally_status") == "PASS"),
        "total_source_active_windows": sum(int(row.get("source_active_windows") or 0) for row in reports),
        "total_policy_eligible_windows": sum(int(row.get("policy_eligible_windows") or 0) for row in reports),
        "tail_bytes": int(args.tail_bytes),
        "since_ts": float(since_ts),
        "since_iso": _iso_from_ts(float(since_ts)),
        "replay_ordering_snapshot": str(ordering_path),
        "replay_ordering_wallet_count": len(all_wallets),
        "replay_ordering_sha256": ordering_sha256,
        "reports": reports,
    }
    summary_path = output_dir / f"source_active_liveness_batch_{batch_id}_offset{start:03d}_limit{len(wallets):03d}.json"
    atomic_write_json(summary_path, summary)
    latest_name = Path(str(args.latest_name or "")).name
    if not latest_name:
        raise SystemExit("--latest-name must name a file")
    atomic_write_json(output_dir / latest_name, summary)
    return summary


def main() -> int:
    summary = build_batch(parse_args())
    print(json.dumps({k: summary[k] for k in ("batch_id", "wallet_offset", "wallet_count", "source_active_pass", "policy_eligible_pass")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
