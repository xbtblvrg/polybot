#!/usr/bin/env python3
"""Recover exact-policy paper fills from contemporaneous historical CLOB captures.

Flow stage: PROMOTE/LEARN. This is paper-only. Source trades are candidates,
never fills: a row is emitted only when the historical capture contains a
full CLOB ask book observed within the exact policy's five-second lag fence
and that book can fill the $1 paper order inside the unchanged slippage cap.
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.reconcile_wide_exact_policy_paper import wide_policy_identity
from src.wallet_copy.live_tracker import CLOBMarketClient
from src.wallet_copy.models import stable_id, utc_now_iso
from src.wallet_copy.store import atomic_write_json


WALLET = "0x3048d65321be3497164cdfc2996f94f98a2e7537"
FINGERPRINT = "2a1a8285a611541e3d1eaa7168730ce89f8d05a825abd65017ee1eac6dd4a191"
MOVE_SLICE_KEYS = (
    "000-060|0.25-0.50",
    "180-240|0.25-0.50",
    "180-240|0.50-0.75",
    "unknown_seconds|>0.75",
)
DEFAULT_REPORT_GLOB = "data/research/alpha_decay_report_wide_*.json"
DEFAULT_OUTPUT = (
    "data/research/exact_wallet_source_history_acquisition_3048_2a1a.json"
)


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _paths(value: Any) -> list[Path]:
    raw = value if isinstance(value, list) else [value]
    result: list[Path] = []
    for item in raw:
        text = str(item or "").strip()
        if not text:
            continue
        path = Path(text)
        result.append(path if path.is_absolute() else ROOT / path)
    return result


def _snapshot_key(asset_id: str, captured_at_s: float) -> tuple[str, int]:
    return str(asset_id), round(float(captured_at_s) * 1_000_000)


def _candidate_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    alpha = report.get("alpha_decay") if isinstance(report.get("alpha_decay"), dict) else {}
    rows: list[dict[str, Any]] = []
    for row in alpha.get("sample_rows") or []:
        if not isinstance(row, dict):
            continue
        horizon = (
            (row.get("horizons") or {}).get("2s")
            if isinstance(row.get("horizons"), dict)
            else {}
        )
        if (
            str(row.get("wallet") or "").lower() != WALLET
            or str(row.get("side") or "").upper() != "BUY"
            or str(row.get("move_slice_key") or "") not in MOVE_SLICE_KEYS
            or not isinstance(horizon, dict)
        ):
            continue
        observed_at_s = float(horizon.get("observed_ts") or 0.0)
        event_at_s = float(row.get("block_ts") or 0.0)
        if event_at_s <= 0 or observed_at_s < event_at_s:
            continue
        if observed_at_s - event_at_s > 5.0:
            continue
        rows.append({**row, "_observed_at_s": observed_at_s})
    return rows


def _load_matching_snapshots(
    paths: list[Path],
    wanted: set[tuple[str, int]],
) -> dict[tuple[str, int], dict[str, Any]]:
    matches: dict[tuple[str, int], dict[str, Any]] = {}
    for path in paths:
        try:
            handle = path.open(encoding="utf-8", errors="replace")
        except OSError:
            continue
        with handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(row, dict):
                    continue
                key = _snapshot_key(
                    str(row.get("asset_id") or ""),
                    float(row.get("captured_at_s") or 0.0),
                )
                if key in wanted and row.get("asks"):
                    matches.setdefault(key, row)
    return matches


def acquire(report_paths: list[Path]) -> dict[str, Any]:
    identity = wide_policy_identity(wallet=WALLET, move_slice_keys=MOVE_SLICE_KEYS)
    if identity["wide_policy_fingerprint"] != FINGERPRINT:
        raise RuntimeError("directed exact-policy fingerprint no longer matches identity")

    candidates: dict[str, tuple[dict[str, Any], list[Path]]] = {}
    reports_scanned = 0
    for path in report_paths:
        report = _load(path)
        if not report:
            continue
        reports_scanned += 1
        clob_paths = _paths(report.get("clob_jsonl"))
        for row in _candidate_rows(report):
            source_key = "|".join(
                (
                    str(row.get("tx") or "").lower(),
                    str(row.get("asset_id") or ""),
                )
            )
            if source_key.strip("|"):
                candidates.setdefault(source_key, (row, clob_paths))

    by_path_set: dict[tuple[str, ...], list[tuple[str, dict[str, Any]]]] = {}
    for source_key, (row, paths) in candidates.items():
        by_path_set.setdefault(tuple(str(path) for path in paths), []).append(
            (source_key, row)
        )

    orders: list[dict[str, Any]] = []
    no_snapshot = 0
    not_fillable = 0
    for raw_paths, rows in by_path_set.items():
        wanted = {
            _snapshot_key(
                str(row.get("asset_id") or ""),
                float(row["_observed_at_s"]),
            )
            for _, row in rows
        }
        snapshots = _load_matching_snapshots(
            [Path(path) for path in raw_paths], wanted
        )
        for source_key, row in rows:
            snapshot = snapshots.get(
                _snapshot_key(
                    str(row.get("asset_id") or ""),
                    float(row["_observed_at_s"]),
                )
            )
            if not snapshot:
                no_snapshot += 1
                continue
            scored = CLOBMarketClient.summarize_book(
                snapshot,
                copy_size_usd=1.0,
                source_price=float(row.get("fill_price") or 0.0),
                max_slippage_bps=250.0,
            )
            if (
                scored.get("instant_fill_status") != "PASS"
                or float(scored.get("fill_ratio") or 0.0) < 0.999
            ):
                not_fillable += 1
                continue
            fill_price = float(scored.get("avg_fill_price") or 0.0)
            filled_shares = float(scored.get("fillable_shares") or 0.0)
            filled_cost = float(
                scored.get("fillable_usd") or filled_shares * fill_price
            )
            order_id = stable_id(
                "widehistory",
                {
                    "wallet": WALLET,
                    "fingerprint": FINGERPRINT,
                    "source_key": source_key,
                    "observed_at_s": row["_observed_at_s"],
                },
                length=32,
            )
            orders.append(
                {
                    "schema_version": 1,
                    "order_id": order_id,
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "source": "historical_simultaneous_clob_capture",
                    "wallet": WALLET,
                    "wide_policy_fingerprint": FINGERPRINT,
                    "wide_policy_identity": identity,
                    "transaction_hash": str(row.get("tx") or "").lower(),
                    "token_id": str(row.get("asset_id") or ""),
                    "condition_id": str(row.get("condition_id") or ""),
                    "market_slug": str(row.get("market_slug") or ""),
                    "source_event_ts": float(row.get("block_ts") or 0.0),
                    "source_price": float(row.get("fill_price") or 0.0),
                    "fill_price": round(fill_price, 9),
                    "filled_shares": round(filled_shares, 9),
                    "filled_cost_usd": round(filled_cost, 9),
                    "receipt_to_book_fetch_lag_s": round(
                        float(row["_observed_at_s"])
                        - float(row.get("block_ts") or 0.0),
                        6,
                    ),
                    "alpha_move_slice": {
                        "move_slice_key": str(row.get("move_slice_key") or ""),
                        "seconds_bucket": str(row.get("seconds_bucket") or ""),
                        "entry_price_band": str(
                            row.get("entry_price_band") or ""
                        ),
                    },
                    "our_price_evidence": {
                        "source": "clob_rest_book_snapshot",
                        "captured_at_s": float(snapshot.get("captured_at_s") or 0.0),
                        "best_ask": snapshot.get("best_ask"),
                        "asks": snapshot.get("asks"),
                        "route_fingerprint": (
                            snapshot.get("route_report") or {}
                        ).get("request_fingerprint"),
                    },
                    "resolved": False,
                    "expected_fee_usd": None,
                    "pre_fee_pnl_usd": None,
                    "post_fee_pnl_usd": None,
                }
            )

    orders.sort(
        key=lambda row: (
            float(row.get("source_event_ts") or 0.0),
            str(row.get("order_id") or ""),
        )
    )
    return {
        "schema_version": 1,
        "kind": "exact_wallet_source_history_acquisition",
        "flow_stage": "PROMOTE/LEARN",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "live_mutation": False,
        "acquisition_authority": {
            "wallet": WALLET,
            "wide_policy_fingerprint": FINGERPRINT,
            "move_slice_keys": list(MOVE_SLICE_KEYS),
            "price_authority": "contemporaneous_clob_ask_book_within_5s",
        },
        "summary": {
            "reports_scanned": reports_scanned,
            "source_candidates": len(candidates),
            "rows_admitted_exact_fp": len(orders),
            "refused_missing_exact_snapshot": no_snapshot,
            "refused_not_fillable_at_our_price": not_fillable,
        },
        "orders": orders,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-glob", default=DEFAULT_REPORT_GLOB)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    reports = [Path(path) for path in sorted(glob.glob(args.report_glob))]
    payload = acquire(reports)
    atomic_write_json(args.output, payload)
    print(json.dumps({"output": args.output, **payload["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
