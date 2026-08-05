#!/usr/bin/env python3
"""Replay event-level BTC-5m history for source-active policy candidates.

Flow stage: LEARN/PROMOTE. This is bounded research replay only: it fetches
remote wallet history for Fable-approved source-active candidates, writes one
paper-only history artifact per wallet, and appends those artifacts to the
temporal supplemental manifest. It does not mutate live config or submit orders.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import build_wallet_market_scan_intake as intake  # noqa: E402
from scripts.build_strategy_decompiler_intake import _float, _parse_ts  # noqa: E402
from scripts.build_wallet_market_cohort_replay import _fetch_wallet_page, _norm_wallet  # noqa: E402
from scripts.build_wallet_temporal_profitability_registry import (  # noqa: E402
    DEFAULT_SUPPLEMENTAL_HISTORY_MANIFEST,
)
from src.wallet_copy.http_client import PolymarketHttpClient, PolymarketRouteError  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_PACKET = "data/research/cohort_alive_admission_packets_latest.json"
DEFAULT_OUTPUT_DIR = "data/research"
DEFAULT_SUMMARY = "data/research/source_active_policy_history_replay_latest.json"
REPLAY_ARTIFACT_RE = re.compile(
    r"^source_active_policy_history_([0-9a-f]{10})_(\d{8}T\d{6}Z)\.json$"
)


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _utc_now_iso() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _iso(ts: float | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(float(ts), tz=UTC).isoformat().replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC)


def _packet_policy_windows(row: dict[str, Any]) -> int:
    source_active = row.get("source_active") if isinstance(row.get("source_active"), dict) else {}
    return int(source_active.get("policy_eligible_windows") or 0)


def _select_targets(
    packet: dict[str, Any],
    *,
    limit: int,
    priority_wallets: list[str] | None = None,
    already_replayed_wallets: set[str] | None = None,
) -> list[dict[str, Any]]:
    rows = packet.get("packets") if isinstance(packet.get("packets"), list) else []
    targets: list[dict[str, Any]] = []
    already_replayed_wallets = already_replayed_wallets or set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        source_active = row.get("source_active") if isinstance(row.get("source_active"), dict) else {}
        if not wallet:
            continue
        if wallet in already_replayed_wallets:
            continue
        if source_active.get("policy_eligible_tally_status") != "PASS":
            continue
        if row.get("already_active_runtime_member"):
            continue
        if row.get("denylist_cells"):
            continue
        targets.append(row)
    by_wallet = {_norm_wallet(row.get("wallet")): row for row in targets}
    priority_rows: list[dict[str, Any]] = []
    for wallet in priority_wallets or []:
        norm = _norm_wallet(wallet)
        row = by_wallet.pop(norm, None)
        if row is not None:
            priority_rows.append(row)
    targets = list(by_wallet.values())
    targets.sort(
        key=lambda row: (
            -_packet_policy_windows(row),
            _float(row.get("latest_trade_age_h"), 1_000_000.0),
            -_float(row.get("paper_pnl_usd"), 0.0),
            _norm_wallet(row.get("wallet")),
        )
    )
    return [*priority_rows, *targets][: max(0, int(limit))]


def _select_recurring_cohort_targets(
    cohort: dict[str, Any],
    manifest: dict[str, Any] | list[Any],
    *,
    limit: int,
    priority_wallets: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Round-robin a bounded wallet cohort by its oldest replay batch."""
    raw_rows = cohort.get("nearest_frontier") if isinstance(cohort, dict) else []
    rows_by_wallet: dict[str, dict[str, Any]] = {}
    for row in raw_rows if isinstance(raw_rows, list) else []:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if wallet and wallet not in rows_by_wallet:
            rows_by_wallet[wallet] = row

    files = (
        manifest.get("supplemental_history_files") or []
        if isinstance(manifest, dict)
        else manifest
        if isinstance(manifest, list)
        else []
    )
    latest_by_prefix: dict[str, str] = {}
    for raw_path in files:
        match = REPLAY_ARTIFACT_RE.match(Path(str(raw_path or "")).name)
        if not match:
            continue
        prefix, batch_id = match.groups()
        latest_by_prefix[prefix] = max(latest_by_prefix.get(prefix, ""), batch_id)

    ranked = sorted(
        rows_by_wallet.items(),
        key=lambda item: (
            latest_by_prefix.get(item[0][2:12], ""),
            item[0],
        ),
    )
    priority = {
        wallet: index
        for index, raw in enumerate(priority_wallets or [])
        if (wallet := _norm_wallet(raw))
    }
    ranked.sort(
        key=lambda item: (
            0 if item[0] in priority else 1,
            priority.get(item[0], 0),
            latest_by_prefix.get(item[0][2:12], ""),
            item[0],
        )
    )
    return [
        {
            **row,
            "wallet": wallet,
            "source_active": {
                "policy_eligible_tally_status": "PASS",
                "policy_eligible_windows": _packet_policy_windows(row),
                "source": "recurring_wide_frontier",
            },
        }
        for wallet, row in ranked[: max(0, int(limit))]
    ]


def _trade_wallet(row: dict[str, Any]) -> str:
    return _norm_wallet(row.get("proxyWallet") or row.get("proxy_wallet") or row.get("source_wallet"))


def _is_btc5m_trade(row: dict[str, Any]) -> bool:
    slug = str(row.get("slug") or row.get("marketSlug") or row.get("eventSlug") or "").strip().lower()
    return slug.startswith("btc-updown-5m-") and intake.is_crypto_5m_trade(row)


def _normalize_history_event(row: dict[str, Any], *, wallet: str, wallet_name: str, cutoff_ts: float) -> dict[str, Any] | None:
    if _trade_wallet(row) and _trade_wallet(row) != wallet:
        return None
    if not _is_btc5m_trade(row):
        return None
    if str(row.get("side") or "").upper() != "BUY":
        return None
    ts = _parse_ts(row.get("timestamp") or row.get("event_ts"))
    if ts <= 0.0 or ts < cutoff_ts:
        return None
    price = _float(row.get("price"), 0.0)
    size = _float(row.get("size"), 0.0)
    if price <= 0.0 or price >= 1.0 or size <= 0.0:
        return None
    slug = str(row.get("slug") or row.get("marketSlug") or row.get("eventSlug") or "").strip().lower()
    condition_id = str(row.get("conditionId") or row.get("condition_id") or "").strip()
    tx = str(row.get("transactionHash") or row.get("transaction_hash") or "").strip()
    return {
        "schema_version": 1,
        "row_type": "trade",
        "source": "polymarket_data_api",
        "source_wallet": wallet,
        "wallet_name": wallet_name,
        "action": "BUY",
        "market_slug": slug,
        "event_slug": slug,
        "condition_id": condition_id,
        "market_id": condition_id,
        "outcome": row.get("outcome"),
        "outcome_index": row.get("outcomeIndex") or row.get("outcome_index"),
        "price": price,
        "size": size,
        "usdc_size": round(price * size, 6),
        "event_ts": ts,
        "observed_ts": _utc_now().timestamp(),
        "transaction_hash": tx,
        "raw": row,
    }


def _artifact_path(output_dir: Path, *, wallet: str, batch_id: str) -> Path:
    return output_dir / f"source_active_policy_history_{wallet[2:12]}_{batch_id}.json"


def _relative(path: Path) -> str:
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)


def _append_manifest(manifest_path: Path, artifact_paths: list[Path], *, batch_id: str) -> dict[str, Any]:
    existing = load_json(manifest_path, default={})
    if isinstance(existing, list):
        payload: dict[str, Any] = {
            "schema_version": 1,
            "kind": "temporal_supplemental_history_manifest",
            "flow_stage": "LEARN/PROMOTE",
            "supplemental_history_files": [str(path) for path in existing],
        }
    elif isinstance(existing, dict):
        payload = dict(existing)
    else:
        payload = {
            "schema_version": 1,
            "kind": "temporal_supplemental_history_manifest",
            "flow_stage": "LEARN/PROMOTE",
            "supplemental_history_files": [],
        }
    files = [str(path) for path in payload.get("supplemental_history_files") or [] if str(path or "").strip()]
    files.extend(_relative(path) for path in artifact_paths)
    payload["supplemental_history_files"] = sorted(dict.fromkeys(files))
    payload["updated_at"] = _utc_now_iso()
    payload["last_batch_id"] = batch_id
    atomic_write_json(manifest_path, payload)
    return payload


def _manifest_wallets(manifest_path: Path) -> set[str]:
    payload = load_json(manifest_path, default={})
    if isinstance(payload, dict):
        files = payload.get("supplemental_history_files") or []
    elif isinstance(payload, list):
        files = payload
    else:
        files = []
    wallets: set[str] = set()
    for raw_path in files:
        text = str(raw_path or "").strip()
        if not text:
            continue
        path = Path(text)
        if not path.is_absolute():
            path = ROOT / path
        artifact = load_json(path, default={})
        if not isinstance(artifact, dict):
            continue
        for row in artifact.get("wallets") or []:
            if isinstance(row, dict):
                wallet = _norm_wallet(row.get("address") or row.get("wallet"))
                if wallet:
                    wallets.add(wallet)
        for row in artifact.get("wallet_results") or []:
            if isinstance(row, dict):
                wallet_obj = row.get("wallet") if isinstance(row.get("wallet"), dict) else {}
                wallet = _norm_wallet(wallet_obj.get("address") or row.get("source_wallet"))
                if wallet:
                    wallets.add(wallet)
    return wallets


def replay_wallet(
    *,
    client: PolymarketHttpClient,
    wallet: str,
    rank: int,
    packet_row: dict[str, Any],
    output_dir: Path,
    batch_id: str,
    cutoff_ts: float,
    history_limit: int,
    max_events_per_wallet: int,
    timeout_s: float,
    sleep_s: float,
    hard_stop: datetime | None,
) -> dict[str, Any]:
    wallet_name = f"source_active_policy_{wallet[2:10]}"
    pages_cap = int(math.ceil(max(1, int(max_events_per_wallet)) / max(1, int(history_limit))))
    route_classes: Counter[str] = Counter()
    api_errors: list[str] = []
    events: list[dict[str, Any]] = []
    raw_rows_seen = 0
    oldest_ts: float | None = None
    latest_ts: float | None = None
    stop_reason = "max_pages_or_event_cap"
    pagination_cap_reached = False

    for page in range(pages_cap):
        if hard_stop is not None and _utc_now() >= hard_stop:
            stop_reason = "hard_stop_utc"
            break
        offset = page * int(history_limit)
        try:
            rows, route_report = _fetch_wallet_page(
                client,
                wallet=wallet,
                limit=int(history_limit),
                offset=offset,
                timeout_s=float(timeout_s),
            )
        except (PolymarketRouteError, requests.RequestException, ValueError) as exc:
            api_errors.append(f"{type(exc).__name__}: {exc}")
            stop_reason = "api_error"
            break
        route_class = str(route_report.get("route_class") or "UNKNOWN")
        route_classes[route_class] += 1
        if bool(route_report.get("pagination_cap_reached")):
            pagination_cap_reached = True
            stop_reason = "pagination_cap_reached"
            break
        if not rows:
            stop_reason = "empty_page"
            break
        raw_rows_seen += len(rows)
        page_oldest = 0.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = _parse_ts(row.get("timestamp") or row.get("event_ts"))
            if ts > 0:
                page_oldest = ts if page_oldest <= 0.0 else min(page_oldest, ts)
                oldest_ts = ts if oldest_ts is None else min(oldest_ts, ts)
                latest_ts = ts if latest_ts is None else max(latest_ts, ts)
            event = _normalize_history_event(row, wallet=wallet, wallet_name=wallet_name, cutoff_ts=cutoff_ts)
            if event is not None:
                events.append(event)
        if len(events) >= int(max_events_per_wallet):
            events = events[: int(max_events_per_wallet)]
            stop_reason = "max_events_per_wallet"
            break
        if len(rows) < int(history_limit):
            stop_reason = "short_page"
            break
        if page_oldest and page_oldest < cutoff_ts:
            stop_reason = "lookback_cutoff_reached"
            break
        if float(sleep_s) > 0:
            time.sleep(float(sleep_s))

    output = _artifact_path(output_dir, wallet=wallet, batch_id=batch_id)
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_history_state",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": _utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "wallets": [
            {
                "address": wallet,
                "name": wallet_name,
                "enabled": True,
                "market_filter": "btc_5m",
                "asset_allowlist": ["BTC"],
                "tags": ["source_active_policy_replay", "paper_only"],
            }
        ],
        "events": sorted(events, key=lambda row: (float(row.get("event_ts") or 0.0), str(row.get("transaction_hash") or ""))),
        "copy_intents": [],
        "wallet_results": [
            {
                "wallet": {"address": wallet, "name": wallet_name},
                "rank": rank,
                "source": "source_active_policy_remote_replay",
                "events": len(events),
                "latest_event_ts": _iso(latest_ts),
                "policy_eligible_windows": _packet_policy_windows(packet_row),
                "paper_pnl_usd": packet_row.get("paper_pnl_usd"),
                "resolved_copyable_events": packet_row.get("resolved_copyable_events"),
            }
        ],
        "replay": {
            "batch_id": batch_id,
            "rank": rank,
            "lookback_cutoff_iso": _iso(cutoff_ts),
            "max_events_per_wallet": int(max_events_per_wallet),
            "history_limit": int(history_limit),
            "raw_rows_seen": raw_rows_seen,
            "normalized_btc5m_buy_events": len(events),
            "oldest_remote_event_ts": _iso(oldest_ts),
            "latest_remote_event_ts": _iso(latest_ts),
            "stop_reason": stop_reason,
            "pagination_cap_reached": pagination_cap_reached,
            "route_class_counts": dict(sorted(route_classes.items())),
            "api_errors": api_errors,
            "packet_source": {
                "wallet": packet_row.get("wallet"),
                "policy_eligible_windows": _packet_policy_windows(packet_row),
                "source_active_windows": (packet_row.get("source_active") or {}).get("source_active_windows")
                if isinstance(packet_row.get("source_active"), dict)
                else None,
                "latest_trade_age_h": packet_row.get("latest_trade_age_h"),
                "paper_pnl_usd": packet_row.get("paper_pnl_usd"),
                "resolved_copyable_events": packet_row.get("resolved_copyable_events"),
            },
        },
    }
    atomic_write_json(output, payload)
    return {
        "wallet": wallet,
        "rank": rank,
        "output": _relative(output),
        "raw_rows_seen": raw_rows_seen,
        "normalized_btc5m_buy_events": len(events),
        "stop_reason": stop_reason,
        "pagination_cap_reached": pagination_cap_reached,
        "route_class_counts": dict(sorted(route_classes.items())),
        "api_errors": api_errors,
        "policy_eligible_windows": _packet_policy_windows(packet_row),
        "latest_trade_age_h": packet_row.get("latest_trade_age_h"),
        "paper_pnl_usd": packet_row.get("paper_pnl_usd"),
        "resolved_copyable_events": packet_row.get("resolved_copyable_events"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", default=DEFAULT_PACKET)
    parser.add_argument(
        "--recurring-cohort",
        default="",
        help=(
            "Optional frontier JSON whose nearest_frontier wallets are replayed "
            "oldest-batch-first, bounded by --max-wallets."
        ),
    )
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--summary-output", default=DEFAULT_SUMMARY)
    parser.add_argument("--manifest", default=DEFAULT_SUPPLEMENTAL_HISTORY_MANIFEST)
    parser.add_argument("--batch-id", default="")
    parser.add_argument("--max-wallets", type=int, default=20)
    parser.add_argument("--lookback-days", type=float, default=30.0)
    parser.add_argument("--max-events-per-wallet", type=int, default=10_000)
    parser.add_argument("--history-limit", type=int, default=500)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--sleep-s", type=float, default=0.12)
    parser.add_argument("--hard-stop-utc", default="")
    parser.add_argument("--priority-wallet", action="append", default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    batch_id = str(args.batch_id or "").strip() or _utc_now().strftime("%Y%m%dT%H%M%SZ")
    packet = load_json(args.packet, default={})
    packet = packet if isinstance(packet, dict) else {}
    manifest_path = ROOT / args.manifest
    manifest_before = load_json(manifest_path, default={})
    already_replayed_wallets = _manifest_wallets(manifest_path)
    if str(args.recurring_cohort or "").strip():
        cohort = load_json(ROOT / args.recurring_cohort, default={})
        targets = _select_recurring_cohort_targets(
            cohort if isinstance(cohort, dict) else {},
            manifest_before if isinstance(manifest_before, (dict, list)) else {},
            limit=int(args.max_wallets),
            priority_wallets=list(args.priority_wallet or []),
        )
    else:
        targets = _select_targets(
            packet,
            limit=int(args.max_wallets),
            priority_wallets=list(args.priority_wallet or []),
            already_replayed_wallets=already_replayed_wallets,
        )
    generated_at = _utc_now()
    cutoff_ts = (generated_at - timedelta(days=max(0.0, float(args.lookback_days)))).timestamp()
    hard_stop = _parse_utc(args.hard_stop_utc)
    output_dir = ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    client = PolymarketHttpClient(
        timeout_s=float(args.timeout_s),
        retries=1,
        user_agent="source-active-policy-history-replay/1.0",
    )
    results: list[dict[str, Any]] = []
    artifact_paths: list[Path] = []
    for idx, row in enumerate(targets, start=1):
        if hard_stop is not None and _utc_now() >= hard_stop:
            break
        wallet = _norm_wallet(row.get("wallet"))
        if not wallet:
            continue
        result = replay_wallet(
            client=client,
            wallet=wallet,
            rank=idx,
            packet_row=row,
            output_dir=output_dir,
            batch_id=batch_id,
            cutoff_ts=cutoff_ts,
            history_limit=int(args.history_limit),
            max_events_per_wallet=int(args.max_events_per_wallet),
            timeout_s=float(args.timeout_s),
            sleep_s=float(args.sleep_s),
            hard_stop=hard_stop,
        )
        results.append(result)
        artifact_paths.append(ROOT / result["output"])
    manifest = _append_manifest(manifest_path, artifact_paths, batch_id=batch_id) if artifact_paths else {}
    summary = {
        "schema_version": 1,
        "kind": "source_active_policy_history_replay",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": _utc_now_iso(),
        "batch_id": batch_id,
        "paper_only": True,
        "live_orders_allowed": False,
        "packet": args.packet,
        "recurring_cohort": str(args.recurring_cohort or "") or None,
        "criteria": {
            "max_wallets": int(args.max_wallets),
            "lookback_days": float(args.lookback_days),
            "max_events_per_wallet": int(args.max_events_per_wallet),
            "hard_stop_utc": args.hard_stop_utc,
            "ranking": (
                "oldest_manifest_batch_then_wallet"
                if str(args.recurring_cohort or "").strip()
                else "policy_eligible_windows_desc_then_external_liveness_recency"
            ),
            "priority_wallets": list(args.priority_wallet or []),
            "already_replayed_wallets_known": len(already_replayed_wallets),
        },
        "summary": {
            "targets_selected": len(targets),
            "wallets_replayed": len(results),
            "raw_rows_seen": sum(int(row.get("raw_rows_seen") or 0) for row in results),
            "normalized_btc5m_buy_events": sum(int(row.get("normalized_btc5m_buy_events") or 0) for row in results),
            "api_error_wallets": sum(1 for row in results if row.get("api_errors")),
            "manifest_files_after": len(manifest.get("supplemental_history_files") or []) if isinstance(manifest, dict) else None,
        },
        "targets": [
            {
                "wallet": _norm_wallet(row.get("wallet")),
                "policy_eligible_windows": _packet_policy_windows(row),
                "latest_trade_age_h": row.get("latest_trade_age_h"),
                "paper_pnl_usd": row.get("paper_pnl_usd"),
                "resolved_copyable_events": row.get("resolved_copyable_events"),
            }
            for row in targets
        ],
        "results": results,
    }
    atomic_write_json(ROOT / args.summary_output, summary)
    print(json.dumps({"output": args.summary_output, "summary": summary["summary"]}, sort_keys=True))
    return 0 if int(summary["summary"]["api_error_wallets"] or 0) == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
