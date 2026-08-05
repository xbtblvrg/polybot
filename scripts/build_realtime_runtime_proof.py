#!/usr/bin/env python3
"""Build paper-only runtime proof rows from realtime wallet fills.

This bridges the accepted Polygon WSS detection lane into the existing
candidate runtime proof contract. It never submits orders and never flips live
permissions: output is a paper tracker-state whose event_scores are consumed by
the profit engine through --candidate-forward-probe-live-tracker-state.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.live_tracker import CLOBMarketClient
from src.wallet_copy.models import WalletEvent, parse_ts, stable_id, utc_now_iso
from src.wallet_copy.profit_engine import CandidatePolicy, fast_candidate_policies, policy_accepts_event
from src.wallet_copy.realtime_feed import normalize_polygon_orderfilled_row
from src.wallet_copy.store import atomic_write_json, load_json


DEFAULT_POLYGON_JSONL = "data/research/polygon_orderfilled_ws_capture.jsonl"
DEFAULT_HISTORY_STATE = "data/research/wallet_copy_history_state.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_realtime_runtime_tracking_state.json"
DEFAULT_CLOB_BASE = "http://127.0.0.1:8787/clob"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--polygon-jsonl", default=DEFAULT_POLYGON_JSONL)
    parser.add_argument("--rtds-jsonl", default="")
    parser.add_argument("--history-state", default=DEFAULT_HISTORY_STATE)
    parser.add_argument("--dataapi-first-seen-jsonl", default="")
    parser.add_argument("--gamma-base-url", default="http://127.0.0.1:8787/gamma-api")
    parser.add_argument("--gamma-window-start", action="append", default=[])
    parser.add_argument("--gamma-window-lookaround", type=int, default=3)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--source-wallet", required=True)
    parser.add_argument("--policy-id", required=True)
    parser.add_argument("--wallet-name", default="")
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=1.5)
    parser.add_argument("--slippage-bps", type=float, default=250.0)
    parser.add_argument("--max-event-age-s", type=float, default=30.0)
    parser.add_argument("--min-required-buy-copy-events", type=int, default=3)
    parser.add_argument("--min-required-market-windows", type=int, default=2)
    parser.add_argument("--scan-limit", type=int, default=50_000)
    parser.add_argument("--max-proof-rows", type=int, default=25)
    parser.add_argument("--now-ts", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _policy_by_id(policy_id: str) -> CandidatePolicy:
    for policy in fast_candidate_policies():
        if policy.policy_id == policy_id:
            return policy
    raise SystemExit(f"unknown fast policy id: {policy_id}")


def _walk_history_events(obj: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        if obj.get("token_id") and (obj.get("market_slug") or obj.get("condition_id")):
            rows.append(obj)
        for value in obj.values():
            rows.extend(_walk_history_events(value))
    elif isinstance(obj, list):
        for value in obj:
            rows.extend(_walk_history_events(value))
    return rows


def _token_metadata(history_state_path: str) -> dict[str, dict[str, Any]]:
    payload = load_json(history_state_path, default={})
    grouped: dict[str, dict[str, Counter[str]]] = {}
    for row in _walk_history_events(payload):
        token_id = str(row.get("token_id") or "")
        if not token_id:
            continue
        entry = grouped.setdefault(
            token_id,
            {
                "market_slug": Counter(),
                "condition_id": Counter(),
                "outcome": Counter(),
            },
        )
        for key in ("market_slug", "condition_id", "outcome"):
            value = str(row.get(key) or "")
            if value:
                entry[key][value] += 1
    meta: dict[str, dict[str, Any]] = {}
    for token_id, counters in grouped.items():
        meta[token_id] = {
            key: (counter.most_common(1)[0][0] if counter else "")
            for key, counter in counters.items()
        }
    return meta


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _gamma_token_metadata(base_url: str, *, starts: list[int], timeout_s: float = 8.0) -> dict[str, dict[str, Any]]:
    meta: dict[str, dict[str, Any]] = {}
    if not base_url:
        return meta
    session = requests.Session()
    for start in starts:
        slug = f"btc-updown-5m-{int(start)}"
        # Gamma's /markets?slug= excludes closed markets by default; a BTC-5m
        # window is closed five minutes after it opens, so an open-only lookup
        # returns [] for every window we replay. Retry with closed=true.
        market: dict[str, Any] = {}
        for params in ({"slug": slug}, {"slug": slug, "closed": "true"}):
            try:
                response = session.get(
                    base_url.rstrip("/") + "/markets",
                    params=params,
                    timeout=timeout_s,
                    headers={"Accept": "application/json", "User-Agent": "wallet-copy-realtime-proof-gamma-map/1.0"},
                )
                response.raise_for_status()
                payload = response.json()
            except Exception:
                continue
            candidate = payload[0] if isinstance(payload, list) and payload else payload if isinstance(payload, dict) else {}
            if isinstance(candidate, dict) and candidate:
                market = candidate
                break
        if not market:
            continue
        tokens = [str(item) for item in _json_list(market.get("clobTokenIds") or market.get("clob_token_ids"))]
        outcomes = [str(item) for item in _json_list(market.get("outcomes"))]
        if not outcomes and len(tokens) >= 2:
            outcomes = ["Up", "Down"]
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "")
        for index, token_id in enumerate(tokens):
            if not token_id:
                continue
            meta[token_id] = {
                "market_slug": slug,
                "condition_id": condition_id,
                "outcome": outcomes[index] if index < len(outcomes) else "",
                "token_mapping_source": "gamma_relay",
            }
    return meta


def _iter_recent_jsonl(path: str, limit: int) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    with target.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        chunks: list[bytes] = []
        scanned = 0
        while position > 0 and scanned < 96_000_000 and len(rows) <= limit:
            size = min(1_048_576, position, 96_000_000 - scanned)
            position -= size
            handle.seek(position)
            chunk = handle.read(size)
            chunks.append(chunk)
            scanned += len(chunk)
            if b"\n" in chunk:
                lines = b"".join(reversed(chunks)).splitlines()
                if len(lines) > limit:
                    break
    raw_lines = b"".join(reversed(chunks)).splitlines()[-max(1, int(limit)) :]
    for raw in raw_lines:
        try:
            row = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _market_slug_from_ts(ts: float | None) -> str:
    if ts is None:
        return ""
    start = int(float(ts) // 300 * 300)
    return f"btc-updown-5m-{start}"


def _dataapi_seen_keys(path: str) -> set[tuple[str, str]]:
    if not path:
        return set()
    keys: set[tuple[str, str]] = set()
    for row in _iter_recent_jsonl(path, 250_000):
        if str(row.get("event") or "") != "dataapi_first_seen":
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        tx = str(row.get("transactionHash") or row.get("transaction_hash") or "").strip().lower()
        if wallet and tx:
            keys.add((wallet, tx))
    return keys


def _wallet_event_from_polygon(
    row: dict[str, Any],
    *,
    source_wallet: str,
    wallet_name: str,
    token_meta: dict[str, dict[str, Any]],
    dataapi_seen_keys: set[tuple[str, str]],
    diagnostics: Counter[str],
) -> WalletEvent | None:
    event = normalize_polygon_orderfilled_row(row)
    if event is None:
        return None
    if _norm_wallet(event.source_wallet) != source_wallet:
        return None
    if str(event.side or "").upper() != "BUY":
        return None
    token_id = str(event.asset or "")
    if not token_id or event.price is None or event.size is None:
        return None
    meta = token_meta.get(token_id, {})
    if not meta.get("market_slug") or not meta.get("condition_id") or not meta.get("outcome"):
        diagnostics["token_mapping_missing"] += 1
        return None
    tx = str(event.transaction_hash or "").strip().lower()
    if dataapi_seen_keys and (source_wallet, tx) not in dataapi_seen_keys:
        diagnostics["dataapi_reconciliation_missing"] += 1
        return None
    event_ts = event.event_ts
    market_slug = str(meta.get("market_slug") or "")
    outcome = str(meta.get("outcome") or "")
    condition_id = str(meta.get("condition_id") or "")
    usdc_size = float(event.price) * float(event.size)
    return WalletEvent(
        source_wallet=source_wallet,
        wallet_name=wallet_name or source_wallet,
        row_type="TRADE",
        action="BUY",
        condition_id=condition_id,
        market_slug=market_slug,
        outcome=outcome,
        price=float(event.price),
        size=float(event.size),
        usdc_size=usdc_size,
        event_ts=event_ts,
        observed_ts=float(event.received_at_s),
        source="polygon_orderfilled_ws_runtime_proof",
        token_id=token_id,
        transaction_hash=event.transaction_hash,
        raw=row,
    )


def _wallet_event_from_rtds(
    row: dict[str, Any],
    *,
    source_wallet: str,
    wallet_name: str,
    dataapi_seen_keys: set[tuple[str, str]],
    diagnostics: Counter[str],
) -> WalletEvent | None:
    if str(row.get("event") or "") != "rtds_trade_event":
        return None
    if _norm_wallet(row.get("source_wallet")) != source_wallet:
        return None
    if str(row.get("side") or "").upper() != "BUY":
        return None
    token_id = str(row.get("asset") or "")
    condition_id = str(row.get("condition_id") or "")
    market_slug = str(row.get("market_slug") or "")
    tx = str(row.get("transaction_hash") or "").strip().lower()
    price = _float(row.get("price"))
    size = _float(row.get("size"))
    if not token_id or not condition_id or not market_slug or price is None or size is None:
        diagnostics["rtds_required_field_missing"] += 1
        return None
    if dataapi_seen_keys and (source_wallet, tx) not in dataapi_seen_keys:
        diagnostics["dataapi_reconciliation_missing"] += 1
        return None
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    outcome = str(raw.get("outcome") or row.get("outcome") or "")
    if not outcome:
        diagnostics["rtds_outcome_missing"] += 1
        return None
    observed_ts = _float(row.get("received_at_s") or row.get("observed_ts") or row.get("captured_at_s"))
    if observed_ts is None:
        diagnostics["rtds_observed_ts_missing"] += 1
        return None
    return WalletEvent(
        source_wallet=source_wallet,
        wallet_name=wallet_name or source_wallet,
        row_type="TRADE",
        action="BUY",
        condition_id=condition_id,
        market_slug=market_slug,
        outcome=outcome,
        price=float(price),
        size=float(size),
        usdc_size=float(price) * float(size),
        event_ts=parse_ts(row.get("event_ts") or raw.get("timestamp")),
        observed_ts=float(observed_ts),
        source="rtds_activity_runtime_proof",
        token_id=token_id,
        transaction_hash=tx,
        raw=row,
    )


def _proof_row(
    event: WalletEvent,
    *,
    candidate_id: str,
    policy: CandidatePolicy,
    book_summary: dict[str, Any],
    route_report: dict[str, Any],
) -> dict[str, Any]:
    event_age_s = event.age_s
    return {
        "schema_version": 1,
        "source": "runtime_tracker_state",
        "state_path": DEFAULT_OUTPUT,
        "candidate_id": candidate_id,
        "profit_policy_candidate_id": candidate_id,
        "policy_id": policy.policy_id,
        "profit_policy_id": policy.policy_id,
        "profit_policy_context_id": policy.policy_id,
        "source_event_id": event.event_id,
        "source_fingerprint": event.source_fingerprint,
        "source_wallet": event.source_wallet.lower(),
        "wallet_name": event.wallet_name,
        "wallet_action": "BUY",
        "condition_id": event.condition_id,
        "market_slug": event.market_slug,
        "outcome": event.outcome,
        "token_id": event.token_id,
        "source_price": round(float(event.price), 6),
        "source_shares": round(float(event.size), 6),
        "source_usdc_size": round(float(event.usdc_size), 6),
        "source_event_ts": event.event_ts,
        "observed_ts": event.observed_ts,
        "api_latency_s": round(float(event_age_s or 0.0), 6),
        "api_latency_basis": "polygon_wss_receive_minus_block_ts",
        "event_age_s": round(float(event_age_s or 0.0), 6),
        "copy_status": "COPIED_FILLED",
        "coverage_status": "MIRRORED",
        "profit_policy_accepted": True,
        "profit_policy_reason": "accepted",
        "intent_id": stable_id("ci", {"event": event.source_fingerprint, "policy": policy.policy_id}),
        "copy_size_usd": book_summary.get("copy_size_usd"),
        "copy_shares_requested": book_summary.get("fillable_shares"),
        "paper_final_status": "FILLED",
        "fill_source": "clob_book_evidence",
        "filled_size_usd": book_summary.get("fillable_usd"),
        "filled_shares": book_summary.get("fillable_shares"),
        "fill_ratio": book_summary.get("fill_ratio"),
        "share_fill_ratio": book_summary.get("fill_ratio"),
        "fill_price": book_summary.get("avg_fill_price"),
        "slippage_bps": (
            round((float(book_summary.get("avg_fill_price") or 0.0) - float(event.price)) / float(event.price) * 10_000.0, 6)
            if float(event.price) > 0 and float(book_summary.get("avg_fill_price") or 0.0) > 0
            else None
        ),
        "book_hash": book_summary.get("book_hash"),
        "book_timestamp": book_summary.get("book_timestamp"),
        "clob_book_status": "OK",
        "clob_instant_fill_status": book_summary.get("instant_fill_status"),
        "clob_best_bid": book_summary.get("best_bid"),
        "clob_best_ask": book_summary.get("best_ask"),
        "clob_spread": book_summary.get("spread"),
        "clob_fillable_usd": book_summary.get("fillable_usd"),
        "clob_remaining_usd": book_summary.get("remaining_usd"),
        "clob_fill_ratio": book_summary.get("fill_ratio"),
        "clob_max_copy_price": book_summary.get("max_copy_price"),
        "clob_route_status": route_report.get("status"),
        "clob_route_class": route_report.get("route_class"),
        "clob_route_report_id": route_report.get("route_report_id"),
        "clob_routed_host": route_report.get("routed_host"),
        "clob_source_base_override_configured": route_report.get("source_base_override_configured"),
        "clob_request_fingerprint": route_report.get("request_fingerprint"),
        "generated_at": utc_now_iso(),
    }


def build_realtime_runtime_tracker_state(
    *,
    polygon_rows: list[dict[str, Any]],
    token_meta: dict[str, dict[str, Any]],
    rtds_rows: list[dict[str, Any]] | None = None,
    dataapi_seen_keys: set[tuple[str, str]] | None = None,
    candidate_id: str,
    source_wallet: str,
    wallet_name: str,
    policy: CandidatePolicy,
    clob: CLOBMarketClient,
    max_event_age_s: float,
    max_proof_rows: int,
    min_required_buy_copy_events: int,
    min_required_market_windows: int,
    slippage_bps: float,
) -> dict[str, Any]:
    source_wallet = _norm_wallet(source_wallet)
    rows: list[dict[str, Any]] = []
    seen_events: set[str] = set()
    diagnostics = Counter()
    reconciled_keys = dataapi_seen_keys or set()
    input_rows = [*polygon_rows, *(rtds_rows or [])]
    for raw in reversed(input_rows):
        event = _wallet_event_from_polygon(
            raw,
            source_wallet=source_wallet,
            wallet_name=wallet_name,
            token_meta=token_meta,
            dataapi_seen_keys=reconciled_keys,
            diagnostics=diagnostics,
        )
        if event is None:
            event = _wallet_event_from_rtds(
                raw,
                source_wallet=source_wallet,
                wallet_name=wallet_name,
                dataapi_seen_keys=reconciled_keys,
                diagnostics=diagnostics,
            )
        if event is None:
            diagnostics["not_target_buy"] += 1
            continue
        if event.event_id in seen_events:
            diagnostics["duplicate_event"] += 1
            continue
        seen_events.add(event.event_id)
        if event.age_s is None or float(event.age_s) > float(max_event_age_s):
            diagnostics["stale_event"] += 1
            continue
        accepted, reason = policy_accepts_event(policy, event)
        if not accepted:
            diagnostics[f"policy_reject:{reason}"] += 1
            continue
        copy_size_usd = min(float(event.usdc_size) * float(policy.wallet_fraction), float(policy.max_order_usd))
        if copy_size_usd < float(policy.min_order_usd):
            diagnostics["copy_size_below_minimum"] += 1
            continue
        try:
            book = clob.get_book(event.token_id)
            summary = CLOBMarketClient.summarize_book(
                book,
                copy_size_usd=copy_size_usd,
                source_price=float(event.price),
                max_slippage_bps=slippage_bps,
            )
        except Exception as exc:  # noqa: BLE001 - proof script records measurement failures.
            diagnostics[f"clob_error:{type(exc).__name__}"] += 1
            continue
        if summary.get("instant_fill_status") != "PASS":
            diagnostics[f"clob_blocked:{summary.get('blocking_reason') or 'unknown'}"] += 1
            continue
        rows.append(
            _proof_row(
                event,
                candidate_id=candidate_id,
                policy=policy,
                book_summary=summary,
                route_report=clob.last_route_report if isinstance(clob.last_route_report, dict) else {},
            )
        )
        if len(rows) >= max(1, int(max_proof_rows)):
            break
    source_events = {str(row.get("source_event_id")) for row in rows if row.get("source_event_id")}
    market_windows = {str(row.get("market_slug")) for row in rows if row.get("market_slug")}
    status = (
        "PASS"
        if len(source_events) >= int(min_required_buy_copy_events)
        and len(market_windows) >= int(min_required_market_windows)
        else "ANALYZE"
    )
    candidate_summary = {
        "required_buy_copy_events": len(source_events),
        "clob_filled_buy_copy_events": len(source_events),
        "fallback_filled_buy_copy_events": 0,
        "rejected_buy_copy_events": 0,
        "missed_buy_copy_events": 0,
        "required_event_age_p95_s": _percentile([float(row.get("event_age_s") or 0.0) for row in rows], 95),
    }
    return {
        "schema_version": 1,
        "kind": "wallet_copy_realtime_runtime_tracking_state",
        "generated_at": utc_now_iso(),
        "status": status,
        "paper_only": True,
        "live_orders_allowed": False,
        "source": "realtime_wallet_feed_plus_clob_book",
        "summary": {
            "candidate_id": candidate_id,
            "candidate_policy_id": policy.policy_id,
            "candidate_source_wallet": source_wallet,
            "candidate_policy_required_buy_copy_events": int(min_required_buy_copy_events),
            "candidate_copy_truth_summary": candidate_summary,
            "copy_efficiency_summary": candidate_summary,
            "copy_efficiency": {"event_scores": rows},
            "realtime_runtime_proof": {
                "proof_rows": len(rows),
                "source_events": len(source_events),
                "market_windows": len(market_windows),
                "dataapi_reconciliation_required": bool(reconciled_keys),
                "diagnostics": dict(diagnostics),
                "input_rows": len(input_rows),
                "polygon_rows": len(polygon_rows),
                "rtds_rows": len(rtds_rows or []),
            },
        },
    }


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * max(0.0, min(100.0, pct)) / 100.0))
    return round(ordered[index], 6)


def main() -> int:
    args = parse_args()
    source_wallet = _norm_wallet(args.source_wallet)
    if not source_wallet:
        raise SystemExit("--source-wallet must be a 0x address")
    policy = _policy_by_id(args.policy_id)
    polygon_rows = _iter_recent_jsonl(args.polygon_jsonl, args.scan_limit) if args.polygon_jsonl else []
    rtds_rows = _iter_recent_jsonl(args.rtds_jsonl, args.scan_limit) if args.rtds_jsonl else []
    token_meta = _token_metadata(args.history_state)
    now_start = int((args.now_ts or time.time()) // 300 * 300)
    requested_starts = [int(value) for value in args.gamma_window_start or []]
    requested_starts.extend(
        now_start + offset * 300
        for offset in range(-max(0, int(args.gamma_window_lookaround)), max(0, int(args.gamma_window_lookaround)) + 1)
    )
    token_meta.update(_gamma_token_metadata(args.gamma_base_url, starts=sorted(set(requested_starts))))
    dataapi_seen = _dataapi_seen_keys(args.dataapi_first_seen_jsonl)
    clob = CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s))
    state = build_realtime_runtime_tracker_state(
        polygon_rows=polygon_rows,
        rtds_rows=rtds_rows,
        token_meta=token_meta,
        dataapi_seen_keys=dataapi_seen,
        candidate_id=str(args.candidate_id),
        source_wallet=source_wallet,
        wallet_name=args.wallet_name,
        policy=policy,
        clob=clob,
        max_event_age_s=float(args.max_event_age_s),
        max_proof_rows=int(args.max_proof_rows),
        min_required_buy_copy_events=int(args.min_required_buy_copy_events),
        min_required_market_windows=int(args.min_required_market_windows),
        slippage_bps=float(args.slippage_bps),
    )
    state["state_path"] = args.output
    for row in state["summary"]["copy_efficiency"]["event_scores"]:
        row["state_path"] = args.output
    if not args.dry_run:
        atomic_write_json(args.output, state)
    print(json.dumps({
        "status": state.get("status"),
        "output": args.output,
        "paper_only": True,
        "live_orders_allowed": False,
        "proof": state.get("summary", {}).get("realtime_runtime_proof", {}),
    }, indent=2, sort_keys=True))
    return 0 if state.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
