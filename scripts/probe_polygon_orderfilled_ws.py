#!/usr/bin/env python3
"""Probe Polygon OrderFilled logs as a read-only wallet-copy detection fallback."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.registry import load_wallet_registry
from src.wallet_copy.realtime_feed import decode_polygon_orderfilled_v2
from src.wallet_copy.store import append_jsonl, append_jsonl_many, atomic_write_json, load_json


DEFAULT_HTTP_RPC = "https://polygon-bor-rpc.publicnode.com"
DEFAULT_WSS_RPC = "wss://polygon-bor-rpc.publicnode.com"
DEFAULT_OUTPUT = "data/research/polygon_orderfilled_ws_capture.jsonl"
DEFAULT_ORDERFILLED_OUTPUT = "data/research/polygon_orderfilled_ws_orderfilled_only.jsonl"
DEFAULT_REPORT = "data/research/detection_latency_report.json"
DEFAULT_COMPARISON = "data/research/polygon_ws_dataapi_active_set_comparison.jsonl"
DEFAULT_DATAAPI_FIRST_SEEN = "data/research/dataapi_first_seen.jsonl"
DEFAULT_ORDERFILLED_WAKE_SOCKET = "/tmp/polymarket_orderfilled_fast_lane.sock"
DEFAULT_REGISTRY = "configs/wallet_copy/wallets.json"

FABLE_EXCHANGE_ADDRESSES = (
    "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
    "0xC5d563A36AE78145C45a50134d48A1215220f80a",
)
CURRENT_V2_EXCHANGE_ADDRESSES = (
    "0xE111180000d2663C0091e4f400237545B87B996B",
    "0xe2222d279d744050d28e00520010520000310F59",
)

# Observed via eth_getLogs on 2026-07-03 from V2 CTF Exchange
# 0xE111180000d2663C0091e4f400237545B87B996B, tx
# 0x7c4081582a7728fa255a31c65c318f598d4f0f5f5d32ab30c8f8fb793a984f90.
ORDER_FILLED_TOPIC0 = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--polygon-rpc-url", default=os.getenv("POLYGON_RPC_URL", DEFAULT_HTTP_RPC))
    parser.add_argument("--polygon-wss-url", default=os.getenv("POLYGON_WSS_URL", DEFAULT_WSS_RPC))
    parser.add_argument(
        "--polygon-wss-fallback-url",
        action="append",
        default=[item for item in os.getenv("POLYGON_WSS_FALLBACK_URLS", "").split(",") if item.strip()],
        help="Extra Polygon WSS endpoints tried after --polygon-wss-url on subscribe/connect failure.",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--orderfilled-output",
        default=DEFAULT_ORDERFILLED_OUTPUT,
        help="Dedicated append-only JSONL containing decoded polygon_orderfilled_log rows only.",
    )
    parser.add_argument(
        "--orderfilled-wake-socket",
        default=DEFAULT_ORDERFILLED_WAKE_SOCKET,
        help="Best-effort Unix datagram wake-up sent after each dedicated OrderFilled append.",
    )
    parser.add_argument(
        "--orderfilled-fanout-socket",
        default="",
        help="Best-effort Unix datagram carrying the full realtime OrderFilled row.",
    )
    parser.add_argument("--report", default=DEFAULT_REPORT)
    parser.add_argument("--comparison-jsonl", default=DEFAULT_COMPARISON)
    parser.add_argument("--dataapi-first-seen-jsonl", default=DEFAULT_DATAAPI_FIRST_SEEN)
    parser.add_argument("--duration-s", type=float, default=120.0)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--http-poll-s", type=float, default=5.0)
    parser.add_argument("--ws-retry-s", type=float, default=5.0)
    parser.add_argument("--lookback-blocks", type=int, default=100)
    parser.add_argument("--address", action="append", default=[])
    parser.add_argument("--registry", action="append", default=[])
    parser.add_argument("--active-registry", default="data/research/wallet_copy_active_hotlane_registry.json")
    parser.add_argument(
        "--disable-default-registry",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Do not load configs/wallet_copy/wallets.json; use only explicit/active registries.",
    )
    parser.add_argument("--topic0", default=ORDER_FILLED_TOPIC0)
    return parser.parse_args()


def _notify_orderfilled_wake(path: str) -> bool:
    target = str(path or "").strip()
    if not target:
        return False
    client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        client.setblocking(False)
        client.sendto(b"1", target)
        return True
    except OSError:
        return False
    finally:
        client.close()


def _fanout_orderfilled(path: str, payload: dict[str, Any]) -> bool:
    target = str(path or "").strip()
    if not target:
        return False
    client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        client.setblocking(False)
        client.sendto(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            target,
        )
        return True
    except OSError:
        return False
    finally:
        client.close()


def _compact_orderfilled_fanout(payload: dict[str, Any]) -> dict[str, Any]:
    decoded = payload.get("decoded") if isinstance(payload.get("decoded"), dict) else {}
    return {
        "schema_version": 1,
        "event": "polygon_orderfilled_log",
        "source": payload.get("source"),
        "transaction_hash": payload.get("transaction_hash") or payload.get("transactionHash"),
        "log_index": payload.get("log_index"),
        "block_number": payload.get("block_number"),
        "block_ts": payload.get("block_ts"),
        "received_at_s": payload.get("received_at_s"),
        "recv_monotonic_s": payload.get("recv_monotonic_s"),
        "fanout_sent_monotonic_s": payload.get("fanout_sent_monotonic_s"),
        "maker": payload.get("maker") or decoded.get("maker"),
        "taker": payload.get("taker") or decoded.get("taker"),
        "selected_wallet": payload.get("selected_wallet"),
        "decoded": {
            key: decoded.get(key)
            for key in (
                "decode_status",
                "asset",
                "price",
                "size",
                "maker",
                "taker",
                "maker_side",
                "side",
            )
        },
    }


def _append_orderfilled(
    args: argparse.Namespace,
    row: dict[str, Any],
    *,
    realtime: bool = True,
) -> None:
    if not realtime:
        return
    output = str(getattr(args, "orderfilled_output", "") or "")
    payload = {
        "event": "polygon_orderfilled_log",
        **row,
        "sidecar_appended_at_s": time.time(),
        "fanout_sent_monotonic_s": time.monotonic(),
    }
    if output:
        append_jsonl(output, payload)
    if payload.get("is_registry_wallet") is not False:
        _fanout_orderfilled(
            str(getattr(args, "orderfilled_fanout_socket", "") or ""),
            _compact_orderfilled_fanout(payload),
        )
    _notify_orderfilled_wake(str(getattr(args, "orderfilled_wake_socket", "") or ""))


def _rpc_post(url: str, method: str, params: list[Any], *, timeout_s: float) -> Any:
    response = requests.post(
        url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=timeout_s,
        headers={"Accept": "application/json", "User-Agent": "wallet-copy-polygon-orderfilled-probe/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and payload.get("error"):
        raise RuntimeError(payload["error"])
    return payload.get("result") if isinstance(payload, dict) else None


def _norm_address(value: str) -> str:
    text = str(value or "").strip()
    if not text.startswith("0x") or len(text) != 42:
        raise ValueError(f"invalid address: {value!r}")
    return text


def _utc_iso_from_s(value: float) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def _topic_address(topic: Any) -> str:
    text = str(topic or "").lower()
    if text.startswith("0x") and len(text) == 66:
        return "0x" + text[-40:]
    return ""


def _registry_wallets(args: argparse.Namespace) -> set[str]:
    paths = [] if bool(getattr(args, "disable_default_registry", False)) else [DEFAULT_REGISTRY]
    paths.extend(args.registry or [])
    if args.active_registry:
        paths.append(args.active_registry)
    wallets: set[str] = set()
    for path in paths:
        try:
            wallets.update(spec.normalized_address() for spec in load_wallet_registry(path) if spec.enabled)
        except Exception:
            continue
    return wallets


def _hex_int(value: Any) -> int | None:
    text = str(value or "")
    if text.startswith("0x"):
        try:
            return int(text, 16)
        except ValueError:
            return None
    return None


def _block_timestamp(
    args: argparse.Namespace,
    block_number: int | None,
    cache: dict[int, float],
) -> float | None:
    if block_number is None:
        return None
    if block_number in cache:
        return cache[block_number]
    block = _rpc_post(
        args.polygon_rpc_url,
        "eth_getBlockByNumber",
        [hex(block_number), False],
        timeout_s=float(args.timeout_s),
    )
    if not isinstance(block, dict):
        return None
    timestamp = _hex_int(block.get("timestamp"))
    if timestamp is None:
        return None
    cache[block_number] = float(timestamp)
    return float(timestamp)


def _summarize_log(
    row: dict[str, Any],
    *,
    args: argparse.Namespace,
    source: str,
    registry_wallets: set[str],
    exchange_addresses: set[str],
    block_ts_cache: dict[int, float],
    received_at_s: float | None = None,
) -> dict[str, Any]:
    topics = row.get("topics") if isinstance(row.get("topics"), list) else []
    block_number = _hex_int(row.get("blockNumber"))
    block_ts_error = None
    try:
        block_ts = _block_timestamp(args, block_number, block_ts_cache)
    except Exception as exc:  # noqa: BLE001 - timestamp enrichment must not kill WS capture.
        block_ts = None
        block_ts_error = {"error_type": type(exc).__name__, "error": str(exc)}
    topic1 = str(topics[1] if len(topics) > 1 else "")
    maker = _topic_address(topics[2] if len(topics) > 2 else "")
    taker = _topic_address(topics[3] if len(topics) > 3 else "")
    registry_matches = [wallet for wallet in (maker, taker) if wallet in registry_wallets]
    selected_wallet = registry_matches[0] if registry_matches else ""
    captured_at_s = received_at_s or time.time()
    captured_at_iso = _utc_iso_from_s(captured_at_s)
    receive_lag_signed_s = None if block_ts is None else captured_at_s - block_ts
    summary: dict[str, Any] = {
        "source": source,
        "captured_at_s": captured_at_s,
        "captured_at_iso": captured_at_iso,
        "received_at_s": captured_at_s,
        "received_at_iso": captured_at_iso,
        "recv_monotonic_s": time.monotonic(),
        "address": str(row.get("address") or "").lower(),
        "transaction_hash": row.get("transactionHash"),
        "block_number": block_number,
        "block_ts": block_ts,
        "event_ts": block_ts,
        "block_ts_error": block_ts_error,
        "ws_receive_lag_signed_s": receive_lag_signed_s,
        "log_index": _hex_int(row.get("logIndex")),
        "topic0": topics[0] if topics else "",
        "topic1": topic1,
        "order_hash": topic1,
        "maker": maker,
        "taker": taker,
        "topic3": taker,
        "is_registry_wallet": bool(registry_matches),
        "registry_wallets": registry_matches,
        "selected_wallet": selected_wallet,
        "topics": topics,
        "data": row.get("data"),
    }
    summary["decoded"] = decode_polygon_orderfilled_v2(summary, exchange_addresses=exchange_addresses)
    return summary


def _raw_log_key(row: dict[str, Any]) -> tuple[str, int | None]:
    return (str(row.get("transactionHash") or ""), _hex_int(row.get("logIndex")))


def _summary_log_key(row: dict[str, Any]) -> tuple[str, int | None]:
    return (str(row.get("transaction_hash") or ""), row.get("log_index") if isinstance(row.get("log_index"), int) else None)


def _dedupe_log_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, int | None], dict[str, Any]] = {}
    seen_urls: dict[tuple[str, int | None], set[str]] = {}
    passthrough: list[dict[str, Any]] = []
    for row in rows:
        key = _summary_log_key(row)
        if not key[0]:
            passthrough.append(row)
            continue
        wss_url = str(row.get("wss_url") or "").strip()
        seen_urls.setdefault(key, set())
        if wss_url:
            seen_urls[key].add(wss_url)
        existing = by_key.get(key)
        if existing is None or float(row.get("captured_at_s") or row.get("received_at_s") or 0.0) < float(
            existing.get("captured_at_s") or existing.get("received_at_s") or 0.0
        ):
            by_key[key] = dict(row)
    deduped = passthrough + list(by_key.values())
    for row in deduped:
        key = _summary_log_key(row)
        urls = sorted(seen_urls.get(key, set()))
        if urls:
            row["wss_urls_seen"] = urls
    deduped.sort(key=lambda item: float(item.get("captured_at_s") or item.get("received_at_s") or 0.0))
    return deduped


def _append_report(path: str, rows: list[dict[str, Any]], meta: dict[str, Any]) -> None:
    report = load_json(path, default={})
    if not isinstance(report, dict):
        report = {}
    sources = report.get("sources") if isinstance(report.get("sources"), dict) else {}
    prior = sources.get("polygon_ws") if isinstance(sources.get("polygon_ws"), dict) else {}
    existing_rows = prior.get("rows") if isinstance(prior.get("rows"), list) else []
    sources["polygon_ws"] = {
        **prior,
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "meta": meta,
        "rows": (existing_rows + rows)[-500:],
    }
    report["sources"] = sources
    report["updated_at"] = utc_now_iso()
    atomic_write_json(path, report)


def _iter_jsonl(path: str) -> list[dict[str, Any]]:
    target = Path(path)
    if not path or not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    with target.open(encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _comparison_key(row: dict[str, Any]) -> tuple[str, str]:
    wallet = str(row.get("selected_wallet") or row.get("wallet") or "").strip().lower()
    tx = str(row.get("transaction_hash") or row.get("transactionHash") or "").strip().lower()
    return wallet, tx


def _dataapi_first_seen_index(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if row.get("event") != "dataapi_first_seen":
            continue
        wallet, tx = _comparison_key(row)
        if not wallet or not tx:
            continue
        prior = index.get((wallet, tx))
        if prior is None or float(row.get("captured_at_s") or 0.0) < float(prior.get("captured_at_s") or 0.0):
            index[(wallet, tx)] = row
    return index


def _comparison_rows(
    ws_rows: list[dict[str, Any]],
    *,
    dataapi_first_seen_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    dataapi_index = _dataapi_first_seen_index(dataapi_first_seen_rows)
    rows: list[dict[str, Any]] = []
    for row in ws_rows:
        wallet, tx = _comparison_key(row)
        if not wallet or not tx:
            continue
        dataapi = dataapi_index.get((wallet, tx))
        event_ts = row.get("block_ts")
        ws_ts = row.get("received_at_s")
        dataapi_ts = dataapi.get("captured_at_s") if isinstance(dataapi, dict) else None
        ws_detection_lag_s = None
        dataapi_detection_lag_s = None
        ws_lead_s = None
        if event_ts is not None and ws_ts is not None:
            ws_detection_lag_s = round(float(ws_ts) - float(event_ts), 6)
        if event_ts is not None and dataapi_ts is not None:
            dataapi_detection_lag_s = round(float(dataapi_ts) - float(event_ts), 6)
        if ws_ts is not None and dataapi_ts is not None:
            ws_lead_s = round(float(dataapi_ts) - float(ws_ts), 6)
        rows.append(
            {
                "event": "polygon_ws_dataapi_first_seen_comparison",
                "captured_at_s": time.time(),
                "captured_at_iso": utc_now_iso(),
                "wallet": wallet,
                "transaction_hash": tx,
                "event_ts": event_ts,
                "ws_ts": ws_ts,
                "dataapi_observed_ts": dataapi_ts,
                "ws_detection_lag_s": ws_detection_lag_s,
                "dataapi_detection_lag_s": dataapi_detection_lag_s,
                "ws_lead_s": ws_lead_s,
                "match_status": "MATCHED_DATAAPI_FIRST_SEEN" if dataapi is not None else "NO_DATAAPI_FIRST_SEEN_MATCH_YET",
                "poll_interval_s": dataapi.get("poll_interval_s") if isinstance(dataapi, dict) else None,
                "backfill": dataapi.get("backfill") if isinstance(dataapi, dict) else None,
                "paper_only": True,
                "live_orders_allowed": False,
            }
        )
    return rows


def _wss_endpoints(args: argparse.Namespace) -> list[str]:
    endpoints: list[str] = []
    for value in [args.polygon_wss_url, *(args.polygon_wss_fallback_url or [])]:
        text = str(value or "").strip()
        if text and text not in endpoints:
            endpoints.append(text)
    return endpoints


def _fetch_http_logs(
    args: argparse.Namespace,
    addresses: list[str],
    *,
    from_block: int,
    to_block: int | str,
    source: str,
    registry_wallets: set[str],
    exchange_addresses: set[str],
    block_ts_cache: dict[int, float],
) -> list[dict[str, Any]]:
    params = [
        {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block) if isinstance(to_block, int) else to_block,
            "address": addresses,
            "topics": [args.topic0],
        }
    ]
    logs = _rpc_post(args.polygon_rpc_url, "eth_getLogs", params, timeout_s=float(args.timeout_s))
    received_at_s = time.time()
    captured_at_iso = utc_now_iso()
    rows = [
        _summarize_log(
            row,
            args=args,
            source=source,
            registry_wallets=registry_wallets,
            exchange_addresses=exchange_addresses,
            block_ts_cache=block_ts_cache,
            received_at_s=received_at_s,
        )
        for row in logs or []
        if isinstance(row, dict)
    ]
    for row in rows:
        row["http_capture_kind"] = "tail" if source.endswith("_tail") else "seed"
        row["http_capture_from_block"] = from_block
        row["http_capture_to_block"] = to_block
        row["http_captured_at_s"] = received_at_s
        row["http_captured_at_iso"] = captured_at_iso
    return rows


def _discover_recent(
    args: argparse.Namespace,
    addresses: list[str],
    *,
    registry_wallets: set[str],
    exchange_addresses: set[str],
    block_ts_cache: dict[int, float],
) -> tuple[list[dict[str, Any]], int]:
    latest_hex = _rpc_post(args.polygon_rpc_url, "eth_blockNumber", [], timeout_s=float(args.timeout_s))
    latest = int(str(latest_hex), 16)
    from_block = max(0, latest - max(1, int(args.lookback_blocks)))
    rows = _fetch_http_logs(
        args,
        addresses,
        from_block=from_block,
        to_block=latest,
        source="polygon_http_getLogs_seed",
        registry_wallets=registry_wallets,
        exchange_addresses=exchange_addresses,
        block_ts_cache=block_ts_cache,
    )
    return rows, latest


def _poll_http_tail(
    args: argparse.Namespace,
    addresses: list[str],
    *,
    last_seen_block: int | None,
    seen_log_keys: set[tuple[str, int | None]],
    registry_wallets: set[str],
    exchange_addresses: set[str],
    block_ts_cache: dict[int, float],
) -> tuple[list[dict[str, Any]], int | None]:
    latest_hex = _rpc_post(args.polygon_rpc_url, "eth_blockNumber", [], timeout_s=float(args.timeout_s))
    latest = int(str(latest_hex), 16)
    if last_seen_block is None:
        return [], latest
    from_block = int(last_seen_block) + 1
    if latest < from_block:
        return [], latest
    rows = _fetch_http_logs(
        args,
        addresses,
        from_block=from_block,
        to_block=latest,
        source="polygon_http_getLogs_tail",
        registry_wallets=registry_wallets,
        exchange_addresses=exchange_addresses,
        block_ts_cache=block_ts_cache,
    )
    deduped: list[dict[str, Any]] = []
    for row in rows:
        key = _summary_log_key(row)
        if key in seen_log_keys:
            continue
        seen_log_keys.add(key)
        deduped.append(row)
    return deduped, latest


def _subscribe_ws(
    args: argparse.Namespace,
    addresses: list[str],
    *,
    registry_wallets: set[str],
    exchange_addresses: set[str],
    block_ts_cache: dict[int, float],
    emit_orderfilled: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    import websocket

    ws = websocket.create_connection(args.polygon_wss_url, timeout=float(args.timeout_s))
    connected_at_s = time.time()
    append_jsonl(
        args.output,
        {
            "event": "polygon_ws_connection_open",
            "captured_at_s": connected_at_s,
            "captured_at_iso": utc_now_iso(),
            "wss_url": args.polygon_wss_url,
            "addresses": [address.lower() for address in addresses],
        },
    )
    ws.settimeout(min(5.0, max(1.0, float(args.timeout_s))))
    timeout_errors: tuple[type[BaseException], ...]
    ws_timeout_error = getattr(websocket, "WebSocketTimeoutException", None)
    if isinstance(ws_timeout_error, type) and issubclass(ws_timeout_error, BaseException):
        timeout_errors = (TimeoutError, ws_timeout_error)
    else:
        timeout_errors = (TimeoutError,)
    log_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": ["logs", {"address": addresses, "topics": [args.topic0]}],
    }
    head_payload = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "eth_subscribe",
        "params": ["newHeads"],
    }
    ws.send(json.dumps(log_payload))
    ws.send(json.dumps(head_payload))
    rows: list[dict[str, Any]] = []
    first_frame_s: float | None = None
    last_frame_s: float | None = None
    max_frame_gap_s = 0.0
    frame_count = 0
    deadline = time.time() + max(1.0, float(args.duration_s))
    try:
        while time.time() < deadline:
            try:
                raw = ws.recv()
            except timeout_errors:
                continue
            received_at_s = time.time()
            frame_count += 1
            if first_frame_s is None:
                first_frame_s = received_at_s
            if last_frame_s is not None:
                max_frame_gap_s = max(max_frame_gap_s, received_at_s - last_frame_s)
            last_frame_s = received_at_s
            append_jsonl(
                args.output,
                {
                    "event": "polygon_ws_raw_frame",
                    "captured_at_s": received_at_s,
                    "captured_at_iso": utc_now_iso(),
                    "raw": raw,
                },
            )
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(message, dict) and message.get("id") in {1, 2}:
                append_jsonl(
                    args.output,
                    {
                        "event": "polygon_ws_subscription_ack",
                        "captured_at_s": received_at_s,
                        "captured_at_iso": utc_now_iso(),
                        "message": message,
                    },
                )
                continue
            params = message.get("params") if isinstance(message, dict) else None
            result = params.get("result") if isinstance(params, dict) else None
            if isinstance(result, dict):
                head_number = _hex_int(result.get("number"))
                head_timestamp = _hex_int(result.get("timestamp"))
                if not isinstance(result.get("topics"), list) and head_number is not None and head_timestamp is not None:
                    block_ts_cache[head_number] = float(head_timestamp)
                    append_jsonl(
                        args.output,
                        {
                            "event": "polygon_ws_new_head",
                            "captured_at_s": received_at_s,
                            "captured_at_iso": utc_now_iso(),
                            "block_number": head_number,
                            "block_ts": float(head_timestamp),
                        },
                    )
                    continue
                row = _summarize_log(
                    result,
                    args=args,
                    source="polygon_ws",
                    registry_wallets=registry_wallets,
                    exchange_addresses=exchange_addresses,
                    block_ts_cache=block_ts_cache,
                    received_at_s=received_at_s,
                )
                row["wss_url"] = args.polygon_wss_url
                rows.append(row)
                orderfilled_output = str(getattr(args, "orderfilled_output", "") or "")
                if orderfilled_output:
                    _append_orderfilled(args, row)
                if emit_orderfilled:
                    append_jsonl(args.output, {"event": "polygon_orderfilled_log", **row})
    finally:
        ws.close()
        append_jsonl(
            args.output,
            {
                "event": "polygon_ws_connection_close",
                "captured_at_s": time.time(),
                "captured_at_iso": utc_now_iso(),
                "wss_url": args.polygon_wss_url,
                "rows": len(rows),
                "frames": frame_count,
                "first_frame_s": first_frame_s,
                "last_frame_s": last_frame_s,
                "max_frame_gap_s": max_frame_gap_s,
            },
        )
    return rows, {
        "connections": 1,
        "frames": frame_count,
        "first_frame_s": first_frame_s,
        "last_frame_s": last_frame_s,
        "max_frame_gap_s": max_frame_gap_s,
    }


def _subscribe_ws_union_slice(
    args: argparse.Namespace,
    addresses: list[str],
    *,
    registry_wallets: set[str],
    exchange_addresses: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    endpoints = _wss_endpoints(args)
    stats_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    collected: list[dict[str, Any]] = []
    max_workers = max(1, len(endpoints))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures: dict[concurrent.futures.Future[tuple[list[dict[str, Any]], dict[str, Any]]], str] = {}
        for endpoint in endpoints:
            attempt_args = argparse.Namespace(**vars(args))
            attempt_args.polygon_wss_url = endpoint
            futures[
                executor.submit(
                    _subscribe_ws,
                    attempt_args,
                    addresses,
                    registry_wallets=registry_wallets,
                    exchange_addresses=exchange_addresses,
                    block_ts_cache={},
                    emit_orderfilled=False,
                )
            ] = endpoint
        for future in concurrent.futures.as_completed(futures):
            endpoint = futures[future]
            try:
                rows, stats = future.result()
                stats["wss_url"] = endpoint
                stats_rows.append(stats)
                collected.extend(rows)
            except Exception as exc:  # noqa: BLE001 - network probe evidence.
                errors.append(
                    {
                        "stage": "ws_subscribe_endpoint",
                        "wss_url": endpoint,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "failed_at_s": time.time(),
                    }
                )
    return _dedupe_log_rows(collected), stats_rows, errors


def main() -> int:
    args = parse_args()
    addresses = [_norm_address(item) for item in (args.address or [])]
    if not addresses:
        addresses = [_norm_address(item) for item in (*FABLE_EXCHANGE_ADDRESSES, *CURRENT_V2_EXCHANGE_ADDRESSES)]
    exchange_addresses = {address.lower() for address in addresses}
    registry_wallets = _registry_wallets(args)
    block_ts_cache: dict[int, float] = {}

    http_rows: list[dict[str, Any]] = []
    http_seed_rows: list[dict[str, Any]] = []
    http_tail_rows: list[dict[str, Any]] = []
    ws_rows: list[dict[str, Any]] = []
    ws_connection_stats: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    latest_block = None
    last_seen_block = None
    seen_log_keys: set[tuple[str, int | None]] = set()
    try:
        http_seed_rows, latest_block = _discover_recent(
            args,
            addresses,
            registry_wallets=registry_wallets,
            exchange_addresses=exchange_addresses,
            block_ts_cache=block_ts_cache,
        )
        http_rows.extend(http_seed_rows)
        last_seen_block = latest_block
        seen_log_keys.update(_summary_log_key(row) for row in http_seed_rows)
        for row in http_seed_rows[:100]:
            append_jsonl(args.output, {"event": "polygon_orderfilled_log", **row})
            if args.orderfilled_output:
                _append_orderfilled(args, row, realtime=False)
    except Exception as exc:  # noqa: BLE001 - network probe evidence.
        errors.append({"stage": "http_getLogs", "error_type": type(exc).__name__, "error": str(exc)})

    ws_deadline = time.time() + max(1.0, float(args.duration_s))
    next_http_poll_s = time.time() + max(1.0, float(args.http_poll_s))
    while time.time() < ws_deadline:
        now = time.time()
        if now >= next_http_poll_s:
            try:
                new_tail_rows, latest_block = _poll_http_tail(
                    args,
                    addresses,
                    last_seen_block=last_seen_block,
                    seen_log_keys=seen_log_keys,
                    registry_wallets=registry_wallets,
                    exchange_addresses=exchange_addresses,
                    block_ts_cache=block_ts_cache,
                )
                last_seen_block = latest_block
                http_tail_rows.extend(new_tail_rows)
                http_rows.extend(new_tail_rows)
                for row in new_tail_rows:
                    append_jsonl(args.output, {"event": "polygon_orderfilled_log", **row})
                    if args.orderfilled_output:
                        _append_orderfilled(args, row, realtime=False)
            except Exception as exc:  # noqa: BLE001 - network probe evidence.
                errors.append({"stage": "http_getLogs_tail", "error_type": type(exc).__name__, "error": str(exc)})
            next_http_poll_s = time.time() + max(1.0, float(args.http_poll_s))
        attempt_args = argparse.Namespace(**vars(args))
        attempt_args.duration_s = min(max(1.0, float(args.http_poll_s)), max(1.0, ws_deadline - time.time()))
        attempt_args.timeout_s = min(max(1.0, float(args.http_poll_s)), max(1.0, float(args.timeout_s)))
        new_rows, stats_rows, endpoint_errors = _subscribe_ws_union_slice(
            attempt_args,
            addresses,
            registry_wallets=registry_wallets,
            exchange_addresses=exchange_addresses,
        )
        errors.extend(endpoint_errors)
        ws_rows.extend(new_rows)
        ws_connection_stats.extend(stats_rows)
        for row in new_rows:
            append_jsonl(args.output, {"event": "polygon_orderfilled_log", **row})
        if not stats_rows and endpoint_errors:
            exc = endpoint_errors[-1]
            failed_at_s = time.time()
            retry_delay_s = max(0.0, float(args.ws_retry_s)) * (2 ** min(len(errors), 4))
            retry_at_s = failed_at_s + retry_delay_s
            errors.append(
                {
                    "stage": "ws_subscribe",
                    "error_type": exc.get("error_type"),
                    "error": exc.get("error"),
                    "failed_at_s": failed_at_s,
                    "retry_delay_s": retry_delay_s,
                    "retry_at_s": retry_at_s,
                }
            )
            append_jsonl(
                args.output,
                {
                    "event": "polygon_ws_connection_error",
                    "captured_at_s": failed_at_s,
                    "captured_at_iso": utc_now_iso(),
                    "wss_urls": _wss_endpoints(args),
                    "error_type": exc.get("error_type"),
                    "error": exc.get("error"),
                    "retry_delay_s": retry_delay_s,
                    "retry_at_s": retry_at_s,
                },
            )
            if retry_at_s >= ws_deadline:
                break
            time.sleep(min(retry_delay_s, max(0.0, next_http_poll_s - time.time()), max(1.0, float(args.http_poll_s))))

    ws_max_frame_gap_s = max((float(item.get("max_frame_gap_s") or 0.0) for item in ws_connection_stats), default=0.0)

    meta = {
        "rpc_url": args.polygon_rpc_url,
        "wss_url": args.polygon_wss_url,
        "wss_mode": "concurrent_union",
        "addresses": [address.lower() for address in addresses],
        "topic0": args.topic0,
        "latest_block": latest_block,
        "lookback_blocks": int(args.lookback_blocks),
        "http_rows": len(http_rows),
        "http_seed_rows": len(http_seed_rows),
        "http_tail_rows": len(http_tail_rows),
        "ws_rows": len(ws_rows),
        "registry_wallets_loaded": len(registry_wallets),
        "http_registry_rows": sum(1 for row in http_rows if row.get("is_registry_wallet")),
        "http_seed_registry_rows": sum(1 for row in http_seed_rows if row.get("is_registry_wallet")),
        "http_tail_registry_rows": sum(1 for row in http_tail_rows if row.get("is_registry_wallet")),
        "ws_registry_rows": sum(1 for row in ws_rows if row.get("is_registry_wallet")),
        "block_timestamp_cache_entries": len(block_ts_cache),
        "errors": errors,
        "ws_retry_s": float(args.ws_retry_s),
        "ws_connection_count": len(ws_connection_stats),
        "ws_frame_count": sum(int(item.get("frames") or 0) for item in ws_connection_stats),
        "ws_max_frame_gap_s": ws_max_frame_gap_s,
    }
    comparison_rows = _comparison_rows(
        ws_rows,
        dataapi_first_seen_rows=_iter_jsonl(str(getattr(args, "dataapi_first_seen_jsonl", ""))),
    )
    if comparison_rows:
        append_jsonl_many(args.comparison_jsonl, comparison_rows)
    meta["comparison_rows"] = len(comparison_rows)
    meta["comparison_jsonl"] = args.comparison_jsonl
    meta["wss_urls"] = _wss_endpoints(args)
    _append_report(args.report, ws_rows or http_tail_rows or http_seed_rows[:50], meta)
    summary = {
        "event": "polygon_orderfilled_probe_summary",
        "captured_at_s": time.time(),
        "captured_at_iso": utc_now_iso(),
        **meta,
        "output": args.output,
        "orderfilled_output": args.orderfilled_output,
        "report": args.report,
    }
    append_jsonl(args.output, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if ws_rows or ((http_rows or ws_rows) and not any(err.get("stage") == "ws_subscribe" for err in errors)) else 3


if __name__ == "__main__":
    raise SystemExit(main())
