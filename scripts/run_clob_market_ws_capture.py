#!/usr/bin/env python3
"""Capture public CLOB market WebSocket rows for preconfirm correlation.

Market-channel rows are fast but not wallet-attributed. The live tracker can
later match these rows against wallet Data API truth with
``--market-ws-jsonl``.
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import append_jsonl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset-id", action="append", default=[], help="CLOB token/asset id to subscribe to")
    parser.add_argument("--asset-ids-file", default="", help="JSON/JSONL/text file with token ids")
    parser.add_argument("--output", default="data/research/clob_market_ws_events.jsonl")
    parser.add_argument("--duration-s", type=float, default=60.0)
    parser.add_argument("--url", default="wss://ws-subscriptions-clob.polymarket.com/ws/market")
    parser.add_argument("--custom-feature-enabled", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--ssl-no-verify",
        action="store_true",
        help="Connect without TLS certificate verification. Intended only for public market-data diagnostics.",
    )
    parser.add_argument(
        "--ssl-no-verify-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Retry without TLS certificate verification when the local CA bundle rejects the public market-data WS cert.",
    )
    return parser.parse_args()


def _ids_from_file(path: str) -> list[str]:
    if not path:
        return []
    target = Path(path)
    if not target.exists():
        return []
    text = target.read_text(encoding="utf-8")
    ids: list[str] = []
    try:
        payload = json.loads(text)
        if isinstance(payload, list):
            ids.extend(str(item) for item in payload)
        elif isinstance(payload, dict):
            for key in ("asset_ids", "assets_ids", "clob_token_ids", "token_ids"):
                value = payload.get(key)
                if isinstance(value, list):
                    ids.extend(str(item) for item in value)
    except json.JSONDecodeError:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if isinstance(row, dict):
                    ids.extend(str(row.get(key)) for key in ("asset_id", "token_id", "asset") if row.get(key))
                    continue
            except json.JSONDecodeError:
                pass
            ids.append(line)
    return ids


def _write_rows(output: str, payload: Any) -> None:
    rows = payload if isinstance(payload, list) else [payload]
    for row in rows:
        if isinstance(row, dict):
            append_jsonl(output, {"captured_at_s": time.time(), **row})


def _is_cert_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return isinstance(exc, ssl.SSLError) or "certificate_verify_failed" in text or "cert" in text


def _connect_market_ws(
    websocket_module: Any,
    *,
    url: str,
    output: str,
    ssl_no_verify: bool,
    ssl_no_verify_fallback: bool,
) -> tuple[Any, str]:
    attempts: list[tuple[str, dict[str, Any]]] = []
    if ssl_no_verify:
        attempts.append(("ssl_no_verify", {"sslopt": {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}}))
    else:
        attempts.append(("verified", {}))
        if ssl_no_verify_fallback:
            attempts.append(("ssl_no_verify_fallback", {"sslopt": {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}}))

    errors: list[dict[str, str]] = []
    for index, (mode, kwargs) in enumerate(attempts):
        try:
            ws = websocket_module.create_connection(url, timeout=10, **kwargs)
            if errors or mode != "verified":
                append_jsonl(
                    output,
                    {
                        "captured_at_s": time.time(),
                        "event": "clob_market_ws_connection",
                        "ssl_mode": mode,
                        "fallback_errors": errors,
                    },
                )
            return ws, mode
        except Exception as exc:
            errors.append({"ssl_mode": mode, "error_type": type(exc).__name__, "error": str(exc)})
            is_last = index == len(attempts) - 1
            if is_last or not _is_cert_error(exc):
                append_jsonl(
                    output,
                    {
                        "captured_at_s": time.time(),
                        "event": "clob_market_ws_connection_failed",
                        "ssl_mode": mode,
                        "fallback_errors": errors,
                    },
                )
                raise

    raise RuntimeError("unreachable websocket connection attempt state")


def main() -> int:
    args = parse_args()
    try:
        import websocket
    except ImportError as exc:
        raise SystemExit("websocket-client is required; install requirements.txt") from exc

    asset_ids = sorted({str(item) for item in [*args.asset_id, *_ids_from_file(args.asset_ids_file)] if str(item)})
    if not asset_ids:
        raise SystemExit("at least one --asset-id or --asset-ids-file token id is required")

    deadline = time.time() + max(1.0, float(args.duration_s))
    count = 0
    ws, ssl_mode = _connect_market_ws(
        websocket,
        url=args.url,
        output=args.output,
        ssl_no_verify=bool(args.ssl_no_verify),
        ssl_no_verify_fallback=bool(args.ssl_no_verify_fallback),
    )
    try:
        ws.send(
            json.dumps(
                {
                    "assets_ids": asset_ids,
                    "type": "market",
                    "custom_feature_enabled": bool(args.custom_feature_enabled),
                }
            )
        )
        while time.time() < deadline:
            try:
                raw = ws.recv()
            except TimeoutError:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                append_jsonl(args.output, {"captured_at_s": time.time(), "raw": raw})
                count += 1
                continue
            before = count
            if isinstance(payload, list):
                count += sum(1 for item in payload if isinstance(item, dict))
            elif isinstance(payload, dict):
                count += 1
            _write_rows(args.output, payload)
            if count == before:
                count += 1
    finally:
        ws.close()
    print(
        json.dumps(
            {"output": args.output, "asset_ids": len(asset_ids), "rows": count, "ssl_mode": ssl_mode},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
