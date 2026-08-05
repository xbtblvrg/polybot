#!/usr/bin/env python3
"""Capture raw Polymarket realtime activity WebSocket frames.

This probe is deliberately read-only. It does not build CopyIntents, mutate
paper/live ledgers, or affect live gates. The goal is to pin down the public
RTDS activity payload shape before implementing the realtime parser.
"""

from __future__ import annotations

import argparse
import json
import re
import ssl
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import append_jsonl


DEFAULT_URL = "wss://ws-live-data.polymarket.com"
DEFAULT_OUTPUT = "data/research/rtds_activity_raw_capture.jsonl"


SUBSCRIPTION_VARIANTS: tuple[tuple[str, Any], ...] = (
    (
        "activity_trades",
        {"action": "subscribe", "subscriptions": [{"topic": "activity", "type": "trades"}]},
    ),
    (
        "activity_trades_empty_filters",
        {"action": "subscribe", "subscriptions": [{"topic": "activity", "type": "trades", "filters": ""}]},
    ),
    (
        "activity_wildcard",
        {"action": "subscribe", "subscriptions": [{"topic": "activity", "type": "*"}]},
    ),
    (
        "activity_channel",
        {"type": "subscribe", "channel": "activity"},
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--duration-s", type=float, default=900.0)
    parser.add_argument("--variant-silence-s", type=float, default=60.0)
    parser.add_argument("--ping-s", type=float, default=10.0)
    parser.add_argument("--connect-timeout-s", type=float, default=10.0)
    parser.add_argument("--recv-timeout-s", type=float, default=5.0)
    parser.add_argument("--origin", default="https://polymarket.com")
    parser.add_argument("--http-proxy-host", default="")
    parser.add_argument("--http-proxy-port", type=int, default=0)
    parser.add_argument("--proxy-type", default="http")
    parser.add_argument(
        "--user-agent",
        default="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) polymarket-wallet-copy-rtds-probe/1.0",
    )
    parser.add_argument("--ssl-no-verify", action="store_true")
    parser.add_argument(
        "--ssl-no-verify-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Retry without TLS verification when the local CA bundle rejects the public RTDS cert.",
    )
    parser.add_argument(
        "--discover-js-on-silence",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fetch polymarket.com JS bundles and log snippets around ws-live-data if all variants stay silent.",
    )
    parser.add_argument("--site-url", default="https://polymarket.com")
    parser.add_argument(
        "--jina-discovery-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When direct Polymarket HTTPS resets, fetch read-only discovery pages through Jina Reader.",
    )
    parser.add_argument("--max-js-bundles", type=int, default=24)
    return parser.parse_args()


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int((time.time() % 1) * 1_000_000):06d}Z"


def _ssl_attempts(*, ssl_no_verify: bool, ssl_no_verify_fallback: bool) -> list[tuple[str, dict[str, Any]]]:
    no_verify = {"sslopt": {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}}
    if ssl_no_verify:
        return [("ssl_no_verify", no_verify)]
    attempts: list[tuple[str, dict[str, Any]]] = [("verified", {})]
    if ssl_no_verify_fallback:
        attempts.append(("ssl_no_verify_fallback", no_verify))
    return attempts


def _is_cert_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return isinstance(exc, ssl.SSLError) or "certificate_verify_failed" in text or "cert" in text


def _connect(websocket_module: Any, args: argparse.Namespace) -> tuple[Any, str]:
    errors: list[dict[str, str]] = []
    proxy_kwargs: dict[str, Any] = {}
    if args.http_proxy_host and int(args.http_proxy_port or 0) > 0:
        proxy_kwargs = {
            "http_proxy_host": args.http_proxy_host,
            "http_proxy_port": int(args.http_proxy_port),
            "proxy_type": args.proxy_type,
        }
    for index, (ssl_mode, kwargs) in enumerate(
        _ssl_attempts(ssl_no_verify=bool(args.ssl_no_verify), ssl_no_verify_fallback=bool(args.ssl_no_verify_fallback))
    ):
        try:
            headers = [f"User-Agent: {args.user_agent}"]
            ws = websocket_module.create_connection(
                args.url,
                timeout=float(args.connect_timeout_s),
                origin=args.origin,
                header=headers,
                **proxy_kwargs,
                **kwargs,
            )
            try:
                ws.settimeout(float(args.recv_timeout_s))
            except Exception:
                pass
            append_jsonl(
                args.output,
                {
                    "event": "rtds_connection",
                    "captured_at_s": time.time(),
                    "captured_at_iso": _utc_now_iso(),
                    "url": args.url,
                    "ssl_mode": ssl_mode,
                    "proxy": proxy_kwargs,
                    "fallback_errors": errors,
                },
            )
            return ws, ssl_mode
        except Exception as exc:
            errors.append({"ssl_mode": ssl_mode, "error_type": type(exc).__name__, "error": str(exc)})
            is_last = index == len(
                _ssl_attempts(
                    ssl_no_verify=bool(args.ssl_no_verify),
                    ssl_no_verify_fallback=bool(args.ssl_no_verify_fallback),
                )
            ) - 1
            if is_last:
                append_jsonl(
                    args.output,
                    {
                        "event": "rtds_connection_failed",
                        "captured_at_s": time.time(),
                        "captured_at_iso": _utc_now_iso(),
                        "url": args.url,
                        "ssl_mode": ssl_mode,
                        "proxy": proxy_kwargs,
                        "fallback_errors": errors,
                    },
                )
                raise
    raise RuntimeError("unreachable RTDS connection attempt state")


def _data_frame_like(raw: str) -> bool:
    text = raw.strip()
    if not text:
        return False
    lowered = text.lower()
    if lowered in {"pong", "ping"}:
        return False
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return True
    if isinstance(payload, dict):
        event = str(payload.get("event") or payload.get("type") or payload.get("action") or "").lower()
        if event in {"pong", "ping", "subscribed", "subscription", "ack", "connected", "heartbeat"}:
            return False
        return True
    return isinstance(payload, list) and bool(payload)


def _record_raw_frame(output: str, *, raw: str, variant: str, ssl_mode: str, frame_index: int) -> bool:
    captured_at_s = time.time()
    row: dict[str, Any] = {
        "event": "rtds_raw_frame",
        "captured_at_s": captured_at_s,
        "captured_at_iso": _utc_now_iso(),
        "recv_monotonic_s": time.monotonic(),
        "subscription_variant": variant,
        "ssl_mode": ssl_mode,
        "frame_index": frame_index,
        "raw": raw,
    }
    try:
        payload = json.loads(raw)
        row["json"] = payload
        row["json_type"] = type(payload).__name__
    except json.JSONDecodeError:
        row["json_type"] = "raw_text"
    data_like = _data_frame_like(raw)
    row["data_frame_like"] = data_like
    append_jsonl(output, row)
    return data_like


def _send_ping(ws: Any, output: str, *, variant: str) -> None:
    for payload in ({"action": "ping"}, "PING"):
        try:
            text = json.dumps(payload) if isinstance(payload, dict) else payload
            ws.send(text)
            append_jsonl(
                output,
                {
                    "event": "rtds_ping_sent",
                    "captured_at_s": time.time(),
                    "captured_at_iso": _utc_now_iso(),
                    "subscription_variant": variant,
                    "payload": payload,
                },
            )
        except Exception as exc:
            append_jsonl(
                output,
                {
                    "event": "rtds_ping_error",
                    "captured_at_s": time.time(),
                    "captured_at_iso": _utc_now_iso(),
                    "subscription_variant": variant,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )


def _extract_js_urls(html: str, base_url: str) -> list[str]:
    urls: list[str] = []
    patterns = [
        r'<script[^>]+src=["\']([^"\']+\.js(?:\?[^"\']*)?)["\']',
        r'["\']([^"\']+/_next/static/[^"\']+\.js(?:\?[^"\']*)?)["\']',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, html):
            url = urljoin(base_url, match.group(1))
            if url not in urls:
                urls.append(url)
    return urls


def _snippet(text: str, index: int, *, width: int = 900) -> str:
    start = max(0, index - width)
    end = min(len(text), index + width)
    return text[start:end]


def _jina_reader_url(url: str) -> str:
    return f"https://r.jina.ai/http://{url}"


def _safe_get_text(session: requests.Session, url: str, *, timeout_s: float = 15.0) -> tuple[str | None, dict[str, Any]]:
    started = time.time()
    try:
        response = session.get(url, timeout=timeout_s)
        elapsed_ms = round((time.time() - started) * 1000.0, 3)
        info: dict[str, Any] = {
            "url": url,
            "elapsed_ms": elapsed_ms,
            "http_status": response.status_code,
            "bytes": len(response.content),
        }
        response.raise_for_status()
        return response.text, info
    except requests.RequestException as exc:
        elapsed_ms = round((time.time() - started) * 1000.0, 3)
        return None, {
            "url": url,
            "elapsed_ms": elapsed_ms,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def discover_js_subscription_shape(args: argparse.Namespace) -> dict[str, Any]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": args.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    result: dict[str, Any] = {
        "event": "rtds_js_discovery",
        "captured_at_s": time.time(),
        "captured_at_iso": _utc_now_iso(),
        "site_url": args.site_url,
        "bundle_count": 0,
        "matches": [],
        "fetch_attempts": [],
        "jina_fallback_used": False,
        "rtds_http_probe": None,
    }
    html, direct_info = _safe_get_text(session, args.site_url)
    direct_info["role"] = "direct_site_html"
    result["fetch_attempts"].append(direct_info)
    if html is None and bool(args.jina_discovery_fallback):
        jina_url = _jina_reader_url(args.site_url)
        html, jina_info = _safe_get_text(session, jina_url, timeout_s=25.0)
        jina_info["role"] = "jina_site_markdown"
        result["fetch_attempts"].append(jina_info)
        result["jina_fallback_used"] = True

    parsed_url = urlsplit(args.url)
    if bool(args.jina_discovery_fallback) and parsed_url.hostname:
        rtds_scheme = "https" if parsed_url.scheme == "wss" else parsed_url.scheme or "https"
        rtds_http_url = f"{rtds_scheme}://{parsed_url.netloc}{parsed_url.path or ''}"
        _, rtds_info = _safe_get_text(session, _jina_reader_url(rtds_http_url), timeout_s=25.0)
        rtds_info["role"] = "jina_rtds_http_upgrade_probe"
        result["rtds_http_probe"] = rtds_info

    if html is None:
        result["error_type"] = direct_info.get("error_type")
        result["error"] = direct_info.get("error")
        append_jsonl(args.output, result)
        return result

    urls = _extract_js_urls(html, args.site_url)[: max(1, int(args.max_js_bundles))]
    result["bundle_count"] = len(urls)
    for url in urls:
        try:
            bundle = session.get(url, timeout=15)
            bundle.raise_for_status()
        except requests.RequestException:
            continue
        text = bundle.text
        lowered = text.lower()
        indexes = [
            idx
            for needle in ("ws-live-data", "subscriptions", "activity", "subscribe")
            for idx in [lowered.find(needle)]
            if idx >= 0
        ]
        if not indexes:
            continue
        first = min(indexes)
        match = {"url": url, "snippet": _snippet(text, first)}
        result.setdefault("matches", []).append(match)
        if len(result["matches"]) >= 8:
            break
    append_jsonl(args.output, result)
    return result


def main() -> int:
    args = parse_args()
    try:
        import websocket
    except ImportError as exc:
        raise SystemExit("websocket-client is required; install requirements.txt") from exc

    deadline = time.time() + max(1.0, float(args.duration_s))
    frame_count = 0
    data_frame_count = 0
    variant_results: list[dict[str, Any]] = []
    ssl_mode = ""

    for variant_name, subscription in SUBSCRIPTION_VARIANTS:
        if time.time() >= deadline:
            break
        variant_deadline = min(deadline, time.time() + max(1.0, float(args.variant_silence_s)))
        try:
            ws, ssl_mode = _connect(websocket, args)
        except Exception as exc:
            variant_results.append(
                {
                    "variant": variant_name,
                    "data_frames": 0,
                    "subscription": subscription,
                    "connection_error_type": type(exc).__name__,
                    "connection_error": str(exc),
                }
            )
            continue
        variant_data_frames = 0
        next_ping = time.time()
        append_jsonl(
            args.output,
            {
                "event": "rtds_subscription_sent",
                "captured_at_s": time.time(),
                "captured_at_iso": _utc_now_iso(),
                "subscription_variant": variant_name,
                "payload": subscription,
            },
        )
        try:
            ws.send(json.dumps(subscription))
            while time.time() < variant_deadline:
                if time.time() >= next_ping:
                    _send_ping(ws, args.output, variant=variant_name)
                    next_ping = time.time() + max(1.0, float(args.ping_s))
                try:
                    raw = ws.recv()
                except TimeoutError:
                    continue
                except Exception as exc:
                    append_jsonl(
                        args.output,
                        {
                            "event": "rtds_recv_error",
                            "captured_at_s": time.time(),
                            "captured_at_iso": _utc_now_iso(),
                            "subscription_variant": variant_name,
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        },
                    )
                    break
                frame_count += 1
                if _record_raw_frame(
                    args.output,
                    raw=str(raw),
                    variant=variant_name,
                    ssl_mode=ssl_mode,
                    frame_index=frame_count,
                ):
                    data_frame_count += 1
                    variant_data_frames += 1
            variant_results.append(
                {
                    "variant": variant_name,
                    "data_frames": variant_data_frames,
                    "subscription": subscription,
                }
            )
        finally:
            try:
                ws.close()
            except Exception:
                pass
        if variant_data_frames > 0:
            break

    discovery: dict[str, Any] | None = None
    if data_frame_count <= 0 and bool(args.discover_js_on_silence):
        discovery = discover_js_subscription_shape(args)

    summary = {
        "output": args.output,
        "url": args.url,
        "ssl_mode": ssl_mode,
        "proxy": {
            "http_proxy_host": args.http_proxy_host,
            "http_proxy_port": int(args.http_proxy_port or 0),
            "proxy_type": args.proxy_type,
        },
        "frames": frame_count,
        "data_frames": data_frame_count,
        "variant_results": variant_results,
        "js_discovery_matches": len(discovery.get("matches", [])) if isinstance(discovery, dict) else None,
    }
    append_jsonl(
        args.output,
        {
            "event": "rtds_probe_summary",
            "captured_at_s": time.time(),
            "captured_at_iso": _utc_now_iso(),
            **summary,
        },
    )
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0 if data_frame_count > 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
