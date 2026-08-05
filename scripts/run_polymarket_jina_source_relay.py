#!/usr/bin/env python3
"""Local read-only Polymarket source relay through Jina Reader.

This is a bounded recovery route for public Data API, Gamma, and CLOB reads
when the local direct Polymarket TLS route resets. It never proxies writes and
is intended only for wallet-copy paper/proof measurement.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit, urlunsplit

import requests


DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 8787
DEFAULT_TIMEOUT_S = 25.0
DEFAULT_CACHE_TTL_S = 2.0
DEFAULT_MAX_BYTES = 16_000_000
DEFAULT_UPSTREAM_RETRIES = 6
DEFAULT_UPSTREAM_MIN_INTERVAL_S = 1.25
DEFAULT_MAX_UPSTREAM_IN_FLIGHT = 4
DEFAULT_BUSY_TIMEOUT_S = 0.75
JINA_READER_PREFIX = "https://r.jina.ai/http://"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 Chrome/126 Safari/537.36"
)


@dataclass(frozen=True)
class RelayTarget:
    prefix: str
    upstream_base: str


TARGETS = (
    RelayTarget("/data-api", "https://data-api.polymarket.com"),
    RelayTarget("/gamma-api", "https://gamma-api.polymarket.com"),
    RelayTarget("/clob", "https://clob.polymarket.com"),
)


_CACHE: dict[str, tuple[float, bytes, str]] = {}
_UPSTREAM_LOCK = threading.Lock()
_UPSTREAM_NEXT_REQUEST_AT = 0.0
_UPSTREAM_BACKOFF_UNTIL = 0.0


def _path_class(path: str) -> str:
    for target in TARGETS:
        if path == target.prefix or path.startswith(f"{target.prefix}/"):
            return path.split("?", 1)[0]
    return path.split("?", 1)[0] or "unknown"


def _target_log_meta(target_url: str) -> dict[str, Any]:
    parsed = urlsplit(target_url)
    query_keys = sorted({part.split("=", 1)[0] for part in parsed.query.split("&") if part})
    return {
        "target_host": parsed.netloc,
        "target_path": parsed.path or "/",
        "query_param_keys": query_keys,
    }


def _percentile(values: list[float], pct: float) -> float:
    clean = sorted(value for value in values if value >= 0)
    if not clean:
        return 0.0
    if len(clean) == 1:
        return round(clean[0], 6)
    rank = (len(clean) - 1) * pct
    lower = int(rank)
    upper = min(len(clean) - 1, lower + 1)
    if lower == upper:
        return round(clean[lower], 6)
    return round(clean[lower] + (clean[upper] - clean[lower]) * (rank - lower), 6)


def _record_path_metric(server: Any, path_class: str, outcome: str, elapsed_s: float) -> None:
    metrics_lock = getattr(server, "relay_metrics_lock", None)
    metrics = getattr(server, "relay_path_metrics", None)
    if metrics_lock is None or metrics is None:
        return
    with metrics_lock:
        bucket = metrics.setdefault(path_class, {})
        outcome_bucket = bucket.setdefault(outcome, {"count": 0, "latencies_s": []})
        outcome_bucket["count"] = int(outcome_bucket.get("count") or 0) + 1
        latencies = outcome_bucket.setdefault("latencies_s", [])
        if isinstance(latencies, list) and elapsed_s >= 0:
            latencies.append(round(float(elapsed_s), 6))
            del latencies[:-200]


def _path_metrics_snapshot(server: Any) -> dict[str, Any]:
    metrics_lock = getattr(server, "relay_metrics_lock", None)
    metrics = getattr(server, "relay_path_metrics", None)
    if metrics_lock is None or metrics is None:
        return {}
    snapshot: dict[str, Any] = {}
    with metrics_lock:
        for path_class, bucket in metrics.items():
            path_outcomes: dict[str, Any] = {}
            for outcome, values in bucket.items():
                latencies = [float(value) for value in values.get("latencies_s", []) if isinstance(value, (int, float))]
                path_outcomes[outcome] = {
                    "count": int(values.get("count") or 0),
                    "latency_p50_s": _percentile(latencies, 0.50),
                    "latency_p90_s": _percentile(latencies, 0.90),
                    "latency_max_s": round(max(latencies), 6) if latencies else 0.0,
                }
            snapshot[str(path_class)] = path_outcomes
    return snapshot


def _active_slot_snapshot(server: Any) -> list[dict[str, Any]]:
    in_flight_lock = getattr(server, "relay_in_flight_lock", None)
    in_flight = getattr(server, "relay_in_flight", {})
    if in_flight_lock is None:
        return []
    with in_flight_lock:
        now = time.monotonic()
        return [
            {
                "slot_id": str(slot_id),
                "age_s": round(now - float(row.get("acquired_at", now)), 3),
                "path_class": str(row.get("path_class") or ""),
                "target_host": str(row.get("target_host") or ""),
                "target_path": str(row.get("target_path") or ""),
                "query_param_keys": row.get("query_param_keys") if isinstance(row.get("query_param_keys"), list) else [],
            }
            for slot_id, row in in_flight.items()
        ]


def _log_event(event: str, payload: dict[str, Any]) -> None:
    sys.stderr.write(json.dumps({"event": event, **payload}, sort_keys=True) + "\n")
    sys.stderr.flush()


def _json_response(payload: dict[str, Any], *, status: int = 200) -> bytes:
    return json.dumps(payload, sort_keys=True).encode("utf-8")


def _target_url(path: str, query: str) -> tuple[str | None, str | None]:
    for target in TARGETS:
        if path == target.prefix:
            suffix = "/"
        elif path.startswith(f"{target.prefix}/"):
            suffix = path[len(target.prefix) :]
        else:
            continue
        if target.prefix == "/data-api" and suffix == "/leaderboard":
            suffix = "/v1/leaderboard"
        return urlunsplit(("https", urlsplit(target.upstream_base).netloc, suffix, query, "")), None
    allowed = ", ".join(target.prefix for target in TARGETS)
    return None, f"path must start with one of: {allowed}"


def _extract_jina_markdown_content(text: str) -> str:
    marker = "Markdown Content:"
    if marker not in text:
        raise ValueError("Jina response did not include Markdown Content marker")
    return text.split(marker, 1)[1].strip()


def _read_limited(response: requests.Response, *, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    for chunk in response.iter_content(chunk_size=65_536):
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise ValueError(f"upstream response exceeded max_bytes={max_bytes}")
        chunks.append(chunk)
    return b"".join(chunks)


def _empty_clob_book_payload(target_url: str, body: bytes) -> bytes:
    parsed = urlsplit(target_url)
    token_id = str((parse_qs(parsed.query).get("token_id") or [""])[0])
    message = ""
    try:
        decoded = json.loads(body.decode("utf-8", "replace"))
        if isinstance(decoded, dict):
            message = str(decoded.get("error") or decoded.get("message") or "")
    except Exception:  # noqa: BLE001 - body is optional diagnostics only.
        message = body[:200].decode("utf-8", "replace")
    payload = {
        "asset_id": token_id,
        "asks": [],
        "bids": [],
        "empty_book_truth": True,
        "empty_book_reason": "clob_book_http_404",
        "empty_book_message": message,
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _retry_after_seconds(response: requests.Response, body: bytes) -> float:
    header_value = response.headers.get("Retry-After") if hasattr(response, "headers") else None
    if header_value:
        try:
            return max(0.0, float(header_value))
        except ValueError:
            pass
    try:
        parsed = json.loads(body.decode("utf-8", "replace"))
    except Exception:  # noqa: BLE001 - best-effort throttle metadata.
        return 1.0
    if isinstance(parsed, dict):
        retry_after = parsed.get("retryAfter")
        if isinstance(retry_after, (int, float)):
            return max(0.0, float(retry_after))
    return 1.0


def _wait_for_upstream_slot(*, min_interval_s: float) -> None:
    global _UPSTREAM_NEXT_REQUEST_AT
    with _UPSTREAM_LOCK:
        now = time.monotonic()
        wait_s = max(0.0, _UPSTREAM_BACKOFF_UNTIL - now, _UPSTREAM_NEXT_REQUEST_AT - now)
        if wait_s > 0:
            time.sleep(wait_s)
        _UPSTREAM_NEXT_REQUEST_AT = time.monotonic() + max(0.0, min_interval_s)


def _record_upstream_backoff(response: requests.Response, body: bytes) -> None:
    global _UPSTREAM_BACKOFF_UNTIL
    retry_after_s = _retry_after_seconds(response, body)
    with _UPSTREAM_LOCK:
        _UPSTREAM_BACKOFF_UNTIL = max(_UPSTREAM_BACKOFF_UNTIL, time.monotonic() + retry_after_s)


def fetch_via_jina(
    target_url: str,
    *,
    timeout_s: float,
    cache_ttl_s: float,
    max_bytes: int,
    upstream_retries: int = DEFAULT_UPSTREAM_RETRIES,
    upstream_min_interval_s: float = DEFAULT_UPSTREAM_MIN_INTERVAL_S,
) -> tuple[bytes, str]:
    now = time.monotonic()
    cached = _CACHE.get(target_url)
    if cached and cached[0] > now:
        return cached[1], "cache"

    reader_url = f"{JINA_READER_PREFIX}{target_url}"
    errors: list[str] = []
    for attempt_index in range(max(1, int(upstream_retries))):
        _wait_for_upstream_slot(min_interval_s=upstream_min_interval_s)
        response = requests.get(
            reader_url,
            headers={"Accept": "text/plain,*/*", "User-Agent": BROWSER_USER_AGENT},
            stream=True,
            timeout=timeout_s,
        )
        body = _read_limited(response, max_bytes=max_bytes)
        try:
            if not (200 <= response.status_code < 300):
                if response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
                    _record_upstream_backoff(response, body)
                raise requests.HTTPError(
                    f"Jina relay returned HTTP {response.status_code}: {body[:500].decode('utf-8', 'replace')}",
                    response=response,
                )
            markdown = _extract_jina_markdown_content(body.decode("utf-8", "replace"))
            parsed = json.loads(markdown)
            payload = json.dumps(parsed, separators=(",", ":"), sort_keys=True).encode("utf-8")
            _CACHE[target_url] = (time.monotonic() + cache_ttl_s, payload, reader_url)
            return payload, "jina"
        except Exception as exc:  # noqa: BLE001 - retry transient relay/parse errors.
            errors.append(f"attempt_{attempt_index + 1}:{type(exc).__name__}:{str(exc)[:240]}")
            if attempt_index + 1 < max(1, int(upstream_retries)):
                time.sleep(min(0.25 * (2**attempt_index), 2.0))
    raise RuntimeError("; ".join(errors[-3:]))


def fetch_direct_json(
    target_url: str,
    *,
    timeout_s: float,
    cache_ttl_s: float,
    max_bytes: int,
    empty_clob_book_404: bool = False,
) -> tuple[bytes, str]:
    now = time.monotonic()
    cached = _CACHE.get(target_url)
    if cached and cached[0] > now:
        return cached[1], "cache"
    response = requests.get(
        target_url,
        headers={"Accept": "application/json,*/*", "User-Agent": BROWSER_USER_AGENT},
        stream=True,
        timeout=timeout_s,
    )
    body = _read_limited(response, max_bytes=max_bytes)
    if empty_clob_book_404 and response.status_code == HTTPStatus.NOT_FOUND:
        payload = _empty_clob_book_payload(target_url, body)
        _CACHE[target_url] = (time.monotonic() + cache_ttl_s, payload, target_url)
        return payload, "direct"
    if not (200 <= response.status_code < 300):
        raise requests.HTTPError(
            f"direct upstream returned HTTP {response.status_code}: {body[:500].decode('utf-8', 'replace')}",
            response=response,
        )
    parsed = json.loads(body.decode("utf-8", "replace"))
    payload = json.dumps(parsed, separators=(",", ":"), sort_keys=True).encode("utf-8")
    _CACHE[target_url] = (time.monotonic() + cache_ttl_s, payload, target_url)
    return payload, "direct"


def fetch_relay_payload(
    target_url: str,
    *,
    path_class: str,
    timeout_s: float,
    cache_ttl_s: float,
    max_bytes: int,
    upstream_retries: int = DEFAULT_UPSTREAM_RETRIES,
    upstream_min_interval_s: float = DEFAULT_UPSTREAM_MIN_INTERVAL_S,
) -> tuple[bytes, str]:
    if (
        path_class == "/clob/book"
        or path_class.startswith("/gamma-api")
        or path_class.startswith("/data-api")
    ):
        is_clob_book = path_class == "/clob/book"
        is_data_api = path_class.startswith("/data-api")
        try:
            return fetch_direct_json(
                target_url,
                timeout_s=timeout_s,
                cache_ttl_s=cache_ttl_s,
                max_bytes=max_bytes,
                empty_clob_book_404=is_clob_book,
            )
        except Exception as direct_exc:  # noqa: BLE001 - direct read failure falls back through Jina.
            direct_error_kind = (
                "relay_direct_book_error"
                if is_clob_book
                else "relay_direct_data_api_error"
                if is_data_api
                else "relay_direct_gamma_error"
            )
            _log_event(
                direct_error_kind,
                {
                    "path_class": path_class,
                    **_target_log_meta(target_url),
                    "exception": type(direct_exc).__name__,
                    "detail": str(direct_exc)[:500],
                    "paper_only": True,
                    "live_orders_allowed": False,
                },
            )
            payload, source = fetch_via_jina(
                target_url,
                timeout_s=min(4.0, float(timeout_s)),
                cache_ttl_s=cache_ttl_s,
                max_bytes=max_bytes,
                upstream_retries=0,
                upstream_min_interval_s=upstream_min_interval_s,
            )
            return payload, f"{source}_fallback"
    return fetch_via_jina(
        target_url,
        timeout_s=timeout_s,
        cache_ttl_s=cache_ttl_s,
        max_bytes=max_bytes,
        upstream_retries=upstream_retries,
        upstream_min_interval_s=upstream_min_interval_s,
    )


def _health_payload(server: Any) -> dict[str, Any]:
    return {
        "status": "PASS",
        "kind": "wallet_copy_jina_source_relay",
        "paper_only": True,
        "live_orders_allowed": False,
        "timeout_s": float(getattr(server, "relay_timeout_s", DEFAULT_TIMEOUT_S)),
        "cache_ttl_s": float(getattr(server, "relay_cache_ttl_s", DEFAULT_CACHE_TTL_S)),
        "upstream_retries": int(getattr(server, "relay_upstream_retries", DEFAULT_UPSTREAM_RETRIES)),
        "upstream_min_interval_s": float(
            getattr(server, "relay_upstream_min_interval_s", DEFAULT_UPSTREAM_MIN_INTERVAL_S)
        ),
        "max_upstream_in_flight": int(
            getattr(server, "relay_max_upstream_in_flight", DEFAULT_MAX_UPSTREAM_IN_FLIGHT)
        ),
        "busy_timeout_s": float(getattr(server, "relay_busy_timeout_s", DEFAULT_BUSY_TIMEOUT_S)),
        "path_metrics": _path_metrics_snapshot(server),
    }


class RelayHandler(BaseHTTPRequestHandler):
    server_version = "WalletCopyJinaRelay/1.0"

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib hook name.
        self._handle_request(write_body=False)

    def do_GET(self) -> None:  # noqa: N802 - stdlib hook name.
        self._handle_request(write_body=True)

    def do_POST(self) -> None:  # noqa: N802 - stdlib hook name.
        self._reject_method()

    def do_PUT(self) -> None:  # noqa: N802 - stdlib hook name.
        self._reject_method()

    def do_DELETE(self) -> None:  # noqa: N802 - stdlib hook name.
        self._reject_method()

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write(f"{self.log_date_time_string()} {self.address_string()} {fmt % args}\n")

    def _reject_method(self) -> None:
        payload = _json_response(
            {
                "error": "method_not_allowed",
                "allowed_methods": ["GET", "HEAD"],
                "paper_only": True,
                "live_orders_allowed": False,
            },
            status=HTTPStatus.METHOD_NOT_ALLOWED,
        )
        self._send_payload(HTTPStatus.METHOD_NOT_ALLOWED, payload, write_body=True)

    def _handle_request(self, *, write_body: bool) -> None:
        parsed = urlsplit(self.path)
        if parsed.path == "/healthz":
            payload = _json_response(_health_payload(self.server))
            self._send_payload(HTTPStatus.OK, payload, write_body=write_body)
            return
        target_url, error = _target_url(parsed.path, parsed.query)
        if error or target_url is None:
            payload = _json_response(
                {
                    "error": "invalid_relay_path",
                    "detail": error,
                    "paper_only": True,
                    "live_orders_allowed": False,
                },
                status=HTTPStatus.BAD_REQUEST,
            )
            self._send_payload(HTTPStatus.BAD_REQUEST, payload, write_body=write_body)
            return

        server = self.server
        timeout_s = float(getattr(server, "relay_timeout_s", DEFAULT_TIMEOUT_S))
        cache_ttl_s = float(getattr(server, "relay_cache_ttl_s", DEFAULT_CACHE_TTL_S))
        max_bytes = int(getattr(server, "relay_max_bytes", DEFAULT_MAX_BYTES))
        upstream_retries = int(getattr(server, "relay_upstream_retries", DEFAULT_UPSTREAM_RETRIES))
        upstream_min_interval_s = float(
            getattr(server, "relay_upstream_min_interval_s", DEFAULT_UPSTREAM_MIN_INTERVAL_S)
        )
        upstream_semaphore = getattr(server, "relay_upstream_semaphore", None)
        busy_timeout_s = float(getattr(server, "relay_busy_timeout_s", DEFAULT_BUSY_TIMEOUT_S))
        request_slot_id = threading.get_ident()
        request_path_class = _path_class(parsed.path)
        target_meta = _target_log_meta(target_url)
        acquired_upstream_slot = True
        if upstream_semaphore is not None:
            acquired_upstream_slot = upstream_semaphore.acquire(timeout=max(0.0, busy_timeout_s))
        if not acquired_upstream_slot:
            held_slots = _active_slot_snapshot(server)
            _record_path_metric(server, request_path_class, "busy_503", busy_timeout_s)
            _log_event(
                "relay_busy_503",
                {
                    "path_class": request_path_class,
                    **target_meta,
                    "in_flight_count": len(held_slots),
                    "max_upstream_in_flight": int(
                        getattr(server, "relay_max_upstream_in_flight", DEFAULT_MAX_UPSTREAM_IN_FLIGHT)
                    ),
                    "busy_timeout_s": busy_timeout_s,
                    "held_slots": held_slots[:20],
                    "paper_only": True,
                    "live_orders_allowed": False,
                },
            )
            error_payload = _json_response(
                {
                    "error": "relay_busy",
                    "detail": "bounded source relay already has the configured maximum upstream requests in flight",
                    "target_url": target_url,
                    "max_upstream_in_flight": int(
                        getattr(server, "relay_max_upstream_in_flight", DEFAULT_MAX_UPSTREAM_IN_FLIGHT)
                    ),
                    "busy_timeout_s": busy_timeout_s,
                    "paper_only": True,
                    "live_orders_allowed": False,
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            self._send_payload(HTTPStatus.SERVICE_UNAVAILABLE, error_payload, write_body=write_body)
            return
        in_flight_lock = getattr(server, "relay_in_flight_lock", None)
        if upstream_semaphore is not None and in_flight_lock is not None:
            with in_flight_lock:
                server.relay_in_flight[request_slot_id] = {
                    "acquired_at": time.monotonic(),
                    "path_class": request_path_class,
                    **target_meta,
                }
        try:
            upstream_started = time.perf_counter()
            payload, source = fetch_relay_payload(
                target_url,
                path_class=request_path_class,
                timeout_s=timeout_s,
                cache_ttl_s=cache_ttl_s,
                max_bytes=max_bytes,
                upstream_retries=upstream_retries,
                upstream_min_interval_s=upstream_min_interval_s,
            )
        except Exception as exc:  # noqa: BLE001 - relay must preserve external failure.
            elapsed_s = time.perf_counter() - upstream_started
            _record_path_metric(server, request_path_class, "upstream_502", elapsed_s)
            _log_event(
                "relay_upstream_502",
                {
                    "path_class": request_path_class,
                    **target_meta,
                    "elapsed_s": round(elapsed_s, 3),
                    "exception": type(exc).__name__,
                    "detail": str(exc)[:500],
                    "paper_only": True,
                    "live_orders_allowed": False,
                },
            )
            error_payload = _json_response(
                {
                    "error": "upstream_relay_error",
                    "exception": type(exc).__name__,
                    "detail": str(exc)[:500],
                    "target_url": target_url,
                    "paper_only": True,
                    "live_orders_allowed": False,
                },
                status=HTTPStatus.BAD_GATEWAY,
            )
            self._send_payload(HTTPStatus.BAD_GATEWAY, error_payload, write_body=write_body)
            return
        finally:
            if upstream_semaphore is not None:
                if in_flight_lock is not None:
                    with in_flight_lock:
                        server.relay_in_flight.pop(request_slot_id, None)
                upstream_semaphore.release()
        _record_path_metric(server, request_path_class, f"success_{source}", time.perf_counter() - upstream_started)

        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("X-Wallet-Copy-Relay", "jina-reader")
            self.send_header("X-Wallet-Copy-Relay-Source", source)
            self.send_header("X-Wallet-Copy-Paper-Only", "true")
            self.send_header("X-Wallet-Copy-Live-Orders-Allowed", "false")
            self.end_headers()
            if write_body:
                self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            return

    def _send_payload(self, status: HTTPStatus, payload: bytes, *, write_body: bool) -> None:
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("X-Wallet-Copy-Paper-Only", "true")
            self.send_header("X-Wallet-Copy-Live-Orders-Allowed", "false")
            self.end_headers()
            if write_body:
                self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            return


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default=os.getenv("POLYMARKET_JINA_RELAY_BIND", DEFAULT_BIND))
    parser.add_argument("--port", type=int, default=int(os.getenv("POLYMARKET_JINA_RELAY_PORT", str(DEFAULT_PORT))))
    parser.add_argument("--timeout-s", type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--cache-ttl-s", type=float, default=DEFAULT_CACHE_TTL_S)
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--upstream-retries", type=int, default=DEFAULT_UPSTREAM_RETRIES)
    parser.add_argument("--upstream-min-interval-s", type=float, default=DEFAULT_UPSTREAM_MIN_INTERVAL_S)
    parser.add_argument("--max-upstream-in-flight", type=int, default=DEFAULT_MAX_UPSTREAM_IN_FLIGHT)
    parser.add_argument("--busy-timeout-s", type=float, default=DEFAULT_BUSY_TIMEOUT_S)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.bind, args.port), RelayHandler)
    server.relay_timeout_s = float(args.timeout_s)
    server.relay_cache_ttl_s = float(args.cache_ttl_s)
    server.relay_max_bytes = int(args.max_bytes)
    server.relay_upstream_retries = int(args.upstream_retries)
    server.relay_upstream_min_interval_s = float(args.upstream_min_interval_s)
    server.relay_max_upstream_in_flight = max(1, int(args.max_upstream_in_flight))
    server.relay_upstream_semaphore = threading.BoundedSemaphore(server.relay_max_upstream_in_flight)
    server.relay_in_flight_lock = threading.Lock()
    server.relay_in_flight = {}
    server.relay_metrics_lock = threading.Lock()
    server.relay_path_metrics = {}
    server.relay_busy_timeout_s = max(0.0, float(args.busy_timeout_s))
    print(
        json.dumps(
            {
                "status": "LISTENING",
                "bind": args.bind,
                "port": args.port,
                "cache_ttl_s": float(args.cache_ttl_s),
                "upstream_min_interval_s": float(args.upstream_min_interval_s),
                "max_upstream_in_flight": server.relay_max_upstream_in_flight,
                "busy_timeout_s": server.relay_busy_timeout_s,
                "paper_only": True,
                "live_orders_allowed": False,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 130
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
