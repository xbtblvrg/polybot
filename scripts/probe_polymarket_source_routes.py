#!/usr/bin/env python3
"""Probe Polymarket source-route health for wallet-copy admission.

The wallet-copy workflow must not go green when direct Data API, Gamma, or CLOB
truth checks are failing. This probe writes a compact state file that separates
local internet health from Polymarket/Cloudflare route resets.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

try:
    import httpx
except Exception:  # pragma: no cover - optional diagnostic dependency.
    httpx = None

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.http_client import redact_source_url_for_state, resolve_polymarket_source_url, source_base_overrides
from src.wallet_copy.source_route import LIVE_SOURCE_ROUTE_OPERATOR_APPROVAL_ENV


DEFAULT_OUTPUT = Path("data/research/wallet_copy_source_route_state.json")
ACTIVE_CLOB_ASSET_IDS = ROOT / "data/research/wallet_copy_active_hotlane_clob_asset_ids.json"

ENDPOINTS: list[dict[str, Any]] = [
    {
        "name": "data_leaderboard",
        "host": "data-api.polymarket.com",
        "url": "https://data-api.polymarket.com/v1/leaderboard",
        "params": {"limit": 1},
    },
    {
        "name": "data_trades_weird_peak",
        "host": "data-api.polymarket.com",
        "url": "https://data-api.polymarket.com/trades",
        "params": {
            "user": "0x9f5ffe76a818dce37c70f947998b52b70671a008",
            "takerOnly": "false",
            "limit": 1,
        },
    },
    {
        "name": "gamma_markets",
        "host": "gamma-api.polymarket.com",
        "url": "https://gamma-api.polymarket.com/markets",
        "params": {"limit": 1},
    },
    {
        "name": "clob_sampling",
        "host": "clob.polymarket.com",
        "url": "https://clob.polymarket.com/sampling-markets",
        "params": None,
    },
    {
        "name": "internet_control_google",
        "host": "www.google.com",
        "url": "https://www.google.com",
        "params": None,
        "control": True,
    },
]

BTC_5M_SLUG_PREFIX = "btc-updown-5m"

REQUEST_VARIANTS: list[dict[str, Any]] = [
    {"name": "requests_default", "kwargs": {}},
    {
        "name": "requests_browser_ua_connection_close",
        "kwargs": {
            "headers": {
                "Accept": "application/json,text/plain,*/*",
                "Connection": "close",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 Chrome/126 Safari/537.36"
                ),
            }
        },
    },
]

CURL_VARIANTS: list[dict[str, Any]] = [
    {"name": "curl_http1_head", "http": "http1", "method": "HEAD"},
    {"name": "curl_http1_get", "http": "http1", "method": "GET"},
    {"name": "curl_ipv4_http1_head", "http": "http1", "method": "HEAD", "ipv4": True},
    {"name": "curl_ipv6_http1_head", "http": "http1", "method": "HEAD", "ipv6": True},
    {"name": "curl_tlsv1_2_http1_head", "http": "http1", "method": "HEAD", "tlsv1_2": True},
    {"name": "curl_compressed_http1_get", "http": "http1", "method": "GET", "compressed": True},
    {"name": "curl_http2_head", "http": "http2", "method": "HEAD"},
    {
        "name": "curl_resolve_ipv4_http1_head",
        "http": "http1",
        "method": "HEAD",
        "resolve_first_ipv4": True,
    },
]

HTTPX_VARIANTS: list[dict[str, Any]] = [
    {"name": "httpx_http1_browser_ua", "http2": False},
    {"name": "httpx_http2_browser_ua", "http2": True},
    {"name": "httpx_http1_browser_ua_trust_env", "http2": False, "trust_env": True},
]

HEARTBEAT_REQUEST_VARIANT_NAMES = {"requests_browser_ua_connection_close"}
HEARTBEAT_CURL_VARIANT_NAMES = {"curl_http1_get", "curl_http1_head"}
HEARTBEAT_HTTPX_VARIANT_NAMES = {"httpx_http1_browser_ua"}
HEARTBEAT_BASE_OVERRIDE_REQUEST_ATTEMPTS = 3
HEARTBEAT_BASE_OVERRIDE_RETRY_SLEEP_S = 0.5

GENERIC_PROXY_ENV_VARS = ("HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "NO_PROXY")


def _source_proxy_url() -> tuple[str, str]:
    for env_var in ("POLYMARKET_SOURCE_PROXY_URL", "POLYMARKET_HTTPS_PROXY"):
        value = os.getenv(env_var, "").strip()
        if value:
            return env_var, value
    return "", ""


def _generic_proxy_env_state() -> dict[str, Any]:
    """Persist whether process-level proxy env could affect trust_env probes."""

    entries: dict[str, dict[str, Any]] = {}
    proxy_configured = False
    no_proxy_configured = False
    for env_var in GENERIC_PROXY_ENV_VARS:
        value = os.getenv(env_var, "").strip()
        configured = bool(value)
        entry: dict[str, Any] = {"configured": configured}
        if configured and env_var == "NO_PROXY":
            parts = [part.strip() for part in value.split(",") if part.strip()]
            no_proxy_configured = True
            entry.update(
                {
                    "entry_count": len(parts),
                    "mentions_polymarket": any("polymarket" in part.lower() for part in parts),
                    "value_redacted": ",".join(parts[:8]) if len(parts) <= 8 else ",".join(parts[:8]) + ",...",
                }
            )
        elif configured:
            proxy_configured = True
            redacted = redact_source_url_for_state(value)
            entry.update(
                {
                    "url": redacted["url"],
                    "scheme": redacted["scheme"],
                    "host": redacted["host"],
                    "contains_sensitive_material": redacted["contains_sensitive_material"],
                }
            )
        entries[env_var] = entry
    return {
        "configured": proxy_configured or no_proxy_configured,
        "proxy_configured": proxy_configured,
        "no_proxy_configured": no_proxy_configured,
        "env": entries,
    }


def _filter_profile_variants(
    variants: list[dict[str, Any]],
    *,
    profile: str,
    allowed_names: set[str],
) -> list[dict[str, Any]]:
    if profile == "heartbeat":
        return [variant for variant in variants if str(variant.get("name") or "") in allowed_names]
    return variants


def _as_list_payload(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("markets", "data", "results"):
            rows = value.get(key)
            if isinstance(rows, list):
                return rows
    return []


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


def _active_hotlane_clob_book_endpoint_from_state(path: Path | None = None) -> dict[str, Any] | None:
    """Use the latest hot-lane CLOB asset cache as a route-health fallback."""

    path = path or ACTIVE_CLOB_ASSET_IDS
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    token_ids = [str(item) for item in _json_list(payload.get("asset_ids")) if str(item)]
    if not token_ids:
        return None
    return {
        "name": "clob_active_btc5m_book",
        "host": "clob.polymarket.com",
        "url": "https://clob.polymarket.com/book",
        "params": {"token_id": token_ids[0]},
        "dynamic_probe": True,
        "source_endpoint": "wallet_copy_active_hotlane_clob_asset_ids",
    }


def _active_btc5m_clob_book_endpoint(timeout_s: float) -> dict[str, Any] | None:
    """Resolve a lightweight active BTC 5m CLOB book probe endpoint.

    ``/sampling-markets`` can be slow through the read-only relay and does not
    represent the token-specific book reads required by wallet-copy admission.
    """

    now_s = int(time.time())
    base_start = int(now_s // 300 * 300)
    # Try the current and nearby windows. Some Gamma rows are published just
    # ahead of the active 5m window, and the previous window can stay active
    # briefly around the boundary.
    candidate_starts = [base_start - 300, base_start, base_start + 300]
    lookup_timeout_s = max(0.5, min(float(timeout_s), 1.0))
    seen: set[int] = set()
    for start_s in candidate_starts:
        if start_s in seen:
            continue
        seen.add(start_s)
        slug = f"{BTC_5M_SLUG_PREFIX}-{start_s}"
        gamma_url, _route_meta = resolve_polymarket_source_url("https://gamma-api.polymarket.com/markets")
        try:
            response = requests.get(
                gamma_url,
                params={"slug": slug},
                timeout=lookup_timeout_s,
                headers={
                    "Accept": "application/json,text/plain,*/*",
                    "Connection": "close",
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 Chrome/126 Safari/537.36"
                    ),
                },
            )
            if not (200 <= response.status_code < 300):
                continue
            rows = _as_list_payload(response.json())
        except Exception:
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("closed") is True:
                continue
            token_ids = [str(item) for item in _json_list(row.get("clobTokenIds")) if str(item)]
            if not token_ids:
                continue
            return {
                "name": "clob_active_btc5m_book",
                "host": "clob.polymarket.com",
                "url": "https://clob.polymarket.com/book",
                "params": {"token_id": token_ids[0]},
                "dynamic_probe": True,
                "market_slug": str(row.get("slug") or slug),
                "source_endpoint": "gamma_markets",
            }
    return _active_hotlane_clob_book_endpoint_from_state()


def _runtime_endpoints(timeout_s: float) -> list[dict[str, Any]]:
    endpoints: list[dict[str, Any]] = []
    for endpoint in ENDPOINTS:
        if str(endpoint.get("name") or "") == "clob_sampling":
            endpoints.append(_active_btc5m_clob_book_endpoint(timeout_s) or endpoint)
        else:
            endpoints.append(endpoint)
    return endpoints


def _request_variants(profile: str = "exhaustive") -> list[dict[str, Any]]:
    variants = list(REQUEST_VARIANTS)
    env_var, proxy_url = _source_proxy_url()
    if proxy_url:
        variants.append(
            {
                "name": "requests_source_proxy_browser_ua",
                "proxy_env_var": env_var,
                "kwargs": {
                    "headers": {
                        "Accept": "application/json,text/plain,*/*",
                        "Connection": "close",
                        "User-Agent": (
                            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                            "AppleWebKit/537.36 Chrome/126 Safari/537.36"
                        ),
                    },
                    "proxies": {"http": proxy_url, "https": proxy_url},
                },
            }
        )
    return _filter_profile_variants(
        variants,
        profile=profile,
        allowed_names=HEARTBEAT_REQUEST_VARIANT_NAMES | {"requests_source_proxy_browser_ua"},
    )


def _curl_variants(profile: str = "exhaustive") -> list[dict[str, Any]]:
    variants = list(CURL_VARIANTS)
    env_var, proxy_url = _source_proxy_url()
    if proxy_url:
        variants.append(
            {
                "name": "curl_source_proxy_http1_head",
                "http": "http1",
                "method": "HEAD",
                "proxy_env_var": env_var,
                "proxy_url": proxy_url,
            }
        )
    return _filter_profile_variants(
        variants,
        profile=profile,
        allowed_names=HEARTBEAT_CURL_VARIANT_NAMES | {"curl_source_proxy_http1_head"},
    )


def _httpx_variants(profile: str = "exhaustive") -> list[dict[str, Any]]:
    variants = list(HTTPX_VARIANTS)
    env_var, proxy_url = _source_proxy_url()
    if proxy_url:
        variants.append(
            {
                "name": "httpx_source_proxy_http1_browser_ua",
                "http2": False,
                "proxy_env_var": env_var,
                "proxy_url": proxy_url,
            }
        )
    return _filter_profile_variants(
        variants,
        profile=profile,
        allowed_names=HEARTBEAT_HTTPX_VARIANT_NAMES | {"httpx_source_proxy_http1_browser_ua"},
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _dns_probe(host: str) -> dict[str, Any]:
    try:
        rows = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except OSError as exc:
        return {"status": "ERROR", "error": repr(exc), "addresses": []}
    addresses: list[str] = []
    for row in rows:
        sockaddr = row[4]
        if sockaddr:
            addresses.append(str(sockaddr[0]))
    return {
        "status": "OK" if addresses else "EMPTY",
        "addresses": sorted(set(addresses)),
    }


def _endpoint_url(endpoint: dict[str, Any]) -> str:
    url = str(endpoint["url"])
    params = endpoint.get("params")
    if not isinstance(params, dict) or not params:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}{urlencode(params)}"


def _query_param_keys(endpoint: dict[str, Any]) -> list[str]:
    params = endpoint.get("params")
    if not isinstance(params, dict):
        return []
    return sorted(str(key) for key in params)


def _request_fingerprint(endpoint: dict[str, Any], *, request_role: str) -> str:
    payload = "|".join(
        [
            str(request_role or "source_route_probe"),
            str(endpoint.get("url") or ""),
            ",".join(_query_param_keys(endpoint)),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _probe_rows(endpoint_result: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in ("requests", "curl_variants", "httpx_variants"):
        for row in endpoint_result.get(key) or []:
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _row_transport(row: dict[str, Any]) -> str:
    return str(row.get("variant") or row.get("transport") or "unknown")


def _row_is_reset(row: dict[str, Any]) -> bool:
    text = " ".join(
        str(row.get(key) or "")
        for key in ("error", "exception", "stderr_tail", "status")
    ).lower()
    return "connection reset" in text or "connectionreseterror" in text or "recv failure" in text


def _row_is_timeout(row: dict[str, Any]) -> bool:
    text = " ".join(
        str(row.get(key) or "")
        for key in ("error", "exception", "stderr_tail", "status")
    ).lower()
    return "timed out" in text or "timeout" in text or "read timed out" in text


def _reset_by_transport(endpoint_result: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in _probe_rows(endpoint_result):
        if _row_is_reset(row):
            transport = _row_transport(row)
            counts[transport] = counts.get(transport, 0) + 1
    return dict(sorted(counts.items()))


def _timeout_by_transport(endpoint_result: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in _probe_rows(endpoint_result):
        if _row_is_timeout(row):
            transport = _row_transport(row)
            counts[transport] = counts.get(transport, 0) + 1
    return dict(sorted(counts.items()))


def _best_nonpassing_variant(endpoint_result: dict[str, Any]) -> str:
    rows = [row for row in _probe_rows(endpoint_result) if row.get("status") != "PASS"]
    if not rows:
        return ""

    def elapsed(row: dict[str, Any]) -> float:
        try:
            return float(row.get("elapsed_ms") or 0.0)
        except (TypeError, ValueError):
            return 0.0

    return _row_transport(min(rows, key=elapsed))


def _dns_family_counts(dns_result: dict[str, Any]) -> dict[str, int]:
    addresses = dns_result.get("addresses") if isinstance(dns_result, dict) else []
    ipv4 = sum(1 for address in addresses or [] if "." in str(address))
    ipv6 = sum(1 for address in addresses or [] if ":" in str(address))
    return {"ipv4": ipv4, "ipv6": ipv6}


def _reset_attempt_count(endpoint_result: dict[str, Any]) -> int:
    count = 0
    for row in _probe_rows(endpoint_result):
        if _row_is_reset(row):
            count += 1
    return count


def _timeout_attempt_count(endpoint_result: dict[str, Any]) -> int:
    count = 0
    for row in _probe_rows(endpoint_result):
        if _row_is_timeout(row):
            count += 1
    return count


def _elapsed_ms_total(endpoint_result: dict[str, Any]) -> float:
    total = 0.0
    for row in _probe_rows(endpoint_result):
        try:
            total += float(row.get("elapsed_ms") or 0.0)
        except (TypeError, ValueError):
            continue
    return round(total, 3)


def _attach_probe_metadata(endpoint_result: dict[str, Any], endpoint: dict[str, Any], *, request_role: str) -> None:
    fingerprint = _request_fingerprint(endpoint, request_role=request_role)
    endpoint_result.update(
        {
            "attempt_count": len(_probe_rows(endpoint_result)),
            "elapsed_ms_total": _elapsed_ms_total(endpoint_result),
            "query_param_keys": _query_param_keys(endpoint),
            "request_fingerprint": fingerprint,
            "request_role": request_role,
            "reset_attempt_count": _reset_attempt_count(endpoint_result),
            "reset_by_transport": _reset_by_transport(endpoint_result),
            "timeout_attempt_count": _timeout_attempt_count(endpoint_result),
            "timeout_by_transport": _timeout_by_transport(endpoint_result),
            "best_nonpassing_variant": _best_nonpassing_variant(endpoint_result),
            "route_report_id": f"rr_{fingerprint}",
        }
    )


def _routed_endpoint(endpoint: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    routed_url, route_meta = resolve_polymarket_source_url(str(endpoint["url"]))
    routed = dict(endpoint)
    routed["url"] = routed_url
    routed["host"] = str(route_meta.get("routed_host") or endpoint["host"])
    return routed, route_meta


def _same_source_route_without_override(
    endpoint: dict[str, Any],
    routed_endpoint: dict[str, Any],
    route_meta: dict[str, Any],
    proxy_url: str,
) -> bool:
    if proxy_url:
        return False
    if bool(route_meta.get("source_base_override_configured")):
        return False
    return (
        str(endpoint.get("host") or "").lower() == str(routed_endpoint.get("host") or "").lower()
        and _endpoint_url(endpoint) == _endpoint_url(routed_endpoint)
    )


def _heartbeat_base_override_single_request(route_meta: dict[str, Any], *, probe_profile: str) -> bool:
    return probe_profile == "heartbeat" and bool(route_meta.get("source_base_override_configured"))


def _heartbeat_base_override_requests(
    routed_endpoint: dict[str, Any],
    timeout_s: float,
    *,
    probe_profile: str,
) -> list[dict[str, Any]]:
    """Probe a configured base/relay route with bounded sequential retries.

    Heartbeat mode intentionally avoids curl/httpx fan-out against the local
    relay. A single transient 502/503 from relay backpressure, however, should
    not become the whole source-of-truth verdict when a short retry can prove
    the same read-only route. Every failed attempt is still persisted.
    """

    variants = _request_variants(probe_profile)[:1]
    if not variants:
        return []
    variant = variants[0]
    rows: list[dict[str, Any]] = []
    for attempt_index in range(HEARTBEAT_BASE_OVERRIDE_REQUEST_ATTEMPTS):
        row = _requests_probe(routed_endpoint, variant, timeout_s)
        row["routed_probe_attempt"] = attempt_index + 1
        row["routed_probe_max_attempts"] = HEARTBEAT_BASE_OVERRIDE_REQUEST_ATTEMPTS
        rows.append(row)
        if row.get("status") == "PASS":
            break
        if attempt_index + 1 < HEARTBEAT_BASE_OVERRIDE_REQUEST_ATTEMPTS:
            time.sleep(HEARTBEAT_BASE_OVERRIDE_RETRY_SLEEP_S)
    return rows


def _ipv4_addresses(dns_result: dict[str, Any]) -> list[str]:
    addresses = dns_result.get("addresses") if isinstance(dns_result, dict) else []
    return [str(address) for address in addresses or [] if "." in str(address)]


def _requests_probe(endpoint: dict[str, Any], variant: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        response = requests.get(
            str(endpoint["url"]),
            params=endpoint.get("params"),
            timeout=timeout_s,
            **variant["kwargs"],
        )
    except Exception as exc:  # noqa: BLE001 - state should preserve exact external failure.
        return {
            "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
            "error": str(exc)[:500],
            "exception": type(exc).__name__,
            "proxy_configured": bool(variant.get("proxy_env_var")),
            "proxy_env_var": variant.get("proxy_env_var"),
            "status": "ERROR",
            "variant": variant["name"],
        }
    return {
        "bytes": len(response.content),
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
        "http_status": response.status_code,
        "proxy_configured": bool(variant.get("proxy_env_var")),
        "proxy_env_var": variant.get("proxy_env_var"),
        "status": "PASS" if 200 <= response.status_code < 300 else "HTTP_NON_2XX",
        "variant": variant["name"],
    }


def _httpx_probe(endpoint: dict[str, Any], variant: dict[str, Any], timeout_s: float) -> dict[str, Any]:
    if httpx is None:
        return {
            "error": "httpx_not_installed",
            "http2": bool(variant.get("http2")),
            "proxy_configured": bool(variant.get("proxy_env_var")),
            "proxy_env_var": variant.get("proxy_env_var"),
            "status": "SKIPPED",
            "variant": variant["name"],
        }
    started = time.perf_counter()
    headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 Chrome/126 Safari/537.36"
        ),
    }
    client_kwargs: dict[str, Any] = {
        "follow_redirects": False,
        "headers": headers,
        "http2": bool(variant.get("http2")),
        "timeout": timeout_s,
        "trust_env": bool(variant.get("trust_env", False)),
    }
    if variant.get("proxy_url"):
        client_kwargs["proxy"] = str(variant["proxy_url"])
    try:
        with httpx.Client(**client_kwargs) as client:
            response = client.get(str(endpoint["url"]), params=endpoint.get("params"))
    except Exception as exc:  # noqa: BLE001 - preserve external transport failure.
        return {
            "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
            "error": str(exc)[:500],
            "exception": type(exc).__name__,
            "http2": bool(variant.get("http2")),
            "proxy_configured": bool(variant.get("proxy_env_var")),
            "proxy_env_var": variant.get("proxy_env_var"),
            "status": "ERROR",
            "variant": variant["name"],
        }
    return {
        "bytes": len(response.content),
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
        "http2": bool(variant.get("http2")),
        "http_status": response.status_code,
        "proxy_configured": bool(variant.get("proxy_env_var")),
        "proxy_env_var": variant.get("proxy_env_var"),
        "status": "PASS" if 200 <= response.status_code < 300 else "HTTP_NON_2XX",
        "trust_env": bool(variant.get("trust_env", False)),
        "variant": variant["name"],
    }
def _curl_probe(
    endpoint: dict[str, Any],
    variant: dict[str, Any],
    timeout_s: float,
    dns_result: dict[str, Any],
) -> dict[str, Any]:
    url = _endpoint_url(endpoint)
    cmd = [
        "curl",
        "-sS",
        "-A",
        "Mozilla/5.0",
        "--max-time",
        str(timeout_s),
    ]
    if variant.get("http") == "http2":
        cmd.append("--http2")
    else:
        cmd.append("--http1.1")
    if variant.get("ipv4"):
        cmd.append("-4")
    if variant.get("ipv6"):
        cmd.append("-6")
    if variant.get("tlsv1_2"):
        cmd.append("--tlsv1.2")
    if variant.get("compressed"):
        cmd.append("--compressed")
    if variant.get("resolve_first_ipv4"):
        ipv4 = _ipv4_addresses(dns_result)
        if ipv4:
            cmd.extend(["--resolve", f"{endpoint['host']}:443:{ipv4[0]}"])
    if variant.get("proxy_url"):
        cmd.extend(["--proxy", str(variant["proxy_url"])])
    if variant.get("method") == "GET":
        cmd.extend(["-D", "-", "-o", "/dev/null"])
    else:
        cmd.append("-I")
    cmd.append(url)
    started = time.perf_counter()
    try:
        completed = subprocess.run(cmd, capture_output=True, check=False, text=True, timeout=timeout_s + 1.0)
    except Exception as exc:  # noqa: BLE001 - diagnostic state should keep exact failure.
        return {
            "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
            "error": repr(exc),
            "proxy_configured": bool(variant.get("proxy_env_var")),
            "proxy_env_var": variant.get("proxy_env_var"),
            "status": "ERROR",
            "transport": variant["name"],
        }
    first_line = ""
    for line in completed.stdout.splitlines():
        if line.strip():
            first_line = line.strip()
            break
    return {
        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
        "proxy_configured": bool(variant.get("proxy_env_var")),
        "proxy_env_var": variant.get("proxy_env_var"),
        "returncode": completed.returncode,
        "status": "PASS" if completed.returncode == 0 and first_line.startswith("HTTP/") else "ERROR",
        "stderr_tail": completed.stderr[-500:],
        "stdout_first_line": first_line,
        "transport": variant["name"],
    }


def _route_variant_pass_counts(endpoint_result: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in endpoint_result.get("requests") or []:
        if isinstance(row, dict) and row.get("status") == "PASS":
            counts[str(row.get("variant") or "requests_unknown")] = counts.get(str(row.get("variant") or "requests_unknown"), 0) + 1
    for row in endpoint_result.get("curl_variants") or []:
        if isinstance(row, dict) and row.get("status") == "PASS":
            counts[str(row.get("transport") or "curl_unknown")] = counts.get(str(row.get("transport") or "curl_unknown"), 0) + 1
    for row in endpoint_result.get("httpx_variants") or []:
        if isinstance(row, dict) and row.get("status") == "PASS":
            counts[str(row.get("variant") or "httpx_unknown")] = counts.get(str(row.get("variant") or "httpx_unknown"), 0) + 1
    return counts


def _best_route_variant(endpoint_result: dict[str, Any]) -> str:
    for row in endpoint_result.get("requests") or []:
        if isinstance(row, dict) and row.get("status") == "PASS":
            return str(row.get("variant") or "requests_unknown")
    for row in endpoint_result.get("curl_variants") or []:
        if isinstance(row, dict) and row.get("status") == "PASS":
            return str(row.get("transport") or "curl_unknown")
    for row in endpoint_result.get("httpx_variants") or []:
        if isinstance(row, dict) and row.get("status") == "PASS":
            return str(row.get("variant") or "httpx_unknown")
    return ""


def _rate_limit_suspected(endpoint_result: dict[str, Any]) -> bool:
    text = json.dumps(endpoint_result, sort_keys=True).lower()
    return any(token in text for token in ("429", "rate limit", "too many requests"))


def _endpoint_status(endpoint_result: dict[str, Any]) -> str:
    request_pass = any(row.get("status") == "PASS" for row in endpoint_result.get("requests", []))
    curl_pass = any(row.get("status") == "PASS" for row in endpoint_result.get("curl_variants", []))
    httpx_pass = any(row.get("status") == "PASS" for row in endpoint_result.get("httpx_variants", []))
    if request_pass or curl_pass or httpx_pass:
        return "PASS"
    if _rate_limit_suspected(endpoint_result):
        return "RATE_LIMIT_SUSPECTED"
    if any(_row_is_reset(row) for row in _probe_rows(endpoint_result)):
        return "CONNECTION_RESET"
    if any(_row_is_timeout(row) for row in _probe_rows(endpoint_result)):
        return "TIMEOUT"
    return "ERROR"


def _run_parallel_probe_rows(tasks: list[tuple[int, Any]]) -> list[Any]:
    if not tasks:
        return []
    results: list[Any] = [None for _ in tasks]
    max_workers = min(8, len(tasks))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {executor.submit(task): index for index, task in tasks}
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                results[index] = future.result()
            except Exception as exc:  # noqa: BLE001 - this is diagnostic state.
                results[index] = {"error": repr(exc), "status": "ERROR"}
    return results


def _direct_probe(endpoint: dict[str, Any], timeout_s: float, *, probe_profile: str = "exhaustive") -> dict[str, Any]:
    dns_result = _dns_probe(str(endpoint["host"]))
    curl_variants = _curl_variants(probe_profile)
    httpx_variants = _httpx_variants(probe_profile)
    request_variants = _request_variants(probe_profile)
    result = {
        "curl_variants": _run_parallel_probe_rows(
            [(idx, lambda variant=variant: _curl_probe(endpoint, variant, timeout_s, dns_result)) for idx, variant in enumerate(curl_variants)]
        ),
        "dns": dns_result,
        "host": endpoint["host"],
        "httpx_variants": _run_parallel_probe_rows(
            [(idx, lambda variant=variant: _httpx_probe(endpoint, variant, timeout_s)) for idx, variant in enumerate(httpx_variants)]
        ),
        "requests": _run_parallel_probe_rows(
            [(idx, lambda variant=variant: _requests_probe(endpoint, variant, timeout_s)) for idx, variant in enumerate(request_variants)]
        ),
        "probe_profile": probe_profile,
        "url": endpoint["url"],
    }
    result["status"] = _endpoint_status(result)
    result["best_route_variant"] = _best_route_variant(result)
    result["route_variant_pass_counts"] = _route_variant_pass_counts(result)
    result["rate_limit_suspected"] = _rate_limit_suspected(result)
    _attach_probe_metadata(result, endpoint, request_role="direct_source_route_probe")
    return result


def _route_class(endpoint_result: dict[str, Any]) -> str:
    direct_status = str(endpoint_result.get("direct_status") or "")
    routed_status = str(endpoint_result.get("routed_status") or endpoint_result.get("status") or "")
    override_recovered = bool(endpoint_result.get("source_base_override_configured")) and routed_status == "PASS"
    proxy_recovered = any(
        isinstance(row, dict) and row.get("status") == "PASS" and row.get("proxy_configured")
        for row in (
            list(endpoint_result.get("requests") or [])
            + list(endpoint_result.get("curl_variants") or [])
            + list(endpoint_result.get("httpx_variants") or [])
        )
    )
    if direct_status == "PASS":
        return "DIRECT_PASS"
    if direct_status == "CONNECTION_RESET" and override_recovered:
        return "ROUTE_RECOVERED_DEGRADED"
    if direct_status == "CONNECTION_RESET" and proxy_recovered:
        return "PROXY_RECOVERED_DEGRADED"
    if direct_status == "CONNECTION_RESET":
        return "DIRECT_RESET"
    if direct_status == "RATE_LIMIT_SUSPECTED" and override_recovered:
        return "ROUTE_RECOVERED_DEGRADED"
    if direct_status == "RATE_LIMIT_SUSPECTED" and proxy_recovered:
        return "PROXY_RECOVERED_DEGRADED"
    if direct_status == "RATE_LIMIT_SUSPECTED":
        return "DIRECT_RATE_LIMIT_SUSPECTED"
    if routed_status == "PASS" and override_recovered:
        return "ROUTE_RECOVERED_DEGRADED"
    if routed_status == "PASS" and proxy_recovered:
        return "PROXY_RECOVERED_DEGRADED"
    return "DIRECT_ERROR"


def _wall_budget_remaining_s(started: float, max_wall_runtime_s: float) -> float:
    if max_wall_runtime_s <= 0:
        return float("inf")
    return float(max_wall_runtime_s) - (time.perf_counter() - started)


def _call_direct_probe(endpoint: dict[str, Any], timeout_s: float, *, probe_profile: str) -> dict[str, Any]:
    try:
        return _direct_probe(endpoint, timeout_s, probe_profile=probe_profile)
    except TypeError as exc:
        if "probe_profile" not in str(exc):
            raise
        return _direct_probe(endpoint, timeout_s)


def build_state(timeout_s: float, *, probe_profile: str = "exhaustive", max_wall_runtime_s: float = 0.0) -> dict[str, Any]:
    started = time.perf_counter()
    endpoint_results: list[dict[str, Any]] = []
    skipped_endpoints: list[dict[str, Any]] = []
    proxy_env_var, proxy_url = _source_proxy_url()
    for endpoint in _runtime_endpoints(timeout_s):
        if _wall_budget_remaining_s(started, max_wall_runtime_s) <= max(float(timeout_s), 1.0):
            skipped_endpoints.append(
                {
                    "name": endpoint["name"],
                    "host": endpoint["host"],
                    "reason": "probe_wall_runtime_budget_exhausted",
                }
            )
            continue
        direct_result = _call_direct_probe(endpoint, timeout_s, probe_profile=probe_profile)
        routed_endpoint, route_meta = _routed_endpoint(endpoint)
        dns_result = _dns_probe(str(routed_endpoint["host"]))
        reuse_direct_probe = _same_source_route_without_override(
            endpoint,
            routed_endpoint,
            route_meta,
            proxy_url,
        )
        heartbeat_base_override_single_request = _heartbeat_base_override_single_request(
            route_meta,
            probe_profile=probe_profile,
        )
        if reuse_direct_probe:
            routed_httpx_variants = list(direct_result.get("httpx_variants", []))
            routed_requests = list(direct_result.get("requests", []))
            routed_curl_variants = list(direct_result.get("curl_variants", []))
        elif heartbeat_base_override_single_request:
            # The heartbeat profile already proves direct route failure with
            # multiple transports. For a configured local/base relay, use a
            # bounded sequential GET retry instead of curl/httpx fan-out. This
            # avoids self-inflicted relay load while preventing one relay-busy
            # transient from falsely marking the route reset.
            routed_httpx_variants = []
            routed_curl_variants = []
            routed_requests = _heartbeat_base_override_requests(
                routed_endpoint,
                timeout_s,
                probe_profile=probe_profile,
            )
        else:
            routed_httpx_variants = _run_parallel_probe_rows(
                [
                    (idx, lambda variant=variant: _httpx_probe(routed_endpoint, variant, timeout_s))
                    for idx, variant in enumerate(_httpx_variants(probe_profile))
                ]
            )
            routed_requests = _run_parallel_probe_rows(
                [
                    (idx, lambda variant=variant: _requests_probe(routed_endpoint, variant, timeout_s))
                    for idx, variant in enumerate(_request_variants(probe_profile))
                ]
            )
            routed_curl_variants = _run_parallel_probe_rows(
                [
                    (idx, lambda variant=variant: _curl_probe(routed_endpoint, variant, timeout_s, dns_result))
                    for idx, variant in enumerate(_curl_variants(probe_profile))
                ]
            )
        result = {
            "control": bool(endpoint.get("control")),
            "direct_best_route_variant": direct_result.get("best_route_variant", ""),
            "direct_curl_variants": direct_result.get("curl_variants", []),
            "direct_dns": direct_result.get("dns", {}),
            "direct_dns_family_counts": _dns_family_counts(direct_result.get("dns", {})),
            "direct_rate_limit_suspected": bool(direct_result.get("rate_limit_suspected")),
            "direct_requests": direct_result.get("requests", []),
            "direct_status": direct_result.get("status"),
            "dns": dns_result,
            "dns_family_counts": _dns_family_counts(dns_result),
            "host": endpoint["host"],
            "dynamic_probe": bool(endpoint.get("dynamic_probe")),
            "market_slug": endpoint.get("market_slug"),
            "source_endpoint": endpoint.get("source_endpoint"),
            "name": endpoint["name"],
            "original_host": route_meta.get("original_host") or endpoint["host"],
            "httpx_variants": routed_httpx_variants,
            "requests": routed_requests,
            "routed_reuse_reason": (
                "identical_direct_route_without_proxy_or_base_override"
                if reuse_direct_probe
                else None
            ),
            "routed_reused_direct_probe": reuse_direct_probe,
            "routed_host": route_meta.get("routed_host") or endpoint["host"],
            "routed_url": (
                route_meta.get("routed_url_redacted")
                or redact_source_url_for_state(str(routed_endpoint["url"]))["url"]
            ),
            "routed_url_contains_sensitive_material": bool(
                route_meta.get("routed_url_contains_sensitive_material")
            ),
            "routed_probe_strategy": (
                "heartbeat_base_override_single_request"
                if heartbeat_base_override_single_request
                else "full_routed_variant_set"
            ),
            "source_base_override_configured": bool(route_meta.get("source_base_override_configured")),
            "source_base_override_env_var": route_meta.get("source_base_override_env_var"),
            "url": endpoint["url"],
        }
        result["curl_variants"] = routed_curl_variants
        result["routed_status"] = _endpoint_status(result)
        result["status"] = result["routed_status"]
        result["route_class"] = _route_class(result)
        result["route_variant_pass_counts"] = _route_variant_pass_counts(result)
        result["best_route_variant"] = _best_route_variant(result)
        result["rate_limit_suspected"] = _rate_limit_suspected(result)
        _attach_probe_metadata(result, routed_endpoint, request_role="routed_source_route_probe")
        endpoint_results.append(result)

    controls = [row for row in endpoint_results if row.get("control")]
    polymarket = [row for row in endpoint_results if not row.get("control")]
    partial_probe = bool(skipped_endpoints)
    control_pass = any(row.get("routed_status") == "PASS" for row in controls)
    polymarket_direct_pass = bool(polymarket) and not partial_probe and all(
        row.get("route_class") == "DIRECT_PASS" for row in polymarket
    )
    polymarket_recovered_degraded = all(
        row.get("route_class") in {"DIRECT_PASS", "ROUTE_RECOVERED_DEGRADED", "PROXY_RECOVERED_DEGRADED"}
        for row in polymarket
    ) and any(
        row.get("route_class") in {"ROUTE_RECOVERED_DEGRADED", "PROXY_RECOVERED_DEGRADED"}
        for row in polymarket
    ) and not partial_probe
    measured_base_or_proxy_route_pass = bool(polymarket_recovered_degraded) and bool(polymarket) and all(
        row.get("routed_status") == "PASS" for row in polymarket
    )
    configured_base_route_timeout = any(
        bool(row.get("source_base_override_configured")) and int(row.get("timeout_attempt_count") or 0) > 0
        for row in polymarket
    )
    polymarket_reset = any(row.get("route_class") == "DIRECT_RESET" for row in polymarket)
    rate_limit_suspected = any(bool(row.get("rate_limit_suspected")) for row in polymarket)
    if polymarket_direct_pass:
        status = "PASS"
    elif partial_probe:
        status = "POLYMARKET_ROUTE_PROBE_PARTIAL"
    elif control_pass and polymarket_recovered_degraded:
        status = "POLYMARKET_ROUTE_RECOVERED_DEGRADED"
    elif control_pass and rate_limit_suspected:
        status = "POLYMARKET_RATE_LIMIT_SUSPECTED"
    elif control_pass and polymarket_reset:
        status = "POLYMARKET_ROUTE_RESET"
    elif control_pass:
        status = "POLYMARKET_SOURCE_ERROR"
    else:
        status = "LOCAL_NETWORK_ERROR"
    base_overrides = source_base_overrides()
    generic_proxy_env = _generic_proxy_env_state()
    configured_base_overrides = [
        row["env_var"]
        for row in base_overrides.values()
        if isinstance(row, dict) and row.get("configured") and row.get("env_var")
    ]
    direct_dns_family_counts_by_endpoint = {
        row["name"]: row.get("direct_dns_family_counts", {})
        for row in endpoint_results
    }
    polymarket_dns_ok = all(
        bool((direct_dns_family_counts_by_endpoint.get(row.get("name")) or {}).get("ipv4"))
        or bool((direct_dns_family_counts_by_endpoint.get(row.get("name")) or {}).get("ipv6"))
        for row in endpoint_results
        if not row.get("control")
    )
    external_route_required = (
        status == "POLYMARKET_ROUTE_RESET"
        and control_pass
        and polymarket_dns_ok
        and not bool(proxy_url)
        and not bool(generic_proxy_env.get("proxy_configured"))
        and not configured_base_overrides
    )
    code_route_recovery_exhausted = external_route_required
    live_source_route_operator_approval_id = os.getenv(LIVE_SOURCE_ROUTE_OPERATOR_APPROVAL_ENV, "").strip()
    required_operator_inputs = (
        [
            "POLYMARKET_SOURCE_PROXY_URL",
            "POLYMARKET_HTTPS_PROXY",
            "POLYMARKET_DATA_API_BASE_URL",
            "POLYMARKET_GAMMA_API_BASE_URL",
            "POLYMARKET_CLOB_API_BASE_URL",
        ]
        if external_route_required
        else []
    )
    if status == "PASS":
        next_action = "source route currently healthy; rely on normal direct source checks"
    elif configured_base_overrides and measured_base_or_proxy_route_pass:
        next_action = (
            "configured Polymarket relay/base override is measured PASS for read-only paper/proof sources; "
            "direct Polymarket route still resets or rate-limits, so keep live admission blocked until direct or "
            "operator-approved live-source route is admissible"
        )
    elif configured_base_overrides and configured_base_route_timeout:
        next_action = (
            "configured Polymarket base override/proxy route timed out while reading; inspect the local relay "
            "upstream fetch path, retry budget, and endpoint path mapping before live admission"
        )
    elif configured_base_overrides:
        next_action = (
            "configured Polymarket base override/proxy route still does not pass; inspect relay health, "
            "TLS/Cloudflare behavior, and endpoint path mapping before live admission"
        )
    elif generic_proxy_env.get("proxy_configured"):
        next_action = (
            "generic process proxy environment is configured but did not recover Polymarket routes; "
            "keep live admission blocked and configure a named POLYMARKET_SOURCE_PROXY_URL/base override "
            "so the recovery route is auditable"
        )
    elif external_route_required:
        next_action = (
            "direct internet and DNS are healthy but Polymarket routes reset across transports; "
            "treat this as an external route requirement and configure a measured proxy/base override "
            "before spending more live-readiness poll budget"
        )
    else:
        next_action = (
            "keep live admission blocked; configure and measure one of "
            "POLYMARKET_SOURCE_PROXY_URL, POLYMARKET_HTTPS_PROXY, POLYMARKET_DATA_API_BASE_URL, "
            "POLYMARKET_GAMMA_API_BASE_URL, or POLYMARKET_CLOB_API_BASE_URL, then rerun this probe"
        )

    return {
        "generated_at": _utc_now(),
        "kind": "wallet_copy_source_route_probe",
        "probe_profile": probe_profile,
        "partial_probe": partial_probe,
        "skipped_endpoints": skipped_endpoints,
        "next_action": next_action,
        "paper_only": True,
        "live_orders_allowed": False,
        "schema_version": 1,
        "status": status,
        "timeout_s": timeout_s,
        "max_wall_runtime_s": max_wall_runtime_s,
        "rate_limit_suspected": rate_limit_suspected,
        "source_proxy_configured": bool(proxy_url),
        "source_proxy_env_var": proxy_env_var or None,
        "generic_proxy_env": generic_proxy_env,
        "generic_proxy_configured": bool(generic_proxy_env.get("proxy_configured")),
        "generic_no_proxy_configured": bool(generic_proxy_env.get("no_proxy_configured")),
        "source_base_overrides": base_overrides,
        "operator_live_source_route_approval_id": live_source_route_operator_approval_id or None,
        "operator_live_source_route_approval_source": (
            LIVE_SOURCE_ROUTE_OPERATOR_APPROVAL_ENV if live_source_route_operator_approval_id else None
        ),
        "measured_base_or_proxy_route_pass": measured_base_or_proxy_route_pass,
        "measured_relay_base_route_status": (
            "PASS" if measured_base_or_proxy_route_pass else "TIMEOUT" if configured_base_route_timeout else "BLOCKED"
        ),
        "configured_base_route_timeout": configured_base_route_timeout,
        "external_route_required": external_route_required,
        "code_route_recovery_exhausted": code_route_recovery_exhausted,
        "required_operator_inputs": required_operator_inputs,
        "required_inputs": required_operator_inputs,
        "route_class_counts": {
            route_class: sum(1 for row in endpoint_results if row.get("route_class") == route_class)
            for route_class in sorted({str(row.get("route_class") or "") for row in endpoint_results})
            if route_class
        },
        "reset_by_host": {
            row["name"]: row.get("reset_attempt_count", 0)
            for row in endpoint_results
            if not row.get("control")
        },
        "reset_by_transport": {
            row["name"]: row.get("reset_by_transport", {})
            for row in endpoint_results
            if not row.get("control")
        },
        "timeout_by_host": {
            row["name"]: row.get("timeout_attempt_count", 0)
            for row in endpoint_results
            if not row.get("control")
        },
        "timeout_by_transport": {
            row["name"]: row.get("timeout_by_transport", {})
            for row in endpoint_results
            if not row.get("control")
        },
        "best_nonpassing_variants": {
            row["name"]: row.get("best_nonpassing_variant", "")
            for row in endpoint_results
            if not row.get("control")
        },
        "direct_dns_family_counts_by_endpoint": direct_dns_family_counts_by_endpoint,
        "dns_family_counts_by_endpoint": {
            row["name"]: row.get("dns_family_counts", {})
            for row in endpoint_results
        },
        "route_variant_pass_counts": {
            row["name"]: row.get("route_variant_pass_counts", {})
            for row in endpoint_results
        },
        "best_route_variants": {
            row["name"]: row.get("best_route_variant", "")
            for row in endpoint_results
        },
        "endpoints": endpoint_results,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout-s", type=float, default=5.0)
    parser.add_argument("--max-wall-runtime-s", type=float, default=0.0)
    parser.add_argument("--probe-profile", choices=("exhaustive", "heartbeat"), default="exhaustive")
    parser.add_argument("--print", action="store_true", dest="print_state")
    args = parser.parse_args()

    state = build_state(
        args.timeout_s,
        probe_profile=str(args.probe_profile),
        max_wall_runtime_s=float(args.max_wall_runtime_s),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    if args.print_state:
        print(json.dumps(state, indent=2, sort_keys=True))
    return 0 if state["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
