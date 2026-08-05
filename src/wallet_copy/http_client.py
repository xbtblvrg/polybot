"""Shared HTTP transport helpers for Polymarket source routes.

The wallet-copy workflow depends on several Polymarket hosts.  A plain
``requests.get`` call makes route resets look like ordinary missing wallet
activity, so this module centralizes retries, route variants, and transport
telemetry without weakening any live-readiness gate.
"""

from __future__ import annotations

import os
import hashlib
import time
import threading
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse, urlunparse

import requests

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - dotenv is an optional runtime nicety.
    load_dotenv = None
else:
    load_dotenv()


BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 Chrome/126 Safari/537.36"
)

SOURCE_BASE_ENV_BY_HOST = {
    "data-api.polymarket.com": "POLYMARKET_DATA_API_BASE_URL",
    "gamma-api.polymarket.com": "POLYMARKET_GAMMA_API_BASE_URL",
    "clob.polymarket.com": "POLYMARKET_CLOB_API_BASE_URL",
}

SOURCE_ROUTE_REQUIRED_OPERATOR_INPUTS = [
    "POLYMARKET_SOURCE_PROXY_URL",
    "POLYMARKET_HTTPS_PROXY",
    "POLYMARKET_DATA_API_BASE_URL",
    "POLYMARKET_GAMMA_API_BASE_URL",
    "POLYMARKET_CLOB_API_BASE_URL",
]
SOURCE_ROUTE_PROBE_COMMAND = (
    "python3 scripts/probe_polymarket_source_routes.py "
    "--output data/research/wallet_copy_source_route_state.json --print"
)


@dataclass(frozen=True)
class RouteVariant:
    name: str
    headers: dict[str, str]
    proxy_env_var: str = ""
    proxy_url: str = ""
    reuse_session: bool = True
    trust_env: bool | None = None


class PolymarketRouteError(requests.RequestException):
    """Raised when every measured route variant fails before HTTP response."""

    def __init__(self, message: str, *, route_report: dict[str, Any]):
        super().__init__(message)
        self.route_report = route_report


_SESSION_BY_VARIANT_HOST: dict[tuple[str, str, bool | None], requests.Session] = {}
_DIRECT_RESET_SUPPRESSION_BY_KEY: dict[tuple[str, str], float] = {}
_DIRECT_RESET_SUPPRESSION_LOCK = threading.Lock()


def clear_polymarket_route_reset_suppression_for_tests() -> None:
    """Clear process-local direct-route reset suppression state for tests."""

    with _DIRECT_RESET_SUPPRESSION_LOCK:
        _DIRECT_RESET_SUPPRESSION_BY_KEY.clear()


def _direct_reset_suppression_ttl_s() -> float:
    raw = os.getenv("POLYMARKET_DIRECT_RESET_SUPPRESSION_TTL_S", "120").strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 120.0


def _normalized_source_host(host: str) -> str:
    raw = str(host or "").strip().lower()
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"//{raw}")
    return (parsed.hostname or raw.split("@")[-1].split(":")[0]).lower()


def _is_known_polymarket_source_host(host: str) -> bool:
    return _normalized_source_host(host) in SOURCE_BASE_ENV_BY_HOST


def _source_route_recovery_configured(route_override: dict[str, Any]) -> bool:
    return bool(route_override.get("source_base_override_configured")) or bool(_source_proxy_url()[1])


def _direct_reset_suppression_key(host: str, request_role: str) -> tuple[str, str]:
    return (_normalized_source_host(host), str(request_role or "source_request"))


def _direct_reset_suppressed_until(key: tuple[str, str]) -> float | None:
    ttl_s = _direct_reset_suppression_ttl_s()
    if ttl_s <= 0.0:
        return None
    now = time.monotonic()
    with _DIRECT_RESET_SUPPRESSION_LOCK:
        suppressed_until = _DIRECT_RESET_SUPPRESSION_BY_KEY.get(key)
        if suppressed_until is None:
            return None
        if suppressed_until <= now:
            _DIRECT_RESET_SUPPRESSION_BY_KEY.pop(key, None)
            return None
        return suppressed_until


def _set_direct_reset_suppression(key: tuple[str, str]) -> float | None:
    ttl_s = _direct_reset_suppression_ttl_s()
    if ttl_s <= 0.0:
        return None
    suppressed_until = time.monotonic() + ttl_s
    with _DIRECT_RESET_SUPPRESSION_LOCK:
        _DIRECT_RESET_SUPPRESSION_BY_KEY[key] = suppressed_until
    return suppressed_until


def _clear_direct_reset_suppression(key: tuple[str, str]) -> None:
    with _DIRECT_RESET_SUPPRESSION_LOCK:
        _DIRECT_RESET_SUPPRESSION_BY_KEY.pop(key, None)


def _direct_reset_recovery_diagnostic(
    *,
    suppressed: bool,
    suppression_key: tuple[str, str],
    suppressed_until: float | None,
    route_override: dict[str, Any],
) -> dict[str, Any]:
    return {
        "code_route_recovery_exhausted": True,
        "direct_route_suppressed": bool(suppressed),
        "direct_route_suppression_armed": not bool(suppressed),
        "direct_route_suppression_key": "|".join(suppression_key),
        "direct_route_suppression_until_monotonic": round(float(suppressed_until), 3)
        if suppressed_until is not None
        else None,
        "external_route_required": True,
        "next_command": SOURCE_ROUTE_PROBE_COMMAND,
        "required_operator_inputs": list(SOURCE_ROUTE_REQUIRED_OPERATOR_INPUTS),
        "source_base_override_configured": bool(route_override.get("source_base_override_configured")),
        "source_proxy_configured": bool(_source_proxy_url()[1]),
        "suppression_reason": "previous_direct_reset_without_recovery_route"
        if suppressed
        else "direct_reset_without_recovery_route",
    }


def _session_for(variant: RouteVariant, host: str) -> requests.Session:
    key = (variant.name, host, variant.trust_env)
    session = _SESSION_BY_VARIANT_HOST.get(key)
    if session is None:
        session = requests.Session()
        if variant.trust_env is not None:
            session.trust_env = bool(variant.trust_env)
        _SESSION_BY_VARIANT_HOST[key] = session
    return session


def _variant_headers(base_headers: dict[str, str], variant: RouteVariant) -> dict[str, str]:
    headers = dict(base_headers)
    headers.update(variant.headers)
    return headers


def _source_proxy_url() -> tuple[str, str]:
    """Return the explicit wallet-copy source proxy, if configured.

    Requests can already honor global proxy environment variables.  The wallet
    copy workflow needs a named, auditable source-route variant so a local
    Polymarket TLS reset is not mistaken for an empty wallet feed.
    """

    for env_var in ("POLYMARKET_SOURCE_PROXY_URL", "POLYMARKET_HTTPS_PROXY"):
        value = os.getenv(env_var, "").strip()
        if value:
            return env_var, value
    return "", ""


def _variant_proxies(variant: RouteVariant) -> dict[str, str] | None:
    if not variant.proxy_url:
        return None
    return {"http": variant.proxy_url, "https": variant.proxy_url}


def _extra_direct_route_variants_enabled() -> bool:
    value = os.getenv("POLYMARKET_ENABLE_EXTRA_DIRECT_ROUTE_VARIANTS", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _source_base_override(host: str) -> tuple[str, str]:
    env_var = SOURCE_BASE_ENV_BY_HOST.get(str(host).lower(), "")
    if not env_var:
        return "", ""
    value = os.getenv(env_var, "").strip()
    if not value:
        return "", ""
    return env_var, value


def _safe_netloc(parsed) -> str:
    host = parsed.hostname or ""
    port = None
    try:
        port = parsed.port
    except ValueError:
        port = None
    if port is not None:
        return f"{host}:{port}" if host else str(port)
    return host


def redact_source_url_for_state(value: str) -> dict[str, Any]:
    """Return URL metadata safe for persisted source-route state files."""

    raw = str(value or "").strip()
    empty = {
        "url": None,
        "scheme": None,
        "host": None,
        "path": None,
        "contains_sensitive_material": False,
    }
    if not raw:
        return empty
    parsed = urlparse(raw)
    parse_target = raw
    if not parsed.scheme and not parsed.netloc:
        parse_target = f"//{raw}"
        parsed = urlparse(parse_target)
    netloc = _safe_netloc(parsed)
    path = parsed.path or ""
    redacted = urlunparse((parsed.scheme, netloc, path, "", "", ""))
    if not parsed.scheme and parse_target.startswith("//"):
        redacted = redacted.removeprefix("//")
    return {
        "url": redacted or None,
        "scheme": parsed.scheme or None,
        "host": netloc or None,
        "path": path or None,
        "contains_sensitive_material": bool(parsed.username or parsed.password or parsed.query),
    }


def _redacted_source_base_url(value: str) -> dict[str, Any]:
    """Return persisted source-route metadata without credentials or query tokens."""

    redacted = redact_source_url_for_state(value)
    return {
        "base_url": redacted["url"],
        "base_url_redacted": redacted["url"],
        "base_url_scheme": redacted["scheme"],
        "base_url_host": redacted["host"],
        "base_url_path": redacted["path"],
        "base_url_contains_sensitive_material": redacted["contains_sensitive_material"],
    }


def source_base_overrides() -> dict[str, dict[str, Any]]:
    """Return configured host-specific source relay/base URL overrides."""

    out: dict[str, dict[str, Any]] = {}
    for host, env_var in SOURCE_BASE_ENV_BY_HOST.items():
        value = os.getenv(env_var, "").strip()
        out[host] = {
            "configured": bool(value),
            "env_var": env_var,
            **_redacted_source_base_url(value),
        }
    return out


def resolve_polymarket_source_url(url: str) -> tuple[str, dict[str, Any]]:
    """Apply audited host-specific Polymarket source base overrides.

    This is for measured source-route recovery only. It preserves the original
    endpoint path and query while routing known Polymarket hosts through an
    explicitly configured mirror/relay base URL.
    """

    original = str(url)
    parsed = urlparse(original)
    env_var, base_url = _source_base_override(parsed.netloc)
    if not base_url:
        return original, {
            "original_host": parsed.netloc,
            "routed_host": parsed.netloc,
            "source_base_override_configured": False,
            "source_base_override_env_var": None,
        }

    base = urlparse(base_url)
    base_path = base.path.rstrip("/")
    original_path = parsed.path or "/"
    routed_path = f"{base_path}/{original_path.lstrip('/')}" if base_path else original_path
    routed = urlunparse(
        (
            base.scheme or parsed.scheme,
            base.netloc,
            routed_path,
            "",
            parsed.query,
            parsed.fragment,
        )
    )
    safe_routed = redact_source_url_for_state(routed)
    return routed, {
        "original_host": parsed.netloc,
        "routed_host": safe_routed["host"],
        "routed_url_redacted": safe_routed["url"],
        "routed_url_contains_sensitive_material": safe_routed["contains_sensitive_material"],
        "source_base_override_configured": True,
        "source_base_override_env_var": env_var,
    }


def _attempt_text(attempts: list[dict[str, Any]]) -> str:
    return " ".join(str(attempt.get("error") or "") for attempt in attempts if isinstance(attempt, dict)).lower()


def _query_param_keys(params: dict[str, Any] | None) -> list[str]:
    if not isinstance(params, dict):
        return []
    return sorted(str(key) for key in params)


def _request_fingerprint(
    *,
    method: str,
    original_url: str,
    routed_url: str,
    params: dict[str, Any] | None,
    request_role: str,
) -> str:
    payload = "|".join(
        [
            str(method).upper(),
            str(original_url),
            str(routed_url),
            ",".join(_query_param_keys(params)),
            str(request_role or "source_request"),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _timeout_pair(
    *,
    timeout_s: float | tuple[float, float] | None,
    connect_timeout_s: float | None,
    read_timeout_s: float | None,
) -> tuple[float, float]:
    if isinstance(timeout_s, tuple):
        raw_connect, raw_read = timeout_s
    else:
        fallback = float(timeout_s if timeout_s is not None else 8.0)
        raw_connect = fallback
        raw_read = fallback
    if connect_timeout_s is not None:
        raw_connect = float(connect_timeout_s)
    if read_timeout_s is not None:
        raw_read = float(read_timeout_s)
    connect = min(10.0, max(0.001, float(raw_connect)))
    read = min(30.0, max(0.001, float(raw_read)))
    return connect, read


def _reset_attempt_count(attempts: list[dict[str, Any]]) -> int:
    count = 0
    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        text = " ".join(
            str(attempt.get(key) or "")
            for key in ("error", "exception", "stderr_tail", "status")
        ).lower()
        if "connection reset" in text or "connectionreseterror" in text or "recv failure" in text:
            count += 1
    return count


def _route_report_base(
    *,
    attempts: list[dict[str, Any]],
    elapsed_ms_total: float,
    host: str,
    method: str,
    original_url: str,
    params: dict[str, Any] | None,
    request_role: str,
    routed_url: str,
) -> dict[str, Any]:
    fingerprint = _request_fingerprint(
        method=method,
        original_url=original_url,
        routed_url=routed_url,
        params=params,
        request_role=request_role,
    )
    return {
        "attempt_count": len(attempts),
        "elapsed_ms_total": round(float(elapsed_ms_total), 3),
        "host": host,
        "method": str(method).upper(),
        "query_param_keys": _query_param_keys(params),
        "request_fingerprint": fingerprint,
        "request_role": str(request_role or "source_request"),
        "reset_attempt_count": _reset_attempt_count(attempts),
        "route_report_id": f"rr_{fingerprint}",
    }


def classify_route_report(
    *,
    status: str,
    attempts: list[dict[str, Any]],
    route_override: dict[str, Any],
    proxy_configured: bool = False,
) -> str:
    """Classify source route truth without turning recovered routes into green.

    A relay/proxy can be useful paper measurement infrastructure, but it is a
    degraded recovery path until direct source truth is healthy and candidate
    proof carries the route provenance.
    """

    normalized_status = str(status or "").upper()
    override_configured = bool(route_override.get("source_base_override_configured"))
    if normalized_status == "PASS":
        if override_configured:
            return "ROUTE_RECOVERED_DEGRADED"
        if proxy_configured:
            return "PROXY_RECOVERED_DEGRADED"
        return "DIRECT_PASS"
    if normalized_status == "RATE_LIMIT_SUSPECTED":
        return "DIRECT_RATE_LIMIT_SUSPECTED"
    if "connection reset" in _attempt_text(attempts) or "connectionreseterror" in _attempt_text(attempts):
        return "DIRECT_RESET"
    return "DIRECT_ERROR"


class PolymarketHttpClient:
    """Measured request client with route variants for Polymarket APIs."""

    def __init__(
        self,
        *,
        timeout_s: float | tuple[float, float] = 8.0,
        connect_timeout_s: float | None = None,
        read_timeout_s: float | None = None,
        retries: int = 3,
        user_agent: str = "polymarket-wallet-copy/1.0",
    ) -> None:
        self.timeout_s = _timeout_pair(
            timeout_s=timeout_s,
            connect_timeout_s=connect_timeout_s,
            read_timeout_s=read_timeout_s,
        )
        self.connect_timeout_s = self.timeout_s[0]
        self.read_timeout_s = self.timeout_s[1]
        self.retries = max(1, int(retries))
        self.user_agent = str(user_agent or "polymarket-wallet-copy/1.0")

    def variants(self, *, route_override: dict[str, Any] | None = None) -> tuple[RouteVariant, ...]:
        env_var, proxy_url = _source_proxy_url()
        default_variant = RouteVariant(
            name="session_default_ua",
            headers={"Accept": "application/json", "User-Agent": self.user_agent},
            reuse_session=True,
        )
        browser_session_variant = RouteVariant(
            name="session_browser_ua_connection_close",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Connection": "close",
                "User-Agent": BROWSER_USER_AGENT,
            },
            reuse_session=True,
        )
        fresh_browser_variant = RouteVariant(
            name="fresh_browser_ua_connection_close",
            headers={
                "Accept": "application/json,text/plain,*/*",
                "Connection": "close",
                "User-Agent": BROWSER_USER_AGENT,
            },
            reuse_session=False,
        )
        if bool((route_override or {}).get("source_base_override_configured")):
            variants = [browser_session_variant, default_variant, fresh_browser_variant]
        else:
            variants = [default_variant, browser_session_variant, fresh_browser_variant]
        if proxy_url:
            variants.append(
                RouteVariant(
                    name="source_proxy_browser_ua_connection_close",
                    headers={
                        "Accept": "application/json,text/plain,*/*",
                        "Connection": "close",
                        "User-Agent": BROWSER_USER_AGENT,
                    },
                    proxy_env_var=env_var,
                    proxy_url=proxy_url,
                    reuse_session=False,
                )
            )
        if _extra_direct_route_variants_enabled():
            variants.extend(
                [
                    RouteVariant(
                        name="fresh_browser_ua_compressed_connection_close",
                        headers={
                            "Accept": "application/json,text/plain,*/*",
                            "Accept-Encoding": "gzip, deflate, br",
                            "Connection": "close",
                            "User-Agent": BROWSER_USER_AGENT,
                        },
                        reuse_session=False,
                    ),
                    RouteVariant(
                        name="fresh_browser_ua_trust_env_false",
                        headers={
                            "Accept": "application/json,text/plain,*/*",
                            "Connection": "close",
                            "User-Agent": BROWSER_USER_AGENT,
                        },
                        reuse_session=False,
                        trust_env=False,
                    ),
                ]
            )
        return tuple(variants)

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json_payload: Any | None = None,
        headers: dict[str, str] | None = None,
        request_role: str = "source_request",
        timeout_s: float | tuple[float, float] | None = None,
        connect_timeout_s: float | None = None,
        read_timeout_s: float | None = None,
    ) -> requests.Response:
        original_url = str(url)
        routed_url, route_override = resolve_polymarket_source_url(original_url)
        request_host = urlparse(routed_url).netloc
        host = str(route_override.get("routed_host") or request_host)
        routed_url_redacted = str(
            route_override.get("routed_url_redacted") or redact_source_url_for_state(routed_url)["url"] or routed_url
        )
        original_host = str(route_override.get("original_host") or urlparse(original_url).netloc)
        recovery_configured = _source_route_recovery_configured(route_override)
        suppression_key = _direct_reset_suppression_key(original_host, request_role)
        request_started = time.perf_counter()
        attempts: list[dict[str, Any]] = []
        last_exc: requests.RequestException | None = None
        last_response: requests.Response | None = None
        timeout = _timeout_pair(
            timeout_s=timeout_s if timeout_s is not None else self.timeout_s,
            connect_timeout_s=connect_timeout_s,
            read_timeout_s=read_timeout_s,
        )
        base_headers = dict(headers or {})

        suppressed_until = (
            _direct_reset_suppressed_until(suppression_key)
            if _is_known_polymarket_source_host(original_host) and not recovery_configured
            else None
        )
        if suppressed_until is not None:
            attempts.append(
                {
                    "direct_route_suppressed": True,
                    "elapsed_ms": 0.0,
                    "error": (
                        "direct Polymarket route reset suppression active; configure measured "
                        "proxy/base override before retrying direct source route"
                    ),
                    "exception": "RouteResetSuppressed",
                    "proxy_configured": False,
                    "proxy_env_var": None,
                    "request_role": request_role,
                    "retry_index": 0,
                    "source_base_override_configured": bool(route_override.get("source_base_override_configured")),
                    "source_base_override_env_var": route_override.get("source_base_override_env_var"),
                    "status": "ERROR",
                    "variant": "direct_route_reset_suppression",
                }
            )
            route_report = {
                "attempts": attempts,
                "best_variant": None,
                **_route_report_base(
                    attempts=attempts,
                    elapsed_ms_total=(time.perf_counter() - request_started) * 1000.0,
                    host=host,
                    method=method,
                    original_url=original_url,
                    params=params,
                    request_role=request_role,
                    routed_url=routed_url_redacted,
                ),
                **route_override,
                "route_class": "DIRECT_RESET",
                "status": "TRANSPORT_ERROR",
                **_direct_reset_recovery_diagnostic(
                    suppressed=True,
                    suppression_key=suppression_key,
                    suppressed_until=suppressed_until,
                    route_override=route_override,
                ),
            }
            raise PolymarketRouteError(
                f"direct Polymarket route reset suppression active for {original_host}; configure measured source route",
                route_report=route_report,
            )

        for retry_index in range(self.retries):
            for variant in self.variants(route_override=route_override):
                started = time.perf_counter()
                try:
                    session = _session_for(variant, host) if variant.reuse_session else requests.Session()
                    response = session.request(
                        str(method).upper(),
                        routed_url,
                        params=params,
                        json=json_payload,
                        timeout=timeout,
                        headers=_variant_headers(base_headers, variant),
                        proxies=_variant_proxies(variant),
                    )
                except requests.RequestException as exc:
                    last_exc = exc
                    error_text = str(exc)[:500].replace(routed_url, routed_url_redacted)
                    attempts.append(
                        {
                            "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
                            "error": error_text,
                            "exception": type(exc).__name__,
                            "timeout_connect_s": round(timeout[0], 6),
                            "timeout_read_s": round(timeout[1], 6),
                            "proxy_env_var": variant.proxy_env_var or None,
                            "proxy_configured": bool(variant.proxy_url),
                            "retry_index": retry_index,
                            "source_base_override_configured": bool(
                                route_override.get("source_base_override_configured")
                            ),
                            "request_role": request_role,
                            "source_base_override_env_var": route_override.get("source_base_override_env_var"),
                            "status": "ERROR",
                            "variant": variant.name,
                        }
                    )
                    continue

                last_response = response
                status = "PASS" if 200 <= response.status_code < 300 else "HTTP_NON_2XX"
                attempts.append(
                    {
                        "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
                        "http_status": response.status_code,
                        "timeout_connect_s": round(timeout[0], 6),
                        "timeout_read_s": round(timeout[1], 6),
                        "proxy_env_var": variant.proxy_env_var or None,
                        "proxy_configured": bool(variant.proxy_url),
                        "retry_index": retry_index,
                        "request_role": request_role,
                        "source_base_override_configured": bool(
                            route_override.get("source_base_override_configured")
                        ),
                        "source_base_override_env_var": route_override.get("source_base_override_env_var"),
                        "status": status,
                        "variant": variant.name,
                    }
                )
                report = {
                    "attempts": attempts,
                    "best_variant": variant.name,
                    **_route_report_base(
                        attempts=attempts,
                        elapsed_ms_total=(time.perf_counter() - request_started) * 1000.0,
                        host=host,
                        method=method,
                        original_url=original_url,
                        params=params,
                        request_role=request_role,
                        routed_url=routed_url_redacted,
                    ),
                    **route_override,
                    "route_class": classify_route_report(
                        status=status,
                        attempts=attempts,
                        route_override=route_override,
                        proxy_configured=bool(variant.proxy_url),
                    ),
                    "status": status,
                    "timeout_connect_s": round(timeout[0], 6),
                    "timeout_read_s": round(timeout[1], 6),
                }
                setattr(response, "wallet_copy_route_report", report)
                if status == "PASS":
                    _clear_direct_reset_suppression(suppression_key)
                    return response

            if retry_index + 1 < self.retries:
                time.sleep(min(0.25 * (2**retry_index), 1.0))

        if last_response is not None:
            setattr(
                last_response,
                "wallet_copy_route_report",
                {
                    "attempts": attempts,
                    "best_variant": None,
                    **_route_report_base(
                        attempts=attempts,
                        elapsed_ms_total=(time.perf_counter() - request_started) * 1000.0,
                        host=host,
                        method=method,
                        original_url=original_url,
                        params=params,
                        request_role=request_role,
                        routed_url=routed_url_redacted,
                    ),
                    **route_override,
                    "route_class": classify_route_report(
                        status="HTTP_NON_2XX",
                        attempts=attempts,
                        route_override=route_override,
                    ),
                    "status": "HTTP_NON_2XX",
                    "timeout_connect_s": round(timeout[0], 6),
                    "timeout_read_s": round(timeout[1], 6),
                },
            )
            return last_response

        route_class = classify_route_report(
            status="TRANSPORT_ERROR",
            attempts=attempts,
            route_override=route_override,
        )
        diagnostic: dict[str, Any] = {}
        if (
            route_class == "DIRECT_RESET"
            and _is_known_polymarket_source_host(original_host)
            and not recovery_configured
        ):
            suppressed_until = _set_direct_reset_suppression(suppression_key)
            diagnostic = _direct_reset_recovery_diagnostic(
                suppressed=False,
                suppression_key=suppression_key,
                suppressed_until=suppressed_until,
                route_override=route_override,
            )

        route_report = {
            "attempts": attempts,
            "best_variant": None,
            **_route_report_base(
                attempts=attempts,
                elapsed_ms_total=(time.perf_counter() - request_started) * 1000.0,
                host=host,
                method=method,
                original_url=original_url,
                params=params,
                request_role=request_role,
                routed_url=routed_url_redacted,
            ),
            **route_override,
            "route_class": route_class,
            "status": "TRANSPORT_ERROR",
            "timeout_connect_s": round(timeout[0], 6),
            "timeout_read_s": round(timeout[1], 6),
            **diagnostic,
        }
        last_exc_text = str(last_exc).replace(routed_url, routed_url_redacted)
        raise PolymarketRouteError(
            f"all Polymarket route variants failed for {host}: {last_exc_text}",
            route_report=route_report,
        )

    def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        request_role: str = "source_request",
        timeout_s: float | tuple[float, float] | None = None,
        connect_timeout_s: float | None = None,
        read_timeout_s: float | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        response = self.request(
            "GET",
            url,
            params=params,
            headers=headers,
            request_role=request_role,
            timeout_s=timeout_s,
            connect_timeout_s=connect_timeout_s,
            read_timeout_s=read_timeout_s,
        )
        response.raise_for_status()
        return response.json(), getattr(response, "wallet_copy_route_report", {})
