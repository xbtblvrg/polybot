"""Source-route status helpers for wallet-copy measurement gates."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional runtime nicety.
    load_dotenv = None
else:  # pragma: no cover - environment setup.
    load_dotenv()


DEFAULT_AUTONOMOUS_REPAIR_PROGRESS_STATE = "data/research/wallet_copy_autonomous_repair_command_progress.json"
ACTIVE_AUTONOMOUS_REPAIR_PROGRESS_STATUSES = {"STARTING", "RUNNING"}

MEASUREMENT_ALLOWED_SOURCE_ROUTE_STATUSES = {
    "PASS",
    "POLYMARKET_ROUTE_RECOVERED_DEGRADED",
    "POLYMARKET_PROXY_RECOVERED_DEGRADED",
}

DEGRADED_RECOVERED_SOURCE_ROUTE_STATUSES = {
    "POLYMARKET_ROUTE_RECOVERED_DEGRADED",
    "POLYMARKET_PROXY_RECOVERED_DEGRADED",
}

LIVE_ADMISSIBLE_SOURCE_ROUTE_STATUSES = {"PASS"}
LIVE_SOURCE_ROUTE_OPERATOR_APPROVAL_ENV = "WALLET_COPY_OPERATOR_APPROVED_LIVE_SOURCE_ROUTE"


def source_route_status(source_route: dict[str, Any] | str | None) -> str:
    """Return the normalized source-route status string."""

    if isinstance(source_route, dict):
        return str(source_route.get("status") or "")
    return str(source_route or "")


def source_route_allows_measurement(source_route: dict[str, Any] | str | None) -> bool:
    """Return whether read-source measurement may continue.

    Degraded recovered routes are good enough for paper/proof measurement, but
    they are not equivalent to direct PASS for live-readiness.
    """

    return source_route_status(source_route) in MEASUREMENT_ALLOWED_SOURCE_ROUTE_STATUSES


def source_route_is_recovered_degraded(source_route: dict[str, Any] | str | None) -> bool:
    """Return whether the route is a measured degraded recovery path."""

    return source_route_status(source_route) in DEGRADED_RECOVERED_SOURCE_ROUTE_STATUSES


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "approved"}


def source_route_live_operator_approval(source_route: dict[str, Any] | str | None) -> str:
    """Return explicit operator approval id for a degraded live source route."""

    if isinstance(source_route, dict):
        for key in (
            "operator_live_source_route_approval_id",
            "live_source_route_operator_approval_id",
            "live_source_route_approval_id",
        ):
            value = str(source_route.get(key) or "").strip()
            if value:
                return value
        if _truthy(source_route.get("operator_live_source_route_approved")):
            return "state_operator_approved"
    return os.getenv(LIVE_SOURCE_ROUTE_OPERATOR_APPROVAL_ENV, "").strip()


def source_route_allows_live_execution(source_route: dict[str, Any] | str | None) -> bool:
    """Return whether source route truth is good enough for guarded live copy.

    A recovered relay/proxy route can keep paper measurement moving, but live
    copy requires a direct PASS or a separately promoted live-admissible route.
    """

    status = source_route_status(source_route)
    if status in LIVE_ADMISSIBLE_SOURCE_ROUTE_STATUSES:
        return True
    if status not in DEGRADED_RECOVERED_SOURCE_ROUTE_STATUSES:
        return False
    if not isinstance(source_route, dict):
        return False
    if not source_route_live_operator_approval(source_route):
        return False
    return bool(source_route.get("measured_base_or_proxy_route_pass"))


def source_route_probe_progress_blocker(
    progress_state: str | Path | None = None,
    *,
    max_age_s: float = 900.0,
) -> dict[str, Any] | None:
    """Return a route-blocker when the autonomous source-route probe is running.

    Guards use this to avoid competing with the source-route proof itself. A
    recovered degraded route can allow ordinary measurement, but not while the
    fresh source-route probe is actively trying to establish the current truth.
    """

    path = Path(progress_state or DEFAULT_AUTONOMOUS_REPAIR_PROGRESS_STATE)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if str(payload.get("status") or "").upper() != "RUNNING":
        return None
    if str(payload.get("name") or "") != "source_route_probe":
        return None

    pid = 0
    try:
        pid = int(payload.get("pid") or 0)
    except (TypeError, ValueError):
        pid = 0
    try:
        age_s = time.time() - path.stat().st_mtime
    except OSError:
        return None
    if age_s > max(0.0, float(max_age_s)):
        return None
    if pid > 0:
        try:
            os.kill(pid, 0)
        except OSError:
            return None

    return {
        "status": "SOURCE_ROUTE_PROBE_RUNNING",
        "name": "source_route_probe",
        "progress_state": str(path),
        "pid": pid or None,
        "generated_at": payload.get("generated_at"),
        "elapsed_s": payload.get("elapsed_s"),
        "paper_only": True,
        "live_orders_allowed": False,
    }


def autonomous_repair_progress_blocker(
    progress_state: str | Path | None = None,
    *,
    max_age_s: float = 900.0,
) -> dict[str, Any] | None:
    """Return a blocker while the main autonomous repair loop is active.

    Background guards use this to avoid competing with the repair loop's
    memory-heavy live tracker children. Stale progress is ignored so a crashed
    or completed repair does not permanently pause paper measurement.
    """

    path = Path(progress_state or DEFAULT_AUTONOMOUS_REPAIR_PROGRESS_STATE)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    progress_status = str(payload.get("status") or "").upper()
    if progress_status not in ACTIVE_AUTONOMOUS_REPAIR_PROGRESS_STATUSES:
        return None

    pid = 0
    try:
        pid = int(payload.get("pid") or 0)
    except (TypeError, ValueError):
        pid = 0
    try:
        age_s = time.time() - path.stat().st_mtime
    except OSError:
        return None
    if age_s > max(0.0, float(max_age_s)):
        return None
    if pid > 0:
        try:
            os.kill(pid, 0)
        except OSError:
            return None

    return {
        "status": "AUTONOMOUS_REPAIR_RUNNING",
        "name": payload.get("name"),
        "progress_status": progress_status,
        "progress_state": str(path),
        "pid": pid or None,
        "generated_at": payload.get("generated_at"),
        "elapsed_s": payload.get("elapsed_s"),
        "paper_only": True,
        "live_orders_allowed": False,
    }
