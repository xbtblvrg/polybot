#!/usr/bin/env python3
"""Probe the live CLOB auth path with a read-only call.

This intentionally mirrors the TradeExecutor credential derivation path and
does not place, cancel, or modify orders.
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config import Config


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return repr(value)


def _http_status(exc: BaseException) -> int | None:
    for attr in ("status", "status_code", "code"):
        raw = getattr(exc, attr, None)
        if raw is not None:
            try:
                return int(raw)
            except (TypeError, ValueError):
                pass
    response = getattr(exc, "response", None)
    if response is not None:
        raw = getattr(response, "status_code", None) or getattr(response, "status", None)
        if raw is not None:
            try:
                return int(raw)
            except (TypeError, ValueError):
                pass
    return None


def run_probe() -> dict[str, Any]:
    try:
        from py_clob_client_v2.client import ClobClient
        from py_clob_client_v2.clob_types import OpenOrderParams
        client_module = "py_clob_client_v2"
    except ImportError:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OpenOrderParams
        client_module = "py_clob_client"

    cfg = Config()
    cfg.validate_execution_ready()
    funder = cfg.polymarket_proxy if cfg.polymarket_proxy else None
    signature_type = 1 if funder else 0
    client = ClobClient(
        host=cfg.clob_host,
        key=cfg.private_key,
        chain_id=cfg.chain_id,
        funder=funder,
        signature_type=signature_type,
    )

    credential_method = None
    if hasattr(client, "create_or_derive_api_key"):
        credential_method = "create_or_derive_api_key"
        creds = client.create_or_derive_api_key()
    elif hasattr(client, "create_or_derive_api_creds"):
        credential_method = "create_or_derive_api_creds"
        creds = client.create_or_derive_api_creds()
    elif hasattr(client, "derive_api_key"):
        credential_method = "derive_api_key"
        creds = client.derive_api_key()
    else:
        raise RuntimeError("ClobClient has no supported API credential derivation method")

    client.set_api_creds(creds)
    if hasattr(client, "get_orders"):
        read_call = "get_orders(OpenOrderParams())"
        orders = client.get_orders(OpenOrderParams())
    elif hasattr(client, "get_open_orders"):
        read_call = "get_open_orders()"
        orders = client.get_open_orders()
    elif hasattr(client, "get_api_keys"):
        read_call = "get_api_keys()"
        orders = client.get_api_keys()
    else:
        raise RuntimeError("ClobClient has no supported read-only authenticated probe method")
    return {
        "status": "PASS",
        "generated_at": _now_iso(),
        "flow_stage": "LIVE/DEFEND",
        "probe": "clob_auth_readonly",
        "client_module": client_module,
        "credential_method": credential_method,
        "call": read_call,
        "host": cfg.clob_host,
        "chain_id": cfg.chain_id,
        "signature_type": signature_type,
        "funder_present": bool(funder),
        "open_order_count": len(orders) if orders else 0,
        "orders_type": type(orders).__name__,
        "mutation": "none",
        "secret_material_recorded": False,
        "ruling_branch": "PASS_SCORECARD_LANE_NUISANCE",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default="data/research/clob_auth_readonly_probe_latest.json",
        help="Path to write the probe result JSON.",
    )
    args = parser.parse_args()

    try:
        result = run_probe()
        rc = 0
    except Exception as exc:  # pragma: no cover - exercised against live service.
        status = _http_status(exc)
        result = {
            "status": "FAIL",
            "generated_at": _now_iso(),
            "flow_stage": "LIVE/DEFEND",
            "probe": "clob_auth_readonly",
            "call": "derive creds + get_orders(OpenOrderParams())",
            "http_status": status,
            "exception_type": type(exc).__name__,
            "exception": str(exc)[:1000],
            "traceback_tail": traceback.format_exc().splitlines()[-8:],
            "mutation": "none",
            "secret_material_recorded": False,
            "ruling_branch": "FAIL_LIVE_CLOB_CREDENTIAL_PATH_REPAIR_TOP_QUEUE"
            if status in {400, 401}
            else "FAIL_UNCLASSIFIED_AUTH_PROBE",
        }
        rc = 1

    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_jsonable(result), indent=2, sort_keys=True) + "\n")
    print(json.dumps(_jsonable(result), indent=2, sort_keys=True))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
