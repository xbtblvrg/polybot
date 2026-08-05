#!/usr/bin/env python3
"""Redeem own-wallet resolved positions through the Polymarket relayer."""

from __future__ import annotations

import argparse
import inspect
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional runtime nicety.
    load_dotenv = None
else:
    load_dotenv(".env")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.own_positions import (
    POLYMARKET_CTF,
    append_redeem_event,
    ledger_estimate_redeem_candidates_from_report,
    redeem_calldata,
    redeem_candidates_from_report,
)
from src.wallet_copy.store import atomic_write_json, load_json


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--positions", default="data/research/own_positions_latest.json")
    parser.add_argument("--output", default="data/research/own_redeemer_state.json")
    parser.add_argument("--events", default="data/research/own_redeem_events.jsonl")
    parser.add_argument("--post-redeem-scorecard", default="data/research/wallet_copy_post_redeem_scorecard_latest.json")
    parser.add_argument("--execute", action="store_true", help="Broadcast via Polymarket relayer when candidates exist.")
    parser.add_argument("--wait", action="store_true", help="Wait for relayer mined/confirmed state after submit.")
    parser.add_argument(
        "--ledger-estimate-fallback",
        action="store_true",
        help="When Data API positions are unavailable, redeem ledger-estimated resolved winning conditions.",
    )
    parser.add_argument("--max-conditions", type=int, default=20)
    return parser.parse_args()


def _env(name: str, fallback: str = "") -> str:
    return os.getenv(name, fallback).strip()


def _redact(value: str) -> str:
    text = str(value or "")
    if len(text) <= 14:
        return text
    return f"{text[:6]}...{text[-4:]}"


def _builder_config():
    from py_builder_signing_sdk.config import BuilderConfig
    from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds

    key = _env("BUILDER_API_KEY")
    secret = _env("BUILDER_SECRET")
    passphrase = _env("BUILDER_PASSPHRASE") or _env("BUILDER_PASS_PHRASE")
    missing = [
        name
        for name, value in (
            ("BUILDER_API_KEY", key),
            ("BUILDER_SECRET", secret),
            ("BUILDER_PASSPHRASE", passphrase),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(f"missing builder credentials: {','.join(missing)}")
    return BuilderConfig(local_builder_creds=BuilderApiKeyCreds(key=key, secret=secret, passphrase=passphrase))


def _load_relayer_symbols():
    from py_builder_relayer_client.client import RelayClient
    try:
        from py_builder_relayer_client.models import RelayerTxType, Transaction

        return RelayClient, RelayerTxType, Transaction
    except ImportError:
        from py_builder_relayer_client.models import OperationType, SafeTransaction

        def Transaction(*, to: str, data: str, value: str):
            return SafeTransaction(to=to, operation=OperationType.Call, data=data, value=value)

        return RelayClient, None, Transaction


def _post_redeem_scorecard(path: Path) -> dict[str, Any]:
    env = dict(os.environ)
    env.setdefault("WALLET_COPY_BALANCE_MISMATCH_RESAMPLE_COUNT", "0")
    proc = subprocess.run(
        [sys.executable, "scripts/daily_scorecard.py", "--format", "json", "--balance-sample-count", "1"],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        check=False,
        timeout=90,
        env=env,
    )
    if proc.returncode != 0:
        return {
            "status": "ERROR",
            "returncode": proc.returncode,
            "stderr_tail": proc.stderr[-1200:],
        }
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        return {"status": "ERROR", "error": f"JSONDecodeError: {exc}"}
    atomic_write_json(path, payload)
    return {
        "status": "PASS",
        "path": str(path),
        "generated_at": payload.get("generated_at") if isinstance(payload, dict) else None,
    }


def _execute_redeem_batch(candidates: list[dict[str, Any]], *, wait: bool) -> dict[str, Any]:
    RelayClient, RelayerTxType, Transaction = _load_relayer_symbols()
    private_key = _env("PRIVATE_KEY")
    if not private_key:
        raise RuntimeError("PRIVATE_KEY missing")
    chain_id = int(_env("CHAIN_ID", "137"))
    relayer_url = _env("RELAYER_URL", "https://relayer-v2.polymarket.com/")
    rpc_url = _env("POLYGON_RPC_URL") or None
    proxy = _env("POLYMARKET_PROXY").lower()
    client_kwargs: dict[str, Any] = {"builder_config": _builder_config()}
    client_params = inspect.signature(RelayClient).parameters
    if "relay_tx_type" in client_params and RelayerTxType is not None:
        client_kwargs["relay_tx_type"] = RelayerTxType.PROXY
    if "rpc_url" in client_params:
        client_kwargs["rpc_url"] = rpc_url
    client = RelayClient(relayer_url, chain_id, private_key, **client_kwargs)
    expected_safe = client.get_expected_safe().lower()
    expected_proxy_getter = getattr(client, "get_expected_proxy_wallet", None)
    expected_proxy = expected_proxy_getter().lower() if callable(expected_proxy_getter) else (proxy or expected_safe)
    if callable(expected_proxy_getter) and proxy and proxy != expected_proxy:
        raise RuntimeError(
            f"POLYMARKET_PROXY {_redact(proxy)} does not match derived proxy {_redact(expected_proxy)}; "
            f"derived safe {_redact(expected_safe)}"
        )
    transactions = [
        Transaction(to=POLYMARKET_CTF, data=redeem_calldata(str(row["condition_id"])), value="0")
        for row in candidates
    ]
    response = client.execute(
        transactions,
        metadata=f"wallet-copy auto-redeem {len(candidates)} conditions",
    )
    waited = response.wait() if wait else None
    terminal_state = waited.get("state") if isinstance(waited, dict) else None
    return {
        "status": terminal_state or "SUBMITTED",
        "transaction_id": getattr(response, "transaction_id", None),
        "transaction_hash": getattr(response, "transaction_hash", None) or getattr(response, "hash", None),
        "waited": bool(wait),
        "wait_result": waited if isinstance(waited, dict) else None,
        "relay_tx_type": "PROXY",
        "proxy_wallet": _redact(expected_proxy),
        "condition_count": len(candidates),
    }


def _is_zero_position_balance_error(exc: Exception) -> bool:
    return "zero position balance" in str(exc).lower()


def _is_zero_position_balance_precheck(result: Any) -> bool:
    try:
        text = json.dumps(result, sort_keys=True, default=str).lower()
    except TypeError:
        text = str(result).lower()
    return "precheck_skipped" in text and "zero position balance" in text


def _zero_balance_skip_update(*, result: dict[str, Any] | None = None) -> dict[str, Any]:
    update = {
        "status": "SKIPPED_ZERO_BALANCE",
        "executed": False,
        "skipped": True,
        "skip_reason": "PRECHECK_SKIPPED_ZERO_POSITION_BALANCE",
        "next_action": "treat stale ledger-estimate zero-balance redemption as benign; keep own-position visibility refresh running",
    }
    if result is not None:
        update["result"] = result
    return update


def _is_proxy_sdk_unsupported_error(message: str) -> bool:
    text = str(message).lower()
    return "expected safe" in text and "is not deployed" in text


def _proxy_sdk_unsupported_update(error: str) -> dict[str, Any]:
    return {
        "status": "RELAYER_PROXY_SDK_UNSUPPORTED",
        "executed": False,
        "skipped": True,
        "error": error,
        "next_action": (
            "pin/install a proxy-capable py_builder_relayer_client or build/test direct proxy "
            "redemption after Fable direction; keep own-position visibility refresh running"
        ),
    }


def _quota_reset_seconds(message: str) -> float | None:
    if "quota exceeded" not in str(message).lower():
        return None
    match = re.search(r"resets in\s+([0-9]+(?:\.[0-9]+)?)\s+seconds", str(message), flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _quota_state_from_error(message: str, *, base_ts: datetime | None = None) -> dict[str, Any]:
    reset_s = _quota_reset_seconds(message)
    base = base_ts or datetime.now(timezone.utc)
    next_retry = base + timedelta(seconds=max(0.0, float(reset_s or 0.0)))
    return {
        "status": "RELAYER_QUOTA_EXHAUSTED",
        "relayer_quota_reset_s": reset_s,
        "next_retry_at": next_retry.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "next_action": "skip relayer redemption attempts until next_retry_at; keep own-position visibility refresh running",
    }


def _active_quota_wait(previous: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(previous, dict):
        return {}
    message = str(previous.get("error") or previous.get("batch_error") or "")
    if previous.get("status") != "RELAYER_QUOTA_EXHAUSTED" and "quota exceeded" not in message.lower():
        return {}
    next_retry = _parse_iso(previous.get("next_retry_at"))
    if next_retry is None and message:
        generated = _parse_iso(previous.get("generated_at")) or datetime.now(timezone.utc)
        quota = _quota_state_from_error(message, base_ts=generated)
        next_retry = _parse_iso(quota.get("next_retry_at"))
    if next_retry is None or next_retry <= datetime.now(timezone.utc):
        return {}
    return {
        "status": "RELAYER_QUOTA_EXHAUSTED",
        "next_retry_at": next_retry.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "previous_status": previous.get("status"),
        "previous_error": message,
    }


def _estimated_value(candidates: list[dict[str, Any]]) -> float:
    return round(sum(float(row.get("estimated_value_usd") or 0.0) for row in candidates), 6)


def _redeem_event(
    *,
    status: str,
    candidates: list[dict[str, Any]],
    candidate_source: str,
    result: dict[str, Any] | None = None,
    error: str = "",
) -> dict[str, Any]:
    result = result if isinstance(result, dict) else {}
    event = {
        "ts": _utc_now_iso(),
        "status": status,
        "conditions": candidates,
        "transaction_id": result.get("transaction_id"),
        "transaction_hash": result.get("transaction_hash"),
        "relay_tx_type": result.get("relay_tx_type"),
        "candidate_source": candidate_source,
        "estimated_redeemed_usd": _estimated_value(candidates),
    }
    if error:
        event["error"] = error
    return event


def _execute_redeem_isolated(
    candidates: list[dict[str, Any]],
    *,
    wait: bool,
    events_path: Path,
    candidate_source: str,
) -> dict[str, Any]:
    submitted: list[dict[str, Any]] = []
    zero_balance: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for candidate in candidates:
        one = [candidate]
        try:
            result = _execute_redeem_batch(one, wait=wait)
            submitted.append({"condition": candidate, "result": result})
            append_redeem_event(
                events_path,
                _redeem_event(
                    status=str(result.get("status") or "SUBMITTED"),
                    candidates=one,
                    candidate_source=candidate_source,
                    result=result,
                ),
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            quota_reset = _quota_reset_seconds(error)
            if quota_reset is not None:
                errors.append({"condition": candidate, "error": error})
                return {
                    **_quota_state_from_error(error),
                    "mode": "isolated_per_condition",
                    "submitted_count": len(submitted),
                    "zero_balance_count": len(zero_balance),
                    "error_count": len(errors),
                    "submitted": submitted,
                    "zero_balance": zero_balance,
                    "errors": errors,
                    "estimated_submitted_usd": _estimated_value([row["condition"] for row in submitted]),
                    "estimated_zero_balance_cleared_usd": _estimated_value([row["condition"] for row in zero_balance]),
                }
            if _is_zero_position_balance_error(exc):
                zero_balance.append({"condition": candidate, "error": error})
                append_redeem_event(
                    events_path,
                    _redeem_event(
                        status="ZERO_POSITION_BALANCE",
                        candidates=one,
                        candidate_source=candidate_source,
                        error=error,
                    ),
                )
                continue
            errors.append({"condition": candidate, "error": error})
    if submitted and zero_balance:
        status = "PARTIAL_SUBMITTED_WITH_ZERO_BALANCE_CLEARED"
    elif submitted:
        status = str((submitted[-1].get("result") or {}).get("status") or "SUBMITTED")
    elif zero_balance and not errors:
        status = "ZERO_POSITION_BALANCE_CLEARED"
    else:
        status = "ERROR"
    if errors and (submitted or zero_balance):
        status = "PARTIAL_ERRORS"
    return {
        "status": status,
        "mode": "isolated_per_condition",
        "submitted_count": len(submitted),
        "zero_balance_count": len(zero_balance),
        "error_count": len(errors),
        "submitted": submitted,
        "zero_balance": zero_balance,
        "errors": errors,
        "estimated_submitted_usd": _estimated_value([row["condition"] for row in submitted]),
        "estimated_zero_balance_cleared_usd": _estimated_value([row["condition"] for row in zero_balance]),
    }


def main() -> int:
    args = parse_args()
    previous_state = load_json(args.output, default={})
    previous_state = previous_state if isinstance(previous_state, dict) else {}
    report = load_json(args.positions, default={})
    report = report if isinstance(report, dict) else {}
    candidate_source = "data_api_redeemable_position"
    candidates = redeem_candidates_from_report(report)
    if not candidates and bool(args.ledger_estimate_fallback):
        candidates = ledger_estimate_redeem_candidates_from_report(report)
        if candidates:
            candidate_source = "ledger_estimate_data_api_unavailable"
    candidates = candidates[: max(0, int(args.max_conditions))]
    state: dict[str, Any] = {
        "kind": "own_position_redeemer_state",
        "generated_at": _utc_now_iso(),
        "flow_stage": "LIVE",
        "positions_report": args.positions,
        "execute_requested": bool(args.execute),
        "ledger_estimate_fallback_requested": bool(args.ledger_estimate_fallback),
        "candidate_source": candidate_source,
        "candidate_count": len(candidates),
        "conditions": candidates,
        "live_orders_allowed": False,
        "single_submitter_invariant": "redemption is not order placement; live guard remains sole order submitter",
    }
    quota_wait = _active_quota_wait(previous_state)
    if args.execute and quota_wait:
        state.update(
            {
                "status": "RELAYER_QUOTA_EXHAUSTED",
                "executed": False,
                "skipped": True,
                "next_retry_at": quota_wait.get("next_retry_at"),
                "previous_status": quota_wait.get("previous_status"),
                "previous_error": quota_wait.get("previous_error"),
                "next_action": "skip relayer redemption attempts until next_retry_at; keep own-position visibility refresh running",
            }
        )
        atomic_write_json(args.output, state)
        print(json.dumps(state, indent=2, sort_keys=True))
        return 0
    if not candidates:
        summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
        locked = float(summary.get("redeemable_locked_usd") or 0.0)
        source = str(summary.get("locked_value_source") or "")
        status = "NO_DATA_API_REDEEMABLE_STANDARD_CTF_POSITIONS"
        if locked > 0 and source == "ledger_estimate_data_api_unavailable":
            status = "DATA_API_UNAVAILABLE_LEDGER_FALLBACK_NOT_ENABLED"
        state.update({"status": status, "executed": False})
        atomic_write_json(args.output, state)
        print(json.dumps(state, indent=2, sort_keys=True))
        return 0
    if not args.execute:
        state.update({"status": "DRY_RUN_READY", "executed": False})
        atomic_write_json(args.output, state)
        print(json.dumps(state, indent=2, sort_keys=True))
        return 0
    try:
        result = _execute_redeem_batch(candidates, wait=bool(args.wait))
        if _is_zero_position_balance_precheck(result):
            state.update(_zero_balance_skip_update(result=result))
            append_redeem_event(
                Path(args.events),
                _redeem_event(
                    status="SKIPPED_ZERO_BALANCE",
                    candidates=candidates,
                    candidate_source=candidate_source,
                    result=result,
                    error="PRECHECK_SKIPPED: redeem skipped: zero position balance",
                ),
            )
            atomic_write_json(args.output, state)
            print(json.dumps(state, indent=2, sort_keys=True))
            return 0
        state.update({"status": result.get("status") or "SUBMITTED", "executed": True, "result": result})
        event = {
            **_redeem_event(
                status=state["status"],
                candidates=candidates,
                candidate_source=candidate_source,
                result=result,
            )
        }
        append_redeem_event(Path(args.events), event)
        if state["status"] in {"STATE_MINED", "STATE_CONFIRMED", "SUBMITTED"}:
            state["post_redeem_recon"] = _post_redeem_scorecard(Path(args.post_redeem_scorecard))
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        quota_reset = _quota_reset_seconds(error)
        if quota_reset is not None:
            state.update({"executed": False, "error": error, **_quota_state_from_error(error)})
            atomic_write_json(args.output, state)
            print(json.dumps(state, indent=2, sort_keys=True))
            return 0
        if _is_proxy_sdk_unsupported_error(error):
            state.update(_proxy_sdk_unsupported_update(error))
            append_redeem_event(
                Path(args.events),
                _redeem_event(
                    status="RELAYER_PROXY_SDK_UNSUPPORTED",
                    candidates=candidates,
                    candidate_source=candidate_source,
                    error=error,
                ),
            )
            atomic_write_json(args.output, state)
            print(json.dumps(state, indent=2, sort_keys=True))
            return 0
        if _is_zero_position_balance_error(exc):
            isolated = _execute_redeem_isolated(
                candidates,
                wait=bool(args.wait),
                events_path=Path(args.events),
                candidate_source=candidate_source,
            )
            state.update(
                {
                    "status": isolated.get("status") or "ERROR",
                    "executed": bool(int(isolated.get("submitted_count") or 0) > 0),
                    "result": isolated,
                    "batch_error": f"{type(exc).__name__}: {exc}",
                }
            )
            if str(isolated.get("status") or "") == "ZERO_POSITION_BALANCE_CLEARED":
                state.update(_zero_balance_skip_update(result=isolated))
            if int(isolated.get("submitted_count") or 0) > 0:
                state["post_redeem_recon"] = _post_redeem_scorecard(Path(args.post_redeem_scorecard))
            atomic_write_json(args.output, state)
            print(json.dumps(state, indent=2, sort_keys=True))
            return 2 if state["status"] == "ERROR" else 0
        state.update({"status": "ERROR", "executed": False, "error": error})
        atomic_write_json(args.output, state)
        print(json.dumps(state, indent=2, sort_keys=True))
        return 2
    atomic_write_json(args.output, state)
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
