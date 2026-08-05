"""Own-wallet position visibility and redemption readiness helpers."""

from __future__ import annotations

import json
import os
import hashlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional runtime nicety.
    load_dotenv = None
else:
    load_dotenv(".env")

from eth_abi import encode
from eth_utils import function_signature_to_4byte_selector, to_checksum_address

from src.wallet_copy.http_client import PolymarketHttpClient
from src.wallet_copy.models import num, parse_ts, utc_now_iso
from src.wallet_copy.performance import load_resolutions
from src.wallet_copy.pnl_truth import score_order
from src.wallet_copy.store import append_jsonl, atomic_write_json, load_json


POLYMARKET_PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
POLYMARKET_CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
ZERO_BYTES32 = "0x" + ("00" * 32)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None = None) -> str:
    return (dt or _utc_now()).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _env_value(name: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    env_path = Path(".env")
    if not env_path.exists():
        return ""
    prefix = f"{name}="
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        stripped = line.strip()
        if stripped.startswith(prefix):
            return stripped.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _default_data_api_bases() -> list[str]:
    configured = _env_value("POLYMARKET_DATA_API_BASE_URL").rstrip("/")
    bases = [
        configured,
        "https://data-api.polymarket.com",
        "http://127.0.0.1:8787/data-api",
    ]
    return list(dict.fromkeys(base for base in bases if base))


def _boolish(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on", "redeemable", "resolved", "claimable"}:
        return True
    if text in {"0", "false", "no", "n", "off", "open", "unresolved", "closed"}:
        return False
    return None


def _short(value: str) -> str:
    text = str(value or "")
    if len(text) <= 14:
        return text
    return f"{text[:6]}...{text[-4:]}"


def _route_summary(report: dict[str, Any] | None) -> dict[str, Any]:
    report = report if isinstance(report, dict) else {}
    return {
        "status": report.get("status"),
        "route_class": report.get("route_class"),
        "best_variant": report.get("best_variant"),
        "attempt_count": report.get("attempt_count"),
        "elapsed_ms_total": report.get("elapsed_ms_total"),
        "original_host": report.get("original_host"),
        "routed_host": report.get("routed_host") or report.get("host"),
        "source_base_override_configured": report.get("source_base_override_configured"),
        "source_proxy_configured": any(
            bool(row.get("proxy_configured"))
            for row in (report.get("attempts") if isinstance(report.get("attempts"), list) else [])
            if isinstance(row, dict)
        ),
    }


def _rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "positions", "results"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
    return []


def fetch_data_api_positions(
    *,
    user: str,
    bases: list[str] | None = None,
    limit: int = 500,
    max_pages: int = 50,
    timeout_s: float = 8.0,
    client: PolymarketHttpClient | None = None,
) -> dict[str, Any]:
    if not user:
        return {"status": "UNAVAILABLE", "reason": "POLYMARKET_PROXY_missing", "rows": []}
    http = client or PolymarketHttpClient(
        timeout_s=float(timeout_s),
        retries=2,
        user_agent="polymarket-wallet-copy-own-positions/1.0",
    )
    errors: list[dict[str, Any]] = []
    for base in bases or _default_data_api_bases():
        rows: list[dict[str, Any]] = []
        route_reports: list[dict[str, Any]] = []
        base_failed = False
        truncated = True
        for page in range(max(1, int(max_pages))):
            params = {
                "user": user,
                "sizeThreshold": ".001",
                "limit": int(limit),
                "offset": page * int(limit),
            }
            try:
                response = http.request(
                    "GET",
                    f"{base.rstrip('/')}/positions",
                    params=params,
                    request_role="own_positions",
                    timeout_s=float(timeout_s),
                )
                route_reports.append(_route_summary(getattr(response, "wallet_copy_route_report", {})))
                if not (200 <= int(response.status_code) < 300):
                    errors.append(
                        {
                            "base": base,
                            "http_status": int(response.status_code),
                            "route": route_reports[-1] if route_reports else {},
                        }
                    )
                    base_failed = True
                    break
                page_rows = _rows_from_payload(response.json())
            except Exception as exc:
                errors.append({"base": base, "error": f"{type(exc).__name__}: {str(exc)[:240]}"})
                base_failed = True
                break
            rows.extend(page_rows)
            if len(page_rows) < int(limit):
                truncated = False
                break
        if not base_failed:
            return {
                "status": "OK",
                "base": base,
                "rows": rows,
                "rows_fetched": len(rows),
                "page_limit": int(limit),
                "truncated": truncated,
                "route_reports": route_reports,
            }
    direct = _fetch_data_api_positions_direct_no_env(
        user=user,
        base="https://data-api.polymarket.com",
        limit=limit,
        max_pages=max_pages,
        timeout_s=timeout_s,
    )
    if direct.get("status") == "OK":
        return direct
    errors.extend(direct.get("errors") or [])
    return {"status": "UNAVAILABLE", "reason": "positions_fetch_failed", "rows": [], "errors": errors}


def _fetch_data_api_positions_direct_no_env(
    *,
    user: str,
    base: str,
    limit: int,
    max_pages: int,
    timeout_s: float,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    route_reports: list[dict[str, Any]] = []
    session = requests.Session()
    session.trust_env = False
    truncated = True
    for page in range(max(1, int(max_pages))):
        params = {
            "user": user,
            "sizeThreshold": ".001",
            "limit": int(limit),
            "offset": page * int(limit),
        }
        try:
            response = session.get(
                f"{base.rstrip('/')}/positions",
                params=params,
                headers={
                    "Accept": "application/json,text/plain,*/*",
                    "Connection": "close",
                    "User-Agent": "polymarket-wallet-copy-own-positions/direct-no-env",
                },
                timeout=float(timeout_s),
            )
        except Exception as exc:
            errors.append({"base": base, "route_class": "DIRECT_NO_ENV_ERROR", "error": f"{type(exc).__name__}: {str(exc)[:240]}"})
            return {"status": "UNAVAILABLE", "reason": "direct_no_env_positions_fetch_failed", "rows": [], "errors": errors}
        route_reports.append(
            {
                "status": "PASS" if 200 <= int(response.status_code) < 300 else "HTTP_NON_2XX",
                "route_class": "DIRECT_NO_ENV",
                "original_host": "data-api.polymarket.com",
                "routed_host": "data-api.polymarket.com",
                "source_base_override_configured": False,
                "source_proxy_configured": False,
                "http_status": int(response.status_code),
            }
        )
        if not (200 <= int(response.status_code) < 300):
            errors.append(
                {
                    "base": base,
                    "http_status": int(response.status_code),
                    "route": route_reports[-1],
                    "body_tail": response.text[-240:],
                }
            )
            return {"status": "UNAVAILABLE", "reason": "direct_no_env_positions_http_non_2xx", "rows": [], "errors": errors}
        page_rows = _rows_from_payload(response.json())
        rows.extend(page_rows)
        if len(page_rows) < int(limit):
            truncated = False
            break
    return {
        "status": "OK",
        "base": base,
        "rows": rows,
        "rows_fetched": len(rows),
        "page_limit": int(limit),
        "truncated": truncated,
        "route_reports": route_reports,
        "direct_no_env": True,
    }


def normalize_position_row(row: dict[str, Any]) -> dict[str, Any]:
    condition_id = str(
        row.get("conditionId")
        or row.get("condition_id")
        or row.get("condition")
        or row.get("market")
        or row.get("marketId")
        or ""
    )
    token_id = str(row.get("asset") or row.get("assetId") or row.get("tokenId") or row.get("token_id") or "")
    market_slug = str(row.get("slug") or row.get("eventSlug") or row.get("marketSlug") or "")
    title = str(row.get("title") or row.get("question") or row.get("eventTitle") or "")
    outcome = str(row.get("outcome") or row.get("side") or row.get("outcomeName") or "")
    size = max(num(row.get("size")), num(row.get("shares")), num(row.get("balance")))
    current_value = max(num(row.get("currentValue")), num(row.get("current_value")), num(row.get("value")))
    initial_value = max(num(row.get("initialValue")), num(row.get("initial_value")))
    avg_price = max(num(row.get("avgPrice")), num(row.get("avg_price")), num(row.get("averagePrice")))
    cash_pnl = max(num(row.get("cashPnl")), num(row.get("cash_pnl")))
    explicit_redeemable = None
    for key in ("redeemable", "isRedeemable", "claimable", "canRedeem"):
        explicit_redeemable = _boolish(row.get(key))
        if explicit_redeemable is not None:
            break
    resolved = None
    for key in ("resolved", "isResolved", "marketResolved", "resolutionStatus", "umaResolutionStatus", "status"):
        resolved = _boolish(row.get(key))
        if resolved is not None:
            break
    redeemable = bool(explicit_redeemable) if explicit_redeemable is not None else False
    redeemable_value = 0.0
    if redeemable:
        redeemable_value = max(
            num(row.get("redeemableValue")),
            num(row.get("redeemableValueUsd")),
            num(row.get("claimableValue")),
            current_value,
        )
    return {
        "condition_id": condition_id,
        "token_id": token_id,
        "market_slug": market_slug,
        "title": title,
        "outcome": outcome,
        "size": round(size, 6),
        "avg_price": round(avg_price, 6),
        "current_value_usd": round(current_value, 6),
        "initial_value_usd": round(initial_value, 6),
        "cash_pnl_usd": round(cash_pnl, 6),
        "redeemable": redeemable,
        "resolved": resolved,
        "redeemable_value_usd": round(redeemable_value, 6),
        "neg_risk": bool(_boolish(row.get("negRisk") or row.get("negativeRisk"))),
    }


def _load_local_redeemed_by_condition(events_path: Path) -> dict[str, float]:
    redeemed: dict[str, float] = defaultdict(float)
    if not events_path.exists():
        return dict(redeemed)
    for line in events_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "").upper()
        if status not in {
            "STATE_CONFIRMED",
            "STATE_MINED",
            "CONFIRMED",
            "MINED",
            "SUBMITTED",
            "ZERO_POSITION_BALANCE",
        }:
            continue
        for item in row.get("conditions") or []:
            if not isinstance(item, dict):
                continue
            condition_id = str(item.get("condition_id") or "")
            if condition_id:
                redeemed[condition_id] += num(item.get("estimated_value_usd"))
    return dict(redeemed)


def ledger_redeemable_estimate(
    *,
    ledger_path: Path,
    resolutions_path: Path,
    redeem_events_path: Path,
) -> dict[str, Any]:
    ledger = load_json(ledger_path, default={})
    ledger = ledger if isinstance(ledger, dict) else {}
    resolutions = load_resolutions(resolutions_path)
    local_redeemed = _load_local_redeemed_by_condition(redeem_events_path)
    grouped: dict[str, dict[str, Any]] = {}
    inspected_fills = 0
    resolved_winning_fills = 0
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        status = str(order.get("final_status") or order.get("status") or "").upper()
        if status != "FILLED":
            continue
        inspected_fills += 1
        event = score_order(order, resolutions)
        if not event.get("resolved") or num(event.get("payout_usd")) <= 0:
            continue
        resolved_winning_fills += 1
        condition_id = str(event.get("condition_id") or order.get("condition_id") or "")
        if not condition_id:
            continue
        item = grouped.setdefault(
            condition_id,
            {
                "condition_id": condition_id,
                "market_slug": str(event.get("market_slug") or order.get("market_slug") or ""),
                "winning_side": str(event.get("winner") or ""),
                "shares": 0.0,
                "estimated_value_usd": 0.0,
                "orders": 0,
                "first_fill_ts": None,
                "last_fill_ts": None,
                "source": "live_ledger_resolution_estimate",
            },
        )
        item["shares"] = round(float(item["shares"]) + num(event.get("shares")), 6)
        item["estimated_value_usd"] = round(float(item["estimated_value_usd"]) + num(event.get("payout_usd")), 6)
        item["orders"] = int(item["orders"]) + 1
        ts = parse_ts(event.get("submitted_at"))
        if ts is not None:
            item["first_fill_ts"] = ts if item["first_fill_ts"] is None else min(float(item["first_fill_ts"]), ts)
            item["last_fill_ts"] = ts if item["last_fill_ts"] is None else max(float(item["last_fill_ts"]), ts)
    rows: list[dict[str, Any]] = []
    for condition_id, item in grouped.items():
        redeemed = local_redeemed.get(condition_id, 0.0)
        pending = round(max(0.0, float(item["estimated_value_usd"]) - redeemed), 6)
        if pending <= 0:
            continue
        out = dict(item)
        out["local_redeemed_usd"] = round(redeemed, 6)
        out["estimated_value_usd"] = pending
        rows.append(out)
    rows.sort(key=lambda row: float(row.get("estimated_value_usd") or 0.0), reverse=True)
    return {
        "status": "ESTIMATE",
        "source": "live_ledger_resolved_winning_fills_minus_local_redeem_events",
        "inspected_filled_orders": inspected_fills,
        "resolved_winning_fills": resolved_winning_fills,
        "conditions": rows,
        "total_redeemable_locked_usd": round(sum(num(row.get("estimated_value_usd")) for row in rows), 6),
        "confidence": "LOW_WITHOUT_DATA_API_POSITIONS",
    }


def build_positions_report(
    *,
    output_path: Path,
    ledger_path: Path,
    resolutions_path: Path,
    redeem_events_path: Path,
    data_api_bases: list[str] | None = None,
    timeout_s: float = 8.0,
) -> dict[str, Any]:
    user = _env_value("POLYMARKET_PROXY")
    data_api = fetch_data_api_positions(user=user, bases=data_api_bases, timeout_s=timeout_s)
    normalized = [normalize_position_row(row) for row in data_api.get("rows") or [] if isinstance(row, dict)]
    data_api_redeemable = [
        row for row in normalized if row.get("redeemable") and num(row.get("redeemable_value_usd")) > 0
    ]
    data_api_total = round(sum(num(row.get("redeemable_value_usd")) for row in data_api_redeemable), 6)
    ledger_estimate = ledger_redeemable_estimate(
        ledger_path=ledger_path,
        resolutions_path=resolutions_path,
        redeem_events_path=redeem_events_path,
    )
    if data_api.get("status") == "OK":
        locked_source = "data_api_positions"
        locked_usd = data_api_total
    else:
        locked_source = "ledger_estimate_data_api_unavailable"
        locked_usd = num(ledger_estimate.get("total_redeemable_locked_usd"))
    report = {
        "kind": "own_positions_report",
        "generated_at": _iso(),
        "flow_stage": "LIVE",
        "wallet": _short(user),
        "wallet_full": user,
        "status": "PASS" if data_api.get("status") == "OK" else "DEGRADED_DATA_API_UNAVAILABLE",
        "data_api": {key: value for key, value in data_api.items() if key != "rows"},
        "summary": {
            "positions_rows": len(normalized),
            "data_api_redeemable_rows": len(data_api_redeemable),
            "data_api_redeemable_locked_usd": data_api_total,
            "ledger_estimated_redeemable_locked_usd": ledger_estimate.get("total_redeemable_locked_usd"),
            "redeemable_locked_usd": round(float(locked_usd), 6),
            "locked_value_source": locked_source,
            "current_value_usd": round(sum(num(row.get("current_value_usd")) for row in normalized), 6),
            "initial_value_usd": round(sum(num(row.get("initial_value_usd")) for row in normalized), 6),
        },
        "positions": normalized,
        "redeemable_positions": data_api_redeemable,
        "ledger_estimate": ledger_estimate,
        "raw_positions": data_api.get("rows") if data_api.get("status") == "OK" else [],
    }
    atomic_write_json(output_path, report)
    return report


def _redeemable_condition_ids(report: dict[str, Any]) -> list[str]:
    ids: set[str] = set()
    for row in report.get("redeemable_positions") or []:
        if not isinstance(row, dict):
            continue
        condition_id = str(row.get("condition_id") or "")
        if condition_id:
            ids.add(condition_id)
    source = str((report.get("summary") or {}).get("locked_value_source") or "")
    if source == "data_api_positions":
        return sorted(ids)
    ledger_estimate = report.get("ledger_estimate") if isinstance(report.get("ledger_estimate"), dict) else {}
    for row in ledger_estimate.get("conditions") or []:
        if not isinstance(row, dict):
            continue
        condition_id = str(row.get("condition_id") or "")
        if condition_id:
            ids.add(condition_id)
    return sorted(ids)


def update_redeem_deadman(
    *,
    report: dict[str, Any],
    state_path: Path,
    redeemer_state_path: Path | None = None,
    threshold_usd: float = 20.0,
    threshold_age_s: float = 3600.0,
    handoff_path: Path | None = None,
    append_handoff_on_incident: bool = False,
) -> dict[str, Any]:
    now = _utc_now()
    previous = load_json(state_path, default={})
    previous = previous if isinstance(previous, dict) else {}
    locked = num((report.get("summary") or {}).get("redeemable_locked_usd"))
    source = str((report.get("summary") or {}).get("locked_value_source") or "")
    condition_ids = _redeemable_condition_ids(report)
    condition_hash = hashlib.sha256("\n".join(condition_ids).encode("utf-8")).hexdigest()[:24]
    above = locked > float(threshold_usd)
    first_seen = previous.get("first_seen_above_threshold_at") if above else None
    if above and first_seen and previous.get("condition_set_hash") != condition_hash:
        first_seen = None
    if above and not first_seen:
        first_seen = _iso(now)
    try:
        first_dt = datetime.fromisoformat(str(first_seen).replace("Z", "+00:00")) if first_seen else None
    except ValueError:
        first_seen = _iso(now) if above else None
        first_dt = now if above else None
    age_s = max(0.0, (now - first_dt.astimezone(timezone.utc)).total_seconds()) if first_dt else 0.0
    incident = bool(above and age_s >= float(threshold_age_s))
    acknowledged_reason = None
    acknowledged_until = None
    if above and redeemer_state_path is not None:
        redeemer_state = load_json(redeemer_state_path, default={})
        redeemer_state = redeemer_state if isinstance(redeemer_state, dict) else {}
        next_retry = redeemer_state.get("next_retry_at")
        try:
            next_retry_dt = (
                datetime.fromisoformat(str(next_retry).replace("Z", "+00:00")).astimezone(timezone.utc)
                if next_retry
                else None
            )
        except ValueError:
            next_retry_dt = None
        quota_wait_until = next_retry_dt + timedelta(seconds=1800) if next_retry_dt else None
        if redeemer_state.get("status") == "RELAYER_QUOTA_EXHAUSTED" and quota_wait_until and now < quota_wait_until:
            incident = False
            acknowledged_reason = "QUOTA_WAIT_UNTIL_NEXT_RETRY"
            acknowledged_until = _iso(quota_wait_until)
    status = "INCIDENT_REDEEMABLE_LOCKED" if incident else ("WATCH_REDEEMABLE_LOCKED" if above else "OK")
    state = {
        "kind": "own_position_deadman",
        "checked_at": _iso(now),
        "status": status,
        "incident": incident,
        "redeemable_locked_usd": round(locked, 6),
        "locked_value_source": source,
        "threshold_usd": float(threshold_usd),
        "threshold_age_s": float(threshold_age_s),
        "first_seen_above_threshold_at": first_seen,
        "age_s": round(age_s, 6),
        "condition_count": len(condition_ids),
        "condition_set_hash": condition_hash,
        "next_action": "run own-position redeemer now" if incident else "continue 10-minute refresh/redeem cycle",
    }
    if acknowledged_reason:
        state["acknowledged_reason"] = acknowledged_reason
        state["acknowledged_until"] = acknowledged_until
    atomic_write_json(state_path, state)
    should_append = (
        append_handoff_on_incident
        and incident
        and handoff_path is not None
        and previous.get("status") != "INCIDENT_REDEEMABLE_LOCKED"
    )
    if should_append:
        with handoff_path.open("a", encoding="utf-8") as handle:
            handle.write(
                "\n"
                f"## {_iso(now)} brainless NOTIFY [LIVE]\n"
                f"- redeem_deadman [LIVE]: status={status}; redeemable_locked_usd={locked:.6f}; "
                f"source={source}; next=run `python3 scripts/run_own_position_redeemer.py --execute` and reconcile.\n"
            )
    return state


def redeem_calldata(condition_id: str, *, collateral_token: str = POLYMARKET_PUSD) -> str:
    condition = str(condition_id or "")
    if not (condition.startswith("0x") and len(condition) == 66):
        raise ValueError("condition_id must be a bytes32 hex string")
    selector = function_signature_to_4byte_selector("redeemPositions(address,bytes32,bytes32,uint256[])")
    encoded = encode(
        ["address", "bytes32", "bytes32", "uint256[]"],
        [to_checksum_address(collateral_token), bytes.fromhex("00" * 32), bytes.fromhex(condition[2:]), [1, 2]],
    )
    return "0x" + (selector + encoded).hex()


def split_position_calldata(
    condition_id: str,
    *,
    amount_usd: float = 1.0,
    collateral_token: str = POLYMARKET_PUSD,
) -> str:
    """Encode an exact binary CTF collateral split (USDC/pUSD uses 6 decimals)."""
    condition = str(condition_id or "")
    if not (condition.startswith("0x") and len(condition) == 66):
        raise ValueError("condition_id must be a bytes32 hex string")
    amount = int(round(float(amount_usd) * 1_000_000))
    if amount <= 0 or abs(amount / 1_000_000 - float(amount_usd)) > 1e-9:
        raise ValueError("split amount must be positive and exactly representable at 6 decimals")
    selector = function_signature_to_4byte_selector(
        "splitPosition(address,bytes32,bytes32,uint256[],uint256)"
    )
    encoded = encode(
        ["address", "bytes32", "bytes32", "uint256[]", "uint256"],
        [
            to_checksum_address(collateral_token),
            bytes.fromhex("00" * 32),
            bytes.fromhex(condition[2:]),
            [1, 2],
            amount,
        ],
    )
    return "0x" + (selector + encoded).hex()


def redeem_candidates_from_report(report: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in report.get("redeemable_positions") or []:
        if not isinstance(row, dict):
            continue
        condition_id = str(row.get("condition_id") or "")
        if not (condition_id.startswith("0x") and len(condition_id) == 66):
            continue
        if bool(row.get("neg_risk")):
            continue
        value = num(row.get("redeemable_value_usd"))
        if value <= 0:
            continue
        candidates.append(
            {
                "condition_id": condition_id,
                "market_slug": row.get("market_slug"),
                "estimated_value_usd": round(value, 6),
                "source": "data_api_redeemable_position",
                "neg_risk": False,
            }
        )
    unique: dict[str, dict[str, Any]] = {}
    for row in candidates:
        unique[str(row["condition_id"])] = row
    return sorted(unique.values(), key=lambda row: num(row.get("estimated_value_usd")), reverse=True)


def ledger_estimate_redeem_candidates_from_report(report: dict[str, Any]) -> list[dict[str, Any]]:
    ledger_estimate = report.get("ledger_estimate") if isinstance(report.get("ledger_estimate"), dict) else {}
    candidates: list[dict[str, Any]] = []
    for row in ledger_estimate.get("conditions") or []:
        if not isinstance(row, dict):
            continue
        condition_id = str(row.get("condition_id") or "")
        if not (condition_id.startswith("0x") and len(condition_id) == 66):
            continue
        value = num(row.get("estimated_value_usd"))
        if value <= 0:
            continue
        candidates.append(
            {
                "condition_id": condition_id,
                "market_slug": row.get("market_slug"),
                "estimated_value_usd": round(value, 6),
                "source": "ledger_estimate_data_api_unavailable",
                "winning_side": row.get("winning_side"),
                "orders": row.get("orders"),
                "neg_risk": False,
            }
        )
    unique: dict[str, dict[str, Any]] = {}
    for row in candidates:
        unique[str(row["condition_id"])] = row
    return sorted(unique.values(), key=lambda row: num(row.get("estimated_value_usd")), reverse=True)


def append_redeem_event(path: Path, payload: dict[str, Any]) -> None:
    append_jsonl(path, payload)
