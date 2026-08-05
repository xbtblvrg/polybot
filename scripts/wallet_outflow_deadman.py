#!/usr/bin/env python3
"""Detect own-wallet pUSD outflows not explained by orders or redemptions.

Flow stage: LIVE/DEFEND/SELF-DEV. This is a read-only deadman: it scans
collateral Transfer rows touching the guard wallet, matches outgoing rows by
transaction hash to the live order ledger or redemption records, and raises a
P0 incident if any outgoing transfer remains unmatched for one brainless cycle.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import report_item4_nonfill_cash_audit as cash_audit  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from src.wallet_copy.scorecard import load_fresh_scorecard  # noqa: E402


DEFAULT_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_H2_EXTERNAL = "data/research/h2_external_redemption_ingestion_latest.json"
DEFAULT_OWN_REDEEM_EVENTS = "data/research/own_redeem_events.jsonl"
DEFAULT_STATE = "data/research/wallet_outflow_deadman_state.json"
DEFAULT_HANDOFF = "docs/agents/HANDOFF.md"
DEFAULT_LIVE_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_POLYGON_RPC_URL = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
DEFAULT_POLYGON_RPC_FALLBACK_URL = os.getenv(
    "POLYGON_RPC_FALLBACK_URL",
    os.getenv("POLYGON_SECONDARY_RPC_URL", "https://polygon-pokt.nodies.app"),
)
DEFAULT_DATA_API_BASE = os.getenv("POLYMARKET_DATA_API_BASE_URL", "https://data-api.polymarket.com")
DEFAULT_BLOCKSCOUT_BASE = "https://polygon.blockscout.com/api"
TRANSFER_FETCH_BACKOFF_CAP_S = 5.0
DEFAULT_TRANSFER_403_COOLDOWN_S = 10 * 60.0
DEFAULT_TRANSFER_5XX_RETRY_BACKOFF_S = 1.0
DEFAULT_LIVE_ARMED_AFTER_ISO = "2026-07-13T00:00:00Z"
TRANSFER_FETCH_RECORDED_HEADERS = {
    "retry-after",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "ratelimit-limit",
    "ratelimit-remaining",
    "ratelimit-reset",
    "cf-ray",
    "cf-cache-status",
    "server",
    "date",
    "content-type",
}


class TransferFetchError(RuntimeError):
    """Carry transfer-source attempts through total fetch failure."""

    def __init__(self, message: str, *, attempts: list[dict[str, Any]], fetch: dict[str, Any]) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.fetch = fetch


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--h2-external", default=DEFAULT_H2_EXTERNAL)
    parser.add_argument("--own-redeem-events", default=DEFAULT_OWN_REDEEM_EVENTS)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--handoff", default=DEFAULT_HANDOFF)
    parser.add_argument("--live-guard-state", default=DEFAULT_LIVE_GUARD_STATE)
    parser.add_argument("--user", default="")
    parser.add_argument("--start-iso", default="")
    parser.add_argument("--end-iso", default="")
    parser.add_argument("--first-run-lookback-s", type=float, default=3600.0)
    parser.add_argument("--overlap-s", type=float, default=30 * 60.0)
    parser.add_argument("--max-fetch-gap-s", type=float, default=24 * 60 * 60.0)
    parser.add_argument("--max-unmatched-age-s", type=float, default=10 * 60.0)
    parser.add_argument("--degraded-notify-threshold", type=int, default=3)
    parser.add_argument("--min-outflow-usd", type=float, default=0.000001)
    parser.add_argument("--polygon-rpc-url", default=DEFAULT_POLYGON_RPC_URL)
    parser.add_argument(
        "--polygon-rpc-secondary-url",
        "--polygon-rpc-fallback-url",
        dest="polygon_rpc_fallback_url",
        default=DEFAULT_POLYGON_RPC_FALLBACK_URL,
    )
    parser.add_argument("--data-api-base-url", default=DEFAULT_DATA_API_BASE)
    parser.add_argument("--blockscout-base-url", default=DEFAULT_BLOCKSCOUT_BASE)
    parser.add_argument("--transfer-source", choices=("auto", "rpc", "blockscout", "rpc_secondary"), default="auto")
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--transfer-403-cooldown-s", type=float, default=DEFAULT_TRANSFER_403_COOLDOWN_S)
    parser.add_argument("--transfer-5xx-retry-backoff-s", type=float, default=DEFAULT_TRANSFER_5XX_RETRY_BACKOFF_S)
    parser.add_argument("--live-armed-after-iso", default=DEFAULT_LIVE_ARMED_AFTER_ISO)
    parser.add_argument("--live-armed-degraded-incident-threshold", type=int, default=12)
    parser.add_argument("--chunk-blocks", type=int, default=7_500)
    parser.add_argument("--rpc-secondary-chunk-blocks", type=int, default=50)
    parser.add_argument("--rpc-secondary-from-block-safety-blocks", type=int, default=0)
    parser.add_argument("--rpc-secondary-to-block-safety-blocks", type=int, default=0)
    parser.add_argument("--block-mode", choices=("estimate", "exact"), default="estimate")
    parser.add_argument("--seconds-per-block", type=float, default=2.1)
    parser.add_argument("--data-api-limit", type=int, default=500)
    parser.add_argument("--data-api-max-pages", type=int, default=5)
    parser.add_argument("--enable-data-api-activity", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--write-handoff-on-incident", action="store_true")
    return parser.parse_args()


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return num(text, 0.0)


def _iso(ts: float | None = None) -> str | None:
    if ts is None:
        return utc_now_iso()
    if ts <= 0:
        return None
    return datetime.fromtimestamp(float(ts), tz=UTC).isoformat().replace("+00:00", "Z")


def _new_timing() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "enabled": True,
        "started_at": utc_now_iso(),
        "phases": [],
    }


def _finish_timing_phase(timing: dict[str, Any], name: str, started_at: float, **extra: Any) -> None:
    phases = timing.setdefault("phases", [])
    row = {
        "name": name,
        "duration_s": round(max(0.0, time.perf_counter() - started_at), 6),
    }
    row.update(extra)
    phases.append(row)


def _transfer_attempt_timing_rows(attempts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = (
        "source",
        "status",
        "duration_s",
        "rows",
        "fetch_status",
        "http_status",
        "error_type",
        "error",
        "fallback_source",
        "retry_source",
        "backoff_s",
        "backoff_applied",
        "cooldown_until",
        "cooldown_reason",
    )
    rows: list[dict[str, Any]] = []
    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        row = {key: attempt.get(key) for key in keys if key in attempt}
        if "error" in row:
            row["error"] = str(row["error"])[:500]
        rows.append(row)
    return rows


def _pre_blockscout_attempt_duration_s(attempts: list[dict[str, Any]]) -> float | None:
    total = 0.0
    found_successful_blockscout = False
    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        source = str(attempt.get("source") or "")
        status = str(attempt.get("status") or "")
        if source == "blockscout_account_tokentx" and status == "OK":
            found_successful_blockscout = True
            break
        if source != "blockscout_account_tokentx":
            total += num(attempt.get("duration_s"), 0.0)
    if not found_successful_blockscout:
        return None
    return round(total, 3)


def _load_dotenv_value(name: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    env_path = ROOT / ".env"
    if not env_path.exists():
        return ""
    prefix = f"{name}="
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        stripped = line.strip()
        if stripped.startswith(prefix):
            return stripped.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _norm_addr(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _tx(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") else ""


def _event_key(row: dict[str, Any]) -> str:
    tx = _tx(row.get("tx"))
    log_index = row.get("log_index")
    return f"{tx}:{log_index}" if tx else f"no-tx:{row.get('block_number')}:{log_index}:{row.get('amount_usd')}"


def _order_fill_ts(order: dict[str, Any]) -> float:
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
    candidates: list[Any] = [
        trade_result.get("filled_at"),
        trade_result.get("filled_at_iso"),
        trade_result.get("updated_at"),
        order.get("updated_at"),
    ]
    for item in reversed(lifecycle):
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or item.get("event") or "").upper()
        if "FILL" in status or status in {"MATCHED", "FILLED"}:
            candidates.extend([item.get("ts"), item.get("timestamp"), item.get("at"), item.get("iso")])
    candidates.extend([trade_result.get("timestamp"), order.get("submitted_at")])
    for value in candidates:
        ts = _parse_ts(value)
        if ts > 0:
            return ts
    return 0.0


def _order_filled_cost_usd(order: dict[str, Any]) -> float:
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    candidates: list[Any] = [
        trade_result.get("actual_trade_cost_usd"),
        trade_result.get("response_filled_size_usd"),
        trade_result.get("filled_size_usd"),
        trade_result.get("making_amount"),
        order.get("actual_trade_cost_usd"),
        order.get("response_filled_size_usd"),
        order.get("filled_size_usd"),
        order.get("notional_usd"),
        order.get("size_usd"),
    ]
    lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
    for item in reversed(lifecycle):
        if not isinstance(item, dict):
            continue
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else item
        candidates.extend(
            [
                payload.get("actual_trade_cost_usd"),
                payload.get("response_filled_size_usd"),
                payload.get("filled_size_usd"),
            ]
        )
    for value in candidates:
        amount = num(value, 0.0)
        if amount > 0:
            return float(amount)
    return 0.0


def _ledger_fill_settlement_candidates(ledger: Any, *, start_ts: float, end_ts: float) -> list[dict[str, Any]]:
    orders = ledger.get("orders") if isinstance(ledger, dict) else ledger
    rows = [row for row in orders or [] if isinstance(row, dict)]
    out: list[dict[str, Any]] = []
    for order in rows:
        status = str(order.get("final_status") or order.get("status") or "").upper()
        if status != "FILLED":
            continue
        fill_ts = _order_fill_ts(order)
        if not (start_ts <= fill_ts <= end_ts):
            continue
        cost_usd = _order_filled_cost_usd(order)
        if cost_usd <= 0:
            continue
        out.append(
            {
                "source": "live_execution_ledger_amount_time",
                "match_method": "known_settlement_counterparty_amount_time",
                "order_id": order.get("order_id"),
                "intent_id": order.get("intent_id"),
                "source_wallet": _norm_addr(order.get("source_wallet")),
                "market_slug": order.get("market_slug"),
                "condition_id": order.get("condition_id"),
                "outcome": order.get("outcome"),
                "cost_usd": round(cost_usd, 6),
                "filled_at_ts": fill_ts,
                "filled_at_iso": _iso(fill_ts),
            }
        )
    return out


def _match_ledger_settlement_by_amount_time(
    row: dict[str, Any],
    candidates: list[dict[str, Any]],
    *,
    amount_tolerance_usd: float = 0.02,
    time_tolerance_s: float = 2 * 60 * 60,
) -> dict[str, Any]:
    if row.get("matched_evidence"):
        return row
    if str(row.get("direction") or "") != "OUT":
        return row
    counterparty = _norm_addr(row.get("counterparty"))
    if counterparty not in cash_audit.KNOWN_SETTLEMENT_COUNTERPARTIES:
        return row
    amount = num(row.get("amount_usd"), 0.0)
    block_ts = num(row.get("block_ts"), 0.0)
    if amount <= 0 or block_ts <= 0:
        return row
    matches = []
    for candidate in candidates:
        cost = num(candidate.get("cost_usd"), 0.0)
        fill_ts = num(candidate.get("filled_at_ts"), 0.0)
        amount_delta = abs(cost - amount)
        time_delta = abs(fill_ts - block_ts)
        if amount_delta <= float(amount_tolerance_usd) and time_delta <= float(time_tolerance_s):
            matches.append((amount_delta, time_delta, candidate))
    if not matches:
        return row
    if len(matches) > 1:
        return {
            **row,
            "settlement_match_ambiguous": True,
            "settlement_match_candidates": [
                {
                    "order_id": candidate.get("order_id"),
                    "market_slug": candidate.get("market_slug"),
                    "condition_id": candidate.get("condition_id"),
                    "cost_usd": candidate.get("cost_usd"),
                    "filled_at_iso": candidate.get("filled_at_iso"),
                }
                for _, _, candidate in sorted(
                    matches, key=lambda item: (item[0], item[1], str(item[2].get("order_id") or ""))
                )[:5]
            ],
            "next_action": "add tx/condition-specific evidence before marking settlement as matched",
        }
    _, time_delta, candidate = min(matches, key=lambda item: (item[0], item[1], str(item[2].get("order_id") or "")))
    evidence = {
        **candidate,
        "transfer_tx": _tx(row.get("tx")),
        "transfer_log_index": row.get("log_index"),
        "transfer_amount_usd": round(amount, 6),
        "transfer_counterparty": counterparty,
        "time_delta_s": round(time_delta, 6),
    }
    return {
        **row,
        "classification": "fill_settlement",
        "classification_reason": "known settlement outflow matched FILLED live ledger order by amount/time",
        "matched_evidence": evidence,
    }


def _scorecard_baseline_start(scorecard: dict[str, Any]) -> float:
    since = scorecard.get("since_topup_truth") if isinstance(scorecard.get("since_topup_truth"), dict) else {}
    chain = scorecard.get("chain_reconciliation") if isinstance(scorecard.get("chain_reconciliation"), dict) else {}
    return _parse_ts(since.get("baseline_iso") or chain.get("reconciliation_start_iso"))


def _previous_fetch_was_degraded(previous: dict[str, Any]) -> bool:
    status = str(previous.get("status") or "")
    fetch = previous.get("fetch") if isinstance(previous.get("fetch"), dict) else {}
    return status.startswith("DEGRADED_FETCH") or str(fetch.get("status") or "") not in ("", "OK")


def _previous_last_ok_checked_at(previous: dict[str, Any]) -> float:
    last_ok = _parse_ts(previous.get("last_ok_checked_at"))
    if last_ok > 0:
        return last_ok
    if not _previous_fetch_was_degraded(previous):
        return _parse_ts(previous.get("checked_at"))
    return 0.0


def _window(args: argparse.Namespace, previous: dict[str, Any]) -> tuple[float, float, str, bool]:
    end_ts = _parse_ts(args.end_iso) if args.end_iso else time.time()
    if args.start_iso:
        return _parse_ts(args.start_iso), end_ts, "explicit_start_iso", False
    if _previous_fetch_was_degraded(previous):
        last_ok = _previous_last_ok_checked_at(previous)
        if last_ok > 0:
            requested_start = max(0.0, last_ok - float(args.overlap_s))
            capped_start = max(0.0, end_ts - float(args.max_fetch_gap_s))
            if requested_start < capped_start:
                return capped_start, end_ts, "last_ok_gap_exceeded_capped", True
            return requested_start, end_ts, "last_ok_checked_overlap_after_degraded_fetch", False
    prev_checked = _parse_ts(previous.get("checked_at"))
    if prev_checked > 0:
        return max(0.0, prev_checked - float(args.overlap_s)), end_ts, "previous_checked_overlap", False
    scorecard = load_fresh_scorecard(args.scorecard)
    baseline = _scorecard_baseline_start(scorecard)
    if baseline > 0:
        return max(baseline, end_ts - float(args.first_run_lookback_s)), end_ts, "bounded_first_run_since_topup", False
    return max(0.0, end_ts - float(args.first_run_lookback_s)), end_ts, "first_run_lookback", False


def _own_redeem_txs(path: str | Path) -> dict[str, dict[str, Any]]:
    target = Path(path)
    out: dict[str, dict[str, Any]] = {}
    if not target.exists():
        return out
    for line in target.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        tx = _tx(row.get("transaction_hash") or row.get("tx") or (row.get("result") or {}).get("transaction_hash"))
        if not tx:
            continue
        out[tx] = {
            "tx": tx,
            "source": "own_redeem_events",
            "status": row.get("status"),
            "estimated_redeemed_usd": row.get("estimated_redeemed_usd"),
            "ts": row.get("ts"),
        }
    return out


def _activity_trade_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if str(row.get("type") or "").upper() != "TRADE":
            continue
        tx = _tx(row.get("transactionHash") or row.get("transaction_hash"))
        if not tx:
            continue
        amount = num(row.get("usdcSize"), 0.0) or (
            num(row.get("size"), 0.0) * num(row.get("price"), 0.0)
        )
        out.append(
            {
                "tx": tx,
                "condition_id": str(row.get("conditionId") or row.get("condition_id") or ""),
                "token_id": str(row.get("asset") or row.get("token_id") or ""),
                "timestamp": row.get("timestamp"),
                "ts": _parse_ts(row.get("timestamp")),
                "cost_usd": round(float(amount), 6),
                "size": num(row.get("size"), 0.0),
                "price": num(row.get("price"), 0.0),
                "side": row.get("side"),
                "market_slug": row.get("slug"),
                "outcome": row.get("outcome"),
                "raw_type": row.get("type"),
            }
        )
    return out


def _activity_live_order_txs(args: argparse.Namespace, *, wallet: str, start_ts: float, end_ts: float) -> tuple[dict[str, dict[str, Any]], dict[str, Any], dict[str, dict[str, Any]]]:
    if not args.enable_data_api_activity:
        return {}, {"status": "SKIPPED"}, {}
    try:
        rows, fetch = cash_audit._fetch_data_api_activity(
            user=wallet,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=int(args.data_api_limit),
            max_pages=int(args.data_api_max_pages),
            timeout_s=float(args.timeout_s),
            base_url=str(args.data_api_base_url),
        )
        redeem_rows = cash_audit._activity_redeem_rows(rows)
        trade_rows = _activity_trade_rows(rows)
    except Exception as exc:  # noqa: BLE001 - redemptions are a supplement for this outflow detector.
        return {}, {"status": "ERROR", "error_type": type(exc).__name__, "error": str(exc)}, {}
    redeem_txs = {
        _tx(row.get("tx")): {**row, "source": "data_api_activity_redeem"}
        for row in redeem_rows
        if _tx(row.get("tx"))
    }
    trade_txs = {
        _tx(row.get("tx")): {
            **row,
            "source": "data_api_activity_trade",
            "match_method": "data_api_activity_trade_tx",
        }
        for row in trade_rows
        if _tx(row.get("tx"))
    }
    public_fetch = {key: value for key, value in fetch.items() if key != "urls"}
    public_fetch["trade_txs"] = len(trade_txs)
    public_fetch["redeem_txs"] = len(redeem_txs)
    return redeem_txs, public_fetch, trade_txs


def _http_status_from_exception(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    for value in (
        getattr(response, "status_code", None),
        getattr(response, "status", None),
        getattr(exc, "code", None),
        getattr(exc, "status", None),
    ):
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _headers_from_exception(exc: Exception) -> dict[str, str]:
    response = getattr(exc, "response", None)
    raw_headers = getattr(response, "headers", None) or getattr(exc, "headers", None)
    if not raw_headers:
        return {}
    try:
        items = raw_headers.items()
    except AttributeError:
        return {}
    headers: dict[str, str] = {}
    for key, value in items:
        normalized = str(key or "").strip().lower()
        if normalized not in TRANSFER_FETCH_RECORDED_HEADERS:
            continue
        headers[normalized] = str(value)[:200]
    return headers


def _retry_after_s(headers: dict[str, str]) -> float | None:
    value = headers.get("retry-after")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def _transfer_backoff_s(exc: Exception, *, failure_index: int) -> float:
    http_status = _http_status_from_exception(exc)
    headers = _headers_from_exception(exc)
    retry_after = _retry_after_s(headers)
    if retry_after is not None:
        return round(min(TRANSFER_FETCH_BACKOFF_CAP_S, retry_after), 3)
    if http_status in {403, 429}:
        return round(min(TRANSFER_FETCH_BACKOFF_CAP_S, 2.0 ** max(0, int(failure_index))), 3)
    return 0.0


def _transfer_source_order(transfer_source: str) -> list[str]:
    if transfer_source == "blockscout":
        return ["blockscout", "rpc", "rpc_secondary"]
    if transfer_source == "rpc":
        return ["rpc", "rpc_secondary"]
    if transfer_source == "rpc_secondary":
        return ["rpc_secondary"]
    return ["blockscout", "rpc", "rpc_secondary"]


def _source_label(source: str) -> str:
    if source == "rpc":
        return "polygon_rpc_eth_getLogs"
    if source in {"rpc_secondary", "rpc_fallback"}:
        return "polygon_rpc_secondary_eth_getLogs"
    return "blockscout_account_tokentx"


def _transfer_attempt(
    *,
    source: str,
    status: str,
    started_at: float,
    rows: list[dict[str, Any]] | None = None,
    fetch: dict[str, Any] | None = None,
    exc: Exception | None = None,
    next_source: str | None = None,
    retry_source: str | None = None,
    backoff_s: float = 0.0,
    cooldown_until: str | None = None,
    cooldown_reason: str | None = None,
) -> dict[str, Any]:
    attempt: dict[str, Any] = {
        "source": _source_label(source),
        "url_source": _source_label(source),
        "status": status,
        "started_at": _iso(started_at),
        "duration_s": round(max(0.0, time.time() - started_at), 3),
        "backoff_s": round(max(0.0, float(backoff_s)), 3),
        "backoff_applied": bool(backoff_s > 0),
    }
    if rows is not None:
        attempt["rows"] = len(rows)
    if fetch:
        attempt["fetch_status"] = fetch.get("status")
        attempt["fetch_source"] = fetch.get("source") or _source_label(source)
        for key in ("from_block", "to_block", "block_mode", "raw_count", "rows_in_window"):
            if key in fetch:
                attempt[key] = fetch.get(key)
    if exc is not None:
        attempt["error_type"] = type(exc).__name__
        attempt["error"] = str(exc)
        http_status = _http_status_from_exception(exc)
        if http_status is not None:
            attempt["http_status"] = http_status
        headers = _headers_from_exception(exc)
        if headers:
            attempt["response_headers"] = headers
        if http_status in {403, 429}:
            attempt["rate_limit_or_policy_suspected"] = True
    if next_source:
        attempt["fallback_source"] = _source_label(next_source)
        attempt["next"] = f"fallback_to_{_source_label(next_source)}"
    if retry_source:
        attempt["retry_source"] = _source_label(retry_source)
        attempt["next"] = f"retry_{_source_label(retry_source)}"
    if cooldown_until:
        attempt["cooldown_until"] = cooldown_until
    if cooldown_reason:
        attempt["cooldown_reason"] = cooldown_reason
    return attempt


def _fetch_transfer_source(
    source: str,
    args: argparse.Namespace,
    *,
    wallet: str,
    start_ts: float,
    end_ts: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if source in {"rpc", "rpc_secondary", "rpc_fallback"}:
        rpc_url = str(args.polygon_rpc_url if source == "rpc" else args.polygon_rpc_fallback_url)
        chunk_blocks = int(args.chunk_blocks)
        from_block_safety_blocks = 5_000
        to_block_safety_blocks = 5_000
        directions = ("out", "in")
        if source in {"rpc_secondary", "rpc_fallback"}:
            chunk_blocks = min(chunk_blocks, int(getattr(args, "rpc_secondary_chunk_blocks", 50)))
            from_block_safety_blocks = max(0, int(getattr(args, "rpc_secondary_from_block_safety_blocks", 0)))
            to_block_safety_blocks = max(0, int(getattr(args, "rpc_secondary_to_block_safety_blocks", 0)))
            directions = ("out",)
        rows, fetch = cash_audit._fetch_transfer_logs(
            wallet=wallet,
            rpc_url=rpc_url,
            start_ts=start_ts,
            end_ts=end_ts,
            chunk_blocks=chunk_blocks,
            timeout_s=float(args.timeout_s),
            block_mode=str(args.block_mode),
            seconds_per_block=float(args.seconds_per_block),
            from_block_safety_blocks=from_block_safety_blocks,
            to_block_safety_blocks=to_block_safety_blocks,
            directions=directions,
        )
        fetch["source"] = _source_label(source)
        fetch["rpc_url"] = rpc_url
        fetch["chunk_blocks"] = chunk_blocks
        return rows, fetch
    rows, fetch = cash_audit._fetch_blockscout_transfer_rows(
        wallet=wallet,
        start_ts=start_ts,
        end_ts=end_ts,
        base_url=str(args.blockscout_base_url),
        timeout_s=float(args.timeout_s),
    )
    return rows, fetch


def _active_transfer_source_cooldowns(previous: dict[str, Any], *, now_ts: float) -> dict[str, dict[str, Any]]:
    raw = previous.get("transfer_source_cooldowns") if isinstance(previous, dict) else {}
    raw = raw if isinstance(raw, dict) else {}
    active: dict[str, dict[str, Any]] = {}
    for source, row in raw.items():
        if not isinstance(row, dict):
            continue
        source_key = str(source or "")
        if source_key == "rpc_fallback":
            source_key = "rpc_secondary"
        if source_key not in {"rpc", "rpc_secondary", "blockscout"}:
            continue
        until_ts = _parse_ts(row.get("cooldown_until"))
        if until_ts <= now_ts:
            continue
        active[source_key] = {
            **row,
            "source": source_key,
            "source_label": _source_label(source_key),
            "cooldown_until": _iso(until_ts),
            "remaining_s": round(max(0.0, until_ts - now_ts), 3),
        }
    return active


def _record_transfer_source_cooldown(
    cooldowns: dict[str, dict[str, Any]],
    *,
    source: str,
    now_ts: float,
    cooldown_s: float,
    exc: Exception,
) -> None:
    until_ts = now_ts + max(0.0, float(cooldown_s))
    cooldowns[source] = {
        "source": source,
        "source_label": _source_label(source),
        "cooldown_until": _iso(until_ts),
        "cooldown_s": round(max(0.0, float(cooldown_s)), 3),
        "reason": "http_403_source_cooldown",
        "http_status": _http_status_from_exception(exc),
        "error_type": type(exc).__name__,
        "set_at": _iso(now_ts),
    }


def _is_retryable_transfer_5xx(exc: Exception) -> bool:
    http_status = _http_status_from_exception(exc)
    return http_status is not None and 500 <= int(http_status) <= 599


def _fetch_transfers(
    args: argparse.Namespace,
    *,
    wallet: str,
    start_ts: float,
    end_ts: float,
    previous: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    transfer_source = str(args.transfer_source or "auto")
    attempts: list[dict[str, Any]] = []
    sources = _transfer_source_order(transfer_source)
    now_ts = end_ts if end_ts > 0 else time.time()
    cooldowns = _active_transfer_source_cooldowns(previous or {}, now_ts=now_ts)
    last_fetch: dict[str, Any] = {
        "status": "ERROR",
        "source_order": [_source_label(source) for source in sources],
        "source_cooldowns": cooldowns,
    }
    for index, source in enumerate(sources):
        next_source = sources[index + 1] if index + 1 < len(sources) else None
        cooldown = cooldowns.get(source)
        if cooldown:
            started_at = time.time()
            attempts.append(
                _transfer_attempt(
                    source=source,
                    status="SKIPPED_COOLDOWN",
                    started_at=started_at,
                    next_source=next_source,
                    cooldown_until=str(cooldown.get("cooldown_until") or ""),
                    cooldown_reason=str(cooldown.get("reason") or "source_cooldown_active"),
                )
            )
            continue
        retry_index = 0
        while True:
            started_at = time.time()
            try:
                rows, fetch = _fetch_transfer_source(
                    source,
                    args,
                    wallet=wallet,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
            except Exception as exc:
                retry_same_source = retry_index == 0 and _is_retryable_transfer_5xx(exc)
                if retry_same_source:
                    backoff_s = max(0.0, float(getattr(args, "transfer_5xx_retry_backoff_s", 1.0)))
                    attempts.append(
                        _transfer_attempt(
                            source=source,
                            status="ERROR",
                            started_at=started_at,
                            exc=exc,
                            retry_source=source,
                            backoff_s=backoff_s,
                        )
                    )
                    if backoff_s > 0:
                        time.sleep(backoff_s)
                    retry_index += 1
                    continue
                http_status = _http_status_from_exception(exc)
                if http_status == 403:
                    _record_transfer_source_cooldown(
                        cooldowns,
                        source=source,
                        now_ts=now_ts,
                        cooldown_s=float(getattr(args, "transfer_403_cooldown_s", DEFAULT_TRANSFER_403_COOLDOWN_S)),
                        exc=exc,
                    )
                planned_backoff_s = _transfer_backoff_s(exc, failure_index=len(attempts))
                backoff_s = planned_backoff_s if next_source else 0.0
                attempts.append(
                    _transfer_attempt(
                        source=source,
                        status="ERROR",
                        started_at=started_at,
                        exc=exc,
                        next_source=next_source,
                        backoff_s=backoff_s,
                    )
                )
                last_fetch = {
                    "status": "ERROR",
                    "source": _source_label(source),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "source_order": [_source_label(item) for item in sources],
                    "source_cooldowns": cooldowns,
                    "attempt_count": len(attempts),
                }
                if http_status is not None:
                    last_fetch["http_status"] = http_status
                headers = _headers_from_exception(exc)
                if headers:
                    last_fetch["response_headers"] = headers
                if next_source and backoff_s > 0:
                    time.sleep(backoff_s)
                break
            cooldowns.pop(source, None)
            attempts.append(_transfer_attempt(source=source, status="OK", started_at=started_at, rows=rows, fetch=fetch))
            fetch["attempt_count"] = len(attempts)
            fetch["source_order"] = [_source_label(item) for item in sources]
            fetch["source_cooldowns"] = cooldowns
            if index > 0:
                fetch["fallback_after_source_error"] = True
                fetch["primary_source_error"] = next(
                    (attempt for attempt in attempts if attempt.get("status") == "ERROR"),
                    attempts[0] if attempts else {},
                )
                if sources[0] == "rpc":
                    fetch["fallback_after_rpc_error"] = True
                if sources[0] == "blockscout":
                    fetch["fallback_after_blockscout_error"] = True
            return rows, fetch, attempts
    raise TransferFetchError(
        "all transfer fetch sources failed",
        attempts=attempts,
        fetch={**last_fetch, "source_cooldowns": cooldowns, "attempts_exhausted": True},
    )




def _annotate_outflow(
    row: dict[str, Any],
    *,
    ledger_txs: dict[str, dict[str, Any]],
    redemption_txs: dict[str, dict[str, Any]],
    previous_pending: dict[str, Any],
    now_ts: float,
    max_unmatched_age_s: float,
    min_outflow_usd: float,
) -> dict[str, Any]:
    tx = _tx(row.get("tx"))
    direction = str(row.get("direction") or "")
    amount = num(row.get("amount_usd"), 0.0)
    out = dict(row)
    out["deadman_relevant"] = False
    out["outflow_match_status"] = "NOT_OUTFLOW"
    out["first_seen_unmatched_at"] = None
    out["unmatched_age_s"] = 0.0
    out["block_age_s"] = None
    out["incident_overdue"] = False
    if direction != "OUT" or amount < float(min_outflow_usd):
        return out
    out["deadman_relevant"] = True
    if tx and tx in ledger_txs:
        out["outflow_match_status"] = "MATCHED_LIVE_ORDER_LEDGER"
        out["matched_evidence"] = ledger_txs[tx]
        return out
    if tx and tx in redemption_txs:
        out["outflow_match_status"] = "MATCHED_REDEMPTION_RECORD"
        out["matched_evidence"] = redemption_txs[tx]
        return out
    matched_evidence = row.get("matched_evidence") if isinstance(row.get("matched_evidence"), dict) else {}
    if matched_evidence.get("source") == "live_execution_ledger_amount_time":
        out["outflow_match_status"] = "MATCHED_LIVE_ORDER_LEDGER"
        out["matched_evidence"] = matched_evidence
        return out
    key = _event_key(row)
    prev = previous_pending.get(key) if isinstance(previous_pending.get(key), dict) else {}
    first_seen = str(prev.get("first_seen_unmatched_at") or _iso(now_ts))
    first_seen_ts = _parse_ts(first_seen)
    block_ts = num(row.get("block_ts"), 0.0)
    block_age_s = max(0.0, now_ts - block_ts) if block_ts > 0 else None
    age_s = max(0.0, now_ts - first_seen_ts) if first_seen_ts > 0 else 0.0
    overdue = age_s >= float(max_unmatched_age_s) or (block_age_s is not None and block_age_s >= float(max_unmatched_age_s))
    out["outflow_match_status"] = "UNMATCHED_OUTFLOW"
    out["first_seen_unmatched_at"] = first_seen
    out["unmatched_age_s"] = round(age_s, 6)
    out["block_age_s"] = round(block_age_s, 6) if block_age_s is not None else None
    out["incident_overdue"] = bool(overdue)
    out["deadman_key"] = key
    out["next_action"] = "match tx to ledger/redemption or treat as wallet-security incident"
    return out


def _live_guard_outflow_escalation_state(args: argparse.Namespace, *, end_ts: float) -> dict[str, Any]:
    guard = load_json(getattr(args, "live_guard_state", DEFAULT_LIVE_GUARD_STATE), default={})
    guard = guard if isinstance(guard, dict) else {}
    armed_after_ts = _parse_ts(getattr(args, "live_armed_after_iso", DEFAULT_LIVE_ARMED_AFTER_ISO))
    post_activation = bool(armed_after_ts > 0 and end_ts >= armed_after_ts)
    execute_live = bool(guard.get("execute_live"))
    live_orders_allowed = bool(guard.get("live_orders_allowed"))
    armed = bool(live_orders_allowed or (execute_live and post_activation))
    return {
        "live_armed_for_outflow_watch": armed,
        "live_orders_allowed": live_orders_allowed,
        "execute_live": execute_live,
        "post_activation": post_activation,
        "armed_after_iso": _iso(armed_after_ts),
        "guard_status": guard.get("status"),
        "guard_generated_at": guard.get("generated_at"),
        "guard_state": str(getattr(args, "live_guard_state", DEFAULT_LIVE_GUARD_STATE)),
    }


def _append_handoff(report: dict[str, Any], *, handoff_path: Path, previous: dict[str, Any]) -> dict[str, Any]:
    status = str(report.get("status") or "")
    updates: dict[str, Any] = {}
    handoff_path.parent.mkdir(parents=True, exist_ok=True)
    live_armed_incident = report.get("live_armed_degraded_fetch_incident")
    live_armed_incident = live_armed_incident if isinstance(live_armed_incident, dict) else {}
    if live_armed_incident.get("active") is True:
        already_written = str(previous.get("live_armed_degraded_fetch_notify_written_at") or "")
        if not already_written:
            fetch = report.get("fetch") if isinstance(report.get("fetch"), dict) else {}
            transfers = fetch.get("transfers") if isinstance(fetch.get("transfers"), dict) else {}
            attempts = fetch.get("transfer_attempts") if isinstance(fetch.get("transfer_attempts"), list) else []
            attempted_sources = ",".join(str(row.get("source") or "") for row in attempts[-5:] if isinstance(row, dict))
            with handoff_path.open("a", encoding="utf-8") as handle:
                handle.write(
                    "\n"
                    f"## {report.get('checked_at')} brainless NOTIFY - WALLET_OUTFLOW_DEADMAN INCIDENT\n"
                    f"- outflow_deadman [LIVE/DEFEND]: live-armed transfer fetch blind; status={status} "
                    f"consecutive_degraded_fetches={report.get('consecutive_degraded_fetches')}/"
                    f"{live_armed_incident.get('threshold')}; "
                    f"fetch_error={transfers.get('error_type') or transfers.get('status')}; "
                    f"attempted_sources={attempted_sources}; "
                    "next=restore transfer fetch coverage before tolerating live order flow.\n"
                )
            updates["live_armed_degraded_fetch_notify_written_at"] = str(report.get("checked_at") or utc_now_iso())

    incident_keys = sorted(report.get("incident_keys") or [])
    if incident_keys:
        current_keys = sorted(
            row.get("deadman_key") for row in report.get("unmatched_outflows") or [] if row.get("incident_overdue")
        )
        previous_keys = sorted(previous.get("incident_keys") or [])
        if not (previous.get("status") == "INCIDENT_UNEXPLAINED_OUTFLOW" and current_keys == previous_keys):
            total = num((report.get("summary") or {}).get("unmatched_outflow_usd"), 0.0)
            txs = ", ".join(str(row.get("tx")) for row in (report.get("unmatched_outflows") or [])[:5])
            with handoff_path.open("a", encoding="utf-8") as handle:
                handle.write(
                    "\n"
                    f"## {report.get('checked_at')} brainless NOTIFY - WALLET_OUTFLOW_DEADMAN INCIDENT\n"
                    f"- outflow_deadman [LIVE/DEFEND]: unmatched outgoing pUSD rows="
                    f"{len(report.get('unmatched_outflows') or [])} total_usd={total:.6f}; txs={txs}; "
                    "next=inspect on-chain txs, match to ledger/redemptions if legitimate, otherwise rotate keys/allowances and ask Fable.\n"
                )

    if not status.startswith("DEGRADED_FETCH"):
        return updates
    consecutive = int(report.get("consecutive_degraded_fetches") or 0)
    threshold = int(report.get("degraded_notify_threshold") or 3)
    previous_status = str(previous.get("status") or "")
    previous_notify_written_at = str(previous.get("degraded_notify_written_at") or "")
    gap_exceeded = bool((report.get("window") or {}).get("fetch_gap_exceeded"))
    if consecutive < threshold and not gap_exceeded:
        return updates
    if previous_notify_written_at and previous_status == status:
        return updates
    window = report.get("window") if isinstance(report.get("window"), dict) else {}
    fetch = report.get("fetch") if isinstance(report.get("fetch"), dict) else {}
    transfers = fetch.get("transfers") if isinstance(fetch.get("transfers"), dict) else {}
    with handoff_path.open("a", encoding="utf-8") as handle:
        handle.write(
            "\n"
            f"## {report.get('checked_at')} brainless NOTIFY - WALLET_OUTFLOW_DEADMAN DEGRADED\n"
            f"- outflow_deadman [LIVE/DEFEND]: status={status} consecutive_degraded_fetches="
            f"{consecutive}/{threshold}; window={window.get('start_iso')}..{window.get('end_iso')} "
            f"last_ok_checked_at={report.get('last_ok_checked_at')} "
            f"fetch_error={transfers.get('error_type') or transfers.get('status')}; "
            "next=restore transfer fetch source or use a working indexed/RPC source, then rerun the deadman before treating wallet outflow state as covered.\n"
        )
    updates["degraded_notify_written_at"] = str(report.get("checked_at") or utc_now_iso())
    return updates


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    timing = _new_timing()
    phase_started = time.perf_counter()
    previous = load_json(args.state, default={})
    previous = previous if isinstance(previous, dict) else {}
    start_ts, end_ts, window_source, fetch_gap_exceeded = _window(args, previous)
    checked_at = _iso(end_ts) or utc_now_iso()
    wallet = _norm_addr(args.user) or _norm_addr(_load_dotenv_value("POLYMARKET_PROXY"))
    _finish_timing_phase(
        timing,
        "load_previous_window_wallet",
        phase_started,
        window_source=window_source,
        fetch_gap_exceeded=fetch_gap_exceeded,
        wallet_present=bool(wallet),
    )
    if not wallet:
        report = {
            "kind": "wallet_outflow_deadman",
            "flow_stage": "LIVE/DEFEND/SELF-DEV",
            "checked_at": checked_at,
            "status": "DEGRADED_WALLET_MISSING",
            "incident": False,
            "next_action": "set POLYMARKET_PROXY or pass --user so outflow deadman can run",
            "live_path_mutated": False,
            "timing": timing,
        }
        atomic_write_json(args.state, report)
        return report

    phase_started = time.perf_counter()
    ledger = load_json(args.ledger, default={})
    ledger = ledger if isinstance(ledger, dict) else {}
    ledger_txs = cash_audit._ledger_fill_txs(ledger, start_ts=start_ts, end_ts=end_ts)
    ledger_settlement_candidates = _ledger_fill_settlement_candidates(ledger, start_ts=start_ts, end_ts=end_ts)
    h2_external = load_json(args.h2_external, default={})
    h2_external = h2_external if isinstance(h2_external, dict) else {}
    redemption_txs = {
        **cash_audit._h2_redeem_txs(h2_external),
        **_own_redeem_txs(args.own_redeem_events),
    }
    _finish_timing_phase(
        timing,
        "load_ledger_and_match_sets",
        phase_started,
        ledger_fill_txs=len(ledger_txs),
        settlement_candidates=len(ledger_settlement_candidates),
        redemption_txs=len(redemption_txs),
    )
    phase_started = time.perf_counter()
    activity_txs, activity_fetch, activity_trade_txs = _activity_live_order_txs(
        args,
        wallet=wallet,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    ledger_txs.update(activity_trade_txs)
    redemption_txs.update(activity_txs)
    _finish_timing_phase(
        timing,
        "fetch_data_api_activity",
        phase_started,
        status=activity_fetch.get("status") if isinstance(activity_fetch, dict) else None,
        trade_txs=len(activity_trade_txs),
        redeem_txs=len(activity_txs),
    )
    transfer_attempts: list[dict[str, Any]] = []
    phase_started = time.perf_counter()
    try:
        transfer_rows, transfer_fetch, transfer_attempts = _fetch_transfers(
            args,
            wallet=wallet,
            start_ts=start_ts,
            end_ts=end_ts,
            previous=previous,
        )
        fetch_status = "OK"
    except TransferFetchError as exc:
        transfer_rows = []
        transfer_fetch = {**exc.fetch, "status": "ERROR", "error_type": type(exc).__name__, "error": str(exc)}
        transfer_attempts = exc.attempts
        fetch_status = "ERROR"
    except Exception as exc:  # noqa: BLE001 - write a state artifact instead of losing the deadman result.
        transfer_rows = []
        transfer_fetch = {"status": "ERROR", "error_type": type(exc).__name__, "error": str(exc)}
        fetch_status = "ERROR"
    _finish_timing_phase(
        timing,
        "fetch_transfers",
        phase_started,
        status=fetch_status,
        rows=len(transfer_rows),
        attempted_sources=[row.get("source") for row in transfer_attempts if isinstance(row, dict)],
        transfer_attempts=_transfer_attempt_timing_rows(transfer_attempts),
        pre_blockscout_attempt_duration_s=_pre_blockscout_attempt_duration_s(transfer_attempts),
    )
    previous_degraded_fetches = int(previous.get("consecutive_degraded_fetches") or 0)
    consecutive_degraded_fetches = previous_degraded_fetches + 1 if fetch_status != "OK" else 0
    previous_last_ok = _previous_last_ok_checked_at(previous)
    last_ok_checked_at = checked_at if fetch_status == "OK" else _iso(previous_last_ok)
    previous_pending = previous.get("pending_unmatched") if isinstance(previous.get("pending_unmatched"), dict) else {}
    now_ts = end_ts
    phase_started = time.perf_counter()
    classified = [
        _match_ledger_settlement_by_amount_time(
            cash_audit.classify_transfer(
                row,
                wallet=wallet,
                start_ts=start_ts,
                ledger_txs=ledger_txs,
                redeem_txs=redemption_txs,
                activity_redeem_txs=activity_txs,
            ),
            ledger_settlement_candidates,
        )
        for row in transfer_rows
    ]
    annotated = [
        _annotate_outflow(
            row,
            ledger_txs=ledger_txs,
            redemption_txs=redemption_txs,
            previous_pending=previous_pending,
            now_ts=now_ts,
            max_unmatched_age_s=float(args.max_unmatched_age_s),
            min_outflow_usd=float(args.min_outflow_usd),
        )
        for row in classified
    ]
    relevant = [row for row in annotated if row.get("deadman_relevant")]
    unmatched = [row for row in relevant if row.get("outflow_match_status") == "UNMATCHED_OUTFLOW"]
    incident_rows = [row for row in unmatched if row.get("incident_overdue")]
    _finish_timing_phase(
        timing,
        "classify_and_match_transfers",
        phase_started,
        transfer_rows=len(transfer_rows),
        relevant_outflows=len(relevant),
        unmatched=len(unmatched),
        incident_rows=len(incident_rows),
    )
    pending = {
        str(row.get("deadman_key")): {
            "first_seen_unmatched_at": row.get("first_seen_unmatched_at"),
            "tx": row.get("tx"),
            "amount_usd": row.get("amount_usd"),
            "block_iso": row.get("block_iso"),
        }
        for row in unmatched
        if row.get("deadman_key")
    }
    live_guard_escalation = _live_guard_outflow_escalation_state(args, end_ts=end_ts)
    live_armed_degraded_fetch_incident = bool(
        fetch_status != "OK"
        and live_guard_escalation.get("live_armed_for_outflow_watch") is True
        and consecutive_degraded_fetches >= int(getattr(args, "live_armed_degraded_incident_threshold", 12))
    )
    incident = bool(incident_rows) or live_armed_degraded_fetch_incident
    if live_armed_degraded_fetch_incident:
        status = "INCIDENT_OUTFLOW_FETCH_BLIND_LIVE_ARMED"
    elif fetch_gap_exceeded:
        status = "DEGRADED_FETCH_GAP_EXCEEDED"
    elif fetch_status != "OK":
        status = "DEGRADED_FETCH"
    elif incident:
        status = "INCIDENT_UNEXPLAINED_OUTFLOW"
    elif unmatched:
        status = "WATCH_UNMATCHED_OUTFLOW"
    else:
        status = "OK"
    report = {
        "kind": "wallet_outflow_deadman",
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "checked_at": checked_at,
        "status": status,
        "incident": incident,
        "wallet": wallet,
        "collateral_token": cash_audit.POLYMARKET_COLLATERAL_TOKEN_ADDRESS,
        "window": {
            "start_iso": _iso(start_ts),
            "end_iso": _iso(end_ts),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "source": window_source,
            "fetch_gap_exceeded": fetch_gap_exceeded,
            "max_fetch_gap_s": float(args.max_fetch_gap_s),
        },
        "fetch": {
            "status": fetch_status,
            "transfers": transfer_fetch,
            "transfer_attempts": transfer_attempts,
            "data_api_activity": activity_fetch,
            "ledger_fill_txs": len(ledger_txs),
            "data_api_activity_trade_txs": len(activity_trade_txs),
            "ledger_settlement_amount_time_candidates": len(ledger_settlement_candidates),
            "redemption_txs": len(redemption_txs),
        },
        "transfer_source_cooldowns": transfer_fetch.get("source_cooldowns")
        if isinstance(transfer_fetch.get("source_cooldowns"), dict)
        else _active_transfer_source_cooldowns(previous, now_ts=end_ts),
        "last_ok_checked_at": last_ok_checked_at,
        "consecutive_degraded_fetches": consecutive_degraded_fetches,
        "degraded_notify_threshold": int(args.degraded_notify_threshold),
        "degraded_notify_written_at": str(previous.get("degraded_notify_written_at") or "")
        if status.startswith("DEGRADED_FETCH")
        else "",
        "live_armed_degraded_fetch_notify_written_at": str(
            previous.get("live_armed_degraded_fetch_notify_written_at") or ""
        )
        if live_armed_degraded_fetch_incident
        else "",
        "live_armed_degraded_fetch_incident": {
            "active": live_armed_degraded_fetch_incident,
            "threshold": int(getattr(args, "live_armed_degraded_incident_threshold", 12)),
            "consecutive_degraded_fetches": consecutive_degraded_fetches,
            "guard": live_guard_escalation,
        },
        "summary": {
            "transfer_rows": len(annotated),
            "outflow_rows": len(relevant),
            "matched_order_outflows": sum(1 for row in relevant if row.get("outflow_match_status") == "MATCHED_LIVE_ORDER_LEDGER"),
            "matched_redemption_outflows": sum(1 for row in relevant if row.get("outflow_match_status") == "MATCHED_REDEMPTION_RECORD"),
            "unmatched_outflows": len(unmatched),
            "incident_outflows": len(incident_rows),
            "unmatched_outflow_usd": round(sum(num(row.get("amount_usd"), 0.0) for row in unmatched), 6),
            "incident_outflow_usd": round(sum(num(row.get("amount_usd"), 0.0) for row in incident_rows), 6),
            "max_unmatched_age_s": float(args.max_unmatched_age_s),
            "min_outflow_usd": float(args.min_outflow_usd),
        },
        "pending_unmatched": pending,
        "incident_keys": sorted(row.get("deadman_key") for row in incident_rows if row.get("deadman_key")),
        "unmatched_outflows": unmatched[:50],
        "recent_outflows": relevant[-50:],
        "timing": timing,
        "next_action": (
            "restore transfer fetch coverage before live order flow continues"
            if live_armed_degraded_fetch_incident
            else "inspect on-chain txs and rotate keys/allowances if no ledger/redemption match"
            if incident_rows
            else (
                "re-run next brainless cycle; incident if still unmatched after one cycle"
                if unmatched
                else "continue brainless outflow watch"
            )
        ),
        "live_path_mutated": False,
        "paper_only": True,
    }
    if args.write_handoff_on_incident:
        notify_updates = _append_handoff(report, handoff_path=Path(args.handoff), previous=previous)
        if notify_updates:
            report.update(notify_updates)
    if args.write_handoff_on_incident:
        report["notify_state_persisted_after_handoff_append"] = True
    timing["total_s_before_write"] = round(
        sum(float(row.get("duration_s") or 0.0) for row in timing.get("phases", []) if isinstance(row, dict)),
        6,
    )
    timing["completed_before_write_at"] = utc_now_iso()
    atomic_write_json(args.state, report)
    return report


def main() -> int:
    args = parse_args()
    report = build_report(args)
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "incident": report.get("incident"),
                "summary": report.get("summary"),
                "state": str(args.state),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
