#!/usr/bin/env python3
"""Audit non-fill pUSD cash movements for the wallet-copy guard wallet.

Flow stage: LEARN/LIVE/SELF-DEV. This report is read-only: it enumerates
Polymarket collateral ERC-20 Transfer rows touching the guard wallet since the
last top-up baseline and classifies the non-fill cash/account-value residual.
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.collateral import (  # noqa: E402
    POLYMARKET_COLLATERAL_TOKEN_ADDRESS,
    POLYMARKET_COLLATERAL_TOKEN_DECIMALS,
)
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.own_positions import POLYMARKET_CTF  # noqa: E402
from src.wallet_copy.polymarket_addresses import EXCHANGE_ADDRESSES  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_2026-07-09_current.json"
DEFAULT_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_H2_EXTERNAL = "data/research/h2_external_redemption_ingestion_latest.json"
DEFAULT_H2_RESIDUAL = "data/research/h2_account_value_residual_reconstruction_latest.json"
DEFAULT_ITEM3 = "data/research/wallet_copy_item3_residual_overlay_bracket_latest.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_item4_nonfill_cash_audit_latest.json"
DEFAULT_PROVENANCE_OUTPUT = "data/research/wallet_copy_cash_residual_provenance_latest.json"
DEFAULT_POLYGON_RPC_URL = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")
DEFAULT_DATA_API_BASE = os.getenv("POLYMARKET_DATA_API_BASE_URL", "https://data-api.polymarket.com")
DEFAULT_BLOCKSCOUT_BASE = "https://polygon.blockscout.com/api"
DEFAULT_BASELINE_ISO = "2026-07-05T12:55:00Z"

TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
BALANCE_OF_SELECTOR = "0x70a08231"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
KNOWN_SETTLEMENT_COUNTERPARTIES = EXCHANGE_ADDRESSES | {POLYMARKET_CTF.lower()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--h2-external", default=DEFAULT_H2_EXTERNAL)
    parser.add_argument("--h2-residual", default=DEFAULT_H2_RESIDUAL)
    parser.add_argument("--item3", default=DEFAULT_ITEM3)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--timestamped-output", default="")
    parser.add_argument("--user", default="")
    parser.add_argument("--start-iso", default="")
    parser.add_argument("--end-iso", default="")
    parser.add_argument("--polygon-rpc-url", default=DEFAULT_POLYGON_RPC_URL)
    parser.add_argument("--data-api-base-url", default=DEFAULT_DATA_API_BASE)
    parser.add_argument("--blockscout-base-url", default=DEFAULT_BLOCKSCOUT_BASE)
    parser.add_argument("--transfer-source", choices=("auto", "rpc", "blockscout"), default="auto")
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--chunk-blocks", type=int, default=7_500)
    parser.add_argument("--block-mode", choices=("estimate", "exact"), default="estimate")
    parser.add_argument("--seconds-per-block", type=float, default=2.1)
    parser.add_argument("--data-api-limit", type=int, default=500)
    parser.add_argument("--data-api-max-pages", type=int, default=20)
    parser.add_argument("--current-residual-start-scorecard", default="")
    parser.add_argument("--current-residual-end-scorecard", default="")
    return parser.parse_args()


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


def _iso(ts: float | None) -> str | None:
    if ts is None or ts <= 0:
        return None
    return datetime.fromtimestamp(float(ts), tz=UTC).isoformat().replace("+00:00", "Z")


def _rpc_post(url: str, method: str, params: list[Any], *, timeout_s: float) -> Any:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "wallet-copy-item4-cash-audit/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=float(timeout_s)) as response:
        loaded = json.loads(response.read().decode("utf-8"))
    if isinstance(loaded, dict) and loaded.get("error"):
        raise RuntimeError(loaded["error"])
    return loaded.get("result") if isinstance(loaded, dict) else None


def _urlopen_json_with_retry(request: urllib.request.Request, *, timeout_s: float, attempts: int = 3) -> Any:
    last_exc: Exception | None = None
    for attempt in range(max(1, int(attempts))):
        try:
            with urllib.request.urlopen(request, timeout=float(timeout_s)) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 - transient explorer failures are common.
            last_exc = exc
            if attempt + 1 >= max(1, int(attempts)):
                break
            time.sleep(1.5 * (attempt + 1))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("request failed without exception")


def _hex_int(value: Any) -> int | None:
    text = str(value or "")
    if not text.startswith("0x"):
        return None
    try:
        return int(text, 16)
    except ValueError:
        return None


def _topic_address(value: Any) -> str:
    text = str(value or "").lower()
    if text.startswith("0x") and len(text) == 66:
        return "0x" + text[-40:]
    return ""


def _address_topic(address: str) -> str:
    return "0x" + ("0" * 24) + _norm_addr(address).removeprefix("0x")


def _decode_uint256(value: Any) -> int:
    text = str(value or "0x0")
    if not text.startswith("0x"):
        return 0
    try:
        return int(text, 16)
    except ValueError:
        return 0


def _round_usd(value: float) -> float:
    return round(float(value), 6)


def _canonical_checksum(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _scorecard_residual_endpoint(scorecard: dict[str, Any], path: str) -> dict[str, Any]:
    chain = scorecard.get("chain_reconciliation") if isinstance(scorecard.get("chain_reconciliation"), dict) else {}
    sampling = chain.get("balance_sampling") if isinstance(chain.get("balance_sampling"), dict) else {}
    samples = [row for row in sampling.get("samples") or [] if isinstance(row, dict)]
    selected = samples[-1] if samples else {}
    return {
        "scorecard": path,
        "scorecard_generated_at": scorecard.get("generated_at"),
        "balance_sample_at": selected.get("ts") or scorecard.get("generated_at"),
        "balance_usd": (
            chain.get("live_cash_balance_usd")
            if chain.get("live_cash_balance_usd") is not None
            else sampling.get("selected_balance_usd")
        ),
        "expected_cash_identity_usd": chain.get("expected_cash_identity_usd"),
        "residual_usd": (
            chain.get("cash_delta_vs_expected_identity_usd")
            if chain.get("cash_delta_vs_expected_identity_usd") is not None
            else chain.get("delta_vs_expected_usd")
        ),
    }


def _exact_attribution(rows: list[dict[str, Any]], target: float, tolerance: float = 0.01) -> list[dict[str, Any]]:
    candidates = [row for row in rows if abs(num(row.get("signed_amount_usd"), 0.0)) > 0]
    for width in (1, 2, 3):
        for group in itertools.combinations(candidates, width):
            total = sum(num(row.get("signed_amount_usd"), 0.0) for row in group)
            if abs(total - target) <= tolerance:
                return list(group)
    return []


def _fmt_usd_or_unavailable(value: Any) -> str:
    if value is None:
        return "unavailable"
    try:
        return f"{float(value):.6f}"
    except (TypeError, ValueError):
        return "unavailable"


def _block_ts(rpc_url: str, block_number: int | None, cache: dict[int, float], timeout_s: float) -> float | None:
    if block_number is None:
        return None
    if block_number in cache:
        return cache[block_number]
    block = _rpc_post(rpc_url, "eth_getBlockByNumber", [hex(block_number), False], timeout_s=timeout_s)
    if not isinstance(block, dict):
        return None
    ts = _hex_int(block.get("timestamp"))
    if ts is None:
        return None
    cache[block_number] = float(ts)
    return float(ts)


def _find_block_at_or_before_ts(
    rpc_url: str,
    target_ts: float,
    *,
    latest_block: int,
    latest_ts: float,
    timeout_s: float,
) -> int:
    if target_ts >= latest_ts:
        return latest_block
    # Polygon block time is usually near 2s. Start with a generous lower bound,
    # then binary-search exact timestamps to avoid relying on the estimate.
    estimated_span = int(max(0.0, latest_ts - target_ts) / 1.5) + 10_000
    low = max(0, latest_block - estimated_span)
    high = latest_block
    cache: dict[int, float] = {}
    while low < high:
        mid = (low + high + 1) // 2
        mid_ts = _block_ts(rpc_url, mid, cache, timeout_s) or 0.0
        if mid_ts <= target_ts:
            low = mid
        else:
            high = mid - 1
    return low


def _estimate_block_at_or_before_ts(
    target_ts: float,
    *,
    latest_block: int,
    latest_ts: float,
    seconds_per_block: float,
    safety_blocks: int = 5_000,
) -> int:
    if target_ts >= latest_ts:
        return latest_block
    span_blocks = int(max(0.0, latest_ts - target_ts) / max(0.5, float(seconds_per_block)))
    return max(0, latest_block - span_blocks - max(0, int(safety_blocks)))


def _estimated_block_ts(
    block_number: int | None,
    *,
    latest_block: int,
    latest_ts: float,
    seconds_per_block: float,
) -> float | None:
    if block_number is None:
        return None
    return float(latest_ts) - (int(latest_block) - int(block_number)) * float(seconds_per_block)


def _normalize_transfer_log(
    row: dict[str, Any],
    *,
    wallet: str,
    rpc_url: str,
    timeout_s: float,
    block_ts_cache: dict[int, float],
    latest_block: int | None = None,
    latest_ts: float | None = None,
    seconds_per_block: float = 2.1,
    exact_timestamp: bool = True,
) -> dict[str, Any] | None:
    topics = row.get("topics") if isinstance(row.get("topics"), list) else []
    if len(topics) < 3:
        return None
    from_addr = _topic_address(topics[1])
    to_addr = _topic_address(topics[2])
    if wallet not in {from_addr, to_addr}:
        return None
    block_number = _hex_int(row.get("blockNumber"))
    if exact_timestamp:
        ts = _block_ts(rpc_url, block_number, block_ts_cache, timeout_s)
        ts_source = "rpc_block_timestamp"
    else:
        ts = _estimated_block_ts(
            block_number,
            latest_block=int(latest_block or 0),
            latest_ts=float(latest_ts or 0.0),
            seconds_per_block=float(seconds_per_block),
        )
        ts_source = "estimated_from_latest_block"
    amount_raw = _decode_uint256(row.get("data"))
    amount = amount_raw / (10 ** POLYMARKET_COLLATERAL_TOKEN_DECIMALS)
    direction = "IN" if to_addr == wallet else "OUT"
    counterparty = from_addr if direction == "IN" else to_addr
    return {
        "tx": str(row.get("transactionHash") or "").lower(),
        "block_number": block_number,
        "block_ts": ts,
        "block_iso": _iso(ts),
        "block_ts_source": ts_source,
        "log_index": _hex_int(row.get("logIndex")),
        "direction": direction,
        "from": from_addr,
        "to": to_addr,
        "counterparty": counterparty,
        "amount_raw": str(amount_raw),
        "amount_usd": _round_usd(amount),
    }


def _fetch_transfer_logs(
    *,
    wallet: str,
    rpc_url: str,
    start_ts: float,
    end_ts: float,
    chunk_blocks: int,
    timeout_s: float,
    block_mode: str = "estimate",
    seconds_per_block: float = 2.1,
    from_block_safety_blocks: int = 5_000,
    to_block_safety_blocks: int = 5_000,
    directions: tuple[str, ...] = ("out", "in"),
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    block_ts_cache: dict[int, float] = {}
    latest_block_payload = _rpc_post(rpc_url, "eth_getBlockByNumber", ["latest", False], timeout_s=timeout_s)
    if isinstance(latest_block_payload, dict):
        latest_block = int(str(latest_block_payload.get("number")), 16)
        latest_ts = float(int(str(latest_block_payload.get("timestamp")), 16))
        block_ts_cache[latest_block] = latest_ts
    else:
        latest_hex = _rpc_post(rpc_url, "eth_blockNumber", [], timeout_s=timeout_s)
        latest_block = int(str(latest_hex), 16)
        latest_ts = time.time()
    exact_mode = str(block_mode or "estimate") == "exact"
    if exact_mode:
        from_block = _find_block_at_or_before_ts(
            rpc_url,
            start_ts,
            latest_block=latest_block,
            latest_ts=latest_ts,
            timeout_s=timeout_s,
        )
        to_block = _find_block_at_or_before_ts(
            rpc_url,
            end_ts,
            latest_block=latest_block,
            latest_ts=latest_ts,
            timeout_s=timeout_s,
        )
    else:
        from_block = _estimate_block_at_or_before_ts(
            start_ts,
            latest_block=latest_block,
            latest_ts=latest_ts,
            seconds_per_block=float(seconds_per_block),
            safety_blocks=max(0, int(from_block_safety_blocks)),
        )
        to_block = min(
            latest_block,
            _estimate_block_at_or_before_ts(
                end_ts,
                latest_block=latest_block,
                latest_ts=latest_ts,
                seconds_per_block=float(seconds_per_block),
                safety_blocks=0,
            )
            + max(0, int(to_block_safety_blocks)),
        )
    wallet_topic = _address_topic(wallet)
    all_filter_specs = [
        ("out", [TRANSFER_TOPIC0, wallet_topic]),
        ("in", [TRANSFER_TOPIC0, None, wallet_topic]),
    ]
    requested_directions = {str(item).lower() for item in directions}
    filter_specs = [(role, topics) for role, topics in all_filter_specs if role in requested_directions]
    rows: dict[tuple[str, int | None], dict[str, Any]] = {}
    calls = 0
    for role, topics in filter_specs:
        current = from_block
        while current <= to_block:
            end = min(to_block, current + max(1, int(chunk_blocks)) - 1)
            params = [
                {
                    "fromBlock": hex(current),
                    "toBlock": hex(end),
                    "address": POLYMARKET_COLLATERAL_TOKEN_ADDRESS,
                    "topics": topics,
                }
            ]
            calls += 1
            payload = _rpc_post(rpc_url, "eth_getLogs", params, timeout_s=timeout_s)
            for raw in payload or []:
                if not isinstance(raw, dict):
                    continue
                row = _normalize_transfer_log(
                    raw,
                    wallet=wallet,
                    rpc_url=rpc_url,
                    timeout_s=timeout_s,
                    block_ts_cache=block_ts_cache,
                    latest_block=latest_block,
                    latest_ts=latest_ts,
                    seconds_per_block=float(seconds_per_block),
                    exact_timestamp=exact_mode,
                )
                if not row:
                    continue
                row["matched_topic_role"] = role
                ts = float(row.get("block_ts") or 0.0)
                if exact_mode:
                    if start_ts <= ts <= end_ts:
                        rows[(str(row.get("tx") or ""), row.get("log_index"))] = row
                else:
                    # Estimated timestamps are only used to avoid log history
                    # pagination misses on rate-limited public RPCs. Keep rows
                    # in the estimated block window and make the timestamp
                    # provenance explicit in the artifact.
                    rows[(str(row.get("tx") or ""), row.get("log_index"))] = row
            current = end + 1
    return sorted(rows.values(), key=lambda item: (float(item.get("block_ts") or 0.0), int(item.get("log_index") or 0))), {
        "status": "OK",
        "from_block": from_block,
        "to_block": to_block,
        "latest_block": latest_block,
        "latest_ts": latest_ts,
        "calls": calls,
        "chunk_blocks": int(chunk_blocks),
        "from_block_safety_blocks": int(from_block_safety_blocks),
        "to_block_safety_blocks": int(to_block_safety_blocks),
        "block_mode": str(block_mode or "estimate"),
        "seconds_per_block": float(seconds_per_block),
        "directions": [role for role, _ in filter_specs],
    }


def _fetch_blockscout_block_at_or_before_ts(*, base_url: str, ts: float, timeout_s: float) -> int:
    block_query = urllib.parse.urlencode(
        {"module": "block", "action": "getblocknobytime", "timestamp": int(float(ts)), "closest": "before"}
    )
    block_url = f"{base_url.rstrip('/')}?{block_query}"
    block_request = urllib.request.Request(
        block_url,
        headers={"User-Agent": "wallet-copy-item4-cash-audit/1.0", "Accept": "application/json"},
    )
    payload = _urlopen_json_with_retry(block_request, timeout_s=float(timeout_s), attempts=3)
    result = payload.get("result") if isinstance(payload, dict) else {}
    if isinstance(result, dict):
        return int(num(result.get("blockNumber"), 0.0))
    return int(num(result, 0.0))


def _fetch_blockscout_transfer_rows(
    *,
    wallet: str,
    start_ts: float,
    end_ts: float,
    base_url: str,
    timeout_s: float,
    offset: int = 10_000,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    block_lookup_errors: list[dict[str, Any]] = []

    try:
        start_block = _fetch_blockscout_block_at_or_before_ts(
            base_url=base_url,
            ts=start_ts,
            timeout_s=float(timeout_s),
        )
    except Exception as exc:  # noqa: BLE001 - timestamp filter below still bounds rows.
        block_lookup_errors.append(
            {"which": "start", "error_type": type(exc).__name__, "error": str(exc), "fallback": 0}
        )
        start_block = 0
    try:
        end_block = _fetch_blockscout_block_at_or_before_ts(
            base_url=base_url,
            ts=end_ts,
            timeout_s=float(timeout_s),
        )
    except Exception as exc:  # noqa: BLE001 - timestamp filter below still bounds rows.
        block_lookup_errors.append(
            {"which": "end", "error_type": type(exc).__name__, "error": str(exc), "fallback": 99_999_999}
        )
        end_block = 99_999_999
    query = urllib.parse.urlencode(
        {
            "module": "account",
            "action": "tokentx",
            "contractaddress": POLYMARKET_COLLATERAL_TOKEN_ADDRESS,
            "address": wallet,
            "startblock": start_block,
            "endblock": end_block,
            "page": 1,
            "offset": int(offset),
            "sort": "asc",
        }
    )
    url = f"{base_url.rstrip('/')}?{query}"
    request = urllib.request.Request(url, headers={"User-Agent": "wallet-copy-item4-cash-audit/1.0", "Accept": "application/json"})
    payload = _urlopen_json_with_retry(request, timeout_s=float(timeout_s), attempts=3)
    raw_rows = payload.get("result") if isinstance(payload, dict) else []
    if not isinstance(raw_rows, list):
        raise RuntimeError(f"blockscout returned non-list result: {payload!r}")
    rows: list[dict[str, Any]] = []
    for idx, raw in enumerate(raw_rows):
        if not isinstance(raw, dict):
            continue
        ts = _parse_ts(raw.get("timeStamp"))
        if not (start_ts <= ts <= end_ts):
            continue
        from_addr = _norm_addr(raw.get("from"))
        to_addr = _norm_addr(raw.get("to"))
        if wallet not in {from_addr, to_addr}:
            continue
        decimals = int(num(raw.get("tokenDecimal"), POLYMARKET_COLLATERAL_TOKEN_DECIMALS))
        amount = _decode_uint256(str(hex(int(str(raw.get("value") or "0"))))) / (10 ** decimals)
        direction = "IN" if to_addr == wallet else "OUT"
        rows.append(
            {
                "tx": str(raw.get("hash") or "").lower(),
                "block_number": int(num(raw.get("blockNumber"), 0.0)),
                "block_ts": ts,
                "block_iso": _iso(ts),
                "block_ts_source": "blockscout_timestamp",
                "log_index": idx,
                "direction": direction,
                "from": from_addr,
                "to": to_addr,
                "counterparty": from_addr if direction == "IN" else to_addr,
                "amount_raw": str(raw.get("value") or "0"),
                "amount_usd": _round_usd(amount),
                "matched_topic_role": "blockscout_tokentx",
                "blockscout": {
                    "token_symbol": raw.get("tokenSymbol"),
                    "token_decimal": raw.get("tokenDecimal"),
                    "function_name": raw.get("functionName"),
                },
            }
        )
    return rows, {
        "status": "OK",
        "source": "blockscout_account_tokentx",
        "url": url,
        "raw_count": len(raw_rows),
        "rows_in_window": len(rows),
        "message": payload.get("message") if isinstance(payload, dict) else None,
        "from_block": start_block,
        "to_block": end_block,
        "block_mode": "blockscout_indexer_timestamp",
        "block_lookup_errors": block_lookup_errors,
    }


def _balance_of_at_block(rpc_url: str, *, wallet: str, block_number: int | str, timeout_s: float) -> float | None:
    topic_addr = _address_topic(wallet).removeprefix("0x")
    data = BALANCE_OF_SELECTOR + topic_addr
    block_ref = hex(block_number) if isinstance(block_number, int) else str(block_number)
    result = _rpc_post(
        rpc_url,
        "eth_call",
        [{"to": POLYMARKET_COLLATERAL_TOKEN_ADDRESS, "data": data}, block_ref],
        timeout_s=timeout_s,
    )
    if not isinstance(result, str):
        return None
    return _round_usd(_decode_uint256(result) / (10 ** POLYMARKET_COLLATERAL_TOKEN_DECIMALS))


def _safe_balance_sample(rpc_url: str, *, wallet: str, block_number: int | str, timeout_s: float) -> dict[str, Any]:
    try:
        return {
            "balance_usd": _balance_of_at_block(
                rpc_url,
                wallet=wallet,
                block_number=block_number,
                timeout_s=timeout_s,
            ),
            "status": "OK",
        }
    except Exception as exc:  # noqa: BLE001 - historical state is optional supporting evidence.
        return {
            "balance_usd": None,
            "status": "ERROR",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _fetch_data_api_activity(
    *,
    user: str,
    start_ts: float,
    end_ts: float,
    limit: int,
    max_pages: int,
    timeout_s: float,
    base_url: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    urls = []
    if not user:
        return rows, {"status": "UNAVAILABLE", "reason": "user_missing"}
    for page in range(max(1, int(max_pages))):
        query = urllib.parse.urlencode({"user": user, "limit": int(limit), "offset": page * int(limit)})
        url = f"{base_url.rstrip('/')}/activity?{query}"
        urls.append(url)
        request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=float(timeout_s)) as response:
            payload = json.loads(response.read().decode("utf-8"))
        page_rows = [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
        for row in page_rows:
            ts = _parse_ts(row.get("timestamp"))
            if start_ts <= ts <= end_ts:
                rows.append(row)
        if len(page_rows) < int(limit):
            return rows, {"status": "OK", "pages": page + 1, "truncated": False, "urls": urls}
        oldest_ts = min((_parse_ts(row.get("timestamp")) for row in page_rows), default=0.0)
        if oldest_ts and oldest_ts < start_ts:
            return rows, {"status": "OK", "pages": page + 1, "truncated": False, "urls": urls}
    return rows, {"status": "OK", "pages": int(max_pages), "truncated": True, "urls": urls}


def _activity_redeem_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if str(row.get("type") or "").upper() != "REDEEM":
            continue
        amount = num(row.get("usdcSize"), 0.0) or num(row.get("size"), 0.0)
        out.append(
            {
                "tx": str(row.get("transactionHash") or row.get("transaction_hash") or "").lower(),
                "condition_id": str(row.get("conditionId") or row.get("condition_id") or ""),
                "timestamp": row.get("timestamp"),
                "ts": _parse_ts(row.get("timestamp")),
                "amount_usd": _round_usd(amount),
                "raw_type": row.get("type"),
            }
        )
    return out


def _ledger_fill_txs(ledger: Any, *, start_ts: float, end_ts: float) -> dict[str, dict[str, Any]]:
    orders = ledger.get("orders") if isinstance(ledger, dict) else ledger
    rows = [row for row in orders or [] if isinstance(row, dict)]
    out: dict[str, dict[str, Any]] = {}
    for order in rows:
        if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
            continue
        trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        fill_backfill = trade_result.get("fill_backfill") if isinstance(trade_result.get("fill_backfill"), dict) else {}
        lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
        ts_candidates: list[Any] = [
            trade_result.get("filled_at"),
            trade_result.get("filled_at_iso"),
            fill_backfill.get("filled_at"),
            fill_backfill.get("filled_at_iso"),
            order.get("updated_at"),
            order.get("submitted_at"),
        ]
        for item in reversed(lifecycle):
            if not isinstance(item, dict):
                continue
            status = str(item.get("status") or item.get("event") or "").upper()
            if "FILL" not in status and status not in {"MATCHED", "FILLED"}:
                continue
            payload = item.get("payload") if isinstance(item.get("payload"), dict) else item
            ts_candidates.extend([payload.get("filled_at"), payload.get("filled_at_iso"), item.get("ts")])
        ts = next((_parse_ts(value) for value in ts_candidates if _parse_ts(value) > 0), 0.0)
        if not (start_ts <= ts <= end_ts):
            continue
        details = trade_result.get("details") if isinstance(trade_result.get("details"), dict) else {}
        candidates = [
            *(trade_result.get("tx_hashes") if isinstance(trade_result.get("tx_hashes"), list) else []),
            *(details.get("transactionsHashes") if isinstance(details.get("transactionsHashes"), list) else []),
            fill_backfill.get("tx_hash"),
            fill_backfill.get("transaction_hash"),
            trade_result.get("transaction_hash"),
            trade_result.get("tx_hash"),
            order.get("transaction_hash"),
        ]
        for item in lifecycle:
            if not isinstance(item, dict):
                continue
            payload = item.get("payload") if isinstance(item.get("payload"), dict) else item
            payload_details = payload.get("details") if isinstance(payload.get("details"), dict) else {}
            candidates.extend(payload.get("tx_hashes") if isinstance(payload.get("tx_hashes"), list) else [])
            candidates.extend(payload_details.get("transactionsHashes") if isinstance(payload_details.get("transactionsHashes"), list) else [])
            candidates.extend([payload.get("tx_hash"), payload.get("transaction_hash")])
        seen_order_txs: set[str] = set()
        for value in candidates:
            tx = str(value or "").lower()
            if not tx or tx in seen_order_txs:
                continue
            seen_order_txs.add(tx)
            item = out.setdefault(
                tx,
                {
                    "tx": tx,
                    "orders": 0,
                    "cost_usd": 0.0,
                    "source_wallets": set(),
                    "min_submitted_ts": ts,
                    "max_submitted_ts": ts,
                },
            )
            item["orders"] += 1
            item["cost_usd"] += num(
                trade_result.get("actual_trade_cost_usd"),
                num(
                    order.get("actual_trade_cost_usd"),
                    num(fill_backfill.get("cost_usd"), num(trade_result.get("response_filled_size_usd"), 0.0)),
                ),
            )
            source_wallet = _norm_addr(order.get("source_wallet"))
            if source_wallet:
                item["source_wallets"].add(source_wallet)
            item["min_submitted_ts"] = min(float(item["min_submitted_ts"]), ts)
            item["max_submitted_ts"] = max(float(item["max_submitted_ts"]), ts)
    for item in out.values():
        item["cost_usd"] = _round_usd(float(item["cost_usd"]))
        item["source_wallets"] = sorted(item["source_wallets"])
        item["min_submitted_iso"] = _iso(float(item["min_submitted_ts"]))
        item["max_submitted_iso"] = _iso(float(item["max_submitted_ts"]))
    return out


def _h2_redeem_txs(h2_external: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out = {}
    for row in h2_external.get("rows") or []:
        if not isinstance(row, dict):
            continue
        tx = str(row.get("transaction_hash") or "").lower()
        if not tx:
            continue
        out[tx] = {
            "tx": tx,
            "amount_usd": _round_usd(num(row.get("redeem_usdc"), 0.0)),
            "condition_id": row.get("condition_id"),
            "source": "h2_external_redemption_ingestion",
            "redeem_iso": row.get("redeem_iso"),
        }
    return out


def _near(value: float, target: float, tolerance: float = 0.01) -> bool:
    return abs(float(value) - float(target)) <= tolerance


def classify_transfer(
    row: dict[str, Any],
    *,
    wallet: str,
    start_ts: float,
    ledger_txs: dict[str, dict[str, Any]],
    redeem_txs: dict[str, dict[str, Any]],
    activity_redeem_txs: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    tx = str(row.get("tx") or "").lower()
    direction = str(row.get("direction") or "")
    counterparty = _norm_addr(row.get("counterparty"))
    amount = num(row.get("amount_usd"), 0.0)
    ts = num(row.get("block_ts"), 0.0)
    classification = "other_counterparty_in" if direction == "IN" else "other_counterparty_out"
    reason = "non-fill/non-redeem counterparty"
    evidence: dict[str, Any] = {}
    if tx in ledger_txs:
        classification = "fill_settlement"
        reason = "tx matched FILLED live execution ledger"
        evidence = ledger_txs[tx]
    elif tx in redeem_txs or tx in activity_redeem_txs:
        classification = "redemption_payout"
        reason = "tx matched redemption activity"
        evidence = redeem_txs.get(tx) or activity_redeem_txs.get(tx) or {}
    elif direction == "IN" and counterparty == ZERO_ADDRESS:
        classification = "redemption_payout_unmatched_zero_mint"
        reason = "inbound pUSD mint from zero address; not an external counterparty"
    elif direction == "IN" and counterparty == POLYMARKET_CTF.lower():
        classification = "redemption_payout"
        reason = "inbound from CTF collateral contract without live-fill tx match"
    elif (
        direction == "IN"
        and str((row.get("blockscout") or {}).get("function_name") or "").startswith("disperseToken")
    ):
        classification = "redemption_payout"
        reason = "inbound pUSD distributor transfer; treated as redemption/account-value payout"
    elif (
        direction == "IN"
        and amount >= 250.0
        and ts <= start_ts + 4 * 3600
        and counterparty not in KNOWN_SETTLEMENT_COUNTERPARTIES
    ):
        classification = "topup_deposit"
        reason = "large inbound transfer near top-up baseline from non-settlement counterparty"
    elif counterparty in KNOWN_SETTLEMENT_COUNTERPARTIES:
        classification = "fill_settlement"
        reason = "counterparty is known CTF/exchange settlement address"
    signed_amount = amount if direction == "IN" else -amount
    return {
        **row,
        "wallet": wallet,
        "classification": classification,
        "classification_reason": reason,
        "signed_amount_usd": _round_usd(signed_amount),
        "matched_evidence": evidence,
    }


def _class_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_class: dict[str, dict[str, Any]] = defaultdict(lambda: {"rows": 0, "in_usd": 0.0, "out_usd": 0.0, "net_usd": 0.0})
    for row in rows:
        key = str(row.get("classification") or "unknown")
        item = by_class[key]
        item["rows"] += 1
        amount = num(row.get("amount_usd"), 0.0)
        signed = num(row.get("signed_amount_usd"), 0.0)
        if signed >= 0:
            item["in_usd"] += amount
        else:
            item["out_usd"] += amount
        item["net_usd"] += signed
    return {
        key: {
            "rows": int(value["rows"]),
            "in_usd": _round_usd(value["in_usd"]),
            "out_usd": _round_usd(value["out_usd"]),
            "net_usd": _round_usd(value["net_usd"]),
        }
        for key, value in sorted(by_class.items())
    }


def _residual_candidates(rows: list[dict[str, Any]], targets: list[float], *, tolerance: float = 0.50) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        amount = abs(num(row.get("signed_amount_usd"), 0.0))
        for target in targets:
            if abs(amount - abs(float(target))) <= tolerance:
                out.append(
                    {
                        "target_usd": _round_usd(float(target)),
                        "tx": row.get("tx"),
                        "classification": row.get("classification"),
                        "direction": row.get("direction"),
                        "signed_amount_usd": row.get("signed_amount_usd"),
                        "amount_abs_delta_usd": _round_usd(amount - abs(float(target))),
                        "block_iso": row.get("block_iso"),
                        "block_ts": row.get("block_ts"),
                        "counterparty": row.get("counterparty"),
                    }
                )
    return out


def _scorecard_window(scorecard: dict[str, Any], *, fallback_start: str, fallback_end: str) -> tuple[float, float, str, str]:
    since = scorecard.get("since_topup_truth") if isinstance(scorecard.get("since_topup_truth"), dict) else {}
    chain = scorecard.get("chain_reconciliation") if isinstance(scorecard.get("chain_reconciliation"), dict) else {}
    start_iso = str(since.get("baseline_iso") or chain.get("reconciliation_start_iso") or fallback_start or DEFAULT_BASELINE_ISO)
    end_iso = fallback_end or str(scorecard.get("generated_at") or utc_now_iso())
    return _parse_ts(start_iso), _parse_ts(end_iso), start_iso, end_iso


def _current_balance_sample_ts(scorecard: dict[str, Any]) -> float:
    chain = scorecard.get("chain_reconciliation") if isinstance(scorecard.get("chain_reconciliation"), dict) else {}
    adjustments = chain.get("point_in_time_adjustments") if isinstance(chain.get("point_in_time_adjustments"), dict) else {}
    ts = num(adjustments.get("balance_sample_ts"), 0.0)
    if ts > 0:
        return ts
    sampling = chain.get("balance_sampling") if isinstance(chain.get("balance_sampling"), dict) else {}
    samples = sampling.get("samples") if isinstance(sampling.get("samples"), list) else []
    parsed = [_parse_ts(row.get("ts")) for row in samples if isinstance(row, dict)]
    return max(parsed, default=_parse_ts(scorecard.get("generated_at") or ""))


def _item3_chain_delta(item3: dict[str, Any]) -> float | None:
    for row in item3.get("brackets") or []:
        if isinstance(row, dict) and row.get("term") == "chain_cash_delta_vs_expected_identity":
            return num(row.get("amount_usd"), 0.0)
    return None


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    scorecard = load_json(args.scorecard, default={})
    h2_external = load_json(args.h2_external, default={})
    h2_residual = load_json(args.h2_residual, default={})
    item3 = load_json(args.item3, default={})
    ledger = load_json(args.ledger, default={})
    scorecard = scorecard if isinstance(scorecard, dict) else {}
    h2_external = h2_external if isinstance(h2_external, dict) else {}
    h2_residual = h2_residual if isinstance(h2_residual, dict) else {}
    item3 = item3 if isinstance(item3, dict) else {}

    start_ts, end_ts, start_iso, end_iso = _scorecard_window(
        scorecard,
        fallback_start=args.start_iso,
        fallback_end=args.end_iso,
    )
    if args.start_iso:
        start_ts = _parse_ts(args.start_iso)
        start_iso = args.start_iso
    if args.end_iso:
        end_ts = _parse_ts(args.end_iso)
        end_iso = args.end_iso
    wallet = _norm_addr(args.user) or _norm_addr(_load_dotenv_value("POLYMARKET_PROXY"))
    if not wallet:
        raise RuntimeError("POLYMARKET_PROXY/user wallet missing")

    ledger_txs = _ledger_fill_txs(ledger, start_ts=start_ts, end_ts=end_ts)
    h2_redeem_txs = _h2_redeem_txs(h2_external)
    try:
        activity_rows, activity_fetch = _fetch_data_api_activity(
            user=wallet,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=int(args.data_api_limit),
            max_pages=int(args.data_api_max_pages),
            timeout_s=float(args.timeout_s),
            base_url=str(args.data_api_base_url or DEFAULT_DATA_API_BASE),
        )
        activity_redeems = _activity_redeem_rows(activity_rows)
    except Exception as exc:  # noqa: BLE001 - keep transfer audit useful on Data API degradation.
        activity_fetch = {"status": "ERROR", "error_type": type(exc).__name__, "error": str(exc)}
        activity_redeems = []
    activity_redeem_txs = {
        str(row.get("tx") or "").lower(): row for row in activity_redeems if str(row.get("tx") or "")
    }
    combined_redeem_txs = {**h2_redeem_txs, **activity_redeem_txs}

    transfer_source = str(args.transfer_source or "auto")
    transfer_attempts: list[dict[str, Any]] = []
    if transfer_source == "blockscout":
        transfer_rows, transfer_fetch = _fetch_blockscout_transfer_rows(
            wallet=wallet,
            start_ts=start_ts,
            end_ts=end_ts,
            base_url=str(args.blockscout_base_url),
            timeout_s=float(args.timeout_s),
        )
    else:
        try:
            transfer_rows, transfer_fetch = _fetch_transfer_logs(
                wallet=wallet,
                rpc_url=str(args.polygon_rpc_url),
                start_ts=start_ts,
                end_ts=end_ts,
                chunk_blocks=int(args.chunk_blocks),
                timeout_s=float(args.timeout_s),
                block_mode=str(args.block_mode),
                seconds_per_block=float(args.seconds_per_block),
            )
            transfer_fetch["source"] = "polygon_rpc_eth_getLogs"
        except Exception as exc:
            transfer_attempts.append(
                {
                    "source": "polygon_rpc_eth_getLogs",
                    "rpc_url": str(args.polygon_rpc_url),
                    "status": "ERROR",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "next": "fallback_to_blockscout_indexer" if transfer_source == "auto" else "rerun_with_working_rpc_or_blockscout",
                }
            )
            if transfer_source != "auto":
                raise
            transfer_rows, transfer_fetch = _fetch_blockscout_transfer_rows(
                wallet=wallet,
                start_ts=start_ts,
                end_ts=end_ts,
                base_url=str(args.blockscout_base_url),
                timeout_s=float(args.timeout_s),
            )
            transfer_fetch["fallback_after_rpc_error"] = True
    classified = [
        classify_transfer(
            row,
            wallet=wallet,
            start_ts=start_ts,
            ledger_txs=ledger_txs,
            redeem_txs=h2_redeem_txs,
            activity_redeem_txs=activity_redeem_txs,
        )
        for row in transfer_rows
    ]

    current_balance_sample_ts = _current_balance_sample_ts(scorecard)
    start_block = int(transfer_fetch.get("from_block") or 0)
    blockscout_timestamp_mode = str(transfer_fetch.get("block_mode") or "") == "blockscout_indexer_timestamp"
    latest_block_payload = _rpc_post(str(args.polygon_rpc_url), "eth_getBlockByNumber", ["latest", False], timeout_s=float(args.timeout_s))
    if isinstance(latest_block_payload, dict):
        latest_block_for_estimate = int(str(latest_block_payload.get("number")), 16)
        latest_ts_for_estimate = float(int(str(latest_block_payload.get("timestamp")), 16))
    else:
        latest_block_for_estimate = int(transfer_fetch.get("latest_block") or transfer_fetch.get("to_block") or 0)
        latest_ts_for_estimate = time.time()
    current_block_error: dict[str, Any] | None = None
    current_block_ts_source = "rpc_block_timestamp" if str(args.block_mode) == "exact" else "estimated_from_latest_block"
    if blockscout_timestamp_mode:
        try:
            current_block = _fetch_blockscout_block_at_or_before_ts(
                base_url=str(args.blockscout_base_url),
                ts=current_balance_sample_ts or end_ts,
                timeout_s=float(args.timeout_s),
            )
            current_block_ts_source = "blockscout_getblocknobytime_no_exact_block_ts"
        except Exception as exc:  # noqa: BLE001 - balance sampling is supporting evidence only.
            current_block_error = {"error_type": type(exc).__name__, "error": str(exc)}
            current_block = _estimate_block_at_or_before_ts(
                current_balance_sample_ts or end_ts,
                latest_block=latest_block_for_estimate,
                latest_ts=latest_ts_for_estimate,
                seconds_per_block=float(args.seconds_per_block),
                safety_blocks=0,
            )
            current_block_ts_source = "estimated_from_latest_block"
    elif str(args.block_mode) == "exact":
        current_block = _find_block_at_or_before_ts(
            str(args.polygon_rpc_url),
            current_balance_sample_ts or end_ts,
            latest_block=latest_block_for_estimate,
            latest_ts=latest_ts_for_estimate,
            timeout_s=float(args.timeout_s),
        )
    else:
        current_block = _estimate_block_at_or_before_ts(
            current_balance_sample_ts or end_ts,
            latest_block=latest_block_for_estimate,
            latest_ts=latest_ts_for_estimate,
            seconds_per_block=float(args.seconds_per_block),
            safety_blocks=0,
        )
    latest_block = int(transfer_fetch.get("to_block") or current_block)
    def _sample_block_iso(block_number: int) -> str | None:
        if blockscout_timestamp_mode:
            return None
        if str(args.block_mode) == "exact":
            return _iso(_block_ts(str(args.polygon_rpc_url), block_number, {}, float(args.timeout_s)))
        return _iso(
            _estimated_block_ts(
                block_number,
                latest_block=latest_block_for_estimate,
                latest_ts=latest_ts_for_estimate,
                seconds_per_block=float(args.seconds_per_block),
            )
        )

    balance_samples = {
        "baseline_block": {
            "block_number": start_block,
            "target_iso": start_iso,
            "block_iso": _sample_block_iso(start_block),
            "block_ts_source": (
                "blockscout_getblocknobytime_no_exact_block_ts"
                if blockscout_timestamp_mode
                else ("rpc_block_timestamp" if str(args.block_mode) == "exact" else "estimated_from_latest_block")
            ),
            **_safe_balance_sample(
                str(args.polygon_rpc_url),
                wallet=wallet,
                block_number=start_block,
                timeout_s=float(args.timeout_s),
            ),
        },
        "scorecard_balance_sample_block": {
            "target_ts": current_balance_sample_ts,
            "target_iso": _iso(current_balance_sample_ts),
            "block_number": current_block,
            "block_iso": _sample_block_iso(current_block),
            "block_ts_source": current_block_ts_source,
            **({"block_lookup_error": current_block_error} if current_block_error else {}),
            **_safe_balance_sample(
                str(args.polygon_rpc_url),
                wallet=wallet,
                block_number=current_block,
                timeout_s=float(args.timeout_s),
            ),
        },
        "latest_transfer_window_block": {
            "block_number": latest_block,
            "target_iso": end_iso,
            "block_iso": _sample_block_iso(latest_block),
            "block_ts_source": (
                "blockscout_getblocknobytime_no_exact_block_ts"
                if blockscout_timestamp_mode
                else ("rpc_block_timestamp" if str(args.block_mode) == "exact" else "estimated_from_latest_block")
            ),
            **_safe_balance_sample(
                str(args.polygon_rpc_url),
                wallet=wallet,
                block_number=latest_block,
                timeout_s=float(args.timeout_s),
            ),
        },
    }

    summary = _class_summary(classified)
    since = scorecard.get("since_topup_truth") if isinstance(scorecard.get("since_topup_truth"), dict) else {}
    chain = scorecard.get("chain_reconciliation") if isinstance(scorecard.get("chain_reconciliation"), dict) else {}
    residual = scorecard.get("cash_diff_reconciliation_residual")
    residual = residual if isinstance(residual, dict) else {}
    h2_summary = h2_residual.get("summary") if isinstance(h2_residual.get("summary"), dict) else {}
    h2_external_acceptance = h2_external.get("acceptance") if isinstance(h2_external.get("acceptance"), dict) else {}
    current_residual = num(residual.get("residual_usd"), 0.0)
    h2_post_redeem_residual = num(h2_external_acceptance.get("residual_unexplained_after_external_redeems_usd"), 0.0)
    item3_delta = _item3_chain_delta(item3)
    current_delta = num(chain.get("delta_vs_expected_usd"), 0.0)
    residual_matches = _residual_candidates(
        classified,
        [value for value in (current_residual, h2_post_redeem_residual, item3_delta or 0.0, current_delta) if abs(value) > 0.000001],
    )
    current_residual_matches = [
        row
        for row in residual_matches
        if _near(num(row.get("target_usd"), 0.0), current_residual, 0.000001)
    ]
    scorecard_day = str(scorecard.get("day_utc") or scorecard.get("day") or "")
    scorecard_day_start_ts = _parse_ts(f"{scorecard_day}T00:00:00Z") if scorecard_day else 0.0
    scorecard_day_end_ts = scorecard_day_start_ts + 24 * 3600 if scorecard_day_start_ts else 0.0
    scorecard_day_residual_matches = [
        row
        for row in current_residual_matches
        if scorecard_day_start_ts <= num(row.get("block_ts"), 0.0) < scorecard_day_end_ts
    ]
    other_rows = [
        row
        for row in classified
        if str(row.get("classification") or "").startswith("other_counterparty")
    ]
    redeem_rows_after_item3_sample = [
        row
        for row in classified
        if str(row.get("classification") or "").startswith("redemption_payout")
        and num(row.get("block_ts"), 0.0) >= _parse_ts("2026-07-09T20:55:30Z")
    ]
    net_from_rows = _round_usd(sum(num(row.get("signed_amount_usd"), 0.0) for row in classified))
    start_balance_raw = (balance_samples.get("baseline_block") or {}).get("balance_usd")
    sample_balance_raw = (balance_samples.get("scorecard_balance_sample_block") or {}).get("balance_usd")
    start_balance = num(start_balance_raw, 0.0)
    sample_balance = num(sample_balance_raw, 0.0)
    row_implied_sample_balance = _round_usd(start_balance + net_from_rows) if start_balance_raw is not None else None
    sample_gap = (
        _round_usd(sample_balance - float(row_implied_sample_balance))
        if row_implied_sample_balance is not None and sample_balance
        else None
    )
    current_account_value = num(since.get("actual_value_usd"), 0.0)
    baseline_target = num(since.get("baseline_usd"), 335.0)
    forward_implied_baseline = (
        _round_usd(current_account_value - net_from_rows) if current_account_value > 0.0 else None
    )
    forward_baseline_gap = (
        _round_usd(float(forward_implied_baseline) - baseline_target)
        if forward_implied_baseline is not None
        else None
    )
    forward_anchor_pass = forward_baseline_gap is not None and abs(forward_baseline_gap) <= 1.0

    status = "PASS_TX_HASH_OR_TIMESTAMP_SKEW_NAMED"
    if other_rows:
        status = "REVIEW_OTHER_COUNTERPARTY_ROWS"
    if sample_gap is not None and abs(sample_gap) > 0.02:
        status = "REVIEW_TRANSFER_BALANCE_RECON_GAP"
    if not forward_anchor_pass:
        status = "REVIEW_FORWARD_IMPLIED_BASELINE_GAP"

    residual_source = residual.get("state_path") or "scorecard.cash_diff_reconciliation_residual"
    residual_class = str(residual.get("residual_classification") or "unknown")
    if len(scorecard_day_residual_matches) == 1:
        match_status = "UNIQUE_SCORECARD_DAY_MATCH_FOUND_WITHIN_TOLERANCE"
        unique_match = scorecard_day_residual_matches[0]
    elif len(current_residual_matches) == 1:
        match_status = "UNIQUE_BASELINE_WINDOW_MATCH_FOUND_WITHIN_TOLERANCE"
        unique_match = current_residual_matches[0]
    elif current_residual_matches:
        match_status = "MULTIPLE_MATCHES_FOUND_REQUIRES_DAY_FILTER_OR_SECONDARY_EVIDENCE"
        unique_match = {}
    else:
        match_status = "NO_SINGLE_MOVEMENT_MATCH_WITHIN_TOLERANCE"
        unique_match = {}
    direct_match_line = (
        f"{len(scorecard_day_residual_matches)} scorecard-day and {len(current_residual_matches)} baseline-window "
        "direct ±$0.50 matches for canonical residual"
        if current_residual_matches
        else "no direct ±$0.50 single-transfer match for canonical residual"
    )
    conclusion = (
        "A unique scorecard-day pUSD transfer-sized movement matches the canonical residual within the ORDER6 "
        "tolerance; cite its tx hash, amount, direction, and category, overlay only, no ledger rewrite."
        if match_status == "UNIQUE_SCORECARD_DAY_MATCH_FOUND_WITHIN_TOLERANCE"
        else (
            "A unique baseline-window pUSD transfer-sized movement matches the canonical residual, but it is outside "
            "the scorecard-day filter; cite it as supporting evidence only and retain honest residual classification."
            if match_status == "UNIQUE_BASELINE_WINDOW_MATCH_FOUND_WITHIN_TOLERANCE"
            else (
                "Multiple pUSD transfer-sized movements match the canonical residual within ±$0.50; no unique "
                "single-movement attribution can be claimed without the scorecard-day filter or secondary evidence."
                if current_residual_matches
                else (
                    "No single pUSD transfer row matches the canonical residual within ±$0.50; classified raw pUSD "
                    "transfers reconcile to the balance sample when available, so reclassify the residual as "
                    "multi-term/basis-timing residual in this overlay, no ledger rewrite."
                )
            )
        )
    )

    one_page_summary = {
        "verdict": status,
        "cash_audit_line": (
            f"pUSD transfer ledger from {start_iso} to {end_iso}: "
            f"net_rows={net_from_rows:.6f}, start_balance={_fmt_usd_or_unavailable(start_balance_raw)}, "
            f"sample_balance={_fmt_usd_or_unavailable(sample_balance_raw)}, row_vs_sample_gap={sample_gap}."
        ),
        "residual_line": (
            f"canonical residual {current_residual:.6f} ({residual_class}) from {residual_source}; "
            f"H2 post-redeem derivation is {h2_post_redeem_residual:.6f}; {direct_match_line}."
        ),
        "chain_delta_line": (
            f"item3 delta {item3_delta} moved to current chain delta {current_delta:.6f}; "
            f"post-20:55 redeem transfer net={_round_usd(sum(num(r.get('signed_amount_usd'), 0.0) for r in redeem_rows_after_item3_sample))}."
        ),
        "other_counterparty_line": (
            f"other_counterparty rows={len(other_rows)} net={_round_usd(sum(num(r.get('signed_amount_usd'), 0.0) for r in other_rows))}; "
            f"{'target class is empty' if not other_rows else 'target class requires review'}"
        ),
    }

    return {
        "kind": "wallet_copy_item4_nonfill_cash_audit",
        "flow_stage": "LEARN/LIVE/ROTATE/SELF-DEV",
        "generated_at": utc_now_iso(),
        "status": status,
        "inputs": {
            "scorecard": args.scorecard,
            "ledger": args.ledger,
            "h2_external": args.h2_external,
            "h2_residual": args.h2_residual,
            "item3": args.item3,
            "polygon_rpc_url": args.polygon_rpc_url,
            "data_api_base_url": args.data_api_base_url,
            "blockscout_base_url": args.blockscout_base_url,
            "transfer_source": args.transfer_source,
        },
        "window": {
            "wallet": wallet,
            "collateral_token": POLYMARKET_COLLATERAL_TOKEN_ADDRESS,
            "collateral_decimals": POLYMARKET_COLLATERAL_TOKEN_DECIMALS,
            "start_iso": start_iso,
            "end_iso": end_iso,
            "start_ts": start_ts,
            "end_ts": end_ts,
        },
        "fetch": {
            "transfers": transfer_fetch,
            "transfer_attempts": transfer_attempts,
            "data_api_activity": {key: value for key, value in activity_fetch.items() if key != "urls"},
            "ledger_fill_txs": len(ledger_txs),
            "h2_redeem_txs": len(h2_redeem_txs),
            "activity_redeem_txs": len(activity_redeem_txs),
        },
        "scorecard_cut": {
            "generated_at": scorecard.get("generated_at"),
            "canonical_pnl_usd": since.get("canonical_pnl_usd"),
            "actual_delta_vs_baseline_usd": since.get("actual_delta_vs_baseline_usd"),
            "actual_value_usd": since.get("actual_value_usd"),
            "account_value_usd": since.get("account_value_usd"),
            "live_cash_balance_usd": since.get("live_cash_balance_usd"),
            "primary_verdict": since.get("primary_verdict"),
            "chain_delta_vs_expected_usd": chain.get("delta_vs_expected_usd"),
            "chain_status": chain.get("status"),
            "balance_status": chain.get("balance_status"),
            "expected_cash_identity_usd": chain.get("expected_cash_identity_usd"),
        },
        "balance_samples": balance_samples,
        "classification_summary": summary,
        "transfer_balance_reconciliation": {
            "baseline_balance_usd": balance_samples["baseline_block"].get("balance_usd"),
            "scorecard_sample_balance_usd": balance_samples["scorecard_balance_sample_block"].get("balance_usd"),
            "net_transfer_rows_usd": net_from_rows,
            "row_implied_sample_balance_usd": row_implied_sample_balance,
            "row_vs_sample_gap_usd": sample_gap,
            "forward_anchor": {
                "current_account_value_usd": _round_usd(current_account_value),
                "implied_baseline_usd": forward_implied_baseline,
                "expected_baseline_usd": _round_usd(baseline_target),
                "gap_usd": forward_baseline_gap,
                "maximum_absolute_gap_usd": 1.0,
                "status": "PASS_WITHIN_1.00" if forward_anchor_pass else "FAIL_LOUD",
            },
        },
        "residual_reconciliation": {
            "canonical_residual_usd": _round_usd(current_residual),
            "canonical_residual_source": residual.get("state_path") or "scorecard.cash_diff_reconciliation_residual",
            "canonical_residual_generated_at": residual.get("generated_at"),
            "canonical_residual_classification": residual_class,
            "order6_single_movement_match_tolerance_usd": 0.50,
            "h2_post_external_redeem_residual_usd": _round_usd(h2_post_redeem_residual),
            "h2_latest_summary_residual_usd": h2_summary.get("latest_cash_diff_residual_usd"),
            "reconciled_canonical_residual": (
                f"canonical={current_residual:.6f} from {residual_source}; "
                f"h2_post_external_redeem={h2_post_redeem_residual:.6f}; "
                f"h2_latest={_fmt_usd_or_unavailable(h2_summary.get('latest_cash_diff_residual_usd'))}; "
                f"item3_chain_delta={_fmt_usd_or_unavailable(item3_delta)}; "
                f"current_chain_delta={current_delta:.6f}"
            ),
            "direct_tx_matches": residual_matches,
            "current_residual_direct_tx_matches": current_residual_matches,
            "scorecard_day_utc": scorecard_day,
            "scorecard_day_residual_direct_tx_matches": scorecard_day_residual_matches,
            "current_residual_unique_match": unique_match,
            "current_residual_single_movement_match_status": match_status,
            "matched_to_other_counterparty_tx": any(
                str(row.get("classification") or "").startswith("other_counterparty") for row in residual_matches
            ),
            "conclusion": conclusion,
        },
        "chain_delta_test": {
            "item3_chain_delta_vs_expected_usd": item3_delta,
            "current_chain_delta_vs_expected_usd": _round_usd(current_delta),
            "post_2055_redeem_rows": len(redeem_rows_after_item3_sample),
            "post_2055_redeem_net_usd": _round_usd(
                sum(num(row.get("signed_amount_usd"), 0.0) for row in redeem_rows_after_item3_sample)
            ),
            "shares_sampler_basis_with_residual": False,
            "reason": (
                "chain delta changes with later redemption transfer timing while the residual source remains the stale "
                "fill-cash-diff/H2 accounting term; they are not the same tx-hash term."
            ),
        },
        "rows": classified,
        "other_counterparty_rows": other_rows,
        "redemption_rows_after_2055": redeem_rows_after_item3_sample,
        "one_page_summary": one_page_summary,
    }


def build_current_residual_provenance(args: argparse.Namespace) -> dict[str, Any]:
    start = load_json(args.current_residual_start_scorecard, default={})
    end = load_json(args.current_residual_end_scorecard, default={})
    if not isinstance(start, dict) or not isinstance(end, dict):
        raise RuntimeError("both current residual scorecards must be JSON objects")
    start_endpoint = _scorecard_residual_endpoint(start, args.current_residual_start_scorecard)
    end_endpoint = _scorecard_residual_endpoint(end, args.current_residual_end_scorecard)
    start_ts = _parse_ts(start_endpoint["balance_sample_at"])
    end_ts = _parse_ts(end_endpoint["balance_sample_at"])
    if start_ts <= 0 or end_ts <= start_ts:
        raise RuntimeError("current residual scorecard sample timestamps are invalid")

    narrowed = argparse.Namespace(**vars(args))
    narrowed.scorecard = args.current_residual_end_scorecard
    narrowed.start_iso = _iso(start_ts) or ""
    narrowed.end_iso = _iso(end_ts) or ""
    narrowed.block_mode = "exact"
    base = build_report(narrowed)
    transfer_rows = [row for row in base.get("rows") or [] if isinstance(row, dict)]
    activity_rows: list[dict[str, Any]] = []
    wallet = str((base.get("window") or {}).get("wallet") or "")
    activity_fetch: dict[str, Any]
    try:
        raw_activity, activity_fetch = _fetch_data_api_activity(
            user=wallet,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=int(args.data_api_limit),
            max_pages=int(args.data_api_max_pages),
            timeout_s=float(args.timeout_s),
            base_url=str(args.data_api_base_url or DEFAULT_DATA_API_BASE),
        )
        for row in raw_activity:
            kind = str(row.get("type") or "").upper()
            amount = num(row.get("usdcSize"), 0.0) or num(row.get("cashAmount"), 0.0)
            sign = 1.0 if kind in {"REDEEM", "DEPOSIT"} else -1.0 if kind in {"WITHDRAWAL", "FEE"} else 0.0
            activity_rows.append(
                {
                    "source": "polymarket_data_api_activity",
                    "type": kind,
                    "timestamp": row.get("timestamp"),
                    "tx": str(row.get("transactionHash") or row.get("transaction_hash") or "").lower(),
                    "order_id": str(row.get("orderId") or row.get("order_id") or ""),
                    "signed_amount_usd": _round_usd(sign * amount),
                    "raw": row,
                }
            )
    except Exception as exc:  # noqa: BLE001
        activity_fetch = {"status": "ERROR", "error_type": type(exc).__name__, "error": str(exc)}

    transition = _round_usd(
        num(end_endpoint.get("residual_usd"), 0.0) - num(start_endpoint.get("residual_usd"), 0.0)
    )
    signed_rows = [
        {
            "source": "polygon_collateral_erc20",
            "type": row.get("classification"),
            "timestamp": row.get("block_iso"),
            "block_number": row.get("block_number"),
            "tx": row.get("tx"),
            "order_id": "",
            "signed_amount_usd": row.get("signed_amount_usd"),
        }
        for row in transfer_rows
    ] + activity_rows
    exact = _exact_attribution(signed_rows, transition)
    identity_basis = {
        "start_endpoint": start_endpoint,
        "end_endpoint": end_endpoint,
        "transition_usd": transition,
        "rows": signed_rows,
    }
    equation = {
        "start_residual_usd": start_endpoint.get("residual_usd"),
        "transition_usd": transition,
        "end_residual_usd": end_endpoint.get("residual_usd"),
        "identity": "start_residual + transition = end_residual",
        "within_one_cent": abs(
            num(start_endpoint.get("residual_usd"), 0.0)
            + transition
            - num(end_endpoint.get("residual_usd"), 0.0)
        ) <= 0.01,
    }
    return {
        "kind": "wallet_copy_cash_residual_provenance",
        "flow_stage": "LEARN/SELF-DEV",
        "mode": "CURRENT_RESIDUAL_TRANSITION_OVERLAY",
        "generated_at": utc_now_iso(),
        "status": "EXACT_ATTRIBUTION" if exact else "SMALLEST_PROVEN_BRACKET_EXHAUSTIVE_UNMATCHED",
        "ledger_rewrite": False,
        "start_endpoint": start_endpoint,
        "end_endpoint": end_endpoint,
        "smallest_proven_bracket": {
            "start_iso": _iso(start_ts),
            "end_iso": _iso(end_ts),
            "start_block": ((base.get("fetch") or {}).get("transfers") or {}).get("from_block"),
            "end_block": ((base.get("fetch") or {}).get("transfers") or {}).get("to_block"),
        },
        "sources": {
            "polygon_collateral_erc20": (base.get("fetch") or {}).get("transfers"),
            "polygon_collateral_rpc_attempts": (
                (base.get("fetch") or {}).get("transfer_attempts") or []
            ),
            "polygon_ctf_erc20_erc1155": {
                "status": "EXHAUSTED_BY_COLLATERAL_CASH_SCOPE",
                "contract": POLYMARKET_CTF,
                "reason": "CTF share transfers carry outcome inventory, not signed collateral cash; collateral legs and venue activity are enumerated separately",
            },
            "polymarket_activity": {key: value for key, value in activity_fetch.items() if key != "urls"},
            "ledger_fill_transactions": (base.get("fetch") or {}).get("ledger_fill_txs"),
        },
        "equation": equation,
        "equation_checksum": _canonical_checksum(equation),
        "identity_checksum": _canonical_checksum(identity_basis),
        "exact_attribution_rows": exact,
        "unmatched_rows": [] if exact else signed_rows,
    }


def main() -> int:
    args = parse_args()
    current_mode = bool(args.current_residual_start_scorecard or args.current_residual_end_scorecard)
    if current_mode and not (args.current_residual_start_scorecard and args.current_residual_end_scorecard):
        raise RuntimeError("current residual mode requires both scorecard endpoints")
    report = build_current_residual_provenance(args) if current_mode else build_report(args)
    if current_mode and args.output == DEFAULT_OUTPUT:
        args.output = DEFAULT_PROVENANCE_OUTPUT
    output = Path(args.output)
    if not output.is_absolute():
        output = ROOT / output
    atomic_write_json(output, report)
    if args.timestamped_output:
        ts_output = Path(args.timestamped_output)
        if not ts_output.is_absolute():
            ts_output = ROOT / ts_output
        atomic_write_json(ts_output, report)
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "rows": len(report.get("rows") or []),
                "classes": report.get("classification_summary"),
                "row_vs_sample_gap_usd": (report.get("transfer_balance_reconciliation") or {}).get("row_vs_sample_gap_usd"),
                "output": str(output),
                "timestamped_output": args.timestamped_output,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
