"""Read-only Weird-Peak wallet tracker.

This module intentionally has no execution dependency.  It only normalizes
Polymarket wallet API rows, verifies known transaction hashes on Polygon, and
summarizes BTC 5m window flow for downstream shadow/paper consumers.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import time
from collections import defaultdict
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from src.log_retention import maybe_truncate_log_tail
from src.wallet_copy.collateral import POLYMARKET_COLLATERAL_TOKEN_ADDRESS


DEFAULT_WEIRD_PEAK_WALLET = "0x9f5ffe76a818dce37c70f947998b52b70671a008"
DATA_API = "https://data-api.polymarket.com"
DEFAULT_POLYGON_RPC = "https://polygon-bor-rpc.publicnode.com"
BTC_5M_SLUG_RE = re.compile(r"^btc-updown-5m-(?P<window>\d+)$")
DEFAULT_ONCHAIN_LOG_ADDRESSES = (
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",  # Polygon USDC
    "0x4d97dcd97ec945f40cf65f87097ace5ea0476045",  # Conditional token transfers seen in PM receipts
    POLYMARKET_COLLATERAL_TOKEN_ADDRESS,  # Polymarket USD (pUSD) collateral token
    "0xe111180000d2663c0091e4f400237545b87b996b",  # Polymarket settlement helper seen in receipts
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_unix_ts(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        if not value.isdigit():
            return 0
        value = int(value)
    try:
        ts = int(value)
    except (TypeError, ValueError):
        return 0
    if ts > 1_000_000_000_000_000:
        return ts // 1_000_000_000
    if ts > 1_000_000_000_000:
        return ts // 1000
    return ts


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def append_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        for record in records:
            handle.write(json.dumps(record, separators=(",", ":"), sort_keys=True))
            handle.write("\n")
    maybe_truncate_log_tail(path)


@dataclass(frozen=True)
class WalletTrackConfig:
    target_wallet: str = DEFAULT_WEIRD_PEAK_WALLET
    data_api: str = DATA_API
    poll_limit: int = 100
    timeout_s: float = 6.0
    btc_5m_only: bool = True
    min_usdc_size: float = 0.0
    include_activity_types: tuple[str, ...] = ("TRADE", "MERGE", "REDEEM")
    enable_onchain: bool = True
    polygon_rpc_url: str = field(
        default_factory=lambda: os.getenv("POLYGON_RPC_URL") or DEFAULT_POLYGON_RPC
    )
    max_onchain_hashes_per_poll: int = 3
    onchain_rpc_timeout_s: float = 1.5
    onchain_pending_hash_limit: int = 5_000
    recent_block_scan_depth: int = 0
    max_onchain_logs_per_poll: int = 200
    onchain_scan_interval_s: float = 2.0
    onchain_hash_check_interval_s: float = 5.0
    onchain_log_addresses: tuple[str, ...] = DEFAULT_ONCHAIN_LOG_ADDRESSES
    enable_market_ws_corroboration: bool = True
    market_ws_event_log_path: str = "data/lead_lag_raw_pm_events.jsonl"
    market_ws_tail_lines: int = 150_000
    market_ws_tail_max_bytes: int = 128 * 1024 * 1024
    market_ws_match_window_s: float = 180.0
    latency_pass_s: float = 8.0
    latency_watch_s: float = 30.0
    event_log_path: str = "data/research/weird_peak_wallet_tracker_events.jsonl"
    state_path: str = "data/research/weird_peak_wallet_tracker_state.json"
    recent_state_event_limit: int = 250
    user_agent: str = "PolymarketWeirdPeakTracker/1.0"


@dataclass
class WalletEvent:
    source: str
    row_type: str
    target_wallet: str
    condition_id: str
    market_slug: str
    event_slug: str
    title: str
    asset: str
    window_start_s: int | None
    side: str
    outcome: str
    outcome_index: int | None
    token_id: str
    price: float
    size: float
    usdc_size: float
    event_ts: int
    observed_ts: float
    api_latency_s: float | None
    transaction_hash: str
    dedupe_key: str
    raw: dict[str, Any] = field(repr=False)
    onchain: dict[str, Any] | None = None
    market_ws_corroboration: dict[str, Any] | None = None

    def as_record(self) -> dict[str, Any]:
        data = asdict(self)
        data["raw"] = self.raw
        return data


@dataclass(frozen=True)
class SourceFetchSnapshot:
    source: str
    request_start_ts: float
    request_end_ts: float
    duration_s: float
    row_count: int
    ok: bool
    error: str = ""

    def as_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WindowFlowSummary:
    condition_id: str
    market_slug: str
    title: str
    window_start_s: int | None
    first_event_ts: int
    last_event_ts: int
    latest_observed_ts: float
    trade_count: int
    transaction_count: int
    buy_up_count: int
    buy_down_count: int
    buy_up_usdc: float
    buy_down_usdc: float
    sell_up_count: int
    sell_down_count: int
    latest_side: str
    latest_outcome: str
    latest_price: float
    latest_usdc_size: float
    dominant_buy_outcome: str
    dominant_buy_usdc_delta: float
    dominant_buy_ratio: float
    both_buy_outcomes: bool
    only_buy_side: bool
    event_age_s: float | None

    def as_record(self) -> dict[str, Any]:
        return asdict(self)


class PolymarketWalletApiClient:
    def __init__(self, config: WalletTrackConfig):
        self.config = config
        self.sessions = {
            "trades": self._new_session(),
            "activity": self._new_session(),
        }

    def _new_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "User-Agent": self.config.user_agent,
            "Accept": "application/json",
        })
        return session

    def fetch_trades(self) -> list[dict[str, Any]]:
        params = {
            "user": self.config.target_wallet,
            "takerOnly": "false",
            "limit": max(1, min(1000, int(self.config.poll_limit))),
            "offset": 0,
        }
        return self._get_list("/trades", params=params, source="trades")

    def fetch_activity(self) -> list[dict[str, Any]]:
        params = {
            "user": self.config.target_wallet,
            "sortDirection": "DESC",
            "limit": max(1, min(500, int(self.config.poll_limit))),
        }
        return self._get_list("/activity", params=params, source="activity")

    def fetch_all_sources(self) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str], dict[str, dict[str, Any]]]:
        raw_by_source: dict[str, list[dict[str, Any]]] = {}
        api_errors: dict[str, str] = {}
        snapshots: dict[str, dict[str, Any]] = {}
        fetchers = {
            "trades": self.fetch_trades,
            "activity": self.fetch_activity,
        }

        def _fetch(source: str) -> tuple[str, list[dict[str, Any]], SourceFetchSnapshot]:
            start = time.time()
            try:
                rows = fetchers[source]()
                end = time.time()
                return source, rows, SourceFetchSnapshot(
                    source=source,
                    request_start_ts=start,
                    request_end_ts=end,
                    duration_s=round(end - start, 6),
                    row_count=len(rows),
                    ok=True,
                )
            except Exception as exc:
                end = time.time()
                return source, [], SourceFetchSnapshot(
                    source=source,
                    request_start_ts=start,
                    request_end_ts=end,
                    duration_s=round(end - start, 6),
                    row_count=0,
                    ok=False,
                    error=str(exc),
                )

        with ThreadPoolExecutor(max_workers=len(fetchers)) as pool:
            futures = [pool.submit(_fetch, source) for source in fetchers]
            for future in as_completed(futures):
                source, rows, snapshot = future.result()
                raw_by_source[source] = rows
                snapshots[source] = snapshot.as_record()
                if not snapshot.ok:
                    api_errors[source] = snapshot.error
        for source in fetchers:
            raw_by_source.setdefault(source, [])
            snapshots.setdefault(source, SourceFetchSnapshot(
                source=source,
                request_start_ts=0.0,
                request_end_ts=0.0,
                duration_s=0.0,
                row_count=0,
                ok=False,
                error="source_not_fetched",
            ).as_record())
        return raw_by_source, api_errors, snapshots

    def _get_list(self, path: str, *, params: dict[str, Any], source: str) -> list[dict[str, Any]]:
        url = f"{self.config.data_api.rstrip('/')}{path}"
        response = self.sessions[source].get(url, params=params, timeout=self.config.timeout_s)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list):
            return []
        return [row for row in payload if isinstance(row, dict)]


class OnchainReceiptVerifier:
    def __init__(self, rpc_url: str, *, timeout_s: float = 8.0):
        self.rpc_url = rpc_url
        self.timeout_s = timeout_s
        self._w3: Any | None = None

    def verify_hashes(self, hashes: list[str], *, wallet: str) -> dict[str, dict[str, Any]]:
        if not hashes:
            return {}
        from web3.exceptions import TransactionNotFound

        self._ensure_w3()

        wallet_l = wallet.lower()
        wallet_topic = "0x" + ("0" * 24) + wallet_l.removeprefix("0x")
        out: dict[str, dict[str, Any]] = {}
        for tx_hash in hashes:
            try:
                receipt = self._w3.eth.get_transaction_receipt(tx_hash)
                try:
                    tx = self._w3.eth.get_transaction(tx_hash)
                except Exception:
                    tx = {}
                logs = list(receipt.get("logs", []) or [])
                touched_contracts = sorted({
                    str(log.get("address", "")).lower()
                    for log in logs
                    if log.get("address")
                })
                mentioned = self._mentions_wallet(tx, logs, wallet_l=wallet_l, wallet_topic=wallet_topic)
                status = int(receipt.get("status", 0))
                out[tx_hash] = {
                    "checked": True,
                    "found": True,
                    "success": status == 1,
                    "status": status,
                    "block_number": int(receipt.get("blockNumber")) if receipt.get("blockNumber") is not None else None,
                    "gas_used": int(receipt.get("gasUsed")) if receipt.get("gasUsed") is not None else None,
                    "logs_count": len(logs),
                    "wallet_mentioned_in_tx_or_logs": mentioned,
                    "touched_contracts": touched_contracts[:50],
                    "explorer_url": f"https://polygonscan.com/tx/{tx_hash}",
                    "rpc_source": "configured" if self.rpc_url != DEFAULT_POLYGON_RPC else "public_default",
                    "verified_at": utc_now_iso(),
                }
            except TransactionNotFound:
                out[tx_hash] = {
                    "checked": True,
                    "found": False,
                    "success": False,
                    "error": "transaction_not_found",
                    "explorer_url": f"https://polygonscan.com/tx/{tx_hash}",
                    "rpc_source": "configured" if self.rpc_url != DEFAULT_POLYGON_RPC else "public_default",
                    "verified_at": utc_now_iso(),
                }
            except Exception as exc:
                out[tx_hash] = {
                    "checked": True,
                    "found": False,
                    "success": False,
                    "error": str(exc),
                    "explorer_url": f"https://polygonscan.com/tx/{tx_hash}",
                    "rpc_source": "configured" if self.rpc_url != DEFAULT_POLYGON_RPC else "public_default",
                    "verified_at": utc_now_iso(),
                }
        return out

    def scan_recent_wallet_logs(
        self,
        *,
        wallet: str,
        block_depth: int,
        max_logs: int = 200,
        addresses: tuple[str, ...] = DEFAULT_ONCHAIN_LOG_ADDRESSES,
    ) -> dict[str, Any]:
        self._ensure_w3()
        wallet_l = wallet.lower()
        wallet_topic = "0x" + ("0" * 24) + wallet_l.removeprefix("0x")
        try:
            latest_block = int(self._w3.eth.block_number)
        except Exception as exc:
            return {
                "enabled": True,
                "error": str(exc),
                "latest_block": None,
                "from_block": None,
                "to_block": None,
                "logs": [],
            }

        depth = max(1, int(block_depth))
        from_block = max(0, latest_block - depth + 1)
        checksum_addresses = []
        for address in addresses:
            try:
                checksum_addresses.append(self._w3.to_checksum_address(address))
            except Exception:
                continue
        logs_out: list[dict[str, Any]] = []
        errors: list[str] = []
        seen: set[tuple[int, str, int]] = set()

        # Common ERC20/ERC1155 indexed address positions. This is intentionally
        # broad and read-only; downstream code treats it as onchain corroboration,
        # not as a decoded trade.
        for topic_index in (1, 2, 3):
            topics: list[Any] = [None, None, None, None]
            topics[topic_index] = wallet_topic
            try:
                logs = self._w3.eth.get_logs({
                    "address": checksum_addresses,
                    "fromBlock": from_block,
                    "toBlock": latest_block,
                    "topics": topics,
                })
            except Exception as exc:
                errors.append(f"topic{topic_index}:{exc}")
                continue
            for log in logs:
                block_number = int(log.get("blockNumber") or 0)
                tx_hash = _to_hex(log.get("transactionHash"))
                log_index = int(log.get("logIndex") or 0)
                key = (block_number, tx_hash, log_index)
                if key in seen:
                    continue
                seen.add(key)
                logs_out.append({
                    "block_number": block_number,
                    "transaction_hash": tx_hash,
                    "log_index": log_index,
                    "address": str(log.get("address") or "").lower(),
                    "matched_topic_index": topic_index,
                    "topics": [_to_hex(topic) for topic in list(log.get("topics", []) or [])],
                    "data": _to_hex(log.get("data")),
                })
                if len(logs_out) >= max_logs:
                    break
            if len(logs_out) >= max_logs:
                break

        logs_out.sort(key=lambda row: (row["block_number"], row["log_index"]), reverse=True)
        return {
            "enabled": True,
            "latest_block": latest_block,
            "from_block": from_block,
            "to_block": latest_block,
            "block_depth": depth,
            "addresses": [str(address).lower() for address in addresses],
            "wallet_topic": wallet_topic,
            "log_count": len(logs_out),
            "truncated": len(logs_out) >= max_logs,
            "errors": errors,
            "logs": logs_out[:max_logs],
            "rpc_source": "configured" if self.rpc_url != DEFAULT_POLYGON_RPC else "public_default",
            "scanned_at": utc_now_iso(),
        }

    def _ensure_w3(self) -> None:
        if self._w3 is None:
            from web3 import Web3

            self._w3 = Web3(Web3.HTTPProvider(self.rpc_url, request_kwargs={"timeout": self.timeout_s}))

    @staticmethod
    def _mentions_wallet(tx: Any, logs: list[Any], *, wallet_l: str, wallet_topic: str) -> bool:
        tx_from = str(tx.get("from", "") if isinstance(tx, dict) else "").lower()
        tx_to = str(tx.get("to", "") if isinstance(tx, dict) else "").lower()
        if wallet_l and wallet_l in {tx_from, tx_to}:
            return True
        wallet_plain = wallet_l.removeprefix("0x")
        for log in logs:
            address = str(log.get("address", "")).lower()
            if address == wallet_l:
                return True
            for topic in list(log.get("topics", []) or []):
                if str(topic).lower() == wallet_topic:
                    return True
            data = str(log.get("data", "")).lower()
            if wallet_plain and wallet_plain in data:
                return True
        return False


def normalize_wallet_row(
    row: dict[str, Any],
    *,
    source: str,
    target_wallet: str,
    observed_ts: float | None = None,
    btc_5m_only: bool = True,
    min_usdc_size: float = 0.0,
    include_activity_types: tuple[str, ...] = ("TRADE", "MERGE", "REDEEM"),
) -> WalletEvent | None:
    wallet = str(row.get("proxyWallet") or row.get("user") or target_wallet).lower()
    if wallet != target_wallet.lower():
        return None

    row_type = str(row.get("type") or "TRADE").upper()
    if source == "activity" and row_type not in {x.upper() for x in include_activity_types}:
        return None

    slug = str(row.get("slug") or row.get("eventSlug") or "")
    event_slug = str(row.get("eventSlug") or slug)
    match = BTC_5M_SLUG_RE.match(slug) or BTC_5M_SLUG_RE.match(event_slug)
    asset = "BTC" if match else _asset_from_slug_or_title(slug, str(row.get("title") or ""))
    window_start_s = int(match.group("window")) if match else None
    if btc_5m_only and (asset != "BTC" or window_start_s is None):
        return None

    price = safe_float(row.get("price"))
    size = safe_float(row.get("size") or row.get("shares"))
    usdc_size = safe_float(row.get("usdcSize"))
    if usdc_size <= 0 and price > 0 and size > 0:
        usdc_size = price * size
    if row_type == "TRADE" and usdc_size < min_usdc_size:
        return None

    event_ts = parse_unix_ts(row.get("timestamp") or row.get("_unix_ts"))
    observed = float(observed_ts if observed_ts is not None else time.time())
    tx_hash = str(row.get("transactionHash") or row.get("transaction_hash") or row.get("txHash") or "")
    condition_id = str(row.get("conditionId") or row.get("condition_id") or "")
    token_id = str(row.get("asset") or row.get("assetId") or row.get("token_id") or "")
    side = str(row.get("side") or "").upper()
    outcome = str(row.get("outcome") or "")
    outcome_index = _optional_int(row.get("outcomeIndex"))
    dedupe_key = "|".join([
        source,
        row_type,
        tx_hash,
        condition_id,
        token_id,
        side,
        outcome,
        str(event_ts),
        f"{price:.10f}",
        f"{size:.10f}",
    ])
    latency = (observed - event_ts) if event_ts > 0 else None
    return WalletEvent(
        source=source,
        row_type=row_type,
        target_wallet=target_wallet.lower(),
        condition_id=condition_id,
        market_slug=slug,
        event_slug=event_slug,
        title=str(row.get("title") or ""),
        asset=asset,
        window_start_s=window_start_s,
        side=side,
        outcome=outcome,
        outcome_index=outcome_index,
        token_id=token_id,
        price=price,
        size=size,
        usdc_size=usdc_size,
        event_ts=event_ts,
        observed_ts=observed,
        api_latency_s=latency,
        transaction_hash=tx_hash,
        dedupe_key=dedupe_key,
        raw=dict(row),
    )


def _asset_from_slug_or_title(slug: str, title: str) -> str:
    text = f"{slug} {title}".lower()
    if "btc" in text or "bitcoin" in text:
        return "BTC"
    return ""


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_hex(value: Any) -> str:
    if value is None:
        return ""
    try:
        return value.hex()
    except AttributeError:
        return str(value)


def _percentile(values: list[float], pct: float) -> float | None:
    finite = sorted(value for value in values if value is not None)
    if not finite:
        return None
    if len(finite) == 1:
        return finite[0]
    rank = (len(finite) - 1) * max(0.0, min(100.0, pct)) / 100.0
    lower = int(rank)
    upper = min(lower + 1, len(finite) - 1)
    weight = rank - lower
    return finite[lower] * (1.0 - weight) + finite[upper] * weight


def _tail_text_lines(path: Path, max_lines: int, *, max_bytes: int = 64 * 1024 * 1024) -> list[str]:
    if max_lines <= 0 or not path.exists():
        return []
    block_size = 64 * 1024
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            position = handle.tell()
            chunks: list[bytes] = []
            newline_count = 0
            bytes_read = 0
            while position > 0 and newline_count <= max_lines and bytes_read < max(1, int(max_bytes)):
                read_size = min(block_size, position, max(1, int(max_bytes)) - bytes_read)
                position -= read_size
                handle.seek(position)
                chunk = handle.read(read_size)
                chunks.append(chunk)
                bytes_read += len(chunk)
                newline_count += chunk.count(b"\n")
    except OSError:
        return []
    if not chunks:
        return []
    data = b"".join(reversed(chunks)).decode("utf-8", errors="replace")
    return [line for line in data.splitlines() if line.strip()][-max_lines:]


def _tail_jsonl(path: Path, max_lines: int, *, max_bytes: int = 64 * 1024 * 1024) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in _tail_text_lines(path, max_lines, max_bytes=max_bytes):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _rows_time_span(rows: list[dict[str, Any]]) -> dict[str, Any]:
    timestamps = [ts for row in rows if (ts := _row_ts_s(row)) is not None]
    if not timestamps:
        return {
            "first_ts": None,
            "last_ts": None,
            "coverage_s": None,
        }
    first_ts = min(timestamps)
    last_ts = max(timestamps)
    return {
        "first_ts": round(first_ts, 6),
        "last_ts": round(last_ts, 6),
        "coverage_s": round(max(0.0, last_ts - first_ts), 6),
    }


class MarketWsEventLogBuffer:
    """Incrementally tail the raw market WS log for near-live corroboration."""

    def __init__(self, path: Path, *, max_lines: int, max_bytes: int):
        self.path = Path(path)
        self.max_lines = max(0, int(max_lines))
        self.max_bytes = max(1, int(max_bytes))
        self.rows: deque[dict[str, Any]] = deque(maxlen=max(1, self.max_lines))
        self.offset = 0
        self.file_id: tuple[int, int] | None = None
        self.stats: dict[str, Any] = {
            "mode": "not_loaded",
            "path": str(self.path),
            "row_count": 0,
            "new_rows": 0,
        }

    def refresh(self) -> list[dict[str, Any]]:
        if self.max_lines <= 0 or not self.path.exists():
            self.rows.clear()
            self.offset = 0
            self.file_id = None
            self.stats = {
                "mode": "missing_or_disabled",
                "path": str(self.path),
                "row_count": 0,
                "new_rows": 0,
            }
            return []
        try:
            stat = self.path.stat()
        except OSError as exc:
            self.stats = {
                "mode": "stat_error",
                "path": str(self.path),
                "row_count": len(self.rows),
                "new_rows": 0,
                "error": str(exc),
            }
            return list(self.rows)

        current_id = (int(stat.st_dev), int(stat.st_ino))
        should_reload = (
            self.offset <= 0
            or self.file_id != current_id
            or int(stat.st_size) < self.offset
            or int(stat.st_size) - self.offset > self.max_bytes
        )
        if should_reload:
            rows = _tail_jsonl(self.path, self.max_lines, max_bytes=self.max_bytes)
            self.rows = deque(rows, maxlen=max(1, self.max_lines))
            self.offset = int(stat.st_size)
            self.file_id = current_id
            span = _rows_time_span(list(self.rows))
            self.stats = {
                "mode": "tail_reload",
                "path": str(self.path),
                "row_count": len(self.rows),
                "new_rows": len(rows),
                "offset": self.offset,
                "max_lines": self.max_lines,
                "max_bytes": self.max_bytes,
                **span,
            }
            return list(self.rows)

        new_rows: list[dict[str, Any]] = []
        try:
            with self.path.open("rb") as handle:
                handle.seek(self.offset)
                chunk = handle.read()
                self.offset = handle.tell()
        except OSError as exc:
            self.stats = {
                "mode": "read_error",
                "path": str(self.path),
                "row_count": len(self.rows),
                "new_rows": 0,
                "error": str(exc),
            }
            return list(self.rows)

        if chunk:
            text = chunk.decode("utf-8", errors="replace")
            lines = [line for line in text.splitlines() if line.strip()]
            for line in lines:
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict):
                    new_rows.append(value)
                    self.rows.append(value)
        span = _rows_time_span(list(self.rows))
        self.stats = {
            "mode": "incremental",
            "path": str(self.path),
            "row_count": len(self.rows),
            "new_rows": len(new_rows),
            "offset": self.offset,
            "max_lines": self.max_lines,
            "max_bytes": self.max_bytes,
            **span,
        }
        return list(self.rows)


def _raw_ws_event(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw")
    return raw if isinstance(raw, dict) else row


def _ws_items(row: dict[str, Any]) -> list[dict[str, Any]]:
    raw = _raw_ws_event(row)
    event_type = str(raw.get("event_type") or raw.get("type") or "").lower()
    market = str(raw.get("market") or "")
    timestamp = raw.get("timestamp")
    items: list[dict[str, Any]] = []
    if event_type == "price_change":
        for change in raw.get("price_changes") or []:
            if not isinstance(change, dict):
                continue
            items.append({
                "row": row,
                "raw": raw,
                "change": change,
                "event_type": event_type,
                "market": market,
                "timestamp": timestamp,
                "token_id": str(change.get("asset_id") or change.get("token_id") or ""),
                "price": safe_float(change.get("price")),
                "size": safe_float(change.get("size")),
                "side": change.get("side"),
                "book_hash": change.get("hash"),
                "transaction_hash": "",
            })
        return items
    items.append({
        "row": row,
        "raw": raw,
        "change": None,
        "event_type": event_type,
        "market": market,
        "timestamp": timestamp,
        "token_id": str(raw.get("asset_id") or raw.get("token_id") or ""),
        "price": safe_float(raw.get("price")),
        "size": safe_float(raw.get("size")),
        "side": raw.get("side"),
        "book_hash": raw.get("hash"),
        "transaction_hash": str(raw.get("transaction_hash") or raw.get("transactionHash") or ""),
    })
    return items


def _row_ts_s(row: dict[str, Any]) -> float | None:
    for key in ("ts_recv_ms", "receive_ms", "ts_ms"):
        value = row.get(key)
        try:
            if value is not None:
                return float(value) / 1000.0
        except (TypeError, ValueError):
            pass
    ts_iso = row.get("ts_iso") or row.get("receive_iso")
    if ts_iso:
        try:
            return datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def _market_ws_corroborate(
    events: list[WalletEvent],
    *,
    path: Path,
    max_lines: int,
    max_bytes: int,
    match_window_s: float,
    rows: list[dict[str, Any]] | None = None,
    buffer_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not events:
        return {
            "enabled": True,
            "path": str(path),
            "loaded_rows": 0,
            "matched_events": 0,
            "status": "IDLE_NO_EVENTS",
            "matches": {},
        }
    rows = rows if rows is not None else _tail_jsonl(path, max_lines, max_bytes=max_bytes)
    if not rows:
        return {
            "enabled": True,
            "path": str(path),
            "loaded_rows": 0,
            "matched_events": 0,
            "status": "NO_MARKET_WS_ROWS",
            "buffer": buffer_stats or {},
            "matches": {},
        }

    target_tokens = {str(event.token_id) for event in events if event.token_id}
    target_markets = {str(event.condition_id).lower() for event in events if event.condition_id}
    target_tx_hashes = {str(event.transaction_hash).lower() for event in events if event.transaction_hash}
    by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_token: dict[str, list[dict[str, Any]]] = defaultdict(list)
    indexed_rows = 0
    indexed_items = 0
    for row in rows:
        raw = _raw_ws_event(row)
        raw_market = str(raw.get("market") or "").lower()
        if raw_market and target_markets and raw_market not in target_markets:
            continue
        indexed_rows += 1
        for item in _ws_items(row):
            token = str(item.get("token_id") or "")
            tx_hash = str(item.get("transaction_hash") or "").lower()
            if (
                target_tokens
                and target_tx_hashes
                and token not in target_tokens
                and tx_hash not in target_tx_hashes
            ):
                continue
            if target_tokens and not target_tx_hashes and token not in target_tokens:
                continue
            if target_tx_hashes and not target_tokens and tx_hash not in target_tx_hashes:
                continue
            indexed_items += 1
            if token:
                by_token[token].append(item)
            if tx_hash:
                by_tx[tx_hash].append(item)

    matches: dict[str, dict[str, Any]] = {}
    latency_samples: list[float] = []
    for event in events:
        tx_hash = event.transaction_hash.lower()
        candidates = by_tx.get(tx_hash) or []
        fallback_candidates = by_token.get(event.token_id) or []
        best: dict[str, Any] | None = None
        best_delta: float | None = None
        best_method = ""

        def _candidate_ok(item: dict[str, Any], *, require_trade_fingerprint: bool) -> tuple[bool, float | None]:
            row = item.get("row") if isinstance(item.get("row"), dict) else {}
            token = str(item.get("token_id") or "")
            if event.token_id and token and token != event.token_id:
                return False, None
            market = str(item.get("market") or "")
            if event.condition_id and market and market.lower() != event.condition_id.lower():
                return False, None
            ws_ts = _row_ts_s(row)
            if ws_ts is None:
                return False, None
            delta = abs(float(event.event_ts or 0) - ws_ts) if event.event_ts else 0.0
            if match_window_s > 0 and delta > match_window_s:
                return False, None
            if require_trade_fingerprint:
                price_tol = max(1e-6, min(0.005, max(event.price, 0.0) * 0.0025))
                size_tol = max(0.01, max(event.size, 0.0) * 0.01)
                if event.price > 0 and abs(safe_float(item.get("price")) - event.price) > price_tol:
                    return False, None
                if event.size > 0 and abs(safe_float(item.get("size")) - event.size) > size_tol:
                    return False, None
            return True, delta

        for item in candidates:
            ok, delta = _candidate_ok(item, require_trade_fingerprint=False)
            if ok and (best is None or best_delta is None or (delta is not None and delta < best_delta)):
                best = item
                best_delta = delta
                best_method = "transaction_hash"
        if best is None:
            for item in fallback_candidates:
                ok, delta = _candidate_ok(item, require_trade_fingerprint=True)
                if ok and (best is None or best_delta is None or (delta is not None and delta < best_delta)):
                    best = item
                    best_delta = delta
                    best_method = "token_price_size_market_time"
        if best is None:
            continue
        row = best.get("row") if isinstance(best.get("row"), dict) else {}
        ws_recv_ts = _row_ts_s(row)
        ws_to_api_s = (event.observed_ts - ws_recv_ts) if ws_recv_ts is not None else None
        if ws_to_api_s is not None:
            latency_samples.append(ws_to_api_s)
        record = {
            "matched": True,
            "source": "lead_lag_raw_pm_events",
            "match_method": best_method,
            "transaction_hash": event.transaction_hash,
            "token_id": event.token_id,
            "ws_event_type": best.get("event_type"),
            "ws_market": best.get("market"),
            "ws_book_hash": best.get("book_hash"),
            "ws_price": safe_float(best.get("price"), default=0.0),
            "ws_size": safe_float(best.get("size"), default=0.0),
            "ws_side": best.get("side"),
            "ws_recv_ts": ws_recv_ts,
            "ws_pm_ts_ms": row.get("ts_pm_ms"),
            "ws_network_lag_ms": row.get("network_lag_ms"),
            "ws_to_api_observed_latency_s": round(ws_to_api_s, 6) if ws_to_api_s is not None else None,
            "match_delta_s": round(best_delta, 6) if best_delta is not None else None,
        }
        matches[canonical_trade_key(event)] = record
        event.market_ws_corroboration = record

    p50 = _percentile(latency_samples, 50)
    p95 = _percentile(latency_samples, 95)
    return {
        "enabled": True,
        "path": str(path),
        "loaded_rows": len(rows),
        "indexed_rows": indexed_rows,
        "indexed_items": indexed_items,
        "matched_events": len(matches),
        "status": "MATCHED" if matches else "NO_MATCHES",
        "buffer": buffer_stats or _rows_time_span(rows),
        "ws_to_api_observed_latency_s": {
            "min": round(min(latency_samples), 6) if latency_samples else None,
            "p50": round(p50, 6) if p50 is not None else None,
            "p95": round(p95, 6) if p95 is not None else None,
            "max": round(max(latency_samples), 6) if latency_samples else None,
        },
        "matches": matches,
    }


def latency_posture(
    events: list[WalletEvent],
    *,
    source_metrics: dict[str, dict[str, Any]],
    api_errors: dict[str, str],
    pass_s: float,
    watch_s: float,
    market_ws_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request_durations = [
        safe_float(row.get("duration_s"))
        for row in source_metrics.values()
        if isinstance(row, dict) and row.get("ok") is True
    ]
    api_latencies = [
        float(event.api_latency_s)
        for event in events
        if event.row_type == "TRADE" and event.api_latency_s is not None
    ]
    latest_event_ts = max((event.event_ts for event in events if event.event_ts), default=0)
    now_ts = max(
        [time.time()] + [
            safe_float(row.get("request_end_ts"))
            for row in source_metrics.values()
            if isinstance(row, dict)
        ]
    )
    latest_event_age_s = (now_ts - latest_event_ts) if latest_event_ts else None
    min_latency = min(api_latencies) if api_latencies else None
    p50_latency = _percentile(api_latencies, 50)
    p95_latency = _percentile(api_latencies, 95)

    status = "PASS"
    reasons: list[str] = []
    if api_errors:
        status = "REPAIR"
        reasons.append("api_errors")
    elif not events:
        status = "WATCH"
        reasons.append("no_recent_wallet_events_in_api_page")
    elif min_latency is None:
        status = "WATCH"
        reasons.append("no_trade_latency_sample")
    elif min_latency > watch_s:
        status = "WATCH"
        reasons.append("confirmed_wallet_events_are_not_near_live")
    elif min_latency > pass_s:
        status = "WATCH"
        reasons.append("confirmed_wallet_event_latency_above_pass_threshold")

    ws_status = (market_ws_summary or {}).get("status") if isinstance(market_ws_summary, dict) else None
    if ws_status in {"NO_MARKET_WS_ROWS", "NO_MATCHES"} and events:
        reasons.append("market_ws_corroboration_missing")
        if status == "PASS":
            status = "WATCH"

    return {
        "status": status,
        "reasons": reasons,
        "thresholds": {
            "pass_s": float(pass_s),
            "watch_s": float(watch_s),
        },
        "request_duration_s": {
            "min": round(min(request_durations), 6) if request_durations else None,
            "max": round(max(request_durations), 6) if request_durations else None,
        },
        "api_event_latency_s": {
            "min": round(min_latency, 6) if min_latency is not None else None,
            "p50": round(p50_latency, 6) if p50_latency is not None else None,
            "p95": round(p95_latency, 6) if p95_latency is not None else None,
            "max": round(max(api_latencies), 6) if api_latencies else None,
        },
        "latest_event_age_s": round(latest_event_age_s, 6) if latest_event_age_s is not None else None,
        "source_metrics": source_metrics,
        "api_errors": api_errors,
        "market_ws_status": ws_status,
    }


def aggregate_window_flow(events: list[WalletEvent], *, now_ts: float | None = None) -> list[WindowFlowSummary]:
    now = float(now_ts if now_ts is not None else time.time())
    grouped: dict[str, list[WalletEvent]] = defaultdict(list)
    for event in events:
        if event.condition_id:
            grouped[event.condition_id].append(event)

    summaries: list[WindowFlowSummary] = []
    for condition_id, rows in grouped.items():
        trades = _canonical_trade_events([r for r in rows if r.row_type == "TRADE"])
        if not trades:
            continue
        rows_sorted = sorted(trades, key=lambda r: (r.event_ts, r.observed_ts, r.dedupe_key))
        latest = rows_sorted[-1]
        buy_up = [r for r in rows_sorted if r.side == "BUY" and r.outcome.lower() == "up"]
        buy_down = [r for r in rows_sorted if r.side == "BUY" and r.outcome.lower() == "down"]
        sell_up = [r for r in rows_sorted if r.side == "SELL" and r.outcome.lower() == "up"]
        sell_down = [r for r in rows_sorted if r.side == "SELL" and r.outcome.lower() == "down"]
        buy_up_usdc = sum(r.usdc_size for r in buy_up)
        buy_down_usdc = sum(r.usdc_size for r in buy_down)
        total_buy = buy_up_usdc + buy_down_usdc
        delta = buy_up_usdc - buy_down_usdc
        if total_buy <= 0:
            dominant = ""
            ratio = 0.0
        elif abs(delta) < 1e-9:
            dominant = "flat"
            ratio = 0.0
        else:
            dominant = "Up" if delta > 0 else "Down"
            ratio = abs(delta) / total_buy
        sides = {r.side for r in rows_sorted if r.side}
        summaries.append(WindowFlowSummary(
            condition_id=condition_id,
            market_slug=latest.market_slug,
            title=latest.title,
            window_start_s=latest.window_start_s,
            first_event_ts=min(r.event_ts for r in rows_sorted if r.event_ts),
            last_event_ts=max(r.event_ts for r in rows_sorted if r.event_ts),
            latest_observed_ts=max(r.observed_ts for r in rows_sorted),
            trade_count=len(rows_sorted),
            transaction_count=len({r.transaction_hash for r in rows_sorted if r.transaction_hash}),
            buy_up_count=len(buy_up),
            buy_down_count=len(buy_down),
            buy_up_usdc=round(buy_up_usdc, 6),
            buy_down_usdc=round(buy_down_usdc, 6),
            sell_up_count=len(sell_up),
            sell_down_count=len(sell_down),
            latest_side=latest.side,
            latest_outcome=latest.outcome,
            latest_price=latest.price,
            latest_usdc_size=round(latest.usdc_size, 6),
            dominant_buy_outcome=dominant,
            dominant_buy_usdc_delta=round(delta, 6),
            dominant_buy_ratio=round(ratio, 6),
            both_buy_outcomes=bool(buy_up and buy_down),
            only_buy_side=sides == {"BUY"},
            event_age_s=round(now - latest.event_ts, 3) if latest.event_ts else None,
        ))
    return sorted(summaries, key=lambda item: (item.last_event_ts, item.latest_observed_ts), reverse=True)


def canonical_trade_key(event: WalletEvent) -> str:
    if event.transaction_hash:
        return "|".join([
            event.transaction_hash,
            event.condition_id,
            event.token_id,
            event.side,
            event.outcome,
            f"{event.price:.10f}",
            f"{event.size:.10f}",
            str(event.event_ts),
        ])
    return event.dedupe_key


def _canonical_trade_events(events: list[WalletEvent]) -> list[WalletEvent]:
    by_key: dict[str, WalletEvent] = {}
    for event in events:
        key = canonical_trade_key(event)
        previous = by_key.get(key)
        if previous is None:
            by_key[key] = event
            continue
        # Prefer activity rows for raw type fidelity, otherwise keep the most
        # recently observed row.
        if previous.source != "activity" and event.source == "activity":
            by_key[key] = event
        elif event.observed_ts > previous.observed_ts and previous.source != "activity":
            by_key[key] = event
    return list(by_key.values())


class WeirdPeakWalletTracker:
    def __init__(self, config: WalletTrackConfig):
        self.config = config
        self.api = PolymarketWalletApiClient(config)
        self.onchain = (
            OnchainReceiptVerifier(config.polygon_rpc_url, timeout_s=config.onchain_rpc_timeout_s)
            if config.enable_onchain
            else None
        )
        self._last_recent_log_scan_ts = 0.0
        self._last_recent_onchain_logs: dict[str, Any] = {
            "enabled": bool(self.onchain is not None and self.config.recent_block_scan_depth > 0),
            "logs": [],
            "status": "not_scanned_yet",
        }
        self._last_onchain_hash_check_ts = 0.0
        self.market_ws_buffer = (
            MarketWsEventLogBuffer(
                Path(self.config.market_ws_event_log_path),
                max_lines=max(0, int(self.config.market_ws_tail_lines)),
                max_bytes=max(1, int(self.config.market_ws_tail_max_bytes)),
            )
            if self.config.enable_market_ws_corroboration
            else None
        )

    def poll_once(self) -> dict[str, Any]:
        poll_start_ts = time.time()
        raw_by_source, api_errors, source_metrics = self.api.fetch_all_sources()

        recent_events: list[WalletEvent] = []
        for source, rows in raw_by_source.items():
            observed_ts = safe_float(
                (source_metrics.get(source) or {}).get("request_end_ts"),
                default=poll_start_ts,
            )
            for row in rows:
                event = normalize_wallet_row(
                    row,
                    source=source,
                    target_wallet=self.config.target_wallet,
                    observed_ts=observed_ts,
                    btc_5m_only=self.config.btc_5m_only,
                    min_usdc_size=self.config.min_usdc_size,
                    include_activity_types=self.config.include_activity_types,
                )
                if event is not None:
                    recent_events.append(event)

        state = self._load_state()
        seen = set(state.get("seen_dedupe_keys") or [])
        new_events = [event for event in recent_events if event.dedupe_key not in seen]

        market_ws_summary: dict[str, Any] = {"enabled": False}
        if self.config.enable_market_ws_corroboration:
            buffered_rows: list[dict[str, Any]] | None = None
            buffer_stats: dict[str, Any] | None = None
            if self.market_ws_buffer is not None:
                buffered_rows = self.market_ws_buffer.refresh()
                buffer_stats = dict(self.market_ws_buffer.stats)
            ws_event_max_age_s = max(float(self.config.market_ws_match_window_s), float(self.config.latency_watch_s))
            market_ws_events = [
                event
                for event in recent_events
                if event.event_ts and poll_start_ts - float(event.event_ts) <= ws_event_max_age_s
            ]
            market_ws_summary = _market_ws_corroborate(
                market_ws_events,
                path=Path(self.config.market_ws_event_log_path),
                max_lines=max(0, int(self.config.market_ws_tail_lines)),
                max_bytes=max(1, int(self.config.market_ws_tail_max_bytes)),
                match_window_s=float(self.config.market_ws_match_window_s),
                rows=buffered_rows,
                buffer_stats=buffer_stats,
            )
            market_ws_summary["recent_event_filter"] = {
                "source_event_count": len(recent_events),
                "corroborated_event_count": len(market_ws_events),
                "max_age_s": round(ws_event_max_age_s, 6),
            }

        onchain_by_hash: dict[str, dict[str, Any]] = {}
        recent_onchain_logs: dict[str, Any] = {
            "enabled": bool(self.onchain is not None and self.config.recent_block_scan_depth > 0),
            "logs": [],
        }
        pending_hashes = [
            str(item)
            for item in state.get("pending_onchain_hashes") or []
            if isinstance(item, str) and item.startswith("0x")
        ]
        new_hashes = []
        for event in new_events:
            if event.transaction_hash and event.transaction_hash.startswith("0x"):
                new_hashes.append(event.transaction_hash)
        combined_hashes = list(dict.fromkeys(pending_hashes + new_hashes))
        max_hashes = max(0, int(self.config.max_onchain_hashes_per_poll))
        hashes_to_check = combined_hashes[:max_hashes]
        remaining_hashes = combined_hashes[max_hashes:]
        onchain_hash_check_due = False
        if self.onchain is not None and hashes_to_check:
            hash_interval = max(0.0, float(self.config.onchain_hash_check_interval_s))
            onchain_hash_check_due = (
                self._last_onchain_hash_check_ts <= 0.0
                or hash_interval <= 0.0
                or time.time() - self._last_onchain_hash_check_ts >= hash_interval
            )
        if self.onchain is not None and hashes_to_check and onchain_hash_check_due:
            onchain_by_hash = self.onchain.verify_hashes(hashes_to_check, wallet=self.config.target_wallet)
            self._last_onchain_hash_check_ts = time.time()
            for event in new_events:
                if event.transaction_hash in onchain_by_hash:
                    event.onchain = onchain_by_hash[event.transaction_hash]
            retry_hashes = [
                tx_hash
                for tx_hash, row in onchain_by_hash.items()
                if row.get("error") and row.get("error") != "transaction_not_found"
            ]
            remaining_hashes = list(dict.fromkeys(remaining_hashes + retry_hashes))[
                -max(0, int(self.config.onchain_pending_hash_limit)) :
            ]
        elif self.onchain is not None and hashes_to_check:
            remaining_hashes = combined_hashes[-max(0, int(self.config.onchain_pending_hash_limit)) :]
        if self.onchain is not None and self.config.recent_block_scan_depth > 0:
            scan_interval = max(0.0, float(self.config.onchain_scan_interval_s))
            should_scan = (
                self._last_recent_log_scan_ts <= 0.0
                or scan_interval <= 0.0
                or time.time() - self._last_recent_log_scan_ts >= scan_interval
                or bool(new_events)
            )
            if should_scan:
                recent_onchain_logs = self.onchain.scan_recent_wallet_logs(
                    wallet=self.config.target_wallet,
                    block_depth=self.config.recent_block_scan_depth,
                    max_logs=max(1, int(self.config.max_onchain_logs_per_poll)),
                    addresses=self.config.onchain_log_addresses,
                )
                self._last_recent_log_scan_ts = time.time()
                self._last_recent_onchain_logs = recent_onchain_logs
            else:
                recent_onchain_logs = dict(self._last_recent_onchain_logs)
                recent_onchain_logs["reused_cached_scan"] = True
                recent_onchain_logs["cached_scan_age_s"] = round(time.time() - self._last_recent_log_scan_ts, 6)

        append_jsonl(Path(self.config.event_log_path), [event.as_record() for event in new_events])

        poll_end_ts = time.time()
        summaries = aggregate_window_flow(recent_events, now_ts=poll_end_ts)
        recent_unique_trade_count = len(_canonical_trade_events([event for event in recent_events if event.row_type == "TRADE"]))
        new_unique_trade_count = len(_canonical_trade_events([event for event in new_events if event.row_type == "TRADE"]))
        seen.update(event.dedupe_key for event in new_events)
        seen_trimmed = list(seen)[-5000:]
        latest_event_ts = max((event.event_ts for event in recent_events if event.event_ts), default=0)
        latest_api_latency_s = min(
            (event.api_latency_s for event in recent_events if event.api_latency_s is not None),
            default=None,
        )
        latency = latency_posture(
            recent_events,
            source_metrics=source_metrics,
            api_errors=api_errors,
            pass_s=float(self.config.latency_pass_s),
            watch_s=float(self.config.latency_watch_s),
            market_ws_summary=market_ws_summary,
        )
        payload = {
            "generated_at": utc_now_iso(),
            "read_only": True,
            "can_trade": False,
            "target_wallet": self.config.target_wallet.lower(),
            "btc_5m_only": self.config.btc_5m_only,
            "poll": {
                "start_ts": poll_start_ts,
                "end_ts": poll_end_ts,
                "duration_s": round(poll_end_ts - poll_start_ts, 6),
                "mode": "parallel_rest_plus_onchain_plus_market_ws_corroboration",
                "interval_target_s": None,
            },
            "api_errors": api_errors,
            "source_fetch_metrics": source_metrics,
            "latency_posture": latency,
            "raw_counts": {source: len(rows) for source, rows in raw_by_source.items()},
            "recent_event_count": len(recent_events),
            "recent_unique_trade_count": recent_unique_trade_count,
            "new_event_count": len(new_events),
            "new_unique_trade_count": new_unique_trade_count,
            "latest_event_ts": latest_event_ts,
            "latest_api_latency_s": round(latest_api_latency_s, 3) if latest_api_latency_s is not None else None,
            "onchain": {
                "enabled": self.onchain is not None,
                "checked_hashes": len(onchain_by_hash),
                "pending_hashes": len(remaining_hashes),
                "hash_budget_per_poll": max_hashes,
                "hash_check_due": onchain_hash_check_due,
                "hash_check_interval_s": float(self.config.onchain_hash_check_interval_s),
                "rpc_timeout_s": float(self.config.onchain_rpc_timeout_s),
                "found_hashes": sum(1 for row in onchain_by_hash.values() if row.get("found")),
                "success_hashes": sum(1 for row in onchain_by_hash.values() if row.get("success")),
                "wallet_mentioned_hashes": sum(
                    1 for row in onchain_by_hash.values() if row.get("wallet_mentioned_in_tx_or_logs")
                ),
                "rpc_source": (
                    "configured"
                    if self.config.polygon_rpc_url != DEFAULT_POLYGON_RPC
                    else "public_default"
                ),
            },
            "recent_onchain_logs": recent_onchain_logs,
            "market_ws_corroboration": market_ws_summary,
            "latest_windows": [summary.as_record() for summary in summaries[:25]],
            "pending_onchain_hashes": remaining_hashes,
            "recent_events": [
                event.as_record()
                for event in sorted(recent_events, key=lambda item: (item.event_ts, item.observed_ts), reverse=True)[
                    : max(0, int(self.config.recent_state_event_limit))
                ]
            ],
            "seen_dedupe_keys": seen_trimmed,
        }
        atomic_write_json(Path(self.config.state_path), payload)
        return payload

    def _load_state(self) -> dict[str, Any]:
        path = Path(self.config.state_path)
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}
