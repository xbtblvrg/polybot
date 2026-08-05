#!/usr/bin/env python3
"""Merge RTDS wallet-attributed trade events into wallet-copy history state."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict, deque
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import requests  # noqa: E402

from src.wallet_copy.models import WalletEvent, parse_ts, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.profit_engine import build_history_window_index, load_history_window_index  # noqa: E402
from src.wallet_copy.realtime_feed import normalize_polygon_orderfilled_row  # noqa: E402
from src.wallet_copy.research import unique_wallet_events  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402

DEFAULT_COLD_TAIL_BYTES = 5 * 1024 * 1024
DEFAULT_WATERMARK_STATE = "data/research/wallet_copy_rtds_observation_watermarks.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rtds-jsonl", required=True)
    parser.add_argument("--source-wallet", required=True)
    parser.add_argument("--wallet-name", default="")
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--history-window-index", default="data/research/wallet_copy_history_window_index.json")
    parser.add_argument("--wallet-event-log", default="data/research/wallet_copy_events.jsonl")
    parser.add_argument("--scan-limit", type=int, default=50_000)
    parser.add_argument("--tail-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--cold-tail-bytes", type=int, default=DEFAULT_COLD_TAIL_BYTES)
    parser.add_argument("--offset-state", default="")
    parser.add_argument("--watermark-state", default=DEFAULT_WATERMARK_STATE)
    parser.add_argument("--max-new-events", type=int, default=500)
    parser.add_argument("--history-retain-events", type=int, default=250_000)
    parser.add_argument("--history-retain-copy-intents", type=int, default=250_000)
    parser.add_argument("--polygon-jsonl", default="data/research/polygon_orderfilled_ws_shadow_resident.jsonl")
    parser.add_argument("--polygon-tail-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--gamma-base-url", default=os.getenv("POLYMARKET_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"))
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _raw_dict(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw")
    return raw if isinstance(raw, dict) else {}


def _window_start_from_slug(slug: str) -> int | None:
    marker = str(slug or "").rsplit("-", 1)[-1]
    return int(marker) if marker.isdigit() else None


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


def _iter_tail_lines(path: str, *, tail_bytes: int) -> list[str]:
    lines, _profile = _iter_tail_lines_profiled(path, tail_bytes=tail_bytes)
    return lines


def _iter_line_records_profiled(
    path: str,
    *,
    start_offset: int,
    drop_partial_first_line: bool,
    bytes_requested: int,
    max_bytes: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        raise SystemExit(f"RTDS jsonl not found: {path}")
    started = time.perf_counter()
    size = target.stat().st_size
    start = min(max(0, int(start_offset)), int(size))
    records: list[dict[str, Any]] = []
    read_limit = None if max_bytes is None or int(max_bytes) <= 0 else max(0, int(max_bytes))
    end_offset = int(size) if read_limit is None else min(int(size), int(start) + int(read_limit))
    with target.open("rb") as raw:
        raw.seek(start)
        if start > 0 and drop_partial_first_line:
            raw.readline()
        actual_start = raw.tell()
        while raw.tell() < end_offset:
            line_start = raw.tell()
            raw_line = raw.readline()
            if not raw_line:
                break
            line_end = raw.tell()
            if not raw_line.strip():
                continue
            records.append(
                {
                    "start_offset": int(line_start),
                    "end_offset": int(line_end),
                    "line": raw_line.decode("utf-8", errors="replace"),
                }
            )
    return records, {
        "stage": "tail_open_seek_read",
        "duration_s": round(time.perf_counter() - started, 6),
        "file_size": int(size),
        "start_offset": int(actual_start),
        "end_offset": int(end_offset),
        "max_bytes": read_limit,
        "bytes_requested": int(bytes_requested),
        "bytes_read": max(0, int(end_offset) - int(actual_start)),
        "line_count": len(records),
    }


def _iter_tail_lines_profiled(path: str, *, tail_bytes: int) -> tuple[list[str], dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        raise SystemExit(f"RTDS jsonl not found: {path}")
    size = target.stat().st_size
    start = max(0, size - max(0, int(tail_bytes)))
    records, profile = _iter_line_records_profiled(
        path,
        start_offset=start,
        drop_partial_first_line=start > 0,
        bytes_requested=int(tail_bytes),
        max_bytes=int(tail_bytes),
    )
    return [str(record.get("line") or "") for record in records], profile


def _default_offset_state(history_state: str, rtds_jsonl: str) -> str:
    suffix = stable_id("rtds_offset", {"history_state": history_state, "rtds_jsonl": rtds_jsonl})[-12:]
    return f"{history_state}.{suffix}.offset.json"


def _file_identity(path: str) -> dict[str, int]:
    stat = os.stat(path)
    return {"inode": int(stat.st_ino), "size": int(stat.st_size), "mtime_s": int(stat.st_mtime)}


def _load_offset_state(path: str) -> dict[str, Any]:
    try:
        row = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return row if isinstance(row, dict) else {}


def _write_offset_state(path: str, payload: dict[str, Any]) -> None:
    atomic_write_json(path, payload)


def _offset_gap_limit_bytes(*, tail_bytes: int, cold_tail_bytes: int) -> int:
    configured_tail = max(0, int(tail_bytes or 0))
    if configured_tail > 0:
        return configured_tail
    configured_cold_tail = max(0, int(cold_tail_bytes or 0))
    if configured_cold_tail > 0:
        return configured_cold_tail
    return DEFAULT_COLD_TAIL_BYTES


def _iter_incremental_lines(
    path: str,
    *,
    offset_state: str,
    tail_bytes: int,
    cold_tail_bytes: int = DEFAULT_COLD_TAIL_BYTES,
) -> tuple[list[str], dict[str, Any]]:
    read_profile: dict[str, Any]
    target = Path(path)
    if not target.exists():
        raise SystemExit(f"RTDS jsonl not found: {path}")
    identity = _file_identity(path)
    state = _load_offset_state(offset_state)
    offset = _int(state.get("offset"))
    identity_matches = (
        str(state.get("path") or "") == str(path)
        and _int(state.get("inode")) == identity["inode"]
        and offset is not None
        and 0 <= int(offset) <= identity["size"]
    )
    if not identity_matches:
        fallback_tail_bytes = max(0, int(cold_tail_bytes)) or int(tail_bytes)
        lines, read_profile = _iter_tail_lines_profiled(path, tail_bytes=fallback_tail_bytes)
        mode = "tail_fallback"
    else:
        offset_gap = max(0, identity["size"] - int(offset))
        offset_gap_limit = _offset_gap_limit_bytes(tail_bytes=tail_bytes, cold_tail_bytes=cold_tail_bytes)
        if offset_gap > offset_gap_limit:
            effective_start, _size = _line_start_for_tail(path, tail_bytes=offset_gap_limit)
            mode = "offset_gap_tail_clamp"
        else:
            effective_start = int(offset)
            mode = "offset"
        fallback_tail_bytes = int(offset_gap_limit)
        records, read_profile = _iter_line_records_profiled(
            path,
            start_offset=int(effective_start),
            drop_partial_first_line=False,
            bytes_requested=max(0, identity["size"] - int(effective_start)),
            max_bytes=offset_gap_limit,
        )
        lines = [str(record.get("line") or "") for record in records]
        read_profile.update(
            {
                "previous_offset": int(offset),
                "offset_gap_bytes": int(offset_gap),
                "offset_gap_limit_bytes": int(offset_gap_limit),
            }
        )
    offset_write_started = time.perf_counter()
    _write_offset_state(
        offset_state,
        {
            "schema_version": 1,
            "path": str(path),
            "inode": identity["inode"],
            "offset": identity["size"],
            "size": identity["size"],
            "mtime_s": identity["mtime_s"],
            "updated_at": utc_now_iso(),
        },
    )
    offset_write_profile = {
        "stage": "offset_state_write",
        "duration_s": round(time.perf_counter() - offset_write_started, 6),
        "offset_state": offset_state,
        "next_offset": identity["size"],
    }
    return lines, {
        "mode": mode,
        "offset_state": offset_state,
        "previous_offset": int(offset) if identity_matches and offset is not None else None,
        "next_offset": identity["size"],
        "file_size": identity["size"],
        "file_mtime_s": identity["mtime_s"],
        "fallback_tail_bytes": fallback_tail_bytes,
        "premerge_substage_profile": {
            "tail_open_seek_read": read_profile,
            "offset_state_write": offset_write_profile,
        },
    }


def _iter_recent_jsonl(path: str, limit: int, *, tail_bytes: int = 256 * 1024 * 1024) -> list[dict[str, Any]]:
    recent_lines: deque[str] = deque(maxlen=max(1, int(limit)))
    for line in _iter_tail_lines(path, tail_bytes=tail_bytes):
        recent_lines.append(line)
    rows: list[dict[str, Any]] = []
    for line in recent_lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _walk_history_events(obj: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        if obj.get("token_id") and (obj.get("market_slug") or obj.get("condition_id")):
            rows.append(obj)
        for value in obj.values():
            rows.extend(_walk_history_events(value))
    elif isinstance(obj, list):
        for value in obj:
            rows.extend(_walk_history_events(value))
    return rows


def _token_metadata_from_history(history_payload: dict[str, Any]) -> dict[str, dict[str, str]]:
    grouped: dict[str, dict[str, Counter[str]]] = {}
    event_rows = history_payload.get("events") if isinstance(history_payload.get("events"), list) else []
    rows = [row for row in event_rows if isinstance(row, dict)]
    if not rows:
        rows = _walk_history_events(history_payload)
    for row in rows:
        token_id = str(row.get("token_id") or "")
        if not token_id:
            continue
        entry = grouped.setdefault(
            token_id,
            {"market_slug": Counter(), "condition_id": Counter(), "outcome": Counter()},
        )
        for key in ("market_slug", "condition_id", "outcome"):
            value = str(row.get(key) or "")
            if value:
                entry[key][value] += 1
    return {
        token_id: {
            key: (counter.most_common(1)[0][0] if counter else "")
            for key, counter in counters.items()
        }
        for token_id, counters in grouped.items()
    }


def _token_metadata_from_events(events: list[WalletEvent]) -> dict[str, dict[str, str]]:
    meta: dict[str, dict[str, str]] = {}
    for event in events:
        token_id = str(event.token_id or "")
        if not token_id:
            continue
        market_slug = str(event.market_slug or "")
        condition_id = str(event.condition_id or "")
        outcome = str(event.outcome or "")
        if not (market_slug and condition_id and outcome):
            continue
        existing = meta.setdefault(token_id, {"market_slug": "", "condition_id": "", "outcome": ""})
        existing["market_slug"] = existing["market_slug"] or market_slug
        existing["condition_id"] = existing["condition_id"] or condition_id
        existing["outcome"] = existing["outcome"] or outcome
    return meta


def _fill_token_metadata_from_events(
    token_meta: dict[str, dict[str, str]],
    events: list[WalletEvent],
) -> dict[str, dict[str, str]]:
    for token_id, seeded in _token_metadata_from_events(events).items():
        entry = token_meta.setdefault(token_id, {"market_slug": "", "condition_id": "", "outcome": ""})
        for key, value in seeded.items():
            if value and not entry.get(key):
                entry[key] = value
    return token_meta


def _gamma_token_metadata(
    base_url: str,
    *,
    starts: set[int],
    timeout_s: float = 3.0,
    stats: dict[str, int] | None = None,
) -> dict[str, dict[str, str]]:
    # Gamma's /markets?slug= filter EXCLUDES closed markets by default. A BTC-5m
    # window closes five minutes after it opens, so every window a resident or
    # historical capture replays is already closed and resolves to []. Asking
    # once, open-only, silently returns an empty map that is indistinguishable
    # from "these tokens are not BTC-5m" — that is what pinned the OrderFilled
    # stakeout at unique_resolved_source_events=0. Retry every empty lookup with
    # closed=true, and count request failures separately so an outage can never
    # again be read as out-of-lane supply.
    if stats is not None:
        stats.setdefault("starts_requested", 0)
        stats.setdefault("slugs_resolved", 0)
        stats.setdefault("slugs_resolved_via_closed_retry", 0)
        stats.setdefault("slugs_empty", 0)
        stats.setdefault("request_failures", 0)
    if not base_url or not starts:
        return {}
    meta: dict[str, dict[str, str]] = {}
    session = requests.Session()
    for start in sorted(starts):
        slug = f"btc-updown-5m-{int(start)}"
        if stats is not None:
            stats["starts_requested"] += 1
        market: dict[str, Any] = {}
        failed = False
        via_closed_retry = False
        for attempt, params in enumerate(({"slug": slug}, {"slug": slug, "closed": "true"})):
            try:
                response = session.get(
                    base_url.rstrip("/") + "/markets",
                    params=params,
                    timeout=timeout_s,
                    headers={"Accept": "application/json", "User-Agent": "wallet-copy-polygon-premerge/1.0"},
                )
                response.raise_for_status()
                payload = response.json()
            except Exception:
                failed = True
                continue
            failed = False
            candidate = payload[0] if isinstance(payload, list) and payload else payload if isinstance(payload, dict) else {}
            if isinstance(candidate, dict) and candidate:
                market = candidate
                via_closed_retry = attempt == 1
                break
        if stats is not None:
            if failed:
                stats["request_failures"] += 1
            elif market:
                stats["slugs_resolved"] += 1
                if via_closed_retry:
                    stats["slugs_resolved_via_closed_retry"] += 1
            else:
                stats["slugs_empty"] += 1
        if not market:
            continue
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "")
        tokens = [str(item) for item in _json_list(market.get("clobTokenIds") or market.get("clob_token_ids"))]
        outcomes = [str(item) for item in _json_list(market.get("outcomes"))]
        if not outcomes and len(tokens) >= 2:
            outcomes = ["Up", "Down"]
        outcome_prices = _json_list(market.get("outcomePrices") or market.get("outcome_prices"))
        priced_outcomes: list[tuple[float, str]] = []
        for idx, value in enumerate(outcome_prices):
            if idx >= len(outcomes):
                break
            try:
                priced_outcomes.append((float(value), outcomes[idx]))
            except (TypeError, ValueError):
                continue
        winning_outcome = ""
        if bool(market.get("closed")) and priced_outcomes:
            winning_price, candidate_winner = max(priced_outcomes)
            if winning_price >= 0.99:
                winning_outcome = candidate_winner
        for idx, token_id in enumerate(tokens):
            if not token_id:
                continue
            meta[token_id] = {
                "market_slug": slug,
                "condition_id": condition_id,
                "outcome": outcomes[idx] if idx < len(outcomes) else "",
                "winning_outcome": winning_outcome,
            }
    return meta


def _wallet_event_from_polygon(
    row: dict[str, Any],
    *,
    source_wallet: str,
    wallet_name: str,
    token_meta: dict[str, dict[str, str]],
    diagnostics: Counter[str],
) -> WalletEvent | None:
    if row.get("event") != "polygon_orderfilled_log":
        return None
    event = normalize_polygon_orderfilled_row(row)
    if event is None:
        diagnostics["normalize_missing"] += 1
        return None
    wallet = _norm_wallet(event.source_wallet)
    if wallet != source_wallet:
        return None
    if str(event.side or "").upper() != "BUY":
        diagnostics["non_buy"] += 1
        return None
    token_id = str(event.asset or "")
    meta = token_meta.get(token_id) or {}
    market_slug = str(meta.get("market_slug") or "")
    condition_id = str(meta.get("condition_id") or "")
    outcome = str(meta.get("outcome") or "")
    if not (token_id and market_slug and condition_id and outcome):
        diagnostics["token_mapping_missing"] += 1
        return None
    if event.price is None or event.size is None or event.event_ts is None or not event.received_at_s:
        diagnostics["required_field_missing"] += 1
        return None
    event_ts = float(event.event_ts)
    observed_ts = float(event.received_at_s)
    return WalletEvent(
        source_wallet=source_wallet,
        wallet_name=wallet_name or source_wallet,
        row_type="trade",
        action="BUY",
        condition_id=condition_id,
        market_slug=market_slug,
        outcome=outcome,
        price=float(event.price),
        size=float(event.size),
        usdc_size=round(float(event.price) * float(event.size), 6),
        event_ts=event_ts,
        observed_ts=observed_ts,
        source="polygon_orderfilled_ws_premerge",
        market_id=condition_id,
        event_slug=market_slug,
        asset="BTC",
        duration="5m",
        window_start_s=_window_start_from_slug(market_slug),
        token_id=token_id,
        transaction_hash=str(event.transaction_hash or ""),
        api_latency_s=max(0.0, observed_ts - event_ts),
        raw={**row, "_walletCopySource": "polygon_orderfilled_ws_premerge"},
    )


def _iter_polygon_wallet_events(
    path: str,
    *,
    wallets: list[str],
    wallet_names: dict[str, str],
    token_meta: dict[str, dict[str, str]],
    tail_bytes: int,
    max_events_per_wallet: int,
) -> tuple[dict[str, list[WalletEvent]], dict[str, Any]]:
    started = time.perf_counter()
    events_by_wallet: dict[str, deque[WalletEvent]] = {
        wallet: deque(maxlen=max(1, int(max_events_per_wallet))) for wallet in wallets
    }
    diagnostics: Counter[str] = Counter()
    if not path or not Path(path).exists() or not wallets:
        return {wallet: [] for wallet in wallets}, {
            "stage": "polygon_ws_premerge_parse",
            "duration_s": round(time.perf_counter() - started, 6),
            "path": path,
            "status": "MISSING_OR_DISABLED",
            "matching_events": 0,
            "new_source": "polygon_orderfilled_ws_premerge",
            "diagnostics": dict(diagnostics),
        }
    lines, read_profile = _iter_tail_lines_profiled(path, tail_bytes=max(1, int(tail_bytes)))
    parsed_rows = 0
    json_decode_errors = 0
    starts: set[int] = set()
    parsed: list[dict[str, Any]] = []
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            json_decode_errors += 1
            continue
        if not isinstance(row, dict):
            continue
        parsed_rows += 1
        parsed.append(row)
        decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
        token_id = str(decoded.get("asset") or row.get("asset") or "")
        ts = parse_ts(row.get("event_ts") or row.get("block_ts") or row.get("captured_at_s"))
        if ts and token_id and token_id not in token_meta:
            starts.add(int(float(ts) // 300 * 300))
    if starts:
        token_meta.update({k: v for k, v in _gamma_token_metadata(os.getenv("POLYMARKET_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"), starts=starts).items() if k not in token_meta})
    wallet_set = set(wallets)
    for row in parsed:
        candidate_wallets = [
            _norm_wallet(row.get("selected_wallet")),
            _norm_wallet(row.get("maker")),
            _norm_wallet(row.get("taker")),
        ]
        for wallet in dict.fromkeys(item for item in candidate_wallets if item in wallet_set):
            event = _wallet_event_from_polygon(
                row,
                source_wallet=wallet,
                wallet_name=str(wallet_names.get(wallet) or wallet),
                token_meta=token_meta,
                diagnostics=diagnostics,
            )
            if event is not None:
                events_by_wallet[wallet].append(event)
    profile = {
        "stage": "polygon_ws_premerge_parse",
        "duration_s": round(time.perf_counter() - started, 6),
        "path": path,
        "tail_bytes": int(tail_bytes),
        "line_count": len(lines),
        "parsed_rows": parsed_rows,
        "json_decode_errors": json_decode_errors,
        "matching_events": sum(len(rows) for rows in events_by_wallet.values()),
        "diagnostics": dict(diagnostics),
        "tail_open_seek_read": read_profile,
        "new_source": "polygon_orderfilled_ws_premerge",
    }
    return {wallet: list(rows) for wallet, rows in events_by_wallet.items()}, profile


def _merge_polygon_profiles(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    if not profiles:
        return {}
    merged = dict(profiles[0])
    diagnostics: Counter[str] = Counter()
    first_pass_token_mapping_missing = 0
    token_mapping_unresolved_final = 0
    for idx, profile in enumerate(profiles):
        for key in ("duration_s", "line_count", "parsed_rows", "json_decode_errors", "matching_events"):
            try:
                merged[key] = round(float(merged.get(key) or 0.0) + float(profile.get(key) or 0.0), 6)
            except (TypeError, ValueError):
                pass
        row_diagnostics = profile.get("diagnostics") if isinstance(profile.get("diagnostics"), dict) else {}
        diagnostics.update(row_diagnostics)
        try:
            missing = int(row_diagnostics.get("token_mapping_missing") or 0)
        except (TypeError, ValueError):
            missing = 0
        if len(profiles) > 1 and idx == 0:
            first_pass_token_mapping_missing = missing
        else:
            token_mapping_unresolved_final += missing
    merged["diagnostics"] = dict(diagnostics)
    merged["stage"] = "polygon_ws_premerge_parse"
    merged["new_source"] = "polygon_orderfilled_ws_premerge"
    merged["legacy_selected_wallet_ordering"] = len(profiles) > 1
    merged["first_pass_token_mapping_missing"] = first_pass_token_mapping_missing
    merged["token_mapping_unresolved_final"] = token_mapping_unresolved_final
    return merged


def _iter_recent_wallet_events(
    path: str,
    *,
    source_wallet: str,
    wallet_name: str,
    limit: int,
    tail_bytes: int = 256 * 1024 * 1024,
    cold_tail_bytes: int = DEFAULT_COLD_TAIL_BYTES,
    offset_state: str = "",
) -> tuple[int, list[WalletEvent], dict[str, Any]]:
    scanned_rows = 0
    events: deque[WalletEvent] = deque(maxlen=max(1, int(limit)))
    if offset_state:
        lines, ingest_state = _iter_incremental_lines(
            path,
            offset_state=offset_state,
            tail_bytes=tail_bytes,
            cold_tail_bytes=cold_tail_bytes,
        )
    else:
        lines, read_profile = _iter_tail_lines_profiled(path, tail_bytes=tail_bytes)
        ingest_state = {
            "mode": "tail",
            "premerge_substage_profile": {"tail_open_seek_read": read_profile},
        }
    latest_processed_captured_at_s: float | None = None
    parse_started = time.perf_counter()
    json_decode_errors = 0
    parsed_rows = 0
    for line in lines:
        scanned_rows += 1
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            json_decode_errors += 1
            continue
        if not isinstance(row, dict):
            continue
        parsed_rows += 1
        captured_at_s = _float(row.get("captured_at_s") or row.get("received_at_s"))
        if captured_at_s is not None:
            latest_processed_captured_at_s = max(float(captured_at_s), latest_processed_captured_at_s or 0.0)
        event = _wallet_event_from_rtds(row, source_wallet=source_wallet, wallet_name=wallet_name)
        if event is not None:
            events.append(event)
    premerge_profile = ingest_state.setdefault("premerge_substage_profile", {})
    premerge_profile["line_json_parse"] = {
        "stage": "line_json_parse",
        "duration_s": round(time.perf_counter() - parse_started, 6),
        "line_count": len(lines),
        "scanned_rows": scanned_rows,
        "parsed_rows": parsed_rows,
        "json_decode_errors": json_decode_errors,
        "matching_events": len(events),
    }
    if latest_processed_captured_at_s is not None:
        ingest_state["latest_processed_captured_at_s"] = latest_processed_captured_at_s
        ingest_state["rtds_catchup_lag_s"] = round(max(0.0, time.time() - latest_processed_captured_at_s), 6)
    elif not lines:
        ingest_state["latest_processed_captured_at_s"] = None
        ingest_state["rtds_catchup_lag_s"] = 0.0
    return scanned_rows, list(events), ingest_state


def _write_observation_watermark(
    path: str,
    *,
    source_wallet: str,
    rtds_jsonl: str,
    offset_state: str,
    ingest_state: dict[str, Any],
    latest_event_ts: float,
    latest_observed_ts: float,
    retained_matching_rows: int,
    new_matching_events: int,
) -> dict[str, Any]:
    target = Path(path)
    existing = load_json(path, default={})
    existing = existing if isinstance(existing, dict) else {}
    wallets = existing.get("wallets") if isinstance(existing.get("wallets"), dict) else {}
    latest_processed = _float(ingest_state.get("latest_processed_captured_at_s"))
    file_mtime = _float(ingest_state.get("file_mtime_s"))
    latest_checked_ts = max(latest_processed or 0.0, file_mtime or 0.0, latest_observed_ts or 0.0)
    row = {
        "source_wallet": source_wallet,
        "rtds_jsonl": rtds_jsonl,
        "offset_state": offset_state,
        "latest_checked_ts": round(float(latest_checked_ts), 6),
        "latest_matching_event_ts": round(float(latest_event_ts), 6) if latest_event_ts > 0 else None,
        "latest_matching_observed_ts": round(float(latest_observed_ts), 6) if latest_observed_ts > 0 else None,
        "retained_matching_rows": int(retained_matching_rows),
        "new_matching_events": int(new_matching_events),
        "history_write_skipped_safe": bool(new_matching_events == 0),
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
    }
    wallets[source_wallet] = row
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_rtds_observation_watermarks",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "wallets": wallets,
    }
    atomic_write_json(target, payload)
    return row


def _wallet_event_from_rtds(row: dict[str, Any], *, source_wallet: str, wallet_name: str) -> WalletEvent | None:
    if row.get("event") != "rtds_trade_event":
        return None
    raw = _raw_dict(row)
    wallet = _norm_wallet(row.get("source_wallet") or raw.get("proxyWallet"))
    if wallet != source_wallet:
        return None
    side = str(row.get("side") or raw.get("side") or "").upper()
    if side not in {"BUY", "SELL"}:
        return None
    market_slug = str(row.get("market_slug") or raw.get("slug") or raw.get("eventSlug") or "")
    condition_id = str(row.get("condition_id") or raw.get("conditionId") or "")
    token_id = str(row.get("asset") or raw.get("asset") or raw.get("tokenId") or "")
    outcome = str(raw.get("outcome") or row.get("outcome") or "")
    price = _float(row.get("price") if row.get("price") is not None else raw.get("price"))
    size = _float(row.get("size") if row.get("size") is not None else raw.get("size"))
    event_ts = _float(row.get("event_ts") if row.get("event_ts") is not None else raw.get("timestamp"))
    observed_ts = _float(
        row.get("received_at_s")
        or row.get("captured_at_s")
        or raw.get("received_at_s")
        or raw.get("captured_at_s")
        or row.get("event_ts")
        or raw.get("timestamp")
    )
    if not (market_slug and condition_id and token_id and outcome and price and size and event_ts and observed_ts):
        return None
    transaction_hash = str(row.get("transaction_hash") or raw.get("transactionHash") or "")
    event_id = stable_id(
        "we",
        {
            "source": "rtds_activity",
            "wallet": wallet,
            "tx": transaction_hash,
            "condition_id": condition_id,
            "token_id": token_id,
            "outcome": outcome,
            "side": side,
            "price": round(float(price), 8),
            "size": round(float(size), 8),
            "event_ts": event_ts,
        },
    )
    return WalletEvent(
        source_wallet=wallet,
        wallet_name=wallet_name or wallet,
        row_type="trade",
        action=side,
        condition_id=condition_id,
        market_slug=market_slug,
        outcome=outcome,
        price=float(price),
        size=float(size),
        usdc_size=round(float(price) * float(size), 6),
        event_ts=float(event_ts),
        observed_ts=float(observed_ts),
        event_id=event_id,
        source="rtds_activity",
        market_id=condition_id,
        event_slug=market_slug,
        title=str(raw.get("title") or ""),
        asset="BTC" if market_slug.startswith("btc-updown-5m-") else "",
        duration="5m" if market_slug.startswith("btc-updown-5m-") else "",
        window_start_s=_window_start_from_slug(market_slug),
        token_id=token_id,
        outcome_index=_int(raw.get("outcomeIndex")),
        transaction_hash=transaction_hash,
        api_latency_s=max(0.0, float(observed_ts) - float(event_ts)),
        raw={**raw, "_walletCopySource": "rtds_activity"},
    )


def _event_sort_key(row: dict[str, Any]) -> tuple[float, str]:
    try:
        event_ts = float(row.get("event_ts") or 0.0)
    except (TypeError, ValueError):
        event_ts = 0.0
    return event_ts, str(row.get("event_id") or row.get("source_fingerprint") or "")


def _event_identity_key_from_row(row: dict[str, Any]) -> str:
    return str(row.get("source_fingerprint") or row.get("event_id") or (
        f"{str(row.get('source_wallet') or '').lower()}:{row.get('transaction_hash')}:"
        f"{row.get('condition_id')}:{row.get('action')}:{row.get('outcome')}:"
        f"{row.get('event_ts')}:{row.get('price')}:{row.get('usdc_size')}"
    ))


def _event_identity_key_from_event(event: WalletEvent) -> str:
    return event.source_fingerprint or event.event_id or (
        f"{event.source_wallet.lower()}:{event.transaction_hash}:"
        f"{event.condition_id}:{event.action}:{event.outcome}:"
        f"{event.event_ts}:{event.price}:{event.usdc_size}"
    )


def _prefer_event_row(current_row: dict[str, Any], candidate: WalletEvent) -> dict[str, Any]:
    try:
        current = WalletEvent.from_dict(current_row)
    except TypeError:
        return current_row
    preferred = unique_wallet_events([current, candidate])[0]
    return preferred.asdict()


def _merge_matching_events_into_rows(
    existing_rows: list[dict[str, Any]],
    matching_events: list[WalletEvent],
) -> tuple[list[dict[str, Any]], int]:
    """Merge small fresh event deltas into existing history without objectizing all history."""

    rows_by_key: dict[str, dict[str, Any]] = {}
    for row in existing_rows:
        if not isinstance(row, dict):
            continue
        rows_by_key[_event_identity_key_from_row(row)] = row
    for event in matching_events:
        key = _event_identity_key_from_event(event)
        existing_row = rows_by_key.get(key)
        if existing_row is None:
            rows_by_key[key] = event.asdict()
        else:
            rows_by_key[key] = _prefer_event_row(existing_row, event)
    return list(rows_by_key.values()), len(rows_by_key)


def _wallet_event_rows(rows: list[dict[str, Any]], wallet: str) -> list[dict[str, Any]]:
    wallet = _norm_wallet(wallet)
    return [row for row in rows if _norm_wallet(row.get("source_wallet")) == wallet]


def _latest_event_ts_from_rows(rows: list[dict[str, Any]]) -> float:
    latest = 0.0
    for row in rows:
        parsed = _float(row.get("event_ts"))
        if parsed is not None:
            latest = max(latest, float(parsed))
    return latest


def _latest_observed_ts_from_rows(rows: list[dict[str, Any]]) -> float:
    latest = 0.0
    for row in rows:
        parsed = _float(row.get("observed_ts"))
        if parsed is not None:
            latest = max(latest, float(parsed))
    return latest


def _intent_sort_key(row: dict[str, Any]) -> tuple[float, str]:
    try:
        event_ts = float(
            row.get("observed_ts")
            or row.get("source_event_ts")
            or row.get("created_ts")
            or 0.0
        )
    except (TypeError, ValueError):
        event_ts = 0.0
    return event_ts, str(row.get("intent_id") or "")


def _retain_tail(rows: list[dict[str, Any]], retain: int) -> list[dict[str, Any]]:
    return rows if int(retain) <= 0 else rows[-int(retain) :]


def _events_new_to_history(existing_events: list[WalletEvent], candidate_events: list[WalletEvent]) -> list[WalletEvent]:
    existing_ids = {str(event.event_id) for event in existing_events if str(event.event_id)}
    return _events_new_to_history_with_ids(existing_ids, candidate_events)


def _events_new_to_history_with_ids(existing_ids: set[str], candidate_events: list[WalletEvent]) -> list[WalletEvent]:
    new_events: list[WalletEvent] = []
    seen_new_ids: set[str] = set()
    for event in candidate_events:
        event_id = str(event.event_id or "")
        if not event_id or event_id in existing_ids or event_id in seen_new_ids:
            continue
        new_events.append(event)
        seen_new_ids.add(event_id)
    return new_events


def _line_start_for_tail(path: str, *, tail_bytes: int) -> tuple[int, int]:
    target = Path(path)
    size = target.stat().st_size
    start = max(0, size - max(0, int(tail_bytes)))
    if start <= 0:
        return 0, size
    with target.open("rb") as raw:
        raw.seek(start)
        raw.readline()
        return int(raw.tell()), size


def _batch_line_plan(
    path: str,
    *,
    wallets: list[str],
    offset_states: dict[str, str],
    tail_bytes: int,
    cold_tail_bytes: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    identity = _file_identity(path)
    plans: dict[str, dict[str, Any]] = {}
    min_start: int | None = None
    batch_tail_bytes_limit = _offset_gap_limit_bytes(tail_bytes=tail_bytes, cold_tail_bytes=cold_tail_bytes)
    batch_tail_start, _size = _line_start_for_tail(path, tail_bytes=batch_tail_bytes_limit)
    for wallet in wallets:
        offset_state = offset_states[wallet]
        state = _load_offset_state(offset_state)
        offset = _int(state.get("offset"))
        identity_matches = (
            str(state.get("path") or "") == str(path)
            and _int(state.get("inode")) == identity["inode"]
            and offset is not None
            and 0 <= int(offset) <= identity["size"]
        )
        if identity_matches:
            offset_gap = max(0, identity["size"] - int(offset))
            fallback_tail_bytes = _offset_gap_limit_bytes(tail_bytes=tail_bytes, cold_tail_bytes=cold_tail_bytes)
            if offset_gap > fallback_tail_bytes:
                effective_start, _size = _line_start_for_tail(path, tail_bytes=fallback_tail_bytes)
                mode = "offset_gap_tail_clamp"
            else:
                effective_start = int(offset)
                mode = "offset"
            previous_offset = int(offset)
        else:
            fallback_tail_bytes = max(0, int(cold_tail_bytes)) or int(tail_bytes)
            effective_start, _size = _line_start_for_tail(path, tail_bytes=fallback_tail_bytes)
            mode = "tail_fallback"
            previous_offset = None
        min_start = effective_start if min_start is None else min(min_start, effective_start)
        plans[wallet] = {
            "mode": mode,
            "offset_state": offset_state,
            "previous_offset": previous_offset,
            "next_offset": identity["size"],
            "file_size": identity["size"],
            "file_mtime_s": identity["mtime_s"],
            "fallback_tail_bytes": fallback_tail_bytes,
            "effective_start": effective_start,
        }
    raw_min_start = int(min_start or 0)
    capped_min_start = max(raw_min_start, int(batch_tail_start))
    if capped_min_start > raw_min_start:
        for plan in plans.values():
            if int(plan["effective_start"]) < capped_min_start:
                plan["effective_start_before_batch_clamp"] = int(plan["effective_start"])
                plan["effective_start"] = int(capped_min_start)
                plan["batch_tail_clamped"] = True
                plan["mode"] = f"{plan['mode']}_batch_tail_clamp"
    return plans, {
        "identity": identity,
        "min_start": int(capped_min_start),
        "raw_min_start": int(raw_min_start),
        "batch_tail_bytes_limit": int(batch_tail_bytes_limit),
        "batch_tail_start": int(batch_tail_start),
        "batch_min_start_clamped": bool(capped_min_start > raw_min_start),
    }


def _read_batch_lines(
    path: str,
    *,
    min_start: int,
    max_bytes: int | None = None,
) -> tuple[list[tuple[int, str]], dict[str, Any]]:
    target = Path(path)
    started = time.perf_counter()
    size = target.stat().st_size
    start = min(max(0, int(min_start)), int(size))
    read_limit = None if max_bytes is None or int(max_bytes) <= 0 else max(0, int(max_bytes))
    end_offset = int(size) if read_limit is None else min(int(size), int(start) + int(read_limit))
    rows: list[tuple[int, str]] = []
    with target.open("rb") as raw:
        raw.seek(start)
        while raw.tell() < end_offset:
            line_start = raw.tell()
            line = raw.readline()
            if not line:
                break
            if not line.strip():
                continue
            rows.append((int(line_start), line.decode("utf-8", errors="replace")))
    return rows, {
        "stage": "tail_open_seek_read",
        "duration_s": round(time.perf_counter() - started, 6),
        "file_size": int(size),
        "start_offset": int(start),
        "end_offset": int(end_offset),
        "max_bytes": read_limit,
        "bytes_requested": max(0, int(size) - int(start)),
        "bytes_read": max(0, int(end_offset) - int(start)),
        "line_count": len(rows),
    }


def _write_offset_states_after_success(
    *,
    path: str,
    plans: dict[str, dict[str, Any]],
    identity: dict[str, int],
    aggregate_offset_state: str = "",
) -> dict[str, Any]:
    started = time.perf_counter()
    for plan in plans.values():
        _write_offset_state(
            str(plan["offset_state"]),
            {
                "schema_version": 1,
                "path": str(path),
                "inode": identity["inode"],
                "offset": identity["size"],
                "size": identity["size"],
                "mtime_s": identity["mtime_s"],
                "updated_at": utc_now_iso(),
            },
        )
    aggregate_written = False
    if aggregate_offset_state:
        _write_offset_state(
            aggregate_offset_state,
            {
                "schema_version": 1,
                "path": str(path),
                "inode": identity["inode"],
                "offset": identity["size"],
                "size": identity["size"],
                "mtime_s": identity["mtime_s"],
                "updated_at": utc_now_iso(),
                "mode": "multi_wallet_aggregate",
                "wallets": sorted(str(wallet) for wallet in plans),
                "offset_state_count": len(plans),
            },
        )
        aggregate_written = True
    return {
        "stage": "offset_state_write",
        "duration_s": round(time.perf_counter() - started, 6),
        "offset_writes": len(plans),
        "aggregate_offset_state": aggregate_offset_state or None,
        "aggregate_offset_written": aggregate_written,
        "next_offset": identity["size"],
    }


def run_multi_wallet_merge(args: argparse.Namespace) -> dict[str, Any]:
    wallets = list(dict.fromkeys(_norm_wallet(wallet) for wallet in getattr(args, "source_wallets", []) or []))
    wallets = [wallet for wallet in wallets if wallet]
    if not wallets:
        raise SystemExit("--source-wallets must contain at least one 0x address")
    wallet_names = getattr(args, "wallet_names", {}) or {}
    offset_states = getattr(args, "offset_states", {}) or {}
    offset_states = {
        wallet: str(offset_states.get(wallet) or _default_offset_state(args.history_state, args.rtds_jsonl))
        for wallet in wallets
    }
    plans, batch_plan = _batch_line_plan(
        args.rtds_jsonl,
        wallets=wallets,
        offset_states=offset_states,
        tail_bytes=int(args.tail_bytes),
        cold_tail_bytes=int(getattr(args, "cold_tail_bytes", DEFAULT_COLD_TAIL_BYTES)),
    )
    line_rows, read_profile = _read_batch_lines(
        args.rtds_jsonl,
        min_start=int(batch_plan["min_start"]),
        max_bytes=int(batch_plan["batch_tail_bytes_limit"]),
    )
    read_profile.update({key: value for key, value in batch_plan.items() if key != "identity"})
    events_by_wallet: dict[str, deque[WalletEvent]] = {
        wallet: deque(maxlen=max(1, int(args.scan_limit))) for wallet in wallets
    }
    parse_started = time.perf_counter()
    json_decode_errors = 0
    parsed_rows = 0
    latest_processed_by_wallet: dict[str, float] = {}
    for line_start, line in line_rows:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            json_decode_errors += 1
            continue
        if not isinstance(row, dict):
            continue
        parsed_rows += 1
        wallet = _norm_wallet(row.get("source_wallet") or _raw_dict(row).get("proxyWallet"))
        if wallet not in events_by_wallet:
            continue
        plan = plans[wallet]
        if int(line_start) < int(plan["effective_start"]):
            continue
        captured_at_s = _float(row.get("captured_at_s") or row.get("received_at_s"))
        if captured_at_s is not None:
            latest_processed_by_wallet[wallet] = max(float(captured_at_s), latest_processed_by_wallet.get(wallet, 0.0))
        event = _wallet_event_from_rtds(row, source_wallet=wallet, wallet_name=str(wallet_names.get(wallet) or wallet))
        if event is not None:
            events_by_wallet[wallet].append(event)
    parse_profile = {
        "stage": "line_json_parse",
        "duration_s": round(time.perf_counter() - parse_started, 6),
        "line_count": len(line_rows),
        "scanned_rows": len(line_rows),
        "parsed_rows": parsed_rows,
        "json_decode_errors": json_decode_errors,
        "matching_events": sum(len(events) for events in events_by_wallet.values()),
    }

    merge_started = time.perf_counter()
    existing = load_json(args.history_state, default={})
    if not isinstance(existing, dict) or existing.get("kind") != "wallet_copy_history_state":
        existing = {
            "schema_version": 1,
            "kind": "wallet_copy_history_state",
            "paper_only": True,
            "live_orders_allowed": False,
            "events": [],
            "copy_intents": [],
            "wallets": [],
            "wallet_results": [],
        }
    existing_event_rows = [row for row in existing.get("events") or [] if isinstance(row, dict)]
    existing_event_ids = {str(row.get("event_id") or "") for row in existing_event_rows if str(row.get("event_id") or "")}
    selected_wallet = wallets[0] if wallets else ""
    polygon_path = str(getattr(args, "polygon_jsonl", "") or "")
    polygon_tail_bytes = int(getattr(args, "polygon_tail_bytes", 64 * 1024 * 1024) or 0)
    token_meta = _token_metadata_from_history(existing)
    polygon_by_wallet: dict[str, list[WalletEvent]] = {wallet: [] for wallet in wallets}
    polygon_profiles: list[dict[str, Any]] = []
    if selected_wallet:
        selected_polygon, selected_polygon_profile = _iter_polygon_wallet_events(
            polygon_path,
            wallets=[selected_wallet],
            wallet_names={selected_wallet: str(wallet_names.get(selected_wallet) or selected_wallet)},
            token_meta=dict(token_meta),
            tail_bytes=polygon_tail_bytes,
            max_events_per_wallet=int(args.scan_limit),
        )
        polygon_by_wallet.update(selected_polygon)
        polygon_profiles.append(selected_polygon_profile)
        token_meta = _fill_token_metadata_from_events(
            token_meta,
            [*events_by_wallet[selected_wallet], *selected_polygon.get(selected_wallet, [])],
        )
    remaining_wallets = [wallet for wallet in wallets if wallet != selected_wallet]
    if remaining_wallets:
        remaining_polygon, remaining_polygon_profile = _iter_polygon_wallet_events(
            polygon_path,
            wallets=remaining_wallets,
            wallet_names={wallet: str(wallet_names.get(wallet) or wallet) for wallet in remaining_wallets},
            token_meta=token_meta,
            tail_bytes=polygon_tail_bytes,
            max_events_per_wallet=int(args.scan_limit),
        )
        polygon_by_wallet.update(remaining_polygon)
        polygon_profiles.append(remaining_polygon_profile)
    polygon_profile = _merge_polygon_profiles(polygon_profiles)
    for wallet, events in polygon_by_wallet.items():
        for event in events:
            events_by_wallet[wallet].append(event)
    matching_by_wallet = {
        wallet: list(events)[-max(1, int(args.max_new_events)) :]
        for wallet, events in events_by_wallet.items()
    }
    new_by_wallet = {
        wallet: _events_new_to_history_with_ids(existing_event_ids, events)
        for wallet, events in matching_by_wallet.items()
    }
    all_matching_events = [event for events in matching_by_wallet.values() for event in events]
    all_new_events = [event for events in new_by_wallet.values() for event in events]
    existing_wallet_rows = {
        _norm_wallet(row.get("address"))
        for row in existing.get("wallets") or []
        if isinstance(row, dict)
    }
    should_write_history = bool(all_new_events) or any(wallet not in existing_wallet_rows for wallet in wallets)
    merged_event_rows, merged_event_count = _merge_matching_events_into_rows(existing_event_rows, all_matching_events)
    if should_write_history:
        event_rows = _retain_tail(
            sorted(merged_event_rows, key=_event_sort_key),
            int(args.history_retain_events),
        )
        wallet_rows = [
            row
            for row in existing.get("wallets") or []
            if isinstance(row, dict) and _norm_wallet(row.get("address")) not in set(wallets)
        ]
        for wallet in wallets:
            wallet_rows.append(
                {
                    "name": str(wallet_names.get(wallet) or f"rtds_{wallet[-8:]}"),
                    "address": wallet,
                    "enabled": True,
                    "market_filter": "btc_5m",
                    "asset_allowlist": ["BTC"],
                    "tags": ["rtds_activity"],
                    "notes": "Merged from RTDS realtime activity feed.",
                }
            )
        intent_rows = _retain_tail(
            sorted(
                [row for row in existing.get("copy_intents") or [] if isinstance(row, dict)],
                key=_intent_sort_key,
            ),
            int(args.history_retain_copy_intents),
        )
    else:
        event_rows = existing_event_rows
        wallet_rows = [row for row in existing.get("wallets") or [] if isinstance(row, dict)]
        intent_rows = [row for row in existing.get("copy_intents") or [] if isinstance(row, dict)]
    merge_profile = {
        "stage": "merge_dedupe",
        "duration_s": round(time.perf_counter() - merge_started, 6),
        "existing_events": len(existing_event_rows),
        "matching_events": len(all_matching_events),
        "new_events": len(all_new_events),
        "merged_events": merged_event_count,
    }
    history_write_started = time.perf_counter()
    if should_write_history:
        payload = dict(existing)
        payload.update(
            {
                "schema_version": 1,
                "kind": "wallet_copy_history_state",
                "generated_at": utc_now_iso(),
                "paper_only": True,
                "live_orders_allowed": False,
                "events": sorted(event_rows, key=_event_sort_key),
                "copy_intents": intent_rows,
                "wallets": wallet_rows,
                "wallet_results": [
                    *(row for row in existing.get("wallet_results") or [] if isinstance(row, dict)),
                    *[
                        {
                            "wallet": {"name": str(wallet_names.get(wallet) or f"rtds_{wallet[-8:]}"), "address": wallet},
                            "events": len(_wallet_event_rows(merged_event_rows, wallet)),
                            "copy_intents": None,
                            "latest_event_ts": _latest_event_ts_from_rows(_wallet_event_rows(merged_event_rows, wallet)),
                            "source": "rtds_activity_merge",
                        }
                        for wallet in wallets
                    ],
                ][-10_000:],
            }
        )
        atomic_write_json(args.history_state, payload)
        history_window_index = build_history_window_index(args.history_state, index_path=args.history_window_index)
    else:
        history_window_index = load_history_window_index(args.history_state, index_path=args.history_window_index)
    history_write_profile = {
        "stage": "history_write",
        "duration_s": round(time.perf_counter() - history_write_started, 6),
        "executed": bool(should_write_history),
        "history_state": args.history_state,
    }
    offset_write_profile = _write_offset_states_after_success(
        path=args.rtds_jsonl,
        plans=plans,
        identity=batch_plan["identity"],
        aggregate_offset_state=str(getattr(args, "aggregate_offset_state", "") or ""),
    )
    history_index_summary = {
        "path": str(args.history_window_index),
        "indexed_rows": int(history_window_index.get("indexed_rows") or 0) if isinstance(history_window_index, dict) else 0,
        "skipped_rows": int(history_window_index.get("skipped_rows") or 0) if isinstance(history_window_index, dict) else 0,
        "windows": len(history_window_index.get("windows") or {}) if isinstance(history_window_index, dict) else 0,
        "rebuilt": bool(should_write_history),
    }
    append_jsonl_many(
        args.wallet_event_log,
        [
            {
                "event": "wallet_copy_wallet_event",
                "generated_at": utc_now_iso(),
                **event.asdict(),
            }
            for event in all_new_events
        ],
    )
    summaries: dict[str, dict[str, Any]] = {}
    for idx, wallet in enumerate(wallets):
        matching_events = matching_by_wallet[wallet]
        new_events = new_by_wallet[wallet]
        wallet_rows_for_summary = _wallet_event_rows(merged_event_rows, wallet)
        latest_event_ts = _latest_event_ts_from_rows(wallet_rows_for_summary)
        latest_observed_ts = _latest_observed_ts_from_rows(wallet_rows_for_summary)
        plan = plans[wallet]
        ingest_state = {
            "mode": plan["mode"],
            "offset_state": plan["offset_state"],
            "previous_offset": plan["previous_offset"],
            "next_offset": plan["next_offset"],
            "file_size": plan["file_size"],
            "file_mtime_s": plan["file_mtime_s"],
            "fallback_tail_bytes": plan["fallback_tail_bytes"],
            "latest_processed_captured_at_s": latest_processed_by_wallet.get(wallet),
            "rtds_catchup_lag_s": (
                round(max(0.0, time.time() - latest_processed_by_wallet[wallet]), 6)
                if wallet in latest_processed_by_wallet
                else 0.0
            ),
        }
        watermark = _write_observation_watermark(
            args.watermark_state,
            source_wallet=wallet,
            rtds_jsonl=args.rtds_jsonl,
            offset_state=str(plan["offset_state"]),
            ingest_state=ingest_state,
            latest_event_ts=latest_event_ts,
            latest_observed_ts=latest_observed_ts,
            retained_matching_rows=len(matching_events),
            new_matching_events=len(new_events),
        )
        profile = {
            "tail_open_seek_read": read_profile if idx == 0 else {**read_profile, "duration_s": 0.0, "bytes_requested": 0, "bytes_read": 0},
            "line_json_parse": parse_profile if idx == 0 else {**parse_profile, "duration_s": 0.0, "line_count": 0, "scanned_rows": 0, "parsed_rows": 0, "json_decode_errors": 0, "matching_events": 0},
            "polygon_ws_premerge_parse": polygon_profile if idx == 0 else {**polygon_profile, "duration_s": 0.0, "line_count": 0, "parsed_rows": 0, "json_decode_errors": 0, "matching_events": 0},
            "merge_dedupe": merge_profile if idx == 0 else {**merge_profile, "duration_s": 0.0, "existing_events": 0, "matching_events": 0, "new_events": 0, "merged_events": 0},
            "history_write": history_write_profile if idx == 0 else {**history_write_profile, "duration_s": 0.0},
            "offset_state_write": offset_write_profile if idx == 0 else {**offset_write_profile, "duration_s": 0.0, "offset_writes": 0},
        }
        summaries[wallet] = {
            "status": "PASS" if new_events else "ANALYZE",
            "history_state": args.history_state,
            "source_wallet": wallet,
            "rtds_rows": sum(1 for line_start, _line in line_rows if line_start >= int(plan["effective_start"])),
            "tail_bytes": int(args.tail_bytes),
            "cold_tail_bytes": int(getattr(args, "cold_tail_bytes", DEFAULT_COLD_TAIL_BYTES)),
            "offset_state": str(plan["offset_state"]),
            "ingest_mode": plan["mode"],
            "previous_offset": plan["previous_offset"],
            "next_offset": plan["next_offset"],
            "fallback_tail_bytes": plan["fallback_tail_bytes"],
            "premerge_substage_profile": profile,
            "retained_matching_rows": len(matching_events),
            "new_matching_events": len(new_events),
            "new_events_delta": [event.asdict() for event in new_events],
            "matching_events_delta": [event.asdict() for event in matching_events],
            "deduped_matching_events": max(0, len(matching_events) - len(new_events)),
            "history_write_skipped": not should_write_history,
            "latest_event_ts": latest_event_ts,
            "latest_observed_ts": latest_observed_ts,
            "observation_watermark": watermark,
            "latest_processed_captured_at_s": latest_processed_by_wallet.get(wallet),
            "rtds_catchup_lag_s": ingest_state["rtds_catchup_lag_s"],
            "merged_events": len(event_rows),
            "history_window_index": history_index_summary,
            "polygon_ws_premerge": {
                "matching_events": len(polygon_by_wallet.get(wallet, [])),
                "profile": polygon_profile if idx == 0 else {},
                "path": str(getattr(args, "polygon_jsonl", "") or ""),
            },
            "paper_only": True,
            "live_orders_allowed": False,
        }
    return {
        "status": "PASS",
        "schema_version": 1,
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "wallet_summaries": summaries,
        "wallets_refreshed": len(wallets),
        "new_matching_events": sum(int(row.get("new_matching_events") or 0) for row in summaries.values()),
        "new_events_delta": [
            event.asdict()
            for wallet in wallets
            for event in new_by_wallet.get(wallet, [])
        ][-max(1, int(args.max_new_events)) :],
        "matching_events_delta": [
            event.asdict()
            for wallet in wallets
            for event in matching_by_wallet.get(wallet, [])
        ][-max(1, int(args.max_new_events)) :],
        "retained_matching_rows": sum(int(row.get("retained_matching_rows") or 0) for row in summaries.values()),
        "history_write_executed": bool(should_write_history),
        "premerge_substage_profile": {
            "tail_open_seek_read": read_profile,
            "line_json_parse": parse_profile,
            "polygon_ws_premerge_parse": polygon_profile,
            "merge_dedupe": merge_profile,
            "history_write": history_write_profile,
            "offset_state_write": offset_write_profile,
        },
        "paper_only": True,
        "live_orders_allowed": False,
    }


def run_merge(args: argparse.Namespace) -> dict[str, Any]:
    source_wallet = _norm_wallet(args.source_wallet)
    if not source_wallet:
        raise SystemExit("--source-wallet must be a 0x address")
    wallet_name = args.wallet_name or f"rtds_{source_wallet[-8:]}"
    offset_state = args.offset_state or _default_offset_state(args.history_state, args.rtds_jsonl)
    scanned_rows, matching_events, ingest_state = _iter_recent_wallet_events(
        args.rtds_jsonl,
        source_wallet=source_wallet,
        wallet_name=wallet_name,
        limit=int(args.scan_limit),
        tail_bytes=int(args.tail_bytes),
        cold_tail_bytes=int(getattr(args, "cold_tail_bytes", DEFAULT_COLD_TAIL_BYTES)),
        offset_state=offset_state,
    )
    matching_events = matching_events[-max(1, int(args.max_new_events)) :]

    merge_started = time.perf_counter()
    existing = load_json(args.history_state, default={})
    if not isinstance(existing, dict) or existing.get("kind") != "wallet_copy_history_state":
        existing = {
            "schema_version": 1,
            "kind": "wallet_copy_history_state",
            "paper_only": True,
            "live_orders_allowed": False,
            "events": [],
            "copy_intents": [],
            "wallets": [],
            "wallet_results": [],
        }
    existing_event_rows = [row for row in existing.get("events") or [] if isinstance(row, dict)]
    existing_event_ids = {str(row.get("event_id") or "") for row in existing_event_rows if str(row.get("event_id") or "")}
    token_meta = _fill_token_metadata_from_events(_token_metadata_from_history(existing), matching_events)
    polygon_by_wallet, polygon_profile = _iter_polygon_wallet_events(
        str(getattr(args, "polygon_jsonl", "") or ""),
        wallets=[source_wallet],
        wallet_names={source_wallet: wallet_name},
        token_meta=token_meta,
        tail_bytes=int(getattr(args, "polygon_tail_bytes", 64 * 1024 * 1024) or 0),
        max_events_per_wallet=int(args.scan_limit),
    )
    if polygon_by_wallet.get(source_wallet):
        matching_events = unique_wallet_events([*matching_events, *polygon_by_wallet[source_wallet]])
    new_events = _events_new_to_history_with_ids(existing_event_ids, matching_events)
    existing_wallet_rows = {
        _norm_wallet(row.get("address"))
        for row in existing.get("wallets") or []
        if isinstance(row, dict)
    }
    # The history state is ~100MB; rewriting it every guard cycle dominates the
    # live intent-build latency, so skip the rewrite when nothing changed.
    skip_history_write = (
        not new_events
        and existing.get("kind") == "wallet_copy_history_state"
        and source_wallet in existing_wallet_rows
    )
    merged_event_rows, merged_event_count = _merge_matching_events_into_rows(existing_event_rows, matching_events)
    if skip_history_write:
        event_rows = existing_event_rows
        wallet_rows = [row for row in existing.get("wallets") or [] if isinstance(row, dict)]
        intent_rows = [row for row in existing.get("copy_intents") or [] if isinstance(row, dict)]
    else:
        event_rows = _retain_tail(sorted(merged_event_rows, key=_event_sort_key), int(args.history_retain_events))
        wallet_rows = [
            row
            for row in existing.get("wallets") or []
            if isinstance(row, dict) and _norm_wallet(row.get("address")) != source_wallet
        ]
        wallet_rows.append(
            {
                "name": wallet_name,
                "address": source_wallet,
                "enabled": True,
                "market_filter": "btc_5m",
                "asset_allowlist": ["BTC"],
                "tags": ["rtds_activity"],
                "notes": "Merged from RTDS realtime activity feed.",
            }
        )
        intent_rows = _retain_tail(
            sorted(
                [row for row in existing.get("copy_intents") or [] if isinstance(row, dict)],
                key=_intent_sort_key,
            ),
            int(args.history_retain_copy_intents),
        )
    merge_dedupe_profile = {
        "stage": "merge_dedupe",
        "duration_s": round(time.perf_counter() - merge_started, 6),
        "existing_events": len(existing_event_rows),
        "matching_events": len(matching_events),
        "new_events": len(new_events),
        "merged_events": merged_event_count,
    }
    premerge_profile = ingest_state.setdefault("premerge_substage_profile", {})
    premerge_profile["polygon_ws_premerge_parse"] = polygon_profile
    premerge_profile["merge_dedupe"] = merge_dedupe_profile
    wallet_rows_for_summary = _wallet_event_rows(merged_event_rows, source_wallet)
    latest_event_ts = _latest_event_ts_from_rows(wallet_rows_for_summary)
    latest_observed_ts = _latest_observed_ts_from_rows(wallet_rows_for_summary)
    watermark = _write_observation_watermark(
        args.watermark_state,
        source_wallet=source_wallet,
        rtds_jsonl=args.rtds_jsonl,
        offset_state=offset_state,
        ingest_state=ingest_state,
        latest_event_ts=latest_event_ts,
        latest_observed_ts=latest_observed_ts,
        retained_matching_rows=len(matching_events),
        new_matching_events=len(new_events),
    )
    if not skip_history_write:
        payload = dict(existing)
        payload.update(
            {
                "schema_version": 1,
                "kind": "wallet_copy_history_state",
                "generated_at": utc_now_iso(),
                "paper_only": True,
                "live_orders_allowed": False,
                "events": sorted(event_rows, key=_event_sort_key),
                "copy_intents": intent_rows,
                "wallets": wallet_rows,
                "wallet_results": [
                    *(row for row in existing.get("wallet_results") or [] if isinstance(row, dict)),
                    {
                        "wallet": {"name": wallet_name, "address": source_wallet},
                        "events": len(wallet_rows_for_summary),
                        "copy_intents": None,
                        "latest_event_ts": latest_event_ts,
                        "source": "rtds_activity_merge",
                    },
                ][-10_000:],
                "rtds_ingest": {
                    "source_wallet": source_wallet,
                    "wallet_name": wallet_name,
                    "rtds_jsonl": args.rtds_jsonl,
                    "offset_state": offset_state,
                    "ingest_mode": ingest_state.get("mode"),
                    "previous_offset": ingest_state.get("previous_offset"),
                    "next_offset": ingest_state.get("next_offset"),
                    "scanned_rows": scanned_rows,
                    "tail_bytes": int(args.tail_bytes),
                    "cold_tail_bytes": int(getattr(args, "cold_tail_bytes", DEFAULT_COLD_TAIL_BYTES)),
                    "retained_matching_rows": len(matching_events),
                    "new_matching_events": len(new_events),
                    "deduped_matching_events": max(0, len(matching_events) - len(new_events)),
                    "merged_events": len(event_rows),
                    "latest_event_ts": latest_event_ts,
                    "latest_observed_ts": latest_observed_ts,
                    "latest_processed_captured_at_s": ingest_state.get("latest_processed_captured_at_s"),
                    "rtds_catchup_lag_s": ingest_state.get("rtds_catchup_lag_s"),
                    "premerge_substage_profile": ingest_state.get("premerge_substage_profile")
                    if isinstance(ingest_state.get("premerge_substage_profile"), dict)
                    else {},
                    "polygon_ws_premerge": {
                        "matching_events": len(polygon_by_wallet.get(source_wallet, [])),
                        "profile": polygon_profile,
                        "path": str(getattr(args, "polygon_jsonl", "") or ""),
                    },
                },
            }
        )
        atomic_write_json(args.history_state, payload)
        history_window_index = build_history_window_index(args.history_state, index_path=args.history_window_index)
    else:
        history_window_index = load_history_window_index(args.history_state, index_path=args.history_window_index)
    history_index_summary = {
        "path": str(args.history_window_index),
        "indexed_rows": int(history_window_index.get("indexed_rows") or 0) if isinstance(history_window_index, dict) else 0,
        "skipped_rows": int(history_window_index.get("skipped_rows") or 0) if isinstance(history_window_index, dict) else 0,
        "windows": len(history_window_index.get("windows") or {}) if isinstance(history_window_index, dict) else 0,
        "rebuilt": not skip_history_write,
    }
    append_jsonl_many(
        args.wallet_event_log,
        [
            {
                "event": "wallet_copy_wallet_event",
                "generated_at": utc_now_iso(),
                **event.asdict(),
            }
            for event in new_events
        ],
    )
    summary = {
        "status": "PASS" if new_events else "ANALYZE",
        "history_state": args.history_state,
        "source_wallet": source_wallet,
        "rtds_rows": scanned_rows,
        "tail_bytes": int(args.tail_bytes),
        "cold_tail_bytes": int(getattr(args, "cold_tail_bytes", DEFAULT_COLD_TAIL_BYTES)),
        "offset_state": offset_state,
        "ingest_mode": ingest_state.get("mode"),
        "previous_offset": ingest_state.get("previous_offset"),
        "next_offset": ingest_state.get("next_offset"),
        "fallback_tail_bytes": ingest_state.get("fallback_tail_bytes"),
        "retained_matching_rows": len(matching_events),
        "new_matching_events": len(new_events),
        "deduped_matching_events": max(0, len(matching_events) - len(new_events)),
        "history_write_skipped": skip_history_write,
        "latest_event_ts": latest_event_ts,
        "latest_observed_ts": latest_observed_ts,
        "observation_watermark": watermark,
        "latest_processed_captured_at_s": ingest_state.get("latest_processed_captured_at_s"),
        "rtds_catchup_lag_s": ingest_state.get("rtds_catchup_lag_s"),
        "premerge_substage_profile": ingest_state.get("premerge_substage_profile")
        if isinstance(ingest_state.get("premerge_substage_profile"), dict)
        else {},
        "merged_events": len(event_rows),
        "history_window_index": history_index_summary,
        "polygon_ws_premerge": {
            "matching_events": len(polygon_by_wallet.get(source_wallet, [])),
            "profile": polygon_profile,
            "path": str(getattr(args, "polygon_jsonl", "") or ""),
        },
        "paper_only": True,
        "live_orders_allowed": False,
    }
    return summary


def main() -> int:
    summary = run_merge(parse_args())
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
