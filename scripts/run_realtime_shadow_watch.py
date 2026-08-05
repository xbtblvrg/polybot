#!/usr/bin/env python3
"""Paper-only realtime shadow scorer for watch-list wallets."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_WATCH_STATE = "data/research/wallet_copy_realtime_shadow_watch_state.json"
DEFAULT_RTDS = DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_OUTPUT = "data/research/wallet_copy_realtime_shadow_watch_scored_state.json"
DEFAULT_EVENTS = "data/research/wallet_copy_realtime_shadow_watch_events.jsonl"
DEFAULT_OFFSET = "data/research/wallet_copy_realtime_shadow_watch_offset_state.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watch-state", default=DEFAULT_WATCH_STATE)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--event-log", default=DEFAULT_EVENTS)
    parser.add_argument("--offset-state", default=DEFAULT_OFFSET)
    parser.add_argument("--tail-bytes", type=int, default=8_000_000)
    parser.add_argument("--max-events", type=int, default=200)
    parser.add_argument("--book-timeout-s", type=float, default=1.5)
    parser.add_argument("--copy-size-usd", type=float, default=1.0)
    parser.add_argument("--strict-slippage-bps", type=float, default=250.0)
    parser.add_argument("--drift-buffer-price", type=float, default=0.05)
    parser.add_argument(
        "--max-source-age-s",
        type=float,
        default=5.0,
        help="Only score source events observed this many seconds ago; 0 disables the freshness gate.",
    )
    parser.add_argument("--buy-only", action="store_true", default=True)
    parser.add_argument("--market-slug-prefix", default="btc-updown-5m-")
    parser.add_argument("--iterations", type=int, default=1, help="Loop count; 0 runs forever.")
    parser.add_argument("--sleep-s", type=float, default=2.0)
    parser.add_argument("--max-retained-events-per-wallet", type=int, default=200)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _iter_values(payload: Any):
    if isinstance(payload, dict):
        for value in payload.values():
            yield value
            yield from _iter_values(value)
    elif isinstance(payload, list):
        for value in payload:
            yield value
            yield from _iter_values(value)


def _find_key(payload: Any, names: set[str]) -> Any:
    if isinstance(payload, dict):
        for key, value in payload.items():
            if str(key).lower() in names:
                return value
        for value in payload.values():
            found = _find_key(value, names)
            if found not in (None, ""):
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _find_key(value, names)
            if found not in (None, ""):
                return found
    return None


def _event_wallet(payload: dict[str, Any], watched: set[str]) -> str:
    for value in _iter_values(payload):
        wallet = _norm_wallet(value)
        if wallet in watched:
            return wallet
    return ""


def _event_ts(payload: dict[str, Any]) -> float | None:
    value = _find_key(payload, {"event_ts", "timestamp", "time", "createdat", "created_at", "observed_ts"})
    if isinstance(value, str) and value.isdigit():
        raw = float(value)
    else:
        raw = num(value, 0.0)
    if raw <= 0:
        return None
    return raw / 1000.0 if raw > 10_000_000_000 else raw


def _event_received_at_s(payload: dict[str, Any]) -> float | None:
    value = _find_key(payload, {"received_at_s", "captured_at_s", "observed_at_s"})
    raw = num(value, 0.0)
    if raw <= 0:
        return None
    return raw / 1000.0 if raw > 10_000_000_000 else raw


def _event_side(payload: dict[str, Any]) -> str:
    return str(_find_key(payload, {"side", "action"}) or "").upper()


def _event_market_slug(payload: dict[str, Any]) -> str:
    return str(_find_key(payload, {"market_slug", "slug", "eventslug", "event_slug"}) or "")


def _event_id(payload: dict[str, Any], line: str) -> str:
    value = _find_key(payload, {"source_event_id", "event_id", "id", "transaction_hash", "hash"})
    if value:
        return str(value)
    return "shadow_evt_" + hashlib.sha1(line.encode("utf-8", errors="ignore")).hexdigest()[:20]


def _book_ts_s_near_source(value: Any, source_ts: float | None, fetch_completed_s: float) -> float:
    raw = num(value, 0.0)
    if raw <= 0:
        return float(fetch_completed_s)
    candidates = [raw]
    if raw > 10_000_000_000:
        candidates.append(raw / 1000.0)
        candidates.append(raw / 1_000_000.0)
    if source_ts and source_ts > 0:
        return float(min(candidates, key=lambda item: abs(float(item) - float(source_ts))))
    return float(raw / 1000.0 if raw > 10_000_000_000 else raw)


def _read_tail_lines(path: Path, tail_bytes: int) -> list[str]:
    if not path.exists():
        return []
    size = path.stat().st_size
    with path.open("rb") as handle:
        handle.seek(max(0, size - max(1, int(tail_bytes))))
        data = handle.read()
    return data.decode("utf-8", errors="ignore").splitlines()


def _read_incremental_lines(path: Path, offset_path: Path, tail_bytes: int) -> tuple[list[str], dict[str, Any]]:
    previous = load_json(offset_path, default={})
    if not path.exists():
        state = {
            "schema_version": 1,
            "kind": "wallet_copy_realtime_shadow_watch_offset",
            "path": str(path),
            "offset": 0,
            "size": 0,
            "status": "MISSING_RTDS_FILE",
            "updated_at": utc_now_iso(),
        }
        atomic_write_json(offset_path, state)
        return [], state
    stat = path.stat()
    size = stat.st_size
    previous_inode = int(num(previous.get("inode"), 0.0)) if isinstance(previous, dict) else 0
    same_file = previous.get("path") == str(path) and (previous_inode <= 0 or previous_inode == int(stat.st_ino))
    prior_offset = int(num(previous.get("offset"), 0.0)) if same_file else 0
    if prior_offset <= 0 or prior_offset > size:
        prior_offset = max(0, size - max(1, int(tail_bytes)))
    with path.open("rb") as handle:
        handle.seek(prior_offset)
        data = handle.read()
        new_offset = handle.tell()
    complete = data
    if data and not data.endswith(b"\n"):
        last_newline = data.rfind(b"\n")
        if last_newline >= 0:
            complete = data[: last_newline + 1]
            new_offset = prior_offset + last_newline + 1
        else:
            complete = b""
            new_offset = prior_offset
    size = max(int(path.stat().st_size), int(new_offset))
    state = {
        "schema_version": 1,
        "kind": "wallet_copy_realtime_shadow_watch_offset",
        "path": str(path),
        "inode": int(stat.st_ino),
        "offset": int(new_offset),
        "previous_offset": int(prior_offset),
        "size": int(size),
        "bytes_read": int(max(0, new_offset - prior_offset)),
        "partial_trailing_bytes": int(prior_offset + len(data) - new_offset),
        "status": "OK",
        "updated_at": utc_now_iso(),
    }
    atomic_write_json(offset_path, state)
    return complete.decode("utf-8", errors="ignore").splitlines(), state


def _resident_snapshot(
    *,
    args: argparse.Namespace,
    started_at: str,
    iteration: int,
    offset_state: dict[str, Any] | None = None,
    status: str = "RUNNING",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "wallet_copy_realtime_shadow_watch_resident_scorer",
        "status": status,
        "pid": os.getpid(),
        "started_at": started_at,
        "last_heartbeat_at": utc_now_iso(),
        "iteration": int(iteration),
        "iterations": int(args.iterations),
        "sleep_s": float(args.sleep_s),
        "rtds_jsonl": str(args.rtds_jsonl),
        "offset_state": str(args.offset_state),
        "offset": offset_state or {},
        "paper_only": True,
        "live_orders_allowed": False,
        "orders_submitted": 0,
    }


def _write_resident_watch_state(args: argparse.Namespace, snapshot: dict[str, Any]) -> None:
    watch_state = load_json(args.watch_state, default={})
    if not isinstance(watch_state, dict):
        watch_state = {}
    watch_state["resident_scorer"] = snapshot
    summary = watch_state.get("summary") if isinstance(watch_state.get("summary"), dict) else {}
    summary["resident_scorer_pid"] = int(snapshot.get("pid") or 0)
    summary["resident_scorer_status"] = snapshot.get("status")
    summary["resident_scorer_iteration"] = int(snapshot.get("iteration") or 0)
    watch_state["summary"] = summary
    atomic_write_json(args.watch_state, watch_state)


def _is_realtime_taker_evidence(event: dict[str, Any]) -> bool:
    return (
        event.get("within_copy_latency_window") is True
        and event.get("book_age_s") is not None
        and event.get("taker_fillable") is True
        and bool(_norm_wallet(event.get("wallet")))
        and bool(str(event.get("market_slug") or ""))
    )


def _seed_cumulative_windows(prior: dict[str, Any], prior_events: list[Any]) -> dict[str, set[str]]:
    seeded: dict[str, set[str]] = {}
    summary = prior.get("summary") if isinstance(prior.get("summary"), dict) else {}
    raw = summary.get("realtime_taker_market_windows_by_wallet")
    if isinstance(raw, dict):
        for wallet, windows in raw.items():
            normalized = _norm_wallet(wallet)
            if not normalized or not isinstance(windows, list):
                continue
            seeded[normalized] = {str(window) for window in windows if str(window or "")}
    for event in prior_events:
        if not isinstance(event, dict) or not _is_realtime_taker_evidence(event):
            continue
        wallet = _norm_wallet(event.get("wallet"))
        seeded.setdefault(wallet, set()).add(str(event.get("market_slug")))
    return seeded


def _cap_events_per_wallet(events: list[dict[str, Any]], max_per_wallet: int) -> tuple[list[dict[str, Any]], int]:
    if max_per_wallet <= 0:
        return events, 0
    kept: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    evicted = 0
    for event in events:
        wallet = _norm_wallet(event.get("wallet")) or "unknown"
        if counts[wallet] >= max_per_wallet:
            evicted += 1
            continue
        counts[wallet] += 1
        kept.append(event)
    return kept, evicted


def _event_summary_counts(events: list[dict[str, Any]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for event in events:
        if not isinstance(event, dict) or event.get("status") != "SCORED":
            continue
        counts["scored_events"] += 1
        if event.get("book_age_s") is not None:
            counts["events_with_book_age_s"] += 1
        if event.get("within_copy_latency_window") is True:
            counts["fresh_scored_events"] += 1
            counts["within_copy_latency_window_events"] += 1
        elif event.get("within_copy_latency_window") is False:
            counts["stale_scored_events"] += 1
        if _is_realtime_taker_evidence(event):
            counts["promotion_eligible_realtime_taker_events"] += 1
    return counts


def _seed_cumulative_counts(prior: dict[str, Any], prior_events: list[dict[str, Any]]) -> Counter[str]:
    summary = prior.get("summary") if isinstance(prior.get("summary"), dict) else {}
    keys = (
        "scored_events",
        "fresh_scored_events",
        "stale_scored_events",
        "within_copy_latency_window_events",
        "events_with_book_age_s",
        "promotion_eligible_realtime_taker_events",
    )
    if any(f"cumulative_{key}" in summary for key in keys):
        return Counter({key: int(summary.get(f"cumulative_{key}") or summary.get(key) or 0) for key in keys})
    return _event_summary_counts(prior_events)


def _score_event(
    payload: dict[str, Any],
    *,
    wallet: str,
    source_line: str,
    client: CLOBMarketClient,
    args: argparse.Namespace,
) -> dict[str, Any]:
    event_ts = _event_ts(payload)
    received_at_s = _event_received_at_s(payload)
    token_id = str(_find_key(payload, {"token_id", "asset", "asset_id", "assetid"}) or "")
    source_price = num(_find_key(payload, {"price", "source_price", "limit_price"}), 0.0)
    fetch_started_s = time.time()
    receipt_to_fetch_latency_ms = (
        round(max(0.0, fetch_started_s - received_at_s) * 1000.0, 3) if received_at_s else None
    )
    latency_basis_s = (
        max(0.0, fetch_started_s - received_at_s)
        if received_at_s
        else max(0.0, fetch_started_s - event_ts) if event_ts else None
    )
    base = {
        "schema_version": 1,
        "kind": "wallet_copy_realtime_shadow_watch_event",
        "flow_stage": "PROMOTE/LEARN/OBSERVE",
        "paper_only": True,
        "live_orders_allowed": False,
        "orders_submitted": 0,
        "wallet": wallet,
        "event_id": _event_id(payload, source_line),
        "source_ts": event_ts,
        "received_at_s": received_at_s,
        "observed_at": utc_now_iso(),
        "book_ts": None,
        "book_age_s": None,
        "book_fetch_started_at_s": round(fetch_started_s, 6),
        "book_fetch_completed_at_s": None,
        "receipt_to_fetch_latency_ms": receipt_to_fetch_latency_ms,
        "source_age_s": round(max(0.0, fetch_started_s - event_ts), 6) if event_ts else None,
        "within_copy_latency_window": bool(latency_basis_s <= 5.0) if latency_basis_s is not None else None,
        "taker_fillable": False,
        "parity_fillable": False,
        "needed_bps": None,
        "token_id": token_id,
        "source_price": source_price or None,
        "side": _event_side(payload) or None,
        "market_slug": _event_market_slug(payload) or None,
        "drip_tranche_usd": float(args.copy_size_usd),
        "drift_buffer_price": float(args.drift_buffer_price),
    }
    if not token_id or source_price <= 0:
        return {**base, "status": "NEEDS_BOOK_FETCH_INPUTS", "reason": "missing_token_or_source_price"}
    try:
        book = client.get_book(token_id)
        fetch_completed_s = time.time()
        route_report = client.last_route_report
        book_ts = _book_ts_s_near_source(book.get("timestamp"), event_ts, fetch_completed_s)
        strict = CLOBMarketClient.summarize_book(
            book,
            copy_size_usd=float(args.copy_size_usd),
            source_price=float(source_price),
            max_slippage_bps=float(args.strict_slippage_bps),
        )
        parity_bps = max(0.0, (((float(source_price) + float(args.drift_buffer_price)) / float(source_price)) - 1.0) * 10000.0)
        parity = CLOBMarketClient.summarize_book(
            book,
            copy_size_usd=float(args.copy_size_usd),
            source_price=float(source_price),
            max_slippage_bps=parity_bps,
        )
        best_ask = num(strict.get("best_ask"), 0.0)
        needed_bps = round(max(0.0, ((best_ask / float(source_price)) - 1.0) * 10000.0), 6) if best_ask > 0 else None
        return {
            **base,
            "status": "SCORED",
            "book_fetch_completed_at_s": round(fetch_completed_s, 6),
            "book_ts": book_ts,
            "book_age_s": round(max(0.0, book_ts - float(event_ts)), 6) if event_ts else None,
            "taker_fillable": str(strict.get("instant_fill_status")) == "PASS",
            "parity_fillable": str(parity.get("instant_fill_status")) == "PASS",
            "needed_bps": needed_bps,
            "parity_limit_price": round(min(0.99, float(source_price) + float(args.drift_buffer_price)), 6),
            "strict": strict,
            "parity": parity,
            "route_report": route_report,
        }
    except Exception as exc:  # noqa: BLE001 - persisted as paper-only evidence.
        return {**base, "status": "BOOK_FETCH_ERROR", "error": str(exc)[:500]}


def build_scored_state(
    *,
    watch_state: dict[str, Any],
    rtds_lines: list[str],
    args: argparse.Namespace,
) -> dict[str, Any]:
    watched = {_norm_wallet(row.get("wallet")) for row in watch_state.get("wallets") or [] if isinstance(row, dict)}
    passive = {_norm_wallet(row.get("wallet")) for row in watch_state.get("passive_wallets") or [] if isinstance(row, dict)}
    watched = {wallet for wallet in watched if wallet}
    passive = {wallet for wallet in passive if wallet}
    all_wallets = watched | passive
    prior = load_json(args.output, default={})
    prior_events = prior.get("events") if isinstance(prior.get("events"), list) else []
    prior_events = [row for row in prior_events if isinstance(row, dict)]
    cumulative_windows = _seed_cumulative_windows(prior, prior_events)
    cumulative_counts = _seed_cumulative_counts(prior, prior_events)
    seen = {str(row.get("event_id")) for row in prior_events if isinstance(row, dict)}
    client = CLOBMarketClient(timeout_s=float(args.book_timeout_s), retries=1)
    new_events: list[dict[str, Any]] = []
    diagnostics: Counter[str] = Counter()
    now_s = float(getattr(args, "now_s", 0.0) or time.time())
    for line in reversed(rtds_lines):
        if len(new_events) >= int(args.max_events):
            break
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            diagnostics["json_decode_error"] += 1
            continue
        if not isinstance(payload, dict):
            diagnostics["non_object_row"] += 1
            continue
        if payload.get("event") and payload.get("event") != "rtds_trade_event":
            diagnostics["non_trade_event_row"] += 1
            continue
        wallet = _event_wallet(payload, all_wallets)
        if not wallet:
            diagnostics["unwatched_wallet"] += 1
            continue
        side = _event_side(payload)
        if bool(getattr(args, "buy_only", True)) and side != "BUY":
            diagnostics["non_buy_event"] += 1
            continue
        market_slug = _event_market_slug(payload)
        prefix = str(getattr(args, "market_slug_prefix", "") or "")
        if prefix and not market_slug.startswith(prefix):
            diagnostics["market_slug_prefix_mismatch"] += 1
            continue
        source_ref_s = _event_received_at_s(payload) or _event_ts(payload)
        max_source_age_s = float(getattr(args, "max_source_age_s", 0.0) or 0.0)
        if max_source_age_s > 0:
            if source_ref_s is None:
                diagnostics["source_time_missing"] += 1
                continue
            if max(0.0, now_s - float(source_ref_s)) > max_source_age_s:
                diagnostics["source_age_gt_cap"] += 1
                continue
        event_id = _event_id(payload, line)
        if event_id in seen:
            diagnostics["duplicate_event"] += 1
            continue
        scored = _score_event(payload, wallet=wallet, source_line=line, client=client, args=args)
        scored["watch_role"] = "primary" if wallet in watched else "passive"
        new_events.append(scored)
        seen.add(event_id)
    for event in new_events:
        if _is_realtime_taker_evidence(event):
            wallet = _norm_wallet(event.get("wallet"))
            cumulative_windows.setdefault(wallet, set()).add(str(event.get("market_slug")))
    cumulative_counts.update(_event_summary_counts(new_events))
    uncapped_events = (new_events + prior_events)[:5000]
    max_retained_per_wallet = int(getattr(args, "max_retained_events_per_wallet", 200) or 0)
    events, evicted_events = _cap_events_per_wallet(uncapped_events, max_retained_per_wallet)
    scored_events = [row for row in events if isinstance(row, dict) and row.get("status") == "SCORED"]
    retained_counts = _event_summary_counts(events)
    cumulative_windows_json = {wallet: sorted(windows) for wallet, windows in sorted(cumulative_windows.items())}
    cumulative_window_counts = {wallet: len(windows) for wallet, windows in cumulative_windows_json.items()}
    return {
        "schema_version": 1,
        "kind": "wallet_copy_realtime_shadow_watch_scored_state",
        "flow_stage": "PROMOTE/LEARN/OBSERVE",
        "paper_only": True,
        "live_orders_allowed": False,
        "orders_submitted": 0,
        "generated_at": utc_now_iso(),
        "status": "ARMED",
        "summary": {
            "registered_wallets": len(watched),
            "passive_wallets": len(passive),
            "events_seen": int(cumulative_counts.get("scored_events") or 0),
            "new_events": len(new_events),
            "scored_events": int(cumulative_counts.get("scored_events") or 0),
            "fresh_scored_events": int(cumulative_counts.get("fresh_scored_events") or 0),
            "stale_scored_events": int(cumulative_counts.get("stale_scored_events") or 0),
            "within_copy_latency_window_events": int(cumulative_counts.get("within_copy_latency_window_events") or 0),
            "events_with_book_age_s": int(cumulative_counts.get("events_with_book_age_s") or 0),
            "promotion_eligible_realtime_taker_events": int(
                cumulative_counts.get("promotion_eligible_realtime_taker_events") or 0
            ),
            "cumulative_scored_events": int(cumulative_counts.get("scored_events") or 0),
            "cumulative_fresh_scored_events": int(cumulative_counts.get("fresh_scored_events") or 0),
            "cumulative_stale_scored_events": int(cumulative_counts.get("stale_scored_events") or 0),
            "cumulative_within_copy_latency_window_events": int(
                cumulative_counts.get("within_copy_latency_window_events") or 0
            ),
            "cumulative_events_with_book_age_s": int(cumulative_counts.get("events_with_book_age_s") or 0),
            "cumulative_promotion_eligible_realtime_taker_events": int(
                cumulative_counts.get("promotion_eligible_realtime_taker_events") or 0
            ),
            "promotion_unit": "distinct_market_window",
            "promotion_ready_window_bar": 5,
            "realtime_taker_market_windows_by_wallet": cumulative_windows_json,
            "realtime_taker_distinct_market_windows_by_wallet": cumulative_window_counts,
            "realtime_taker_wallets_ge_5_windows": [
                wallet for wallet, count in sorted(cumulative_window_counts.items()) if count >= 5
            ],
            "retained_events": len(events),
            "retained_scored_events": int(retained_counts.get("scored_events") or 0),
            "retained_fresh_scored_events": int(retained_counts.get("fresh_scored_events") or 0),
            "retained_promotion_eligible_realtime_taker_events": int(
                retained_counts.get("promotion_eligible_realtime_taker_events") or 0
            ),
            "uncapped_events": len(uncapped_events),
            "evicted_events": evicted_events,
            "max_retained_events_per_wallet": max_retained_per_wallet,
            "freshness_gate_s": float(getattr(args, "max_source_age_s", 0.0) or 0.0),
            "orders_submitted": 0,
        },
        "diagnostics": dict(sorted(diagnostics.items())),
        "events": events,
        "next": "run repeatedly from heartbeat/automation; any watched source buy must get book_age_s or a named fetch/input reason",
    }


def run_resident_loop(args: argparse.Namespace) -> int:
    started_at = utc_now_iso()
    iteration = 0
    Path(args.event_log).parent.mkdir(parents=True, exist_ok=True)
    while True:
        rtds_lines, offset_state = _read_incremental_lines(
            Path(args.rtds_jsonl),
            Path(args.offset_state),
            int(args.tail_bytes),
        )
        resident = _resident_snapshot(args=args, started_at=started_at, iteration=iteration, offset_state=offset_state)
        _write_resident_watch_state(args, resident)
        state = build_scored_state(
            watch_state=load_json(args.watch_state, default={}),
            rtds_lines=rtds_lines,
            args=args,
        )
        state["resident_scorer"] = resident
        state["summary"]["resident_pid"] = os.getpid()
        state["summary"]["resident_iteration"] = iteration
        atomic_write_json(args.output, state)
        with Path(args.event_log).open("a", encoding="utf-8") as handle:
            for event in state.get("events", [])[: int(state.get("summary", {}).get("new_events") or 0)]:
                handle.write(json.dumps(event, sort_keys=True) + "\n")
        print(json.dumps(state["summary"], indent=2, sort_keys=True), flush=True)
        iteration += 1
        if int(args.iterations) > 0 and iteration >= int(args.iterations):
            final_resident = _resident_snapshot(
                args=args,
                started_at=started_at,
                iteration=iteration,
                offset_state=offset_state,
                status="EXITED",
            )
            _write_resident_watch_state(args, final_resident)
            state["resident_scorer"] = final_resident
            atomic_write_json(args.output, state)
            break
        time.sleep(max(0.1, float(args.sleep_s)))
    return 0


def main() -> int:
    return run_resident_loop(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
