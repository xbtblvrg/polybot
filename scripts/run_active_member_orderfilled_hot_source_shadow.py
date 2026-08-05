#!/usr/bin/env python3
"""Resident paper shadow for active-member Polygon OrderFilled hot-source parity."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import resource
import statistics
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.realtime_feed import normalize_polygon_orderfilled_row  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.polymarket_addresses import EXCHANGE_ADDRESSES  # noqa: E402
from scripts.merge_rtds_wallet_events import _gamma_token_metadata  # noqa: E402


def _load(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _tail_jsonl(path: Path, *, tail_bytes: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    size = path.stat().st_size
    with path.open("rb") as handle:
        start = max(0, size - max(1, int(tail_bytes)))
        handle.seek(start)
        if start:
            handle.readline()
        rows = []
        for raw in handle:
            try:
                row = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _read_jsonl_incremental(
    path: Path,
    *,
    byte_offset: int,
    bootstrap_tail_bytes: int,
    active_wallets: set[str] | None = None,
) -> tuple[list[dict[str, Any]], int, bool]:
    if not path.exists():
        return [], 0, False
    size = path.stat().st_size
    reset = byte_offset < 0 or byte_offset > size
    start = int(byte_offset)
    align_to_next_line = reset
    if start == 0 or reset:
        start = max(0, size - max(1, int(bootstrap_tail_bytes)))
        align_to_next_line = start > 0
    rows: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        handle.seek(start)
        if align_to_next_line:
            handle.readline()
        while True:
            raw = handle.readline()
            if not raw:
                break
            try:
                row = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(row, dict):
                if active_wallets is not None:
                    row_wallets = {
                        str(row.get(key) or "").strip().lower()
                        for key in ("selected_wallet", "maker", "taker")
                    }
                    if not (row_wallets & active_wallets):
                        continue
                rows.append(row)
        next_offset = handle.tell()
    return rows, next_offset, reset


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _raw_orderfilled_identity(row: dict[str, Any]) -> str:
    transaction_hash = str(row.get("transaction_hash") or "").lower()
    log_index = row.get("log_index")
    return f"{transaction_hash}|{log_index}" if transaction_hash and isinstance(log_index, int) else ""


def _top_of_book(book: dict[str, Any]) -> tuple[float | None, float | None]:
    asks = sorted(
        float(row["price"])
        for row in book.get("asks") or []
        if isinstance(row, dict) and row.get("price") is not None
    )
    bids = sorted(
        (
            float(row["price"])
            for row in book.get("bids") or []
            if isinstance(row, dict) and row.get("price") is not None
        ),
        reverse=True,
    )
    return (bids[0] if bids else None, asks[0] if asks else None)


def attach_forward_signal_books(
    signal_rows: list[dict[str, Any]],
    *,
    new_identities: set[str],
    snapshot_cache: dict[str, dict[str, Any]],
    book_fetcher: Any,
    now_ts: float,
    max_detection_age_s: float = 60.0,
    max_new_captures: int = 20,
) -> dict[str, int]:
    """Attach a forward CLOB snapshot to a newly detected early-01a signal row."""
    stats = {"eligible": 0, "captured": 0, "errors": 0, "cache_attached": 0}
    captures = 0
    for row in signal_rows:
        identity = str(row.get("identity") or row.get("event_id") or "")
        cached = snapshot_cache.get(identity)
        if cached:
            row["signal_detection_book"] = dict(cached)
            stats["cache_attached"] += 1
            continue
        try:
            price = float(row.get("price") or 0.0)
            event_ts = float(row.get("event_ts") or 0.0)
            received_at_s = float(row.get("received_at_s") or row.get("observed_ts") or 0.0)
            window_start = int(str(row.get("market_slug") or "").rsplit("-", 1)[-1])
        except (TypeError, ValueError):
            continue
        if (
            identity not in new_identities
            or captures >= max(0, int(max_new_captures))
            or not (0.25 <= price < 0.32)
            or not (0.0 <= event_ts - window_start < 60.0)
            or not (0.0 <= now_ts - received_at_s <= float(max_detection_age_s))
            or not row.get("token_id")
            or str(row.get("source_wallet") or "").lower() in EXCHANGE_ADDRESSES
        ):
            continue
        stats["eligible"] += 1
        capture_started_at_s = time.time()
        try:
            book = book_fetcher(str(row["token_id"]))
            best_bid, best_ask = _top_of_book(book if isinstance(book, dict) else {})
            captured_at_s = time.time()
            snapshot = {
                "status": "OK" if best_bid is not None or best_ask is not None else "EMPTY_BOOK",
                "capture_mode": "forward_at_signal_detection",
                "asset_id": str(row["token_id"]),
                "signal_received_at_s": received_at_s,
                "capture_started_at_s": capture_started_at_s,
                "captured_at_s": captured_at_s,
                "detection_to_capture_start_s": round(capture_started_at_s - received_at_s, 6),
                "capture_latency_s": round(captured_at_s - capture_started_at_s, 6),
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": round(best_ask - best_bid, 6) if best_bid is not None and best_ask is not None else None,
            }
            stats["captured"] += 1
        except Exception as exc:  # noqa: BLE001 - the signal row must retain failed capture truth.
            snapshot = {
                "status": "ERROR",
                "capture_mode": "forward_at_signal_detection",
                "asset_id": str(row["token_id"]),
                "signal_received_at_s": received_at_s,
                "capture_started_at_s": capture_started_at_s,
                "captured_at_s": time.time(),
                "error": f"{type(exc).__name__}:{str(exc)[:240]}",
            }
            stats["errors"] += 1
        snapshot_cache[identity] = snapshot
        row["signal_detection_book"] = dict(snapshot)
        captures += 1
    return stats


def raw_forward_book_candidate_rows(
    rows: list[dict[str, Any]],
    *,
    token_meta_cache: dict[str, dict[str, Any]],
    watch_wallets: set[str],
) -> list[dict[str, Any]]:
    """Normalize already-decoded raw OrderFilled rows before batch enrichment."""
    candidates: list[dict[str, Any]] = []
    for raw in rows:
        decoded = raw.get("decoded") if isinstance(raw.get("decoded"), dict) else {}
        identity = _raw_orderfilled_identity(raw)
        wallet = str(raw.get("selected_wallet") or decoded.get("maker") or "").lower()
        asset = str(decoded.get("asset") or "")
        meta = token_meta_cache.get(asset) if asset else None
        if not identity or wallet not in watch_wallets or not isinstance(meta, dict):
            continue
        try:
            price = float(decoded.get("price") or 0.0)
            event_ts = float(raw.get("event_ts") or 0.0)
            received_at_s = float(raw.get("received_at_s") or raw.get("captured_at_s") or 0.0)
            slug = str(meta.get("market_slug") or "")
            window_start = int(slug.rsplit("-", 1)[-1])
        except (TypeError, ValueError):
            continue
        if (
            str(decoded.get("side") or decoded.get("maker_side") or "").upper() != "BUY"
            or not (0.25 <= price < 0.32)
            or not (0.0 <= event_ts - window_start < 60.0)
        ):
            continue
        candidates.append(
            {
                "identity": identity,
                "source_wallet": wallet,
                "token_id": asset,
                "market_slug": slug,
                "price": price,
                "event_ts": event_ts,
                "received_at_s": received_at_s,
                "source": str(raw.get("source") or "polygon_orderfilled"),
                "forward_book_roster_rule": "top_8_by_qualifying_window_count_exchange_filtered_wallet_lexicographic_tiebreak",
            }
        )
    return candidates


def decision_time_book_signal_row(row: dict[str, Any]) -> dict[str, Any] | None:
    snapshot = row.get("signal_detection_book")
    if not isinstance(snapshot, dict):
        return None
    identity = str(row.get("identity") or row.get("event_id") or "")
    best_bid = snapshot.get("best_bid")
    best_ask = snapshot.get("best_ask")
    source_price = float(row.get("price") or 0.0)
    captured_at_s = float(snapshot.get("captured_at_s") or 0.0)
    capture_started_at_s = float(snapshot.get("capture_started_at_s") or 0.0)
    detected_at_s = float(snapshot.get("signal_received_at_s") or capture_started_at_s)
    book_lag_s = captured_at_s - detected_at_s if captured_at_s and detected_at_s else None
    raw_book_status = str(snapshot.get("status") or "UNKNOWN")
    book_status = "STALE_DETECTION" if book_lag_s is not None and book_lag_s > 2.0 else raw_book_status
    return {
        "signal_id": identity,
        "signal_detected_at": detected_at_s,
        "trade_ts": float(row.get("event_ts") or 0.0) or None,
        "wallet": str(row.get("source_wallet") or "").lower(),
        "asset_id": str(row.get("token_id") or ""),
        "slug": str(row.get("market_slug") or ""),
        "source_price": source_price,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "book_captured_at_s": captured_at_s,
        "book_lag_s": round(book_lag_s, 6) if book_lag_s is not None else None,
        "book_fetch_rtt_s": (
            round(captured_at_s - capture_started_at_s, 6)
            if captured_at_s and capture_started_at_s
            else None
        ),
        "executable": book_status == "OK" and best_ask is not None and float(best_ask) < 0.32,
        "book_status": book_status,
        "capture_source": "decision_time_forward",
        "source_feed": str(row.get("source") or (row.get("capture_provenance") or {}).get("source") or "polygon_orderfilled"),
        "roster_rule": str(row.get("forward_book_roster_rule") or "active_or_qualified_polygon_orderfilled_watch"),
        "paper_only": True,
        "live_orders_allowed": False,
        "live_mutation": False,
    }


def build_forward_book_summary(rows: list[dict[str, Any]], *, generated_at: str) -> dict[str, Any]:
    matched = [row for row in rows if row.get("best_ask") is not None]
    asks = sorted(float(row["best_ask"]) for row in matched)
    lags = sorted(float(row["book_lag_s"]) for row in matched if row.get("book_lag_s") is not None)
    slippages = [float(row["best_ask"]) - float(row["source_price"]) for row in matched]
    executable = sum(bool(row.get("executable")) for row in matched)
    return {
        "schema_version": 1,
        "kind": "early_01a_decision_time_book_capture",
        "generated_at": generated_at,
        "flow_stage": "MINE/MEASURE/MONEY/DEFEND",
        "status": "ACCRUING_FORWARD_BOOK",
        "paper_only": True,
        "live_orders_allowed": False,
        "live_mutation": False,
        "signal_count": len(rows),
        "matched_book_count": len(matched),
        "book_status_fail_count": len(rows) - len(matched),
        "book_status_fail_pct": round(100.0 * (len(rows) - len(matched)) / len(rows), 6) if rows else None,
        "distinct_windows": len({str(row.get("slug") or "") for row in rows if row.get("slug")}),
        "executable_count": executable,
        "executable_fraction": round(executable / len(matched), 6) if matched else None,
        "best_ask": {
            "mean": round(sum(asks) / len(asks), 6) if asks else None,
            "median": round(statistics.median(asks), 6) if asks else None,
        },
        "mean_slippage_vs_source_fill_pp": (
            round(100.0 * sum(slippages) / len(slippages), 6) if slippages else None
        ),
        "book_lag_s": {
            "min": lags[0] if lags else None,
            "median": round(statistics.median(lags), 6) if lags else None,
            "max": lags[-1] if lags else None,
        },
        "lane_decision_status": "ACCRUING_DISTINCT_WINDOWS_FOR_DROP_K",
    }


def _append_jsonl_locked(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
        handle.flush()
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _active_member_wallets(guard: dict[str, Any]) -> set[str]:
    return {
        str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
        for row in ((guard.get("active_set") or {}).get("members") or [])
        if isinstance(row, dict)
        and row.get("enabled") is not False
        and str(row.get("source_wallet") or row.get("wallet") or "").strip()
    }


def _prospective_wallets(path: Path) -> set[str]:
    payload = _load(path)
    rows = payload.get("wallets") or []
    wallets: set[str] = set()
    for row in rows if isinstance(rows, list) else []:
        value = row.get("wallet") if isinstance(row, dict) else row
        wallet = str(value or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42:
            wallets.add(wallet)
    return wallets


def _early_01a_watch_wallets(path: Path, *, limit: int = 8) -> set[str]:
    payload = _load(path)
    ranked: list[tuple[int, str]] = []
    for row in payload.get("candidates") or []:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("wallet") or row.get("source_wallet") or "").strip().lower()
        if not wallet.startswith("0x") or len(wallet) != 42 or wallet in EXCHANGE_ADDRESSES:
            continue
        ranked.append((int(row.get("qualifying_window_count") or 0), wallet))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    return {wallet for _count, wallet in ranked[: max(0, int(limit))]}


def _otherwise_qualified_wallets(candidate_state: dict[str, Any]) -> tuple[set[str], list[dict[str, Any]]]:
    """Return candidates passing every unchanged Rung-C check except F2."""
    policy_choke = (
        candidate_state.get("policy_choke")
        if isinstance(candidate_state.get("policy_choke"), dict)
        else {}
    )
    actuator = (
        policy_choke.get("actuator")
        if isinstance(policy_choke.get("actuator"), dict)
        else {}
    )
    evidence_blocks = [
        actuator.get("candidate_evidence"),
        actuator.get("rung_b_candidate_evidence"),
    ]
    wallets: set[str] = set()
    roster: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for block in evidence_blocks:
        if not isinstance(block, dict):
            continue
        for row in block.get("rows") or []:
            if not isinstance(row, dict):
                continue
            wallet = str(row.get("wallet") or row.get("source_wallet") or "").strip().lower()
            fingerprint = str(row.get("wide_policy_fingerprint") or "").strip()
            checks = row.get("checks") if isinstance(row.get("checks"), dict) else {}
            required = {
                key: value
                for key, value in checks.items()
                if key != "f2_fresh_rows_and_own_policy_copyable"
            }
            if (
                not (wallet.startswith("0x") and len(wallet) == 42)
                or not required
                or not all(value is True for value in required.values())
                or not fingerprint
                or (wallet, fingerprint) in seen
            ):
                continue
            seen.add((wallet, fingerprint))
            wallets.add(wallet)
            roster.append(
                {
                    "wallet": wallet,
                    "candidate_id": row.get("candidate_id"),
                    "paper_policy_id": row.get("paper_policy_id"),
                    "supply_source": row.get("supply_source"),
                    "wide_policy_fingerprint": fingerprint,
                    "regime_evidence": row.get("regime_evidence"),
                    "checks": checks,
                    "excluded_check": "f2_fresh_rows_and_own_policy_copyable",
                }
            )
    return wallets, roster


def _max_rss_gib() -> float:
    raw = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    bytes_used = raw if sys.platform == "darwin" else raw * 1024.0
    return round(bytes_used / (1024.0**3), 6)


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * pct))))
    return round(ordered[index], 6)


def _capture_transport(row: dict[str, Any]) -> str:
    source = str(row.get("source") or "")
    if source == "polygon_ws":
        return "push"
    if source == "polygon_http_getLogs_tail":
        return "batch"
    return "unknown"


def _row_identity(row: dict[str, Any]) -> str | None:
    tx = str(row.get("transaction_hash") or "").strip().lower()
    log_index = row.get("log_index")
    if tx and isinstance(log_index, int):
        return f"{tx}|{log_index}"
    return None


def _row_event_ts(row: dict[str, Any]) -> float | None:
    try:
        value = float(row.get("block_ts") or row.get("event_ts") or 0.0)
    except (TypeError, ValueError):
        return None
    return value if value > 0.0 else None


def _row_received_at_s(row: dict[str, Any]) -> float | None:
    try:
        value = float(row.get("received_at_s") or 0.0)
    except (TypeError, ValueError):
        return None
    return value if value > 0.0 else None


def _row_wallets(row: dict[str, Any]) -> set[str]:
    return {
        str(row.get(key) or "").strip().lower()
        for key in ("selected_wallet", "maker", "taker", "source_wallet", "wallet")
        if str(row.get(key) or "").strip()
    }


def build_shadow(
    *,
    polygon_rows: list[dict[str, Any]],
    guard: dict[str, Any],
    history: dict[str, Any],
    now_ts: float,
    gamma_base_url: str = "",
    token_meta_seed: dict[str, dict[str, str]] | None = None,
    prospective_wallets: set[str] | None = None,
    prospective_roster: list[dict[str, Any]] | None = None,
    prior_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    members = [
        row
        for row in ((guard.get("active_set") or {}).get("members") or [])
        if isinstance(row, dict) and row.get("enabled") is not False
    ]
    active_wallets = {
        str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
        for row in members
        if str(row.get("source_wallet") or row.get("wallet") or "").strip()
    }
    prospective_wallets = {
        str(wallet).strip().lower()
        for wallet in (prospective_wallets or set())
        if str(wallet).strip()
    } - active_wallets
    prospective_evidence = {
        (
            str(row.get("wallet") or "").strip().lower(),
            str(row.get("wide_policy_fingerprint") or "").strip(),
        ): row
        for row in (prospective_roster or [])
        if isinstance(row, dict)
        and str(row.get("wallet") or "").strip()
        and str(row.get("wide_policy_fingerprint") or "").strip()
    }
    observed_wallets = active_wallets | prospective_wallets
    token_meta: dict[str, dict[str, str]] = {
        str(token): {
            "market_slug": str(meta.get("market_slug") or ""),
            "condition_id": str(meta.get("condition_id") or ""),
            "outcome": str(meta.get("outcome") or ""),
        }
        for token, meta in (token_meta_seed or {}).items()
        if isinstance(meta, dict)
    }
    dataapi_by_tx: dict[str, dict[str, Any]] = {}
    for row in history.get("events") or []:
        if not isinstance(row, dict):
            continue
        token = str(row.get("token_id") or "")
        if token and row.get("market_slug") and row.get("condition_id") and row.get("outcome"):
            token_meta[token] = {
                "market_slug": str(row["market_slug"]),
                "condition_id": str(row["condition_id"]),
                "outcome": str(row["outcome"]),
            }
        tx = str(row.get("transaction_hash") or "").lower()
        if tx:
            dataapi_by_tx[tx] = row
    gamma_lookup_stats: dict[str, int] = {}
    if gamma_base_url:
        starts: set[int] = set()
        for row in polygon_rows:
            decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
            token_id = str(decoded.get("asset") or "")
            if token_id and token_id in token_meta:
                continue
            event_ts = float(row.get("block_ts") or 0.0)
            if event_ts <= 0:
                continue
            base = int(event_ts // 300) * 300
            starts.update((base - 300, base, base + 300))
        token_meta.update(
            {
                key: value
                for key, value in _gamma_token_metadata(
                    gamma_base_url, starts=starts, stats=gamma_lookup_stats
                ).items()
                if key not in token_meta
            }
        )

    resolved_window_starts: set[int] = set()
    for meta_entry in token_meta.values():
        tail = str(meta_entry.get("market_slug") or "").rsplit("-", 1)[-1]
        if tail.isdigit():
            resolved_window_starts.add(int(tail))

    identities: set[str] = set()
    active_rows: list[dict[str, Any]] = []
    duplicate_rows = 0
    mapping_missing = 0
    unmapped_out_of_scope_rows = 0
    unmapped_unresolved_window_rows = 0
    parity_violations = 0
    detection_leads: list[float] = []
    current_window = int(now_ts // 300) * 300
    current_or_next = 0
    prospective_terminals: list[dict[str, Any]] = []
    prospective_terminal_counts: dict[str, int] = {}
    prospective_f2_identities: set[str] = set()
    prospective_f2_events: list[dict[str, Any]] = []
    prospective_wallet_identities: dict[str, set[str]] = {}
    lookback_start = now_ts - 1800.0
    prior_state = prior_state if isinstance(prior_state, dict) else {}
    prior_prospective = (
        prior_state.get("prospective_current_market")
        if isinstance(prior_state.get("prospective_current_market"), dict)
        else {}
    )
    prior_gate = (
        prior_prospective.get("actuator_consumption_gate")
        if isinstance(prior_prospective.get("actuator_consumption_gate"), dict)
        else {}
    )
    order143_measurement_started_at_s = float(
        prior_gate.get("order143_measurement_started_at_s") or now_ts
    )
    order144_code_resident_started_at_s = float(
        prior_gate.get("order144_code_resident_started_at_s") or now_ts
    )
    prior_f2_appended_at = {
        str(row.get("identity") or row.get("event_id") or ""): float(
            row.get("f2_appended_at_s") or 0.0
        )
        for row in prior_prospective.get("identity_clean_events") or []
        if isinstance(row, dict)
        and str(row.get("identity") or row.get("event_id") or "")
        and float(row.get("f2_appended_at_s") or 0.0) > 0.0
    }

    measured_capture_rows = [
        row
        for row in polygon_rows
        if row.get("event") == "polygon_orderfilled_log"
        and _capture_transport(row) in {"push", "batch"}
        and _row_identity(row)
        and _row_event_ts(row) is not None
        and _row_received_at_s(row) is not None
    ]
    transport_event_times = {
        transport: [
            float(_row_event_ts(row) or 0.0)
            for row in measured_capture_rows
            if _capture_transport(row) == transport
        ]
        for transport in ("push", "batch")
    }
    common_event_window = (
        (
            max(min(transport_event_times["push"]), min(transport_event_times["batch"])),
            min(max(transport_event_times["push"]), max(transport_event_times["batch"])),
        )
        if transport_event_times["push"] and transport_event_times["batch"]
        else None
    )
    if common_event_window and common_event_window[0] > common_event_window[1]:
        common_event_window = None
    identities_by_transport = {
        transport: {
            str(_row_identity(row))
            for row in measured_capture_rows
            if _capture_transport(row) == transport
            and common_event_window
            and common_event_window[0]
            <= float(_row_event_ts(row) or 0.0)
            <= common_event_window[1]
        }
        for transport in ("push", "batch")
    }
    def candidate_f2_capture_eligible(row: dict[str, Any]) -> bool:
        event = normalize_polygon_orderfilled_row(row)
        if event is None or str(event.side or "").upper() != "BUY":
            return False
        meta = token_meta.get(str(event.asset or ""))
        if not meta:
            return False
        try:
            window_start = int(str(meta["market_slug"]).rsplit("-", 1)[-1])
        except (KeyError, ValueError):
            return False
        received_at_s = float(event.received_at_s or 0.0)
        return (
            window_start <= received_at_s < window_start + 300
            and received_at_s >= lookback_start
        )

    post_order144_candidate_rows = [
        row
        for row in measured_capture_rows
        if prospective_wallets & _row_wallets(row)
        and float(_row_received_at_s(row) or 0.0) >= order144_code_resident_started_at_s
        and candidate_f2_capture_eligible(row)
    ]
    post_order144_candidate_by_transport = {
        transport: {
            str(_row_identity(row))
            for row in post_order144_candidate_rows
            if _capture_transport(row) == transport
        }
        for transport in ("push", "batch")
    }
    earliest_candidate_capture: dict[str, tuple[float, float]] = {}
    for row in post_order144_candidate_rows:
        identity = str(_row_identity(row))
        received_at_s = float(_row_received_at_s(row) or 0.0)
        event_ts = float(_row_event_ts(row) or 0.0)
        prior = earliest_candidate_capture.get(identity)
        if prior is None or received_at_s < prior[0]:
            earliest_candidate_capture[identity] = (received_at_s, event_ts)
    union_earliest_receipt = [
        received_at_s - event_ts
        for received_at_s, event_ts in earliest_candidate_capture.values()
    ]
    post_order144_push_block_to_receipt = [
        float(_row_received_at_s(row) or 0.0) - float(_row_event_ts(row) or 0.0)
        for row in post_order144_candidate_rows
        if _capture_transport(row) == "push"
    ]

    def record_prospective_terminal(
        *,
        wallet: str,
        identity: str | None,
        terminal: str,
        event_ts: float | None = None,
        received_at_s: float | None = None,
        market_slug: str | None = None,
    ) -> None:
        prospective_terminal_counts[terminal] = prospective_terminal_counts.get(terminal, 0) + 1
        prospective_terminals.append(
            {
                "source_wallet": wallet,
                "identity": identity,
                "terminal": terminal,
                "event_ts": event_ts,
                "received_at_s": received_at_s,
                "market_slug": market_slug,
            }
        )

    for row in polygon_rows:
        if row.get("event") != "polygon_orderfilled_log":
            continue
        event = normalize_polygon_orderfilled_row(row)
        if event is None:
            continue
        wallet = str(event.source_wallet or "").lower()
        if wallet not in observed_wallets:
            continue
        prospective = wallet in prospective_wallets
        transaction_hash = str(event.transaction_hash or "").lower()
        log_index = row.get("log_index")
        if not transaction_hash or not isinstance(log_index, int):
            if prospective:
                record_prospective_terminal(
                    wallet=wallet,
                    identity=None,
                    terminal="identity_missing",
                    event_ts=event.event_ts,
                    received_at_s=event.received_at_s,
                )
            else:
                parity_violations += 1
            continue
        identity = f"{transaction_hash}|{log_index}"
        if identity in identities:
            duplicate_rows += 1
            continue
        identities.add(identity)
        if str(event.side or "").upper() != "BUY":
            if prospective:
                record_prospective_terminal(
                    wallet=wallet,
                    identity=identity,
                    terminal="not_buy",
                    event_ts=event.event_ts,
                    received_at_s=event.received_at_s,
                )
            continue
        meta = token_meta.get(str(event.asset or ""))
        if not meta:
            # An unmapped token is only "out of lane scope" if we actually
            # resolved that window's BTC-5m market and this token was not one of
            # its two outcomes. If the window itself never resolved, scope is
            # UNKNOWN and calling it out-of-scope is the assertion that pinned
            # this stakeout at zero. Split the two; never merge them again.
            window_base = int(float(event.event_ts or 0.0) // 300) * 300
            window_resolved = any(
                (window_base + delta) in resolved_window_starts for delta in (-300, 0, 300)
            )
            if window_resolved:
                unmapped_out_of_scope_rows += 1
            else:
                unmapped_unresolved_window_rows += 1
            if prospective:
                record_prospective_terminal(
                    wallet=wallet,
                    identity=identity,
                    terminal="token_out_of_btc5m_scope" if window_resolved else "window_slug_unresolved",
                    event_ts=event.event_ts,
                    received_at_s=event.received_at_s,
                )
            continue
        slug = str(meta["market_slug"])
        try:
            window_start = int(slug.rsplit("-", 1)[-1])
        except ValueError:
            if prospective:
                record_prospective_terminal(
                    wallet=wallet,
                    identity=identity,
                    terminal="market_slug_invalid",
                    event_ts=event.event_ts,
                    received_at_s=event.received_at_s,
                    market_slug=slug,
                )
            else:
                parity_violations += 1
            continue
        received_at_s = float(event.received_at_s or 0.0)
        if prospective:
            is_current_at_receipt = window_start <= received_at_s < window_start + 300
            is_fresh_30m = received_at_s >= lookback_start
            terminal = (
                "f2_current_market_buy_30m"
                if is_current_at_receipt and is_fresh_30m
                else "closed_window_or_backfill_f2_zero"
            )
            if terminal == "f2_current_market_buy_30m":
                f2_appended_at_s = prior_f2_appended_at.get(identity, now_ts)
                capture_source = str(row.get("source") or "unknown")
                http_capture_kind = row.get("http_capture_kind")
                transport = _capture_transport(row)
                prospective_f2_identities.add(identity)
                prospective_wallet_identities.setdefault(wallet, set()).add(identity)
                prospective_f2_events.append(
                    {
                        "event_id": identity,
                        "identity": identity,
                        "transaction_hash": transaction_hash,
                        "log_index": log_index,
                        "source_wallet": wallet,
                        "proxy_to_wallet": {
                            "selected_wallet": str(row.get("selected_wallet") or "").lower(),
                            "maker": str(row.get("maker") or "").lower(),
                            "taker": str(row.get("taker") or "").lower(),
                            "resolved_source_wallet": wallet,
                        },
                        "token_id": event.asset,
                        "condition_id": meta["condition_id"],
                        "market_slug": slug,
                        "outcome": meta["outcome"],
                        "action": "BUY",
                        "side": "BUY",
                        "price": event.price,
                        "size": event.size,
                        "event_ts": event.event_ts,
                        "observed_ts": event.received_at_s,
                        "received_at_s": event.received_at_s,
                        "f2_appended_at_s": f2_appended_at_s,
                        "source": capture_source,
                        "http_capture_kind": http_capture_kind,
                        "transport": transport,
                        "order143_measurement_eligible": bool(
                            float(event.received_at_s or 0.0)
                            >= order143_measurement_started_at_s
                        ),
                        "capture_provenance": {
                            "source": capture_source,
                            "http_capture_kind": http_capture_kind,
                            "transport": transport,
                        },
                        "paper_only": True,
                    }
                )
            record_prospective_terminal(
                wallet=wallet,
                identity=identity,
                terminal=terminal,
                event_ts=event.event_ts,
                received_at_s=event.received_at_s,
                market_slug=slug,
            )
            continue
        if window_start in {current_window, current_window + 300}:
            current_or_next += 1
        dataapi = dataapi_by_tx.get(str(event.transaction_hash or "").lower())
        if dataapi and event.received_at_s:
            detection_leads.append(float(dataapi.get("observed_ts") or 0.0) - float(event.received_at_s))
        active_rows.append(
            {
                "identity": identity,
                "source_wallet": wallet,
                "transaction_hash": event.transaction_hash,
                "log_index": log_index,
                "token_id": event.asset,
                "condition_id": meta["condition_id"],
                "market_slug": slug,
                "outcome": meta["outcome"],
                "price": event.price,
                "size": event.size,
                "event_ts": event.event_ts,
                "received_at_s": event.received_at_s,
            }
        )

    resolved = len(active_rows)
    gate_pass = resolved >= 100 and parity_violations == 0
    prospective_unique_fills = len(
        {
            str(row.get("identity") or "")
            for row in prospective_terminals
            if str(row.get("identity") or "")
        }
    )
    prospective_wallet_buy_counts = {
        wallet: len(identities)
        for wallet, identities in sorted(prospective_wallet_identities.items())
    }
    block_receipt_rows = [
        (
            row,
            float(row.get("received_at_s") or 0.0)
            - float(row.get("event_ts") or 0.0),
        )
        for row in prospective_f2_events
        if float(row.get("received_at_s") or 0.0) > 0.0
        and float(row.get("event_ts") or 0.0) > 0.0
    ]
    signed_block_to_receipt = [value for _row, value in block_receipt_rows]
    push_block_to_receipt = [
        value
        for row, value in block_receipt_rows
        if (row.get("capture_provenance") or {}).get("transport") == "push"
    ]
    batch_block_to_receipt = [
        value
        for row, value in block_receipt_rows
        if (row.get("capture_provenance") or {}).get("transport") == "batch"
    ]
    receipt_to_f2_admission = [
        float(row.get("f2_appended_at_s") or 0.0)
        - float(row.get("received_at_s") or 0.0)
        for row in prospective_f2_events
        if row.get("order143_measurement_eligible") is True
        and float(row.get("f2_appended_at_s") or 0.0) > 0.0
        and float(row.get("received_at_s") or 0.0) > 0.0
    ]
    admission_stamps = sorted(
        {
            float(row.get("f2_appended_at_s") or 0.0)
            for row in prospective_f2_events
            if row.get("order143_measurement_eligible") is True
            and float(row.get("f2_appended_at_s") or 0.0) > 0.0
            and float(row.get("received_at_s") or 0.0) > 0.0
        }
    )
    admission_cluster_sizes = [
        sum(
            float(row.get("f2_appended_at_s") or 0.0) == stamp
            for row in prospective_f2_events
            if row.get("order143_measurement_eligible") is True
            and float(row.get("received_at_s") or 0.0) > 0.0
        )
        for stamp in admission_stamps
    ]
    admission_stamp_periods = [
        later - earlier
        for earlier, later in zip(admission_stamps, admission_stamps[1:])
    ]
    chronological_holdout_by_wallet: dict[str, dict[str, dict[str, Any]]] = {}
    for (wallet, fingerprint), evidence_row in sorted(prospective_evidence.items()):
        if wallet not in prospective_wallets:
            continue
        regime_evidence = (
            evidence_row.get("regime_evidence")
            if isinstance(evidence_row.get("regime_evidence"), dict)
            else {}
        )
        first_half_pnl = regime_evidence.get("first_half_post_fee_pnl_usd")
        second_half_pnl = regime_evidence.get("second_half_post_fee_pnl_usd")
        passed = bool(
            fingerprint
            and first_half_pnl is not None
            and float(first_half_pnl) > 0.0
            and second_half_pnl is not None
            and float(second_half_pnl) > 0.0
        )
        chronological_holdout_by_wallet.setdefault(wallet, {})[fingerprint] = {
            "wide_policy_fingerprint": fingerprint or None,
            "first_half_post_fee_pnl_usd": first_half_pnl,
            "second_half_post_fee_pnl_usd": second_half_pnl,
            "resolved_signals": regime_evidence.get("resolved_signals"),
            "passed": passed,
            "source": regime_evidence.get("source"),
        }
    missing_holdout_wallets = sorted(
        wallet for wallet in prospective_wallets
        if not chronological_holdout_by_wallet.get(wallet)
    )
    failed_holdout_cells = [
        (wallet, fingerprint)
        for wallet, wallet_rows in chronological_holdout_by_wallet.items()
        for fingerprint, row in wallet_rows.items()
        if row["passed"] is not True
    ]
    positive_exact_policy_chronological_holdout: bool | str = (
        "UNKNOWN"
        if not prospective_wallets or missing_holdout_wallets
        else False
        if failed_holdout_cells
        else True
    )
    prospective_f2_gate = {
        "passed": False,
        "unique_fills": prospective_unique_fills,
        "required_unique_fills": 100,
        "identity_market_outcome_parity_violations": parity_violations,
        "max_current_market_buy_count": max(
            prospective_wallet_buy_counts.values(),
            default=0,
        ),
        "required_current_market_buy_count": 10,
        "wallet_current_market_buy_counts": prospective_wallet_buy_counts,
        "receipt_to_f2_p50_s": _percentile(post_order144_push_block_to_receipt, 0.50),
        "receipt_to_f2_p95_s": _percentile(post_order144_push_block_to_receipt, 0.95),
        "receipt_to_f2_samples": len(post_order144_push_block_to_receipt),
        "receipt_to_f2_status": (
            "measured" if post_order144_push_block_to_receipt else "insufficient_samples"
        ),
        "receipt_to_f2_metric_definition": "candidate post-ORDER144 push block timestamp to local receipt; null when candidate push coverage is absent",
        "batch_block_to_receipt": {
            "samples": len(batch_block_to_receipt),
            "p50_s": _percentile(batch_block_to_receipt, 0.50),
            "p95_s": _percentile(batch_block_to_receipt, 0.95),
        },
        "push_block_to_receipt": {
            "samples": len(push_block_to_receipt),
            "p50_s": _percentile(push_block_to_receipt, 0.50),
            "p95_s": _percentile(push_block_to_receipt, 0.95),
        },
        "push_recall_vs_batch": {
            "common_event_ts_start_s": (
                round(common_event_window[0], 6) if common_event_window else None
            ),
            "common_event_ts_end_s": (
                round(common_event_window[1], 6) if common_event_window else None
            ),
            "common_event_ts_window_s": (
                round(common_event_window[1] - common_event_window[0], 6)
                if common_event_window
                else None
            ),
            "push": len(identities_by_transport["push"]),
            "batch": len(identities_by_transport["batch"]),
            "both": len(identities_by_transport["push"] & identities_by_transport["batch"]),
            "push_only": len(identities_by_transport["push"] - identities_by_transport["batch"]),
            "batch_only": len(identities_by_transport["batch"] - identities_by_transport["push"]),
            "push_recall_fraction": (
                round(
                    len(identities_by_transport["push"] & identities_by_transport["batch"])
                    / len(identities_by_transport["batch"]),
                    6,
                )
                if identities_by_transport["batch"]
                else None
            ),
        },
        "candidate_push_coverage": {
            "measurement_started_at_s": order144_code_resident_started_at_s,
            "row_counts_by_transport": {
                "push": len(post_order144_candidate_by_transport["push"]),
                "batch": len(post_order144_candidate_by_transport["batch"]),
            },
            "push_rows_for_candidate": len(post_order144_candidate_by_transport["push"]),
            "total_rows_for_candidate": (
                len(post_order144_candidate_by_transport["push"])
                + len(post_order144_candidate_by_transport["batch"])
            ),
            "push_fraction_for_candidate": (
                round(
                    len(post_order144_candidate_by_transport["push"])
                    / (
                        len(post_order144_candidate_by_transport["push"])
                        + len(post_order144_candidate_by_transport["batch"])
                    ),
                    6,
                )
                if post_order144_candidate_by_transport["push"]
                or post_order144_candidate_by_transport["batch"]
                else None
            ),
            "status": (
                "measured"
                if post_order144_candidate_by_transport["push"]
                else "insufficient_samples"
            ),
        },
        "union_earliest_receipt_block_to_receipt": {
            "samples": len(union_earliest_receipt),
            "p50_s": _percentile(union_earliest_receipt, 0.50),
            "p95_s": _percentile(union_earliest_receipt, 0.95),
        },
        "signed_block_to_receipt": {
            "samples": len(signed_block_to_receipt),
            "min_s": min(signed_block_to_receipt) if signed_block_to_receipt else None,
            "p50_s": _percentile(signed_block_to_receipt, 0.50),
            "p95_s": _percentile(signed_block_to_receipt, 0.95),
            "max_s": max(signed_block_to_receipt) if signed_block_to_receipt else None,
            "negative_sample_fraction": (
                sum(value < 0.0 for value in signed_block_to_receipt)
                / len(signed_block_to_receipt)
                if signed_block_to_receipt
                else None
            ),
        },
        "receipt_to_f2_admission_p50_s": _percentile(receipt_to_f2_admission, 0.50),
        "receipt_to_f2_admission_p95_s": _percentile(receipt_to_f2_admission, 0.95),
        "receipt_to_f2_admission_samples": len(receipt_to_f2_admission),
        "receipt_to_f2_admission_metric_definition": "first F2 append timestamp minus local receipt timestamp for post-ORDER143 events",
        "receipt_to_f2_admission_status": "diagnostic_only_not_a_gate_conjunct",
        "admission_stamp_clusters": {
            "samples": len(receipt_to_f2_admission),
            "distinct_stamps": len(admission_stamps),
            "mean_cluster_size": (
                round(statistics.mean(admission_cluster_sizes), 6)
                if admission_cluster_sizes
                else None
            ),
            "max_cluster_size": max(admission_cluster_sizes, default=0),
            "iteration_period_p50_s": _percentile(admission_stamp_periods, 0.50),
        },
        "iteration_period_p50_s": _percentile(admission_stamp_periods, 0.50),
        "order143_measurement_started_at_s": order143_measurement_started_at_s,
        "order144_code_resident_started_at_s": order144_code_resident_started_at_s,
        "post_order144_push_block_to_receipt": {
            "samples": len(post_order144_push_block_to_receipt),
            "p50_s": _percentile(post_order144_push_block_to_receipt, 0.50),
            "p95_s": _percentile(post_order144_push_block_to_receipt, 0.95),
            "status": (
                "insufficient_samples"
                if not post_order144_candidate_by_transport["push"]
                else "measured"
            ),
        },
        "required_receipt_to_f2_p95_lt_s": 2.0,
        "positive_exact_policy_chronological_holdout": (
            positive_exact_policy_chronological_holdout
        ),
        "exact_policy_holdout_candidate_denominator": len(prospective_wallets),
        "exact_policy_holdout_covered_wallets": len(
            set(prospective_wallets) - set(missing_holdout_wallets)
        ),
        "exact_policy_holdout_missing_wallets": missing_holdout_wallets,
        "exact_policy_chronological_holdout_by_wallet": (
            chronological_holdout_by_wallet
        ),
        "sub_gate_measurements": {
            "unique_fills": {
                "value": prospective_unique_fills,
                "threshold": 100,
                "green": prospective_unique_fills >= 100,
            },
            "current_market_buy_count": {
                "value": max(prospective_wallet_buy_counts.values(), default=0),
                "threshold": 10,
                "green": max(prospective_wallet_buy_counts.values(), default=0) >= 10,
            },
            "parity_violations": {
                "value": parity_violations,
                "threshold": 0,
                "green": parity_violations == 0,
            },
            "receipt_to_f2_p95_s": {
                "value": _percentile(union_earliest_receipt, 0.95),
                "threshold_lt": 2.0,
                "status": (
                    "measured"
                    if union_earliest_receipt
                    else "insufficient_samples"
                ),
                "green": bool(
                    union_earliest_receipt
                    and float(_percentile(union_earliest_receipt, 0.95) or 0.0) < 2.0
                ),
            },
            "push_only_receipt_to_f2_p95_s": {
                "value": _percentile(post_order144_push_block_to_receipt, 0.95),
                "status": "diagnostic_only_not_a_gate_conjunct",
            },
            "receipt_to_f2_admission_p95_s": {
                "value": _percentile(receipt_to_f2_admission, 0.95),
                "status": "diagnostic_only_not_a_gate_conjunct",
            },
            "exact_policy_chronological_holdout": {
                "value": positive_exact_policy_chronological_holdout,
                "green": positive_exact_policy_chronological_holdout is True,
            },
        },
        "reason": (
            "paper evidence only; actuator consumption remains closed until "
            "all preregistered parity, latency, volume, and holdout gates pass"
        ),
    }
    return {
        "schema_version": 1,
        "kind": "active_member_orderfilled_hot_source_shadow",
        "flow_stage": "LEARN/OBSERVE/SELF-DEV",
        "status": "PASS" if gate_pass else "ACCRUING",
        "paper_only": True,
        "live_orders_allowed": False,
        "active_member_count": len(active_wallets),
        "prospective_wallet_count": len(prospective_wallets),
        "prospective_wallets": sorted(prospective_wallets),
        "prospective_current_market": {
            "lookback_s": 1800,
            "required_genuine_buy_identities": 10,
            "genuine_buy_identities": len(prospective_f2_identities),
            "lifetime_received_during_own_market_buy_identities": len(
                prospective_f2_identities
            ),
            "count_semantics": "cumulative accumulator identities received during each event's own live BTC5m market; not a single-current-window rate",
            "gate_passed": len(prospective_f2_identities) >= 10,
            "terminal_counts": prospective_terminal_counts,
            "terminal_rows": len(prospective_terminals),
            "reconciled": sum(prospective_terminal_counts.values()) == len(prospective_terminals),
            "identity_clean_events": prospective_f2_events,
            "actuator_consumption_gate": prospective_f2_gate,
            "sample_terminals": prospective_terminals[-20:],
            "rule": "F2 counts only generation-observed BUY identities whose BTC5M slug was active at receipt and whose receipt is inside the trailing 30m; closed-window/backfill rows remain F2=0",
        },
        "unique_resolved_source_events": resolved,
        "required_unique_resolved_source_events": 100,
        "current_or_next_window_events": current_or_next,
        "duplicate_rows": duplicate_rows,
        "token_mapping_missing": mapping_missing,
        "unmapped_out_of_scope_rows": unmapped_out_of_scope_rows,
        "unmapped_unresolved_window_rows": unmapped_unresolved_window_rows,
        "gamma_slug_lookup": dict(gamma_lookup_stats),
        "identity_market_outcome_parity_violations": parity_violations,
        "detection_lead_s": {
            "matched_events": len(detection_leads),
            "p50": round(statistics.median(detection_leads), 6) if detection_leads else None,
            "p95": _percentile(detection_leads, 0.95),
        },
        "live_source_wiring_gate_passed": gate_pass,
        "identity_rule": "polygon primary identity is transaction_hash|log_index; bare transaction hash never merges logs",
        "token_metadata_cache": token_meta,
        "rule": "paper-first >=100 unique resolved active-member OrderFilled events and zero identity/market/outcome parity violations",
        "identity_clean_events": active_rows,
        "sample_rows": active_rows[-20:],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--polygon-jsonl", default="data/research/polygon_orderfilled_ws_shadow_resident.jsonl")
    parser.add_argument("--guard-state", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--state", default="data/research/active_member_orderfilled_hot_source_shadow_state.json")
    parser.add_argument(
        "--accumulator-state",
        default="data/research/active_member_orderfilled_hot_source_shadow_accumulator.json",
    )
    parser.add_argument(
        "--prospective-wallets",
        default="configs/wallet_copy/prospective_hot_source_wallets.json",
    )
    parser.add_argument(
        "--candidate-evidence-state",
        default="data/research/order_flow_deadman_state.json",
        help="Latest unchanged-bar Rung B/C evidence used to build the pre-F2 roster.",
    )
    parser.add_argument(
        "--qualified-pool-only",
        action="store_true",
        help="Observe only the otherwise-qualified pre-F2 candidate pool, excluding active members.",
    )
    parser.add_argument("--bootstrap-tail-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--max-accumulator-events", type=int, default=5000)
    parser.add_argument("--iterations", type=int, default=0, help="0 runs forever.")
    parser.add_argument("--sleep-s", type=float, default=30.0)
    parser.add_argument(
        "--gamma-base-url",
        default=os.getenv("POLYMARKET_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"),
    )
    parser.add_argument("--clob-base-url", default=os.getenv("POLYMARKET_CLOB_BASE_URL", "https://clob.polymarket.com"))
    parser.add_argument("--clob-timeout-s", type=float, default=1.5)
    parser.add_argument("--forward-book-max-detection-age-s", type=float, default=60.0)
    parser.add_argument("--forward-book-capture", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--forward-book-only",
        action="store_true",
        help="Run the lightweight raw OrderFilled-to-book path without the batch shadow build.",
    )
    parser.add_argument(
        "--token-meta-seed",
        default="data/research/copy_qualified_pool_orderfilled_resident_stakeout_accumulator.json",
    )
    parser.add_argument(
        "--forward-book-signals",
        default="data/research/early_01a_decision_time_book_signals.jsonl",
    )
    parser.add_argument(
        "--forward-book-latest",
        default="data/research/early_01a_decision_time_book_latest.json",
    )
    parser.add_argument(
        "--early-01a-candidates",
        default="data/research/early_01a_candidate_edge_latest.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    polygon_path = ROOT / args.polygon_jsonl
    accumulator_path = ROOT / args.accumulator_state
    accumulator = _load(accumulator_path)
    accumulated_rows = [
        row for row in accumulator.get("rows") or [] if isinstance(row, dict)
    ][-max(1, int(args.max_accumulator_events)) :]
    token_meta_cache = {
        str(token): meta
        for token, meta in (accumulator.get("token_metadata_cache") or {}).items()
        if isinstance(meta, dict)
    }
    seed_payload = _load(ROOT / args.token_meta_seed)
    seed_meta = seed_payload.get("token_metadata_cache") or {}
    token_meta_cache.update(
        {str(token): meta for token, meta in seed_meta.items() if isinstance(meta, dict)}
    )
    forward_book_snapshots = {
        str(identity): dict(snapshot)
        for identity, snapshot in (accumulator.get("forward_book_snapshots") or {}).items()
        if isinstance(snapshot, dict)
    }
    forward_signal_rows = [
        dict(row) for row in accumulator.get("forward_signal_rows") or [] if isinstance(row, dict)
    ][-max(1, int(args.max_accumulator_events)) :]
    if not forward_signal_rows:
        forward_signal_rows = [
            dict(row)
            for row in seed_payload.get("forward_signal_rows") or []
            if isinstance(row, dict)
        ][-max(1, int(args.max_accumulator_events)) :]
    if not forward_book_snapshots:
        forward_book_snapshots = {
            str(identity): dict(snapshot)
            for identity, snapshot in (seed_payload.get("forward_book_snapshots") or {}).items()
            if isinstance(snapshot, dict)
        }
    clob = CLOBMarketClient(str(args.clob_base_url), timeout_s=float(args.clob_timeout_s), retries=1)
    byte_offset = int(accumulator.get("next_byte_offset") or 0)
    iteration = 0
    while args.iterations <= 0 or iteration < args.iterations:
        iteration += 1
        early_01a_wallets = _early_01a_watch_wallets(ROOT / args.early_01a_candidates)
        if args.forward_book_only:
            guard: dict[str, Any] = {}
            qualified_wallets: set[str] = set()
            qualified_roster: list[dict[str, Any]] = []
            prior_state: dict[str, Any] = {}
            prospective_wallets = set(early_01a_wallets)
            build_guard = {"active_set": {"members": []}}
        else:
            guard = _load(ROOT / args.guard_state)
            configured_wallets = _prospective_wallets(ROOT / args.prospective_wallets)
            qualified_wallets, qualified_roster = _otherwise_qualified_wallets(
                _load(ROOT / args.candidate_evidence_state)
            )
            prior_state = _load(ROOT / args.state)
            prospective_wallets = (
                qualified_wallets | early_01a_wallets
                if args.qualified_pool_only
                else configured_wallets | qualified_wallets | early_01a_wallets
            )
            build_guard = {"active_set": {"members": []}} if args.qualified_pool_only else guard
        new_rows, byte_offset, cursor_reset = _read_jsonl_incremental(
            polygon_path,
            byte_offset=byte_offset,
            bootstrap_tail_bytes=args.bootstrap_tail_bytes,
            active_wallets=_active_member_wallets(build_guard) | prospective_wallets,
        )
        accumulated_rows.extend(new_rows)
        accumulated_rows = accumulated_rows[-max(1, int(args.max_accumulator_events)) :]
        new_identities = {_raw_orderfilled_identity(row) for row in new_rows}
        new_identities.discard("")
        raw_book_rows = raw_forward_book_candidate_rows(
            new_rows,
            token_meta_cache=token_meta_cache,
            watch_wallets=early_01a_wallets,
        )
        raw_forward_stats = (
            attach_forward_signal_books(
                raw_book_rows,
                new_identities=new_identities,
                snapshot_cache=forward_book_snapshots,
                book_fetcher=clob.get_book,
                now_ts=time.time(),
                max_detection_age_s=float(args.forward_book_max_detection_age_s),
            )
            if args.forward_book_capture
            else {"eligible": 0, "captured": 0, "errors": 0, "cache_attached": 0}
        )
        if args.forward_book_only:
            prior_output_ids = {str(row.get("signal_id") or "") for row in forward_signal_rows}
            new_forward_rows = [
                output
                for row in raw_book_rows
                if (output := decision_time_book_signal_row(row)) is not None
                and str(output.get("signal_id") or "") not in prior_output_ids
            ]
            _append_jsonl_locked(ROOT / args.forward_book_signals, new_forward_rows)
            forward_signal_rows.extend(new_forward_rows)
            forward_signal_rows = forward_signal_rows[-max(1, int(args.max_accumulator_events)) :]
            generated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            _atomic_write_json(
                ROOT / args.forward_book_latest,
                build_forward_book_summary(forward_signal_rows, generated_at=generated_at),
            )
            payload = {
                "schema_version": 1,
                "kind": "early_01a_decision_time_book_capture_observer",
                "generated_at": generated_at,
                "heartbeat_at": generated_at,
                "pid": os.getpid(),
                "iteration": iteration,
                "forward_signal_book_capture": {
                    **raw_forward_stats,
                    "status": "ARMED_RAW_PRE_BATCH",
                    "unit": "new raw early-01a BUY OrderFilled with cached token metadata",
                    "early_01a_watch_wallet_count": len(early_01a_wallets),
                    "early_01a_watch_wallets": sorted(early_01a_wallets),
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "live_mutation": False,
                },
                "incremental_reader": {
                    "next_byte_offset": byte_offset,
                    "new_rows": len(new_rows),
                    "cursor_reset": cursor_reset,
                },
            }
            _atomic_write_json(
                accumulator_path,
                {
                    "schema_version": 1,
                    "next_byte_offset": byte_offset,
                    "rows": [],
                    "token_metadata_cache": token_meta_cache,
                    "forward_book_snapshots": dict(list(forward_book_snapshots.items())[-max(1, int(args.max_accumulator_events)) :]),
                    "forward_signal_rows": forward_signal_rows,
                },
            )
            _atomic_write_json(ROOT / args.state, payload)
            if args.iterations > 0 and iteration >= args.iterations:
                break
            time.sleep(max(0.1, float(args.sleep_s)))
            continue
        payload = build_shadow(
            polygon_rows=accumulated_rows,
            guard=build_guard,
            history={},
            now_ts=time.time(),
            gamma_base_url=str(args.gamma_base_url or ""),
            token_meta_seed=token_meta_cache,
            prospective_wallets=prospective_wallets,
            prospective_roster=qualified_roster,
            prior_state=prior_state,
        )
        signal_rows = [
            *(((payload.get("prospective_current_market") or {}).get("identity_clean_events") or [])),
            *(payload.get("identity_clean_events") or []),
        ]
        signal_rows = [row for row in signal_rows if isinstance(row, dict)]
        for signal_row in signal_rows:
            if str(signal_row.get("source_wallet") or "").lower() in early_01a_wallets:
                signal_row["forward_book_roster_rule"] = (
                    "top_8_by_qualifying_window_count_exchange_filtered_wallet_lexicographic_tiebreak"
                )
        prior_output_ids = {str(row.get("signal_id") or "") for row in forward_signal_rows}
        forward_stats = (
            attach_forward_signal_books(
                signal_rows,
                new_identities=new_identities,
                snapshot_cache=forward_book_snapshots,
                book_fetcher=clob.get_book,
                now_ts=time.time(),
                max_detection_age_s=float(args.forward_book_max_detection_age_s),
            )
            if args.forward_book_capture
            else {"eligible": 0, "captured": 0, "errors": 0, "cache_attached": 0}
        )
        new_forward_rows = [
            output
            for row in signal_rows
            if (output := decision_time_book_signal_row(row)) is not None
            and str(output.get("signal_id") or "") not in prior_output_ids
        ]
        if args.forward_book_capture:
            _append_jsonl_locked(ROOT / args.forward_book_signals, new_forward_rows)
            forward_signal_rows.extend(new_forward_rows)
            forward_signal_rows = forward_signal_rows[-max(1, int(args.max_accumulator_events)) :]
            _atomic_write_json(
                ROOT / args.forward_book_latest,
                build_forward_book_summary(forward_signal_rows, generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            )
        payload["forward_signal_book_capture"] = {
            **{key: int(raw_forward_stats.get(key, 0)) + int(forward_stats.get(key, 0)) for key in forward_stats},
            "status": "ARMED_FORWARD_ONLY",
            "unit": "new early-01a BUY signal with event offset <60s",
            "row_field": "signal_detection_book",
            "source": str(args.clob_base_url),
            "max_detection_age_s": float(args.forward_book_max_detection_age_s),
            "cached_snapshots": len(forward_book_snapshots),
            "early_01a_watch_wallet_count": len(early_01a_wallets),
            "early_01a_watch_wallets": sorted(early_01a_wallets),
            "paper_only": True,
            "live_orders_allowed": False,
            "signals_output": args.forward_book_signals,
            "latest_output": args.forward_book_latest,
        }
        token_meta_cache = dict(payload.pop("token_metadata_cache", {}))
        payload["generated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        payload["pid"] = os.getpid()
        payload["iteration"] = iteration
        payload["heartbeat_at"] = payload["generated_at"]
        payload["qualified_pool_roster"] = {
            "source": args.candidate_evidence_state,
            "construction_rule": (
                "unchanged Rung B/C checks F1,F3,F4, exact policy and active temporal "
                "must pass before F2; F2 is measured prospectively here"
            ),
            "wallet_count": len(qualified_wallets),
            "wallets": qualified_roster,
            "qualified_pool_only": bool(args.qualified_pool_only),
        }
        payload["resource_usage"] = {
            "max_rss_gib": _max_rss_gib(),
            "rss_limit_gib": 1.0,
            "under_limit": _max_rss_gib() < 1.0,
        }
        payload["incremental_reader"] = {
            "next_byte_offset": byte_offset,
            "new_rows": len(new_rows),
            "accumulator_rows": len(accumulated_rows),
            "max_accumulator_events": int(args.max_accumulator_events),
            "cursor_reset": cursor_reset,
            "bootstrap_tail_bytes": int(args.bootstrap_tail_bytes),
        }
        _atomic_write_json(
            accumulator_path,
            {
                "schema_version": 1,
                "next_byte_offset": byte_offset,
                "rows": accumulated_rows,
                "token_metadata_cache": token_meta_cache,
                "forward_book_snapshots": dict(list(forward_book_snapshots.items())[-max(1, int(args.max_accumulator_events)) :]),
                "forward_signal_rows": forward_signal_rows,
            },
        )
        state = ROOT / args.state
        _atomic_write_json(state, payload)
        if args.iterations > 0 and iteration >= args.iterations:
            break
        time.sleep(max(0.1, float(args.sleep_s)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
