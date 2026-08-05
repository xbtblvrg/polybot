#!/usr/bin/env python3
"""Census wallet-agnostic early 01a BTC-5m supply from resident OrderFilled rows."""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Iterator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.merge_rtds_wallet_events import _gamma_token_metadata  # noqa: E402
from src.wallet_copy.polymarket_addresses import EXCHANGE_ADDRESSES  # noqa: E402
from src.wallet_copy.realtime_feed import normalize_polygon_orderfilled_row  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402

WINDOW_RE = re.compile(r"btc-(?:updown|up-or-down)-5m-(\d+)")
DEFAULT_MAX_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_LOOKBACK_S = 72 * 60 * 60
TARGET_WINDOWS_PER_DAY = 30.0


def _tail_bounds(path: Path, max_bytes: int) -> tuple[int, int, int]:
    size = path.stat().st_size
    return max(0, size - max(1, int(max_bytes))), size, size


def _bounded_rows_handle(handle: BinaryIO, *, start: int, end: int) -> Iterator[dict[str, Any]]:
    handle.seek(start)
    if start:
        handle.readline()
    while handle.tell() < end:
        raw = handle.readline(end - handle.tell())
        if not raw:
            break
        try:
            row = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if isinstance(row, dict):
            yield row


def _bounded_rows(path: Path, *, start: int, end: int) -> Iterator[dict[str, Any]]:
    with path.open("rb") as handle:
        yield from _bounded_rows_handle(handle, start=start, end=end)


def _span_days(starts: Iterable[int]) -> float | None:
    values = list(starts)
    return (max(values) - min(values) + 300.0) / 86400.0 if values else None


def _rate(count: int, span_days: float | None) -> float | None:
    return round(count / span_days, 6) if span_days is not None else None


def build_census(
    rows: Iterable[dict[str, Any]],
    *,
    token_metadata: dict[str, dict[str, str]],
    since_block_ts: float,
    generated_at: str,
) -> dict[str, Any]:
    diagnostics = {
        "rows_seen": 0,
        "rows_before_since_block_ts": 0,
        "normalize_missing": 0,
        "required_field_missing": 0,
        "token_mapping_missing": 0,
        "non_btc5m_mapping": 0,
        "negative_or_post_window_offset": 0,
        "attributions_to_settlement_contracts": 0,
    }
    wallet_starts: dict[str, set[int]] = {}
    wallet_qualifying: dict[str, set[str]] = {}
    common_starts: set[int] = set()
    in_band_windows: set[str] = set()
    union_qualifying: set[str] = set()

    for row in rows:
        diagnostics["rows_seen"] += 1
        event = normalize_polygon_orderfilled_row(row)
        if event is None:
            diagnostics["normalize_missing"] += 1
            continue
        if event.event_ts is None or event.price is None or not event.asset:
            diagnostics["required_field_missing"] += 1
            continue
        block_ts = float(event.event_ts)
        if block_ts < since_block_ts:
            diagnostics["rows_before_since_block_ts"] += 1
            continue
        metadata = token_metadata.get(str(event.asset)) or {}
        slug = str(metadata.get("market_slug") or "")
        match = WINDOW_RE.search(slug)
        if not metadata:
            diagnostics["token_mapping_missing"] += 1
            continue
        if not match:
            diagnostics["non_btc5m_mapping"] += 1
            continue
        start = int(match.group(1))
        offset_s = block_ts - start
        if offset_s < 0.0 or offset_s >= 300.0:
            diagnostics["negative_or_post_window_offset"] += 1
            continue
        wallet = event.maker if str(event.maker_side).upper() == "BUY" else event.taker
        wallet = str(wallet or "").lower()
        if not re.fullmatch(r"0x[0-9a-f]{40}", wallet):
            diagnostics["required_field_missing"] += 1
            continue
        if wallet in EXCHANGE_ADDRESSES:
            diagnostics["attributions_to_settlement_contracts"] += 1
            continue
        wallet_starts.setdefault(wallet, set()).add(start)
        common_starts.add(start)
        if 0.25 <= float(event.price) < 0.32:
            in_band_windows.add(slug)
            if offset_s < 60.0:
                wallet_qualifying.setdefault(wallet, set()).add(slug)
                union_qualifying.add(slug)

    common_span = _span_days(common_starts)
    rung_count_threshold = math.ceil(TARGET_WINDOWS_PER_DAY * common_span) if common_span is not None else None
    per_wallet: list[dict[str, Any]] = []
    for wallet, starts in wallet_starts.items():
        qualifying_count = len(wallet_qualifying.get(wallet, set()))
        own_span = _span_days(starts)
        own_daily = _rate(qualifying_count, own_span)
        daily = _rate(qualifying_count, common_span)
        per_wallet.append(
            {
                "wallet": wallet,
                "observed_btc5m_window_count": len(starts),
                "qualifying_window_count": qualifying_count,
                "span_days": round(common_span, 6) if common_span is not None else None,
                "span_days_below_1": common_span is not None and common_span < 1.0,
                "rate_confidence": "LOW_CONFIDENCE_SUB_DAY_SPAN" if common_span is not None and common_span < 1.0 else "OBSERVED_AT_LEAST_ONE_DAY",
                "qualifying_01a_windows_per_day": daily,
                "contribution_toward_30_per_day": round((daily or 0.0) / TARGET_WINDOWS_PER_DAY, 6),
                "diagnostic_wallet_observed_span_days": round(own_span, 6) if own_span is not None else None,
                "diagnostic_wallet_span_qualifying_01a_windows_per_day": own_daily,
            }
        )
    per_wallet.sort(key=lambda item: (-(item["qualifying_01a_windows_per_day"] or 0.0), item["wallet"]))
    union_daily = _rate(len(union_qualifying), common_span)
    qualifying_wallets = [item for item in per_wallet if item["qualifying_window_count"] >= 1]

    return {
        "kind": "orderfilled_early_01a_supply_census",
        "generated_at": generated_at,
        "flow_stage": "MINE/MEASURE/MONEY",
        "paper_only": True,
        "live_mutation": False,
        "measurement_contract": {
            "candidate_scope": "every valid BUY-side counterparty in the bounded resident corpus; no roster or allowlist filter",
            "buy_side_counterparty": "maker when decoded maker_side is BUY, otherwise taker; event.side is forbidden because it is the selected-wallet viewpoint",
            "01a_price_band": {"min_inclusive": 0.25, "max_exclusive": 0.32},
            "early_gate_s": 60.0,
            "offset_basis": "block_ts minus BTC-5m window epoch parsed from market slug",
            "offset_semantics": "chain-clock offset; tighter than submitted_at and not interchangeable with submitted_at or our-clock latency",
            "per_wallet_rate_denominator": "same common true first-to-last observed BTC-5m window span used by the union",
            "per_wallet_own_span_rate": "diagnostic only; never summed or used for contribution ranking",
            "union_rate_denominator": "one common true first-to-last observed BTC-5m window span plus 300s",
        },
        "since_block_ts": since_block_ts,
        "distinct_btc5m_01a_windows_observed": len(in_band_windows),
        "distinct_btc5m_01a_within_60s_windows_observed": len(union_qualifying),
        "distinct_wallets_observed": len(per_wallet),
        "distinct_wallets_with_at_least_1_qualifying_window": len(qualifying_wallets),
        "rung_clearing_qualifying_window_count_threshold": rung_count_threshold,
        "rung_clearing_wallet_count": sum(
            1
            for item in qualifying_wallets
            if rung_count_threshold is not None and item["qualifying_window_count"] >= rung_count_threshold
        ),
        "union_qualifying_window_count": len(union_qualifying),
        "union_span_days": round(common_span, 6) if common_span is not None else None,
        "union_span_days_below_1": common_span is not None and common_span < 1.0,
        "union_rate_confidence": (
            "LOW_CONFIDENCE_SUB_DAY_SPAN"
            if common_span is not None and common_span < 1.0
            else ("OBSERVED_AT_LEAST_ONE_DAY" if common_span is not None else "UNOBSERVED")
        ),
        "union_qualifying_01a_windows_per_day": union_daily,
        "first_rung_target_qualifying_01a_windows_per_day": TARGET_WINDOWS_PER_DAY,
        "ranking": per_wallet,
        "diagnostics": diagnostics,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", default="data/research/polygon_orderfilled_ws_shadow_resident.jsonl")
    parser.add_argument("--since-block-ts", type=float)
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--gamma-base-url", default=os.getenv("POLYMARKET_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"))
    parser.add_argument("--gamma-timeout-s", type=float, default=3.0)
    parser.add_argument("--output", default="data/research/orderfilled_early_01a_supply_census_latest.json")
    args = parser.parse_args()

    path = Path(args.events)
    if not path.exists():
        parser.error(f"events file does not exist: {path}")
    with path.open("rb") as events_handle:
        file_size = os.fstat(events_handle.fileno()).st_size
        start_offset, end_offset = max(0, file_size - max(1, int(args.max_bytes))), file_size
        latest_block_ts = 0.0
        earliest_block_ts = 0.0
        starts: set[int] = set()
        for row in _bounded_rows_handle(events_handle, start=start_offset, end=end_offset):
            event = normalize_polygon_orderfilled_row(row)
            if event is None or event.event_ts is None:
                continue
            latest_block_ts = max(latest_block_ts, float(event.event_ts))
            earliest_block_ts = min(earliest_block_ts or float(event.event_ts), float(event.event_ts))
            base = int(float(event.event_ts) // 300) * 300
            starts.update((base - 300, base, base + 300))
        if latest_block_ts <= 0:
            parser.error("bounded tail contains no decodable block timestamps")
        since_block_ts = float(args.since_block_ts) if args.since_block_ts is not None else latest_block_ts - DEFAULT_LOOKBACK_S
        gamma_stats: dict[str, int] = {}
        token_metadata = _gamma_token_metadata(
            str(args.gamma_base_url), starts=starts, timeout_s=float(args.gamma_timeout_s), stats=gamma_stats
        )
        generated_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
        report = build_census(
            _bounded_rows_handle(events_handle, start=start_offset, end=end_offset),
            token_metadata=token_metadata,
            since_block_ts=since_block_ts,
            generated_at=generated_at,
        )
    if report["diagnostics"]["rows_seen"] <= 0 or report["union_span_days"] is None:
        parser.error("stable bounded cut was lost before the second pass; refusing to publish")
    report["input"] = {
        "events": str(path),
        "file_size_bytes_at_open": file_size,
        "tail_start_offset": start_offset,
        "tail_end_offset": end_offset,
        "max_bytes": int(args.max_bytes),
        "tail_truncated": start_offset > 0,
        "latest_block_ts_in_bounded_tail": latest_block_ts,
        "default_lookback_s": DEFAULT_LOOKBACK_S,
        "requested_since_block_ts": args.since_block_ts,
        "earliest_block_ts_in_bounded_tail": earliest_block_ts,
        "bounded_tail_may_cover_less_than_requested_lookback": since_block_ts < earliest_block_ts,
    }
    report["gamma_lookup"] = gamma_stats
    atomic_write_json(Path(args.output), report)
    print(json.dumps({
        "generated_at": generated_at,
        "distinct_btc5m_01a_windows_observed": report["distinct_btc5m_01a_windows_observed"],
        "distinct_wallets_with_at_least_1_qualifying_window": report["distinct_wallets_with_at_least_1_qualifying_window"],
        "rung_clearing_qualifying_window_count_threshold": report["rung_clearing_qualifying_window_count_threshold"],
        "rung_clearing_wallet_count": report["rung_clearing_wallet_count"],
        "union_qualifying_01a_windows_per_day": report["union_qualifying_01a_windows_per_day"],
    }), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
