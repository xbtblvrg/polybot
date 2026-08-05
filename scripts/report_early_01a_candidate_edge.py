#!/usr/bin/env python3
"""Join early-01a OrderFilled supply candidates to resolved BTC-5m outcomes."""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.merge_rtds_wallet_events import _gamma_token_metadata  # noqa: E402
from scripts.report_orderfilled_early_01a_supply_census import (  # noqa: E402
    DEFAULT_LOOKBACK_S,
    DEFAULT_MAX_BYTES,
    TARGET_WINDOWS_PER_DAY,
    WINDOW_RE,
    _bounded_rows,
    _bounded_rows_handle,
    _span_days,
    _tail_bounds,
)
from src.wallet_copy.polymarket_addresses import EXCHANGE_ADDRESSES  # noqa: E402
from src.wallet_copy.realtime_feed import normalize_polygon_orderfilled_row  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402

COHORT_REFERENCE_BREAKEVEN_PCT = 23.861261
MIN_RESOLVED = 30
QUOTING_COVERAGE_PCT = 95.0


def _resolution_index(rows: Iterable[dict[str, Any]]) -> dict[str, str]:
    resolutions: dict[str, str] = {}
    for row in rows:
        winner = str(row.get("direction") or row.get("winning_outcome") or "").strip().upper()
        if winner not in {"UP", "DOWN"}:
            continue
        for value in (row.get("market_slug"), row.get("condition_id"), row.get("market")):
            key = str(value or "").strip().lower()
            if key:
                resolutions[key] = winner
    return resolutions


def _valid_wallet(value: Any) -> str:
    wallet = str(value or "").lower()
    return wallet if re.fullmatch(r"0x[0-9a-f]{40}", wallet) else ""


def build_report(
    rows: Iterable[dict[str, Any]],
    *,
    token_metadata: dict[str, dict[str, str]],
    resolution_rows: Iterable[dict[str, Any]],
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
        "outside_window": 0,
        "qualifying_pairs_unresolved": 0,
        "qualifying_pairs_missing_outcome": 0,
        "attributions_to_settlement_contracts": 0,
    }
    resolutions = _resolution_index(resolution_rows)
    common_starts: set[int] = set()
    in_band_windows: set[str] = set()
    wallet_observed: dict[str, set[str]] = {}
    # The earliest qualifying buy is the copy trigger represented by one
    # (wallet, window) unit. It prevents repeated quoting from inflating n.
    earliest_pair: dict[tuple[str, str], tuple[float, str, str, str, float]] = {}

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
            diagnostics["outside_window"] += 1
            continue
        wallet = _valid_wallet(event.maker if str(event.maker_side).upper() == "BUY" else event.taker)
        if not wallet:
            diagnostics["required_field_missing"] += 1
            continue
        if wallet in EXCHANGE_ADDRESSES:
            diagnostics["attributions_to_settlement_contracts"] += 1
            continue
        common_starts.add(start)
        wallet_observed.setdefault(wallet, set()).add(slug)
        price = float(event.price)
        if not 0.25 <= price < 0.32:
            continue
        in_band_windows.add(slug)
        if offset_s >= 60.0:
            continue
        outcome = str(metadata.get("outcome") or "").strip().upper()
        gamma_winner = str(metadata.get("winning_outcome") or "").strip().upper()
        key = (wallet, slug)
        candidate = (block_ts, str(event.asset), outcome, gamma_winner, price)
        if key not in earliest_pair or candidate[:2] < earliest_pair[key][:2]:
            earliest_pair[key] = candidate

    common_span = _span_days(common_starts)
    rung_threshold = math.ceil(TARGET_WINDOWS_PER_DAY * common_span) if common_span is not None else None
    pair_count: dict[str, int] = {}
    pair_wins: dict[str, int] = {}
    pair_resolved: dict[str, int] = {}
    resolved_pairs: dict[str, list[tuple[str, float, bool]]] = {}
    gamma_resolution_pairs = 0
    for (wallet, slug), (_block_ts, _asset, outcome, gamma_winner, price) in earliest_pair.items():
        pair_count[wallet] = pair_count.get(wallet, 0) + 1
        if outcome not in {"UP", "DOWN"}:
            diagnostics["qualifying_pairs_missing_outcome"] += 1
            continue
        winner = resolutions.get(slug.lower())
        if not winner and gamma_winner in {"UP", "DOWN"}:
            winner = gamma_winner
            gamma_resolution_pairs += 1
        if not winner:
            diagnostics["qualifying_pairs_unresolved"] += 1
            continue
        pair_resolved[wallet] = pair_resolved.get(wallet, 0) + 1
        won = outcome == winner
        pair_wins[wallet] = pair_wins.get(wallet, 0) + int(won)
        resolved_pairs.setdefault(wallet, []).append((slug, price, won))
    diagnostics["qualifying_pairs_resolved_from_gamma_metadata"] = gamma_resolution_pairs

    candidates: list[dict[str, Any]] = []
    in_band_denominator = len(in_band_windows)
    presence_denominator = len(common_starts)
    for wallet, qualifying_count in pair_count.items():
        if rung_threshold is None or qualifying_count < rung_threshold:
            continue
        n_resolved = pair_resolved.get(wallet, 0)
        wins = pair_wins.get(wallet, 0)
        n_distinct_windows = len({slug for slug, _price, _won in resolved_pairs.get(wallet, [])})
        resolved_to_distinct_window_ratio = round(n_resolved / n_distinct_windows, 6) if n_distinct_windows else None
        win_rate = round(100.0 * wins / n_resolved, 6) if n_resolved else None
        pairs = resolved_pairs.get(wallet, [])
        mean_entry = round(sum(price for _slug, price, _won in pairs) / n_resolved, 6) if n_resolved else None
        flat_pnl = sum(((1.0 / price) if won else 0.0) - 1.0 for _slug, price, won in pairs)
        flat_roi = round(100.0 * flat_pnl / n_resolved, 6) if n_resolved else None
        own_count_breakeven = round(100.0 * mean_entry, 6) if mean_entry is not None else None
        observed_windows = len(wallet_observed.get(wallet, set()))
        in_band_coverage = round(100.0 * qualifying_count / in_band_denominator, 6) if in_band_denominator else None
        presence_coverage = round(100.0 * observed_windows / presence_denominator, 6) if presence_denominator else None
        suspected_quoting = presence_coverage is not None and presence_coverage >= QUOTING_COVERAGE_PCT
        clears_edge = (
            n_distinct_windows >= MIN_RESOLVED
            and flat_roi is not None
            and flat_roi > 0.0
            and win_rate is not None
            and own_count_breakeven is not None
            and win_rate > own_count_breakeven
        )
        candidates.append(
            {
                "wallet": wallet,
                "qualifying_window_count": qualifying_count,
                "n_resolved": n_resolved,
                "n_distinct_windows": n_distinct_windows,
                "resolved_to_distinct_window_ratio": resolved_to_distinct_window_ratio,
                "wins": wins,
                "realized_01a_win_rate_pct": win_rate,
                "cost_weighted_mean_entry_price": mean_entry,
                "own_count_breakeven_win_rate_pct": own_count_breakeven,
                "realized_flat_1usd_pnl_usd": round(flat_pnl, 6),
                "realized_flat_1usd_roi_pct": flat_roi,
                "cohort_cost_weighted_win_share_breakeven_reference_pct": COHORT_REFERENCE_BREAKEVEN_PCT,
                "edge_over_own_count_breakeven_pp": (
                    round(win_rate - own_count_breakeven, 6)
                    if win_rate is not None and own_count_breakeven is not None
                    else None
                ),
                "windows_observed": observed_windows,
                "windows_in_band": in_band_denominator,
                "total_distinct_btc5m_windows_in_span": presence_denominator,
                "in_band_coverage_pct": in_band_coverage,
                "presence_coverage_pct": presence_coverage,
                "suspected_two_sided_quoting": suspected_quoting,
                "grade": (
                    "OUTCOME_CORRELATED_POOL_NOT_INDEPENDENT"
                    if resolved_to_distinct_window_ratio is not None and resolved_to_distinct_window_ratio >= 2.0
                    else "INSUFFICIENT_N"
                    if n_distinct_windows < MIN_RESOLVED
                    else "PASS_POSITIVE_ROI_ABOVE_OWN_BREAKEVEN"
                    if clears_edge
                    else "FAIL_NONPOSITIVE_ROI_OR_OWN_BREAKEVEN"
                ),
            }
        )
    candidates.sort(
        key=lambda item: (
            -int(item["grade"] == "PASS_POSITIVE_ROI_ABOVE_OWN_BREAKEVEN"),
            -(item["realized_flat_1usd_roi_pct"] or -999.0),
            -item["n_resolved"],
            item["wallet"],
        )
    )
    edge_pass = [item for item in candidates if item["grade"] == "PASS_POSITIVE_ROI_ABOVE_OWN_BREAKEVEN"]

    def pooled(wallets: set[str]) -> dict[str, Any]:
        pairs = [pair for wallet in wallets for pair in resolved_pairs.get(wallet, [])]
        n_resolved = len(pairs)
        n_distinct_windows = len({slug for slug, _price, _won in pairs})
        resolved_to_distinct_window_ratio = round(n_resolved / n_distinct_windows, 6) if n_distinct_windows else None
        wins = sum(won for _slug, _price, won in pairs)
        mean_entry = sum(price for _slug, price, _won in pairs) / n_resolved if n_resolved else None
        pnl = sum(((1.0 / price) if won else 0.0) - 1.0 for _slug, price, won in pairs)
        win_rate = round(100.0 * wins / n_resolved, 6) if n_resolved else None
        roi = round(100.0 * pnl / n_resolved, 6) if n_resolved else None
        implausible = bool((roi is not None and roi > 30.0) or (win_rate is not None and win_rate > 40.0))
        correlated = bool(resolved_to_distinct_window_ratio is not None and resolved_to_distinct_window_ratio >= 2.0)
        return {
            "candidate_wallet_count": len(wallets),
            "n_resolved": n_resolved,
            "n_distinct_windows": n_distinct_windows,
            "resolved_to_distinct_window_ratio": resolved_to_distinct_window_ratio,
            "wins": wins,
            "count_win_rate_pct": win_rate,
            "cost_weighted_mean_entry_price": round(mean_entry, 6) if mean_entry is not None else None,
            "realized_flat_1usd_pnl_usd": round(pnl, 6),
            "realized_flat_1usd_roi_pct": roi,
            "sanity_status": "IMPLAUSIBLE_REVIEW_INSTRUMENT" if implausible else "WITHIN_SANITY_BRACKET",
            "grade": (
                "OUTCOME_CORRELATED_POOL_NOT_INDEPENDENT"
                if correlated
                else "IMPLAUSIBLE_REVIEW_INSTRUMENT"
                if implausible
                else "POSITIVE_POOLED_ROI"
                if roi is not None and roi > 0.0
                else "NONPOSITIVE_POOLED_ROI"
            ),
        }

    all_candidate_wallets = {item["wallet"] for item in candidates}
    nonquoting_wallets = {item["wallet"] for item in candidates if not item["suspected_two_sided_quoting"]}
    pooled_including = pooled(all_candidate_wallets)
    pooled_excluding = pooled(nonquoting_wallets)
    return {
        "kind": "early_01a_candidate_edge",
        "generated_at": generated_at,
        "flow_stage": "MINE/MEASURE/MONEY",
        "paper_only": True,
        "live_mutation": False,
        "measurement_contract": {
            "candidate_gate": "qualifying_window_count >= ceil(30 * common_span_days)",
            "candidate_unit": "earliest qualifying BUY-side (wallet, BTC-5m window) pair",
            "confidence_unit": "n_distinct_windows; n_resolved is disclosure only because wallet pools share BTC-5m outcomes",
            "qualifying_gate": "price in [0.25,0.32) and chain-clock offset <60s",
            "outcome_source": "resolved BTC-5m direction indexed by market_slug",
            "primary_acceptance": "pooled realized ROI on flat $1 with n_distinct_windows confidence and correlation refusal",
            "secondary_per_wallet_gate": f"positive flat-$1 ROI and count win rate above own mean-entry breakeven at n_distinct_windows >= {MIN_RESOLVED}",
            "correlation_refusal": "OUTCOME_CORRELATED_POOL_NOT_INDEPENDENT when n_resolved / n_distinct_windows >= 2.0",
            "cohort_reference_only_not_a_gate_pct": COHORT_REFERENCE_BREAKEVEN_PCT,
            "quoting_flag": f"presence_coverage_pct >= {QUOTING_COVERAGE_PCT}%",
        },
        "since_block_ts": since_block_ts,
        "common_span_days": round(common_span, 6) if common_span is not None else None,
        "rung_clearing_qualifying_window_count_threshold": rung_threshold,
        "distinct_btc5m_01a_windows_observed": in_band_denominator,
        "rung_clearing_candidate_count": len(candidates),
        "per_wallet_gradable_at_n_gte_30_count": sum(
            item["n_distinct_windows"] >= MIN_RESOLVED for item in candidates
        ),
        "secondary_per_wallet_positive_roi_pass_count": len(edge_pass),
        "pooled_cohort_including_quoting": pooled_including,
        "pooled_cohort_excluding_suspected_quoting": pooled_excluding,
        "publication_status": (
            "QUARANTINED_OUTCOME_CORRELATED_POOL"
            if "OUTCOME_CORRELATED_POOL_NOT_INDEPENDENT"
            in {pooled_including["grade"], pooled_excluding["grade"]}
            else "QUARANTINED_IMPLAUSIBLE_REVIEW_INSTRUMENT"
            if "IMPLAUSIBLE_REVIEW_INSTRUMENT"
            in {pooled_including["sanity_status"], pooled_excluding["sanity_status"]}
            else "PUBLISHABLE_MEASUREMENT"
        ),
        "candidates": candidates,
        "diagnostics": diagnostics,
    }


def _jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", default="data/research/polygon_orderfilled_ws_shadow_resident.jsonl")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--since-block-ts", type=float)
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--gamma-base-url", default=os.getenv("POLYMARKET_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"))
    parser.add_argument("--gamma-timeout-s", type=float, default=3.0)
    parser.add_argument("--output", default="data/research/early_01a_candidate_edge_latest.json")
    args = parser.parse_args()

    events_path = Path(args.events)
    resolutions_path = Path(args.resolutions)
    if not events_path.exists() or not resolutions_path.exists():
        parser.error("events and resolutions inputs must exist")
    with events_path.open("rb") as events_handle:
        file_size = os.fstat(events_handle.fileno()).st_size
        start_offset, end_offset = max(0, file_size - max(1, int(args.max_bytes))), file_size
        latest_block_ts = 0.0
        earliest_block_ts = 0.0
        starts: set[int] = set()
        for row in _bounded_rows_handle(events_handle, start=start_offset, end=end_offset):
            event = normalize_polygon_orderfilled_row(row)
            if event is None or event.event_ts is None:
                continue
            event_ts = float(event.event_ts)
            latest_block_ts = max(latest_block_ts, event_ts)
            earliest_block_ts = min(earliest_block_ts or event_ts, event_ts)
            base = int(event_ts // 300) * 300
            starts.update((base - 300, base, base + 300))
        if latest_block_ts <= 0:
            parser.error("bounded tail contains no decodable block timestamps")
        since_block_ts = float(args.since_block_ts) if args.since_block_ts is not None else latest_block_ts - DEFAULT_LOOKBACK_S
        gamma_stats: dict[str, int] = {}
        token_metadata = _gamma_token_metadata(
            str(args.gamma_base_url), starts=starts, timeout_s=float(args.gamma_timeout_s), stats=gamma_stats
        )
        generated_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
        report = build_report(
            _bounded_rows_handle(events_handle, start=start_offset, end=end_offset),
            token_metadata=token_metadata,
            resolution_rows=_jsonl(resolutions_path),
            since_block_ts=since_block_ts,
            generated_at=generated_at,
        )
    if report["diagnostics"]["rows_seen"] <= 0 or report["common_span_days"] is None:
        parser.error("stable bounded cut was lost before the second pass; refusing to publish")
    report["input"] = {
        "events": str(events_path),
        "resolutions": str(resolutions_path),
        "file_size_bytes_at_open": file_size,
        "tail_start_offset": start_offset,
        "tail_end_offset": end_offset,
        "max_bytes": int(args.max_bytes),
        "tail_truncated": start_offset > 0,
        "latest_block_ts_in_bounded_tail": latest_block_ts,
        "earliest_block_ts_in_bounded_tail": earliest_block_ts,
        "bounded_tail_may_cover_less_than_requested_lookback": since_block_ts < earliest_block_ts,
    }
    report["gamma_lookup"] = gamma_stats
    atomic_write_json(Path(args.output), report)
    print(
        json.dumps(
            {
                "generated_at": generated_at,
                "rung_clearing_candidate_count": report["rung_clearing_candidate_count"],
                "pooled_roi_including_quoting_pct": report["pooled_cohort_including_quoting"]["realized_flat_1usd_roi_pct"],
                "pooled_roi_excluding_quoting_pct": report["pooled_cohort_excluding_suspected_quoting"]["realized_flat_1usd_roi_pct"],
                "per_wallet_gradable_at_n_gte_30_count": report["per_wallet_gradable_at_n_gte_30_count"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
