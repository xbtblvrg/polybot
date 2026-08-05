#!/usr/bin/env python3
"""Build alpha-decay/copyability report from wallet fills and CLOB market data."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_detection_latency import _iter_jsonl
from src.wallet_copy.alpha_decay import (
    ExecutionProfileConfig,
    build_alpha_decay_report,
    build_execution_profiles,
    clob_book_source_diagnostics,
    clob_market_points,
    polygon_fill_observations,
)
from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.profit_engine import load_events_from_history
from src.wallet_copy.store import atomic_write_json


DEFAULT_POLYGON_JSONL = "data/research/polygon_orderfilled_ws_capture_2c_publicnode_v2only_20260703T1546Z.jsonl"
DEFAULT_CLOB_JSONL = "data/research/clob_market_ws_events.jsonl"
DEFAULT_REPORT = "data/research/alpha_decay_report.json"
DEFAULT_ASSET_IDS_OUTPUT = "data/research/alpha_decay_target_asset_ids.json"
DEFAULT_HISTORY_STATE = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_TOKEN_METADATA_CACHE = "data/research/wide_token_metadata_cache.json"
SAME_WINDOW_CAPTURE_DIR = "data/research/same_window_capture"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--polygon-jsonl", default=DEFAULT_POLYGON_JSONL)
    parser.add_argument("--clob-jsonl", action="append", default=[])
    parser.add_argument("--report", default=DEFAULT_REPORT)
    parser.add_argument("--asset-ids-output", default=DEFAULT_ASSET_IDS_OUTPUT)
    parser.add_argument(
        "--history-state",
        default=None,
        help="Optional wallet history state used to map token ids to BTC-5m market windows for per-move slices.",
    )
    parser.add_argument(
        "--token-metadata-cache",
        default=DEFAULT_TOKEN_METADATA_CACHE,
        help="Token-id to market-slug fallback for fills absent from the rolling history join.",
    )
    parser.add_argument(
        "--fill-source",
        action="append",
        default=[],
        help="Polygon OrderFilled source rows to include. Defaults to polygon_ws; add polygon_http_getLogs for LEARN/OBSERVE fallback evidence.",
    )
    parser.add_argument("--sample-limit", type=int, default=500)
    parser.add_argument("--horizon-s", action="append", type=float, default=[])
    parser.add_argument("--profile-horizon-s", type=float, default=2.0)
    parser.add_argument("--profile-min-fills", type=int, default=20)
    parser.add_argument("--profile-min-positive-edge-fraction", type=float, default=0.70)
    parser.add_argument("--profile-min-mean-edge", type=float, default=0.0)
    parser.add_argument("--profile-min-median-edge", type=float, default=0.0)
    parser.add_argument("--profile-max-observation-lag-s", type=float, default=5.0)
    parser.add_argument(
        "--prior-report",
        default="",
        help="Optional prior alpha cut used for signed eligibility deltas.",
    )
    args = parser.parse_args()
    args.history_state_explicit = args.history_state is not None
    args.history_state = args.history_state or DEFAULT_HISTORY_STATE
    return args


def _load_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _latest_same_window_pass_report(root: Path) -> tuple[Path, dict] | None:
    candidates: list[tuple[float, Path, dict]] = []
    capture_root = root / SAME_WINDOW_CAPTURE_DIR
    for path in capture_root.glob("*/run*_final/alpha_decay_report.json"):
        report = _load_json(path)
        alpha = report.get("alpha_decay") if isinstance(report.get("alpha_decay"), dict) else {}
        profiles = report.get("execution_profiles") if isinstance(report.get("execution_profiles"), dict) else {}
        if alpha.get("status") != "PASS":
            continue
        if int(profiles.get("eligible_profile_count") or 0) <= 0:
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:
            mtime = 0.0
        candidates.append((mtime, path, report))
    if not candidates:
        return None
    _, path, report = max(candidates, key=lambda row: (row[0], str(row[1])))
    return path, report


def _using_default_stale_inputs(args: argparse.Namespace, clob_jsonl_paths: list[str]) -> bool:
    return (
        args.polygon_jsonl == DEFAULT_POLYGON_JSONL
        and clob_jsonl_paths == [DEFAULT_CLOB_JSONL]
    )


def _asset_ids_payload(report_path: str, report: dict) -> dict:
    alpha_decay = report.get("alpha_decay") if isinstance(report.get("alpha_decay"), dict) else {}
    book_diagnostics = alpha_decay.get("book_source_diagnostics") if isinstance(alpha_decay.get("book_source_diagnostics"), dict) else {}
    empty_book_assets = {
        str(asset_id)
        for asset_id in (book_diagnostics.get("empty_book_truth_asset_ids") or [])
        if str(asset_id)
    }
    target_assets = [
        row for row in alpha_decay.get("missing_assets_top", [])
        if str(row.get("asset_id") or "") not in empty_book_assets
    ]
    return {
        "updated_at": utc_now_iso(),
        "source_report": report_path,
        "reason": "top_missing_fill_assets_for_next_simultaneous_clob_market_ws_capture",
        "excluded_empty_book_truth_assets": sorted(empty_book_assets),
        "asset_ids": [row["asset_id"] for row in target_assets],
        "assets": target_assets,
    }


def _parse_ts(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _newest_row_ts(rows: list[dict]) -> float:
    return max(
        (
            _parse_ts(
                row.get("captured_at_s")
                or row.get("observed_ts")
                or row.get("event_ts")
                or row.get("timestamp")
                or row.get("captured_at_iso")
            )
            for row in rows
        ),
        default=0.0,
    )


def _promote_same_window_report_if_default_stale(args: argparse.Namespace, clob_jsonl_paths: list[str]) -> dict | None:
    if bool(getattr(args, "history_state_explicit", False)):
        return None
    if not _using_default_stale_inputs(args, clob_jsonl_paths):
        return None
    latest = _latest_same_window_pass_report(ROOT)
    if latest is None:
        return None
    source_path, report = latest
    report = dict(report)
    updated_ts = _parse_ts(report.get("updated_at"))
    age_s = max(0.0, datetime.now(tz=UTC).timestamp() - updated_ts) if updated_ts else None
    history_is_frozen = Path(str(report.get("history_state") or "")).name == "wallet_copy_history_state.json"
    report["source_freshness"] = {
        "newest_source_event_age_s": round(age_s, 6) if age_s is not None else None,
        "freshness_limit_s": 86400.0,
        "history_is_frozen_d97": history_is_frozen,
        "pass": bool(age_s is not None and age_s <= 86400.0 and not history_is_frozen),
        "basis": "promoted same-window artifact updated_at plus history identity",
    }
    report["promotion_grade"] = report["source_freshness"]["pass"]
    report["status"] = "PASS_CURRENT_SOURCE" if report["promotion_grade"] else "STALE_SOURCE_FAIL_CLOSED"
    atomic_write_json(args.report, report)
    if args.report == DEFAULT_REPORT:
        atomic_write_json("data/research/alpha_decay_report_latest.json", report)
    if args.asset_ids_output:
        atomic_write_json(args.asset_ids_output, _asset_ids_payload(args.report, report))
    promoted = dict(report)
    promoted["promoted_from_same_window_report"] = str(source_path)
    print(json.dumps(promoted, indent=2, sort_keys=True))
    return promoted


def _asset_context_from_history(path: str) -> dict[str, dict[str, str]]:
    try:
        events = load_events_from_history(path)
    except Exception:
        return {}
    context: dict[str, dict[str, str]] = {}
    for event in events:
        token_id = str(getattr(event, "token_id", "") or "")
        if not token_id or token_id in context:
            continue
        market_slug = str(getattr(event, "market_slug", "") or "")
        condition_id = str(getattr(event, "condition_id", "") or "")
        if not market_slug and not condition_id:
            continue
        context[token_id] = {
            "market_slug": market_slug,
            "condition_id": condition_id,
        }
    return context


def _asset_context_from_token_metadata(path: str) -> dict[str, dict[str, str]]:
    payload = _load_json(Path(path))
    context: dict[str, dict[str, str]] = {}
    for token_id, row in payload.items():
        if not isinstance(row, dict):
            continue
        market_slug = str(row.get("market_slug") or "")
        condition_id = str(row.get("condition_id") or "")
        if not market_slug and not condition_id:
            continue
        context[str(token_id)] = {
            "market_slug": market_slug,
            "condition_id": condition_id,
        }
    return context


def _merge_asset_context(
    metadata: dict[str, dict[str, str]],
    history: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    """Merge token context with the fresher rolling-history row winning."""

    return {**metadata, **history}


def _eligibility_delta(
    prior_report: dict[str, object],
    current_profiles: dict[str, object],
) -> dict[str, object]:
    prior_profiles = (
        prior_report.get("execution_profiles")
        if isinstance(prior_report.get("execution_profiles"), dict)
        else {}
    )

    def eligible_wallets(payload: dict[str, object]) -> set[str]:
        profiles = payload.get("profiles_by_wallet")
        profiles = profiles if isinstance(profiles, dict) else {}
        return {
            str(wallet)
            for wallet, row in profiles.items()
            if isinstance(row, dict) and row.get("eligible") is True
        }

    def eligible_slices(payload: dict[str, object]) -> set[tuple[str, str]]:
        profiles = payload.get("profiles_by_wallet")
        profiles = profiles if isinstance(profiles, dict) else {}
        return {
            (str(wallet), str(row.get("move_slice_key") or ""))
            for wallet, profile in profiles.items()
            if isinstance(profile, dict)
            for row in (profile.get("move_slices") or [])
            if isinstance(row, dict) and row.get("eligible") is True
        }

    before_wallets = eligible_wallets(prior_profiles)
    after_wallets = eligible_wallets(current_profiles)
    before_slices = eligible_slices(prior_profiles)
    after_slices = eligible_slices(current_profiles)
    return {
        "status": "MEASURED" if prior_report else "PRIOR_REPORT_NOT_SUPPLIED",
        "eligible_profile_count": {"before": len(before_wallets), "after": len(after_wallets)},
        "eligible_profile_wallet_adds": sorted(after_wallets - before_wallets),
        "eligible_profile_wallet_drops": sorted(before_wallets - after_wallets),
        "eligible_move_slice_count": {"before": len(before_slices), "after": len(after_slices)},
        "eligible_move_slice_wallet_adds": sorted(
            {wallet for wallet, _slice in after_slices - before_slices}
        ),
        "eligible_move_slice_wallet_drops": sorted(
            {wallet for wallet, _slice in before_slices - after_slices}
        ),
    }


def _move_slice_context_diagnostics(sample_rows: list[dict]) -> dict[str, object]:
    unknown = [
        row for row in sample_rows
        if str(row.get("seconds_bucket") or "") == "unknown_seconds"
    ]
    return {
        "sample_rows": len(sample_rows),
        "unknown_seconds_rows": len(unknown),
        "unknown_seconds_event_ts_absent": sum(row.get("block_ts") is None for row in unknown),
        "unknown_seconds_market_slug_absent": sum(not str(row.get("market_slug") or "") for row in unknown),
        "unknown_seconds_non_btc_slug": sum(
            bool(str(row.get("market_slug") or ""))
            and not str(row.get("market_slug") or "").startswith("btc-updown-5m-")
            for row in unknown
        ),
        "join_seam": "polygon fill asset_id -> rolling history token_id, then wide token metadata fallback",
    }


def _fill_source_diagnostics(rows: list[dict]) -> dict[str, object]:
    event_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for row in rows:
        event = str(row.get("event") or "")
        source = str(row.get("source") or "")
        if event:
            event_counts[event] = event_counts.get(event, 0) + 1
        if source:
            source_counts[source] = source_counts.get(source, 0) + 1
    ws_open = int(event_counts.get("polygon_ws_connection_open") or 0)
    ws_close = int(event_counts.get("polygon_ws_connection_close") or 0)
    ws_error = int(event_counts.get("polygon_ws_connection_error") or 0)
    return {
        "raw_event_counts": dict(sorted(event_counts.items())),
        "raw_source_counts": dict(sorted(source_counts.items())),
        "ws_flap_cycle_count": max(ws_open, ws_close, ws_error),
        "ws_connection_open_count": ws_open,
        "ws_connection_close_count": ws_close,
        "ws_connection_error_count": ws_error,
    }


def main() -> int:
    args = parse_args()
    horizons = tuple(args.horizon_s or [1.0, 2.0, 5.0, 30.0])
    fill_sources = tuple(args.fill_source or ["polygon_ws"])
    clob_jsonl_paths = args.clob_jsonl or [DEFAULT_CLOB_JSONL]
    if _promote_same_window_report_if_default_stale(args, clob_jsonl_paths) is not None:
        return 0
    history_asset_context = _asset_context_from_history(args.history_state)
    metadata_asset_context = _asset_context_from_token_metadata(
        args.token_metadata_cache
    )
    asset_context = _merge_asset_context(metadata_asset_context, history_asset_context)
    polygon_rows = list(_iter_jsonl(args.polygon_jsonl))
    fills = polygon_fill_observations(
        polygon_rows,
        sources=fill_sources,
        asset_context=asset_context,
    )
    clob_rows: list[dict] = []
    for path in clob_jsonl_paths:
        clob_rows.extend(_iter_jsonl(path))
    points = clob_market_points(clob_rows)
    alpha_decay = build_alpha_decay_report(
        fills,
        points,
        horizons_s=horizons,
        sample_limit=int(args.sample_limit),
        book_source_diagnostics=clob_book_source_diagnostics(clob_rows, fill_assets={fill.asset_id for fill in fills}),
        fill_source_diagnostics=_fill_source_diagnostics(polygon_rows),
    )
    execution_profiles = build_execution_profiles(
        alpha_decay,
        config=ExecutionProfileConfig(
            latency_horizon_s=float(args.profile_horizon_s),
            min_fills=int(args.profile_min_fills),
            min_positive_edge_fraction=float(args.profile_min_positive_edge_fraction),
            min_mean_edge=float(args.profile_min_mean_edge),
            min_median_edge=float(args.profile_min_median_edge),
            max_observation_lag_s=float(args.profile_max_observation_lag_s),
        ),
    )
    now_ts = datetime.now(tz=UTC).timestamp()
    polygon_newest_ts = _newest_row_ts(polygon_rows)
    clob_newest_ts = _newest_row_ts(clob_rows)
    polygon_age_s = max(0.0, now_ts - polygon_newest_ts) if polygon_newest_ts else None
    clob_age_s = max(0.0, now_ts - clob_newest_ts) if clob_newest_ts else None
    history_is_frozen = Path(args.history_state).name == "wallet_copy_history_state.json"
    history_path = Path(args.history_state)
    try:
        history_mtime_age_s = max(0.0, now_ts - history_path.stat().st_mtime)
    except OSError:
        history_mtime_age_s = None
    history_current_source_pass = bool(
        not history_is_frozen
        and history_mtime_age_s is not None
        and history_mtime_age_s <= 86400.0
    )
    promotion_grade = bool(
        history_current_source_pass
        and polygon_age_s is not None
        and clob_age_s is not None
        and polygon_age_s <= 86400.0
        and clob_age_s <= 86400.0
    )
    report = {
        "updated_at": utc_now_iso(),
        "status": "PASS_CURRENT_SOURCE" if promotion_grade else "STALE_SOURCE_FAIL_CLOSED",
        "promotion_grade": promotion_grade,
        "paper_only": True,
        "live_orders_allowed": False,
        "flow_stages": ["OBSERVE", "LEARN"],
        "polygon_jsonl": args.polygon_jsonl,
        "history_state": args.history_state,
        "asset_context_entries": len(asset_context),
        "asset_context_sources": {
            "rolling_history_entries": len(history_asset_context),
            "token_metadata_entries": len(metadata_asset_context),
            "merged_entries": len(asset_context),
            "history_precedence": True,
        },
        "fill_sources": list(fill_sources),
        "clob_jsonl": clob_jsonl_paths,
        "source_freshness": {
            "newest_source_event_age_s": round(max(polygon_age_s, clob_age_s), 6)
            if polygon_age_s is not None and clob_age_s is not None
            else None,
            "polygon_newest_age_s": round(polygon_age_s, 6) if polygon_age_s is not None else None,
            "clob_newest_age_s": round(clob_age_s, 6) if clob_age_s is not None else None,
            "freshness_limit_s": 86400.0,
            "history_is_frozen_d97": history_is_frozen,
            "history_state_explicit": bool(getattr(args, "history_state_explicit", False)),
            "history_state_mtime_age_s": round(history_mtime_age_s, 6)
            if history_mtime_age_s is not None
            else None,
            "history_current_source_pass": history_current_source_pass,
            "pass": promotion_grade,
        },
        "alpha_decay": alpha_decay,
        "move_slice_context_diagnostics": _move_slice_context_diagnostics(
            alpha_decay.get("sample_rows")
            if isinstance(alpha_decay.get("sample_rows"), list)
            else []
        ),
        "execution_profiles": execution_profiles,
        "eligibility_delta": _eligibility_delta(
            _load_json(Path(args.prior_report)) if args.prior_report else {},
            execution_profiles,
        ),
    }
    atomic_write_json(args.report, report)
    if args.asset_ids_output:
        atomic_write_json(args.asset_ids_output, _asset_ids_payload(args.report, report))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
