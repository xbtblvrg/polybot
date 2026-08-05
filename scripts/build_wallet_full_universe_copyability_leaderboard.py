#!/usr/bin/env python3
"""Score every wallet in the registry for copyability.

Flow stage: LEARN/PROMOTE. This is the full-universe pass ordered by Fable:
every registered wallet gets a row, while only wallets with positive
copy-PnL and sufficient paper evidence enter the ranked staging queue.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_strategy_decompiler_intake import (  # noqa: E402
    _default_resolutions_path,
    _float,
    _load_resolutions,
    _norm_outcome,
    _parse_ts,
    _utc_now_iso,
)
from scripts.build_queue_clearance_gaps import (  # noqa: E402
    _attributable_reject_metrics,
    _prospective_reject_taxonomy,
)


DEFAULT_REGISTRY = "configs/wallet_copy/wallets.json"
DEFAULT_HISTORY = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_REPLAY = "data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json"
DEFAULT_SHORTLIST = "data/research/active_set_expansion_full_pool_shortlist.json"
DEFAULT_FOLLOWABILITY = "data/research/wallet_copy_followability_leaderboard_latest.json"
DEFAULT_ACTIVE_SET_OVERLAY = "data/research/wallet_copy_active_set_auto_degrade_state.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_full_universe_copyability_latest.json"
LEGACY_OUTPUTS = (
    "data/research/wallet_copy_full_universe_copyability_leaderboard_latest.json",
    "data/research/wallet_copy_full_universe_copyability_leaderboard_summary_latest.json",
)


def _write_legacy_deprecated_pointers(root: Path, canonical_output: Path, generated_at: str) -> None:
    try:
        canonical_path = str(canonical_output.relative_to(root))
    except ValueError:
        canonical_path = str(canonical_output)
    pointer = {
        "schema_version": 1,
        "kind": "wallet_copy_full_universe_copyability_deprecated_pointer",
        "status": "DEPRECATED_POINTER",
        "generated_at": generated_at,
        "canonical_path": canonical_path,
        "reason": "legacy leaderboard-named twin retired to prevent stale contradictory audit reads",
    }
    for legacy in LEGACY_OUTPUTS:
        target = root / legacy
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(pointer, indent=2, sort_keys=True) + "\n")


def _load_json(path: Path, default: Any) -> Any:
    try:
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                return json.load(handle)
        return json.loads(path.read_text())
    except Exception:
        return default


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _prior_live_dispositions(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return explicit negative live dispositions that require fresh re-admission evidence."""
    rows = payload.get("members") if isinstance(payload.get("members"), list) else []
    dispositions: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("source_wallet") or row.get("wallet"))
        status = str(row.get("status") or "").upper()
        disabled = row.get("enabled") is False or status.startswith(("DEMOTED", "DISABLED"))
        if not wallet or not disabled:
            continue
        dispositions[wallet] = {
            "status": status or "DISABLED",
            "disabled_at": row.get("disabled_at") or row.get("demoted_at"),
            "reason": row.get("disable_reason") or row.get("demotion_reason") or "",
        }
    return dispositions


def _stake(row: dict[str, Any]) -> float:
    price = _float(row.get("price"), 0.0)
    size = _float(row.get("size"), 0.0)
    return _float(row.get("usdc_size"), 0.0) or (price * size if price > 0 and size > 0 else 0.0)


def _iso(ts: float | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(float(ts), tz=UTC).isoformat().replace("+00:00", "Z")


def _new_row(wallet: str, *, registry_row: dict[str, Any] | None = None) -> dict[str, Any]:
    registry_row = registry_row if isinstance(registry_row, dict) else {}
    return {
        "wallet": wallet,
        "registry_name": registry_row.get("name") or "",
        "registry_enabled": registry_row.get("enabled") is not False,
        "registry_tags": [str(item) for item in (registry_row.get("tags") or []) if str(item or "")],
        "registry_status": "REGISTERED" if registry_row else "UNREGISTERED_EVIDENCE",
        "resolved_buy_events": 0,
        "source_wins": 0,
        "source_stake_usd": 0.0,
        "source_pnl_usd": 0.0,
        "conditions": set(),
        "markets": set(),
        "activity_hours_utc": set(),
        "first_event_ts": None,
        "latest_event_ts": None,
        "replay_candidate_id": "",
        "replay_status": "MISSING",
        "replay_policy_id": "",
        "paper_pnl_usd": None,
        "paper_orders": 0,
        "resolved_orders": 0,
        "copyable_buy_events": 0,
        "candidate_clob_backed_orders": 0,
        "unresolved_ratio": None,
        "replay_unique_windows": 0,
        "replay_total_source_usd": 0.0,
        "max_buy_price": None,
        "failure_reasons": [],
        "reject_attribution": {},
        "best_band": {},
        "shortlist_source": "",
        "shortlist_resolved_pnl": None,
        "shortlist_copyable_rate_pct": None,
        "shortlist_fill_sample": None,
        "shortlist_mean_edge": None,
        "shortlist_median_edge": None,
        "followability_score": 0.0,
        "followability_windows": 0,
        "followability_predictiveness_pct": None,
        "followability_win_rate_pct": None,
        "followability_avg_continuation_usd": None,
    }


def _registry_rows(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for item in payload.get("wallets") or []:
        if not isinstance(item, dict):
            continue
        wallet = _norm_wallet(item.get("address") or item.get("wallet"))
        if wallet:
            rows[wallet] = _new_row(wallet, registry_row=item)
    return rows


def _ensure_row(rows: dict[str, dict[str, Any]], wallet: str) -> dict[str, Any]:
    row = rows.get(wallet)
    if row is None:
        row = _new_row(wallet)
        rows[wallet] = row
    return row


def _add_history(rows: dict[str, dict[str, Any]], root: Path, args: argparse.Namespace) -> dict[str, int]:
    history = _load_json(root / args.history, {})
    resolutions_path = root / args.resolutions if args.resolutions else _default_resolutions_path(root)
    winners = _load_resolutions(resolutions_path)
    events = history.get("events") if isinstance(history.get("events"), list) else []
    skipped = {"non_buy": 0, "unresolved": 0, "missing_wallet": 0, "bad_price_or_side": 0}
    for event in events:
        if not isinstance(event, dict):
            continue
        if str(event.get("action") or "").upper() != "BUY":
            skipped["non_buy"] += 1
            continue
        wallet = _norm_wallet(event.get("source_wallet") or event.get("wallet"))
        if not wallet:
            skipped["missing_wallet"] += 1
            continue
        winner = winners.get(str(event.get("market_slug") or "")) or winners.get(str(event.get("condition_id") or ""))
        if not winner:
            skipped["unresolved"] += 1
            continue
        outcome = _norm_outcome(event.get("outcome"))
        price = _float(event.get("price"), 0.0)
        stake = _stake(event)
        if not outcome or price <= 0.0 or price >= 1.0 or stake <= 0:
            skipped["bad_price_or_side"] += 1
            continue
        row = _ensure_row(rows, wallet)
        win = outcome == winner
        ts = _parse_ts(event.get("event_ts") or event.get("observed_ts"))
        row["resolved_buy_events"] += 1
        row["source_wins"] += int(win)
        row["source_stake_usd"] += stake
        row["source_pnl_usd"] += ((1.0 / price - 1.0) if win else -1.0) * stake
        if event.get("condition_id"):
            row["conditions"].add(str(event.get("condition_id")))
        if event.get("market_slug"):
            row["markets"].add(str(event.get("market_slug")))
        if ts > 0:
            row["activity_hours_utc"].add(datetime.fromtimestamp(ts, tz=UTC).hour)
            row["first_event_ts"] = ts if row["first_event_ts"] is None else min(row["first_event_ts"], ts)
            row["latest_event_ts"] = ts if row["latest_event_ts"] is None else max(row["latest_event_ts"], ts)
    skipped["events_scanned"] = len(events)
    return skipped


def _replay_score(candidate: dict[str, Any]) -> tuple[float, int, int, int]:
    replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
    return (
        _float(replay.get("paper_pnl_usd"), -1_000_000.0),
        int(replay.get("copyable_buy_events") or 0),
        int(replay.get("candidate_clob_backed_orders") or 0),
        int(replay.get("resolved_orders") or 0),
    )


def _add_replay(rows: dict[str, dict[str, Any]], root: Path, args: argparse.Namespace) -> dict[str, int]:
    replay_payload = _load_json(root / args.replay, {})
    best: dict[str, dict[str, Any]] = {}
    for candidate in replay_payload.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        wallet = _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet"))
        if not wallet:
            continue
        current = best.get(wallet)
        if current is None or _replay_score(candidate) > _replay_score(current):
            best[wallet] = candidate
    for wallet, candidate in best.items():
        replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
        row = _ensure_row(rows, wallet)
        row["replay_candidate_id"] = str(candidate.get("candidate_id") or "")
        row["replay_status"] = str(replay.get("eligibility_status") or replay.get("status") or "UNKNOWN")
        row["replay_policy_id"] = str(replay.get("policy_id") or "")
        row["paper_pnl_usd"] = _float(replay.get("paper_pnl_usd"), 0.0)
        row["paper_orders"] = int(replay.get("paper_orders") or 0)
        row["resolved_orders"] = int(replay.get("resolved_orders") or 0)
        row["copyable_buy_events"] = int(replay.get("copyable_buy_events") or 0)
        row["candidate_clob_backed_orders"] = int(replay.get("candidate_clob_backed_orders") or 0)
        row["unresolved_ratio"] = _float(replay.get("unresolved_ratio"), None)
        row["replay_unique_windows"] = int(candidate.get("unique_windows") or 0)
        row["replay_total_source_usd"] = _float(candidate.get("total_source_usd"), 0.0)
        row["max_buy_price"] = replay.get("max_buy_price")
        row["failure_reasons"] = [str(item) for item in (replay.get("failure_reasons") or []) if str(item or "")]
        reject_reasons: Counter[str] = Counter()
        filled_orders = 0
        for order in replay.get("replay_orders") or []:
            if not isinstance(order, dict):
                continue
            status = str(order.get("final_status") or order.get("status") or "").upper()
            if status == "FILLED":
                filled_orders += 1
                continue
            if status != "REJECTED":
                continue
            fill_estimate = order.get("fill_estimate") if isinstance(order.get("fill_estimate"), dict) else {}
            reject_details = (
                fill_estimate.get("reject_details")
                if isinstance(fill_estimate.get("reject_details"), dict)
                else {}
            )
            reason = str(
                reject_details.get("blocking_reason")
                or fill_estimate.get("blocking_reason")
                or order.get("dominant_skip_reason")
                or "unknown_reject"
            )
            reject_reasons[reason] += 1
        metrics = _attributable_reject_metrics(
            filled_orders=filled_orders,
            reject_reasons=reject_reasons,
            max_rejected_fill_ratio=0.60,
        )
        row["reject_attribution"] = {
            **metrics,
            "prospective_reject_taxonomy": _prospective_reject_taxonomy(reject_reasons),
            "raw_reject_count": sum(reject_reasons.values()),
            "reject_reasons": dict(sorted(reject_reasons.items())),
        }
    return {
        "replay_candidates": len(replay_payload.get("candidates") or []),
        "wallets_with_replay": len(best),
    }


def _best_band(shortlist_row: dict[str, Any]) -> dict[str, Any]:
    profile = shortlist_row.get("eligible_profile") if isinstance(shortlist_row.get("eligible_profile"), dict) else {}
    band = profile.get("best_eligible_move_slice") if isinstance(profile.get("best_eligible_move_slice"), dict) else {}
    return {
        "move_slice_key": band.get("move_slice_key") or "",
        "seconds_bucket": band.get("seconds_bucket") or "",
        "entry_price_band": band.get("entry_price_band") or "",
        "latency_horizon_s": band.get("latency_horizon_s"),
        "copyable_rate_pct": band.get("copyable_rate_pct"),
        "fill_sample": band.get("fill_sample"),
        "mean_edge": band.get("mean_edge"),
        "median_edge": band.get("median_edge"),
        "status": band.get("status") or profile.get("status") or "",
    }


def _add_shortlist(rows: dict[str, dict[str, Any]], root: Path, args: argparse.Namespace) -> int:
    shortlist = _load_json(root / args.shortlist, {})
    count = 0
    for source_key in ("top_candidates", "pnl_only_no_lane_evidence"):
        for item in shortlist.get(source_key) or []:
            if not isinstance(item, dict):
                continue
            wallet = _norm_wallet(item.get("wallet"))
            if not wallet:
                continue
            row = _ensure_row(rows, wallet)
            row["shortlist_source"] = source_key
            row["best_band"] = _best_band(item)
            row["shortlist_resolved_pnl"] = item.get("resolved_pnl")
            row["shortlist_copyable_rate_pct"] = item.get("copyable_rate_pct")
            row["shortlist_fill_sample"] = item.get("fill_sample")
            row["shortlist_mean_edge"] = item.get("mean_edge")
            row["shortlist_median_edge"] = item.get("median_edge")
            count += 1
    return count


def _add_followability(rows: dict[str, dict[str, Any]], root: Path, args: argparse.Namespace) -> int:
    followability = _load_json(root / args.followability, {})
    count = 0
    for item in followability.get("leaderboard") or followability.get("selected_wallets") or []:
        if not isinstance(item, dict):
            continue
        wallet = _norm_wallet(item.get("wallet"))
        if not wallet:
            continue
        row = _ensure_row(rows, wallet)
        row["followability_score"] = _float(item.get("followability_score"), 0.0)
        row["followability_windows"] = int(item.get("eligible_windows") or 0)
        row["followability_predictiveness_pct"] = item.get("early_side_predictiveness_pct")
        row["followability_win_rate_pct"] = item.get("early_win_rate_pct")
        row["followability_avg_continuation_usd"] = item.get("avg_continuation_same_side_usd")
        count += 1
    return count


def _admission_status(row: dict[str, Any], args: argparse.Namespace) -> str:
    paper_pnl = row.get("paper_pnl_usd")
    if paper_pnl is None:
        return "NO_COPY_REPLAY"
    if float(paper_pnl) <= float(args.min_paper_pnl_usd):
        return "NEGATIVE_COPY_PNL"
    if int(row.get("copyable_buy_events") or 0) < int(args.min_copyable_events):
        return "POSITIVE_THIN_COPYABLE_SAMPLE"
    if int(row.get("candidate_clob_backed_orders") or 0) < int(args.min_clob_backed_orders):
        return "POSITIVE_THIN_CLOB_SAMPLE"
    if int(row.get("resolved_orders") or 0) < int(args.min_resolved_orders):
        return "POSITIVE_THIN_RESOLUTION_SAMPLE"
    if int(row.get("replay_unique_windows") or 0) < int(args.min_replay_windows):
        return "POSITIVE_THIN_WINDOW_SAMPLE"
    return "READY_QUEUE"


def _wash_status(row: dict[str, Any], args: argparse.Namespace) -> str:
    conditions = len(row["conditions"])
    resolved = int(row.get("resolved_buy_events") or 0)
    if resolved <= 0:
        if int(row.get("replay_unique_windows") or 0) >= int(args.min_replay_windows):
            return "REPLAY_WINDOW_FILTER_PASS"
        return "NO_RESOLVED_HISTORY"
    if conditions < int(args.min_unique_conditions):
        return "CONCENTRATED_SAMPLE"
    return "PASS"


def _copyability_score_before_reject_penalty(row: dict[str, Any]) -> float:
    paper_pnl = row.get("paper_pnl_usd")
    paper_component = _float(paper_pnl, 0.0) if paper_pnl is not None else 0.0
    if paper_component < 0:
        paper_component *= 0.1
    source_pnl = max(0.0, _float(row.get("source_pnl_usd"), 0.0))
    source_stake = _float(row.get("source_stake_usd"), 0.0)
    source_roi = (source_pnl / source_stake * 100.0) if source_stake > 0 else 0.0
    hours_pct = len(row["activity_hours_utc"]) / 24.0 * 100.0
    mean_edge = max(0.0, _float(row.get("shortlist_mean_edge"), 0.0))
    fill_sample = int(row.get("shortlist_fill_sample") or 0)
    return round(
        paper_component
        + 0.2 * int(row.get("copyable_buy_events") or 0)
        + 0.1 * int(row.get("candidate_clob_backed_orders") or 0)
        + 0.35 * _float(row.get("followability_score"), 0.0)
        + 0.03 * source_pnl
        + 0.1 * source_roi
        + 0.05 * hours_pct
        + mean_edge * max(1, fill_sample),
        6,
    )


def _reject_attribution_penalty(row: dict[str, Any], score_before_penalty: float) -> float:
    attribution = row.get("reject_attribution") if isinstance(row.get("reject_attribution"), dict) else {}
    if attribution.get("attributable_sample_floor_met") is not True:
        return 0.0
    ratio = min(1.0, max(0.0, _float(attribution.get("attributable_reject_ratio"), 0.0)))
    return round(max(0.0, score_before_penalty) * ratio, 6)


def _copyability_score(row: dict[str, Any]) -> float:
    score_before_penalty = _copyability_score_before_reject_penalty(row)
    return round(score_before_penalty - _reject_attribution_penalty(row, score_before_penalty), 6)


def _finalize(row: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    conditions = len(row["conditions"])
    markets = len(row["markets"])
    hours = sorted(int(item) for item in row["activity_hours_utc"])
    resolved = int(row.get("resolved_buy_events") or 0)
    wins = int(row.get("source_wins") or 0)
    source_stake = _float(row.get("source_stake_usd"), 0.0)
    source_pnl = _float(row.get("source_pnl_usd"), 0.0)
    admission_status = _admission_status(row, args)
    wash_status = _wash_status(row, args)
    score_before_penalty = _copyability_score_before_reject_penalty(row)
    reject_penalty = _reject_attribution_penalty(row, score_before_penalty)
    score = round(score_before_penalty - reject_penalty, 6)
    queue_eligible = admission_status == "READY_QUEUE" and wash_status not in {"CONCENTRATED_SAMPLE"}
    if admission_status == "READY_QUEUE" and not queue_eligible:
        admission_status = f"{admission_status}_WASH_REVIEW"
    evidence_sources = []
    if row.get("paper_pnl_usd") is not None:
        evidence_sources.append("full_pool_replay")
    if resolved:
        evidence_sources.append("resolved_history")
    if row.get("followability_windows"):
        evidence_sources.append("followability")
    if row.get("shortlist_source"):
        evidence_sources.append("profile_shortlist")
    reject_attribution = row.get("reject_attribution") or {}
    if reject_attribution.get("attributable_sample_floor_met") is True:
        evidence_sources.append("reject_attribution_feedback")
    return {
        "wallet": row["wallet"],
        "registry_status": row["registry_status"],
        "registry_enabled": row["registry_enabled"],
        "registry_name": row["registry_name"],
        "registry_tags": row["registry_tags"],
        "copyability_score": score,
        "copyability_score_before_reject_penalty": score_before_penalty,
        "reject_attribution_feedback": {
            "status": (
                "APPLIED_RATIO_PENALTY"
                if reject_attribution.get("attributable_sample_floor_met") is True
                else "NO_CANONICAL_SAMPLE"
            ),
            "penalty_usd_equivalent_score": reject_penalty,
            "penalty_ratio": reject_attribution.get("attributable_reject_ratio"),
            "penalty_formula": "max(0, score_before_penalty) * attributable_reject_ratio",
            "raw_reject_count_not_used_for_penalty": reject_attribution.get("raw_reject_count", 0),
            "attributable_rejects": reject_attribution.get("attributable_rejects", 0),
            "filled_orders": int(
                (reject_attribution.get("attributable_reject_denominator") or 0)
                - (reject_attribution.get("attributable_rejects") or 0)
            ),
            "attributable_denominator": reject_attribution.get("attributable_reject_denominator", 0),
            "sample_floor_met": reject_attribution.get("attributable_sample_floor_met", False),
            "taxonomy": reject_attribution.get("prospective_reject_taxonomy") or {},
            "reject_reasons": reject_attribution.get("reject_reasons") or {},
        },
        "admission_status": admission_status,
        "queue_eligible": queue_eligible,
        "wash_filter_status": wash_status,
        "evidence_sources": evidence_sources,
        "copy_replay": {
            "candidate_id": row["replay_candidate_id"],
            "status": row["replay_status"],
            "policy_id": row["replay_policy_id"],
            "paper_pnl_usd": round(_float(row.get("paper_pnl_usd"), 0.0), 6)
            if row.get("paper_pnl_usd") is not None
            else None,
            "paper_orders": int(row.get("paper_orders") or 0),
            "resolved_orders": int(row.get("resolved_orders") or 0),
            "copyable_buy_events": int(row.get("copyable_buy_events") or 0),
            "candidate_clob_backed_orders": int(row.get("candidate_clob_backed_orders") or 0),
            "unresolved_ratio": row.get("unresolved_ratio"),
            "unique_windows": int(row.get("replay_unique_windows") or 0),
            "total_source_usd": round(_float(row.get("replay_total_source_usd"), 0.0), 6),
            "max_buy_price": row.get("max_buy_price"),
            "failure_reasons": row["failure_reasons"],
        },
        "source_history": {
            "resolved_buy_events": resolved,
            "wins": wins,
            "win_rate_pct": round(wins / resolved * 100.0, 6) if resolved else None,
            "stake_usd": round(source_stake, 6),
            "pnl_usd": round(source_pnl, 6),
            "roi_pct": round(source_pnl / source_stake * 100.0, 6) if source_stake else None,
            "unique_conditions": conditions,
            "unique_markets": markets,
            "activity_hours_utc": hours,
            "activity_hour_coverage_pct": round(len(hours) / 24.0 * 100.0, 6),
            "first_event_ts": _iso(row["first_event_ts"]),
            "latest_event_ts": _iso(row["latest_event_ts"]),
        },
        "followability": {
            "score": round(_float(row.get("followability_score"), 0.0), 6),
            "eligible_windows": int(row.get("followability_windows") or 0),
            "early_side_predictiveness_pct": row.get("followability_predictiveness_pct"),
            "early_win_rate_pct": row.get("followability_win_rate_pct"),
            "avg_continuation_same_side_usd": row.get("followability_avg_continuation_usd"),
        },
        "profile_shortlist": {
            "source": row.get("shortlist_source"),
            "resolved_pnl": row.get("shortlist_resolved_pnl"),
            "copyable_rate_pct": row.get("shortlist_copyable_rate_pct"),
            "fill_sample": row.get("shortlist_fill_sample"),
            "mean_edge": row.get("shortlist_mean_edge"),
            "median_edge": row.get("shortlist_median_edge"),
            "best_band": row.get("best_band") or {},
        },
        "next_action": (
            "stage_for_standard_paper_to_live_gates"
            if queue_eligible
            else "keep paper-only until positive copy-PnL sample, CLOB evidence, and wash checks pass"
        ),
    }


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    rows = _registry_rows(_load_json(root / args.registry, {}))
    registry_wallets = set(rows)
    history_payload = _load_json(root / args.history, {})
    history_events = history_payload.get("events") if isinstance(history_payload.get("events"), list) else []
    newest_source_event_ts = max(
        (_parse_ts(row.get("event_ts") or row.get("observed_ts")) for row in history_events if isinstance(row, dict)),
        default=0.0,
    )
    now_ts = datetime.now(tz=UTC).timestamp()
    newest_source_event_age_s = max(0.0, now_ts - newest_source_event_ts) if newest_source_event_ts else None
    source_is_frozen_d97 = Path(args.history).name == "wallet_copy_history_state.json"
    source_fresh = bool(
        not source_is_frozen_d97
        and newest_source_event_age_s is not None
        and newest_source_event_age_s <= 86400.0
    )
    followability_payload = _load_json(root / args.followability, {})
    followability_fresh = bool(
        followability_payload.get("promotion_grade") is True
        and ((followability_payload.get("source_freshness") or {}).get("pass") is True)
    )
    replay_payload = _load_json(root / args.replay, {})
    replay_freshness = replay_payload.get("source_freshness") or {}
    replay_age = replay_freshness.get("newest_source_event_age_s")
    replay_fresh = bool(
        replay_freshness.get("pass") is True
        and replay_age is not None
        and float(replay_age) <= 86400.0
    )
    promotion_grade = source_fresh and followability_fresh and replay_fresh
    history_summary = _add_history(rows, root, args)
    replay_summary = _add_replay(rows, root, args)
    shortlist_wallets = _add_shortlist(rows, root, args)
    followability_wallets = _add_followability(rows, root, args)

    overlay_path = getattr(args, "active_set_overlay", DEFAULT_ACTIVE_SET_OVERLAY)
    prior_live_dispositions = _prior_live_dispositions(_load_json(root / overlay_path, {}))
    finalized = [_finalize(row, args) for row in rows.values()]
    for row in finalized:
        disposition = prior_live_dispositions.get(row["wallet"])
        if not disposition:
            continue
        row["prior_live_disposition"] = disposition
        row["queue_eligible_before_live_disposition"] = bool(row["queue_eligible"])
        row["queue_eligible"] = False
        row["admission_status"] = "PRIOR_LIVE_DEMOTION_REQUIRES_FRESH_READMISSION"
        row["next_action"] = (
            "exclude stale paper score from actionable ranking; require fresh local-flow and "
            "paper-at-our-prices evidence before Fable re-admission"
        )
    finalized.sort(
        key=lambda item: (
            0 if item["queue_eligible"] else 1,
            -float(item["copyability_score"]),
            -float((item["copy_replay"] or {}).get("paper_pnl_usd") or -1_000_000.0),
            -int((item["copy_replay"] or {}).get("copyable_buy_events") or 0),
            item["wallet"],
        )
    )
    for idx, row in enumerate(finalized, start=1):
        row["universe_rank"] = idx
    ranked_queue = [row for row in finalized if row["queue_eligible"]] if promotion_grade else []
    for idx, row in enumerate(ranked_queue, start=1):
        row["queue_rank"] = idx
    status_counts: dict[str, int] = defaultdict(int)
    evidence_counts: dict[str, int] = defaultdict(int)
    wash_counts: dict[str, int] = defaultdict(int)
    for row in finalized:
        status_counts[row["admission_status"]] += 1
        wash_counts[row["wash_filter_status"]] += 1
        for source in row["evidence_sources"]:
            evidence_counts[source] += 1
    return {
        "schema_version": 1,
        "kind": "wallet_copy_full_universe_copyability_leaderboard",
        "flow_stage": "LEARN/PROMOTE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": _utc_now_iso(),
        "status": "PASS_CURRENT_SOURCE" if promotion_grade else "DEPENDENCY_FRESHNESS_FAIL_CLOSED",
        "promotion_grade": promotion_grade,
        "inputs": {
            "registry": args.registry,
            "history": args.history,
            "replay": args.replay,
            "shortlist": args.shortlist,
            "followability": args.followability,
            "active_set_overlay": overlay_path,
            "resolutions": str(
                (root / args.resolutions if args.resolutions else _default_resolutions_path(root)).relative_to(root)
            )
            if (root / args.resolutions if args.resolutions else _default_resolutions_path(root)).is_relative_to(root)
            else str(root / args.resolutions if args.resolutions else _default_resolutions_path(root)),
            "newest_source_event_ts": newest_source_event_ts or None,
            "newest_source_event_age_s": round(newest_source_event_age_s, 6)
            if newest_source_event_age_s is not None
            else None,
            "freshness_limit_s": 86400.0,
            "source_is_frozen_d97": source_is_frozen_d97,
            "source_freshness_pass": source_fresh,
            "followability_freshness_pass": followability_fresh,
            "replay_freshness_pass": replay_fresh,
        },
        "criteria": {
            "min_paper_pnl_usd": float(args.min_paper_pnl_usd),
            "min_copyable_events": int(args.min_copyable_events),
            "min_clob_backed_orders": int(args.min_clob_backed_orders),
            "min_resolved_orders": int(args.min_resolved_orders),
            "min_replay_windows": int(args.min_replay_windows),
            "min_unique_conditions": int(args.min_unique_conditions),
            "score_formula": (
                "paper_pnl + copyable_sample + CLOB_sample + followability + source_pnl/roi "
                "+ activity_hours + shortlist_edge, then multiply positive score by "
                "(1 - attributable_reject_ratio) when the canonical sample floor is met"
            ),
            "queue_rule": (
                "positive paper copy-PnL with sufficient copyable/CLOB/resolved/window sample; "
                "concentrated resolved-history samples stay paper-only for wash review"
            ),
        },
        "summary": {
            "registry_wallets": len(registry_wallets),
            "wallets_scored": len(finalized),
            "unregistered_evidence_wallets": len([row for row in finalized if row["registry_status"] != "REGISTERED"]),
            "wallets_with_any_evidence": sum(1 for row in finalized if row["evidence_sources"]),
            "wallets_with_replay": replay_summary["wallets_with_replay"],
            "replay_candidates": replay_summary["replay_candidates"],
            "wallets_with_resolved_history": evidence_counts.get("resolved_history", 0),
            "wallets_with_followability": followability_wallets,
            "wallets_with_profile_shortlist": shortlist_wallets,
            "positive_copy_pnl_wallets": sum(
                1
                for row in finalized
                if (row.get("copy_replay") or {}).get("paper_pnl_usd") is not None
                and float((row.get("copy_replay") or {}).get("paper_pnl_usd") or 0.0) > 0.0
            ),
            "ranked_queue_depth": len(ranked_queue),
            "prior_live_demotion_excluded": len(prior_live_dispositions),
            "reject_ratio_penalty_applied": sum(
                1
                for row in finalized
                if (row.get("reject_attribution_feedback") or {}).get("status") == "APPLIED_RATIO_PENALTY"
            ),
            "admission_status_counts": dict(sorted(status_counts.items())),
            "wash_filter_counts": dict(sorted(wash_counts.items())),
            "evidence_source_counts": dict(sorted(evidence_counts.items())),
            "history_skipped": history_summary,
        },
        "ranked_queue": ranked_queue,
        "top_wallets": finalized[: max(1, int(args.top_n))],
        "leaderboard": finalized,
        "promotion_path": (
            "ranked_queue wallets are staged candidates only; promotion still requires standard paper/live gates "
            "and the single live guard remains the only submitter"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--replay", default=DEFAULT_REPLAY)
    parser.add_argument("--shortlist", default=DEFAULT_SHORTLIST)
    parser.add_argument("--followability", default=DEFAULT_FOLLOWABILITY)
    parser.add_argument("--active-set-overlay", default=DEFAULT_ACTIVE_SET_OVERLAY)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--min-paper-pnl-usd", type=float, default=0.0)
    parser.add_argument("--min-copyable-events", type=int, default=20)
    parser.add_argument("--min-clob-backed-orders", type=int, default=20)
    parser.add_argument("--min-resolved-orders", type=int, default=20)
    parser.add_argument("--min-replay-windows", type=int, default=3)
    parser.add_argument("--min-unique-conditions", type=int, default=3)
    args = parser.parse_args()
    report = build_report(ROOT, args)
    output = ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    if args.output == DEFAULT_OUTPUT:
        _write_legacy_deprecated_pointers(ROOT, output, str(report["generated_at"]))
    print(json.dumps(report["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
