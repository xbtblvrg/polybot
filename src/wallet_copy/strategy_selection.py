"""Rank wallet-copy strategy directions from persisted evidence.

This module deliberately separates two questions that are easy to conflate:

1. Which architecture has the strongest profit hypothesis?
2. Which architecture is closest to live-admissible copy execution?

The answer can differ. A multi-wallet inventory replay may have the best ROI,
while a single-wallet lane may be the only path that can currently prove
low-latency CLOB-backed CopyIntent parity.
"""

from __future__ import annotations

from typing import Any

from src.wallet_copy.mission import mission_contract
from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.source_route import (
    source_route_allows_live_execution,
    source_route_is_recovered_degraded,
    source_route_live_operator_approval,
    source_route_status,
)
from src.wallet_copy.status import ANALYZE, CORRECTION, PASS, WATCH, active_status_from_blockers
from src.wallet_copy.store import load_json


MIN_RESOLVED_ORDERS = 100
MIN_WR_PCT = 70.0
MIN_ROI_PCT = 5.0
MIN_AVG_ORDERS_PER_WINDOW = 2.0
MIN_COPY_COVERAGE_PCT = 95.0
MIN_CLOB_FILL_RATE_PCT = 99.0
MAX_REJECT_RATE_PCT = 5.0
MIN_PROFITABLE_COPY_ACTIVE_BUYS = 20
MIN_PROFITABLE_COPY_DEVELOPMENT_FILL_RATE_PCT = 70.0
TARGET_PROFITABLE_COPY_FILL_RATE_PCT = 95.0
TARGET_LIVE_ARCHITECTURE = "weighted_multi_wallet_inventory_by_window_with_multi_wallet_filter"
PRIMARY_LIVE_ARCHITECTURE = "single_wallet_copy_promotion_with_background_paper_backup_pool"
PRIMARY_COPY_REQUIRED_DIRECTION_IDS = (
    "single_wallet_best_copyable",
)
MULTI_WALLET_UPGRADE_DIRECTION_IDS = (
    "weighted_wallet_inventory_by_window",
    "multi_wallet_filter_consensus",
)
STATUS_PROGRESS_RANK = {
    "BUG_SUSPECT": 0,
    CORRECTION: 1,
    ANALYZE: 2,
    WATCH: 3,
    PASS: 4,
}
DEVELOPMENT_LANE_LIMITS: dict[str, dict[str, Any]] = {
    "profitable_wallet_copy_efficiency": {
        "priority": 10,
        "max_no_improvement_cycles": 3,
        "max_same_blocker_cycles": 3,
        "max_deep_research_attempts_before_reevaluation": 2,
        "positive_metric_paths": [
            "metrics.active_copied_buy_events",
            "metrics.active_fill_rate_pct",
            "metrics.active_fresh_le_10s",
            "metrics.paper_resolved_orders",
            "metrics.paper_roi_pct",
            "metrics.paper_wr_pct",
        ],
        "negative_metric_paths": ["metrics.active_rejected_buy_events"],
        "required_change_action": "rerank_by_current_copyability_and_paper_profit_then_try_next_wallet_batch",
    },
    "single_wallet_best_copyable": {
        "priority": 20,
        "max_no_improvement_cycles": 3,
        "max_same_blocker_cycles": 3,
        "max_deep_research_attempts_before_reevaluation": 2,
        "positive_metric_paths": [
            "runtime_proof.source_events",
            "runtime_proof.market_windows",
            "paper.resolved_orders",
            "paper.roi_pct",
            "paper.wr_pct",
            "copyability.policy_copy_coverage_pct",
            "copyability.all_order_fill_rate_pct",
        ],
        "negative_metric_paths": [
            "runtime_proof.event_age_p95_s",
            "copyability.all_order_rejected_buy_events",
            "copyability.all_order_reject_rate_pct",
        ],
        "required_change_action": "promote_next_best_runtime_proof_or_rebuild_candidate_forward_queue",
    },
    "wr_repair_single_wallet": {
        "priority": 30,
        "max_no_improvement_cycles": 3,
        "max_same_blocker_cycles": 3,
        "max_deep_research_attempts_before_reevaluation": 2,
        "positive_metric_paths": [
            "runtime_proof.source_events",
            "runtime_proof.market_windows",
            "paper.resolved_orders",
            "paper.roi_pct",
            "paper.wr_pct",
            "paper.validation_wr_pct",
            "copyability.all_order_fill_rate_pct",
        ],
        "negative_metric_paths": [
            "runtime_proof.event_age_p95_s",
            "paper.distance_to_70_wr_pct",
            "paper.distance_to_70_validation_wr_pct",
            "copyability.all_order_rejected_buy_events",
        ],
        "required_change_action": "switch_wr_repair_target_or_rebuild_profit_queue_from_fresh_runtime_proof",
    },
    "multi_wallet_all_order_exact_copy": {
        "priority": 60,
        "max_no_improvement_cycles": 2,
        "max_same_blocker_cycles": 2,
        "max_deep_research_attempts_before_reevaluation": 1,
        "positive_metric_paths": [
            "metrics.source_buy_events",
            "metrics.clob_or_book_filled_buy_events",
            "metrics.fill_rate_pct",
        ],
        "negative_metric_paths": [
            "metrics.rejected_buy_events",
            "metrics.fallback_buy_events",
            "metrics.reject_rate_pct",
        ],
        "required_change_action": "repair_copyintent_lifecycle_or_keep_all_order_as_diagnostic_only",
    },
    "weighted_wallet_inventory_by_window": {
        "priority": 40,
        "max_no_improvement_cycles": 3,
        "max_same_blocker_cycles": 3,
        "max_deep_research_attempts_before_reevaluation": 2,
        "positive_metric_paths": [
            "candidate.resolved_orders",
            "candidate.roi_pct",
            "candidate.wr_pct",
            "candidate.validation_wr_pct",
            "candidate.avg_orders_per_window",
            "candidate.clob_backed_fill_rate_pct",
            "active_hotlane_cohorts.selected_cohorts",
        ],
        "negative_metric_paths": [
            "candidate.fallback_filled_orders",
            "candidate.unresolved_ratio",
        ],
        "required_change_action": "rebuild_scaled_multi_wallet_inventory_from_full_per_wallet_copy_universe",
    },
    "multi_wallet_filter_consensus": {
        "priority": 50,
        "max_no_improvement_cycles": 2,
        "max_same_blocker_cycles": 2,
        "max_deep_research_attempts_before_reevaluation": 1,
        "positive_metric_paths": [
            "metrics.pass_signals",
            "metrics.paper_fills",
            "metrics.runtime_eligible_wallets",
            "metrics.selected_cohorts",
            "metrics.tracker_time_inventory_candidates",
        ],
        "negative_metric_paths": ["metrics.paper_rejects"],
        "required_change_action": "rotate_multi_wallet_cohorts_and_force_current_poll_burnin",
    },
}
PROGRAM_RETHINK_UNIVERSE_WALLET_FLOOR = 500
PROGRAM_RETHINK_ACTIVE_BUY_FLOOR = 100
TARGET_PER_WALLET_COPY_SOURCE_BUYS = 1000
TARGET_PER_WALLET_COPY_SOURCE_WALLETS = 100
TARGET_MULTI_WALLET_INVENTORY_ORDER_SLOTS = 1000


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _hot_path_metric(hot_path: dict[str, Any], key: str, default: Any = None) -> Any:
    if key in hot_path and hot_path.get(key) is not None:
        return hot_path.get(key)
    summary = _dict(hot_path.get("summary"))
    return summary.get(key, default)


def _hot_path_strength(hot_path: dict[str, Any]) -> tuple[int, ...]:
    return (
        int(str(hot_path.get("status") or "") == PASS),
        int(num(_hot_path_metric(hot_path, "pass_signals"))),
        int(num(_hot_path_metric(hot_path, "hot_path_intents_created"))),
        int(num(_hot_path_metric(hot_path, "hot_path_filled_orders"))),
        int(num(_hot_path_metric(hot_path, "hot_path_inventory_intents_created"))),
        int(num(_hot_path_metric(hot_path, "hot_path_inventory_filled_orders"))),
        int(num(_hot_path_metric(hot_path, "runtime_eligible_wallets"))),
        int(num(_hot_path_metric(hot_path, "runtime_fresh_buy_events_le_cap"))),
        int(num(_hot_path_metric(hot_path, "current_poll_moves"))),
    )


def _hot_path_from_hotlane_tick(hotlane_tick_state: dict[str, Any] | None) -> dict[str, Any]:
    tick = _dict(hotlane_tick_state)
    summary = _dict(tick.get("summary"))
    latest = _dict(summary.get("latest_tracker"))
    if not summary and not latest:
        return {}
    bridge_burnin = _dict(summary.get("current_poll_inventory_bridge_burnin")) or _dict(
        tick.get("current_poll_inventory_bridge_burnin")
    )
    window_indexed_bridge_burnin = _dict(summary.get("window_indexed_inventory_bridge_burnin")) or _dict(
        tick.get("window_indexed_inventory_bridge_burnin")
    )
    live_feed_bridge_burnin = _dict(summary.get("live_feed_inventory_bridge_burnin")) or _dict(
        tick.get("live_feed_inventory_bridge_burnin")
    )

    def latest_or_best(latest_key: str, best_key: str, default: Any = 0) -> Any:
        value = latest.get(latest_key)
        if value is not None:
            return value
        return summary.get(best_key, default)

    tracker_time_summary = {
        "pass_signals": latest_or_best(
            "hot_path_tracker_time_replay_pass_signals",
            "best_hot_path_tracker_time_replay_pass_signals",
        ),
        "tracker_time_replay_intents_created": latest_or_best(
            "hot_path_tracker_time_replay_intents_created",
            "best_hot_path_tracker_time_replay_intents_created",
        ),
        "tracker_time_replay_filled_orders": latest_or_best(
            "hot_path_tracker_time_replay_filled_orders",
            "best_hot_path_tracker_time_replay_filled_orders",
        ),
        "tracker_time_replay_rejected_orders": latest_or_best(
            "hot_path_tracker_time_replay_rejected_orders",
            "best_hot_path_tracker_time_replay_rejected_orders",
        ),
        "inventory_candidates": latest.get("hot_path_tracker_time_inventory_research_candidates")
        or latest.get("hot_path_runtime_inventory_research_candidates")
        or summary.get("best_hot_path_runtime_inventory_research_candidates")
        or 0,
    }
    return {
        "status": latest.get("hot_path_adaptive_status") or tick.get("status"),
        "blockers": latest.get("hot_path_adaptive_blockers") or tick.get("blockers") or [],
        "evidence_source": "hotlane_tick_best_tracker",
        "current_poll_moves": latest.get("hot_path_current_poll_moves") or latest.get("new_wallet_events") or 0,
        "pass_signals": latest_or_best("hot_path_pass_signals", "best_hot_path_pass_signals"),
        "runtime_fresh_buy_events_le_cap": latest_or_best(
            "hot_path_runtime_fresh_buy_events_le_cap",
            "best_hot_path_runtime_fresh_buy_events_le_cap",
        ),
        "runtime_eligible_wallets": latest_or_best(
            "hot_path_runtime_eligible_wallets",
            "best_hot_path_runtime_eligible_wallets",
        ),
        "runtime_inventory_research_candidates": latest_or_best(
            "hot_path_runtime_inventory_research_candidates",
            "best_hot_path_runtime_inventory_research_candidates",
        ),
        "hot_path_intents_created": latest_or_best("hot_path_intents_created", "best_hot_path_intents_created"),
        "hot_path_filled_orders": latest_or_best("hot_path_filled_orders", "best_hot_path_filled_orders"),
        "hot_path_rejected_orders": latest_or_best("hot_path_rejected_orders", "best_hot_path_rejected_orders"),
        "hot_path_inventory_intents_created": latest_or_best(
            "hot_path_inventory_intents_created",
            "best_hot_path_inventory_intents_created",
        ),
        "hot_path_inventory_filled_orders": latest_or_best(
            "hot_path_inventory_filled_orders",
            "best_hot_path_inventory_filled_orders",
        ),
        "hot_path_inventory_rejected_orders": latest_or_best(
            "hot_path_inventory_rejected_orders",
            "best_hot_path_inventory_rejected_orders",
        ),
        "current_poll_inventory_bridge_burnin": bridge_burnin,
        "window_indexed_inventory_bridge_burnin": window_indexed_bridge_burnin,
        "live_feed_inventory_bridge_burnin": live_feed_bridge_burnin,
        "tracker_time_replay": {
            "status": latest.get("hot_path_tracker_time_replay_status"),
            "blockers": latest.get("hot_path_tracker_time_replay_blockers") or [],
            "summary": tracker_time_summary,
        },
    }


def _active_tracking_with_hotlane_tick_best(
    active_tracking: dict[str, Any],
    hotlane_tick_state: dict[str, Any] | None,
) -> dict[str, Any]:
    tick_hot_path = _hot_path_from_hotlane_tick(hotlane_tick_state)
    if not tick_hot_path:
        return active_tracking
    tracking_summary = dict(_dict(active_tracking.get("summary")))
    current_hot_path = _dict(tracking_summary.get("hot_path_adaptive"))
    if _hot_path_strength(tick_hot_path) <= _hot_path_strength(current_hot_path):
        bridge_burnin = _dict(tick_hot_path.get("current_poll_inventory_bridge_burnin"))
        window_indexed_bridge_burnin = _dict(tick_hot_path.get("window_indexed_inventory_bridge_burnin"))
        live_feed_bridge_burnin = _dict(tick_hot_path.get("live_feed_inventory_bridge_burnin"))
        if bridge_burnin or window_indexed_bridge_burnin or live_feed_bridge_burnin:
            return {
                **active_tracking,
                "hotlane_tick_bridge_burnin_applied": True,
                "summary": {
                    **tracking_summary,
                    "hotlane_tick_bridge_burnin_applied": True,
                    "hot_path_adaptive": {
                        **current_hot_path,
                        "current_poll_inventory_bridge_burnin": bridge_burnin,
                        "window_indexed_inventory_bridge_burnin": window_indexed_bridge_burnin,
                        "live_feed_inventory_bridge_burnin": live_feed_bridge_burnin,
                    },
                },
            }
        return active_tracking

    latest = _dict(_dict(hotlane_tick_state).get("summary")).get("latest_tracker")
    latest = _dict(latest)
    copy_efficiency = dict(_dict(tracking_summary.get("copy_efficiency")))
    copy_summary = dict(_dict(copy_efficiency.get("summary")))
    if int(num(latest.get("required_buy_copy_events"))) > int(num(copy_summary.get("required_buy_copy_events"))):
        copy_efficiency = {
            **copy_efficiency,
            "status": latest.get("copy_efficiency_status") or copy_efficiency.get("status"),
            "blockers": latest.get("copy_efficiency_blockers") or copy_efficiency.get("blockers") or [],
            "summary": {
                **copy_summary,
                "source_fresh_buy_events_le_10s": latest.get("source_fresh_buy_events_le_10s"),
                "required_buy_copy_events": latest.get("required_buy_copy_events"),
                "clob_filled_buy_copy_events": latest.get("clob_filled_buy_copy_events"),
                "fallback_filled_buy_copy_events": latest.get("fallback_filled_buy_copy_events"),
                "rejected_buy_copy_events": latest.get("rejected_buy_copy_events"),
                "missed_buy_copy_events": latest.get("missed_buy_copy_events"),
            },
        }

    return {
        **active_tracking,
        "hotlane_tick_best_tracker_applied": True,
        "summary": {
            **tracking_summary,
            "hotlane_tick_best_tracker_applied": True,
            "hot_path_adaptive": tick_hot_path,
            "hot_path_adaptive_status": tick_hot_path.get("status"),
            "hot_path_current_poll_moves": _hot_path_metric(tick_hot_path, "current_poll_moves"),
            "copy_efficiency": copy_efficiency,
        },
    }


def _path_value(row: dict[str, Any], path: str) -> Any:
    current: Any = row
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _numeric_path_value(row: dict[str, Any], path: str) -> float | None:
    value = _path_value(row, path)
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return round(float(value), 6)
    return None


def _metric_snapshot(direction: dict[str, Any], policy: dict[str, Any]) -> dict[str, float]:
    paths = [*list(policy.get("positive_metric_paths") or []), *list(policy.get("negative_metric_paths") or [])]
    snapshot: dict[str, float] = {}
    for path in paths:
        value = _numeric_path_value(direction, str(path))
        if value is not None:
            snapshot[str(path)] = value
    return snapshot


def _metric_snapshot_improved(
    current: dict[str, float],
    previous: dict[str, float],
    *,
    policy: dict[str, Any],
) -> bool:
    for path in policy.get("positive_metric_paths") or []:
        key = str(path)
        if key in current and key in previous and current[key] > previous[key]:
            return True
    for path in policy.get("negative_metric_paths") or []:
        key = str(path)
        if key in current and key in previous and current[key] < previous[key]:
            return True
    return False


def _status_improved(current: str, previous: str) -> bool:
    return STATUS_PROGRESS_RANK.get(current, 0) > STATUS_PROGRESS_RANK.get(previous, 0)


def _pct(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return round(float(numerator) / float(denominator) * 100.0, 6)


def _status_from_blockers(blockers: list[str], *, has_repair_defect: bool = False) -> str:
    if not blockers:
        return PASS
    if has_repair_defect or any(
        key in blocker
        for blocker in blockers
        for key in ("rejected", "fallback", "missing_clob", "copyability", "coverage", "latency", "wrong")
    ):
        return CORRECTION
    return ANALYZE


def _paper_quality(row: dict[str, Any]) -> dict[str, Any]:
    resolved = int(num(row.get("paper_resolved_orders")))
    roi = num(row.get("paper_roi_pct"))
    wr = num(row.get("paper_wr_pct"))
    orders = int(num(row.get("paper_orders")))
    pnl = num(row.get("paper_total_realized_plus_resolved_pnl_usd"), num(row.get("paper_pnl_usd")))
    return {
        "orders": orders,
        "resolved_orders": resolved,
        "pnl_usd": round(pnl, 6),
        "roi_pct": round(roi, 6),
        "wr_pct": round(wr, 6),
        "resolved_pass": resolved >= MIN_RESOLVED_ORDERS,
        "roi_pass": roi >= MIN_ROI_PCT,
        "wr_pass": wr >= MIN_WR_PCT,
    }


def _copy_quality(row: dict[str, Any]) -> dict[str, Any]:
    quality = _dict(row.get("source_vs_paper_copy_quality"))
    active_buy_events = int(num(quality.get("active_buy_events"), num(row.get("active_tracking_buy_events"))))
    all_order_copied = int(
        num(quality.get("active_all_order_copied_buy_events"), num(row.get("active_tracking_all_order_copied_buy_events")))
    )
    all_order_rejected = int(
        num(
            quality.get("active_all_order_rejected_buy_events"),
            num(row.get("active_tracking_all_order_rejected_buy_events")),
        )
    )
    policy_copied = int(
        num(quality.get("active_policy_copied_buy_events"), num(row.get("active_tracking_policy_copied_buy_events")))
    )
    policy_coverage = quality.get("active_policy_copy_coverage_pct")
    if policy_coverage is None:
        policy_coverage = _pct(policy_copied, active_buy_events)
    all_order_fill_rate = quality.get("active_all_order_fill_rate_pct")
    if all_order_fill_rate is None:
        all_order_fill_rate = _pct(all_order_copied, all_order_copied + all_order_rejected)
    all_order_reject_rate = quality.get("active_all_order_reject_rate_pct")
    if all_order_reject_rate is None:
        all_order_reject_rate = _pct(all_order_rejected, all_order_copied + all_order_rejected)
    return {
        "current_tracking_seen": bool(quality.get("current_tracking_seen")) or active_buy_events > 0,
        "active_buy_events": active_buy_events,
        "policy_copied_buy_events": policy_copied,
        "policy_copy_coverage_pct": round(num(policy_coverage), 6) if policy_coverage is not None else None,
        "all_order_copied_buy_events": all_order_copied,
        "all_order_rejected_buy_events": all_order_rejected,
        "all_order_fill_rate_pct": round(num(all_order_fill_rate), 6) if all_order_fill_rate is not None else None,
        "all_order_reject_rate_pct": round(num(all_order_reject_rate), 6) if all_order_reject_rate is not None else None,
        "dominant_reject_reason": quality.get("dominant_active_all_order_reject_reason"),
        "dominant_copyability_reason": quality.get("dominant_active_copyability_reason"),
        "status": quality.get("status") or "UNKNOWN",
        "blockers": [str(blocker) for blocker in _list(quality.get("blockers"))],
        "live_candidate_score": round(num(quality.get("live_candidate_score")), 6),
    }


def _copy_quality_with_attached_runtime_proof(
    copy: dict[str, Any],
    proof: dict[str, Any] | None,
    *,
    proof_attached_to_profit: bool,
) -> dict[str, Any]:
    if not proof_attached_to_profit or not isinstance(proof, dict) or not proof:
        return copy
    source_events = int(num(proof.get("source_events")))
    market_windows = int(num(proof.get("market_windows")))
    clean_rows = int(num(proof.get("clean_clob_filled_buy_rows"), num(proof.get("proof_rows"))))
    p95_age = proof.get("event_age_p95_s")
    if (
        source_events < 10
        or market_windows < 3
        or p95_age is None
        or num(p95_age) > 10.0
        or clean_rows < source_events
    ):
        return copy

    updated = dict(copy)
    updated.update(
        {
            "current_tracking_seen": True,
            "active_buy_events": max(int(num(updated.get("active_buy_events"))), source_events),
            "policy_copied_buy_events": max(int(num(updated.get("policy_copied_buy_events"))), clean_rows),
            "policy_copy_coverage_pct": 100.0,
            "all_order_copied_buy_events": max(int(num(updated.get("all_order_copied_buy_events"))), clean_rows),
            "all_order_rejected_buy_events": 0,
            "all_order_fill_rate_pct": 100.0,
            "all_order_reject_rate_pct": 0.0,
            "dominant_reject_reason": None,
            "dominant_copyability_reason": None,
            "status": PASS,
            "blockers": [],
            "live_candidate_score": max(num(updated.get("live_candidate_score")), _proof_score(proof)),
        }
    )
    return updated


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 6)
    rank = (len(ordered) - 1) * max(0.0, min(100.0, float(pct))) / 100.0
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return round(ordered[lower] * (1.0 - weight) + ordered[upper] * weight, 6)


def _runtime_proof_groups(runtime_proof_index: dict[str, Any]) -> dict[str, dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for row in _list(runtime_proof_index.get("proof_rows")):
        proof = _dict(row)
        wallet = str(proof.get("source_wallet") or "").lower()
        policy_id = str(proof.get("policy_id") or "")
        candidate_id = str(proof.get("candidate_id") or "")
        if not wallet or not policy_id or not candidate_id:
            continue
        key = f"{wallet}|{policy_id}|{candidate_id}"
        group = groups.setdefault(
            key,
            {
                "source_wallet": wallet,
                "policy_id": policy_id,
                "candidate_id": candidate_id,
                "proof_rows": 0,
                "source_events": set(),
                "market_windows": set(),
                "event_ages": [],
                "sources": set(),
            },
        )
        group["proof_rows"] += 1
        if proof.get("source_event_id"):
            group["source_events"].add(str(proof.get("source_event_id")))
        if proof.get("market_slug"):
            group["market_windows"].add(str(proof.get("market_slug")))
        event_age = proof.get("event_age_s")
        if isinstance(event_age, (int, float)):
            group["event_ages"].append(float(event_age))
        if proof.get("source"):
            group["sources"].add(str(proof.get("source")))
    public: dict[str, dict[str, Any]] = {}
    for key, group in groups.items():
        event_ages = [float(value) for value in group.get("event_ages") or []]
        public[key] = {
            "source_wallet": group["source_wallet"],
            "policy_id": group["policy_id"],
            "candidate_id": group["candidate_id"],
            "proof_rows": int(group["proof_rows"]),
            "source_events": len(group["source_events"]),
            "market_windows": len(group["market_windows"]),
            "event_age_p50_s": _percentile(event_ages, 50.0),
            "event_age_p95_s": _percentile(event_ages, 95.0),
            "sources": sorted(group["sources"]),
            "clean_clob_filled_buy_rows": int(group["proof_rows"]),
        }
    return public


def _proof_score(proof: dict[str, Any] | None) -> float:
    if not isinstance(proof, dict) or not proof:
        return 0.0
    p95 = proof.get("event_age_p95_s")
    latency_bonus = 100.0 if isinstance(p95, (int, float)) and float(p95) <= 10.0 else 0.0
    return round(
        int(num(proof.get("proof_rows"))) * 25.0
        + int(num(proof.get("source_events"))) * 15.0
        + int(num(proof.get("market_windows"))) * 50.0
        + latency_bonus,
        6,
    )


def _wallet_rank(row: dict[str, Any], proof: dict[str, Any] | None = None) -> float:
    paper = _paper_quality(row)
    copy = _copy_quality(row)
    score = 0.0
    score += min(max(paper["roi_pct"], -100.0), 100.0) * 0.6
    score += paper["wr_pct"] * 2.0
    score += min(paper["resolved_orders"], 1000) * 0.05
    if copy["current_tracking_seen"]:
        score += 30.0
    score += min(num(copy["policy_copy_coverage_pct"]), 100.0) * 0.5
    score += num(copy["all_order_fill_rate_pct"]) * 0.7
    score -= num(copy["all_order_reject_rate_pct"]) * 1.5
    score += copy["live_candidate_score"] * 0.2
    score += _proof_score(proof) * 3.0
    return round(score, 6)


def _candidate_wallet_policy_ref(row: dict[str, Any]) -> tuple[str, str] | None:
    if not isinstance(row, dict):
        return None
    metadata = _dict(row.get("metadata"))
    wallet = str(row.get("source_wallet") or metadata.get("source_wallet") or "").lower()
    policy = _dict(row.get("policy"))
    policy_id = str(row.get("policy_id") or policy.get("policy_id") or "")
    if not wallet or not policy_id:
        return None
    return wallet, policy_id


def _profit_candidate_references(profit_state: dict[str, Any]) -> tuple[set[str], set[tuple[str, str]]]:
    ids: set[str] = set()
    wallet_policy_refs: set[tuple[str, str]] = set()

    def add_candidate(candidate: dict[str, Any]) -> None:
        candidate_id = _dict(candidate).get("candidate_id")
        if candidate_id:
            ids.add(str(candidate_id))
        ref = _candidate_wallet_policy_ref(candidate)
        if ref is not None:
            wallet_policy_refs.add(ref)

    for key in (
        "best_candidate",
        "forward_candidate",
        "runtime_admission_candidate",
        "best_runtime_candidate",
        "forward_runtime_candidate",
    ):
        add_candidate(_dict(profit_state.get(key)))
    for key in ("ranked_candidates", "forward_tracking_queue", "pass_candidates", "forward_queue_runtime_candidates"):
        for row in _list(profit_state.get(key)):
            add_candidate(_dict(row))
    return ids, wallet_policy_refs


def _profit_candidate_ids(profit_state: dict[str, Any]) -> set[str]:
    ids, _wallet_policy_refs = _profit_candidate_references(profit_state)
    return ids


def _runtime_proof_attached_to_profit(
    proof: dict[str, Any] | None,
    *,
    candidate_ids: set[str],
    wallet_policy_refs: set[tuple[str, str]],
) -> bool:
    if not isinstance(proof, dict) or not proof:
        return False
    candidate_id = str(proof.get("candidate_id") or "")
    if candidate_id and candidate_id in candidate_ids:
        return True
    wallet = str(proof.get("source_wallet") or "").lower()
    policy_id = str(proof.get("policy_id") or "")
    return bool(wallet and policy_id and (wallet, policy_id) in wallet_policy_refs)


def _best_single_wallet(
    wallet_analysis: dict[str, Any],
    runtime_proof_index: dict[str, Any],
    profit_state: dict[str, Any],
) -> dict[str, Any]:
    wallets = [*_profit_single_wallet_rows(profit_state), *[_dict(row) for row in _list(wallet_analysis.get("wallets"))]]
    if not wallets:
        wallets = [_dict(row) for row in _list(wallet_analysis.get("top_wallets"))]
    proofs = _runtime_proof_groups(runtime_proof_index)
    current_profit_candidate_ids, current_profit_wallet_policy_refs = _profit_candidate_references(profit_state)
    best_proof_by_wallet: dict[str, dict[str, Any]] = {}
    strongest_proof_by_wallet: dict[str, dict[str, Any]] = {}
    for proof in proofs.values():
        wallet = str(proof.get("source_wallet") or "").lower()
        current_strongest = strongest_proof_by_wallet.get(wallet)
        if current_strongest is None or _proof_score(proof) > _proof_score(current_strongest):
            strongest_proof_by_wallet[wallet] = proof
        current = best_proof_by_wallet.get(wallet)
        proof_attached = _runtime_proof_attached_to_profit(
            proof,
            candidate_ids=current_profit_candidate_ids,
            wallet_policy_refs=current_profit_wallet_policy_refs,
        )
        current_attached = _runtime_proof_attached_to_profit(
            current,
            candidate_ids=current_profit_candidate_ids,
            wallet_policy_refs=current_profit_wallet_policy_refs,
        )
        if current is None or (proof_attached, _proof_score(proof)) > (current_attached, _proof_score(current)):
            best_proof_by_wallet[wallet] = proof
    def proof_led_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
        proof = best_proof_by_wallet.get(str(row.get("wallet") or "").lower())
        paper = _paper_quality(row)
        return (
            1 if proof else 0,
            1 if paper["resolved_pass"] and paper["roi_pass"] and paper["wr_pass"] else 0,
            _proof_score(proof),
            _wallet_rank(row, proof),
        )

    ranked = sorted((row for row in wallets if row.get("wallet")), key=proof_led_rank_key, reverse=True)
    best = ranked[0] if ranked else {}
    best_wallet = str(best.get("wallet") or "").lower()
    proof = best_proof_by_wallet.get(best_wallet)
    strongest_proof = strongest_proof_by_wallet.get(best_wallet)
    proof_attached_to_profit = _runtime_proof_attached_to_profit(
        proof,
        candidate_ids=current_profit_candidate_ids,
        wallet_policy_refs=current_profit_wallet_policy_refs,
    )
    paper = _paper_quality(best)
    copy = _copy_quality(best)
    proof_usable = bool(
        proof
        and int(num(proof.get("source_events"))) >= 10
        and int(num(proof.get("market_windows"))) >= 3
        and proof.get("event_age_p95_s") is not None
        and num(proof.get("event_age_p95_s")) <= 10.0
    )
    copy = _copy_quality_with_attached_runtime_proof(
        copy,
        proof,
        proof_attached_to_profit=proof_attached_to_profit,
    )
    blockers: list[str] = []
    if not best:
        blockers.append("no_wallet_rows_available")
    if not paper["resolved_pass"]:
        blockers.append("single_wallet_resolved_orders_below_100")
    if not paper["wr_pass"]:
        blockers.append("single_wallet_wr_below_70pct")
    if not paper["roi_pass"]:
        blockers.append("single_wallet_roi_below_5pct")
    if not copy["current_tracking_seen"] and not proof_usable:
        blockers.append("single_wallet_not_currently_tracked")
    if proof:
        if int(num(proof.get("source_events"))) < 10:
            blockers.append("single_wallet_candidate_runtime_required_buys_below_10")
        if int(num(proof.get("market_windows"))) < 3:
            blockers.append("single_wallet_candidate_runtime_windows_below_3")
        if proof.get("event_age_p95_s") is None or num(proof.get("event_age_p95_s")) > 10.0:
            blockers.append("single_wallet_candidate_runtime_event_age_p95_above_10s")
        if not proof_attached_to_profit:
            blockers.append("proof_led_candidate_not_attached_to_current_profit_rankings")
    else:
        blockers.append("single_wallet_candidate_runtime_clob_proof_missing")
        if num(copy["policy_copy_coverage_pct"]) < MIN_COPY_COVERAGE_PCT:
            blockers.append("single_wallet_policy_copy_coverage_below_95pct")
        if num(copy["all_order_fill_rate_pct"]) < MIN_CLOB_FILL_RATE_PCT:
            blockers.append("single_wallet_all_order_fill_rate_below_99pct")
        if num(copy["all_order_reject_rate_pct"]) > MAX_REJECT_RATE_PCT:
            blockers.append("single_wallet_all_order_reject_rate_above_5pct")
    status = _status_from_blockers(
        blockers,
        has_repair_defect=bool(
            "runtime" in " ".join(blockers)
            or copy["all_order_rejected_buy_events"] > 0
            or ("coverage" in " ".join(blockers) and not proof)
            or ("fill_rate" in " ".join(blockers) and not proof)
        ),
    )
    return {
        "id": "single_wallet_best_copyable",
        "label": "Copy the single wallet/policy we can copy best",
        "status": status,
        "rank_score": _wallet_rank(best, proof) if best else 0.0,
        "wallet": best.get("wallet"),
        "wallet_name": best.get("wallet_name"),
        "runtime_proof": proof or {},
        "runtime_proof_attached_to_profit": proof_attached_to_profit,
        "stronger_unattached_runtime_proof": (
            strongest_proof
            if (
                isinstance(strongest_proof, dict)
                and strongest_proof != proof
                and not _runtime_proof_attached_to_profit(
                    strongest_proof,
                    candidate_ids=current_profit_candidate_ids,
                    wallet_policy_refs=current_profit_wallet_policy_refs,
                )
            )
            else {}
        ),
        "paper": paper,
        "copyability": copy,
        "blockers": blockers,
        "next_step": (
            "Promote proof-led single wallet+policy pairs into sub-minute candidate-forward tracking until "
            "candidate-scoped CLOB proof has at least 10 BUYs, 3 windows, p95 age <=10s, and paper WR/ROI pass."
        ),
    }


def _strongest_runtime_proof_by_wallet(runtime_proof_index: dict[str, Any]) -> dict[str, dict[str, Any]]:
    strongest: dict[str, dict[str, Any]] = {}
    for proof in _runtime_proof_groups(runtime_proof_index).values():
        wallet = str(proof.get("source_wallet") or "").lower()
        current = strongest.get(wallet)
        if current is None or _proof_score(proof) > _proof_score(current):
            strongest[wallet] = proof
    return strongest


def _validation_wr(row: dict[str, Any]) -> float:
    return num(row.get("validation_wr_pct"), num(row.get("source_proxy_wr_pct"), num(row.get("paper_wr_pct"))))


def _wr_repair_rank(row: dict[str, Any], proof: dict[str, Any] | None) -> float:
    paper = _paper_quality(row)
    copy = _copy_quality(row)
    validation_wr = _validation_wr(row)
    drawdown = abs(num(row.get("paper_max_drawdown_usd"), num(row.get("drawdown_usd"))))
    reject_rate = num(copy["all_order_reject_rate_pct"])
    paper_gate_penalty = 0.0
    if paper["roi_pct"] < 2.0:
        paper_gate_penalty += 1500.0
    if paper["wr_pct"] < MIN_WR_PCT:
        paper_gate_penalty += 1500.0
    if validation_wr < MIN_WR_PCT:
        paper_gate_penalty += 1000.0
    return round(
        (1000.0 if paper["resolved_pass"] else 0.0)
        + validation_wr * 6.0
        + paper["wr_pct"] * 5.0
        + min(max(paper["roi_pct"], -50.0), 150.0) * 2.0
        + min(paper["resolved_orders"], 2500) * 0.05
        + _proof_score(proof) * 0.15
        + (50.0 if copy["current_tracking_seen"] else 0.0)
        - min(drawdown, 500.0) * 0.1
        - min(reject_rate, 100.0) * 0.5
        - paper_gate_penalty,
        6,
    )


def _profit_single_wallet_rows(profit_state: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def add(candidate: dict[str, Any]) -> None:
        if not isinstance(candidate, dict) or candidate.get("candidate_type") != "SINGLE_WALLET":
            return
        metadata = _dict(candidate.get("metadata"))
        policy = _dict(candidate.get("policy"))
        summary = _dict(candidate.get("summary"))
        validation = _dict(candidate.get("validation_summary"))
        wallet = str(candidate.get("source_wallet") or metadata.get("source_wallet") or "").lower()
        policy_id = str(candidate.get("policy_id") or policy.get("policy_id") or "")
        if not wallet:
            return
        key = (wallet, policy_id)
        if key in seen:
            return
        seen.add(key)
        rows.append(
            {
                "wallet": wallet,
                "wallet_name": metadata.get("wallet_name") or wallet,
                "paper_orders": summary.get("orders"),
                "paper_resolved_orders": summary.get("resolved_orders"),
                "paper_total_realized_plus_resolved_pnl_usd": summary.get("pnl_usd"),
                "paper_roi_pct": summary.get("roi_pct"),
                "paper_wr_pct": summary.get("wr_pct"),
                "validation_wr_pct": validation.get("wr_pct"),
                "paper_max_drawdown_usd": candidate.get("max_drawdown_usd"),
                "profit_candidate_id": candidate.get("candidate_id"),
                "profit_candidate_policy_id": policy_id,
                "profit_candidate_blockers": list(candidate.get("blockers") or []),
                "source_vs_paper_copy_quality": {
                    "current_tracking_seen": bool(candidate.get("runtime_copy_evidence")),
                    "status": candidate.get("status"),
                    "blockers": list(candidate.get("blockers") or []),
                    "active_policy_copy_coverage_pct": None,
                    "active_all_order_rejected_buy_events": 0,
                    "active_all_order_reject_rate_pct": 0.0,
                },
            }
        )

    for key in ("forward_candidate", "runtime_admission_candidate", "best_runtime_candidate", "forward_runtime_candidate"):
        add(_dict(profit_state.get(key)))
    for key in ("forward_tracking_queue", "forward_queue_runtime_candidates", "ranked_candidates", "pass_candidates"):
        for candidate in _list(profit_state.get(key)):
            add(_dict(candidate))
    return rows


def _wr_repair_single_wallet(
    wallet_analysis: dict[str, Any],
    runtime_proof_index: dict[str, Any],
    profit_state: dict[str, Any],
) -> dict[str, Any]:
    wallets = [*_profit_single_wallet_rows(profit_state), *[_dict(row) for row in _list(wallet_analysis.get("wallets"))]]
    if not wallets:
        wallets = [_dict(row) for row in _list(wallet_analysis.get("top_wallets"))]
    proofs = _strongest_runtime_proof_by_wallet(runtime_proof_index)
    ranked = sorted(
        (row for row in wallets if row.get("wallet")),
        key=lambda row: _wr_repair_rank(row, proofs.get(str(row.get("wallet") or "").lower())),
        reverse=True,
    )
    best = ranked[0] if ranked else {}
    wallet = str(best.get("wallet") or "").lower()
    proof = proofs.get(wallet)
    paper = _paper_quality(best)
    copy = _copy_quality(best)
    validation_wr = _validation_wr(best)
    proof_usable = bool(
        proof
        and int(num(proof.get("source_events"))) >= 10
        and int(num(proof.get("market_windows"))) >= 3
        and proof.get("event_age_p95_s") is not None
        and num(proof.get("event_age_p95_s")) <= 10.0
    )
    blockers: list[str] = []
    if not best:
        blockers.append("wr_repair_no_wallet_rows_available")
    if not paper["resolved_pass"]:
        blockers.append("wr_repair_resolved_orders_below_100")
    if paper["wr_pct"] < MIN_WR_PCT:
        blockers.append("wr_repair_all_wr_below_70pct")
    if validation_wr < MIN_WR_PCT:
        blockers.append("wr_repair_validation_wr_below_70pct")
    if paper["roi_pct"] < 2.0:
        blockers.append("wr_repair_roi_below_2pct")
    if not copy["current_tracking_seen"] and not proof_usable:
        blockers.append("wr_repair_not_currently_tracked")
    if proof is None:
        blockers.append("wr_repair_runtime_clob_proof_missing")
    elif not proof_usable:
        if int(num(proof.get("source_events"))) < 10:
            blockers.append("wr_repair_runtime_required_buys_below_10")
        if int(num(proof.get("market_windows"))) < 3:
            blockers.append("wr_repair_runtime_windows_below_3")
        if proof.get("event_age_p95_s") is None or num(proof.get("event_age_p95_s")) > 10.0:
            blockers.append("wr_repair_runtime_event_age_p95_above_10s")
    if not proof_usable and num(copy["policy_copy_coverage_pct"]) < MIN_COPY_COVERAGE_PCT:
        blockers.append("wr_repair_policy_copy_coverage_below_95pct")
    if copy["all_order_rejected_buy_events"] > 0:
        blockers.append("wr_repair_all_order_rejected_buys_present")
    if num(copy["all_order_reject_rate_pct"]) > MAX_REJECT_RATE_PCT:
        blockers.append("wr_repair_all_order_reject_rate_above_5pct")
    profit_blockers = [str(blocker) for blocker in best.get("profit_candidate_blockers") or []]
    if any("research_only" in blocker for blocker in profit_blockers):
        blockers.append("wr_repair_candidate_uses_research_only_resolution")
    return {
        "id": "wr_repair_single_wallet",
        "label": "Repair toward a 70pct WR single-wallet copy candidate",
        "status": _status_from_blockers(
            blockers,
            has_repair_defect=bool(
                copy["all_order_rejected_buy_events"] > 0
                or "runtime_clob_proof_missing" in " ".join(blockers)
                or "coverage" in " ".join(blockers)
            ),
        ),
        "rank_score": _wr_repair_rank(best, proof) if best else 0.0,
        "wallet": best.get("wallet"),
        "wallet_name": best.get("wallet_name"),
        "paper": {
            **paper,
            "validation_wr_pct": round(validation_wr, 6),
            "distance_to_70_wr_pct": round(max(0.0, MIN_WR_PCT - paper["wr_pct"]), 6),
            "distance_to_70_validation_wr_pct": round(max(0.0, MIN_WR_PCT - validation_wr), 6),
        },
        "runtime_proof": proof or {},
        "copyability": copy,
        "blockers": blockers,
        "next_step": (
            "Use this as the 70pct-WR repair lane: prioritize candidate-forward current-poll proof and "
            "copyability fixes for the highest-WR positive-ROI wallet instead of over-focusing on a clean "
            "runtime-proof wallet whose paper WR/ROI fail."
        ),
    }


def _profitable_copy_efficiency_rank(row: dict[str, Any]) -> float:
    active_buy_events = int(num(row.get("active_buy_events")))
    copied = int(num(row.get("active_copied_buy_events")))
    rejected = int(num(row.get("active_rejected_buy_events")))
    fill_rate = num(row.get("active_fill_rate_pct"))
    fresh_le_10s = int(num(row.get("active_fresh_le_10s")))
    fresh_le_30s = int(num(row.get("active_fresh_le_30s")))
    source_pnl = num(row.get("source_leaderboard_best_pnl_usd"))
    source_bonus = 250.0 if row.get("source_high_confidence") is True else 0.0
    return round(
        min(max(source_pnl, 0.0) / 100.0, 2000.0)
        + min(active_buy_events, 1000) * 2.0
        + min(copied, 1000) * 2.5
        + fill_rate * 8.0
        + fresh_le_10s * 4.0
        + fresh_le_30s * 1.5
        + source_bonus
        - rejected * 1.25,
        6,
    )


def _profitable_copy_efficiency_candidates(wallet_analysis: dict[str, Any]) -> list[dict[str, Any]]:
    summary = _dict(wallet_analysis.get("summary"))
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(row: Any) -> None:
        item = _dict(row)
        wallet = str(item.get("wallet") or "").lower()
        if not wallet or wallet in seen:
            return
        seen.add(wallet)
        candidates.append(item)

    for key in (
        "best_source_current_copyable_wallet_large_sample",
        "best_source_current_copyable_wallet",
        "best_current_copyable_wallet_large_sample",
        "best_current_copyable_wallet",
        "best_copyable_wallet",
    ):
        add(summary.get(key))
    for key in (
        "top_source_current_copyable_wallets_large_sample",
        "top_source_current_copyable_wallets",
        "top_current_copyable_wallets_large_sample",
        "top_current_copyable_wallets",
        "top_copyable_wallets",
    ):
        for row in _list(wallet_analysis.get(key)):
            add(row)
    return candidates


def _profitable_copy_efficiency_lane(wallet_analysis: dict[str, Any]) -> dict[str, Any]:
    candidates = _profitable_copy_efficiency_candidates(wallet_analysis)
    ranked = sorted(candidates, key=_profitable_copy_efficiency_rank, reverse=True)
    best = ranked[0] if ranked else {}
    active_buy_events = int(num(best.get("active_buy_events")))
    copied = int(num(best.get("active_copied_buy_events")))
    rejected = int(num(best.get("active_rejected_buy_events")))
    fill_rate = num(best.get("active_fill_rate_pct"))
    fresh_le_10s = int(num(best.get("active_fresh_le_10s")))
    fresh_le_30s = int(num(best.get("active_fresh_le_30s")))
    source_pnl = num(best.get("source_leaderboard_best_pnl_usd"))
    paper_orders = int(num(best.get("paper_orders")))
    paper_resolved = int(num(best.get("paper_resolved_orders")))
    paper_roi = num(best.get("paper_roi_pct"))
    paper_wr = num(best.get("paper_wr_pct"))
    dominant_reject = str(best.get("dominant_active_all_order_reject_reason") or "")
    dominant_copyability = str(best.get("dominant_active_copyability_reason") or "")
    fresh_10_of_30_pct = _pct(fresh_le_10s, fresh_le_30s) if fresh_le_30s > 0 else None

    blockers: list[str] = []
    if not best:
        blockers.append("profitable_wallet_copy_efficiency_candidate_missing")
    if source_pnl <= 0.0 and best.get("source_high_confidence") is not True:
        blockers.append("profitable_wallet_source_profit_not_positive_or_unproven")
    if active_buy_events < MIN_PROFITABLE_COPY_ACTIVE_BUYS:
        blockers.append("profitable_wallet_active_buy_sample_below_20")
    if fill_rate < MIN_PROFITABLE_COPY_DEVELOPMENT_FILL_RATE_PCT:
        blockers.append("profitable_wallet_development_fill_rate_below_70pct")
    if fill_rate < TARGET_PROFITABLE_COPY_FILL_RATE_PCT:
        blockers.append("profitable_wallet_live_fill_rate_below_95pct")
    if rejected > 0:
        blockers.append("profitable_wallet_rejected_buys_present")
    if fresh_le_10s <= 0 and active_buy_events > 0:
        blockers.append("profitable_wallet_no_fresh_buy_events_le_10s")
    if fresh_le_30s > fresh_le_10s and (fresh_10_of_30_pct is None or fresh_10_of_30_pct < 80.0):
        blockers.append("profitable_wallet_source_latency_gap_le_10_vs_30")
    if dominant_reject in {"clob_price_above_slippage_cap", "best_ask_above_slippage_cap"}:
        blockers.append("profitable_wallet_slippage_shadow_policy_needed")
    if dominant_copyability in {"event_age_above_cap", "fetch_duration_above_cap"}:
        blockers.append("profitable_wallet_source_latency_copyability_blocker")
    if paper_resolved < MIN_RESOLVED_ORDERS or paper_roi < MIN_ROI_PCT or paper_wr < MIN_WR_PCT:
        blockers.append("profitable_wallet_paper_profit_proof_missing_or_below_gate")

    return {
        "id": "profitable_wallet_copy_efficiency",
        "label": "Develop the best profitable wallet order-flow copy surface in paper",
        "status": _status_from_blockers(
            blockers,
            has_repair_defect=bool(
                rejected > 0
                or fill_rate < TARGET_PROFITABLE_COPY_FILL_RATE_PCT
                or "latency" in " ".join(blockers)
                or "slippage" in " ".join(blockers)
            ),
        ),
        "rank_score": _profitable_copy_efficiency_rank(best) if best else 0.0,
        "wallet": best.get("wallet"),
        "wallet_name": best.get("name") or best.get("wallet_name"),
        "metrics": {
            "source_high_confidence": bool(best.get("source_high_confidence")),
            "source_leaderboard_best_pnl_usd": round(source_pnl, 6),
            "active_buy_events": active_buy_events,
            "active_copied_buy_events": copied,
            "active_rejected_buy_events": rejected,
            "active_fill_rate_pct": round(fill_rate, 6),
            "active_fresh_le_10s": fresh_le_10s,
            "active_fresh_le_30s": fresh_le_30s,
            "fresh_le_10s_of_30s_pct": round(num(fresh_10_of_30_pct), 6) if fresh_10_of_30_pct is not None else None,
            "dominant_active_all_order_reject_reason": dominant_reject or None,
            "dominant_active_copyability_reason": dominant_copyability or None,
            "copy_edge_loss_primary_reason": best.get("copy_edge_loss_primary_reason"),
            "best_surface": best.get("best_surface"),
            "paper_orders": paper_orders,
            "paper_resolved_orders": paper_resolved,
            "paper_roi_pct": round(paper_roi, 6),
            "paper_wr_pct": round(paper_wr, 6),
        },
        "blockers": blockers,
        "next_step": (
            "Keep this wallet pinned in current-poll measurement, run paper-only strict-vs-relaxed copyability "
            "profiles on its BUY stream, and repair source latency/slippage/fill defects until >=100 resolved "
            "paper orders, positive ROI, >=70pct WR, and low reject/miss rates are proven without live orders."
        ),
    }


def _all_order_multi_wallet(wallet_analysis: dict[str, Any], active_tracking: dict[str, Any]) -> dict[str, Any]:
    summary = _dict(wallet_analysis.get("summary"))
    active_probe = _dict(summary.get("active_all_order_micro_batch_probe"))
    active_all_order = _dict(_dict(active_tracking.get("summary")).get("all_order_exact_copy"))
    source_events = int(
        num(
            active_all_order.get("buy_source_events"),
            num(active_all_order.get("source_buy_intents"), num(active_all_order.get("source_events"))),
        )
    )
    copied = int(num(active_all_order.get("clob_filled_buy_copy_events")))
    if copied <= 0 and "clob_filled_buy_copy_events" not in active_all_order:
        copied = int(num(active_all_order.get("filled_buy_copy_events")))
    filled_total = int(num(active_all_order.get("filled_buy_copy_events")))
    rejected = int(num(active_all_order.get("rejected_buy_copy_events")))
    fallback = int(num(active_all_order.get("fallback_filled_buy_copy_events")))
    if source_events <= 0:
        source_events = int(num(active_probe.get("source_buy_intents")))
    if copied <= 0:
        copied = int(num(active_probe.get("filled_child_events")))
    if rejected <= 0:
        rejected = int(num(active_probe.get("rejected_child_events")))
    denominator = source_events if source_events > 0 else copied + rejected + fallback
    fill_rate = _pct(copied, denominator) or 0.0
    reject_rate = _pct(rejected, denominator) or 0.0
    fallback_rate = _pct(fallback, denominator) or 0.0
    blockers: list[str] = []
    if source_events <= 0:
        blockers.append("all_order_no_current_source_buy_events")
    if fill_rate < MIN_CLOB_FILL_RATE_PCT:
        blockers.append("all_order_fill_rate_below_99pct")
    if reject_rate > MAX_REJECT_RATE_PCT:
        blockers.append("all_order_reject_rate_above_5pct")
    if fallback > 0:
        blockers.append("all_order_fallback_fills_present")
    if active_probe.get("micro_batch_exact_no_overcopy", {}).get("status") != PASS:
        blockers.append("exact_micro_batch_no_overcopy_not_passing")
    return {
        "id": "multi_wallet_all_order_exact_copy",
        "label": "Copy multiple wallets and every order exactly",
        "status": _status_from_blockers(blockers, has_repair_defect=bool(rejected or fallback)),
        "rank_score": round(fill_rate - reject_rate - fallback * 10.0, 6),
        "metrics": {
            "source_buy_events": source_events,
            "clob_or_book_filled_buy_events": copied,
            "filled_buy_events": filled_total,
            "rejected_buy_events": rejected,
            "fallback_buy_events": fallback,
            "fill_rate_pct": round(fill_rate, 6),
            "reject_rate_pct": round(reject_rate, 6),
            "fallback_rate_pct": round(fallback_rate, 6),
            "copyability_accepted_buy_events": int(num(active_all_order.get("copyability_accepted_buy_events"))),
            "copyability_rejected_buy_events": int(num(active_all_order.get("copyability_rejected_buy_events"))),
            "active_micro_batch_status": active_probe.get("status"),
            "exact_no_overcopy_status": _dict(active_probe.get("micro_batch_exact_no_overcopy")).get("status"),
            "min_order_overcopy_research_status": _dict(active_probe.get("micro_batch_min_order_research")).get("status"),
            "reject_reason_counts": active_probe.get("reject_reason_counts") or active_all_order.get("reject_reason_counts") or {},
            "copyability_reason_counts": active_all_order.get("copyability_reason_counts") or {},
        },
        "blockers": blockers,
        "next_step": (
            "Keep this as a diagnostic lane; do not choose it as primary live architecture until exact "
            "no-overcopy micro-batches and ordinary CopyIntent all-order fills pass with near-zero rejects."
        ),
    }


def _weighted_inventory(
    profit_state: dict[str, Any],
    active_hotlane: dict[str, Any],
    active_tracking: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate = _dict(profit_state.get("best_candidate"))
    summary = _dict(candidate.get("summary"))
    validation = _dict(candidate.get("validation_summary"))
    live_target = _dict(candidate.get("live_target_profile"))
    fill_evidence = _dict(candidate.get("fill_evidence_summary") or candidate.get("executable_copy_summary"))
    window_metrics = _dict(summary.get("window_metrics"))
    selected_cohorts = [_dict(row) for row in _list(active_hotlane.get("selected_cohorts"))]
    blockers = [str(blocker) for blocker in _list(candidate.get("blockers"))]
    if candidate.get("candidate_type") != "MULTI_WALLET_INVENTORY":
        blockers.append("best_candidate_is_not_multi_wallet_inventory")
    if int(num(summary.get("resolved_orders"))) < MIN_RESOLVED_ORDERS:
        blockers.append("inventory_resolved_orders_below_100")
    if num(summary.get("wr_pct")) < MIN_WR_PCT:
        blockers.append("inventory_wr_below_70pct")
    if num(validation.get("wr_pct")) < MIN_WR_PCT:
        blockers.append("inventory_validation_wr_below_70pct")
    if num(window_metrics.get("avg_orders_per_window")) < MIN_AVG_ORDERS_PER_WINDOW:
        blockers.append("inventory_avg_orders_per_window_below_2")
    if num(fill_evidence.get("candidate_clob_backed_fill_rate_pct")) < MIN_CLOB_FILL_RATE_PCT:
        blockers.append("inventory_missing_current_clob_fill_evidence")
    if num(summary.get("unresolved_ratio")) > 0.5:
        blockers.append("inventory_unresolved_ratio_above_50pct")
    hot_path = _dict(_dict(_dict(active_tracking).get("summary")).get("hot_path_adaptive"))
    bridge_burnin = _dict(hot_path.get("current_poll_inventory_bridge_burnin"))
    window_indexed_bridge_burnin = _dict(hot_path.get("window_indexed_inventory_bridge_burnin"))
    live_feed_bridge_burnin = _dict(hot_path.get("live_feed_inventory_bridge_burnin"))
    bridge_candidate_ids = {
        str(item)
        for item in _list(bridge_burnin.get("candidate_ids"))
        if str(item or "")
    }
    bridge_candidate_id = str(bridge_burnin.get("candidate_id") or "")
    if bridge_candidate_id:
        bridge_candidate_ids.add(bridge_candidate_id)
    candidate_id = str(candidate.get("candidate_id") or "")
    bridge_matches_candidate = bool(candidate_id and candidate_id in bridge_candidate_ids)
    current_poll_truth_attached = bool(bridge_matches_candidate and bridge_burnin.get("status") == PASS)
    window_indexed_bridge_candidate_ids = {
        str(item)
        for item in _list(window_indexed_bridge_burnin.get("candidate_ids"))
        if str(item or "")
    }
    window_indexed_bridge_candidate_id = str(window_indexed_bridge_burnin.get("candidate_id") or "")
    if window_indexed_bridge_candidate_id:
        window_indexed_bridge_candidate_ids.add(window_indexed_bridge_candidate_id)
    window_indexed_bridge_matches_candidate = bool(candidate_id and candidate_id in window_indexed_bridge_candidate_ids)
    window_indexed_truth_attached = bool(
        window_indexed_bridge_matches_candidate and int(num(window_indexed_bridge_burnin.get("raw_rows"))) > 0
    )
    live_feed_bridge_candidate_ids = {
        str(item)
        for item in _list(live_feed_bridge_burnin.get("candidate_ids"))
        if str(item or "")
    }
    live_feed_bridge_candidate_id = str(live_feed_bridge_burnin.get("candidate_id") or "")
    if live_feed_bridge_candidate_id:
        live_feed_bridge_candidate_ids.add(live_feed_bridge_candidate_id)
    live_feed_bridge_matches_candidate = bool(candidate_id and candidate_id in live_feed_bridge_candidate_ids)
    live_feed_truth_attached = bool(
        live_feed_bridge_matches_candidate and live_feed_bridge_burnin.get("status") == PASS
    )
    if bridge_candidate_ids and candidate_id and not bridge_matches_candidate:
        blockers.append("inventory_current_poll_bridge_target_mismatch")
    if window_indexed_bridge_candidate_ids and candidate_id and not window_indexed_bridge_matches_candidate:
        blockers.append("inventory_window_indexed_bridge_target_mismatch")
    if live_feed_bridge_candidate_ids and candidate_id and not live_feed_bridge_matches_candidate:
        blockers.append("inventory_live_feed_bridge_target_mismatch")
    if (
        window_indexed_bridge_burnin
        and window_indexed_bridge_matches_candidate
        and int(num(window_indexed_bridge_burnin.get("raw_rows"))) > 0
        and window_indexed_bridge_burnin.get("status") != PASS
    ):
        blockers.append("inventory_window_indexed_bridge_multi_wallet_or_buy_truth_missing")
    if (
        live_feed_bridge_burnin
        and live_feed_bridge_matches_candidate
        and live_feed_bridge_burnin.get("status") != PASS
        and int(num(live_feed_bridge_burnin.get("matching_buy_events"))) > 0
    ):
        blockers.append("inventory_live_feed_bridge_clob_truth_or_freshness_missing")
    return {
        "id": "weighted_wallet_inventory_by_window",
        "label": "Build weighted wallet-based inventory every window",
        "status": _status_from_blockers(blockers),
        "rank_score": round(
            min(num(summary.get("roi_pct")), 200.0)
            + num(summary.get("wr_pct")) * 2.0
            + min(int(num(summary.get("resolved_orders"))), 1000) * 0.2
            + min(len(selected_cohorts), 20) * 3.0
            - (100.0 - num(fill_evidence.get("candidate_clob_backed_fill_rate_pct"))),
            6,
        ),
        "candidate": {
            "candidate_id": candidate.get("candidate_id"),
            "candidate_type": candidate.get("candidate_type"),
            "policy_id": _dict(candidate.get("policy")).get("policy_id"),
            "orders": int(num(summary.get("orders"))),
            "resolved_orders": int(num(summary.get("resolved_orders"))),
            "pnl_usd": round(num(summary.get("pnl_usd")), 6),
            "roi_pct": round(num(summary.get("roi_pct")), 6),
            "wr_pct": round(num(summary.get("wr_pct")), 6),
            "validation_wr_pct": round(num(validation.get("wr_pct")), 6),
            "avg_orders_per_window": round(num(window_metrics.get("avg_orders_per_window")), 6),
            "unresolved_ratio": round(num(summary.get("unresolved_ratio")), 6),
            "clob_backed_fill_rate_pct": round(num(fill_evidence.get("candidate_clob_backed_fill_rate_pct")), 6),
            "fallback_filled_orders": int(num(fill_evidence.get("candidate_fallback_filled_orders"))),
            "inventory_profile": _dict(_dict(candidate.get("metadata")).get("inventory_profile")),
            "current_poll_inventory_bridge_burnin": bridge_burnin,
            "current_poll_bridge_candidate_ids": sorted(bridge_candidate_ids),
            "current_poll_bridge_matches_candidate": bridge_matches_candidate,
            "current_poll_clob_truth_attached": current_poll_truth_attached,
            "current_poll_clob_truth_preserves_historical_fallback_blocker": bool(
                current_poll_truth_attached and int(num(fill_evidence.get("candidate_fallback_filled_orders"))) > 0
            ),
            "window_indexed_inventory_bridge_burnin": window_indexed_bridge_burnin,
            "window_indexed_bridge_candidate_ids": sorted(window_indexed_bridge_candidate_ids),
            "window_indexed_bridge_matches_candidate": window_indexed_bridge_matches_candidate,
            "window_indexed_truth_attached": window_indexed_truth_attached,
            "live_feed_inventory_bridge_burnin": live_feed_bridge_burnin,
            "live_feed_bridge_candidate_ids": sorted(live_feed_bridge_candidate_ids),
            "live_feed_bridge_matches_candidate": live_feed_bridge_matches_candidate,
            "live_feed_clob_truth_attached": live_feed_truth_attached,
            "live_feed_clob_truth_preserves_historical_fallback_blocker": bool(
                live_feed_truth_attached and int(num(fill_evidence.get("candidate_fallback_filled_orders"))) > 0
            ),
        },
        "active_hotlane_cohorts": {
            "selected_cohorts": len(selected_cohorts),
            "top_cohorts": selected_cohorts[:5],
            "selection_order_basis": active_hotlane.get("selection_order_basis"),
        },
        "blockers": blockers,
        "next_step": (
            "Treat this as the target profit architecture, but require fresh candidate-specific window-indexed "
            "bridge evidence with CLOB-backed fills and at least 100 resolved orders before live admission."
        ),
    }


def _multi_wallet_filter(active_tracking: dict[str, Any], active_hotlane: dict[str, Any]) -> dict[str, Any]:
    hot_path = _dict(_dict(active_tracking.get("summary")).get("hot_path_adaptive"))
    tracker_replay = _dict(hot_path.get("tracker_time_replay"))
    tracker_summary = _dict(tracker_replay.get("summary"))
    selected_cohorts = [_dict(row) for row in _list(active_hotlane.get("selected_cohorts"))]
    current_poll_moves = int(num(_hot_path_metric(hot_path, "current_poll_moves")))
    pass_signals = int(num(_hot_path_metric(hot_path, "pass_signals")))
    fresh = int(num(_hot_path_metric(hot_path, "runtime_fresh_buy_events_le_cap")))
    eligible_wallets = int(num(_hot_path_metric(hot_path, "runtime_eligible_wallets")))
    intents = int(num(_hot_path_metric(hot_path, "hot_path_intents_created")))
    fills = int(num(_hot_path_metric(hot_path, "hot_path_filled_orders")))
    rejects = int(num(_hot_path_metric(hot_path, "hot_path_rejected_orders")))
    blockers = [str(blocker) for blocker in _list(hot_path.get("blockers"))]
    if not selected_cohorts:
        blockers.append("no_selected_multi_wallet_cohorts")
    if eligible_wallets < 2:
        blockers.append("current_poll_has_less_than_two_eligible_wallets")
    if pass_signals <= 0:
        blockers.append("no_current_poll_multi_wallet_pass_signals")
    if fills <= 0:
        blockers.append("no_current_poll_multi_wallet_paper_fills")
    if rejects > 0:
        blockers.append("current_poll_multi_wallet_rejects_present")
    return {
        "id": "multi_wallet_filter_consensus",
        "label": "Send orders only when multiple wallets agree/filter passes",
        "status": _status_from_blockers(blockers, has_repair_defect=bool(rejects)),
        "rank_score": round(
            pass_signals * 40.0
            + fills * 30.0
            + eligible_wallets * 20.0
            + min(len(selected_cohorts), 20) * 4.0
            + int(num(tracker_summary.get("inventory_candidates"))) * 3.0
            - rejects * 50.0,
            6,
        ),
        "metrics": {
            "current_poll_moves": current_poll_moves,
            "pass_signals": pass_signals,
            "runtime_fresh_buy_events_le_cap": fresh,
            "runtime_eligible_wallets": eligible_wallets,
            "paper_intents": intents,
            "paper_fills": fills,
            "paper_rejects": rejects,
            "tracker_time_replay_status": tracker_replay.get("status"),
            "tracker_time_replay_pass_signals": int(num(tracker_summary.get("pass_signals"))),
            "tracker_time_inventory_candidates": int(num(tracker_summary.get("inventory_candidates"))),
            "selected_cohorts": len(selected_cohorts),
            "evidence_source": hot_path.get("evidence_source") or "active_tracking_state",
        },
        "blockers": blockers,
        "next_step": (
            "Use tracker-time cohorts for poll ranking only, then force current-poll proof with two or more "
            "wallets agreeing inside the same BTC 5m window."
        ),
    }


def _direction_by_id(directions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("id") or ""): row for row in directions if isinstance(row, dict) and row.get("id")}


def _previous_limit_lanes(previous_strategy_state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    review = (
        previous_strategy_state.get("development_limit_review")
        if isinstance(previous_strategy_state.get("development_limit_review"), dict)
        else {}
    )
    lanes = review.get("lanes") if isinstance(review.get("lanes"), dict) else {}
    return {str(key): _dict(value) for key, value in lanes.items()}


def _development_limit_review(
    *,
    directions: list[dict[str, Any]],
    previous_strategy_state: dict[str, Any] | None,
) -> dict[str, Any]:
    previous_state = _dict(previous_strategy_state)
    previous_directions = _direction_by_id(_list(previous_state.get("directions")))
    previous_limits = _previous_limit_lanes(previous_state)
    lanes: dict[str, dict[str, Any]] = {}
    limit_hit_lanes: list[dict[str, Any]] = []

    for direction in directions:
        lane_id = str(direction.get("id") or "")
        policy = DEVELOPMENT_LANE_LIMITS.get(lane_id)
        if not policy:
            continue
        previous_direction = _dict(previous_directions.get(lane_id))
        previous_lane_limit = _dict(previous_limits.get(lane_id))
        current_blockers = sorted(str(blocker) for blocker in _list(direction.get("blockers")) if blocker)
        previous_blockers = sorted(str(blocker) for blocker in _list(previous_direction.get("blockers")) if blocker)
        blocker_set_same = bool(previous_direction) and current_blockers == previous_blockers
        blocker_count_improved = bool(previous_direction) and len(current_blockers) < len(previous_blockers)
        current_status = str(direction.get("status") or "")
        previous_status = str(previous_direction.get("status") or "")
        current_metrics = _metric_snapshot(direction, policy)
        previous_metrics = _metric_snapshot(previous_direction, policy)
        metric_improved = _metric_snapshot_improved(current_metrics, previous_metrics, policy=policy)
        improved = (
            not previous_direction
            or current_status == PASS
            or _status_improved(current_status, previous_status)
            or blocker_count_improved
            or metric_improved
        )
        previous_no_improvement = int(num(previous_lane_limit.get("cycles_without_improvement")))
        previous_same_blockers = int(num(previous_lane_limit.get("same_blocker_cycles")))
        if current_status == PASS:
            cycles_without_improvement = 0
            same_blocker_cycles = 0
        else:
            cycles_without_improvement = 0 if improved else previous_no_improvement + 1
            same_blocker_cycles = previous_same_blockers + 1 if blocker_set_same and current_blockers else 0
        max_no_improvement = int(num(policy.get("max_no_improvement_cycles"), 3))
        max_same_blockers = int(num(policy.get("max_same_blocker_cycles"), 3))
        limit_hit = bool(
            current_status != PASS
            and (
                cycles_without_improvement >= max_no_improvement
                or same_blocker_cycles >= max_same_blockers
            )
        )
        lane_review = {
            "lane_id": lane_id,
            "status": current_status,
            "logical_limits": {
                "max_no_improvement_cycles": max_no_improvement,
                "max_same_blocker_cycles": max_same_blockers,
                "max_deep_research_attempts_before_reevaluation": int(
                    num(policy.get("max_deep_research_attempts_before_reevaluation"), 1)
                ),
            },
            "cycles_without_improvement": cycles_without_improvement,
            "same_blocker_cycles": same_blocker_cycles,
            "improved_since_previous": bool(improved),
            "metric_improved_since_previous": bool(metric_improved),
            "blocker_count_improved_since_previous": bool(blocker_count_improved),
            "blocker_set_same_as_previous": bool(blocker_set_same),
            "metric_snapshot": current_metrics,
            "previous_metric_snapshot": previous_metrics,
            "blockers": current_blockers,
            "limit_hit": limit_hit,
            "required_change_action": policy.get("required_change_action"),
        }
        lanes[lane_id] = lane_review
        if limit_hit:
            limit_hit_lanes.append(
                {
                    "lane_id": lane_id,
                    "status": current_status,
                    "cycles_without_improvement": cycles_without_improvement,
                    "same_blocker_cycles": same_blocker_cycles,
                    "required_change_action": policy.get("required_change_action"),
                    "priority": int(num(policy.get("priority"), 100)),
                    "blockers": current_blockers,
                }
            )

    ranked_hits = sorted(limit_hit_lanes, key=lambda row: int(num(row.get("priority"), 100)))
    next_action = (
        str(ranked_hits[0].get("required_change_action"))
        if ranked_hits
        else "continue_current_lane_until_logical_limit_or_live_ready"
    )
    return {
        "schema_version": 1,
        "status": CORRECTION if ranked_hits else PASS,
        "rule": (
            "each development lane has a finite logical budget; when blockers and metrics stop improving, "
            "the workflow must reevaluate, rerank, rebuild, or repair the lane instead of repeating it"
        ),
        "limit_hit": bool(ranked_hits),
        "limit_hit_lanes": ranked_hits,
        "next_change_action": next_action,
        "lanes": lanes,
    }


def _development_program_review(
    *,
    directions: list[dict[str, Any]],
    profit_state: dict[str, Any],
    leaderboard_state: dict[str, Any] | None,
    development_limit_review: dict[str, Any],
) -> dict[str, Any]:
    by_id = _direction_by_id(directions)
    leaderboard_summary = _dict(_dict(leaderboard_state).get("summary"))
    unique_wallets = int(num(leaderboard_summary.get("unique_wallets")))
    current_unique_wallets = int(num(leaderboard_summary.get("current_unique_wallets"), unique_wallets))
    profitable = _dict(by_id.get("profitable_wallet_copy_efficiency"))
    profitable_metrics = _dict(profitable.get("metrics"))
    single = _dict(by_id.get("single_wallet_best_copyable"))
    wr_repair = _dict(by_id.get("wr_repair_single_wallet"))
    all_order = _dict(by_id.get("multi_wallet_all_order_exact_copy"))
    all_order_metrics = _dict(all_order.get("metrics"))
    inventory = _dict(by_id.get("weighted_wallet_inventory_by_window"))
    inventory_candidate = _dict(inventory.get("candidate"))
    current_poll_bridge = _dict(inventory_candidate.get("current_poll_inventory_bridge_burnin"))
    window_indexed_bridge = _dict(inventory_candidate.get("window_indexed_inventory_bridge_burnin"))
    live_feed_bridge = _dict(inventory_candidate.get("live_feed_inventory_bridge_burnin"))
    multi_filter = _dict(by_id.get("multi_wallet_filter_consensus"))
    filter_metrics = _dict(multi_filter.get("metrics"))
    bridge_state = _dict(profit_state.get("development_program_bridge"))
    bridge_active = bool(
        bridge_state.get("active")
        and int(num(bridge_state.get("forward_queue_bridge_entries"))) > 0
    )
    bridge_wallets = [str(wallet).lower() for wallet in _list(bridge_state.get("forward_queue_bridge_wallets"))]
    individual_universe = _dict(profit_state.get("individual_wallet_copy_universe"))
    individual_coverage = _dict(individual_universe.get("coverage"))
    inventory_universe = _dict(profit_state.get("multi_wallet_inventory_universe"))
    inventory_coverage = _dict(inventory_universe.get("coverage"))
    per_wallet_source_buys = int(num(individual_coverage.get("source_buy_events")))
    per_wallet_source_wallets = int(num(individual_coverage.get("source_wallets_with_buy_events")))
    inventory_order_slots = int(num(inventory_coverage.get("inventory_candidate_order_slots")))
    inventory_candidate_count = int(num(inventory_coverage.get("inventory_candidate_count")))

    traps: list[dict[str, Any]] = []
    stop_doing: list[str] = []
    hypotheses: list[dict[str, Any]] = []

    single_proof_pass = single.get("status") == PASS or wr_repair.get("status") == PASS
    inventory_blocked = inventory.get("status") != PASS
    if individual_universe and individual_universe.get("status") != PASS:
        traps.append(
            {
                "id": "per_wallet_copy_universe_below_scale_target",
                "meaning": (
                    "the task requires copying many leaderboard wallets individually, but the persisted profit "
                    "state has not yet reached the scale target for source BUYs, wallets, or resolved order slots"
                ),
                "required_change": "expand_or_resume_per_wallet_copy_universe_before_inventory_burnin",
            }
        )
        stop_doing.append("do_not_keep_a_narrow_forward_queue_as_the_primary_wallet_universe")
    if inventory_universe and inventory_universe.get("status") != PASS:
        traps.append(
            {
                "id": "multi_wallet_inventory_universe_not_scaled_or_not_profitable",
                "meaning": (
                    "the per-wallet copy surface must be promoted into hundreds/thousands of weighted inventory "
                    "order slots before live readiness can be evaluated"
                ),
                "required_change": "rebuild_scaled_multi_wallet_inventory_from_full_per_wallet_copy_universe",
            }
        )
    if (
        (per_wallet_source_buys >= TARGET_PER_WALLET_COPY_SOURCE_BUYS
        or per_wallet_source_wallets >= TARGET_PER_WALLET_COPY_SOURCE_WALLETS)
        and (inventory_candidate_count <= 0 or inventory_order_slots < TARGET_MULTI_WALLET_INVENTORY_ORDER_SLOTS)
    ):
        traps.append(
            {
                "id": "large_per_wallet_copy_surface_not_promoted_to_inventory",
                "meaning": (
                    "there is already enough per-wallet source flow to stop treating single-wallet probing as "
                    "the product; the bottleneck is building and burning in multi-wallet inventory"
                ),
                "required_change": "rebuild_scaled_multi_wallet_inventory_from_full_per_wallet_copy_universe",
            }
        )
        stop_doing.append("do_not_treat_single_wallet_probe_queue_as_the_final_architecture")
    if single_proof_pass and inventory_blocked:
        traps.append(
            {
                "id": "single_wallet_proof_not_promoted_to_multi_wallet_inventory",
                "meaning": (
                    "single-wallet copyability has usable proof, but the target live architecture still cannot "
                    "turn that proof into a multi-wallet inventory candidate"
                ),
                "required_change": "build_current_poll_inventory_bridge_from_profitable_single_wallet_and_weighted_inventory_candidate",
            }
        )
        stop_doing.append("do_not_spend_primary_cycles_proving_the_same_single_wallet_again")

    active_buys = int(num(profitable_metrics.get("active_buy_events")))
    fill_rate = num(profitable_metrics.get("active_fill_rate_pct"))
    rejected = int(num(profitable_metrics.get("active_rejected_buy_events")))
    if (
        unique_wallets >= PROGRAM_RETHINK_UNIVERSE_WALLET_FLOOR
        and active_buys >= PROGRAM_RETHINK_ACTIVE_BUY_FLOOR
        and (fill_rate < MIN_PROFITABLE_COPY_DEVELOPMENT_FILL_RATE_PCT or rejected > 0)
    ):
        traps.append(
            {
                "id": "large_wallet_universe_but_copy_execution_edge_loss",
                "meaning": (
                    "the observation universe is already large enough that more wallet discovery is unlikely "
                    "to be the primary unlock while fill/reject/latency defects remain dominant"
                ),
                "required_change": "repair_copy_execution_latency_slippage_and_rejects_before_more_wallet_expansion",
            }
        )
        stop_doing.append("do_not_treat_more_leaderboard_pages_as_the_primary_solution")

    inventory_profitable = (
        num(inventory_candidate.get("roi_pct")) >= MIN_ROI_PCT
        and num(inventory_candidate.get("wr_pct")) >= MIN_WR_PCT
    )
    clob_fill_rate = num(inventory_candidate.get("clob_backed_fill_rate_pct"))
    fallback_orders = int(num(inventory_candidate.get("fallback_filled_orders")))
    if inventory_profitable and (clob_fill_rate < MIN_CLOB_FILL_RATE_PCT or fallback_orders > 0):
        traps.append(
            {
                "id": "paper_inventory_profit_without_current_clob_truth",
                "meaning": (
                    "the inventory hypothesis can look profitable in paper, but live readiness is blocked by "
                    "candidate-specific CLOB fill truth and fallback-filled paper evidence"
                ),
                "required_change": "attach_current_poll_clob_truth_to_weighted_inventory_candidate",
            }
        )
        stop_doing.append("do_not_count_fallback_inventory_profit_as_live_admissible_progress")

    tracker_inventory_candidates = int(num(filter_metrics.get("tracker_time_inventory_candidates")))
    pass_signals = int(num(filter_metrics.get("pass_signals")))
    eligible_wallets = int(num(filter_metrics.get("runtime_eligible_wallets")))
    if tracker_inventory_candidates > 0 and (pass_signals <= 0 or eligible_wallets < 2):
        traps.append(
            {
                "id": "tracker_replay_inventory_not_reaching_current_poll",
                "meaning": (
                    "tracker-time inventory research exists, but the current-poll filter does not produce "
                    "enough fresh agreeing wallets for admissible execution"
                ),
                "required_change": "rebuild_current_poll_cohort_router_from_tracker_replay_and_leaderboard_registry",
            }
        )

    if int(num(all_order_metrics.get("fallback_buy_events"))) > 0 or int(num(all_order_metrics.get("rejected_buy_events"))) > 0:
        traps.append(
            {
                "id": "all_order_copyintent_lifecycle_not_parity_clean",
                "meaning": (
                    "all-order exact copy still has fallback or rejected BUY evidence, so the live-equivalent "
                    "CopyIntent lifecycle is not clean enough for admission"
                ),
                "required_change": "repair_all_order_copyintent_lifecycle_before_live_admission",
            }
        )

    hypotheses.append(
        {
            "id": "copy_each_wallet_individually_then_promote_to_inventory",
            "status": (
                "SCALE_REQUIRED"
                if individual_universe and individual_universe.get("status") != PASS
                else (
                    "INVENTORY_PROMOTION_REQUIRED"
                    if inventory_universe and inventory_universe.get("status") != PASS
                    else "ACTIVE"
                )
            ),
            "evidence": {
                "per_wallet_source_wallets_with_buy_events": per_wallet_source_wallets,
                "per_wallet_source_buy_events": per_wallet_source_buys,
                "single_wallet_candidate_count": int(num(individual_coverage.get("single_wallet_candidate_count"))),
                "candidate_policy_resolved_order_slots": int(
                    num(individual_coverage.get("candidate_policy_resolved_order_slots"))
                ),
                "inventory_candidate_count": inventory_candidate_count,
                "inventory_candidate_order_slots": inventory_order_slots,
                "inventory_candidate_resolved_order_slots": int(
                    num(inventory_coverage.get("inventory_candidate_resolved_order_slots"))
                ),
            },
            "falsification_rule": (
                "if the per-wallet universe cannot reach hundreds/thousands of BUY/order slots, the next "
                "change is coverage/resume; if it can, the next change is multi-wallet inventory burn-in"
            ),
        }
    )
    hypotheses.append(
        {
            "id": "more_profitable_leaderboard_wallets_will_solve_it",
            "status": "DEPRIORITIZE" if "do_not_treat_more_leaderboard_pages_as_the_primary_solution" in stop_doing else "ACTIVE",
            "evidence": {
                "unique_wallets": unique_wallets,
                "current_unique_wallets": current_unique_wallets,
                "profitable_lane_active_buy_events": active_buys,
                "profitable_lane_fill_rate_pct": round(fill_rate, 6),
                "profitable_lane_rejected_buy_events": rejected,
            },
            "falsification_rule": (
                "if >=500 leaderboard wallets and >=100 active BUYs still show fill/reject/latency blockers, "
                "wallet discovery is no longer the primary unlock"
            ),
        }
    )
    hypotheses.append(
        {
            "id": "single_wallet_copyability_unlock_is_enough",
            "status": "BRIDGE_REQUIRED" if single_proof_pass and inventory_blocked else "ACTIVE",
            "evidence": {
                "single_wallet_status": single.get("status"),
                "wr_repair_status": wr_repair.get("status"),
                "inventory_status": inventory.get("status"),
            },
            "falsification_rule": (
                "if single-wallet proof passes while weighted inventory remains blocked, stop re-proving the "
                "single wallet and build the bridge into multi-wallet current-poll inventory"
            ),
        }
    )
    hypotheses.append(
        {
            "id": "paper_inventory_profit_is_live_relevant",
            "status": "CLOB_TRUTH_REQUIRED" if inventory_profitable and clob_fill_rate < MIN_CLOB_FILL_RATE_PCT else "ACTIVE",
            "evidence": {
                "inventory_roi_pct": round(num(inventory_candidate.get("roi_pct")), 6),
                "inventory_wr_pct": round(num(inventory_candidate.get("wr_pct")), 6),
                "inventory_clob_fill_rate_pct": round(clob_fill_rate, 6),
                "inventory_fallback_filled_orders": fallback_orders,
            },
            "falsification_rule": (
                "paper inventory remains research-only until current-poll candidate-specific CLOB truth replaces "
                "fallback-filled evidence"
            ),
        }
    )

    priority = [
        "per_wallet_copy_universe_below_scale_target",
        "large_per_wallet_copy_surface_not_promoted_to_inventory",
        "multi_wallet_inventory_universe_not_scaled_or_not_profitable",
        "single_wallet_proof_not_promoted_to_multi_wallet_inventory",
        "paper_inventory_profit_without_current_clob_truth",
        "tracker_replay_inventory_not_reaching_current_poll",
        "large_wallet_universe_but_copy_execution_edge_loss",
        "all_order_copyintent_lifecycle_not_parity_clean",
    ]
    traps_by_id = {str(row.get("id")): row for row in traps}
    selected_trap = next((traps_by_id[item] for item in priority if item in traps_by_id), {})
    full_rethink_required = bool(development_limit_review.get("limit_hit") or len(traps) >= 2)
    next_major_change = (
        str(selected_trap.get("required_change"))
        if selected_trap
        else (
            str(development_limit_review.get("next_change_action"))
            if development_limit_review.get("limit_hit")
            else "continue_current_hypothesis_until_program_rethink_trigger"
        )
    )
    bridge_burnin_actions = {
        "rebuild_scaled_multi_wallet_inventory_from_full_per_wallet_copy_universe",
        "build_current_poll_inventory_bridge_from_profitable_single_wallet_and_weighted_inventory_candidate",
        "attach_current_poll_clob_truth_to_weighted_inventory_candidate",
    }
    if bridge_active and next_major_change in bridge_burnin_actions:
        window_indexed_has_target_rows = bool(
            inventory_candidate.get("window_indexed_bridge_matches_candidate")
            and int(num(window_indexed_bridge.get("raw_rows"))) > 0
        )
        live_feed_has_target_rows = bool(
            inventory_candidate.get("live_feed_bridge_matches_candidate")
            and int(num(live_feed_bridge.get("matching_buy_events"))) > 0
        )
        current_poll_exhausted_without_rows = bool(
            current_poll_bridge.get("status") == WATCH
            and int(num(current_poll_bridge.get("attempted_wallet_count"))) > 0
            and int(num(current_poll_bridge.get("current_poll_moves"))) <= 0
        )
        if window_indexed_has_target_rows:
            next_major_change = "attach_window_indexed_inventory_bridge_truth_to_weighted_inventory_candidate"
        elif live_feed_has_target_rows and live_feed_bridge.get("status") != PASS:
            next_major_change = "attach_live_feed_inventory_bridge_clob_truth_to_weighted_inventory_candidate"
        elif current_poll_exhausted_without_rows:
            next_major_change = "migrate_bridge_burnin_to_window_indexed_wallet_events"
        else:
            next_major_change = "run_window_indexed_inventory_bridge_burnin_and_attach_truth"
    implementation_backlog: list[dict[str, Any]] = []
    if full_rethink_required:
        if bridge_active:
            implementation_backlog.append(
                {
                    "file": "src/wallet_copy/hotlane.py",
                    "function": "development bridge window-indexed burn-in routing",
                    "action": next_major_change,
                    "verify": (
                        "python3 scripts/select_wallet_copy_active_hotlane.py && "
                        "python3 scripts/run_wallet_copy_hotlane_tick.py --ticks 1 --force-development-bridge-probe"
                    ),
                }
            )
            implementation_backlog.append(
                {
                    "file": "src/wallet_copy/profit_engine.py",
                    "function": "development_program_bridge forward queue",
                    "action": (
                        "keep bridge queue pinned until target inventory has candidate-specific "
                        "window-indexed and CLOB-backed fills"
                    ),
                    "verify": (
                        "python3 scripts/run_wallet_copy_profit_engine.py --policy-preset fast "
                        "--max-wallets-for-search 0 --max-single-wallet-candidate-intents 0 "
                        "--max-multi-wallet-base-intents 0 "
                        "--live-today-sprint-operator-approval-id OP-LIVE-20260703-BELA"
                    ),
                }
            )
        else:
            implementation_backlog.append(
                {
                    "file": "src/wallet_copy/profit_engine.py",
                    "function": "individual_wallet_copy_universe / multi_wallet_inventory_universe",
                    "action": next_major_change,
                    "verify": (
                        "python3 scripts/select_wallet_copy_strategy_direction.py && "
                        "jq '.development_program_review' data/research/wallet_copy_strategy_direction_state.json"
                    ),
                }
            )
            implementation_backlog.append(
                {
                    "file": "src/wallet_copy/hotlane.py",
                    "function": "_strategy_direction_wallet_scores and hotlane cohort routing",
                    "action": "route proven single-wallet/current leaderboard evidence into fresh multi-wallet cohorts",
                    "verify": "python3 scripts/run_wallet_copy_hotlane_tick.py --ticks 1",
                }
            )

    return {
        "schema_version": 1,
        "status": CORRECTION if full_rethink_required else PASS,
        "full_rethink_required": full_rethink_required,
        "next_major_change_action": next_major_change,
        "bridge_applied": bridge_active,
        "bridge_forward_queue_entries": int(num(bridge_state.get("forward_queue_bridge_entries"))),
        "bridge_forward_queue_wallets": bridge_wallets,
        "rule": (
            "if the evidence says the current hypothesis is not the bottleneck, stop searching for a nonexistent "
            "green path in that lane and switch to the implementation bottleneck shown by the proof split"
        ),
        "strategic_traps": traps,
        "hypotheses": hypotheses,
        "stop_doing": sorted(set(stop_doing)),
        "implementation_backlog": implementation_backlog,
    }


def _profitability_proven(profit_state: dict[str, Any]) -> bool:
    pass_candidates = [_dict(row) for row in _list(profit_state.get("pass_candidates"))]
    if pass_candidates:
        return True
    decision = _dict(profit_state.get("decision"))
    runtime_candidate = _dict(profit_state.get("runtime_admission_candidate"))
    if (
        decision.get("status") == PASS
        and decision.get("live_admission_status") == PASS
        and runtime_candidate.get("status") == PASS
        and runtime_candidate.get("candidate_type") == "SINGLE_WALLET"
    ):
        return True
    candidate = _dict(profit_state.get("best_candidate"))
    if candidate.get("status") == PASS and not _list(candidate.get("blockers")):
        return True
    certificate = _dict(profit_state.get("live_readiness_certificate"))
    return bool(certificate.get("profitability_proven") and certificate.get("live_ready"))


def _certificate_primary_live_candidate(profit_state: dict[str, Any]) -> dict[str, Any]:
    decision = _dict(profit_state.get("decision"))
    runtime_candidate = _dict(profit_state.get("runtime_admission_candidate"))
    if (
        decision.get("status") == PASS
        and decision.get("live_admission_status") == PASS
        and runtime_candidate.get("status") == PASS
        and runtime_candidate.get("candidate_type") == "SINGLE_WALLET"
    ):
        summary = _dict(runtime_candidate.get("summary"))
        validation = _dict(runtime_candidate.get("validation_summary"))
        metadata = _dict(runtime_candidate.get("metadata"))
        policy = _dict(runtime_candidate.get("policy"))
        runtime_evidence = _dict(runtime_candidate.get("runtime_copy_evidence"))
        return {
            "candidate_id": runtime_candidate.get("candidate_id"),
            "wallet": metadata.get("source_wallet")
            or runtime_evidence.get("candidate_source_wallet")
            or runtime_candidate.get("source_wallet"),
            "status": runtime_candidate.get("status"),
            "policy_id": policy.get("policy_id") or runtime_evidence.get("candidate_policy_id"),
            "resolved_orders": summary.get("resolved_orders"),
            "wr_pct": summary.get("wr_pct"),
            "roi_pct": summary.get("roi_pct"),
            "validation_wr_pct": validation.get("wr_pct"),
        }
    certificate = _dict(profit_state.get("live_readiness_certificate"))
    if not (certificate.get("profitability_proven") and certificate.get("live_ready")):
        return {}
    paper = _dict(certificate.get("paper_results"))
    if paper.get("candidate_type") != "SINGLE_WALLET":
        return {}
    return {
        "candidate_id": paper.get("candidate_id"),
        "wallet": paper.get("source_wallet"),
        "status": paper.get("candidate_status") or certificate.get("status"),
        "policy_id": paper.get("policy_id"),
        "resolved_orders": paper.get("resolved_orders"),
        "wr_pct": paper.get("wr_pct"),
        "roi_pct": paper.get("roi_pct"),
        "validation_wr_pct": paper.get("validation_wr_pct"),
    }


def _candidate_summary_from_profit_row(candidate: dict[str, Any]) -> dict[str, Any]:
    row = _dict(candidate)
    if not row:
        return {}
    summary = _dict(row.get("summary"))
    validation = _dict(row.get("validation_summary"))
    metadata = _dict(row.get("metadata"))
    policy = _dict(row.get("policy"))
    runtime_evidence = _dict(row.get("runtime_copy_evidence"))
    return {
        "candidate_id": row.get("candidate_id"),
        "wallet": metadata.get("source_wallet")
        or runtime_evidence.get("candidate_source_wallet")
        or row.get("source_wallet"),
        "status": row.get("status"),
        "policy_id": policy.get("policy_id") or runtime_evidence.get("candidate_policy_id") or row.get("policy_id"),
        "resolved_orders": summary.get("resolved_orders"),
        "wr_pct": summary.get("wr_pct"),
        "roi_pct": summary.get("roi_pct"),
        "validation_wr_pct": validation.get("wr_pct"),
    }


def _mission_primary_live_candidate() -> dict[str, Any]:
    runtime_phase = _dict(mission_contract().get("current_runtime_phase_contract"))
    primary = _dict(runtime_phase.get("primary_live_candidate"))
    if not primary:
        return {}
    return {
        "candidate_id": primary.get("candidate_id"),
        "wallet": primary.get("source_wallet"),
        "policy_id": primary.get("policy_id"),
        "candidate_type": primary.get("candidate_type"),
    }


def _profit_primary_candidate_for_mission(profit_state: dict[str, Any], mission_candidate: dict[str, Any]) -> dict[str, Any]:
    mission_candidate_id = str(mission_candidate.get("candidate_id") or "")
    mission_wallet = str(mission_candidate.get("wallet") or "").lower()
    mission_policy_id = str(mission_candidate.get("policy_id") or "")

    def matches(row: dict[str, Any]) -> bool:
        summary = _candidate_summary_from_profit_row(row)
        candidate_id = str(summary.get("candidate_id") or "")
        wallet = str(summary.get("wallet") or "").lower()
        policy_id = str(summary.get("policy_id") or "")
        return bool(
            (mission_candidate_id and candidate_id == mission_candidate_id)
            or (mission_wallet and wallet == mission_wallet and (not mission_policy_id or policy_id == mission_policy_id))
        )

    for key in (
        "runtime_admission_candidate",
        "forward_runtime_candidate",
        "best_runtime_candidate",
        "forward_candidate",
        "best_candidate",
    ):
        row = _dict(profit_state.get(key))
        if matches(row):
            return _candidate_summary_from_profit_row(row)
    for key in ("forward_queue_runtime_candidates", "pass_candidates", "forward_tracking_queue", "ranked_candidates"):
        for row in _list(profit_state.get(key)):
            candidate = _dict(row)
            if matches(candidate):
                return _candidate_summary_from_profit_row(candidate)
    return {}


def _global_live_readiness(
    *,
    directions: list[dict[str, Any]],
    profit_state: dict[str, Any],
    source_route_state: dict[str, Any] | None,
    runtime_live_progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    by_id = _direction_by_id(directions)
    source_status = source_route_status(source_route_state)
    source_route_live_admissible = source_route_allows_live_execution(source_route_state)
    source_route_approval_id = source_route_live_operator_approval(source_route_state)
    copy_blockers: list[str] = []
    live_blockers: list[str] = []
    multi_wallet_upgrade_blockers: list[str] = []

    if not source_status:
        copy_blockers.append("source_route_state_missing")
    elif source_status != PASS and not source_route_live_admissible:
        copy_blockers.append(f"source_route_{source_status.lower()}_not_direct_pass")
        if source_route_is_recovered_degraded(source_status):
            live_blockers.append("source_route_recovered_degraded_not_live_admissible")

    for direction_id in PRIMARY_COPY_REQUIRED_DIRECTION_IDS:
        direction = by_id.get(direction_id, {})
        if direction.get("status") != PASS:
            copy_blockers.append(f"{direction_id}_not_pass")
            copy_blockers.extend(str(blocker) for blocker in _list(direction.get("blockers"))[:8])

    certificate_candidate = _certificate_primary_live_candidate(profit_state)
    mission_candidate = _mission_primary_live_candidate()
    mission_profit_candidate = _profit_primary_candidate_for_mission(profit_state, mission_candidate)
    primary_candidate = {
        **certificate_candidate,
        **mission_profit_candidate,
        **{key: value for key, value in mission_candidate.items() if value},
    } if mission_candidate else certificate_candidate
    primary_runtime_candidate_ready = bool(
        primary_candidate.get("candidate_id")
        and primary_candidate.get("status") == PASS
        and _profitability_proven(profit_state)
    )
    single = by_id.get("single_wallet_best_copyable", {})
    single_copyability = _dict(single.get("copyability"))
    single_copyability_status = str(single_copyability.get("status") or "")
    single_copyability_blockers = [str(item) for item in _list(single_copyability.get("blockers"))]
    if (
        single.get("status") == PASS
        and single_copyability_status != PASS
        and not primary_runtime_candidate_ready
    ):
        copy_blockers.append("single_wallet_candidate_current_poll_clob_truth_missing")
        copy_blockers.extend(single_copyability_blockers[:8])
    if (
        single.get("status") == PASS
        and single_copyability.get("current_tracking_seen") is not True
        and not primary_runtime_candidate_ready
    ):
        copy_blockers.append("single_wallet_current_tracking_not_seen")
    single_all_order_fill_rate = single_copyability.get("all_order_fill_rate_pct")
    single_all_order_reject_rate = single_copyability.get("all_order_reject_rate_pct")
    if single_all_order_fill_rate is not None and num(single_all_order_fill_rate) < MIN_CLOB_FILL_RATE_PCT:
        copy_blockers.append("single_wallet_all_order_fill_rate_below_99pct")
    if single_all_order_reject_rate is not None and num(single_all_order_reject_rate) > MAX_REJECT_RATE_PCT:
        copy_blockers.append("single_wallet_all_order_reject_rate_above_5pct")
    if int(num(single_copyability.get("all_order_rejected_buy_events"))) > 0:
        copy_blockers.append("single_wallet_all_order_rejected_buy_events_present")

    all_order = by_id.get("multi_wallet_all_order_exact_copy", {})
    all_order_metrics = _dict(all_order.get("metrics"))
    if int(num(all_order_metrics.get("fallback_buy_events"))) > 0:
        multi_wallet_upgrade_blockers.append("all_order_fallback_buy_events_present")
    if int(num(all_order_metrics.get("rejected_buy_events"))) > 0:
        multi_wallet_upgrade_blockers.append("all_order_rejected_buy_events_present")

    if not _profitability_proven(profit_state):
        live_blockers.append("profit_engine_no_live_admissible_pass_candidate")

    target = by_id.get("weighted_wallet_inventory_by_window", {})
    target_candidate = _dict(target.get("candidate"))
    for direction_id in MULTI_WALLET_UPGRADE_DIRECTION_IDS:
        direction = by_id.get(direction_id, {})
        if direction.get("status") != PASS:
            multi_wallet_upgrade_blockers.append(f"{direction_id}_not_pass")
    if int(num(target_candidate.get("fallback_filled_orders"))) > 0:
        multi_wallet_upgrade_blockers.append("target_inventory_fallback_filled_orders_present")
    if num(target_candidate.get("clob_backed_fill_rate_pct")) < MIN_CLOB_FILL_RATE_PCT:
        multi_wallet_upgrade_blockers.append("target_inventory_missing_candidate_specific_current_clob_truth")

    copy_trading_green = not copy_blockers
    live_ready = copy_trading_green and not live_blockers
    blockers = [*copy_blockers, *live_blockers]
    live_progress = _dict(runtime_live_progress)
    if live_ready and live_progress.get("status") == CORRECTION:
        live_blockers.extend(str(item) for item in _list(live_progress.get("blockers")))
        blockers = [*copy_blockers, *live_blockers]
    return {
        "primary_live_architecture": PRIMARY_LIVE_ARCHITECTURE,
        "upgrade_live_architecture": TARGET_LIVE_ARCHITECTURE,
        "primary_live_candidate_id": primary_candidate.get("candidate_id") or single.get("id"),
        "primary_live_candidate_wallet": primary_candidate.get("wallet") or single.get("wallet"),
        "primary_live_candidate_status": primary_candidate.get("status") or single.get("status"),
        "primary_live_candidate_policy_id": primary_candidate.get("policy_id"),
        "primary_live_candidate_resolved_orders": primary_candidate.get("resolved_orders"),
        "primary_live_candidate_wr_pct": primary_candidate.get("wr_pct"),
        "primary_live_candidate_roi_pct": primary_candidate.get("roi_pct"),
        "primary_live_candidate_validation_wr_pct": primary_candidate.get("validation_wr_pct"),
        "copy_trading_green": copy_trading_green,
        "bot_green": bool(live_ready and live_progress.get("status") != CORRECTION),
        "live_ready": live_ready,
        "actual_live_trading": bool(live_progress.get("actual_live_trading", live_ready)),
        "live_copy_progress_status": live_progress.get("status") or (PASS if live_ready else ANALYZE),
        "live_copy_progress": live_progress,
        "profitability_proven": _profitability_proven(profit_state),
        "source_route_status": source_status or None,
        "source_route_live_admissible": source_route_live_admissible,
        "source_route_live_operator_approval_id": source_route_approval_id or None,
        "copy_trading_blockers": copy_blockers,
        "live_readiness_blockers": live_blockers,
        "multi_wallet_upgrade_blockers": sorted(set(multi_wallet_upgrade_blockers)),
        "blockers": blockers,
        "status": CORRECTION if live_ready and live_progress.get("status") == CORRECTION else (
            PASS if live_ready else active_status_from_blockers(blockers, default=ANALYZE)
        ),
    }


def _runtime_live_progress_health(
    *,
    live_guard_state: dict[str, Any] | None,
    live_arm_state: dict[str, Any] | None,
    live_ledger_state: dict[str, Any] | None,
) -> dict[str, Any]:
    guard = _dict(live_guard_state)
    arm = _dict(live_arm_state)
    ledger = _dict(live_ledger_state)
    arm_summary = _dict(arm.get("candidate_intent_summary"))
    latest_candidate = _dict(arm_summary.get("latest_candidate_intent_runtime"))
    pipeline = _dict(guard.get("pipeline"))
    pipeline_json = _dict(pipeline.get("stdout_json"))
    pipeline_summary = _dict(pipeline_json.get("summary"))
    ledger_summary = _dict(ledger.get("summary"))
    runtime_permission = _dict(ledger.get("runtime_permission"))
    ledger_orders = [_dict(item) for item in _list(ledger.get("orders"))]
    lifecycle_events = [_dict(item) for item in _list(ledger.get("lifecycle_events"))]
    today_prefix = utc_now_iso()[:10]
    live_order_statuses = {"SUBMITTED", "FILLED", "REJECTED", "LIVE_SUBMITTED", "LIVE_FILLED", "LIVE_REJECTED"}
    recent_live_orders = [
        order
        for order in ledger_orders
        if str(order.get("submitted_at") or order.get("updated_at") or "").startswith(today_prefix)
        and str(order.get("status") or order.get("final_status") or "").upper() in live_order_statuses
    ]
    recent_lifecycle_events = [
        event
        for event in lifecycle_events
        if str(event.get("ts") or "").startswith(today_prefix)
        and str(event.get("status") or "").upper() in live_order_statuses
    ]
    latest_live_order = recent_live_orders[-1] if recent_live_orders else {}
    latest_live_event = recent_lifecycle_events[-1] if recent_lifecycle_events else {}

    arm_status = str(arm.get("status") or "")
    guard_status = str(guard.get("status") or "")
    fresh_candidate_intents = int(num(arm_summary.get("fresh_candidate_intents")))
    live_tradeable_window_open = int(num(arm_summary.get("candidate_intents_live_tradeable_window_open")))
    pipeline_copy_intents = int(num(pipeline_summary.get("copy_intents")))
    orders_submitted = int(num(arm.get("orders_submitted")))
    execution_result = _dict(arm.get("execution_result"))
    execution_status = str(execution_result.get("status") or "")
    can_trade = bool(runtime_permission.get("can_trade") or ledger.get("can_trade"))
    live_blockers = [
        str(item)
        for item in [
            *_list(guard.get("blockers")),
            *_list(arm.get("blockers")),
            *_list(arm.get("proof_blockers")),
            *_list(arm.get("token_blockers")),
            *_list(arm.get("operator_blockers")),
        ]
    ]

    no_fresh_statuses = {"LIVE_ARMED_NO_FRESH_INTENTS", "LIVE_ARMED_NO_NEW_INTENTS"}
    blockers: list[str] = []
    if guard and guard_status != "LIVE_GUARD_RUNNING":
        blockers.append("live_guard_not_running")
    if ledger and runtime_permission and not can_trade:
        blockers.append("live_runtime_permission_cannot_trade")
    if arm_status in no_fresh_statuses:
        blockers.append(str(arm_status).lower())
    if arm_summary and fresh_candidate_intents <= 0:
        blockers.append("fresh_candidate_intents_zero")
    if arm_summary and live_tradeable_window_open <= 0:
        blockers.append("live_tradeable_window_open_zero")
    if pipeline_summary and pipeline_copy_intents <= 0:
        blockers.append("pipeline_copy_intents_zero")
    if bool(latest_candidate.get("market_closed_now")):
        blockers.append("latest_candidate_market_closed_now")
    if bool(latest_candidate.get("observed_after_market_close")):
        blockers.append("latest_candidate_observed_after_market_close")
    blockers.extend(live_blockers)

    armed_ready = (
        arm_status
        and arm_status not in no_fresh_statuses
        and fresh_candidate_intents > 0
        and live_tradeable_window_open > 0
        and not live_blockers
    )
    live_ledger_delta = bool(recent_live_orders or recent_lifecycle_events)
    actual_live_trading = bool(
        can_trade
        and (
            live_ledger_delta
            or (
                not blockers
                and (orders_submitted > 0 or armed_ready or execution_status in {"LIVE_SUBMITTED", "LIVE_FILLED"})
            )
        )
    )
    status = PASS if actual_live_trading else (CORRECTION if blockers else ANALYZE)
    next_repair_action = "continue_monitoring_live_copy_progress"
    if status == CORRECTION:
        next_repair_action = "repair_source_latency_or_operator_approved_rotate_primary_wallet"
    return {
        "status": status,
        "actual_live_trading": actual_live_trading,
        "guard_status": guard_status or None,
        "arm_status": arm_status or None,
        "runtime_permission_can_trade": can_trade,
        "fresh_candidate_intents": fresh_candidate_intents,
        "live_tradeable_window_open": live_tradeable_window_open,
        "pipeline_copy_intents": pipeline_copy_intents,
        "orders_submitted": orders_submitted,
        "execution_status": execution_status or None,
        "live_ledger_delta_today": live_ledger_delta,
        "live_ledger_orders_today": len(recent_live_orders),
        "latest_order_ts": ledger_summary.get("latest_order_ts")
        or latest_live_order.get("submitted_at")
        or latest_live_order.get("updated_at")
        or latest_live_event.get("ts"),
        "latest_order_status": latest_live_order.get("status") or latest_live_event.get("status"),
        "latest_candidate_event_age_s": latest_candidate.get("event_age_s"),
        "latest_candidate_observation_event_age_s": latest_candidate.get("observation_event_age_s"),
        "latest_candidate_market_closed_now": bool(latest_candidate.get("market_closed_now")),
        "latest_candidate_observed_after_market_close": bool(latest_candidate.get("observed_after_market_close")),
        "blockers": sorted(set(blockers)),
        "next_repair_action": next_repair_action,
    }


def _development_research_page(
    *,
    leaderboard_state: dict[str, Any] | None,
    profit_state: dict[str, Any],
    directions: list[dict[str, Any]],
    global_readiness: dict[str, Any],
    development_limit_review: dict[str, Any],
    development_program_review: dict[str, Any],
) -> dict[str, Any]:
    leaderboard = _dict(leaderboard_state)
    leaderboard_summary = _dict(leaderboard.get("summary"))
    observation_policy = _dict(leaderboard.get("observation_policy"))
    by_id = _direction_by_id(directions)
    target = _dict(by_id.get("weighted_wallet_inventory_by_window"))
    copyability = _dict(by_id.get("profitable_wallet_copy_efficiency"))
    single = _dict(by_id.get("single_wallet_best_copyable"))
    filter_lane = _dict(by_id.get("multi_wallet_filter_consensus"))
    all_order = _dict(by_id.get("multi_wallet_all_order_exact_copy"))
    individual_universe = _dict(profit_state.get("individual_wallet_copy_universe"))
    individual_coverage = _dict(individual_universe.get("coverage"))
    inventory_universe = _dict(profit_state.get("multi_wallet_inventory_universe"))
    inventory_coverage = _dict(inventory_universe.get("coverage"))

    unique_wallets = int(num(leaderboard_summary.get("unique_wallets")))
    current_unique = int(num(leaderboard_summary.get("current_unique_wallets"), unique_wallets))
    weekly_wallets = int(num(leaderboard_summary.get("weekly_wallets")))
    monthly_wallets = int(num(leaderboard_summary.get("monthly_wallets")))
    blockers: list[str] = []
    if unique_wallets <= 0:
        blockers.append("leaderboard_weekly_monthly_wallet_universe_missing")
    if weekly_wallets <= 0:
        blockers.append("weekly_crypto_leaderboard_wallets_missing")
    if monthly_wallets <= 0:
        blockers.append("monthly_crypto_leaderboard_wallets_missing")
    if observation_policy.get("copy_all_fetched_wallets_to_registry") is not True:
        blockers.append("leaderboard_not_copying_all_fetched_wallets_to_registry")
    if single.get("status") != PASS:
        blockers.append("single_wallet_live_promotion_candidate_not_ready")
    if target.get("status") != PASS:
        blockers.append("weighted_multi_wallet_inventory_upgrade_not_live_admissible")
    if filter_lane.get("status") != PASS:
        blockers.append("multi_wallet_upgrade_current_poll_filter_not_passing")
    if all_order.get("status") != PASS:
        blockers.append("multi_wallet_upgrade_all_order_exact_copy_not_passing")
    if copyability.get("wallet") in {None, ""}:
        blockers.append("copyability_ranked_wallet_missing")
    if individual_universe and individual_universe.get("status") != PASS:
        blockers.append("individual_wallet_copy_universe_not_scaled")
    if inventory_universe and inventory_universe.get("status") != PASS:
        blockers.append("multi_wallet_inventory_universe_not_scaled_or_not_profitable")
    if development_limit_review.get("limit_hit") is True:
        blockers.append("development_lane_logical_limit_hit")
    if development_program_review.get("full_rethink_required") is True:
        blockers.append("development_program_full_rethink_required")
    blockers.extend(str(item) for item in _list(global_readiness.get("blockers")) if str(item) not in blockers)

    single_copyability = _dict(single.get("copyability"))
    single_live_burnin_required = bool(
        single.get("status") == PASS
        and (
            str(single_copyability.get("status") or "") != PASS
            or single_copyability.get("current_tracking_seen") is not True
        )
    )
    if single_live_burnin_required:
        next_change = "promote_best_single_wallet_candidate_through_current_poll_copyintent_burnin"
    elif development_program_review.get("full_rethink_required") is True:
        next_change = str(
            development_program_review.get("next_major_change_action")
            or "full_development_rethink_before_repeating_current_lane"
        )
    elif development_limit_review.get("limit_hit") is True:
        next_change = str(
            development_limit_review.get("next_change_action")
            or "reevaluate_development_lane_after_logical_limit"
        )
    elif unique_wallets <= 0 or weekly_wallets <= 0 or monthly_wallets <= 0:
        next_change = "refresh_weekly_monthly_leaderboards_with_full_pagination"
    elif individual_universe and individual_universe.get("status") != PASS:
        next_change = str(
            individual_universe.get("next_action")
            or "expand_or_resume_per_wallet_copy_universe_before_inventory_burnin"
        )
    elif inventory_universe and inventory_universe.get("status") != PASS:
        next_change = str(
            inventory_universe.get("next_action")
            or "rebuild_scaled_multi_wallet_inventory_from_full_per_wallet_copy_universe"
        )
    elif not global_readiness.get("live_ready"):
        next_change = "promote_best_single_wallet_candidate_through_current_poll_copyintent_burnin"
    elif target.get("status") != PASS:
        next_change = "rebuild_scaled_multi_wallet_inventory_from_full_per_wallet_copy_universe"
    elif filter_lane.get("status") != PASS:
        next_change = "force_current_poll_multi_wallet_consensus_burnin"
    elif all_order.get("status") != PASS:
        next_change = "repair_multi_wallet_all_order_copyability_before_upgrade_admission"
    else:
        next_change = "no_change_required_live_ready"

    return {
        "page_id": "multi_wallet_copy_trader_inventory_builder",
        "status": PASS if global_readiness.get("live_ready") else active_status_from_blockers(blockers, default=ANALYZE),
        "objective": (
            "watch weekly/monthly Polymarket CRYPTO leaderboard wallets, copy all BTC-5m BUY flow in paper, "
            "and build profitable weighted multi-wallet inventory per eligible window"
        ),
        "observation": {
            "leaderboard_state_status": leaderboard.get("status"),
            "unique_wallets": unique_wallets,
            "current_unique_wallets": current_unique,
            "weekly_wallets": weekly_wallets,
            "monthly_wallets": monthly_wallets,
            "weekly_monthly_overlap_wallets": int(num(leaderboard_summary.get("weekly_monthly_overlap_wallets"))),
            "preserved_historical_wallets": int(num(leaderboard_summary.get("preserved_historical_wallets"))),
            "observation_mode": leaderboard_summary.get("observation_mode") or observation_policy.get("page_mode"),
            "max_pages_per_period": int(
                num(
                    leaderboard_summary.get("leaderboard_max_pages_per_period"),
                    num(observation_policy.get("max_pages_per_period"), 20),
                )
            ),
            "copy_all_fetched_wallets_to_registry": bool(
                observation_policy.get("copy_all_fetched_wallets_to_registry")
                if "copy_all_fetched_wallets_to_registry" in observation_policy
                else leaderboard_summary.get("copy_all_fetched_wallets_to_registry")
            ),
        },
        "copyability_unlock": {
            "lane_id": copyability.get("id"),
            "wallet": copyability.get("wallet"),
            "wallet_name": copyability.get("wallet_name"),
            "status": copyability.get("status"),
            "metrics": copyability.get("metrics") or {},
            "blockers": copyability.get("blockers") or [],
        },
        "single_wallet_live_promotion_candidate": {
            "primary_live_architecture": global_readiness.get("primary_live_architecture"),
            "lane_id": single.get("id"),
            "wallet": single.get("wallet"),
            "wallet_name": single.get("wallet_name"),
            "status": single.get("status"),
            "paper": single.get("paper") or {},
            "copyability": single.get("copyability") or {},
            "runtime_proof": single.get("runtime_proof") or {},
            "blockers": single.get("blockers") or [],
        },
        "background_backup_wallet_pool": {
            "mode": "paper_only_background_rotation",
            "source": "weekly_monthly_leaderboard_and_operator_wallets",
            "enabled_wallets_estimate": current_unique or unique_wallets,
            "continues_while_primary_live_candidate_is_burned_in": True,
            "live_authority": False,
        },
        "inventory_target": {
            "lane_id": target.get("id"),
            "upgrade_live_architecture": global_readiness.get("upgrade_live_architecture"),
            "status": target.get("status"),
            "candidate": target.get("candidate") or {},
            "blockers": target.get("blockers") or [],
        },
        "scale_universe": {
            "individual_wallet_copy_universe": {
                "status": individual_universe.get("status"),
                "blockers": individual_universe.get("blockers") or [],
                "source_wallets_with_buy_events": individual_coverage.get("source_wallets_with_buy_events"),
                "source_buy_events": individual_coverage.get("source_buy_events"),
                "single_wallet_candidate_count": individual_coverage.get("single_wallet_candidate_count"),
                "candidate_policy_order_slots": individual_coverage.get("candidate_policy_order_slots"),
                "candidate_policy_resolved_order_slots": individual_coverage.get(
                    "candidate_policy_resolved_order_slots"
                ),
            },
            "multi_wallet_inventory_universe": {
                "status": inventory_universe.get("status"),
                "blockers": inventory_universe.get("blockers") or [],
                "inventory_candidate_count": inventory_coverage.get("inventory_candidate_count"),
                "inventory_candidate_order_slots": inventory_coverage.get("inventory_candidate_order_slots"),
                "inventory_candidate_resolved_order_slots": inventory_coverage.get(
                    "inventory_candidate_resolved_order_slots"
                ),
                "inventory_plan_slots": inventory_coverage.get("inventory_plan_slots"),
                "max_original_base_intents": inventory_coverage.get("max_original_base_intents"),
            },
        },
        "change_policy": {
            "not_enough_result_policy": "initialize_change_not_idle",
            "initialize_change": not bool(global_readiness.get("live_ready")),
            "next_change_action": next_change,
            "development_limit_review_status": development_limit_review.get("status"),
            "development_limit_hit": bool(development_limit_review.get("limit_hit")),
            "development_program_review_status": development_program_review.get("status"),
            "development_program_full_rethink_required": bool(development_program_review.get("full_rethink_required")),
            "allowed_change_actions": [
                "expand_or_resume_leaderboard_wallet_coverage",
                "rerank_by_current_copyability_and_paper_profit",
                "pin_best_copyable_wallets_for_current_poll_burnin",
                "promote_best_single_wallet_candidate_through_current_poll_copyintent_burnin",
                "rebuild_weighted_multi_wallet_inventory_search",
                "expand_or_resume_per_wallet_copy_universe_before_inventory_burnin",
                "rebuild_scaled_multi_wallet_inventory_from_full_per_wallet_copy_universe",
                "run_current_poll_multi_wallet_inventory_burnin_and_attach_candidate_specific_clob_truth",
                "reevaluate_lane_when_logical_limit_is_hit",
                "full_development_rethink_when_hypothesis_portfolio_stalls",
                "write_code_level_backlog_if_measurement_or_copy_path_blocks_progress",
            ],
        },
        "development_limit_review": development_limit_review,
        "development_program_review": development_program_review,
        "blockers": blockers,
    }


def _single_wallet_live_burnin_command(
    *,
    wallet: str,
    wallet_name: str | None,
    policy_id: str | None,
    allow_source_base_overrides_in_admission: bool = False,
) -> str:
    name_arg = ""
    if wallet_name and not str(wallet_name).lower().startswith("0x"):
        name_arg = f"--wallet-name {wallet_name} "
    source_route_flag = (
        "--allow-source-base-overrides-in-admission "
        if allow_source_base_overrides_in_admission
        else "--no-allow-source-base-overrides-in-admission "
    )
    return (
        "python3 scripts/run_wallet_live_tracker.py "
        "--registry data/research/wallet_copy_active_hotlane_registry.json "
        "--state data/research/wallet_copy_primary_single_wallet_live_tracking_state.json "
        "--event-log data/research/wallet_copy_primary_single_wallet_live_events.jsonl "
        "--paper-state data/research/wallet_copy_primary_single_wallet_paper_state.json "
        "--paper-event-log data/research/wallet_copy_primary_single_wallet_paper_events.jsonl "
        "--single-wallet-exact-copy-paper-state data/research/wallet_copy_primary_single_wallet_exact_paper_state.json "
        "--single-wallet-exact-copy-paper-event-log data/research/wallet_copy_primary_single_wallet_exact_paper_events.jsonl "
        f"--wallet-address {wallet} {name_arg}"
        "--iterations 450 --poll-interval-s 2 --max-runtime-s 900 --max-poll-runtime-s 20 "
        "--limit 50 --pages 1 --data-api-timeout-s 1.5 --data-api-retries 1 "
        "--data-api-trade-query-keys user,proxyWallet "
        "--parallel-data-api-sources --parallel-wallet-fetches 1 "
        f"--enable-clob-books --admission-mode {source_route_flag}"
        "--stop-on-admission-evidence --stop-min-required-buy-copy-events 10 "
        "--stop-min-clob-filled-buy-copy-events 10 --max-copyability-event-age-s 10 "
        "--max-wallet-fetch-duration-s 2 --strict-mirror-coverage "
        "--strategy-direction-state data/research/wallet_copy_strategy_direction_state.json"
    )


def _operating_redesign(
    *,
    directions: list[dict[str, Any]],
    profit_state: dict[str, Any],
    active_hotlane: dict[str, Any],
    active_tracking: dict[str, Any],
    leaderboard_state: dict[str, Any] | None,
    global_readiness: dict[str, Any],
    development_limit_review: dict[str, Any],
    development_program_review: dict[str, Any],
    development_research_page: dict[str, Any],
    runtime_live_progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Runtime system design that turns the rethink into executable flow control."""

    mission = mission_contract()
    runtime_phase = _dict(mission.get("current_runtime_phase_contract"))
    runtime_candidate = _dict(runtime_phase.get("primary_live_candidate"))
    runtime_guard = _dict(runtime_phase.get("live_guard"))
    runtime_filter = _dict(runtime_phase.get("profitability_filter_contract"))
    live_progress = _dict(runtime_live_progress)
    live_progress_blockers = [str(item) for item in _list(live_progress.get("blockers"))]
    live_progress_correction = bool(
        global_readiness.get("live_ready") and live_progress.get("status") == CORRECTION
    )
    by_id = _direction_by_id(directions)
    leaderboard = _dict(leaderboard_state)
    leaderboard_summary = _dict(leaderboard.get("summary"))
    hotlane_summary = _dict(active_hotlane.get("summary"))
    selected_cohort_rows = _list(active_hotlane.get("selected_cohorts"))
    selected_wallet_rows = _list(active_hotlane.get("selected_wallets"))
    selected_tracker_inventory_rows = _list(active_hotlane.get("selected_tracker_time_inventory_cohorts"))
    tracking_summary = _dict(active_tracking.get("summary"))
    information_source_fusion = _dict(tracking_summary.get("information_source_fusion"))
    fusion_sources = _dict(information_source_fusion.get("sources"))
    fusion_wallet_api = _dict(fusion_sources.get("wallet_data_api"))
    fusion_clob_books = _dict(fusion_sources.get("clob_books"))
    fusion_polling_plan = _dict(information_source_fusion.get("polling_plan"))
    fusion_copy_plan = _dict(information_source_fusion.get("copy_plan"))
    fusion_execution_plan = _dict(information_source_fusion.get("execution_plan"))
    current_poll = _dict(tracking_summary.get("current_poll_diagnostics"))
    current_ladder = _dict(current_poll.get("current_poll_ladder"))
    copy_efficiency = _dict(tracking_summary.get("copy_efficiency"))
    copy_summary = _dict(copy_efficiency.get("summary"))
    hot_path = _dict(tracking_summary.get("hot_path_adaptive"))
    hot_path_summary = _dict(hot_path.get("summary"))
    hot_path_current_poll_moves = int(num(_hot_path_metric(hot_path, "current_poll_moves")))
    hot_path_pass_signals = int(num(_hot_path_metric(hot_path, "pass_signals")))
    hot_path_runtime_eligible_wallets = int(num(_hot_path_metric(hot_path, "runtime_eligible_wallets")))
    hot_path_filled_orders = int(num(_hot_path_metric(hot_path, "hot_path_filled_orders")))
    hot_path_rejected_orders = int(num(_hot_path_metric(hot_path, "hot_path_rejected_orders")))
    hot_path_current_poll_pass = (
        hot_path_current_poll_moves > 0
        and hot_path_pass_signals > 0
        and hot_path_runtime_eligible_wallets >= 2
        and hot_path_filled_orders > 0
        and hot_path_rejected_orders <= 0
    )
    inventory = _dict(by_id.get("weighted_wallet_inventory_by_window"))
    inventory_candidate = _dict(inventory.get("candidate"))
    single = _dict(by_id.get("single_wallet_best_copyable"))
    single_copyability = _dict(single.get("copyability"))
    single_runtime_proof = _dict(single.get("runtime_proof"))
    all_order = _dict(by_id.get("multi_wallet_all_order_exact_copy"))
    all_order_metrics = _dict(all_order.get("metrics"))
    multi_filter = _dict(by_id.get("multi_wallet_filter_consensus"))
    individual_universe = _dict(profit_state.get("individual_wallet_copy_universe"))
    individual_coverage = _dict(individual_universe.get("coverage"))
    inventory_universe = _dict(profit_state.get("multi_wallet_inventory_universe"))
    inventory_coverage = _dict(inventory_universe.get("coverage"))

    selected_cohorts = int(num(hotlane_summary.get("selected_cohorts"))) or len(selected_cohort_rows)
    selected_wallets = int(num(hotlane_summary.get("selected_wallets"))) or len(selected_wallet_rows)
    if selected_wallets <= 0 and selected_cohort_rows:
        selected_wallets = len(
            {
                str(wallet).lower()
                for cohort in selected_cohort_rows
                if isinstance(cohort, dict)
                for wallet in _list(cohort.get("selected_wallets") or cohort.get("wallets"))
                if wallet
            }
        )
    selected_tracker_inventory = int(num(hotlane_summary.get("selected_tracker_time_inventory_cohorts"))) or len(
        selected_tracker_inventory_rows
    )
    unique_wallets = int(num(leaderboard_summary.get("unique_wallets")))
    weekly_wallets = int(num(leaderboard_summary.get("weekly_wallets")))
    monthly_wallets = int(num(leaderboard_summary.get("monthly_wallets")))
    fusion_status = str(information_source_fusion.get("status") or "")
    fusion_next_action = str(information_source_fusion.get("next_action") or "")
    source_route_status_value = str(global_readiness.get("source_route_status") or "")
    required_buys = int(num(copy_summary.get("required_buy_copy_events")))
    clob_filled_buys = int(num(copy_summary.get("clob_filled_buy_copy_events")))
    fallback_buys = int(num(copy_summary.get("fallback_filled_buy_copy_events")))
    rejected_buys = int(num(copy_summary.get("rejected_buy_copy_events")))
    missed_buys = int(num(copy_summary.get("missed_buy_copy_events")))

    source_universe_blockers: list[str] = []
    if unique_wallets <= 0:
        source_universe_blockers.append("weekly_monthly_leaderboard_wallet_universe_missing")
    if weekly_wallets <= 0:
        source_universe_blockers.append("weekly_leaderboard_wallets_missing")
    if monthly_wallets <= 0:
        source_universe_blockers.append("monthly_leaderboard_wallets_missing")
    if source_route_status_value != PASS and not bool(global_readiness.get("source_route_live_admissible")):
        source_universe_blockers.append("source_route_not_pass")

    primary_single_wallet = str(global_readiness.get("primary_live_candidate_wallet") or single.get("wallet") or "")
    primary_single_policy = str(
        global_readiness.get("primary_live_candidate_policy_id")
        or single_runtime_proof.get("policy_id")
        or ""
    )
    primary_single_live_ready = bool(
        global_readiness.get("live_ready")
        and global_readiness.get("primary_live_candidate_id")
        and global_readiness.get("profitability_proven")
    )
    single_burnin_blockers: list[str] = []
    if not primary_single_live_ready and single.get("status") != PASS:
        single_burnin_blockers.append("single_wallet_live_promotion_candidate_not_pass")
        single_burnin_blockers.extend(str(item) for item in _list(single.get("blockers"))[:6])
    if not primary_single_wallet:
        single_burnin_blockers.append("primary_single_wallet_address_missing")
    if not primary_single_live_ready and str(single_copyability.get("status") or "") != PASS:
        single_burnin_blockers.append("single_wallet_candidate_current_poll_clob_truth_missing")
        single_burnin_blockers.extend(str(item) for item in _list(single_copyability.get("blockers"))[:6])
    if not primary_single_live_ready and single_copyability.get("current_tracking_seen") is not True:
        single_burnin_blockers.append("single_wallet_current_tracking_not_seen")

    polling_blockers: list[str] = []
    if not information_source_fusion:
        if not hot_path_current_poll_pass:
            polling_blockers.append("information_source_fusion_missing")
    elif fusion_status != PASS:
        if not hot_path_current_poll_pass:
            polling_blockers.extend(
                f"information_source:{blocker}"
                for blocker in _list(information_source_fusion.get("blockers"))[:8]
            )
    if selected_wallets <= 0:
        polling_blockers.append("active_hotlane_no_wallets_selected")
    if selected_cohorts <= 0:
        polling_blockers.append("active_hotlane_no_multi_wallet_cohorts_selected")

    copy_execution_blockers: list[str] = []
    if required_buys <= 0:
        copy_execution_blockers.append("no_required_buy_copy_events")
    if clob_filled_buys < required_buys:
        copy_execution_blockers.append("required_buys_not_all_clob_filled")
    if fallback_buys > 0:
        copy_execution_blockers.append("fallback_filled_buy_copy_events_present")
    if rejected_buys > 0:
        copy_execution_blockers.append("rejected_buy_copy_events_present")
    if missed_buys > 0:
        copy_execution_blockers.append("missed_buy_copy_events_present")
    inventory_blockers: list[str] = []
    if all_order.get("status") != PASS:
        inventory_blockers.extend(str(item) for item in _list(all_order.get("blockers"))[:8])
    if individual_universe and individual_universe.get("status") != PASS:
        inventory_blockers.append("per_wallet_copy_universe_not_scaled")
    if inventory_universe and inventory_universe.get("status") != PASS:
        inventory_blockers.append("multi_wallet_inventory_universe_not_scaled_or_not_profitable")
    if inventory.get("status") != PASS:
        inventory_blockers.extend(str(item) for item in _list(inventory.get("blockers"))[:8])
    if selected_tracker_inventory <= 0:
        inventory_blockers.append("tracker_time_inventory_cohorts_not_selected")

    live_gate_blockers = [
        *[str(item) for item in _list(global_readiness.get("copy_trading_blockers"))],
        *[str(item) for item in _list(global_readiness.get("live_readiness_blockers"))],
    ]

    phases = [
        {
            "id": "source_universe",
            "status": PASS if not source_universe_blockers else active_status_from_blockers(source_universe_blockers, default=ANALYZE),
            "objective": "maximize weekly/monthly crypto leaderboard plus operator wallet universe without narrowing failing sources",
            "metrics": {
                "unique_wallets": unique_wallets,
                "weekly_wallets": weekly_wallets,
                "monthly_wallets": monthly_wallets,
                "source_route_status": source_route_status_value,
                "wallet_data_api_status": fusion_wallet_api.get("status"),
                "wallet_data_api_raw_rows": fusion_wallet_api.get("raw_rows"),
                "wallet_data_api_fresh_buy_rows_le_10s": fusion_wallet_api.get("fresh_buy_rows_le_10s"),
            },
            "blockers": source_universe_blockers,
        },
        {
            "id": "single_wallet_first_live_burnin",
            "status": PASS if global_readiness.get("live_ready") else active_status_from_blockers(
                single_burnin_blockers,
                default=ANALYZE,
            ),
            "objective": (
                "burn in the best single-wallet candidate through current-poll CLOB-backed CopyIntent proof "
                "before treating multi-wallet inventory as a live dependency"
            ),
            "metrics": {
                "wallet": primary_single_wallet or None,
                "wallet_name": single.get("wallet_name"),
                "policy_id": primary_single_policy or None,
                "paper_resolved_orders": _dict(single.get("paper")).get("resolved_orders"),
                "paper_wr_pct": _dict(single.get("paper")).get("wr_pct"),
                "paper_roi_pct": _dict(single.get("paper")).get("roi_pct"),
                "runtime_proof_rows": single_runtime_proof.get("proof_rows"),
                "runtime_proof_windows": single_runtime_proof.get("market_windows"),
                "runtime_proof_event_age_p95_s": single_runtime_proof.get("event_age_p95_s"),
                "copyability_status": single_copyability.get("status"),
                "current_tracking_seen": single_copyability.get("current_tracking_seen"),
                "policy_copied_buy_events": single_copyability.get("policy_copied_buy_events"),
                "all_order_fill_rate_pct": single_copyability.get("all_order_fill_rate_pct"),
                "all_order_reject_rate_pct": single_copyability.get("all_order_reject_rate_pct"),
            },
            "blockers": single_burnin_blockers,
        },
        {
            "id": "checkpointed_source_fusion_polling",
            "status": PASS if not polling_blockers else active_status_from_blockers(polling_blockers, default=ANALYZE),
            "objective": "poll selected wallets/cohorts in short checkpointed parallel slices driven by source-fusion truth",
            "metrics": {
                "information_source_fusion_status": fusion_status or None,
                "information_source_next_action": fusion_next_action or None,
                "selected_wallets": selected_wallets,
                "selected_cohorts": selected_cohorts,
                "selected_tracker_time_inventory_cohorts": selected_tracker_inventory,
                "poll_runtime_limited": fusion_polling_plan.get("runtime_limited"),
                "parallel_wallet_fetches": fusion_polling_plan.get("parallel_wallet_fetches"),
                "current_poll_raw_rows": current_ladder.get("raw_rows"),
                "current_poll_fresh_buy_rows_le_10s": current_ladder.get("fresh_buy_rows_le_10s"),
                "hot_path_evidence_source": hot_path.get("evidence_source") or "active_tracking_state",
                "hot_path_current_poll_moves": hot_path_current_poll_moves,
                "hot_path_pass_signals": hot_path_pass_signals,
                "hot_path_runtime_eligible_wallets": hot_path_runtime_eligible_wallets,
                "hot_path_filled_orders": hot_path_filled_orders,
                "hot_path_rejected_orders": hot_path_rejected_orders,
            },
            "blockers": polling_blockers,
        },
        {
            "id": "copy_execution_repair",
            "status": PASS if not copy_execution_blockers else active_status_from_blockers(copy_execution_blockers, default=ANALYZE),
            "objective": "make every required paper BUY become CLOB-backed through the same CopyIntent lifecycle with zero fallback/reject/miss",
            "metrics": {
                "required_buy_copy_events": required_buys,
                "clob_filled_buy_copy_events": clob_filled_buys,
                "fallback_filled_buy_copy_events": fallback_buys,
                "rejected_buy_copy_events": rejected_buys,
                "missed_buy_copy_events": missed_buys,
                "clob_books_status": fusion_clob_books.get("status"),
                "clob_books_current_poll_ok_rows": fusion_clob_books.get("current_poll_ok_rows"),
                "clob_books_admission_window_ok_rows": fusion_clob_books.get("admission_window_ok_rows"),
            },
            "blockers": copy_execution_blockers,
        },
        {
            "id": "multi_wallet_inventory_promotion",
            "status": PASS if not inventory_blockers else active_status_from_blockers(inventory_blockers, default=ANALYZE),
            "objective": "paper-upgrade lane: copy each wallet individually, then promote proven copyable flow into weighted multi-wallet inventory per window",
            "metrics": {
                "individual_wallet_source_buy_events": individual_coverage.get("source_buy_events"),
                "individual_wallet_source_wallets_with_buy_events": individual_coverage.get("source_wallets_with_buy_events"),
                "inventory_candidate_order_slots": inventory_coverage.get("inventory_candidate_order_slots"),
                "inventory_candidate_resolved_order_slots": inventory_coverage.get("inventory_candidate_resolved_order_slots"),
                "target_inventory_candidate_id": inventory_candidate.get("candidate_id"),
                "target_inventory_roi_pct": inventory_candidate.get("roi_pct"),
                "target_inventory_wr_pct": inventory_candidate.get("wr_pct"),
                "target_inventory_fallback_filled_orders": inventory_candidate.get("fallback_filled_orders"),
                "target_inventory_clob_backed_fill_rate_pct": inventory_candidate.get("clob_backed_fill_rate_pct"),
            },
            "blockers": inventory_blockers,
        },
        {
            "id": "live_parity_gate",
            "status": CORRECTION if live_progress_correction else (
                PASS if global_readiness.get("live_ready") else active_status_from_blockers(live_gate_blockers, default=ANALYZE)
            ),
            "objective": (
                "primary first-live lane: run one best single-wallet policy-filtered copy candidate "
                "behind an explicit operator gate after selected-intent CopyIntent parity"
            ),
            "metrics": {
                "primary_live_architecture": global_readiness.get("primary_live_architecture"),
                "primary_live_candidate_id": global_readiness.get("primary_live_candidate_id"),
                "primary_live_candidate_wallet": global_readiness.get("primary_live_candidate_wallet"),
                "primary_live_candidate_status": global_readiness.get("primary_live_candidate_status"),
                "upgrade_live_architecture": global_readiness.get("upgrade_live_architecture"),
                "copy_trading_green": bool(global_readiness.get("copy_trading_green")),
                "live_ready": bool(global_readiness.get("live_ready")),
                "profitability_proven": bool(global_readiness.get("profitability_proven")),
                "copy_mode": runtime_phase.get("live_mode"),
                "copy_style": runtime_phase.get("copy_style"),
                "profitability_first": bool(runtime_phase.get("profitability_first")),
                "strict_source_order_1_to_1_required": bool(runtime_phase.get("strict_source_order_1_to_1_required")),
                "selected_intent_parity_required": bool(runtime_phase.get("selected_intent_parity_required")),
                "paper_only": True,
                "live_orders_allowed": False,
            },
            "blockers": live_gate_blockers,
        },
        {
            "id": "single_wallet_live_guard_runtime",
            "status": CORRECTION if live_progress_correction else (
                PASS if global_readiness.get("live_ready") else ANALYZE
            ),
            "objective": (
                "operate and monitor the approved single-wallet live guard; the guard is the only "
                "live order authority while leaderboard, backup-wallet, and multi-wallet work remain paper-only"
            ),
            "metrics": {
                "runtime_phase": runtime_phase.get("phase_id"),
                "candidate_id": runtime_candidate.get("candidate_id")
                or global_readiness.get("primary_live_candidate_id"),
                "candidate_type": runtime_candidate.get("candidate_type") or "SINGLE_WALLET",
                "source_wallet": runtime_candidate.get("source_wallet")
                or global_readiness.get("primary_live_candidate_wallet"),
                "policy_id": runtime_candidate.get("policy_id")
                or global_readiness.get("primary_live_candidate_policy_id"),
                "copy_mode": runtime_phase.get("live_mode"),
                "copy_style": runtime_phase.get("copy_style"),
                "profitability_first": bool(runtime_phase.get("profitability_first")),
                "strict_source_order_1_to_1_required": bool(runtime_phase.get("strict_source_order_1_to_1_required")),
                "selected_intent_parity_required": bool(runtime_phase.get("selected_intent_parity_required")),
                "profitability_filter": runtime_filter,
                "guard_script": runtime_guard.get("script"),
                "guard_state": runtime_guard.get("state"),
                "arm_state": runtime_guard.get("arm_state"),
                "ledger_state": runtime_guard.get("ledger_state"),
                "expected_status": runtime_guard.get("expected_status"),
                "runtime_paper_only": False if global_readiness.get("live_ready") else True,
                "runtime_live_orders_allowed": bool(global_readiness.get("live_ready")),
                "proof_states_remain_paper_only": True,
                "actual_live_trading": live_progress.get("actual_live_trading"),
                "live_copy_progress_status": live_progress.get("status"),
                "fresh_candidate_intents": live_progress.get("fresh_candidate_intents"),
                "live_tradeable_window_open": live_progress.get("live_tradeable_window_open"),
                "pipeline_copy_intents": live_progress.get("pipeline_copy_intents"),
                "latest_order_ts": live_progress.get("latest_order_ts"),
                "latest_candidate_event_age_s": live_progress.get("latest_candidate_event_age_s"),
                "latest_candidate_market_closed_now": live_progress.get("latest_candidate_market_closed_now"),
                "latest_candidate_observed_after_market_close": live_progress.get(
                    "latest_candidate_observed_after_market_close"
                ),
            },
            "blockers": live_progress_blockers
            if live_progress_correction
            else ([] if global_readiness.get("live_ready") else ["single_wallet_live_guard_waiting_for_live_ready"]),
        },
    ]

    phase_by_id = {str(row.get("id")): row for row in phases}
    if live_progress_correction:
        active_phase = "single_wallet_live_guard_runtime"
        action = "repair_live_copy_stall_source_latency_or_operator_approved_rotation"
        command = "python3 scripts/run_wallet_copy_autonomous_repair.py --deep-research --command-timeout-s 900"
        file = "src/wallet_copy/strategy_selection.py"
        function = "_runtime_live_progress_health / _operating_redesign"
    elif global_readiness.get("live_ready"):
        active_phase = "single_wallet_live_guard_runtime"
        action = "monitor_pinned_single_wallet_live_guard_and_lifecycle_ledger"
        command = "python3 scripts/select_wallet_copy_strategy_direction.py"
        file = "scripts/run_wallet_copy_live_guard.py"
        function = "main / live execution arm state"
    elif source_universe_blockers:
        active_phase = "source_universe"
        action = "repair_or_refresh_source_universe_before_copy_burnin"
        command = "python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 240"
        file = "scripts/run_wallet_copy_autonomous_repair.py"
        function = "_source_route_backlog_actions / leaderboard discovery refresh"
    elif single_burnin_blockers:
        active_phase = "single_wallet_first_live_burnin"
        action = "burn_in_primary_single_wallet_until_candidate_scoped_clob_copyintent_truth"
        command = _single_wallet_live_burnin_command(
            wallet=primary_single_wallet,
            wallet_name=str(single.get("wallet_name") or "primary_live_candidate"),
            policy_id=primary_single_policy or None,
            allow_source_base_overrides_in_admission=bool(
                global_readiness.get("source_route_live_admissible")
                and global_readiness.get("source_route_live_operator_approval_id")
            ),
        )
        file = "scripts/run_wallet_live_tracker.py"
        function = "LiveWalletTracker.poll_once / current_poll_single_wallet_exact_copy"
    elif polling_blockers:
        active_phase = "checkpointed_source_fusion_polling"
        action = "split_wallet_universe_into_checkpointed_parallel_poll_slices"
        command = (
            "python3 scripts/select_wallet_copy_active_hotlane.py --max-wallets 32 && "
            "python3 scripts/run_wallet_copy_hotlane_tick.py --ticks 1 --wallets-per-tick 8 "
            "--parallel-wallet-fetches 8 --max-poll-runtime-s 30 --command-timeout-s 90"
        )
        file = "src/wallet_copy/hotlane.py"
        function = "build_active_hotlane / run_wallet_copy_hotlane_tick"
    elif copy_execution_blockers:
        active_phase = "copy_execution_repair"
        action = "repair_copy_execution_tactics_until_zero_fallback_reject_miss_buy"
        command = "python3 scripts/run_wallet_copy_autonomous_repair.py --deep-research --command-timeout-s 900"
        file = "src/wallet_copy/copy_tactics.py"
        function = "build_copy_execution_tactic_plan"
    elif not global_readiness.get("live_ready"):
        active_phase = "live_parity_gate"
        action = "burn_in_best_single_wallet_copy_candidate_until_live_ready_without_placing_live_orders"
        command = "python3 scripts/run_wallet_copy_autonomous_repair.py --deep-research --command-timeout-s 900"
        file = "src/wallet_copy/execution.py"
        function = "build_copy_intent_parity_capsule / LiveWalletCopyLifecycle"
    else:
        active_phase = "multi_wallet_inventory_promotion"
        action = "paper_upgrade_multi_wallet_inventory_after_primary_single_wallet_gate"
        command = (
            "python3 scripts/run_wallet_copy_profit_engine.py --policy-preset fast "
            "--max-wallets-for-search 0 --max-single-wallet-candidate-intents 0 "
            "--max-multi-wallet-base-intents 0 "
            "--live-today-sprint-operator-approval-id OP-LIVE-20260703-BELA"
        )
        file = "src/wallet_copy/profit_engine.py"
        function = "evaluate_profit_candidates / multi_wallet_inventory_universe"

    blockers = [str(item) for phase in phases for item in _list(phase.get("blockers"))]
    next_system_action = {
        "phase": active_phase,
        "action": action,
        "file": file,
        "function": function,
        "verify": command,
        "next_command": command,
        "blockers": phase_by_id.get(active_phase, {}).get("blockers") or [],
    }
    return {
        "schema_version": 1,
        "status": CORRECTION if live_progress_correction else (
            PASS if global_readiness.get("live_ready") else active_status_from_blockers(blockers, default=ANALYZE)
        ),
        "primary_live_architecture": PRIMARY_LIVE_ARCHITECTURE,
        "target_architecture": TARGET_LIVE_ARCHITECTURE,
        "upgrade_live_architecture": TARGET_LIVE_ARCHITECTURE,
        "design_principles": [
            "maximize_source_universe_without_hiding_failures",
            "source_fusion_drives_polling_copy_execution",
            "short_checkpointed_parallel_poll_slices_before_long_burnin",
            "promote_one_best_single_wallet_copy_candidate_first",
            "live_single_wallet_copy_is_profitability_filtered_not_strict_1_to_1",
            "keep_background_wallet_copy_rotation_paper_only",
            "copy_each_wallet_individually_then_promote_to_multi_wallet_inventory_as_upgrade",
            "paper_live_same_copyintent_path_with_operator_gate_only",
        ],
        "phase_order": [str(row["id"]) for row in phases],
        "active_phase": active_phase,
        "next_system_action": next_system_action,
        "phases": phases,
        "development_program_next_major_change_action": development_program_review.get("next_major_change_action"),
        "development_research_next_change_action": _dict(development_research_page.get("change_policy")).get("next_change_action"),
        "logical_limit_hit": bool(development_limit_review.get("limit_hit")),
        "paper_only": True,
        "live_orders_allowed": False,
        "runtime_phase": runtime_phase.get("phase_id"),
        "copy_mode": runtime_phase.get("live_mode"),
        "copy_style": runtime_phase.get("copy_style"),
        "profitability_first": bool(runtime_phase.get("profitability_first")),
        "strict_source_order_1_to_1_required": bool(runtime_phase.get("strict_source_order_1_to_1_required")),
        "selected_intent_parity_required": bool(runtime_phase.get("selected_intent_parity_required")),
        "profitability_filter": _dict(runtime_phase.get("profitability_filter_contract")),
        "runtime_paper_only": False if global_readiness.get("live_ready") else True,
        "runtime_live_orders_allowed": bool(global_readiness.get("live_ready")),
    }


def build_strategy_direction_state(
    *,
    wallet_analysis: dict[str, Any],
    profit_state: dict[str, Any],
    active_hotlane: dict[str, Any],
    active_tracking: dict[str, Any],
    hotlane_tick_state: dict[str, Any] | None = None,
    runtime_proof_index: dict[str, Any] | None = None,
    source_route_state: dict[str, Any] | None = None,
    leaderboard_state: dict[str, Any] | None = None,
    previous_strategy_state: dict[str, Any] | None = None,
    live_guard_state: dict[str, Any] | None = None,
    live_arm_state: dict[str, Any] | None = None,
    live_ledger_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_tracking = _active_tracking_with_hotlane_tick_best(active_tracking, hotlane_tick_state)
    directions = [
        _profitable_copy_efficiency_lane(wallet_analysis),
        _best_single_wallet(wallet_analysis, runtime_proof_index or {}, profit_state),
        _wr_repair_single_wallet(wallet_analysis, runtime_proof_index or {}, profit_state),
        _all_order_multi_wallet(wallet_analysis, active_tracking),
        _weighted_inventory(profit_state, active_hotlane, active_tracking=active_tracking),
        _multi_wallet_filter(active_tracking, active_hotlane),
    ]
    ranked = sorted(
        directions,
        key=lambda row: (
            1 if row.get("status") == PASS else 0,
            num(row.get("rank_score")),
        ),
        reverse=True,
    )
    runtime_live_progress = _runtime_live_progress_health(
        live_guard_state=live_guard_state,
        live_arm_state=live_arm_state,
        live_ledger_state=live_ledger_state,
    )
    global_readiness = _global_live_readiness(
        directions=ranked,
        profit_state=profit_state,
        source_route_state=source_route_state,
        runtime_live_progress=runtime_live_progress,
    )
    development_limit_review = _development_limit_review(
        directions=ranked,
        previous_strategy_state=previous_strategy_state,
    )
    development_program_review = _development_program_review(
        directions=ranked,
        profit_state=profit_state,
        leaderboard_state=leaderboard_state or {},
        development_limit_review=development_limit_review,
    )
    if development_limit_review.get("limit_hit") is True and not global_readiness.get("live_ready"):
        limit_blockers = [
            f"development_lane_logical_limit_hit_{row.get('lane_id')}"
            for row in _list(development_limit_review.get("limit_hit_lanes"))
            if row.get("lane_id")
        ]
        global_readiness = {
            **global_readiness,
            "status": CORRECTION,
            "blockers": [*global_readiness.get("blockers", []), *limit_blockers],
            "live_readiness_blockers": [
                *global_readiness.get("live_readiness_blockers", []),
                *limit_blockers,
            ],
        }
    if development_program_review.get("full_rethink_required") is True and not global_readiness.get("live_ready"):
        global_readiness = {
            **global_readiness,
            "status": CORRECTION,
            "blockers": [*global_readiness.get("blockers", []), "development_program_full_rethink_required"],
            "live_readiness_blockers": [
                *global_readiness.get("live_readiness_blockers", []),
                "development_program_full_rethink_required",
            ],
        }
    development_research = _development_research_page(
        leaderboard_state=leaderboard_state or {},
        profit_state=profit_state,
        directions=ranked,
        global_readiness=global_readiness,
        development_limit_review=development_limit_review,
        development_program_review=development_program_review,
    )
    operating_redesign = _operating_redesign(
        directions=ranked,
        profit_state=profit_state,
        active_hotlane=active_hotlane,
        active_tracking=active_tracking,
        leaderboard_state=leaderboard_state or {},
        global_readiness=global_readiness,
        development_limit_review=development_limit_review,
        development_program_review=development_program_review,
        development_research_page=development_research,
        runtime_live_progress=runtime_live_progress,
    )
    target = next((row for row in ranked if row.get("id") == "weighted_wallet_inventory_by_window"), ranked[0])
    copyability_unlock = next(
        (
            row
            for row in directions
            if row.get("id") == "profitable_wallet_copy_efficiency" and row.get("wallet")
        ),
        next(
            (
                row
                for row in directions
                if row.get("id") == "wr_repair_single_wallet" and row.get("wallet")
            ),
            next((row for row in directions if row.get("id") == "single_wallet_best_copyable"), ranked[0]),
        ),
    )
    if global_readiness.get("status") == CORRECTION and runtime_live_progress.get("status") == CORRECTION:
        recommended_now = "repair_single_wallet_live_copy_stall"
        decision_status = CORRECTION
        decision_reason = (
            "The live guard is authorized and running, but actual live copy trading is stalled: no fresh "
            "policy-eligible CopyIntent is armed inside the live window and the live ledger has no new submit, "
            "fill, or reject progress. Treat this as NOT OK and repair source latency or make an "
            "operator-approved primary-wallet rotation; do not bypass the guard or operator gates."
        )
    elif global_readiness["live_ready"]:
        recommended_now = "single_wallet_live_guard_monitor_with_multi_wallet_paper_upgrade"
        decision_status = PASS
        decision_reason = (
            "The first-live single-wallet copy lane is active as a profitability-filtered, policy-sized "
            "CopyIntent path rather than a strict 1-to-1 source-order mirror. Monitor the pinned live guard "
            "and lifecycle ledger, keep proof states paper-only, and continue backup wallet and multi-wallet "
            "work only as paper upgrade lanes unless a separate operator-approved rotation is made."
        )
    else:
        recommended_now = "single_wallet_copyability_unlock_then_multi_wallet_inventory"
        decision_status = global_readiness["status"]
        decision_reason = (
            "The development loop must first improve paper-only copying of profitable wallet order-flow. "
            "Keep the best source-profitable current-copy wallet pinned for latency, slippage, fill, and "
            "paper-profit proof, then promote proven CopyIntent evidence into weighted multi-wallet inventory."
        )
    runtime_phase = _dict(mission_contract().get("current_runtime_phase_contract"))
    decision = {
        "status": decision_status,
        "recommended_now": recommended_now,
        "target_live_architecture": TARGET_LIVE_ARCHITECTURE,
        "primary_live_architecture": global_readiness["primary_live_architecture"],
        "upgrade_live_architecture": global_readiness["upgrade_live_architecture"],
        "primary_live_candidate_id": global_readiness["primary_live_candidate_id"],
        "primary_live_candidate_wallet": global_readiness["primary_live_candidate_wallet"],
        "primary_live_candidate_status": global_readiness["primary_live_candidate_status"],
        "primary_live_candidate_policy_id": global_readiness.get("primary_live_candidate_policy_id"),
        "primary_live_candidate_resolved_orders": global_readiness.get("primary_live_candidate_resolved_orders"),
        "primary_live_candidate_wr_pct": global_readiness.get("primary_live_candidate_wr_pct"),
        "primary_live_candidate_roi_pct": global_readiness.get("primary_live_candidate_roi_pct"),
        "primary_live_candidate_validation_wr_pct": global_readiness.get(
            "primary_live_candidate_validation_wr_pct"
        ),
        "copyability_unlock_lane": copyability_unlock.get("id"),
        "reason": decision_reason,
        "live_ready": bool(global_readiness["live_ready"]),
        "copy_trading_green": bool(global_readiness["copy_trading_green"]),
        "bot_green": bool(global_readiness["bot_green"]),
        "actual_live_trading": bool(global_readiness["actual_live_trading"]),
        "live_copy_progress_status": global_readiness["live_copy_progress_status"],
        "live_copy_progress": runtime_live_progress,
        "profitability_proven": bool(global_readiness["profitability_proven"]),
        "source_route_status": global_readiness["source_route_status"],
        "source_route_live_admissible": bool(global_readiness["source_route_live_admissible"]),
        "source_route_live_operator_approval_id": global_readiness.get("source_route_live_operator_approval_id"),
        "copy_trading_blockers": global_readiness["copy_trading_blockers"],
        "live_readiness_blockers": global_readiness["live_readiness_blockers"],
        "multi_wallet_upgrade_blockers": global_readiness["multi_wallet_upgrade_blockers"],
        "blockers": global_readiness["blockers"],
        "development_research_page": development_research,
        "development_limit_review": development_limit_review,
        "development_program_review": development_program_review,
        "operating_redesign": operating_redesign,
        "runtime_phase": runtime_phase.get("phase_id"),
        "copy_mode": runtime_phase.get("live_mode"),
        "copy_style": runtime_phase.get("copy_style"),
        "profitability_first": bool(runtime_phase.get("profitability_first")),
        "strict_source_order_1_to_1_required": bool(runtime_phase.get("strict_source_order_1_to_1_required")),
        "selected_intent_parity_required": bool(runtime_phase.get("selected_intent_parity_required")),
        "profitability_filter": _dict(runtime_phase.get("profitability_filter_contract")),
        "runtime_paper_only": False if global_readiness["live_ready"] else True,
        "runtime_live_orders_allowed": bool(global_readiness["live_ready"]),
    }
    return {
        "schema_version": 2,
        "kind": "wallet_copy_strategy_direction_state",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "runtime_phase": decision["runtime_phase"],
        "copy_mode": decision["copy_mode"],
        "copy_style": decision["copy_style"],
        "profitability_first": decision["profitability_first"],
        "strict_source_order_1_to_1_required": decision["strict_source_order_1_to_1_required"],
        "selected_intent_parity_required": decision["selected_intent_parity_required"],
        "profitability_filter": decision["profitability_filter"],
        "runtime_paper_only": decision["runtime_paper_only"],
        "runtime_live_orders_allowed": decision["runtime_live_orders_allowed"],
        "mission_contract": mission_contract(),
        "status": decision["status"],
        "recommended_now": decision["recommended_now"],
        "primary_live_architecture": decision["primary_live_architecture"],
        "target_live_architecture": decision["target_live_architecture"],
        "upgrade_live_architecture": decision["upgrade_live_architecture"],
        "primary_live_candidate_id": decision["primary_live_candidate_id"],
        "primary_live_candidate_wallet": decision["primary_live_candidate_wallet"],
        "primary_live_candidate_status": decision["primary_live_candidate_status"],
        "primary_live_candidate_policy_id": decision["primary_live_candidate_policy_id"],
        "primary_live_candidate_resolved_orders": decision["primary_live_candidate_resolved_orders"],
        "primary_live_candidate_wr_pct": decision["primary_live_candidate_wr_pct"],
        "primary_live_candidate_roi_pct": decision["primary_live_candidate_roi_pct"],
        "primary_live_candidate_validation_wr_pct": decision["primary_live_candidate_validation_wr_pct"],
        "copyability_unlock_lane": decision["copyability_unlock_lane"],
        "live_ready": decision["live_ready"],
        "copy_trading_green": decision["copy_trading_green"],
        "bot_green": decision["bot_green"],
        "actual_live_trading": decision["actual_live_trading"],
        "live_copy_progress_status": decision["live_copy_progress_status"],
        "live_copy_progress": decision["live_copy_progress"],
        "profitability_proven": decision["profitability_proven"],
        "source_route_status": decision["source_route_status"],
        "source_route_live_admissible": decision["source_route_live_admissible"],
        "source_route_live_operator_approval_id": decision["source_route_live_operator_approval_id"],
        "development_limit_review": development_limit_review,
        "development_program_review": development_program_review,
        "copy_trading_blockers": decision["copy_trading_blockers"],
        "live_readiness_blockers": decision["live_readiness_blockers"],
        "multi_wallet_upgrade_blockers": decision["multi_wallet_upgrade_blockers"],
        "blockers": decision["blockers"],
        "development_research_page": development_research,
        "operating_redesign": operating_redesign,
        "decision": decision,
        "directions": ranked,
    }


def build_strategy_direction_state_from_paths(
    *,
    wallet_analysis_path: str,
    profit_state_path: str,
    active_hotlane_state_path: str,
    active_tracking_state_path: str,
    hotlane_tick_state_path: str = "data/research/wallet_copy_hotlane_tick_state.json",
    candidate_runtime_proof_index_path: str = "",
    source_route_state_path: str = "data/research/wallet_copy_source_route_state.json",
    leaderboard_state_path: str = "data/research/wallet_copy_leaderboard_crypto_state.json",
    previous_strategy_state_path: str = "",
    live_guard_state_path: str = "data/research/wallet_copy_live_guard_state.json",
    live_arm_state_path: str = "data/research/wallet_copy_live_execution_arm_state.json",
    live_ledger_state_path: str = "data/research/wallet_copy_live_execution_state.json",
) -> dict[str, Any]:
    return build_strategy_direction_state(
        wallet_analysis=_dict(load_json(wallet_analysis_path, default={})),
        profit_state=_dict(load_json(profit_state_path, default={})),
        active_hotlane=_dict(load_json(active_hotlane_state_path, default={})),
        active_tracking=_dict(load_json(active_tracking_state_path, default={})),
        hotlane_tick_state=_dict(load_json(hotlane_tick_state_path, default={})) if hotlane_tick_state_path else {},
        runtime_proof_index=_dict(load_json(candidate_runtime_proof_index_path, default={}))
        if candidate_runtime_proof_index_path
        else {},
        source_route_state=_dict(load_json(source_route_state_path, default={})) if source_route_state_path else {},
        leaderboard_state=_dict(load_json(leaderboard_state_path, default={})) if leaderboard_state_path else {},
        previous_strategy_state=_dict(load_json(previous_strategy_state_path, default={}))
        if previous_strategy_state_path
        else {},
        live_guard_state=_dict(load_json(live_guard_state_path, default={})) if live_guard_state_path else {},
        live_arm_state=_dict(load_json(live_arm_state_path, default={})) if live_arm_state_path else {},
        live_ledger_state=_dict(load_json(live_ledger_state_path, default={})) if live_ledger_state_path else {},
    )
