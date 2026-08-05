#!/usr/bin/env python3
"""Search profitable wallet-copy candidates and write a paper-only admission state."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.profit_engine import CandidatePolicy, ProfitEngineConfig, fast_candidate_policies, run_profit_engine


POLICY_ID_RE = re.compile(
    r"^(?:fast_)?wf_(?P<fraction>[0-9.]+)_cap_(?P<cap>[0-9.]+).*?_minusd_(?P<min_wallet>[0-9.]+)_"
    r"(?P<timing>all_window|first_90s|middle_90s|last_120s|first_120s|last_180s|first_60s|last_120s)$"
)

TIMING_BANDS = {
    "all_window": (None, None),
    "first_90s": (0.0, 90.0),
    "middle_90s": (90.0, 180.0),
    "last_120s": (180.0, 300.0),
    "first_120s": (0.0, 120.0),
    "last_180s": (120.0, 300.0),
    "first_60s": (0.0, 60.0),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-state", action="append", default=None)
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--output", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--min-train-resolved", type=int, default=20)
    parser.add_argument("--min-validation-resolved", type=int, default=10)
    parser.add_argument("--min-all-resolved", type=int, default=30)
    parser.add_argument("--min-roi-pct", type=float, default=2.0)
    parser.add_argument("--min-wr-pct", type=float, default=70.0)
    parser.add_argument("--max-drawdown-usd", type=float, default=20.0)
    parser.add_argument("--max-unresolved-ratio", type=float, default=0.5)
    parser.add_argument("--train-fraction", type=float, default=0.7)
    parser.add_argument("--slippage-bps", type=float, default=250.0)
    parser.add_argument("--max-candidates", type=int, default=200)
    parser.add_argument("--no-raw-baseline-guard", action="store_true")
    parser.add_argument("--min-raw-baseline-resolved", type=int, default=30)
    parser.add_argument("--min-filtered-resolved-when-raw-negative", type=int, default=100)
    parser.add_argument("--min-candidate-vs-raw-delta-roi-pct", type=float, default=5.0)
    parser.add_argument("--live-tracker-state", default="data/research/wallet_copy_live_tracking_state.json")
    parser.add_argument("--active-hotlane-state", default="data/research/wallet_copy_active_hotlane_state.json")
    parser.add_argument(
        "--active-hotlane-live-tracker-state",
        default="data/research/wallet_copy_active_hotlane_live_tracking_state.json",
    )
    parser.add_argument(
        "--candidate-forward-live-tracker-state",
        default="data/research/wallet_copy_candidate_forward_live_tracking_state.json",
    )
    parser.add_argument(
        "--candidate-runtime-proof-index",
        default="data/research/wallet_copy_candidate_runtime_proof_index.json",
        help="Persist clean candidate-specific CLOB-backed runtime copy proof across polls",
    )
    parser.add_argument(
        "--strategy-direction-state",
        default="data/research/wallet_copy_strategy_direction_state.json",
        help="Read development-program review to turn profit probes into the active bridge work queue",
    )
    parser.add_argument(
        "--source-route-state",
        default="data/research/wallet_copy_source_route_state.json",
        help="Source-route truth state that must be reflected in the live-readiness certificate",
    )
    parser.add_argument("--candidate-forward-probe-live-tracker-state", action="append", default=None)
    parser.add_argument("--no-require-live-tracker-truth", action="store_true")
    parser.add_argument("--no-require-copy-efficiency-truth", action="store_true")
    parser.add_argument(
        "--allow-candidate-fallback-fill-evidence",
        action="store_true",
        help="allow fallback-only historical replay candidates to PASS candidate selection; live admission still needs tracker truth",
    )
    parser.add_argument("--min-agreeing-wallets", type=int, default=2)
    parser.add_argument("--max-price-spread", type=float, default=0.08)
    parser.add_argument("--inventory-max-window-usd", type=float, default=10.0)
    parser.add_argument("--inventory-max-per-wallet-usd", type=float, default=2.0)
    parser.add_argument("--inventory-min-plan-usd", type=float, default=1.0)
    parser.add_argument("--no-consensus-search", action="store_true")
    parser.add_argument("--no-inventory-search", action="store_true")
    parser.add_argument(
        "--max-multi-wallet-base-intents",
        type=int,
        default=0,
        help="Limit recent base intents per policy for multi-wallet consensus/inventory; use 0 for exhaustive",
    )
    parser.add_argument(
        "--max-wallets-for-search",
        type=int,
        default=0,
        help="Limit wallet candidate search to the most active BTC-5m wallets; use 0 for all",
    )
    parser.add_argument(
        "--max-single-wallet-candidate-intents",
        type=int,
        default=0,
        help="Limit recent intents per single-wallet candidate; use 0 for exhaustive",
    )
    parser.add_argument("--no-skip-low-intent-candidates", action="store_true")
    parser.add_argument("--min-all-unique-windows", type=int, default=0)
    parser.add_argument("--min-train-unique-windows", type=int, default=0)
    parser.add_argument("--min-validation-unique-windows", type=int, default=0)
    parser.add_argument(
        "--max-candidate-orders-per-window",
        type=int,
        default=25,
        help="Block candidates whose resolved replay is overly concentrated in one 5m market window; use 0 to disable",
    )
    parser.add_argument(
        "--max-candidate-orders-per-window-ratio",
        type=float,
        default=0.08,
        help="Block candidates when one 5m market window exceeds this share of resolved orders; use 0 to disable",
    )
    parser.add_argument("--forward-tracking-queue-size", type=int, default=12)
    parser.add_argument("--forward-fresh-lag-cap-s", type=float, default=1800.0)
    parser.add_argument("--forward-probe-fresh-lag-cap-s", type=float, default=60.0)
    parser.add_argument("--live-target-min-resolved-orders", type=int, default=100)
    parser.add_argument("--live-target-min-unique-windows", type=int, default=10)
    parser.add_argument("--live-target-min-avg-orders-per-window", type=float, default=2.0)
    parser.add_argument("--live-target-min-wr-pct", type=float, default=70.0)
    parser.add_argument("--live-target-min-validation-wr-pct", type=float, default=70.0)
    parser.add_argument("--live-target-min-roi-pct", type=float, default=2.0)
    parser.add_argument("--policy-preset", choices=("default", "fast"), default="default")
    parser.add_argument(
        "--policy-id",
        default="",
        help="Evaluate only this policy id. When not present in the preset grid, fraction/cap/timing are parsed from the id.",
    )
    parser.add_argument("--min-buy-price", type=float, default=0.01)
    parser.add_argument("--max-buy-price", type=float, default=1.0)
    parser.add_argument(
        "--live-today-sprint-operator-approval-id",
        default="",
        help="Explicit operator approval id for LIVE_TODAY_SPRINT proof-led runtime admission.",
    )
    parser.add_argument(
        "--print-full-summary",
        action="store_true",
        help="Print the legacy verbose summary; default stdout is heartbeat-safe and compact",
    )
    return parser.parse_args()


def _policy_from_id(args: argparse.Namespace) -> CandidatePolicy:
    policy_id = str(args.policy_id or "").strip()
    match = POLICY_ID_RE.match(policy_id)
    min_seconds_from_open: float | None = None
    max_seconds_from_open: float | None = None
    if match:
        timing = TIMING_BANDS.get(match.group("timing"), (None, None))
        min_seconds_from_open, max_seconds_from_open = timing
        wallet_fraction = float(match.group("fraction"))
        max_order_usd = float(match.group("cap"))
        min_wallet_usdc = float(match.group("min_wallet"))
    else:
        wallet_fraction = 0.10
        max_order_usd = 4.0
        min_wallet_usdc = 0.0
    return CandidatePolicy(
        policy_id=policy_id,
        min_price=float(args.min_buy_price),
        max_price=float(args.max_buy_price),
        min_wallet_usdc=min_wallet_usdc,
        wallet_fraction=wallet_fraction,
        max_order_usd=max_order_usd,
        min_seconds_from_open=min_seconds_from_open,
        max_seconds_from_open=max_seconds_from_open,
    )


def _candidate_policies_for_args(args: argparse.Namespace) -> list[CandidatePolicy] | None:
    if not str(args.policy_id or "").strip():
        return fast_candidate_policies() if args.policy_preset == "fast" else None
    return [_policy_from_id(args)]


def main() -> int:
    args = parse_args()
    cfg = ProfitEngineConfig(
        min_train_resolved=args.min_train_resolved,
        min_validation_resolved=args.min_validation_resolved,
        min_all_resolved=args.min_all_resolved,
        min_roi_pct=args.min_roi_pct,
        min_wr_pct=args.min_wr_pct,
        max_drawdown_usd=args.max_drawdown_usd,
        max_unresolved_ratio=args.max_unresolved_ratio,
        train_fraction=args.train_fraction,
        slippage_bps=args.slippage_bps,
        max_candidates=args.max_candidates,
        raw_baseline_guard=not args.no_raw_baseline_guard,
        min_raw_baseline_resolved=args.min_raw_baseline_resolved,
        min_filtered_resolved_when_raw_negative=args.min_filtered_resolved_when_raw_negative,
        min_candidate_vs_raw_delta_roi_pct=args.min_candidate_vs_raw_delta_roi_pct,
        require_live_tracker_truth_for_live_admission=not args.no_require_live_tracker_truth,
        live_tracker_state_path=args.live_tracker_state,
        active_hotlane_state_path=args.active_hotlane_state,
        active_hotlane_live_tracker_state_path=args.active_hotlane_live_tracker_state,
        candidate_forward_live_tracker_state_path=args.candidate_forward_live_tracker_state,
        candidate_runtime_proof_index_path=args.candidate_runtime_proof_index,
        strategy_direction_state_path=args.strategy_direction_state,
        source_route_state_path=args.source_route_state,
        live_today_sprint_operator_approval_id=args.live_today_sprint_operator_approval_id,
        candidate_forward_probe_live_tracker_state_paths=tuple(args.candidate_forward_probe_live_tracker_state or []),
        require_candidate_clob_fill_evidence=not args.allow_candidate_fallback_fill_evidence,
        require_copy_efficiency_truth=not args.no_require_copy_efficiency_truth,
        min_agreeing_wallets=args.min_agreeing_wallets,
        max_price_spread=args.max_price_spread,
        inventory_max_window_usd=args.inventory_max_window_usd,
        inventory_max_per_wallet_usd=args.inventory_max_per_wallet_usd,
        inventory_min_plan_usd=args.inventory_min_plan_usd,
        enable_consensus_search=not args.no_consensus_search,
        enable_inventory_search=not args.no_inventory_search,
        max_multi_wallet_base_intents=args.max_multi_wallet_base_intents,
        max_wallets_for_search=args.max_wallets_for_search,
        max_single_wallet_candidate_intents=args.max_single_wallet_candidate_intents,
        skip_candidates_below_min_intent_count=not args.no_skip_low_intent_candidates,
        min_all_unique_windows=args.min_all_unique_windows,
        min_train_unique_windows=args.min_train_unique_windows,
        min_validation_unique_windows=args.min_validation_unique_windows,
        max_candidate_orders_per_window=args.max_candidate_orders_per_window,
        max_candidate_orders_per_window_ratio=args.max_candidate_orders_per_window_ratio,
        forward_tracking_queue_size=args.forward_tracking_queue_size,
        forward_fresh_lag_cap_s=args.forward_fresh_lag_cap_s,
        forward_probe_fresh_lag_cap_s=args.forward_probe_fresh_lag_cap_s,
        live_target_min_resolved_orders=args.live_target_min_resolved_orders,
        live_target_min_unique_windows=args.live_target_min_unique_windows,
        live_target_min_avg_orders_per_window=args.live_target_min_avg_orders_per_window,
        live_target_min_wr_pct=args.live_target_min_wr_pct,
        live_target_min_validation_wr_pct=args.live_target_min_validation_wr_pct,
        live_target_min_roi_pct=args.live_target_min_roi_pct,
    )
    report = run_profit_engine(
        history_states=args.history_state or ["data/research/wallet_copy_history_state.json"],
        resolutions_path=args.resolutions,
        output_path=args.output,
        config=cfg,
        policies=_candidate_policies_for_args(args),
        policy_preset=args.policy_preset,
    )
    best_candidate = report.get("best_candidate") if isinstance(report.get("best_candidate"), dict) else {}
    best_summary = best_candidate.get("summary") if isinstance(best_candidate.get("summary"), dict) else {}
    best_validation = (
        best_candidate.get("validation_summary") if isinstance(best_candidate.get("validation_summary"), dict) else {}
    )
    individual_universe = (
        report.get("individual_wallet_copy_universe")
        if isinstance(report.get("individual_wallet_copy_universe"), dict)
        else {}
    )
    individual_coverage = (
        individual_universe.get("coverage") if isinstance(individual_universe.get("coverage"), dict) else {}
    )
    inventory_universe = (
        report.get("multi_wallet_inventory_universe")
        if isinstance(report.get("multi_wallet_inventory_universe"), dict)
        else {}
    )
    inventory_coverage = (
        inventory_universe.get("coverage") if isinstance(inventory_universe.get("coverage"), dict) else {}
    )
    compact_summary = {
        "output": args.output,
        "decision_status": (report.get("decision") or {}).get("status")
        if isinstance(report.get("decision"), dict)
        else None,
        "live_admission_status": (report.get("decision") or {}).get("live_admission_status")
        if isinstance(report.get("decision"), dict)
        else None,
        "live_admission_blockers": (report.get("decision") or {}).get("live_admission_blockers")
        if isinstance(report.get("decision"), dict)
        else [],
        "history_events": (report.get("source_contract") or {}).get("history_events")
        if isinstance(report.get("source_contract"), dict)
        else None,
        "wallet_count": (report.get("source_contract") or {}).get("wallet_count")
        if isinstance(report.get("source_contract"), dict)
        else None,
        "ranked_candidates": len(report.get("ranked_candidates") or []),
        "pass_candidates": len(report.get("pass_candidates") or []),
        "best_candidate": {
            "candidate_id": best_candidate.get("candidate_id"),
            "candidate_type": best_candidate.get("candidate_type"),
            "status": best_candidate.get("status"),
            "blockers": best_candidate.get("blockers") or [],
            "resolved_orders": best_summary.get("resolved_orders"),
            "roi_pct": best_summary.get("roi_pct"),
            "wr_pct": best_summary.get("wr_pct"),
            "pnl_usd": best_summary.get("pnl_usd"),
            "validation_resolved_orders": best_validation.get("resolved_orders"),
            "validation_roi_pct": best_validation.get("roi_pct"),
            "validation_wr_pct": best_validation.get("wr_pct"),
        },
        "individual_wallet_copy_universe": {
            "status": individual_universe.get("status"),
            "blockers": individual_universe.get("blockers") or [],
            "source_wallets_with_buy_events": individual_coverage.get("source_wallets_with_buy_events"),
            "source_buy_events": individual_coverage.get("source_buy_events"),
            "single_wallet_candidate_count": individual_coverage.get("single_wallet_candidate_count"),
            "candidate_policy_order_slots": individual_coverage.get("candidate_policy_order_slots"),
            "candidate_policy_resolved_order_slots": individual_coverage.get("candidate_policy_resolved_order_slots"),
        },
        "multi_wallet_inventory_universe": {
            "status": inventory_universe.get("status"),
            "blockers": inventory_universe.get("blockers") or [],
            "inventory_candidate_count": inventory_coverage.get("inventory_candidate_count"),
            "inventory_candidate_order_slots": inventory_coverage.get("inventory_candidate_order_slots"),
            "inventory_candidate_resolved_order_slots": inventory_coverage.get("inventory_candidate_resolved_order_slots"),
            "inventory_plan_slots": inventory_coverage.get("inventory_plan_slots"),
            "max_original_base_intents": inventory_coverage.get("max_original_base_intents"),
        },
        "forward_tracking_queue_size": len(report.get("forward_tracking_queue") or []),
        "development_program_bridge": report.get("development_program_bridge"),
        "runtime_admission_candidate_id": (report.get("decision") or {}).get("runtime_admission_candidate_id")
        if isinstance(report.get("decision"), dict)
        else None,
    }
    verbose_summary = {
        "output": args.output,
        "decision": report.get("decision"),
        "source_contract": report.get("source_contract"),
        "best_candidate": {
            key: best_candidate.get(key)
            for key in (
                "candidate_id",
                "candidate_type",
                "status",
                "blockers",
                "profit_score",
                "summary",
                "validation_summary",
                "raw_baseline_summary",
                "max_drawdown_usd",
                "policy",
                "metadata",
            )
        },
        "development_program_bridge": report.get("development_program_bridge"),
        "individual_wallet_copy_universe": report.get("individual_wallet_copy_universe"),
        "multi_wallet_inventory_universe": report.get("multi_wallet_inventory_universe"),
        "forward_candidate": report.get("forward_candidate"),
        "forward_tracking_queue": report.get("forward_tracking_queue"),
        "pass_candidates": len(report.get("pass_candidates") or []),
        "ranked_candidates": len(report.get("ranked_candidates") or []),
    }
    summary = verbose_summary if args.print_full_summary else compact_summary
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
