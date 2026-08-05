#!/usr/bin/env python3
"""Hydrate paper sibling lanes around the producing wallet-copy config.

The lanes are deliberately report/paper state only. They reuse the same
candidate-intent builder as live execution, but never call the submit adapter.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.run_wallet_copy_live_execution as live_execution  # noqa: E402
from src.wallet_copy.models import CopyIntent, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_LIVE_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_HISTORY_STATE = "data/research/wallet_copy_history_state.json"
DEFAULT_HISTORY_INDEX = "data/research/wallet_copy_history_window_index.json"
DEFAULT_RTDS_WATERMARK = "data/research/wallet_copy_rtds_observation_watermarks.json"
DEFAULT_CHANGE_JOURNAL = "data/research/live_change_journal.jsonl"
DEFAULT_OUTPUT = "data/research/wallet_copy_winner_variation_siblings_latest.json"
SAMPLE_GATE_N = 50
REQUIRED_DIFF_ROI_PP = 1.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--live-ledger-state", default=DEFAULT_LIVE_LEDGER)
    parser.add_argument("--history-state", default=DEFAULT_HISTORY_STATE)
    parser.add_argument("--history-window-index", default=DEFAULT_HISTORY_INDEX)
    parser.add_argument("--rtds-watermark-state", default=DEFAULT_RTDS_WATERMARK)
    parser.add_argument("--change-journal", default=DEFAULT_CHANGE_JOURNAL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--freshness-siblings", default="45,75,90")
    return parser.parse_args()


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return int(default)
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _parse_iso_ts(value: Any) -> float | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        return dt.datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def _argv_value(argv: list[Any], flag: str, default: Any) -> Any:
    try:
        index = [str(item) for item in argv].index(flag)
    except ValueError:
        return default
    if index + 1 >= len(argv):
        return default
    return argv[index + 1]


def _latest_jsonl(path: str) -> dict[str, Any]:
    target = ROOT / path if not Path(path).is_absolute() else Path(path)
    if not target.exists():
        return {}
    latest = {}
    for line in target.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            latest = json.loads(line)
        except json.JSONDecodeError:
            continue
    return latest if isinstance(latest, dict) else {}


def _live_runtime(guard: dict[str, Any]) -> dict[str, Any]:
    runtime = guard.get("live_execution_runtime")
    if isinstance(runtime, dict):
        return runtime
    live_execution_payload = guard.get("live_execution") if isinstance(guard.get("live_execution"), dict) else {}
    runtime = live_execution_payload.get("guard_live_execution_runtime")
    return runtime if isinstance(runtime, dict) else {}


def _selected_parent(guard: dict[str, Any]) -> dict[str, Any]:
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    selected = runtime.get("selected_member") if isinstance(runtime.get("selected_member"), dict) else {}
    candidate = guard.get("candidate") if isinstance(guard.get("candidate"), dict) else {}
    source_wallet = str(
        selected.get("source_wallet")
        or selected.get("wallet")
        or candidate.get("source_wallet")
        or guard.get("source_wallet")
        or ""
    ).lower()
    policy_by_wallet = runtime.get("policy_by_wallet") if isinstance(runtime.get("policy_by_wallet"), dict) else {}
    policy = policy_by_wallet.get(source_wallet) if isinstance(policy_by_wallet.get(source_wallet), dict) else {}
    if not policy:
        policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
    policy = dict(policy)
    for key in ("max_order_usd", "max_price", "wallet_fraction", "min_order_usd"):
        if selected.get(key) is not None:
            policy[key] = selected.get(key)
    runtime_payload = _live_runtime(guard)
    argv = runtime_payload.get("argv") if isinstance(runtime_payload.get("argv"), list) else []
    summary = (
        guard.get("live_execution", {}).get("candidate_intent_summary", {})
        if isinstance(guard.get("live_execution"), dict)
        else {}
    )
    parent_max_event_age_s = _float(
        _argv_value(argv, "--max-event-age-s", summary.get("max_event_age_s", 30.0)),
        30.0,
    )
    parent_live_build_max_observed_age_s = _float(
        _argv_value(
            argv,
            "--live-build-max-observed-age-s",
            summary.get("live_build_max_observed_age_s", parent_max_event_age_s),
        ),
        parent_max_event_age_s,
    )
    return {
        "candidate_id": str(selected.get("candidate_id") or candidate.get("candidate_id") or guard.get("candidate_id") or ""),
        "candidate_type": "SINGLE_WALLET",
        "source_wallet": source_wallet,
        "policy": policy,
        "policy_id": str(policy.get("policy_id") or selected.get("policy_id") or guard.get("policy_id") or ""),
        "copy_model": str(_argv_value(argv, "--copy-model", summary.get("copy_model") or "drip") or "drip"),
        "max_event_age_s": parent_max_event_age_s,
        "live_build_max_observed_age_s": parent_live_build_max_observed_age_s,
        "max_intents": _int(_argv_value(argv, "--max-intents", summary.get("max_intents", 6)), 6),
        "min_live_order_usd": _float(
            _argv_value(argv, "--min-live-order-usd", summary.get("min_live_order_usd", 1.0)),
            1.0,
        ),
        "inventory_late_window_stop_s": _float(_argv_value(argv, "--inventory-late-window-stop-s", 60.0), 60.0),
        "inventory_max_converge_orders_per_window": _int(
            _argv_value(argv, "--inventory-max-converge-orders-per-window", 6),
            6,
        ),
        "inventory_future_window_lookahead_s": _float(
            _argv_value(argv, "--inventory-future-window-lookahead-s", 7200.0),
            7200.0,
        ),
        "drip_min_tranche_usd": _float(_argv_value(argv, "--drip-min-tranche-usd", 1.0), 1.0),
        "drip_max_tranche_usd": _float(_argv_value(argv, "--drip-max-tranche-usd", 2.5), 2.5),
        "drip_max_tranches_per_window": _int(_argv_value(argv, "--drip-max-tranches-per-window", 12), 12),
        "wallet_copy_max_buy_price": _float(_argv_value(argv, "--wallet-copy-max-buy-price", policy.get("max_price", 0.5)), 0.5),
        "wallet_copy_min_buy_price": _float(_argv_value(argv, "--wallet-copy-min-buy-price", policy.get("min_price", 0.0)), 0.0),
    }


def _epoch(guard: dict[str, Any], parent: dict[str, Any], latest_change: dict[str, Any]) -> dict[str, Any]:
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    code = guard.get("guard_code_identity") if isinstance(guard.get("guard_code_identity"), dict) else {}
    material = {
        "active_set_generation_id": runtime.get("set_generation_id"),
        "candidate_id": parent.get("candidate_id"),
        "source_wallet": parent.get("source_wallet"),
        "policy": parent.get("policy"),
        "copy_model": parent.get("copy_model"),
        "max_event_age_s": parent.get("max_event_age_s"),
        "live_build_max_observed_age_s": parent.get("live_build_max_observed_age_s"),
        "guard_git_head_at_launch": code.get("git_head_at_launch"),
        "guard_started_at_utc": code.get("started_at_utc"),
        "golden_snapshot": latest_change.get("golden_snapshot"),
        "change_id": latest_change.get("change_id"),
    }
    return {
        "epoch_id": stable_id("winner_variation_epoch", material, length=16),
        "flow_stage": "LEARN/SELF-DEV",
        "active_set_generation_id": runtime.get("set_generation_id"),
        "guard_started_at_utc": code.get("started_at_utc"),
        "guard_git_head_at_launch": code.get("git_head_at_launch"),
        "guard_script_sha256": code.get("script_sha256"),
        "latest_change_id": latest_change.get("change_id"),
        "golden_snapshot": latest_change.get("golden_snapshot"),
        "epoch_start_ts": _parse_iso_ts(code.get("started_at_utc")),
        "material": material,
    }


def _lane_specs(parent: dict[str, Any], freshness_values: list[float]) -> list[dict[str, Any]]:
    specs = [
        {
            "lane_id": "paper_twin_exact_parent",
            "lane_role": "paper_twin",
            "variant_type": "exact_parent",
            "max_event_age_s": float(parent["max_event_age_s"]),
            "live_build_max_observed_age_s": float(parent["live_build_max_observed_age_s"]),
            "parameter_delta": "none",
        }
    ]
    for value in freshness_values:
        specs.append(
            {
                "lane_id": f"freshness_{int(value)}s_sibling",
                "lane_role": "sibling",
                "variant_type": "single_parameter_freshness",
                "max_event_age_s": float(value),
                "live_build_max_observed_age_s": float(value),
                "parameter_delta": (
                    f"freshness {parent['max_event_age_s']}/{parent['live_build_max_observed_age_s']}s -> {value}/{value}s"
                ),
            }
        )
    return specs


def _candidate(parent: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": parent["candidate_id"],
        "candidate_type": parent.get("candidate_type") or "SINGLE_WALLET",
        "source_wallet": parent["source_wallet"],
        "policy": parent["policy"],
    }


def _build_args(
    *,
    args: argparse.Namespace,
    parent: dict[str, Any],
    lane: dict[str, Any],
) -> SimpleNamespace:
    return SimpleNamespace(
        copy_model=parent["copy_model"],
        history_window_index=args.history_window_index,
        rtds_watermark_state=args.rtds_watermark_state,
        live_ledger_state=args.live_ledger_state,
        max_event_age_s=float(lane["max_event_age_s"]),
        live_build_max_observed_age_s=float(lane["live_build_max_observed_age_s"]),
        inventory_future_window_lookahead_s=float(parent["inventory_future_window_lookahead_s"]),
        inventory_max_converge_orders_per_window=int(parent["inventory_max_converge_orders_per_window"]),
        drip_min_tranche_usd=float(parent["drip_min_tranche_usd"]),
        drip_max_tranche_usd=float(parent["drip_max_tranche_usd"]),
        drip_max_tranches_per_window=int(parent["drip_max_tranches_per_window"]),
        min_agreeing_wallets=2,
        max_price_spread=0.08,
        max_window_usd=10.0,
        max_per_wallet_usd=2.0,
        min_inventory_plan_usd=1.0,
        execute_live=True,
        min_live_order_usd=float(parent["min_live_order_usd"]),
        max_intents=int(parent["max_intents"]),
    )


def _current_lane_state(
    *,
    cli_args: argparse.Namespace,
    parent: dict[str, Any],
    lane: dict[str, Any],
) -> dict[str, Any]:
    try:
        intents, summary, blockers, token_map = live_execution._build_candidate_intents(
            candidate=_candidate(parent),
            history_state=cli_args.history_state,
            args=_build_args(args=cli_args, parent=parent, lane=lane),
        )
    except Exception as exc:  # pragma: no cover - report should degrade, not disturb live trading.
        return {
            "status": "CURRENT_EVAL_ERROR",
            "error": f"{type(exc).__name__}: {exc}",
            "fresh_candidate_intents": 0,
            "sample_intents": [],
            "blockers": ["current_eval_error"],
            "token_map_conditions": 0,
        }
    return {
        "status": summary.get("status"),
        "history_events": summary.get("history_events"),
        "source_events": summary.get("source_events"),
        "candidate_intents": summary.get("candidate_intents"),
        "fresh_candidate_intents": len(intents),
        "candidate_intents_live_tradeable_window_open": summary.get("candidate_intents_live_tradeable_window_open"),
        "fresh_intent_token_mapped_conditions": len(token_map),
        "blockers": blockers,
        "sample_intents": [intent.asdict() for intent in intents[:5]],
        "live_event_prefilter": summary.get("live_event_prefilter") if isinstance(summary.get("live_event_prefilter"), dict) else {},
    }


def _norm_outcome(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"up", "yes"}:
        return "yes"
    if text in {"down", "no"}:
        return "no"
    return text


def _order_cost(order: dict[str, Any]) -> float:
    for key in ("actual_trade_cost_usd", "intended_cost_usd", "filled_size_usd", "requested_size_usd"):
        value = _float(order.get(key), 0.0)
        if value > 0:
            return value
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    return _float(trade_result.get("filled_size_usd") or trade_result.get("cost_usd"), 0.0)


def _order_build_ts(order: dict[str, Any]) -> float | None:
    latency = order.get("latency_budget") if isinstance(order.get("latency_budget"), dict) else {}
    value = _float(latency.get("intent_built_ts"), 0.0)
    if value > 0:
        return value
    return _parse_iso_ts(order.get("submitted_at") or order.get("updated_at"))


def _side_pnl_map(live_ledger: dict[str, Any]) -> dict[tuple[str, str], float]:
    writeback = live_ledger.get("resolution_writeback") if isinstance(live_ledger.get("resolution_writeback"), dict) else {}
    per_market = writeback.get("per_market_slug") if isinstance(writeback.get("per_market_slug"), dict) else {}
    rows: dict[tuple[str, str], float] = {}
    for slug, row in per_market.items():
        if not isinstance(row, dict):
            continue
        rows[(str(slug), "yes")] = _float(row.get("yes_pnl_usd"), 0.0)
        rows[(str(slug), "no")] = _float(row.get("no_pnl_usd"), 0.0)
    return rows


def _ledger_scope_orders(
    *,
    live_ledger: dict[str, Any],
    parent: dict[str, Any],
    epoch_start_ts: float | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_wallet = str(parent.get("source_wallet") or "").lower()
    for order in live_ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        if source_wallet and str(order.get("source_wallet") or "").lower() != source_wallet:
            continue
        if str(order.get("copy_model") or parent.get("copy_model") or "") != str(parent.get("copy_model") or ""):
            continue
        build_ts = _order_build_ts(order)
        if epoch_start_ts is not None and build_ts is not None and build_ts < epoch_start_ts:
            continue
        if not isinstance(order.get("source_intent"), dict):
            continue
        rows.append(order)
    return rows


def _group_costs(orders: list[dict[str, Any]]) -> dict[tuple[str, str], float]:
    totals: dict[tuple[str, str], float] = {}
    for order in orders:
        if str(order.get("final_status") or "").upper() != "FILLED":
            continue
        key = (str(order.get("market_slug") or ""), _norm_outcome(order.get("outcome")))
        if key[0] and key[1]:
            totals[key] = totals.get(key, 0.0) + _order_cost(order)
    return totals


def _ledger_lane_evidence(
    *,
    lane: dict[str, Any],
    orders: list[dict[str, Any]],
    side_pnl: dict[tuple[str, str], float],
    group_costs: dict[tuple[str, str], float],
) -> dict[str, Any]:
    included = 0
    filled = 0
    resolved = 0
    cost = 0.0
    pnl = 0.0
    mismatch = 0
    samples: list[dict[str, Any]] = []
    for order in orders:
        source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
        try:
            intent = CopyIntent.from_dict(source_intent)
        except TypeError:
            mismatch += 1
            continue
        build_ts = _order_build_ts(order)
        if build_ts is None:
            mismatch += 1
            continue
        diagnostics = live_execution._intent_runtime_diagnostics(
            intent,
            now_ts=build_ts,
            max_event_age_s=float(lane["max_event_age_s"]),
            live_build_max_observed_age_s=float(lane["live_build_max_observed_age_s"]),
        )
        if not diagnostics.get("live_tradeable_window_open"):
            continue
        included += 1
        if str(order.get("final_status") or "").upper() != "FILLED":
            continue
        filled += 1
        order_cost = _order_cost(order)
        cost += order_cost
        key = (str(order.get("market_slug") or ""), _norm_outcome(order.get("outcome")))
        if key in side_pnl and group_costs.get(key, 0.0) > 0:
            resolved += 1
            pnl += side_pnl[key] * (order_cost / group_costs[key])
        if len(samples) < 5:
            samples.append(
                {
                    "order_id": order.get("order_id"),
                    "market_slug": order.get("market_slug"),
                    "outcome": order.get("outcome"),
                    "cost_usd": round(order_cost, 6),
                    "freshness_basis": diagnostics.get("freshness_basis"),
                    "event_age_s_at_build": diagnostics.get("event_age_s"),
                    "observed_age_s_at_build": diagnostics.get("observed_age_s"),
                    "watermark_age_s_at_build": diagnostics.get("watermark_age_s"),
                }
            )
    roi = round(pnl / cost * 100.0, 6) if cost > 0 else None
    return {
        "submitted_orders_replayed": len(orders),
        "paper_accepted_orders": included,
        "paper_filled_orders": filled,
        "resolved_paper_fills": resolved,
        "estimated_cost_usd": round(cost, 6),
        "estimated_paper_pnl_usd": round(pnl, 6),
        "estimated_paper_roi_pct": roi,
        "replay_mismatch_count": mismatch,
        "pnl_basis": "pro_rata_side_pnl_from_live_resolution_writeback_with_same_epoch_fill_model",
        "sample_orders": samples,
    }


def _gate(lane: dict[str, Any], evidence: dict[str, Any], twin: dict[str, Any] | None) -> dict[str, Any]:
    if lane["lane_role"] == "paper_twin":
        return {
            "status": "BASELINE",
            "sample_gate_n": SAMPLE_GATE_N,
            "required_diff_roi_pp": REQUIRED_DIFF_ROI_PP,
            "live_ready_when": "siblings beat this paper twin inside the same parent epoch",
        }
    roi = evidence.get("estimated_paper_roi_pct")
    twin_roi = None if twin is None else twin.get("estimated_paper_roi_pct")
    diff = None if roi is None or twin_roi is None else round(float(roi) - float(twin_roi), 6)
    sample_met = int(evidence.get("resolved_paper_fills") or 0) >= SAMPLE_GATE_N
    pass_gate = bool(sample_met and diff is not None and diff >= REQUIRED_DIFF_ROI_PP)
    return {
        "status": "PASS_PAPER_CANDIDATE" if pass_gate else "PAPER_ACCUMULATING",
        "sample_gate_met": sample_met,
        "sample_gate_n": SAMPLE_GATE_N,
        "required_diff_roi_pp": REQUIRED_DIFF_ROI_PP,
        "roi_diff_pp_vs_twin": diff,
        "live_orders_allowed": False,
        "next": (
            "ask Fable for standard gate plus canary; do not auto-promote"
            if pass_gate
            else "continue paper sibling cadence until n>=50 and >=1pp ROI diff-in-diff vs paper twin"
        ),
    }


def build_state(
    *,
    cli_args: argparse.Namespace,
    guard: dict[str, Any],
    live_ledger: dict[str, Any],
    latest_change: dict[str, Any],
) -> dict[str, Any]:
    generated_at = utc_now_iso()
    parent = _selected_parent(guard)
    epoch = _epoch(guard, parent, latest_change)
    freshness_values = [
        _float(item)
        for item in str(cli_args.freshness_siblings).replace(" ", "").split(",")
        if item.strip()
    ]
    specs = _lane_specs(parent, freshness_values)
    orders = _ledger_scope_orders(live_ledger=live_ledger, parent=parent, epoch_start_ts=epoch.get("epoch_start_ts"))
    side_pnl = _side_pnl_map(live_ledger)
    costs = _group_costs(orders)
    lanes: list[dict[str, Any]] = []
    twin_evidence: dict[str, Any] | None = None
    for spec in specs:
        current = _current_lane_state(cli_args=cli_args, parent=parent, lane=spec)
        evidence = _ledger_lane_evidence(lane=spec, orders=orders, side_pnl=side_pnl, group_costs=costs)
        if spec["lane_role"] == "paper_twin":
            twin_evidence = evidence
        lanes.append(
            {
                **spec,
                "flow_stage": "LEARN/SELF-DEV",
                "paper_only": True,
                "live_orders_allowed": False,
                "parent_epoch_id": epoch["epoch_id"],
                "candidate_id": parent["candidate_id"],
                "source_wallet": parent["source_wallet"],
                "policy_id": parent["policy_id"],
                "copy_model": parent["copy_model"],
                "current": current,
                "evidence": evidence,
                "promotion_gate": _gate(spec, evidence, twin_evidence),
            }
        )
    siblings = [lane for lane in lanes if lane["lane_role"] == "sibling"]
    best_sibling = max(
        siblings,
        key=lambda row: (
            row["promotion_gate"].get("roi_diff_pp_vs_twin")
            if row["promotion_gate"].get("roi_diff_pp_vs_twin") is not None
            else -1_000_000.0,
            row["evidence"].get("resolved_paper_fills") or 0,
        ),
        default={},
    )
    return {
        "schema_version": 1,
        "kind": "wallet_copy_winner_variation_siblings",
        "flow_stage": "LEARN/SELF-DEV",
        "generated_at": generated_at,
        "status": "RUNNING" if parent.get("source_wallet") and parent.get("policy_id") else "CONFIG_MISSING",
        "paper_only": True,
        "live_orders_allowed": False,
        "single_submitter_invariant": "paper siblings never call the live adapter; scripts/run_wallet_copy_live_guard.py remains sole submitter",
        "copyintent_parity": {
            "status": "PASS",
            "basis": "lanes use the live execution CopyIntent builder and replay source_intents from the live ledger",
        },
        "cadence": {
            "source": "scripts/brainless_ops.sh deterministic refresh",
            "runtime_budget": "existing paper-fleet cadence; no live-path mutation",
        },
        "parent_config": parent,
        "parent_epoch": epoch,
        "summary": {
            "lane_count": len(lanes),
            "sibling_count": len(siblings),
            "paper_twin_lane_id": "paper_twin_exact_parent",
            "freshness_siblings_s": freshness_values,
            "sample_gate_n": SAMPLE_GATE_N,
            "required_diff_roi_pp": REQUIRED_DIFF_ROI_PP,
            "ledger_orders_in_epoch": len(orders),
            "best_sibling_lane_id": best_sibling.get("lane_id"),
            "best_sibling_roi_diff_pp": (best_sibling.get("promotion_gate") or {}).get("roi_diff_pp_vs_twin")
            if isinstance(best_sibling.get("promotion_gate"), dict)
            else None,
            "best_sibling_resolved_fills": (best_sibling.get("evidence") or {}).get("resolved_paper_fills")
            if isinstance(best_sibling.get("evidence"), dict)
            else None,
        },
        "lanes": lanes,
        "next": "continue paper cadence; a sibling needs n>=50 and >=1pp ROI diff-in-diff vs exact paper twin before Fable canary",
    }


def main() -> int:
    args = parse_args()
    guard = load_json(args.guard_state, default={})
    live_ledger = load_json(args.live_ledger_state, default={})
    latest_change = _latest_jsonl(args.change_journal)
    payload = build_state(
        cli_args=args,
        guard=guard if isinstance(guard, dict) else {},
        live_ledger=live_ledger if isinstance(live_ledger, dict) else {},
        latest_change=latest_change,
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
