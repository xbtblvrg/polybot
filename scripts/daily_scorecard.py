#!/usr/bin/env python3
"""Build the wallet-copy daily live scorecard from day-scoped ledger rows."""

from __future__ import annotations

import argparse
import json
import os
import plistlib
import subprocess
import sys
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.gate_registry import PRE_SUBMIT_REFUSAL_CLASSES  # noqa: E402
from src.wallet_copy.pnl_truth import (  # noqa: E402
    build_pnl_truth,
    chain_reconciliation,
    discrepancy_report,
    order_ts,
    resolution_snapshot_status,
    score_order,
    unresolved_position_bounds,
    validate_resolutions_nonempty_for_fills,
)
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from scripts.build_direction_leaderboard import build_leaderboard  # noqa: E402
from scripts.report_pipeline_slo import (  # noqa: E402
    build_report as build_pipeline_slo_report,
    read_wide_supervisor_heartbeat,
)


POINTER_PHRASE = "execute it literally as this run's instructions"
HEARTBEAT_POINTER = (
    "Read docs/agents/HEARTBEAT_PROMPT.md in "
    "/Users/belavarga/claudecode/polymarket-agent and execute it literally as this run's instructions."
)
RESEARCH_POINTER = (
    "Read docs/agents/RESEARCH_PROMPT.md in "
    "/Users/belavarga/claudecode/polymarket-agent and execute it literally as this run's instructions."
)
D97 = "0xd97ae021645712fe5cf73139049383a100cac068"
BTC_5M_WINDOWS_PER_DAY = 288
PHASE2_RESOLVED_FILL_TARGET = 300
PHASE2_POSITIVE_DAY_TARGET = 3
SIZING_BLOCK_FILLS = 100
SIZING_STEPS_USD = [8.0, 12.0, 16.0, 20.0]
LANE_PAPER_PROMOTION_FILLS = 50
LANE_LIVE_SCALE_FILLS = 150
LATE_WINDOW_EXPERIMENT_START = "2026-07-05T22:38:00Z"
LATE_WINDOW_EXPERIMENT_TRIPWIRE_USD = -10.0
EXPANSION_COHORT_START = "2026-07-06T15:51:00Z"
EXPANSION_COHORT_EVALUATION_DUE = "2026-07-07T00:00:00Z"
INVENTORY_LIKE_COPY_MODELS = {"inventory", "drip"}
PAPER_LANE_STATES = (
    ("whale_consensus", ROOT / "data/research/whale_consensus_paper_state.json"),
    ("track2_inventory", ROOT / "data/research/wallet_copy_inventory_paper_state.json"),
    ("e5_maker_first_btc5m", ROOT / "data/research/maker_first_btc5m_paper_state.json"),
    ("e6_whale_net_flow", ROOT / "data/research/e6_whale_net_flow_paper_lane_state.json"),
)
ENGINE_RACE_STATES = (
    ("E5_maker_first", ROOT / "data/research/maker_first_btc5m_paper_state.json"),
    ("E6_whale_net_flow", ROOT / "data/research/e6_whale_net_flow_paper_lane_state.json"),
)
DEFAULT_RECONCILIATION_START = "2026-07-05T12:55:00Z"
SINCE_TOPUP_OPERATOR_DECISION = "OP-SINCE-TOPUP-20260706"
DEFAULT_BALANCE_FEED_STATE = ROOT / "data/research/wallet_copy_balance_feed_state.json"
DEFAULT_ROUTING_SHADOW_STATE = ROOT / "data/research/routing_shadow_validation_latest.json"
DEFAULT_PEER_ACTIVE_IDLE_RESET_STATE = ROOT / "data/research/peer_active_idle_reset_state.json"
DEFENSE_REGRET_STANDARD_TRANCHE_USD = 2.5
DEFENSE_REGRET_ADVERSE_PRICE_TICK = 0.01
DEFAULT_SCORECARD_BALANCE_SAMPLE_COUNT = 1
DEFAULT_SCORECARD_BALANCE_SAMPLE_INTERVAL_S = 0.0
DEFAULT_SCORECARD_UNAVAILABLE_RESAMPLE_COUNT = 0
DEFAULT_SCORECARD_UNAVAILABLE_RESAMPLE_INTERVAL_S = 0.0
DEFAULT_SCORECARD_MISMATCH_RESAMPLE_COUNT = 0
DEFAULT_SCORECARD_MISMATCH_RESAMPLE_INTERVAL_S = 0.0
CURRENT_SCORECARD_POINTER = ROOT / "data/research/wallet_copy_daily_scorecard_current.json"
DIRECTION_LEADERBOARD = ROOT / "data/research/direction_leaderboard_latest.json"


def _write_scorecard_outputs(output: str, scorecard: dict[str, Any], *, now_day: str | None = None) -> None:
    # The current-day pointer is the canonical day basis for state_digest and
    # for every day-scoped defense tripwire, so it must refresh on the same
    # cadence as the heartbeat scorecard.  It deliberately does NOT depend on
    # --output: the heartbeat callers build today's scorecard for text display
    # and pass no output path, and gating the pointer on that argument pinned
    # the money basis to whichever day last ran with an explicit --output.
    current_day = now_day or datetime.now(tz=UTC).date().isoformat()
    if str(scorecard.get("day_utc") or "") == current_day:
        atomic_write_json(CURRENT_SCORECARD_POINTER, scorecard)
    if not output:
        return
    atomic_write_json(output, scorecard)


def _defense_regret_metric(
    day: str,
    actual_pnl_usd: float,
    routing_shadow: dict[str, Any],
    *,
    previous_scorecard: dict[str, Any] | None = None,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Measure whether defensive sizing plausibly changed a red day's sign.

    The counterfactual deliberately stays paper-only. It chooses the first
    routing-shadow winner in each market window, sizes that same signal at the
    normal $2.50 drip tranche, subtracts the recorded fee model, and applies
    the standing one-tick adverse-entry haircut to winning rows. Losing rows
    retain the full standard-tranche loss.
    """
    start_ts = datetime.fromisoformat(f"{day}T00:00:00+00:00").timestamp()
    end_ts = start_ts + 86400.0
    now_ts = datetime.now(tz=UTC).timestamp() if now_ts is None else float(now_ts)
    source_rows = [row for row in routing_shadow.get("rows") or [] if isinstance(row, dict)]
    day_rows = []
    for row in source_rows:
        ts = _parse_any_ts(row.get("winning_observed_ts") or row.get("observed_ts"))
        if ts is not None and start_ts <= ts < end_ts:
            day_rows.append(row)
    by_window: dict[str, dict[str, Any]] = {}
    for row in sorted(
        day_rows,
        key=lambda item: (
            _parse_any_ts(item.get("winning_observed_ts") or item.get("observed_ts")) or 0.0,
            str(item.get("winning_intent_id") or item.get("intent_id") or ""),
        ),
    ):
        window = str(row.get("market_slug") or f"window:{row.get('window_start_s')}")
        by_window.setdefault(window, row)

    raw_post_fee = 0.0
    haircut_post_fee = 0.0
    expected_fee_raw = 0.0
    expected_fee_haircut = 0.0
    wins = losses = resolved = 0
    for row in by_window.values():
        outcome = row.get("realized_paper_outcome") if isinstance(row.get("realized_paper_outcome"), dict) else {}
        if outcome.get("status") != "RESOLVED" or outcome.get("wins") is None:
            continue
        price = num(row.get("limit_price"), 0.0)
        if price <= 0.0 or price >= 1.0:
            continue
        resolved += 1
        won = bool(outcome.get("wins"))
        wins += int(won)
        losses += int(not won)
        fee_rate = max(0.0, num(row.get("expected_fee_rate"), 0.0))
        cost = DEFENSE_REGRET_STANDARD_TRANCHE_USD
        raw_fee = fee_rate * (cost / price) * price * (1.0 - price)
        raw_pre_fee = (cost / price) - cost if won else -cost
        raw_post_fee += raw_pre_fee - raw_fee
        expected_fee_raw += raw_fee

        haircut_price = min(0.999999, price + DEFENSE_REGRET_ADVERSE_PRICE_TICK)
        haircut_fee = fee_rate * (cost / haircut_price) * haircut_price * (1.0 - haircut_price)
        haircut_pre_fee = (cost / haircut_price) - cost if won else -cost
        haircut_post_fee += haircut_pre_fee - haircut_fee
        expected_fee_haircut += haircut_fee

    actual_red = float(actual_pnl_usd) < 0.0
    counterfactual_green = resolved > 0 and haircut_post_fee > 0.0
    sign_flipped = actual_red and counterfactual_green
    previous = (
        previous_scorecard.get("defense_regret")
        if isinstance(previous_scorecard, dict) and isinstance(previous_scorecard.get("defense_regret"), dict)
        else {}
    )
    previous_flipped = bool(previous.get("defense_flipped_sign"))
    two_day_trigger = bool(sign_flipped and previous_flipped)
    observed_ts = [
        _parse_any_ts(row.get("winning_observed_ts") or row.get("observed_ts"))
        for row in day_rows
    ]
    observed_ts = [value for value in observed_ts if value is not None]
    closed_day = now_ts >= end_ts
    retention_complete = bool(
        observed_ts
        and min(observed_ts) <= start_ts + 900.0
        and max(observed_ts) >= min(end_ts, now_ts) - 1800.0
    )
    status = "COMPLETE" if closed_day and retention_complete else "PARTIAL_OPEN_DAY" if not closed_day else "PARTIAL_RETENTION"
    return {
        "flow_stage": "LIVE/LEARN/DEFEND",
        "status": status,
        "measurement_only": True,
        "day_utc": day,
        "actual_probe_capped_pnl_usd": round(float(actual_pnl_usd), 6),
        "standard_caps": {"max_order_usd": 8.0, "drip_max_tranche_usd": DEFENSE_REGRET_STANDARD_TRANCHE_USD},
        "basis": "earliest_routing_shadow_would_submit_per_market_window",
        "routing_shadow_source": str(DEFAULT_ROUTING_SHADOW_STATE.relative_to(ROOT)),
        "candidate_windows": len(by_window),
        "resolved_windows": resolved,
        "unresolved_windows": max(0, len(by_window) - resolved),
        "wins": wins,
        "losses": losses,
        "raw_standard_cap_post_fee_pnl_usd": round(raw_post_fee, 6),
        "fill_realism_haircut": {
            "model": "one_tick_adverse_entry_on_winners_full_cost_loss_on_losers",
            "adverse_price_tick": DEFENSE_REGRET_ADVERSE_PRICE_TICK,
            "post_fee_pnl_usd": round(haircut_post_fee, 6),
            "delta_vs_raw_usd": round(haircut_post_fee - raw_post_fee, 6),
            "expected_fee_usd": round(expected_fee_haircut, 6),
        },
        "raw_expected_fee_usd": round(expected_fee_raw, 6),
        "regret_usd": round(haircut_post_fee - float(actual_pnl_usd), 6),
        "actual_closed_red": actual_red,
        "counterfactual_green": counterfactual_green,
        "defense_flipped_sign": sign_flipped,
        "previous_day_defense_flipped_sign": previous_flipped,
        "two_consecutive_sign_flips": two_day_trigger,
        "framework_audit_auto_pull": two_day_trigger,
        "rule": "two consecutive actual-red/counterfactual-green days auto-pull framework audit and defense recalibration review",
        "symmetry_guard": "measurement only; no live cap or threshold mutation without the framework audit and Fable authority",
    }


def _peer_active_idle_windows(
    volume_kpi: dict[str, Any],
    *,
    end_ts: float,
    now_ts: float | None = None,
    reset_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Count elapsed windows where peers traded but no order was accepted."""
    now_ts = datetime.now(tz=UTC).timestamp() if now_ts is None else float(now_ts)
    cutoff = min(float(end_ts), now_ts) - 300.0
    missed = (
        volume_kpi.get("missed_window_attribution")
        if isinstance(volume_kpi.get("missed_window_attribution"), dict)
        else {}
    )
    rows = [row for row in missed.get("rows") or [] if isinstance(row, dict)]
    elapsed = sorted(
        (row for row in rows if num(row.get("window_start_s"), 0.0) <= cutoff),
        key=lambda row: num(row.get("window_start_s"), 0.0),
    )
    peer_idle = [row for row in elapsed if str(row.get("attribution") or "") == "guard_reject"]
    reset_state = reset_state if isinstance(reset_state, dict) else {}
    reset_at = _parse_utc_ts(str(reset_state.get("reset_at") or ""))
    consecutive_rows = (
        [row for row in elapsed if num(row.get("window_start_s"), 0.0) >= float(reset_at)]
        if reset_at is not None
        else elapsed
    )
    consecutive = 0
    for row in reversed(consecutive_rows):
        if str(row.get("attribution") or "") != "guard_reject":
            break
        consecutive += 1
    incident_triggered = consecutive >= 3
    swap_gate = {
        "status": "FABLE_TARGET_VALIDATION_REQUIRED" if incident_triggered else "NOT_APPLICABLE",
        "swap_authorized": False,
        "required_evidence": [
            "named rung-A target with materially_nonzero own-source acceptance",
            "completed standby evidence clock",
        ],
        "rule": (
            "peer-active idle detects RED but cannot assert a prevalidated standby exists; "
            "Fable owns seat/rotation and validates the target before swap"
        ),
    }
    return {
        "flow_stage": "LIVE/DEFEND/ROTATE",
        "status": "RED_INCIDENT" if incident_triggered else "CLEAR",
        "peer_active_idle_windows": len(peer_idle),
        "consecutive_peer_active_idle_windows": consecutive,
        "incident_threshold_windows": 3,
        "incident_triggered": incident_triggered,
        "elapsed_windows_evaluated": len(elapsed),
        "post_reset_windows_evaluated": len(consecutive_rows),
        "counter_reset": {
            "active": reset_at is not None,
            "reset_at": reset_state.get("reset_at") if reset_at is not None else None,
            "reason": reset_state.get("reason") if reset_at is not None else None,
            "direction_id": reset_state.get("direction_id") if reset_at is not None else None,
        },
        "definition": "elapsed canonical BTC-5m windows attributed guard_reject: watched source traded, our accepted order count was zero",
        "swap_gate": swap_gate,
        "next_action": (
            "ASK_FABLE: peer-active idle RED; validate a named materially-nonzero rung-A target and completed standby clock before any swap"
            if incident_triggered
            else "continue per-window evaluation"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument("--guard-state", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--ready-shadow-state", default="data/research/wallet_copy_ready_shadow_lanes_state.json")
    parser.add_argument("--peer-active-idle-reset-state", default=str(DEFAULT_PEER_ACTIVE_IDLE_RESET_STATE))
    parser.add_argument(
        "--resolutions",
        default="",
        help="Resolution JSONL path; default selects newest btc_resolutions_*.jsonl snapshot.",
    )
    parser.add_argument("--bankroll-usd", type=float, default=335.0)
    parser.add_argument(
        "--reconciliation-start",
        default=os.getenv("WALLET_COPY_RECONCILIATION_START", DEFAULT_RECONCILIATION_START),
        help="UTC timestamp where the bankroll baseline resets; chain cash identity uses PnL from this point.",
    )
    parser.add_argument(
        "--balance-sample-count",
        type=int,
        default=int(
            os.getenv(
                "WALLET_COPY_SCORECARD_BALANCE_SAMPLE_COUNT",
                str(DEFAULT_SCORECARD_BALANCE_SAMPLE_COUNT),
            )
        ),
        help="Number of CLOB balance samples for scorecard reconciliation; heartbeat default is bounded.",
    )
    parser.add_argument(
        "--balance-sample-interval-s",
        type=float,
        default=float(
            os.getenv(
                "WALLET_COPY_SCORECARD_BALANCE_SAMPLE_INTERVAL_S",
                str(DEFAULT_SCORECARD_BALANCE_SAMPLE_INTERVAL_S),
            )
        ),
        help="Seconds between balance samples for scorecard reconciliation; heartbeat default is zero.",
    )
    parser.add_argument(
        "--balance-unavailable-resample-count",
        type=int,
        default=int(
            os.getenv(
                "WALLET_COPY_SCORECARD_BALANCE_UNAVAILABLE_RESAMPLE_COUNT",
                str(DEFAULT_SCORECARD_UNAVAILABLE_RESAMPLE_COUNT),
            )
        ),
        help="Extra balance samples after an unavailable scorecard fetch; default avoids deterministic-run stalls.",
    )
    parser.add_argument(
        "--balance-unavailable-resample-interval-s",
        type=float,
        default=float(
            os.getenv(
                "WALLET_COPY_SCORECARD_BALANCE_UNAVAILABLE_RESAMPLE_INTERVAL_S",
                str(DEFAULT_SCORECARD_UNAVAILABLE_RESAMPLE_INTERVAL_S),
            )
        ),
        help="Seconds between unavailable-balance resamples.",
    )
    parser.add_argument(
        "--balance-mismatch-resample-count",
        type=int,
        default=int(
            os.getenv(
                "WALLET_COPY_SCORECARD_BALANCE_MISMATCH_RESAMPLE_COUNT",
                str(DEFAULT_SCORECARD_MISMATCH_RESAMPLE_COUNT),
            )
        ),
        help="Extra balance samples after a mismatch; default avoids repeatedly sampling known named residuals.",
    )
    parser.add_argument(
        "--balance-mismatch-resample-interval-s",
        type=float,
        default=float(
            os.getenv(
                "WALLET_COPY_SCORECARD_BALANCE_MISMATCH_RESAMPLE_INTERVAL_S",
                str(DEFAULT_SCORECARD_MISMATCH_RESAMPLE_INTERVAL_S),
            )
        ),
        help="Seconds between mismatch resamples.",
    )
    parser.add_argument(
        "--offline-no-chain",
        action="store_true",
        help="Build from local ledger/fill data only; skip CLOB/chain balance reconciliation.",
    )
    parser.add_argument("--target-phase", choices=("phase1",), default="phase1")
    parser.add_argument("--day", default="", help="UTC day YYYY-MM-DD; default is today UTC.")
    parser.add_argument("--output", default="", help="Optional JSON output path.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args()


def _day_bounds(day: str) -> tuple[str, float, float]:
    if day:
        start_dt = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=UTC)
    else:
        now = datetime.now(tz=UTC)
        start_dt = datetime(now.year, now.month, now.day, tzinfo=UTC)
    end_dt = start_dt + timedelta(days=1)
    return start_dt.date().isoformat(), start_dt.timestamp(), end_dt.timestamp()


def _order_time_scopes(
    orders: list[dict[str, Any]],
    *,
    start_ts: float,
    end_ts: float,
    reconciliation_start_ts: float | None,
) -> dict[str, Any]:
    """Pre-scope immutable day slices so scorecard refresh cost grows slower."""

    previous_start_ts = float(start_ts) - 86400.0
    since_start_ts = float(reconciliation_start_ts) if reconciliation_start_ts is not None else None
    day_orders: list[dict[str, Any]] = []
    previous_day_orders: list[dict[str, Any]] = []
    since_reconciliation_orders: list[dict[str, Any]] = []
    missing_ts = 0
    for order in orders:
        if not isinstance(order, dict):
            continue
        ts = order_ts(order)
        if ts is None:
            missing_ts += 1
            continue
        value = float(ts)
        if float(start_ts) <= value < float(end_ts):
            day_orders.append(order)
        if previous_start_ts <= value < float(start_ts):
            previous_day_orders.append(order)
        if since_start_ts is None or value >= since_start_ts:
            since_reconciliation_orders.append(order)
    return {
        "flow_stage": "LIVE/SELF-DEV",
        "source": "ledger.orders pre-scoped by order_ts before PnL scoring",
        "ledger_orders": len(orders),
        "missing_ts_orders": missing_ts,
        "day_orders": day_orders,
        "previous_day_orders": previous_day_orders,
        "since_reconciliation_orders": since_reconciliation_orders,
        "summary": {
            "ledger_orders": len(orders),
            "missing_ts_orders": missing_ts,
            "day_orders": len(day_orders),
            "previous_day_orders": len(previous_day_orders),
            "since_reconciliation_orders": len(since_reconciliation_orders),
            "day_start_ts": float(start_ts),
            "day_end_ts": float(end_ts),
            "reconciliation_start_ts": since_start_ts,
        },
    }


def _parse_utc_ts(value: str) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def _default_resolutions_path() -> str:
    candidates = [
        path
        for path in (ROOT / "data" / "research").glob("btc_resolutions_*.jsonl")
        if path.is_file()
    ]
    if not candidates:
        return "data/research/btc_resolutions_from_gamma_live_ledger_20260705_1857.jsonl"
    newest = max(candidates, key=lambda path: path.stat().st_mtime)
    return str(newest.relative_to(ROOT))


def _default_receipt_costs_path() -> Path | None:
    candidates = [
        path
        for path in (ROOT / "data" / "research").glob("wallet_copy_cash_flow_replay_tx_match_*.json")
        if path.is_file()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _default_actual_trade_costs_path() -> Path | None:
    path = ROOT / "data" / "research" / "wallet_copy_today_fill_cash_diff_latest.json"
    return path if path.is_file() else None


def _load_receipt_costs(path: Path | None = None) -> tuple[dict[str, float], dict[str, Any]]:
    source_path = path if path is not None else _default_receipt_costs_path()
    if source_path is None:
        return {}, {"status": "MISSING", "cost_basis_source": "response_filled_size_usd"}
    payload = load_json(source_path, default={})
    rows = payload.get("rows") if isinstance(payload, dict) and isinstance(payload.get("rows"), list) else []
    costs: dict[str, float] = {}
    skipped_multi_order_txs = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        amount = num(row.get("pUSD_out"))
        if amount <= 0:
            continue
        order_count = int(num(row.get("orders")))
        if order_count > 1:
            skipped_multi_order_txs += 1
            continue
        for key in (row.get("tx"), row.get("sample_order")):
            text = str(key or "").strip().lower()
            if text:
                costs[text] = amount
    summary = payload.get("summary") if isinstance(payload, dict) and isinstance(payload.get("summary"), dict) else {}
    return costs, {
        "status": "LOADED" if costs else "EMPTY",
        "path": str(source_path),
        "mapped_keys": len(costs),
        "tx_count": summary.get("tx_count", len(rows)),
        "pUSD_out_sum": summary.get("pUSD_out_sum"),
        "ledger_cost_sum": summary.get("ledger_cost_sum"),
        "out_minus_cost_sum": summary.get("out_minus_cost_sum"),
        "skipped_multi_order_txs": skipped_multi_order_txs,
        "cost_basis_source": "tx_receipt_pusd_debit" if costs else "response_filled_size_usd",
        "fallback_cost_basis_source": "response_filled_size_usd",
    }


def _load_actual_trade_costs(path: Path | None = None) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    source_path = path if path is not None else _default_actual_trade_costs_path()
    if source_path is None:
        return {}, {"status": "MISSING", "cost_basis_source": "actual_trade_record"}
    payload = load_json(source_path, default={})
    rows = payload.get("rows") if isinstance(payload, dict) and isinstance(payload.get("rows"), list) else []
    costs: dict[str, dict[str, Any]] = {}
    joined_rows = 0
    skipped_multi_order_txs = 0
    for row in rows:
        if not isinstance(row, dict) or not str(row.get("join_status") or "").startswith("JOINED"):
            continue
        actual_cost = num(row.get("actual_cost_usd"))
        if actual_cost <= 0:
            continue
        order_ids = [str(value or "").strip().lower() for value in row.get("order_ids") or [] if str(value or "").strip()]
        if len(order_ids) != 1:
            skipped_multi_order_txs += 1
            continue
        tx = str(row.get("tx") or "").strip().lower()
        record = {
            "actual_cost_usd": round(actual_cost, 6),
            "source": "actual_trade_record",
            "tx": tx,
            "join_key": tx,
            "join_key_source": "wallet_copy_today_fill_cash_diff.rows[].tx",
        }
        if tx:
            costs[tx] = record
        costs[order_ids[0]] = record
        joined_rows += 1
    summary = payload.get("summary") if isinstance(payload, dict) and isinstance(payload.get("summary"), dict) else {}
    return costs, {
        "status": "LOADED" if costs else "EMPTY",
        "path": str(source_path),
        "mapped_keys": len(costs),
        "joined_tx_groups": summary.get("joined_tx_groups", joined_rows),
        "fills_missing_tx": summary.get("ledger_fills_missing_tx"),
        "skipped_multi_order_txs": skipped_multi_order_txs,
        "cost_basis_source": "actual_trade_record" if costs else "response_filled_size_usd",
        "fallback_cost_basis_source": "response_filled_size_usd",
    }


def _actual_cost_backfill_coverage_watermark_ts(
    path: Path = ROOT / "data/research/wallet_copy_actual_trade_cost_backfill_latest.json",
) -> float | None:
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        return None
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    for value in (summary.get("updated_at"), payload.get("generated_at")):
        ts = _parse_any_ts(value)
        if ts > 0:
            return ts
    return None


def _chain_reconciliation_scope(
    orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    *,
    baseline_usd: float,
    reconciliation_start: str,
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
    unjoined_actual_gap_start_ts: float | None = None,
    balance_sample_count: int | None = None,
    balance_sample_interval_s: float | None = None,
    balance_unavailable_resample_count: int | None = None,
    balance_unavailable_resample_interval_s: float | None = None,
    balance_mismatch_resample_count: int | None = None,
    balance_mismatch_resample_interval_s: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    reconciliation_start_ts = _parse_utc_ts(reconciliation_start)
    truth = build_pnl_truth(
        {"orders": orders},
        resolutions,
        start_ts=reconciliation_start_ts,
        receipt_costs=receipt_costs,
        actual_trade_costs=actual_trade_costs,
    )
    bounds = unresolved_position_bounds(truth)
    reconciliation = chain_reconciliation(
        baseline_usd=float(baseline_usd),
        canonical_pnl_usd=float(truth.get("total", {}).get("pnl_usd") or 0.0),
        unresolved_open_cost_usd=float(bounds.get("open_cost_usd") or 0.0),
        unresolved_max_payout_usd=float(bounds.get("max_payout_usd") or 0.0),
        balance_sample_count=balance_sample_count,
        balance_sample_interval_s=balance_sample_interval_s,
        unavailable_resample_count=balance_unavailable_resample_count,
        unavailable_resample_interval_s=balance_unavailable_resample_interval_s,
        mismatch_resample_count=balance_mismatch_resample_count,
        mismatch_resample_interval_s=balance_mismatch_resample_interval_s,
    )
    reconciliation = _attach_point_in_time_adjustments(
        reconciliation,
        orders,
        truth,
        reconciliation_start_ts=reconciliation_start_ts,
        actual_trade_costs=actual_trade_costs,
        unjoined_actual_gap_start_ts=unjoined_actual_gap_start_ts,
    )
    reconciliation["canonical_pnl_scope"] = "since_reconciliation_start"
    reconciliation["reconciliation_start_iso"] = str(reconciliation_start or "")
    reconciliation["reconciliation_start_ts"] = reconciliation_start_ts
    return reconciliation, truth, bounds


def _offline_no_chain_reconciliation_scope(
    orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    *,
    baseline_usd: float,
    reconciliation_start: str,
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    reconciliation_start_ts = _parse_utc_ts(reconciliation_start)
    truth = build_pnl_truth(
        {"orders": orders},
        resolutions,
        start_ts=reconciliation_start_ts,
        receipt_costs=receipt_costs,
        actual_trade_costs=actual_trade_costs,
    )
    bounds = unresolved_position_bounds(truth)
    canonical_pnl = float(truth.get("total", {}).get("pnl_usd") or 0.0)
    reconciliation = {
        "schema_version": 1,
        "kind": "wallet_copy_chain_reconciliation",
        "status": "OFFLINE_NO_CHAIN",
        "balance_status": "UNAVAILABLE",
        "balance_reason": "offline_no_chain_scorecard_mode",
        "basis": "local_ledger_fill_data_only",
        "chain_reconciliation_available": False,
        "live_cash_balance_usd": None,
        "account_value_usd": None,
        "baseline_usd": round(float(baseline_usd), 6),
        "canonical_pnl_usd": round(canonical_pnl, 6),
        "expected_value_usd": round(float(baseline_usd) + canonical_pnl, 6),
        "expected_cash_identity_usd": round(float(baseline_usd) + canonical_pnl, 6),
        "delta_vs_expected_usd": None,
        "unresolved_open_cost_usd": round(float(bounds.get("open_cost_usd") or 0.0), 6),
        "unresolved_max_payout_usd": round(float(bounds.get("max_payout_usd") or 0.0), 6),
        "unresolved_position_value_bounds_usd": bounds.get("position_value_bounds_usd"),
        "canonical_pnl_scope": "since_reconciliation_start",
        "reconciliation_start_iso": str(reconciliation_start or ""),
        "reconciliation_start_ts": reconciliation_start_ts,
        "pnl_claim_rule": (
            "machine money-truth only; chain reconciliation unavailable in offline/no-chain mode"
        ),
    }
    return reconciliation, truth, bounds


def _maybe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_or_none(value: float | None) -> float | None:
    return round(float(value), 6) if value is not None else None


def _load_dotenv_value(name: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    env_path = ROOT / ".env"
    if not env_path.exists():
        return ""
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _parse_any_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "")
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return num(text, 0.0)


def _order_tx(order: dict[str, Any]) -> str:
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    tx_hashes = trade_result.get("tx_hashes") if isinstance(trade_result.get("tx_hashes"), list) else []
    candidates = [
        *tx_hashes,
        trade_result.get("transaction_hash"),
        order.get("transaction_hash"),
    ]
    return next((str(value).lower() for value in candidates if value), "")


def _btc_window_close_ts(slug: str) -> float | None:
    text = str(slug or "")
    if not text.startswith("btc-updown-5m-"):
        return None
    marker = text.rsplit("-", 1)[-1]
    if not marker.isdigit():
        return None
    return float(int(marker) + 300)


def _latest_balance_sample_ts(reconciliation: dict[str, Any]) -> float:
    sampling = reconciliation.get("balance_sampling") if isinstance(reconciliation.get("balance_sampling"), dict) else {}
    samples = sampling.get("samples") if isinstance(sampling.get("samples"), list) else []
    return max((_parse_any_ts(sample.get("ts")) for sample in samples if isinstance(sample, dict)), default=time_now_ts())


def time_now_ts() -> float:
    return datetime.now(tz=UTC).timestamp()


def _fetch_data_api_activity(
    *,
    user: str,
    start_ts: float,
    end_ts: float,
    limit: int = 500,
    max_pages: int = 20,
    timeout_s: float = 10.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not user:
        return [], {"status": "UNAVAILABLE", "reason": "POLYMARKET_PROXY_missing"}
    rows: list[dict[str, Any]] = []
    urls: list[str] = []
    for page in range(max_pages):
        query = urllib.parse.urlencode({"user": user, "limit": int(limit), "offset": page * int(limit)})
        url = f"https://data-api.polymarket.com/activity?{query}"
        urls.append(url)
        request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=float(timeout_s)) as response:
            payload = json.loads(response.read().decode("utf-8"))
        page_rows = [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
        for row in page_rows:
            ts = _parse_any_ts(row.get("timestamp"))
            if start_ts <= ts <= end_ts:
                rows.append(row)
        if len(page_rows) < int(limit):
            return rows, {"status": "OK", "pages": page + 1, "truncated": False, "urls": urls}
        oldest_ts = min((_parse_any_ts(row.get("timestamp")) for row in page_rows), default=0.0)
        if oldest_ts and oldest_ts < start_ts:
            return rows, {"status": "OK", "pages": page + 1, "truncated": False, "urls": urls}
    return rows, {"status": "OK", "pages": int(max_pages), "truncated": True, "urls": urls}


def _pending_redemption_adjustment(
    truth: dict[str, Any],
    *,
    reconciliation_start_ts: float,
    scorecard_ts: float,
) -> dict[str, Any]:
    events = [row for row in truth.get("events") or [] if isinstance(row, dict)]
    payout_by_condition: dict[str, float] = defaultdict(float)
    window_close_by_condition: dict[str, float] = {}
    for event in events:
        payout = num(event.get("payout_usd"), 0.0)
        condition_id = str(event.get("condition_id") or "")
        if payout <= 0 or not condition_id:
            continue
        payout_by_condition[condition_id] += payout
        close_ts = _btc_window_close_ts(str(event.get("market_slug") or ""))
        if close_ts is not None:
            window_close_by_condition[condition_id] = max(window_close_by_condition.get(condition_id, 0.0), close_ts)
    if not payout_by_condition:
        return {
            "pending_redemption_usd": 0.0,
            "redemption_credit_fetch": {"status": "SKIPPED", "reason": "no_canonical_payouts"},
            "redemption_lag": {"matched_conditions": 0, "max_observed_lag_s": None},
            "pending_redemption_conditions": [],
        }
    user = _load_dotenv_value("POLYMARKET_PROXY")
    try:
        activity_rows, fetch_meta = _fetch_data_api_activity(
            user=user,
            start_ts=float(reconciliation_start_ts or 0.0),
            end_ts=float(scorecard_ts),
        )
    except Exception as exc:
        return {
            "pending_redemption_usd": 0.0,
            "redemption_credit_fetch": {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}"},
            "redemption_lag": {"matched_conditions": 0, "max_observed_lag_s": None},
            "pending_redemption_conditions": [],
        }
    redeemed_by_condition: dict[str, float] = defaultdict(float)
    redeem_ts_by_condition: dict[str, float] = {}
    for row in activity_rows:
        if str(row.get("type") or "").upper() != "REDEEM":
            continue
        condition_id = str(row.get("conditionId") or row.get("condition_id") or "")
        if not condition_id:
            continue
        amount = num(row.get("usdcSize"), 0.0) or num(row.get("size"), 0.0)
        redeemed_by_condition[condition_id] += amount
        redeem_ts_by_condition[condition_id] = max(redeem_ts_by_condition.get(condition_id, 0.0), _parse_any_ts(row.get("timestamp")))
    pending_rows: list[dict[str, Any]] = []
    observed_lags: list[float] = []
    pending_total = 0.0
    for condition_id, payout in payout_by_condition.items():
        redeemed = redeemed_by_condition.get(condition_id, 0.0)
        pending = round(max(0.0, payout - redeemed), 6)
        if redeem_ts_by_condition.get(condition_id) and window_close_by_condition.get(condition_id):
            observed_lags.append(max(0.0, redeem_ts_by_condition[condition_id] - window_close_by_condition[condition_id]))
        if pending <= 0:
            continue
        pending_total += pending
        pending_rows.append(
            {
                "condition_id": condition_id,
                "canonical_payout_usd": round(payout, 6),
                "redeemed_credit_usd": round(redeemed, 6),
                "pending_redemption_usd": pending,
                "window_close_ts": window_close_by_condition.get(condition_id),
                "matched_redeem_ts": redeem_ts_by_condition.get(condition_id),
            }
        )
    return {
        "pending_redemption_usd": round(pending_total, 6),
        "redemption_credit_fetch": {key: value for key, value in fetch_meta.items() if key != "urls"},
        "redemption_lag": {
            "matched_conditions": len(observed_lags),
            "max_observed_lag_s": round(max(observed_lags), 6) if observed_lags else None,
        },
        "pending_redemption_conditions": sorted(
            pending_rows,
            key=lambda row: row.get("pending_redemption_usd", 0.0),
            reverse=True,
        )[:20],
    }


def _in_flight_fill_adjustment(
    orders: list[dict[str, Any]],
    truth: dict[str, Any],
    *,
    actual_trade_costs: dict[str, Any] | None,
    balance_sample_ts: float,
    unjoined_actual_gap_start_ts: float | None = None,
) -> dict[str, Any]:
    orders_by_id = {str(order.get("order_id") or ""): order for order in orders if isinstance(order, dict)}
    costs = actual_trade_costs or {}
    in_flight_fill_usd = 0.0
    unjoined_actual_gap_usd = 0.0
    in_flight_rows: list[dict[str, Any]] = []
    unjoined_rows: list[dict[str, Any]] = []
    for event in truth.get("events") or []:
        if not isinstance(event, dict):
            continue
        order_id = str(event.get("order_id") or "")
        order = orders_by_id.get(order_id, {})
        if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
            continue
        cost = num(event.get("cost_usd"), 0.0)
        if cost <= 0:
            continue
        fill_ts = _parse_any_ts(order.get("submitted_at") or order.get("updated_at"))
        tx = _order_tx(order)
        if fill_ts > float(balance_sample_ts or 0.0):
            in_flight_fill_usd += cost
            in_flight_rows.append({"order_id": order_id, "tx": tx, "cost_usd": round(cost, 6), "fill_ts": fill_ts})
            continue
        trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        order_has_actual_cost = (
            (tx and tx in costs)
            or (order_id and order_id in costs)
            or num(order.get("actual_trade_cost_usd"), 0.0) > 0
            or num(trade_result.get("actual_trade_cost_usd"), 0.0) > 0
        )
        if unjoined_actual_gap_start_ts is not None and fill_ts < float(unjoined_actual_gap_start_ts):
            continue
        if tx and not order_has_actual_cost:
            unjoined_actual_gap_usd += cost
            unjoined_rows.append({"order_id": order_id, "tx": tx, "cost_usd": round(cost, 6), "fill_ts": fill_ts})
    return {
        "in_flight_fill_usd": round(in_flight_fill_usd, 6),
        "unjoined_actual_gap_usd": round(unjoined_actual_gap_usd, 6),
        "in_flight_fill_rows": in_flight_rows[-20:],
        "unjoined_actual_gap_rows": unjoined_rows[-20:],
    }


def _apply_point_in_time_adjustments(
    reconciliation: dict[str, Any],
    *,
    pending_redemption_usd: float,
    in_flight_fill_usd: float,
    unjoined_actual_gap_usd: float,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    delta = _maybe_float(reconciliation.get("delta_vs_expected_usd"))
    tolerance = float(reconciliation.get("tolerance_usd") or 2.0)
    adjusted_delta = None
    if delta is not None:
        adjusted_delta = round(
            delta + float(pending_redemption_usd) + float(in_flight_fill_usd) + float(unjoined_actual_gap_usd),
            6,
        )
    raw_status = str(reconciliation.get("status") or "")
    adjusted_status = raw_status
    if adjusted_delta is not None:
        adjusted_status = "PASS" if abs(adjusted_delta) <= tolerance else "MISMATCH"
    existing = reconciliation.get("point_in_time_adjustments")
    existing = existing if isinstance(existing, dict) else {}
    reconciliation["raw_status_before_point_in_time_adjustment"] = raw_status
    reconciliation["raw_delta_vs_expected_usd"] = reconciliation.get("delta_vs_expected_usd")
    reconciliation["status"] = adjusted_status
    reconciliation["status_basis"] = "adjusted_cash_identity_with_tx_measured_timing_v1"
    reconciliation["point_in_time_adjustments"] = {
        **existing,
        "pending_redemption_usd": round(float(pending_redemption_usd), 6),
        "in_flight_fill_usd": round(float(in_flight_fill_usd), 6),
        "unjoined_actual_gap_usd": round(float(unjoined_actual_gap_usd), 6),
        "adjusted_delta_vs_expected_usd": adjusted_delta,
        "raw_delta_vs_expected_usd": reconciliation.get("delta_vs_expected_usd"),
        "raw_status": raw_status,
        "adjusted_status": adjusted_status,
        "status_basis": "delta + tx_measured_pending_redemption + in_flight_fill + unjoined_actual_gap",
        **(details or {}),
    }
    return reconciliation


def _attach_point_in_time_adjustments(
    reconciliation: dict[str, Any],
    orders: list[dict[str, Any]],
    truth: dict[str, Any],
    *,
    reconciliation_start_ts: float | None,
    actual_trade_costs: dict[str, Any] | None,
    unjoined_actual_gap_start_ts: float | None = None,
) -> dict[str, Any]:
    if _maybe_float(reconciliation.get("delta_vs_expected_usd")) is None:
        return reconciliation
    balance_sample_ts = _latest_balance_sample_ts(reconciliation)
    scorecard_ts = max(balance_sample_ts, time_now_ts())
    pending = _pending_redemption_adjustment(
        truth,
        reconciliation_start_ts=float(reconciliation_start_ts or 0.0),
        scorecard_ts=scorecard_ts,
    )
    in_flight = _in_flight_fill_adjustment(
        orders,
        truth,
        actual_trade_costs=actual_trade_costs,
        balance_sample_ts=balance_sample_ts,
        unjoined_actual_gap_start_ts=unjoined_actual_gap_start_ts,
    )
    return _apply_point_in_time_adjustments(
        reconciliation,
        pending_redemption_usd=float(pending.get("pending_redemption_usd") or 0.0),
        in_flight_fill_usd=float(in_flight.get("in_flight_fill_usd") or 0.0),
        unjoined_actual_gap_usd=float(in_flight.get("unjoined_actual_gap_usd") or 0.0),
        details={
            "balance_sample_ts": round(balance_sample_ts, 6),
            "scorecard_ts": round(scorecard_ts, 6),
            "unjoined_actual_gap_start_ts": _round_or_none(unjoined_actual_gap_start_ts),
            **pending,
            **in_flight,
        },
    )


def _since_topup_truth(reconciliation: dict[str, Any], truth: dict[str, Any]) -> dict[str, Any]:
    """Primary production verdict since the last wallet top-up baseline."""

    total = truth.get("total") if isinstance(truth.get("total"), dict) else {}
    baseline = float(reconciliation.get("baseline_usd") or 0.0)
    canonical_pnl = float(reconciliation.get("canonical_pnl_usd") or total.get("pnl_usd") or 0.0)
    live_cash = _maybe_float(reconciliation.get("live_cash_balance_usd"))
    account_value = _maybe_float(reconciliation.get("account_value_usd"))
    actual_value = account_value if account_value is not None else live_cash
    actual_delta = None if actual_value is None else actual_value - baseline
    cash_delta = None if live_cash is None else live_cash - baseline
    expected_value = _maybe_float(reconciliation.get("expected_value_usd"))
    expected_delta = None if expected_value is None else expected_value - baseline
    balance_available = actual_value is not None
    actual_positive = bool(balance_available and actual_delta is not None and actual_delta > 0)
    if not balance_available:
        verdict = "UNKNOWN_BALANCE"
    elif actual_positive:
        verdict = "PRODUCING"
    else:
        verdict = "NOT_PRODUCING"
    canonical_positive = canonical_pnl > 0
    return {
        "flow_stage": "LIVE/SELF-DEV",
        "operator_decision": SINCE_TOPUP_OPERATOR_DECISION,
        "baseline_usd": round(baseline, 6),
        "baseline_iso": str(reconciliation.get("reconciliation_start_iso") or ""),
        "baseline_source": "last_wallet_topup_bankroll_baseline",
        "canonical_pnl_scope": "since_last_wallet_topup",
        "cost_basis_source": str(truth.get("cost_basis_source") or "response_filled_size_usd"),
        "cost_basis_counts": (truth.get("scope") or {}).get("cost_basis_counts", {}),
        "canonical_pnl_usd": round(canonical_pnl, 6),
        "canonical_pnl_pct": round((100.0 * canonical_pnl / baseline), 6) if baseline > 0 else 0.0,
        "canonical_producing": canonical_positive,
        "resolved_fills": int(total.get("resolved_fills") or 0),
        "expected_account_value_usd": _round_or_none(expected_value),
        "expected_delta_vs_baseline_usd": _round_or_none(expected_delta),
        "expected_cash_identity_usd": _round_or_none(_maybe_float(reconciliation.get("expected_cash_identity_usd"))),
        "unresolved_open_cost_usd": _round_or_none(_maybe_float(reconciliation.get("unresolved_open_cost_usd"))),
        "unresolved_position_value_bounds_usd": reconciliation.get("unresolved_position_value_bounds_usd"),
        "live_cash_balance_usd": _round_or_none(live_cash),
        "actual_cash_delta_vs_baseline_usd": _round_or_none(cash_delta),
        "account_value_usd": _round_or_none(account_value),
        "actual_account_delta_vs_baseline_usd": _round_or_none(None if account_value is None else account_value - baseline),
        "actual_value_basis": "account_value" if account_value is not None else "live_cash_balance",
        "actual_value_usd": _round_or_none(actual_value),
        "actual_delta_vs_baseline_usd": _round_or_none(actual_delta),
        "actual_producing": actual_positive,
        "primary_verdict": verdict,
        "primary_verdict_basis": "chain_anchored_actual",
        "canonical_pnl_reporting_only": True,
        "reconciliation_status": reconciliation.get("status"),
        "balance_status": reconciliation.get("balance_status"),
        "balance_reason": reconciliation.get("balance_reason"),
        "daily_delta_success_claim_allowed": False,
        "status_basis": "Fable 2026-07-09T21:55Z: production verdict uses chain-anchored actual account value since last top-up; canonical PnL remains reporting-only",
    }


def _balance_feed_monitor(
    reconciliation: dict[str, Any],
    *,
    state_path: Path | None = None,
) -> dict[str, Any]:
    """Track consecutive scorecard generations with unavailable live balance."""

    path = Path(os.getenv("WALLET_COPY_BALANCE_FEED_STATE_PATH", str(state_path or DEFAULT_BALANCE_FEED_STATE)))
    previous = load_json(path, {}) if path.exists() else {}
    previous_streak = int(num(previous.get("consecutive_unavailable_generations"), 0)) if isinstance(previous, dict) else 0
    balance_status = str(reconciliation.get("balance_status") or "UNAVAILABLE").upper()
    if str(reconciliation.get("balance_reason") or "") == "offline_no_chain_scorecard_mode":
        monitor = {
            "kind": "wallet_copy_balance_feed_monitor",
            "flow_stage": "LIVE/DEFEND/SELF-DEV",
            "generated_at": utc_now_iso(),
            "state_path": str(path),
            "balance_status": balance_status,
            "balance_reason": "offline_no_chain_scorecard_mode",
            "consecutive_unavailable_generations": previous_streak,
            "previous_consecutive_unavailable_generations": previous_streak,
            "defect": False,
            "status": "OFFLINE_NO_CHAIN_SKIPPED",
            "rule": "offline/no-chain scorecard intentionally skips live balance sampling; this does not increment the balance-feed failure streak",
        }
        atomic_write_json(path, monitor)
        return monitor
    unavailable = balance_status == "UNAVAILABLE"
    streak = previous_streak + 1 if unavailable else 0
    monitor = {
        "kind": "wallet_copy_balance_feed_monitor",
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "generated_at": utc_now_iso(),
        "state_path": str(path),
        "balance_status": balance_status,
        "balance_reason": str(reconciliation.get("balance_reason") or ""),
        "consecutive_unavailable_generations": streak,
        "previous_consecutive_unavailable_generations": previous_streak,
        "defect": unavailable and streak >= 2,
        "status": "DEFECT" if unavailable and streak >= 2 else ("UNAVAILABLE" if unavailable else "PASS"),
        "rule": "two consecutive UNAVAILABLE balance generations = balance-feed defect",
    }
    atomic_write_json(path, monitor)
    return monitor


def _empty_metric() -> dict[str, Any]:
    return {
        "orders": 0,
        "fills": 0,
        "rejects": 0,
        "resolved_fills": 0,
        "unresolved_fills": 0,
        "pnl_usd": 0.0,
    }


def _add_metric(row: dict[str, Any], *, status: str, resolved: bool, pnl: float) -> None:
    row["orders"] += 1
    if status == "FILLED":
        row["fills"] += 1
        if resolved:
            row["resolved_fills"] += 1
            row["pnl_usd"] = round(float(row["pnl_usd"]) + pnl, 6)
        else:
            row["unresolved_fills"] += 1
    elif status == "REJECTED":
        row["rejects"] += 1


def _score_orders(
    orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    *,
    start_ts: float,
    end_ts: float,
    wallet_filter: str = "",
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    scoped = build_pnl_truth(
        {"orders": orders},
        resolutions,
        start_ts=start_ts,
        end_ts=end_ts,
        receipt_costs=receipt_costs,
        actual_trade_costs=actual_trade_costs,
    )
    wallet_filter = wallet_filter.lower()
    if wallet_filter:
        events = [row for row in scoped.get("events", []) if str(row.get("source_wallet") or "") == wallet_filter]
        scoped = _truth_from_events(events)
    return _score_from_truth(scoped)


def _score_from_truth(scoped: dict[str, Any]) -> dict[str, Any]:
    return {
        "orders_in_scope": scoped["scope"]["orders_in_scope"],
        "newest_submitted_at": scoped["scope"]["latest_order_ts"],
        "status_counts": scoped["scope"]["status_counts"],
        "total": scoped["total"],
        "per_member": scoped["by_member"],
        "by_lane": scoped["by_lane"],
        "price_band_buckets": scoped["by_price_band"],
        "per_window_pnl_histogram": _per_window_pnl_histogram(scoped.get("events", [])),
    }


def _standing_price_band_evidence(lifetime_truth: dict[str, Any]) -> dict[str, Any]:
    """Publish computed whole-book money, never frozen band-money literals."""

    total = lifetime_truth.get("total") if isinstance(lifetime_truth.get("total"), dict) else {}
    cost = num(total.get("cost_usd"))
    realized = num(total.get("pnl_usd"))
    return {
        "scope": "whole_book_resolved_fills",
        "source": "computed_lifetime_pnl_truth.total",
        "cost_usd": round(cost, 6),
        "payout_usd": round(num(total.get("payout_usd")), 6),
        "pnl_usd_realized": round(realized, 6),
        "roi_pct_realized": round(100.0 * realized / cost, 6) if cost else 0.0,
        "resolved_fills": int(num(total.get("resolved_fills"))),
        "decision_rule": (
            "no price-band routing or size change without positive realized, "
            "day-bounded and concentration-robust evidence"
        ),
    }


def _actual_basis_coverage(truth: dict[str, Any]) -> dict[str, Any]:
    events = [
        row
        for row in truth.get("events", [])
        if isinstance(row, dict)
        and str(row.get("status") or "") == "FILLED"
        and bool(row.get("resolved"))
    ]
    joined = sum(1 for row in events if str(row.get("cost_basis_source") or "") == "actual_trade_record")
    missing = max(0, len(events) - joined)
    return {
        "basis": "actual_trade_record",
        "fallback_basis": "response_filled_size_usd",
        "joined": joined,
        "missing": missing,
        "total_resolved_fills": len(events),
        "coverage_pct": round(100.0 * joined / len(events), 6) if events else 0.0,
        "source": "same_generation_actual_basis_day_events",
    }


def _basis_split_delta(response_score: dict[str, Any], actual_score: dict[str, Any]) -> float:
    response_pnl = float(((response_score.get("total") or {}).get("pnl_usd")) or 0.0)
    actual_pnl = float(((actual_score.get("total") or {}).get("pnl_usd")) or 0.0)
    return round(actual_pnl - response_pnl, 6)


def _day_pnl_resolution_split(truth: dict[str, Any]) -> dict[str, Any]:
    events = [row for row in truth.get("events", []) if isinstance(row, dict)]
    filled = [row for row in events if str(row.get("status") or "").upper() == "FILLED"]
    resolved = [row for row in filled if bool(row.get("resolved"))]
    unresolved = [row for row in filled if not bool(row.get("resolved"))]
    realized_pnl = round(sum(float(row.get("pnl_usd") or 0.0) for row in resolved), 6)
    total_pnl = round(float(((truth.get("total") or {}).get("pnl_usd")) or 0.0), 6)
    open_mark_pnl = round(total_pnl - realized_pnl, 6)
    open_cost = round(sum(float(row.get("cost_usd") or 0.0) for row in unresolved), 6)
    open_shares = round(sum(float(row.get("shares") or 0.0) for row in unresolved), 6)
    bounds = unresolved_position_bounds(truth)
    submitted = sorted(str(row.get("submitted_at") or "") for row in unresolved if row.get("submitted_at"))
    return {
        "flow_stage": "LIVE/DEFEND",
        "reporting_only": True,
        "basis": truth.get("cost_basis_source") or "response_filled_size_usd",
        "status": "OPEN_MARK_ACTIVE" if unresolved else "NO_OPEN_MARK",
        "total_day_pnl_usd": total_pnl,
        "realized_closed_pnl_usd": realized_pnl,
        "open_mark_pnl_usd": open_mark_pnl,
        "realized_closed_fills": len(resolved),
        "unresolved_open_fills": len(unresolved),
        "open_cost_usd": open_cost,
        "open_shares": open_shares,
        "unresolved_position_value_bounds_usd": bounds.get("position_value_bounds_usd"),
        "max_open_payout_usd": bounds.get("max_payout_usd"),
        "latest_unresolved_submitted_at": submitted[-1] if submitted else None,
        "unresolved_market_slugs_sample": sorted(
            {str(row.get("market_slug") or "") for row in unresolved if row.get("market_slug")}
        )[:10],
        "rule": (
            "realized_closed_pnl_usd is resolved filled orders only; open_mark_pnl_usd is the "
            "current scorecard contribution from unresolved fills, with exposure bounds shown separately"
        ),
    }


def _actual_cost_record(
    actual_event: dict[str, Any],
    actual_trade_costs: dict[str, Any] | None,
) -> dict[str, Any]:
    costs = actual_trade_costs or {}
    order_id = str(actual_event.get("order_id") or "").strip().lower()
    matched_key = str(actual_event.get("cost_basis_key") or "").strip().lower()
    for key in (matched_key, order_id):
        record = costs.get(key)
        if isinstance(record, dict):
            return record
    return {}


def _actual_basis_join_row(
    actual_event: dict[str, Any],
    response_event: dict[str, Any],
    actual_trade_costs: dict[str, Any] | None,
) -> dict[str, Any]:
    record = _actual_cost_record(actual_event, actual_trade_costs)
    order_id = str(actual_event.get("order_id") or "")
    matched_key = str(actual_event.get("cost_basis_key") or "")
    join_key = str(record.get("join_key") or record.get("tx") or matched_key or order_id)
    actual_cost = float(actual_event.get("cost_usd") or 0.0)
    response_cost = float(response_event.get("cost_usd") or actual_event.get("intended_cost_usd") or 0.0)
    improvement = round(response_cost - actual_cost, 6)
    if matched_key and matched_key == order_id:
        matched_key_source = "order_id_alias"
    elif matched_key and matched_key == join_key:
        matched_key_source = "join_key"
    else:
        matched_key_source = "unknown"
    if join_key and join_key == order_id:
        join_key_label_status = "JOIN_KEY_EQUALS_ORDER_ID"
    else:
        join_key_label_status = "JOIN_KEY_DISTINCT_FROM_ORDER_ID"
    return {
        "join_key": join_key,
        "join_key_source": str(record.get("join_key_source") or "unknown"),
        "matched_cost_key": matched_key,
        "matched_cost_key_source": matched_key_source,
        "join_key_label_status": join_key_label_status,
        "order_id": order_id,
        "actual_cost_usd": round(actual_cost, 6),
        "response_cost_usd": round(response_cost, 6),
        "response_minus_actual_cost_usd": improvement,
    }


def _actual_basis_spot_audit(
    response_truth: dict[str, Any],
    actual_truth: dict[str, Any],
    actual_trade_costs: dict[str, Any] | None = None,
    *,
    limit: int = 5,
) -> dict[str, Any]:
    response_by_order = {
        str(row.get("order_id") or ""): row
        for row in response_truth.get("events", [])
        if isinstance(row, dict) and str(row.get("order_id") or "")
    }
    actual_events = [
        row
        for row in actual_truth.get("events", [])
        if isinstance(row, dict)
        and str(row.get("status") or "") == "FILLED"
        and bool(row.get("resolved"))
        and str(row.get("cost_basis_source") or "") == "actual_trade_record"
    ]
    join_rows = []
    for actual in actual_events:
        order_id = str(actual.get("order_id") or "")
        response = response_by_order.get(order_id, {})
        join_rows.append(_actual_basis_join_row(actual, response, actual_trade_costs))
    join_key_counts = Counter(str(row.get("join_key") or "") for row in join_rows if row.get("join_key"))
    rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for row in join_rows:
        delta = round(float(row["actual_cost_usd"]) - float(row["response_cost_usd"]), 6)
        if row["join_key_label_status"] == "JOIN_KEY_EQUALS_ORDER_ID":
            classification = "join_key_equals_order_id"
        elif float(row["actual_cost_usd"]) <= 0.0 or float(row["response_cost_usd"]) <= 0.0:
            classification = "missing_leg"
        elif join_key_counts.get(str(row.get("join_key") or ""), 0) > 1:
            classification = "double_join"
        elif abs(delta) <= 0.000001:
            classification = "exact_match"
        elif delta < 0.0:
            classification = "price_improvement"
        else:
            classification = "fee_or_price_worse"
        counts[classification] += 1
        row["actual_minus_response_usd"] = delta
        row["classification"] = classification
        rows.append(row)
        if len(rows) >= int(limit):
            break
    return {
        "flow_stage": "LIVE/SELF-DEV",
        "reporting_only": True,
        "sample_size": int(limit),
        "sampled_joined_tx_groups": len(rows),
        "classification_counts": dict(sorted(counts.items())),
        "rows": rows,
    }


def _basis_split_decomposition(
    response_truth: dict[str, Any],
    actual_truth: dict[str, Any],
    actual_trade_costs: dict[str, Any] | None,
    *,
    basis_split_delta_usd: float,
) -> dict[str, Any]:
    response_by_order = {
        str(row.get("order_id") or ""): row
        for row in response_truth.get("events", [])
        if isinstance(row, dict) and str(row.get("order_id") or "")
    }
    rows: list[dict[str, Any]] = []
    for actual in actual_truth.get("events", []):
        if not isinstance(actual, dict):
            continue
        if str(actual.get("status") or "") != "FILLED" or not actual.get("resolved"):
            continue
        if str(actual.get("cost_basis_source") or "") != "actual_trade_record":
            continue
        order_id = str(actual.get("order_id") or "")
        response = response_by_order.get(order_id, {})
        rows.append(_actual_basis_join_row(actual, response, actual_trade_costs))
    rows.sort(key=lambda row: (str(row.get("join_key") or ""), str(row.get("order_id") or "")))
    joined_sum = round(sum(float(row.get("response_minus_actual_cost_usd") or 0.0) for row in rows), 6)
    remainder = round(float(basis_split_delta_usd) - joined_sum, 6)
    join_key_sources = Counter(str(row.get("join_key_source") or "unknown") for row in rows)
    match_key_sources = Counter(str(row.get("matched_cost_key_source") or "unknown") for row in rows)
    return {
        "flow_stage": "LIVE/SELF-DEV",
        "reporting_only": True,
        "basis_split_delta_usd": round(float(basis_split_delta_usd), 6),
        "joined_group_count": len(rows),
        "sum_joined_improvement_usd": joined_sum,
        "remainder_usd": remainder,
        "assertion": "PASS_WITHIN_0.01" if abs(remainder) <= 0.01 else "REMAINDER_NAMED",
        "remainder_term": None if abs(remainder) <= 0.01 else "basis_split_not_explained_by_joined_cost_improvement",
        "join_key_schema": {
            "source_field": "wallet_copy_today_fill_cash_diff.rows[].tx",
            "source_field_origin": "data-api trades transactionHash via scripts/report_today_fill_cash_diff.py",
            "loader_alias": "order_ids[0]",
            "loader_alias_reason": "score_order probes order_id before tx in _receipt_cost_keys; alias is not the independent join key",
            "join_key_source_counts": dict(sorted(join_key_sources.items())),
            "matched_cost_key_source_counts": dict(sorted(match_key_sources.items())),
        },
        "top_improvements": sorted(
            rows,
            key=lambda row: float(row.get("response_minus_actual_cost_usd") or 0.0),
            reverse=True,
        )[:5],
        "rows": rows,
    }


def _window_pnl_bucket(pnl_usd: float) -> str:
    if pnl_usd <= -5.0:
        return "lte_-5"
    if pnl_usd <= -1.0:
        return "-5_to_-1"
    if pnl_usd < 0.0:
        return "-1_to_0"
    if pnl_usd == 0.0:
        return "zero"
    if pnl_usd < 1.0:
        return "0_to_1"
    if pnl_usd < 5.0:
        return "1_to_5"
    return "gte_5"


def _per_window_pnl_histogram(events: list[dict[str, Any]]) -> dict[str, Any]:
    buckets = Counter({key: 0 for key in ("lte_-5", "-5_to_-1", "-1_to_0", "zero", "0_to_1", "1_to_5", "gte_5")})
    resolved_by_slug: dict[str, dict[str, Any]] = {}
    unresolved_filled_slugs: set[str] = set()
    submitted_slugs: set[str] = set()
    filled_slugs: set[str] = set()
    for event in events:
        if not isinstance(event, dict):
            continue
        slug = str(event.get("market_slug") or "")
        if not slug.startswith("btc-updown-5m-"):
            continue
        submitted_slugs.add(slug)
        status = str(event.get("status") or "").upper()
        if status != "FILLED":
            continue
        filled_slugs.add(slug)
        if not event.get("resolved"):
            unresolved_filled_slugs.add(slug)
            continue
        row = resolved_by_slug.setdefault(
            slug,
            {
                "market_slug": slug,
                "resolved_fills": 0,
                "pnl_usd": 0.0,
            },
        )
        row["resolved_fills"] += 1
        row["pnl_usd"] = round(float(row["pnl_usd"]) + float(event.get("pnl_usd") or 0.0), 6)
    rows = []
    for row in resolved_by_slug.values():
        pnl = round(float(row.get("pnl_usd") or 0.0), 6)
        bucket = _window_pnl_bucket(pnl)
        buckets[bucket] += 1
        rows.append(
            {
                "market_slug": row["market_slug"],
                "resolved_fills": int(row.get("resolved_fills") or 0),
                "pnl_usd": pnl,
                "bucket": bucket,
            }
        )
    rows.sort(key=lambda row: (float(row.get("pnl_usd") or 0.0), str(row.get("market_slug") or "")))
    return {
        "flow_stage": "LIVE/MEASURE",
        "reporting_only": True,
        "gate_use_allowed": False,
        "source": "score_order resolved live fills grouped by BTC5M market_slug",
        "submitted_windows": len(submitted_slugs),
        "filled_windows": len(filled_slugs),
        "resolved_windows": len(rows),
        "unresolved_filled_windows": len(unresolved_filled_slugs),
        "positive_windows": sum(1 for row in rows if float(row.get("pnl_usd") or 0.0) > 0.0),
        "negative_windows": sum(1 for row in rows if float(row.get("pnl_usd") or 0.0) < 0.0),
        "zero_windows": sum(1 for row in rows if float(row.get("pnl_usd") or 0.0) == 0.0),
        "min_window_pnl_usd": rows[0]["pnl_usd"] if rows else None,
        "bucket_counts": dict(sorted(buckets.items())),
        "worst_windows": rows[:5],
        "top_windows": list(reversed(rows[-5:])),
    }


def _truth_from_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    total = _empty_metric()
    by_member: dict[str, dict[str, Any]] = defaultdict(_empty_metric)
    by_lane: dict[str, dict[str, Any]] = defaultdict(_empty_metric)
    by_band: dict[str, dict[str, Any]] = defaultdict(_empty_metric)
    status_counts: Counter[str] = Counter()
    latest = ""
    for event in events:
        status = str(event.get("status") or "")
        status_counts[status] += 1
        latest = max(latest, str(event.get("submitted_at") or ""))
        resolved = bool(event.get("resolved"))
        pnl = float(event.get("pnl_usd") or 0.0)
        _add_metric(total, status=status, resolved=resolved, pnl=pnl)
        _add_metric(by_member[str(event.get("source_wallet") or "unknown")], status=status, resolved=resolved, pnl=pnl)
        _add_metric(by_lane[str(event.get("lane") or "unknown")], status=status, resolved=resolved, pnl=pnl)
        _add_metric(by_band[str(event.get("price_bucket") or "unknown")], status=status, resolved=resolved, pnl=pnl)
    return {
        "scope": {"orders_in_scope": len(events), "latest_order_ts": latest, "status_counts": dict(sorted(status_counts.items()))},
        "total": _finalize_metric(total),
        "by_member": {key: _finalize_metric(value) for key, value in sorted(by_member.items())},
        "by_lane": {key: _finalize_metric(value) for key, value in sorted(by_lane.items())},
        "by_price_band": {key: _finalize_metric(value) for key, value in sorted(by_band.items())},
        "events": events,
    }


def _finalize_metric(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out["pnl_usd"] = round(float(out.get("pnl_usd") or 0.0), 6)
    fills = int(out.get("fills") or 0)
    out["resolved_fill_rate_pct"] = round((100.0 * int(out.get("resolved_fills") or 0) / fills), 6) if fills else 0.0
    return out


def _btc5m_window_start_from_slug(slug: str) -> float | None:
    text = str(slug or "")
    if not text.startswith("btc-updown-5m-"):
        return None
    marker = text.rsplit("-", 1)[-1]
    return float(marker) if marker.isdigit() else None


def _seconds_to_close_bucket(seconds_to_close: float | None) -> str:
    if seconds_to_close is None:
        return "unknown"
    value = float(seconds_to_close)
    if value < 0:
        return "after_close"
    if value < 30:
        return "000_030"
    if value < 60:
        return "030_060"
    if value < 120:
        return "060_120"
    return "rest"


def _late_window_empty_bucket() -> dict[str, Any]:
    return {
        "fills": 0,
        "resolved_fills": 0,
        "unresolved_fills": 0,
        "cost_usd": 0.0,
        "pnl_usd": 0.0,
        "roi_pct": 0.0,
    }


def _late_window_finalize_bucket(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out["cost_usd"] = round(float(out.get("cost_usd") or 0.0), 6)
    out["pnl_usd"] = round(float(out.get("pnl_usd") or 0.0), 6)
    out["roi_pct"] = round(100.0 * out["pnl_usd"] / out["cost_usd"], 6) if out["cost_usd"] else 0.0
    return out


def _order_metadata(order: dict[str, Any]) -> dict[str, Any]:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
    return metadata


def _order_copy_model(order: dict[str, Any]) -> str:
    copy_model = str(order.get("copy_model") or "").strip()
    if copy_model:
        return copy_model
    return str(_order_metadata(order).get("copy_model") or "").strip()


def _order_inventory_metadata(order: dict[str, Any]) -> dict[str, Any]:
    if isinstance(order.get("wallet_copy_inventory"), dict):
        return order["wallet_copy_inventory"]
    metadata = _order_metadata(order)
    inventory = metadata.get("inventory_v2")
    return inventory if isinstance(inventory, dict) else {}


def _order_drip_metadata(order: dict[str, Any]) -> dict[str, Any]:
    if isinstance(order.get("wallet_copy_drip"), dict):
        return order["wallet_copy_drip"]
    metadata = _order_metadata(order)
    drip = metadata.get("inventory_v3_drip")
    if isinstance(drip, dict):
        return drip
    inventory = metadata.get("inventory_v2")
    if isinstance(inventory, dict) and isinstance(inventory.get("inventory_v3_drip"), dict):
        return inventory["inventory_v3_drip"]
    return {}


def _order_signal_tier(order: dict[str, Any]) -> str:
    drip = _order_drip_metadata(order)
    if drip.get("signal_tier"):
        return str(drip.get("signal_tier") or "")
    inventory = _order_inventory_metadata(order)
    if inventory.get("signal_tier"):
        return str(inventory.get("signal_tier") or "")
    metadata = _order_metadata(order)
    return str(order.get("signal_tier") or metadata.get("signal_tier") or "").strip()


def _late_window_inventory_copy_order(order: dict[str, Any]) -> bool:
    return _order_copy_model(order) in INVENTORY_LIKE_COPY_MODELS


def _late_window_cohort(
    orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    *,
    start_ts: float,
    end_ts: float,
    experiment_start: str = LATE_WINDOW_EXPERIMENT_START,
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    buckets = {key: _late_window_empty_bucket() for key in ("000_030", "030_060", "060_120", "rest", "unknown", "after_close")}
    experiment_start_ts = _parse_utc_ts(experiment_start) or 0.0
    experiment_bucket = _late_window_empty_bucket()
    rows: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        if not _late_window_inventory_copy_order(order):
            continue
        event = score_order(
            order,
            resolutions,
            receipt_costs=receipt_costs,
            actual_trade_costs=actual_trade_costs,
        )
        if str(event.get("status") or "") != "FILLED":
            continue
        ts = event.get("ts")
        if ts is None or float(ts) < start_ts or float(ts) >= end_ts:
            continue
        window_start = _btc5m_window_start_from_slug(str(event.get("market_slug") or ""))
        seconds_to_close = None if window_start is None else window_start + 300.0 - float(ts)
        bucket_name = _seconds_to_close_bucket(seconds_to_close)
        bucket = buckets[bucket_name]
        bucket["fills"] += 1
        resolved = bool(event.get("resolved"))
        pnl = float(event.get("pnl_usd") or 0.0)
        cost = float(event.get("cost_usd") or 0.0)
        if resolved:
            bucket["resolved_fills"] += 1
            bucket["cost_usd"] = round(float(bucket["cost_usd"]) + cost, 6)
            bucket["pnl_usd"] = round(float(bucket["pnl_usd"]) + pnl, 6)
        else:
            bucket["unresolved_fills"] += 1
        if bucket_name == "030_060" and float(ts) >= experiment_start_ts:
            experiment_bucket["fills"] += 1
            if resolved:
                experiment_bucket["resolved_fills"] += 1
                experiment_bucket["cost_usd"] = round(float(experiment_bucket["cost_usd"]) + cost, 6)
                experiment_bucket["pnl_usd"] = round(float(experiment_bucket["pnl_usd"]) + pnl, 6)
            else:
                experiment_bucket["unresolved_fills"] += 1
        rows.append(
            {
                "submitted_at": event.get("submitted_at"),
                "market_slug": event.get("market_slug"),
                "seconds_to_close_s": None if seconds_to_close is None else round(float(seconds_to_close), 6),
                "bucket": bucket_name,
                "resolved": resolved,
                "pnl_usd": round(pnl, 6) if resolved else 0.0,
            }
        )
    finalized = {key: _late_window_finalize_bucket(value) for key, value in buckets.items()}
    experiment = _late_window_finalize_bucket(experiment_bucket)
    return {
        "flow_stage": "LIVE/MEASURE",
        "scope": "inventory_like_copy_models",
        "experiment": {
            "start_iso": experiment_start,
            "cohort_bucket": "030_060",
            "tripwire_pnl_usd_lte": LATE_WINDOW_EXPERIMENT_TRIPWIRE_USD,
            "tripwire_triggered": float(experiment.get("pnl_usd") or 0.0) <= LATE_WINDOW_EXPERIMENT_TRIPWIRE_USD,
            "verdict_due": "2026-07-07T00:05:00Z",
        },
        "buckets": finalized,
        "experiment_cohort": experiment,
        "rows": rows,
    }


def _automation_drift() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    auto_root = Path.home() / ".codex" / "automations"
    for toml in sorted(auto_root.glob("*/automation.toml")):
        text = toml.read_text(errors="replace")
        prompt_line = next((line for line in text.splitlines() if line.startswith("prompt = ")), "")
        pointer = POINTER_PHRASE in prompt_line and (
            "docs/agents/HEARTBEAT_PROMPT.md" in prompt_line or "docs/agents/RESEARCH_PROMPT.md" in prompt_line
        )
        rows.append(
            {
                "kind": "codex_app_automation",
                "name": toml.parent.name,
                "path": str(toml),
                "pointer_status": "POINTER" if pointer else "CONTENT_DEFECT",
            }
        )
    for script in (
        ROOT / "scripts" / "codex_heartbeat.sh",
        ROOT / "scripts" / "agent_loop.sh",
        ROOT / "scripts" / "fable_pulse.sh",
    ):
        text = script.read_text(errors="replace")
        if script.name == "fable_pulse.sh":
            status = (
                "REPO_TRACKED_GATEWAY_PROMPT"
                if "./scripts/ask_fable.sh" in text and "PROACTIVE STEERING PULSE" in text
                else "CONTENT_DEFECT"
            )
        else:
            status = "POINTER" if HEARTBEAT_POINTER in text else "CONTENT_DEFECT"
        rows.append(
            {
                "kind": "local_runner",
                "name": script.name,
                "path": str(script),
                "pointer_status": status,
            }
        )
    rows.extend(_launchd_rows())
    rows.append(_crontab_row())
    return rows


def _launchd_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    launch_dir = Path.home() / "Library" / "LaunchAgents"
    for plist in sorted(launch_dir.glob("com.belavarga.polymarket*.plist")):
        status = "COMMAND_ONLY"
        try:
            with plist.open("rb") as fh:
                data = plistlib.load(fh)
            args = data.get("ProgramArguments") if isinstance(data, dict) else []
            text = " ".join(str(part) for part in args)
            if "codex_heartbeat.sh" in text:
                status = "POINTER_VIA_SCRIPT"
            elif "fable_pulse.sh" in text:
                status = "REPO_TRACKED_GATEWAY_PROMPT"
            elif "codex exec" in text and POINTER_PHRASE not in text:
                status = "CONTENT_DEFECT"
        except Exception as exc:  # pragma: no cover - defensive runtime audit
            status = f"INSPECT_ERROR:{type(exc).__name__}"
        rows.append({"kind": "launchd", "name": plist.name, "path": str(plist), "pointer_status": status})
    return rows


def _crontab_row() -> dict[str, Any]:
    try:
        proc = subprocess.run(["crontab", "-l"], text=True, capture_output=True, check=False)
        text = proc.stdout.strip()
    except Exception as exc:  # pragma: no cover - defensive runtime audit
        return {"kind": "cron", "name": "user-crontab", "path": "crontab -l", "pointer_status": f"INSPECT_ERROR:{type(exc).__name__}"}
    if not text:
        return {"kind": "cron", "name": "user-crontab", "path": "crontab -l", "pointer_status": "EMPTY"}
    pointer = POINTER_PHRASE in text
    return {"kind": "cron", "name": "user-crontab", "path": "crontab -l", "pointer_status": "POINTER" if pointer else "CONTENT_DEFECT"}


def _volume_kpi(
    guard_state_path: str,
    *,
    orders: list[dict[str, Any]] | None = None,
    start_ts: float = 0.0,
    end_ts: float = 0.0,
) -> dict[str, Any]:
    filled_slugs: set[str] = set()
    submitted_slugs: set[str] = set()
    policy_cap_refusal_slugs: set[str] = set()
    pre_submit_refusal_slugs: set[str] = set()
    for order in orders or []:
        if not isinstance(order, dict):
            continue
        order_ts_value = order_ts(order)
        if order_ts_value is None or order_ts_value < start_ts or order_ts_value >= end_ts:
            continue
        slug = str(order.get("market_slug") or "")
        if not slug.startswith("btc-updown-5m-"):
            continue
        status = str(order.get("final_status") or order.get("status") or "").upper()
        trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        error_class = str(order.get("error_class") or trade_result.get("error_class") or "")
        venue_order_id = str(trade_result.get("order_id") or "").strip()
        if error_class in PRE_SUBMIT_REFUSAL_CLASSES and not venue_order_id:
            pre_submit_refusal_slugs.add(slug)
        if error_class == "maker_min_share_bump_exceeds_policy_cap" and not venue_order_id:
            policy_cap_refusal_slugs.add(slug)
        if status == "REJECTED" and not venue_order_id:
            pre_submit_refusal_slugs.add(slug)
        if venue_order_id:
            submitted_slugs.add(slug)
        if status == "FILLED":
            filled_slugs.add(slug)

    guard = load_json(guard_state_path, default={})
    participation = guard.get("window_participation") if isinstance(guard.get("window_participation"), dict) else {}
    rollups = participation.get("window_rollups") if isinstance(participation.get("window_rollups"), list) else []
    windows: dict[str, dict[str, Any]] = {}
    for row in rollups:
        if not isinstance(row, dict):
            continue
        key = str(row.get("market_slug") or row.get("window_start_s") or "")
        if not key:
            continue
        bucket = windows.setdefault(
            key,
            {
                "market_slug": str(row.get("market_slug") or ""),
                "window_start_s": row.get("window_start_s"),
                "wallet_eligible_orders": 0,
                "our_submits": 0,
                "our_fills": 0,
                "pending_market_lifecycle": False,
                "missed_active_window": False,
                "skip_reasons": Counter(),
            },
        )
        bucket["wallet_eligible_orders"] += int(row.get("wallet_eligible_orders") or 0)
        bucket["our_submits"] += int(row.get("our_submits") or 0)
        bucket["our_fills"] += int(row.get("our_fills") or 0)
        bucket["pending_market_lifecycle"] = bool(bucket["pending_market_lifecycle"] or row.get("miss_pending_market_lifecycle"))
        bucket["missed_active_window"] = bool(bucket["missed_active_window"] or row.get("missed_active_window"))
        reasons = row.get("dominant_skip_reason_counts") if isinstance(row.get("dominant_skip_reason_counts"), dict) else {}
        if reasons:
            bucket["skip_reasons"].update({str(k): int(v or 0) for k, v in reasons.items()})
        elif row.get("dominant_skip_reason"):
            bucket["skip_reasons"][str(row.get("dominant_skip_reason"))] += 1

    taxonomy: Counter[str] = Counter()
    missed_window_attribution: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    traded = 0
    guarded_late_window = 0
    for key, row in sorted(windows.items(), key=lambda item: (float(item[1].get("window_start_s") or 0), item[0])):
        has_trade = int(row["our_submits"]) > 0 or int(row["our_fills"]) > 0
        if has_trade:
            traded += 1
            reason = "traded"
        elif row["pending_market_lifecycle"]:
            reason = "pending_market_lifecycle"
        elif row["skip_reasons"].get("inventory_late_window_guard"):
            guarded_late_window += 1
            reason = "guarded_late_window"
        elif row["missed_active_window"] or int(row["wallet_eligible_orders"]) > 0:
            reason = "signal_but_missed"
        elif row["skip_reasons"]:
            reason = "filtered_by_band"
        else:
            reason = "no_signal"
        taxonomy[reason] += 1
        if int(row["our_fills"]) > 0:
            attribution = "filled"
        elif str(row.get("market_slug") or key) in policy_cap_refusal_slugs:
            attribution = "policy_cap_refusal"
        elif str(row.get("market_slug") or key) in pre_submit_refusal_slugs:
            attribution = "pre_submit_refusal"
        elif int(row["our_submits"]) > 0:
            attribution = "submitted_not_filled"
        elif row["missed_active_window"] or int(row["wallet_eligible_orders"]) > 0 or row["skip_reasons"]:
            attribution = "guard_reject"
        else:
            attribution = "no_copy_signal"
        if attribution != "filled":
            missed_window_attribution[attribution] += 1
        rows.append(
            {
                "market_slug": row["market_slug"],
                "window_start_s": row["window_start_s"],
                "wallet_eligible_orders": row["wallet_eligible_orders"],
                "our_submits": row["our_submits"],
                "our_fills": row["our_fills"],
                "empty_window_reason": reason,
                "missed_window_attribution": attribution,
                "skip_reasons": dict(sorted(row["skip_reasons"].items())),
            }
        )
    canonical_attribution: Counter[str] = Counter(
        {
            "filled": 0,
            "submitted_not_filled": 0,
            "guard_reject": 0,
            "policy_cap_refusal": 0,
            "pre_submit_refusal": 0,
            "no_copy_signal": 0,
        }
    )
    canonical_rows: list[dict[str, Any]] = []
    day_start = int(start_ts // 300) * 300 if start_ts > 0 else 0
    for offset in range(BTC_5M_WINDOWS_PER_DAY):
        window_start_s = day_start + offset * 300
        slug = f"btc-updown-5m-{window_start_s}"
        row = windows.get(slug) or windows.get(str(float(window_start_s))) or windows.get(str(window_start_s)) or {}
        if slug in filled_slugs or int(row.get("our_fills") or 0) > 0:
            attribution = "filled"
        elif slug in policy_cap_refusal_slugs:
            attribution = "policy_cap_refusal"
        elif slug in pre_submit_refusal_slugs:
            attribution = "pre_submit_refusal"
        elif slug in submitted_slugs or int(row.get("our_submits") or 0) > 0:
            attribution = "submitted_not_filled"
        elif row and (row.get("missed_active_window") or int(row.get("wallet_eligible_orders") or 0) > 0 or row.get("skip_reasons")):
            attribution = "guard_reject"
        else:
            attribution = "no_copy_signal"
        canonical_attribution[attribution] += 1
        canonical_rows.append(
            {
                "market_slug": slug,
                "window_start_s": window_start_s,
                "attribution": attribution,
                "observed_guard_rollup": bool(row),
            }
        )
    scope_total = len(windows)
    ex_guard_total = max(0, scope_total - guarded_late_window)
    canonical_total = BTC_5M_WINDOWS_PER_DAY
    canonical_filled = len(filled_slugs)
    canonical_submitted = len(submitted_slugs)
    return {
        "source": guard_state_path,
        "denominator": "canonical_daily_btc_5m_windows",
        "windows_total": canonical_total,
        "windows_traded": canonical_filled,
        "windows_empty": max(0, canonical_total - canonical_filled),
        "windows_traded_pct": round((100.0 * canonical_filled / canonical_total), 6),
        "windows_submitted": canonical_submitted,
        "windows_submitted_pct": round((100.0 * canonical_submitted / canonical_total), 6),
        "canonical_daily": {
            "definition": "distinct BTC-5m market_slug with at least one FILLED live ledger order in the UTC day",
            "denominator_windows": canonical_total,
            "windows_filled": canonical_filled,
            "windows_filled_pct": round((100.0 * canonical_filled / canonical_total), 6),
            "windows_submitted": canonical_submitted,
            "windows_submitted_pct": round((100.0 * canonical_submitted / canonical_total), 6),
        },
        "scope_local": {
            "windows_total": scope_total,
            "windows_traded": traded,
            "windows_traded_pct": round((100.0 * traded / scope_total), 6) if scope_total else 0.0,
        },
        "coverage_gross": {
            "windows_total": scope_total,
            "windows_traded": traded,
            "windows_traded_pct": round((100.0 * traded / scope_total), 6) if scope_total else 0.0,
        },
        "coverage_ex_guard": {
            "windows_total": ex_guard_total,
            "guarded_late_window": guarded_late_window,
            "windows_traded": traded,
            "windows_traded_pct": round((100.0 * traded / ex_guard_total), 6) if ex_guard_total else 0.0,
        },
        "empty_window_taxonomy": dict(sorted(taxonomy.items())),
        "missed_window_attribution": {
            "scope": "full_day_btc_5m_denominator",
            "denominator_windows": canonical_total,
            "categories": {
                key: int(canonical_attribution[key])
                for key in (
                    "filled",
                    "submitted_not_filled",
                    "guard_reject",
                    "policy_cap_refusal",
                    "pre_submit_refusal",
                    "no_copy_signal",
                )
            },
            "observed_rollup_non_filled_categories": dict(sorted(missed_window_attribution.items())),
            "definition": (
                "Each canonical BTC-5m UTC day window is classified exactly once; venue submits require "
                "a non-empty order_id and pre-submit maker policy-cap refusals are policy_cap_refusal."
            ),
            "rows": canonical_rows,
        },
        "rows": rows,
    }


def _order_entry_price(order: dict[str, Any]) -> float:
    shares = num(order.get("requested_shares"), 0.0)
    if shares <= 0:
        shares = num(order.get("filled_shares"), 0.0)
    size_usd = num(order.get("requested_size_usd"), 0.0)
    if size_usd <= 0:
        size_usd = num(order.get("filled_size_usd"), 0.0)
    if size_usd <= 0:
        size_usd = num(order.get("response_filled_size_usd"), 0.0)
    if shares > 0 and size_usd > 0:
        return size_usd / shares
    return num(order.get("limit_price"), 0.0)


def _execution_model_kpi(
    orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    guard_state: dict[str, Any],
    *,
    start_ts: float,
    end_ts: float,
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    copy_models: Counter[str] = Counter()
    filled_by_model: Counter[str] = Counter()
    submitted_slugs: set[str] = set()
    filled_slugs: set[str] = set()
    drip_slugs: set[str] = set()
    strong_slugs: set[str] = set()
    submitted_orders = 0
    filled_orders = 0
    drip_orders = 0
    drip_fills = 0
    strong_orders = 0
    strong_fills = 0
    strong_resolved_fills = 0
    strong_cost = 0.0
    strong_pnl = 0.0
    entry_minus_vwap_sum = 0.0
    entry_minus_vwap_count = 0

    for order in orders:
        if not isinstance(order, dict):
            continue
        ts = order_ts(order)
        if ts is None or ts < start_ts or ts >= end_ts:
            continue
        slug = str(order.get("market_slug") or "")
        if not slug.startswith("btc-updown-5m-"):
            continue
        copy_model = _order_copy_model(order) or "unknown"
        status = str(order.get("final_status") or order.get("status") or "").upper()
        submitted_orders += 1
        submitted_slugs.add(slug)
        copy_models[copy_model] += 1
        signal_tier = _order_signal_tier(order)
        if copy_model == "drip":
            drip_orders += 1
            drip_slugs.add(slug)
            inventory = _order_inventory_metadata(order)
            vwap = num(inventory.get("source_inventory_vwap"), 0.0)
            entry = _order_entry_price(order)
            if entry > 0 and vwap > 0:
                entry_minus_vwap_sum += entry - vwap
                entry_minus_vwap_count += 1
        is_strong = copy_model == "drip" and signal_tier == "strong"
        if is_strong:
            strong_orders += 1
            strong_slugs.add(slug)
        if status != "FILLED":
            continue
        filled_orders += 1
        filled_slugs.add(slug)
        filled_by_model[copy_model] += 1
        if copy_model == "drip":
            drip_fills += 1
        if is_strong:
            strong_fills += 1
            event = score_order(
                order,
                resolutions,
                receipt_costs=receipt_costs,
                actual_trade_costs=actual_trade_costs,
            )
            if bool(event.get("resolved")):
                strong_resolved_fills += 1
                strong_cost += float(event.get("cost_usd") or 0.0)
                strong_pnl += float(event.get("pnl_usd") or 0.0)

    drip_stop_saves = 0
    participation = guard_state.get("window_participation") if isinstance(guard_state.get("window_participation"), dict) else {}
    rollups = participation.get("window_rollups") if isinstance(participation.get("window_rollups"), list) else []
    for row in rollups:
        if not isinstance(row, dict):
            continue
        reasons = row.get("dominant_skip_reason_counts") if isinstance(row.get("dominant_skip_reason_counts"), dict) else {}
        if reasons:
            drip_stop_saves += int(reasons.get("drip_residual_gap_below_min_tranche") or 0)
        elif str(row.get("dominant_skip_reason") or "") == "drip_residual_gap_below_min_tranche":
            drip_stop_saves += 1

    avg_entry_vs_vwap = (
        round(entry_minus_vwap_sum / entry_minus_vwap_count, 9)
        if entry_minus_vwap_count
        else 0.0
    )
    return {
        "flow_stage": "LIVE/MEASURE",
        "copy_model_counts": dict(sorted(copy_models.items())),
        "filled_by_copy_model": dict(sorted(filled_by_model.items())),
        "submitted_orders": submitted_orders,
        "filled_orders": filled_orders,
        "submitted_windows": len(submitted_slugs),
        "filled_windows": len(filled_slugs),
        "orders_per_submitted_window": round(submitted_orders / len(submitted_slugs), 6) if submitted_slugs else 0.0,
        "orders_per_filled_window": round(filled_orders / len(filled_slugs), 6) if filled_slugs else 0.0,
        "fill_rate_pct": round(100.0 * filled_orders / submitted_orders, 6) if submitted_orders else 0.0,
        "drip": {
            "orders": drip_orders,
            "fills": drip_fills,
            "submitted_windows": len(drip_slugs),
            "orders_per_submitted_window": round(drip_orders / len(drip_slugs), 6) if drip_slugs else 0.0,
            "fill_rate_pct": round(100.0 * drip_fills / drip_orders, 6) if drip_orders else 0.0,
            "avg_entry_minus_source_vwap": avg_entry_vs_vwap,
            "entry_vwap_sample": entry_minus_vwap_count,
            "drip_stop_saves": drip_stop_saves,
        },
        "strong_tier": {
            "orders": strong_orders,
            "fills": strong_fills,
            "submitted_windows": len(strong_slugs),
            "resolved_fills": strong_resolved_fills,
            "resolved_cost_usd": round(strong_cost, 6),
            "resolved_pnl_usd": round(strong_pnl, 6),
            "resolved_roi_pct": round(100.0 * strong_pnl / strong_cost, 6) if strong_cost else 0.0,
            "max_windows_per_day": 6,
            "max_concurrent_windows": 2,
        },
    }


def _active_set_roster(guard_state: dict[str, Any], *, current_only: bool = True) -> dict[str, Any]:
    active_set = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    historical_members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    members = [row for row in historical_members if isinstance(row, dict) and (not current_only or row.get("is_current_cycle_member") is True) and row.get("enabled") is not False]
    rows = []
    for member in members:
        if not isinstance(member, dict):
            continue
        rows.append(
            {
                "candidate_id": member.get("candidate_id"),
                "candidate_type": member.get("candidate_type"),
                "source_wallet": member.get("source_wallet"),
                "policy_id": member.get("policy_id"),
                "status": member.get("status"),
                "max_price": member.get("max_price"),
                "max_order_usd": member.get("max_order_usd"),
                "wallet_fraction": member.get("wallet_fraction"),
                "is_current_cycle_member": bool(member.get("is_current_cycle_member")),
                "enabled": member.get("enabled", True),
            }
        )
    return {
        "flow_stage": active_set.get("flow_stage") or "LIVE/PROMOTE/ROTATE",
        "generated_at": guard_state.get("generated_at"),
        "membership_source": "guard_runtime_active_artifact_current_cycle",
        "qualified_member_count": len(rows),
        "current_cycle_member_count": len(rows),
        "target_member_count_min": active_set.get("target_member_count_min"),
        "target_member_count_max": active_set.get("target_member_count_max"),
        "members": rows,
        "historical_qualified_members": [row for row in historical_members if isinstance(row, dict)],
    }


def _empty_expansion_member(candidate_id: str, source_wallet: str, policy_id: str) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "source_wallet": source_wallet,
        "policy_id": policy_id,
        "submit_attempts": 0,
        "fills": 0,
        "rejects": 0,
        "open_submitted_orders": 0,
        "resolved_fills": 0,
        "unresolved_fills": 0,
        "resolved_cost_usd": 0.0,
        "resolved_payout_usd": 0.0,
        "resolved_pnl_usd": 0.0,
        "first_submit_at": "",
        "latest_submit_at": "",
    }


def _finalize_expansion_member(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key in ("resolved_cost_usd", "resolved_payout_usd", "resolved_pnl_usd"):
        out[key] = round(float(out.get(key) or 0.0), 6)
    out["has_resolved_fill"] = int(out.get("resolved_fills") or 0) >= 1
    return out


def _expansion_cohort(
    orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    guard_state: dict[str, Any],
    *,
    start_ts: float,
    end_ts: float,
    cohort_start: str = EXPANSION_COHORT_START,
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cohort_start_ts = _parse_utc_ts(cohort_start) or start_ts
    effective_start_ts = max(start_ts, cohort_start_ts)
    roster = _active_set_roster(guard_state, current_only=False)
    members = []
    for member in roster.get("members") or []:
        if not isinstance(member, dict):
            continue
        candidate_id = str(member.get("candidate_id") or "")
        if not candidate_id.startswith("expansion_rank_"):
            continue
        wallet = str(member.get("source_wallet") or "").lower()
        if not wallet:
            continue
        members.append(
            _empty_expansion_member(
                candidate_id=candidate_id,
                source_wallet=wallet,
                policy_id=str(member.get("policy_id") or ""),
            )
        )
    by_wallet = {row["source_wallet"]: row for row in members}

    event_rows: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        event = score_order(
            order,
            resolutions,
            receipt_costs=receipt_costs,
            actual_trade_costs=actual_trade_costs,
        )
        ts = event.get("ts")
        if ts is None or float(ts) < effective_start_ts or float(ts) >= end_ts:
            continue
        wallet = str(event.get("source_wallet") or "").lower()
        row = by_wallet.get(wallet)
        if row is None:
            continue
        status = str(event.get("status") or "")
        row["submit_attempts"] += 1
        submitted_at = str(event.get("submitted_at") or "")
        if submitted_at:
            row["first_submit_at"] = min(row["first_submit_at"] or submitted_at, submitted_at)
            row["latest_submit_at"] = max(row["latest_submit_at"], submitted_at)
        if status == "FILLED":
            row["fills"] += 1
            if event.get("resolved"):
                row["resolved_fills"] += 1
                row["resolved_cost_usd"] = round(float(row["resolved_cost_usd"]) + float(event.get("cost_usd") or 0.0), 6)
                row["resolved_payout_usd"] = round(float(row["resolved_payout_usd"]) + float(event.get("payout_usd") or 0.0), 6)
                row["resolved_pnl_usd"] = round(float(row["resolved_pnl_usd"]) + float(event.get("pnl_usd") or 0.0), 6)
            else:
                row["unresolved_fills"] += 1
        elif status == "REJECTED":
            row["rejects"] += 1
        elif status == "SUBMITTED":
            row["open_submitted_orders"] += 1
        event_rows.append(
            {
                "candidate_id": row["candidate_id"],
                "source_wallet": wallet,
                "submitted_at": submitted_at,
                "status": status,
                "market_slug": event.get("market_slug"),
                "resolved": bool(event.get("resolved")),
                "pnl_usd": round(float(event.get("pnl_usd") or 0.0), 6) if event.get("resolved") else 0.0,
            }
        )

    finalized = [_finalize_expansion_member(row) for row in members]
    summary = {
        "members": len(finalized),
        "members_with_submit_attempt": sum(1 for row in finalized if int(row.get("submit_attempts") or 0) > 0),
        "members_with_resolved_fill": sum(1 for row in finalized if int(row.get("resolved_fills") or 0) >= 1),
        "submit_attempts": sum(int(row.get("submit_attempts") or 0) for row in finalized),
        "fills": sum(int(row.get("fills") or 0) for row in finalized),
        "rejects": sum(int(row.get("rejects") or 0) for row in finalized),
        "open_submitted_orders": sum(int(row.get("open_submitted_orders") or 0) for row in finalized),
        "resolved_fills": sum(int(row.get("resolved_fills") or 0) for row in finalized),
        "unresolved_fills": sum(int(row.get("unresolved_fills") or 0) for row in finalized),
        "resolved_pnl_usd": round(sum(float(row.get("resolved_pnl_usd") or 0.0) for row in finalized), 6),
    }
    raise_ready = bool(finalized) and summary["members_with_resolved_fill"] == len(finalized) and summary["resolved_pnl_usd"] >= 0.0
    return {
        "flow_stage": "PROMOTE/LIVE/ROTATE",
        "source_direction": "2026-07-06T15:58:00Z, 2026-07-06T16:00:16Z, 2026-07-06T16:13:22Z, and 2026-07-06T16:48:30Z fable DIRECTION",
        "cohort_start_iso": cohort_start,
        "effective_start_iso": datetime.fromtimestamp(effective_start_ts, tz=UTC).isoformat().replace("+00:00", "Z"),
        "evaluation_due_iso": EXPANSION_COHORT_EVALUATION_DUE,
        "scope": "active_set_members_with_candidate_id_prefix_expansion_rank_",
        "summary": summary,
        "raise_rule": {
            "target_member_count_max_if_ready": 10,
            "requires_each_member_resolved_fills_gte": 1,
            "requires_cohort_resolved_pnl_gte": 0.0,
            "raise_to_10_ready_now": raise_ready,
            "negative_member_blocks_ready_resolved_fills_gte": 3,
            "if_not_ready_next": "apply the 2026-07-06T16:48:30Z branch precedence; ask only if the negative attempted-member branch has >=3 resolved fills",
            "branch_precedence": [
                "negative_attempted_member_with_resolved_fills_gte_3",
                "ready",
                "unresolved_attempt_member",
                "no_attempt_member",
            ],
            "mechanical_eod_branches": {
                "negative_attempted_member_with_resolved_fills_gte_3": "if any attempted cohort member has negative resolved PnL on >=3 resolved fills, hold at 8, do not rotate, and escalate with this table",
                "ready": "if every cohort member has >=1 resolved fill and cohort resolved PnL >=0, raise target max to 10; a negative member with <3 resolved fills is watch noise, not a ready blocker",
                "no_attempt_member": "if a positive resolved member remains and another member has 0 attempts, hold at 8 and rotate the no-attempt slot to the next ranked ready-backlog member",
                "unresolved_attempt_member": "if a member has attempts but no resolved fill yet, hold at 8 until the branch is decidable from resolved evidence",
            },
            "post_raise_watch": "if a raised cohort member remains resolved-negative on <3 fills, keep it in watch and demote/rotate mechanically once it reaches resolved-negative on >=3 resolved fills",
        },
        "members": finalized,
        "events": event_rows,
    }


def _window_coverage(volume_kpi: dict[str, Any]) -> dict[str, Any]:
    canonical = volume_kpi.get("canonical_daily") if isinstance(volume_kpi.get("canonical_daily"), dict) else {}
    windows_total = int(canonical.get("denominator_windows", volume_kpi.get("windows_total", BTC_5M_WINDOWS_PER_DAY)) or 0)
    windows_traded = int(canonical.get("windows_filled", volume_kpi.get("windows_traded", 0)) or 0)
    windows_submitted = int(canonical.get("windows_submitted", volume_kpi.get("windows_submitted", 0)) or 0)
    return {
        "source": volume_kpi.get("source"),
        "definition": canonical.get(
            "definition",
            "distinct BTC-5m market_slug with at least one FILLED live ledger order in the UTC day",
        ),
        "windows_total": windows_total,
        "windows_traded": windows_traded,
        "coverage_pct": round((100.0 * windows_traded / windows_total), 6) if windows_total else 0.0,
        "windows_submitted": windows_submitted,
        "submitted_coverage_pct": round((100.0 * windows_submitted / windows_total), 6) if windows_total else 0.0,
    }


def _guard_skip_histogram(guard_state: dict[str, Any]) -> dict[str, Any]:
    participation = guard_state.get("window_participation") if isinstance(guard_state.get("window_participation"), dict) else {}
    rollups = participation.get("window_rollups") if isinstance(participation.get("window_rollups"), list) else []
    skip_counts: Counter[str] = Counter()
    missed_active_windows = 0
    rows_seen = 0
    for row in rollups:
        if not isinstance(row, dict):
            continue
        rows_seen += 1
        if row.get("missed_active_window"):
            missed_active_windows += 1
        reasons = row.get("dominant_skip_reason_counts") if isinstance(row.get("dominant_skip_reason_counts"), dict) else {}
        if reasons:
            skip_counts.update({str(key): int(value or 0) for key, value in reasons.items()})
        elif row.get("dominant_skip_reason"):
            skip_counts[str(row.get("dominant_skip_reason"))] += 1
    return {
        "source": "guard_state.window_participation.window_rollups",
        "rollup_rows": rows_seen,
        "missed_active_windows": missed_active_windows,
        "skip_reason_counts": dict(sorted(skip_counts.items())),
    }


def _live_book_age_gate_evidence(guard_state: dict[str, Any]) -> dict[str, Any]:
    """Expose resident gate configuration and fill-free gate evaluations."""
    live_execution = (
        guard_state.get("live_execution")
        if isinstance(guard_state.get("live_execution"), dict)
        else {}
    )
    intent_summary = (
        live_execution.get("candidate_intent_summary")
        if isinstance(live_execution.get("candidate_intent_summary"), dict)
        else {}
    )
    gate = (
        intent_summary.get("inventory_best_ask_gate")
        if isinstance(intent_summary.get("inventory_best_ask_gate"), dict)
        else {}
    )
    identity = (
        guard_state.get("guard_code_identity")
        if isinstance(guard_state.get("guard_code_identity"), dict)
        else {}
    )
    generation_sha256 = str(
        gate.get("generation_sha256")
        or identity.get("live_guard_generation_sha256")
        or ""
    ).strip() or None
    return {
        "source": "guard_state.live_execution.candidate_intent_summary.inventory_best_ask_gate",
        "generation_sha256": generation_sha256,
        "book_cache_ttl_s": gate.get("book_cache_ttl_s"),
        "max_gate_probe_best_ask_age_at_gate_s": gate.get(
            "max_gate_probe_best_ask_age_at_gate_s"
        ),
        "threshold_independent_of_cache_ttl": gate.get(
            "threshold_independent_of_cache_ttl"
        ),
        "gate_probe_best_ask_age_at_gate_s": gate.get(
            "gate_probe_best_ask_age_at_gate_s"
        ),
        "blocker_counts": gate.get("blocker_counts")
        if isinstance(gate.get("blocker_counts"), dict)
        else {},
        "evidence_available_without_fill": bool(
            generation_sha256
            and isinstance(gate.get("threshold_independent_of_cache_ttl"), bool)
        ),
    }


def _paper_lane_gate_snapshots() -> dict[str, Any]:
    paths = {
        "e5_maker_first_btc5m": ROOT / "data/research/maker_first_btc5m_paper_state.json",
        "e5_maker_first_btc5m_resolution": ROOT / "data/research/maker_first_btc5m_resolution_state.json",
        "e6_whale_net_flow": ROOT / "data/research/e6_whale_net_flow_paper_lane_state.json",
        "btc5m_late_window_penny_watcher": ROOT / "data/research/btc5m_late_window_penny_watcher_state.json",
    }
    snapshots: dict[str, Any] = {}
    for name, path in paths.items():
        state = load_json(path, default={})
        state = state if isinstance(state, dict) else {}
        snapshots[name] = {
            "state_path": str(path),
            "updated_at": state.get("updated_at"),
            "paper_only": bool(state.get("paper_only")),
            "live_orders_allowed": bool(state.get("live_orders_allowed")),
            "summary": state.get("summary") if isinstance(state.get("summary"), dict) else {},
            "promotion_gate": state.get("promotion_gate") if isinstance(state.get("promotion_gate"), dict) else {},
        }
    return snapshots


def _target_ladder(scorecard: dict[str, Any], guard_state: dict[str, Any], *, bankroll_usd: float) -> dict[str, Any]:
    bankroll = max(0.0, float(bankroll_usd))
    pnl = float(scorecard["today"]["total"]["pnl_usd"])
    volume = scorecard.get("volume_kpi", {})
    active_set = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    active_members = [row for row in members if isinstance(row, dict) and row.get("enabled") is not False]
    weekly_min_pct = 3.0
    weekly_max_pct = 5.0
    daily_min_pct = weekly_min_pct / 7.0
    actual_pct = round((100.0 * pnl / bankroll), 6) if bankroll > 0 else 0.0
    coverage_pct = float(volume.get("windows_traded_pct") or 0.0)
    return {
        "flow_stage": "LIVE/ROTATE/SELF-DEV",
        "operator_decision": "OP-TARGET-20260705-BELA",
        "proof_gate_update": "OP-FASTPROOF-20260705-BELA",
        "phase": "phase1",
        "bankroll_usd": round(bankroll, 6),
        "north_star": {
            "daily_pct": 2.0,
            "daily_usd": round(bankroll * 0.02, 6),
            "weekly_pct_range": [10.0, 15.0],
            "weekly_usd_range": [round(bankroll * 0.10, 6), round(bankroll * 0.15, 6)],
        },
        "phase_target": {
            "weekly_pct_range": [weekly_min_pct, weekly_max_pct],
            "weekly_usd_range": [round(bankroll * weekly_min_pct / 100.0, 6), round(bankroll * weekly_max_pct / 100.0, 6)],
            "daily_min_run_rate_pct": round(daily_min_pct, 6),
            "daily_min_run_rate_usd": round(bankroll * daily_min_pct / 100.0, 6),
            "process_minimums": {
                "window_coverage_pct_gt": 50.0,
                "active_member_count_gte": 5,
                "consensus_verdict_due": "2026-07-07T14:15:00Z",
            },
        },
        "actual": {
            "day_pnl_usd": round(pnl, 6),
            "day_pnl_pct": actual_pct,
            "above_zero": pnl > 0,
            "meets_daily_min_run_rate": actual_pct >= daily_min_pct,
            "north_star_daily_gap_usd": round(pnl - bankroll * 0.02, 6),
            "phase_daily_min_gap_usd": round(pnl - bankroll * daily_min_pct / 100.0, 6),
        },
        "process_actual": {
            "window_coverage_pct": round(coverage_pct, 6),
            "window_coverage_gt_50": coverage_pct > 50.0,
            "active_member_count": len(active_members),
            "active_set_held": len(active_members) >= 5,
        },
    }


def _goal_arithmetic_invariant(
    scorecard: dict[str, Any],
    guard_state: dict[str, Any],
    *,
    goal_band_floor_usd: float = 100.0,
) -> dict[str, Any]:
    """Prove whether the live caps can reach the operator's daily goal.

    The ceiling deliberately grants all 288 BTC-5m windows while preserving
    the currently selected member's effective order/tranche cap, the global
    per-window fill cap, and the measured resolved-fill ROI. This separates a
    config contradiction from the additional observed-participation gap.
    """

    candidate = guard_state.get("candidate") if isinstance(guard_state.get("candidate"), dict) else {}
    policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
    runtime = (
        guard_state.get("guard_runtime_filter")
        if isinstance(guard_state.get("guard_runtime_filter"), dict)
        else {}
    )
    max_order_usd = num(policy.get("max_order_usd"), num(runtime.get("max_order_usd"), 0.0))
    copy_model = str(runtime.get("copy_model") or guard_state.get("copy_style") or "")
    drip_max_tranche_usd = num(
        policy.get("drip_max_tranche_usd"),
        num(runtime.get("drip_max_tranche_usd"), max_order_usd),
    )
    effective_fill_cap_usd = (
        min(max_order_usd, drip_max_tranche_usd)
        if copy_model == "drip" and drip_max_tranche_usd > 0
        else max_order_usd
    )
    per_window_fill_cap = max(0, int(num(runtime.get("per_window_fill_cap"), 0.0)))
    total = ((scorecard.get("today") or {}).get("total") or {})
    measured_roi_pct = num(total.get("roi_pct"), 0.0)
    measured_edge = max(0.0, measured_roi_pct / 100.0)
    volume = scorecard.get("volume_kpi") if isinstance(scorecard.get("volume_kpi"), dict) else {}
    canonical = volume.get("canonical_daily") if isinstance(volume.get("canonical_daily"), dict) else {}
    denominator_windows = max(0, int(num(canonical.get("denominator_windows"), 288.0)))
    observed_submitted_windows = max(0, int(num(canonical.get("windows_submitted"), 0.0)))
    observed_filled_windows = max(0, int(num(canonical.get("windows_filled"), 0.0)))
    full_day_ceiling = (
        effective_fill_cap_usd * per_window_fill_cap * denominator_windows * measured_edge
    )
    observed_participation_capacity = (
        effective_fill_cap_usd * per_window_fill_cap * observed_submitted_windows * measured_edge
    )
    inputs_complete = effective_fill_cap_usd > 0 and per_window_fill_cap > 0 and denominator_windows > 0
    contradiction = not inputs_complete or full_day_ceiling < float(goal_band_floor_usd)
    return {
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "status": "RED_CONFIG_GOAL_CONTRADICTION" if contradiction else "PASS_GOAL_ARITHMETIC_REACHABLE",
        "standing_red": contradiction,
        "operator_decision": "OP-USD-TARGET-20260707-BELA",
        "goal_band_floor_usd": round(float(goal_band_floor_usd), 6),
        "max_achievable_day_usd": round(full_day_ceiling, 6),
        "observed_participation_capacity_usd": round(observed_participation_capacity, 6),
        "gap_to_goal_floor_usd": round(full_day_ceiling - float(goal_band_floor_usd), 6),
        "inputs": {
            "selected_candidate_id": candidate.get("candidate_id") or guard_state.get("candidate_id"),
            "source_wallet": candidate.get("source_wallet") or guard_state.get("source_wallet"),
            "copy_model": copy_model,
            "max_order_usd": round(max_order_usd, 6),
            "drip_max_tranche_usd": round(drip_max_tranche_usd, 6),
            "effective_fill_cap_usd": round(effective_fill_cap_usd, 6),
            "per_window_fill_cap": per_window_fill_cap,
            "denominator_windows": denominator_windows,
            "observed_submitted_windows": observed_submitted_windows,
            "observed_filled_windows": observed_filled_windows,
            "measured_resolved_fill_roi_pct": round(measured_roi_pct, 6),
            "measured_positive_edge_fraction": round(measured_edge, 9),
            "inputs_complete": inputs_complete,
        },
        "formula": "effective_fill_cap_usd * per_window_fill_cap * windows * max(0, measured_resolved_fill_roi_pct/100)",
        "next_action": (
            "framework audit decides evidence-backed base-size/pre-Phase-2 canary amendment; no blind live size change"
            if contradiction
            else "retain arithmetic check on every scorecard and scale only under measured EV/revert gates"
        ),
    }


def _resolved_fill_events(
    orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    *,
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        event = score_order(
            order,
            resolutions,
            receipt_costs=receipt_costs,
            actual_trade_costs=actual_trade_costs,
        )
        if str(event.get("status") or "") != "FILLED" or not event.get("resolved") or event.get("ts") is None:
            continue
        events.append(
            {
                "ts": float(event["ts"]),
                "day_utc": str(event.get("day_utc") or ""),
                "pnl_usd": round(float(event.get("pnl_usd") or 0.0), 6),
                "source_wallet": str(event.get("source_wallet") or "unknown"),
                "condition_id": str(event.get("condition_id") or ""),
            }
        )
    events.sort(key=lambda row: float(row["ts"]))
    return events


def _sum_pnl(events: list[dict[str, Any]]) -> float:
    return round(sum(float(row.get("pnl_usd") or 0.0) for row in events), 6)


def _positive_pnl_days(events: list[dict[str, Any]]) -> dict[str, Any]:
    by_day: dict[str, float] = defaultdict(float)
    for event in events:
        by_day[str(event.get("day_utc") or "")] += float(event.get("pnl_usd") or 0.0)
    positive_days = [day for day, pnl in sorted(by_day.items()) if pnl > 0.0]
    return {
        "positive_pnl_days": len(positive_days),
        "positive_pnl_day_list": positive_days,
    }


def _rolling_tail_pnl(events: list[dict[str, Any]], window: int) -> float:
    if not events:
        return 0.0
    return _sum_pnl(events[-window:])


def _sizing_ramp_progress(events: list[dict[str, Any]]) -> dict[str, Any]:
    completed_blocks = len(events) // SIZING_BLOCK_FILLS
    positive_blocks = 0
    block_pnls: list[float] = []
    for idx in range(completed_blocks):
        block = events[idx * SIZING_BLOCK_FILLS : (idx + 1) * SIZING_BLOCK_FILLS]
        block_pnl = _sum_pnl(block)
        block_pnls.append(block_pnl)
        if block_pnl > 0.0:
            positive_blocks += 1
    current_step_index = min(positive_blocks, len(SIZING_STEPS_USD) - 1)
    next_step_index = min(current_step_index + 1, len(SIZING_STEPS_USD) - 1)
    fills_into_block = len(events) % SIZING_BLOCK_FILLS
    return {
        "block_size_resolved_fills": SIZING_BLOCK_FILLS,
        "resolved_fills": len(events),
        "completed_blocks": completed_blocks,
        "positive_completed_blocks": positive_blocks,
        "latest_completed_block_pnl_usd": block_pnls[-1] if block_pnls else 0.0,
        "current_rolling_100_pnl_usd": _rolling_tail_pnl(events, SIZING_BLOCK_FILLS),
        "fills_into_current_block": fills_into_block,
        "fills_to_next_block": SIZING_BLOCK_FILLS - fills_into_block if fills_into_block else SIZING_BLOCK_FILLS,
        "reported_unlocked_size_usd": SIZING_STEPS_USD[current_step_index],
        "next_positive_block_size_usd": SIZING_STEPS_USD[next_step_index],
        "ramp_paused_by_latest_completed_block": bool(block_pnls and block_pnls[-1] <= 0.0),
    }


def _lane_paper_gate(
    name: str,
    path: Path,
    resolutions: dict[str, dict[str, Any]],
    *,
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = load_json(path, default={})
    state = state if isinstance(state, dict) else {}
    gate = state.get("promotion_gate") if isinstance(state.get("promotion_gate"), dict) else {}
    if gate:
        resolved = int(gate.get("resolved_paper_fills") or 0)
        required = int(gate.get("resolved_paper_fills_required") or LANE_PAPER_PROMOTION_FILLS)
        pnl = round(float(gate.get("resolved_paper_pnl_usd") or 0.0), 6)
        return {
            "lane": name,
            "state_path": str(path),
            "resolved_paper_fills": resolved,
            "target_resolved_paper_fills": required,
            "paper_fills_remaining": max(0, required - resolved),
            "paper_pnl_usd": pnl,
            "paper_pnl_positive": pnl > 0.0,
            "small_live_promotion_ready": resolved >= required and pnl > 0.0,
            "paper_only": bool(state.get("paper_only")),
        }
    lane_orders = state.get("orders") if isinstance(state.get("orders"), list) else []
    events = _resolved_fill_events(
        lane_orders,
        resolutions,
        receipt_costs=receipt_costs,
        actual_trade_costs=actual_trade_costs,
    )
    pnl = _sum_pnl(events)
    return {
        "lane": name,
        "state_path": str(path),
        "resolved_paper_fills": len(events),
        "target_resolved_paper_fills": LANE_PAPER_PROMOTION_FILLS,
        "paper_fills_remaining": max(0, LANE_PAPER_PROMOTION_FILLS - len(events)),
        "paper_pnl_usd": pnl,
        "paper_pnl_positive": pnl > 0.0,
        "small_live_promotion_ready": len(events) >= LANE_PAPER_PROMOTION_FILLS and pnl > 0.0,
        "paper_only": bool(state.get("paper_only")),
    }


def _daily_how_line(scorecard: dict[str, Any]) -> dict[str, Any]:
    pnl = float(((scorecard.get("today") or {}).get("total") or {}).get("pnl_usd") or 0.0)
    volume = scorecard.get("volume_kpi") if isinstance(scorecard.get("volume_kpi"), dict) else {}
    canonical = volume.get("canonical_daily") if isinstance(volume.get("canonical_daily"), dict) else {}
    windows_filled = int(canonical.get("windows_filled") or volume.get("windows_traded") or 0)
    denominator = int(canonical.get("denominator_windows") or volume.get("windows_total") or 288)
    gap_to_144 = max(0, 144 - windows_filled)
    pnl_per_filled_window = pnl / windows_filled if windows_filled > 0 else 0.0
    retention_expected = round(max(0.0, pnl_per_filled_window) * gap_to_144, 6)
    min_band = 100.0
    max_band = 300.0
    gap_to_min = round(max(0.0, min_band - pnl), 6)
    return {
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "operator_decision": "OP-DAILY-HOW-20260710",
        "report_layer_only": True,
        "profit_band_usd": [min_band, max_band],
        "current_day_pnl_usd": round(pnl, 6),
        "gap_to_min_band_usd": gap_to_min,
        "current_windows_filled": windows_filled,
        "denominator_windows": denominator,
        "path_to_band_today": (
            f"current ${pnl:.2f}; need ${gap_to_min:.2f} to $100. Best safe route today is preserve "
            f"the producing selective-copy lane, close {gap_to_144} filled-window OP_VOLUME gap via "
            "CAMPAIGN-LAT retention/selection visibility, and keep dead-band recruiting plus winner variants "
            "paper-only until their gates pass."
        ),
        "alternatives": [
            {
                "rank": 1,
                "id": "campaign_lat_retention_selection",
                "expected_usd_today": retention_expected,
                "basis": "current_day_pnl_per_filled_window_times_gap_to_144",
                "live_path": "no live loosening; Fable gate plus canary only",
            },
            {
                "rank": 2,
                "id": "dead_band_specialist_recruiting",
                "expected_usd_today": 0.0,
                "basis": "watch-tier/probation evidence first; targets 18-22 UTC source gap",
                "live_path": "dry-run probation then standard admission gate",
            },
            {
                "rank": 3,
                "id": "winner_single_parameter_variants",
                "expected_usd_today": 0.0,
                "basis": "paper siblings measure freshness/band/sizing neighbors against paper twin inside parent-config epochs; promote only after n>=50 and >=1pp ROI diff-in-diff win",
                "live_path": "standard canary after attributable single-parameter win vs paper twin",
            },
        ],
    }


def _proof_gates(
    orders: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    *,
    paper_lane_paths: tuple[tuple[str, Path], ...] = PAPER_LANE_STATES,
    receipt_costs: dict[str, float] | None = None,
    actual_trade_costs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    live_events = _resolved_fill_events(
        orders,
        resolutions,
        receipt_costs=receipt_costs,
        actual_trade_costs=actual_trade_costs,
    )
    live_pnl = _sum_pnl(live_events)
    day_progress = _positive_pnl_days(live_events)
    rolling_100_pnl = _rolling_tail_pnl(live_events, SIZING_BLOCK_FILLS)
    phase2_ready = (
        len(live_events) >= PHASE2_RESOLVED_FILL_TARGET
        and live_pnl > 0.0
        and int(day_progress["positive_pnl_days"]) >= PHASE2_POSITIVE_DAY_TARGET
        and rolling_100_pnl > 0.0
    )
    return {
        "flow_stage": "LIVE/PROMOTE/ROTATE/TRACK2",
        "operator_decision": "OP-FASTPROOF-20260705-BELA",
        "source_direction": "2026-07-05T19:20:00Z fable DIRECTION",
        "phase2_promotion": {
            "resolved_live_fills": len(live_events),
            "target_resolved_live_fills": PHASE2_RESOLVED_FILL_TARGET,
            "live_fills_remaining": max(0, PHASE2_RESOLVED_FILL_TARGET - len(live_events)),
            "cumulative_live_pnl_usd": live_pnl,
            "cumulative_live_pnl_positive": live_pnl > 0.0,
            "positive_pnl_days": day_progress["positive_pnl_days"],
            "target_positive_pnl_days": PHASE2_POSITIVE_DAY_TARGET,
            "positive_pnl_days_remaining": max(
                0, PHASE2_POSITIVE_DAY_TARGET - int(day_progress["positive_pnl_days"])
            ),
            "positive_pnl_day_list": day_progress["positive_pnl_day_list"],
            "current_rolling_100_pnl_usd": rolling_100_pnl,
            "ready_for_phase2": phase2_ready,
        },
        "sizing_ramp": _sizing_ramp_progress(live_events),
        "lane_promotion": {
            "paper_small_live_target_resolved_fills": LANE_PAPER_PROMOTION_FILLS,
            "live_scale_target_resolved_fills": LANE_LIVE_SCALE_FILLS,
            "live_scale_progress": {
                "resolved_live_fills": len(live_events),
                "target_resolved_live_fills": LANE_LIVE_SCALE_FILLS,
                "live_fills_remaining": max(0, LANE_LIVE_SCALE_FILLS - len(live_events)),
                "cumulative_live_pnl_usd": live_pnl,
                "scale_ready": len(live_events) >= LANE_LIVE_SCALE_FILLS and live_pnl > 0.0,
            },
            "paper_lanes": [
                _lane_paper_gate(
                    name,
                    path,
                    resolutions,
                    receipt_costs=receipt_costs,
                    actual_trade_costs=actual_trade_costs,
                )
                for name, path in paper_lane_paths
            ],
        },
    }


def _engine_race_gate(name: str, path: Path) -> dict[str, Any]:
    state = load_json(path, default={})
    state = state if isinstance(state, dict) else {}
    gate = state.get("promotion_gate") if isinstance(state.get("promotion_gate"), dict) else {}
    resolved = int(gate.get("resolved_paper_fills") or 0)
    required = int(gate.get("resolved_paper_fills_required") or LANE_PAPER_PROMOTION_FILLS)
    pnl = round(float(gate.get("resolved_paper_pnl_usd") or 0.0), 6)
    fills = int(gate.get("filled_orders") or gate.get("paper_filled_orders") or 0)
    paper_quotes = int(gate.get("paper_quotes") or len(state.get("orders") if isinstance(state.get("orders"), list) else []))
    fill_rate = gate.get("maker_fill_rate_pct")
    if fill_rate is None and paper_quotes:
        fill_rate = round(100.0 * fills / paper_quotes, 6)
    roi = gate.get("resolved_paper_roi_pct")
    wins = int(gate.get("resolved_paper_wins") or 0)
    losses = int(gate.get("resolved_paper_losses") or 0)
    promotion_status = str(gate.get("promotion_50_resolved_positive") or "PENDING")
    no_old_unresolved = gate.get("no_unresolved_inventory_older_than_one_window")
    parity_violations = int(gate.get("copyintent_parity_violations") or 0)
    ready_reasons: list[str] = []
    if resolved < required:
        ready_reasons.append("resolved_paper_fills_below_required")
    if pnl <= 0.0:
        ready_reasons.append("resolved_paper_pnl_not_positive")
    if promotion_status == "PENDING":
        ready_reasons.append("lane_promotion_gate_pending")
    if no_old_unresolved is not True:
        ready_reasons.append("unresolved_inventory_not_cleared")
    if parity_violations:
        ready_reasons.append("copyintent_parity_violations")
    gate_ready = not ready_reasons
    return {
        "lane": name,
        "state_path": str(path),
        "updated_at": str(state.get("updated_at") or ""),
        "paper_only": bool(state.get("paper_only")),
        "live_orders_allowed": bool(state.get("live_orders_allowed")),
        "can_trade": bool(state.get("can_trade")),
        "paper_quotes": paper_quotes,
        "paper_filled_orders": fills,
        "maker_fill_rate_pct": round(float(fill_rate or 0.0), 6),
        "resolved_paper_fills": resolved,
        "target_resolved_paper_fills": required,
        "paper_fills_remaining": max(0, required - resolved),
        "unresolved_paper_fills": int(gate.get("unresolved_paper_fills") or 0),
        "resolved_paper_wins": wins,
        "resolved_paper_losses": losses,
        "resolved_paper_wr_pct": round(float(gate.get("resolved_paper_wr_pct") or 0.0), 6),
        "resolved_paper_pnl_usd": pnl,
        "resolved_paper_roi_pct": round(float(roi or 0.0), 6),
        "promotion_50_resolved_positive": promotion_status,
        "no_unresolved_inventory_older_than_one_window": no_old_unresolved is True,
        "gate_ready": gate_ready,
        "gate_ready_reasons": ready_reasons,
        "copyintent_parity_violations": parity_violations,
    }


def _engine_race_gates(
    states: tuple[tuple[str, Path], ...] = ENGINE_RACE_STATES,
) -> dict[str, Any]:
    lanes = [_engine_race_gate(name, path) for name, path in states]
    return {
        "paper_small_live_target_resolved_fills": LANE_PAPER_PROMOTION_FILLS,
        "lanes": lanes,
        "gate_ready_lanes": [row["lane"] for row in lanes if row.get("gate_ready")],
        "parity_violations": sum(int(row.get("copyintent_parity_violations") or 0) for row in lanes),
    }


def _member_factory_kpi(path: Path = ROOT / "data" / "research" / "member_factory_kpi_state.json") -> dict[str, Any]:
    loaded = load_json(path, default={})
    if not isinstance(loaded, dict):
        return {}
    return {
        "state_path": str(path),
        "generated_at": str(loaded.get("generated_at") or ""),
        "queue_depth": loaded.get("queue_depth") if isinstance(loaded.get("queue_depth"), dict) else {},
        "set_trajectory": loaded.get("set_trajectory") if isinstance(loaded.get("set_trajectory"), dict) else {},
        "member_freshness": loaded.get("member_freshness") if isinstance(loaded.get("member_freshness"), dict) else {},
        "factory_throughput": loaded.get("factory_throughput") if isinstance(loaded.get("factory_throughput"), dict) else {},
        "defects": loaded.get("defects") if isinstance(loaded.get("defects"), list) else [],
    }


def _self_feed_vs_ledger(
    path: Path = ROOT / "data" / "research" / "wallet_copy_self_feed_vs_ledger_latest.json",
) -> dict[str, Any]:
    loaded = load_json(path, default={})
    if not isinstance(loaded, dict):
        return {"status": "MISSING", "state_path": str(path), "summary": {}}
    summary = loaded.get("summary") if isinstance(loaded.get("summary"), dict) else {}
    return {
        "state_path": str(path),
        "generated_at": str(loaded.get("generated_at") or ""),
        "status": str(loaded.get("status") or summary.get("status") or "UNKNOWN"),
        "user": str(loaded.get("user") or ""),
        "window": loaded.get("window") if isinstance(loaded.get("window"), dict) else {},
        "summary": summary,
        "top_diffs": loaded.get("top_diffs") if isinstance(loaded.get("top_diffs"), list) else [],
    }


def _first_num(*values: Any) -> float:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _self_feed_reconciliation_overlay(
    path: Path = ROOT / "data" / "research" / "wallet_copy_self_feed_duckdb_benchmark_latest.json",
) -> dict[str, Any]:
    loaded = load_json(path, default={})
    if not isinstance(loaded, dict):
        return {"status": "MISSING", "state_path": str(path)}
    packet = loaded.get("classification_packet") if isinstance(loaded.get("classification_packet"), dict) else {}
    overlay = packet.get("resolved_pnl_overlay") if isinstance(packet.get("resolved_pnl_overlay"), dict) else {}
    recommendation = packet.get("recommendation") if isinstance(packet.get("recommendation"), dict) else {}
    overlay_delta = _first_num(overlay.get("overlay_delta_usd"), overlay.get("pnl_usd"))
    raw_actual = num(overlay.get("actual_delta_usd"), 0.0)
    reconciled = raw_actual + overlay_delta
    generated_at = str(loaded.get("generated_at") or "")
    generated_date = ""
    try:
        generated_date = datetime.fromisoformat(generated_at.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        generated_date = ""
    current_date = datetime.now(UTC).date().isoformat()
    freshness_status = "CURRENT_DAY" if generated_date == current_date else "STALE_FALLBACK"
    h2_external = _h2_external_redemption_overlay_source()
    named_sources = {"h2_external_redemptions": h2_external} if h2_external else {}
    return {
        "state_path": str(path),
        "generated_at": generated_at,
        "generated_date_utc": generated_date,
        "current_date_utc": current_date,
        "freshness_status": freshness_status,
        "status": str(loaded.get("status") or "UNKNOWN"),
        "mode": recommendation.get("mode"),
        "ledger_rewrite": bool(recommendation.get("ledger_rewrite")),
        "backfill_allowed": bool(recommendation.get("backfill_allowed")),
        "overlay_delta_usd": round(overlay_delta, 6),
        "raw_missing_pnl_upper_bound_usd": overlay.get("raw_missing_pnl_upper_bound_usd"),
        "double_count_excluded_usd": overlay.get("double_count_excluded_usd"),
        "class_decomposition": overlay.get("class_decomposition")
        if isinstance(overlay.get("class_decomposition"), dict)
        else {},
        "raw_actual_delta_usd": round(raw_actual, 6),
        "ledger_actual_delta_usd": round(raw_actual, 6),
        "reconciled_actual_delta_usd": round(reconciled, 6),
        "reconciled_actual_producing": reconciled > 0,
        "resolved_tx_groups": overlay.get("resolved_tx_groups"),
        "unresolved_tx_groups": overlay.get("unresolved_tx_groups"),
        "source_status": loaded.get("status"),
        "source_gap_status": (loaded.get("gap_scan") or {}).get("status")
        if isinstance(loaded.get("gap_scan"), dict)
        else None,
        "named_sources": named_sources,
        "next_action": recommendation.get("next_action") or loaded.get("next_action"),
    }


def _h2_external_redemption_overlay_source(
    path: Path = ROOT / "data" / "research" / "h2_external_redemption_ingestion_latest.json",
) -> dict[str, Any]:
    loaded = load_json(path, default={})
    if not isinstance(loaded, dict) or loaded.get("status") != "PASS":
        return {}
    summary = loaded.get("summary") if isinstance(loaded.get("summary"), dict) else {}
    acceptance = loaded.get("acceptance") if isinstance(loaded.get("acceptance"), dict) else {}
    return {
        "source_path": str(path),
        "status": loaded.get("status"),
        "overlay_source_name": loaded.get("overlay_source_name"),
        "ledger_rewrite": bool(loaded.get("ledger_rewrite")),
        "external_redeem_rows": summary.get("external_redeem_rows"),
        "confirmed_external_redeem_rows": summary.get("confirmed_external_redeem_rows"),
        "total_redeem_usdc": summary.get("total_redeem_usdc"),
        "max_abs_delta_usd": summary.get("max_abs_delta_usd"),
        "cash_diff_residual_usd": acceptance.get("cash_diff_residual_usd"),
        "residual_explained_by_external_redeems_usd": acceptance.get(
            "residual_explained_by_external_redeems_usd"
        ),
        "residual_unexplained_after_external_redeems_usd": acceptance.get(
            "residual_unexplained_after_external_redeems_usd"
        ),
        "anchor_relabel": acceptance.get("anchor_relabel"),
    }


def _cash_diff_reconciliation_residual(
    path: Path = ROOT / "data" / "research" / "wallet_copy_today_fill_cash_diff_latest.json",
) -> dict[str, Any]:
    loaded = load_json(path, default={})
    if not isinstance(loaded, dict):
        return {"status": "MISSING", "state_path": str(path)}
    summary = loaded.get("summary") if isinstance(loaded.get("summary"), dict) else {}
    scorecard_delta = summary.get("scorecard_reconciliation_delta_usd")
    if scorecard_delta is None:
        return {"status": "MISSING_SCORECARD_DELTA", "state_path": str(path), "generated_at": loaded.get("generated_at")}
    explained = _first_num(
        summary.get("scorecard_delta_explained_by_fill_cost_payout_usd"),
        summary.get("sum_explained_surplus_usd"),
    )
    residual = round(num(scorecard_delta, 0.0) - explained, 6)
    missing_or_unjoined = int(summary.get("ledger_fills_missing_tx") or 0) + int(summary.get("unjoined_tx_groups") or 0)
    status = "RECONCILED_BY_FILL_COST_PAYOUT" if abs(residual) <= 0.000001 else "NAMED_RESIDUAL"
    if missing_or_unjoined > 0 and status == "NAMED_RESIDUAL":
        status = "NAMED_RESIDUAL_WITH_INCOMPLETE_FILL_JOIN"
    return {
        "state_path": str(path),
        "generated_at": loaded.get("generated_at"),
        "status": status,
        "scorecard_reconciliation_delta_usd": round(num(scorecard_delta, 0.0), 6),
        "fill_cost_payout_explained_usd": round(explained, 6),
        "residual_usd": residual,
        "residual_classification": summary.get("scorecard_delta_residual_classification")
        or "unaccounted_one_time_cash_movement",
        "joined_tx_groups": summary.get("joined_tx_groups"),
        "ledger_tx_groups": summary.get("ledger_tx_groups"),
        "unjoined_tx_groups": summary.get("unjoined_tx_groups"),
        "ledger_fills_missing_tx": summary.get("ledger_fills_missing_tx"),
        "next_action": summary.get("scorecard_delta_residual_next_action")
        or "audit account-value timing, non-fill cash movements, fees/rounding, or balance sampling",
    }


def _retrace_reconciliation_equation(
    path: Path = ROOT / "data" / "research" / "wallet_copy_self_feed_full_ledger_retrace_latest.json",
) -> dict[str, Any]:
    loaded = load_json(path, default={})
    if not isinstance(loaded, dict):
        return {"status": "MISSING", "state_path": str(path)}
    equation = loaded.get("reconciliation_equation") if isinstance(loaded.get("reconciliation_equation"), dict) else {}
    if not equation:
        return {"status": "MISSING_EQUATION", "state_path": str(path), "generated_at": loaded.get("generated_at")}
    return {
        "state_path": str(path),
        "generated_at": loaded.get("generated_at"),
        "status": equation.get("status") or "UNKNOWN",
        "actual_delta_usd": equation.get("actual_delta_usd"),
        "b3_join_scope_effect_usd": equation.get("b3_join_scope_effect_usd"),
        "account_value_residual_usd": equation.get("account_value_residual_usd"),
        "unexplained_usd": equation.get("unexplained_usd"),
        "target_abs_unexplained_lt_usd": equation.get("target_abs_unexplained_lt_usd"),
    }


def _apply_self_feed_reconciliation_overlay(
    since_topup: dict[str, Any],
    overlay: dict[str, Any],
) -> dict[str, Any]:
    if not since_topup or overlay.get("status") != "PASS":
        return since_topup
    out = dict(since_topup)
    scorecard_actual = _maybe_float(out.get("actual_delta_vs_baseline_usd"))
    overlay_raw_actual = num(overlay.get("raw_actual_delta_usd"), 0.0)
    raw_actual = scorecard_actual if scorecard_actual is not None else overlay_raw_actual
    overlay_delta = num(overlay.get("overlay_delta_usd"), 0.0)
    reconciled = round(raw_actual + overlay_delta, 6)
    stale_balance_fallback = scorecard_actual is None and overlay.get("freshness_status") == "STALE_FALLBACK"
    verdict = (
        "UNKNOWN_BALANCE_STALE_FALLBACK"
        if stale_balance_fallback
        else ("PRODUCING_RECONCILED_BASIS" if reconciled > 0 else "NOT_PRODUCING_RECONCILED_BASIS")
    )
    out["self_feed_reconciliation_overlay"] = {
        "mode": overlay.get("mode"),
        "ledger_rewrite": bool(overlay.get("ledger_rewrite")),
        "generated_at": overlay.get("generated_at"),
        "generated_date_utc": overlay.get("generated_date_utc"),
        "current_date_utc": overlay.get("current_date_utc"),
        "freshness_status": overlay.get("freshness_status"),
        "fallback_status": "STALE_FALLBACK" if stale_balance_fallback else "CURRENT_SCORECARD_ACTUAL",
        "overlay_delta_usd": round(overlay_delta, 6),
        "raw_missing_pnl_upper_bound_usd": overlay.get("raw_missing_pnl_upper_bound_usd"),
        "double_count_excluded_usd": overlay.get("double_count_excluded_usd"),
        "class_decomposition": overlay.get("class_decomposition")
        if isinstance(overlay.get("class_decomposition"), dict)
        else {},
        "raw_actual_delta_vs_baseline_usd": round(raw_actual, 6),
        "ledger_actual_delta_vs_baseline_usd": round(raw_actual, 6),
        "reconciled_actual_delta_vs_baseline_usd": None if stale_balance_fallback else reconciled,
        "stale_reconciled_actual_delta_vs_baseline_usd": reconciled if stale_balance_fallback else None,
        "reconciled_actual_producing": None if stale_balance_fallback else reconciled > 0,
        "reconciled_verdict": verdict,
        "resolved_tx_groups": overlay.get("resolved_tx_groups"),
        "unresolved_tx_groups": overlay.get("unresolved_tx_groups"),
        "named_sources": overlay.get("named_sources") if isinstance(overlay.get("named_sources"), dict) else {},
    }
    out["actual_basis_reconciled_delta_vs_baseline_usd"] = None if stale_balance_fallback else reconciled
    out["actual_basis_reconciled_producing"] = None if stale_balance_fallback else reconciled > 0
    out["actual_basis_reconciled_verdict"] = verdict
    return out


def _apply_cash_diff_reconciliation_residual(
    since_topup: dict[str, Any],
    residual: dict[str, Any],
    retrace_equation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not since_topup or residual.get("status") in {"MISSING", "MISSING_SCORECARD_DELTA", None}:
        return since_topup
    out = dict(since_topup)
    raw_status = str(out.get("reconciliation_status") or "")
    if raw_status and "raw_reconciliation_status" not in out:
        out["raw_reconciliation_status"] = raw_status
    if raw_status and "unadjusted_reconciliation_status" not in out:
        out["unadjusted_reconciliation_status"] = raw_status
    retrace = retrace_equation if isinstance(retrace_equation, dict) else {}
    if retrace.get("status") == "RECONCILED":
        out["raw_reconciliation_status"] = "RECONCILED"
    out["cash_diff_reconciliation_residual"] = {
        "status": residual.get("status"),
        "scorecard_reconciliation_delta_usd": residual.get("scorecard_reconciliation_delta_usd"),
        "fill_cost_payout_explained_usd": residual.get("fill_cost_payout_explained_usd"),
        "residual_usd": residual.get("residual_usd"),
        "residual_classification": residual.get("residual_classification"),
        "joined_tx_groups": residual.get("joined_tx_groups"),
        "ledger_tx_groups": residual.get("ledger_tx_groups"),
        "unjoined_tx_groups": residual.get("unjoined_tx_groups"),
        "ledger_fills_missing_tx": residual.get("ledger_fills_missing_tx"),
        "generated_at": residual.get("generated_at"),
        "retrace_equation_status": retrace.get("status"),
        "retrace_unexplained_usd": retrace.get("unexplained_usd"),
        "next_action": residual.get("next_action"),
    }
    fill_explained = abs(num(residual.get("fill_cost_payout_explained_usd"), 0.0)) > 0.000001
    retrace_reconciled = retrace.get("status") == "RECONCILED"
    if raw_status == "MISMATCH" and not retrace_reconciled:
        out["reconciliation_status"] = "MISMATCH"
        out["residual_honesty_status"] = "PASS_NO_UNPROVEN_RECONCILED_LABEL"
    elif retrace_reconciled:
        out["reconciliation_status"] = "RECONCILED_BY_RETRACE_EQUATION"
        out["residual_honesty_status"] = "PASS_PROVEN_RETRACE"
    elif fill_explained:
        out["reconciliation_status"] = "RECONCILED_BY_FILL_COST_PAYOUT"
        out["residual_honesty_status"] = "PASS_PROVEN_FILL_EXPLANATION"
    return out


def _annotate_chain_reconciliation(
    reconciliation: dict[str, Any],
    since_topup: dict[str, Any],
) -> dict[str, Any]:
    out = dict(reconciliation)
    named_residual = (
        since_topup.get("cash_diff_reconciliation_residual")
        if isinstance(since_topup.get("cash_diff_reconciliation_residual"), dict)
        else {}
    )
    out["adjusted_status"] = since_topup.get("reconciliation_status")
    out["cash_diff_residual"] = named_residual
    out["cash_diff_residual_usd"] = named_residual.get("residual_usd")
    out["residual_class"] = named_residual.get("residual_classification")
    return out


def _text(scorecard: dict[str, Any]) -> str:
    lines = [
        f"day_utc={scorecard['day_utc']} window={scorecard['window']['start_iso']}..{scorecard['window']['end_iso']}",
        f"total orders={scorecard['today']['total']['orders']} fills={scorecard['today']['total']['fills']} "
        f"resolved={scorecard['today']['total']['resolved_fills']} rejects={scorecard['today']['total']['rejects']} "
        f"pnl={scorecard['today']['total']['pnl_usd']:+.6f}",
        "per_member:",
    ]
    basis = scorecard.get("day_pnl_basis") if isinstance(scorecard.get("day_pnl_basis"), dict) else {}
    goal_math = (
        scorecard.get("goal_arithmetic_invariant")
        if isinstance(scorecard.get("goal_arithmetic_invariant"), dict)
        else {}
    )
    if goal_math:
        goal_inputs = goal_math.get("inputs") if isinstance(goal_math.get("inputs"), dict) else {}
        lines.insert(
            2,
            "goal_arithmetic: "
            f"status={goal_math.get('status')} "
            f"max_achievable_day=${float(goal_math.get('max_achievable_day_usd') or 0.0):.6f} "
            f"goal_floor=${float(goal_math.get('goal_band_floor_usd') or 0.0):.2f} "
            f"cap=${float(goal_inputs.get('effective_fill_cap_usd') or 0.0):.2f} "
            f"fill_cap={goal_inputs.get('per_window_fill_cap')} "
            f"windows={goal_inputs.get('denominator_windows')} "
            f"edge={float(goal_inputs.get('measured_resolved_fill_roi_pct') or 0.0):+.3f}%",
        )
    if basis:
        coverage = basis.get("actual_basis_coverage") if isinstance(basis.get("actual_basis_coverage"), dict) else {}
        lines.insert(
            2,
            "day_pnl_basis: "
            f"primary={basis.get('primary_basis')} "
            f"day_response={basis.get('day_pnl_response_basis')} "
            f"day_actual={basis.get('day_pnl_actual_basis')} "
            f"basis_split={basis.get('basis_split_delta_usd')} "
            f"actual_coverage={coverage.get('joined')}/{coverage.get('total_resolved_fills')} "
            f"missing={coverage.get('missing')}",
        )
    resolution_split = (
        scorecard.get("day_pnl_resolution_split")
        if isinstance(scorecard.get("day_pnl_resolution_split"), dict)
        else {}
    )
    if resolution_split:
        lines.insert(
            2,
            "day_pnl_resolution_split: "
            f"status={resolution_split.get('status')} "
            f"realized_closed={float(resolution_split.get('realized_closed_pnl_usd') or 0.0):+.6f} "
            f"open_mark={float(resolution_split.get('open_mark_pnl_usd') or 0.0):+.6f} "
            f"unresolved_fills={int(resolution_split.get('unresolved_open_fills') or 0)} "
            f"open_cost={float(resolution_split.get('open_cost_usd') or 0.0):.6f} "
            f"bounds={resolution_split.get('unresolved_position_value_bounds_usd')}",
        )
    since_topup = scorecard.get("since_topup_truth") if isinstance(scorecard.get("since_topup_truth"), dict) else {}
    if since_topup:
        lines.insert(
            2,
            "since_topup_truth: "
            f"verdict={since_topup.get('primary_verdict')} "
            f"baseline=${since_topup.get('baseline_usd', 0.0):.2f} "
            f"start={since_topup.get('baseline_iso')} "
            f"cost_basis={since_topup.get('cost_basis_source')} "
            f"canonical_pnl={since_topup.get('canonical_pnl_usd', 0.0):+.6f}/"
            f"{since_topup.get('canonical_pnl_pct', 0.0):+.3f}% "
            f"actual_value={since_topup.get('actual_value_usd')} "
            f"actual_delta={since_topup.get('actual_delta_vs_baseline_usd')} "
            f"basis={since_topup.get('actual_value_basis')} "
            f"cash={since_topup.get('live_cash_balance_usd')} "
            f"status={since_topup.get('reconciliation_status')}",
        )
        overlay = (
            since_topup.get("self_feed_reconciliation_overlay")
            if isinstance(since_topup.get("self_feed_reconciliation_overlay"), dict)
            else {}
        )
        if overlay:
            lines.insert(
                3,
                "self_feed_reconciled_actual: "
                f"verdict={overlay.get('reconciled_verdict')} "
                f"ledger_actual={overlay.get('ledger_actual_delta_vs_baseline_usd')} "
                f"overlay_delta={overlay.get('overlay_delta_usd')} "
                f"double_count_excluded={overlay.get('double_count_excluded_usd')} "
                f"reconciled_actual={overlay.get('reconciled_actual_delta_vs_baseline_usd')} "
                f"mode={overlay.get('mode')} ledger_rewrite={overlay.get('ledger_rewrite')}",
            )
    for wallet, row in scorecard["today"]["per_member"].items():
        lines.append(
            f"  {wallet}: orders={row['orders']} fills={row['fills']} resolved={row['resolved_fills']} "
            f"rejects={row['rejects']} pnl={row['pnl_usd']:+.6f}"
        )
    by_lane = scorecard.get("by_lane") if isinstance(scorecard.get("by_lane"), dict) else {}
    if by_lane:
        lines.append("by_lane:")
        for lane, row in by_lane.items():
            lines.append(
                f"  {lane}: orders={row['orders']} fills={row['fills']} resolved={row['resolved_fills']} "
                f"rejects={row['rejects']} pnl={row['pnl_usd']:+.6f}"
            )
    baseline = scorecard["d97_prior_day_baseline"]
    lines.append(
        "d97_prior_day_baseline: "
        f"orders={baseline['total']['orders']} fills={baseline['total']['fills']} "
        f"resolved={baseline['total']['resolved_fills']} pnl={baseline['total']['pnl_usd']:+.6f} "
        f"delta_vs_today_set={scorecard['baseline_comparison']['today_set_minus_d97_prior_day_pnl_usd']:+.6f}"
    )
    lines.append("price_bands:")
    for bucket, row in scorecard["today"]["price_band_buckets"].items():
        lines.append(
            f"  {bucket}: orders={row['orders']} fills={row['fills']} resolved={row['resolved_fills']} "
            f"rejects={row['rejects']} pnl={row['pnl_usd']:+.6f}"
        )
    defects = [row for row in scorecard["automation_drift"] if row["pointer_status"] == "CONTENT_DEFECT"]
    volume = scorecard.get("volume_kpi", {})
    canonical = volume.get("canonical_daily") if isinstance(volume.get("canonical_daily"), dict) else {}
    scope_local = volume.get("scope_local") if isinstance(volume.get("scope_local"), dict) else {}
    lines.append(
        "volume_kpi: "
        f"windows_filled={canonical.get('windows_filled', volume.get('windows_traded', 0))}/"
        f"{canonical.get('denominator_windows', volume.get('windows_total', 0))} "
        f"filled_pct={canonical.get('windows_filled_pct', volume.get('windows_traded_pct', 0.0)):.2f} "
        f"windows_submitted={canonical.get('windows_submitted', volume.get('windows_submitted', 0))}/"
        f"{canonical.get('denominator_windows', volume.get('windows_total', 0))} "
        f"scope_local={scope_local.get('windows_traded', 0)}/{scope_local.get('windows_total', 0)} "
        f"gross_pct={(volume.get('coverage_gross') or {}).get('windows_traded_pct', 0.0):.2f} "
        f"ex_guard_pct={(volume.get('coverage_ex_guard') or {}).get('windows_traded_pct', 0.0):.2f} "
        f"taxonomy={json.dumps(volume.get('empty_window_taxonomy', {}), sort_keys=True)}"
    )
    window_histogram = (
        scorecard.get("per_window_pnl_histogram")
        if isinstance(scorecard.get("per_window_pnl_histogram"), dict)
        else {}
    )
    lines.append(
        "per_window_pnl_histogram: "
        f"reporting_only={window_histogram.get('reporting_only')} "
        f"gate_use_allowed={window_histogram.get('gate_use_allowed')} "
        f"resolved_windows={window_histogram.get('resolved_windows', 0)} "
        f"positive={window_histogram.get('positive_windows', 0)} "
        f"negative={window_histogram.get('negative_windows', 0)} "
        f"zero={window_histogram.get('zero_windows', 0)} "
        f"buckets={json.dumps(window_histogram.get('bucket_counts', {}), sort_keys=True)}"
    )
    execution = scorecard.get("execution_model_kpi") if isinstance(scorecard.get("execution_model_kpi"), dict) else {}
    drip = execution.get("drip") if isinstance(execution.get("drip"), dict) else {}
    strong = execution.get("strong_tier") if isinstance(execution.get("strong_tier"), dict) else {}
    lines.append(
        "execution_model_kpi: "
        f"orders_per_submitted_window={float(execution.get('orders_per_submitted_window') or 0.0):.3f} "
        f"orders_per_filled_window={float(execution.get('orders_per_filled_window') or 0.0):.3f} "
        f"fill_rate_pct={float(execution.get('fill_rate_pct') or 0.0):.2f} "
        f"drip_orders={int(drip.get('orders') or 0)} "
        f"drip_fills={int(drip.get('fills') or 0)} "
        f"drip_stop_saves={int(drip.get('drip_stop_saves') or 0)} "
        f"drip_avg_entry_minus_vwap={float(drip.get('avg_entry_minus_source_vwap') or 0.0):+.6f} "
        f"strong_orders={int(strong.get('orders') or 0)} "
        f"strong_resolved={int(strong.get('resolved_fills') or 0)} "
        f"strong_pnl={float(strong.get('resolved_pnl_usd') or 0.0):+.6f}"
    )
    roster = scorecard.get("active_set_roster") if isinstance(scorecard.get("active_set_roster"), dict) else {}
    skip_histogram = scorecard.get("guard_skip_histogram") if isinstance(scorecard.get("guard_skip_histogram"), dict) else {}
    lines.append(
        "active_set: "
        f"members={roster.get('qualified_member_count', 0)} "
        f"skip_histogram={json.dumps(skip_histogram.get('skip_reason_counts', {}), sort_keys=True)} "
        f"missed_active_windows={skip_histogram.get('missed_active_windows', 0)}"
    )
    paper_snapshots = scorecard.get("paper_lane_gate_snapshots") if isinstance(scorecard.get("paper_lane_gate_snapshots"), dict) else {}
    if paper_snapshots:
        lane_bits = []
        for name in ("e5_maker_first_btc5m", "e6_whale_net_flow"):
            row = paper_snapshots.get(name) if isinstance(paper_snapshots.get(name), dict) else {}
            summary = row.get("summary") if isinstance(row.get("summary"), dict) else {}
            gate = row.get("promotion_gate") if isinstance(row.get("promotion_gate"), dict) else {}
            lane_bits.append(
                f"{name}:fills={summary.get('filled_orders', summary.get('paper_filled_orders', 0))} "
                f"resolved={gate.get('resolved_paper_fills', 0)} pnl={gate.get('resolved_paper_pnl_usd', 0.0):+.2f} "
                f"gate={gate.get('promotion_50_resolved_positive')}"
            )
        lines.append("paper_lane_gates: " + " | ".join(lane_bits))
    target = scorecard.get("target_ladder", {})
    if target:
        actual = target.get("actual", {})
        phase_target = target.get("phase_target", {})
        process = target.get("process_actual", {})
        lines.append(
            "target_ladder: "
            f"phase={target.get('phase')} bankroll=${target.get('bankroll_usd', 0.0):.2f} "
            f"actual={actual.get('day_pnl_pct', 0.0):+.3f}%/${actual.get('day_pnl_usd', 0.0):+.2f} "
            f"phase_daily_min={phase_target.get('daily_min_run_rate_pct', 0.0):.3f}%/"
            f"${phase_target.get('daily_min_run_rate_usd', 0.0):.2f} "
            f"coverage={process.get('window_coverage_pct', 0.0):.2f}% "
            f"members={process.get('active_member_count', 0)}"
        )
    daily_how = scorecard.get("daily_how") if isinstance(scorecard.get("daily_how"), dict) else {}
    if daily_how:
        alternatives = daily_how.get("alternatives") if isinstance(daily_how.get("alternatives"), list) else []
        alt_bits = []
        for row in alternatives[:3]:
            if not isinstance(row, dict):
                continue
            alt_bits.append(
                f"{row.get('rank')}:{row.get('id')}=${float(row.get('expected_usd_today') or 0.0):.2f}"
            )
        lines.append(
            "daily_how: "
            f"path_to_band_today={daily_how.get('path_to_band_today')} "
            f"alternatives_generated={len(alternatives)} "
            f"ranked_options=[{'; '.join(alt_bits)}]"
        )
    pnl_truth = scorecard.get("canonical_pnl_truth", {})
    if pnl_truth:
        total = pnl_truth.get("total", {})
        discrepancy = scorecard.get("pnl_discrepancy", {})
        chain = scorecard.get("chain_reconciliation", {})
        snapshot = scorecard.get("resolution_snapshot", {})
        lines.append(
            "canonical_pnl_truth: "
            f"resolved={total.get('resolved_fills', 0)} pnl={total.get('pnl_usd', 0.0):+.6f} "
            f"source={pnl_truth.get('source')} cost_basis={pnl_truth.get('cost_basis_source')} "
            f"writeback_delta={discrepancy.get('itemized_delta_pnl_usd', 0.0):+.6f}"
        )
        lines.append(
            "resolution_snapshot: "
            f"status={snapshot.get('status')} rows={snapshot.get('rows', 0)} "
            f"age_s={snapshot.get('age_s')} pnl_citable={snapshot.get('pnl_citable')} "
            f"path={snapshot.get('path')}"
        )
        lines.append(
            "chain_reconciliation: "
            f"status={chain.get('status')} cash={chain.get('live_cash_balance_usd')} "
            f"positions={((chain.get('open_position_value') or {}).get('open_position_value_usd'))} "
            f"expected={chain.get('expected_value_usd')} cash_identity={chain.get('expected_cash_identity_usd')} "
            f"pnl_scope={chain.get('canonical_pnl_scope')} start={chain.get('reconciliation_start_iso')} "
            f"position_bounds={chain.get('unresolved_position_value_bounds_usd')} "
            f"delta={chain.get('delta_vs_expected_usd')} cash_delta={chain.get('cash_delta_vs_expected_identity_usd')} "
            f"balance_status={chain.get('balance_status')}"
        )
    self_feed = scorecard.get("self_feed_vs_ledger") if isinstance(scorecard.get("self_feed_vs_ledger"), dict) else {}
    self_summary = self_feed.get("summary") if isinstance(self_feed.get("summary"), dict) else {}
    if self_feed:
        lines.append(
            "self_feed_vs_ledger: "
            f"status={self_feed.get('status')} "
            f"matched={self_summary.get('matched_ledger_tx_groups', 0)}/"
            f"{self_summary.get('ledger_filled_tx_groups', 0)} "
            f"self_feed_tx={self_summary.get('self_feed_tx_groups', 0)} "
            f"data_api_rows={self_summary.get('data_api_trade_rows', 0)} "
            f"polygon_rows={self_summary.get('polygon_orderfilled_rows', 0)} "
            f"ledger_missing={self_summary.get('ledger_missing_self_feed_critical', 0)} "
            f"self_missing={self_summary.get('self_feed_missing_ledger_critical', 0)} "
            f"amount_mismatch={self_summary.get('amount_mismatch_tx_groups', 0)} "
            f"price_rounding={self_summary.get('price_rounding_mismatch_tx_groups', 0)} "
            f"split_groups={self_summary.get('probable_split_fill_groups', 0)} "
            f"missing_cost={self_summary.get('self_feed_missing_ledger_cost_usd')} "
            f"missing_pnl={self_summary.get('self_feed_missing_ledger_pnl_usd')}"
        )
    proof = scorecard.get("proof_gates", {})
    if proof:
        phase2 = proof.get("phase2_promotion", {})
        sizing = proof.get("sizing_ramp", {})
        lane = proof.get("lane_promotion", {})
        paper_lanes = lane.get("paper_lanes") if isinstance(lane.get("paper_lanes"), list) else []
        lane_bits = [
            f"{row.get('lane')}={row.get('resolved_paper_fills', 0)}/"
            f"{row.get('target_resolved_paper_fills', 0)} pnl={row.get('paper_pnl_usd', 0.0):+.2f}"
            for row in paper_lanes
            if isinstance(row, dict)
        ]
        lines.append(
            "proof_gates: "
            f"phase2_live_resolved={phase2.get('resolved_live_fills', 0)}/"
            f"{phase2.get('target_resolved_live_fills', 0)} "
            f"positive_days={phase2.get('positive_pnl_days', 0)}/"
            f"{phase2.get('target_positive_pnl_days', 0)} "
            f"live_pnl={phase2.get('cumulative_live_pnl_usd', 0.0):+.2f} "
            f"rolling100={phase2.get('current_rolling_100_pnl_usd', 0.0):+.2f} "
            f"sizing_block={sizing.get('fills_into_current_block', 0)}/"
            f"{sizing.get('block_size_resolved_fills', 0)} "
            f"reported_size=${sizing.get('reported_unlocked_size_usd', 0.0):.2f} "
            f"lane_paper={' '.join(lane_bits) if lane_bits else 'none'}"
        )
    engine = scorecard.get("engine_race_gates", {})
    engine_lanes = engine.get("lanes") if isinstance(engine.get("lanes"), list) else []
    if engine_lanes:
        bits = [
            f"{row.get('lane')} quotes={row.get('paper_quotes', 0)} fills={row.get('paper_filled_orders', 0)} "
            f"fill_rate={row.get('maker_fill_rate_pct', 0.0):.2f}% resolved={row.get('resolved_paper_fills', 0)}/"
            f"{row.get('target_resolved_paper_fills', 0)} W-L={row.get('resolved_paper_wins', 0)}-"
            f"{row.get('resolved_paper_losses', 0)} pnl={row.get('resolved_paper_pnl_usd', 0.0):+.2f} "
            f"roi={row.get('resolved_paper_roi_pct', 0.0):+.2f}%"
            for row in engine_lanes
            if isinstance(row, dict)
        ]
        lines.append(
            "engine_race_gates: "
            f"ready={','.join(engine.get('gate_ready_lanes', [])) or 'none'} "
            f"parity_violations={engine.get('parity_violations', 0)} "
            + " | ".join(bits)
        )
    factory = scorecard.get("member_factory_kpi") if isinstance(scorecard.get("member_factory_kpi"), dict) else {}
    if factory:
        queue = factory.get("queue_depth") if isinstance(factory.get("queue_depth"), dict) else {}
        trajectory = factory.get("set_trajectory") if isinstance(factory.get("set_trajectory"), dict) else {}
        freshness = factory.get("member_freshness") if isinstance(factory.get("member_freshness"), dict) else {}
        hours = factory.get("hour_coverage") if isinstance(factory.get("hour_coverage"), dict) else {}
        throughput = factory.get("factory_throughput") if isinstance(factory.get("factory_throughput"), dict) else {}
        series = factory.get("series_census") if isinstance(factory.get("series_census"), dict) else {}
        series_rows = series.get("series") if isinstance(series.get("series"), dict) else {}
        lines.append(
            "member_factory_kpi: "
            f"ready={queue.get('ready_for_live', 0)}/{queue.get('target_ready_for_live', 0)} "
            f"depth={queue.get('queue_depth', 0)} status={queue.get('status', 'UNKNOWN')} "
            f"members={trajectory.get('member_count', 0)} "
            f"delta={trajectory.get('member_count_delta')} basis={trajectory.get('compare_basis')} "
            f"stale={len(freshness.get('stale_members') or [])} "
            f"hour_coverage={hours.get('covered_hours', 0)}/{hours.get('denominator_hours', 24)} "
            f"replay={throughput.get('replay_candidates_total', 0)} "
            f"promotable={throughput.get('replay_promotable', 0)} "
            f"series_btc5m={((series_rows.get('btc_5m') or {}).get('unique_windows_24h'))}/288 "
            f"series_total={(series.get('total_across_series') or {}).get('unique_windows_24h')} "
            f"defects={len(factory.get('defects') or [])}"
        )
    late = scorecard.get("late_window_cohort", {})
    if late:
        experiment = late.get("experiment_cohort") if isinstance(late.get("experiment_cohort"), dict) else {}
        lines.append(
            "late_window_cohort: "
            f"experiment_030_060 fills={experiment.get('fills', 0)} "
            f"resolved={experiment.get('resolved_fills', 0)} "
            f"pnl={experiment.get('pnl_usd', 0.0):+.6f} "
            f"tripwire={bool((late.get('experiment') or {}).get('tripwire_triggered'))}"
        )
    expansion = scorecard.get("expansion_cohort") if isinstance(scorecard.get("expansion_cohort"), dict) else {}
    if expansion:
        summary = expansion.get("summary") if isinstance(expansion.get("summary"), dict) else {}
        rule = expansion.get("raise_rule") if isinstance(expansion.get("raise_rule"), dict) else {}
        bits = []
        for row in expansion.get("members") or []:
            if not isinstance(row, dict):
                continue
            bits.append(
                f"{str(row.get('source_wallet') or '')[-6:]} attempts={row.get('submit_attempts', 0)} "
                f"fills={row.get('fills', 0)} resolved={row.get('resolved_fills', 0)} "
                f"pnl={row.get('resolved_pnl_usd', 0.0):+.6f}"
            )
        lines.append(
            "expansion_cohort: "
            f"members_with_resolved_fill={summary.get('members_with_resolved_fill', 0)}/"
            f"{summary.get('members', 0)} "
            f"pnl={summary.get('resolved_pnl_usd', 0.0):+.6f} "
            f"raise_ready_now={bool(rule.get('raise_to_10_ready_now'))} "
            f"[{' | '.join(bits)}]"
        )
    shadow = scorecard.get("ready_shadow_lanes") if isinstance(scorecard.get("ready_shadow_lanes"), dict) else {}
    if shadow:
        summary = shadow.get("summary") if isinstance(shadow.get("summary"), dict) else {}
        lanes = shadow.get("lanes") if isinstance(shadow.get("lanes"), list) else []
        progress = ", ".join(
            f"{str(row.get('wallet') or '')[-6:]}:{row.get('resolved_paper_fills', 0)}/"
            f"{row.get('promotion_resolved_fill_gate', summary.get('promotion_resolved_fill_gate', 50))}"
            for row in lanes[:5]
            if isinstance(row, dict)
        )
        lines.append(
            "ready_shadow_lanes: "
            f"lanes={summary.get('lane_count', 0)} gate_crossed={summary.get('gate_crossed', 0)} "
            f"min_gap={summary.get('min_resolved_fill_gap', 0)} progress=[{progress}]"
        )
    lines.append(f"automation_drift: entries={len(scorecard['automation_drift'])} content_defects={len(defects)}")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    if not args.resolutions:
        args.resolutions = _default_resolutions_path()
    day, start_ts, end_ts = _day_bounds(args.day)
    ledger = load_json(args.ledger, default={})
    orders = ledger.get("orders") if isinstance(ledger.get("orders"), list) else []
    reconciliation_start_ts = _parse_utc_ts(str(args.reconciliation_start or ""))
    order_scopes = _order_time_scopes(
        orders,
        start_ts=start_ts,
        end_ts=end_ts,
        reconciliation_start_ts=reconciliation_start_ts,
    )
    day_orders = order_scopes["day_orders"]
    previous_day_orders = order_scopes["previous_day_orders"]
    since_reconciliation_orders = order_scopes["since_reconciliation_orders"]
    resolutions = load_resolutions(args.resolutions)
    validate_resolutions_nonempty_for_fills(ledger, resolutions, resolutions_path=args.resolutions)
    snapshot_status = resolution_snapshot_status(args.resolutions, resolutions)
    guard_state = load_json(args.guard_state, default={})
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    ready_shadow = load_json(args.ready_shadow_state, default={})
    ready_shadow = ready_shadow if isinstance(ready_shadow, dict) else {}
    peer_active_idle_reset_state = load_json(args.peer_active_idle_reset_state, default={})
    peer_active_idle_reset_state = (
        peer_active_idle_reset_state if isinstance(peer_active_idle_reset_state, dict) else {}
    )
    observed_receipt_costs, receipt_cost_basis = _load_receipt_costs()
    # Receipt debits are an observation surface, not authority to reprice a
    # fill after it was banked. All canonical money paths keep this empty.
    receipt_costs: dict[str, float] = {}
    actual_trade_costs, actual_trade_cost_basis = _load_actual_trade_costs()
    actual_cost_backfill_watermark_ts = _actual_cost_backfill_coverage_watermark_ts()
    unjoined_actual_gap_start_ts = max(
        float(start_ts),
        float(actual_cost_backfill_watermark_ts or start_ts),
    )
    response_day_truth = build_pnl_truth(
        {"orders": day_orders},
        resolutions,
        start_ts=start_ts,
        end_ts=end_ts,
        receipt_costs={},
        actual_trade_costs={},
    )
    actual_day_truth = build_pnl_truth(
        {"orders": day_orders},
        resolutions,
        start_ts=start_ts,
        end_ts=end_ts,
        receipt_costs={},
        actual_trade_costs=actual_trade_costs,
    )
    today = _score_from_truth(response_day_truth)
    today_actual_basis = _score_from_truth(actual_day_truth)
    actual_basis_coverage = _actual_basis_coverage(actual_day_truth)
    basis_split_delta = _basis_split_delta(today, today_actual_basis)
    baseline = _score_orders(
        previous_day_orders,
        resolutions,
        start_ts=start_ts - 86400,
        end_ts=start_ts,
        wallet_filter=D97,
        receipt_costs={},
        actual_trade_costs={},
    )
    canonical_today = response_day_truth
    lifetime_truth = build_pnl_truth(
        {"orders": orders},
        resolutions,
        receipt_costs=receipt_costs,
        actual_trade_costs=actual_trade_costs,
    )
    lifetime_unresolved_bounds = unresolved_position_bounds(lifetime_truth)
    if args.offline_no_chain:
        reconciliation, reconciliation_truth, reconciliation_unresolved_bounds = _offline_no_chain_reconciliation_scope(
            since_reconciliation_orders,
            resolutions,
            baseline_usd=float(args.bankroll_usd),
            reconciliation_start=str(args.reconciliation_start or ""),
            receipt_costs=receipt_costs,
            actual_trade_costs=actual_trade_costs,
        )
    else:
        reconciliation, reconciliation_truth, reconciliation_unresolved_bounds = _chain_reconciliation_scope(
            since_reconciliation_orders,
            resolutions,
            baseline_usd=float(args.bankroll_usd),
            reconciliation_start=str(args.reconciliation_start or ""),
            receipt_costs=receipt_costs,
            actual_trade_costs=actual_trade_costs,
            unjoined_actual_gap_start_ts=unjoined_actual_gap_start_ts,
            balance_sample_count=int(args.balance_sample_count),
            balance_sample_interval_s=float(args.balance_sample_interval_s),
            balance_unavailable_resample_count=int(args.balance_unavailable_resample_count),
            balance_unavailable_resample_interval_s=float(args.balance_unavailable_resample_interval_s),
            balance_mismatch_resample_count=int(args.balance_mismatch_resample_count),
            balance_mismatch_resample_interval_s=float(args.balance_mismatch_resample_interval_s),
        )
    volume_kpi = _volume_kpi(args.guard_state, orders=day_orders, start_ts=start_ts, end_ts=end_ts)
    since_topup = _since_topup_truth(reconciliation, reconciliation_truth)
    balance_feed_monitor = _balance_feed_monitor(reconciliation)
    self_feed_overlay = _self_feed_reconciliation_overlay()
    since_topup = _apply_self_feed_reconciliation_overlay(since_topup, self_feed_overlay)
    cash_diff_residual = _cash_diff_reconciliation_residual()
    retrace_equation = _retrace_reconciliation_equation()
    since_topup = _apply_cash_diff_reconciliation_residual(since_topup, cash_diff_residual, retrace_equation)
    reconciliation = _annotate_chain_reconciliation(reconciliation, since_topup)
    direction_leaderboard = build_leaderboard(ROOT)
    atomic_write_json(DIRECTION_LEADERBOARD, direction_leaderboard)
    scorecard = {
        "kind": "wallet_copy_daily_scorecard",
        "flow_stage": "LIVE/ROTATE/SELF-DEV",
        "generated_at": utc_now_iso(),
        "day_utc": day,
        "ledger": args.ledger,
        "guard_state": args.guard_state,
        "resolutions": args.resolutions,
        "cost_basis_source": "response_filled_size_usd",
        "scorecard_basis": (
            "offline_no_chain_local_ledger_fill_data_only"
            if args.offline_no_chain
            else "chain_reconciled_with_local_ledger_fill_data"
        ),
        "chain_reconciliation_available": not bool(args.offline_no_chain),
        "pnl_claim_rule": (
            "machine_money_truth_only_until_full_chain_reconciliation_repair"
            if args.offline_no_chain
            else "chain_reconciliation_available"
        ),
        "basis_warning": (
            "chain reconciliation UNAVAILABLE; no ledger rewrite and no silent basis switch"
            if args.offline_no_chain
            else ""
        ),
        "day_pnl_response_basis": today["total"]["pnl_usd"],
        "day_pnl_actual_basis": today_actual_basis["total"]["pnl_usd"],
        "actual_basis_coverage": actual_basis_coverage,
        "basis_split_delta_usd": basis_split_delta,
        "day_pnl_resolution_split": _day_pnl_resolution_split(response_day_truth),
        "day_pnl_basis": {
            "primary_basis": "response_filled_size_usd",
            "secondary_basis": "actual_trade_record",
            "fallback_basis": "response_filled_size_usd",
            "day_pnl_response_basis": today["total"]["pnl_usd"],
            "day_pnl_actual_basis": today_actual_basis["total"]["pnl_usd"],
            "basis_split_delta_usd": basis_split_delta,
            "actual_basis_coverage": actual_basis_coverage,
            "promotion_rule": {
                "actual_trade_record_primary_when": "fills_missing_tx=0 and abs(cash_diff_residual_usd)<=2 or named_cash_movement, by explicit Fable DIRECTION",
                "silent_flip_allowed": False,
            },
        },
        "receipt_cost_basis": receipt_cost_basis,
        "receipt_cost_observation": {
            "rows": len(observed_receipt_costs),
            "accounting_authority": False,
            "banked_cost_basis_immutable": True,
        },
        "actual_trade_cost_basis": actual_trade_cost_basis,
        "cash_diff_reconciliation_residual": cash_diff_residual,
        "retrace_reconciliation_equation": retrace_equation,
        "actual_cost_backfill_coverage_watermark_ts": _round_or_none(actual_cost_backfill_watermark_ts),
        "unjoined_actual_gap_start_ts": _round_or_none(unjoined_actual_gap_start_ts),
        "resolution_snapshot": snapshot_status,
        "scorecard_order_scopes": order_scopes["summary"],
        "window": {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "start_iso": datetime.fromtimestamp(start_ts, tz=UTC).isoformat().replace("+00:00", "Z"),
            "end_iso": datetime.fromtimestamp(end_ts, tz=UTC).isoformat().replace("+00:00", "Z"),
        },
        "today": today,
        "today_actual_basis": today_actual_basis,
        "d97_prior_day_baseline": baseline,
        "baseline_comparison": {
            "today_set_minus_d97_prior_day_pnl_usd": round(
                float(today["total"]["pnl_usd"]) - float(baseline["total"]["pnl_usd"]), 6
            )
        },
        "by_lane": today.get("by_lane", {}),
        "direction_leaderboard": direction_leaderboard,
        "volume_kpi": volume_kpi,
        "peer_active_idle_windows": _peer_active_idle_windows(
            volume_kpi,
            end_ts=end_ts,
            reset_state=peer_active_idle_reset_state,
        ),
        "per_window_pnl_histogram": today.get("per_window_pnl_histogram", {}),
        "actual_basis_spot_audit": _actual_basis_spot_audit(
            response_day_truth,
            actual_day_truth,
            actual_trade_costs,
            limit=5,
        ),
        "basis_split_decomposition": _basis_split_decomposition(
            response_day_truth,
            actual_day_truth,
            actual_trade_costs,
            basis_split_delta_usd=basis_split_delta,
        ),
        "execution_model_kpi": _execution_model_kpi(
            day_orders,
            resolutions,
            guard_state,
            start_ts=start_ts,
            end_ts=end_ts,
            receipt_costs=receipt_costs,
            actual_trade_costs=actual_trade_costs,
        ),
        "window_coverage": _window_coverage(volume_kpi),
        "late_window_cohort": _late_window_cohort(
            day_orders,
            resolutions,
            start_ts=start_ts,
            end_ts=end_ts,
            receipt_costs=receipt_costs,
            actual_trade_costs=actual_trade_costs,
        ),
        "expansion_cohort": _expansion_cohort(
            day_orders,
            resolutions,
            guard_state,
            start_ts=start_ts,
            end_ts=end_ts,
            receipt_costs=receipt_costs,
            actual_trade_costs=actual_trade_costs,
        ),
        "active_set_roster": _active_set_roster(guard_state),
        "guard_skip_histogram": _guard_skip_histogram(guard_state),
        "live_book_age_gate_evidence": _live_book_age_gate_evidence(guard_state),
        "paper_lane_gate_snapshots": _paper_lane_gate_snapshots(),
        "canonical_pnl_truth": canonical_today,
        "lifetime_pnl_truth": {
            key: value
            for key, value in lifetime_truth.items()
            if key != "events"
        },
        "standing_price_band_evidence": _standing_price_band_evidence(lifetime_truth),
        "reconciliation_pnl_truth": {
            key: value
            for key, value in reconciliation_truth.items()
            if key != "events"
        },
        "since_topup_truth": since_topup,
        "unresolved_position_bounds": lifetime_unresolved_bounds,
        "reconciliation_unresolved_position_bounds": reconciliation_unresolved_bounds,
        "pnl_discrepancy": discrepancy_report(ledger, lifetime_truth),
        "chain_reconciliation": reconciliation,
        "balance_feed_monitor": balance_feed_monitor,
        "self_feed_vs_ledger": _self_feed_vs_ledger(),
        "self_feed_reconciliation_overlay": self_feed_overlay,
        "ready_shadow_lanes": ready_shadow,
        "member_factory_kpi": _member_factory_kpi(),
        "automation_drift": _automation_drift(),
    }
    scorecard["pipeline_slo_and_standby_readiness"] = build_pipeline_slo_report(
        ready_shadow=ready_shadow,
        full_pool_queue=load_json(ROOT / "data/research/wallet_copy_full_pool_member_queue.json", default={}),
        structural_scalp=load_json(ROOT / "data/research/btc5m_structural_scalp_paper_lane_state.json", default={}),
        structural_scalp_promotion=load_json(
            ROOT / "data/research/btc5m_structural_scalp_promotion_prep_latest.json",
            default={},
        ),
        volume_standby_promotion=load_json(
            ROOT / "data/research/13e0_exact_policy_promotion_packet_latest.json",
            default={},
        ),
        wide_standby_binding=load_json(
            ROOT / "data/research/82c8_wide_standby_binding_latest.json",
            default={},
        ),
        wide_supervisor_heartbeat=read_wide_supervisor_heartbeat(),
    )
    previous_day = (datetime.fromisoformat(day).date() - timedelta(days=1)).isoformat()
    previous_scorecard = load_json(
        ROOT / f"data/research/wallet_copy_daily_scorecard_{previous_day}.json",
        default={},
    )
    scorecard["defense_regret"] = _defense_regret_metric(
        day,
        float(today["total"]["pnl_usd"]),
        load_json(DEFAULT_ROUTING_SHADOW_STATE, default={}),
        previous_scorecard=previous_scorecard,
    )
    scorecard["target_ladder"] = _target_ladder(scorecard, guard_state, bankroll_usd=float(args.bankroll_usd))
    scorecard["goal_arithmetic_invariant"] = _goal_arithmetic_invariant(scorecard, guard_state)
    scorecard["daily_how"] = _daily_how_line(scorecard)
    scorecard["proof_gates"] = _proof_gates(
        orders,
        resolutions,
        receipt_costs=receipt_costs,
        actual_trade_costs=actual_trade_costs,
    )
    scorecard["engine_race_gates"] = _engine_race_gates()
    _write_scorecard_outputs(args.output, scorecard)
    if args.format == "text":
        print(_text(scorecard))
    else:
        print(json.dumps(scorecard, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
