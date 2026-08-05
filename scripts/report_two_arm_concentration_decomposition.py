#!/usr/bin/env python3
"""Decompose the 31c2 unsliced and 82c8 forward-seat paper arms."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_positive_wallet_slice_selector_falsifier import (  # noqa: E402
    _jsonl,
    _score_wallet_events,
)
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import (  # noqa: E402
    atomic_write_json,
    json_file_lock,
    load_json,
)

WALLET_31C2 = "0x31c290a2772e1e3143bcb6debbdbbf08ac081d13"
WALLET_82C8 = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
SEAT_CLOCK_START = "2026-07-29T06:44:50.331867Z"
SEAT_DECISION_AT = "2026-07-31T06:44:50Z"
LIVE_CELL_82C8 = "cell_scoped_e4debe50da77_bac25bed"
DEFAULT_HISTORY = "data/research/wallet_copy_history_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_WIDE_STATE = "data/research/wide_exact_policy_paper_state.json"
DEFAULT_RUNTIME_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT = "data/research/two_arm_concentration_decomposition_latest.json"


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = fraction * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _summarize(
    rows: list[dict[str, Any]],
    *,
    min_price: float = 0.0,
    market_domain: str = "btc5m_only",
    derive_btc5m_market_from_timestamp: bool = False,
) -> dict[str, Any]:
    if market_domain not in {"btc5m_only", "all_rows"}:
        raise ValueError(f"unsupported market_domain: {market_domain}")
    market_eligible = [
        row
        for row in rows
        if (
            market_domain == "all_rows"
            or str(row.get("market_slug") or "").startswith("btc-updown-5m-")
        )
    ]
    eligible = [
        row
        for row in market_eligible
        if num(row.get("fill_price")) >= min_price
    ]
    pnls = [num(row.get("post_fee_pnl_usd")) for row in eligible]
    prices = [num(row.get("fill_price")) for row in eligible]
    total_pnl = sum(pnls)
    market_pnl: dict[str, float] = defaultdict(float)
    missing_market_identity_rows = 0
    for index, row in enumerate(eligible):
        market_identity = str(
            row.get("market_slug") or row.get("condition_id") or ""
        )
        alpha_move_slice = row.get("alpha_move_slice") or {}
        if (
            not market_identity
            and (
                derive_btc5m_market_from_timestamp
                or str(alpha_move_slice.get("market_type") or "") == "btc_5m"
            )
            and num(row.get("source_event_ts")) > 0
        ):
            market_start_s = int(num(row.get("source_event_ts"))) // 300 * 300
            market_identity = f"btc-updown-5m-{market_start_s}"
        if not market_identity:
            missing_market_identity_rows += 1
            market_identity = f"__missing_market_identity_{index}"
        market_pnl[market_identity] += num(row.get("post_fee_pnl_usd"))
    ranked_markets = sorted(
        market_pnl.items(), key=lambda item: (-item[1], item[0])
    )

    def share(value: float) -> float | None:
        return (
            round(100.0 * value / abs(total_pnl), 6)
            if total_pnl > 0.0
            else None
        )

    wins = sum(pnl > 0.0 for pnl in pnls)
    best_trade = max(pnls) if pnls else 0.0
    top_one_pnl = ranked_markets[0][1] if ranked_markets else 0.0
    top_five_pnl = sum(value for _, value in ranked_markets[:5])
    return {
        "min_price": min_price,
        "market_domain": market_domain,
        "market_identity_strategy": (
            "slug_then_condition_then_btc5m_timestamp"
            if derive_btc5m_market_from_timestamp
            else "slug_then_condition_then_declared_btc5m_timestamp"
        ),
        "input_rows": len(rows),
        "excluded_market_domain_rows": len(rows) - len(market_eligible),
        "missing_market_identity_rows": missing_market_identity_rows,
        "resolved": len(eligible),
        "distinct_markets": len(market_pnl),
        "post_fee_pnl_usd": round(total_pnl, 6),
        "wins": wins,
        "losses": len(eligible) - wins,
        "win_rate_pct": round(100.0 * wins / len(eligible), 6)
        if eligible
        else None,
        "top_1_market": ranked_markets[0][0] if ranked_markets else None,
        "top_1_market_pnl_usd": round(top_one_pnl, 6),
        "top_1_market_share_of_total_pnl_pct": share(top_one_pnl),
        "top_5_market_pnl_usd": round(top_five_pnl, 6),
        "top_5_market_share_of_total_pnl_pct": share(top_five_pnl),
        "single_best_trade_pnl_usd": round(best_trade, 6),
        "single_best_trade_share_of_total_pnl_pct": share(best_trade),
        "price_distribution": {
            "min": round(min(prices), 6) if prices else None,
            "p10": round(_percentile(prices, 0.10) or 0.0, 6)
            if prices
            else None,
            "median": round(_percentile(prices, 0.50) or 0.0, 6)
            if prices
            else None,
            "max": round(max(prices), 6) if prices else None,
            "count_lt_0_10": sum(price < 0.10 for price in prices),
            "count_lt_0_05": sum(price < 0.05 for price in prices),
        },
    }


def _arm_verdict(
    base: dict[str, Any],
    floor_005: dict[str, Any],
    floor_010: dict[str, Any],
) -> dict[str, Any]:
    top_one = num(base.get("top_1_market_share_of_total_pnl_pct"), float("inf"))
    win_rate = num(base.get("win_rate_pct"), 0.0)
    artifact_reasons = []
    if top_one >= 50.0:
        artifact_reasons.append("top_1_market_share_gte_50pct")
    if win_rate < 50.0:
        artifact_reasons.append("win_rate_lt_50pct")
    if num(floor_005.get("post_fee_pnl_usd")) < 0.0:
        artifact_reasons.append("min_price_0_05_pnl_negative")
    genuine = bool(
        num(floor_010.get("post_fee_pnl_usd")) > 0.0
        and top_one < 25.0
        and win_rate >= 50.0
    )
    if "top_1_market_share_gte_50pct" in artifact_reasons:
        classification = "CONCENTRATION_ARTEFACT"
    elif "min_price_0_05_pnl_negative" in artifact_reasons:
        classification = "PRICE_FLOOR_FRAGILE"
    elif "win_rate_lt_50pct" in artifact_reasons:
        classification = "LOW_WIN_RATE_ARTEFACT"
    elif genuine:
        classification = "GENUINE_MEASURED_EDGE"
    else:
        classification = "MIXED_NO_PREREGISTERED_THRESHOLD"
    return {
        "classification": classification,
        "primary_artifact_reason": artifact_reasons[0] if artifact_reasons else None,
        "artifact_reasons": artifact_reasons,
        "genuine_edge_conditions_pass": genuine,
    }


def _seat_rows(wide_state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in wide_state.get("orders") or []:
        if not isinstance(row, dict):
            continue
        terminal = row.get("f1_f4_terminal") or {}
        if (
            str(row.get("wallet") or "").lower() != WALLET_82C8
            or str(row.get("recorded_at") or "") < SEAT_CLOCK_START
            or terminal.get("terminal") != "COPYABLE_EXACT_POLICY_PAPER_FILL"
            or row.get("resolved") is not True
        ):
            continue
        rows.append(
            {
                "event_id": row.get("order_id"),
                "event_ts": row.get("source_event_ts"),
                "market_slug": row.get("market_slug"),
                "fill_price": row.get("fill_price"),
                "post_fee_pnl_usd": row.get("post_fee_pnl_usd"),
            }
        )
    return rows


def _live_cell_loss_disabled(runtime_state: dict[str, Any]) -> bool:
    active_set = (
        runtime_state.get("active_set_runtime")
        or runtime_state.get("runtime_active_set")
        or {}
    )
    total_loss = active_set.get("total_loss_auto_disable") or {}
    return any(
        isinstance(row, dict)
        and str(row.get("candidate_id") or "") == LIVE_CELL_82C8
        for row in total_loss.get("disabled_members") or []
    )


def _arm(name: str, rows: list[dict[str, Any]], **identity: Any) -> dict[str, Any]:
    base = _summarize(rows)
    floor_005 = _summarize(rows, min_price=0.05)
    floor_010 = _summarize(rows, min_price=0.10)
    return {
        "arm": name,
        **identity,
        "base": base,
        "min_price_0_05": floor_005,
        "min_price_0_10": floor_010,
        "verdict": _arm_verdict(base, floor_005, floor_010),
    }


def build_report(
    *,
    history: dict[str, Any],
    resolutions: list[dict[str, Any]],
    wide_state: dict[str, Any],
    runtime_state: dict[str, Any],
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    now = now or dt.datetime.now(dt.timezone.utc)
    scored_31c2, unresolved_31c2 = _score_wallet_events(
        [
            row
            for row in history.get("events") or []
            if isinstance(row, dict)
        ],
        resolutions,
        wallet=WALLET_31C2,
    )
    arm_31c2 = _arm(
        "31c2_unsliced",
        scored_31c2,
        wallet=WALLET_31C2,
        unresolved_events=unresolved_31c2,
    )
    arm_82c8 = _arm(
        "82c8_forward_seat_clock",
        _seat_rows(wide_state),
        wallet=WALLET_82C8,
        clock_start=SEAT_CLOCK_START,
        decision_at=SEAT_DECISION_AT,
    )
    live_cell_loss_disabled = _live_cell_loss_disabled(runtime_state)
    seat_passes_decomposition = bool(
        arm_82c8["verdict"]["genuine_edge_conditions_pass"]
    )
    maturity_checks = {
        "decomposition_clears_genuine_edge_bar": seat_passes_decomposition,
        "live_cell_not_loss_disabled": not live_cell_loss_disabled,
        "maturity_reached": now
        >= dt.datetime.fromisoformat(SEAT_DECISION_AT.replace("Z", "+00:00")),
    }
    return {
        "schema_version": 1,
        "kind": "two_arm_concentration_decomposition",
        "flow_stage": "LEARN/PROMOTE/OBSERVE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "arms": [arm_31c2, arm_82c8],
        "preregistered_rule": {
            "artifact": (
                "top-1 market share >=50% OR win rate <50% OR "
                "min_price=0.05 PnL <0"
            ),
            "genuine_measured_edge": (
                "min_price=0.10 PnL >0 AND top-1 market share <25% "
                "AND win rate >=50%"
            ),
        },
        "seat_82c8_maturity_precommit": {
            "wallet": WALLET_82C8,
            "live_cell": LIVE_CELL_82C8,
            "decision_at": SEAT_DECISION_AT,
            "checks": maturity_checks,
            "decision": (
                "ELIGIBLE_FOR_FABLE_PROMOTION_HANDOFF"
                if all(maturity_checks.values())
                else "PARK_AT_MATURITY"
            ),
            "third_48h_clock_allowed": False,
            "authority": "fable DIRECTION 2026-07-30T03:54:22Z order_2",
        },
        "authority": (
            "measurement and maturity precommit only; no live gate, cap, "
            "roster, flag, policy, or submitter mutation"
        ),
    }


def apply_seat_maturity(
    ready_state: dict[str, Any],
    report: dict[str, Any],
    *,
    now: dt.datetime,
) -> dict[str, Any]:
    precommit = report["seat_82c8_maturity_precommit"]
    decision_at = dt.datetime.fromisoformat(
        str(precommit["decision_at"]).replace("Z", "+00:00")
    )
    if now < decision_at:
        raise ValueError(f"82c8 forward-seat decision is not due before {SEAT_DECISION_AT}")
    prior = ready_state.get("terminal_82c8_forward_seat_decision") or {}
    if prior.get("status") == "PARK_CONCENTRATED_PAPER_SEAT":
        return ready_state
    if precommit.get("decision") != "PARK_AT_MATURITY":
        return ready_state
    lanes = [
        row
        for row in ready_state.get("lanes") or []
        if not (
            isinstance(row, dict)
            and str(row.get("wallet") or "").lower() == WALLET_82C8
            and str(row.get("standby_evidence_started_at") or "")
            == SEAT_CLOCK_START
        )
    ]
    adjudications = [
        row
        for row in ready_state.get("standby_adjudications") or []
        if isinstance(row, dict)
    ]
    status = "PARK_CONCENTRATED_PAPER_SEAT"
    if not any(
        str(row.get("wallet") or "").lower() == WALLET_82C8
        and row.get("status") == status
        for row in adjudications
    ):
        adjudications.append(
            {
                "wallet": WALLET_82C8,
                "status": status,
                "adjudicated_at": now.astimezone(dt.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "clock_start": SEAT_CLOCK_START,
                "decision_at": SEAT_DECISION_AT,
                "decomposition_verdict": next(
                    arm["verdict"]
                    for arm in report["arms"]
                    if arm["arm"] == "82c8_forward_seat_clock"
                ),
                "live_cell_not_loss_disabled": precommit["checks"][
                    "live_cell_not_loss_disabled"
                ],
                "third_48h_clock_allowed": False,
                "slot_action": "RELEASED_TO_NEXT_UNSPENT_WALLET",
                "paper_only": True,
                "live_orders_allowed": False,
                "authority": precommit["authority"],
            }
        )
    return {
        **ready_state,
        "lanes": lanes,
        "standby_adjudications": adjudications,
        "summary": {
            **(ready_state.get("summary") or {}),
            "lane_count": len(lanes),
            "82c8_forward_seat_terminalized": True,
        },
        "terminal_82c8_forward_seat_decision": {
            "status": status,
            "executed_at": now.astimezone(dt.timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "clock_start": SEAT_CLOCK_START,
            "decision_at": SEAT_DECISION_AT,
            "immutable": True,
            "third_48h_clock_allowed": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--wide-state", default=DEFAULT_WIDE_STATE)
    parser.add_argument("--runtime-state", default=DEFAULT_RUNTIME_STATE)
    parser.add_argument(
        "--ready-state",
        default="data/research/wallet_copy_ready_shadow_lanes_state.json",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--now")
    args = parser.parse_args()
    now = (
        dt.datetime.fromisoformat(args.now.replace("Z", "+00:00"))
        if args.now
        else dt.datetime.now(dt.timezone.utc)
    )
    report = build_report(
        history=load_json(args.history, default={}) or {},
        resolutions=_jsonl(Path(args.resolutions)),
        wide_state=load_json(args.wide_state, default={}) or {},
        runtime_state=load_json(args.runtime_state, default={}) or {},
        now=now,
    )
    if args.execute:
        if not report["seat_82c8_maturity_precommit"]["checks"]["maturity_reached"]:
            report["execution_status"] = "NOT_DUE"
            atomic_write_json(args.output, report)
            print(json.dumps(report, sort_keys=True))
            return 2
        with json_file_lock(args.ready_state):
            current = load_json(args.ready_state, default={}) or {}
            updated = apply_seat_maturity(current, report, now=now)
            if updated != current:
                atomic_write_json(args.ready_state, updated)
            report["execution_status"] = (
                "PARK_COMMITTED"
                if report["seat_82c8_maturity_precommit"]["decision"]
                == "PARK_AT_MATURITY"
                else "HANDOFF_ONLY_NO_LIVE_MUTATION"
            )
    atomic_write_json(args.output, report)
    print(
        json.dumps(
            {
                "output": args.output,
                "arms": [
                    {
                        "arm": arm["arm"],
                        "base": arm["base"],
                        "min_price_0_05": arm["min_price_0_05"],
                        "min_price_0_10": arm["min_price_0_10"],
                        "verdict": arm["verdict"],
                    }
                    for arm in report["arms"]
                ],
                "seat_82c8_maturity_precommit": report[
                    "seat_82c8_maturity_precommit"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
