#!/usr/bin/env python3
"""Build the weekend parity preregistration packet from existing evidence."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "research"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def rounded(value: float | int | None, places: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), places)


def short_wallet(wallet: str) -> str:
    if len(wallet) <= 12:
        return wallet
    return f"{wallet[:6]}...{wallet[-4:]}"


def daily_scorecard(day: str) -> dict[str, Any]:
    path = DATA / f"wallet_copy_daily_scorecard_{day}.json"
    if not path.exists():
        raise FileNotFoundError(path)
    return load_json(path)


def compact_day(day: str) -> dict[str, Any]:
    scorecard = daily_scorecard(day)
    today = scorecard.get("today_actual_basis") or scorecard.get("today") or {}
    total = today.get("total") or {}
    coverage = scorecard.get("window_coverage") or {}
    return {
        "day_utc": day,
        "orders": today.get("orders_in_scope"),
        "fills": total.get("fills"),
        "pnl_usd": rounded(total.get("pnl_usd")),
        "roi_pct": rounded(total.get("roi_pct")),
        "windows_filled": coverage.get("windows_traded"),
        "windows_submitted": coverage.get("windows_submitted"),
        "per_member": today.get("per_member") or {},
    }


def weekday_baseline(days: list[str]) -> dict[str, Any]:
    rows = [compact_day(day) for day in days]
    return {
        "days": days,
        "avg_orders": rounded(mean(r["orders"] for r in rows if r["orders"] is not None)),
        "avg_fills": rounded(mean(r["fills"] for r in rows if r["fills"] is not None)),
        "avg_windows_filled": rounded(
            mean(r["windows_filled"] for r in rows if r["windows_filled"] is not None)
        ),
        "avg_pnl_usd": rounded(mean(r["pnl_usd"] for r in rows if r["pnl_usd"] is not None)),
        "avg_roi_pct": rounded(mean(r["roi_pct"] for r in rows if r["roi_pct"] is not None)),
    }


def ratio(numerator: float | int | None, denominator: float | int | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return rounded(float(numerator) / float(denominator), 6)


def q2_member_rows(day: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for wallet, stats in sorted(day["per_member"].items()):
        rows.append(
            {
                "wallet": wallet,
                "wallet_short": short_wallet(wallet),
                "orders": stats.get("orders"),
                "fills": stats.get("fills"),
                "pnl_usd": rounded(stats.get("pnl_usd")),
                "roi_pct": rounded(stats.get("roi_pct")),
                "edge_sign": "positive" if (stats.get("pnl_usd") or 0) > 0 else "negative",
            }
        )
    return rows


def guard_members(guard: dict[str, Any]) -> list[dict[str, Any]]:
    active_set = guard.get("active_set") if isinstance(guard.get("active_set"), dict) else {}
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    if not members and isinstance(guard.get("members"), list):
        members = guard["members"]
    return [member for member in members if isinstance(member, dict)]


def current_day_member_stats(day: str) -> dict[str, dict[str, Any]]:
    try:
        scorecard = daily_scorecard(day)
    except FileNotFoundError:
        return {}
    today = scorecard.get("today_actual_basis") or scorecard.get("today") or {}
    per_member = today.get("per_member") if isinstance(today.get("per_member"), dict) else {}
    return {str(wallet).lower(): stats for wallet, stats in per_member.items() if isinstance(stats, dict)}


def dow_profiles() -> dict[str, dict[str, Any]]:
    path = DATA / "member_dow_profiles_latest.json"
    if not path.exists():
        return {}
    report = load_json(path)
    profiles = report.get("profiles_by_wallet")
    if not isinstance(profiles, dict):
        return {}
    return {str(wallet).lower(): row for wallet, row in profiles.items() if isinstance(row, dict)}


def posture_for_member(
    *,
    wallet: str,
    member: dict[str, Any],
    current_stats: dict[str, Any],
    dow_profile: dict[str, Any] | None,
) -> dict[str, Any]:
    pnl = float(current_stats.get("pnl_usd") or 0.0)
    fills = int(current_stats.get("fills") or 0)
    weekend_status = (dow_profile or {}).get("weekend_evidence_status") or "NO_PROFILE"
    weekend_weight = (dow_profile or {}).get("weekend_activity_weight_vs_weekday")
    weekend_trades = (dow_profile or {}).get("weekend_trade_count")
    loss_line = member.get("rolling_loss_trigger_usd")
    if loss_line is None:
        loss_line = -4.0

    if fills <= 0 or pnl < 0:
        posture = "BENCH"
        reason = "current weekday evidence is absent or negative"
    elif weekend_status in {"NO_WEEKEND_SAMPLE", "NO_LOCAL_HISTORY", "NO_PROFILE"}:
        posture = "TRADE_FLOOR_SIZE"
        reason = "positive weekday evidence, but current-roster weekend sample is absent"
    elif weekend_weight is None or float(weekend_weight) < 0.25:
        posture = "TRADE_FLOOR_SIZE"
        reason = "positive weekday evidence, but weekend activity weight is thin"
    else:
        posture = "TRADE_NORMAL"
        reason = "positive weekday evidence and usable weekend activity sample"

    return {
        "candidate_id": member.get("candidate_id") or wallet,
        "source_wallet": wallet,
        "wallet_short": short_wallet(wallet),
        "policy_id": member.get("policy_id"),
        "current_weekday_evidence": {
            "orders": current_stats.get("orders"),
            "fills": fills,
            "pnl_usd": rounded(pnl),
            "roi_pct": rounded(current_stats.get("roi_pct")),
        },
        "weekend_history": {
            "status": weekend_status,
            "weekend_trade_count": weekend_trades,
            "weekend_activity_weight_vs_weekday": rounded(weekend_weight),
            "first_event_iso": (dow_profile or {}).get("first_event_iso"),
            "last_event_iso": (dow_profile or {}).get("last_event_iso"),
        },
        "weekend_posture": posture,
        "posture_reason": reason,
        "max_order_usd": member.get("max_order_usd"),
        "wallet_fraction": member.get("wallet_fraction"),
        "max_price": member.get("max_price"),
        "first_slice_loss_line_usd": rounded(loss_line),
        "day_probe_trigger_usd": -8.0,
    }


def weekend_bounds(day: str) -> tuple[str, str]:
    """Return the Saturday 00Z -> Monday 00Z interval governing a UTC date."""

    current = date.fromisoformat(day)
    if current.weekday() in (5, 6, 0):
        days_since_saturday = {5: 0, 6: 1, 0: 2}[current.weekday()]
        saturday = current - timedelta(days=days_since_saturday)
    else:
        saturday = current + timedelta(days=5 - current.weekday())
    monday = saturday + timedelta(days=2)
    return f"{saturday.isoformat()}T00:00:00Z", f"{monday.isoformat()}T00:00:00Z"


def current_roster_weekend_plan(current_day: str) -> dict[str, Any]:
    guard_path = DATA / "wallet_copy_live_guard_state.json"
    guard = load_json(guard_path) if guard_path.exists() else {}
    stats_by_wallet = current_day_member_stats(current_day)
    profiles_by_wallet = dow_profiles()
    members = []
    for member in guard_members(guard):
        wallet = str(member.get("source_wallet") or "").lower()
        if not wallet:
            continue
        members.append(
            posture_for_member(
                wallet=wallet,
                member=member,
                current_stats=stats_by_wallet.get(wallet, {}),
                dow_profile=profiles_by_wallet.get(wallet),
            )
        )

    posture_counts: dict[str, int] = {}
    for row in members:
        posture = str(row.get("weekend_posture") or "UNKNOWN")
        posture_counts[posture] = posture_counts.get(posture, 0) + 1

    weekend_starts_at, weekend_ends_at = weekend_bounds(current_day)
    return {
        "direction_id": "2026-07-17T15:52Z-fable-weekend-prep",
        "current_day_utc": current_day,
        "guard_state_source": "data/research/wallet_copy_live_guard_state.json",
        "member_dow_profile_source": "data/research/member_dow_profiles_latest.json",
        "current_scorecard_source": f"data/research/wallet_copy_daily_scorecard_{current_day}.json",
        "weekend_starts_at": weekend_starts_at,
        "weekend_ends_at": weekend_ends_at,
        "rule": "reduced-but-positive posture is the goal; weekday volume parity is not expected and must not be forced",
        "live_path_mutated": False,
        "paper_only": True,
        "live_orders_allowed": False,
        "posture_counts": posture_counts,
        "members": members,
        "weekend_loss_ladder": {
            "day_probe_trigger_usd": -8.0,
            "per_member_first_slice_loss_lines": [
                {
                    "candidate_id": row.get("candidate_id"),
                    "source_wallet": row.get("source_wallet"),
                    "weekend_posture": row.get("weekend_posture"),
                    "first_slice_loss_line_usd": row.get("first_slice_loss_line_usd"),
                }
                for row in members
            ],
        },
    }


def stakeout_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"status": "MISSING", "path": str(path)}
    packet = load_json(path)
    candidates = []
    for candidate in packet.get("weekend_candidates", []):
        weekend = candidate.get("weekend") or {}
        copyability = candidate.get("copyability") or {}
        shadow = candidate.get("watch_tier_shadow_ev") or {}
        candidates.append(
            {
                "wallet": candidate.get("source_wallet"),
                "wallet_short": short_wallet(candidate.get("source_wallet") or ""),
                "classification": candidate.get("classification"),
                "admission_result": candidate.get("admission_result"),
                "weekend_roi_pct": rounded(weekend.get("roi_pct")),
                "weekend_pnl_usd": rounded(weekend.get("pnl_usd")),
                "weekend_resolved_trades": weekend.get("resolved_trades"),
                "copy_pnl_usd": rounded(copyability.get("paper_pnl_usd")),
                "copyable_buy_events": copyability.get("copyable_buy_events"),
                "shadow_status": shadow.get("status"),
            }
        )
    return {
        "status": packet.get("stakeout_status"),
        "generated_at": packet.get("generated_at"),
        "fresh_alerts": len(packet.get("fresh_alerts") or []),
        "poll_alerts": packet.get("poll_alerts") or [],
        "ac05_weekend_slice": packet.get("ac05_weekend_slice_audit", {}).get("weekend"),
        "c03c_ready_for_live": (
            packet.get("c03c_shadow_evidence", {})
            .get("ready_shadow_lane", {})
            .get("ready_for_live")
        ),
        "candidates": candidates,
    }


def lane_summary(path: Path, summary_key: str) -> dict[str, Any]:
    if not path.exists():
        return {"status": "MISSING", "path": str(path)}
    state = load_json(path)
    summary = state.get(summary_key) or {}
    gate = state.get("promotion_gate") or {}
    resolved_ledger = (
        state.get("prospective_no_fallback_resolved_fill_ledger")
        if isinstance(state.get("prospective_no_fallback_resolved_fill_ledger"), dict)
        else {}
    )
    monotonicity = (
        gate.get("monotonicity_tripwire")
        if isinstance(gate.get("monotonicity_tripwire"), dict)
        else resolved_ledger.get("monotonicity_tripwire")
        if isinstance(resolved_ledger.get("monotonicity_tripwire"), dict)
        else {}
    )
    return {
        "path": str(path.relative_to(ROOT)),
        "lane": state.get("lane"),
        "flow_stage": state.get("flow_stage"),
        "can_trade": state.get("can_trade"),
        "live_orders_allowed": state.get("live_orders_allowed"),
        "paper_only": state.get("paper_only"),
        "resolved_paper_fills": summary.get("resolved_paper_fills"),
        "resolved_paper_pnl_usd": rounded(summary.get("resolved_paper_pnl_usd")),
        "resolved_paper_roi_pct": rounded(summary.get("resolved_paper_roi_pct")),
        "maker_fill_rate_pct": rounded(summary.get("maker_fill_rate_pct")),
        "trusted_gate_resolved_fills": gate.get("resolved_paper_fills"),
        "trusted_gate_metric": gate.get("gate_metric"),
        "trusted_gate_source_file": gate.get("gate_source_file"),
        "trusted_gate_lane": gate.get("gate_lane"),
        "trusted_counter_status": monotonicity.get("status"),
        "trusted_counter_distinct_resolved_fill_ids": resolved_ledger.get("distinct_resolved_fill_ids"),
        "trusted_counter_current_source_distinct_resolved_fill_ids": resolved_ledger.get(
            "current_source_distinct_resolved_fill_ids"
        ),
        "trusted_counter_prior_distinct_resolved_fill_ids": resolved_ledger.get("prior_distinct_resolved_fill_ids"),
        "promotion_gate": gate,
    }


def build_packet(day: str, weekday_days: list[str], current_day: str | None = None) -> dict[str, Any]:
    weekend_day = compact_day(day)
    baseline = weekday_baseline(weekday_days)
    member_rows = q2_member_rows(weekend_day)
    positive_members = [r for r in member_rows if r["edge_sign"] == "positive"]
    stakeout = stakeout_summary(DATA / "weekend_specialist_stakeout_packet_latest.json")
    maker_first = lane_summary(
        DATA / "maker_first_btc5m_book_aware_state.json", "prospective_no_fallback_summary"
    )
    e11 = lane_summary(
        DATA / "e11_cross_window_momentum_book_aware_state.json", "book_aware_summary"
    )
    current_plan = current_roster_weekend_plan(
        current_day or datetime.now(timezone.utc).date().isoformat()
    )

    q1 = {
        "answer": "YES_BUT_REDUCED",
        "weekend_orders": weekend_day["orders"],
        "weekend_fills": weekend_day["fills"],
        "weekend_windows_filled": weekend_day["windows_filled"],
        "weekday_baseline": baseline,
        "orders_ratio_vs_weekday_avg": ratio(weekend_day["orders"], baseline["avg_orders"]),
        "fills_ratio_vs_weekday_avg": ratio(weekend_day["fills"], baseline["avg_fills"]),
        "windows_ratio_vs_weekday_avg": ratio(
            weekend_day["windows_filled"], baseline["avg_windows_filled"]
        ),
    }
    q2 = {
        "answer": "NEGATIVE_ALL_LIVE_MEMBERS",
        "positive_member_count": len(positive_members),
        "negative_member_count": len(member_rows) - len(positive_members),
        "members": member_rows,
    }
    q3 = {
        "answer": "UNPROVEN_CONTINUE_STAKEOUT",
        "stakeout": stakeout,
        "basis": "weekend specialist raw regimes exist, but copyability/admission gates are not live-ready.",
    }
    q4 = {
        "answer": "MAKER_FIRST_STRONGEST_PATH_COUNTER_TRUST_DEPENDENT_E11_NEGATIVE",
        "maker_first": maker_first,
        "e11": e11,
        "basis": (
            "maker-first is the strongest named path to 144+ weekend windows, but promotion requires "
            "a trusted monotonic prospective no-fallback counter plus the existing PnL/fill-rate gates; E11 is negative."
        ),
    }
    named_path = {
        "status": "REDUCED_BUT_NEGATIVE_RESEARCH_PATH",
        "steps": [
            "Keep RULED-FLAT-WEEKEND until 2026-07-13T00:00Z.",
            "Continue weekend specialist stakeout; admit only after copyability gates pass.",
            "Treat maker-first as the strongest named weekend path, paper-only until the trusted counter and promotion gates pass.",
            "Use Fable's day<=-15 degrade threshold for the next live day to prevent repeat drawdown.",
        ],
    }

    return {
        "kind": "weekend_parity_packet",
        "experiment_id": "campaign-weekend-parity-20260711",
        "flow_stage": "LEARN/PROMOTE/LIVE",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "live_orders_allowed": False,
        "paper_only": True,
        "day_utc": day,
        "sources": {
            "weekend_scorecard": f"data/research/wallet_copy_daily_scorecard_{day}.json",
            "weekday_baseline_days": [
                f"data/research/wallet_copy_daily_scorecard_{weekday}.json"
                for weekday in weekday_days
            ],
            "stakeout": "data/research/weekend_specialist_stakeout_packet_latest.json",
            "maker_first": "data/research/maker_first_btc5m_book_aware_state.json",
            "e11": "data/research/e11_cross_window_momentum_book_aware_state.json",
        },
        "summary": {
            "verdict": "WEEKEND_PARITY_FAIL_FOR_LIVE_WALLET_COPY",
            "weekend_pnl_usd": weekend_day["pnl_usd"],
            "weekend_roi_pct": weekend_day["roi_pct"],
            "weekend_windows_filled": weekend_day["windows_filled"],
            "weekday_avg_windows_filled": baseline["avg_windows_filled"],
            "primary_cause": "weekday wallets traded weekends at reduced volume, but every live copied member was negative on 2026-07-11.",
            "clean_unproven_specialist_result": True,
            "final_packet_expected_path_status": "REDUCED_BUT_NEGATIVE_RESEARCH_PATH",
            "current_roster_weekend_member_count": len(current_plan["members"]),
            "current_roster_weekend_posture_counts": current_plan["posture_counts"],
        },
        "current_roster_weekend_posture_plan": current_plan,
        "q1_weekday_wallet_weekend_volume_ratio": q1,
        "q2_per_wallet_weekend_our_fill_roi_edge_sign": q2,
        "q3_weekend_specialist_admission_status": q3,
        "q4_maker_e7_weekend_alternative_evidence": q4,
        "named_path_to_144_profitable_weekend_windows": named_path,
    }


def write_markdown(packet: dict[str, Any], path: Path) -> None:
    current_plan = packet["current_roster_weekend_posture_plan"]
    q1 = packet["q1_weekday_wallet_weekend_volume_ratio"]
    q2 = packet["q2_per_wallet_weekend_our_fill_roi_edge_sign"]
    q3 = packet["q3_weekend_specialist_admission_status"]
    q4 = packet["q4_maker_e7_weekend_alternative_evidence"]
    lines = [
        "# Weekend Parity Packet",
        "",
        f"- experiment_id: `{packet['experiment_id']}`",
        f"- generated_at: `{packet['generated_at']}`",
        f"- verdict: `{packet['summary']['verdict']}`",
        f"- weekend_pnl_usd: `{packet['summary']['weekend_pnl_usd']}`",
        f"- weekend_roi_pct: `{packet['summary']['weekend_roi_pct']}`",
        f"- current_roster_postures: `{packet['summary']['current_roster_weekend_posture_counts']}`",
        "",
        "## Current Roster Weekend Plan",
        "",
        f"- rule: `{current_plan['rule']}`",
        f"- day_probe_trigger_usd: `{current_plan['weekend_loss_ladder']['day_probe_trigger_usd']}`",
    ]
    for row in current_plan["members"]:
        lines.append(
            f"- {row['wallet_short']}: posture `{row['weekend_posture']}`, fills `{row['current_weekday_evidence']['fills']}`, pnl `{row['current_weekday_evidence']['pnl_usd']}`, weekend_status `{row['weekend_history']['status']}`, first_slice_loss `{row['first_slice_loss_line_usd']}`"
        )
    lines.extend(
        [
            "",
            "## Legacy Weekend Evidence",
        "",
        "## Q1 Volume",
        "",
        f"- answer: `{q1['answer']}`",
        f"- weekend windows/fills/orders: `{q1['weekend_windows_filled']}` / `{q1['weekend_fills']}` / `{q1['weekend_orders']}`",
        f"- ratios vs weekday avg windows/fills/orders: `{q1['windows_ratio_vs_weekday_avg']}` / `{q1['fills_ratio_vs_weekday_avg']}` / `{q1['orders_ratio_vs_weekday_avg']}`",
        "",
        "## Q2 Per-Wallet Edge",
        "",
        f"- answer: `{q2['answer']}`",
        ]
    )
    for row in q2["members"]:
        lines.append(
            f"- {row['wallet_short']}: fills `{row['fills']}`, pnl `{row['pnl_usd']}`, roi `{row['roi_pct']}`, edge `{row['edge_sign']}`"
        )
    lines.extend(
        [
            "",
            "## Q3 Weekend Specialists",
            "",
            f"- answer: `{q3['answer']}`",
            f"- stakeout_status: `{q3['stakeout'].get('status')}`",
            f"- fresh_alerts: `{q3['stakeout'].get('fresh_alerts')}`",
            "",
            "## Q4 Maker/E7",
            "",
            f"- answer: `{q4['answer']}`",
            f"- maker_first pnl/roi/fills: `{q4['maker_first'].get('resolved_paper_pnl_usd')}` / `{q4['maker_first'].get('resolved_paper_roi_pct')}` / `{q4['maker_first'].get('resolved_paper_fills')}`",
            f"- maker_first trusted gate fills/status: `{q4['maker_first'].get('trusted_gate_resolved_fills')}` / `{q4['maker_first'].get('trusted_counter_status')}`",
            f"- maker_first trusted source: `{q4['maker_first'].get('trusted_gate_source_file')}` / `{q4['maker_first'].get('trusted_gate_lane')}`",
            f"- e11 pnl/roi/fills: `{q4['e11'].get('resolved_paper_pnl_usd')}` / `{q4['e11'].get('resolved_paper_roi_pct')}` / `{q4['e11'].get('resolved_paper_fills')}`",
            "",
            "## Named Path",
            "",
        ]
    )
    for step in packet["named_path_to_144_profitable_weekend_windows"]["steps"]:
        lines.append(f"- {step}")
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", default="2026-07-11")
    parser.add_argument(
        "--weekday-days",
        nargs="+",
        default=["2026-07-06", "2026-07-07", "2026-07-08", "2026-07-09", "2026-07-10"],
    )
    parser.add_argument(
        "--output-json", default="data/research/weekend_parity_packet_latest.json"
    )
    parser.add_argument("--output-md", default="data/research/weekend_parity_packet_latest.md")
    parser.add_argument("--current-day", default=datetime.now(timezone.utc).date().isoformat())
    args = parser.parse_args()

    packet = build_packet(args.day, args.weekday_days, current_day=args.current_day)
    output_json = ROOT / args.output_json
    output_md = ROOT / args.output_md
    output_json.write_text(json.dumps(packet, indent=2, sort_keys=True) + "\n")
    write_markdown(packet, output_md)
    print(json.dumps(packet["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
