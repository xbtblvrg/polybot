#!/usr/bin/env python3
"""Summarize the operator-provided wallet-copy universe.

The report is intentionally paper/research only. It combines per-wallet history,
profit-engine, and live-tracker states with the global universe profit report so
we can rank wallets by evidence instead of by vibes.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {"_load_error": "json_decode_error", "_path": str(path)}
    return payload if isinstance(payload, dict) else {}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_summary(payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload.get("summary")
    if isinstance(summary, dict):
        return summary
    return payload


def _slug_from_history(path: Path) -> str:
    name = path.name
    prefix = "operator_wallet_"
    suffix = "_history_state.json"
    if name.startswith(prefix) and name.endswith(suffix):
        return name[len(prefix) : -len(suffix)]
    return path.stem


def _latest_existing(paths: list[Path]) -> Path | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    return max(existing, key=lambda path: path.stat().st_mtime)


def _profit_path(research_dir: Path, slug: str) -> Path | None:
    candidates = [
        research_dir / f"operator_wallet_{slug}_profit_engine_state_after_candidate_policy.json",
        research_dir / f"operator_wallet_{slug}_profit_engine_state_after_tracker.json",
        research_dir / f"operator_wallet_{slug}_profit_engine_state.json",
    ]
    return _latest_existing(candidates)


def _tracker_paths(research_dir: Path, slug: str) -> list[Path]:
    return sorted(research_dir.glob(f"operator_wallet_{slug}_live_tracking_state*.json"))


def _paper_path(research_dir: Path, slug: str) -> Path | None:
    return _latest_existing([research_dir / f"operator_wallet_{slug}_paper_state.json"])


def _onboarding_path(research_dir: Path, slug: str) -> Path | None:
    return _latest_existing([research_dir / f"operator_wallet_{slug}_onboarding_state.json"])


def _best_tracker_summary(paths: list[Path]) -> dict[str, Any]:
    best: dict[str, Any] = {
        "path": None,
        "copy_efficiency_status": None,
        "required_buy_copy_events": 0,
        "clob_filled_buy_copy_events": 0,
        "fallback_filled_buy_copy_events": 0,
        "source_buy_events": 0,
        "identity_mismatch_quarantined_rows": 0,
        "blockers": [],
    }
    for path in paths:
        payload = _first_summary(_load(path))
        copy = payload.get("copy_efficiency") if isinstance(payload.get("copy_efficiency"), dict) else {}
        coverage = payload.get("buy_fill_coverage") if isinstance(payload.get("buy_fill_coverage"), dict) else {}
        identity = (
            payload.get("identity_contamination")
            if isinstance(payload.get("identity_contamination"), dict)
            else {}
        )
        copy_summary = copy.get("summary") if isinstance(copy.get("summary"), dict) else {}
        row = {
            "path": str(path),
            "copy_efficiency_status": copy.get("status"),
            "required_buy_copy_events": int(_num(coverage.get("required_buy_events"))),
            "clob_filled_buy_copy_events": int(
                _num(
                    coverage.get("filled_buy_copy_events"),
                    _num(copy_summary.get("clob_filled_buy_copy_events")),
                )
            ),
            "fallback_filled_buy_copy_events": int(_num(copy_summary.get("fallback_filled_buy_copy_events"))),
            "source_buy_events": int(_num(coverage.get("source_buy_events"))),
            "identity_mismatch_quarantined_rows": int(
                _num(identity.get("wallet_identity_mismatch_quarantined_rows"))
            ),
            "blockers": copy.get("blockers") or [],
        }
        best_score = (
            int(best.get("copy_efficiency_status") == "PASS"),
            int(best.get("clob_filled_buy_copy_events") or 0),
            int(best.get("required_buy_copy_events") or 0),
            int(best.get("source_buy_events") or 0),
        )
        row_score = (
            int(row.get("copy_efficiency_status") == "PASS"),
            int(row.get("clob_filled_buy_copy_events") or 0),
            int(row.get("required_buy_copy_events") or 0),
            int(row.get("source_buy_events") or 0),
        )
        if row_score > best_score:
            best = row
    return best


def _wallet_row(research_dir: Path, history_path: Path) -> dict[str, Any]:
    slug = _slug_from_history(history_path)
    history = _load(history_path)
    events = history.get("events") if isinstance(history.get("events"), list) else []
    buy_events = [event for event in events if str(event.get("action", "")).upper() == "BUY"]
    wallet_meta = (history.get("wallets") or [{}])[0] if isinstance(history.get("wallets"), list) else {}
    profit_path = _profit_path(research_dir, slug)
    profit = _load(profit_path) if profit_path else {}
    best = profit.get("best_candidate") if isinstance(profit.get("best_candidate"), dict) else {}
    summary = best.get("summary") if isinstance(best.get("summary"), dict) else {}
    validation = best.get("validation_summary") if isinstance(best.get("validation_summary"), dict) else {}
    raw = best.get("raw_baseline_summary") if isinstance(best.get("raw_baseline_summary"), dict) else {}
    decision = profit.get("decision") if isinstance(profit.get("decision"), dict) else {}
    paper_path = _paper_path(research_dir, slug)
    paper = _load(paper_path) if paper_path else {}
    paper_summary = paper.get("summary") if isinstance(paper.get("summary"), dict) else {}
    onboarding_path = _onboarding_path(research_dir, slug)
    onboarding = _load(onboarding_path) if onboarding_path else {}
    tracker = _best_tracker_summary(_tracker_paths(research_dir, slug))
    blockers = best.get("blockers") or decision.get("live_admission_blockers") or []
    if not isinstance(blockers, list):
        blockers = [str(blockers)]
    return {
        "slug": slug,
        "wallet_name": wallet_meta.get("name") or slug,
        "wallet": wallet_meta.get("address"),
        "history_path": str(history_path),
        "profit_path": str(profit_path) if profit_path else None,
        "paper_path": str(paper_path) if paper_path else None,
        "onboarding_status": onboarding.get("status"),
        "paper_only": bool(history.get("paper_only", onboarding.get("paper_only", True))),
        "live_orders_allowed": bool(history.get("live_orders_allowed", onboarding.get("live_orders_allowed", False))),
        "history_events": len(events),
        "history_buy_events": len(buy_events),
        "history_buy_usdc": round(sum(_num(event.get("usdc_size")) for event in buy_events), 6),
        "paper_orders": int(_num(paper_summary.get("paper_orders"))),
        "paper_filled_orders": int(_num(paper_summary.get("filled_orders"))),
        "paper_rejected_orders": int(_num(paper_summary.get("rejected_orders"))),
        "ranked_candidate_count": len(profit.get("ranked_candidates") or []),
        "pass_candidate_count": len(profit.get("pass_candidates") or []),
        "best_candidate_id": best.get("candidate_id"),
        "best_candidate_type": best.get("candidate_type"),
        "best_candidate_status": best.get("status"),
        "best_policy_id": (best.get("policy") or {}).get("policy_id") if isinstance(best.get("policy"), dict) else None,
        "best_orders": int(_num(summary.get("orders"))),
        "best_resolved_orders": int(_num(summary.get("resolved_orders"))),
        "best_unresolved_ratio": _num(summary.get("unresolved_ratio")),
        "best_pnl_usd": _num(summary.get("pnl_usd")),
        "best_roi_pct": _num(summary.get("roi_pct")),
        "best_wr_pct": _num(summary.get("wr_pct")),
        "validation_orders": int(_num(validation.get("orders"))),
        "validation_roi_pct": _num(validation.get("roi_pct")),
        "validation_wr_pct": _num(validation.get("wr_pct")),
        "raw_orders": int(_num(raw.get("orders"))),
        "raw_roi_pct": _num(raw.get("roi_pct")),
        "raw_wr_pct": _num(raw.get("wr_pct")),
        "raw_pnl_usd": _num(raw.get("pnl_usd")),
        "max_drawdown_usd": _num(best.get("max_drawdown_usd")),
        "live_admission_status": decision.get("live_admission_status"),
        "live_admission_blockers": decision.get("live_admission_blockers") or [],
        "blockers": blockers,
        "tracker": tracker,
    }


def _candidate_summary(candidate: dict[str, Any]) -> dict[str, Any]:
    summary = candidate.get("summary") if isinstance(candidate.get("summary"), dict) else {}
    validation = candidate.get("validation_summary") if isinstance(candidate.get("validation_summary"), dict) else {}
    raw = candidate.get("raw_baseline_summary") if isinstance(candidate.get("raw_baseline_summary"), dict) else {}
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    return {
        "candidate_id": candidate.get("candidate_id"),
        "candidate_type": candidate.get("candidate_type"),
        "status": candidate.get("status"),
        "wallet_name": metadata.get("wallet_name"),
        "wallet": metadata.get("source_wallet"),
        "policy_id": (candidate.get("policy") or {}).get("policy_id") if isinstance(candidate.get("policy"), dict) else None,
        "orders": int(_num(summary.get("orders"))),
        "resolved_orders": int(_num(summary.get("resolved_orders"))),
        "roi_pct": _num(summary.get("roi_pct")),
        "pnl_usd": _num(summary.get("pnl_usd")),
        "wr_pct": _num(summary.get("wr_pct")),
        "validation_orders": int(_num(validation.get("orders"))),
        "validation_roi_pct": _num(validation.get("roi_pct")),
        "validation_wr_pct": _num(validation.get("wr_pct")),
        "raw_roi_pct": _num(raw.get("roi_pct")),
        "raw_pnl_usd": _num(raw.get("pnl_usd")),
        "max_drawdown_usd": _num(candidate.get("max_drawdown_usd")),
        "blockers": candidate.get("blockers") or [],
    }


def _write_markdown(report: dict[str, Any], output: Path) -> None:
    lines = [
        "# Wallet Copy Universe Summary",
        "",
        f"Status: **{report['status']}**",
        f"Wallets: {report['wallet_count']} | History events: {report['history_events']} | BUYs: {report['history_buy_events']}",
        f"Universe candidates: {report['universe_candidate_count']} | PASS: {report['universe_pass_candidate_count']}",
        "",
        "## Highest Leverage Conclusion",
        "",
        report["conclusion"],
        "",
        "## Top Research Candidates",
        "",
        "| Rank | Wallet | ROI | PnL | Orders | Val ROI | Raw ROI | CLOB fills | Blockers |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for idx, row in enumerate(report["top_research_candidates"][:20], start=1):
        lines.append(
            "| {rank} | {wallet} | {roi:.2f}% | {pnl:.2f} | {orders} | {val_roi:.2f}% | {raw_roi:.2f}% | {clob} | {blockers} |".format(
                rank=idx,
                wallet=row.get("wallet_name") or row.get("slug"),
                roi=_num(row.get("best_roi_pct")),
                pnl=_num(row.get("best_pnl_usd")),
                orders=row.get("best_resolved_orders") or row.get("best_orders"),
                val_roi=_num(row.get("validation_roi_pct")),
                raw_roi=_num(row.get("raw_roi_pct")),
                clob=(row.get("tracker") or {}).get("clob_filled_buy_copy_events") or 0,
                blockers=", ".join((row.get("blockers") or [])[:4]),
            )
        )
    lines.extend(
        [
            "",
            "## Top Universe Candidates",
            "",
            "| Rank | Type | Wallet | ROI | PnL | Orders | Val ROI | Raw ROI | Blockers |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for idx, row in enumerate(report["top_universe_candidates"][:20], start=1):
        lines.append(
            "| {rank} | {ctype} | {wallet} | {roi:.2f}% | {pnl:.2f} | {orders} | {val_roi:.2f}% | {raw_roi:.2f}% | {blockers} |".format(
                rank=idx,
                ctype=row.get("candidate_type"),
                wallet=row.get("wallet_name") or row.get("wallet") or "",
                roi=_num(row.get("roi_pct")),
                pnl=_num(row.get("pnl_usd")),
                orders=row.get("resolved_orders") or row.get("orders"),
                val_roi=_num(row.get("validation_roi_pct")),
                raw_roi=_num(row.get("raw_roi_pct")),
                blockers=", ".join((row.get("blockers") or [])[:4]),
            )
        )
    lines.extend(["", "## Required Fixes", ""])
    for action in report["next_actions"]:
        lines.append(f"- {action}")
    output.write_text("\n".join(lines) + "\n")


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    research_dir = Path(args.research_dir)
    histories = sorted(research_dir.glob("operator_wallet_*_history_state.json"))
    wallet_rows = [_wallet_row(research_dir, path) for path in histories]
    universe = _load(Path(args.universe_profit_state))
    ranked = [
        _candidate_summary(row)
        for row in universe.get("ranked_candidates", [])
        if isinstance(row, dict)
    ]
    pass_candidates = [
        _candidate_summary(row)
        for row in universe.get("pass_candidates", [])
        if isinstance(row, dict)
    ]
    blocker_counts: Counter[str] = Counter()
    for row in wallet_rows:
        blocker_counts.update(str(blocker) for blocker in row.get("blockers") or [])
        blocker_counts.update(str(blocker) for blocker in (row.get("tracker") or {}).get("blockers") or [])
        blocker_counts.update(str(blocker) for blocker in row.get("live_admission_blockers") or [])
    candidate_type_counts = Counter(str(row.get("candidate_type") or "UNKNOWN") for row in ranked)
    top_research = sorted(
        [
            row
            for row in wallet_rows
            if row.get("best_resolved_orders", 0) >= args.min_resolved_for_top
            and row.get("best_roi_pct", 0.0) > 0
        ],
        key=lambda row: (
            _num(row.get("best_roi_pct")),
            _num(row.get("best_pnl_usd")),
            int(row.get("best_resolved_orders") or 0),
        ),
        reverse=True,
    )
    top_universe = sorted(
        [row for row in ranked if row.get("resolved_orders", 0) >= args.min_resolved_for_top],
        key=lambda row: (
            _num(row.get("roi_pct")),
            _num(row.get("pnl_usd")),
            int(row.get("resolved_orders") or 0),
        ),
        reverse=True,
    )
    clob_positive = [
        row
        for row in wallet_rows
        if int((row.get("tracker") or {}).get("clob_filled_buy_copy_events") or 0) > 0
    ]
    live_ready = [
        row
        for row in wallet_rows
        if row.get("live_admission_status") == "PASS"
        and int((row.get("tracker") or {}).get("clob_filled_buy_copy_events") or 0) > 0
    ]
    source_contract = universe.get("source_contract") if isinstance(universe.get("source_contract"), dict) else {}
    total_events = sum(int(row.get("history_events") or 0) for row in wallet_rows)
    total_buys = sum(int(row.get("history_buy_events") or 0) for row in wallet_rows)
    conclusion = (
        "A profitábilis irány nem vak 1:1 all-buy copy. A legerősebb jelek szűrt, "
        "wallet-specifikus idő/ár/sizing policykben vannak, de jelenleg live-ready PASS nincs, "
        "mert a legjobb replayek research-only resolutionre és fallback fillre támaszkodnak, "
        "nem friss CLOB-backed copy evidence-re."
    )
    next_actions = [
        "Canonical BTC 5m resolution coverage bővítése a top pozitív kandidátusok window-ira.",
        "Top replay ROI walletenként blocked-candidate hotlane tracker futtatása --track-blocked-profit-policy módban.",
        "Csak olyan policy léphet live-ready előszobába, ahol friss required BUY, CLOB fill, identity-clean source és több window validáció együtt PASS.",
        "Consensus/inventory keresést a top pozitív wallet-csoportokra kell fókuszálni; az all-wallet konszenzus túl zajos.",
        "Fallback-only source-price replay soha ne legyen admission truth, csak kutatási shortlist.",
    ]
    return {
        "schema_version": 1,
        "status": "WATCH" if not live_ready else "GREEN_CANDIDATE_FOUND",
        "research_dir": str(research_dir),
        "wallet_count": len(wallet_rows),
        "wallet_count_from_universe_profit_engine": source_contract.get("wallet_count"),
        "history_events": total_events,
        "history_buy_events": total_buys,
        "universe_candidate_count": len(ranked),
        "universe_pass_candidate_count": len(pass_candidates),
        "candidate_type_counts": dict(candidate_type_counts),
        "copyability_clob_positive_wallets": len(clob_positive),
        "live_ready_wallets": len(live_ready),
        "top_blockers": blocker_counts.most_common(20),
        "top_research_candidates": top_research[: args.top],
        "top_universe_candidates": top_universe[: args.top],
        "pass_candidates": pass_candidates[: args.top],
        "clob_positive_wallets": clob_positive[: args.top],
        "conclusion": conclusion,
        "next_actions": next_actions,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-dir", default="data/research")
    parser.add_argument(
        "--universe-profit-state",
        default="data/research/operator_wallet_universe_profit_engine_state.json",
    )
    parser.add_argument(
        "--output-json",
        default="data/research/operator_wallet_universe_summary.json",
    )
    parser.add_argument(
        "--output-md",
        default="data/research/operator_wallet_universe_summary.md",
    )
    parser.add_argument("--top", type=int, default=30)
    parser.add_argument("--min-resolved-for-top", type=int, default=30)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args)
    output_json = Path(args.output_json)
    output_md = Path(args.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    _write_markdown(report, output_md)
    print(
        json.dumps(
            {
                "status": report.get("status"),
                "wallet_count": report.get("wallet_count"),
                "history_events": report.get("history_events"),
                "history_buy_events": report.get("history_buy_events"),
                "universe_candidate_count": report.get("universe_candidate_count"),
                "universe_pass_candidate_count": report.get("universe_pass_candidate_count"),
                "copyability_clob_positive_wallets": report.get("copyability_clob_positive_wallets"),
                "top_research_candidates": [
                    {
                        "wallet_name": row.get("wallet_name"),
                        "best_roi_pct": row.get("best_roi_pct"),
                        "best_pnl_usd": row.get("best_pnl_usd"),
                        "best_resolved_orders": row.get("best_resolved_orders"),
                        "validation_roi_pct": row.get("validation_roi_pct"),
                        "raw_roi_pct": row.get("raw_roi_pct"),
                        "blockers": row.get("blockers"),
                    }
                    for row in report.get("top_research_candidates", [])[:10]
                ],
                "top_universe_candidates": report.get("top_universe_candidates", [])[:10],
                "top_blockers": report.get("top_blockers", [])[:10],
                "output_json": str(output_json),
                "output_md": str(output_md),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
