#!/usr/bin/env python3
"""Report the Fable RULING 10 abstain-count probe.

Flow stage: LIVE/MEASURE. This is a read-only report for the 2026-07-17
12:00Z conditional: if e88d/32de/d60c still have zero resolved fills for the
UTC day, count each member's top abstain reasons. It never mutates live
eligibility, rotation, caps, thresholds, or order submission.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_OUTPUT = "data/research/ruling10_abstain_probe_latest.json"
DEFAULT_LIVE_STATE = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_SCORECARD_TEMPLATE = "data/research/wallet_copy_daily_scorecard_{day}.json"
RULING_ID = "2026-07-17T04:27Z-fable-ruling10-narrowed-12z-abstain-count"


@dataclass(frozen=True)
class TargetMember:
    label: str
    wallet: str
    candidate_id: str
    probe_path: str


DEFAULT_TARGETS: tuple[TargetMember, ...] = (
    TargetMember(
        label="e88d",
        wallet="0xe88db6a8c559410a627a528264a441206ad03db1",
        candidate_id="market_cohort_alive_41206ad03db1",
        probe_path="data/research/wallet_copy_live_execution_probe_market_cohort_alive_41206ad03db1.json",
    ),
    TargetMember(
        label="32de",
        wallet="0x32de91fa203321fa7735e7854f2b1c844e71ce9d",
        candidate_id="runtime_auto_degrade_32de91fa20",
        probe_path="data/research/wallet_copy_live_execution_probe_runtime_auto_degrade_32de91fa20.json",
    ),
    TargetMember(
        label="d60c",
        wallet="0x40138697bf1a0d655593f3be6237d60c1dc7ab35",
        candidate_id="runtime_auto_degrade_40138697bf",
        probe_path="data/research/wallet_copy_live_execution_probe_runtime_auto_degrade_40138697bf.json",
    ),
)


def _resolve(root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else root / path


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso_now() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _day_start(day: str) -> str:
    return f"{day}T00:00:00"


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _order_ts(order: dict[str, Any]) -> str:
    return str(order.get("submitted_at") or order.get("updated_at") or "")


def _orders_for_day(live_state: dict[str, Any], *, wallet: str, day: str) -> list[dict[str, Any]]:
    start = _day_start(day)
    wallet_norm = _norm_wallet(wallet)
    rows = []
    for row in _as_list(live_state.get("orders")):
        if not isinstance(row, dict):
            continue
        if _norm_wallet(row.get("source_wallet")) != wallet_norm:
            continue
        if _order_ts(row) >= start:
            rows.append(row)
    return rows


def _is_filled(order: dict[str, Any]) -> bool:
    status = str(order.get("status") or order.get("final_status") or "").upper()
    return status == "FILLED"


def _top_counts(counter: Counter[str], *, limit: int = 3) -> list[dict[str, Any]]:
    return [{"reason": reason, "count": count} for reason, count in counter.most_common(limit)]


def _reason_counts(probe: dict[str, Any]) -> tuple[Counter[str], list[str], list[dict[str, Any]]]:
    summary = _as_dict(probe.get("candidate_intent_summary"))
    prefilter = _as_dict(summary.get("live_event_prefilter"))
    counts: Counter[str] = Counter()

    skip_counts = _as_dict(prefilter.get("skip_counts"))
    for reason, count in skip_counts.items():
        try:
            numeric = int(count)
        except (TypeError, ValueError):
            continue
        if reason and numeric > 0:
            counts[str(reason)] += numeric

    participation_rows: list[dict[str, Any]] = []
    intent_ids: list[str] = []
    for raw in _as_list(prefilter.get("inventory_window_participation")):
        if not isinstance(raw, dict):
            continue
        row = {
            "market_slug": raw.get("market_slug"),
            "dominant_skip_reason": raw.get("dominant_skip_reason"),
            "wallet_eligible_orders": raw.get("wallet_eligible_orders"),
            "our_submits": raw.get("our_submits"),
            "our_fills": raw.get("our_fills"),
            "intent_id": raw.get("intent_id") or "",
            "status": raw.get("status"),
        }
        participation_rows.append(row)
        reason = str(raw.get("dominant_skip_reason") or "").strip()
        if reason and reason not in counts:
            counts[reason] += 1
        intent_id = str(raw.get("intent_id") or "").strip()
        if intent_id:
            intent_ids.append(intent_id)

    for raw in _as_list(summary.get("sample_intents")):
        if isinstance(raw, dict):
            intent_id = str(raw.get("intent_id") or "").strip()
            if intent_id:
                intent_ids.append(intent_id)

    return counts, sorted(set(intent_ids)), participation_rows


def _guard_stamp(guard_state: dict[str, Any], probe: dict[str, Any], probe_path: Path) -> dict[str, Any]:
    profile = _as_dict(guard_state.get("guard_loop_profile"))
    return {
        "probe_generated_at": probe.get("generated_at"),
        "probe_path": str(probe_path),
        "live_guard_generated_at": guard_state.get("generated_at"),
        "live_guard_pid": guard_state.get("pid"),
        "guard_cycle_started_at": profile.get("cycle_started_at"),
        "guard_cycle_duration_s": profile.get("cycle_duration_s") or profile.get("total_s_before_state_write"),
        "guard_code_identity": guard_state.get("guard_code_identity"),
    }


def _member_report(
    target: TargetMember,
    *,
    root: Path,
    day: str,
    live_state: dict[str, Any],
    guard_state: dict[str, Any],
) -> dict[str, Any]:
    probe_path = _resolve(root, target.probe_path)
    probe = load_json(probe_path, default={})
    probe = probe if isinstance(probe, dict) else {}
    summary = _as_dict(probe.get("candidate_intent_summary"))
    day_orders = _orders_for_day(live_state, wallet=target.wallet, day=day)
    filled_orders = [row for row in day_orders if _is_filled(row)]
    counts, intent_ids, participation_rows = _reason_counts(probe)
    if not counts and int(summary.get("history_events") or 0) == 0:
        counts["source_quiet_no_candidate_events"] = 1
    due = len(filled_orders) == 0
    return {
        "label": target.label,
        "wallet": target.wallet,
        "candidate_id": target.candidate_id,
        "probe_candidate_id": probe.get("candidate_id"),
        "probe_source_wallet": summary.get("source_wallet") or probe.get("source_wallet"),
        "due_for_ruling10": due,
        "zero_resolved_basis": "zero live FILLED orders for the UTC day; zero filled implies zero resolved",
        "day_live_orders": len(day_orders),
        "day_filled_orders": len(filled_orders),
        "candidate_history_events": summary.get("history_events"),
        "candidate_build_events_filtered": summary.get("candidate_build_events_filtered"),
        "fresh_candidate_intents": summary.get("fresh_candidate_intents"),
        "top_abstain_reasons": _top_counts(counts),
        "all_abstain_reason_counts": dict(counts),
        "intent_ids": intent_ids,
        "intent_id_report": intent_ids[0] if intent_ids else None,
        "intent_id_basis": "no emitted CopyIntent in probe artifact" if not intent_ids else "probe CopyIntent id",
        "guard_stamp": _guard_stamp(guard_state, probe, probe_path),
        "participation_rows": participation_rows[:10],
    }


def build_report(
    *,
    root: Path = ROOT,
    day: str | None = None,
    live_state_path: str = DEFAULT_LIVE_STATE,
    guard_state_path: str = DEFAULT_GUARD_STATE,
    scorecard_path: str | None = None,
    targets: tuple[TargetMember, ...] = DEFAULT_TARGETS,
) -> dict[str, Any]:
    day = day or _utc_now().strftime("%Y-%m-%d")
    live_state = load_json(_resolve(root, live_state_path), default={})
    guard_state = load_json(_resolve(root, guard_state_path), default={})
    scorecard_path = scorecard_path or DEFAULT_SCORECARD_TEMPLATE.format(day=day)
    scorecard = load_json(_resolve(root, scorecard_path), default={})
    scorecard = scorecard if isinstance(scorecard, dict) else {}
    members = [
        _member_report(target, root=root, day=day, live_state=_as_dict(live_state), guard_state=_as_dict(guard_state))
        for target in targets
    ]
    due_members = [row for row in members if row["due_for_ruling10"]]
    return {
        "schema_version": 1,
        "kind": "ruling10_abstain_probe",
        "flow_stage": "LIVE/MEASURE",
        "ruling_id": RULING_ID,
        "generated_at": _iso_now(),
        "day": day,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "selection_rule": "include e88d/32de/d60c only when they have zero resolved fills for the UTC day",
        "summary": {
            "members_checked": len(members),
            "members_due": len(due_members),
            "due_labels": [row["label"] for row in due_members],
            "scorecard_generated_at": scorecard.get("generated_at"),
            "scorecard_day_pnl": _as_dict(_as_dict(scorecard.get("canonical_pnl_truth")).get("by_day")).get(day, {}).get(
                "pnl_usd"
            ),
            "scorecard_day_fills": _as_dict(_as_dict(scorecard.get("canonical_pnl_truth")).get("by_day")).get(day, {}).get(
                "fills"
            ),
            "guard_generated_at": guard_state.get("generated_at") if isinstance(guard_state, dict) else None,
        },
        "members": members,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--day", default=None)
    parser.add_argument("--live-state", default=DEFAULT_LIVE_STATE)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--scorecard", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_report(
        day=args.day,
        live_state_path=args.live_state,
        guard_state_path=args.guard_state,
        scorecard_path=args.scorecard,
    )
    output = _resolve(ROOT, args.output)
    atomic_write_json(output, payload)
    print(
        {
            "output": str(output),
            "members_due": payload["summary"]["members_due"],
            "due_labels": payload["summary"]["due_labels"],
            "day": payload["day"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
