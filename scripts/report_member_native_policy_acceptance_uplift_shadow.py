#!/usr/bin/env python3
"""Persist a fail-closed paper cohort for member-native policy uplift."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402

F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
MIN_INCREMENTAL_RESOLVED_WINDOWS = 20


def _parse_utc(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _load(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _snapshot_hash(policies: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(policies).encode()).hexdigest()


def _new_state(policies: dict[str, Any], sample_started_at: str) -> dict[str, Any]:
    frozen = {str(wallet).lower(): value for wallet, value in sorted(policies.items())}
    digest = _snapshot_hash(frozen)
    return {
        "schema_version": 1,
        "kind": "member_native_policy_acceptance_uplift_cohort_state",
        "cohort_id": f"member-native-{sample_started_at}-{digest[:12]}",
        "sample_started_at": sample_started_at,
        "frozen_policy_by_wallet": frozen,
        "frozen_policy_snapshot_hash": digest,
        "intent_observations": {},
    }


def _observation(row: dict[str, Any]) -> dict[str, Any]:
    outcome = row.get("realized_paper_outcome")
    outcome = outcome if isinstance(outcome, dict) else {}
    return {
        "winning_intent_id": str(row.get("winning_intent_id") or ""),
        "winning_source_wallet": str(row.get("winning_source_wallet") or "").lower(),
        "winning_policy_id": str(row.get("winning_policy_id") or ""),
        "market_slug": str(row.get("market_slug") or ""),
        "window_start_s": row.get("window_start_s"),
        "cycle_generated_at": row.get("cycle_generated_at"),
        "expected_fee_usd": row.get("expected_fee_usd"),
        "status": str(outcome.get("status") or ""),
        "paper_pnl_usd": outcome.get("paper_pnl_usd"),
    }


def _observation_rank(row: dict[str, Any]) -> tuple[int, int]:
    resolved = row.get("status") == "RESOLVED"
    measured = _finite(row.get("paper_pnl_usd")) is not None and _finite(
        row.get("expected_fee_usd")
    ) is not None
    return int(resolved), int(resolved and measured)


def _merge_observation(old: dict[str, Any] | None, new: dict[str, Any]) -> dict[str, Any]:
    if not old:
        return new
    old_rank = _observation_rank(old)
    new_rank = _observation_rank(new)
    if new_rank != old_rank:
        return new if new_rank > old_rank else old
    return min((old, new), key=_canonical)


def build_report(
    guard: dict[str, Any],
    poller: dict[str, Any],
    routing: dict[str, Any],
    funnel: dict[str, Any],
    *,
    generated_at: str,
    sample_started_at: str,
    incumbent_wallet: str = F418,
    cohort_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    current_policies = runtime.get("policy_by_wallet") if isinstance(runtime.get("policy_by_wallet"), dict) else {}
    current_policies = {str(wallet).lower(): value for wallet, value in current_policies.items()}
    state = dict(cohort_state or {})
    if not state.get("cohort_id"):
        state = _new_state(current_policies, sample_started_at)
    frozen = state.get("frozen_policy_by_wallet")
    frozen = frozen if isinstance(frozen, dict) else {}
    frozen = {str(wallet).lower(): value for wallet, value in frozen.items()}
    state["frozen_policy_by_wallet"] = frozen
    state["frozen_policy_snapshot_hash"] = _snapshot_hash(frozen)
    state["sample_started_at"] = str(state.get("sample_started_at") or sample_started_at)
    start = _parse_utc(state["sample_started_at"])
    incumbent = incumbent_wallet.lower()

    members = runtime.get("members") if isinstance(runtime.get("members"), list) else []
    member_by_wallet = {
        str(row.get("source_wallet") or "").lower(): row
        for row in members
        if isinstance(row, dict) and row.get("source_wallet")
    }
    observations = state.get("intent_observations")
    observations = dict(observations) if isinstance(observations, dict) else {}
    missing_intent_ids = 0
    policy_mismatches: list[dict[str, str]] = []
    for raw in routing.get("rows") or []:
        if not isinstance(raw, dict):
            continue
        cycle_at = _parse_utc(raw.get("cycle_generated_at"))
        if start is not None and (cycle_at is None or cycle_at < start):
            continue
        row = _observation(raw)
        wallet = row["winning_source_wallet"]
        if wallet not in frozen:
            continue
        intent_id = row["winning_intent_id"]
        if not intent_id:
            missing_intent_ids += 1
            continue
        expected_policy = str((frozen.get(wallet) or {}).get("policy_id") or "")
        if not expected_policy or row["winning_policy_id"] != expected_policy:
            policy_mismatches.append(
                {
                    "winning_intent_id": intent_id,
                    "source_wallet": wallet,
                    "expected_policy_id": expected_policy,
                    "observed_policy_id": row["winning_policy_id"],
                }
            )
            continue
        observations[intent_id] = _merge_observation(observations.get(intent_id), row)
    state["intent_observations"] = dict(sorted(observations.items()))
    state["seen_intent_ids"] = sorted(observations)
    state["last_generated_at"] = generated_at
    state["run_count"] = int(_num(state.get("run_count"))) + 1

    windows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in observations.values():
        wallet = str(row.get("winning_source_wallet") or "").lower()
        if wallet not in frozen:
            continue
        window = row.get("window_start_s")
        window_key = str(window) if window is not None else str(row.get("market_slug") or "")
        if not window_key:
            continue
        windows.setdefault((wallet, window_key), []).append(row)

    resolved_by_wallet: dict[str, dict[str, Any]] = {}
    exclusions = {
        "missing_intent_id_rows": missing_intent_ids,
        "policy_mismatch_rows": len(policy_mismatches),
        "pending_windows": 0,
        "unmeasured_resolved_windows": 0,
        "unmeasured_resolved_intents": 0,
    }
    state_windows: dict[str, Any] = {}
    for (wallet, window_key), intent_rows in sorted(windows.items()):
        statuses = [row.get("status") for row in intent_rows]
        state_key = f"{wallet}|{window_key}"
        if not intent_rows or any(status != "RESOLVED" for status in statuses):
            exclusions["pending_windows"] += 1
            state_windows[state_key] = {"status": "PENDING", "intent_count": len(intent_rows)}
            continue
        measured: list[tuple[float, float]] = []
        for row in intent_rows:
            gross = _finite(row.get("paper_pnl_usd"))
            fee = _finite(row.get("expected_fee_usd"))
            if gross is None or fee is None:
                exclusions["unmeasured_resolved_intents"] += 1
            else:
                measured.append((gross, fee))
        if len(measured) != len(intent_rows):
            exclusions["unmeasured_resolved_windows"] += 1
            state_windows[state_key] = {"status": "UNMEASURED", "intent_count": len(intent_rows)}
            continue
        gross = sum(item[0] for item in measured)
        fee = sum(item[1] for item in measured)
        post_fee = gross - fee
        bucket = resolved_by_wallet.setdefault(
            wallet,
            {
                "measured_windows": 0,
                "measured_intents": 0,
                "gross_pnl_usd": 0.0,
                "expected_fee_usd": 0.0,
                "post_fee_pnl_usd": 0.0,
                "positive_windows": 0,
            },
        )
        bucket["measured_windows"] += 1
        bucket["measured_intents"] += len(intent_rows)
        bucket["gross_pnl_usd"] += gross
        bucket["expected_fee_usd"] += fee
        bucket["post_fee_pnl_usd"] += post_fee
        bucket["positive_windows"] += int(post_fee > 0.0)
        state_windows[state_key] = {
            "status": "MEASURED",
            "intent_count": len(intent_rows),
            "gross_pnl_usd": round(gross, 6),
            "expected_fee_usd": round(fee, 6),
            "post_fee_pnl_usd": round(post_fee, 6),
        }
    state["window_aggregates"] = state_windows

    fetch_meta = poller.get("fetch_meta") if isinstance(poller.get("fetch_meta"), dict) else {}
    rows: list[dict[str, Any]] = []
    empty = {
        "measured_windows": 0,
        "measured_intents": 0,
        "gross_pnl_usd": 0.0,
        "expected_fee_usd": 0.0,
        "post_fee_pnl_usd": 0.0,
        "positive_windows": 0,
    }
    for wallet, policy in sorted(frozen.items()):
        meta = fetch_meta.get(wallet) if isinstance(fetch_meta.get(wallet), dict) else {}
        feedback = meta.get("policy_feedback") if isinstance(meta.get("policy_feedback"), dict) else {}
        measured = resolved_by_wallet.get(wallet, empty)
        rows.append(
            {
                "source_wallet": wallet,
                "candidate_id": (member_by_wallet.get(wallet) or {}).get("candidate_id"),
                "is_incumbent": wallet == incumbent,
                "frozen_policy": dict(policy) if isinstance(policy, dict) else {},
                "raw_rows": int(_num(meta.get("raw_rows"))),
                "normalized_trade_events": int(_num(meta.get("normalized_trade_events"))),
                "policy_compatible_fresh_buy_rows_le_30s": int(
                    _num(feedback.get("policy_compatible_fresh_buy_rows_le_30s"))
                ),
                **{
                    key: round(value, 6) if isinstance(value, float) else value
                    for key, value in measured.items()
                },
                "resolved_windows": int(measured["measured_windows"]),
                "evidence_generated_at": meta.get("generated_at") or poller.get("generated_at"),
            }
        )

    incremental = [row for row in rows if not row["is_incumbent"]]
    def _sum(key: str, selected: list[dict[str, Any]]) -> float:
        return round(sum(_num(row.get(key)) for row in selected), 6)

    incremental_windows = int(_sum("measured_windows", incremental))
    incremental_pnl = _sum("post_fee_pnl_usd", incremental)
    incumbent_rows = [row for row in rows if row["is_incumbent"]]
    incumbent_pnl = _sum("post_fee_pnl_usd", incumbent_rows)
    sample_valid = not policy_mismatches and missing_intent_ids == 0
    sample_gate_pass = (
        sample_valid
        and incremental_windows >= MIN_INCREMENTAL_RESOLVED_WINDOWS
        and incremental_pnl > 0.0
        and incumbent_pnl >= 0.0
    )
    rolling = funnel.get("rolling_30m") if isinstance(funnel.get("rolling_30m"), dict) else {}
    return {
        "schema_version": 2,
        "kind": "member_native_policy_acceptance_uplift_shadow",
        "flow_stage": "OBSERVE/LEARN/SELF-DEV",
        "generated_at": generated_at,
        "cohort_id": state.get("cohort_id"),
        "sample_started_at": state["sample_started_at"],
        "frozen_policy_snapshot_hash": state["frozen_policy_snapshot_hash"],
        "current_policy_snapshot_matches_frozen": _snapshot_hash(current_policies)
        == state["frozen_policy_snapshot_hash"],
        "paper_only": True,
        "live_orders_allowed": False,
        "live_mutation": False,
        "copyintent_parity_violations": 0,
        "single_submitter_preserved": True,
        "sample_valid": sample_valid,
        "sample_invalid_reasons": [
            reason
            for reason, present in (
                ("missing_winning_intent_id", missing_intent_ids > 0),
                ("frozen_policy_mismatch", bool(policy_mismatches)),
            )
            if present
        ],
        "exclusions": exclusions,
        "policy_mismatches": sorted(policy_mismatches, key=_canonical),
        "incumbent_wallet": incumbent,
        "member_count": len(rows),
        "frozen_policy_binding_count": len(frozen),
        "members": rows,
        "incumbent_twin": {
            "measured_windows": int(_sum("measured_windows", incumbent_rows)),
            "measured_intents": int(_sum("measured_intents", incumbent_rows)),
            "gross_pnl_usd": _sum("gross_pnl_usd", incumbent_rows),
            "expected_fee_usd": _sum("expected_fee_usd", incumbent_rows),
            "post_fee_pnl_usd": incumbent_pnl,
            "rolling_30m_policy_eligible_intents": int(
                _num(rolling.get("policy_eligible_unique_intents"))
            ),
            "rolling_30m_accepted_orders": int(_num(rolling.get("actual_accepted_orders"))),
        },
        "incremental": {
            "policy_compatible_fresh_buy_rows_le_30s": int(
                _sum("policy_compatible_fresh_buy_rows_le_30s", incremental)
            ),
            "measured_windows": incremental_windows,
            "resolved_windows": incremental_windows,
            "measured_intents": int(_sum("measured_intents", incremental)),
            "positive_windows": int(_sum("positive_windows", incremental)),
            "gross_pnl_usd": _sum("gross_pnl_usd", incremental),
            "expected_fee_usd": _sum("expected_fee_usd", incremental),
            "post_fee_pnl_usd": incremental_pnl,
        },
        "gate": {
            "minimum_incremental_measured_windows": MIN_INCREMENTAL_RESOLVED_WINDOWS,
            "requires_finite_gross_and_fee": True,
            "requires_incremental_post_fee_pnl_positive": True,
            "requires_no_incumbent_twin_regression": True,
            "sample_valid": sample_valid,
            "pass": sample_gate_pass,
        },
        "cohort_state": state,
        "verdict": (
            "PASS_MEMBER_NATIVE_POLICY_UPLIFT"
            if sample_gate_pass
            else "INVALID_MEMBER_NATIVE_COHORT"
            if not sample_valid
            else "ACCRUE_MEMBER_NATIVE_MEASURED_WINDOWS"
        ),
        "next_action": (
            "Fable promotion ruling with producing-change canary"
            if sample_gate_pass
            else "repair invalid identity/policy rows before accrual"
            if not sample_valid
            else "continue persistent paper cohort until 20 incremental measured windows resolve"
        ),
    }


def _run_once(args: argparse.Namespace) -> dict[str, Any]:
    now = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    state_path = ROOT / args.state
    prior_state = _load(state_path)
    sample_started_at = (
        str(prior_state.get("sample_started_at"))
        if prior_state.get("sample_started_at")
        else args.sample_started_at or now
    )
    report = build_report(
        _load(ROOT / args.guard),
        _load(ROOT / args.poller),
        _load(ROOT / args.routing),
        _load(ROOT / args.funnel),
        generated_at=now,
        sample_started_at=sample_started_at,
        cohort_state=prior_state,
    )
    atomic_write_json(state_path, report["cohort_state"])
    atomic_write_json(ROOT / args.output, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--poller", default="data/research/wallet_copy_active_set_dataapi_poller_state.json")
    parser.add_argument("--routing", default="data/research/routing_shadow_validation_latest.json")
    parser.add_argument("--funnel", default="data/research/f418_acceptance_funnel_latest.json")
    parser.add_argument(
        "--output",
        default="data/research/member_native_policy_acceptance_uplift_shadow_latest.json",
    )
    parser.add_argument(
        "--state",
        default="data/research/member_native_policy_acceptance_uplift_shadow_state.json",
    )
    parser.add_argument("--sample-started-at", default="")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    args = parser.parse_args()
    while True:
        report = _run_once(args)
        print(json.dumps({key: report.get(key) for key in ("generated_at", "cohort_id", "verdict")}, sort_keys=True), flush=True)
        if not args.watch:
            return 0
        time.sleep(max(5.0, args.interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
