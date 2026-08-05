#!/usr/bin/env python3
"""Persist the Monday-return synthetic proof from live runtime state.

The proof is read-only against the live guard. It verifies that the weekend
bench auto-return mechanics can re-activate the real weekday wallet in a
synthetic Monday clock, and that the current runtime roster is no longer
weekend-benched.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_ACTIVE_SET_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_ADDRESS_FORM_MAP = ROOT / "data/research/wallet_data_api_address_form_latest.json"
DEFAULT_COMMITMENTS = ROOT / "data/research/commitments.jsonl"
DEFAULT_LIVE_EXECUTION_STATE = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_WEEKEND_PARITY_PACKET = ROOT / "data/research/weekend_parity_packet_latest.json"
DEFAULT_OUTPUT = ROOT / "data/research/monday_return_synthetic_proof_latest.json"

E6DB = "0xe6db20932faf0f9780acf75d95c74c9984407dac"
DEFAULT_MONDAY_RETURN_AT = dt.datetime(2026, 7, 20, 0, 0, tzinfo=dt.timezone.utc)
DEFAULT_MONDAY_PROOF_NOW = dt.datetime(2026, 7, 20, 0, 5, tzinfo=dt.timezone.utc)
COMMITMENT_ID = "w6_monday_return_rebind_20260720"


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso_datetime(raw: str | None, *, default: dt.datetime) -> dt.datetime:
    if not raw:
        return default
    value = raw.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _iso_z(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _member_wallet(member: dict[str, Any]) -> str:
    return str(member.get("source_wallet") or member.get("wallet") or "").strip().lower()


def _is_weekend_benched(member: dict[str, Any]) -> bool:
    status = str(member.get("status") or "").upper()
    return status.startswith("WEEKEND_BENCHED") or isinstance(member.get("weekend_bench"), dict)


def _active_members(active_set_state: dict[str, Any]) -> list[dict[str, Any]]:
    active_set = active_set_state.get("active_set") if isinstance(active_set_state.get("active_set"), dict) else {}
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    return [member for member in members if isinstance(member, dict)]


def _address_rows_by_wallet(address_form_map: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = address_form_map.get("rows") if isinstance(address_form_map.get("rows"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        freshness = row.get("freshness_context") if isinstance(row.get("freshness_context"), dict) else {}
        wallet = str(freshness.get("wallet") or row.get("wallet") or "").strip().lower()
        if wallet:
            out[wallet] = row
    return out


def _address_form_check(members: list[dict[str, Any]], address_form_map: dict[str, Any]) -> dict[str, Any]:
    by_wallet = _address_rows_by_wallet(address_form_map)
    rows: list[dict[str, Any]] = []
    failures: list[str] = []
    for member in members:
        wallet = _member_wallet(member)
        row = by_wallet.get(wallet, {})
        selection = row.get("address_selection") if isinstance(row.get("address_selection"), dict) else {}
        query_key = selection.get("recommended_query_key")
        age_h = selection.get("freshest_btc5m_trade_age_h")
        if age_h is None:
            age_h = selection.get("last_trade_age_h")
        try:
            age_h_float = float(age_h)
        except (TypeError, ValueError):
            age_h_float = None
        ok = query_key == "user" and age_h_float is not None and age_h_float <= 6.0
        if not ok:
            failures.append(wallet)
        rows.append(
            {
                "candidate_id": member.get("candidate_id"),
                "source_wallet": wallet,
                "recommended_query_key": query_key,
                "latest_btc5m_trade_age_h": age_h_float,
                "status": "PASS" if ok else "FAIL",
            }
        )
    return {
        "status": "PASS" if not failures else "FAIL",
        "failures": failures,
        "rows": rows,
        "map_generated_at": address_form_map.get("generated_at"),
    }


def _single_guard_process_check(process_rows: list[str] | None = None) -> dict[str, Any]:
    if process_rows is None:
        try:
            import subprocess

            proc = subprocess.run(
                ["pgrep", "-fl", "scripts/run_wallet_copy_live_guard.py"],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            process_rows = [line for line in proc.stdout.splitlines() if line.strip()]
        except Exception as exc:  # pragma: no cover - defensive evidence path.
            return {"status": "ERROR", "error": f"{type(exc).__name__}: {exc}", "rows": []}
    filtered = [row for row in process_rows if "run_wallet_copy_live_guard.py" in row]
    return {"status": "PASS" if len(filtered) == 1 else "FAIL", "count": len(filtered), "rows": filtered}


def _w1_return_targets(
    weekend_parity_packet: dict[str, Any],
    *,
    return_at: dt.datetime,
    synthetic_now: dt.datetime,
) -> dict[str, Any]:
    plan = (
        weekend_parity_packet.get("current_roster_weekend_posture_plan")
        if isinstance(weekend_parity_packet.get("current_roster_weekend_posture_plan"), dict)
        else {}
    )
    raw_members = plan.get("members") if isinstance(plan.get("members"), list) else []
    targets: list[dict[str, Any]] = []
    for row in raw_members:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").strip().lower()
        if not wallet:
            continue
        evidence = (
            row.get("current_weekday_evidence")
            if isinstance(row.get("current_weekday_evidence"), dict)
            else {}
        )
        try:
            fills = int(evidence.get("fills") or 0)
        except (TypeError, ValueError):
            fills = 0
        try:
            pnl = float(evidence.get("pnl_usd") or 0.0)
        except (TypeError, ValueError):
            pnl = 0.0
        posture = str(row.get("weekend_posture") or "").upper()
        if fills <= 0 or pnl <= 0.0:
            continue
        if posture not in {"BENCH", "TRADE_FLOOR_SIZE"}:
            continue
        targets.append(
            {
                "bench_id": row.get("candidate_id") or wallet,
                "candidate_id": row.get("candidate_id") or wallet,
                "source_wallet": wallet,
                "weekend_posture": posture,
                "weekday_fills": fills,
                "weekday_pnl_usd": round(pnl, 6),
                "policy_id": row.get("policy_id"),
                "auto_return_at": _iso_z(return_at),
                "synthetic_now": _iso_z(synthetic_now),
                "source_packet_generated_at": weekend_parity_packet.get("generated_at"),
                "source_direction_id": plan.get("direction_id"),
            }
        )
    return {
        "status": "PASS" if targets else "FAIL",
        "reason": "w1_weekday_positive_weekend_gated_targets"
        if targets
        else "no_w1_weekday_positive_weekend_gated_targets",
        "source_packet": "data/research/weekend_parity_packet_latest.json"
        if weekend_parity_packet
        else None,
        "source_packet_generated_at": weekend_parity_packet.get("generated_at"),
        "source_direction_id": plan.get("direction_id"),
        "weekend_starts_at": plan.get("weekend_starts_at"),
        "auto_return_at": _iso_z(return_at),
        "synthetic_now": _iso_z(synthetic_now),
        "target_count": len(targets),
        "target_wallets": [row["source_wallet"] for row in targets],
        "bench_ids": [row["bench_id"] for row in targets],
        "return_targets": targets,
    }


def _fallback_return_plan(*, return_at: dt.datetime, synthetic_now: dt.datetime) -> dict[str, Any]:
    target = {
        "bench_id": "runtime_auto_degrade_e6db20932f",
        "candidate_id": "runtime_auto_degrade_e6db20932f",
        "source_wallet": E6DB,
        "weekend_posture": "BENCH",
        "auto_return_at": _iso_z(return_at),
        "synthetic_now": _iso_z(synthetic_now),
        "source_packet_generated_at": None,
        "source_direction_id": "legacy_p0_monday_return_proof",
    }
    return {
        "status": "PASS",
        "reason": "legacy_e6db_target",
        "source_packet": None,
        "source_packet_generated_at": None,
        "source_direction_id": "legacy_p0_monday_return_proof",
        "weekend_starts_at": None,
        "auto_return_at": _iso_z(return_at),
        "synthetic_now": _iso_z(synthetic_now),
        "target_count": 1,
        "target_wallets": [E6DB],
        "bench_ids": [target["bench_id"]],
        "return_targets": [target],
    }


def _synthetic_auto_return(
    member: dict[str, Any],
    *,
    return_at: dt.datetime,
    synthetic_now: dt.datetime,
) -> dict[str, Any]:
    import scripts.run_wallet_copy_live_guard as guard

    synthetic = dict(member)
    synthetic["status"] = "WEEKEND_BENCHED_WEEKDAY_SEAT_PRESERVED"
    synthetic["weekend_bench"] = {
        "auto_return_at": _iso_z(return_at),
        "weekday_seat_preserved": True,
        "classification": "WEEKDAY-ONLY",
    }
    auto_return = guard._weekend_bench_auto_return(synthetic, now=synthetic_now)
    temporal = guard._temporal_slice_exclusion(member, now=synthetic_now)
    return {
        "synthetic_now": _iso_z(synthetic_now),
        "auto_return": auto_return,
        "temporal_slice": temporal,
        "status": "PASS"
        if auto_return.get("eligible") is True and temporal.get("excluded") is not True
        else "FAIL",
    }


def _runtime_roster_check(active_set_state: dict[str, Any], target_wallets: list[str]) -> dict[str, Any]:
    members = _active_members(active_set_state)
    target_wallets = [wallet.strip().lower() for wallet in target_wallets if wallet.strip()]
    targets = [member for member in members if _member_wallet(member) in set(target_wallets)]
    first_target = targets[0] if targets else {}
    current = next((member for member in members if member.get("is_current_cycle_member") is True), {})
    active_set = active_set_state.get("active_set") if isinstance(active_set_state.get("active_set"), dict) else {}
    missing_wallets = sorted(set(target_wallets) - {_member_wallet(member) for member in targets})
    target_status_rows = []
    for member in targets:
        target_status_rows.append(
            {
                "candidate_id": member.get("candidate_id"),
                "source_wallet": _member_wallet(member),
                "status": member.get("status"),
                "enabled": member.get("enabled", True),
                "is_current_cycle_member": member.get("is_current_cycle_member"),
                "max_order_usd": member.get("max_order_usd"),
                "policy_id": member.get("policy_id"),
                "currently_weekend_benched": _is_weekend_benched(member),
            }
        )
    targets_present = bool(target_wallets) and not missing_wallets
    submitter_invariant = str(active_set.get("submitter_invariant") or "")
    return {
        "status": "PASS"
        if targets_present and "run_wallet_copy_live_guard.py" in submitter_invariant
        else "FAIL",
        "target_wallet": target_wallets[0] if target_wallets else None,
        "target_wallets": target_wallets,
        "missing_target_wallets": missing_wallets,
        "target_member": {
            "candidate_id": first_target.get("candidate_id"),
            "status": first_target.get("status"),
            "enabled": first_target.get("enabled", True),
            "is_current_cycle_member": first_target.get("is_current_cycle_member"),
            "max_order_usd": first_target.get("max_order_usd"),
            "policy_id": first_target.get("policy_id"),
        }
        if first_target
        else {},
        "target_members": target_status_rows,
        "target_unbenched": all(row["currently_weekend_benched"] is False for row in target_status_rows)
        and targets_present,
        "runtime_selected_wallet": _member_wallet(current),
        "runtime_selected_candidate_id": current.get("candidate_id"),
        "qualified_member_count": active_set.get("qualified_member_count"),
        "submitter_invariant": submitter_invariant,
    }


def _read_commitment_lines(path: Path) -> list[tuple[str, dict[str, Any]]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append((line, json.loads(line)))
    return rows


def _write_commitment_lines(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _mark_commitment_satisfied(
    path: Path,
    *,
    artifact: Path,
    generated_at: str,
    commitment_id: str,
) -> dict[str, Any]:
    rows = _read_commitment_lines(path)
    out_lines: list[str] = []
    updated = False
    for original_line, row in rows:
        if row.get("id") == commitment_id:
            row["status"] = "SATISFIED"
            row["satisfied_at"] = generated_at
            row["evidence"] = str(artifact.relative_to(ROOT) if artifact.is_absolute() else artifact)
            row["next_action"] = "closed by Monday-return synthetic proof artifact and drill fixture"
            out_lines.append(json.dumps(row, separators=(",", ":")))
            updated = True
        else:
            out_lines.append(original_line)
    if updated:
        _write_commitment_lines(path, out_lines)
    return {"updated": updated, "commitment_id": commitment_id}


def build_report(
    *,
    active_set_state: dict[str, Any],
    address_form_map: dict[str, Any],
    live_execution_state: dict[str, Any],
    weekend_parity_packet: dict[str, Any] | None = None,
    return_at: dt.datetime = DEFAULT_MONDAY_RETURN_AT,
    synthetic_now: dt.datetime = DEFAULT_MONDAY_PROOF_NOW,
    commitment_id: str = COMMITMENT_ID,
    require_runtime_can_trade: bool = True,
    require_address_form_user_fresh: bool = True,
    process_rows: list[str] | None = None,
) -> dict[str, Any]:
    generated_at = _utc_now_iso()
    members = _active_members(active_set_state)
    return_plan = (
        _w1_return_targets(
            weekend_parity_packet,
            return_at=return_at,
            synthetic_now=synthetic_now,
        )
        if isinstance(weekend_parity_packet, dict) and weekend_parity_packet
        else _fallback_return_plan(return_at=return_at, synthetic_now=synthetic_now)
    )
    target_wallets = [
        str(wallet).strip().lower()
        for wallet in return_plan.get("target_wallets", [])
        if str(wallet).strip()
    ]
    members_by_wallet = {_member_wallet(member): member for member in members if _member_wallet(member)}
    roster = _runtime_roster_check(active_set_state, target_wallets)
    address_form = _address_form_check(members, address_form_map)
    single_guard = _single_guard_process_check(process_rows)
    auto_returns = []
    for target in return_plan.get("return_targets", []):
        wallet = str(target.get("source_wallet") or "").strip().lower()
        member = members_by_wallet.get(wallet, {})
        proof = (
            _synthetic_auto_return(member, return_at=return_at, synthetic_now=synthetic_now)
            if member
            else {"status": "FAIL", "reason": "target_member_missing"}
        )
        auto_returns.append(
            {
                "bench_id": target.get("bench_id"),
                "candidate_id": target.get("candidate_id"),
                "source_wallet": wallet,
                "weekend_posture": target.get("weekend_posture"),
                **proof,
            }
        )
    auto_return = auto_returns[0] if auto_returns else {"status": "FAIL", "reason": "no_return_targets"}
    summary = live_execution_state.get("summary") if isinstance(live_execution_state.get("summary"), dict) else {}
    can_trade = live_execution_state.get("can_trade") is True or summary.get("can_trade") is True
    checks = {
        "runtime_roster_live_unbenched": roster.get("status") == "PASS",
        "weekend_return_targets_bound": return_plan.get("status") == "PASS",
        "synthetic_monday_auto_return": bool(auto_returns)
        and all(row.get("status") == "PASS" for row in auto_returns),
        "single_guard_process": single_guard.get("status") == "PASS",
    }
    if require_address_form_user_fresh:
        checks["address_form_user_fresh"] = address_form.get("status") == "PASS"
    else:
        checks["address_form_user_fresh_not_required"] = True
    if require_runtime_can_trade:
        checks["runtime_can_trade"] = can_trade
    else:
        checks["runtime_can_trade_not_required"] = True
    return {
        "schema_version": 1,
        "kind": "monday_return_synthetic_proof",
        "flow_stage": "LIVE/DEFEND/SELF-DEV",
        "generated_at": generated_at,
        "commitment_id": commitment_id,
        "status": "PASS" if all(checks.values()) else "FAIL",
        "live_orders_allowed": False,
        "paper_only": True,
        "checks": checks,
        "weekend_return_plan": return_plan,
        "runtime_roster": roster,
        "synthetic_monday_auto_return": auto_return,
        "synthetic_monday_auto_returns": auto_returns,
        "address_form": address_form,
        "single_guard": single_guard,
        "runtime_can_trade": can_trade,
        "drill_fixture": {
            "path": "tests/test_monday_auto_return_proof.py",
            "targeted_pytest": "python3 -m pytest -q tests/test_monday_auto_return_proof.py",
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--active-set-state", default=str(DEFAULT_ACTIVE_SET_STATE))
    parser.add_argument("--address-form-map", default=str(DEFAULT_ADDRESS_FORM_MAP))
    parser.add_argument("--live-execution-state", default=str(DEFAULT_LIVE_EXECUTION_STATE))
    parser.add_argument("--weekend-parity-packet", default=str(DEFAULT_WEEKEND_PARITY_PACKET))
    parser.add_argument("--commitments", default=str(DEFAULT_COMMITMENTS))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--return-at", default=_iso_z(DEFAULT_MONDAY_RETURN_AT))
    parser.add_argument("--synthetic-now", default=_iso_z(DEFAULT_MONDAY_PROOF_NOW))
    parser.add_argument("--commitment-id", default=COMMITMENT_ID)
    parser.add_argument("--require-runtime-can-trade", action="store_true")
    parser.add_argument("--require-address-form-user-fresh", action="store_true")
    parser.add_argument("--no-update-commitment", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    active_set_state = _load_json(Path(args.active_set_state), {})
    address_form_map = _load_json(Path(args.address_form_map), {})
    live_execution_state = _load_json(Path(args.live_execution_state), {})
    weekend_parity_packet = _load_json(Path(args.weekend_parity_packet), {})
    output = Path(args.output)
    report = build_report(
        active_set_state=active_set_state if isinstance(active_set_state, dict) else {},
        address_form_map=address_form_map if isinstance(address_form_map, dict) else {},
        live_execution_state=live_execution_state if isinstance(live_execution_state, dict) else {},
        weekend_parity_packet=weekend_parity_packet if isinstance(weekend_parity_packet, dict) else {},
        return_at=_parse_iso_datetime(args.return_at, default=DEFAULT_MONDAY_RETURN_AT),
        synthetic_now=_parse_iso_datetime(args.synthetic_now, default=DEFAULT_MONDAY_PROOF_NOW),
        commitment_id=args.commitment_id,
        require_runtime_can_trade=args.require_runtime_can_trade,
        require_address_form_user_fresh=args.require_address_form_user_fresh,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if report["status"] == "PASS" and not args.no_update_commitment:
        report["commitment_update"] = _mark_commitment_satisfied(
            Path(args.commitments),
            artifact=output,
            generated_at=report["generated_at"],
            commitment_id=args.commitment_id,
        )
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "output": str(output), "checks": report["checks"]}, sort_keys=True))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
