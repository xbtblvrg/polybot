#!/usr/bin/env python3
"""Build the weekend roster exit audit artifact.

The report reconciles the live qualified active set with the auto-degrade
registry and names every weekend-era shrink/exit row with an explicit ruling.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any


DEFAULT_GUARD_STATE = Path("data/research/wallet_copy_live_guard_state.json")
DEFAULT_AUTO_DEGRADE_STATE = Path("data/research/wallet_copy_active_set_auto_degrade_state.json")
DEFAULT_OUTPUT = Path("data/research/weekend_exit_audit_latest.json")
TEMPORAL_LIVE_EFFECT_DIRECTION_ID = "2026-07-11T10:06Z-fable-temporal-label-live-effect"
TEMPORAL_LIVE_EFFECT_REF = "docs/agents/HANDOFF_ARCHIVE_2026-07.md:35400"


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _wallet(row: dict[str, Any]) -> str:
    return str(row.get("source_wallet") or row.get("wallet") or "").lower()


def _candidate(row: dict[str, Any]) -> str:
    return str(row.get("candidate_id") or row.get("member_id") or "")


def _row_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (_wallet(row), _candidate(row), str(row.get("action") or ""))


def _append_row(rows: list[dict[str, Any]], seen: set[tuple[str, str, str]], row: dict[str, Any]) -> None:
    if not row.get("direction_id"):
        row["action"] = "RE_EVALUATE_REQUIRED"
        row["next_action"] = "return member to readmission/re-evaluation queue; no silent exit accepted"
    row.setdefault("next_action", "no re-evaluation required; ruling id present")
    key = _row_key(row)
    if key not in seen:
        seen.add(key)
        rows.append(row)


def _block_row(block: dict[str, Any], action: str, evidence_ref: str) -> dict[str, Any]:
    return {
        "candidate_id": _candidate(block),
        "source_wallet": _wallet(block),
        "action": action,
        "direction_id": block.get("direction_id") or block.get("fable_cap_direction_id"),
        "applied_at": block.get("applied_at") or block.get("demoted_at") or block.get("disabled_at"),
        "evidence_ref": evidence_ref,
        "reason": block.get("reason"),
        "flow_stage": block.get("flow_stage"),
    }


def _weekend_rows(auto_state: dict[str, Any]) -> list[dict[str, Any]]:
    block = auto_state.get("latest_weekend_roster_alignment")
    if not isinstance(block, dict):
        return []
    direction_id = block.get("direction_id")
    applied_at = block.get("applied_at")
    rows: list[dict[str, Any]] = []
    for action in block.get("actions") or []:
        if not isinstance(action, dict):
            continue
        rows.append(
            {
                "candidate_id": _candidate(action),
                "source_wallet": _wallet(action),
                "action": action.get("action") or "WEEKEND_ALIGNMENT",
                "direction_id": direction_id,
                "applied_at": applied_at,
                "evidence_ref": "wallet_copy_active_set_auto_degrade_state.latest_weekend_roster_alignment",
                "classification": action.get("classification"),
                "reason": action.get("reason"),
                "auto_return_at": action.get("auto_return_at"),
                "monday_return_rule": action.get("monday_return_rule"),
                "flow_stage": block.get("flow_stage"),
            }
        )
    return rows


def _temporal_exclusion_rows(
    guard_state: dict[str, Any],
    auto_state: dict[str, Any],
    active_wallets: set[str],
) -> list[dict[str, Any]]:
    runtime = guard_state.get("active_set_runtime") if isinstance(guard_state.get("active_set_runtime"), dict) else {}
    exclusion = runtime.get("temporal_slice_exclusion") if isinstance(runtime.get("temporal_slice_exclusion"), dict) else {}
    excluded_by_wallet = {
        _wallet(row): row
        for row in exclusion.get("excluded_members") or []
        if isinstance(row, dict) and _wallet(row)
    }
    members = auto_state.get("members") if isinstance(auto_state.get("members"), list) else []
    rows: list[dict[str, Any]] = []
    for member in members:
        if not isinstance(member, dict) or member.get("enabled") is not True:
            continue
        wallet = _wallet(member)
        if not wallet or wallet in active_wallets:
            continue
        excluded = excluded_by_wallet.get(wallet)
        if excluded:
            matched_slice = excluded.get("matched_slice") if isinstance(excluded.get("matched_slice"), dict) else {}
            rows.append(
                {
                    "candidate_id": _candidate(member),
                    "source_wallet": wallet,
                    "action": "QUALIFIED_FILTER_TEMPORAL_EXCLUSION",
                    "direction_id": TEMPORAL_LIVE_EFFECT_DIRECTION_ID,
                    "applied_at": exclusion.get("as_of") or guard_state.get("generated_at"),
                    "evidence_ref": (
                        "wallet_copy_live_guard_state.active_set_runtime.temporal_slice_exclusion; "
                        f"{TEMPORAL_LIVE_EFFECT_REF}"
                    ),
                    "classification": excluded.get("classification"),
                    "reason": excluded.get("reason"),
                    "matched_slice": {
                        "slice": matched_slice.get("slice"),
                        "label": matched_slice.get("label"),
                        "resolved_trades": matched_slice.get("resolved_trades"),
                        "roi_pct": matched_slice.get("roi_pct"),
                        "pnl_usd": matched_slice.get("pnl_usd"),
                    },
                    "registry_enabled": True,
                    "active_set_present": False,
                    "flow_stage": exclusion.get("flow_stage"),
                }
            )
        else:
            rows.append(
                {
                    "candidate_id": _candidate(member),
                    "source_wallet": wallet,
                    "action": "REGISTRY_ENABLED_OUTSIDE_QUALIFIED_SET",
                    "direction_id": None,
                    "applied_at": guard_state.get("generated_at"),
                    "evidence_ref": "auto_degrade member enabled=true but absent from active_set.members",
                    "reason": "no temporal exclusion or demotion ruling found in named state inputs",
                    "registry_enabled": True,
                    "active_set_present": False,
                }
            )
    return rows


def build_report(guard_state: dict[str, Any], auto_state: dict[str, Any]) -> dict[str, Any]:
    active_members = guard_state.get("active_set", {}).get("members") if isinstance(guard_state.get("active_set"), dict) else []
    active_members = [row for row in active_members or [] if isinstance(row, dict)]
    active_wallets = {_wallet(row) for row in active_members if _wallet(row)}
    registry_members = auto_state.get("members") if isinstance(auto_state.get("members"), list) else []
    registry_enabled_wallets = {
        _wallet(row)
        for row in registry_members
        if isinstance(row, dict) and row.get("enabled") is True and _wallet(row)
    }

    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in _weekend_rows(auto_state):
        if row["source_wallet"] in active_wallets and row["action"] == "WEEKEND_BENCHED":
            row["action"] = "WEEKEND_BENCHED_AUTO_RETURNED_ACTIVE"
            row["active_set_present"] = True
        _append_row(rows, seen, row)

    for key, action in (
        ("latest_5e4a_t2_defend_demotion", "KEEP_DEMOTED"),
        ("latest_ac05_e4_suppression", "SUPPRESSED"),
    ):
        block = auto_state.get(key)
        if isinstance(block, dict):
            _append_row(rows, seen, _block_row(block, action, f"wallet_copy_active_set_auto_degrade_state.{key}"))

    for row in _temporal_exclusion_rows(guard_state, auto_state, active_wallets):
        _append_row(rows, seen, row)

    rows.sort(key=lambda row: (str(row.get("source_wallet") or ""), str(row.get("candidate_id") or ""), str(row.get("action") or "")))
    re_eval = [row for row in rows if row.get("action") == "RE_EVALUATE_REQUIRED"]
    return {
        "schema_version": 1,
        "kind": "weekend_exit_audit",
        "generated_at": _now(),
        "flow_stage": "LIVE/ROTATE/DEFEND",
        "cutoff_ts": "2026-07-11T00:00:00Z",
        "inputs": {
            "guard_state_generated_at": guard_state.get("generated_at"),
            "auto_degrade_updated_at": auto_state.get("updated_at"),
            "guard_state_path": str(DEFAULT_GUARD_STATE),
            "auto_degrade_state_path": str(DEFAULT_AUTO_DEGRADE_STATE),
        },
        "active_set": {
            "qualified_member_count": len(active_members),
            "wallets": sorted(active_wallets),
            "candidate_ids": sorted(_candidate(row) for row in active_members if _candidate(row)),
        },
        "registry": {
            "enabled_wallet_count": len(registry_enabled_wallets),
            "enabled_outside_active_set_count": len(registry_enabled_wallets - active_wallets),
            "enabled_outside_active_set_wallets": sorted(registry_enabled_wallets - active_wallets),
        },
        "rows": rows,
        "summary": {
            "row_count": len(rows),
            "re_evaluate_required_count": len(re_eval),
            "re_evaluate_required_wallets": [row.get("source_wallet") for row in re_eval],
            "verdict": "RE_EVALUATE_REQUIRED" if re_eval else "ALL_EXITS_HAVE_RULING_ID",
            "next_action": (
                "return null-direction rows for readmission review before address-form map"
                if re_eval
                else "commit artifact, then proceed to address-form map"
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--guard-state", default=str(DEFAULT_GUARD_STATE))
    parser.add_argument("--auto-degrade-state", default=str(DEFAULT_AUTO_DEGRADE_STATE))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    report = build_report(_load_json(Path(args.guard_state)), _load_json(Path(args.auto_degrade_state)))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report["summary"], sort_keys=True))
    return 0 if report["summary"]["re_evaluate_required_count"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
