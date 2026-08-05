#!/usr/bin/env python3
"""Build the paper-only F2 prewarm and frozen-fingerprint backup rank shadow."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.store import atomic_write_json
from src.wallet_copy.venue_executability import venue_gate_summary

DEFAULT_FINGERPRINT_EVIDENCE = (
    "data/research/wide_policy_fingerprint_evidence_latest.json"
)
DEFAULT_DEADMAN = "data/research/order_flow_deadman_state.json"
DEFAULT_OUTPUT = (
    "data/research/frozen_fingerprint_f2_prewarm_backup_rank_shadow_latest.json"
)
DEFAULT_PRIMARY_WALLET = "0xc1b4bfdc36eaa5c09e7231c61e5e44c5463f624f"
DEFAULT_PRIMARY_FINGERPRINT = (
    "c8ee5a7f50008f4f372961d7678319c446f325a07f6d4a97efc4ad239bc23af2"
)
FABLE_PAPER_FREEZE_PRIORITY: tuple[str, ...] = ()


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def _frontier_rows(deadman: dict[str, Any]) -> list[dict[str, Any]]:
    choke = deadman.get("policy_choke")
    if not isinstance(choke, dict):
        return []
    evidence_packets: list[dict[str, Any]] = []
    actuator = choke.get("actuator")
    if isinstance(actuator, dict) and isinstance(actuator.get("candidate_evidence"), dict):
        evidence_packets.append(actuator["candidate_evidence"])
    drought = choke.get("source_roster_drought")
    if isinstance(drought, dict) and isinstance(drought.get("candidate_evidence"), dict):
        evidence_packets.append(drought["candidate_evidence"])
    rows: list[dict[str, Any]] = []
    for evidence in evidence_packets:
        for key in ("rows", "nearest_frontier"):
            rows.extend(
                row
                for row in evidence.get(key) or []
                if isinstance(row, dict)
            )
    return rows


def _cooloffs(deadman: dict[str, Any]) -> dict[str, str]:
    raw = deadman.get("policy_choke_rung_b_cooloffs")
    if not isinstance(raw, dict):
        return {}
    return {str(wallet).lower(): str(until) for wallet, until in raw.items() if until}


def _cell_identity(cell: dict[str, Any]) -> tuple[str, str]:
    identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
    return (
        str(identity.get("wallet") or "").lower(),
        str(cell.get("wide_policy_fingerprint") or ""),
    )


def _healthy_freeze_cell(
    cell: dict[str, Any],
    *,
    cooloffs: dict[str, str],
    deadman: dict[str, Any],
    allow_cooloff_for_paper: bool = False,
) -> bool:
    wallet, fingerprint = _cell_identity(cell)
    rescore = venue_gate_summary(cell)
    frontier = _frontier_rows(deadman)
    exact_rows = [
        row
        for row in frontier
        if (
            str(row.get("wallet") or "").lower(),
            str(row.get("wide_policy_fingerprint") or ""),
        )
        == (wallet, fingerprint)
    ]
    wallet_rows = [
        row
        for row in frontier
        if str(row.get("wallet") or "").lower() == wallet
    ]
    authority_rows = exact_rows or wallet_rows
    terminal_excluded = any(
        isinstance(row.get("checks"), dict)
        and row["checks"].get("not_terminal_park_red_clock_or_measured_loser")
        is False
        for row in authority_rows
    )
    return bool(
        wallet
        and (allow_cooloff_for_paper or wallet not in cooloffs)
        and not terminal_excluded
        and num(rescore.get("post_fee_pnl_usd")) > 0
        and num(rescore.get("roi_pct")) > 0
        and num(rescore.get("first_half_post_fee_pnl_usd")) > 0
        and num(rescore.get("second_half_post_fee_pnl_usd")) > 0
    )


def select_auto_primary(
    fingerprint_evidence: dict[str, Any],
    deadman: dict[str, Any],
    previous_shadow: dict[str, Any],
) -> tuple[str, str, dict[str, Any]]:
    """Hold a healthy primary; otherwise retarget to the best lawful freeze row."""

    cells = [
        cell
        for cell in fingerprint_evidence.get("cells") or []
        if isinstance(cell, dict)
    ]
    by_identity = {_cell_identity(cell): cell for cell in cells}
    cooloffs = _cooloffs(deadman)
    previous = (
        previous_shadow.get("primary")
        if isinstance(previous_shadow.get("primary"), dict)
        else {}
    )
    previous_identity = (
        str(previous.get("wallet") or "").lower(),
        str(previous.get("wide_policy_fingerprint") or ""),
    )
    previous_cell = by_identity.get(previous_identity)
    if previous_cell is not None and _healthy_freeze_cell(
        previous_cell, cooloffs=cooloffs, deadman=deadman
    ):
        return (
            *previous_identity,
            {
                "action": "HOLD_HEALTHY_PRIMARY",
                "previous_wallet": previous_identity[0],
                "previous_fingerprint": previous_identity[1],
            },
        )

    freeze_overrides = (
        fingerprint_evidence.get("freeze_overrides")
        if isinstance(fingerprint_evidence.get("freeze_overrides"), dict)
        else {}
    )
    candidates: list[dict[str, Any]] = []
    for wallet, override in freeze_overrides.items():
        if not isinstance(override, dict):
            continue
        identity = (
            str(wallet).lower(),
            str(override.get("wide_policy_fingerprint") or ""),
        )
        cell = by_identity.get(identity)
        if cell is not None and _healthy_freeze_cell(
            cell, cooloffs=cooloffs, deadman=deadman
        ):
            candidates.append(cell)
    priority_candidates: list[dict[str, Any]] = []
    for wallet in FABLE_PAPER_FREEZE_PRIORITY:
        override = freeze_overrides.get(wallet)
        if not isinstance(override, dict):
            continue
        cell = by_identity.get(
            (wallet, str(override.get("wide_policy_fingerprint") or ""))
        )
        if cell is not None and _healthy_freeze_cell(
            cell,
            cooloffs=cooloffs,
            deadman=deadman,
            allow_cooloff_for_paper=True,
        ):
            priority_candidates.append(cell)
    if priority_candidates:
        wallet, fingerprint = _cell_identity(priority_candidates[0])
        return (
            wallet,
            fingerprint,
            {
                "action": "RETARGET_FABLE_PAPER_FREEZE_PRIORITY",
                "previous_wallet": previous_identity[0] or None,
                "previous_fingerprint": previous_identity[1] or None,
                "selected_wallet": wallet,
                "selected_fingerprint": fingerprint,
                "paper_only": True,
                "live_eligibility_granted": False,
                "paper_cooloff_ignored": wallet in cooloffs,
                "rule": (
                    "exclude terminal/red-clock identities; select the first "
                    "both-halves-positive freeze_override in the directed "
                    "b27b paper-prewarm priority"
                ),
            },
        )
    if not candidates:
        raise ValueError("no non-cooloff both-halves-positive freeze primary")
    candidates.sort(
        key=lambda cell: (
            -int(
                venue_gate_summary(cell).get("resolved")
                or 0
            ),
            -num(
                venue_gate_summary(cell).get("post_fee_pnl_usd")
            ),
            *_cell_identity(cell),
        )
    )
    wallet, fingerprint = _cell_identity(candidates[0])
    return (
        wallet,
        fingerprint,
        {
            "action": "RETARGET_UNHEALTHY_PRIMARY",
            "previous_wallet": previous_identity[0] or None,
            "previous_fingerprint": previous_identity[1] or None,
            "selected_wallet": wallet,
            "selected_fingerprint": fingerprint,
            "rule": (
                "drop immediately when post_fee<=0, roi<=0, either chronological "
                "half<=0, or wallet is in cooloff; select the highest-resolved "
                "non-cooloff both-halves-positive freeze_override"
            ),
        },
    )


def build_shadow(
    fingerprint_evidence: dict[str, Any],
    deadman: dict[str, Any],
    *,
    primary_wallet: str,
    primary_fingerprint: str,
) -> dict[str, Any]:
    primary_wallet = primary_wallet.lower()
    frontier = _frontier_rows(deadman)
    frontier_by_identity = {
        (
            str(row.get("wallet") or "").lower(),
            str(row.get("wide_policy_fingerprint") or ""),
        ): row
        for row in frontier
    }
    frontier_by_wallet: dict[str, dict[str, Any]] = {}
    for row in frontier:
        wallet = str(row.get("wallet") or "").lower()
        if not wallet:
            continue
        current = frontier_by_wallet.get(wallet, {})
        row_checks = row.get("checks") if isinstance(row.get("checks"), dict) else {}
        current_checks = (
            current.get("checks") if isinstance(current.get("checks"), dict) else {}
        )
        row_rank = (
            int(row.get("fresh_own_source_buy_rows_30m") or 0),
            row_checks.get("f4_external_liveness") is True,
            row_checks.get("own_evidenced_policy_available") is True,
        )
        current_rank = (
            int(current.get("fresh_own_source_buy_rows_30m") or 0),
            current_checks.get("f4_external_liveness") is True,
            current_checks.get("own_evidenced_policy_available") is True,
        )
        if not current or row_rank > current_rank:
            frontier_by_wallet[wallet] = row
    cooloffs = _cooloffs(deadman)
    cells = [
        cell
        for cell in fingerprint_evidence.get("cells") or []
        if isinstance(cell, dict)
    ]
    primary = next(
        (
            cell
            for cell in cells
            if str(cell.get("wide_policy_fingerprint") or "") == primary_fingerprint
            and str((cell.get("identity") or {}).get("wallet") or "").lower()
            == primary_wallet
        ),
        None,
    )
    if primary is None:
        raise ValueError("frozen primary wallet/fingerprint cell is absent")

    # F2 source freshness and F4 liveness are wallet-scoped gates. An exact
    # fingerprint row is not required when another row for the same wallet
    # carries the current direct-source authority.
    primary_frontier = frontier_by_wallet.get(
        primary_wallet,
        frontier_by_identity.get((primary_wallet, primary_fingerprint), {}),
    )
    primary_f2_count = int(primary_frontier.get("fresh_own_source_buy_rows_30m") or 0)
    f2_minimum = 10
    candidate_evidence = (
        ((deadman.get("policy_choke") or {}).get("actuator") or {}).get(
            "candidate_evidence"
        )
        or {}
    )
    gate_digits = candidate_evidence.get("gate_digits") or {}
    f2_minimum = int(gate_digits.get("f2_min_fresh_own_source_buy_rows_30m") or 10)

    backups: list[dict[str, Any]] = []
    for cell in cells:
        identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
        wallet = str(identity.get("wallet") or "").lower()
        fingerprint = str(cell.get("wide_policy_fingerprint") or "")
        if (wallet, fingerprint) == (primary_wallet, primary_fingerprint):
            continue
        rescore = venue_gate_summary(cell)
        pnl = num(rescore.get("post_fee_pnl_usd"))
        roi = rescore.get("roi_pct")
        if pnl <= 0 or roi is None or num(roi) <= 0:
            continue
        frontier_row = frontier_by_wallet.get(
            wallet,
            frontier_by_identity.get((wallet, fingerprint), {}),
        )
        f2_count = int(frontier_row.get("fresh_own_source_buy_rows_30m") or 0)
        resolved = int(rescore.get("resolved") or 0)
        deficits: list[str] = []
        if resolved < 200:
            deficits.append(f"f1_resolved_gte_200:{resolved}/200")
        if f2_count < f2_minimum:
            deficits.append(f"f2_fresh_own_source_buy_rows:{f2_count}/{f2_minimum}")
        if wallet in cooloffs:
            deficits.append(f"f3_cooloff_until:{cooloffs[wallet]}")
        if not frontier_row:
            deficits.append("f4_or_frontier_evidence_absent")
        else:
            checks = frontier_row.get("checks") or {}
            if checks.get("f4_external_liveness") is not True:
                deficits.append("f4_external_liveness")
            if checks.get("own_evidenced_policy_available") is not True:
                deficits.append("own_evidenced_policy_unavailable")
            if checks.get("not_terminal_park_red_clock_or_measured_loser") is not True:
                deficits.append("terminal_park_red_clock_or_measured_loser")
        backups.append(
            {
                "wallet": wallet,
                "wide_policy_fingerprint": fingerprint,
                "resolved": resolved,
                "post_fee_pnl_usd": round(pnl, 6),
                "roi_pct": round(num(roi), 6),
                "first_half_post_fee_pnl_usd": rescore.get(
                    "first_half_post_fee_pnl_usd"
                ),
                "second_half_post_fee_pnl_usd": rescore.get(
                    "second_half_post_fee_pnl_usd"
                ),
                "fresh_own_source_buy_rows_30m": f2_count,
                "cooloff_until": cooloffs.get(wallet),
                "all_pass": not deficits,
                "deficits": deficits,
            }
        )
    backups.sort(
        key=lambda row: (
            bool(row["cooloff_until"]),
            len(row["deficits"]),
            -int(row["resolved"]),
            -float(row["post_fee_pnl_usd"]),
            row["wallet"],
            row["wide_policy_fingerprint"],
        )
    )
    freeze_identities = {
        (str(wallet).lower(), str(override.get("wide_policy_fingerprint") or ""))
        for wallet, override in (
            fingerprint_evidence.get("freeze_overrides") or {}
        ).items()
        if isinstance(override, dict)
    }
    lawful_freeze_backups = [
        row
        for row in backups
        if (row["wallet"], row["wide_policy_fingerprint"]) in freeze_identities
        and not row["cooloff_until"]
        and num(row.get("post_fee_pnl_usd")) > 0
        and num(row.get("roi_pct")) > 0
        and num(row.get("first_half_post_fee_pnl_usd")) > 0
        and num(row.get("second_half_post_fee_pnl_usd")) > 0
    ]
    lawful_freeze_backups.sort(
        key=lambda row: (
            -int(row["resolved"]),
            -float(row["post_fee_pnl_usd"]),
            row["wallet"],
            row["wide_policy_fingerprint"],
        )
    )

    primary_rescore = venue_gate_summary(primary)
    return {
        "schema_version": 1,
        "kind": "frozen_fingerprint_f2_prewarm_backup_rank_shadow",
        "flow_stage": "PROMOTE/LEARN/OBSERVE/SELF-DEV",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "rule": (
            "prewarm wallet-scoped F2/F4 for the frozen primary; rank positive "
            "non-primary fingerprint cells without changing F1-F4 or granting "
            "live eligibility"
        ),
        "frontier_evidence_scope": "wallet_scoped_f2_f4",
        "primary": {
            "wallet": primary_wallet,
            "wide_policy_fingerprint": primary_fingerprint,
            "resolved": int(primary_rescore.get("resolved") or 0),
            "resolved_target": 200,
            "post_fee_pnl_usd": primary_rescore.get("post_fee_pnl_usd"),
            "roi_pct": primary_rescore.get("roi_pct"),
            "first_half_post_fee_pnl_usd": primary_rescore.get(
                "first_half_post_fee_pnl_usd"
            ),
            "second_half_post_fee_pnl_usd": primary_rescore.get(
                "second_half_post_fee_pnl_usd"
            ),
            "fresh_own_source_buy_rows_30m": primary_f2_count,
            "f2_minimum": f2_minimum,
            "f2_prewarmed": primary_f2_count >= f2_minimum,
            "live_eligible_from_shadow": False,
        },
        "backup_count": len(backups),
        "backup_rank": backups,
        "lawful_freeze_backup_rank": lawful_freeze_backups,
        "cooloffs": cooloffs,
        "source_generated_at": {
            "fingerprint_evidence": fingerprint_evidence.get("generated_at"),
            "deadman_checked_at": deadman.get("checked_at"),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fingerprint-evidence", default=DEFAULT_FINGERPRINT_EVIDENCE)
    parser.add_argument("--deadman", default=DEFAULT_DEADMAN)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--primary-wallet", default=DEFAULT_PRIMARY_WALLET)
    parser.add_argument("--primary-fingerprint", default=DEFAULT_PRIMARY_FINGERPRINT)
    parser.add_argument(
        "--auto-retarget",
        action="store_true",
        help=(
            "Hold the output's healthy primary or retarget an unhealthy primary "
            "to the highest-resolved lawful freeze_override."
        ),
    )
    args = parser.parse_args()
    fingerprint_evidence = _load(Path(args.fingerprint_evidence))
    deadman = _load(Path(args.deadman))
    primary_wallet = args.primary_wallet
    primary_fingerprint = args.primary_fingerprint
    retarget_decision = None
    if args.auto_retarget:
        output_path = Path(args.output)
        previous_shadow = _load(output_path) if output_path.exists() else {}
        (
            primary_wallet,
            primary_fingerprint,
            retarget_decision,
        ) = select_auto_primary(fingerprint_evidence, deadman, previous_shadow)
    payload = build_shadow(
        fingerprint_evidence,
        deadman,
        primary_wallet=primary_wallet,
        primary_fingerprint=primary_fingerprint,
    )
    if retarget_decision is not None:
        payload["auto_retarget"] = retarget_decision
    atomic_write_json(args.output, payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
