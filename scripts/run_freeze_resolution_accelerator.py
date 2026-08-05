#!/usr/bin/env python3
"""Prioritize unresolved freeze-path paper windows for canonical settlement.

Flow stage: PROMOTE/LEARN/OBSERVE/SELF-DEV.  This resident is paper-only:
it refreshes the shared canonical resolution index while the existing WIDE
supervisor remains the only fingerprint scorer and the live guard remains the
only order submitter.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.venue_executability import venue_gate_summary


DIRECTION_ID = "2026-08-01T09:12:00Z"
DEFAULT_PAPER_STATE = "data/research/wide_exact_policy_paper_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = "data/research/freeze_resolution_accelerator_state.json"
DEFAULT_SUMMARY = "data/research/freeze_resolution_accelerator_refresh_summary.json"
DEFAULT_SIDECAR = "data/research/copy_freeze_near_bar_allpass_dryrun_sidecar_latest.json"
DEFAULT_FRONTIER = "data/research/wide_direct_admissible_frontier_latest.json"
DEFAULT_FINGERPRINT_EVIDENCE = (
    "data/research/wide_policy_fingerprint_evidence_latest.json"
)
# bf337426@4028560f is the supply-space climb primary: 439 resolved,
# +$95.473794 post-fee PnL, 21.748017% ROI, +$38.883958/+56.589836
# chronological halves, and +$63.155821 excluding its top market.
DIRECTION_DIRECT_CLIMB_PRIORITY: tuple[tuple[str, str], ...] = (
    (
        "0xbf337426aa856996b8bb79b238345dd1a0276bf7",
        "4028560ff42ee6da51e715295b2e78eeacceb04d9c538cd2df4eb5f2b98a5c46",
    ),
)
DIRECTION_HOLD_NO_ACTIVE_CLIMB = False
FRESH_FORWARD_CLOCK = {
    "started_at": "2026-07-28T10:05:09Z",
    "closed_at": "2026-07-28T11:35:09Z",
    "authority_direction_ids": [
        "2026-07-28T10:05:09Z",
        DIRECTION_ID,
    ],
    "target_resolved": 100,
    "early_kill_min_resolved": 10,
    "baseline_full_window": {
        "resolved": 0,
        "first_half_post_fee_pnl_usd": 0.0,
        "second_half_post_fee_pnl_usd": 0.0,
        "post_fee_pnl_usd": 0.0,
    },
    "predecessor_kill": {
        "closed_at": "2026-07-28T09:53:23Z",
        "status": "KILLED_FRESH_FORWARD_N10_NONPOSITIVE",
        "sticky_refuse": True,
        "wallet": "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b",
        "wide_policy_fingerprint": (
            "74534a27d83e5120c29d5f09579c88e744e19f1afc23d4a92b34d24450a02ece"
        ),
        "resolved": 10,
        "first_half_post_fee_pnl_usd": -2.727545,
        "second_half_post_fee_pnl_usd": -1.551296,
        "post_fee_pnl_usd": -4.278841,
    },
    "kill_rules": {
        "fresh_from_n_gte_10": "kill if either fresh half or fresh post_fee_pnl_usd <= 0",
        "full_window": "kill immediately if second_half_post_fee_pnl_usd <= 0",
    },
    "source_drought_policy": {
        "direction_id": "2026-07-28T10:05:09Z",
        "cycle_s": 1800,
        "freeze_after_consecutive_cycles": 3,
        "cycle_qualifier": (
            "zero fresh own-source BUY attempts under the exact fingerprint "
            "while the WIDE packet is <=30s old and input_equals_terminal_rows=true"
        ),
        "stale_pipeline_counts_as_drought": False,
        "consequence": "FREEZE_PRESERVE_DIGITS_NO_STICKY_REFUSE",
        "mechanical_rearm_within_s": 86400,
        "rearm_requires": (
            "fresh own-source BUY attempts reappear and full-window "
            "chronological halves remain positive"
        ),
        "past_rearm_or_collapse": (
            "close clock; revert to frontier watch; require fresh two-cut screen"
        ),
        "live_source_no_fresh_resolution_stall_s": 21600,
        "stall_consequence": (
            "package attempts/copyable/resolved taxonomy for explicit Fable ruling; "
            "do not kill"
        ),
    },
    "closing_fresh_forward": {
        "attempted_exact_policy_buys": 0,
        "copyable_exact_policy_buys": 0,
        "resolved_orders": 0,
        "input_equals_terminal": True,
        "state_updated_at": "2026-07-28T11:42:06.929637+00:00",
        "drought_boundaries": [
            "2026-07-28T10:35:09Z",
            "2026-07-28T11:05:09Z",
            "2026-07-28T11:35:09Z",
        ],
    },
    "status": "FROZEN_SOURCE_DROUGHT_3_OF_3",
    "sticky_refuse": False,
}


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _jsonl_count(path: str | Path) -> int:
    try:
        with Path(path).open(encoding="utf-8", errors="replace") as handle:
            return sum(1 for line in handle if line.strip())
    except FileNotFoundError:
        return 0


def _unresolved_windows(path: str | Path) -> list[str]:
    payload = load_json(path, default={})
    rows = payload.get("orders") if isinstance(payload, dict) else []
    seen: set[str] = set()
    windows: list[str] = []
    for row in rows or []:
        if not isinstance(row, dict) or row.get("resolved") is True:
            continue
        slug = str(row.get("market_slug") or "")
        if slug.startswith("btc-updown-5m-") and slug not in seen:
            seen.add(slug)
            windows.append(slug)
    return windows


def _direct_climb_priority(frontier_path: str | Path) -> list[tuple[str, str]]:
    # The exact DIRECTION identity is paper-feedstock authority only. Frontier
    # refusal gates live eligibility, but must not silently remove the very
    # capture needed to measure and close F2. Live admission remains
    # fail-closed in the all-pass sidecar/deadman actuator.
    return list(DIRECTION_DIRECT_CLIMB_PRIORITY)


def _direct_climb_unresolved_windows(
    path: str | Path,
    *,
    direct_climb_priority: list[tuple[str, str]],
) -> list[str]:
    payload = load_json(path, default={})
    rows = payload.get("orders") if isinstance(payload, dict) else []
    priority_index = {
        (wallet, fingerprint): index
        for index, (wallet, fingerprint) in enumerate(direct_climb_priority)
    }
    keyed: dict[str, tuple[int, float]] = {}
    for row in rows or []:
        if not isinstance(row, dict) or row.get("resolved") is True:
            continue
        wallet = str(row.get("wallet") or row.get("source_wallet") or "").lower()
        fingerprint = str(row.get("wide_policy_fingerprint") or "")
        rank = priority_index.get((wallet, fingerprint))
        slug = str(row.get("market_slug") or "")
        if rank is None or not slug.startswith("btc-updown-5m-"):
            continue
        keyed.setdefault(slug, (rank, -float(row.get("recorded_at_s") or 0.0)))
    return [
        slug
        for slug, _ in sorted(
            keyed.items(),
            key=lambda item: (item[1][0], item[1][1], item[0]),
        )
    ]


def _history_backfill_windows(
    *,
    frontier_path: str | Path,
    fingerprint_evidence_path: str | Path,
    direct_climb_priority: list[tuple[str, str]],
) -> tuple[list[str], tuple[str, str] | None]:
    """Return scorer-owned unresolved windows for an exact F1-only green cell."""

    frontier = load_json(frontier_path, default={})
    rows = frontier.get("nearest_frontier") if isinstance(frontier, dict) else []
    frontier_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        identity = (
            str(row.get("wallet") or "").lower(),
            str(row.get("wide_policy_fingerprint") or ""),
        )
        frontier_by_identity[identity] = row

    evidence = load_json(fingerprint_evidence_path, default={})
    cells = evidence.get("cells") if isinstance(evidence, dict) else []
    by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for cell in cells or []:
        if not isinstance(cell, dict):
            continue
        identity_payload = (
            cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
        )
        identity = (
            str(identity_payload.get("wallet") or "").lower(),
            str(cell.get("wide_policy_fingerprint") or ""),
        )
        by_identity[identity] = cell

    for identity in direct_climb_priority:
        frontier_row = frontier_by_identity.get(identity)
        if frontier_row is not None:
            frontier_regime = (
                frontier_row.get("regime_evidence")
                if isinstance(frontier_row.get("regime_evidence"), dict)
                else {}
            )
            if (
                set(frontier_row.get("evidence_deficits") or [])
                != {"f1_measured_positive_regime_cell"}
                or float(frontier_regime.get("pnl_usd") or 0.0) <= 0
                or float(
                    frontier_regime.get("first_half_post_fee_pnl_usd") or 0.0
                )
                <= 0
                or float(
                    frontier_regime.get("second_half_post_fee_pnl_usd") or 0.0
                )
                <= 0
            ):
                continue
        cell = by_identity.get(identity)
        if not cell:
            continue
        full_stream = venue_gate_summary(cell)
        if (
            float(full_stream.get("post_fee_pnl_usd") or 0.0) <= 0
            or float(full_stream.get("first_half_post_fee_pnl_usd") or 0.0)
            <= 0
            or float(full_stream.get("second_half_post_fee_pnl_usd") or 0.0)
            <= 0
            or not set(full_stream.get("f1_deficits") or []).issubset(
                {"resolved_gte_200"}
            )
        ):
            continue
        summary = (
            cell.get("resolution_evidence_summary")
            if isinstance(cell.get("resolution_evidence_summary"), dict)
            else {}
        )
        windows: list[str] = []
        seen: set[str] = set()
        for raw_slug in summary.get("matured_unresolved_windows") or []:
            slug = str(raw_slug or "")
            if slug.startswith("btc-updown-5m-") and slug not in seen:
                seen.add(slug)
                windows.append(slug)
        # Preserve the exact climb identity even when there are no matured
        # unresolved windows yet. This keeps freeze-primary capacity bound to
        # the active direction instead of falling back to a completed legacy
        # sidecar primary.
        return windows, identity
    return [], None


def _offline_resolved_count(
    fingerprint_evidence_path: str | Path,
    identity: tuple[str, str],
) -> int:
    evidence = load_json(fingerprint_evidence_path, default={})
    cells = evidence.get("cells") if isinstance(evidence, dict) else []
    for cell in cells or []:
        if not isinstance(cell, dict):
            continue
        identity_payload = (
            cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
        )
        if (
            str(identity_payload.get("wallet") or "").lower(),
            str(cell.get("wide_policy_fingerprint") or ""),
        ) != identity:
            continue
        full_stream = venue_gate_summary(cell)
        return int(full_stream.get("resolved") or 0)
    return 0


def _refresh_command(
    args: argparse.Namespace,
    *,
    direct_climb_priority_windows: list[str],
) -> list[str]:
    command = [
        sys.executable,
        "scripts/refresh_btc_5m_resolutions_from_gamma.py",
        "--ledger",
        "data/research/wallet_copy_live_execution_state.json",
        "--profit-state",
        args.paper_state,
        "--existing",
        args.resolutions,
        "--output",
        args.resolutions,
        "--summary-output",
        args.summary_output,
        "--merge-existing",
        "--max-windows",
        str(args.max_windows),
        "--max-wall-runtime-s",
        str(args.max_wall_runtime_s),
        "--timeout-s",
        str(args.request_timeout_s),
    ]
    for slug in direct_climb_priority_windows:
        command.extend(["--market-slug", slug])
    return command


def run_once(
    args: argparse.Namespace,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    started = time.monotonic()
    before_count = _jsonl_count(args.resolutions)
    priority_windows = _unresolved_windows(args.paper_state)
    direct_climb_priority = _direct_climb_priority(args.frontier)
    direct_climb_priority_windows = _direct_climb_unresolved_windows(
        args.paper_state,
        direct_climb_priority=direct_climb_priority,
    )
    backfill_windows: list[str] = []
    backfill_identity: tuple[str, str] | None = None
    if direct_climb_priority and not direct_climb_priority_windows:
        backfill_windows, backfill_identity = _history_backfill_windows(
            frontier_path=args.frontier,
            fingerprint_evidence_path=args.fingerprint_evidence,
            direct_climb_priority=direct_climb_priority,
        )
    elif not direct_climb_priority:
        backfill_windows, backfill_identity = _history_backfill_windows(
            frontier_path=args.frontier,
            fingerprint_evidence_path=args.fingerprint_evidence,
            direct_climb_priority=list(DIRECTION_DIRECT_CLIMB_PRIORITY),
        )
        if backfill_identity is not None:
            direct_climb_priority = [backfill_identity]
    if backfill_windows:
        direct_climb_priority_windows = backfill_windows[: args.max_windows]
    proc = runner(
        _refresh_command(
            args,
            direct_climb_priority_windows=direct_climb_priority_windows,
        ),
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=max(30.0, float(args.max_wall_runtime_s) + 30.0),
        check=False,
    )
    after_count = _jsonl_count(args.resolutions)
    sidecar = load_json(args.sidecar, default={})
    primary = sidecar.get("primary") if isinstance(sidecar, dict) else {}
    if not direct_climb_priority and DIRECTION_HOLD_NO_ACTIVE_CLIMB:
        primary = {}
    primary_complete = bool(
        primary
        and (
            int(primary.get("remaining_resolved") or 0) <= 0
            or int(primary.get("resolved") or 0)
            >= int(primary.get("resolved_target") or 200)
        )
    )
    if primary_complete and backfill_identity is not None:
        frontier_resolved = next(
            (
                int(
                    (
                        row.get("regime_evidence")
                        if isinstance(row.get("regime_evidence"), dict)
                        else {}
                    ).get("resolved_signals")
                    or 0
                )
                for row in (
                    load_json(args.frontier, default={}).get(
                        "nearest_frontier", []
                    )
                    or []
                )
                if isinstance(row, dict)
                and str(row.get("wallet") or "").lower()
                == backfill_identity[0]
                and str(row.get("wide_policy_fingerprint") or "")
                == backfill_identity[1]
            ),
            0,
        )
        primary = {
            "wallet": backfill_identity[0],
            "wide_policy_fingerprint": backfill_identity[1],
            "resolved": max(
                frontier_resolved,
                _offline_resolved_count(
                    args.fingerprint_evidence,
                    backfill_identity,
                ),
            ),
            "resolved_target": 200,
        }
        primary["remaining_resolved"] = max(
            0, primary["resolved_target"] - primary["resolved"]
        )
    payload = {
        "schema_version": 1,
        "kind": "freeze_resolution_accelerator",
        "flow_stage": "PROMOTE/LEARN/OBSERVE/SELF-DEV",
        "generated_at": _utc_now(),
        "direction_id": DIRECTION_ID,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_mutation": False,
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
        "status": "PASS" if proc.returncode in (0, 2) else "ERROR",
        "pid": os.getpid(),
        "interval_s": float(args.interval_s),
        "priority_source": args.paper_state,
        "direct_climb_priority": [
            {"wallet": wallet, "wide_policy_fingerprint": fingerprint}
            for wallet, fingerprint in direct_climb_priority
        ],
        "fresh_forward_clock": {
            **FRESH_FORWARD_CLOCK,
            "paper_only": True,
            "promotion_authority": False,
        },
        "direct_climb_priority_source": args.frontier,
        "direct_climb_priority_unresolved_window_count": len(
            direct_climb_priority_windows
        ),
        "direct_climb_priority_window_sample": direct_climb_priority_windows[:10],
        "direct_climb_priority_window_source": (
            "fingerprint_full_stream_history_backfill"
            if backfill_windows
            else "wide_exact_policy_paper_state"
        ),
        "direct_climb_history_backfill_count": len(backfill_windows),
        "direct_climb_history_backfill_identity": (
            {
                "wallet": backfill_identity[0],
                "wide_policy_fingerprint": backfill_identity[1],
            }
            if backfill_identity is not None
            else None
        ),
        "priority_unresolved_window_count": len(priority_windows),
        "priority_window_sample": priority_windows[:10],
        "canonical_resolution_rows_before": before_count,
        "canonical_resolution_rows_after": after_count,
        "canonical_resolution_row_delta": after_count - before_count,
        "refresh": {
            "returncode": proc.returncode,
            "duration_s": round(time.monotonic() - started, 3),
            "stdout_tail": (proc.stdout or "")[-1000:],
            "stderr_tail": (proc.stderr or "")[-1000:],
        },
        "freeze_primary": {
            "wallet": primary.get("wallet"),
            "wide_policy_fingerprint": primary.get("wide_policy_fingerprint"),
            "resolved": primary.get("resolved"),
            "resolved_target": primary.get("resolved_target"),
            "remaining_resolved": primary.get("remaining_resolved"),
        },
        "consumer": {
            "service": "com.polymarket.wide-prospective-supervisor",
            "rule": "existing scorer consumes the atomic canonical resolution index on its next <=30s score cycle",
        },
        "next_action": (
            "continue prioritized canonical settlement refresh; the existing "
            "prewarm/sidecar path invokes deadman DIRECT only at exact ALL_PASS_READY"
        ),
    }
    atomic_write_json(args.output, payload)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-state", default=DEFAULT_PAPER_STATE)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--summary-output", default=DEFAULT_SUMMARY)
    parser.add_argument("--sidecar", default=DEFAULT_SIDECAR)
    parser.add_argument("--frontier", default=DEFAULT_FRONTIER)
    parser.add_argument(
        "--fingerprint-evidence", default=DEFAULT_FINGERPRINT_EVIDENCE
    )
    parser.add_argument("--interval-s", type=float, default=60.0)
    parser.add_argument("--max-windows", type=int, default=250)
    parser.add_argument("--max-wall-runtime-s", type=float, default=45.0)
    parser.add_argument("--request-timeout-s", type=float, default=5.0)
    return parser.parse_args()


def main() -> int:
    os.chdir(ROOT)
    args = parse_args()
    while True:
        payload = run_once(args)
        print(json.dumps(payload, sort_keys=True), flush=True)
        if args.interval_s <= 0:
            return 0 if payload["status"] == "PASS" else 1
        time.sleep(max(1.0, float(args.interval_s)))


if __name__ == "__main__":
    raise SystemExit(main())
