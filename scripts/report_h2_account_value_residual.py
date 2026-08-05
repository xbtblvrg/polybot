#!/usr/bin/env python3
"""Classify the H2 account-value residual across scorecard snapshots."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.store import atomic_write_json

DEFAULT_DATA_DIR = "data/research"
DEFAULT_OUTPUT = "data/research/h2_account_value_residual_reconstruction_latest.json"
LEGACY_UNACCOUNTED_RESIDUAL_CLASSES = {
    "account_value_residual_not_explained_by_joined_fill_cost_or_payout",
    "unaccounted_cash_movement_or_balance_sampling_residual",
}
CANONICAL_UNACCOUNTED_RESIDUAL_CLASS = "unaccounted_one_time_cash_movement"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--min-snapshots", type=int, default=3)
    return parser.parse_args()


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _parse_ts(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return num(text, 0.0)


def _scorecard_files(data_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in data_dir.glob("wallet_copy_daily_scorecard*.json")
        if path.is_file()
    )


def _cash_residual(scorecard: dict[str, Any], since: dict[str, Any]) -> dict[str, Any]:
    residual = since.get("cash_diff_reconciliation_residual")
    if isinstance(residual, dict):
        return residual
    residual = scorecard.get("cash_diff_reconciliation_residual")
    return residual if isinstance(residual, dict) else {}


def _canonical_residual_class(value: Any) -> str:
    text = str(value or "")
    if text in LEGACY_UNACCOUNTED_RESIDUAL_CLASSES:
        return CANONICAL_UNACCOUNTED_RESIDUAL_CLASS
    return text


def _has_number(value: Any) -> bool:
    if value is None:
        return False
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def _snapshot_from_scorecard(path: Path, scorecard: dict[str, Any]) -> dict[str, Any]:
    since = scorecard.get("since_topup_truth") if isinstance(scorecard.get("since_topup_truth"), dict) else {}
    chain = scorecard.get("chain_reconciliation") if isinstance(scorecard.get("chain_reconciliation"), dict) else {}
    if not since:
        return {}
    if str(since.get("balance_status") or "").upper() == "UNAVAILABLE":
        return {}
    actual_raw = since.get("actual_account_delta_vs_baseline_usd", since.get("actual_delta_vs_baseline_usd"))
    account_value_raw = since.get("account_value_usd", since.get("actual_value_usd"))
    canonical_raw = since.get("canonical_pnl_usd")
    if not (_has_number(actual_raw) and _has_number(account_value_raw) and _has_number(canonical_raw)):
        return {}
    generated_at = str(scorecard.get("generated_at") or since.get("generated_at") or "")
    account_value = num(account_value_raw, 0.0)
    cash_value = num(since.get("live_cash_balance_usd"), account_value)
    marked_position_value = round(account_value - cash_value, 6)
    canonical_pnl = num(canonical_raw, 0.0)
    actual_account_delta = num(actual_raw, 0.0)
    actual_cash_delta = num(since.get("actual_cash_delta_vs_baseline_usd"), actual_account_delta)
    unresolved_open_cost = num(since.get("unresolved_open_cost_usd"), max(0.0, marked_position_value))
    baseline = num(since.get("baseline_usd"), 0.0)
    expected_account_value = num(since.get("expected_account_value_usd"), baseline + canonical_pnl)
    expected_cash_identity = num(
        since.get("expected_cash_identity_usd"),
        baseline + canonical_pnl - unresolved_open_cost,
    )
    residual = _cash_residual(scorecard, since)
    fill_explained = num(residual.get("fill_cost_payout_explained_usd"), 0.0)
    return {
        "path": str(path),
        "generated_at": generated_at,
        "generated_ts": _parse_ts(generated_at),
        "day_utc": scorecard.get("day") or scorecard.get("day_utc"),
        "baseline_usd": round(baseline, 6),
        "account_value_usd": round(account_value, 6),
        "live_cash_balance_usd": round(cash_value, 6),
        "marked_position_value_usd": round(marked_position_value, 6),
        "unresolved_open_cost_usd": round(unresolved_open_cost, 6),
        "canonical_pnl_usd": round(canonical_pnl, 6),
        "actual_account_delta_usd": round(actual_account_delta, 6),
        "actual_cash_delta_usd": round(actual_cash_delta, 6),
        "expected_account_value_usd": round(expected_account_value, 6),
        "expected_cash_identity_usd": round(expected_cash_identity, 6),
        "account_value_minus_expected_usd": round(account_value - expected_account_value, 6),
        "cash_minus_expected_cash_identity_usd": round(cash_value - expected_cash_identity, 6),
        "chain_delta_vs_expected_usd": num(
            chain.get("delta_vs_expected_usd"),
            round(actual_account_delta - canonical_pnl, 6),
        ),
        "cash_diff_residual_usd": num(residual.get("residual_usd"), 0.0),
        "cash_diff_residual_classification": _canonical_residual_class(residual.get("residual_classification")),
        "cash_diff_fill_cost_payout_explained_usd": round(fill_explained, 6),
        "cash_diff_joined_tx_groups": residual.get("joined_tx_groups"),
        "cash_diff_unjoined_tx_groups": residual.get("unjoined_tx_groups"),
        "cash_diff_ledger_fills_missing_tx": residual.get("ledger_fills_missing_tx"),
        "cash_diff_retrace_unexplained_usd": residual.get("retrace_unexplained_usd"),
        "balance_status": since.get("balance_status"),
        "actual_value_basis": since.get("actual_value_basis"),
    }


def _snapshots(data_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for path in _scorecard_files(data_dir):
        scorecard = _load_json(path)
        if not isinstance(scorecard, dict) or scorecard.get("kind") != "wallet_copy_daily_scorecard":
            continue
        row = _snapshot_from_scorecard(path, scorecard)
        if not row or not row["generated_ts"]:
            continue
        key = (
            row["generated_at"],
            row["actual_account_delta_usd"],
            row["canonical_pnl_usd"],
            row["cash_diff_residual_usd"],
        )
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return sorted(rows, key=lambda row: row["generated_ts"])


def _intervals(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for prev, cur in zip(rows, rows[1:]):
        out.append(
            {
                "start_generated_at": prev["generated_at"],
                "end_generated_at": cur["generated_at"],
                "delta_account_value_usd": round(cur["account_value_usd"] - prev["account_value_usd"], 6),
                "delta_live_cash_usd": round(cur["live_cash_balance_usd"] - prev["live_cash_balance_usd"], 6),
                "delta_marked_position_value_usd": round(
                    cur["marked_position_value_usd"] - prev["marked_position_value_usd"],
                    6,
                ),
                "delta_canonical_pnl_usd": round(cur["canonical_pnl_usd"] - prev["canonical_pnl_usd"], 6),
                "delta_account_value_gap_usd": round(
                    cur["account_value_minus_expected_usd"] - prev["account_value_minus_expected_usd"],
                    6,
                ),
                "delta_cash_gap_usd": round(
                    cur["cash_minus_expected_cash_identity_usd"] - prev["cash_minus_expected_cash_identity_usd"],
                    6,
                ),
            }
        )
    return out


def _external_redeem_summary(data_dir: Path) -> dict[str, Any]:
    payload = _load_json(data_dir / "h2_external_redemption_ingestion_latest.json")
    acceptance = payload.get("acceptance") if isinstance(payload, dict) and isinstance(payload.get("acceptance"), dict) else {}
    return {
        "status": payload.get("status") if isinstance(payload, dict) else None,
        "residual_explained_by_external_redeems_usd": num(
            acceptance.get("residual_explained_by_external_redeems_usd"),
            0.0,
        ),
        "residual_unexplained_after_external_redeems_usd": num(
            acceptance.get("residual_unexplained_after_external_redeems_usd"),
            0.0,
        ),
        "anchor_relabel": acceptance.get("anchor_relabel"),
    }


def _classify(rows: list[dict[str, Any]], external: dict[str, Any], min_snapshots: int) -> dict[str, Any]:
    latest = rows[-1] if rows else {}
    residual = num(latest.get("cash_diff_residual_usd"), 0.0)
    residual_rows = [row for row in rows if abs(num(row.get("cash_diff_residual_usd"), 0.0)) > 0.000001]
    residual_zero_open_rows = [
        row
        for row in residual_rows
        if abs(num(row.get("unresolved_open_cost_usd"), 0.0)) <= 0.000001
        and abs(num(row.get("marked_position_value_usd"), 0.0)) <= 0.000001
    ]
    missing_or_unjoined = [
        row
        for row in residual_rows
        if num(row.get("cash_diff_unjoined_tx_groups"), 0.0) > 0
        or num(row.get("cash_diff_ledger_fills_missing_tx"), 0.0) > 0
    ]
    fill_explained = max((abs(num(row.get("cash_diff_fill_cost_payout_explained_usd"), 0.0)) for row in residual_rows), default=0.0)
    external_explained = abs(num(external.get("residual_explained_by_external_redeems_usd"), 0.0))
    fee_dust_threshold_usd = 0.25
    status = "PASS" if len(rows) >= int(min_snapshots) and rows else "FAIL_INSUFFICIENT_SNAPSHOTS"
    if abs(residual) <= 0.000001:
        residual_class = "fully_explained_no_account_value_residual"
    elif abs(residual) <= fee_dust_threshold_usd:
        residual_class = "fee_or_rounding_dust"
    elif (
        len(residual_zero_open_rows) >= 2
        and not missing_or_unjoined
        and fill_explained <= 0.000001
        and external_explained <= 0.000001
    ):
        residual_class = "unaccounted_one_time_cash_movement"
    elif len(residual_zero_open_rows) < 2:
        residual_class = "open_position_mark_timing_not_ruled_out"
    else:
        residual_class = "account_value_residual_unclassified"
    return {
        "status": status,
        "snapshot_count": len(rows),
        "residual_class": residual_class,
        "latest_cash_diff_residual_usd": round(residual, 6),
        "fee_dust_threshold_usd": fee_dust_threshold_usd,
        "open_position_mark_timing_ruled_out": len(residual_zero_open_rows) >= 2,
        "fee_or_dust_ruled_out": abs(residual) > fee_dust_threshold_usd,
        "fill_cost_payout_ruled_out": fill_explained <= 0.000001 and not missing_or_unjoined,
        "external_redeem_ruled_out": external_explained <= 0.000001,
        "residual_rows_with_zero_open_position": len(residual_zero_open_rows),
        "residual_rows": len(residual_rows),
        "next_action": (
            "audit non-fill USDC/account-value movements and balance sampler timestamps; overlay only, no ledger rewrite"
            if residual_class == "unaccounted_one_time_cash_movement"
            else "collect more account-value snapshots or inspect open-position marks"
        ),
    }


def build_report(data_dir: Path, *, min_snapshots: int = 3) -> dict[str, Any]:
    rows = _snapshots(data_dir)
    external = _external_redeem_summary(data_dir)
    classification = _classify(rows, external, min_snapshots)
    return {
        "kind": "h2_account_value_residual_reconstruction",
        "flow_stage": "LEARN/SELF-DEV",
        "generated_at": utc_now_iso(),
        "inputs": {
            "data_dir": str(data_dir),
            "scorecard_glob": "wallet_copy_daily_scorecard*.json",
            "external_redemption_artifact": str(data_dir / "h2_external_redemption_ingestion_latest.json"),
            "min_snapshots": int(min_snapshots),
        },
        "summary": classification,
        "external_redemptions": external,
        "intervals": _intervals(rows),
        "snapshots": rows,
    }


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    if not data_dir.is_absolute():
        data_dir = ROOT / data_dir
    report = build_report(data_dir, min_snapshots=int(args.min_snapshots))
    output = Path(args.output)
    if not output.is_absolute():
        output = ROOT / output
    atomic_write_json(output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
