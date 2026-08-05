#!/usr/bin/env python3
"""Stratify the failed event-triggered scheduler paper lane.

Flow stage: PROMOTE/MEASURE. This is paper-only analysis over the existing
R8/22b accumulator. If no stratum survives the pre-registered rule, the
optional retirement executor unloads only the paper-lane LaunchAgent and
archives the state file; it never touches the live guard or CopyIntent path.
"""

from __future__ import annotations

import argparse
import gzip
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.fees import (  # noqa: E402
    POLYMARKET_EMBEDDED_FEE_FORMULA,
    POLYMARKET_EMBEDDED_FEE_RATE,
    POLYMARKET_EMBEDDED_FEE_SOURCE,
    expected_polymarket_buy_fee_usd,
)
from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_STATE = "data/research/copy_event_triggered_cycle_scheduler_paper_lane_latest.json"
DEFAULT_OUTPUT = "data/research/copy_event_triggered_cycle_scheduler_stratification_latest.json"
DEFAULT_RETIREMENT = "data/research/copy_event_triggered_cycle_scheduler_retirement_latest.json"
DEFAULT_ARCHIVE_DIR = "data/research/archive"
LAUNCHD_LABEL = "com.belavarga.polymarket.wallet-copy-event-triggered-scheduler-paper-lane"
LAUNCHD_PLIST = Path.home() / "Library" / "LaunchAgents" / f"{LAUNCHD_LABEL}.plist"
SURVIVAL_MIN_WINDOWS = 50
RULING_ID = "2026-07-18T13:22Z-fable-22b-retire-or-22c-stratification"


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _price_bucket(price: float) -> str:
    if price < 0.25:
        return "00_00_25"
    if price < 0.50:
        return "01_25_50"
    if price < 0.70:
        return "02_50_70"
    return "03_70_100"


def _fee_share_bucket(fee_share_pct: float | None) -> str:
    if fee_share_pct is None:
        return "unknown"
    if fee_share_pct < 2.0:
        return "00_lt_2pct"
    if fee_share_pct < 4.0:
        return "01_2_4pct"
    if fee_share_pct < 6.0:
        return "02_4_6pct"
    return "03_gte_6pct"


def _edge_bucket(roi_pct: float | None) -> str:
    if roi_pct is None:
        return "unknown"
    if roi_pct < -50.0:
        return "00_lt_minus_50pct"
    if roi_pct < 0.0:
        return "01_minus_50_to_0pct"
    if roi_pct < 50.0:
        return "02_0_to_50pct"
    return "03_gte_50pct"


def _pretrade_edge_pct(row: dict[str, Any]) -> float | None:
    candidates: list[Any] = []
    for key in (
        "expected_edge_pct",
        "expected_edge_bps",
        "pre_trade_edge_pct",
        "pretrade_edge_pct",
        "edge_pct",
        "signal_edge_pct",
    ):
        if key in row:
            value = row.get(key)
            if key.endswith("_bps"):
                numeric = _num(value, default=float("nan"))
                candidates.append(None if numeric != numeric else numeric / 100.0)
            else:
                candidates.append(value)
    paper_order = row.get("paper_order") if isinstance(row.get("paper_order"), dict) else {}
    source_intent = paper_order.get("source_intent") if isinstance(paper_order.get("source_intent"), dict) else {}
    metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
    for container in (paper_order, source_intent, metadata):
        for key in (
            "expected_edge_pct",
            "pre_trade_edge_pct",
            "pretrade_edge_pct",
            "edge_pct",
            "signal_edge_pct",
        ):
            if key in container:
                candidates.append(container.get(key))
    for value in candidates:
        numeric = _num(value, default=float("nan"))
        if numeric == numeric:
            return numeric
    return None


def _row_time(row: dict[str, Any]) -> datetime | None:
    for key in ("event_at", "counterfactual_trigger_at_iso", "market_close_iso"):
        parsed = _parse_ts(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _clock_bounds(state: dict[str, Any], rows: list[dict[str, Any]]) -> tuple[datetime | None, datetime | None]:
    start = _parse_ts(state.get("paper_clock_accumulation_started_at") or state.get("clock_start_utc"))
    end = _parse_ts(state.get("generated_at") or state.get("clock_end_utc"))
    row_times = [row_time for row in rows if (row_time := _row_time(row)) is not None]
    if row_times:
        start = min([start, *row_times]) if start is not None else min(row_times)
        end = max([end, *row_times]) if end is not None else max(row_times)
    return start, end


def _half(row: dict[str, Any], *, start: datetime | None, end: datetime | None) -> str:
    row_time = _row_time(row)
    if row_time is None or start is None or end is None or end <= start:
        return "unknown"
    midpoint = start + (end - start) / 2
    return "first" if row_time < midpoint else "second"


def _fee_share_pct(row: dict[str, Any]) -> float | None:
    price = _num(row.get("price"), default=-1.0)
    size_usd = _num(row.get("paper_order_size_usd"), default=0.0)
    if size_usd <= 0.0:
        paper_order = row.get("paper_order") if isinstance(row.get("paper_order"), dict) else {}
        size_usd = _num(paper_order.get("filled_size_usd"), default=0.0)
    if price <= 0.0 or size_usd <= 0.0:
        return None
    shares = size_usd / price
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
    return round(100.0 * fee / size_usd, 6)


def _resolved_rows(state: dict[str, Any]) -> list[dict[str, Any]]:
    accumulator = state.get("paper_clock_accumulator") if isinstance(state.get("paper_clock_accumulator"), dict) else {}
    rows = []
    for row in accumulator.values():
        if not isinstance(row, dict):
            continue
        if row.get("post_fee_would_pnl_status") != "RESOLVED_POST_FEE_MEASURED":
            continue
        rows.append(row)
    return rows


def _empty_group(family: str, key: str) -> dict[str, Any]:
    return {
        "family": family,
        "key": key,
        "rows": 0,
        "resolved_windows": set(),
        "post_fee_pnl_usd": 0.0,
        "halves": {
            "first": {"rows": 0, "resolved_windows": set(), "post_fee_pnl_usd": 0.0},
            "second": {"rows": 0, "resolved_windows": set(), "post_fee_pnl_usd": 0.0},
            "unknown": {"rows": 0, "resolved_windows": set(), "post_fee_pnl_usd": 0.0},
        },
    }


def _add_row(group: dict[str, Any], row: dict[str, Any], *, half: str) -> None:
    pnl = _num(row.get("post_fee_would_pnl_usd"))
    market = str(row.get("market_slug") or row.get("event_id") or "")
    group["rows"] += 1
    group["post_fee_pnl_usd"] += pnl
    if market:
        group["resolved_windows"].add(market)
    half_group = group["halves"][half if half in group["halves"] else "unknown"]
    half_group["rows"] += 1
    half_group["post_fee_pnl_usd"] += pnl
    if market:
        half_group["resolved_windows"].add(market)


def _finalize_group(group: dict[str, Any]) -> dict[str, Any]:
    halves = {}
    for name, half_group in group["halves"].items():
        halves[name] = {
            "rows": half_group["rows"],
            "resolved_windows": len(half_group["resolved_windows"]),
            "post_fee_pnl_usd": round(half_group["post_fee_pnl_usd"], 6),
            "positive": half_group["post_fee_pnl_usd"] > 0.0,
        }
    out = {
        "family": group["family"],
        "key": group["key"],
        "rows": group["rows"],
        "resolved_windows": len(group["resolved_windows"]),
        "post_fee_pnl_usd": round(group["post_fee_pnl_usd"], 6),
        "halves": halves,
    }
    out["survives"] = (
        out["resolved_windows"] >= SURVIVAL_MIN_WINDOWS
        and out["post_fee_pnl_usd"] > 0.0
        and halves["first"]["post_fee_pnl_usd"] > 0.0
        and halves["second"]["post_fee_pnl_usd"] > 0.0
    )
    return out


def build_report(state: dict[str, Any], *, generated_at: str, source_state_path: str = DEFAULT_STATE) -> dict[str, Any]:
    rows = _resolved_rows(state)
    start, end = _clock_bounds(state, rows)
    groups: dict[tuple[str, str], dict[str, Any]] = {}

    def group_for(family: str, key: str) -> dict[str, Any]:
        group_key = (family, key)
        if group_key not in groups:
            groups[group_key] = _empty_group(family, key)
        return groups[group_key]

    for row in rows:
        price = _num(row.get("price"), default=-1.0)
        size_usd = _num(row.get("paper_order_size_usd"), default=0.0)
        price_bucket = _price_bucket(price)
        fee_share_pct = _fee_share_pct(row)
        fee_bucket = _fee_share_bucket(fee_share_pct)
        edge_value = _pretrade_edge_pct(row)
        edge_bucket = "edge_unavailable" if edge_value is None else _edge_bucket(edge_value)
        row_half = _half(row, start=start, end=end)
        stratum_keys = {
            "fee_share_bucket": fee_bucket,
            "entry_price_bucket": price_bucket,
            "edge_bucket": edge_bucket,
            "entry_price_x_edge_bucket": f"{price_bucket}|{edge_bucket}",
            "source_wallet": str(row.get("source_wallet") or "unknown").lower(),
        }
        for family, key in stratum_keys.items():
            _add_row(group_for(family, key), row, half=row_half)

    finalized = [_finalize_group(group) for group in groups.values()]
    finalized.sort(
        key=lambda row: (
            not row["survives"],
            -float(row["post_fee_pnl_usd"]),
            -int(row["resolved_windows"]),
            row["family"],
            row["key"],
        )
    )
    survivors = [row for row in finalized if row["survives"]]
    status = "SURVIVING_STRATUM_FOUND_22C_SPEC_REQUIRED" if survivors else "NO_SURVIVING_STRATUM_RETIRE"
    return {
        "schema_version": 1,
        "kind": "copy_event_triggered_cycle_scheduler_stratification",
        "flow_stage": "PROMOTE/MEASURE",
        "ruling_id": RULING_ID,
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "source_state": {
            "path": source_state_path,
            "generated_at": state.get("generated_at"),
            "status": state.get("status"),
            "clock_start_utc": state.get("clock_start_utc"),
            "clock_end_utc": state.get("clock_end_utc"),
            "paper_clock_accumulation_started_at": state.get("paper_clock_accumulation_started_at"),
        },
        "fee_model": {
            "rate": POLYMARKET_EMBEDDED_FEE_RATE,
            "formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
            "source": POLYMARKET_EMBEDDED_FEE_SOURCE,
            "fee_share_basis": "expected_fee_usd / paper_order_size_usd",
        },
        "half_split": {
            "basis": "observed accumulator span",
            "start": None if start is None else start.isoformat().replace("+00:00", "Z"),
            "end": None if end is None else end.isoformat().replace("+00:00", "Z"),
            "rule": "survivor must be post-fee positive in first and second half",
        },
        "survival_rule": {
            "min_resolved_windows": SURVIVAL_MIN_WINDOWS,
            "post_fee_pnl_gt_zero": True,
            "both_halves_post_fee_positive": True,
        },
        "summary": {
            "status": status,
            "resolved_rows": len(rows),
            "strata": len(finalized),
            "survivors": len(survivors),
            "top_survivor": survivors[0] if survivors else None,
            "top_strata": finalized[:12],
            "next": (
                "draft 22c filtered scheduler spec and ask Fable before any live promotion"
                if survivors
                else "retire scheduler paper lane and unload producer"
            ),
        },
        "survivors": survivors[:50],
        "all_strata": finalized,
    }


def _run_launchctl(args: list[str]) -> dict[str, Any]:
    proc = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=False)
    return {
        "cmd": args,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def execute_retirement(
    *,
    report: dict[str, Any],
    state_path: Path,
    output_path: Path,
    archive_dir: Path,
    generated_at: str,
) -> dict[str, Any]:
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = generated_at.replace("-", "").replace(":", "").replace("Z", "Z")
    archive_path = archive_dir / f"copy_event_triggered_cycle_scheduler_paper_lane_retired_{stamp}.json.gz"
    with state_path.open("rb") as src, gzip.open(archive_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    label_path = f"gui/{subprocess.check_output(['id', '-u'], text=True).strip()}/{LAUNCHD_LABEL}"
    bootout_label = _run_launchctl(["launchctl", "bootout", label_path])
    bootout_plist = None
    if bootout_label["returncode"] != 0 and LAUNCHD_PLIST.exists():
        bootout_plist = _run_launchctl(["launchctl", "bootout", f"gui/{subprocess.check_output(['id', '-u'], text=True).strip()}", str(LAUNCHD_PLIST)])
    print_check = _run_launchctl(["launchctl", "print", label_path])
    payload = {
        "schema_version": 1,
        "kind": "copy_event_triggered_cycle_scheduler_retirement",
        "flow_stage": "PROMOTE/MEASURE/SELF-DEV",
        "ruling_id": RULING_ID,
        "generated_at": generated_at,
        "status": "RETIRED_FAIL_GATE",
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "source_report": str(output_path),
        "archive_path": str(archive_path),
        "launchd_label": LAUNCHD_LABEL,
        "launchd_plist": str(LAUNCHD_PLIST),
        "bootout_label": bootout_label,
        "bootout_plist": bootout_plist,
        "post_bootout_print": print_check,
        "survival_summary": report.get("summary"),
        "retirement_reason": "22b scheduler paper lane failed pre-registered gate and no stratum survived",
    }
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--retirement-output", default=DEFAULT_RETIREMENT)
    parser.add_argument("--archive-dir", default=DEFAULT_ARCHIVE_DIR)
    parser.add_argument("--execute-retire-if-no-survivor", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state_path = Path(args.state)
    output_path = Path(args.output)
    generated_at = _utc_now_iso()
    state = json.loads(state_path.read_text())
    report = build_report(state, generated_at=generated_at, source_state_path=str(state_path))
    atomic_write_json(output_path, report)
    retirement_status = None
    if args.execute_retire_if_no_survivor and not report["survivors"]:
        retirement = execute_retirement(
            report=report,
            state_path=state_path,
            output_path=output_path,
            archive_dir=Path(args.archive_dir),
            generated_at=generated_at,
        )
        atomic_write_json(Path(args.retirement_output), retirement)
        retirement_status = retirement["status"]
    print(
        json.dumps(
            {
                "output": str(output_path),
                "status": report["summary"]["status"],
                "survivors": report["summary"]["survivors"],
                "resolved_rows": report["summary"]["resolved_rows"],
                "retirement_status": retirement_status,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
