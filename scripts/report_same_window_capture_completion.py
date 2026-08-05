#!/usr/bin/env python3
"""Stream a boundary-safe completion audit for a same-window capture."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _stream_summary(
    path: Path,
    *,
    boundary_s: float,
    timestamp: Callable[[dict[str, Any]], float | None],
    boundary_timestamp: Callable[[dict[str, Any]], float | None] | None = None,
    include: Callable[[dict[str, Any]], bool],
    wallet: Callable[[dict[str, Any]], list[str]],
    asset: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    rows = 0
    malformed = 0
    after_boundary = 0
    timestamps: list[float] = []
    wallets: set[str] = set()
    assets: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            if not isinstance(row, dict) or not include(row):
                continue
            ts = timestamp(row)
            captured_ts = boundary_timestamp(row) if boundary_timestamp is not None else ts
            if ts is None or captured_ts is None:
                continue
            if captured_ts > boundary_s:
                after_boundary += 1
                continue
            rows += 1
            timestamps.append(ts)
            wallets.update(value.lower() for value in wallet(row) if value)
            if value := asset(row):
                assets.add(value)
    return {
        "rows": rows,
        "rows_after_boundary_excluded": after_boundary,
        "malformed_rows": malformed,
        "min_ts": min(timestamps) if timestamps else None,
        "max_ts": max(timestamps) if timestamps else None,
        "distinct_wallets": len(wallets),
        "wallets": sorted(wallets),
        "distinct_assets": len(assets),
    }


def build_audit(run_dir: Path, *, boundary_s: float) -> dict[str, Any]:
    polygon = _stream_summary(
        run_dir / "polygon_orderfilled.jsonl",
        boundary_s=boundary_s,
        timestamp=lambda row: _number(row.get("event_ts")),
        boundary_timestamp=lambda row: _number(row.get("captured_at_s")),
        include=lambda row: row.get("event") == "polygon_orderfilled_log",
        wallet=lambda row: list(row.get("registry_wallets") or []) + [str(row.get("selected_wallet") or "")],
        asset=lambda row: str((row.get("decoded") or {}).get("asset") or ""),
    )
    clob = _stream_summary(
        run_dir / "clob_books.jsonl",
        boundary_s=boundary_s,
        timestamp=lambda row: _number(row.get("captured_at_s")),
        include=lambda row: row.get("event_type") == "best_bid_ask",
        wallet=lambda row: [],
        asset=lambda row: str(row.get("asset_id") or ""),
    )
    dataapi = _stream_summary(
        run_dir / "dataapi_wallet_events.jsonl",
        boundary_s=boundary_s,
        timestamp=lambda row: _number(row.get("observed_ts")),
        include=lambda row: row.get("event") == "wallet_copy_wallet_event",
        wallet=lambda row: [str(row.get("source_wallet") or "")],
        asset=lambda row: str(row.get("token_id") or ""),
    )
    starts = [row["min_ts"] for row in (polygon, clob, dataapi) if row["min_ts"] is not None]
    ends = [row["max_ts"] for row in (polygon, clob, dataapi) if row["max_ts"] is not None]
    overlap_start = max(starts) if len(starts) == 3 else None
    overlap_end = min(ends) if len(ends) == 3 else None
    overlap_s = max(0.0, overlap_end - overlap_start) if overlap_start is not None and overlap_end is not None else 0.0
    windows = math.floor(overlap_end / 300) - math.floor(overlap_start / 300) + 1 if overlap_s > 0 else 0
    return {
        "schema_version": 1,
        "kind": "same_window_capture_completion_audit",
        "boundary_s": boundary_s,
        "boundary_iso": datetime.fromtimestamp(boundary_s, tz=timezone.utc).isoformat(),
        "timestamp_basis": {
            "polygon": "event_ts (bounded by captured_at_s)",
            "clob": "captured_at_s",
            "dataapi": "observed_ts",
        },
        "sources": {"polygon": polygon, "clob": clob, "dataapi": dataapi},
        "three_way_overlap": {
            "start_ts": overlap_start,
            "end_ts": overlap_end,
            "duration_s": round(overlap_s, 6),
            "btc5m_windows_spanned": windows,
            "passes_7200s": overlap_s >= 7200.0,
            "passes_24_windows": windows >= 24,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--boundary-iso", required=True)
    parser.add_argument("--output", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    boundary = datetime.fromisoformat(args.boundary_iso.replace("Z", "+00:00")).timestamp()
    report = build_audit(Path(args.run_dir), boundary_s=boundary)
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if report["three_way_overlap"]["passes_7200s"] and report["three_way_overlap"]["passes_24_windows"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
