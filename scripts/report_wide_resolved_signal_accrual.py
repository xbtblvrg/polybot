#!/usr/bin/env python3
"""Measure top-frontier WIDE resolved-signal accrual against the frozen F1 bar."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import parse_ts, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_FRONTIER = "data/research/wide_direct_admissible_frontier_latest.json"
DEFAULT_MANIFEST_GLOB = "data/research/wide_exact_policy_manifest_wide_*.json"
DEFAULT_OUTPUT = "data/research/wide_resolved_signal_accrual_latest.json"
F1_RESOLVED_SIGNAL_BAR = 200
MEASUREMENT_DAYS = 7


def _manifest_set_checksum(paths: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(paths):
        resolved = path.resolve()
        label = (
            str(resolved.relative_to(ROOT))
            if resolved.is_relative_to(ROOT)
            else str(resolved)
        )
        digest.update(label.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def _iso(timestamp: float) -> str:
    return (
        dt.datetime.fromtimestamp(timestamp, tz=dt.UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )


def build_report(
    *,
    frontier: dict[str, Any],
    manifest_paths: list[Path],
    expected_frontier_checksum: str,
    as_of: str,
) -> dict[str, Any]:
    frontier_checksum = str(frontier.get("frontier_checksum") or "")
    if frontier_checksum != expected_frontier_checksum:
        raise ValueError(
            f"frontier checksum changed: {frontier_checksum or 'missing'}"
        )
    candidates = frontier.get("nearest_frontier")
    if not isinstance(candidates, list):
        raise ValueError("nearest_frontier must be a list")
    ranked = sorted(
        (row for row in candidates if isinstance(row, dict)),
        key=lambda row: (
            -int((row.get("regime_evidence") or {}).get("resolved_signals") or 0),
            str(row.get("wallet") or ""),
        ),
    )[:3]
    if len(ranked) != 3:
        raise ValueError("frontier must contain at least three candidate rows")

    manifest_observations: dict[tuple[str, str], list[dict[str, Any]]] = (
        defaultdict(list)
    )
    for path in manifest_paths:
        manifest = load_json(path, default={})
        if not isinstance(manifest, dict):
            continue
        generated_at = str(manifest.get("generated_at") or "")
        generated_ts = parse_ts(generated_at)
        if not generated_at or not generated_ts:
            continue
        for manifest_row in manifest.get("capture_watch_wallets") or []:
            if not isinstance(manifest_row, dict):
                continue
            wallet = str(manifest_row.get("wallet") or "").lower()
            fingerprint = str(
                manifest_row.get("wide_policy_fingerprint") or ""
            )
            freeze = (
                manifest_row.get("slice_freeze")
                if isinstance(manifest_row.get("slice_freeze"), dict)
                else {}
            )
            f1 = freeze.get("f1") if isinstance(freeze.get("f1"), dict) else {}
            if not wallet or not fingerprint:
                continue
            manifest_observations[(wallet, fingerprint)].append(
                {
                    "generated_at": generated_at,
                    "generated_ts": generated_ts,
                    "score_run_id": manifest.get("score_run_id"),
                    "resolved_signals": int(f1.get("resolved") or 0),
                }
            )

    as_of_ts = parse_ts(as_of)
    if as_of_ts <= 0:
        raise ValueError("as_of must be an ISO timestamp")
    window_start_ts = as_of_ts - MEASUREMENT_DAYS * 86400.0
    rows: list[dict[str, Any]] = []
    for rank, candidate in enumerate(ranked, start=1):
        wallet = str(candidate.get("wallet") or "").lower()
        fingerprint = str(candidate.get("wide_policy_fingerprint") or "")
        observations = sorted(
            (
                observation
                for observation in manifest_observations.get(
                    (wallet, fingerprint), []
                )
                if window_start_ts
                < float(observation["generated_ts"])
                <= as_of_ts
            ),
            key=lambda observation: (
                float(observation["generated_ts"]),
                str(observation["score_run_id"]),
            ),
        )
        published = int(
            (candidate.get("regime_evidence") or {}).get("resolved_signals") or 0
        )
        if (
            not observations
            or int(observations[-1]["resolved_signals"]) != published
        ):
            raise ValueError(
                f"{wallet} latest manifest signal count does not match {published}"
            )
        by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for observation in observations:
            day = (
                dt.datetime.fromtimestamp(
                    float(observation["generated_ts"]), tz=dt.UTC
                )
                .date()
                .isoformat()
            )
            by_day[day].append(observation)
        calendar_days = [
            (
                dt.datetime.fromtimestamp(window_start_ts, tz=dt.UTC).date()
                + dt.timedelta(days=offset)
            ).isoformat()
            for offset in range(MEASUREMENT_DAYS + 1)
        ]
        observed_span_days = (
            (
                float(observations[-1]["generated_ts"])
                - float(observations[0]["generated_ts"])
            )
            / 86400.0
            if len(observations) >= 2
            else 0.0
        )
        observed_delta = (
            int(observations[-1]["resolved_signals"])
            - int(observations[0]["resolved_signals"])
            if len(observations) >= 2
            else None
        )
        rate_per_day = (
            round(max(0, int(observed_delta)) / observed_span_days, 6)
            if observed_delta is not None and observed_span_days > 0
            else None
        )
        shortfall = max(0, F1_RESOLVED_SIGNAL_BAR - published)
        projected_crossing_at = None
        if shortfall == 0:
            projected_crossing_at = as_of
        elif rate_per_day is not None and rate_per_day > 0:
            projected_crossing_at = _iso(
                as_of_ts + shortfall / rate_per_day * 86400.0
            )
        rows.append(
            {
                "rank": rank,
                "wallet": wallet,
                "wide_policy_fingerprint": candidate.get(
                    "wide_policy_fingerprint"
                ),
                "resolved_signals": published,
                "f1_resolved_signal_bar": F1_RESOLVED_SIGNAL_BAR,
                "shortfall": shortfall,
                "same_identity_manifest_observations_in_observed_span": len(
                    observations
                ),
                "observed_span_days": round(observed_span_days, 6),
                "observed_resolved_signal_delta": observed_delta,
                "current_rate_per_day": rate_per_day,
                "daily_accrual_utc": [
                    {
                        "date": day,
                        "manifest_observations": len(by_day.get(day) or []),
                        "first_resolved_signals": (
                            int(by_day[day][0]["resolved_signals"])
                            if by_day.get(day)
                            else None
                        ),
                        "last_resolved_signals": (
                            int(by_day[day][-1]["resolved_signals"])
                            if by_day.get(day)
                            else None
                        ),
                        "resolved_signal_delta": (
                            int(by_day[day][-1]["resolved_signals"])
                            - int(by_day[day][0]["resolved_signals"])
                            if len(by_day.get(day) or []) >= 2
                            else None
                        ),
                    }
                    for day in calendar_days
                ],
                "projected_crossing_at": projected_crossing_at,
                "crossing_projection": (
                    "ALREADY_CROSSED"
                    if shortfall == 0
                    else "LINEAR_OBSERVED_SPAN_RATE"
                    if projected_crossing_at
                    else "NONE_INSUFFICIENT_OR_ZERO_SAME_IDENTITY_RATE"
                ),
            }
        )

    return {
        "schema_version": 1,
        "kind": "wide_resolved_signal_accrual",
        "flow_stage": "DISCOVER/PROMOTE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "measurement_only": True,
        "live_orders_allowed": False,
        "admission_authority": False,
        "recommendation": None,
        "source": {
            "frontier_path": DEFAULT_FRONTIER,
            "frontier_generated_at": frontier.get("generated_at"),
            "frontier_checksum": frontier_checksum,
            "frontier_key": frontier.get("frontier_key"),
            "manifest_count": len(manifest_paths),
            "manifest_set_checksum": _manifest_set_checksum(manifest_paths),
        },
        "measurement": {
            "as_of": as_of,
            "maximum_lookback_start": _iso(window_start_ts),
            "maximum_lookback_days": MEASUREMENT_DAYS,
            "effective_window": (
                "bounded by each fingerprint's observed_span_days; the "
                "maximum lookback does not imply seven days of observations"
            ),
            "rate_rule": (
                "same-fingerprint resolved-signal delta divided by elapsed days "
                "between first and last exact-policy manifest observations "
                "within the maximum lookback"
            ),
            "projection_rule": (
                "linear shortfall divided by observed same-identity daily rate"
            ),
            "f1_resolved_signal_bar_unchanged": F1_RESOLVED_SIGNAL_BAR,
        },
        "rows": rows,
        "summary": {
            "top3_current_resolved_signals": [
                row["resolved_signals"] for row in rows
            ],
            "top3_crossing_projection": [
                row["crossing_projection"] for row in rows
            ],
            "none_cross_at_current_rate": all(
                row["projected_crossing_at"] is None for row in rows
            ),
        },
        "decision_rule": (
            "descriptive only; do not lower the 200-signal bar or use a "
            "projection as promotion authority"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frontier", default=DEFAULT_FRONTIER)
    parser.add_argument("--manifest-glob", default=DEFAULT_MANIFEST_GLOB)
    parser.add_argument("--expected-frontier-checksum", required=True)
    parser.add_argument("--as-of", default="")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    frontier = load_json(args.frontier, default={})
    as_of = str(args.as_of or frontier.get("generated_at") or utc_now_iso())
    report = build_report(
        frontier=frontier if isinstance(frontier, dict) else {},
        manifest_paths=sorted(ROOT.glob(args.manifest_glob)),
        expected_frontier_checksum=args.expected_frontier_checksum,
        as_of=as_of,
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
