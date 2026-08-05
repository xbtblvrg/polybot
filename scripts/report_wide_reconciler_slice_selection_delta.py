#!/usr/bin/env python3
"""Report the paper-only WIDE reconciler slice selection delta."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.reconcile_wide_exact_policy_paper import (  # noqa: E402
    _load_fresh_alpha_report,
    _manifest_policy,
    _positive_profile_slices,
)
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


def build_report(
    *, baseline_manifest: dict[str, Any], alpha: dict[str, Any], baseline_path: str, alpha_path: str
) -> dict[str, Any]:
    wallets, before = _manifest_policy(baseline_manifest)
    after = _positive_profile_slices(alpha, wallets)
    rows = []
    for wallet in wallets:
        before_keys = set(before.get(wallet) or set())
        after_keys = set(after.get(wallet) or set())
        rows.append(
            {
                "wallet": wallet,
                "slices_before": sorted(before_keys),
                "slices_after": sorted(after_keys),
                "slice_count_before": len(before_keys),
                "slice_count_after": len(after_keys),
                "adds": sorted(after_keys - before_keys),
                "drops": sorted(before_keys - after_keys),
            }
        )
    return {
        "schema_version": 1,
        "kind": "wide_reconciler_slice_selection_delta",
        "flow_stage": "PROMOTE/LEARN/DEFEND",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "promotion_authority": False,
        "baseline_manifest": baseline_path,
        "source_alpha_report": alpha_path,
        "selection_rule": (
            "mean_edge > 0 and median_edge > 0 and copyable_rate_pct >= 70; "
            "profile-level eligible is not applied a second time"
        ),
        "rows": rows,
        "summary": {
            "wallets": len(rows),
            "wallets_changed": sum(bool(row["adds"] or row["drops"]) for row in rows),
            "slice_count_before": sum(row["slice_count_before"] for row in rows),
            "slice_count_after": sum(row["slice_count_after"] for row in rows),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-manifest", required=True)
    parser.add_argument("--alpha-report", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    baseline = load_json(args.baseline_manifest, default={})
    if not isinstance(baseline, dict) or not baseline:
        raise ValueError(f"BASELINE_MANIFEST_MISSING path={args.baseline_manifest}")
    alpha = _load_fresh_alpha_report(args.alpha_report)
    report = build_report(
        baseline_manifest=baseline,
        alpha=alpha,
        baseline_path=args.baseline_manifest,
        alpha_path=args.alpha_report,
    )
    atomic_write_json(args.output, report)
    print(json.dumps({"output": args.output, **report["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
