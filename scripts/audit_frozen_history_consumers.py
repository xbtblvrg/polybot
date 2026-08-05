#!/usr/bin/env python3
"""Audit research consumers of the frozen rotation-d97 history source."""

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

from src.wallet_copy.store import atomic_write_json  # noqa: E402

FROZEN_SOURCE = "data/research/wallet_copy_history_state.json"
CURRENT_SNAPSHOT = "data/research/wallet_copy_live_guard_hot_history_state.json"
CURRENT_STREAM = "data/research/wallet_copy_live_guard_wallet_events.jsonl"
DEFAULT_OUTPUT = "data/research/frozen_history_consumer_audit_latest.json"

STALE_CONCLUSIONS = {
    "data/research/wallet_copy_followability_leaderboard_latest.json": "followability ranking",
    "data/research/wallet_copy_full_universe_copyability_latest.json": "full-universe copyability ranking",
    "data/research/btc5m_two_sided_prime_study_latest.json": "two-sided/scalp OOS benchmark",
    "data/research/wallet_copy_strategy_decompiler_intake_latest.json": "strategy-decompiler intake ranking",
    "data/research/alpha_decay_report.json": "alpha-decay wallet selection",
}


def _classification(relative_path: str) -> tuple[str, str]:
    name = Path(relative_path).name
    if name == "run_wallet_copy_live_guard.py":
        return (
            "LIVE_PATH_SAFE_HOT_OVERRIDE",
            "frozen reference is producer/warmup fallback only; default live_guard_hot_history=true replaces it before run args are built, so it cannot self-poison live selection",
        )
    if name in {"merge_rtds_wallet_events.py", "merge_dataapi_active_set_events.py", "export_wallet_copy_dataset.py"}:
        return "PRODUCER_OR_EXPORT_TOOL", "maintains or exports historical evidence; it is not promotion-grade by itself"
    if name in {"run_wallet_copy_live_execution.py", "run_wallet_copy_candidate_forward_guard.py"}:
        return "LEGACY_LIVE_OR_FORWARD_PATH_REVIEW", "must not claim current forward evidence while the frozen default remains"
    if relative_path.startswith("src/"):
        return "LIBRARY_DEFAULT_REVIEW", "callers must supply a current source before using output as forward evidence"
    return "RESEARCH_ONLY_STALE_TAINTED", "historical prior only until rebuilt from a current source with an age stamp"


def build_audit(root: Path, *, generated_at: str) -> dict[str, Any]:
    consumers: list[dict[str, Any]] = []
    for base in (root / "scripts", root / "src"):
        for path in sorted(base.rglob("*")):
            if path.suffix not in {".py", ".sh"} or not path.is_file():
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            if FROZEN_SOURCE not in text:
                continue
            relative_path = str(path.relative_to(root))
            classification, impact = _classification(relative_path)
            consumers.append(
                {
                    "path": relative_path,
                    "reference_count": text.count(FROZEN_SOURCE),
                    "classification": classification,
                    "impact": impact,
                }
            )
    scalp_path = root / "scripts/run_btc5m_structural_scalp_paper_lane.py"
    scalp_text = scalp_path.read_text(encoding="utf-8")
    packet_path = root / "scripts/report_btc5m_structural_scalp_promotion_prep.py"
    packet_text = packet_path.read_text(encoding="utf-8")
    promotion_freshness_gate = all(
        token in packet_text
        for token in ("newest_source_event_age_s", "86400.0", "source_fresh", "evidence_pass")
    )
    accumulator = "data/research/btc5m_structural_scalp_forward_source_events.jsonl"
    scalp_repointed = accumulator in scalp_text and CURRENT_SNAPSHOT in scalp_text and FROZEN_SOURCE not in scalp_text
    conclusion_rows = []
    for artifact, conclusion in STALE_CONCLUSIONS.items():
        try:
            payload = json.loads((root / artifact).read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        promotion_grade = payload.get("promotion_grade") is True
        conclusion_rows.append(
            {
                "artifact": artifact,
                "conclusion": conclusion,
                "status": "CURRENT_REBUILT" if promotion_grade else "STALE_TAINTED_REBUILD_REQUIRED",
                "promotion_grade": promotion_grade,
            }
        )
    stale_conclusions = [row for row in conclusion_rows if not row["promotion_grade"]]
    status = "PASS_FAIL_CLOSED" if scalp_repointed and promotion_freshness_gate else "FAIL_OPEN"
    return {
        "schema_version": 1,
        "kind": "frozen_history_consumer_audit",
        "flow_stage": "SELF-DEV/LEARN/PROMOTE",
        "generated_at": generated_at,
        "status": status,
        "frozen_source": FROZEN_SOURCE,
        "frozen_since": "2026-07-15T03:23:00Z",
        "current_successors": {
            "bounded_snapshot": CURRENT_SNAPSHOT,
            "append_only_stream": CURRENT_STREAM,
        },
        "promotion_lane": {
            "path": str(scalp_path.relative_to(root)),
            "repointed_to_current_snapshot": scalp_repointed,
            "forward_accumulator": accumulator,
            "promotion_packet_fails_closed_after_86400s": promotion_freshness_gate,
        },
        "frozen_source_consumers": consumers,
        "live_guard_reference_ruling": next(
            (row for row in consumers if row["path"] == "scripts/run_wallet_copy_live_guard.py"),
            {"status": "REFERENCE_NOT_FOUND"},
        ),
        "standing_conclusions": conclusion_rows,
        "stale_tainted_standing_conclusions": stale_conclusions,
        "rule": "historical artifacts remain usable as historical priors only; no forward or promotion claim may admit without newest_source_event_age_s <= 86400",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    payload = build_audit(ROOT, generated_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"))
    atomic_write_json(args.output, payload)
    print(json.dumps({"status": payload["status"], "consumers": len(payload["frozen_source_consumers"])}))
    return 0 if payload["status"] == "PASS_FAIL_CLOSED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
