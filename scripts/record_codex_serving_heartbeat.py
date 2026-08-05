#!/usr/bin/env python3
"""Record a lightweight Codex serving heartbeat marker.

Flow stage: SELF-DEV. This is evidence-only: it writes a JSONL marker used by
report_runtime_speed_baseline.py to segment long Codex work bursts. It never
touches the live guard or order path.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_OUTPUT = "data/research/codex_serving_heartbeat.jsonl"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--stage", default="SELF-DEV")
    parser.add_argument("--task", default="codex_serving")
    parser.add_argument("--note", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "schema_version": 1,
        "kind": "codex_serving_heartbeat",
        "flow_stage": str(args.stage),
        "generated_at": _utc_now(),
        "task": str(args.task),
        "note": str(args.note),
        "live_orders_allowed": False,
        "paper_only": True,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")
    print(json.dumps({"output": str(path), "generated_at": row["generated_at"], "task": row["task"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
