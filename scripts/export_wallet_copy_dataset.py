#!/usr/bin/env python3
"""Export wallet-copy ML/research rows as JSONL from history + paper labels."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.features import wallet_event_feature_rows
from src.wallet_copy.models import WalletEvent
from src.wallet_copy.performance import load_resolutions, score_order
from src.wallet_copy.store import load_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--paper-state", default="data/research/wallet_copy_paper_state.json")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--output", default="data/research/wallet_copy_ml_dataset.jsonl")
    parser.add_argument("--include-unresolved", action="store_true")
    return parser.parse_args()


def _load_events(path: str) -> list[WalletEvent]:
    payload = load_json(path, default={})
    rows = payload.get("events") if isinstance(payload, dict) else []
    events: list[WalletEvent] = []
    for row in rows or []:
        if isinstance(row, dict):
            try:
                events.append(WalletEvent.from_dict(row))
            except TypeError:
                continue
    return events


def _label_by_source_fingerprint(paper_state: dict[str, Any], resolutions: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    labels: dict[str, dict[str, Any]] = {}
    for order in paper_state.get("orders") or []:
        if not isinstance(order, dict):
            continue
        source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
        metadata = source_intent.get("metadata") if isinstance(source_intent.get("metadata"), dict) else {}
        source_fingerprint = str(metadata.get("source_fingerprint") or "")
        if not source_fingerprint:
            continue
        labels[source_fingerprint] = score_order(order, resolutions)
    return labels


def main() -> int:
    args = parse_args()
    events = _load_events(args.history_state)
    paper_state = load_json(args.paper_state, default={})
    resolutions = load_resolutions(args.resolutions)
    labels = _label_by_source_fingerprint(paper_state if isinstance(paper_state, dict) else {}, resolutions)
    rows = []
    for features in wallet_event_feature_rows(events):
        label = labels.get(str(features.get("source_fingerprint") or ""))
        if label is None:
            continue
        if not args.include_unresolved and not label.get("resolved"):
            continue
        rows.append(
            {
                **features,
                "label_resolved": bool(label.get("resolved")),
                "label_win": label.get("win"),
                "label_pnl_usd": label.get("pnl_usd"),
                "label_roi_pct": label.get("roi_pct"),
                "label_winner": label.get("winner"),
            }
        )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, default=str))
            handle.write("\n")
    print(json.dumps({"output": str(output), "rows": len(rows)}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
