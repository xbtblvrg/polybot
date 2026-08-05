#!/usr/bin/env python3
"""Recover one completed WIDE generation from the append-only terminal ledger."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json


def recover_generation(
    *, run_id: str, manifest: dict[str, Any], ledger_path: Path
) -> dict[str, Any]:
    terminals: dict[str, dict[str, Any]] = {}
    with ledger_path.open(encoding="utf-8") as handle:
        for line in handle:
            if run_id not in line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("run_id") != run_id or not isinstance(row.get("f1_f4_terminal"), dict):
                continue
            attempt_id = str(row.get("attempt_id") or row.get("order_id") or "")
            if attempt_id:
                terminals[attempt_id] = row
    identities = {
        str(row.get("wallet") or "").lower(): {
            "wide_policy_fingerprint": row.get("wide_policy_fingerprint"),
            "move_slice_keys": row.get("move_slice_keys") or [],
        }
        for row in manifest.get("capture_watch_wallets") or []
        if isinstance(row, dict) and row.get("wallet")
    }
    wallet_counts: dict[str, Counter[str]] = {wallet: Counter() for wallet in identities}
    for row in terminals.values():
        wallet = str(row.get("wallet") or "").lower()
        terminal = str((row.get("f1_f4_terminal") or {}).get("terminal") or "")
        wallet_counts.setdefault(wallet, Counter())[terminal] += 1
    wallets = {
        wallet: {
            "attempted_exact_policy_buys": sum(counts.values()),
            "copyable_exact_policy_buys": counts["COPYABLE_EXACT_POLICY_PAPER_FILL"],
            "refusal_counts": {
                key: value
                for key, value in sorted(counts.items())
                if key != "COPYABLE_EXACT_POLICY_PAPER_FILL"
            },
        }
        for wallet, counts in wallet_counts.items()
    }
    rows = list(terminals.values())
    return {
        "schema_version": 1,
        "kind": "wide_exact_policy_generation_measurement_snapshot",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "recovery_source": str(ledger_path),
        "recovery_rule": "last append-only event per immutable attempt_id within exact run_id",
        "manifest": {
            "manifest_id": manifest.get("manifest_id"),
            "manifest_path": manifest.get("score_run_id"),
            "wallet_policy_identities": identities,
        },
        "cohort": {"cohort_id": next((row.get("cohort_id") for row in rows), None)},
        "wallets": wallets,
        "attempt_terminals": rows,
        "terminal_reconciliation": {
            "run_id": run_id,
            "input_rows": len(rows),
            "terminal_rows": len(rows),
            "input_equals_terminal": True,
            "scope": "generation_local_append_only_terminal_recovery",
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--ledger", default="data/research/wide_exact_policy_paper_orders.jsonl")
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    snapshot = recover_generation(
        run_id=args.run_id,
        manifest=load_json(args.manifest, default={}),
        ledger_path=Path(args.ledger),
    )
    if not snapshot["attempt_terminals"]:
        raise RuntimeError("GENERATION_TERMINALS_NOT_FOUND")
    atomic_write_json(args.output, snapshot)
    print(json.dumps(snapshot["terminal_reconciliation"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
