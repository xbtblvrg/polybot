#!/usr/bin/env python3
"""Append and read reconciled WIDE direct-handoff generations."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from scripts.order_flow_incident_archive import append_incident_row, load_incident_rows


DEFAULT_JOURNAL = "data/research/wide_direct_handoff_journal.jsonl"
DEFAULT_TERMINAL_LOG = "data/research/wide_exact_policy_paper_orders.jsonl"


def parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def checksum(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def generation_identity(packet: dict[str, Any]) -> dict[str, Any]:
    manifest = packet.get("manifest") if isinstance(packet.get("manifest"), dict) else {}
    cohort = packet.get("cohort") if isinstance(packet.get("cohort"), dict) else {}
    return {
        "manifest_id": manifest.get("manifest_id"),
        "run_id": cohort.get("run_id") or packet.get("run_id"),
        "cohort_id": cohort.get("cohort_id") or packet.get("cohort_id"),
        "policy_id": packet.get("policy_id"),
    }


def generation_key(identity: dict[str, Any]) -> str:
    return checksum(identity)


def _row_identity(row: dict[str, Any], generation: str) -> str:
    terminal = row.get("f1_f4_terminal") if isinstance(row.get("f1_f4_terminal"), dict) else {}
    transaction_hash = str(row.get("transaction_hash") or "")
    log_index = str(row.get("log_index") or row.get("source_event_id") or "")
    wallet = str(row.get("wallet") or row.get("source_wallet") or "").lower()
    event_identity = (
        {"transaction_hash": transaction_hash, "log_index": log_index, "wallet": wallet}
        if transaction_hash and log_index and wallet
        else {
            "attempt_id": row.get("attempt_id"),
            "order_id": row.get("order_id"),
            "source_event_id": row.get("source_event_id"),
            "wallet": wallet,
        }
    )
    return checksum(
        {
            "generation": generation,
            "event_identity": event_identity,
            "terminal": terminal.get("terminal"),
        }
    )


def row_identity(row: dict[str, Any], generation: str) -> str:
    """Public immutable identity shared by journal consumers."""
    return _row_identity(row, generation)


def envelope_from_packet(packet: dict[str, Any]) -> dict[str, Any] | None:
    reconciliation = packet.get("terminal_reconciliation")
    if not isinstance(reconciliation, dict):
        return None
    input_rows = int(reconciliation.get("input_rows") or 0)
    terminal_rows = int(reconciliation.get("terminal_rows") or 0)
    rows = [dict(row) for row in packet.get("attempt_terminals") or [] if isinstance(row, dict)]
    if not (
        reconciliation.get("direct_event_handoff") is True
        and reconciliation.get("input_equals_terminal") is True
        and input_rows == terminal_rows == len(rows)
    ):
        return None
    identity = generation_identity(packet)
    generation = generation_key(identity)
    normalized = []
    for row in rows:
        row["row_identity"] = _row_identity(row, generation)
        normalized.append(row)
    return {
        "kind": "wide_direct_handoff_generation",
        "captured_at": packet.get("updated_at") or packet.get("generated_at"),
        "source_generation": generation,
        "identity": identity,
        "input_rows": input_rows,
        "terminal_rows": terminal_rows,
        "input_equals_terminal": True,
        "rows": normalized,
        "copyable_orders": [dict(row) for row in packet.get("orders") or [] if isinstance(row, dict)],
        "generation_flow": (
            dict(packet.get("generation_flow"))
            if isinstance(packet.get("generation_flow"), dict)
            else None
        ),
    }


def envelopes_from_incidents(
    incidents: Iterable[dict[str, Any]], terminal_rows: Iterable[dict[str, Any]]
) -> list[dict[str, Any]]:
    snapshots: dict[tuple[str, int], dict[str, Any]] = {}
    for incident in incidents:
        direct = (((incident.get("policy_choke") or {}).get("source_roster_drought") or {}).get("direct_source") or {})
        identity = direct.get("identity") if isinstance(direct.get("identity"), dict) else {}
        run_id = str(identity.get("run_id") or "")
        count = int(direct.get("input_rows") or 0)
        if run_id and count > 0 and direct.get("input_equals_terminal") is True:
            snapshots[(run_id, count)] = direct
    rows_by_run: dict[str, list[dict[str, Any]]] = {}
    for row in terminal_rows:
        if isinstance(row, dict) and row.get("run_id"):
            rows_by_run.setdefault(str(row["run_id"]), []).append(row)
    envelopes = []
    for (run_id, count), direct in sorted(snapshots.items()):
        cutoff = parse_ts(direct.get("updated_at"))
        matching = [
            dict(row) for row in rows_by_run.get(run_id, [])
            if cutoff is not None and (parse_ts(row.get("recorded_at")) or cutoff) <= cutoff
        ]
        rows_by_identity = {
            str(row.get("order_id") or _row_identity(row, run_id)): row
            for row in matching
        }
        rows = list(rows_by_identity.values())
        if len(rows) != count:
            continue
        identity = dict(direct["identity"])
        identity.pop("wallets", None)
        generation = generation_key(identity)
        for row in rows:
            row["row_identity"] = _row_identity(row, generation)
        envelopes.append(
            {
                "kind": "wide_direct_handoff_generation",
                "captured_at": direct.get("updated_at"),
                "source_generation": generation,
                "identity": identity,
                "input_rows": count,
                "terminal_rows": count,
                "input_equals_terminal": True,
                "rows": rows,
            }
        )
    return envelopes


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if path.name == Path(DEFAULT_JOURNAL).name:
        return load_incident_rows(path)
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def append_envelopes(path: Path, envelopes: Iterable[dict[str, Any]]) -> int:
    existing_rows = load_jsonl(path)
    existing_cuts = {
        (str(row.get("source_generation")), int(row.get("input_rows") or 0))
        for row in existing_rows
    }
    known_generations: set[str] = set()
    seen_rows: dict[str, set[str]] = {}
    seen_copyables: dict[str, set[str]] = {}

    def copyable_identity(row: dict[str, Any], generation: str) -> str:
        return _row_identity(row, generation)

    for envelope in existing_rows:
        generation = str(envelope.get("source_generation") or "")
        if not generation:
            continue
        known_generations.add(generation)
        generation_rows = seen_rows.setdefault(generation, set())
        generation_copyables = seen_copyables.setdefault(generation, set())
        for row in envelope.get("rows") or []:
            if isinstance(row, dict):
                generation_rows.add(
                    str(row.get("row_identity") or _row_identity(row, generation))
                )
        for row in envelope.get("copyable_orders") or []:
            if isinstance(row, dict):
                generation_copyables.add(copyable_identity(row, generation))

    additions: list[dict[str, Any]] = []
    for envelope in envelopes:
        generation = str(envelope.get("source_generation") or "")
        cut = (generation, int(envelope.get("input_rows") or 0))
        if not generation or cut in existing_cuts:
            continue
        generation_rows = seen_rows.setdefault(generation, set())
        generation_copyables = seen_copyables.setdefault(generation, set())
        delta_rows: list[dict[str, Any]] = []
        for raw in envelope.get("rows") or []:
            if not isinstance(raw, dict):
                continue
            identity = str(raw.get("row_identity") or _row_identity(raw, generation))
            if identity in generation_rows:
                continue
            generation_rows.add(identity)
            delta_rows.append({**raw, "row_identity": identity})
        delta_copyables: list[dict[str, Any]] = []
        for raw in envelope.get("copyable_orders") or []:
            if not isinstance(raw, dict):
                continue
            identity = copyable_identity(raw, generation)
            if identity in generation_copyables:
                continue
            generation_copyables.add(identity)
            delta_copyables.append(dict(raw))
        was_known = generation in known_generations
        known_generations.add(generation)
        additions.append(
            {
                **envelope,
                "rows": delta_rows,
                "copyable_orders": delta_copyables,
                "delta_encoding": {
                    "schema_version": 1,
                    "cumulative_snapshot": True,
                    "prior_generation_seen": was_known,
                    "delta_rows": len(delta_rows),
                    "delta_copyable_orders": len(delta_copyables),
                    "cumulative_input_rows": int(envelope.get("input_rows") or 0),
                    "cumulative_terminal_rows": int(envelope.get("terminal_rows") or 0),
                },
            }
        )
        existing_cuts.add(cut)
    if not additions:
        return 0
    for row in additions:
        append_incident_row(path, row)
    return len(additions)
