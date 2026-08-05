#!/usr/bin/env python3
"""Fail-closed deterministic arbiter for independently promoted BTC-5m cells."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json


def _checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _ts(value: Any) -> float:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return 0.0


def _validate_selector(path: str, *, now: datetime, max_age_s: float) -> tuple[dict[str, Any] | None, str]:
    selector = load_json(path, default={})
    if not isinstance(selector, dict) or selector.get("status") != "PROMOTED_CELL_READY":
        return None, "not_ready"
    if now.timestamp() - _ts(selector.get("generated_at")) > max_age_s:
        return None, "stale"
    selected = selector.get("selected") if isinstance(selector.get("selected"), dict) else {}
    cells = [row for row in selector.get("cells") or [] if isinstance(row, dict)]
    peer = next((row for row in cells if row.get("record_checksum") == selected.get("record_checksum")), None)
    evidence = selected.get("evidence_snapshot") if isinstance(selected.get("evidence_snapshot"), dict) else {}
    checks = evidence.get("checks") if isinstance(evidence.get("checks"), dict) else {}
    record_body = {key: selected.get(key) for key in ("schema_version", "cell_id", "preregistration_checksum", "model_checksum", "signal_offset_s", "execution_mode", "state_path", "evidence_snapshot")}
    if (
        not peer or selected.get("gate_pass") is not True or not checks or not all(value is True for value in checks.values())
        or selected.get("evidence_snapshot_checksum") != _checksum(evidence)
        or selected.get("record_checksum") != _checksum(record_body)
        or str(selected.get("status") or "") == "TERMINAL_PARK_NEGATIVE"
    ):
        return None, "selector_record_invalid"
    state_path = str(selected.get("state_path") or "")
    state = load_json(state_path, default={})
    prereg = state.get("preregistration") if isinstance(state, dict) and isinstance(state.get("preregistration"), dict) else {}
    prereg_body = {key: value for key, value in prereg.items() if key != "checksum"}
    model = str(selected.get("model_checksum") or "")
    if (
        not state_path or not Path(state_path).exists() or state.get("paper_only") is not True or state.get("live_orders_allowed") is not False
        or str(state.get("generation_checksum") or "") != model or str((state.get("frozen_model") or {}).get("checksum") or "") != model
        or not prereg or prereg.get("checksum") != selected.get("preregistration_checksum") or prereg.get("checksum") != _checksum(prereg_body)
        or str(prereg.get("generation_checksum") or "") != model or str(prereg.get("model_checksum") or "") != model
    ):
        return None, "source_state_or_checksum_invalid"
    return {
        **selected,
        "source_selector_path": path,
        "generation_checksum": model,
        "permanent_promotion_resolved_required": int(
            selector.get("permanent_promotion_resolved_required") or 200
        ),
    }, "ready"


def build_arbiter(*, source_paths: list[str], output_path: str, now: datetime | None = None, max_age_s: float = 30.0) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    prior = load_json(output_path, default={})
    candidates, diagnostics = [], {}
    for path in source_paths:
        candidate, status = _validate_selector(path, now=now, max_age_s=max_age_s)
        diagnostics[path] = status
        if candidate:
            candidates.append(candidate)
    candidates.sort(key=lambda row: (-float((row.get("evidence_snapshot") or {}).get("post_fee_pnl_usd") or 0.0), -int((row.get("evidence_snapshot") or {}).get("resolved_fills") or 0), str(row.get("generation_checksum") or ""), str(row.get("cell_id") or "")))
    selected = dict(candidates[0]) if candidates else None
    if selected:
        activation_id = f"promoted-arbiter-{_checksum({'source': selected['source_selector_path'], 'record': selected['record_checksum']})[:20]}"
        if isinstance(prior, dict) and (prior.get("selected") or {}).get("record_checksum") == selected.get("record_checksum"):
            activation_id = str((prior.get("selected") or {}).get("activation_id") or activation_id)
        selected["activation_id"] = activation_id
    result = {
        "schema_version": 1, "kind": "btc5m_promoted_cell_active_selector", "generated_at": now.isoformat(),
        "status": "PROMOTED_CELL_READY" if selected else "NO_GATE_COMPLETE_CELL", "selected": selected,
        "candidate_count": len(candidates), "source_diagnostics": diagnostics,
        "emergency_forward_resolved_required": 10,
        "permanent_promotion_resolved_required": int(
            selected.get("permanent_promotion_resolved_required") or 200
        ) if selected else 200,
        "selection_order": "post_fee_pnl_desc,resolved_desc,generation_checksum_asc,cell_id_asc",
        "single_submitter": "scripts/run_wallet_copy_live_guard.py", "activation_ttl_s": 3600, "activation_refresh_allowed": False,
    }
    atomic_write_json(output_path, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", action="append", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--max-age-s", type=float, default=30.0)
    args = parser.parse_args()
    print(json.dumps(build_arbiter(source_paths=args.source, output_path=args.output, max_age_s=args.max_age_s), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
