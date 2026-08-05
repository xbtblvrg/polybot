import datetime as dt
import json
from pathlib import Path

from scripts.arbitrate_btc5m_promoted_cells import _checksum, build_arbiter


NOW = dt.datetime(2026, 7, 25, 12, 0, tzinfo=dt.timezone.utc)


def _selector(tmp_path: Path, name: str, *, pnl: float = 1.0, resolved: int = 10, permanent: int = 200) -> Path:
    model = _checksum({"name": name})
    prereg_body = {"generation_checksum": model, "model_checksum": model, "cell_id": name}
    prereg = {**prereg_body, "checksum": _checksum(prereg_body)}
    state_path = tmp_path / f"{name}-state.json"
    state_path.write_text(json.dumps({"paper_only": True, "live_orders_allowed": False, "generation_checksum": model, "frozen_model": {"checksum": model}, "preregistration": prereg}))
    checks = {"resolved_gte_10": True, "aggregate_post_fee_positive": True, "first_half_positive": True, "second_half_positive": True, "paper_only": True}
    evidence = {"resolved_fills": resolved, "post_fee_pnl_usd": pnl, "checks": checks}
    body = {"schema_version": 1, "cell_id": name, "preregistration_checksum": prereg["checksum"], "model_checksum": model, "signal_offset_s": 30, "execution_mode": "passive", "state_path": str(state_path), "evidence_snapshot": evidence}
    selected = {**body, "evidence_snapshot_checksum": _checksum(evidence), "record_checksum": _checksum(body), "gate_pass": True, "status": "ELIGIBLE", "activation_id": f"source-{name}"}
    path = tmp_path / f"{name}-selector.json"
    path.write_text(json.dumps({"generated_at": NOW.isoformat(), "status": "PROMOTED_CELL_READY", "selected": selected, "cells": [selected], "permanent_promotion_resolved_required": permanent}))
    return path


def test_arbiter_zero_selectors_fails_closed(tmp_path: Path) -> None:
    output = tmp_path / "active.json"
    result = build_arbiter(source_paths=[str(tmp_path / "missing.json")], output_path=str(output), now=NOW)
    assert result["status"] == "NO_GATE_COMPLETE_CELL"
    assert result["selected"] is None


def test_arbiter_one_ready_preserves_exact_guard_fields_and_gate_separation(tmp_path: Path) -> None:
    source = _selector(tmp_path, "one")
    result = build_arbiter(source_paths=[str(source)], output_path=str(tmp_path / "active.json"), now=NOW)
    assert result["status"] == "PROMOTED_CELL_READY"
    assert result["selected"]["cell_id"] == "one"
    assert result["selected"]["execution_mode"] == "passive"
    assert result["selected"]["state_path"].endswith("one-state.json")
    assert result["emergency_forward_resolved_required"] == 10
    assert result["permanent_promotion_resolved_required"] == 200


def test_arbiter_two_ready_rank_is_deterministic_and_idempotent(tmp_path: Path) -> None:
    low = _selector(tmp_path, "low", pnl=2.0, resolved=20)
    high = _selector(tmp_path, "high", pnl=3.0, resolved=10)
    output = tmp_path / "active.json"
    first = build_arbiter(source_paths=[str(low), str(high)], output_path=str(output), now=NOW)
    second = build_arbiter(source_paths=[str(high), str(low)], output_path=str(output), now=NOW)
    assert first["selected"]["cell_id"] == "high"
    assert second["selected"]["activation_id"] == first["selected"]["activation_id"]


def test_perp_fourth_source_carries_permanent_50_gate_to_active_arbiter(tmp_path: Path) -> None:
    legacy = _selector(tmp_path, "legacy", pnl=1.0)
    perp_source = _selector(tmp_path, "perp", pnl=2.0, permanent=50)
    result = build_arbiter(
        source_paths=[str(legacy), str(perp_source)],
        output_path=str(tmp_path / "active.json"),
        now=NOW,
    )
    assert result["selected"]["cell_id"] == "perp"
    assert result["permanent_promotion_resolved_required"] == 50


def test_arbiter_rejects_stale_checksum_path_and_terminal_negative(tmp_path: Path) -> None:
    source = _selector(tmp_path, "bad")
    payload = json.loads(source.read_text())
    payload["generated_at"] = "2026-07-25T11:00:00+00:00"
    source.write_text(json.dumps(payload))
    assert build_arbiter(source_paths=[str(source)], output_path=str(tmp_path / "a.json"), now=NOW)["selected"] is None
    payload["generated_at"] = NOW.isoformat()
    payload["selected"]["record_checksum"] = "tampered"
    source.write_text(json.dumps(payload))
    assert build_arbiter(source_paths=[str(source)], output_path=str(tmp_path / "b.json"), now=NOW)["selected"] is None
    payload["selected"]["record_checksum"] = payload["cells"][0]["record_checksum"]
    payload["selected"]["state_path"] = str(tmp_path / "missing-state.json")
    source.write_text(json.dumps(payload))
    assert build_arbiter(source_paths=[str(source)], output_path=str(tmp_path / "c.json"), now=NOW)["selected"] is None
    payload["selected"] = dict(payload["cells"][0], status="TERMINAL_PARK_NEGATIVE")
    source.write_text(json.dumps(payload))
    assert build_arbiter(source_paths=[str(source)], output_path=str(tmp_path / "d.json"), now=NOW)["selected"] is None
