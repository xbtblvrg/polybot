import json

from src.wallet_copy.promoted_cell import reduce_promoted_cells
from src.wallet_copy.store import atomic_write_json


def _write_jsonl(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_selector_fails_closed_below_ten_resolved(tmp_path):
    state = tmp_path / "state.json"
    prereg = tmp_path / "prereg.json"
    terminals = tmp_path / "terminals.jsonl"
    atomic_write_json(state, {"paper_only": True, "live_orders_allowed": False})
    body = {"cell_id": "cell", "model_checksum": "model", "generation_checksum": "generation"}
    import hashlib

    checksum = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    atomic_write_json(prereg, {**body, "checksum": checksum})
    _write_jsonl(terminals, [])
    matrix = {
        "cells": [
            {
                "cell_id": "cell",
                "execution_mode": "taker",
                "signal_offset_s": 15,
                "model_checksum": "model",
                "generation_checksum": "generation",
                "preregistration_checksum": checksum,
                "preregistration_path": str(prereg),
                "state_path": str(state),
                "terminals_path": str(terminals),
                "paper_only": True,
                "actual_depth_verified": True,
            }
        ]
    }
    resolutions = tmp_path / "resolutions.jsonl"
    resolutions.write_text("", encoding="utf-8")

    result = reduce_promoted_cells(matrix, resolutions_path=str(resolutions))

    assert result["status"] == "NO_GATE_COMPLETE_CELL"
    assert result["selected"] is None
    assert result["cells"][0]["evidence_snapshot"]["checks"]["resolved_gte_10"] is False


def test_selector_ranks_positive_gate_complete_cells_deterministically(monkeypatch, tmp_path):
    from src.wallet_copy import promoted_cell

    monkeypatch.setattr(
        promoted_cell,
        "_resolved_rows",
        lambda cell, state, resolutions: [
            {"market_slug": f"m-{i}", "resolved_at_order": str(i), "pnl_usd": cell["unit_pnl"]}
            for i in range(10)
        ],
    )
    cells = []
    for name, pnl, offset in (("b", 0.2, 15), ("a", 0.2, 15), ("c", 0.1, 30)):
        state = tmp_path / f"{name}.json"
        prereg = tmp_path / f"{name}-prereg.json"
        atomic_write_json(state, {"paper_only": True, "live_orders_allowed": False})
        body = {
            "cell_id": name,
            "model_checksum": f"model-{name}",
            "generation_checksum": f"generation-{name}",
        }
        import hashlib

        checksum = hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        atomic_write_json(prereg, {**body, "checksum": checksum})
        cells.append(
            {
                "cell_id": name,
                "execution_mode": "taker",
                "signal_offset_s": offset,
                "model_checksum": f"model-{name}",
                "generation_checksum": f"generation-{name}",
                "preregistration_checksum": checksum,
                "preregistration_path": str(prereg),
                "state_path": str(state),
                "paper_only": True,
                "actual_depth_verified": True,
                "unit_pnl": pnl,
            }
        )
    resolutions = tmp_path / "resolutions.jsonl"
    resolutions.write_text("", encoding="utf-8")

    result = reduce_promoted_cells({"cells": cells}, resolutions_path=str(resolutions))

    assert result["status"] == "PROMOTED_CELL_READY"
    assert result["selected"]["cell_id"] == "a"
    assert result["selected"]["activation_id"].startswith("promoted-cell-")


def test_negative_cell_terminal_park_cannot_later_reenter(monkeypatch, tmp_path):
    from src.wallet_copy import promoted_cell

    state = tmp_path / "state.json"
    prereg = tmp_path / "prereg.json"
    atomic_write_json(state, {"paper_only": True, "live_orders_allowed": False})
    body = {"cell_id": "parked", "model_checksum": "model", "generation_checksum": "generation"}
    import hashlib

    checksum = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    atomic_write_json(prereg, {**body, "checksum": checksum})
    cell = {
        "cell_id": "parked",
        "execution_mode": "taker",
        "signal_offset_s": 15,
        "model_checksum": "model",
        "generation_checksum": "generation",
        "preregistration_checksum": checksum,
        "preregistration_path": str(prereg),
        "state_path": str(state),
        "paper_only": True,
        "actual_depth_verified": True,
    }
    monkeypatch.setattr(
        promoted_cell,
        "_resolved_rows",
        lambda *args: [{"market_slug": str(i), "pnl_usd": 1.0} for i in range(10)],
    )
    resolutions = tmp_path / "resolutions.jsonl"
    resolutions.write_text("", encoding="utf-8")

    result = reduce_promoted_cells(
        {"cells": [cell]},
        resolutions_path=str(resolutions),
        prior_cells=[{"cell_id": "parked", "status": "TERMINAL_PARK_NEGATIVE"}],
    )

    assert result["selected"] is None
    assert result["cells"][0]["status"] == "TERMINAL_PARK_NEGATIVE"
    assert result["cells"][0]["evidence_snapshot"]["checks"]["not_previously_terminal_parked"] is False


def test_passive_selector_uses_only_checksum_bound_verified_fill_events(monkeypatch, tmp_path):
    from src.wallet_copy import promoted_cell

    monkeypatch.setattr(
        promoted_cell,
        "score_order",
        lambda order, resolutions: {
            "resolved": True,
            "pnl_usd": 0.5,
            "market_slug": order["market_slug"],
        },
    )
    events = tmp_path / "events.jsonl"
    valid = {
        "event": "passive_quote_filled",
        "status": "FILLED",
        "generation_checksum": "generation",
        "cell_id": "passive",
        "passive_fill_model_checksum": "fill-model",
        "intent_id": "intent",
        "market_slug": "btc-updown-5m-300",
        "limit_price": 0.4,
        "fill_price": 0.4,
        "requested_size_usd": 1.0,
        "requested_shares": 2.5,
        "submitted_at": "2026-07-25T07:00:00Z",
        "fill_evidence": {
            "kind": "sequence_continuous_queue_depletion",
            "verified": True,
            "generation_checksum": "generation",
            "cell_id": "passive",
            "intent_id": "intent",
            "passive_fill_model_checksum": "fill-model",
            "cumulative_verified_depletion_shares": 5.0,
            "required_depletion_shares": 5.0,
        },
    }
    crossed_only = {
        **valid,
        "intent_id": "crossed-only",
        "fill_evidence": {
            **valid["fill_evidence"],
            "intent_id": "crossed-only",
            "verified": False,
        },
    }
    _write_jsonl(events, [valid, crossed_only])
    rows = promoted_cell._resolved_rows(
        {
            "cell_id": "passive",
            "execution_mode": "passive",
            "generation_checksum": "generation",
            "passive_fill_model_checksum": "fill-model",
            "fill_events_path": str(events),
        },
        {},
        {},
    )
    assert len(rows) == 1
    assert rows[0]["market_slug"] == "btc-updown-5m-300"


def test_perp_cell_requires_positive_incremental_pnl_vs_matched_terminal(monkeypatch, tmp_path):
    from src.wallet_copy import promoted_cell

    state = tmp_path / "state.json"
    prereg = tmp_path / "prereg.json"
    baseline = tmp_path / "baseline.jsonl"
    atomic_write_json(state, {"paper_only": True, "live_orders_allowed": False})
    body = {"cell_id": "perp", "model_checksum": "generation", "generation_checksum": "generation"}
    import hashlib

    checksum = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    atomic_write_json(prereg, {**body, "checksum": checksum})
    baseline.write_text("", encoding="utf-8")

    def resolved(cell, state_payload, resolutions):
        unit = 0.2 if cell.get("terminals_path") == str(baseline) else 0.1
        return [{"market_slug": f"m-{i}", "pnl_usd": unit} for i in range(10)]

    monkeypatch.setattr(promoted_cell, "_resolved_rows", resolved)
    result = reduce_promoted_cells(
        {"cells": [{
            "cell_id": "perp", "execution_mode": "taker", "signal_offset_s": 15,
            "model_checksum": "generation", "generation_checksum": "generation",
            "preregistration_checksum": checksum, "preregistration_path": str(prereg),
            "state_path": str(state), "paper_only": True, "actual_depth_verified": True,
            "requires_positive_incremental_vs_matched_cross_exchange": True,
            "matched_cross_exchange_terminals_path": str(baseline),
        }]},
        resolutions_path=str(tmp_path / "resolutions.jsonl"),
    )
    checks = result["cells"][0]["evidence_snapshot"]["checks"]
    assert checks["positive_incremental_vs_matched_cross_exchange"] is False
    assert result["selected"] is None


def test_paired_bundle_reducer_counts_cycles_once_and_includes_orphan_loss(monkeypatch, tmp_path):
    from src.wallet_copy import promoted_cell

    events = tmp_path / "paired.jsonl"
    verified = {"verified": True, "kind": "sequence_continuous_queue_depletion"}
    dual = [
        {"status": "FILLED", "market_slug": "m-dual", "outcome": outcome, "fill_price": 0.4, "filled_size_usd": 0.5, "submitted_at": "1", "fill_evidence": verified}
        for outcome in ("Up", "Down")
    ]
    orphan = [
        {"status": "FILLED", "market_slug": "m-orphan", "outcome": "Up", "fill_price": 0.4, "filled_size_usd": 0.5, "submitted_at": "2", "fill_evidence": verified},
        {"status": "CANCELLED", "market_slug": "m-orphan", "outcome": "Down", "submitted_at": "2"},
    ]
    events.write_text("\n".join(json.dumps(row) for row in (
        {"event": "paired_bundle_terminal", "paired_bundle_id": "dual", "bundle_status": "DUAL_LEG_FILLED", "dual_leg_fill_verified": True, "generation_checksum": "g", "cell_id": "c", "legs": dual},
        {"event": "paired_bundle_terminal", "paired_bundle_id": "orphan", "bundle_status": "ORPHAN_RESOLUTION", "dual_leg_fill_verified": False, "generation_checksum": "g", "cell_id": "c", "legs": orphan},
    )) + "\n", encoding="utf-8")
    monkeypatch.setattr(promoted_cell, "score_order", lambda order, resolutions: {"resolved": True, "pnl_usd": 0.2 if order["market_slug"] == "m-dual" else -0.5})
    monkeypatch.setattr(promoted_cell, "expected_polymarket_buy_fee_usd", lambda **kwargs: 0.0)
    rows = promoted_cell._resolved_rows(
        {"execution_mode": "paired_passive", "paired_bundle_required": True, "generation_checksum": "g", "cell_id": "c", "fill_events_path": str(events)},
        {},
        {},
    )
    assert len(rows) == 2
    assert rows[0]["dual_leg_filled"] is True
    assert rows[0]["pnl_usd"] == 0.4
    assert rows[1]["orphan_leg_count"] == 1
    assert rows[1]["pnl_usd"] == -0.5


def test_split_sell_bundle_uses_locked_post_cost_pnl_and_exact_inventory(tmp_path):
    from src.wallet_copy import promoted_cell

    events = tmp_path / "split-sell.jsonl"
    legs = [
        {"status": "FILLED", "market_slug": "m", "outcome": outcome, "submitted_at": "1"}
        for outcome in ("Up", "Down")
    ]
    _write_jsonl(events, [{
        "event": "paired_bundle_terminal", "paired_bundle_id": "split",
        "dual_leg_fill_verified": True, "generation_checksum": "g", "cell_id": "c",
        "legs": legs,
        "split_sell_accounting": {
            "split_collateral_usd": 1.0,
            "realized_post_cost_pnl_usd": 0.031,
            "inventory_conservation": {"disagreement": 0},
        },
    }])
    rows = promoted_cell._resolved_rows(
        {"cell_id": "c", "generation_checksum": "g", "paired_bundle_required": True, "fill_events_path": str(events)},
        {}, {},
    )
    assert rows == [{
        "market_slug": "m", "resolved_at_order": "1", "pnl_usd": 0.031,
        "paired_bundle_id": "split", "dual_leg_filled": True,
        "orphan_leg_count": 0, "split_inventory_conservation_exact": True,
    }]
