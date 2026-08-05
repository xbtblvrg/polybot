import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.run_wide_multiwallet_consensus_slice import (
    CELL_2OFN,
    CELL_WEIGHTED,
    run_once,
)


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        measurement=str(tmp_path / "measurement.json"),
        standings=str(tmp_path / "standings.json"),
        preregistration=str(tmp_path / "prereg.json"),
        state=str(tmp_path / "state.json"),
        two_events=str(tmp_path / "two.events.jsonl"),
        two_intents=str(tmp_path / "two.intents.jsonl"),
        two_terminals=str(tmp_path / "two.terminals.jsonl"),
        weighted_events=str(tmp_path / "weighted.events.jsonl"),
        weighted_intents=str(tmp_path / "weighted.intents.jsonl"),
        weighted_terminals=str(tmp_path / "weighted.terminals.jsonl"),
    )


def _write_inputs(tmp_path: Path, orders: list[dict]) -> None:
    wallets = {"0xa": {}, "0xb": {}, "0xc": {}}
    measurement = {
        "policy_id": "policy",
        "manifest": {"manifest_id": "manifest"},
        "wallets": wallets,
        "orders": orders,
    }
    standings = {
        "manifest_reconciliation": {
            "manifest_identity_exact": True,
            "manifest_id": "manifest",
            "cohort_id": "cohort",
        },
        "standings": [
            {"wallet": "0xa", "queue_rank": 1},
            {"wallet": "0xb", "queue_rank": 4},
            {"wallet": "0xc", "queue_rank": 9},
        ],
    }
    (tmp_path / "measurement.json").write_text(json.dumps(measurement))
    (tmp_path / "standings.json").write_text(json.dumps(standings))


def _order(order_id: str, wallet: str, receipt: float, *, price: float = 0.4) -> dict:
    return {
        "order_id": order_id,
        "wallet": wallet,
        "condition_id": "condition",
        "market_slug": "btc-updown-5m-100",
        "outcome": "Up",
        "fill_price": price,
        "filled_cost_usd": 1.0,
        "recorded_at": datetime.fromtimestamp(receipt, timezone.utc).isoformat(),
        "source_received_at_s": receipt,
        "receipt_to_book_fetch_lag_s": 0.1,
        "f1_f4_terminal": {
            "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
            "F4_executable_book": "PASS",
        },
        "alpha_move_slice": {"market_type": "btc_5m"},
        "transaction_hash": f"tx-{order_id}",
        "log_index": order_id,
        "run_id": "run",
        "cohort_id": "cohort",
        "book_hash": "book",
        "book_timestamp": "1",
        "resolved": False,
    }


def _register_then_write(tmp_path: Path, rows: list[dict]) -> argparse.Namespace:
    args = _args(tmp_path)
    _write_inputs(tmp_path, [])
    run_once(args)
    prereg = json.loads((tmp_path / "prereg.json").read_text())
    registered = datetime.fromisoformat(prereg["registered_at"]).timestamp()
    _write_inputs(tmp_path, [{**row, "recorded_at": datetime.fromtimestamp(registered + 1, timezone.utc).isoformat()} for row in rows])
    return args


def test_emits_both_cells_for_two_distinct_wallets(tmp_path: Path) -> None:
    args = _register_then_write(
        tmp_path,
        [_order("a", "0xa", 100.0), _order("b", "0xb", 101.0)],
    )
    state = run_once(args)
    assert state["cells"][CELL_2OFN]["prospective_intents"] == 1
    assert state["cells"][CELL_WEIGHTED]["prospective_intents"] == 1
    assert state["cells"][CELL_2OFN]["intent_records"][0]["component_wallets"] == ["0xa", "0xb"]
    assert state["paper_only"] is True
    assert state["live_orders_allowed"] is False


def test_one_wallet_cannot_satisfy_consensus_twice(tmp_path: Path) -> None:
    args = _register_then_write(
        tmp_path,
        [_order("a1", "0xa", 100.0), _order("a2", "0xa", 101.0)],
    )
    state = run_once(args)
    assert state["cells"][CELL_2OFN]["prospective_intents"] == 0
    assert state["cells"][CELL_WEIGHTED]["prospective_intents"] == 0


def test_interval_price_and_weight_gates_are_fail_closed(tmp_path: Path) -> None:
    args = _register_then_write(
        tmp_path,
        [
            _order("b", "0xb", 100.0),
            _order("c", "0xc", 101.0),
            _order("late", "0xa", 104.0),
            _order("price", "0xa", 100.5, price=0.7),
        ],
    )
    state = run_once(args)
    assert state["cells"][CELL_2OFN]["prospective_intents"] == 1
    assert state["cells"][CELL_WEIGHTED]["prospective_intents"] == 0


def test_no_backfill_and_one_intent_per_window(tmp_path: Path) -> None:
    args = _args(tmp_path)
    baseline = [_order("old-a", "0xa", 100.0), _order("old-b", "0xb", 101.0)]
    _write_inputs(tmp_path, baseline)
    run_once(args)
    state = run_once(args)
    assert state["cells"][CELL_2OFN]["prospective_intents"] == 0

    prereg = json.loads((tmp_path / "prereg.json").read_text())
    registered = datetime.fromisoformat(prereg["registered_at"]).timestamp()
    fresh = [
        {**_order("new-a", "0xa", 200.0), "recorded_at": datetime.fromtimestamp(registered + 1, timezone.utc).isoformat()},
        {**_order("new-b", "0xb", 201.0), "recorded_at": datetime.fromtimestamp(registered + 2, timezone.utc).isoformat()},
        {**_order("new-c", "0xc", 201.5), "recorded_at": datetime.fromtimestamp(registered + 3, timezone.utc).isoformat()},
    ]
    _write_inputs(tmp_path, baseline + fresh)
    first = run_once(args)
    second = run_once(args)
    assert first["cells"][CELL_2OFN]["prospective_intents"] == 1
    assert second["cells"][CELL_2OFN]["prospective_intents"] == 1


def test_preregistration_tamper_is_rejected(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _write_inputs(tmp_path, [])
    run_once(args)
    prereg = json.loads((tmp_path / "prereg.json").read_text())
    prereg["receipt_interval_s"] = 99
    (tmp_path / "prereg.json").write_text(json.dumps(prereg))
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        run_once(args)
