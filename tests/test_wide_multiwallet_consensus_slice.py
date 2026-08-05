import argparse
import json
import math
from pathlib import Path

import pytest

from scripts.run_wide_multiwallet_consensus_slice import run_once


def _write(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _args(tmp_path: Path) -> argparse.Namespace:
    values = {
        "measurement": str(tmp_path / "measurement.json"),
        "manifest": str(tmp_path / "manifest.json"),
        "resolutions": str(tmp_path / "resolutions.jsonl"),
        "preregistration": str(tmp_path / "prereg.json"),
        "supervisor_state": str(tmp_path / "supervisor.json"),
    }
    for cell in ("two_of_n", "score_weighted"):
        for kind in ("state", "events", "intents", "terminals"):
            values[f"{cell}_{kind}"] = str(tmp_path / f"{cell}_{kind}.jsonl")
        values[f"{cell}_state"] = str(tmp_path / f"{cell}_state.json")
    return argparse.Namespace(**values)


def _wallet(index: int) -> str:
    return f"0x{index:040x}"


def _manifest() -> dict:
    return {
        "manifest_id": "manifest",
        "capture_watch_wallets": [
            {"wallet": _wallet(index), "queue_rank": index} for index in range(1, 32)
        ],
    }


def _measurement(orders: list[dict] | None = None) -> dict:
    return {
        "policy_id": "policy",
        "manifest": {"manifest_id": "manifest"},
        "cohort": {"run_id": "run", "cohort_id": "cohort"},
        "wallets": {_wallet(index): {"decision_rank": index} for index in range(1, 32)},
        "orders": orders or [],
    }


def _order(order_id: str, wallet: str, receipt: float, *, outcome: str = "Up") -> dict:
    return {
        "order_id": order_id,
        "recorded_at": "2099-01-01T00:00:01+00:00",
        "source_received_at_s": receipt,
        "wallet": wallet,
        "run_id": "run",
        "cohort_id": "cohort",
        "condition_id": "condition",
        "market_slug": "btc-updown-5m-4070908800",
        "outcome": outcome,
        "token_id": "yes-token",
        "fill_price": 0.4,
        "filled_cost_usd": 1.0,
        "transaction_hash": f"tx-{order_id}",
        "log_index": order_id,
        "book_hash": f"book-{order_id}",
        "book_timestamp": str(receipt),
        "receipt_to_book_fetch_lag_s": 0.2,
        "f1_f4_terminal": {
            "F4_executable_book": "PASS",
            "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
        },
    }


def _preregister(args: argparse.Namespace) -> None:
    _write(Path(args.manifest), _manifest())
    _write(Path(args.measurement), _measurement())
    run_once(args)


def test_preregistration_freezes_manifest_ranks_weights_and_separate_cells(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _preregister(args)
    prereg = json.loads(Path(args.preregistration).read_text())
    assert prereg["capture_wallet_count"] == 31
    assert prereg["capture_wallets"][1]["queue_rank"] == 2
    assert prereg["capture_wallets"][1]["score_weight"] == round(1 / math.sqrt(2), 12)
    assert prereg["cells"]["two_of_n"]["cell_checksum"] != prereg["cells"]["score_weighted"]["cell_checksum"]
    assert prereg["receipt_interval_rule"] == "floor(source_received_at_s/2)*2"


def test_two_distinct_wallets_emit_once_per_cell_and_window(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _preregister(args)
    measurement = _measurement(
        [
            _order("a", _wallet(1), 4070908800.1),
            _order("b", _wallet(2), 4070908801.9),
        ]
    )
    _write(Path(args.measurement), measurement)
    state = run_once(args)
    assert state["cells"]["two_of_n"]["prospective_intents"] == 1
    assert state["cells"]["score_weighted"]["prospective_intents"] == 1
    assert len(Path(args.two_of_n_intents).read_text().splitlines()) == 1
    assert len(Path(args.score_weighted_intents).read_text().splitlines()) == 1
    run_once(args)
    assert len(Path(args.two_of_n_intents).read_text().splitlines()) == 1


def test_same_wallet_cannot_vote_twice_and_interval_is_exact(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _preregister(args)
    _write(
        Path(args.measurement),
        _measurement(
            [
                _order("a", _wallet(1), 4070908800.1),
                _order("b", _wallet(1), 4070908801.0),
                _order("c", _wallet(2), 4070908802.1),
            ]
        ),
    )
    state = run_once(args)
    assert state["cells"]["two_of_n"]["prospective_intents"] == 0
    cell = json.loads(Path(args.two_of_n_state).read_text())
    assert cell["refusal_taxonomy"]["fewer_than_two_distinct_wallets"] == 2


def test_cells_score_canonical_resolution_and_append_one_terminal_each(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _preregister(args)
    _write(
        Path(args.measurement),
        _measurement(
            [
                _order("a", _wallet(1), 4070908800.1),
                _order("b", _wallet(2), 4070908801.9),
            ]
        ),
    )
    run_once(args)
    Path(args.resolutions).write_text(
        json.dumps(
            {
                "market_slug": "btc-updown-5m-4070908800",
                "condition_id": "condition",
                "direction": "UP",
                "yes_token": "yes-token",
                "no_token": "no-token",
                "source": "test",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    run_once(args)
    two = json.loads(Path(args.two_of_n_state).read_text())
    weighted = json.loads(Path(args.score_weighted_state).read_text())
    assert two["summary"]["resolved_orders"] == 1
    assert weighted["summary"]["resolved_orders"] == 1
    assert two["summary"]["post_fee_pnl_usd"] > 0
    assert len(Path(args.two_of_n_terminals).read_text().splitlines()) == 1
    run_once(args)
    assert len(Path(args.two_of_n_terminals).read_text().splitlines()) == 1


def test_tampered_preregistration_fails_closed(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _preregister(args)
    prereg = json.loads(Path(args.preregistration).read_text())
    prereg["receipt_interval_s"] = 3
    _write(Path(args.preregistration), prereg)
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        run_once(args)


def test_accepts_and_preserves_checksum_valid_legacy_preregistration(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _write(Path(args.manifest), _manifest())
    _write(Path(args.measurement), _measurement())
    wallets = [_wallet(index) for index in range(1, 32)]
    legacy = {
        "schema_version": 1,
        "kind": "wide_multiwallet_consensus_preregistration",
        "registered_at": "2026-07-25T09:24:15+00:00",
        "paper_only": True,
        "live_orders_allowed": False,
        "manifest_id": "manifest",
        "cohort_id": "cohort",
        "policy_id": "policy",
        "scope": "btc_5m",
        "receipt_interval_s": 2.0,
        "entry_price_band": "0.25-0.50",
        "fixed_order_usd": 1.0,
        "max_receipt_lag_s": 5.0,
        "minimum_distinct_wallets": 2,
        "minimum_resolved_per_cell": 50,
        "weight_formula": "1/sqrt(preregistered_queue_rank)",
        "weighted_threshold": 1.0,
        "capture_wallets": wallets,
        "capture_wallets_sha256": "",
        "frozen_queue_ranks": {wallet: index for index, wallet in enumerate(wallets, 1)},
        "baseline_order_ids": [],
        "baseline_order_ids_sha256": "",
    }
    from scripts.run_wide_multiwallet_consensus_slice import _checksum

    legacy["capture_wallets_sha256"] = _checksum(wallets)
    legacy["baseline_order_ids_sha256"] = _checksum([])
    legacy["envelope_checksum"] = _checksum(legacy)
    legacy["cell_checksums"] = {
        "two_of_n": "two",
        "score_weighted": "weighted",
    }
    _write(Path(args.preregistration), legacy)
    state = run_once(args)
    assert state["preregistration_checksum"] == legacy["envelope_checksum"]
    assert state["cell_checksums"]["two_of_n"] == "two"
