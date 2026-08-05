import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts.run_wide_sequential_quorum_slice import run_once


def _wallet(index: int) -> str:
    return f"0x{index:040x}"


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        measurement=str(tmp_path / "measurement.json"),
        direct_journal=str(tmp_path / "direct_journal.jsonl"),
        standings=str(tmp_path / "standings.json"),
        manifest=str(tmp_path / "manifest.json"),
        resolutions=str(tmp_path / "resolutions.jsonl"),
        preregistration=str(tmp_path / "prereg.json"),
        state=str(tmp_path / "state.json"),
        cell_state=str(tmp_path / "cell.json"),
        events=str(tmp_path / "events.jsonl"),
        intents=str(tmp_path / "intents.jsonl"),
        terminals=str(tmp_path / "terminals.jsonl"),
    )


def _write(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def _measurement(orders: list[dict] | None = None) -> dict:
    return {
        "policy_id": "policy",
        "manifest": {"manifest_id": "manifest"},
        "cohort": {"run_id": "run", "cohort_id": "cohort"},
        "wallets": {_wallet(index): {} for index in range(1, 32)},
        "orders": orders or [],
    }


def _order(order_id: str, wallet: str, receipt: float) -> dict:
    return {
        "order_id": order_id,
        "recorded_at": "2099-01-01T00:00:01+00:00",
        "source_received_at_s": receipt,
        "wallet": wallet,
        "condition_id": "condition",
        "market_slug": "btc-updown-5m-4070908800",
        "outcome": "Up",
        "token_id": "yes-token",
        "fill_price": 0.4,
        "receipt_to_book_fetch_lag_s": 0.2,
        "f1_f4_terminal": {
            "F4_executable_book": "PASS",
            "terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL",
        },
        "transaction_hash": f"tx-{order_id}",
        "log_index": order_id,
        "book_hash": f"book-{order_id}",
        "book_timestamp": str(receipt),
        "run_id": "run",
        "cohort_id": "cohort",
    }


def _preregister(tmp_path: Path) -> argparse.Namespace:
    args = _args(tmp_path)
    _write(
        Path(args.manifest),
        {
            "capture_watch_wallets": [
                {"wallet": _wallet(index), "queue_rank": index}
                for index in range(1, 32)
            ]
        },
    )
    _write(Path(args.measurement), _measurement())
    run_once(args)
    return args


def test_preregistration_is_separate_checksum_isolated_and_frozen(tmp_path: Path) -> None:
    args = _preregister(tmp_path)
    prereg = json.loads(Path(args.preregistration).read_text())
    assert prereg["maximum_sequential_gap_s"] == 30.0
    assert prereg["capture_wallet_count"] == 31
    assert prereg["cells"]["sequential_quorum_30s"]["cell_checksum"]
    assert prereg["paper_only"] is True
    assert prereg["live_orders_allowed"] is False


def test_distinct_wallets_strictly_sequential_within_30s_emit_once(tmp_path: Path) -> None:
    args = _preregister(tmp_path)
    _write(
        Path(args.measurement),
        _measurement(
            [
                _order("a", _wallet(1), 4070908800.1),
                _order("b", _wallet(2), 4070908829.9),
                _order("c", _wallet(3), 4070908830.0),
            ]
        ),
    )
    state = run_once(args)
    assert state["cell"]["summary"]["prospective_intents"] == 1
    assert state["cell"]["intent_records"][0]["component_wallets"] == [
        _wallet(1),
        _wallet(2),
    ]
    run_once(args)
    assert len(Path(args.intents).read_text().splitlines()) == 1


def test_same_wallet_or_gap_over_30s_yields_exact_refusal(tmp_path: Path) -> None:
    args = _preregister(tmp_path)
    _write(
        Path(args.measurement),
        _measurement(
            [
                _order("a", _wallet(1), 4070908800.0),
                _order("a2", _wallet(1), 4070908810.0),
                _order("b", _wallet(2), 4070908840.1),
            ]
        ),
    )
    state = run_once(args)
    assert state["cell"]["summary"]["prospective_intents"] == 0
    assert state["cell"]["refusal_taxonomy"]["no_second_wallet_within_30s"] == 1


def test_baseline_rows_never_backfill(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _write(
        Path(args.manifest),
        {
            "capture_watch_wallets": [
                {"wallet": _wallet(index), "queue_rank": index}
                for index in range(1, 32)
            ]
        },
    )
    baseline = [
        _order("a", _wallet(1), 4070908800.1),
        _order("b", _wallet(2), 4070908801.1),
    ]
    _write(Path(args.measurement), _measurement(baseline))
    run_once(args)
    state = run_once(args)
    assert state["cell"]["summary"]["prospective_intents"] == 0


def test_tampered_preregistration_fails_closed(tmp_path: Path) -> None:
    args = _preregister(tmp_path)
    prereg = json.loads(Path(args.preregistration).read_text())
    prereg["maximum_sequential_gap_s"] = 31
    _write(Path(args.preregistration), prereg)
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        run_once(args)


def test_empty_order_generation_exposes_terminal_taxonomy_without_forming_sources(tmp_path: Path) -> None:
    args = _preregister(tmp_path)
    prereg = json.loads(Path(args.preregistration).read_text())
    rows = [
        {
            "row_identity": f"row-{index}",
            "wallet": _wallet(1),
            "recorded_at": "2099-01-01T00:00:02+00:00",
            "order_id": f"refused-{index}",
            "f1_f4_terminal": {
                "terminal": "REFUSED_ALPHA_PROFILE_FILTER" if index < 3 else "REFUSED_METADATA_MISSING"
            },
        }
        for index in range(5)
    ]
    Path(args.direct_journal).write_text(
        json.dumps(
            {
                "identity": {"run_id": prereg["source_run_id"]},
                "input_equals_terminal": True,
                "rows": rows,
            }
        ) + "\n",
        encoding="utf-8",
    )
    state = run_once(args)
    assert state["source_records"] == []
    assert state["cell"]["refusal_taxonomy"]["alpha_profile_filter"] == 3
    assert state["cell"]["refusal_taxonomy"]["metadata_missing"] == 2
    assert state["attrition_funnel"]["attempt"] == 5
