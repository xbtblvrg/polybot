import argparse
import json
from pathlib import Path

import pytest

from scripts.run_wide_conversion_recovery_cell import run_once


def _args(tmp_path: Path, cell: str = "token_map") -> argparse.Namespace:
    return argparse.Namespace(
        cell=cell,
        measurement=str(tmp_path / "measurement.json"),
        journal=str(tmp_path / "journal.jsonl"),
        terminal_log=str(tmp_path / "terminals.jsonl"),
        token_metadata=str(tmp_path / "tokens.json"),
        resolutions=str(tmp_path / "resolutions.jsonl"),
        clob_jsonl=str(tmp_path / "books.jsonl"),
        preregistration=str(tmp_path / f"{cell}_prereg.json"),
        state=str(tmp_path / f"{cell}_state.json"),
    )


def _seed(args: argparse.Namespace) -> None:
    Path(args.measurement).write_text(
        json.dumps(
            {
                "policy_id": "exact-policy",
                "manifest": {"manifest_id": "manifest"},
                "cohort": {"run_id": "run", "cohort_id": "cohort"},
                "wallets": {"0x" + "1" * 40: {}},
            }
        ),
        encoding="utf-8",
    )
    Path(args.token_metadata).write_text("{}", encoding="utf-8")


@pytest.mark.parametrize("cell", ["token_map", "alpha_counterfactual"])
def test_cells_preregister_checksum_isolated_and_paper_only(tmp_path: Path, cell: str) -> None:
    args = _args(tmp_path, cell)
    _seed(args)
    state = run_once(args)
    prereg = json.loads(Path(args.preregistration).read_text())
    assert prereg["cell"] == cell
    assert prereg["checksum"] == state["preregistration_checksum"]
    assert prereg["permanent_gate"]["minimum_resolved"] == 200
    assert state["paper_only"] is True
    assert state["live_orders_allowed"] is False


def test_tampered_recovery_preregistration_fails_closed(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _seed(args)
    run_once(args)
    prereg = json.loads(Path(args.preregistration).read_text())
    prereg["permanent_gate"]["minimum_resolved"] = 199
    Path(args.preregistration).write_text(json.dumps(prereg), encoding="utf-8")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        run_once(args)
