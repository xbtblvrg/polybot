import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_wallet_market_mining_cadence.py"


def test_mining_cadence_dry_run_registers_all_due_steps(tmp_path: Path) -> None:
    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--root",
            str(tmp_path),
            "--force",
            "--skip-execution",
        ],
        text=True,
        capture_output=True,
        check=True,
    )
    summary = json.loads(proc.stdout)
    state = json.loads((tmp_path / "data/research/wallet_market_mining_cadence_state.json").read_text())

    assert summary["status"] == "DRY_RUN_PLANNED"
    assert state["paper_only"] is True
    assert state["live_orders_allowed"] is False
    assert state["live_path_mutated"] is False
    assert state["due_steps"] == [
        "intake",
        "replay",
        "external_liveness",
        "source_active_liveness",
        "admission_packets",
    ]
    assert state["null_cycle"]["status"] == "DRY_RUN_NO_MINING_EXECUTED"
    assert {row["id"] for row in state["commitment_rows"]} == {
        "continuous_mining_intake_loop",
        "continuous_mining_replay_loop",
        "continuous_mining_liveness_loop",
        "continuous_mining_null_cycle_rule",
    }
    source_command = state["steps"]["source_active_liveness"]["command"]
    assert source_command[source_command.index("--latest-name") + 1] == (
        "wallet_market_mining_source_active_liveness_batch_latest.json"
    )
    assert source_command[source_command.index("--ordering-latest-name") + 1] == (
        "wallet_market_mining_source_active_liveness_replay_ordering_latest.json"
    )
    admission_command = state["steps"]["admission_packets"]["command"]
    assert admission_command[admission_command.index("--source-active-cohort") + 1] == (
        "data/research/wallet_market_mining_source_active_liveness_cohort_latest.json"
    )
    assert admission_command[admission_command.index("--output") + 1] == (
        "data/research/wallet_market_mining_cohort_alive_admission_packets_latest.json"
    )
    assert state["artifacts"]["source_active_cohort"].startswith(
        "data/research/wallet_market_mining_"
    )
    assert state["artifacts"]["admission_packets"].startswith(
        "data/research/wallet_market_mining_"
    )


def test_mining_cadence_resource_pause_writes_non_live_state(tmp_path: Path) -> None:
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--root",
            str(tmp_path),
            "--resource-paused",
        ],
        text=True,
        capture_output=True,
        check=True,
    )
    state = json.loads((tmp_path / "data/research/wallet_market_mining_cadence_state.json").read_text())

    assert state["status"] == "RESOURCE_PAUSED"
    assert state["live_orders_allowed"] is False
    assert state["null_cycle"]["status"] == "NOT_A_MINING_CYCLE_RESOURCE_PAUSED"
