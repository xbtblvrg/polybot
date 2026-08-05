import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_factory_funnel.py"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def test_factory_funnel_names_first_broken_conversion(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    _write_json(
        data / "wallet_copy_daily_scorecard_current.json",
        {"generated_at": datetime.now(UTC).isoformat()},
    )
    _write_json(data / "wallet_market_scan_ranked.json", {"summary": {"wallets_ranked": 1000, "active_wallets": 900}})
    _write_json(
        data / "wallet_market_cohort_replay_latest.json",
        {"summary": {"cohort_size": 800, "cohort_shadow_positive": 400, "live_ready_picks": 300}},
    )
    _write_json(
        data / "cohort_alive_admission_packets_latest.json",
        {"summary": {"packet_count": 25, "four_way_admission_ready": 20}},
    )
    _write_json(
        data / "wallet_copy_active_set_auto_degrade_state.json",
        {"latest_admission_wave": {"admitted_count": 10, "runtime_loaded_count": 10}},
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {"active_set_runtime": {"members": [{"source_wallet": f"0x{'1' * 40}"}]}},
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {
            "orders": [
                {
                    "final_status": "FILLED",
                    "market_slug": "btc-updown-5m-1784050200",
                    "lifecycle": [{"status": "INTENT_RECEIVED", "ts": "2026-07-14T17:31:05Z"}],
                }
            ]
        },
    )
    (data / "brainless_ops_scorecard.out").write_text(
        "total orders=1 fills=1 resolved=1 rejects=0 pnl=-1.000000\n"
    )

    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(tmp_path), "--day", "2026-07-14"],
        text=True,
        capture_output=True,
        check=True,
    )
    summary = json.loads(proc.stdout)
    payload = json.loads((data / "factory_funnel_latest.json").read_text())

    assert summary["first_materially_broken_link"] == "live_ready_to_admitted"
    assert payload["enemy_line"]["status"] == "RED"
    assert payload["counts"]["live_ready"] == 300
    assert payload["counts"]["admitted"] == 10
    assert payload["first_materially_broken_link"]["next_action"].startswith("run wave admissions")


def test_factory_funnel_holds_admission_when_tripwires_triggered(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    _write_json(
        data / "wallet_copy_daily_scorecard_current.json",
        {"generated_at": datetime.now(UTC).isoformat()},
    )
    _write_json(data / "state_digest.json", {"defense_tripwires": {"status": "TRIGGERED"}})
    _write_json(data / "wallet_market_scan_ranked.json", {"summary": {"wallets_ranked": 1000, "active_wallets": 900}})
    _write_json(
        data / "wallet_market_cohort_replay_latest.json",
        {"summary": {"cohort_size": 800, "cohort_shadow_positive": 400, "live_ready_picks": 300}},
    )
    _write_json(data / "cohort_alive_admission_packets_latest.json", {"summary": {"packet_count": 25}})
    _write_json(
        data / "wallet_copy_active_set_auto_degrade_state.json",
        {"latest_admission_wave": {"admitted_count": 10, "runtime_loaded_count": 10}},
    )
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {"active_set_runtime": {"members": [{"source_wallet": f"0x{'1' * 40}"}]}},
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {
            "orders": [
                {
                    "final_status": "FILLED",
                    "market_slug": "btc-updown-5m-1784050200",
                    "lifecycle": [{"status": "INTENT_RECEIVED", "ts": "2026-07-14T17:31:05Z"}],
                }
            ]
        },
    )
    (data / "brainless_ops_scorecard.out").write_text(
        "total orders=1 fills=1 resolved=1 rejects=0 pnl=-1.000000\n"
    )

    subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(tmp_path), "--day", "2026-07-14"],
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads((data / "factory_funnel_latest.json").read_text())
    first = payload["first_materially_broken_link"]

    assert first["id"] == "live_ready_to_admitted"
    assert first["next_action"].startswith("HOLD admission widening")
    assert payload["posture_gate"]["admission_widening_allowed"] is False
    assert payload["enemy_line"]["link_id"] == "filled_to_profitable_day"
    assert payload["enemy_line"]["topological_first_link_id"] == "live_ready_to_admitted"
