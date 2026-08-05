from argparse import Namespace
from pathlib import Path

from scripts import report_scoped_market_frontier_flip_experiment as report
from src.wallet_copy.store import atomic_write_json


def test_scoped_market_frontier_experiment_keeps_raw_rows_out_of_f1(tmp_path: Path) -> None:
    checksum = "frontier-checksum"
    intake = tmp_path / "intake.json"
    deadman = tmp_path / "deadman.json"
    output = tmp_path / "report.json"
    atomic_write_json(
        intake,
        {
            "source": {
                "market_scoped": True,
                "market_scope_condition_ids_requested": 150,
                "market_scope_condition_ids_completed": 150,
            },
            "window": {
                "lookback_complete": False,
                "oldest_trade_iso_seen": "2026-07-24T00:00:00Z",
            },
            "summary": {
                "trades_scanned": 74444,
                "crypto5m_trades_matched": 74442,
                "wallets_ranked": 5783,
            },
            "ranked_wallets": [{"wallet": "0xabc"}],
        },
    )
    atomic_write_json(
        deadman,
        {
            "policy_choke": {
                "source_roster_drought": {
                    "candidate_evidence": {
                        "frontier_checksum": checksum,
                        "candidate_count": 1,
                        "rows": [
                            {
                                "wallet": "0xabc",
                                "wide_policy_fingerprint": "fingerprint",
                                "source_generation": "generation",
                                "checks": {
                                    "f1_walk_forward_admissible": False,
                                    "f1_concentration_admissible": False,
                                    "both_resolved_halves_positive": False,
                                },
                            }
                        ],
                    }
                }
            }
        },
    )

    payload = report.build_report(
        Namespace(
            intake=str(intake),
            deadman=str(deadman),
            expected_frontier_checksum=checksum,
            output=str(output),
        )
    )

    assert payload["market_scope"]["frontier_wallets_observed"] == 1
    assert payload["market_scope"]["trades_scanned"] == 74444
    assert payload["authoritative_rescore_status"] == "NO_NEW_GATE_AUTHORITY_ELIGIBLE_ROWS"
    assert payload["flip_count"] == 0
    assert payload["acceptance"]["passed"] is False
    assert payload["verdict"] == "COPY_WIDE_MEASUREMENT_CAPPED_MOVE_TO_LIVE_FILL_REALISM"
