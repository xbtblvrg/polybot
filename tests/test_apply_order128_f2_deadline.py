import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from scripts.apply_order128_f2_deadline import (
    DEADLINE_AT,
    FINGERPRINT,
    WALLET,
    build_decision,
)


def test_direct_script_entrypoint_resolves_repo_src_import() -> None:
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "scripts/apply_order128_f2_deadline.py", "--help"],
        cwd=root,
        env={**os.environ, "PYTHONPATH": ""},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def deadman(cut: str, f2: int = 0) -> dict:
    return {
        "checked_at": cut,
        "policy_choke": {
            "source_roster_drought": {
                "candidate_evidence": {
                    "rows": [
                        {
                            "wallet": WALLET,
                            "wide_policy_fingerprint": FINGERPRINT,
                            "source_generation": "gen",
                            "f2_evaluated_copyable": f2,
                            "direct_source": {
                                "attempts": 16,
                                "copyable": f2,
                                "policy_depth_pass": f2,
                                "packet_checksum": "sum",
                            },
                        }
                    ]
                }
            }
        },
    }


def test_deadline_refuses_early_execution() -> None:
    with pytest.raises(ValueError, match="DEADLINE_NOT_DUE"):
        build_decision(
            now=DEADLINE_AT.replace(hour=4),
            incident_cuts=[],
            current_deadman=deadman("2026-08-01T04:00:00Z"),
            packet={},
            manifest={},
            frontier={},
        )


def test_drop_records_series_and_lawful_empty_refill() -> None:
    current = deadman("2026-08-01T04:47:00Z")
    packet = {
        "score_run_id": "wide_cut",
        "deadman_checked_at": current["checked_at"],
    }
    result = build_decision(
        now=datetime(2026, 8, 1, 5, 0, 2, tzinfo=UTC),
        incident_cuts=[
            {
                "deadman_checked_at": "2026-08-01T03:00:00Z",
                "manifest_score_run_id": "UNASSERTABLE",
                "cuts_agree": "UNASSERTABLE",
                "f2_evaluated_copyable": 0,
                "attempts": 20,
                "copyable": 0,
                "policy_depth_pass": 0,
                "source_generation": "old",
                "packet_checksum": "oldsum",
            }
        ],
        current_deadman=current,
        packet=packet,
        manifest={"score_run_id": "wide_cut"},
        frontier={
            "candidate_count": 156,
            "eligible_count": 0,
            "supply_dropouts": [{"wallet": "0xdropped"}],
            "nearest_frontier": [
                {
                    "wallet": WALLET,
                    "source_generation": "gen-a",
                    "evidence_deficits": ["f1_walk_forward_admissible", "f2"],
                    "checks": {
                        "f1_walk_forward_admissible": False,
                        "not_terminal_park_red_clock_or_measured_loser": True,
                    },
                    "eligible": False,
                },
                {
                    "wallet": "0xtied",
                    "source_generation": "gen-b",
                    "evidence_deficits": ["f1_walk_forward_admissible", "f4"],
                    "checks": {
                        "f1_walk_forward_admissible": False,
                        "not_terminal_park_red_clock_or_measured_loser": True,
                    },
                    "eligible": False,
                },
                {
                    "wallet": "0x82c8",
                    "source_generation": "gen-b",
                    "evidence_deficits": ["park"],
                    "checks": {
                        "f1_walk_forward_admissible": True,
                        "not_terminal_park_red_clock_or_measured_loser": False,
                    },
                    "eligible": False,
                },
            ],
        },
    )

    assert result["focus_action"] == "DROP"
    assert result["reason"] == "NO_QUALIFYING_CUT_MEASURED_ZERO_AT_EVERY_CUT"
    assert result["basis"] == "MEASURED_ZERO_AT_EVERY_CUT"
    assert result["standing"] == "0/2"
    assert result["executed_late_s"] == 2.0
    assert result["cuts_observed_in_window"] == 2
    assert result["per_cut_series"][-1]["cuts_agree"] is True
    assert result["per_cut_series"][-1]["disqualified_on"] == "f2_measured_zero"
    assert result["recorded_as_measured_negative"] is False
    assert result["refill"] == {
        "status": "NO_ADMISSIBLE_SUCCESSOR",
        "candidate_count": 156,
        "eligible_count": 0,
        "tie_at_fewest_deficits": ["0xtied"],
        "rejected_on": ["f1_walk_forward_admissible", "f4"],
        "rejected_on_by_wallet": {
            "0xtied": ["f1_walk_forward_admissible", "f4"]
        },
    }
    assert result["retained_coupling_cuts"] == 0
    assert result["retained_coupling_invariant"] == (
        "INCIDENT_REPLAY_DOES_NOT_RETAIN_MANIFEST_COUPLING"
    )
    assert result["focus_was_never_lawful"] is True
    assert result["frontier_generation_split"] == {
        "row_counts": {"gen-a": 1, "gen-b": 2},
        "supply_dropouts": [{"wallet": "0xdropped"}],
    }


def test_two_distinct_exact_positive_cuts_hold_focus() -> None:
    current = deadman("2026-08-01T04:47:00Z", f2=1)
    result = build_decision(
        now=DEADLINE_AT,
        incident_cuts=[
            {
                "deadman_checked_at": "2026-08-01T04:30:00Z",
                "manifest_score_run_id": "wide_prior",
                "cuts_agree": True,
                "f2_evaluated_copyable": 1,
            }
        ],
        current_deadman=current,
        packet={"score_run_id": "wide_current", "deadman_checked_at": current["checked_at"]},
        manifest={"score_run_id": "wide_current"},
        frontier={},
    )

    assert result["focus_action"] == "HOLD"
    assert result["reason"] == "QUALIFYING_CUTS_MET"
    assert result["standing"] == "2/2"


def multi_generation_deadman(cut: str, f2_by_generation: dict[str, int]) -> dict:
    payload = deadman(cut)
    rows = payload["policy_choke"]["source_roster_drought"]["candidate_evidence"]["rows"]
    template = rows[0]
    payload["policy_choke"]["source_roster_drought"]["candidate_evidence"]["rows"] = [
        {**template, "source_generation": generation, "f2_evaluated_copyable": f2}
        for generation, f2 in f2_by_generation.items()
    ]
    return payload


def test_second_generation_supplies_the_graded_quantity() -> None:
    current = multi_generation_deadman("2026-08-01T04:47:00Z", {"gen-a": 0, "gen-b": 1})
    result = build_decision(
        now=DEADLINE_AT,
        incident_cuts=[
            {
                "deadman_checked_at": "2026-08-01T04:30:00Z",
                "manifest_score_run_id": "wide_prior",
                "cuts_agree": True,
                "f2_evaluated_copyable": 1,
            }
        ],
        current_deadman=current,
        packet={"score_run_id": "wide_current", "deadman_checked_at": current["checked_at"]},
        manifest={"score_run_id": "wide_current"},
        frontier={},
    )

    assert result["per_cut_series"][-1]["f2_evaluated_copyable"] == 1
    assert result["per_cut_series"][-1]["f2_by_generation"] == {"gen-a": 0, "gen-b": 1}
    assert result["per_cut_series"][-1]["generations_observed"] == 2
    assert result["focus_action"] == "HOLD"


def test_unretained_coupling_is_not_reported_as_measured_zero() -> None:
    current = deadman("2026-08-01T04:47:00Z", f2=3)
    result = build_decision(
        now=DEADLINE_AT,
        incident_cuts=[],
        current_deadman=current,
        packet={"score_run_id": "wide_current", "deadman_checked_at": "2026-08-01T04:01:00Z"},
        manifest={"score_run_id": "wide_current"},
        frontier={},
    )

    assert result["focus_action"] == "DROP"
    assert result["basis"] == "NONZERO_BUT_COUPLING_UNRETAINED"
    assert result["coupling_unassertable_cuts"] == 1
    assert result["max_assertable_coupled_cuts"] == 1
    assert result["gate_satisfiable_in_single_execution"] is False
    assert result["recorded_as_measured_negative"] is False
    assert result["per_cut_series"][-1]["disqualified_on"] == "coupling_unassertable"


def test_no_cut_in_window_is_its_own_absence_class() -> None:
    result = build_decision(
        now=DEADLINE_AT,
        incident_cuts=[],
        current_deadman=deadman("2026-08-01T05:30:00Z"),
        packet={},
        manifest={},
        frontier={},
    )

    assert result["basis"] == "NO_CUT_IN_WINDOW"
    assert result["reason"] == "NO_QUALIFYING_CUT_NO_CUT_IN_WINDOW"
    assert result["supply_gap_s"] is None
    assert result["max_assertable_coupled_cuts"] == 0


def test_absent_frontier_row_leaves_lawfulness_unassertable() -> None:
    result = build_decision(
        now=DEADLINE_AT,
        incident_cuts=[],
        current_deadman=deadman("2026-08-01T04:47:00Z"),
        packet={},
        manifest={},
        frontier={"nearest_frontier": [{"wallet": "0xother", "checks": {}}]},
    )

    assert result["focus_was_never_lawful"] == "UNASSERTABLE"


def test_multiple_admissible_successors_refuse_an_invented_tiebreak() -> None:
    admissible = {
        "checks": {
            "f1_walk_forward_admissible": True,
            "not_terminal_park_red_clock_or_measured_loser": True,
        },
        "eligible": True,
        "evidence_deficits": [],
    }
    result = build_decision(
        now=DEADLINE_AT,
        incident_cuts=[],
        current_deadman=deadman("2026-08-01T04:47:00Z"),
        packet={},
        manifest={},
        frontier={
            "nearest_frontier": [
                {**admissible, "wallet": "0xaaa"},
                {**admissible, "wallet": "0xbbb"},
            ]
        },
    )

    assert result["refill"]["status"] == "AMBIGUOUS_ADMISSIBLE_SUCCESSORS_NO_TIEBREAK_INVENTED"
    assert result["refill"]["wallets"] == ["0xaaa", "0xbbb"]
    assert "wallet" not in result["refill"]
