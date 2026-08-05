from scripts.report_order149_rotation_qualification import build_report


WALLET_4C = "0x4c9497941333332d29f1c235dd23200f3623ffad"
WALLET_56 = "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b"


def test_order149_stops_on_exact_missing_own_policy_inputs() -> None:
    replay = {
        "status": "INSUFFICIENT_CAPTURE_FIELDS",
        "raw_join_coverage": 1.0,
        "stopped_before_policy_replay": True,
        "candidates": [
            {
                "wallet": WALLET_4C,
                "wide_policy_fingerprint": "f1",
                "joined_raw_attempts": 100,
                "own_policy_replay_copyables": None,
                "missing_required_fields": {
                    "f4_book_snapshot_for_own_policy_pass": 44,
                    "token_metadata.market_slug": 43,
                },
            },
            # Duplicate generation rows are collapsed by wallet + fingerprint.
            {
                "wallet": WALLET_4C,
                "wide_policy_fingerprint": "f1",
                "joined_raw_attempts": 90,
                "own_policy_replay_copyables": None,
                "missing_required_fields": {
                    "f4_book_snapshot_for_own_policy_pass": 40,
                },
            },
            {
                "wallet": WALLET_56,
                "wide_policy_fingerprint": "f2",
                "joined_raw_attempts": 82,
                "own_policy_replay_copyables": None,
                "missing_required_fields": {"token_metadata.outcome": 26},
            },
            {"wallet": "0x" + "0" * 40, "wide_policy_fingerprint": "excluded"},
        ],
    }
    report = build_report(replay_audit=replay)
    assert report["pre_registered_branch"] == "E3⁹"
    assert report["status"] == "E3_OWN_POLICY_REPLAY_INPUTS_NOT_PERSISTED_STOP"
    assert report["shortlist_wallet_count"] == 2
    assert report["shortlist_candidate_count"] == 2
    assert report["raw_join_coverage"] == 1.0
    assert report["exact_blocking_fields"] == [
        "f4_book_snapshot_for_own_policy_pass",
        "token_metadata.market_slug",
        "token_metadata.outcome",
    ]
    assert report["positive_copyable_candidates"] == []
    assert report["synthesis_permitted"] is False


def test_order149_names_positive_own_policy_candidate_without_bending_f1() -> None:
    replay = {
        "status": "REPLAY_COMPLETE",
        "candidates": [
            {
                "wallet": WALLET_4C,
                "wide_policy_fingerprint": "f1",
                "joined_raw_attempts": 100,
                "own_policy_replay_copyables": 3,
                "missing_required_fields": {},
            }
        ],
    }
    report = build_report(replay_audit=replay)
    assert report["pre_registered_branch"] == "E2⁹"
    assert report["positive_copyable_candidates"][0]["wallet"] == WALLET_4C
    assert "no F1-bar change" in report["rule"]


def test_order149_positive_lower_bound_dominates_additive_missing_rows() -> None:
    replay = {
        "status": "OWN_POLICY_COPYABLE_FOUND",
        "verdict": True,
        "candidates": [{
            "wallet": WALLET_4C,
            "wide_policy_fingerprint": "f1",
            "joined_raw_attempts": 80,
            "own_policy_replay_copyables": 2,
            "missing_required_fields": {"token_metadata_join": 4},
        }],
    }
    report = build_report(replay_audit=replay)
    assert report["pre_registered_branch"] == "E2⁹"
    assert report["status"] == "E2_OWN_POLICY_COPYABLE_CANDIDATES_FOUND"
    assert report["exact_blocking_fields"] == ["token_metadata_join"]


def test_order149_refuses_historical_replay_as_fresh_f2_gate() -> None:
    replay = {"status": "OWN_POLICY_COPYABLE_FOUND", "f2_gate_comparable": False, "candidates": [{
        "wallet": WALLET_4C, "wide_policy_fingerprint": "f1", "own_policy_replay_copyables": 3,
    }]}
    report = build_report(replay_audit=replay)
    assert report["pre_registered_branch"] == "E3⁹"
    assert report["status"] == "E3_SCOPE_MISMATCH_NOT_A_FRESH_F2_VERDICT"
