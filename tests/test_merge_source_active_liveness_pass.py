import pytest

from scripts.merge_source_active_liveness_pass import merge_pass


def _row(wallet: str) -> dict:
    return {
        "wallet": wallet,
        "source_active_windows": 1,
        "policy_eligible_windows": 1,
        "source_active_tally_status": "PASS",
        "policy_eligible_tally_status": "PASS",
    }


def test_identity_rebased_pass_merges_without_duplicate_wallets() -> None:
    wallets = ["0x" + char * 40 for char in "abcde"]
    replay = {"live_ready_picks": [{"wallet": wallet} for wallet in wallets]}
    cohort = {"reports": [_row(wallets[0])], "source_active_pass": 1, "policy_eligible_pass": 1, "total_source_active_windows": 1, "total_policy_eligible_windows": 1}
    batch = {"batch_id": "p2", "generated_at": "2026-07-20T15:00:00Z", "wallet_offset": 1, "reports": [_row(wallets[1]), _row(wallets[2])], "source_active_pass": 2, "policy_eligible_pass": 2, "total_source_active_windows": 2, "total_policy_eligible_windows": 2}

    updated_batch, updated_cohort = merge_pass(replay=replay, batch=batch, cohort=cohort, pass_number=2, expected_offset=1, previous_last_wallet=wallets[0])

    assert updated_batch["identity_rebase"]["acceptance_gate"] == "PASS_ZERO_ALREADY_PROCESSED_IDENTITIES"
    assert updated_cohort["wallet_count"] == 3
    assert updated_cohort["source_active_pass"] == 3
    assert updated_cohort["source_active_cumulative"] == {
        "reports": 3,
        "source_active_pass": 3,
        "policy_eligible_pass": 3,
    }
    assert updated_cohort["last_processed_wallet"] == wallets[2]


def test_identity_rebased_pass_rejects_prior_identity_overlap() -> None:
    wallets = ["0x" + char * 40 for char in "abc"]
    replay = {"live_ready_picks": [{"wallet": wallet} for wallet in wallets]}
    batch = {"wallet_offset": 1, "reports": [_row(wallets[1])]}

    with pytest.raises(ValueError, match="already-processed identities"):
        merge_pass(replay=replay, batch=batch, cohort={"reports": [_row(wallets[0]), _row(wallets[1])]}, pass_number=2, expected_offset=1, previous_last_wallet=wallets[0])


def test_identity_rebased_pass_rejects_numeric_offset_without_identity_match() -> None:
    wallets = ["0x" + char * 40 for char in "abc"]
    replay = {"live_ready_picks": [{"wallet": wallet} for wallet in wallets]}
    batch = {"wallet_offset": 2, "reports": [_row(wallets[1])]}

    with pytest.raises(ValueError, match="identity-rebased replay slice"):
        merge_pass(replay=replay, batch=batch, cohort={"reports": [_row(wallets[0])]}, pass_number=2, expected_offset=1, previous_last_wallet=wallets[0])
