import json

from scripts.snapshot_wide_generation_measurement import recover_generation


def test_recovers_exact_run_and_deduplicates_attempts(tmp_path) -> None:
    ledger = tmp_path / "ledger.jsonl"
    rows = [
        {
            "run_id": "wanted",
            "attempt_id": "a",
            "wallet": "0xabc",
            "cohort_id": "cohort",
            "f1_f4_terminal": {"terminal": "REFUSED_ALPHA_PROFILE_FILTER"},
        },
        {
            "run_id": "wanted",
            "attempt_id": "a",
            "wallet": "0xabc",
            "cohort_id": "cohort",
            "f1_f4_terminal": {"terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL"},
        },
        {
            "run_id": "other",
            "attempt_id": "b",
            "wallet": "0xabc",
            "f1_f4_terminal": {"terminal": "REFUSED_METADATA_MISSING"},
        },
    ]
    ledger.write_text("".join(json.dumps(row) + "\n" for row in rows))
    manifest = {
        "manifest_id": "manifest",
        "score_run_id": "wanted",
        "capture_watch_wallets": [
            {
                "wallet": "0xabc",
                "wide_policy_fingerprint": "fp",
                "move_slice_keys": ["slice"],
            }
        ],
    }

    snapshot = recover_generation(run_id="wanted", manifest=manifest, ledger_path=ledger)

    assert snapshot["terminal_reconciliation"]["terminal_rows"] == 1
    assert snapshot["wallets"]["0xabc"]["attempted_exact_policy_buys"] == 1
    assert snapshot["wallets"]["0xabc"]["copyable_exact_policy_buys"] == 1
