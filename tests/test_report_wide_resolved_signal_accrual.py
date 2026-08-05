import json
from pathlib import Path

from scripts.report_wide_resolved_signal_accrual import build_report


WALLET_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
WALLET_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
WALLET_C = "0xcccccccccccccccccccccccccccccccccccccccc"


def _candidate(wallet: str, resolved: int) -> dict:
    return {
        "wallet": wallet,
        "wide_policy_fingerprint": f"fp-{wallet}",
        "regime_evidence": {"resolved_signals": resolved},
    }


def test_reports_manifest_accrual_and_insufficient_history(
    tmp_path: Path,
) -> None:
    manifests: list[Path] = []
    for index, (generated_at, counts) in enumerate(
        [
            (
                "2026-07-29T20:00:00Z",
                {WALLET_A: 1, WALLET_B: 1, WALLET_C: 1},
            ),
            (
                "2026-07-30T20:00:00Z",
                {WALLET_A: 2, WALLET_C: 1},
            ),
        ]
    ):
        manifest = tmp_path / (
            f"wide_exact_policy_manifest_wide_run{index}.json"
        )
        manifest.write_text(
            json.dumps(
                {
                    "generated_at": generated_at,
                    "score_run_id": f"run{index}",
                    "manifest_id": f"manifest{index}",
                    "capture_watch_wallets": [
                        {
                            "wallet": wallet,
                            "wide_policy_fingerprint": f"fp-{wallet}",
                            "slice_freeze": {
                                "f1": {"resolved": resolved}
                            },
                        }
                        for wallet, resolved in counts.items()
                    ],
                }
            ),
            encoding="utf-8",
        )
        manifests.append(manifest)
    frontier = {
        "frontier_checksum": "frontier",
        "frontier_key": "wallet|wide_policy_fingerprint|source_generation",
        "nearest_frontier": [
            _candidate(WALLET_A, 2),
            _candidate(WALLET_B, 1),
            _candidate(WALLET_C, 1),
        ],
    }

    report = build_report(
        frontier=frontier,
        manifest_paths=manifests,
        expected_frontier_checksum="frontier",
        as_of="2026-07-30T20:00:00Z",
    )

    assert report["rows"][0]["observed_resolved_signal_delta"] == 1
    assert report["rows"][0]["current_rate_per_day"] == 1.0
    assert report["rows"][1][
        "same_identity_manifest_observations_in_observed_span"
    ] == 1
    assert report["rows"][1]["projected_crossing_at"] is None
    assert report["rows"][1]["crossing_projection"] == (
        "NONE_INSUFFICIENT_OR_ZERO_SAME_IDENTITY_RATE"
    )
    assert report["rows"][2]["observed_resolved_signal_delta"] == 0
    assert report["rows"][2]["projected_crossing_at"] is None
    assert report["rows"][0]["crossing_projection"] == (
        "LINEAR_OBSERVED_SPAN_RATE"
    )
    assert report["measurement"]["maximum_lookback_days"] == 7
    assert "window_days" not in report["measurement"]
