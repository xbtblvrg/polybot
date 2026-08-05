import json

from scripts.report_widened_fingerprint_paper_candidates import (
    _family_exhaustion_verdict,
    _venue_disclosure,
    build_packet,
)


def test_widened_candidate_stays_paper_only(tmp_path) -> None:
    wallet = "0x" + "a" * 40
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "manifest_id": "manifest",
                "score_run_id": "run",
                "capture_watch_wallets": [
                    {
                        "wallet": wallet,
                        "move_slice_keys": ["a", "b"],
                    }
                ],
            }
        )
    )
    packet = build_packet(
        ledger_rows=[],
        resolution_rows=[],
        manifests=[manifest],
        reconciliation={
            "rows": [
                {
                    "wallet": wallet,
                    "move_slice_keys": ["a"],
                }
            ]
        },
        generated_at="2026-07-31T08:00:00+00:00",
    )

    row = packet["rows"][0]
    assert row["current_move_slice_keys"] == ["a"]
    assert row["widened_move_slice_keys"] == ["a", "b"]
    assert row["additional_move_slice_key_count"] == 1
    assert packet["paper_only"] is True
    assert packet["live_orders_allowed"] is False
    assert packet["live_mutation"] is False
    assert packet["f1_floor_unchanged"] == 200
    assert packet["family_exhaustion_verdict"] == "NON_NEGATIVE"
    assert packet["decision"] == "PAPER_ONLY_NO_PROMOTION_THIS_PULSE"


def test_venue_disclosure_distinguishes_unmeasured_concentration() -> None:
    disclosure = _venue_disclosure(
        {
            "resolved": 250,
            "post_fee_pnl_usd": 25.0,
            "roi_pct": 5.0,
            "first_half_post_fee_pnl_usd": 15.0,
            "second_half_post_fee_pnl_usd": 10.0,
            "f1_venue_reachable_admissible": True,
            "venue_reachable_share_pct": 45.0,
            "concentration_admissible": False,
            "top_1_market_share_pct": None,
            "pnl_excluding_top_1_market": 20.0,
        }
    )

    assert disclosure["concentration_basis"] == "unmeasured_null_share"
    assert disclosure["concentration_threshold_pct"] == 50.0
    assert disclosure["pnl_excluding_top_1_market"] == 20.0
    assert disclosure["f1_conjuncts"]["concentration_admissible"] is False
    assert disclosure["blocking_conjuncts"] == ["concentration_admissible"]


def test_family_exhaustion_persists_negative_widening_gradient() -> None:
    rows = [
        {
            "additional_move_slice_key_count": 12,
            "baseline_to_widened": {
                "baseline_all_f1_conjuncts_pass": True,
                "widened_all_f1_conjuncts_pass": False,
                "roi_pct_delta": -14.193469,
                "venue_reachable_share_pct_delta": -8.09767,
            },
        }
    ]

    assert _family_exhaustion_verdict(rows) == "NEGATIVE"
