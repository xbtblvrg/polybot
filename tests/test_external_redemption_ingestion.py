import json
from pathlib import Path

from scripts.ingest_external_redemptions import build_artifact, relabel_packet


def test_external_redemption_ingestion_confirms_packet_rows_without_residual_rewrite() -> None:
    packet = {
        "money_truth": {
            "since_topup": {
                "cash_diff_reconciliation_residual": {"residual_usd": -7.590385},
                "self_feed_reconciliation_overlay": {"overlay_delta_usd": -20.158263},
            }
        },
        "round_trip": {
            "anchor": {
                "condition_id": "0xa3",
                "confirmed": False,
                "h2_classification": "H2_PENDING",
                "transaction_hash": "0xc017aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            },
            "recent_drain_match_table": [
                {
                    "condition_id": "0xa3",
                    "market_slug": "btc-updown-5m-1",
                    "transaction_hash": "0xc017aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "redeem_iso": "2026-07-08T16:55:59Z",
                    "redeem_usdc": 4.998,
                    "canonical_payout_usd": 4.998,
                    "delta_usd": 0.0,
                    "h2_classification": "H2_PENDING",
                    "match_status": "PROVISIONAL_EXTERNAL_REDEEM_MATCH",
                },
                {
                    "condition_id": "0x7844",
                    "market_slug": "btc-updown-5m-2",
                    "transaction_hash": "0xe88cbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "redeem_iso": "2026-07-08T17:15:31Z",
                    "redeem_usdc": 12.448674,
                    "canonical_payout_usd": 12.448674,
                    "delta_usd": 0.0,
                    "h2_classification": "H2_PENDING",
                    "match_status": "PROVISIONAL_EXTERNAL_REDEEM_MATCH",
                },
            ],
        },
    }

    artifact = build_artifact(packet, packet_path="packet.json")

    assert artifact["status"] == "PASS"
    assert artifact["ledger_rewrite"] is False
    assert artifact["overlay_source_name"] == "external_data_api_redeem_condition_join"
    assert artifact["summary"]["external_redeem_rows"] == 2
    assert artifact["summary"]["confirmed_external_redeem_rows"] == 2
    assert artifact["summary"]["total_redeem_usdc"] == 17.446674
    assert artifact["acceptance"]["required_anchor_txs_present"] == {"0xc017": True, "0xe88c": True}
    assert artifact["acceptance"]["cash_diff_residual_usd"] == -7.590385
    assert artifact["acceptance"]["residual_explained_by_external_redeems_usd"] == 0.0
    assert artifact["acceptance"]["residual_unexplained_after_external_redeems_usd"] == -7.590385
    assert artifact["rows"][0]["match_status"] == "CONFIRMED_EXTERNAL_REDEEM"

    relabeled = relabel_packet(packet, artifact)
    assert relabeled["round_trip"]["anchor"]["confirmed"] is True
    assert relabeled["round_trip"]["anchor"]["h2_classification"] == "CONFIRMED_EXTERNAL_REDEEM"
    assert relabeled["round_trip"]["recent_drain_match_table"][0]["match_status"] == "CONFIRMED_EXTERNAL_REDEEM"
    assert relabeled["round_trip"]["h2_status"]["ledger_rewrite"] is False


def test_external_redemption_ingestion_cli_writes_artifact_and_relabels(tmp_path: Path) -> None:
    from scripts.ingest_external_redemptions import main
    import sys

    packet = tmp_path / "packet.json"
    output = tmp_path / "h2.json"
    packet.write_text(
        json.dumps(
            {
                "money_truth": {"since_topup": {"cash_diff_reconciliation_residual": {"residual_usd": -1.0}}},
                "round_trip": {
                    "anchor": {"confirmed": False},
                    "recent_drain_match_table": [
                        {
                            "condition_id": "0xa3",
                            "transaction_hash": "0xc017aaa",
                            "redeem_usdc": 4.998,
                            "canonical_payout_usd": 4.998,
                        }
                    ],
                },
            }
        )
    )
    old_argv = sys.argv
    sys.argv = [
        "ingest_external_redemptions.py",
        "--packet",
        str(packet),
        "--output",
        str(output),
    ]
    try:
        assert main() == 0
    finally:
        sys.argv = old_argv

    artifact = json.loads(output.read_text())
    assert artifact["summary"]["external_redeem_rows"] == 1
    relabeled = json.loads(packet.read_text())
    assert relabeled["round_trip"]["anchor_verdict"] == "CONFIRMED_EXTERNAL_REDEEM_ACCOUNTING_INGESTION_ATTACHED_NO_LEDGER_REWRITE"
