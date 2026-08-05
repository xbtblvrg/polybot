from datetime import datetime, timezone

from scripts.report_wide_wallet_terminal_breakdown import build_report


def test_build_report_surfaces_wallet_terminal_taxonomy(monkeypatch) -> None:
    wallet = "0x" + "8" * 40
    snapshot = {
        "lookback_s": 1800.0,
        "checksum": "snapshot",
        "per_wallet": {
            wallet: {
                "attempts": 10,
                "copyable": 2,
                "policy_depth_pass": 2,
                "terminal_taxonomy": {
                    "COPYABLE_EXACT_POLICY_PAPER_FILL": 2,
                    "REFUSED_METADATA_MISSING": 7,
                    "REFUSED_STALE_RECEIPT_TO_FETCH": 1,
                },
                "source_generation": "generation",
                "generation_identity": {"manifest_id": "manifest"},
                "latest_receipt_at": "2026-07-31T02:42:00+00:00",
            }
        },
    }
    monkeypatch.setattr(
        "scripts.report_wide_wallet_terminal_breakdown._wide_direct_source_snapshot",
        lambda *_args, **_kwargs: snapshot,
    )

    report = build_report(
        wallet=wallet.upper(),
        now=datetime(2026, 7, 31, 2, 42, tzinfo=timezone.utc),
        direct_state={},
        journal=[],
    )

    assert report["attempts"] == 10
    assert report["terminal_taxonomy_total"] == 10
    assert report["metadata_missing_share_pct"] == 70.0
    assert report["metadata_missing_predominant"] is True
    assert report["live_orders_allowed"] is False
