from scripts.report_payout_receipt_reconciliation import build_report


def test_build_report_places_booked_payout_beside_observed_credit_and_names_seam():
    truth = {"events": [
        {"order_id": "win", "market_slug": "m1", "condition_id": "c1", "submitted_at": "2026-07-06T00:00:00Z", "ts": 2.0, "status": "FILLED", "resolved": True, "payout_usd": 5.0},
        {"order_id": "loss", "market_slug": "m2", "condition_id": "c2", "submitted_at": "2026-07-06T00:01:00Z", "ts": 3.0, "status": "FILLED", "resolved": True, "payout_usd": 0.0},
    ]}
    cash = {"rows": [{"classification": "redemption_payout", "signed_amount_usd": 5.0, "tx": "tx1", "block_iso": "now", "matched_evidence": {"condition_id": "c1"}}]}
    fee = {"rows": [{"order_id": "win", "market_slug": "m1", "submitted_at": "2026-07-06T00:00:00Z"}]}
    report = build_report(truth=truth, cash_audit=cash, fee_report=fee, start_iso="1970-01-01T00:00:01Z", generated_at="now")
    assert report["rows"][0]["booked_payout_usd"] == 5.0
    assert report["rows"][0]["observed_pusd_credit_usd"] == 5.0
    assert report["rows"][0]["payout_delta_usd"] == 0.0
    assert report["coverage_seam"]["named_rows"][0]["order_id"] == "loss"
    assert report["status"] == "PASS_ALL_DIFFERENCES_NAMED"


def test_build_report_never_interpolates_missing_redemption_credit():
    truth = {"events": [{"order_id": "win", "market_slug": "m", "condition_id": "missing", "submitted_at": "2026-07-06T00:00:00Z", "ts": 2.0, "status": "FILLED", "resolved": True, "payout_usd": 1.0}]}
    report = build_report(truth=truth, cash_audit={"rows": []}, fee_report={"rows": []}, start_iso="1970-01-01T00:00:01Z", generated_at="now")
    assert report["rows"][0]["observed_pusd_credit_usd"] is None
    assert report["rows"][0]["status"] == "NAMED_GAP_NO_REDEMPTION_CREDIT"
