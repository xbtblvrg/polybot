from scripts.report_order151_fee_bps import build_report


def test_fee_bps_is_proportional_across_notionals():
    report = build_report({"embedded_fee_evidence": {"fee_pct_of_response_cost_weighted": 5.0, "row_count": 2}})
    assert [row["fee_bps"] for row in report["notional_rows"]] == [500.0, 500.0, 500.0]
    assert [row["fee_usd_proportional"] for row in report["notional_rows"]] == [0.025, 0.05, 0.6069]
