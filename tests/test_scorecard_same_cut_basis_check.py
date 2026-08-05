import scripts.report_scorecard_same_cut_basis_check as report


def test_parse_text_totals() -> None:
    totals = report.parse_text_totals(
        "total orders=13 fills=11 resolved=11 rejects=1 pnl=+10.971504\n"
    )

    assert totals == {
        "orders": 13,
        "fills": 11,
        "resolved_fills": 11,
        "rejects": 1,
        "pnl_usd": 10.971504,
    }
