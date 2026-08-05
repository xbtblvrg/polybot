from scripts.report_band_pnl_surface_reconciliation import build_report


def _truth(slug: str, submitted_at: str, pnl: float, *, band: str = "01a_25_32") -> dict:
    return {
        "status": "FILLED",
        "resolved": True,
        "market_slug": slug,
        "submitted_at": submitted_at,
        "price_subbucket": band,
        "cost_usd": 1.0,
        "shares": 2.0,
        "limit_price": 0.30,
        "pnl_usd": pnl,
    }


def _artifact(slug: str, submitted_at: str, pnl: float, fee: float) -> dict:
    return {
        "market_slug": slug,
        "submitted_at": submitted_at,
        "entry_price": 0.30,
        "filled_cost_usd": 1.0,
        "pnl_usd_realized": pnl,
        "modeled_unvalidated_fee_usd": fee,
    }


def test_surface_gap_decomposes_population_fee_and_residual() -> None:
    report = build_report(
        truth_events=[
            _truth("matched", "2026-07-01T00:00:00Z", 1.0),
            _truth("ledger-only", "2026-07-02T00:00:00Z", -0.5),
        ],
        artifact_rows=[
            _artifact("matched", "2026-07-01T00:00:00Z", 0.8, 0.1),
            _artifact("artifact-only", "2026-07-03T00:00:00Z", -1.1, 0.1),
        ],
        generated_at="now",
    )

    band = report["bands"]["01a_25_32"]
    assert band["rows_only_in_ledger"] == 1
    assert band["rows_only_in_artefact"] == 1
    assert band["observed_pnl_gap_usd"] == 0.8
    assert band["pnl_gap_decomposition"] == {
        "population_usd": 0.6,
        "matched_method_residual_usd": 0.2,
        "sum_usd": 0.8,
        "error_usd": 0.0,
        "status": "PASS_WITHIN_0.01",
    }
    assert band["modeled_unvalidated_fee_usd"] == 0.2
    assert report["payout_usd_semantics"]["classification"] == (
        "REALIZED_PAYOUT_MINUS_RECEIPT_MAPPED_COST"
    )


def test_whole_book_decision_uses_realized_money_only() -> None:
    rows = []
    for day in range(1, 9):
        for index in range(20):
            row = _truth(
                f"slug-{day}-{index}",
                f"2026-07-{day:02d}T00:{index:02d}:00Z",
                0.01,
            )
            row["shares"] = 2.0
            row["cost_usd"] = 1.0
            rows.append(row)

    report = build_report(truth_events=rows, artifact_rows=[], generated_at="now")

    holdout = report["whole_book_day_bounded"]["chronological_holdout"]
    assert holdout["pnl_usd_realized"] > 0
    assert report["decision"] == "REALIZED_EDGE_BOTH_HALVES_POSITIVE"
    assert holdout["accounting_authority"] is False


def test_reconciliation_preserves_artifact_exact_0_50_boundary() -> None:
    artifact = _artifact("exact-50", "2026-07-01T00:00:00Z", -1.0, 0.1)
    artifact["entry_price"] = 0.50

    report = build_report(truth_events=[], artifact_rows=[artifact], generated_at="now")

    assert report["bands"]["01c_40_50"]["artefact_rows"] == 1
    assert report["band_definition_mismatch"]["exact_0_50_artefact_rows"] == 1
