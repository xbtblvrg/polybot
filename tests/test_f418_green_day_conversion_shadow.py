from scripts.report_f418_green_day_conversion_shadow import build_report


WALLET = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
SIGN = "2026-07-23T12:10:42Z"
SIGN_TS = 1_784_808_642.0


def _event(offset, window, status, pnl=0.0, resolved=False, error_class=None):
    row = {
        "source_wallet": WALLET,
        "ts": SIGN_TS + offset,
        "market_slug": f"btc-updown-5m-{window}",
        "status": status,
        "pnl_usd": pnl,
        "resolved": resolved,
    }
    if error_class:
        row["trade_result"] = {"error_class": error_class}
    return row


def test_shadow_counts_unique_submitted_windows_and_resolved_pnl():
    events = [
        _event(-100_000, 50, "FILLED", pnl=99.0, resolved=True),
        _event(-10, 100, "REJECTED", error_class="fak_no_match"),
        _event(-9, 100, "REJECTED", error_class="maker_cap"),
        _event(-8, 100, "FILLED", pnl=2.0, resolved=True),
        _event(1, 200, "FILLED", pnl=-1.0, resolved=True),
        _event(2, 201, "REJECTED", error_class="fak_no_match"),
    ]
    report = build_report(events, green_sign_from=SIGN, generated_at=SIGN)
    assert report["control_pre_sign"]["submitted_windows"] == 1
    assert report["control_pre_sign"]["filled_windows"] == 1
    assert report["control_pre_sign"]["canonical_post_fee_pnl_usd"] == 2.0
    assert report["green_sign_post"]["submitted_windows"] == 2
    assert report["green_sign_post"]["filled_windows"] == 1
    assert report["green_sign_post"]["canonical_post_fee_ev_per_submitted_window_usd"] == -0.5
    assert report["green_sign_post"]["rejection_taxonomy"] == {"fak_no_match": 1}
    assert report["verdict"] == "ACCRUE_PREREGISTERED_SAMPLE"
    assert report["live_mutation"] is False


def test_shadow_names_exchange_conversion_after_preregistered_gate():
    events = []
    for index in range(30):
        events.append(_event(-100 - index, 1000 + index, "FILLED", pnl=0.1, resolved=True))
    for index in range(20):
        status = "FILLED" if index < 5 else "REJECTED"
        events.append(_event(index + 1, 2000 + index, status, pnl=0.1, resolved=status == "FILLED"))
    report = build_report(events, green_sign_from=SIGN, generated_at=SIGN)
    assert report["sample_gate_pass"] is True
    assert report["control_pre_sign"]["submitted_to_filled_pct"] == 100.0
    assert report["green_sign_post"]["submitted_to_filled_pct"] == 25.0
    assert report["dual_bar_bottleneck"] == "EXCHANGE_CONVERSION"
