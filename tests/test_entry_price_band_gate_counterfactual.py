import json

from scripts.report_entry_price_band_gate_counterfactual import build_report, main


def test_counterfactual_report_dedupes_guard_cycles_and_resolves_post_fee_loss():
    base = {
        "event": "wallet_copy_live_entry_price_band_gate_counterfactual",
        "counterfactual_id": "epbgcf_1",
        "intent_id": "ci_1",
        "market_slug": "btc-updown-5m-1",
        "condition_id": "0xabc",
        "outcome": "Up",
        "limit_price": 0.3,
        "shares": 3.0,
        "copy_size_usd": 1.0,
        "expected_fee_usd": 0.05,
    }
    report = build_report(
        event_rows=[base, dict(base)],
        resolution_rows=[{"event_type": "market_resolved", "market": "0xabc", "winning_outcome": "Down"}],
        generated_at="2026-07-20T17:00:00Z",
        deadline_utc="2026-07-27T16:50:00Z",
        min_resolved_windows=100,
    )

    assert report["unique_suppressed_intents"] == 1
    assert report["resolved_gated_flow_windows"] == 1
    assert report["ungated_counterfactual_post_fee_pnl_usd"] == -1.05
    assert report["gated_minus_ungated_post_fee_pnl_usd"] == 1.05
    assert report["status"] == "ACCRUING"


def test_counterfactual_report_resolves_from_canonical_btc_resolution_rows():
    row = {
        "event": "wallet_copy_live_entry_price_band_gate_counterfactual",
        "counterfactual_id": "epbgcf_2",
        "intent_id": "ci_2",
        "market_slug": "btc-updown-5m-1784566800",
        "condition_id": "0xdef",
        "outcome": "Up",
        "limit_price": 0.3,
        "shares": 3.0,
        "copy_size_usd": 1.0,
        "expected_fee_usd": 0.05,
    }
    report = build_report(
        event_rows=[row],
        resolution_rows=[
            {
                "condition_id": "0xother",
                "market_slug": "btc-updown-5m-1784566800",
                "direction": "UP",
                "source": "polymarket_gamma_resolved_outcome",
            }
        ],
        generated_at="2026-07-20T17:00:00Z",
        deadline_utc="2026-07-27T16:50:00Z",
        min_resolved_windows=1,
    )

    assert report["decision_boundary_reached"] is True
    assert report["resolved_gated_flow_windows"] == 1
    assert report["ungated_counterfactual_post_fee_pnl_usd"] == 1.95
    assert report["gated_minus_ungated_post_fee_pnl_usd"] == -1.95
    assert report["status"] == "FAIL"


def test_counterfactual_cli_defaults_to_canonical_btc_resolution_log(monkeypatch, tmp_path):
    read_paths = []

    def fake_jsonl(path):
        read_paths.append(str(path))
        return iter(())

    monkeypatch.setattr("scripts.report_entry_price_band_gate_counterfactual._jsonl", fake_jsonl)
    monkeypatch.setattr(
        "sys.argv",
        [
            "report_entry_price_band_gate_counterfactual.py",
            "--event-log",
            str(tmp_path / "events.jsonl"),
            "--output",
            str(tmp_path / "report.json"),
            "--deadline-utc",
            "2099-01-01T00:00:00Z",
        ],
    )

    assert main() == 0
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["status"] == "ACCRUING"
    assert any(path.endswith("data/research/btc_resolutions_from_btcusdt_ticks.jsonl") for path in read_paths)
    assert not any(path.endswith("data/research/clob_market_ws_events.jsonl") for path in read_paths)
