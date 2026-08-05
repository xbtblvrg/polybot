import json

from scripts.report_profit_latency_suppression_counterfactual import build_report, main


def _event(intent_id, market_slug, window_time_s, outcome="Up", price=0.4, ts="2026-07-21T10:00:00+00:00"):
    return {
        "event": "wallet_copy_live_profit_latency_suppression_reject",
        "intent_id": intent_id,
        "market_slug": market_slug,
        "outcome": outcome,
        "limit_price": price,
        "copy_size_usd": 1.0,
        "window_time_s": window_time_s,
        "ts": ts,
        "taxonomy": "window_time_gte_60s",
        "taxonomy_tags": ["window_time_gte_60s"],
    }


def test_report_dedupes_intents_and_windows_and_buckets_post_fee_pnl():
    report = build_report(
        event_rows=[
            _event("ci_1", "btc-updown-5m-1", 80),
            _event("ci_1", "btc-updown-5m-1", 90),
            _event("ci_2", "btc-updown-5m-1", 130, outcome="Down"),
            _event("ci_3", "btc-updown-5m-2", 150, outcome="Down", price=0.5),
            _event("ci_4", "btc-updown-5m-3", 210),
        ],
        resolution_rows=[
            {"market_slug": "btc-updown-5m-1", "direction": "UP"},
            {"market_slug": "btc-updown-5m-2", "direction": "DOWN"},
            {"market_slug": "btc-updown-5m-3", "direction": "DOWN"},
        ],
        generated_at="2026-07-21T10:00:00Z",
        min_resolved_windows=2,
    )

    assert report["unique_suppressed_intents"] == 4
    assert report["suppressed_windows"] == 3
    assert report["buckets"]["60_120"]["resolved_windows"] == 1
    assert report["buckets"]["120_180"]["resolved_windows"] == 1
    assert report["buckets"]["180_plus"]["resolved_windows"] == 1
    assert report["decision_band_60_180"]["post_fee_counterfactual_pnl_usd"] > 0
    assert report["status"] == "RAISE_TO_180"


def test_report_keeps_60_when_decision_band_is_not_positive():
    report = build_report(
        event_rows=[_event("ci_1", "btc-updown-5m-1", 100)],
        resolution_rows=[{"market_slug": "btc-updown-5m-1", "direction": "DOWN"}],
        generated_at="2026-07-21T10:00:00Z",
        min_resolved_windows=1,
    )
    assert report["decision_band_60_180"]["post_fee_counterfactual_pnl_usd"] < 0
    assert report["status"] == "KEEP_60"


def test_report_ignores_signal_age_only_rejects():
    row = _event("ci_1", "btc-updown-5m-1", 100)
    row["taxonomy"] = "signal_age_gte_60s"
    row["taxonomy_tags"] = ["signal_age_gte_60s"]
    report = build_report(
        event_rows=[row],
        resolution_rows=[{"market_slug": "btc-updown-5m-1", "direction": "UP"}],
        generated_at="2026-07-21T10:00:00Z",
    )
    assert report["unique_suppressed_intents"] == 0
    assert report["resolved_suppressed_windows"] == 0


def test_decision_boundary_counts_all_resolved_suppressed_windows():
    report = build_report(
        event_rows=[
            _event("ci_1", "btc-updown-5m-1", 100),
            _event("ci_2", "btc-updown-5m-2", 210),
        ],
        resolution_rows=[
            {"market_slug": "btc-updown-5m-1", "direction": "UP"},
            {"market_slug": "btc-updown-5m-2", "direction": "DOWN"},
        ],
        generated_at="2026-07-21T10:00:00Z",
        min_resolved_windows=2,
    )
    assert report["resolved_suppressed_windows"] == 2
    assert report["decision_boundary_reached"] is True
    assert report["status"] == "RAISE_TO_180"


def test_price_band_buckets_separate_01a_and_cover_every_resolved_row():
    report = build_report(
        event_rows=[
            _event("ci_1", "btc-updown-5m-1", 80, price=0.27),
            _event("ci_2", "btc-updown-5m-2", 80, price=0.27, outcome="Down"),
            _event("ci_3", "btc-updown-5m-3", 130, price=0.45),
        ],
        resolution_rows=[
            {"market_slug": "btc-updown-5m-1", "direction": "UP"},
            {"market_slug": "btc-updown-5m-2", "direction": "UP"},
            {"market_slug": "btc-updown-5m-3", "direction": "UP"},
        ],
        generated_at="2026-07-21T10:00:00Z",
        min_resolved_windows=1,
    )

    bands = report["price_band_buckets"]
    assert bands["01a_25_32"]["60_120"]["resolved_windows"] == 2
    assert bands["out_of_01a"]["120_180"]["resolved_windows"] == 1
    assert "180_plus" not in bands["01a_25_32"]
    counted = sum(
        cell["resolved_windows"] for band in bands.values() for cell in band.values()
    )
    assert counted == report["resolved_suppressed_windows"] == report["rows_total"]


def test_focus_01a_60_120_split_is_day_bounded_and_needs_two_days():
    single_day = build_report(
        event_rows=[_event("ci_1", "btc-updown-5m-1", 80, price=0.27)],
        resolution_rows=[{"market_slug": "btc-updown-5m-1", "direction": "UP"}],
        generated_at="2026-07-21T10:00:00Z",
        min_resolved_windows=1,
    )
    assert single_day["focus_01a_60_120"]["status"] == "ACCRUING_INSUFFICIENT_DAYS"

    two_day = build_report(
        event_rows=[
            _event("ci_1", "btc-updown-5m-1", 80, price=0.27, ts="2026-07-20T10:00:00+00:00"),
            _event("ci_2", "btc-updown-5m-2", 80, price=0.27, ts="2026-07-21T10:00:00+00:00"),
        ],
        resolution_rows=[
            {"market_slug": "btc-updown-5m-1", "direction": "DOWN"},
            {"market_slug": "btc-updown-5m-2", "direction": "UP"},
        ],
        generated_at="2026-07-21T10:00:00Z",
        min_resolved_windows=1,
    )
    focus = two_day["focus_01a_60_120"]
    assert focus["split_integrity"] == "DAY_BOUNDED"
    assert focus["split_day"] == "2026-07-21"
    assert focus["development"]["rows"] == 1
    assert focus["chronological_holdout"]["rows"] == 1
    assert focus["status"] == "POSITIVE_HOLDOUT"


def test_cli_uses_canonical_resolution_log(monkeypatch, tmp_path):
    paths = []

    def fake_jsonl(path):
        paths.append(str(path))
        return iter(())

    monkeypatch.setattr("scripts.report_profit_latency_suppression_counterfactual._jsonl", fake_jsonl)
    monkeypatch.setattr(
        "sys.argv",
        [
            "report_profit_latency_suppression_counterfactual.py",
            "--event-log",
            str(tmp_path / "events.jsonl"),
            "--output",
            str(tmp_path / "report.json"),
        ],
    )
    assert main() == 0
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["status"] == "ACCRUING"
    assert any(path.endswith("btc_resolutions_from_btcusdt_ticks.jsonl") for path in paths)
