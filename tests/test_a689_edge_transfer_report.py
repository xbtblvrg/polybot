import json
from pathlib import Path

from scripts.report_a689_edge_transfer import build_report


WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_a689_edge_transfer_recomputes_guard_subset(tmp_path: Path) -> None:
    data = tmp_path / "data"
    live_guard = data / "live_guard.json"
    watch_history = data / "watch_history.json"
    watch_report = data / "watch_report.json"
    hot_standby = data / "hot.json"
    price_state = data / "price_state.json"
    price_events = data / "price_events.jsonl"
    resolutions = data / "resolutions.jsonl"

    _write_json(
        live_guard,
        {
            "guard_runtime_filter": {
                "profit_latency_window_time_suppress_gte_s": 60.0,
                "inventory_late_window_stop_s": 5.0,
            }
        },
    )
    _write_jsonl(
        resolutions,
        [
            {"condition_id": "0xpass", "direction": "Up"},
            {"condition_id": "0xsource_late", "direction": "Down"},
            {"condition_id": "0xpipe_late", "direction": "Down"},
        ],
    )
    watch_rows = [
        {
            "source_wallet": WALLET,
            "action": "BUY",
            "market_slug": "btc-updown-5m-1000",
            "condition_id": "0xpass",
            "event_ts": 1030.0,
            "observed_ts": 1031.0,
            "outcome": "Up",
            "price": 0.4,
        },
        {
            "source_wallet": WALLET,
            "action": "BUY",
            "market_slug": "btc-updown-5m-1000",
            "condition_id": "0xpipe_late",
            "event_ts": 1020.0,
            "observed_ts": 1070.0,
            "outcome": "Up",
            "price": 0.4,
        },
    ]
    _write_json(watch_history, {"events": watch_rows})
    _write_json(
        watch_report,
        {
            "generated_at": "2026-07-15T00:00:00Z",
            "criteria": {
                "max_entry_offset_s": 60.0,
                "min_price": 0.25,
                "max_price": 0.5,
                "order_usd": 1.0,
            },
            "wallets": [{"source_wallet": WALLET, "eligible_signals": 2, "resolved_signals": 2, "pnl_usd": 0.5}],
        },
    )
    _write_json(hot_standby, {"status": "HOT_STANDBY_READY_PAPER_LANE", "summary": {"resolved_paper_fills": 2}})
    price_rows = [
        {
            "event_key": "pass",
            "source_wallet": WALLET,
            "market_slug": "btc-updown-5m-1000",
            "condition_id": "0xpass",
            "event_ts": 1030.0,
            "observed_ts": 1031.0,
            "outcome": "Up",
            "price": 0.5,
        },
        {
            "event_key": "source_late",
            "source_wallet": WALLET,
            "market_slug": "btc-updown-5m-1000",
            "condition_id": "0xsource_late",
            "event_ts": 1070.0,
            "observed_ts": 1071.0,
            "outcome": "Up",
            "price": 0.5,
        },
        {
            "event_key": "pipe_late",
            "source_wallet": WALLET,
            "market_slug": "btc-updown-5m-1000",
            "condition_id": "0xpipe_late",
            "event_ts": 1020.0,
            "observed_ts": 1070.0,
            "outcome": "Up",
            "price": 0.5,
        },
    ]
    _write_jsonl(price_events, price_rows)
    _write_json(
        price_state,
        {
            "generated_at": "2026-07-15T00:00:00Z",
            "filters": {"canary_size_usd": 2.0},
            "summary": {"candidate_events": 3, "resolved_n": 3, "hypothetical_pnl_usd": 0.0},
        },
    )

    report = build_report(
        wallet=WALLET,
        watch_tier_history_path=watch_history,
        watch_tier_report_path=watch_report,
        hot_standby_path=hot_standby,
        price_reject_state_path=price_state,
        price_reject_events_path=price_events,
        live_guard_state_path=live_guard,
        resolutions_path=resolutions,
    )

    price = report["price_reject_counterfactual"]["guard_replay"]
    assert price["full"]["resolved_n"] == 3
    assert price["guard_eligible"]["resolved_n"] == 1
    assert price["guard_late_class_counts"]["SOURCE_LATE"] == 1
    assert price["guard_late_class_counts"]["PIPELINE_LATE"] == 1
    assert price["guard_eligible"]["pnl_usd"] > 0

    watch = report["watch_tier_paper_lane"]["current_history_guard_replay"]
    assert watch["full"]["events"] == 2
    assert watch["guard_eligible"]["events"] == 1
    assert report["same_gate_verdict"]["watch_tier_paper_lane"]["same_as_live_late_gates"] is False
