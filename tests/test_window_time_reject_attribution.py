import json
from datetime import datetime, timezone
from pathlib import Path

from scripts.report_window_time_reject_attribution import build_report


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload))


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def test_window_time_reject_attribution_flags_fixable_pipeline_lag(tmp_path: Path) -> None:
    guard = tmp_path / "guard.json"
    live = tmp_path / "live.json"
    events = tmp_path / "events.jsonl"
    _write_json(
        guard,
        {
            "live_execution_runtime": {
                "argv": [
                    "python",
                    "scripts/run_wallet_copy_live_execution.py",
                    "--profit-latency-window-time-suppress-gte-s",
                    "60.0",
                    "--profit-latency-signal-age-suppress-gte-s",
                    "60.0",
                ]
            },
            "window_participation": {
                "rows": [
                    {
                        "intent_id": "ci_a",
                        "dominant_skip_reason": "window_time_gte_180s",
                        "market_slug": "btc-updown-5m-1000",
                        "window_start_s": 1000.0,
                        "source_wallet": "0xabc",
                        "outcome": "Down",
                        "latest_observed_ts": 1190.0,
                        "first_seen_at": "1970-01-01T00:19:55+00:00",
                        "source_latest_observed_age_s": 155.0,
                    }
                ]
            }
        },
    )
    _write_json(
        live,
        {
            "summary": {"latest_order_ts": "1970-01-01T00:16:00+00:00"},
            "orders": [
                {
                    "status": "FILLED",
                    "market_slug": "btc-updown-5m-1000",
                    "updated_at": f"{datetime.now(timezone.utc).date().isoformat()}T00:16:10+00:00",
                },
                {
                    "status": "FILLED",
                    "market_slug": "btc-updown-5m-1000",
                    "updated_at": f"{datetime.now(timezone.utc).date().isoformat()}T00:16:20+00:00",
                },
            ],
        },
    )
    _write_jsonl(
        events,
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "intent_id": "ci_a",
                "source_wallet": "0xabc",
                "market_slug": "btc-updown-5m-1000",
                "outcome": "Down",
                "event_ts": 1040.0,
                "dataapi_first_seen_ts": 1190.0,
                "taxonomy": "window_time_gte_180s",
                "taxonomy_tags": ["window_time_gte_180s", "signal_age_gte_60s"],
                "reject_reason": "window_time_gte_180s+signal_age_gte_60s",
                "window_time_suppress_gte_s": 60.0,
                "signal_age_suppress_gte_s": 60.0,
            }
        ],
    )

    report = build_report(guard_state_path=guard, live_ledger_state_path=live, event_log_path=events)

    assert report["current_guard_summary"]["ruling_input"] == "FIXABLE_PIPELINE_LAG"
    assert report["current_guard_rows"][0]["source_lateness_s"] == 40.0
    assert report["current_guard_rows"][0]["detection_latency_s"] == 150.0
    assert report["current_guard_rows"][0]["window_time_suppress_gte_s"] == 60.0
    assert report["flow_money_reconciliation"]["classification"] == "BENIGN_MULTIPLE_FILLS_PER_WINDOW"


def test_window_time_reject_attribution_flags_source_lateness(tmp_path: Path) -> None:
    guard = tmp_path / "guard.json"
    live = tmp_path / "live.json"
    events = tmp_path / "events.jsonl"
    _write_json(
        guard,
        {
            "live_execution_runtime": {
                "argv": [
                    "python",
                    "scripts/run_wallet_copy_live_execution.py",
                    "--profit-latency-window-time-suppress-gte-s",
                    "60.0",
                    "--profit-latency-signal-age-suppress-gte-s",
                    "60.0",
                ]
            },
            "window_participation": {
                "rows": [
                    {
                        "intent_id": "ci_late",
                        "dominant_skip_reason": "window_time_gte_180s",
                        "market_slug": "btc-updown-5m-2000",
                        "window_start_s": 2000.0,
                        "source_wallet": "0xdef",
                        "outcome": "Up",
                        "latest_observed_ts": 2225.0,
                    }
                ]
            }
        },
    )
    _write_json(live, {"summary": {"latest_order_ts": "1970-01-01T00:30:00+00:00"}, "orders": []})
    _write_jsonl(
        events,
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "intent_id": "ci_late",
                "source_wallet": "0xdef",
                "market_slug": "btc-updown-5m-2000",
                "outcome": "Up",
                "event_ts": 2200.0,
                "dataapi_first_seen_ts": 2225.0,
                "taxonomy_tags": ["window_time_gte_180s"],
            }
        ],
    )

    report = build_report(guard_state_path=guard, live_ledger_state_path=live, event_log_path=events)

    assert report["current_guard_summary"]["ruling_input"] == "SOURCE_LATENESS_DOMINATES"
    assert report["current_guard_rows"][0]["source_traded_after_180s"] is True
    assert report["current_guard_rows"][0]["detection_latency_gt_60s"] is False
    assert report["current_guard_rows"][0]["window_time_suppress_gte_s"] == 60.0
    assert report["current_guard_rows"][0]["threshold_source"] == "guard_runtime"


def test_window_time_reject_attribution_flags_rtds_window_time_tripwire(tmp_path: Path) -> None:
    guard = tmp_path / "guard.json"
    live = tmp_path / "live.json"
    events = tmp_path / "events.jsonl"
    _write_json(guard, {"window_participation": {"rows": []}})
    _write_json(live, {"summary": {"latest_order_ts": "1970-01-01T00:45:00+00:00"}, "orders": []})
    _write_jsonl(
        events,
        [
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "intent_id": "ci_rtds_late_gate",
                "source_wallet": "0xdef",
                "market_slug": "btc-updown-5m-2000",
                "outcome": "Down",
                "event_ts": 2141.0,
                "detection_observed_ts": 2141.2,
                "detection_source": "rtds_activity",
                "observation_sources": ["rtds_activity"],
                "taxonomy_tags": ["window_time_gte_180s"],
                "reject_reason": "window_time_gte_180s",
                "ts": "1970-01-01T00:39:13+00:00",
                "window_time_s": 253.0,
                "window_time_suppress_gte_s": 60.0,
            }
        ],
    )

    report = build_report(
        guard_state_path=guard,
        live_ledger_state_path=live,
        event_log_path=events,
        tripwire_start_iso="1970-01-01T00:30:00+00:00",
    )

    assert report["post_latest_order_event_summary"]["rows"] == 0
    summary = report["post_tripwire_start_event_summary"]
    assert summary["median_detection_latency_s"] == 0.2
    assert summary["rtds_window_time_tripwire_status"] == "TRIPWIRE_OPEN"
    assert summary["rtds_window_time_tripwire_rows"] == 1
    assert report["post_tripwire_start_event_rows"][0]["gate_eval_delay_after_detection_s"] == 211.8
    assert report["post_tripwire_start_event_rows"][0]["window_time_suppress_gte_s"] == 60.0
