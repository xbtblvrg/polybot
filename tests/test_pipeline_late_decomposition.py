import json
from pathlib import Path

import scripts.report_pipeline_late_decomposition as report_pipeline


WALLET = "0xa6896d11f76dfa2820662c1f441496f51553559b"


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_events(path: Path, events: list[dict]) -> Path:
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")
    return path


def _pipeline_row(window_start_s: float, **overrides):
    row = {
        "source_wallet": WALLET,
        "market_slug": f"btc-updown-5m-{int(window_start_s)}",
        "outcome": "Down",
        "window_start_s": window_start_s,
        "latest_observed_ts": window_start_s + 20,
        "source_detection_observed_ts": window_start_s + 20,
        "effective_latest_observed_ts": window_start_s + 240,
        "dominant_skip_reason": "window_time_gte_180s",
        "participation_skip_category": "PROTECTED_SKIP",
        "wallet_eligible_orders": 1,
        "our_submits": 0,
        "our_fills": 0,
        "first_seen_at": "1970-01-01T00:04:00+00:00",
        "last_seen_at": "1970-01-01T00:04:10+00:00",
    }
    row.update(overrides)
    return row


def _build(tmp_path: Path, rows: list[dict], events: list[dict]):
    guard_state = _write_json(tmp_path / "guard.json", {"window_participation": {"rows": rows}})
    guard_events = _write_events(tmp_path / "events.jsonl", events)
    tripwire = _write_json(
        tmp_path / "tripwire.json",
        {"verdict": "REAL_DEFECT_NOTIFY_NO_SELF_REMEDIATION", "pipeline_late": {"rows": len(rows)}},
    )
    return report_pipeline.build_report(
        guard_state_path=guard_state,
        guard_events_path=guard_events,
        wallet=WALLET,
        postfix_start_iso="1970-01-01T00:00:00Z",
        tripwire_path=tripwire,
    )


def test_pipeline_latency_confirmed_when_first_touch_late_majority(tmp_path: Path):
    rows = [
        _pipeline_row(1000.0, market_slug="btc-updown-5m-1000", first_seen_at="1970-01-01T00:20:40+00:00"),
        _pipeline_row(2000.0, market_slug="btc-updown-5m-2000", first_seen_at="1970-01-01T00:37:00+00:00"),
    ]
    events = [
        {
            "generated_at": "1970-01-01T00:20:40+00:00",
            "cycle": 1,
            "guard_loop_profile": {"total_s_before_state_write": 12.0},
            "window_participation": {
                "recent_window_rollups": [
                    {
                        "source_wallet": WALLET,
                        "market_slug": "btc-updown-5m-1000",
                        "window_start_s": 1000.0,
                        "dominant_skip_reason": "window_time_gte_180s",
                    }
                ]
            },
        },
        {
            "generated_at": "1970-01-01T00:37:00+00:00",
            "cycle": 2,
            "guard_loop_profile": {"total_s_before_state_write": 14.0},
            "window_participation": {
                "recent_window_rollups": [
                    {
                        "source_wallet": WALLET,
                        "market_slug": "btc-updown-5m-2000",
                        "window_start_s": 2000.0,
                        "dominant_skip_reason": "window_time_gte_180s",
                    }
                ]
            },
        },
    ]

    report = _build(tmp_path, rows, events)

    assert report["verdict"] == "PIPELINE_LATENCY_CONFIRMED"
    assert report["first_evaluation_late_gte_180s"]["windows"] == 2
    assert report["guard_cycle_wall_time_estimate"]["cycle_duration_s"]["p50"] == 13.0


def test_taxonomy_artifact_when_first_touch_timely_but_late_label_later(tmp_path: Path):
    rows = [
        _pipeline_row(1000.0, market_slug="btc-updown-5m-1000", first_seen_at="1970-01-01T00:17:20+00:00"),
    ]
    events = [
        {
            "generated_at": "1970-01-01T00:17:20+00:00",
            "cycle": 1,
            "guard_loop_profile": {"total_s_before_state_write": 7.0},
            "window_participation": {
                "recent_window_rollups": [
                    {
                        "source_wallet": WALLET,
                        "market_slug": "btc-updown-5m-1000",
                        "window_start_s": 1000.0,
                        "dominant_skip_reason": "inventory_best_ask_above_vwap_plus_buffer",
                    }
                ]
            },
        },
        {
            "generated_at": "1970-01-01T00:20:00+00:00",
            "cycle": 2,
            "guard_loop_profile": {"total_s_before_state_write": 8.0},
            "window_participation": {
                "recent_window_rollups": [
                    {
                        "source_wallet": WALLET,
                        "market_slug": "btc-updown-5m-1000",
                        "window_start_s": 1000.0,
                        "dominant_skip_reason": "window_time_gte_180s",
                    }
                ]
            },
        },
    ]

    report = _build(tmp_path, rows, events)

    assert report["verdict"] == "TAXONOMY_ARTIFACT_FIRST_TOUCH_TIMELY"
    assert report["window_records"][0]["first_evaluation_delta_s"] == 40.0
    assert report["window_records"][0]["first_late_evaluation_delta_s"] == 200.0
    assert report["window_records"][0]["timely_first_eval_with_later_late_skip"] is True
