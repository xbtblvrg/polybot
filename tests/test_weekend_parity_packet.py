import json
from pathlib import Path

from scripts import report_weekend_parity_packet as weekend_packet


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_current_roster_weekend_plan_uses_top_level_guard_members(tmp_path, monkeypatch) -> None:
    data = tmp_path / "data" / "research"
    wallet = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
    monkeypatch.setattr(weekend_packet, "DATA", data)

    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "members": [
                {
                    "candidate_id": "runtime_auto_degrade_f418d3a1a9",
                    "source_wallet": wallet,
                    "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "rolling_loss_trigger_usd": -4.0,
                    "wallet_fraction": 0.1,
                    "max_order_usd": 0.5,
                }
            ]
        },
    )
    _write_json(
        data / "wallet_copy_daily_scorecard_2026-07-17.json",
        {
            "today_actual_basis": {
                "per_member": {
                    wallet: {
                        "orders": 146,
                        "fills": 54,
                        "pnl_usd": 42.293924,
                        "roi_pct": 48.604226,
                    }
                }
            }
        },
    )
    _write_json(
        data / "member_dow_profiles_latest.json",
        {
            "profiles_by_wallet": {
                wallet: {
                    "weekend_evidence_status": "NO_WEEKEND_SAMPLE",
                    "weekend_trade_count": 0,
                    "weekend_activity_weight_vs_weekday": None,
                }
            }
        },
    )

    plan = weekend_packet.current_roster_weekend_plan("2026-07-17")

    assert plan["posture_counts"] == {"TRADE_FLOOR_SIZE": 1}
    assert plan["members"][0]["weekend_posture"] == "TRADE_FLOOR_SIZE"
    assert plan["members"][0]["first_slice_loss_line_usd"] == -4.0
    assert plan["weekend_loss_ladder"]["day_probe_trigger_usd"] == -8.0
    assert plan["weekend_starts_at"] == "2026-07-18T00:00:00Z"
    assert plan["weekend_ends_at"] == "2026-07-20T00:00:00Z"


def test_calendar_weekend_bounds_cover_friday_saturday_sunday_and_monday() -> None:
    expected = ("2026-07-25T00:00:00Z", "2026-07-27T00:00:00Z")
    assert weekend_packet.weekend_bounds("2026-07-24") == expected
    assert weekend_packet.weekend_bounds("2026-07-25") == expected
    assert weekend_packet.weekend_bounds("2026-07-26") == expected
    assert weekend_packet.weekend_bounds("2026-07-27") == expected
