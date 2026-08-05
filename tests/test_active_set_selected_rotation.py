from __future__ import annotations

import pytest

from scripts.execute_active_set_selected_rotation import execute_rotation


F418 = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
OTHER = "0x32de91fa203321fa7735e7854f2b1c844e71ce9d"


def _packet(*, fires_now: bool = True) -> dict:
    return {
        "presumptive_target": F418,
        "quiet_clock": {
            "fires_now": fires_now,
            "anchor_iso": "2026-07-17T23:55:03.914090Z",
            "earliest_fire_iso": "2026-07-18T03:55:03.914090Z",
        },
        "ranked_candidates": [
            {
                "source_wallet": F418,
                "candidate_id": "runtime_auto_degrade_f418d3a1a9",
                "fresh_matching_events_4h": 944,
                "fresh_matching_event_rate_per_hour": 236.0,
                "local_entry_latency_p50_s": 0.504422,
                "routing_shadow_post_fee_pnl_usd": -59.807174,
            }
        ],
    }


def _overlay() -> dict:
    return {
        "selection_pin": {"enabled": True, "source_wallet": OTHER, "pin_id": "old"},
        "members": [
            {
                "candidate_id": "runtime_auto_degrade_f418d3a1a9",
                "source_wallet": F418,
                "policy_id": "fast_wf",
                "enabled": True,
            }
        ],
    }


def test_execute_rotation_writes_f418_selection_pin_and_edge_snapshot() -> None:
    overlay, report = execute_rotation(
        packet=_packet(),
        overlay=_overlay(),
        now_iso="2026-07-18T06:05:00Z",
    )

    assert overlay["selection_pin"]["source_wallet"] == F418
    assert overlay["selection_pin"]["candidate_id"] == "runtime_auto_degrade_f418d3a1a9"
    assert overlay["selection_pin"]["enabled"] is True
    assert overlay["selection_pin"]["expires_at"] == "2026-07-18T07:00:00Z"
    assert report["status"] == "SELECTED_ROTATION_PIN_WRITTEN"
    assert report["edge_snapshot"]["fresh_matching_events_4h"] == 944
    assert report["edge_snapshot"]["routing_shadow_post_fee_pnl_usd"] == -59.807174
    assert report["live_path_mutated"] is False


def test_execute_rotation_refuses_open_clock() -> None:
    with pytest.raises(ValueError, match="fires_now"):
        execute_rotation(packet=_packet(fires_now=False), overlay=_overlay())
