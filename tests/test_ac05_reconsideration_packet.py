import json
from argparse import Namespace
from pathlib import Path

from scripts import report_ac05_reconsideration_packet as ac05_packet


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload))


def test_ac05_packet_stats_prefer_summary_rollup_when_rows_absent(tmp_path: Path) -> None:
    summary = {
        "fee_gated_intents": 6,
        "measurable_resolved_intents": 5,
        "resolved_intents": 6,
        "unmeasured_resolved_intents": 1,
        "unresolved_intents": 0,
        "wins": 5,
        "losses": 0,
        "pre_fee_pnl_usd": 12.981115,
        "expected_fee_usd_sum": 0.436312,
        "post_fee_pnl_usd": 12.544803,
        "unique_windows": 6,
        "resolved_unique_windows": 6,
    }
    latest_path = tmp_path / "routing_shadow_latest.json"
    pin_path = tmp_path / "routing_shadow_pin.json"
    _write_json(
        latest_path,
        {
            "summary": {
                "extra_would_submit_post_fee_measurement": {
                    "by_member": {ac05_packet.AC05_WALLET: summary}
                }
            },
            "fee_gated_measurement_rows": [],
        },
    )
    _write_json(
        pin_path,
        {
            "summary": {"fee_gate_calibration_retained": {"by_member": {}}},
            "fee_gated_measurement_rows": [],
        },
    )

    packet = ac05_packet.build_packet(
        Namespace(
            routing_shadow_latest=str(latest_path),
            routing_shadow_attribution_pin=str(pin_path),
        )
    )

    assert packet["current_latest"]["stats"]["measurable_resolved_intents"] == 5
    assert packet["current_latest"]["stats"]["post_fee_pnl_usd"] == 12.544803
    assert packet["current_latest"]["raw_row_stats"]["measurable_resolved_intents"] == 0
    assert packet["decision_basis"] == "current_latest_n_lt_10"
