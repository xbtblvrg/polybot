import pytest

from scripts.report_heartbeat_status_money import build_money_line


def test_heartbeat_money_line_reads_numeric_packet_fields() -> None:
    packet = {
        "pnl": {
            "day_pnl_usd": 0.0,
            "day_resolved_fills": 0,
            "since_topup_actual_delta_usd": -18.737923,
        }
    }

    assert build_money_line(packet) == (
        "day 0.000000, fills 0, since_topup actual -18.737923"
    )


def test_heartbeat_money_line_rejects_shell_text_in_day_field() -> None:
    packet = {
        "pnl": {
            "day_pnl_usd": "/bin/zsh",
            "day_resolved_fills": 0,
            "since_topup_actual_delta_usd": -18.737923,
        }
    }

    with pytest.raises(ValueError, match="pnl.day_pnl_usd must be numeric"):
        build_money_line(packet)
