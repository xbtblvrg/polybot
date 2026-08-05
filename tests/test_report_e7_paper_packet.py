from __future__ import annotations

import json
from datetime import UTC, datetime

from scripts.report_e7_paper_packet import build_report


def _state(
    *,
    windows: int,
    resolved_fills: int,
    wins: int,
    pnl: float,
    size_mode: str = "equal_dollars",
    topology_counts: dict[str, int] | None = None,
) -> dict:
    quotes = []
    orders = []
    for index in range(windows):
        window_start = 1783340000 + index * 300
        quotes.append(
            {
                "quote_id": f"q-up-{index}",
                "quote_status": "QUOTED",
                "outcome": "Up",
                "window_start_s": window_start,
                "generated_at": "2026-07-06T13:00:00Z",
            }
        )
        quotes.append(
            {
                "quote_id": f"q-down-{index}",
                "quote_status": "QUOTED",
                "outcome": "Down",
                "window_start_s": window_start,
                "generated_at": "2026-07-06T13:00:00Z",
            }
        )
        orders.append({"order_id": f"o-{index}", "source_intent": {"mode": "paper", "live_orders_allowed": False}})
    topology_counts = topology_counts if topology_counts is not None else {"both_sides_filled": windows}
    return {
        "lane": "e7_test",
        "updated_at": "2026-07-06T13:30:00Z",
        "paper_only": True,
        "live_orders_allowed": False,
        "zero_live_assertion": {"status": "PASS", "orders_submitted": 0},
        "parameters": {"paper_quote_offset_s": 30.0, "paper_size_mode": size_mode},
        "paper_quote_events": quotes,
        "orders": orders,
        "summary": {
            "paper_orders": len(orders),
            "paper_filled_orders": resolved_fills,
            "book_verified_fills": resolved_fills,
            "resolved_paper_fills": resolved_fills,
            "resolved_paper_wins": wins,
            "resolved_paper_losses": max(resolved_fills - wins, 0),
            "unresolved_paper_fills": 0,
            "resolved_paper_pnl_usd": pnl,
            "resolved_paper_wr_pct": (wins / resolved_fills) * 100 if resolved_fills else 0.0,
            "paper_quote_fill_topology": {
                "windows": windows,
                "topology_counts": topology_counts,
                "resolved_pnl_by_topology": {next(iter(topology_counts or {"none_filled": 0})): pnl},
            },
        },
    }


def test_e7_packet_marks_positive_24_window_packet_live_ready(tmp_path) -> None:
    e7_0 = tmp_path / "e7_0.json"
    e7_1 = tmp_path / "e7_1.json"
    e7_0.write_text(json.dumps(_state(windows=24, resolved_fills=12, wins=7, pnl=3.25)))
    e7_1.write_text(json.dumps(_state(windows=2, resolved_fills=0, wins=0, pnl=0.0, size_mode="equal_shares")))

    report = build_report(
        e7_0_state=e7_0,
        e7_1_state=e7_1,
        e7_0_stop_at="2026-07-06T18:00:00Z",
        now=datetime(2026, 7, 6, 15, 0, tzinfo=UTC),
    )

    e7 = report["variants"][0]
    assert e7["packet_clock"]["unique_quoted_windows"] == 24
    assert e7["packet_clock"]["packet_close_reasons"] == ["quoted_windows_gte_24"]
    assert e7["gate_status"] == "PASS_LIVE_READY_BY_PRECOMMITTED_RULE"
    assert report["mechanical_next_action"] == "start_E7.0_live_under_single_guard_per_precommitted_rule"


def test_e7_packet_tripwire_fails_and_continues_e7_1(tmp_path) -> None:
    e7_0 = tmp_path / "e7_0.json"
    e7_1 = tmp_path / "e7_1.json"
    e7_0.write_text(json.dumps(_state(windows=9, resolved_fills=10, wins=0, pnl=-80.0)))
    e7_1.write_text(json.dumps(_state(windows=3, resolved_fills=0, wins=0, pnl=0.0, size_mode="equal_shares")))

    report = build_report(
        e7_0_state=e7_0,
        e7_1_state=e7_1,
        e7_0_stop_at="2026-07-06T18:00:00Z",
        now=datetime(2026, 7, 6, 15, 0, tzinfo=UTC),
    )

    e7 = report["variants"][0]
    assert e7["packet_clock"]["packet_closed"] is True
    assert e7["packet_clock"]["packet_close_reasons"] == ["zero_wins_10_fill_tripwire"]
    assert e7["gate_status"] == "FAILS_PRECOMMITTED_RULE"
    assert report["mechanical_next_action"] == "keep_E7.0_paper_dead_continue_E7.1_measurement"


def test_e7_1_stop_time_is_five_hours_after_first_quote(tmp_path) -> None:
    e7_0 = tmp_path / "e7_0.json"
    e7_1 = tmp_path / "e7_1.json"
    e7_0.write_text(json.dumps(_state(windows=1, resolved_fills=0, wins=0, pnl=0.0)))
    e7_1.write_text(json.dumps(_state(windows=1, resolved_fills=0, wins=0, pnl=0.0, size_mode="equal_shares")))

    report = build_report(
        e7_0_state=e7_0,
        e7_1_state=e7_1,
        e7_0_stop_at="2026-07-06T18:00:00Z",
        now=datetime(2026, 7, 6, 17, 59, tzinfo=UTC),
    )

    e7_1_variant = report["variants"][1]
    assert e7_1_variant["packet_clock"]["first_quote_at"] == "2026-07-06T13:00:00Z"
    assert e7_1_variant["packet_clock"]["stop_at"] == "2026-07-06T18:00:00Z"
    assert e7_1_variant["gate_status"] == "COLLECTING"


def test_e7_1_positive_packet_requires_three_both_side_fills(tmp_path) -> None:
    e7_0 = tmp_path / "e7_0.json"
    e7_1 = tmp_path / "e7_1.json"
    e7_0.write_text(json.dumps(_state(windows=24, resolved_fills=12, wins=2, pnl=-12.0)))
    e7_1.write_text(
        json.dumps(
            _state(
                windows=24,
                resolved_fills=12,
                wins=8,
                pnl=4.5,
                size_mode="equal_shares",
                topology_counts={"one_sided_filled": 24},
            )
        )
    )

    report = build_report(
        e7_0_state=e7_0,
        e7_1_state=e7_1,
        e7_0_stop_at="2026-07-06T18:00:00Z",
        now=datetime(2026, 7, 6, 18, 45, tzinfo=UTC),
    )

    e7_1_variant = report["variants"][1]
    assert e7_1_variant["summary"]["both_sides_filled_windows"] == 0
    assert e7_1_variant["summary"]["one_sided_filled_windows"] == 24
    assert e7_1_variant["precommitted_live_criteria"]["both_sides_filled_gte_3"] is False
    assert e7_1_variant["gate_status"] == "FAILS_PRECOMMITTED_RULE"
    assert report["mechanical_next_action"] == "park_E7_after_two_negative_packets"
