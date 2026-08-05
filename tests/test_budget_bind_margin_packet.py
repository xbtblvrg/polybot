import json
from pathlib import Path

import scripts.report_budget_bind_margin_packet as report_budget


WALLET = "0x3048d65321be3497164cdfc2996f94f98a2e7537"


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: list[dict]) -> Path:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    return path


def _row(window_start: int, *, budget: float = 0.80, effective_min: float = 1.0, outcome: str = "Up") -> dict:
    return {
        "source_wallet": WALLET,
        "market_slug": f"btc-updown-5m-{window_start}",
        "condition_id": f"cond-{window_start}",
        "outcome": outcome,
        "window_start_s": float(window_start),
        "latest_observed_ts": float(window_start + 30),
        "dominant_skip_reason": "drip_min_tranche_exceeds_window_budget",
        "participation_skip_category": "FLOOR_BLOCKED_MISS",
        "wallet_eligible_orders": 1,
        "window_budget_usd": budget,
        "drip_min_tranche_usd": effective_min,
        "min_order_usd": 1.0,
        "source_inventory_vwap": 0.5,
        "target_usd_at_vwap": budget,
        "target_shares": budget / 0.5,
    }


def _resolution(window_start: int, direction: str) -> dict:
    return {
        "condition_id": f"cond-{window_start}",
        "expiry_unix_ts": window_start + 300,
        "window_type": "5m",
        "direction": direction,
        "source": "polymarket_gamma",
    }


def test_round_up_gate_passes_when_resolved_pnl_non_negative(tmp_path: Path):
    rows = [_row(1000 + i * 300, outcome="Up") for i in range(10)]
    guard_state = _write_json(tmp_path / "guard.json", {"window_participation": {"rows": rows}})
    resolutions = _write_jsonl(tmp_path / "resolutions.jsonl", [_resolution(1000 + i * 300, "UP") for i in range(10)])

    packet = report_budget.build_report(
        guard_state_path=guard_state,
        resolutions_path=resolutions,
        lookback_hours=1000000.0,
    )

    assert packet["verdict"] == "ROUND_UP_CLAMP_GATE_PASS_REQUIRES_FABLE_TUNE"
    assert packet["summary"]["bind_rows"] == 10
    assert packet["summary"]["round_up_eligible_rows"] == 10
    assert packet["summary"]["round_up_would_pnl_usd"] == 10.0


def test_round_up_skips_rows_below_margin_threshold(tmp_path: Path):
    rows = [_row(1000 + i * 300, budget=0.70, effective_min=1.0, outcome="Up") for i in range(10)]
    guard_state = _write_json(tmp_path / "guard.json", {"window_participation": {"rows": rows}})
    resolutions = _write_jsonl(tmp_path / "resolutions.jsonl", [_resolution(1000 + i * 300, "UP") for i in range(10)])

    packet = report_budget.build_report(
        guard_state_path=guard_state,
        resolutions_path=resolutions,
        lookback_hours=1000000.0,
    )

    assert packet["verdict"] == "KEEP_CURRENT_BEHAVIOR_DEMOTE_DRIP_MIN_ONE_LINER"
    assert packet["summary"]["bind_rows"] == 10
    assert packet["summary"]["round_up_eligible_rows"] == 0
    assert packet["summary"]["round_up_would_pnl_usd"] == 0.0
