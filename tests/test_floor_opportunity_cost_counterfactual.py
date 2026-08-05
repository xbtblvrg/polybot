import json
from pathlib import Path

from scripts import report_floor_opportunity_cost_counterfactual as report


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _resolution(idx: int, *, direction: str = "UP") -> dict:
    start = 1783950000 + idx * 300
    return {
        "condition_id": f"0x{idx:064x}",
        "yes_token": f"token-{idx}",
        "direction": direction,
        "expiry_unix_ts": start + 300,
        "window_type": "5m",
        "source": "test",
    }


def _floor_row(
    idx: int,
    *,
    source_wallet: str = "0x3048d65321be3497164cdfc2996f94f98a2e7537",
    outcome: str = "Up",
    price: float = 0.5,
    wallet_eligible_orders: int = 3,
) -> dict:
    start = 1783950000 + idx * 300
    return {
        "source_wallet": source_wallet,
        "market_slug": f"btc-updown-5m-{start}",
        "condition_id": f"0x{idx:064x}",
        "token_id": f"token-{idx}",
        "outcome": outcome,
        "window_start_s": float(start),
        "latest_observed_ts": float(start + 10),
        "first_seen_at": "2026-07-15T12:01:00+00:00",
        "last_seen_at": "2026-07-15T12:03:00+00:00",
        "source_inventory_vwap": price,
        "guard_sized_copy_usd": 1.0,
        "target_usd_at_vwap": 1.0,
        "target_shares": round(1.0 / price, 6),
        "wallet_eligible_orders": wallet_eligible_orders,
        "dominant_skip_reason": "drip_min_tranche_exceeds_window_budget",
        "floor_blocked_miss": True,
        "participation_skip_category": "FLOOR_BLOCKED_MISS",
    }


def _selector_abstain_row(idx: int) -> dict:
    row = _floor_row(idx)
    row.update(
        {
            "dominant_skip_reason": "filtered_after_inventory_build",
            "floor_blocked_miss": False,
            "participation_skip_category": "MEASURED_SKIP_WINDOW",
        }
    )
    return row


def _freeze_payload() -> dict:
    return {
        "generated_at": "2026-07-15T13:00:00Z",
        "summary": {
            "quiet_stretch_start": "2026-07-13T00:00:00Z",
            "floor_wallet_window_rows": 1,
            "floor_wallet_eligible_orders_sum": 3,
            "floor_distinct_windows": 1,
            "floor_wallet_counts": {"0x3048d65321be3497164cdfc2996f94f98a2e7537": 1},
        },
    }


def test_floor_counterfactual_filters_floor_rows_without_selector_double_count(tmp_path: Path):
    guard_state = tmp_path / "guard.json"
    freeze = tmp_path / "freeze.json"
    guard_events = tmp_path / "guard_events.jsonl"
    resolutions = tmp_path / "resolutions.jsonl"
    state = tmp_path / "state.json"
    event_log = tmp_path / "events.jsonl"
    _write_json(
        guard_state,
        {
            "window_participation": {
                "rows": [
                    _floor_row(1),
                    _selector_abstain_row(1),
                    {**_floor_row(2), "floor_blocked_miss": False},
                ]
            }
        },
    )
    _write_json(freeze, _freeze_payload())
    guard_events.write_text("", encoding="utf-8")
    _write_jsonl(resolutions, [_resolution(1), _resolution(2)])

    payload = report.build_report(
        guard_state_path=guard_state,
        freeze_harvest_path=freeze,
        guard_events_path=guard_events,
        resolutions_path=resolutions,
        state_path=state,
        event_log=event_log,
        min_resolved=1,
    )

    assert payload["source_snapshot_candidate_events"] == 1
    assert payload["summary"]["candidate_events"] == 1
    assert payload["summary"]["wallet_eligible_orders"] == 3
    assert payload["summary"]["resolved_n"] == 1
    assert payload["summary"]["positive_gate"] is True
    assert payload["population_filter"]["exclude_selector_abstain_rows"] is True
    assert len(event_log.read_text(encoding="utf-8").splitlines()) == 1


def test_floor_counterfactual_event_log_universe_keeps_resolved_monotonic_when_source_shrinks(
    tmp_path: Path,
):
    guard_state = tmp_path / "guard.json"
    freeze = tmp_path / "freeze.json"
    guard_events = tmp_path / "guard_events.jsonl"
    resolutions = tmp_path / "resolutions.jsonl"
    state = tmp_path / "state.json"
    event_log = tmp_path / "events.jsonl"
    _write_json(freeze, _freeze_payload())
    guard_events.write_text("", encoding="utf-8")
    _write_jsonl(resolutions, [_resolution(1), _resolution(2)])
    _write_json(
        guard_state,
        {"window_participation": {"rows": [_floor_row(1), _floor_row(2)]}},
    )

    first = report.build_report(
        guard_state_path=guard_state,
        freeze_harvest_path=freeze,
        guard_events_path=guard_events,
        resolutions_path=resolutions,
        state_path=state,
        event_log=event_log,
        min_resolved=1,
    )
    _write_json(guard_state, {"window_participation": {"rows": [_floor_row(1)]}})
    second = report.build_report(
        guard_state_path=guard_state,
        freeze_harvest_path=freeze,
        guard_events_path=guard_events,
        resolutions_path=resolutions,
        state_path=state,
        event_log=event_log,
        min_resolved=1,
    )

    assert first["summary"]["resolved_n"] == 2
    assert second["source_snapshot_candidate_events"] == 1
    assert second["summary"]["candidate_events"] == 2
    assert second["summary"]["resolved_n"] == 2
    assert second["prev_resolved_n"] == 2
    assert second["resolved_n_delta"] == 0
