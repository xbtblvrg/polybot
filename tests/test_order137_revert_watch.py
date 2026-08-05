"""ORDER137 first-6 accepted-orders mechanical revert (Fable 2026-08-01)."""

from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest

import scripts.order137_revert_watch as watch
import src.trade_executor as trade_executor


WALLET = watch.ORDER137_PINNED_WALLET


def _order(idx: int, *, pnl_cost: float = 1.0, status: str = "FILLED", price: float = 0.41):
    return {
        "order_id": f"lo_{idx:03d}",
        "status": status,
        "final_status": status,
        "source_wallet": WALLET,
        "wallet_name": WALLET,
        "submitted_at": f"2026-08-01T17:0{idx}:00+00:00",
        "market_slug": f"btc-updown-5m-17856{idx:05d}",
        "limit_price": price,
        "requested_shares": 5.0,
        "requested_size_usd": pnl_cost,
        "paper_only": False,
    }


def _write_state(tmp_path: Path, **extra) -> Path:
    path = tmp_path / "order137_min_share_cap_state.json"
    payload = {"activated_at": "2026-08-01T16:30:00Z", **extra}
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _run(tmp_path: Path, orders, *, scores, execute=False, state_path=None, **kwargs):
    def fake_score_order(order, resolutions):
        return scores.get(str(order.get("order_id")), {"resolved": False})

    original = watch.score_order
    watch.score_order = fake_score_order
    try:
        ledger = tmp_path / "ledger.json"
        ledger.write_text(json.dumps({"orders": orders}), encoding="utf-8")
        return watch.evaluate(
            state_path=state_path or _write_state(tmp_path),
            ledger_path=ledger,
            resolutions_path=tmp_path / "missing_resolutions.jsonl",
            event_log=tmp_path / "events.jsonl",
            live_change_journal=tmp_path / "journal.jsonl",
            execute=execute,
            now="2026-08-01T18:00:00Z",
            **kwargs,
        )
    finally:
        watch.score_order = original


def _loss(cost: float):
    return {"resolved": True, "status": "FILLED", "cost_usd": cost, "pnl_usd": -cost, "win": False}


def _win(pnl: float):
    return {"resolved": True, "status": "FILLED", "cost_usd": 1.0, "pnl_usd": pnl, "win": True}


def test_not_armed_without_activation_timestamp(tmp_path):
    state = tmp_path / "state.json"
    state.write_text(json.dumps({}), encoding="utf-8")
    result = _run(tmp_path, [], scores={}, state_path=state)
    assert result["status"] == "NOT_ARMED_NO_ACTIVATION_TIMESTAMP"
    assert result["revert_active"] is False


def test_watch_when_net_above_threshold(tmp_path):
    orders = [_order(i) for i in range(1, 7)]
    scores = {f"lo_{i:03d}": _loss(0.3) for i in range(1, 7)}
    result = _run(tmp_path, orders, scores=scores)
    assert result["status"] == "WOULD_KEEP"
    assert result["revert_active"] is False
    assert result["attribution"]["net_resolved_pnl_usd"] == pytest.approx(-1.8)


def test_boundary_minus_two_dollars_fires(tmp_path):
    orders = [_order(i) for i in range(1, 7)]
    scores = {f"lo_{i:03d}": _loss(1.0) for i in range(1, 3)}
    result = _run(tmp_path, orders, scores=scores, execute=True)
    assert result["status"] == "ORDER137_REVERTED"
    assert result["revert_active"] is True
    assert result["attribution"]["net_resolved_pnl_usd"] == pytest.approx(-2.0)


def test_just_above_boundary_does_not_fire(tmp_path):
    orders = [_order(i) for i in range(1, 7)]
    scores = {"lo_001": _loss(1.0), "lo_002": _loss(0.99)}
    result = _run(tmp_path, orders, scores=scores)
    assert result["revert_active"] is False
    assert result["status"] == "WATCH_WINDOW_FULL"


def test_execute_terminal_keep_is_idempotent(tmp_path):
    state = _write_state(tmp_path)
    orders = [_order(i) for i in range(1, 7)]
    scores = {f"lo_{i:03d}": _win(0.5) for i in range(1, 7)}
    first = _run(tmp_path, orders, scores=scores, execute=True, state_path=state)
    assert first["status"] == "ORDER137_KEPT"
    assert first["terminal"] is True
    assert first["revert_active"] is False
    second = _run(tmp_path, orders, scores=scores, execute=True, state_path=state)
    assert second["status"] == "ALREADY_KEPT"
    assert second["verdict"] == "ORDER137_KEPT"


def test_later_orders_beyond_first_n_are_ignored(tmp_path):
    orders = [_order(i) for i in range(1, 9)]
    scores = {f"lo_{i:03d}": _loss(0.5) for i in range(1, 9)}
    result = _run(tmp_path, orders, scores=scores)
    # Only the first 6 count: 6 x -0.5 = -3.0, orders 7-8 never enter the window.
    assert result["attribution"]["accepted_orders"] == 6
    assert result["attribution"]["net_resolved_pnl_usd"] == pytest.approx(-3.0)
    assert result["status"] == "WOULD_REVERT"


def test_rejected_rows_are_not_accepted_attempts(tmp_path):
    orders = [
        {**_order(1, status="REJECTED"), "order_id": ""},
        _order(2),
    ]
    scores = {"lo_002": _loss(0.5)}
    result = _run(tmp_path, orders, scores=scores)
    assert result["attribution"]["accepted_orders"] == 1
    assert result["revert_active"] is False


def test_orders_before_activation_are_excluded(tmp_path):
    stale = _order(1)
    stale["submitted_at"] = "2026-08-01T15:00:00+00:00"
    scores = {"lo_001": _loss(5.0)}
    result = _run(tmp_path, [stale], scores=scores)
    assert result["attribution"]["accepted_orders"] == 0
    assert result["revert_active"] is False


def test_dry_run_does_not_write_state(tmp_path):
    state = _write_state(tmp_path)
    orders = [_order(i) for i in range(1, 7)]
    scores = {f"lo_{i:03d}": _loss(1.0) for i in range(1, 4)}
    result = _run(tmp_path, orders, scores=scores, execute=False, state_path=state)
    assert result["status"] == "WOULD_REVERT"
    assert result["executed"] is False
    assert json.loads(state.read_text(encoding="utf-8")).get("revert_active") is None


def test_execute_is_idempotent_and_terminal(tmp_path):
    state = _write_state(tmp_path)
    orders = [_order(i) for i in range(1, 7)]
    scores = {f"lo_{i:03d}": _loss(1.0) for i in range(1, 4)}
    first = _run(tmp_path, orders, scores=scores, execute=True, state_path=state)
    assert first["executed"] is True
    stored = json.loads(state.read_text(encoding="utf-8"))
    assert stored["revert_active"] is True
    assert stored["reverted_at"] == "2026-08-01T18:00:00Z"
    # A subsequent profitable run must not re-arm the widened cap.
    second = _run(tmp_path, orders, scores={f"lo_{i:03d}": _win(9.0) for i in range(1, 7)},
                  execute=True, state_path=state)
    assert second["status"] == "ALREADY_REVERTED"
    assert second["revert_active"] is True


def test_executor_gate_reads_state_file(tmp_path, monkeypatch):
    state = tmp_path / "order137_min_share_cap_state.json"
    monkeypatch.setattr(trade_executor, "ORDER137_REVERT_STATE_PATH", state)
    monkeypatch.setattr(trade_executor, "ORDER137_REVERT_CACHE_TTL_S", -1.0)
    trade_executor._ORDER137_REVERT_CACHE.update(
        {"checked_at": 0.0, "mtime_ns": None, "revert_active": False}
    )

    # Missing file -> armed (not reverted).
    assert trade_executor._order137_revert_active() is False

    state.write_text(json.dumps({"revert_active": True}), encoding="utf-8")
    assert trade_executor._order137_revert_active() is True

    state.write_text(json.dumps({"revert_active": False}), encoding="utf-8")
    assert trade_executor._order137_revert_active() is False

    # A torn/invalid write keeps the last good value instead of re-arming.
    state.write_text(json.dumps({"revert_active": True}), encoding="utf-8")
    assert trade_executor._order137_revert_active() is True
    state.write_text("{not json", encoding="utf-8")
    assert trade_executor._order137_revert_active() is True


def test_watch_module_imports_clean():
    assert importlib.reload(watch) is not None
