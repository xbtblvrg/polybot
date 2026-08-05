"""ORDER138 first-8 accepted-orders mechanical revert."""

from __future__ import annotations

import importlib
import json
import plistlib
from pathlib import Path

import pytest

import scripts.order138_revert_watch as watch


WALLET = watch.ORDER138_PINNED_WALLET


def _write_launcher(tmp_path: Path, fraction: str = "0.20") -> Path:
    path = tmp_path / "start_live_guard.sh"
    path.write_text(
        "#!/bin/sh\nexec python guard.py --max-intents 6 "
        f"--wallet-fraction {fraction} --max-order-usd 8.0 "
        "--drip-min-tranche-usd 1.0 --drip-max-tranche-usd 2.5 "
        "--per-window-fill-cap 1\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _order(idx: int, *, status: str = "FILLED", price: float = 0.41) -> dict:
    return {
        "order_id": f"o138_{idx:03d}",
        "status": status,
        "final_status": status,
        "source_wallet": WALLET,
        "wallet_name": WALLET,
        "submitted_at": f"2026-08-02T00:0{idx}:00+00:00",
        "market_slug": f"btc-updown-5m-17857{idx:05d}",
        "limit_price": price,
        "requested_shares": 5.0,
        "requested_size_usd": 1.0,
        "paper_only": False,
    }


def _write_state(tmp_path: Path, **extra) -> Path:
    path = tmp_path / "order138_wallet_fraction_state.json"
    payload = {"activated_at": "2026-08-02T00:00:00Z", **extra}
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_plist(tmp_path: Path, fraction: str = "0.20") -> Path:
    path = tmp_path / "live.plist"
    path.write_bytes(plistlib.dumps({"ProgramArguments": ["python", "guard.py", "--wallet-fraction", fraction]}))
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
            launcher_path=_write_launcher(tmp_path),
            launchd_plist_path=_write_plist(tmp_path),
            guard_state_path=tmp_path / "guard.json",
            execute=execute,
            now="2026-08-02T01:00:00Z",
            **kwargs,
        )
    finally:
        watch.score_order = original


def _loss(cost: float) -> dict:
    return {"resolved": True, "cost_usd": cost, "pnl_usd": -cost, "win": False}


def _win(pnl: float) -> dict:
    return {"resolved": True, "cost_usd": 1.0, "pnl_usd": pnl, "win": True}


def test_inactive_until_activation_timestamp(tmp_path):
    state = tmp_path / "state.json"
    state.write_text(json.dumps({}), encoding="utf-8")
    result = _run(tmp_path, [], scores={}, state_path=state)
    assert result["status"] == "STAGED_NOT_ACTIVE"
    assert result["revert_active"] is False


def test_watch_when_first_eight_net_above_threshold(tmp_path):
    orders = [_order(i) for i in range(1, 9)]
    scores = {f"o138_{i:03d}": _loss(0.49) for i in range(1, 9)}
    result = _run(tmp_path, orders, scores=scores)
    assert result["status"] == "WATCH_WINDOW_FULL"
    assert result["revert_active"] is False
    assert result["attribution"]["net_resolved_pnl_usd"] == pytest.approx(-3.92)


def test_boundary_minus_four_dollars_fires(tmp_path):
    orders = [_order(i) for i in range(1, 9)]
    scores = {f"o138_{i:03d}": _loss(1.0) for i in range(1, 5)}
    result = _run(tmp_path, orders, scores=scores, execute=True)
    assert result["status"] == "ORDER138_REVERT_PENDING_RELOAD"
    assert result["revert_active"] is False
    assert result["restore_wallet_fraction"] == pytest.approx(0.10)
    assert result["canonical_launcher_restore"]["status"] == "RESTORED"
    assert result["resident_reload_required"] is True
    assert "--wallet-fraction 0.10" in (tmp_path / "start_live_guard.sh").read_text()
    assert plistlib.loads((tmp_path / "live.plist").read_bytes())["ProgramArguments"][-1] == "0.10"
    assert result["attribution"]["net_resolved_pnl_usd"] == pytest.approx(-4.0)


def test_rejected_rows_are_excluded_from_first_eight(tmp_path):
    orders = [{**_order(1, status="REJECTED"), "order_id": ""}, _order(2)]
    result = _run(tmp_path, orders, scores={"o138_002": _loss(1.0)})
    assert result["attribution"]["accepted_orders"] == 1
    assert result["revert_active"] is False


def test_orders_before_activation_are_excluded(tmp_path):
    stale = _order(1)
    stale["submitted_at"] = "2026-08-01T23:59:00+00:00"
    result = _run(tmp_path, [stale], scores={"o138_001": _loss(5.0)})
    assert result["attribution"]["accepted_orders"] == 0
    assert result["status"] == "WATCH"


def test_accepted_order_from_current_selected_wallet_is_attributed(tmp_path):
    selected_wallet = "0x2d7c9298b64713de86402bd8a41695e31865a945"
    order = {**_order(1), "source_wallet": selected_wallet, "wallet_name": selected_wallet}
    result = _run(tmp_path, [order], scores={"o138_001": _loss(1.0)})
    assert result["pinned_wallet"] == WALLET
    assert result["attribution_scope"] == "resident_guard_activation_window_all_source_wallets"
    assert result["attribution"]["accepted_orders"] == 1
    assert result["attribution"]["rows"][0]["source_wallet"] == selected_wallet


def test_dry_run_does_not_write_state(tmp_path):
    state = _write_state(tmp_path)
    orders = [_order(i) for i in range(1, 9)]
    scores = {f"o138_{i:03d}": _loss(1.0) for i in range(1, 5)}
    result = _run(tmp_path, orders, scores=scores, execute=False, state_path=state)
    assert result["status"] == "WOULD_REVERT"
    assert result["executed"] is False
    assert json.loads(state.read_text(encoding="utf-8")).get("revert_active") is None


def test_execute_is_terminal(tmp_path):
    state = _write_state(tmp_path)
    orders = [_order(i) for i in range(1, 9)]
    scores = {f"o138_{i:03d}": _loss(1.0) for i in range(1, 5)}
    first = _run(tmp_path, orders, scores=scores, execute=True, state_path=state)
    assert first["executed"] is False
    stored = json.loads(state.read_text(encoding="utf-8"))
    assert stored["revert_active"] is False
    assert stored["revert_pending"]


def test_pending_revert_only_executes_after_new_resident_is_point_one(tmp_path, monkeypatch):
    state = _write_state(
        tmp_path,
        revert_pending={"requested_at": "2026-08-02T01:00:00Z", "resident_pid_before": 10},
    )
    monkeypatch.setattr(
        watch,
        "_resident_wallet_fraction",
        lambda path: {"status": "PASS", "pid": 11, "wallet_fraction": 0.10},
    )
    result = _run(tmp_path, [], scores={}, execute=True, state_path=state)
    assert result["status"] == "ORDER138_REVERTED"
    assert result["revert_active"] is True
    assert result["executed"] is True


def test_pending_revert_stamps_did_not_land_on_new_point_two_resident(tmp_path, monkeypatch):
    state = _write_state(
        tmp_path,
        revert_pending={"requested_at": "2026-08-02T01:00:00Z", "resident_pid_before": 10},
    )
    monkeypatch.setattr(
        watch,
        "_resident_wallet_fraction",
        lambda path: {"status": "PASS", "pid": 11, "wallet_fraction": 0.20},
    )
    result = _run(tmp_path, [], scores={}, execute=True, state_path=state)
    assert result["status"] == "ORDER138_REVERT_DID_NOT_LAND"
    assert result["revert_active"] is False
    assert result["executed"] is False
    assert result["requires_fable_ping"] is True


def test_pending_revert_ambiguous_resident_fails_open(tmp_path, monkeypatch):
    state = _write_state(
        tmp_path,
        revert_pending={"requested_at": "2026-08-02T01:00:00Z", "resident_pid_before": 10},
    )
    monkeypatch.setattr(
        watch,
        "_resident_wallet_fraction",
        lambda path: {"status": "AMBIGUOUS_FAIL_OPEN", "pids": []},
    )
    result = _run(tmp_path, [], scores={}, execute=True, state_path=state)
    assert result["status"] == "ORDER138_REVERT_PENDING_RELOAD"
    assert result["executed"] is False


def test_execute_auto_arms_from_resident_reload_evidence(tmp_path, monkeypatch):
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"activated_at": None}), encoding="utf-8")
    monkeypatch.setattr(
        watch,
        "_resident_activation",
        lambda path: {
            "activated_at": "2026-08-02T00:01:00Z",
            "authorized_activation": True,
            "pid": 123,
            "guard_code_identity": "PASS",
            "resident_wallet_fraction": 0.2,
        },
    )
    result = _run(tmp_path, [], scores={}, execute=True, state_path=state)
    stored = json.loads(state.read_text())
    assert result["status"] == "WATCH"
    assert stored["activated_at"] == "2026-08-02T00:01:00Z"
    assert stored["authorized_activation"] is True


def test_execute_auto_arms_early_resident_activation_loudly(tmp_path, monkeypatch):
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"activated_at": None}), encoding="utf-8")
    monkeypatch.setattr(
        watch,
        "_resident_activation",
        lambda path: {
            "activated_at": "2026-08-01T21:00:00Z",
            "authorized_activation": False,
            "pid": 123,
            "guard_code_identity": "PASS",
            "resident_wallet_fraction": 0.2,
        },
    )
    result = _run(tmp_path, [], scores={}, execute=True, state_path=state)
    stored = json.loads(state.read_text())
    assert result["status"] == "WATCH"
    assert stored["status"] == "ARMED_EARLY_UNAUTHORIZED_ACTIVATION"
    assert stored["authorized_activation"] is False


def test_active_watch_reanchors_current_resident_without_resetting_clock(tmp_path, monkeypatch):
    state = _write_state(
        tmp_path,
        activation_evidence={"pid": 10},
    )
    monkeypatch.setattr(
        watch,
        "_resident_activation",
        lambda path: {
            "activated_at": "2026-08-02T02:30:00Z",
            "authorized_activation": True,
            "pid": 20,
            "guard_code_identity": "PASS",
            "resident_wallet_fraction": 0.20,
        },
    )

    result = _run(tmp_path, [], scores={}, execute=True, state_path=state)
    stored = json.loads(state.read_text())
    assert result["status"] == "WATCH"
    assert stored["activated_at"] == "2026-08-02T00:00:00Z"
    assert stored["resident_observation"]["pid"] == 20
    assert stored["resident_observation"]["activation_clock_preserved_at"] == (
        "2026-08-02T00:00:00Z"
    )


def test_resident_activation_marks_authorized_after_floor(tmp_path, monkeypatch):
    guard = tmp_path / "guard.json"
    guard.write_text(
        json.dumps(
            {
                "guard_code_identity": {
                    "status": "PASS",
                    "pid": 123,
                    "started_at_utc": "2026-08-02T00:01:00Z",
                }
            }
        ),
        encoding="utf-8",
    )

    class Result:
        def __init__(self, stdout: str):
            self.stdout = stdout

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["pgrep", "-f"]:
            return Result("123\n")
        if cmd[:3] == ["ps", "-p", "123"]:
            return Result("python scripts/run_wallet_copy_live_guard.py --wallet-fraction 0.20")
        raise AssertionError(cmd)

    monkeypatch.setattr(watch.subprocess, "run", fake_run)
    result = watch._resident_activation(guard)
    assert result is not None
    assert result["authorized_activation"] is True


def test_resident_activation_marks_early_activation_unauthorized(tmp_path, monkeypatch):
    guard = tmp_path / "guard.json"
    guard.write_text(
        json.dumps(
            {
                "guard_code_identity": {
                    "status": "PASS",
                    "pid": 123,
                    "started_at_utc": "2026-08-01T21:00:00Z",
                }
            }
        ),
        encoding="utf-8",
    )

    class Result:
        def __init__(self, stdout: str):
            self.stdout = stdout

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["pgrep", "-f"]:
            return Result("123\n")
        if cmd[:3] == ["ps", "-p", "123"]:
            return Result("python scripts/run_wallet_copy_live_guard.py --wallet-fraction 0.20")
        raise AssertionError(cmd)

    monkeypatch.setattr(watch.subprocess, "run", fake_run)
    result = watch._resident_activation(guard)
    assert result is not None
    assert result["authorized_activation"] is False


def test_launcher_restore_refuses_loss_limit_drift(tmp_path):
    launcher = _write_launcher(tmp_path)
    launcher.write_text(
        launcher.read_text().replace("--drip-max-tranche-usd 2.5", "--drip-max-tranche-usd 3.0"),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="launcher invariant"):
        watch._restore_launcher(launcher)


def test_module_imports_clean():
    assert importlib.reload(watch) is not None
