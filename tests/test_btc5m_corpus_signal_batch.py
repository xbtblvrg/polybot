from __future__ import annotations

import json
from pathlib import Path

from scripts import run_btc5m_corpus_signal_batch as batch


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n")


def test_btc5m_corpus_batch_scores_flow_and_active_wallet(tmp_path: Path) -> None:
    resolutions = tmp_path / "resolutions.jsonl"
    rtds = tmp_path / "rtds.jsonl"
    state = tmp_path / "state.json"
    output = tmp_path / "report.json"
    wallet = "0x1111111111111111111111111111111111111111"
    _write_jsonl(
        resolutions,
        [
            {
                "asset": "BTC",
                "window_type": "5m",
                "market_slug": "btc-updown-5m-1000",
                "window_start_unix_ts": 1000,
                "direction": "UP",
            },
            {
                "asset": "BTC",
                "window_type": "5m",
                "market_slug": "btc-updown-5m-1300",
                "window_start_unix_ts": 1300,
                "direction": "DOWN",
            },
        ],
    )
    _write_jsonl(
        rtds,
        [
            {
                "event": "rtds_trade_event",
                "market_slug": "btc-updown-5m-1000",
                "outcome": "Up",
                "side": "BUY",
                "price": 0.4,
                "size": 300,
                "event_ts": 1010,
                "source_wallet": wallet,
            },
            {
                "event": "rtds_trade_event",
                "market_slug": "btc-updown-5m-1000",
                "outcome": "Down",
                "side": "SELL",
                "price": 0.6,
                "size": 100,
                "event_ts": 1020,
                "source_wallet": "0x2222222222222222222222222222222222222222",
            },
            {
                "event": "rtds_trade_event",
                "market_slug": "btc-updown-5m-1300",
                "outcome": "Up",
                "side": "BUY",
                "price": 0.4,
                "size": 300,
                "event_ts": 1310,
                "source_wallet": wallet,
            },
        ],
    )
    resolved, summary = batch._load_resolutions(resolutions)
    windows, scan, active_grid = batch.scan_corpus(
        rtds_path=rtds,
        resolutions=resolved,
        active_wallets={wallet: {"candidate_id": "member"}},
        max_bytes=0,
        max_lines=0,
        order_usd=1.0,
        tick_size=0.01,
        state_path=state,
        split_start_s=1300,
    )

    assert summary["resolved_windows"] == 2
    assert scan["diagnostics"]["accepted_trade"] == 3
    assert set(windows) == {"btc-updown-5m-1000", "btc-updown-5m-1300"}
    active = batch._finalize_grid(active_grid, min_trades_for_positive=1)
    assert active["best"]["trades"] == 2
    assert active["best"]["test"]["trades"] == 1
    assert active["status"] == "KILL_NO_POSITIVE_OOS_REGION"

    flow = batch._flow_grids(
        windows,
        resolved,
        order_usd=1.0,
        tick_size=0.01,
        max_samples=3,
        split_start_s=1300,
    )
    assert flow["E4_inventory_flow"]["best"]["trades"] >= 2
    assert flow["E4_inventory_flow"]["status"] in {"POSITIVE_OOS_REGION", "KILL_NO_POSITIVE_OOS_REGION"}

    report = {
        "studies": {
            "E11_cross_window_momentum": batch._cross_window_momentum(resolved, 1.0, 3, 1300),
            "E14_hour_seasonality": batch._hour_seasonality(resolved, 1.0, 3, 1300),
        }
    }
    output.write_text(json.dumps(report))
    assert json.loads(output.read_text())["studies"]["E11_cross_window_momentum"]["best"]["trades"] == 1
