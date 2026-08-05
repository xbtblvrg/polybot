from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

from scripts.report_sub25_bucket_accounting_spot_check import build_report


def _order(idx: int, *, price: float = 0.20, outcome: str = "Up", token_id: str = "yes") -> dict:
    return {
        "order_id": f"o-{idx}",
        "intent_id": f"ci-{idx}",
        "market_slug": f"btc-updown-5m-{1000 + idx * 300}",
        "condition_id": f"cond-{idx}",
        "token_id": token_id,
        "outcome": outcome,
        "limit_price": price,
        "filled_size_usd": 1.0,
        "filled_shares": 5.0,
        "final_status": "FILLED",
    }


def test_sub25_spot_check_confirms_zero_win_sample(tmp_path: Path) -> None:
    replay = {
        "candidates": [
            {
                "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "paper_replay": {"replay_orders": [_order(idx) for idx in range(3)]},
            }
        ]
    }
    resolutions = "\n".join(
        json.dumps(
            {
                "condition_id": f"cond-{idx}",
                "yes_token": "yes",
                "no_token": "no",
                "direction": "DOWN",
                "expiry_unix_ts": 1300 + idx * 300,
                "source": "test",
                "window_type": "5m",
            }
        )
        for idx in range(3)
    )
    (tmp_path / "replay.json").write_text(json.dumps(replay), encoding="utf-8")
    (tmp_path / "resolutions.jsonl").write_text(resolutions + "\n", encoding="utf-8")

    report = build_report(
        tmp_path,
        Namespace(
            replay="replay.json",
            resolutions="resolutions.jsonl",
            sample_size=3,
            output="out.json",
        ),
    )

    assert report["summary"]["sampled_orders"] == 3
    assert report["summary"]["wins"] == 0
    assert report["summary"]["token_outcome_mismatches"] == 0
    assert report["summary"]["conclusion"] == "SUB25_ZERO_WIN_CONFIRMED_SAMPLE"


def test_sub25_spot_check_flags_token_outcome_mismatch(tmp_path: Path) -> None:
    replay = {
        "candidates": [
            {
                "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "paper_replay": {"replay_orders": [_order(1, outcome="Up", token_id="no")]},
            }
        ]
    }
    (tmp_path / "replay.json").write_text(json.dumps(replay), encoding="utf-8")
    (tmp_path / "resolutions.jsonl").write_text(
        json.dumps(
            {
                "condition_id": "cond-1",
                "yes_token": "yes",
                "no_token": "no",
                "direction": "DOWN",
                "expiry_unix_ts": 1600,
                "source": "test",
                "window_type": "5m",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    report = build_report(
        tmp_path,
        Namespace(
            replay="replay.json",
            resolutions="resolutions.jsonl",
            sample_size=1,
            output="out.json",
        ),
    )

    assert report["summary"]["token_outcome_mismatches"] == 1
    assert report["summary"]["conclusion"] == "ACCOUNTING_DEFECT_TOKEN_OUTCOME_MISMATCH"
