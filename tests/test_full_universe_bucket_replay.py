import json
from argparse import Namespace
from pathlib import Path

from scripts.report_full_universe_bucket_replay import build_report


def test_bucket_replay_spotcheck_confirms_sub25c_adverse_selection(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    research.mkdir(parents=True)
    replay_path = research / "replay.json"
    resolutions_path = research / "resolutions.jsonl"

    orders = []
    for idx in range(4):
        outcome = "Up" if idx % 2 == 0 else "Down"
        winner = "DOWN" if outcome == "Up" else "UP"
        slug = f"btc-updown-5m-{1_700_000_000 + idx * 300}"
        condition = f"condition-{idx}"
        orders.append(
            {
                "final_status": "FILLED",
                "status": "FILLED",
                "order_id": f"order-{idx}",
                "market_slug": slug,
                "condition_id": condition,
                "outcome": outcome,
                "limit_price": 0.12,
                "filled_size_usd": 1.2,
                "requested_size_usd": 1.2,
                "filled_shares": 10.0,
                "source_intent": {
                    "source_wallet": "0x1111111111111111111111111111111111111111",
                    "outcome": outcome,
                    "event_ts": 1_700_000_000 + idx * 300 + 10,
                },
                "fill_estimate": {"book": {"book_timestamp": (1_700_000_000 + idx * 300 + 20) * 1000}},
            }
        )
        with resolutions_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"market_slug": slug, "condition_id": condition, "direction": winner}) + "\n")

    replay_path.write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "wallet": "0x1111111111111111111111111111111111111111",
                        "paper_replay": {"replay_orders": orders},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    report = build_report(
        root,
        Namespace(
            replay="data/research/replay.json",
            resolutions="data/research/resolutions.jsonl",
            output="data/research/out.json",
            min_orders=1,
            top_n=5,
            spotcheck_sample_size=3,
        ),
    )

    spotcheck = report["sub_25c_accounting_spotcheck"]
    assert report["by_price_bucket"]["00_00_25"]["wins"] == 0
    assert spotcheck["sample_size"] == 3
    assert spotcheck["sample_wins"] == 0
    assert spotcheck["verdict"] == "FILL_CONDITIONED_ADVERSE_SELECTION_CONFIRMED"
    assert "fill-conditioned adverse-selection" in spotcheck["conclusion"]
