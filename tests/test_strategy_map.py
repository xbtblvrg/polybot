import argparse
import json
from pathlib import Path

from scripts import build_strategy_map


def test_strategy_map_marks_rows_stale_and_fresh(tmp_path: Path) -> None:
    mechanisms = tmp_path / "docs/agents/MECHANISMS.md"
    data = tmp_path / "data/research"
    mechanisms.parent.mkdir(parents=True)
    data.mkdir(parents=True)
    mechanisms.write_text(
        """# BTC 5m Mechanism Registry

## Registry

| id | family | mechanism | paper_lane_id | status | owner | live_path |
| --- | --- | --- | --- | --- | --- | --- |
| copy-drip-inventory | copy | live drip | paper_copy_drip_inventory | running | Codex/Fable | existing live guard only |
| stale-lane | signal | stale lane | paper_stale | seeded | Codex/Fable | evidence gate |

## Enemy Campaigns

| id | enemy | baseline metric | hypothesis | paper/shadow lane | success criterion | live gate |
| --- | --- | --- | --- | --- | --- | --- |
| CAMPAIGN-LAT | latency | n=1 | reduce lag | lat_shadow | improve | Fable |
""",
        encoding="utf-8",
    )
    (data / "wallet_copy_daily_scorecard_2026-07-10_current.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-07-10T20:00:00Z",
                "canonical_pnl_truth": {"by_day": {"2026-07-10": {"pnl_usd": 12.5, "fills": 5}}},
            }
        ),
        encoding="utf-8",
    )
    (data / "campaign_lat_p1_packet_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-07-10T19:00:00Z",
                "routing_shadow": {"post_fee_pnl_usd": -1.25, "would_submit_windows": 7},
                "recommended_next_actions": [{"id": "NEXT"}],
            }
        ),
        encoding="utf-8",
    )

    payload = build_strategy_map.build_map(
        tmp_path,
        argparse.Namespace(
            mechanisms=str(mechanisms),
            data_dir=str(data),
            now_iso="2026-07-10T21:00:00Z",
        ),
    )

    assert payload["summary"]["rows"] == 3
    assert payload["summary"]["stale_rows"] == 1
    rows = {row["id"]: row for row in payload["rows"]}
    assert rows["copy-drip-inventory"]["status"] == "LIVE"
    assert rows["copy-drip-inventory"]["freshness"] == "FRESH"
    assert rows["CAMPAIGN-LAT"]["latest_evidence_number"].endswith("=7.0")
    assert rows["stale-lane"]["freshness"] == "STALE"
    assert payload["stale_defects"][0]["id"] == "stale-lane"


def test_strategy_map_prefers_inventory_e4_parking_ref(tmp_path: Path) -> None:
    mechanisms = tmp_path / "docs/agents/MECHANISMS.md"
    data = tmp_path / "data/research"
    mechanisms.parent.mkdir(parents=True)
    data.mkdir(parents=True)
    mechanisms.write_text(
        """# BTC 5m Mechanism Registry

## Registry

| id | family | mechanism | paper_lane_id | status | owner | live_path |
| --- | --- | --- | --- | --- | --- | --- |
| signal-inventory-e4 | signal | inventory E4 style imbalance | paper_signal_inventory_e4 | seeded | Codex/Fable | ensemble gate |

## Enemy Campaigns

| id | enemy | baseline metric | hypothesis | paper/shadow lane | success criterion | live gate |
| --- | --- | --- | --- | --- | --- | --- |
""",
        encoding="utf-8",
    )
    (data / "wallet_copy_inventory_paper_state.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-07-03T11:11:00Z",
                "paper_only": True,
                "live_orders_allowed": False,
                "summary": {"paper_orders": 746},
            }
        ),
        encoding="utf-8",
    )
    (data / "wallet_copy_inventory_e4_parking_latest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-07-10T22:50:00Z",
                "status": "PARKED_NO_CURRENT_INVENTORY_PLAN",
                "paper_only": True,
                "live_orders_allowed": False,
                "summary": {"inventory_plans": 0},
                "next_action": "keep parked until wallet_copy_research_state reports nonzero inventory_plans",
            }
        ),
        encoding="utf-8",
    )

    payload = build_strategy_map.build_map(
        tmp_path,
        argparse.Namespace(
            mechanisms=str(mechanisms),
            data_dir=str(data),
            now_iso="2026-07-10T23:00:00Z",
        ),
    )

    row = payload["rows"][0]
    assert row["id"] == "signal-inventory-e4"
    assert row["artifact"].endswith("wallet_copy_inventory_e4_parking_latest.json")
    assert row["freshness"] == "FRESH"
    assert payload["summary"]["stale_rows"] == 0
    assert payload["stale_defects"] == []


def test_recurring_routing_and_multivenue_outputs_have_freshness_owners() -> None:
    routing = build_strategy_map.RUNTIME_BINDINGS[
        "copy-routing-shadow-validation-lane"
    ]
    multivenue = build_strategy_map.RUNTIME_BINDINGS[
        "structural-btc5m-cross-venue-residual-leadlag"
    ]
    assert routing["label"] == "com.belavarga.polymarket.wallet-copy-live-guard"
    assert routing["freshness_slo_s"] == 180
    assert "zero-submit" in routing["ownership"]
    assert (
        multivenue["label"]
        == "com.belavarga.polymarket.btc5m-multivenue-residual-matrix"
    )
    assert multivenue["freshness_slo_s"] == 60
