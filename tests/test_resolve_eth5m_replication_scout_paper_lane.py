import json
from pathlib import Path

from scripts.resolve_eth5m_replication_scout_paper_lane import resolve_once


def test_resolver_scores_gamma_winner_post_fee_and_keeps_paper_only(tmp_path: Path) -> None:
    state = tmp_path / "state.json"
    events = tmp_path / "events.jsonl"
    resolutions = tmp_path / "resolutions.jsonl"
    state.write_text(json.dumps({"first_observed_at_s": 1000.0, "observations": 1}))
    events.write_text(json.dumps({
        "market_slug": "eth-updown-5m-1000", "outcome": "Up", "source_price": 0.5,
        "paper_notional_usd": 1.0,
    }) + "\n")

    payload = resolve_once(
        state,
        events,
        resolutions,
        now=23000.0,
        fetch=lambda _slug: [{"markets": [{
            "slug": "eth-updown-5m-1000", "outcomes": '["Up","Down"]',
            "outcomePrices": '["1","0"]',
        }]}],
    )

    assert payload["resolved_intents"] == 1
    assert payload["post_fee_pnl_usd"] == 1.0
    assert payload["live_orders_allowed"] is False
    assert payload["promotion_gate_pass"] is False
