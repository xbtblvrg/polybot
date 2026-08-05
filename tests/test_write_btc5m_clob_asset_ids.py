from scripts import write_btc5m_clob_asset_ids as subject


def test_collect_asset_ids_carries_token_metadata(monkeypatch):
    monkeypatch.setattr(subject, "_fetch_gamma_event", lambda *args, **kwargs: [{"markets": [{
        "slug": "btc-updown-5m-1", "conditionId": "condition", "clobTokenIds": '["up","down"]', "outcomes": '["Up","Down"]',
    }]}])
    report = subject.collect_asset_ids(["btc-updown-5m-1"], timeout_s=1, user_agent="test")
    assert report["asset_ids"] == ["up", "down"]
    assert report["token_metadata"]["up"] == {"condition_id": "condition", "market_slug": "btc-updown-5m-1", "outcome": "Up"}
