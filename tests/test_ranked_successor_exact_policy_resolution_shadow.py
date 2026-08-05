from scripts.run_ranked_successor_exact_policy_resolution_shadow import _replay, build_report
def test_successor_shadow_preserves_single_submitter():
    queue={"rows":[{"wallet":"0xabc","raw_own_source_buy_rows":9,"policy_eligible_intents":2}]}
    lanes={"lanes":[{"wallet":"0xabc","resolved_paper_fills":3,"external_latest_trade_age_h":1,
      "live_canary_packet_preconditions":{"fresh_source_active_window":True,"fading_clear":True}}]}
    direct={"updated_at":"2026-07-23T23:00:00Z","wallets":{"0xabc":{"buy_events":9,"copyable_buy_events":2,"sample_status":"HAS_BUY_SAMPLE"}}}
    event={"event":"top10_broad_paper_measurement","side":"BUY","wallet":"0xabc","transaction_hash":"0xtx",
      "asset":"yes","source_size":100,"source_price":.5,"block_ts":100,"received_at_s":101,
      "result":{"status":"FILLED","book":{"avg_fill_price":.5,"fillable_shares":8,"fillable_usd":4}}}
    resolution={"yes_token":"yes","no_token":"no","direction":"UP","condition_id":"0xcond",
      "market_slug":"btc-updown-5m-0"}
    report=build_report(queue,lanes,generated_at="2026-07-23T23:00:00Z",direct=direct,
      direct_events=[event,event],resolutions=[resolution])
    assert report["paper_only"] and not report["live_orders_allowed"] and report["single_submitter_unchanged"]
    assert report["qualified_successors"][0]["wallet"]=="0xabc"
    assert report["candidates"][0]["fee_resolution_attachment"]["attached"]
    assert report["candidates"][0]["event_identity_count"]==1
    assert report["exact_policy_replay"]["builder"].endswith("intents_for_policy")

def test_unresolved_stays_pending_and_settlement_attaches_later():
    event={"event":"top10_broad_paper_measurement","side":"BUY","wallet":"0xabc","transaction_hash":"0xtx",
      "asset":"yes","source_size":100,"source_price":.5,"block_ts":100,"received_at_s":101,
      "result":{"status":"FILLED","book":{"avg_fill_price":.5,"fillable_shares":8,"fillable_usd":4}}}
    assert _replay([event],[])["0xabc"]["pending_resolution"]==1
    settled=_replay([event],[{"yes_token":"yes","direction":"UP","condition_id":"c","market_slug":"btc-updown-5m-0"}])
    assert settled["0xabc"]["resolved_post_fee_windows"]==1

def test_source_active_wallet_accrues_without_ready_lane():
    queue={"rows":[{"wallet":"0xabc"}]}
    direct={"wallets":{"0xabc":{"last_event_ts":1000}}}
    event={"event":"top10_broad_paper_measurement","side":"BUY","wallet":"0xabc","transaction_hash":"0xtx",
      "asset":"yes","source_size":100,"source_price":.5,"block_ts":900,"received_at_s":901,
      "result":{"status":"REJECTED"}}
    report=build_report(queue,{},generated_at="1970-01-01T00:16:50Z",direct=direct,
      direct_events=[event],resolutions=[{"yes_token":"yes","direction":"UP","condition_id":"c","market_slug":"btc-updown-5m-0"}])
    row=report["candidates"][0]
    assert row["raw_own_source_buys"]==1 and row["eligible_intents"]==1
    assert row["would_submit_executable"]==0
    assert "materially_nonzero_acceptance" in row["failed_admission_gates"]
