from scripts import apply_band_scoped_seat_admission as subject


def _cell(wallet: str, *, second_half: float = 2.0):
    return {
        "wide_policy_fingerprint": wallet[-8:] * 8,
        "identity": {"selection_rule_id": "positive_slices"},
        "move_slice_venue_executable_full_stream_rescore": {
            "120-180|>0.75": {
                "evidence_authority": "venue_executable_full_stream_rescore",
                "f1_pass": True,
                "resolved": 250,
                "post_fee_pnl_usd": 5.0,
                "roi_pct": 2.0,
                "first_half_post_fee_pnl_usd": 3.0,
                "second_half_post_fee_pnl_usd": second_half,
            },
            "000-060|<=0.25": {
                "evidence_authority": "venue_executable_full_stream_rescore",
                "f1_pass": False,
                "resolved": 20,
                "post_fee_pnl_usd": 4.0,
                "roi_pct": 20.0,
                "first_half_post_fee_pnl_usd": 2.0,
                "second_half_post_fee_pnl_usd": 2.0,
            },
        },
    }


def test_build_packet_admits_only_f1_dual_half_positive_bands():
    evidence = {
        "generated_at": "2026-07-28T17:00:00Z",
        "best_by_wallet": {
            wallet: _cell(wallet)
            for wallet in subject.TARGETS
        },
    }
    packet = subject.build_packet(
        evidence,
        generated_at="2026-07-28T17:30:00Z",
    )
    assert packet["status"] == "PASS"
    assert packet["paper_only"] is False
    assert packet["live_orders_allowed"] is True
    assert len(packet["members"]) == 2
    assert all(
        row["policy"]["move_slice_keys"] == ["120-180|>0.75"]
        for row in packet["members"]
    )
    assert all(row["max_order_usd"] == 1.0 for row in packet["members"])
    assert all(
        row["policy"]["maker_min_share_funding_cap_usd"] == 2.5
        for row in packet["members"]
    )
    assert packet["guardrails"]["venue_minimum_hard_ceiling_usd"] == 2.5
    assert all(row["rolling_loss_trigger_usd"] == -4.0 for row in packet["members"])


def test_build_packet_fails_closed_when_target_has_no_qualified_band():
    evidence = {
        "best_by_wallet": {
            subject.TARGETS[0]: _cell(subject.TARGETS[0]),
            subject.TARGETS[1]: _cell(subject.TARGETS[1], second_half=-1.0),
        }
    }
    packet = subject.build_packet(
        evidence,
        generated_at="2026-07-28T17:30:00Z",
    )
    assert packet["status"] == "DEFECT"
    assert packet["paper_only"] is True
    assert packet["live_orders_allowed"] is False
    assert packet["defects"] == [
        {
            "source_wallet": subject.TARGETS[1],
            "reason": "no_f1_and_dual_half_positive_band",
        }
    ]
