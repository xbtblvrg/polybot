from scripts.report_positive_wallet_slice_selector_falsifier import build_report


def _event(
    wallet: str,
    *,
    event_id: str,
    start: int,
    offset: int,
    price: float,
    outcome: str,
) -> dict:
    return {
        "source_wallet": wallet,
        "event_id": event_id,
        "transaction_hash": event_id,
        "token_id": f"token-{event_id}",
        "condition_id": f"condition-{event_id}",
        "market_slug": f"btc-updown-5m-{start}",
        "event_ts": start + offset,
        "price": price,
        "outcome": outcome,
        "action": "BUY",
        "asset": "BTC",
        "duration": "5m",
    }


def _resolution(event_id: str, start: int, direction: str) -> dict:
    return {
        "condition_id": f"condition-{event_id}",
        "market_slug": f"btc-updown-5m-{start}",
        "yes_token": f"token-{event_id}" if direction == "UP" else "other",
        "no_token": f"token-{event_id}" if direction == "DOWN" else "other",
        "direction": direction,
    }


def test_slice_selector_falsifier_detects_two_sign_inversions() -> None:
    wallets = tuple(f"0x{index:040x}" for index in range(4))
    events = []
    resolutions = []
    manifest_rows = []
    for index, wallet in enumerate(wallets):
        win_id = f"win-{index}"
        loss_id = f"loss-{index}"
        events.extend(
            [
                _event(
                    wallet,
                    event_id=win_id,
                    start=1000 + index * 1000,
                    offset=130,
                    price=0.4,
                    outcome="Up",
                ),
                _event(
                    wallet,
                    event_id=loss_id,
                    start=2000 + index * 1000,
                    offset=70,
                    price=0.4,
                    outcome="Up",
                ),
            ]
        )
        resolutions.extend(
            [
                _resolution(win_id, 1000 + index * 1000, "UP"),
                _resolution(loss_id, 2000 + index * 1000, "DOWN"),
            ]
        )
        manifest_rows.append(
            {
                "wallet": wallet,
                "move_slice_keys": ["060-120|0.25-0.50"],
            }
        )

    report = build_report(
        events=events,
        resolutions=resolutions,
        manifest={"capture_watch_wallets": manifest_rows},
        alpha={},
        wallets=wallets,
    )

    assert report["sign_inversion_count"] == 4
    assert report["verdict"] == "SLICE_SELECTOR_SIGN_INVERTER"
    assert all(row["unsliced"]["post_fee_pnl_usd"] > 0 for row in report["rows"])
    assert all(row["sliced"]["post_fee_pnl_usd"] < 0 for row in report["rows"])


def test_slice_selector_falsifier_uses_alpha_keys_when_wallet_absent_from_manifest() -> None:
    wallet = "0x" + "a" * 40
    event = _event(
        wallet,
        event_id="alpha",
        start=3000,
        offset=70,
        price=0.4,
        outcome="Up",
    )
    report = build_report(
        events=[event],
        resolutions=[_resolution("alpha", 3000, "UP")],
        manifest={"capture_watch_wallets": []},
        alpha={
            "execution_profiles": {
                "profiles_by_wallet": {
                    wallet: {
                        "move_slices": [
                            {
                                "move_slice_key": "060-120|0.25-0.50",
                                "mean_edge": 0.1,
                                "median_edge": 0.1,
                                "copyable_rate_pct": 80,
                            }
                        ]
                    }
                }
            }
        },
        wallets=(wallet,),
    )

    assert report["rows"][0]["slice_key_source"] == (
        "latest_alpha_positive_70pct_move_slices"
    )
    assert report["rows"][0]["sliced"]["resolved"] == 1
