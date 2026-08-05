from pathlib import Path


def test_start_live_guard_launcher_contains_live_arming_flags() -> None:
    script = Path("scripts/start_live_guard.sh").read_text()

    assert "NOT YET FILLED" not in script
    assert "exec " in script
    assert "--execute-live" in script
    assert "--explicit-live-operator-go" in script
    assert "--live-orders-allowed" in script
    assert "scripts/run_wallet_copy_live_guard.py" in script
    assert script.count("--wallet-fraction 0.20") == 1
    assert "--wallet-fraction 0.10" not in script
    assert script.count("--max-order-usd 8.0") == 1
    assert script.count("--drip-min-tranche-usd 1.0") == 1
    assert script.count("--drip-max-tranche-usd 2.5") == 1
    assert script.count("--per-window-fill-cap 1") == 1
    assert script.count("WALLET_COPY_PRICE_BAND_DECISION_MAX_PRICE=0.32") == 1
    assert script.count("--active-set-live-execution-probes-every-n-cycles 16") == 1


def test_live_guard_launchd_and_parser_share_ruled_price_ceiling(monkeypatch) -> None:
    from scripts.run_wallet_copy_live_guard import parse_args

    plist = Path("launchd/com.belavarga.polymarket.wallet-copy-live-guard.plist").read_text()
    assert "<key>WALLET_COPY_PRICE_BAND_DECISION_MAX_PRICE</key>" in plist
    assert "<string>0.32</string>" in plist

    monkeypatch.setenv("WALLET_COPY_PRICE_BAND_DECISION_MAX_PRICE", "0.32")
    monkeypatch.setattr("sys.argv", ["run_wallet_copy_live_guard.py"])
    assert parse_args().price_band_decision_max_price == 0.32
