from scripts.report_order151_bf337_fading_adjudication import WALLET, build_report


def _payload(recent_roi: float, resolved: int = 200, sigma: float = -1.0):
    return {"generated_at": "t", "criteria": {"min_trades": 5, "fading_min_resolved_trades": 200, "fading_max_gap_in_sigma": -1.0}, "wallets": [{
        "wallet": WALLET, "classification": "FADING", "classification_reason": "decay",
        "all": {"resolved_trades": 100, "roi_pct": 2, "first_event_ts": "a", "latest_event_ts": "z"},
        "recent": {"resolved_trades": resolved, "roi_pct": recent_roi, "gap_in_sigma": sigma, "pnl_usd": -1, "first_event_ts": "y", "latest_event_ts": "z"},
    }]}


def test_refuses_rotation_on_classifier_own_negative_recent_basis():
    report = build_report(temporal=_payload(-1), deadman={})
    assert report["verdict"] == "GENUINE_DECAY_REFUSE_ROTATION"
    assert report["fading_clear"] is False
    assert report["rotation_authorized"] is False


def test_clears_when_recent_basis_is_positive():
    report = build_report(temporal=_payload(1), deadman={})
    assert report["verdict"] == "INSUFFICIENT_POWER_FOR_FADING_ROTATION_AUTHORIZED"
    assert report["fading_clear"] is True


def test_clears_when_negative_recent_basis_lacks_power():
    report = build_report(temporal=_payload(-1, resolved=50, sigma=-0.53), deadman={})
    assert report["rotation_authorized"] is True
