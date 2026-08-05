from scripts.report_temporal_slice_label_divergence import build_report


def test_divergence_report_splits_direction():
    registry = {"generated_at": "cut", "wallets": [
        {"wallet": "a", "slice_labels": {"weekday": {"label": "PROVEN-POSITIVE"}}, "venue_slice_labels": {"weekday": {"label": "PROVEN-NEGATIVE"}}},
        {"wallet": "b", "slice_labels": {"weekday": {"label": "PROVEN-NEGATIVE"}}, "venue_slice_labels": {"weekday": {"label": "PROVEN-POSITIVE"}}},
        {"wallet": "c", "slice_labels": {"weekday": {"label": "PROVEN-POSITIVE"}}, "venue_slice_labels": {"weekday": {"label": "PROVEN-POSITIVE"}}},
    ]}
    report = build_report(registry, slice_name="weekday", generated_at="now")
    assert report["summary"] == {
        "wallets_total": 3,
        "divergent_wallets": 2,
        "all_venue_positive_to_venue_negative": 1,
        "all_venue_negative_to_venue_positive": 1,
        "other_divergence": 0,
    }
