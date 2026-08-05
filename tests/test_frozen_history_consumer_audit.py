import json
from pathlib import Path

from scripts.audit_frozen_history_consumers import build_audit


def test_frozen_history_audit_requires_repoint_and_packet_age_gate(tmp_path: Path) -> None:
    (tmp_path / "scripts").mkdir()
    (tmp_path / "src").mkdir()
    (tmp_path / "scripts/run_btc5m_structural_scalp_paper_lane.py").write_text(
        'DEFAULT_HISTORY = "data/research/btc5m_structural_scalp_forward_source_events.jsonl"\n'
        'DEFAULT_HOT_SOURCE = "data/research/wallet_copy_live_guard_hot_history_state.json"\n',
        encoding="utf-8",
    )
    (tmp_path / "scripts/report_btc5m_structural_scalp_promotion_prep.py").write_text(
        "newest_source_event_age_s = 0\nsource_fresh = newest_source_event_age_s <= 86400.0\nevidence_pass = source_fresh\n",
        encoding="utf-8",
    )
    (tmp_path / "scripts/legacy.py").write_text(
        'SOURCE = "data/research/wallet_copy_history_state.json"\n',
        encoding="utf-8",
    )

    audit = build_audit(tmp_path, generated_at="2026-07-20T02:00:00Z")

    assert audit["status"] == "PASS_FAIL_CLOSED"
    assert audit["promotion_lane"]["repointed_to_current_snapshot"] is True
    assert audit["promotion_lane"]["promotion_packet_fails_closed_after_86400s"] is True
    assert audit["frozen_source_consumers"][0]["path"] == "scripts/legacy.py"
    assert audit["frozen_source_consumers"][0]["classification"] == "RESEARCH_ONLY_STALE_TAINTED"


def test_frozen_history_audit_classifies_live_guard_hot_override(tmp_path: Path) -> None:
    (tmp_path / "scripts").mkdir()
    (tmp_path / "src").mkdir()
    (tmp_path / "scripts/run_btc5m_structural_scalp_paper_lane.py").write_text(
        'CURRENT = "data/research/wallet_copy_live_guard_hot_history_state.json"\n', encoding="utf-8"
    )
    (tmp_path / "scripts/report_btc5m_structural_scalp_promotion_prep.py").write_text(
        "newest_source_event_age_s = 0\nsource_fresh = newest_source_event_age_s <= 86400.0\nevidence_pass = source_fresh\n",
        encoding="utf-8",
    )
    (tmp_path / "scripts/run_wallet_copy_live_guard.py").write_text(
        'FALLBACK = "data/research/wallet_copy_history_state.json"\n', encoding="utf-8"
    )

    audit = build_audit(tmp_path, generated_at="2026-07-20T02:00:00Z")

    assert audit["live_guard_reference_ruling"]["classification"] == "LIVE_PATH_SAFE_HOT_OVERRIDE"
    assert "producer/warmup fallback only" in audit["live_guard_reference_ruling"]["impact"]


def test_frozen_history_audit_clears_only_promotion_grade_rebuilds(tmp_path: Path) -> None:
    (tmp_path / "scripts").mkdir()
    (tmp_path / "src").mkdir()
    (tmp_path / "data/research").mkdir(parents=True)
    (tmp_path / "scripts/run_btc5m_structural_scalp_paper_lane.py").write_text(
        'CURRENT = "data/research/wallet_copy_live_guard_hot_history_state.json"\n'
        'ACCUMULATOR = "data/research/btc5m_structural_scalp_forward_source_events.jsonl"\n',
        encoding="utf-8",
    )
    (tmp_path / "scripts/report_btc5m_structural_scalp_promotion_prep.py").write_text(
        "newest_source_event_age_s = 0\nsource_fresh = newest_source_event_age_s <= 86400.0\nevidence_pass = source_fresh\n",
        encoding="utf-8",
    )
    current = tmp_path / "data/research/wallet_copy_followability_leaderboard_latest.json"
    current.write_text(json.dumps({"promotion_grade": True}), encoding="utf-8")

    audit = build_audit(tmp_path, generated_at="2026-07-20T02:00:00Z")
    by_artifact = {row["artifact"]: row for row in audit["standing_conclusions"]}

    assert by_artifact["data/research/wallet_copy_followability_leaderboard_latest.json"]["status"] == "CURRENT_REBUILT"
    assert by_artifact["data/research/alpha_decay_report.json"]["status"] == "STALE_TAINTED_REBUILD_REQUIRED"
    assert len(audit["stale_tainted_standing_conclusions"]) == 4
