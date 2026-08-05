import json
from argparse import Namespace
from pathlib import Path

from scripts.probe_cohort_source_active_liveness import build_batch


def test_build_batch_persists_exact_ordering_snapshot_and_custom_latest_names(tmp_path: Path) -> None:
    wallets = ["0x" + char * 40 for char in "abc"]
    replay = tmp_path / "replay.json"
    replay.write_text(
        json.dumps(
            {
                "generated_at": "2026-07-20T19:36:00Z",
                "live_ready_picks": [{"wallet": wallet} for wallet in wallets],
            }
        )
    )
    rtds = tmp_path / "events.jsonl"
    rtds.write_text("")
    output = tmp_path / "out"
    args = Namespace(
        cohort_replay=str(replay),
        rtds_jsonl=str(rtds),
        output_dir=str(output),
        latest_name="mining_batch_latest.json",
        ordering_latest_name="mining_ordering_latest.json",
        batch_id="test",
        wallet_offset=1,
        wallet_limit=1,
        since="2026-07-20T00:00:00Z",
        tail_bytes=1024,
        min_offset_s=0.0,
        max_offset_s=300.0,
        max_price=0.5,
        required_windows=1,
    )

    batch = build_batch(args)
    ordering = json.loads((output / "mining_ordering_latest.json").read_text())

    assert (output / "mining_batch_latest.json").exists()
    assert not (output / "source_active_liveness_batch_latest.json").exists()
    assert [row["wallet"] for row in ordering["live_ready_picks"]] == wallets
    assert ordering["wallet_count"] == 3
    assert batch["replay_ordering_snapshot"].endswith("mining_ordering_latest.json")
    assert len(batch["replay_ordering_sha256"]) == 64
