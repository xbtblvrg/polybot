import json
import subprocess
import tarfile
from pathlib import Path
from types import SimpleNamespace

import scripts.dr_preflight as dr_preflight
import scripts.build_wallet_copy_data_layer as data_layer
from scripts.build_wallet_copy_data_layer import _parse_ts, _row
from scripts.dr_preflight import ROOT, build_report, create_bundle_fallback


def test_data_layer_normalizes_partition_fields() -> None:
    row = _row(
        Path("data/research/btc_events.jsonl"),
        7,
        {
            "observed_ts": 1783432801.0,
            "market_slug": "btc-updown-5m",
            "condition_id": "0xabc",
            "source_wallet": "0xWallet",
            "price": 0.52,
        },
    )

    assert _parse_ts("1783423201") is not None
    assert row["day"] == "2026-07-07"
    assert row["series"] == "btc_5m"
    assert row["condition_id"] == "0xabc"
    assert row["wallet"] == "0xwallet"
    assert json.loads(row["raw_json"])["price"] == 0.52


def test_data_layer_records_final_flush_outputs_for_source_refresh(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "wallet_copy_self_trades.jsonl"
    source.write_text(
        "\n".join(
            [
                json.dumps({"event_ts": 1783432801.0, "market_slug": "btc-updown-5m", "tx": "0x1", "cost_usd": 1.0}),
                json.dumps({"event_ts": 1783432861.0, "market_slug": "btc-updown-5m", "tx": "0x2", "cost_usd": 2.0}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeTable:
        @staticmethod
        def from_pylist(rows: list[dict[str, object]]) -> list[dict[str, object]]:
            return rows

    class FakePa:
        Table = FakeTable

    class FakePq:
        @staticmethod
        def write_table(rows: list[dict[str, object]], output: Path) -> None:
            output.write_text(json.dumps(rows), encoding="utf-8")

    class FakeConnection:
        def execute(self, *_args: object, **_kwargs: object) -> "FakeConnection":
            return self

        def fetchone(self) -> tuple[int]:
            return (2,)

        def close(self) -> None:
            return None

    class FakeDuckdb:
        @staticmethod
        def connect(_path: str) -> FakeConnection:
            return FakeConnection()

    monkeypatch.setattr(data_layer, "_dependencies", lambda: (FakePa, FakePq, FakeDuckdb, []))

    args = SimpleNamespace(
        input_glob=[str(source)],
        output_dir=str(tmp_path / "derived"),
        duckdb_path=str(tmp_path / "derived" / "wallet_copy.duckdb"),
        state_output=str(tmp_path / "manifest.json"),
        progress_state=str(tmp_path / "progress.json"),
        batch_size=50,
        max_files=None,
        max_bytes=0,
        max_rows_per_file=None,
        manifest_only=False,
        force=False,
    )

    first = data_layer.build_layer(args)
    assert first["status"] == "PASS"
    output_files = json.loads((tmp_path / "progress.json").read_text())["files"][str(source)]["output_files"]
    assert len(output_files) == 1
    assert Path(output_files[0]).exists()

    second = data_layer.build_layer(args)
    assert second["status"] == "PASS"
    assert second["rows_converted"] == 0
    assert len(list((tmp_path / "derived").glob("dataset=*/day=*/series=*/*.parquet"))) == 1

    args.force = True
    third = data_layer.build_layer(args)
    assert third["status"] == "PASS"
    assert len(list((tmp_path / "derived").glob("dataset=*/day=*/series=*/*.parquet"))) == 1


def test_dr_preflight_reports_missing_remote_without_tracked_env() -> None:
    report = build_report(remote="definitely_missing_remote_for_test")

    assert report["kind"] == "wallet_copy_dr_preflight"
    assert report["status"] in {"OFF_MACHINE_REMOTE_MISSING", "SECRET_PATH_RISK"}
    assert "can_push_now" in report["summary"]
    assert ".env" not in report["summary"]["tracked_secret_paths"]


def test_dr_bundle_fallback_lands_outside_repo_without_secrets(tmp_path: Path, monkeypatch) -> None:
    report = build_report(remote="definitely_missing_remote_for_test")
    bundle_dir = tmp_path / "dr"
    state_file = ROOT / "docs" / "agents" / "HANDOFF.md"

    def fake_run(args: list[str]) -> subprocess.CompletedProcess[str]:
        Path(args[3]).write_text("bundle")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(dr_preflight, "_run", fake_run)
    monkeypatch.setattr(dr_preflight, "_bundle_state_paths", lambda: [state_file])

    bundled = create_bundle_fallback(bundle_dir, report)

    assert bundled["status"] == "BUNDLE_FALLBACK_ONLY"
    fallback = bundled["bundle_fallback"]
    assert Path(fallback["repo_bundle"]).exists()
    assert Path(fallback["state_archive"]).exists()
    assert ROOT.resolve() not in Path(fallback["bundle_dir"]).resolve().parents
    assert fallback["secret_paths_included"] == []
    with tarfile.open(fallback["state_archive"], "r:gz") as archive:
        names = archive.getnames()
    assert names == ["docs/agents/HANDOFF.md"]
    assert all(not dr_preflight._contains_secret_path(name) for name in names)


def test_dr_size_gate_flags_oversized_files() -> None:
    gate = dr_preflight._size_gate(
        [
            {"path": "small.py", "size_bytes": 10},
            {"path": "huge.jsonl", "size_bytes": 101},
        ],
        max_file_bytes=100,
        max_total_bytes=1000,
    )

    assert gate["pass"] is False
    assert gate["oversized_file_count"] == 1
    assert gate["oversized_files"] == [{"path": "huge.jsonl", "size_bytes": 101}]


def test_dr_snapshot_excludes_only_named_regenerable_outputs(monkeypatch) -> None:
    tracked = {
        "docs/agents/HANDOFF.md",
        "data/research/wallet_copy_live_execution_state.json",
        "data/research/wallet_copy_live_guard_state.json",
        "data/research/alpha_decay_13e0_f418_a689_overlap_20260722T0231Z_recap_terminal.json",
        *dr_preflight.SNAPSHOT_REGENERABLE_EXCLUDES,
    }
    monkeypatch.setattr(dr_preflight, "_git_lines_z", lambda *args: sorted(tracked))
    monkeypatch.setattr(dr_preflight, "_bundle_state_paths", lambda: [])
    monkeypatch.setattr(dr_preflight, "_path_size", lambda _path: 10)

    candidates, rejected = dr_preflight._snapshot_candidate_paths(max_file_bytes=100)

    assert rejected == []
    assert tracked - set(dr_preflight.SNAPSHOT_REGENERABLE_EXCLUDES) <= set(candidates)
    assert not set(dr_preflight.SNAPSHOT_REGENERABLE_EXCLUDES) & set(candidates)
    assert "docs/agents/HANDOFF.md" in candidates
    assert "data/research/wallet_copy_live_execution_state.json" in candidates
    assert "data/research/wallet_copy_live_guard_state.json" in candidates
    assert "data/research/alpha_decay_13e0_f418_a689_overlap_20260722T0231Z_recap_terminal.json" in candidates
    for path in (
        "data/research/queue_remote_dataapi_fresh_flow_probe_checkpoint.json",
        "data/research/queue_remote_dataapi_fresh_flow_probe_latest.json",
    ):
        assert path in dr_preflight.SNAPSHOT_REGENERABLE_EXCLUDES
        assert "python3 scripts/probe_queue_remote_dataapi_fresh_flow.py" in (
            dr_preflight.SNAPSHOT_REGENERABLE_EXCLUDES[path]
        )


def test_dr_snapshot_push_uses_temp_index_and_snapshot_ref(monkeypatch) -> None:
    calls = []

    def fake_plan(*, max_file_bytes: int, max_total_bytes: int):
        return {
            "paths": ["docs/agents/HANDOFF.md"],
            "rejected": [],
            "policy_auto_excluded": [],
            "headroom_bytes": max_total_bytes - 123,
            "headroom_floor_bytes": dr_preflight.SNAPSHOT_HEADROOM_FLOOR,
            "headroom_below_floor": True,
            "auto_excludable_class_exhausted": True,
        }

    def fake_size(path):
        return 123

    def fake_run(args: list[str], *, env=None) -> subprocess.CompletedProcess[str]:
        calls.append({"args": args, "env_has_index": bool((env or {}).get("GIT_INDEX_FILE"))})
        if args[:2] == ["git", "write-tree"]:
            return subprocess.CompletedProcess(args, 0, "tree_sha\n", "")
        if args[:2] == ["git", "commit-tree"]:
            return subprocess.CompletedProcess(args, 0, "commit_sha\n", "")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(dr_preflight, "_snapshot_plan", fake_plan)
    monkeypatch.setattr(dr_preflight, "_path_size", fake_size)
    monkeypatch.setattr(dr_preflight, "_run", fake_run)

    result = dr_preflight.push_snapshot(
        remote="dr",
        snapshot_branch="dr-main",
        max_file_bytes=1000,
        max_total_bytes=10000,
    )

    assert result["status"] == "SNAPSHOT_PUSHED"
    assert result["commit_sha"] == "commit_sha"
    assert result["remote_ref"] == "refs/heads/dr-main"
    assert ["git", "read-tree", "--empty"] in [call["args"] for call in calls]
    assert ["git", "push", "--force-with-lease", "dr", "commit_sha:refs/heads/dr-main"] in [
        call["args"] for call in calls
    ]
    assert any(call["env_has_index"] for call in calls if call["args"][:2] != ["git", "push"])
    phase_names = [row["name"] for row in result["timing"]["phases"]]
    assert phase_names == [
        "snapshot_candidate_size_gate",
        "git_read_tree_empty",
        "git_add_snapshot_paths",
        "git_write_tree",
        "git_commit_tree",
        "git_push_snapshot",
    ]
    assert result["timing"]["total_s_before_return"] >= 0


def test_dr_policy_auto_excludes_marker_gated_states_largest_first() -> None:
    result = dr_preflight._apply_policy_auto_exclusions(
        [
            {
                "path": "data/research/small_state.json",
                "size_bytes": 20,
                "marker": "paper_only=true",
            },
            {
                "path": "data/research/large_latest.json",
                "size_bytes": 40,
                "marker": "live_orders_allowed=false",
            },
            {
                "path": "data/research/unmarked_state.json",
                "size_bytes": 50,
                "marker": None,
            },
        ],
        max_total_bytes=130,
        headroom_floor=50,
    )

    assert [row["path"] for row in result["policy_auto_excluded"]] == [
        "data/research/large_latest.json"
    ]
    assert result["headroom_bytes"] == 60
    assert result["headroom_below_floor"] is False


def test_dr_policy_deny_list_refuses_evidence_even_with_paper_marker() -> None:
    for path in (
        "data/research/live_orders_state.json",
        "data/research/wallet_decision_latest.json",
        "data/research/wallet_copy_live_guard_state.json",
        "data/research/state_digest.latest.json",
        "data/research/fills_events.jsonl",
        "data/research/fills_terminals.jsonl",
    ):
        assert dr_preflight._policy_auto_excludable(path, "paper_only=true") is False


def test_dr_policy_floor_is_advisory_but_cap_stays_binding() -> None:
    under_cap = dr_preflight._apply_policy_auto_exclusions(
        [
            {
                "path": "data/research/evidence.json",
                "size_bytes": 90,
                "marker": None,
            }
        ],
        max_total_bytes=100,
        headroom_floor=40,
    )
    assert under_cap["headroom_below_floor"] is True
    assert under_cap["auto_excludable_class_exhausted"] is True
    assert sum(row["size_bytes"] for row in under_cap["rows"]) <= 100

    over_cap = dr_preflight._apply_policy_auto_exclusions(
        [
            {
                "path": "data/research/evidence.json",
                "size_bytes": 110,
                "marker": None,
            }
        ],
        max_total_bytes=100,
        headroom_floor=40,
    )
    assert over_cap["headroom_below_floor"] is True
    assert over_cap["headroom_bytes"] == -10
    assert sum(row["size_bytes"] for row in over_cap["rows"]) > 100
