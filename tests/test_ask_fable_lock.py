from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path


def test_ask_fable_refuses_concurrent_lock(tmp_path: Path):
    lock_dir = tmp_path / "ask_fable.lock.d"
    lock_dir.mkdir()
    sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        (lock_dir / "pid").write_text(f"{sleeper.pid}\n", encoding="utf-8")
        env = {**os.environ, "ASK_FABLE_LOCK_DIR": str(lock_dir)}
        result = subprocess.run(
            ["bash", "scripts/ask_fable.sh", "lock smoke test"],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
            check=False,
        )
        assert result.returncode == 75
        assert f"pid={sleeper.pid}" in result.stdout
        assert "refusing concurrent brain call" in result.stdout
    finally:
        sleeper.terminate()
        try:
            sleeper.wait(timeout=5)
        except subprocess.TimeoutExpired:
            sleeper.kill()


def test_fable_pulse_defers_young_live_ask_fable_lock(tmp_path: Path):
    lock_dir = tmp_path / "ask_fable.lock.d"
    pulse_lock = tmp_path / "pulse.lock"
    pulse_log = tmp_path / "fable_pulse.log"
    lock_dir.mkdir()
    sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        (lock_dir / "pid").write_text(f"{sleeper.pid}\n", encoding="utf-8")
        env = {
            **os.environ,
            "ASK_FABLE_LOCK_DIR": str(lock_dir),
            "FABLE_PULSE_LOCKDIR": str(pulse_lock),
            "FABLE_PULSE_LOG": str(pulse_log),
            "FABLE_PULSE_BUSY_RETRY_S": "0",
            "FABLE_PULSE_INTERVAL_S": "3600",
            "FABLE_PULSE_PREPARE_ONLY": "1",
        }
        result = subprocess.run(
            ["bash", "scripts/fable_pulse.sh"],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
            check=False,
        )
        assert result.returncode == 0
        assert lock_dir.exists()
        log_text = pulse_log.read_text(encoding="utf-8")
        assert "PULSE_BUSY_RETRY" in log_text
        assert "PULSE_DEFER_BUSY" in log_text
    finally:
        sleeper.terminate()
        try:
            sleeper.wait(timeout=5)
        except subprocess.TimeoutExpired:
            sleeper.kill()


def test_fable_pulse_takes_over_stale_live_ask_fable_lock(tmp_path: Path):
    lock_dir = tmp_path / "ask_fable.lock.d"
    pulse_lock = tmp_path / "pulse.lock"
    pulse_log = tmp_path / "fable_pulse.log"
    lock_dir.mkdir()
    sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        (lock_dir / "pid").write_text(f"{sleeper.pid}\n", encoding="utf-8")
        old = time.time() - 7200
        os.utime(lock_dir, (old, old))
        env = {
            **os.environ,
            "ASK_FABLE_LOCK_DIR": str(lock_dir),
            "FABLE_PULSE_LOCKDIR": str(pulse_lock),
            "FABLE_PULSE_LOG": str(pulse_log),
            "FABLE_PULSE_INTERVAL_S": "3600",
            "FABLE_PULSE_PREPARE_ONLY": "1",
        }
        result = subprocess.run(
            ["bash", "scripts/fable_pulse.sh"],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
            check=False,
        )
        assert result.returncode == 0
        assert not lock_dir.exists()
        assert sleeper.poll() is not None
        log_text = pulse_log.read_text(encoding="utf-8")
        assert "PULSE_STALE_LOCK_TAKEOVER" in log_text
    finally:
        if sleeper.poll() is None:
            sleeper.terminate()
            try:
                sleeper.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sleeper.kill()
