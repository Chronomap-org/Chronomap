# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import os
import subprocess
import sys
from pathlib import Path

import pytest

from chronomap import (
    AsyncChronoMap,
    ChronoMap,
    Journal,
    JournalCorruptionError,
    JournalError,
    JournalLockError,
)
from chronomap._journal import MAGIC

pytestmark = pytest.mark.persistence


def subprocess_environment():
    root = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    source_path = str(root / "src")
    existing_path = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = source_path + os.pathsep + existing_path if existing_path else source_path
    return environment


def open_map(path, **kwargs):
    return ChronoMap(wal_path=path, enable_ttl_cleanup=False, **kwargs)


def test_second_journal_on_the_same_path_is_rejected(tmp_path):
    path = tmp_path / "state.wal"
    journal = Journal(path, sync=False)
    try:
        with pytest.raises(JournalLockError, match="locked by another process"):
            Journal(path)
    finally:
        journal.close()
    journal.close()

    reopened = Journal(path)
    assert reopened.records == ()
    reopened.close()


def test_journal_destructor_releases_the_lock(tmp_path):
    path = tmp_path / "state.wal"
    journal = Journal(path)
    journal.__del__()

    reopened = Journal(path)
    reopened.close()


def test_map_lock_survives_checkpoint_and_releases_on_close(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    cm.put("owner", "sync")
    cm.checkpoint()

    with pytest.raises(JournalLockError):
        open_map(path)

    cm.close()
    recovered = open_map(path)
    try:
        assert recovered.get("owner") == "sync"
    finally:
        recovered.close()


async def test_async_map_lock_blocks_a_second_writer(tmp_path):
    path = tmp_path / "state.wal"
    cm = AsyncChronoMap(wal_path=path, enable_ttl_cleanup=False)
    await cm.put("owner", "async")
    try:
        with pytest.raises(JournalLockError):
            AsyncChronoMap(wal_path=path, enable_ttl_cleanup=False)
    finally:
        await cm.aclose()

    recovered = AsyncChronoMap(wal_path=path, enable_ttl_cleanup=False)
    try:
        assert await recovered.get("owner") == "async"
    finally:
        await recovered.aclose()


def test_a_live_process_holds_the_lock(tmp_path):
    path = tmp_path / "state.wal"
    script = """
import sys
from chronomap import ChronoMap

cm = ChronoMap(wal_path=sys.argv[1], enable_ttl_cleanup=False)
cm.put("owner", "child")
print("ready", flush=True)
sys.stdin.readline()
cm.close()
"""
    holder = subprocess.Popen(
        [sys.executable, "-c", script, str(path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=subprocess_environment(),
    )
    stderr = ""
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "ready"
        with pytest.raises(JournalLockError):
            open_map(path)
    finally:
        if holder.poll() is None:
            assert holder.stdin is not None
            try:
                holder.stdin.write("\n")
                holder.stdin.flush()
            except (BrokenPipeError, OSError):
                pass
        try:
            _, stderr = holder.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            holder.kill()
            _, stderr = holder.communicate()

    assert holder.returncode == 0, stderr
    recovered = open_map(path)
    try:
        assert recovered.get("owner") == "child"
    finally:
        recovered.close()


def test_lock_is_released_after_an_abrupt_process_exit(tmp_path):
    path = tmp_path / "state.wal"
    script = """
import os
import sys
from chronomap import ChronoMap

cm = ChronoMap(wal_path=sys.argv[1], enable_ttl_cleanup=False)
cm.put("owner", "crash")
os._exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(path)],
        capture_output=True,
        text=True,
        env=subprocess_environment(),
        timeout=30,
    )
    assert result.returncode == 0, result.stderr

    recovered = open_map(path)
    try:
        assert recovered.get("owner") == "crash"
    finally:
        recovered.close()


def test_failed_journal_open_releases_the_lock(tmp_path):
    path = tmp_path / "state.wal"
    path.write_bytes(b"not-a-journal")
    with pytest.raises(JournalCorruptionError):
        Journal(path)

    path.write_bytes(MAGIC)
    reopened = Journal(path)
    assert reopened.records == ()
    reopened.close()


def test_unopenable_lock_file_raises_a_journal_error(tmp_path):
    path = tmp_path / "state.wal"
    lock_path = path.with_name(path.name + ".lock")
    lock_path.mkdir()
    with pytest.raises(JournalError, match="Could not open journal lock file"):
        Journal(path)
