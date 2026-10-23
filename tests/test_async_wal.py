# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import asyncio
import math
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from chronomap import (
    AsyncChronoMap,
    ChronoMapValueError,
    Journal,
    JournalCorruptionError,
    JournalError,
)
from chronomap._journal import MAGIC, write_checkpoint

pytestmark = [pytest.mark.persistence, pytest.mark.async_map]


def open_map(path, **kwargs):
    return AsyncChronoMap(wal_path=path, enable_ttl_cleanup=False, **kwargs)


def run_async_crash_writer(path, body):
    root = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    source_path = str(root / "src")
    existing_path = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = source_path + os.pathsep + existing_path if existing_path else source_path
    indented = textwrap.indent(textwrap.dedent(body).strip(), "    ")
    script = (
        "import asyncio\n"
        "import os\n"
        "import sys\n"
        "\n"
        "from chronomap import AsyncChronoMap\n"
        "\n"
        "async def main():\n"
        "    cm = AsyncChronoMap(wal_path=sys.argv[1], enable_ttl_cleanup=False)\n" + indented + "\n"
        "    os._exit(0)\n"
        "\n"
        "asyncio.run(main())\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(path)],
        capture_output=True,
        text=True,
        env=environment,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


async def test_async_wal_replays_every_mutation(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    await cm.put("a", 1, timestamp=1)
    await cm.put("a", 2, timestamp=2)
    await cm.put("a", 0, timestamp=0)
    await cm.put_many({"b": 3, "c": 4}, timestamp=3)
    await cm.put_many({"g": 10}, timestamp=4, ttl=1000)
    await cm.setdefault("d", 5)
    await cm.get_or_set("e", lambda: 6)
    await cm.put("ttl", 7, ttl=1000)
    await cm.persist("ttl")
    await cm.put("ttl", 8, ttl=2000)
    await cm.prune_history("a", keep_last=2)
    await cm.put("old", 1, timestamp=1)
    await cm.put("old", 2, timestamp=2)
    await cm.prune_history("old", older_than=2)
    await cm.put("empty", 1, timestamp=1)
    await cm.prune_history("empty", keep_last=0)
    await cm.delete("c")
    await cm.delete_many(["b", "missing"])
    await cm.pop("d")
    await cm.update({"f": 9})

    expected_store = {key: await cm.history(key) for key in await cm.keys()}
    expected_ttl = dict(cm._ttl)
    await cm.aclose()

    recovered = open_map(path)
    try:
        assert {key: await recovered.history(key) for key in await recovered.keys()} == expected_store
        assert recovered._ttl == expected_ttl
        assert await recovered.history("a") == [(1.0, 1), (2.0, 2)]
        assert recovered._wal.records == ()
    finally:
        await recovered.aclose()


async def test_async_wal_replays_popitem_and_clear(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    await cm.put("a", 1, timestamp=1)
    await cm.put("b", 2, timestamp=2)
    assert await cm.popitem() == ("b", 2)
    await cm.aclose()

    recovered = open_map(path)
    try:
        assert list(await recovered.keys()) == ["a"]
    finally:
        await recovered.aclose()

    recovered = open_map(path)
    await recovered.clear()
    await recovered.aclose()

    recovered = open_map(path)
    try:
        assert await recovered.keys() == []
    finally:
        await recovered.aclose()


async def test_async_wal_replays_expiry_cleanup_before_rewrites(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    for key in ("put", "many", "get_or_set", "setdefault", "delete", "delete_many", "persist"):
        await cm.put(key, 1, ttl=0.01)
    await cm.put("pop", 1, ttl=0.01)
    await cm.put("popitem", 1, ttl=0.01)
    await asyncio.sleep(0.03)

    await cm.put("put", 2, timestamp=2)
    await cm.put_many({"many": 2}, timestamp=2)
    assert await cm.get_or_set("get_or_set", lambda: 2) == 2
    assert await cm.setdefault("setdefault", 2) == 2
    await cm.put("live", "kept", timestamp=1)
    assert await cm.pop("pop", None) is None
    assert await cm.popitem() == ("live", "kept")
    assert await cm.delete("delete") is False
    assert await cm.delete_many(["delete_many"]) == 0
    assert await cm.persist("persist") is False
    assert await cm.clean_expired_keys() == 1
    await cm.aclose()

    recovered = open_map(path)
    try:
        assert await recovered.get("put") == 2
        assert await recovered.get("many") == 2
        assert await recovered.get("get_or_set") == 2
        assert await recovered.get("setdefault") == 2
        for key in ("put", "many", "get_or_set", "setdefault", "pop", "popitem"):
            assert key not in recovered._ttl
        for key in ("delete", "delete_many", "persist"):
            assert key not in recovered._store
            assert key not in recovered._ttl
    finally:
        await recovered.aclose()


async def test_async_checkpoint_and_journal_tail_recover_together(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    await cm.put("a", 1, timestamp=1)
    await cm.checkpoint()
    checkpoint_path = tmp_path / "state.wal.checkpoint"
    assert checkpoint_path.exists()
    assert cm._wal.size == len(MAGIC)

    await cm.put("a", 2, timestamp=2)
    await cm.aclose()

    recovered = open_map(path)
    try:
        assert await recovered.history("a") == [(1.0, 1), (2.0, 2)]
    finally:
        await recovered.aclose()


async def test_async_auto_checkpoint_compacts_journal(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path, wal_auto_checkpoint_bytes=1)
    await cm.put("a", 1, timestamp=1)
    assert (tmp_path / "state.wal.checkpoint").exists()
    assert cm._wal.size == len(MAGIC)
    await cm.aclose()

    recovered = open_map(path)
    try:
        assert await recovered.get("a") == 1
    finally:
        await recovered.aclose()


async def test_async_auto_checkpoint_failure_keeps_the_journal(tmp_path, monkeypatch):
    path = tmp_path / "state.wal"
    cm = open_map(path, wal_auto_checkpoint_bytes=1)

    def fail():
        raise RuntimeError("checkpoint failed")

    monkeypatch.setattr(cm, "_checkpoint_unlocked", fail)
    await cm.put("a", 1, timestamp=1)
    assert cm._wal.size > len(MAGIC)
    cm._maybe_checkpoint_unlocked()
    await cm.aclose()


async def test_async_wal_recovers_torn_tail_and_rejects_corruption(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    await cm.put("a", 1, timestamp=1)
    await cm.aclose()
    with open(path, "ab") as handle:
        handle.write(b"\x00\x00\x00")

    recovered = open_map(path)
    try:
        assert await recovered.get("a") == 1
    finally:
        await recovered.aclose()

    with open(path, "r+b") as handle:
        handle.seek(0, 2)
        handle.write(b"\x00")
    recovered = open_map(path)
    try:
        assert await recovered.get("a") == 1
    finally:
        await recovered.aclose()

    with open(path, "r+b") as handle:
        handle.seek(-1, 2)
        value = handle.read(1)
        handle.seek(-1, 2)
        handle.write(bytes([value[0] ^ 0xFF]))
    with pytest.raises(JournalCorruptionError):
        open_map(path)


@pytest.mark.parametrize(
    "state",
    [
        {"sequence": "one", "store": {}, "ttl": {}},
        {"sequence": 1, "store": [], "ttl": {}},
        {"sequence": 1, "store": {}, "ttl": []},
    ],
)
async def test_async_wal_rejects_bad_checkpoint_state(tmp_path, state):
    path = tmp_path / "state.wal"
    write_checkpoint(tmp_path / "state.wal.checkpoint", state)
    with pytest.raises(JournalCorruptionError):
        open_map(path)


async def test_async_wal_rejects_broken_checkpoint_bytes(tmp_path):
    path = tmp_path / "state.wal"
    (tmp_path / "state.wal.checkpoint").write_bytes(b"broken")
    with pytest.raises(JournalCorruptionError):
        open_map(path)


@pytest.mark.parametrize(
    ("operation", "data"),
    [
        ("unknown", {}),
        ("put", {"key": "a", "value": 1, "timestamp": True}),
        ("put", {"key": "a", "value": 1, "timestamp": math.inf}),
        ("put", {"key": "a", "value": 1, "timestamp": 1, "expires_at": True}),
        ("put", {"key": "a", "value": 1, "timestamp": 1, "expires_at": math.nan}),
        ("put", {}),
        ("replace", {"store": [], "ttl": {}}),
        ("merge", {"other_store": [], "other_ttl": {}, "strategy": "timestamp"}),
        ("merge", {"other_store": {}, "other_ttl": [], "strategy": "timestamp"}),
        ("merge", {"other_store": {}, "other_ttl": {}, "strategy": "unknown"}),
    ],
)
async def test_async_wal_rejects_malformed_records(tmp_path, operation, data):
    path = tmp_path / "state.wal"
    journal = Journal(path)
    journal.append(operation, data)
    journal.close()
    with pytest.raises(JournalCorruptionError):
        open_map(path)


@pytest.mark.parametrize(
    "prune",
    [
        {"key": "a", "keep_last": None, "cutoff": True},
        {"key": "a", "keep_last": None, "cutoff": math.inf},
        {"key": "a", "keep_last": -1, "cutoff": None},
        {"key": "a", "keep_last": True, "cutoff": None},
    ],
)
async def test_async_wal_replay_validates_prune_records(tmp_path, prune):
    path = tmp_path / "state.wal"
    journal = Journal(path)
    journal.append("prune", {"key": "missing", "keep_last": None, "cutoff": None})
    journal.append("put", {"key": "a", "value": 1, "timestamp": 1})
    journal.append("prune", prune)
    journal.close()
    with pytest.raises(JournalCorruptionError):
        open_map(path)


async def test_async_wal_replays_merge_rollback_and_replacement(tmp_path):
    source = AsyncChronoMap(enable_ttl_cleanup=False)
    await source.put("k", 1, timestamp=1)
    await source.put("k", 2, timestamp=2, ttl=1000)
    source_ttl = dict(source._ttl)

    timestamp_path = tmp_path / "timestamp.wal"
    cm = open_map(timestamp_path)
    await cm.put("k", 0, timestamp=0)
    await cm.merge(source, "timestamp")
    await cm.aclose()
    recovered = open_map(timestamp_path)
    try:
        assert await recovered.history("k") == [(0.0, 0), (1.0, 1), (2.0, 2)]
        assert recovered._ttl == source_ttl
    finally:
        await recovered.aclose()

    overwrite_path = tmp_path / "overwrite.wal"
    cm = open_map(overwrite_path)
    await cm.put("k", 9, timestamp=9)
    await cm.merge(source, "overwrite")
    await cm.aclose()
    recovered = open_map(overwrite_path)
    try:
        assert await recovered.history("k") == [(1.0, 1), (2.0, 2)]
        assert recovered._ttl == source_ttl
    finally:
        await recovered.aclose()

    rollback_path = tmp_path / "rollback.wal"
    cm = open_map(rollback_path)
    await cm.put("k", 1, timestamp=1)
    snapshot = await cm.snapshot()
    await cm.put("k", 2, timestamp=2)
    await cm.rollback(snapshot)
    await cm.aclose()
    recovered = open_map(rollback_path)
    try:
        assert await recovered.history("k") == [(1.0, 1)]
        assert recovered.snapshot_time is None
    finally:
        await recovered.aclose()


async def test_async_from_dict_with_wal_persists_imported_state(tmp_path):
    path = tmp_path / "state.wal"
    data = {
        "store": {"imported": [(1, "value")]},
        "ttl": {"imported": 2_000_000_000},
        "snapshot_time": 123,
        "version": 1,
        "max_history": None,
    }
    imported = AsyncChronoMap.from_dict(data, wal_path=path)
    await imported.aclose()

    recovered = open_map(path)
    try:
        assert await recovered.get("imported") == "value"
        assert recovered._ttl["imported"] == 2_000_000_000.0
        assert recovered.snapshot_time == 123.0
    finally:
        await recovered.aclose()


async def test_async_from_dict_with_wal_checkpoints_imported_state(tmp_path):
    path = tmp_path / "state.wal"
    data = {"store": {"imported": [(1, "value")]}, "ttl": {}, "snapshot_time": None}
    imported = AsyncChronoMap.from_dict(
        data,
        wal_path=path,
        wal_auto_checkpoint_bytes=1,
    )
    assert (tmp_path / "state.wal.checkpoint").exists()
    await imported.aclose()

    recovered = open_map(path)
    try:
        assert await recovered.get("imported") == "value"
    finally:
        await recovered.aclose()


async def test_async_from_dict_with_wal_replaces_existing_state(tmp_path):
    path = tmp_path / "state.wal"
    original = open_map(path)
    await original.put("old", 1, timestamp=1)
    await original.aclose()

    data = {"store": {"new": [(2, "value")]}, "ttl": {}, "snapshot_time": None}
    imported = AsyncChronoMap.from_dict(data, wal_path=path)
    await imported.aclose()

    recovered = open_map(path)
    try:
        assert await recovered.get("old") is None
        assert await recovered.get("new") == "value"
    finally:
        await recovered.aclose()


async def test_failed_async_wal_import_closes_the_journal(tmp_path):
    path = tmp_path / "state.wal"
    data = {
        "store": {"bad": [(1.0, lambda: None)]},
        "ttl": {},
        "snapshot_time": None,
        "version": 1,
        "max_history": None,
    }
    with pytest.raises(JournalError):
        AsyncChronoMap.from_dict(data, wal_path=path)

    recovered = open_map(path)
    try:
        assert await recovered.get("bad") is None
        assert "bad" not in recovered._store
    finally:
        await recovered.aclose()


async def test_async_checkpoint_sequence_skips_old_journal_records(tmp_path):
    path = tmp_path / "state.wal"
    journal = Journal(path)
    journal.append("put", {"key": "a", "value": "new", "timestamp": 1})
    journal.close()
    write_checkpoint(
        tmp_path / "state.wal.checkpoint",
        {"sequence": 1, "store": {"a": [(1.0, "checkpoint")]}, "ttl": {}},
    )

    cm = open_map(path)
    try:
        assert await cm.get("a") == "checkpoint"
    finally:
        await cm.aclose()


async def test_async_wal_rejects_unpicklable_values_without_mutating(tmp_path):
    cm = open_map(tmp_path / "state.wal")
    with pytest.raises(JournalError):
        await cm.put("bad", lambda: None)
    assert await cm.get("bad") is None
    assert "bad" not in cm._store
    await cm.aclose()


async def test_async_checkpoint_requires_a_journal(tmp_path):
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    with pytest.raises(JournalError):
        await cm.checkpoint()
    with pytest.raises(JournalError):
        cm._journal_checkpoint_path()
    with pytest.raises(JournalError):
        cm._checkpoint_unlocked()


async def test_async_wal_configuration_validation(tmp_path):
    path = tmp_path / "state.wal"
    with pytest.raises(ChronoMapValueError):
        open_map(path, wal_sync=1)
    with pytest.raises(ChronoMapValueError):
        open_map(path, wal_auto_checkpoint_bytes=0)
    with pytest.raises(ChronoMapValueError):
        open_map(path, wal_auto_checkpoint_bytes=True)


async def test_async_wal_stats_close_and_destructor(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    await cm.put("a", 1, timestamp=1)
    stats = cm.get_stats()
    assert stats["journal_path"] == str(path)
    assert stats["journal_bytes"] > len(MAGIC)
    assert stats["journal_sequence"] >= 1
    await cm.aclose()
    await cm.aclose()

    with pytest.raises(JournalError):
        await cm.put("b", 2)
    with pytest.raises(JournalError):
        await cm.checkpoint()

    other = open_map(tmp_path / "other.wal")
    other.__del__()
    AsyncChronoMap(enable_ttl_cleanup=False).__del__()

    broken = open_map(tmp_path / "broken.wal")

    def fail_close():
        raise JournalError("already broken")

    original_close = broken._wal.close
    broken._wal.close = fail_close
    broken.__del__()
    broken._wal.close = original_close
    broken.__del__()


async def test_async_wal_survives_an_abrupt_process_exit(tmp_path):
    path = tmp_path / "state.wal"
    run_async_crash_writer(
        path,
        'await cm.put("acked", {"value": 1}, timestamp=10)',
    )

    recovered = open_map(path)
    try:
        assert await recovered.get("acked") == {"value": 1}
    finally:
        await recovered.aclose()


async def test_async_wal_survives_an_abrupt_exit_after_checkpoint(tmp_path):
    path = tmp_path / "state.wal"
    run_async_crash_writer(
        path,
        'await cm.put("before", 1, timestamp=10)\n'
        "await cm.checkpoint()\n"
        'await cm.put("after", 2, timestamp=20)',
    )

    recovered = open_map(path)
    try:
        assert await recovered.get("before") == 1
        assert await recovered.get("after") == 2
    finally:
        await recovered.aclose()
