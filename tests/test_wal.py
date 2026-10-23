# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import time
from pathlib import Path

import pytest

from chronomap import (
    AsyncChronoMap,
    ChronoMap,
    ChronoMapKeyError,
    ChronoMapValueError,
    Journal,
    JournalCorruptionError,
    JournalError,
)
from chronomap._journal import MAGIC, write_checkpoint


def open_map(path, **kwargs):
    return ChronoMap(wal_path=path, enable_ttl_cleanup=False, **kwargs)


@pytest.mark.persistence
def test_wal_replays_every_mutation(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    cm.put("a", 1, timestamp=1)
    cm.put("a", 2, timestamp=2)
    cm.put("a", 0, timestamp=0)
    cm.put_many({"b": 3, "c": 4}, timestamp=3)
    cm.put_many({"g": 10}, timestamp=4, ttl=1000)
    cm.setdefault("d", 5)
    cm.get_or_set("e", lambda: 6)
    cm.put("ttl", 7, ttl=1000)
    cm.persist("ttl")
    cm.put("ttl", 8, ttl=2000)
    cm.prune_history("a", keep_last=2)
    cm.put("old", 1, timestamp=1)
    cm.put("old", 2, timestamp=2)
    cm.prune_history("old", older_than=2)
    cm.put("empty", 1, timestamp=1)
    cm.prune_history("empty", keep_last=0)
    cm.delete("c")
    cm.delete_many(["b", "missing"])
    cm.pop("d")
    cm.update({"f": 9})

    expected_store = {key: cm.history(key) for key in cm.keys()}
    expected_ttl = dict(cm._ttl)
    cm.close()

    recovered = open_map(path)
    try:
        assert {key: recovered.history(key) for key in recovered.keys()} == expected_store
        assert recovered._ttl == expected_ttl
        assert recovered.history("a") == [(1.0, 1), (2.0, 2)]
        assert recovered._wal.records == ()
    finally:
        recovered.close()


@pytest.mark.persistence
def test_wal_replays_popitem_and_clear(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    cm.put("a", 1, timestamp=1)
    cm.put("b", 2, timestamp=2)
    assert cm.popitem() == ("b", 2)
    cm.close()

    recovered = open_map(path)
    try:
        assert list(recovered.keys()) == ["a"]
    finally:
        recovered.close()

    recovered = open_map(path)
    recovered.clear()
    recovered.close()
    recovered = open_map(path)
    try:
        assert len(recovered) == 0
    finally:
        recovered.close()


@pytest.mark.persistence
def test_checkpoint_and_journal_tail_recover_together(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    cm.put("a", 1, timestamp=1)
    cm.checkpoint()
    checkpoint_path = tmp_path / "state.wal.checkpoint"
    assert checkpoint_path.exists()
    assert cm._wal.size == len(MAGIC)

    cm.put("a", 2, timestamp=2)
    cm.close()

    recovered = open_map(path)
    try:
        assert recovered.history("a") == [(1.0, 1), (2.0, 2)]
    finally:
        recovered.close()


@pytest.mark.persistence
def test_auto_checkpoint_compacts_journal(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path, wal_auto_checkpoint_bytes=1)
    cm.put("a", 1, timestamp=1)
    assert (tmp_path / "state.wal.checkpoint").exists()
    assert cm._wal.size == len(MAGIC)
    cm.close()

    recovered = open_map(path)
    try:
        assert recovered.get("a") == 1
    finally:
        recovered.close()


@pytest.mark.persistence
def test_auto_checkpoint_failure_keeps_the_journal(tmp_path, monkeypatch):
    path = tmp_path / "state.wal"
    cm = open_map(path, wal_auto_checkpoint_bytes=1)

    def fail():
        raise RuntimeError("checkpoint failed")

    monkeypatch.setattr(cm, "_checkpoint_unlocked", fail)
    cm.put("a", 1, timestamp=1)
    assert cm._wal.size > len(MAGIC)
    cm.close()


@pytest.mark.persistence
def test_wal_recovers_torn_tail_and_rejects_corruption(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    cm.put("a", 1, timestamp=1)
    cm.close()
    with open(path, "ab") as handle:
        handle.write(b"\x00\x00\x00")

    recovered = open_map(path)
    try:
        assert recovered.get("a") == 1
    finally:
        recovered.close()

    with open(path, "r+b") as handle:
        handle.seek(0, 2)
        handle.write(b"\x00")
    recovered = open_map(path)
    try:
        assert recovered.get("a") == 1
    finally:
        recovered.close()

    with open(path, "r+b") as handle:
        handle.seek(-1, 2)
        value = handle.read(1)
        handle.seek(-1, 2)
        handle.write(bytes([value[0] ^ 0xFF]))
    with pytest.raises(JournalCorruptionError):
        open_map(path)


@pytest.mark.persistence
def test_wal_replay_failure_releases_the_journal_lock(tmp_path):
    path = tmp_path / "state.wal"
    journal = Journal(path)
    journal.append("unknown", {})
    journal.close()

    with pytest.raises(JournalCorruptionError):
        open_map(path)
    with pytest.raises(JournalCorruptionError):
        open_map(path)


@pytest.mark.persistence
def test_wal_rejects_bad_checkpoint(tmp_path):
    path = tmp_path / "state.wal"
    bad_sequence = tmp_path / "state.wal.checkpoint"
    write_checkpoint(bad_sequence, {"sequence": "one", "store": {}, "ttl": {}})
    with pytest.raises(JournalCorruptionError):
        open_map(path)

    write_checkpoint(bad_sequence, {"sequence": 1, "store": [], "ttl": {}})
    with pytest.raises(JournalCorruptionError):
        open_map(path)

    write_checkpoint(bad_sequence, {"sequence": 1, "store": {}, "ttl": []})
    with pytest.raises(JournalCorruptionError):
        open_map(path)

    bad_sequence.write_bytes(b"broken")
    with pytest.raises(JournalCorruptionError):
        open_map(path)


@pytest.mark.persistence
def test_wal_rejects_malformed_records(tmp_path):
    cases = [
        ("unknown", {}),
        ("put", {"key": "a", "value": 1, "timestamp": True}),
        ("put", {"key": "a", "value": 1, "timestamp": 1, "expires_at": True}),
        ("replace", {"store": [], "ttl": {}}),
        ("merge", {"other_store": [], "other_ttl": {}, "strategy": "timestamp"}),
        ("merge", {"other_store": {}, "other_ttl": [], "strategy": "timestamp"}),
        ("merge", {"other_store": {}, "other_ttl": {}, "strategy": "unknown"}),
        ("put", {}),
    ]
    for index, (operation, data) in enumerate(cases):
        path = tmp_path / f"bad-{index}.wal"
        journal = Journal(path)
        journal.append(operation, data)
        journal.close()
        with pytest.raises(JournalCorruptionError):
            open_map(path)


@pytest.mark.persistence
def test_wal_merge_and_rollback_are_replayed(tmp_path):
    source = ChronoMap(enable_ttl_cleanup=False)
    source.put("k", 1, timestamp=1)
    source.put("k", 2, timestamp=2, ttl=1000)

    timestamp_path = tmp_path / "timestamp.wal"
    cm = open_map(timestamp_path)
    cm.put("k", 0, timestamp=0)
    cm.merge(source, "timestamp")
    cm.close()
    recovered = open_map(timestamp_path)
    try:
        assert recovered.history("k") == [(0.0, 0), (1.0, 1), (2.0, 2)]
    finally:
        recovered.close()

    overwrite_path = tmp_path / "overwrite.wal"
    cm = open_map(overwrite_path)
    cm.put("k", 9, timestamp=9)
    cm.merge(source, "overwrite")
    cm.close()
    recovered = open_map(overwrite_path)
    try:
        assert recovered.history("k") == [(1.0, 1), (2.0, 2)]
    finally:
        recovered.close()

    rollback_path = tmp_path / "rollback.wal"
    cm = open_map(rollback_path)
    cm.put("k", 1, timestamp=1)
    snapshot = cm.snapshot()
    cm.put("k", 2, timestamp=2)
    cm.rollback(snapshot)
    cm.close()
    recovered = open_map(rollback_path)
    try:
        assert recovered.history("k") == [(1.0, 1)]
        assert recovered.snapshot_time is None
    finally:
        recovered.close()


@pytest.mark.persistence
def test_wal_preserves_ttl_deadlines(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    cm.put("durable", 1, ttl=1000)
    cm.put("expiring", 2, ttl=0.01)
    cm.close()
    time.sleep(0.03)

    recovered = open_map(path)
    try:
        assert recovered.get("durable") == 1
        assert recovered.get("expiring") is None
        assert "durable" in recovered._ttl
        assert "expiring" in recovered._ttl
    finally:
        recovered.close()


@pytest.mark.persistence
def test_wal_replay_clears_expired_ttl_before_rewrite(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    cm.put("reborn", 1, ttl=0.01)
    time.sleep(0.03)
    cm.put("reborn", 2)
    cm.close()

    recovered = open_map(path)
    try:
        assert recovered.get("reborn") == 2
        assert "reborn" not in recovered._ttl
    finally:
        recovered.close()


@pytest.mark.persistence
def test_wal_replay_clears_expired_ttl_before_put_many(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    cm.put("reborn", 1, ttl=0.01)
    time.sleep(0.03)
    cm.put_many({"reborn": 2})
    cm.close()

    recovered = open_map(path)
    try:
        assert recovered.get("reborn") == 2
        assert "reborn" not in recovered._ttl
    finally:
        recovered.close()


@pytest.mark.persistence
def test_wal_replay_clears_expired_ttl_before_setdefault(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    cm.put("reborn", 1, ttl=0.01)
    time.sleep(0.03)
    assert cm.setdefault("reborn", 2) == 2
    cm.close()

    recovered = open_map(path)
    try:
        assert recovered.get("reborn") == 2
        assert "reborn" not in recovered._ttl
    finally:
        recovered.close()


@pytest.mark.persistence
def test_wal_replay_clears_keys_removed_by_cleanup(tmp_path):
    path = tmp_path / "state.wal"
    cm = open_map(path)
    cm.put("gone", 1, ttl=0.01)
    time.sleep(0.03)
    assert cm.clean_expired_keys() == 1
    cm.put("gone", 2)
    cm.close()

    recovered = open_map(path)
    try:
        assert recovered.get("gone") == 2
        assert "gone" not in recovered._ttl
    finally:
        recovered.close()


@pytest.mark.persistence
def test_from_dict_with_wal_persists_imported_state(tmp_path):
    source = ChronoMap(enable_ttl_cleanup=False)
    source.put("imported", 1, ttl=1000)
    data = source.to_dict()
    data["snapshot_time"] = 123.0
    source.close()

    path = tmp_path / "state.wal"
    imported = ChronoMap.from_dict(data, wal_path=path, enable_ttl_cleanup=False)
    imported.close()

    recovered = open_map(path)
    try:
        assert recovered.get("imported") == 1
        assert "imported" in recovered._ttl
        assert recovered.snapshot_time == 123.0
    finally:
        recovered.close()


@pytest.mark.persistence
def test_failed_wal_import_closes_the_journal(tmp_path):
    path = tmp_path / "state.wal"
    data = {
        "store": {"bad": [(1.0, lambda: None)]},
        "ttl": {},
        "snapshot_time": None,
        "version": 1,
        "max_history": None,
    }
    with pytest.raises(JournalError):
        ChronoMap.from_dict(data, wal_path=path, enable_ttl_cleanup=False)

    recovered = open_map(path)
    try:
        assert "bad" not in recovered
    finally:
        recovered.close()


@pytest.mark.persistence
def test_wal_rejects_unpicklable_values_without_mutating(tmp_path):
    cm = open_map(tmp_path / "state.wal")
    with pytest.raises(JournalError):
        cm.put("bad", lambda: None)
    assert "bad" not in cm
    cm.close()


@pytest.mark.persistence
def test_checkpoint_requires_a_journal(tmp_path):
    cm = ChronoMap(enable_ttl_cleanup=False)
    with pytest.raises(JournalError):
        cm.checkpoint()
    with pytest.raises(JournalError):
        cm._journal_checkpoint_path()
    with pytest.raises(JournalError):
        cm._checkpoint_unlocked()


@pytest.mark.persistence
def test_checkpoint_sequence_skips_old_journal_records(tmp_path):
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
        assert cm.get("a") == "checkpoint"
    finally:
        cm.close()


@pytest.mark.persistence
@pytest.mark.parametrize(
    "prune",
    [
        {"key": "a", "keep_last": None, "cutoff": True},
        {"key": "a", "keep_last": -1, "cutoff": None},
        {"key": "a", "keep_last": True, "cutoff": None},
    ],
)
def test_wal_replay_validates_prune_records(tmp_path, prune):
    path = tmp_path / "state.wal"
    journal = Journal(path)
    journal.append("prune", {"key": "missing", "keep_last": None, "cutoff": None})
    journal.append("put", {"key": "a", "value": 1, "timestamp": 1})
    journal.append("prune", prune)
    journal.close()
    with pytest.raises(JournalCorruptionError):
        open_map(path)


@pytest.mark.persistence
def test_close_stops_cleanup_and_destructor_tolerates_journal_errors(tmp_path):
    cm = ChronoMap(wal_path=tmp_path / "state.wal", enable_ttl_cleanup=True)
    cm.close()

    cm = ChronoMap(wal_path=tmp_path / "other.wal", enable_ttl_cleanup=False)

    def fail_close():
        raise JournalError("already broken")

    cm._wal.close = fail_close
    cm.__del__()


@pytest.mark.core
def test_empty_history_defensive_paths():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm._store["empty"] = []
    with pytest.raises(ChronoMapKeyError):
        cm.popitem()


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_empty_history_defensive_path():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    cm._store["empty"] = []
    with pytest.raises(ChronoMapKeyError):
        await cm.popitem()


@pytest.mark.persistence
def test_wal_settings_validation(tmp_path):
    path = tmp_path / "state.wal"
    with pytest.raises(ChronoMapValueError):
        ChronoMap(wal_path=path, wal_sync=1)
    with pytest.raises(ChronoMapValueError):
        ChronoMap(wal_path=path, wal_auto_checkpoint_bytes=0)
    with pytest.raises(ChronoMapValueError):
        ChronoMap(wal_path=path, wal_auto_checkpoint_bytes=True)


@pytest.mark.persistence
def test_wal_stats_close_and_context_manager(tmp_path):
    path = tmp_path / "state.wal"
    cm = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    cm.put("a", 1, timestamp=1)
    stats = cm.get_stats()
    assert stats["journal_path"] == str(path)
    assert stats["journal_bytes"] > len(MAGIC)
    assert stats["journal_sequence"] >= 1
    cm.close()
    cm.close()

    with ChronoMap(wal_path=path, enable_ttl_cleanup=False) as recovered:
        assert recovered.get("a") == 1
    with pytest.raises(JournalError):
        recovered.put("b", 2)
    with pytest.raises(JournalError):
        recovered.checkpoint()
    assert not Path(str(path) + ".checkpoint").exists()
