# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import os
import pickle
import struct
import zlib

import pytest

from chronomap import Journal, JournalCorruptionError, JournalError
from chronomap import _journal as journal_module
from chronomap._journal import (
    CHECKPOINT_MAGIC,
    MAGIC,
    _fsync_directory,
    read_checkpoint,
    write_checkpoint,
)


def make_frame(record, *, checksum=None, length=None):
    payload = pickle.dumps(record, protocol=4)
    frame_length = len(payload) if length is None else length
    frame_checksum = zlib.crc32(payload) & 0xFFFFFFFF if checksum is None else checksum
    return struct.pack(">II", frame_length, frame_checksum) + payload


@pytest.mark.persistence
def test_journal_append_replay_and_reset(tmp_path):
    path = tmp_path / "state.wal"
    journal = Journal(path, sync=False)
    assert journal.sequence == 0
    assert journal.records == ()
    assert journal.size == len(MAGIC)

    assert journal.append("put", {"key": "a", "value": 1}) == 1
    journal.advance_to(3)
    assert journal.append("delete", {"key": "b"}) == 4
    assert journal.sequence == 4
    journal.close()
    journal.close()

    reopened = Journal(path)
    assert [record[0] for record in reopened.records] == [1, 4]
    assert reopened.records[1][1] == "delete"
    reopened.clear_records()
    assert reopened.records == ()
    assert reopened.sequence == 4
    reopened.reset()
    assert reopened.records == ()
    assert reopened.size == len(MAGIC)
    assert reopened.sequence == 4
    reopened.close()

    with pytest.raises(JournalError):
        reopened.append("put", {})
    with pytest.raises(JournalError):
        reopened.reset()


@pytest.mark.persistence
def test_journal_streams_records_without_retaining_them(tmp_path):
    path = tmp_path / "state.wal"
    journal = Journal(path, sync=False)
    journal.append("put", {"key": "a", "value": 1})
    journal.append("put", {"key": "b", "value": 2})
    journal.append("delete", {"key": "a"})
    journal.close()

    seen = []
    reopened = Journal(
        path,
        retain_records=False,
        record_consumer=lambda sequence, operation, data: seen.append((sequence, operation, data)),
    )
    try:
        assert reopened.records == ()
        assert [record[0] for record in seen] == [1, 2, 3]
        assert seen[2][1] == "delete"
        assert reopened.sequence == 3
    finally:
        reopened.close()


@pytest.mark.persistence
def test_journal_skips_checkpointed_records_for_the_consumer(tmp_path):
    path = tmp_path / "state.wal"
    journal = Journal(path, sync=False)
    journal.append("put", {"key": "a", "value": 1})
    journal.append("put", {"key": "b", "value": 2})
    journal.append("delete", {"key": "a"})
    journal.close()

    seen = []
    reopened = Journal(
        path,
        retain_records=False,
        skip_through=2,
        record_consumer=lambda sequence, operation, data: seen.append(sequence),
    )
    try:
        assert seen == [3]
        assert reopened.sequence == 3
    finally:
        reopened.close()


@pytest.mark.persistence
def test_journal_rejects_invalid_recovery_options(tmp_path):
    path = tmp_path / "state.wal"
    with pytest.raises(JournalError):
        Journal(path, retain_records=1)
    with pytest.raises(JournalError):
        Journal(path, skip_through=-1)
    with pytest.raises(JournalError):
        Journal(path, skip_through=True)
    with pytest.raises(JournalError):
        Journal(path, record_consumer=object())


@pytest.mark.persistence
def test_journal_consumer_failure_releases_the_path_lock(tmp_path):
    path = tmp_path / "state.wal"
    journal = Journal(path, sync=False)
    journal.append("put", {"key": "a", "value": 1})
    journal.close()

    def fail(sequence, operation, data):
        raise RuntimeError("consumer failed")

    with pytest.raises(RuntimeError, match="consumer failed"):
        Journal(path, record_consumer=fail)

    reopened = Journal(path)
    try:
        assert len(reopened.records) == 1
    finally:
        reopened.close()


@pytest.mark.persistence
def test_journal_context_manager(tmp_path):
    path = tmp_path / "state.wal"
    with Journal(path) as journal:
        journal.append("clear", {})
    reopened = Journal(path)
    assert reopened.sequence == 1
    reopened.close()


@pytest.mark.persistence
def test_journal_rejects_invalid_records(tmp_path, monkeypatch):
    journal = Journal(tmp_path / "state.wal")
    with pytest.raises(JournalError):
        journal.append("", {})
    with pytest.raises(JournalError):
        journal.append("put", [])
    with pytest.raises(JournalError):
        journal.append("put", {"value": lambda: None})
    assert journal.sequence == 0

    monkeypatch.setattr("chronomap._journal.MAX_RECORD_BYTES", 8)
    with pytest.raises(JournalError):
        journal.append("put", {"value": "too large"})
    path = journal.path
    journal.close()
    path.unlink()
    assert journal.size == 0


@pytest.mark.persistence
def test_journal_truncates_a_failed_append(tmp_path):
    path = tmp_path / "state.wal"
    journal = Journal(path)
    original = journal._file

    class FailingHandle:
        def seek(self, *args):
            return original.seek(*args)

        def write(self, data):
            raise OSError("disk full")

        def truncate(self, *args):
            return original.truncate(*args)

    journal._file = FailingHandle()
    try:
        with pytest.raises(OSError):
            journal.append("put", {"value": 1})
    finally:
        journal._file = original
    assert journal.sequence == 0
    journal.close()
    reopened = Journal(path)
    assert reopened.records == ()
    reopened.close()


@pytest.mark.persistence
def test_journal_reset_failure_keeps_the_journal_usable(tmp_path, monkeypatch):
    path = tmp_path / "state.wal"
    journal = Journal(path)
    journal.append("put", {"key": "a", "value": 1})

    def fail(*args):
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(OSError):
        journal.reset()
    assert list(tmp_path.glob("*.tmp")) == []
    assert journal.sequence == 1
    assert journal.append("delete", {"key": "a"}) == 2

    monkeypatch.setattr(os, "unlink", fail)
    with pytest.raises(OSError):
        journal.reset()
    assert journal.sequence == 2
    journal.close()

    reopened = Journal(path)
    try:
        assert [record[0] for record in reopened.records] == [1, 2]
    finally:
        reopened.close()


@pytest.mark.persistence
def test_journal_recovers_a_torn_tail(tmp_path):
    path = tmp_path / "state.wal"
    journal = Journal(path)
    journal.append("put", {"value": 1})
    journal.close()

    with open(path, "ab") as handle:
        handle.write(b"\x00\x00\x00")
    recovered = Journal(path)
    assert len(recovered.records) == 1
    assert recovered.size == len(MAGIC) + len(make_frame((1, "put", {"value": 1})))
    recovered.close()

    with open(path, "ab") as handle:
        handle.write(struct.pack(">II", 20, 0) + b"short")
    recovered = Journal(path)
    assert len(recovered.records) == 1
    recovered.close()


@pytest.mark.persistence
def test_journal_rejects_corrupt_files(tmp_path):
    header_path = tmp_path / "header.wal"
    header_path.write_bytes(b"not-a-journal")
    with pytest.raises(JournalCorruptionError):
        Journal(header_path)

    length_path = tmp_path / "length.wal"
    length_path.write_bytes(MAGIC + struct.pack(">II", 0, 0))
    with pytest.raises(JournalCorruptionError):
        Journal(length_path)

    checksum_path = tmp_path / "checksum.wal"
    payload = make_frame((1, "put", {"value": 1}))
    checksum_path.write_bytes(MAGIC + payload[:-1] + b"\x00")
    with pytest.raises(JournalCorruptionError):
        Journal(checksum_path)

    decode_path = tmp_path / "decode.wal"
    bad_payload = b"not-pickle"
    decode_path.write_bytes(
        MAGIC + struct.pack(">II", len(bad_payload), zlib.crc32(bad_payload) & 0xFFFFFFFF) + bad_payload
    )
    with pytest.raises(JournalCorruptionError):
        Journal(decode_path)

    shape_path = tmp_path / "shape.wal"
    shape_path.write_bytes(MAGIC + make_frame([1, "put", {}]))
    with pytest.raises(JournalCorruptionError):
        Journal(shape_path)

    sequence_path = tmp_path / "sequence.wal"
    sequence_path.write_bytes(MAGIC + make_frame((1, "put", {})) + make_frame((1, "put", {})))
    with pytest.raises(JournalCorruptionError):
        Journal(sequence_path)


def make_checkpoint_payload(payload):
    return CHECKPOINT_MAGIC + struct.pack(">II", len(payload), zlib.crc32(payload) & 0xFFFFFFFF) + payload


@pytest.mark.persistence
def test_checkpoint_round_trip_and_validation(tmp_path):
    path = tmp_path / "state.checkpoint"
    state = {"sequence": 4, "store": {"a": [(1.0, 2)]}}
    write_checkpoint(path, state)
    assert read_checkpoint(path) == state

    bad_header = tmp_path / "bad-header.checkpoint"
    bad_header.write_bytes(b"wrong")
    with pytest.raises(JournalCorruptionError):
        read_checkpoint(bad_header)

    truncated = tmp_path / "truncated.checkpoint"
    truncated.write_bytes(CHECKPOINT_MAGIC)
    with pytest.raises(JournalCorruptionError):
        read_checkpoint(truncated)

    short_payload = tmp_path / "short-payload.checkpoint"
    short_payload.write_bytes(make_checkpoint_payload(pickle.dumps(state))[:-1])
    with pytest.raises(JournalCorruptionError):
        read_checkpoint(short_payload)

    trailing_bytes = tmp_path / "trailing-bytes.checkpoint"
    trailing_bytes.write_bytes(make_checkpoint_payload(pickle.dumps(state)) + b"extra")
    with pytest.raises(JournalCorruptionError):
        read_checkpoint(trailing_bytes)

    bad_checksum = tmp_path / "bad-checksum.checkpoint"
    corrupted = bytearray(make_checkpoint_payload(pickle.dumps(state)))
    corrupted[-1] ^= 0xFF
    bad_checksum.write_bytes(corrupted)
    with pytest.raises(JournalCorruptionError):
        read_checkpoint(bad_checksum)

    bad_payload = tmp_path / "bad-payload.checkpoint"
    bad_payload.write_bytes(make_checkpoint_payload(b"not-pickle"))
    with pytest.raises(JournalCorruptionError):
        read_checkpoint(bad_payload)

    not_mapping = tmp_path / "not-mapping.checkpoint"
    not_mapping.write_bytes(make_checkpoint_payload(pickle.dumps([1, 2, 3])))
    with pytest.raises(JournalCorruptionError):
        read_checkpoint(not_mapping)


@pytest.mark.persistence
def test_checkpoint_write_removes_temporary_file_on_failure(tmp_path, monkeypatch):
    path = tmp_path / "state.checkpoint"

    def fail_replace(source, destination):
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError):
        write_checkpoint(path, {"sequence": 1})
    assert not path.exists()
    assert list(tmp_path.glob("*.tmp")) == []


@pytest.mark.persistence
def test_checkpoint_write_rejects_unpicklable_state(tmp_path):
    path = tmp_path / "state.checkpoint"
    with pytest.raises((pickle.PicklingError, AttributeError, TypeError)):
        write_checkpoint(path, {"bad": lambda: None})
    assert not path.exists()
    assert list(tmp_path.glob("*.tmp")) == []


@pytest.mark.persistence
def test_checkpoint_write_tolerates_unlink_failure(tmp_path, monkeypatch):
    path = tmp_path / "state.checkpoint"

    def fail(*args):
        raise OSError("no")

    with monkeypatch.context() as context:
        context.setattr(os, "replace", fail)
        context.setattr(os, "unlink", fail)
        with pytest.raises(OSError):
            write_checkpoint(path, {"sequence": 1})


@pytest.mark.persistence
def test_fsync_directory_tolerates_unsupported_platforms(monkeypatch, tmp_path):
    monkeypatch.setattr(journal_module.os, "open", lambda path, flags: 7)
    monkeypatch.setattr(journal_module.os, "fsync", lambda descriptor: None)
    monkeypatch.setattr(journal_module.os, "close", lambda descriptor: None)
    _fsync_directory(tmp_path)


@pytest.mark.persistence
def test_fsync_directory_ignores_open_errors(monkeypatch, tmp_path):
    def fail(*args):
        raise OSError("directories cannot be opened")

    monkeypatch.setattr(journal_module.os, "open", fail)
    _fsync_directory(tmp_path)
