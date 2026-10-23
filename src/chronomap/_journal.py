# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Crash-safe write-ahead journal and checkpoint helpers.

The journal is a sequence of length-prefixed, CRC-checked pickle records.
A torn record at the end of the file is discarded during recovery; a complete
record with a bad checksum is treated as corruption. Records carry a
monotonically increasing sequence number so a checkpoint can safely coexist
with the tail of an older journal after a crash.

Pickle can execute code while loading. Journals and checkpoints are trusted
local state files, not a format for untrusted input.
"""

from __future__ import annotations

import os
import pickle
import struct
import tempfile
import threading
import zlib
from pathlib import Path
from typing import IO, Any, Callable, Dict, List, Optional, Tuple, Union

from .exceptions import ChronoMapError

MAGIC = b"CMWAL1\n"
CHECKPOINT_MAGIC = b"CMCHK1\n"
MAX_RECORD_BYTES = 1 << 30
_FRAME = struct.Struct(">II")

PathLike = Union[str, os.PathLike]


class JournalError(ChronoMapError):
    """Base class for journal read/write failures."""


class JournalCorruptionError(JournalError):
    """The journal or checkpoint is malformed or fails its checksum."""


class JournalLockError(JournalError):
    """Another live process already owns the journal lock."""


def _acquire_os_lock(descriptor: int) -> None:
    if os.name == "nt":  # pragma: no cover - exercised on Windows CI
        import msvcrt

        os.lseek(descriptor, 0, os.SEEK_SET)
        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
    else:  # pragma: no cover - exercised on POSIX CI
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[attr-defined]


def _release_os_lock(descriptor: int) -> None:
    if os.name == "nt":  # pragma: no cover - exercised on Windows CI
        import msvcrt

        os.lseek(descriptor, 0, os.SEEK_SET)
        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
    else:  # pragma: no cover - exercised on POSIX CI
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_UN)  # type: ignore[attr-defined]


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(str(path), os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        pass


def write_checkpoint(path: PathLike, state: Dict[str, Any]) -> None:
    """Atomically write a checkpoint state dictionary."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=destination.name + ".",
        suffix=".tmp",
        dir=str(destination.parent),
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            payload = pickle.dumps(state, protocol=4)
            handle.write(CHECKPOINT_MAGIC)
            handle.write(_FRAME.pack(len(payload), zlib.crc32(payload) & 0xFFFFFFFF))
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, destination)
        _fsync_directory(destination.parent)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def read_checkpoint(path: PathLike) -> Dict[str, Any]:
    """Read a checkpoint written by :func:`write_checkpoint`."""
    raw = Path(path).read_bytes()
    if not raw.startswith(CHECKPOINT_MAGIC):
        raise JournalCorruptionError("Checkpoint header is missing or invalid")
    frame_start = len(CHECKPOINT_MAGIC)
    if len(raw) < frame_start + _FRAME.size:
        raise JournalCorruptionError("Checkpoint frame is truncated")
    length, checksum = _FRAME.unpack_from(raw, frame_start)
    payload = raw[frame_start + _FRAME.size :]
    if length != len(payload):
        raise JournalCorruptionError("Checkpoint payload length does not match its frame")
    if zlib.crc32(payload) & 0xFFFFFFFF != checksum:
        raise JournalCorruptionError("Checkpoint checksum failed")
    try:
        state = pickle.loads(payload)
    except Exception as exc:
        raise JournalCorruptionError("Could not decode checkpoint payload") from exc
    if not isinstance(state, dict):
        raise JournalCorruptionError("Checkpoint payload must be a mapping")
    return state


class Journal:
    """Append-only, checksummed journal.

    By default, reopening a journal retains every decoded record in
    ``records``. Recovery-oriented callers can disable retention and pass a
    ``record_consumer`` so records are applied and released one at a time.
    ``skip_through`` suppresses consumer calls for records already covered by
    a checkpoint while still validating and sequencing them.
    """

    def __init__(
        self,
        path: PathLike,
        *,
        sync: bool = True,
        retain_records: bool = True,
        skip_through: int = 0,
        record_consumer: Optional[Callable[[int, str, Dict[str, Any]], None]] = None,
    ) -> None:
        if not isinstance(retain_records, bool):
            raise JournalError("retain_records must be a boolean")
        if isinstance(skip_through, bool) or not isinstance(skip_through, int) or skip_through < 0:
            raise JournalError("skip_through must be a non-negative integer")
        if record_consumer is not None and not callable(record_consumer):
            raise JournalError("record_consumer must be callable")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._sync = sync
        self._lock = threading.RLock()
        self._closed = False
        self._sequence = 0
        self._records: List[Tuple[int, str, Dict[str, Any]]] = []
        self._retain_records = retain_records
        self._skip_through = skip_through
        self._record_consumer = record_consumer
        self._file: Optional[IO[bytes]] = None
        self._lock_fd: Optional[int] = None
        self._acquire_path_lock()
        try:
            self._open()
        except BaseException:
            self._release_path_lock()
            raise

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:  # pragma: no cover - interpreter shutdown
            pass

    @property
    def sequence(self) -> int:
        return self._sequence

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def records(self) -> Tuple[Tuple[int, str, Dict[str, Any]], ...]:
        """Records loaded during the last open, in sequence order."""
        return tuple(self._records)

    @property
    def size(self) -> int:
        try:
            return self.path.stat().st_size
        except OSError:
            return 0

    def _acquire_path_lock(self) -> None:
        lock_path = self.path.with_name(self.path.name + ".lock")
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
        try:
            descriptor = os.open(str(lock_path), flags, 0o600)
        except OSError as exc:
            raise JournalError(f"Could not open journal lock file {lock_path}: {exc}") from exc
        try:
            _acquire_os_lock(descriptor)
        except OSError as exc:
            os.close(descriptor)
            raise JournalLockError(f"Journal {self.path} is locked by another process") from exc
        self._lock_fd = descriptor

    def _release_path_lock(self) -> None:
        descriptor = self._lock_fd
        self._lock_fd = None
        if descriptor is not None:
            try:
                _release_os_lock(descriptor)
            finally:
                os.close(descriptor)

    def _open(self) -> None:
        created = not self.path.exists() or self.path.stat().st_size == 0
        if created:
            with open(self.path, "wb") as writer:
                writer.write(MAGIC)
                writer.flush()
                os.fsync(writer.fileno())
            _fsync_directory(self.path.parent)
        handle = open(self.path, "r+b")
        try:
            header = handle.read(len(MAGIC))
            if header != MAGIC:
                raise JournalCorruptionError("Journal header is missing or invalid")
            valid_end = len(MAGIC)
            last_sequence = 0
            while True:
                record_start = handle.tell()
                frame = handle.read(_FRAME.size)
                if not frame:
                    break
                if len(frame) < _FRAME.size:
                    break
                length, checksum = _FRAME.unpack(frame)
                if length <= 0 or length > MAX_RECORD_BYTES:
                    raise JournalCorruptionError(f"Invalid journal record length: {length}")
                payload = handle.read(length)
                if len(payload) < length:
                    break
                if zlib.crc32(payload) & 0xFFFFFFFF != checksum:
                    raise JournalCorruptionError(f"Journal checksum failed at offset {record_start}")
                try:
                    record = pickle.loads(payload)
                except Exception as exc:
                    raise JournalCorruptionError(
                        f"Could not decode journal record at offset {record_start}"
                    ) from exc
                if (
                    not isinstance(record, tuple)
                    or len(record) != 3
                    or isinstance(record[0], bool)
                    or not isinstance(record[0], int)
                    or not isinstance(record[1], str)
                    or not isinstance(record[2], dict)
                ):
                    raise JournalCorruptionError(f"Invalid journal record at offset {record_start}")
                sequence, operation, data = record
                if sequence <= last_sequence:
                    raise JournalCorruptionError("Journal sequence numbers are not increasing")
                if sequence > self._skip_through and self._record_consumer is not None:
                    self._record_consumer(sequence, operation, data)
                if self._retain_records:
                    self._records.append((sequence, operation, data))
                last_sequence = sequence
                valid_end = handle.tell()

            size = handle.seek(0, os.SEEK_END)
            if size > valid_end:
                handle.truncate(valid_end)
            handle.seek(0, os.SEEK_END)
            self._sequence = last_sequence
            self._file = handle
        except BaseException:
            handle.close()
            raise

    def advance_to(self, sequence: int) -> None:
        """Ensure future records use sequence numbers above ``sequence``."""
        with self._lock:
            self._sequence = max(self._sequence, sequence)

    def clear_records(self) -> None:
        """Release records loaded during the last open."""
        with self._lock:
            self._records.clear()

    def append(self, operation: str, data: Dict[str, Any]) -> int:
        if not isinstance(operation, str) or not operation:
            raise JournalError("Journal operations must be non-empty strings")
        if not isinstance(data, dict):
            raise JournalError("Journal record data must be a mapping")
        with self._lock:
            if self._closed or self._file is None:
                raise JournalError("Journal is closed")
            sequence = self._sequence + 1
            try:
                payload = pickle.dumps((sequence, operation, data), protocol=4)
            except Exception as exc:
                raise JournalError(f"Could not encode journal record: {exc}") from exc
            if len(payload) > MAX_RECORD_BYTES:
                raise JournalError("Journal record is too large")
            frame = _FRAME.pack(len(payload), zlib.crc32(payload) & 0xFFFFFFFF)
            handle = self._file
            record_start = handle.seek(0, os.SEEK_END)
            try:
                handle.write(frame)
                handle.write(payload)
                handle.flush()
                if self._sync:
                    os.fsync(handle.fileno())
            except BaseException:
                handle.truncate(record_start)
                handle.seek(0, os.SEEK_END)
                raise
            self._sequence = sequence
            return sequence

    def reset(self) -> None:
        """Atomically discard all journal records while retaining the sequence counter."""
        with self._lock:
            if self._closed or self._file is None:
                raise JournalError("Journal is closed")
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=self.path.name + ".",
                suffix=".tmp",
                dir=str(self.path.parent),
            )
            handle = self._file
            try:
                with os.fdopen(descriptor, "wb") as writer:
                    writer.write(MAGIC)
                    writer.flush()
                    os.fsync(writer.fileno())
                handle.close()
                self._file = None
                os.replace(temporary_name, self.path)
                _fsync_directory(self.path.parent)
                self._file = open(self.path, "r+b")
                self._records.clear()
            except BaseException:
                try:
                    os.unlink(temporary_name)
                except OSError:
                    pass
                if self._file is None:
                    self._file = open(self.path, "r+b")
                raise

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                if self._file is not None:
                    self._file.close()
                    self._file = None
            finally:
                self._release_path_lock()

    def __enter__(self) -> "Journal":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


__all__ = [
    "CHECKPOINT_MAGIC",
    "MAGIC",
    "Journal",
    "JournalCorruptionError",
    "JournalError",
    "JournalLockError",
    "read_checkpoint",
    "write_checkpoint",
]
