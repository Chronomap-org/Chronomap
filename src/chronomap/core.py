# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""The main ChronoMap class.

This is the synchronous, thread-safe implementation. See `asynchronous.py`
for the asyncio version.
"""

from __future__ import annotations

import bz2
import gzip
import heapq
import json
import logging
import lzma
import math
import pickle
import weakref
import zlib
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Set, Tuple, Union

from ._cache import LRUCache
from ._journal import Journal, JournalCorruptionError, JournalError, read_checkpoint, write_checkpoint
from ._lock import RWLock
from ._memory import MemoryMonitor
from ._snapshot import SnapshotContext
from ._ttl_cleanup import TTLCleanupThread
from ._version import __version__
from ._versionlog import insertion_point, range_versions, value_at
from .exceptions import (
    ChronoMapKeyError,
    ChronoMapTypeError,
    ChronoMapValueError,
)

logger = logging.getLogger(__name__)
_CACHE_MISS = object()
_MISSING = object()


class ChronoMap:
    """Thread-safe, time-versioned key-value store.

    Every write is kept, not just the latest value, so you can ask "what
    was this key at time T" as well as "what is it now." See the README
    for the full walkthrough; this docstring covers just the constructor.

    Args:
        debug: Enable debug logging.
        use_rwlock: Use a read-write lock (multiple concurrent readers)
            instead of a plain mutex. Turn this off if you want strictly
            serialized access for some reason; there's no real perf
            downside to leaving it on for typical read-heavy workloads.
        max_history: Cap on versions kept per key. Older versions are
            dropped automatically once a key exceeds this. None = unbounded,
            which is fine for small maps but will grow forever otherwise.
        cache_size: Size of the internal LRU read cache. 0 disables it.
        enable_ttl_cleanup: Run a background thread that clears expired
            keys on an interval, instead of only on next access.
        ttl_cleanup_interval: Seconds between background cleanup passes.
        max_memory_mb: Soft/hard memory ceiling. Approximate, not exact
            (see MemoryMonitor) — treat it as a tripwire, not a guarantee.
        wal_path: Enable a durable write-ahead journal at this path. State is
            replayed from the journal (and its checkpoint) on construction.
        wal_sync: Flush and fsync every journal record. Disable only when
            losing the most recent writes on a crash is acceptable.
        wal_auto_checkpoint_bytes: Compact the journal into a checkpoint once
            it grows past this many bytes. None disables automatic
            checkpoints.
    """

    def __init__(
        self,
        debug: bool = False,
        use_rwlock: bool = True,
        max_history: Optional[int] = None,
        cache_size: int = 1000,
        enable_ttl_cleanup: bool = True,
        ttl_cleanup_interval: float = 60.0,
        max_memory_mb: Optional[float] = None,
        wal_path: Optional[Union[str, Path]] = None,
        wal_sync: bool = True,
        wal_auto_checkpoint_bytes: Optional[int] = 64 * 1024 * 1024,
    ) -> None:
        if max_history is not None and (
            isinstance(max_history, bool) or not isinstance(max_history, int) or max_history < 1
        ):
            raise ChronoMapValueError("max_history must be a positive integer or None")
        if isinstance(cache_size, bool) or not isinstance(cache_size, int) or cache_size < 0:
            raise ChronoMapValueError("cache_size must be a non-negative integer")
        if (
            isinstance(ttl_cleanup_interval, bool)
            or not isinstance(ttl_cleanup_interval, (int, float))
            or not math.isfinite(ttl_cleanup_interval)
            or ttl_cleanup_interval <= 0
        ):
            raise ChronoMapValueError("ttl_cleanup_interval must be a positive number")
        if max_memory_mb is not None and (
            isinstance(max_memory_mb, bool)
            or not isinstance(max_memory_mb, (int, float))
            or not math.isfinite(max_memory_mb)
            or max_memory_mb <= 0
        ):
            raise ChronoMapValueError("max_memory_mb must be a positive number or None")
        if not isinstance(wal_sync, bool):
            raise ChronoMapValueError("wal_sync must be a boolean")
        if wal_auto_checkpoint_bytes is not None and (
            isinstance(wal_auto_checkpoint_bytes, bool)
            or not isinstance(wal_auto_checkpoint_bytes, int)
            or wal_auto_checkpoint_bytes < 1
        ):
            raise ChronoMapValueError("wal_auto_checkpoint_bytes must be a positive integer or None")

        self._store: Dict[Any, List[Tuple[float, Any]]] = {}
        self._ttl: Dict[Any, float] = {}

        if use_rwlock:
            self._lock: Any = RWLock()
            self._use_rwlock = True
        else:
            import threading

            self._lock = threading.RLock()
            self._use_rwlock = False

        self._snapshot_time: Optional[float] = None
        self._debug = debug

        self._max_history = max_history
        self._cache = LRUCache(capacity=cache_size) if cache_size > 0 else None
        self._memory_monitor = MemoryMonitor(max_memory_mb)

        self._change_callbacks: List[Callable] = []
        self._key_subscribers: Dict[Any, List[Callable]] = {}

        self._stats = {
            "reads": 0,
            "writes": 0,
            "deletes": 0,
            "snapshots": 0,
            "auto_prunes": 0,
            "cache_hits": 0,
            "cache_misses": 0,
        }

        self._wal: Optional[Journal] = None
        self._wal_auto_checkpoint_bytes = wal_auto_checkpoint_bytes
        if wal_path is not None:
            journal_path = Path(wal_path)
            checkpoint_sequence = self._restore_checkpoint(Path(str(journal_path) + ".checkpoint"))
            journal = Journal(
                journal_path,
                sync=wal_sync,
                retain_records=False,
                skip_through=checkpoint_sequence,
                record_consumer=self._consume_journal_record,
            )
            journal.advance_to(checkpoint_sequence)
            self._wal = journal

        self._ttl_cleanup_thread: Optional[TTLCleanupThread] = None
        if enable_ttl_cleanup:
            self._ttl_cleanup_thread = TTLCleanupThread(weakref.ref(self), interval=ttl_cleanup_interval)
            self._ttl_cleanup_thread.start()

        if debug:
            logger.setLevel(logging.DEBUG)
            if not logger.handlers:
                handler = logging.StreamHandler()
                handler.setFormatter(logging.Formatter("[ChronoMap] %(levelname)s: %(message)s"))
                logger.addHandler(handler)

    def __del__(self) -> None:
        cleanup_thread = getattr(self, "_ttl_cleanup_thread", None)
        if cleanup_thread is not None:
            cleanup_thread.stop()
        journal = getattr(self, "_wal", None)
        if journal is not None:
            try:
                journal.close()
            except Exception:
                pass

    def close(self) -> None:
        """Stop background cleanup and close the write-ahead journal."""
        cleanup_thread = getattr(self, "_ttl_cleanup_thread", None)
        if cleanup_thread is not None:
            cleanup_thread.stop()
            self._ttl_cleanup_thread = None
        if self._wal is not None:
            self._wal.close()

    def __enter__(self) -> "ChronoMap":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # -- internal helpers -----------------------------------------------

    def _current_time(self) -> float:
        return datetime.now(timezone.utc).timestamp()

    def _journal_append(self, operation: str, data: Dict[str, Any]) -> None:
        if self._wal is not None:
            self._wal.append(operation, data)

    def _journal_checkpoint_path(self) -> Path:
        if self._wal is None:
            raise JournalError("WAL is not enabled")
        return Path(str(self._wal.path) + ".checkpoint")

    def _checkpoint_unlocked(self) -> None:
        if self._wal is None:
            raise JournalError("WAL is not enabled")
        if self._wal.closed:
            raise JournalError("Journal is closed")
        state = {
            "sequence": self._wal.sequence,
            "store": deepcopy(self._store),
            "ttl": deepcopy(self._ttl),
            "snapshot_time": self._snapshot_time,
            "max_history": self._max_history,
        }
        write_checkpoint(self._journal_checkpoint_path(), state)
        self._wal.reset()

    def checkpoint(self) -> None:
        """Write a durable checkpoint and truncate the journal."""
        self._acquire_write()
        try:
            self._checkpoint_unlocked()
        finally:
            self._release_write()

    def _maybe_checkpoint(self) -> None:
        if (
            self._wal is not None
            and self._wal_auto_checkpoint_bytes is not None
            and self._wal.size >= self._wal_auto_checkpoint_bytes
        ):
            try:
                self._checkpoint_unlocked()
            except Exception as exc:
                logger.warning("WAL auto-checkpoint failed; keeping journal: %s", exc)

    def _apply_journal_put(
        self,
        key: Any,
        value: Any,
        timestamp: Any,
        expires_at: Any,
    ) -> None:
        if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, (int, float))
            or not math.isfinite(timestamp)
        ):
            raise JournalCorruptionError("Journal contains an invalid timestamp")
        if expires_at is not None and (
            isinstance(expires_at, bool)
            or not isinstance(expires_at, (int, float))
            or not math.isfinite(expires_at)
        ):
            raise JournalCorruptionError("Journal contains an invalid TTL deadline")
        ts = float(timestamp)
        versions = self._store.setdefault(key, [])
        if not versions or ts >= versions[-1][0]:
            versions.append((ts, value))
        else:
            versions.insert(insertion_point(versions, ts), (ts, value))
        if expires_at is not None:
            self._ttl[key] = float(expires_at)
        self._auto_prune(key)
        self._stats["writes"] += 1

    def _apply_journal_record(self, operation: str, data: Dict[str, Any]) -> None:
        if operation == "put":
            self._apply_journal_put(
                data["key"],
                data["value"],
                data["timestamp"],
                data.get("expires_at"),
            )
        elif operation == "put_many":
            for key, value in data["items"]:
                self._apply_journal_put(key, value, data["timestamp"], data.get("expires_at"))
        elif operation == "delete":
            key = data["key"]
            if self._store.pop(key, None) is not None:
                self._stats["deletes"] += 1
            self._ttl.pop(key, None)
        elif operation == "delete_many":
            for key in data["keys"]:
                if self._store.pop(key, None) is not None:
                    self._stats["deletes"] += 1
                self._ttl.pop(key, None)
        elif operation == "persist":
            self._ttl.pop(data["key"], None)
        elif operation == "persist_many":
            keys = data["keys"]
            if not isinstance(keys, (list, tuple)):
                raise JournalCorruptionError("Journal contains invalid persistence keys")
            for key in keys:
                self._ttl.pop(key, None)
        elif operation == "expire":
            key = data["key"]
            expires_at = data["expires_at"]
            if (
                isinstance(expires_at, bool)
                or not isinstance(expires_at, (int, float))
                or not math.isfinite(expires_at)
            ):
                raise JournalCorruptionError("Journal contains an invalid expiration deadline")
            if key in self._store:
                self._ttl[key] = float(expires_at)
        elif operation == "expire_many":
            keys = data["keys"]
            expires_at = data["expires_at"]
            if not isinstance(keys, (list, tuple)):
                raise JournalCorruptionError("Journal contains invalid expiration keys")
            if (
                isinstance(expires_at, bool)
                or not isinstance(expires_at, (int, float))
                or not math.isfinite(expires_at)
            ):
                raise JournalCorruptionError("Journal contains an invalid expiration deadline")
            for key in keys:
                if key in self._store:
                    self._ttl[key] = float(expires_at)
        elif operation == "prune":
            key = data["key"]
            versions = self._store.get(key)
            if versions is None:
                return
            keep_last = data.get("keep_last")
            cutoff = data.get("cutoff")
            if keep_last is not None:
                if isinstance(keep_last, bool) or not isinstance(keep_last, int) or keep_last < 0:
                    raise JournalCorruptionError("Journal contains an invalid prune count")
                if keep_last == 0:
                    versions.clear()
                else:
                    versions[:] = versions[-keep_last:]
            if cutoff is not None:
                if (
                    isinstance(cutoff, bool)
                    or not isinstance(cutoff, (int, float))
                    or not math.isfinite(cutoff)
                ):
                    raise JournalCorruptionError("Journal contains an invalid prune cutoff")
                versions[:] = [(ts, value) for ts, value in versions if ts >= cutoff]
            if not versions:
                self._store.pop(key, None)
                self._ttl.pop(key, None)
        elif operation == "clear":
            self._store.clear()
            self._ttl.clear()
            if self._cache:
                self._cache.clear()
        elif operation == "replace":
            store = data["store"]
            ttl = data["ttl"]
            if not isinstance(store, dict) or not isinstance(ttl, dict):
                raise JournalCorruptionError("Journal contains an invalid replacement state")
            self._store = store
            self._ttl = ttl
            if data.get("restore_snapshot_time", False):
                self._snapshot_time = data.get("snapshot_time")
            if self._cache:
                self._cache.clear()
        elif operation == "merge":
            other_store = data["other_store"]
            other_ttl = data["other_ttl"]
            strategy = data["strategy"]
            if not isinstance(other_store, dict) or not isinstance(other_ttl, dict):
                raise JournalCorruptionError("Journal contains an invalid merge state")
            if strategy == "timestamp":
                for key, other_versions in other_store.items():
                    target_versions = self._store.get(key, [])
                    if other_versions:
                        self._store[key] = list(
                            heapq.merge(target_versions, other_versions, key=lambda item: item[0])
                        )
                    self._auto_prune(key)
                    self._stats["writes"] += len(other_versions)
                for key, expiry in other_ttl.items():
                    if key not in self._ttl or expiry > self._ttl[key]:
                        self._ttl[key] = expiry
            elif strategy == "overwrite":
                for key, versions in other_store.items():
                    self._store[key] = versions
                    self._auto_prune(key)
                    self._stats["writes"] += len(versions)
                for key, expiry in other_ttl.items():
                    self._ttl[key] = expiry
            else:
                raise JournalCorruptionError("Journal contains an invalid merge strategy")
            if self._cache:
                self._cache.clear()
        else:
            raise JournalCorruptionError(f"Unknown journal operation: {operation!r}")

    def _restore_checkpoint(self, checkpoint_path: Path) -> int:
        if not checkpoint_path.exists():
            return 0
        state = read_checkpoint(checkpoint_path)
        sequence = state.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise JournalCorruptionError("Checkpoint contains an invalid sequence number")
        store = state.get("store")
        ttl = state.get("ttl")
        if not isinstance(store, dict) or not isinstance(ttl, dict):
            raise JournalCorruptionError("Checkpoint contains an invalid state")
        self._store = store
        self._ttl = ttl
        self._snapshot_time = state.get("snapshot_time")
        return sequence

    def _consume_journal_record(
        self,
        sequence: int,
        operation: str,
        data: Dict[str, Any],
    ) -> None:
        try:
            self._apply_journal_record(operation, data)
        except JournalCorruptionError:
            raise
        except Exception as exc:
            raise JournalCorruptionError(
                f"Could not replay journal record {sequence} ({operation!r})"
            ) from exc

    def _parse_timestamp(self, timestamp: Union[float, str, datetime]) -> float:
        if isinstance(timestamp, bool):
            raise ChronoMapTypeError("Timestamp must be numeric, got bool")
        if isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                return dt.timestamp()
            except ValueError:
                raise ChronoMapValueError(f"Invalid datetime string: {timestamp}")
        elif isinstance(timestamp, datetime):
            return timestamp.timestamp()
        elif isinstance(timestamp, (int, float)):
            return float(timestamp)
        else:
            raise ChronoMapTypeError(f"Invalid timestamp type: {type(timestamp)}")

    def _validate_key(self, key: Any) -> None:
        try:
            hash(key)
        except TypeError:
            raise ChronoMapTypeError(f"Key must be hashable, got {type(key).__name__}")

    def _validate_timestamp(self, timestamp: float) -> None:
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            raise ChronoMapTypeError(f"Timestamp must be numeric, got {type(timestamp).__name__}")
        if not math.isfinite(timestamp):
            raise ChronoMapValueError(f"Timestamp must be finite, got {timestamp}")

    def _validate_ttl(self, ttl: Optional[float]) -> None:
        if ttl is None:
            return
        if isinstance(ttl, bool) or not isinstance(ttl, (int, float)):
            raise ChronoMapTypeError(f"TTL must be numeric, got {type(ttl).__name__}")
        if not math.isfinite(ttl) or ttl <= 0:
            raise ChronoMapValueError(f"TTL must be a positive finite number, got {ttl}")

    def _validate_keep_last(self, keep_last: Optional[int]) -> None:
        if keep_last is None:
            return
        if isinstance(keep_last, bool) or not isinstance(keep_last, int) or keep_last < 0:
            raise ChronoMapValueError("keep_last must be a non-negative integer or None")

    def _is_expired(self, key: Any) -> bool:
        if key not in self._ttl:
            return False
        return self._current_time() >= self._ttl[key]

    def _purge_if_expired(self, key: Any) -> None:
        """Drop an expired key's history and TTL so a fresh write starts clean.

        Expired keys are invisible to readers (get/history/keys all treat them
        as missing), so a write to one must behave like a write to a brand-new
        key. Without this, the stale ``_ttl`` entry outlives the old data and
        the *new* value is born already expired. Caller must hold the write lock.
        """
        if self._is_expired(key):
            self._journal_append("delete", {"key": key})
            self._store.pop(key, None)
            self._ttl.pop(key, None)
            if self._cache:
                self._cache.invalidate(key)
            self._maybe_checkpoint()

    def _auto_prune(self, key: Any) -> None:
        if self._max_history is None:
            return
        versions = self._store.get(key, [])
        if len(versions) > self._max_history:
            removed = len(versions) - self._max_history
            del versions[:removed]
            self._stats["auto_prunes"] += 1
            if self._cache:
                self._cache.invalidate(key)
            logger.debug("AUTO_PRUNE key=%r removed %d old versions", key, removed)

    def _acquire_read(self) -> None:
        self._lock.acquire_read() if self._use_rwlock else self._lock.acquire()

    def _release_read(self) -> None:
        self._lock.release_read() if self._use_rwlock else self._lock.release()

    def _acquire_write(self) -> None:
        self._lock.acquire_write() if self._use_rwlock else self._lock.acquire()

    def _release_write(self) -> None:
        self._lock.release_write() if self._use_rwlock else self._lock.release()

    def _trigger_change_callbacks(self, key: Any, old_value: Any, new_value: Any, timestamp: float) -> None:
        for callback in list(self._change_callbacks):
            try:
                callback(key, old_value, new_value, timestamp)
            except Exception:
                logger.exception("Error in change callback")
        for callback in list(self._key_subscribers.get(key, [])):
            try:
                callback(old_value, new_value, timestamp)
            except Exception:
                logger.exception("Error in key subscriber callback for %r", key)

    def _lookup_unlocked(self, key: Any, ts: float) -> Tuple[bool, Any]:
        """Resolve a key at `ts`; the bool distinguishes a stored None from a miss."""
        if self._is_expired(key):
            return False, None

        versions = self._store.get(key, [])
        if not versions:
            return False, None

        return value_at(versions, ts)

    def _get_unlocked(
        self,
        key: Any,
        timestamp: Optional[Union[float, str, datetime]] = None,
        default: Any = None,
        *,
        strict: bool = False,
    ) -> Any:
        """Same as get(), but assumes the caller already holds a read lock."""
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        found, value = self._lookup_unlocked(key, ts)
        if found:
            return value
        if strict:
            raise ChronoMapKeyError(key)
        return default

    # -- event hooks ------------------------------------------------------

    def on_change(self, callback: Callable[[Any, Any, Any, float], None]) -> None:
        """Register a callback fired on every write: callback(key, old, new, ts)."""
        if not callable(callback):
            raise ChronoMapTypeError("Change callback must be callable")
        self._acquire_write()
        try:
            self._change_callbacks.append(callback)
        finally:
            self._release_write()

    def subscribe(self, key: Any, callback: Callable[[Any, Any, float], None]) -> None:
        """Register a callback for one specific key: callback(old, new, ts)."""
        self._validate_key(key)
        if not callable(callback):
            raise ChronoMapTypeError("Subscriber callback must be callable")
        self._acquire_write()
        try:
            self._key_subscribers.setdefault(key, []).append(callback)
        finally:
            self._release_write()

    def unsubscribe(self, key: Any, callback: Callable) -> bool:
        self._validate_key(key)
        self._acquire_write()
        try:
            callbacks = self._key_subscribers.get(key)
            if not callbacks:
                return False
            try:
                callbacks.remove(callback)
            except ValueError:
                return False
            if not callbacks:
                del self._key_subscribers[key]
            return True
        finally:
            self._release_write()

    def remove_change_callback(self, callback: Callable) -> bool:
        self._acquire_write()
        try:
            try:
                self._change_callbacks.remove(callback)
                return True
            except ValueError:
                return False
        finally:
            self._release_write()

    # -- core read/write ---------------------------------------------------

    def put(
        self,
        key: Any,
        value: Any,
        timestamp: Optional[Union[float, str, datetime]] = None,
        ttl: Optional[float] = None,
    ) -> None:
        """Store a value at the given timestamp (defaults to now)."""
        self._validate_key(key)
        self._validate_ttl(ttl)
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)

        self._acquire_write()
        try:
            self._purge_if_expired(key)

            old_value = None
            if key in self._store and self._store[key]:
                old_value = self._store[key][-1][1]

            expires_at = self._current_time() + ttl if ttl is not None else None
            self._journal_append(
                "put",
                {
                    "key": key,
                    "value": value,
                    "timestamp": ts,
                    "expires_at": expires_at,
                },
            )

            versions = self._store.setdefault(key, [])
            if not versions or ts >= versions[-1][0]:
                versions.append((ts, value))
            else:
                idx = insertion_point(versions, ts)
                versions.insert(idx, (ts, value))

            if expires_at is not None:
                self._ttl[key] = expires_at

            self._auto_prune(key)
            if self._cache:
                self._cache.invalidate(key)
            self._memory_monitor.check_memory(self._store, self._ttl)
            self._maybe_checkpoint()

            self._stats["writes"] += 1
            logger.debug("PUT key=%r value=%r at ts=%f ttl=%s", key, value, ts, ttl)
        finally:
            self._release_write()

        self._trigger_change_callbacks(key, old_value, value, ts)

    def get(
        self,
        key: Any,
        timestamp: Optional[Union[float, str, datetime]] = None,
        default: Any = None,
        *,
        strict: bool = False,
    ) -> Any:
        """Retrieve the value for a key at a given timestamp (cached)."""
        self._validate_key(key)
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        cache_key = (key, None if timestamp is None else ts)

        self._acquire_read()
        try:
            if self._is_expired(key):
                if self._cache:
                    self._cache.invalidate(key)
                if strict:
                    raise ChronoMapKeyError(key)
                return default

            if self._cache:
                cached = self._cache.get(cache_key, _CACHE_MISS)
                if cached is not _CACHE_MISS:
                    self._stats["cache_hits"] += 1
                    self._stats["reads"] += 1
                    return cached
                self._stats["cache_misses"] += 1

            found, value = self._lookup_unlocked(key, ts)
            if not found:
                if strict:
                    raise ChronoMapKeyError(key)
                return default

            if self._cache:
                self._cache.put(cache_key, value)

            self._stats["reads"] += 1
            logger.debug("GET key=%r -> %r at ts=%f", key, value, ts)
            return value
        finally:
            self._release_read()

    def get_or_set(self, key: Any, default_factory: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        """Return the current value, or create/store/return one via default_factory()."""
        self._validate_key(key)
        if not callable(default_factory):
            raise ChronoMapTypeError("default_factory must be callable")
        self._validate_ttl(ttl)

        ts = self._current_time()
        self._validate_timestamp(ts)
        created = False
        value: Any = None

        self._acquire_write()
        try:
            self._purge_if_expired(key)

            versions = self._store.get(key, [])
            if versions:
                self._stats["reads"] += 1
                return versions[-1][1]

            value = default_factory()
            expires_at = self._current_time() + ttl if ttl is not None else None
            self._journal_append(
                "put",
                {
                    "key": key,
                    "value": value,
                    "timestamp": ts,
                    "expires_at": expires_at,
                },
            )
            self._store[key] = [(ts, value)]

            if expires_at is not None:
                self._ttl[key] = expires_at

            self._auto_prune(key)
            if self._cache:
                self._cache.invalidate(key)
            self._memory_monitor.check_memory(self._store, self._ttl)
            self._maybe_checkpoint()

            self._stats["writes"] += 1
            created = True
            logger.debug("GET_OR_SET key=%r value=%r ttl=%s", key, value, ttl)
        finally:
            self._release_write()

        if created:
            self._trigger_change_callbacks(key, None, value, ts)
        return value

    def get_or_default(self, key: Any, default: Any, ttl: Optional[float] = None) -> Any:
        """Like get_or_set, but takes a plain value (or a zero-arg factory)."""
        factory = default if callable(default) else lambda: default
        return self.get_or_set(key, factory, ttl=ttl)

    def get_many(
        self,
        keys: Iterable[Any],
        timestamp: Optional[Union[float, str, datetime]] = None,
        *,
        strict: bool = False,
    ) -> Dict[Any, Any]:
        """Read several keys at one timestamp; missing keys are omitted unless strict."""
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)

        result: Dict[Any, Any] = {}
        missing: List[Any] = []
        self._acquire_read()
        try:
            for key in keys:
                self._validate_key(key)
                found, value = self._lookup_unlocked(key, ts)
                if found:
                    result[key] = value
                elif strict:
                    missing.append(key)
        finally:
            self._release_read()

        if missing:
            raise ChronoMapKeyError(missing[0])
        return result

    def exists(self, key: Any, timestamp: Optional[Union[float, str, datetime]] = None) -> bool:
        """True when the key resolves to a value at the timestamp (a stored None counts)."""
        self._validate_key(key)
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        self._acquire_read()
        try:
            return self._lookup_unlocked(key, ts)[0]
        finally:
            self._release_read()

    def setdefault(self, key: Any, default: Any = None, ttl: Optional[float] = None) -> Any:
        """Return the latest value, inserting `default` first when the key is absent."""
        self._validate_key(key)
        self._validate_ttl(ttl)
        ts = self._current_time()
        created = False

        self._acquire_write()
        try:
            self._purge_if_expired(key)

            versions = self._store.get(key, [])
            if versions:
                self._stats["reads"] += 1
                return versions[-1][1]

            expires_at = self._current_time() + ttl if ttl is not None else None
            self._journal_append(
                "put",
                {
                    "key": key,
                    "value": default,
                    "timestamp": ts,
                    "expires_at": expires_at,
                },
            )
            self._store[key] = [(ts, default)]
            if expires_at is not None:
                self._ttl[key] = expires_at

            if self._cache:
                self._cache.invalidate(key)
            self._memory_monitor.check_memory(self._store, self._ttl)
            self._maybe_checkpoint()
            self._stats["writes"] += 1
            created = True
            logger.debug("SETDEFAULT key=%r value=%r ttl=%s", key, default, ttl)
        finally:
            self._release_write()

        if created:
            self._trigger_change_callbacks(key, None, default, ts)
        return default

    def pop(self, key: Any, default: Any = _MISSING) -> Any:
        """Return the current value and erase the key, or return `default` if absent."""
        self._validate_key(key)
        self._acquire_write()
        try:
            if self._is_expired(key):
                self._purge_if_expired(key)
            found, value = self._lookup_unlocked(key, self._current_time())
            if not found:
                if default is _MISSING:
                    raise ChronoMapKeyError(key)
                return default

            self._journal_append("delete", {"key": key})
            del self._store[key]
            self._ttl.pop(key, None)
            if self._cache:
                self._cache.invalidate(key)
            self._maybe_checkpoint()
            self._stats["deletes"] += 1
            logger.debug("POP key=%r value=%r", key, value)
            return value
        finally:
            self._release_write()

    def pop_many(self, keys: Iterable[Any]) -> Dict[Any, Any]:
        """Remove several keys under one lock and return their latest live values."""
        keys = list(keys)
        for key in keys:
            self._validate_key(key)
        self._acquire_write()
        try:
            live_keys: List[Any] = []
            for key in keys:
                if key not in self._store:
                    continue
                if self._is_expired(key):
                    self._purge_if_expired(key)
                    continue
                live_keys.append(key)
            live_keys = list(dict.fromkeys(live_keys))
            result = {key: self._store[key][-1][1] for key in live_keys}
            if live_keys:
                self._journal_append("delete_many", {"keys": live_keys})
            for key in live_keys:
                self._store.pop(key, None)
                self._ttl.pop(key, None)
                if self._cache:
                    self._cache.invalidate(key)
                self._stats["deletes"] += 1
            if live_keys:
                self._maybe_checkpoint()
            logger.debug("POP_MANY %d/%d keys", len(live_keys), len(keys))
            return result
        finally:
            self._release_write()

    def popitem(self, last: bool = True) -> Tuple[Any, Any]:
        """Remove and return the newest live pair, or the oldest when ``last=False``."""
        self._acquire_write()
        try:
            keys = reversed(list(self._store)) if last else list(self._store)
            for key in keys:
                if self._is_expired(key):
                    self._purge_if_expired(key)
                    continue
                versions = self._store.get(key, [])
                if not versions:
                    continue

                value = versions[-1][1]
                self._journal_append("delete", {"key": key})
                del self._store[key]
                self._ttl.pop(key, None)
                if self._cache:
                    self._cache.invalidate(key)
                self._maybe_checkpoint()
                self._stats["deletes"] += 1
                logger.debug("POPITEM key=%r value=%r", key, value)
                return key, value

            raise ChronoMapKeyError("popitem(): ChronoMap is empty")
        finally:
            self._release_write()

    def delete(self, key: Any) -> bool:
        """Delete all history for a key. Returns True if a live key was removed."""
        self._validate_key(key)
        self._acquire_write()
        try:
            if key not in self._store:
                return False
            if self._is_expired(key):
                self._purge_if_expired(key)
                return False

            self._journal_append("delete", {"key": key})
            del self._store[key]
            self._ttl.pop(key, None)
            if self._cache:
                self._cache.invalidate(key)
            self._maybe_checkpoint()
            self._stats["deletes"] += 1
            logger.debug("DELETE key=%r", key)
            return True
        finally:
            self._release_write()

    def persist(self, key: Any) -> bool:
        """Remove a key's TTL so it stops expiring. Returns True if a TTL was removed."""
        self._validate_key(key)
        self._acquire_write()
        try:
            if self._is_expired(key):
                self._purge_if_expired(key)
                return False
            if key not in self._ttl:
                return False

            self._journal_append("persist", {"key": key})
            del self._ttl[key]
            if self._cache:
                self._cache.invalidate(key)
            self._maybe_checkpoint()
            logger.debug("PERSIST key=%r", key)
            return True
        finally:
            self._release_write()

    def persist_many(self, keys: Iterable[Any]) -> int:
        """Remove TTLs from several live keys. Returns the number persisted."""
        keys = list(keys)
        for key in keys:
            self._validate_key(key)
        self._acquire_write()
        try:
            live_keys: List[Any] = []
            for key in keys:
                if key not in self._store:
                    continue
                if self._is_expired(key):
                    self._purge_if_expired(key)
                    continue
                if key in self._ttl:
                    live_keys.append(key)
            live_keys = list(dict.fromkeys(live_keys))
            if not live_keys:
                return 0

            self._journal_append("persist_many", {"keys": live_keys})
            for key in live_keys:
                del self._ttl[key]
                if self._cache:
                    self._cache.invalidate(key)
            self._maybe_checkpoint()
            return len(live_keys)
        finally:
            self._release_write()

    def ttl_remaining(self, key: Any) -> Optional[float]:
        """Seconds until `key` expires, or None if it is absent or has no TTL.

        An expired key that has not been swept yet returns ``0.0``.
        """
        self._validate_key(key)
        self._acquire_read()
        try:
            if key not in self._store:
                return None
            deadline = self._ttl.get(key)
            if deadline is None:
                return None
            return max(0.0, deadline - self._current_time())
        finally:
            self._release_read()

    def ttl_remaining_many(self, keys: Iterable[Any]) -> Dict[Any, Optional[float]]:
        """Map each key to its remaining TTL, using one consistent snapshot.

        Missing keys and keys without a TTL map to ``None``. An expired key
        that has not been swept yet maps to ``0.0``.
        """
        keys = list(keys)
        for key in keys:
            self._validate_key(key)
        self._acquire_read()
        try:
            now = self._current_time()
            result: Dict[Any, Optional[float]] = {}
            for key in keys:
                if key in result:
                    continue
                deadline = self._ttl.get(key)
                if key not in self._store or deadline is None:
                    result[key] = None
                else:
                    result[key] = max(0.0, deadline - now)
            return result
        finally:
            self._release_read()

    # -- query & analytics --------------------------------------------------

    def query(
        self,
        predicate: Callable[[Any, Any], bool],
        timestamp: Optional[Union[float, str, datetime]] = None,
    ) -> Dict[Any, Any]:
        """Return {key: value} for every key where predicate(key, value) is True."""
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        self._acquire_read()
        try:
            result = {}
            for key in self._store:
                found, value = self._lookup_unlocked(key, ts)
                if found and predicate(key, value):
                    result[key] = value
            return result
        finally:
            self._release_read()

    def aggregate(
        self,
        func: Callable[[List[Any]], Any],
        keys: Optional[List[Any]] = None,
        timestamp: Optional[Union[float, str, datetime]] = None,
    ) -> Any:
        """Apply func to the list of current values (optionally restricted to `keys`)."""
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        self._acquire_read()
        try:
            target_keys = keys if keys is not None else list(self._store.keys())
            values = []
            for key in target_keys:
                found, val = self._lookup_unlocked(key, ts)
                if found:
                    values.append(val)
            return func(values) if values else None
        finally:
            self._release_read()

    def count(
        self,
        predicate: Optional[Callable[[Any, Any], bool]] = None,
        timestamp: Optional[Union[float, str, datetime]] = None,
    ) -> int:
        """Count all keys, or just those matching predicate(key, value)."""
        if predicate is None:
            return len(self)
        return len(self.query(predicate, timestamp))

    # -- history management ---------------------------------------------------

    def prune_history(
        self,
        key: Any,
        keep_last: Optional[int] = None,
        older_than: Optional[Union[float, str, datetime]] = None,
    ) -> int:
        """Remove old versions for one key. Returns the number removed."""
        self._validate_key(key)
        self._validate_keep_last(keep_last)
        self._acquire_write()
        try:
            if key not in self._store:
                return 0

            cutoff_ts = self._parse_timestamp(older_than) if older_than is not None else None
            if cutoff_ts is not None:
                self._validate_timestamp(cutoff_ts)
            self._journal_append(
                "prune",
                {"key": key, "keep_last": keep_last, "cutoff": cutoff_ts},
            )

            versions = self._store[key]
            original_count = len(versions)

            if keep_last is not None:
                if keep_last == 0:
                    versions.clear()
                else:
                    versions[:] = versions[-keep_last:]

            if cutoff_ts is not None:
                versions[:] = [(ts, val) for ts, val in versions if ts >= cutoff_ts]

            if not versions:
                self._store.pop(key, None)
                self._ttl.pop(key, None)

            removed = original_count - len(versions)
            if self._cache and removed > 0:
                self._cache.invalidate(key)
            if removed > 0:
                self._maybe_checkpoint()

            logger.debug("PRUNE key=%r removed %d versions", key, removed)
            return removed
        finally:
            self._release_write()

    def prune_all_history(
        self,
        keep_last: Optional[int] = None,
        older_than: Optional[Union[float, str, datetime]] = None,
    ) -> int:
        """Prune every key. Returns the total number of versions removed."""
        total_removed = 0
        for key in list(self.keys()):
            total_removed += self.prune_history(key, keep_last, older_than)
        return total_removed

    # -- batch operations ---------------------------------------------------

    def put_many(
        self,
        items: Mapping[Any, Any],
        timestamp: Optional[Union[float, str, datetime]] = None,
        ttl: Optional[float] = None,
    ) -> None:
        """Insert multiple key-value pairs under a single lock acquisition."""
        if not isinstance(items, Mapping):
            raise ChronoMapTypeError("put_many() requires a mapping")
        self._validate_ttl(ttl)
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        keys = list(items.keys())
        for key in keys:
            self._validate_key(key)

        events: List[Tuple[Any, Any, Any, float]] = []
        self._acquire_write()
        try:
            for key in keys:
                self._purge_if_expired(key)
            expires_at = self._current_time() + ttl if ttl is not None else None
            self._journal_append(
                "put_many",
                {
                    "items": [(key, items[key]) for key in keys],
                    "timestamp": ts,
                    "expires_at": expires_at,
                },
            )
            for key in keys:
                value = items[key]
                old_value = None
                if key in self._store and self._store[key]:
                    old_value = self._store[key][-1][1]

                versions = self._store.setdefault(key, [])
                if not versions or ts >= versions[-1][0]:
                    versions.append((ts, value))
                else:
                    idx = insertion_point(versions, ts)
                    versions.insert(idx, (ts, value))

                if expires_at is not None:
                    self._ttl[key] = expires_at

                self._auto_prune(key)
                if self._cache:
                    self._cache.invalidate(key)

                events.append((key, old_value, value, ts))

            self._memory_monitor.check_memory(self._store, self._ttl)
            self._maybe_checkpoint()
            self._stats["writes"] += len(keys)
            logger.debug("PUT_MANY %d items", len(keys))
        finally:
            self._release_write()

        for key, old_value, value, event_ts in events:
            self._trigger_change_callbacks(key, old_value, value, event_ts)

    def update(
        self,
        other: Union[Mapping[Any, Any], Iterable[Tuple[Any, Any]]],
        timestamp: Optional[Union[float, str, datetime]] = None,
        ttl: Optional[float] = None,
    ) -> None:
        """Update from a mapping or iterable of pairs, like ``dict.update()``."""
        try:
            items = dict(other)
        except (TypeError, ValueError) as exc:
            raise ChronoMapTypeError("update() requires a mapping or iterable of key/value pairs") from exc
        self.put_many(items, timestamp=timestamp, ttl=ttl)

    def delete_many(self, keys: List[Any]) -> int:
        """Delete multiple keys under a single lock acquisition. Returns count deleted."""
        keys = list(keys)
        for key in keys:
            self._validate_key(key)
        self._acquire_write()
        try:
            live_keys: List[Any] = []
            for key in keys:
                if key not in self._store:
                    continue
                if self._is_expired(key):
                    self._purge_if_expired(key)
                    continue
                live_keys.append(key)
            live_keys = list(dict.fromkeys(live_keys))
            if live_keys:
                self._journal_append("delete_many", {"keys": live_keys})

            count = 0
            for key in live_keys:
                self._store.pop(key, None)
                self._ttl.pop(key, None)
                if self._cache:
                    self._cache.invalidate(key)
                count += 1
                self._stats["deletes"] += 1
            if count:
                self._maybe_checkpoint()
            logger.debug("DELETE_MANY %d/%d keys", count, len(keys))
            return count
        finally:
            self._release_write()

    # -- advanced queries ---------------------------------------------------

    def get_range(
        self,
        key: Any,
        start_ts: Optional[Union[float, str, datetime]] = None,
        end_ts: Optional[Union[float, str, datetime]] = None,
    ) -> List[Tuple[float, Any]]:
        """All (timestamp, value) pairs for a key within [start_ts, end_ts]."""
        self._validate_key(key)
        self._acquire_read()
        try:
            if self._is_expired(key):
                return []
            versions = self._store.get(key, [])
            if not versions:
                return []

            start = self._parse_timestamp(start_ts) if start_ts is not None else float("-inf")
            end = self._parse_timestamp(end_ts) if end_ts is not None else self._current_time()
            if start_ts is not None:
                self._validate_timestamp(start)
            if end_ts is not None:
                self._validate_timestamp(end)
            if start > end:
                return []

            result = range_versions(versions, start, end)
            logger.debug("GET_RANGE key=%r found %d entries", key, len(result))
            return result
        finally:
            self._release_read()

    def get_latest_keys(self, n: int) -> List[Tuple[Any, float, Any]]:
        """The n most recently written (key, timestamp, value) triples."""
        if isinstance(n, bool) or not isinstance(n, int) or n < 0:
            raise ChronoMapValueError("n must be a non-negative integer")
        if n == 0:
            return []
        self._acquire_read()
        try:
            latest_items = []
            for key, versions in self._store.items():
                if self._is_expired(key):
                    continue
                if versions:
                    ts, val = versions[-1]
                    latest_items.append((key, ts, val))
            result = heapq.nlargest(n, latest_items, key=lambda item: item[1])
            logger.debug("GET_LATEST_KEYS returning %d keys", len(result))
            return result
        finally:
            self._release_read()

    def get_keys_by_value(
        self, value: Any, timestamp: Optional[Union[float, str, datetime]] = None
    ) -> List[Any]:
        """All keys whose value at `timestamp` equals `value`."""
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        self._acquire_read()
        try:
            keys = []
            for key in self._store:
                found, current = self._lookup_unlocked(key, ts)
                if found and current == value:
                    keys.append(key)
            logger.debug("GET_KEYS_BY_VALUE found %d keys", len(keys))
            return keys
        finally:
            self._release_read()

    # -- snapshot, diff, rollback --------------------------------------------

    def snapshot(self) -> "ChronoMap":
        """Deep-copy snapshot of the current state."""
        self._acquire_read()
        try:
            snap = ChronoMap(
                debug=self._debug,
                use_rwlock=self._use_rwlock,
                max_history=self._max_history,
                cache_size=0,
                enable_ttl_cleanup=False,
            )
            snap._store = deepcopy(self._store)
            snap._ttl = deepcopy(self._ttl)
            snap._snapshot_time = self._current_time()
            self._stats["snapshots"] += 1
            logger.debug("SNAPSHOT created at ts=%f", snap._snapshot_time)
            return snap
        finally:
            self._release_read()

    def snapshot_context(self) -> SnapshotContext:
        """`with cm.snapshot_context(): ...` — rolls back automatically on exception."""
        return SnapshotContext(self)

    def rollback(self, snapshot: "ChronoMap") -> None:
        """Restore state from a previous snapshot()."""
        if not isinstance(snapshot, ChronoMap):
            raise ChronoMapTypeError("Expected ChronoMap instance for rollback.")
        self._acquire_write()
        try:
            replacement_store = deepcopy(snapshot._store)
            replacement_ttl = deepcopy(snapshot._ttl)
            self._journal_append(
                "replace",
                {
                    "store": replacement_store,
                    "ttl": replacement_ttl,
                    "snapshot_time": snapshot._snapshot_time,
                },
            )
            self._store = replacement_store
            self._ttl = replacement_ttl
            if self._cache:
                self._cache.clear()
            self._maybe_checkpoint()
            logger.debug("ROLLBACK to snapshot at ts=%s", snapshot._snapshot_time)
        finally:
            self._release_write()

    def _visibility_snapshot(self) -> Tuple[Dict[Any, Any], Set[Any]]:
        """Copy latest live values plus the set of expired keys (one lock pass)."""
        live: Dict[Any, Any] = {}
        expired: Set[Any] = set()
        self._acquire_read()
        try:
            for key, versions in self._store.items():
                if self._is_expired(key):
                    expired.add(key)
                elif versions:
                    live[key] = versions[-1][1]
        finally:
            self._release_read()
        return live, expired

    def diff(self, other: "ChronoMap") -> Set[Any]:
        """Keys whose current value differs between self and other."""
        if not isinstance(other, ChronoMap):
            raise ChronoMapTypeError("Expected ChronoMap instance for diff.")
        self_state, _ = self._visibility_snapshot()
        other_state, _ = other._visibility_snapshot()
        changed = set()
        for key in set(self_state) | set(other_state):
            if self_state.get(key, _MISSING) != other_state.get(key, _MISSING):
                changed.add(key)
        logger.debug("DIFF found %d keys", len(changed))
        return changed

    def diff_detailed(self, other: "ChronoMap") -> List[Tuple[Any, Any, Any]]:
        """(key, other's value, self's value) for every key that differs."""
        if not isinstance(other, ChronoMap):
            raise ChronoMapTypeError("Expected ChronoMap instance for diff_detailed.")
        self_state, self_expired = self._visibility_snapshot()
        other_state, other_expired = other._visibility_snapshot()
        changes = []
        for key in set(self_state) | set(other_state) | self_expired | other_expired:
            if key in self_expired or key in other_expired:
                continue
            old_val = other_state.get(key, _MISSING)
            new_val = self_state.get(key, _MISSING)
            if old_val != new_val:
                changes.append(
                    (
                        key,
                        None if old_val is _MISSING else old_val,
                        None if new_val is _MISSING else new_val,
                    )
                )
        return changes

    # -- merge ---------------------------------------------------------------

    def merge(self, other: "ChronoMap", strategy: str = "timestamp") -> None:
        """Merge another ChronoMap into this one.

        strategy='timestamp' interleaves both histories in timestamp order.
        strategy='overwrite' replaces this map's history for any key that
        exists in `other`.
        """
        if not isinstance(other, ChronoMap):
            raise ChronoMapTypeError("Expected ChronoMap instance for merge.")
        if strategy not in ("timestamp", "overwrite"):
            raise ChronoMapValueError(f"Invalid merge strategy: {strategy}")

        other._acquire_read()
        try:
            other_store = deepcopy(other._store)
            other_ttl = deepcopy(other._ttl)
        finally:
            other._release_read()

        self._acquire_write()
        try:
            self._journal_append(
                "merge",
                {
                    "other_store": other_store,
                    "other_ttl": other_ttl,
                    "strategy": strategy,
                },
            )
            if strategy == "timestamp":
                for key, other_versions in other_store.items():
                    target_versions = self._store.get(key, [])
                    if other_versions:
                        self._store[key] = list(
                            heapq.merge(target_versions, other_versions, key=lambda item: item[0])
                        )
                    self._auto_prune(key)
                    self._stats["writes"] += len(other_versions)

                for key, expiry in other_ttl.items():
                    if key not in self._ttl or expiry > self._ttl[key]:
                        self._ttl[key] = expiry
            else:  # overwrite
                for key, versions in other_store.items():
                    self._store[key] = versions
                    self._auto_prune(key)
                    self._stats["writes"] += len(versions)
                for key, expiry in other_ttl.items():
                    self._ttl[key] = expiry

            if self._cache:
                self._cache.clear()
            self._memory_monitor.check_memory(self._store, self._ttl)
            self._maybe_checkpoint()

            logger.debug("MERGE completed with strategy=%s", strategy)
        finally:
            self._release_write()

    # -- utilities -------------------------------------------------------------

    def latest(self) -> Dict[Any, Any]:
        """{key: latest_value} for every non-expired key."""
        self._acquire_read()
        try:
            return {k: v[-1][1] for k, v in self._store.items() if v and not self._is_expired(k)}
        finally:
            self._release_read()

    def history(self, key: Any) -> List[Tuple[float, Any]]:
        """Full (timestamp, value) history for a key."""
        self._validate_key(key)
        self._acquire_read()
        try:
            if self._is_expired(key):
                return []
            return list(self._store.get(key, []))
        finally:
            self._release_read()

    def keys_with_history_count(self) -> Dict[Any, int]:
        """{key: number_of_versions_stored} for every non-expired key."""
        self._acquire_read()
        try:
            return {
                key: len(versions)
                for key, versions in self._store.items()
                if versions and not self._is_expired(key)
            }
        finally:
            self._release_read()

    def clear(self) -> None:
        """Remove everything."""
        self._acquire_write()
        try:
            self._journal_append("clear", {})
            self._store.clear()
            self._ttl.clear()
            if self._cache:
                self._cache.clear()
            self._maybe_checkpoint()
            logger.debug("CLEAR all data")
        finally:
            self._release_write()

    def clean_expired_keys(self) -> int:
        """Manually sweep expired keys. Returns the number removed."""
        self._acquire_write()
        try:
            expired_keys = [key for key in self._ttl if self._is_expired(key)]
            if expired_keys:
                self._journal_append("delete_many", {"keys": expired_keys})
            for key in expired_keys:
                self._store.pop(key, None)
                del self._ttl[key]
                if self._cache:
                    self._cache.invalidate(key)
            if expired_keys:
                self._maybe_checkpoint()
            logger.debug("CLEAN_EXPIRED removed %d keys", len(expired_keys))
            return len(expired_keys)
        finally:
            self._release_write()

    def expire(self, key: Any, ttl: float) -> bool:
        """Set or replace a key's TTL. Returns False if the key is not live."""
        self._validate_key(key)
        self._validate_ttl(ttl)
        self._acquire_write()
        try:
            self._purge_if_expired(key)
            if key not in self._store or not self._store[key]:
                return False
            expires_at = self._current_time() + ttl
            self._journal_append("expire", {"key": key, "expires_at": expires_at})
            self._ttl[key] = expires_at
            if self._cache:
                self._cache.invalidate(key)
            self._maybe_checkpoint()
            return True
        finally:
            self._release_write()

    def expire_many(self, keys: Iterable[Any], ttl: float) -> int:
        """Set or replace the same TTL for several live keys. Returns the count."""
        self._validate_ttl(ttl)
        keys = list(keys)
        for key in keys:
            self._validate_key(key)
        self._acquire_write()
        try:
            live_keys: List[Any] = []
            for key in keys:
                self._purge_if_expired(key)
                if key in self._store and self._store[key]:
                    live_keys.append(key)
            live_keys = list(dict.fromkeys(live_keys))
            if not live_keys:
                return 0

            expires_at = self._current_time() + ttl
            self._journal_append("expire_many", {"keys": live_keys, "expires_at": expires_at})
            for key in live_keys:
                self._ttl[key] = expires_at
                if self._cache:
                    self._cache.invalidate(key)
            self._maybe_checkpoint()
            return len(live_keys)
        finally:
            self._release_write()

    def get_stats(self) -> Dict[str, Any]:
        """Operation counters, cache stats, and size info."""
        stats: Dict[str, Any] = self._stats.copy()

        if self._cache:
            cache_stats = self._cache.get_stats()
            stats.update(
                {
                    "cache_hits": cache_stats["hits"],
                    "cache_misses": cache_stats["misses"],
                    "cache_size": cache_stats["size"],
                    "cache_hit_rate": cache_stats["hit_rate"],
                }
            )

        if self._ttl_cleanup_thread:
            stats["ttl_cleanup_count"] = self._ttl_cleanup_thread.cleaned_count
        if self._wal is not None:
            stats["journal_path"] = str(self._wal.path)
            stats["journal_bytes"] = self._wal.size
            stats["journal_sequence"] = self._wal.sequence

        self._acquire_read()
        try:
            stats["total_keys"] = sum(1 for key in self._store if not self._is_expired(key))
            stats["total_versions"] = sum(len(versions) for versions in self._store.values())
            stats["expired_keys"] = sum(1 for key in self._ttl if self._is_expired(key))
        finally:
            self._release_read()
        return stats

    def memory_usage(self) -> int:
        """Approximate bytes used by stored histories and TTL metadata."""
        self._acquire_read()
        try:
            return MemoryMonitor.estimate_size(self._store) + MemoryMonitor.estimate_size(self._ttl)
        finally:
            self._release_read()

    def reset_stats(self) -> None:
        self._stats = {
            "reads": 0,
            "writes": 0,
            "deletes": 0,
            "snapshots": 0,
            "auto_prunes": 0,
            "cache_hits": 0,
            "cache_misses": 0,
        }
        if self._cache:
            self._cache.clear()
        if self._ttl_cleanup_thread:
            self._ttl_cleanup_thread.cleaned_count = 0

    # -- export -----------------------------------------------------------------

    def iter_rows(self) -> Iterator[Dict[str, Any]]:
        """Stream one row per live version without building the full result.

        Keys and version lists are snapshotted under the read lock; values are
        not deep-copied. Rows match the columns returned by ``to_dataframe()``.
        """
        self._acquire_read()
        try:
            snapshot = [
                (key, list(versions)) for key, versions in self._store.items() if not self._is_expired(key)
            ]
        finally:
            self._release_read()

        for key, versions in snapshot:
            for version_idx, (ts, value) in enumerate(versions):
                yield {
                    "key": key,
                    "value": value,
                    "timestamp": ts,
                    "datetime": datetime.fromtimestamp(ts),
                    "version": version_idx,
                }

    def to_dataframe(self):
        """Export to a pandas DataFrame (columns: key, value, timestamp, datetime, version).

        Requires the optional `pandas` extra: pip install chronomap[pandas]
        """
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError(
                "pandas is required for to_dataframe(). Install with: pip install chronomap[pandas]"
            ) from exc

        return pd.DataFrame(list(self.iter_rows()))

    # -- container protocol -------------------------------------------------------

    def keys(self) -> Iterator[Any]:
        self._acquire_read()
        try:
            keys_list = [key for key in self._store if not self._is_expired(key)]
        finally:
            self._release_read()
        yield from keys_list

    def values(self) -> Iterator[Any]:
        self._acquire_read()
        try:
            values_list = [v[-1][1] for k, v in self._store.items() if not self._is_expired(k) and v]
        finally:
            self._release_read()
        yield from values_list

    def items(self) -> Iterator[Tuple[Any, Any]]:
        self._acquire_read()
        try:
            items_list = [(k, v[-1][1]) for k, v in self._store.items() if not self._is_expired(k) and v]
        finally:
            self._release_read()
        yield from items_list

    def iter_history(self, key: Any) -> Iterator[Tuple[float, Any]]:
        self._validate_key(key)
        self._acquire_read()
        try:
            versions = [] if self._is_expired(key) else list(self._store.get(key, []))
        finally:
            self._release_read()
        yield from versions

    def __iter__(self) -> Iterator[Any]:
        return self.keys()

    def __len__(self) -> int:
        self._acquire_read()
        try:
            expired = sum(1 for key in self._store if self._is_expired(key))
            return len(self._store) - expired
        finally:
            self._release_read()

    def __contains__(self, key: Any) -> bool:
        self._acquire_read()
        try:
            if key not in self._store or self._is_expired(key):
                return False
            return len(self._store.get(key, [])) > 0
        finally:
            self._release_read()

    def __getitem__(self, key: Any) -> Any:
        return self.get(key, strict=True)

    def __setitem__(self, key: Any, value: Any) -> None:
        self.put(key, value)

    def __delitem__(self, key: Any) -> None:
        if not self.delete(key):
            raise ChronoMapKeyError(key)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ChronoMap):
            return NotImplemented
        return self.latest() == other.latest()

    def __bool__(self) -> bool:
        return len(self) > 0

    def __repr__(self) -> str:
        self._acquire_read()
        try:
            non_expired = [k for k in self._store if not self._is_expired(k)]
            preview = non_expired[:10]
            suffix = "..." if len(non_expired) > 10 else ""
            return f"ChronoMap(keys={preview}{suffix})"
        finally:
            self._release_read()

    @property
    def snapshot_time(self) -> Optional[float]:
        return self._snapshot_time

    # -- persistence -----------------------------------------------------------------

    def to_dict(self, compress: Union[bool, str] = False) -> Union[Dict[str, Any], bytes]:
        """Serialize to a plain dict, or to compressed bytes if `compress` is set."""
        self._acquire_read()
        try:
            data = {
                "store": deepcopy(self._store),
                "ttl": deepcopy(self._ttl),
                "snapshot_time": self._snapshot_time,
                "version": __version__,
                "max_history": self._max_history,
            }

            if compress:
                pickled = pickle.dumps(data)
                method = "zlib" if compress is True else compress
                compressors = {
                    "zlib": lambda b: zlib.compress(b, level=6),
                    "gzip": lambda b: gzip.compress(b, compresslevel=6),
                    "bz2": lambda b: bz2.compress(b, compresslevel=6),
                    "lzma": lambda b: lzma.compress(b, preset=6),
                }
                if method not in compressors:
                    raise ChronoMapValueError(f"Unknown compression method: {compress}")
                compressed = compressors[method](pickled)
                logger.debug(
                    "COMPRESS (%s): %d -> %d bytes (%.1f%%)",
                    method,
                    len(pickled),
                    len(compressed),
                    100 * len(compressed) / len(pickled),
                )
                return method.encode() + b"|" + compressed

            return data
        finally:
            self._release_read()

    @classmethod
    def from_dict(
        cls,
        data: Union[Dict[str, Any], bytes],
        debug: bool = False,
        use_rwlock: bool = True,
        **kwargs,
    ) -> "ChronoMap":
        """Reconstruct a ChronoMap from to_dict() output (dict or compressed bytes)."""
        payload: Any = data
        if isinstance(data, bytes):
            decompressors: Dict[bytes, Callable[[bytes], bytes]] = {
                b"zlib": zlib.decompress,
                b"gzip": gzip.decompress,
                b"bz2": bz2.decompress,
                b"lzma": lzma.decompress,
            }
            if b"|" in data[:20]:
                method_bytes, compressed = data.split(b"|", 1)
                decompress = decompressors.get(method_bytes)
                if decompress is None:
                    raise ChronoMapValueError(f"Unknown compression marker: {method_bytes!r}")
                decompressed = decompress(compressed)
            else:
                # Legacy files saved before the method marker existed.
                decompressed = zlib.decompress(data)
            payload = pickle.loads(decompressed)

        if not isinstance(payload, dict):
            raise ChronoMapTypeError("Serialized data must be a mapping")
        raw_store = payload.get("store", {})
        raw_ttl = payload.get("ttl", {})
        if not isinstance(raw_store, dict) or not isinstance(raw_ttl, dict):
            raise ChronoMapTypeError("Serialized store and ttl must be mappings")

        normalized_store: Dict[Any, List[Tuple[float, Any]]] = {}
        for key, versions in raw_store.items():
            if not isinstance(versions, (list, tuple)):
                raise ChronoMapValueError(f"History for key {key!r} must be a sequence")
            normalized: List[Tuple[float, Any]] = []
            for version in versions:
                if not isinstance(version, (list, tuple)) or len(version) != 2:
                    raise ChronoMapValueError(f"Malformed version for key {key!r}: {version!r}")
                timestamp = version[0]
                if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
                    raise ChronoMapTypeError(f"Timestamp for key {key!r} must be numeric")
                if not math.isfinite(timestamp):
                    raise ChronoMapValueError(f"Timestamp for key {key!r} must be finite")
                if normalized and timestamp < normalized[-1][0]:
                    raise ChronoMapValueError(f"History for key {key!r} is not sorted")
                normalized.append((float(timestamp), version[1]))
            if not normalized:
                raise ChronoMapValueError(f"History for key {key!r} must not be empty")
            normalized_store[key] = normalized

        normalized_ttl: Dict[Any, float] = {}
        for key, deadline in raw_ttl.items():
            if isinstance(deadline, bool) or not isinstance(deadline, (int, float)):
                raise ChronoMapTypeError(f"TTL deadline for key {key!r} must be numeric")
            if not math.isfinite(deadline):
                raise ChronoMapValueError(f"TTL deadline for key {key!r} must be finite")
            if key not in normalized_store:
                raise ChronoMapValueError(f"TTL key {key!r} has no stored history")
            normalized_ttl[key] = float(deadline)

        snapshot_time = payload.get("snapshot_time")
        if snapshot_time is not None:
            if isinstance(snapshot_time, bool) or not isinstance(snapshot_time, (int, float)):
                raise ChronoMapTypeError("snapshot_time must be numeric or None")
            if not math.isfinite(snapshot_time):
                raise ChronoMapValueError("snapshot_time must be finite")
            snapshot_time = float(snapshot_time)

        options = dict(kwargs)
        options.setdefault("max_history", payload.get("max_history"))
        instance = cls(debug=debug, use_rwlock=use_rwlock, **options)
        if instance._max_history is not None:
            normalized_store = {
                key: versions[-instance._max_history :] for key, versions in normalized_store.items()
            }
        instance._store = normalized_store
        instance._ttl = normalized_ttl
        instance._snapshot_time = snapshot_time
        if instance._wal is not None:
            try:
                instance._wal.append(
                    "replace",
                    {
                        "store": instance._store,
                        "ttl": instance._ttl,
                        "snapshot_time": instance._snapshot_time,
                        "restore_snapshot_time": True,
                    },
                )
                instance._maybe_checkpoint()
            except BaseException:
                instance.close()
                raise
        return instance

    def save_json(self, file_path: Union[str, Path]) -> None:
        """Save to a JSON file. Keys are coerced to strings (JSON has no non-string keys)."""
        path = Path(file_path)
        raw_data = self.to_dict()
        if not isinstance(raw_data, dict):
            raise ChronoMapTypeError("Uncompressed serialization must produce a mapping")
        data = raw_data
        json_data = {
            "store": {str(k): v for k, v in data["store"].items()},
            "ttl": {str(k): v for k, v in data["ttl"].items()},
            "snapshot_time": data["snapshot_time"],
            "version": data.get("version"),
            "max_history": data.get("max_history"),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=2)
        logger.debug("SAVE_JSON to %s", file_path)

    @classmethod
    def load_json(
        cls, file_path: Union[str, Path], debug: bool = False, use_rwlock: bool = True, **kwargs
    ) -> "ChronoMap":
        path = Path(file_path)
        with open(path, "r", encoding="utf-8") as f:
            json_data = json.load(f)
        if not isinstance(json_data, dict):
            raise ChronoMapTypeError("JSON save file must contain an object")
        if "store" not in json_data:
            raise ChronoMapValueError("JSON save file is missing required 'store' section")
        data = {
            "store": json_data["store"],
            "ttl": json_data.get("ttl", {}),
            "snapshot_time": json_data.get("snapshot_time"),
            "max_history": json_data.get("max_history"),
        }
        logger.debug("LOAD_JSON from %s", file_path)
        return cls.from_dict(data, debug=debug, use_rwlock=use_rwlock, **kwargs)

    def save_pickle(self, file_path: Union[str, Path], compress: Union[bool, str] = False) -> None:
        path = Path(file_path)
        data = self.to_dict(compress=compress)
        with open(path, "wb") as f:
            if isinstance(data, bytes):
                f.write(data)
            else:
                pickle.dump(data, f)
        logger.debug("SAVE_PICKLE to %s (compressed=%s)", file_path, compress)

    @classmethod
    def load_pickle(
        cls, file_path: Union[str, Path], debug: bool = False, use_rwlock: bool = True, **kwargs
    ) -> "ChronoMap":
        path = Path(file_path)
        with open(path, "rb") as f:
            data_bytes = f.read()
        try:
            data = pickle.loads(data_bytes)
        except Exception:
            data = data_bytes  # compressed; let from_dict figure out the method
        logger.debug("LOAD_PICKLE from %s", file_path)
        return cls.from_dict(data, debug=debug, use_rwlock=use_rwlock, **kwargs)


__all__ = ["ChronoMap"]
