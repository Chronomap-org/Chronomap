# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Asyncio version of ChronoMap.

Deliberately a separate implementation rather than a wrapper around the
threaded class: an asyncio.Lock is tied to one event loop and one task
scheduling model, while the synchronous class is built around an
RWLock and real OS threads. Sharing the storage logic would mean sharing
the locking rules, and those rules are not interchangeable.
"""

from __future__ import annotations

import asyncio
import bz2
import contextlib
import gzip
import heapq
import inspect
import json
import lzma
import logging
import math
import pickle
import weakref
import zlib
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

from ._cache import LRUCache
from ._journal import Journal, JournalCorruptionError, JournalError, read_checkpoint, write_checkpoint
from ._memory import MemoryMonitor
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


async def _async_ttl_cleanup_loop(chronomap_ref: "weakref.ref", interval: float) -> None:
    """Wake on an interval and sweep expired keys without keeping the map alive."""
    while True:
        await asyncio.sleep(interval)
        chronomap = chronomap_ref()
        if chronomap is None:
            return
        try:
            await chronomap.clean_expired_keys()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Error in async TTL cleanup task")


class AsyncSnapshotContext:
    """`async with cm.snapshot_context(): ...` rolls back if the block raises."""

    def __init__(self, chronomap: "AsyncChronoMap") -> None:
        self.chronomap = chronomap
        self.snapshot: Optional["AsyncChronoMap"] = None

    async def __aenter__(self) -> "AsyncChronoMap":
        self.snapshot = await self.chronomap.snapshot()
        return self.chronomap

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is not None:
            assert self.snapshot is not None
            await self.chronomap.rollback(self.snapshot)
        return False


class AsyncChronoMap:
    """Async counterpart to ChronoMap, for use inside asyncio applications.

    The store, TTL semantics, history ordering, callback surface, and
    persistence helpers mirror ``ChronoMap``. The differences are the
    ones asyncio forces: locks are awaited, snapshots are awaited, and the
    optional cleanup task is tied to the running event loop.

    Args:
        debug: Enable debug logging.
        max_history: Cap on versions kept per key.
        cache_size: Read cache capacity; 0 disables it.
        enable_ttl_cleanup: Start the background cleanup task on the first
            write. Defaults to False because a task created in ``__init__``
            would outlive maps built outside an event loop.
        ttl_cleanup_interval: Seconds between cleanup passes.
        wal_path: Enable a durable write-ahead journal at this path. State is
            replayed synchronously from the journal and checkpoint on
            construction.
        wal_sync: Flush and fsync every journal record in a worker thread.
            Disable only when losing the most recent writes on a crash is
            acceptable.
        wal_auto_checkpoint_bytes: Compact the journal into a checkpoint once
            it grows past this many bytes. None disables automatic
            checkpoints.
        max_memory_mb: Approximate memory ceiling for stored histories and TTL
            metadata. Crossing 80% logs a warning; crossing 100% raises
            ``ChronoMapMemoryError`` after the mutation is journaled.
    """

    def __init__(
        self,
        debug: bool = False,
        max_history: Optional[int] = None,
        cache_size: int = 1000,
        enable_ttl_cleanup: bool = False,
        ttl_cleanup_interval: float = 60.0,
        wal_path: Optional[Union[str, Path]] = None,
        wal_sync: bool = True,
        wal_auto_checkpoint_bytes: Optional[int] = 64 * 1024 * 1024,
        max_memory_mb: Optional[float] = None,
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
        if not isinstance(wal_sync, bool):
            raise ChronoMapValueError("wal_sync must be a boolean")
        if wal_auto_checkpoint_bytes is not None and (
            isinstance(wal_auto_checkpoint_bytes, bool)
            or not isinstance(wal_auto_checkpoint_bytes, int)
            or wal_auto_checkpoint_bytes < 1
        ):
            raise ChronoMapValueError("wal_auto_checkpoint_bytes must be a positive integer or None")
        if max_memory_mb is not None and (
            isinstance(max_memory_mb, bool)
            or not isinstance(max_memory_mb, (int, float))
            or not math.isfinite(max_memory_mb)
            or max_memory_mb <= 0
        ):
            raise ChronoMapValueError("max_memory_mb must be a positive number or None")

        self._store: Dict[Any, List[Tuple[float, Any]]] = {}
        self._ttl: Dict[Any, float] = {}
        self._lock = asyncio.Lock()
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

        self._enable_ttl_cleanup = enable_ttl_cleanup
        self._ttl_cleanup_interval = float(ttl_cleanup_interval)
        self._cleanup_task: Optional[asyncio.Task] = None
        self.cleaned_count = 0

        if debug:
            logger.setLevel(logging.DEBUG)
            if not logger.handlers:
                handler = logging.StreamHandler()
                handler.setFormatter(logging.Formatter("[AsyncChronoMap] %(levelname)s: %(message)s"))
                logger.addHandler(handler)

    # -- lifecycle ----------------------------------------------------------

    def __del__(self) -> None:
        journal = getattr(self, "_wal", None)
        if journal is not None:
            try:
                journal.close()
            except Exception:
                pass

    def _ensure_cleanup_task(self) -> None:
        if not self._enable_ttl_cleanup:
            return
        if self._cleanup_task is not None and not self._cleanup_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._cleanup_task = loop.create_task(
            _async_ttl_cleanup_loop(weakref.ref(self), self._ttl_cleanup_interval)
        )

    def start_cleanup(self) -> None:
        """Start the background cleanup task in the current event loop."""
        self._enable_ttl_cleanup = True
        self._ensure_cleanup_task()

    def stop_cleanup(self) -> None:
        """Cancel the background cleanup task without awaiting it."""
        task = self._cleanup_task
        self._cleanup_task = None
        if task is not None and not task.done():
            task.cancel()

    async def aclose(self) -> None:
        """Cancel cleanup and close the journal; safe to call more than once."""
        task = self._cleanup_task
        self._cleanup_task = None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        async with self._lock:
            if self._wal is not None:
                await self._run_blocking(self._wal.close)

    async def __aenter__(self) -> "AsyncChronoMap":
        self._ensure_cleanup_task()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.aclose()
        return False

    # -- internal helpers ---------------------------------------------------

    def _current_time(self) -> float:
        return datetime.now(timezone.utc).timestamp()

    # -- write-ahead journal ------------------------------------------------

    async def _journal_append(self, operation: str, data: Dict[str, Any]) -> None:
        if self._wal is not None:
            await self._run_blocking(self._wal.append, operation, data)

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

    async def checkpoint(self) -> None:
        """Write a durable checkpoint and truncate the journal."""
        async with self._lock:
            await self._run_blocking(self._checkpoint_unlocked)

    def _maybe_checkpoint_unlocked(self) -> None:
        if (
            self._wal is not None
            and self._wal_auto_checkpoint_bytes is not None
            and self._wal.size >= self._wal_auto_checkpoint_bytes
        ):
            try:
                self._checkpoint_unlocked()
            except Exception as exc:
                logger.warning("WAL auto-checkpoint failed; keeping journal: %s", exc)

    async def _maybe_checkpoint(self) -> None:
        if (
            self._wal is not None
            and self._wal_auto_checkpoint_bytes is not None
            and self._wal.size >= self._wal_auto_checkpoint_bytes
        ):
            try:
                await self._run_blocking(self._checkpoint_unlocked)
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
                return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
            except ValueError:
                raise ChronoMapValueError(f"Invalid datetime string: {timestamp}")
        if isinstance(timestamp, datetime):
            return timestamp.timestamp()
        if isinstance(timestamp, (int, float)):
            return float(timestamp)
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

    async def _purge_if_expired(self, key: Any) -> None:
        if self._is_expired(key):
            await self._journal_append("delete", {"key": key})
            self._store.pop(key, None)
            self._ttl.pop(key, None)
            if self._cache:
                self._cache.invalidate(key)
            await self._maybe_checkpoint()

    def _lookup_unlocked(self, key: Any, ts: float) -> Tuple[bool, Any]:
        if self._is_expired(key):
            return False, None
        versions = self._store.get(key, [])
        if not versions:
            return False, None
        return value_at(versions, ts)

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

    async def _fire_callbacks(self, key: Any, old_value: Any, value: Any, ts: float) -> None:
        for callback in list(self._change_callbacks):
            try:
                result = callback(key, old_value, value, ts)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("Error in async change callback")
        for callback in list(self._key_subscribers.get(key, [])):
            try:
                result = callback(old_value, value, ts)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("Error in async key subscriber callback for %r", key)

    # -- event hooks --------------------------------------------------------

    def on_change(self, callback: Callable) -> None:
        """Register a sync or async callback fired on every write."""
        if not callable(callback):
            raise ChronoMapTypeError("Change callback must be callable")
        self._change_callbacks.append(callback)

    def subscribe(self, key: Any, callback: Callable) -> None:
        """Register a sync or async callback for one key."""
        self._validate_key(key)
        if not callable(callback):
            raise ChronoMapTypeError("Subscriber callback must be callable")
        self._key_subscribers.setdefault(key, []).append(callback)

    def unsubscribe(self, key: Any, callback: Callable) -> bool:
        self._validate_key(key)
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

    def remove_change_callback(self, callback: Callable) -> bool:
        try:
            self._change_callbacks.remove(callback)
            return True
        except ValueError:
            return False

    # -- reads and writes ---------------------------------------------------

    async def put(
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
        self._ensure_cleanup_task()

        async with self._lock:
            await self._purge_if_expired(key)
            old_value = None
            if key in self._store and self._store[key]:
                old_value = self._store[key][-1][1]

            expires_at = self._current_time() + ttl if ttl is not None else None
            await self._journal_append(
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
            await self._maybe_checkpoint()
            self._stats["writes"] += 1

        await self._fire_callbacks(key, old_value, value, ts)

    async def put_many(
        self,
        items: Mapping[Any, Any],
        timestamp: Optional[Union[float, str, datetime]] = None,
        ttl: Optional[float] = None,
    ) -> None:
        """Insert multiple pairs under a single lock acquisition."""
        if not isinstance(items, Mapping):
            raise ChronoMapTypeError("put_many() requires a mapping")
        self._validate_ttl(ttl)
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        keys = list(items.keys())
        for key in keys:
            self._validate_key(key)
        self._ensure_cleanup_task()

        events: List[Tuple[Any, Any, Any, float]] = []
        async with self._lock:
            for key in keys:
                await self._purge_if_expired(key)
            expires_at = self._current_time() + ttl if ttl is not None else None
            await self._journal_append(
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
            await self._maybe_checkpoint()
            self._stats["writes"] += len(keys)

        for key, old_value, value, event_ts in events:
            await self._fire_callbacks(key, old_value, value, event_ts)

    async def update(
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
        await self.put_many(items, timestamp=timestamp, ttl=ttl)

    async def get(
        self,
        key: Any,
        timestamp: Optional[Union[float, str, datetime]] = None,
        default: Any = None,
        *,
        strict: bool = False,
    ) -> Any:
        """Retrieve the value for a key at a timestamp, with the read cache."""
        self._validate_key(key)
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        cache_key = (key, None if timestamp is None else ts)

        async with self._lock:
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
            return value

    async def get_many(
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

        async with self._lock:
            for key in keys:
                self._validate_key(key)
                found, value = self._lookup_unlocked(key, ts)
                if found:
                    result[key] = value
                elif strict:
                    missing.append(key)

        if missing:
            raise ChronoMapKeyError(missing[0])
        return result

    async def exists(self, key: Any, timestamp: Optional[Union[float, str, datetime]] = None) -> bool:
        """True when the key resolves to a value at the timestamp."""
        self._validate_key(key)
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        async with self._lock:
            return self._lookup_unlocked(key, ts)[0]

    async def get_or_set(
        self, key: Any, default_factory: Callable[[], Any], ttl: Optional[float] = None
    ) -> Any:
        """Return the current value, or create/store one via default_factory()."""
        self._validate_key(key)
        if not callable(default_factory):
            raise ChronoMapTypeError("default_factory must be callable")
        self._validate_ttl(ttl)
        ts = self._current_time()
        self._ensure_cleanup_task()
        value: Any = None

        async with self._lock:
            await self._purge_if_expired(key)
            versions = self._store.get(key, [])
            if versions:
                self._stats["reads"] += 1
                return versions[-1][1]

        value = default_factory()
        if inspect.isawaitable(value):
            value = await value

        async with self._lock:
            await self._purge_if_expired(key)
            versions = self._store.get(key, [])
            if versions:
                self._stats["reads"] += 1
                return versions[-1][1]

            ts = self._current_time()
            expires_at = self._current_time() + ttl if ttl is not None else None
            await self._journal_append(
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
            await self._maybe_checkpoint()
            self._stats["writes"] += 1

        await self._fire_callbacks(key, None, value, ts)
        return value

    async def get_or_default(self, key: Any, default: Any, ttl: Optional[float] = None) -> Any:
        factory = default if callable(default) else lambda: default
        return await self.get_or_set(key, factory, ttl=ttl)

    async def setdefault(self, key: Any, default: Any = None, ttl: Optional[float] = None) -> Any:
        """Return the latest value, inserting `default` first when absent."""
        self._validate_key(key)
        self._validate_ttl(ttl)
        ts = self._current_time()
        self._ensure_cleanup_task()
        created = False

        async with self._lock:
            await self._purge_if_expired(key)

            versions = self._store.get(key, [])
            if versions:
                self._stats["reads"] += 1
                return versions[-1][1]

            expires_at = self._current_time() + ttl if ttl is not None else None
            await self._journal_append(
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
            await self._maybe_checkpoint()
            self._stats["writes"] += 1
            created = True

        if created:
            await self._fire_callbacks(key, None, default, ts)
        return default

    async def pop(self, key: Any, default: Any = _MISSING) -> Any:
        """Return the current value and erase the key, or return `default` if absent."""
        self._validate_key(key)
        async with self._lock:
            if self._is_expired(key):
                await self._purge_if_expired(key)
            found, value = self._lookup_unlocked(key, self._current_time())
            if not found:
                if default is _MISSING:
                    raise ChronoMapKeyError(key)
                return default

            await self._journal_append("delete", {"key": key})
            del self._store[key]
            self._ttl.pop(key, None)
            if self._cache:
                self._cache.invalidate(key)
            await self._maybe_checkpoint()
            self._stats["deletes"] += 1
            return value

    async def pop_many(self, keys: Iterable[Any]) -> Dict[Any, Any]:
        """Remove several keys under one lock and return their latest live values."""
        keys = list(keys)
        for key in keys:
            self._validate_key(key)
        async with self._lock:
            live_keys: List[Any] = []
            for key in keys:
                if key not in self._store:
                    continue
                if self._is_expired(key):
                    await self._purge_if_expired(key)
                    continue
                live_keys.append(key)
            live_keys = list(dict.fromkeys(live_keys))
            result = {key: self._store[key][-1][1] for key in live_keys}
            if live_keys:
                await self._journal_append("delete_many", {"keys": live_keys})
            for key in live_keys:
                self._store.pop(key, None)
                self._ttl.pop(key, None)
                if self._cache:
                    self._cache.invalidate(key)
                self._stats["deletes"] += 1
            if live_keys:
                await self._maybe_checkpoint()
            return result

    async def popitem(self, last: bool = True) -> Tuple[Any, Any]:
        """Remove and return the newest live pair, or the oldest when ``last=False``."""
        async with self._lock:
            keys = reversed(list(self._store)) if last else list(self._store)
            for key in keys:
                if self._is_expired(key):
                    await self._purge_if_expired(key)
                    continue
                versions = self._store.get(key, [])
                if not versions:
                    continue

                value = versions[-1][1]
                await self._journal_append("delete", {"key": key})
                del self._store[key]
                self._ttl.pop(key, None)
                if self._cache:
                    self._cache.invalidate(key)
                await self._maybe_checkpoint()
                self._stats["deletes"] += 1
                return key, value

            raise ChronoMapKeyError("popitem(): ChronoMap is empty")

    async def delete(self, key: Any) -> bool:
        """Delete all history for a key. Returns True if a live key was removed."""
        self._validate_key(key)
        async with self._lock:
            if key not in self._store:
                return False
            if self._is_expired(key):
                await self._purge_if_expired(key)
                return False

            await self._journal_append("delete", {"key": key})
            del self._store[key]
            self._ttl.pop(key, None)
            if self._cache:
                self._cache.invalidate(key)
            await self._maybe_checkpoint()
            self._stats["deletes"] += 1
            return True

    async def delete_many(self, keys: List[Any]) -> int:
        """Delete multiple keys under one lock; expired keys are skipped."""
        keys = list(keys)
        for key in keys:
            self._validate_key(key)
        count = 0
        async with self._lock:
            live_keys: List[Any] = []
            for key in keys:
                if key not in self._store:
                    continue
                if self._is_expired(key):
                    await self._purge_if_expired(key)
                    continue
                live_keys.append(key)
            live_keys = list(dict.fromkeys(live_keys))
            if live_keys:
                await self._journal_append("delete_many", {"keys": live_keys})

            for key in live_keys:
                del self._store[key]
                self._ttl.pop(key, None)
                if self._cache:
                    self._cache.invalidate(key)
                count += 1
                self._stats["deletes"] += 1
            if count:
                await self._maybe_checkpoint()
        return count

    async def persist(self, key: Any) -> bool:
        """Remove a key's TTL so it stops expiring. Returns True if a TTL was removed."""
        self._validate_key(key)
        async with self._lock:
            if self._is_expired(key):
                await self._purge_if_expired(key)
                return False
            if key not in self._ttl:
                return False

            await self._journal_append("persist", {"key": key})
            del self._ttl[key]
            if self._cache:
                self._cache.invalidate(key)
            await self._maybe_checkpoint()
            return True

    async def persist_many(self, keys: Iterable[Any]) -> int:
        """Remove TTLs from several live keys. Returns the number persisted."""
        keys = list(keys)
        for key in keys:
            self._validate_key(key)
        async with self._lock:
            live_keys: List[Any] = []
            for key in keys:
                if key not in self._store:
                    continue
                if self._is_expired(key):
                    await self._purge_if_expired(key)
                    continue
                if key in self._ttl:
                    live_keys.append(key)
            live_keys = list(dict.fromkeys(live_keys))
            if not live_keys:
                return 0

            await self._journal_append("persist_many", {"keys": live_keys})
            for key in live_keys:
                del self._ttl[key]
                if self._cache:
                    self._cache.invalidate(key)
            await self._maybe_checkpoint()
            return len(live_keys)

    async def ttl_remaining(self, key: Any) -> Optional[float]:
        """Seconds until `key` expires, or None if it is absent or has no TTL.

        An expired key that has not been swept yet returns ``0.0``.
        """
        self._validate_key(key)
        async with self._lock:
            if key not in self._store:
                return None
            deadline = self._ttl.get(key)
            if deadline is None:
                return None
            return max(0.0, deadline - self._current_time())

    async def ttl_remaining_many(self, keys: Iterable[Any]) -> Dict[Any, Optional[float]]:
        """Map each key to its remaining TTL, using one consistent snapshot.

        Missing keys and keys without a TTL map to ``None``. An expired key
        that has not been swept yet maps to ``0.0``.
        """
        keys = list(keys)
        for key in keys:
            self._validate_key(key)
        async with self._lock:
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

    # -- queries ------------------------------------------------------------

    async def query(
        self,
        predicate: Callable[[Any, Any], Any],
        timestamp: Optional[Union[float, str, datetime]] = None,
    ) -> Dict[Any, Any]:
        """{key: value} for every key where predicate(key, value) is truthy."""
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        result = {}
        async with self._lock:
            pairs = []
            for key in self._store:
                found, value = self._lookup_unlocked(key, ts)
                if found:
                    pairs.append((key, value))
        for key, value in pairs:
            keep = predicate(key, value)
            if inspect.isawaitable(keep):
                keep = await keep
            if keep:
                result[key] = value
        return result

    async def aggregate(
        self,
        func: Callable[[List[Any]], Any],
        keys: Optional[List[Any]] = None,
        timestamp: Optional[Union[float, str, datetime]] = None,
    ) -> Any:
        """Apply func to current values (optionally limited to `keys`)."""
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        async with self._lock:
            target_keys = keys if keys is not None else list(self._store.keys())
            values = []
            for key in target_keys:
                found, value = self._lookup_unlocked(key, ts)
                if found:
                    values.append(value)
        if not values:
            return None
        result = func(values)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def count(
        self,
        predicate: Optional[Callable[[Any, Any], Any]] = None,
        timestamp: Optional[Union[float, str, datetime]] = None,
    ) -> int:
        """Count live keys, or those matching predicate(key, value)."""
        if predicate is not None:
            return len(await self.query(predicate, timestamp))
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        async with self._lock:
            return sum(1 for key in self._store if self._lookup_unlocked(key, ts)[0])

    # -- history ------------------------------------------------------------

    async def history(self, key: Any) -> List[Tuple[float, Any]]:
        """Full (timestamp, value) history for a key."""
        self._validate_key(key)
        async with self._lock:
            if self._is_expired(key):
                return []
            return list(self._store.get(key, []))

    async def iter_history(self, key: Any) -> "AsyncIterator[Tuple[float, Any]]":
        """Yield a key's versions without exposing the live list."""
        for version in await self.history(key):
            yield version

    async def get_range(
        self,
        key: Any,
        start_ts: Optional[Union[float, str, datetime]] = None,
        end_ts: Optional[Union[float, str, datetime]] = None,
    ) -> List[Tuple[float, Any]]:
        """All (timestamp, value) pairs for a key within [start_ts, end_ts]."""
        self._validate_key(key)
        async with self._lock:
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
            return range_versions(versions, start, end)

    async def get_latest_keys(self, n: int) -> List[Tuple[Any, float, Any]]:
        """The n most recently written (key, timestamp, value) triples."""
        if isinstance(n, bool) or not isinstance(n, int) or n < 0:
            raise ChronoMapValueError("n must be a non-negative integer")
        if n == 0:
            return []
        async with self._lock:
            latest_items = []
            for key, versions in self._store.items():
                if self._is_expired(key):
                    continue
                if versions:
                    ts, value = versions[-1]
                    latest_items.append((key, ts, value))
        return heapq.nlargest(n, latest_items, key=lambda item: item[1])

    async def get_keys_by_value(
        self, value: Any, timestamp: Optional[Union[float, str, datetime]] = None
    ) -> List[Any]:
        """All keys whose value at `timestamp` equals `value`."""
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        self._validate_timestamp(ts)
        async with self._lock:
            keys = []
            for key in self._store:
                found, current = self._lookup_unlocked(key, ts)
                if found and current == value:
                    keys.append(key)
            return keys

    async def keys(self) -> List[Any]:
        """Live keys, in insertion order."""
        async with self._lock:
            return [key for key in self._store if not self._is_expired(key)]

    async def values(self) -> List[Any]:
        """Latest values for every live key."""
        async with self._lock:
            return [
                versions[-1][1]
                for key, versions in self._store.items()
                if versions and not self._is_expired(key)
            ]

    async def items(self) -> List[Tuple[Any, Any]]:
        """(key, latest value) pairs for every live key."""
        async with self._lock:
            return [
                (key, versions[-1][1])
                for key, versions in self._store.items()
                if versions and not self._is_expired(key)
            ]

    async def latest(self) -> Dict[Any, Any]:
        """{key: latest_value} for every non-expired key."""
        async with self._lock:
            return {
                key: versions[-1][1]
                for key, versions in self._store.items()
                if versions and not self._is_expired(key)
            }

    async def keys_with_history_count(self) -> Dict[Any, int]:
        """{key: number_of_versions_stored} for every non-expired key."""
        async with self._lock:
            return {
                key: len(versions)
                for key, versions in self._store.items()
                if versions and not self._is_expired(key)
            }

    async def size(self) -> int:
        """Number of live keys."""
        async with self._lock:
            return sum(1 for key in self._store if not self._is_expired(key))

    async def contains(self, key: Any) -> bool:
        """Async spelling of `key in cm`."""
        self._validate_key(key)
        async with self._lock:
            return self._lookup_unlocked(key, self._current_time())[0]

    # -- pruning ------------------------------------------------------------

    async def prune_history(
        self,
        key: Any,
        keep_last: Optional[int] = None,
        older_than: Optional[Union[float, str, datetime]] = None,
    ) -> int:
        """Remove old versions for one key. Returns the number removed."""
        self._validate_key(key)
        self._validate_keep_last(keep_last)
        async with self._lock:
            if key not in self._store:
                return 0

            cutoff = self._parse_timestamp(older_than) if older_than is not None else None
            if cutoff is not None:
                self._validate_timestamp(cutoff)
            await self._journal_append(
                "prune",
                {"key": key, "keep_last": keep_last, "cutoff": cutoff},
            )

            versions = self._store[key]
            original_count = len(versions)
            if keep_last is not None:
                if keep_last == 0:
                    versions.clear()
                else:
                    versions[:] = versions[-keep_last:]

            if cutoff is not None:
                versions[:] = [(ts, value) for ts, value in versions if ts >= cutoff]

            if not versions:
                self._store.pop(key, None)
                self._ttl.pop(key, None)

            removed = original_count - len(versions)
            if self._cache and removed:
                self._cache.invalidate(key)
            if removed:
                await self._maybe_checkpoint()
            return removed

    async def prune_all_history(
        self,
        keep_last: Optional[int] = None,
        older_than: Optional[Union[float, str, datetime]] = None,
    ) -> int:
        """Prune every live key."""
        total_removed = 0
        for key in await self.keys():
            total_removed += await self.prune_history(key, keep_last, older_than)
        return total_removed

    async def clean_expired_keys(self) -> int:
        """Sweep expired keys. Returns the number removed."""
        async with self._lock:
            expired_keys = [key for key in self._ttl if self._is_expired(key)]
            if expired_keys:
                await self._journal_append("delete_many", {"keys": expired_keys})
            for key in expired_keys:
                self._store.pop(key, None)
                del self._ttl[key]
                if self._cache:
                    self._cache.invalidate(key)
            if expired_keys:
                await self._maybe_checkpoint()
        self.cleaned_count += len(expired_keys)
        return len(expired_keys)

    async def expire(self, key: Any, ttl: float) -> bool:
        """Set or replace a key's TTL. Returns False if the key is not live."""
        self._validate_key(key)
        self._validate_ttl(ttl)
        async with self._lock:
            await self._purge_if_expired(key)
            if key not in self._store or not self._store[key]:
                return False
            expires_at = self._current_time() + ttl
            await self._journal_append("expire", {"key": key, "expires_at": expires_at})
            self._ttl[key] = expires_at
            if self._cache:
                self._cache.invalidate(key)
            await self._maybe_checkpoint()
            return True

    async def expire_many(self, keys: Iterable[Any], ttl: float) -> int:
        """Set or replace the same TTL for several live keys. Returns the count."""
        self._validate_ttl(ttl)
        keys = list(keys)
        for key in keys:
            self._validate_key(key)
        async with self._lock:
            live_keys: List[Any] = []
            for key in keys:
                await self._purge_if_expired(key)
                if key in self._store and self._store[key]:
                    live_keys.append(key)
            live_keys = list(dict.fromkeys(live_keys))
            if not live_keys:
                return 0

            expires_at = self._current_time() + ttl
            await self._journal_append("expire_many", {"keys": live_keys, "expires_at": expires_at})
            for key in live_keys:
                self._ttl[key] = expires_at
                if self._cache:
                    self._cache.invalidate(key)
            await self._maybe_checkpoint()
            return len(live_keys)

    # -- snapshots and merging ---------------------------------------------

    async def snapshot(self) -> "AsyncChronoMap":
        """Deep-copy snapshot of the current state."""
        async with self._lock:
            snap = AsyncChronoMap(
                debug=self._debug,
                max_history=self._max_history,
                cache_size=0,
                enable_ttl_cleanup=False,
            )
            snap._store = deepcopy(self._store)
            snap._ttl = deepcopy(self._ttl)
            snap._snapshot_time = self._current_time()
            self._stats["snapshots"] += 1
            return snap

    def snapshot_context(self) -> AsyncSnapshotContext:
        """`async with cm.snapshot_context(): ...` rolls back on exception."""
        return AsyncSnapshotContext(self)

    async def rollback(self, snapshot: "AsyncChronoMap") -> None:
        """Restore state from a previous snapshot()."""
        if not isinstance(snapshot, AsyncChronoMap):
            raise ChronoMapTypeError("Expected AsyncChronoMap instance for rollback.")
        async with self._lock:
            replacement_store = deepcopy(snapshot._store)
            replacement_ttl = deepcopy(snapshot._ttl)
            await self._journal_append(
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
            await self._maybe_checkpoint()

    async def _visibility_snapshot(self) -> Tuple[Dict[Any, Any], Set[Any]]:
        live: Dict[Any, Any] = {}
        expired: Set[Any] = set()
        async with self._lock:
            for key, versions in self._store.items():
                if self._is_expired(key):
                    expired.add(key)
                elif versions:
                    live[key] = versions[-1][1]
        return live, expired

    async def diff(self, other: "AsyncChronoMap") -> Set[Any]:
        """Keys whose current value differs between self and other."""
        if not isinstance(other, AsyncChronoMap):
            raise ChronoMapTypeError("Expected AsyncChronoMap instance for diff.")
        self_state, _ = await self._visibility_snapshot()
        other_state, _ = await other._visibility_snapshot()
        return {
            key
            for key in set(self_state) | set(other_state)
            if self_state.get(key, _MISSING) != other_state.get(key, _MISSING)
        }

    async def diff_detailed(self, other: "AsyncChronoMap") -> List[Tuple[Any, Any, Any]]:
        """(key, other's value, self's value) for every key that differs."""
        if not isinstance(other, AsyncChronoMap):
            raise ChronoMapTypeError("Expected AsyncChronoMap instance for diff_detailed.")
        self_state, self_expired = await self._visibility_snapshot()
        other_state, other_expired = await other._visibility_snapshot()
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

    async def merge(self, other: "AsyncChronoMap", strategy: str = "timestamp") -> None:
        """Merge another map's history using either timestamp order or overwrite."""
        if not isinstance(other, AsyncChronoMap):
            raise ChronoMapTypeError("Expected AsyncChronoMap instance for merge.")
        if strategy not in ("timestamp", "overwrite"):
            raise ChronoMapValueError(f"Invalid merge strategy: {strategy}")

        async with other._lock:
            other_store = deepcopy(other._store)
            other_ttl = deepcopy(other._ttl)

        async with self._lock:
            await self._journal_append(
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
            else:
                for key, versions in other_store.items():
                    self._store[key] = versions
                    self._auto_prune(key)
                    self._stats["writes"] += len(versions)
                for key, expiry in other_ttl.items():
                    self._ttl[key] = expiry

            if self._cache:
                self._cache.clear()
            self._memory_monitor.check_memory(self._store, self._ttl)
            await self._maybe_checkpoint()

    async def clear(self) -> None:
        """Remove everything."""
        async with self._lock:
            await self._journal_append("clear", {})
            self._store.clear()
            self._ttl.clear()
            if self._cache:
                self._cache.clear()
            await self._maybe_checkpoint()

    # -- stats --------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Operation counters, cache stats, and size information."""
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
        stats["ttl_cleanup_count"] = self.cleaned_count
        if self._wal is not None:
            stats["journal_path"] = str(self._wal.path)
            stats["journal_bytes"] = self._wal.size
            stats["journal_sequence"] = self._wal.sequence
        stats["total_keys"] = sum(1 for key in self._store if not self._is_expired(key))
        stats["total_versions"] = sum(len(versions) for versions in self._store.values())
        stats["expired_keys"] = sum(1 for key in self._ttl if self._is_expired(key))
        return stats

    async def memory_usage(self) -> int:
        """Approximate bytes used by stored histories and TTL metadata."""
        async with self._lock:
            return MemoryMonitor.estimate_size(self._store) + MemoryMonitor.estimate_size(self._ttl)

    def reset_stats(self) -> None:
        """Zero operation counters and the read cache."""
        for key in self._stats:
            self._stats[key] = 0
        if self._cache:
            self._cache.clear()
        self.cleaned_count = 0

    def __repr__(self) -> str:
        non_expired = [key for key in self._store if not self._is_expired(key)]
        preview = non_expired[:10]
        suffix = "..." if len(non_expired) > 10 else ""
        return f"AsyncChronoMap(keys={preview}{suffix})"

    @property
    def snapshot_time(self) -> Optional[float]:
        """Timestamp recorded when this map was created by snapshot()."""
        return self._snapshot_time

    async def iter_rows(self) -> AsyncIterator[Dict[str, Any]]:
        """Stream one row per live version without holding the lock across yields."""
        async with self._lock:
            snapshot = [
                (key, list(versions)) for key, versions in self._store.items() if not self._is_expired(key)
            ]

        for key, versions in snapshot:
            for version_idx, (ts, value) in enumerate(versions):
                yield {
                    "key": key,
                    "value": value,
                    "timestamp": ts,
                    "datetime": datetime.fromtimestamp(ts),
                    "version": version_idx,
                }

    async def to_dataframe(self):
        """Export live history to a pandas DataFrame without blocking the loop."""
        rows = [row async for row in self.iter_rows()]

        def build():
            try:
                import pandas as pd
            except ImportError as exc:
                raise ImportError(
                    "pandas is required for to_dataframe(). " "Install with: pip install chronomap[pandas]"
                ) from exc
            return pd.DataFrame(rows)

        return await self._run_blocking(build)

    # -- persistence --------------------------------------------------------

    def to_dict(self, compress: Union[bool, str] = False) -> Union[Dict[str, Any], bytes]:
        """Serialize to a plain dict, or to compressed bytes."""
        data = {
            "store": deepcopy(self._store),
            "ttl": deepcopy(self._ttl),
            "snapshot_time": self._snapshot_time,
            "version": __version__,
            "max_history": self._max_history,
        }
        if not compress:
            return data

        pickled = pickle.dumps(data)
        method = "zlib" if compress is True else compress
        compressors: Dict[str, Callable[[bytes], bytes]] = {
            "zlib": lambda raw: zlib.compress(raw, level=6),
            "gzip": lambda raw: gzip.compress(raw, compresslevel=6),
            "bz2": lambda raw: bz2.compress(raw, compresslevel=6),
            "lzma": lambda raw: lzma.compress(raw, preset=6),
        }
        if method not in compressors:
            raise ChronoMapValueError(f"Unknown compression method: {compress}")
        return method.encode() + b"|" + compressors[method](pickled)

    @classmethod
    def from_dict(
        cls,
        data: Union[Dict[str, Any], bytes],
        debug: bool = False,
        cache_size: int = 1000,
        **kwargs,
    ) -> "AsyncChronoMap":
        """Reconstruct a map from to_dict() output (dict or compressed bytes)."""
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
        options.setdefault("enable_ttl_cleanup", False)
        instance = cls(
            debug=debug,
            cache_size=cache_size,
            **options,
        )
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
                instance._maybe_checkpoint_unlocked()
            except BaseException:
                instance._wal.close()
                raise
        return instance

    async def save_json(self, file_path: Union[str, Path]) -> None:
        """Write the current state as JSON without blocking the event loop."""
        path = Path(file_path)
        raw_data = self.to_dict()
        if not isinstance(raw_data, dict):
            raise ChronoMapTypeError("Uncompressed serialization must produce a mapping")
        json_data = {
            "store": {str(key): versions for key, versions in raw_data["store"].items()},
            "ttl": {str(key): expiry for key, expiry in raw_data["ttl"].items()},
            "snapshot_time": raw_data["snapshot_time"],
            "version": raw_data.get("version"),
            "max_history": raw_data.get("max_history"),
        }
        text = json.dumps(json_data, indent=2)
        await self._run_blocking(path.write_text, text, encoding="utf-8")

    @classmethod
    async def load_json(
        cls, file_path: Union[str, Path], debug: bool = False, cache_size: int = 1000, **kwargs
    ) -> "AsyncChronoMap":
        """Read a JSON save file in a worker thread."""
        path = Path(file_path)
        text = await cls._run_blocking(path.read_text, encoding="utf-8")
        json_data = json.loads(text)
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
        return cls.from_dict(data, debug=debug, cache_size=cache_size, **kwargs)

    async def save_pickle(self, file_path: Union[str, Path], compress: Union[bool, str] = False) -> None:
        """Write a pickle (optionally compressed) in a worker thread."""
        path = Path(file_path)
        data = self.to_dict(compress=compress)
        if isinstance(data, bytes):
            await self._run_blocking(path.write_bytes, data)
        else:
            await self._run_blocking(path.write_bytes, pickle.dumps(data))

    @classmethod
    async def load_pickle(
        cls, file_path: Union[str, Path], debug: bool = False, cache_size: int = 1000, **kwargs
    ) -> "AsyncChronoMap":
        """Read a pickle (or compressed pickle) in a worker thread."""
        path = Path(file_path)
        data_bytes = await cls._run_blocking(path.read_bytes)
        try:
            data: Any = pickle.loads(data_bytes)
        except Exception:
            data = data_bytes
        return cls.from_dict(data, debug=debug, cache_size=cache_size, **kwargs)

    @staticmethod
    async def _run_blocking(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run file I/O off the event loop, including on Python 3.8."""
        loop = asyncio.get_running_loop()
        if kwargs:
            return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
        return await loop.run_in_executor(None, func, *args)


__all__ = ["AsyncChronoMap", "AsyncSnapshotContext"]
