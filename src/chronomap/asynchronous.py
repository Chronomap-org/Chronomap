# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Asyncio version of ChronoMap.

Deliberately a separate, simpler implementation rather than a wrapper
around ChronoMap — trying to share code between a threading.Lock-based
class and an asyncio.Lock-based class tends to produce something that's
harder to reason about than just writing it twice. If that tradeoff ever
stops being worth it, that's a good refactor to open an issue for.
"""

from __future__ import annotations

import asyncio
import bisect
import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .exceptions import ChronoMapKeyError, ChronoMapTypeError, ChronoMapValueError

logger = logging.getLogger(__name__)


class AsyncChronoMap:
    """Async counterpart to ChronoMap, for use inside asyncio applications."""

    def __init__(self, debug: bool = False, max_history: Optional[int] = None) -> None:
        self._store: Dict[Any, List[Tuple[float, Any]]] = {}
        self._ttl: Dict[Any, float] = {}
        self._lock = asyncio.Lock()
        self._snapshot_time: Optional[float] = None
        self._debug = debug
        self._max_history = max_history
        self._change_callbacks: List[Callable] = []
        self._key_subscribers: Dict[Any, List[Callable]] = {}
        self._stats = {"reads": 0, "writes": 0, "deletes": 0, "snapshots": 0, "auto_prunes": 0}

        if debug:
            logger.setLevel(logging.DEBUG)

    def _current_time(self) -> float:
        return datetime.now(timezone.utc).timestamp()

    def _parse_timestamp(self, timestamp: Union[float, str, datetime]) -> float:
        if isinstance(timestamp, str):
            try:
                return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
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

    def _is_expired(self, key: Any) -> bool:
        if key not in self._ttl:
            return False
        return self._current_time() >= self._ttl[key]

    def _auto_prune(self, key: Any) -> None:
        if self._max_history is None:
            return
        versions = self._store.get(key, [])
        if len(versions) > self._max_history:
            removed = len(versions) - self._max_history
            self._store[key] = versions[-self._max_history :]
            self._stats["auto_prunes"] += 1
            logger.debug("AUTO_PRUNE key=%r removed %d old versions", key, removed)

    async def _fire_callbacks(self, key: Any, old_value: Any, value: Any, ts: float) -> None:
        for callback in list(self._change_callbacks):
            if asyncio.iscoroutinefunction(callback):
                await callback(key, old_value, value, ts)
            else:
                callback(key, old_value, value, ts)
        for callback in list(self._key_subscribers.get(key, [])):
            if asyncio.iscoroutinefunction(callback):
                await callback(old_value, value, ts)
            else:
                callback(old_value, value, ts)

    async def put(
        self,
        key: Any,
        value: Any,
        timestamp: Optional[Union[float, str, datetime]] = None,
        ttl: Optional[float] = None,
    ) -> None:
        self._validate_key(key)
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()
        if ttl is not None and ttl <= 0:
            raise ChronoMapValueError(f"TTL must be positive, got {ttl}")

        async with self._lock:
            old_value = None
            if key in self._store and self._store[key]:
                old_value = self._store[key][-1][1]

            versions = self._store.setdefault(key, [])
            if not versions or ts >= versions[-1][0]:
                versions.append((ts, value))
            else:
                times = [v[0] for v in versions]
                idx = bisect.bisect_right(times, ts)
                versions.insert(idx, (ts, value))

            if ttl is not None:
                self._ttl[key] = self._current_time() + ttl

            self._auto_prune(key)
            self._stats["writes"] += 1

        await self._fire_callbacks(key, old_value, value, ts)

    async def put_many(
        self,
        items: Dict[Any, Any],
        timestamp: Optional[Union[float, str, datetime]] = None,
        ttl: Optional[float] = None,
    ) -> None:
        for key, value in items.items():
            await self.put(key, value, timestamp=timestamp, ttl=ttl)

    async def get(
        self,
        key: Any,
        timestamp: Optional[Union[float, str, datetime]] = None,
        default: Any = None,
        *,
        strict: bool = False,
    ) -> Any:
        ts = self._parse_timestamp(timestamp) if timestamp is not None else self._current_time()

        async with self._lock:
            if self._is_expired(key):
                self._store.pop(key, None)
                self._ttl.pop(key, None)
                if strict:
                    raise ChronoMapKeyError(key)
                return default

            versions = self._store.get(key, [])
            if not versions:
                if strict:
                    raise ChronoMapKeyError(key)
                return default

            times = [v[0] for v in versions]
            idx = bisect.bisect_right(times, ts) - 1
            if idx < 0:
                if strict:
                    raise ChronoMapKeyError(key)
                return default

            self._stats["reads"] += 1
            return versions[idx][1]

    async def get_or_set(
        self, key: Any, default_factory: Callable[[], Any], ttl: Optional[float] = None
    ) -> Any:
        self._validate_key(key)
        if not callable(default_factory):
            raise ChronoMapTypeError("default_factory must be callable")
        if ttl is not None and ttl <= 0:
            raise ChronoMapValueError(f"TTL must be positive, got {ttl}")

        ts = self._current_time()

        async with self._lock:
            if self._is_expired(key):
                self._store.pop(key, None)
                self._ttl.pop(key, None)

            versions = self._store.get(key, [])
            if versions:
                self._stats["reads"] += 1
                return versions[-1][1]

            value = default_factory()
            if asyncio.iscoroutine(value):
                value = await value

            self._store[key] = [(ts, value)]
            if ttl is not None:
                self._ttl[key] = self._current_time() + ttl

            self._auto_prune(key)
            self._stats["writes"] += 1

        await self._fire_callbacks(key, None, value, ts)
        return value

    async def get_or_default(self, key: Any, default: Any, ttl: Optional[float] = None) -> Any:
        factory = default if callable(default) else lambda: default
        return await self.get_or_set(key, factory, ttl=ttl)

    async def delete(self, key: Any) -> bool:
        async with self._lock:
            existed = key in self._store
            if existed:
                del self._store[key]
                self._ttl.pop(key, None)
                self._stats["deletes"] += 1
            return existed

    async def snapshot(self) -> "AsyncChronoMap":
        async with self._lock:
            snap = AsyncChronoMap(debug=self._debug, max_history=self._max_history)
            snap._store = deepcopy(self._store)
            snap._ttl = deepcopy(self._ttl)
            snap._snapshot_time = self._current_time()
            self._stats["snapshots"] += 1
            return snap

    def on_change(self, callback: Callable) -> None:
        self._change_callbacks.append(callback)

    def subscribe(self, key: Any, callback: Callable) -> None:
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

    async def keys(self) -> List[Any]:
        async with self._lock:
            return [k for k in self._store if not self._is_expired(k)]

    async def latest(self) -> Dict[Any, Any]:
        async with self._lock:
            return {k: v[-1][1] for k, v in self._store.items() if v and not self._is_expired(k)}

    async def keys_with_history_count(self) -> Dict[Any, int]:
        async with self._lock:
            return {k: len(v) for k, v in self._store.items() if v and not self._is_expired(k)}

    def get_stats(self) -> Dict[str, int]:
        return self._stats.copy()


__all__ = ["AsyncChronoMap"]
