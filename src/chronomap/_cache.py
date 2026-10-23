# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Small thread-safe LRU cache used internally by ChronoMap for reads.

This isn't meant to be a general-purpose cache — it's shaped specifically
around ChronoMap's cache key of (store_key, timestamp_or_None).
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any, Dict, Optional, Set, Tuple


class LRUCache:
    """Thread-safe LRU cache for frequently accessed keys."""

    def __init__(self, capacity: int = 1000) -> None:
        self.capacity = capacity
        self.cache: "OrderedDict[Tuple[Any, Optional[float]], Any]" = OrderedDict()
        self._keys_by_store_key: Dict[Any, Set[Tuple[Any, Optional[float]]]] = {}
        self.lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: Tuple[Any, Optional[float]], default: Any = None) -> Any:
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                self.hits += 1
                return self.cache[key]
            self.misses += 1
            return default

    def put(self, key: Tuple[Any, Optional[float]], value: Any) -> None:
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            else:
                self._keys_by_store_key.setdefault(key[0], set()).add(key)
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                evicted_key, _ = self.cache.popitem(last=False)
                store_key = evicted_key[0]
                bucket = self._keys_by_store_key.get(store_key)
                if bucket is not None:
                    bucket.discard(evicted_key)
                    if not bucket:
                        del self._keys_by_store_key[store_key]

    def invalidate(self, store_key: Any) -> None:
        """Drop every cached entry belonging to a given store key."""
        with self.lock:
            for cache_key in self._keys_by_store_key.pop(store_key, set()):
                self.cache.pop(cache_key, None)

    def clear(self) -> None:
        with self.lock:
            self.cache.clear()
            self._keys_by_store_key.clear()
            self.hits = 0
            self.misses = 0

    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            total = self.hits + self.misses
            hit_rate = (self.hits / total * 100) if total else 0.0
            return {
                "hits": self.hits,
                "misses": self.misses,
                "size": len(self.cache),
                "capacity": self.capacity,
                "hit_rate": round(hit_rate, 2),
            }


__all__ = ["LRUCache"]
