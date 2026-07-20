# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""
Comprehensive unit tests for ChronoMap.

Run with: pytest tests/test_chronomap.py -v
Coverage: pytest tests/test_chronomap.py --cov=chronomap --cov-report=html
"""

import pytest
import asyncio
import time
import json
import pickle
import tempfile
import threading
import random
from pathlib import Path
from datetime import datetime, timedelta
from chronomap import (
    ChronoMap,
    AsyncChronoMap,
    ChronoMapError,
    ChronoMapKeyError,
    ChronoMapTypeError,
    ChronoMapValueError,
    ChronoMapMemoryError,
    LRUCache,
)

# ============================================================================
# Basic Operations Tests
# ============================================================================


@pytest.mark.core
class TestBasicOperations:
    """Test core put, get, delete operations."""

    def test_put_and_get(self):
        cm = ChronoMap()
        cm.put("key", "value")
        assert cm.get("key") == "value"

    def test_put_with_timestamp(self):
        cm = ChronoMap()
        cm.put("key", "value1", timestamp=100)
        cm.put("key", "value2", timestamp=200)
        assert cm.get("key", timestamp=150) == "value1"
        assert cm.get("key", timestamp=250) == "value2"

    def test_put_with_datetime_string(self):
        cm = ChronoMap()
        cm.put("key", "value1", timestamp="2026-01-01T00:00:00")
        cm.put("key", "value2", timestamp="2026-01-02T00:00:00")

        dt1 = datetime(2026, 1, 1, 12, 0, 0)
        assert cm.get("key", timestamp=dt1) == "value1"

    def test_put_with_datetime_object(self):
        cm = ChronoMap()
        dt1 = datetime(2026, 1, 1, 0, 0, 0)
        dt2 = datetime(2026, 1, 2, 0, 0, 0)

        cm.put("key", "value1", timestamp=dt1)
        cm.put("key", "value2", timestamp=dt2)

        assert cm.get("key", timestamp=dt1) == "value1"
        assert cm.get("key", timestamp=dt2) == "value2"

    def test_get_default(self):
        cm = ChronoMap()
        assert cm.get("nonexistent", default="default") == "default"

    def test_get_strict_raises(self):
        cm = ChronoMap()
        with pytest.raises(ChronoMapKeyError):
            cm.get("nonexistent", strict=True)

    def test_delete(self):
        cm = ChronoMap()
        cm.put("key", "value")
        assert cm.delete("key") is True
        assert cm.delete("key") is False
        assert "key" not in cm

    def test_empty_map(self):
        cm = ChronoMap()
        assert len(cm) == 0
        assert not cm
        assert list(cm.keys()) == []


# ============================================================================
# LRU Cache Tests
# ============================================================================


@pytest.mark.cache
class TestLRUCache:
    """Test LRU cache functionality."""

    def test_lru_cache_basic(self):
        cache = LRUCache(capacity=3)

        cache.put(("key1", 100), "value1")
        cache.put(("key2", 100), "value2")
        cache.put(("key3", 100), "value3")

        assert cache.get(("key1", 100)) == "value1"
        assert cache.get(("key2", 100)) == "value2"

    def test_lru_cache_eviction(self):
        cache = LRUCache(capacity=2)

        cache.put(("key1", 100), "value1")
        cache.put(("key2", 100), "value2")
        cache.put(("key3", 100), "value3")  # Evicts key1

        assert cache.get(("key1", 100)) is None
        assert cache.get(("key2", 100)) == "value2"
        assert cache.get(("key3", 100)) == "value3"

    def test_lru_cache_stats(self):
        cache = LRUCache(capacity=10)

        cache.put(("key1", 100), "value1")
        cache.get(("key1", 100))  # Hit
        cache.get(("key2", 100))  # Miss

        stats = cache.get_stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["size"] == 1

    def test_lru_cache_invalidate(self):
        cache = LRUCache(capacity=10)

        cache.put(("key1", 100), "value1")
        cache.put(("key1", 200), "value2")

        cache.invalidate("key1")

        assert cache.get(("key1", 100)) is None
        assert cache.get(("key1", 200)) is None

    def test_chronomap_with_cache(self):
        cm = ChronoMap(cache_size=100)

        cm["key"] = "value"

        # First read - cache miss
        val1 = cm["key"]

        # Second read - cache hit
        val2 = cm["key"]

        stats = cm.get_stats()
        assert stats["cache_hits"] >= 1
        assert val1 == val2 == "value"

    def test_cache_invalidation_on_write(self):
        cm = ChronoMap(cache_size=100)

        cm["key"] = "value1"
        _ = cm["key"]  # Cache it

        cm["key"] = "value2"  # Should invalidate cache

        assert cm["key"] == "value2"

    def test_cache_disabled(self):
        cm = ChronoMap(cache_size=0)

        cm["key"] = "value"
        _ = cm["key"]

        stats = cm.get_stats()
        assert "cache_hits" not in stats or stats["cache_hits"] == 0


# ============================================================================
# Auto-Pruning Tests
# ============================================================================


@pytest.mark.core
class TestAutoPruning:
    """Test auto-pruning with max_history."""

    def test_auto_prune_basic(self):
        cm = ChronoMap(max_history=10)

        # Write 100 versions
        for i in range(100):
            cm.put("key", i, timestamp=i)

        # Only last 10 should remain
        history = cm.history("key")
        assert len(history) == 10
        assert history[0][1] == 90  # Values 90-99
        assert history[-1][1] == 99

    def test_auto_prune_multiple_keys(self):
        cm = ChronoMap(max_history=5)

        for key in ["a", "b", "c"]:
            for i in range(20):
                cm.put(key, i, timestamp=i)

        for key in ["a", "b", "c"]:
            history = cm.history(key)
            assert len(history) == 5

    def test_auto_prune_stats(self):
        cm = ChronoMap(max_history=10)

        for i in range(50):
            cm.put("key", i, timestamp=i)

        stats = cm.get_stats()
        assert stats["auto_prunes"] > 0

    def test_no_auto_prune_when_disabled(self):
        cm = ChronoMap(max_history=None)

        for i in range(100):
            cm.put("key", i, timestamp=i)

        history = cm.history("key")
        assert len(history) == 100

    def test_auto_prune_with_put_many(self):
        cm = ChronoMap(max_history=5)

        items = {f"key{i}": i for i in range(20)}
        cm.put_many(items)

        # Each key should have only 1 version (all same timestamp)
        for key in items:
            assert len(cm.history(key)) == 1


# ============================================================================
# Background TTL Cleanup Tests
# ============================================================================


@pytest.mark.ttl
class TestBackgroundTTLCleanup:
    """Test background TTL cleanup thread."""

    def test_background_cleanup_enabled(self):
        cm = ChronoMap(enable_ttl_cleanup=True, ttl_cleanup_interval=0.1)

        cm.put("temp", "value", ttl=0.1)
        time.sleep(0.3)  # Wait for cleanup

        assert cm.get("temp") is None

    def test_background_cleanup_disabled(self):
        cm = ChronoMap(enable_ttl_cleanup=False)

        cm.put("temp", "value", ttl=0.1)
        time.sleep(0.2)

        # Key expired but not cleaned (manual cleanup needed)
        assert cm.get("temp") is None  # get() does clean on access

    def test_cleanup_thread_stops_on_gc(self):
        cm = ChronoMap(enable_ttl_cleanup=True)
        thread = cm._ttl_cleanup_thread.thread

        del cm
        time.sleep(0.1)

        # Thread should be daemon and not prevent exit


# ============================================================================
# Memory Monitoring Tests
# ============================================================================


@pytest.mark.memory
class TestMemoryMonitoring:
    """Test memory limits and monitoring."""

    def test_memory_limit_enforcement(self):
        # Set very low limit to trigger easily
        cm = ChronoMap(max_memory_mb=0.001)  # 1KB limit

        with pytest.raises(ChronoMapMemoryError):
            # This should exceed 1KB
            cm.put_many({f"key{i}": "x" * 1000 for i in range(100)})

    def test_no_memory_limit(self):
        cm = ChronoMap(max_memory_mb=None)

        # Should not raise
        cm.put_many({f"key{i}": i for i in range(1000)})

    def test_memory_warning(self, caplog):
        # This is hard to test deterministically, but we can verify the structure
        cm = ChronoMap(max_memory_mb=100)
        assert cm._memory_monitor.max_memory_bytes is not None


# ============================================================================
# Enhanced Compression Tests
# ============================================================================


@pytest.mark.core
class TestEnhancedCompression:
    """Test multiple compression algorithms."""

    def test_compression_zlib(self):
        cm = ChronoMap()
        cm.put_many({f"key{i}": f"value{i}" for i in range(100)})

        compressed = cm.to_dict(compress="zlib")
        assert isinstance(compressed, bytes)
        assert b"zlib|" in compressed

        cm2 = ChronoMap.from_dict(compressed)
        assert cm2["key0"] == "value0"

    def test_compression_gzip(self):
        cm = ChronoMap()
        cm.put_many({f"key{i}": i for i in range(50)})

        compressed = cm.to_dict(compress="gzip")
        assert isinstance(compressed, bytes)
        assert b"gzip|" in compressed

        cm2 = ChronoMap.from_dict(compressed)
        assert cm2["key10"] == 10

    def test_compression_bz2(self):
        cm = ChronoMap()
        cm["test"] = "data"

        compressed = cm.to_dict(compress="bz2")
        assert isinstance(compressed, bytes)

        cm2 = ChronoMap.from_dict(compressed)
        assert cm2["test"] == "data"

    def test_compression_lzma(self):
        cm = ChronoMap()
        cm["test"] = "data"

        compressed = cm.to_dict(compress="lzma")
        assert isinstance(compressed, bytes)

        cm2 = ChronoMap.from_dict(compressed)
        assert cm2["test"] == "data"

    def test_invalid_compression_method(self):
        cm = ChronoMap()
        cm["test"] = "data"

        with pytest.raises(ChronoMapValueError):
            cm.to_dict(compress="invalid")

    def test_save_load_compressed_pickle_zlib(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2, "c": 3})

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.pkl"
            cm.save_pickle(path, compress="zlib")
            cm2 = ChronoMap.load_pickle(path)

            assert cm2["a"] == 1
            assert cm2["b"] == 2

    def test_save_load_compressed_pickle_lzma(self):
        cm = ChronoMap()
        cm["key"] = "value" * 100

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.pkl"
            cm.save_pickle(path, compress="lzma")
            cm2 = ChronoMap.load_pickle(path)

            assert cm2["key"] == "value" * 100

    def test_compression_backward_compatible(self):
        # Old format without method marker
        cm = ChronoMap()
        cm["test"] = "data"

        import zlib, pickle

        data = cm.to_dict(compress=False)
        old_format = zlib.compress(pickle.dumps(data))

        cm2 = ChronoMap.from_dict(old_format)
        assert cm2["test"] == "data"


# ============================================================================
# TTL / Expiry Tests
# ============================================================================


@pytest.mark.ttl
class TestTTL:
    """Test TTL and key expiration."""

    def test_ttl_expiry(self):
        cm = ChronoMap(enable_ttl_cleanup=False)  # Manual cleanup for test
        cm.put("temp", "value", ttl=0.1)
        assert cm.get("temp") == "value"
        time.sleep(0.15)
        assert cm.get("temp") is None

    def test_ttl_strict_raises(self):
        cm = ChronoMap()
        cm.put("temp", "value", ttl=0.1)
        time.sleep(0.15)
        with pytest.raises(ChronoMapKeyError):
            cm.get("temp", strict=True)

    def test_ttl_contains(self):
        cm = ChronoMap()
        cm.put("temp", "value", ttl=0.1)
        assert "temp" in cm
        time.sleep(0.15)
        assert "temp" not in cm

    def test_ttl_negative_raises(self):
        cm = ChronoMap()
        with pytest.raises(ChronoMapValueError):
            cm.put("key", "value", ttl=-1)

    def test_clean_expired_keys(self):
        cm = ChronoMap(enable_ttl_cleanup=False)
        cm.put("temp1", "v1", ttl=0.1)
        cm.put("temp2", "v2", ttl=0.1)
        cm.put("perm", "v3")
        time.sleep(0.15)
        removed = cm.clean_expired_keys()
        assert removed == 2
        assert "perm" in cm


# ============================================================================
# Batch Operations Tests
# ============================================================================


@pytest.mark.core
class TestBatchOperations:
    """Test batch put and delete operations."""

    def test_put_many(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2, "c": 3})
        assert cm["a"] == 1
        assert cm["b"] == 2
        assert cm["c"] == 3

    def test_put_many_with_ttl(self):
        cm = ChronoMap(enable_ttl_cleanup=False)
        cm.put_many({"a": 1, "b": 2}, ttl=0.1)
        assert cm["a"] == 1
        time.sleep(0.15)
        assert cm.get("a") is None

    def test_delete_many(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2, "c": 3})
        deleted = cm.delete_many(["a", "b", "nonexistent"])
        assert deleted == 2
        assert "a" not in cm
        assert "b" not in cm
        assert "c" in cm

    def test_put_many_optimized(self):
        # Test that put_many is optimized (single lock)
        cm = ChronoMap()

        start = time.time()
        cm.put_many({f"key{i}": i for i in range(1000)})
        batch_time = time.time() - start

        # Should be faster than individual puts
        # (We're not testing timing strictly, just that it works)
        assert len(cm) == 1000


# ============================================================================
# Advanced Query Tests
# ============================================================================


@pytest.mark.core
class TestAdvancedQueries:
    """Test range queries and latest keys."""

    def test_get_range(self):
        cm = ChronoMap()
        cm.put("temp", 20, timestamp=100)
        cm.put("temp", 22, timestamp=200)
        cm.put("temp", 24, timestamp=300)

        result = cm.get_range("temp", start_ts=150, end_ts=250)
        assert len(result) == 1
        assert result[0] == (200, 22)

    def test_get_range_all(self):
        cm = ChronoMap()
        cm.put("temp", 20, timestamp=100)
        cm.put("temp", 22, timestamp=200)

        result = cm.get_range("temp")
        assert len(result) == 2

    def test_get_latest_keys(self):
        cm = ChronoMap()
        cm.put("a", 1, timestamp=100)
        cm.put("b", 2, timestamp=200)
        cm.put("c", 3, timestamp=300)

        latest = cm.get_latest_keys(2)
        assert len(latest) == 2
        assert latest[0][0] == "c"  # Most recent
        assert latest[1][0] == "b"

    def test_get_keys_by_value(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 1, "c": 2})
        keys = cm.get_keys_by_value(1)
        assert set(keys) == {"a", "b"}


# ============================================================================
# Snapshot, Diff, Rollback Tests
# ============================================================================


@pytest.mark.snapshot
class TestSnapshotDiffRollback:
    """Test snapshot, diff, and rollback functionality."""

    def test_snapshot(self):
        cm = ChronoMap()
        cm["key"] = "value1"
        snap = cm.snapshot()
        cm["key"] = "value2"
        assert cm["key"] == "value2"
        assert snap["key"] == "value1"

    def test_rollback(self):
        cm = ChronoMap()
        cm["key"] = "value1"
        snap = cm.snapshot()
        cm["key"] = "value2"
        cm["new_key"] = "new_value"
        cm.rollback(snap)
        assert cm["key"] == "value1"
        assert "new_key" not in cm

    def test_rollback_invalid_type(self):
        cm = ChronoMap()
        with pytest.raises(ChronoMapTypeError):
            cm.rollback({"not": "a chronomap"})

    def test_diff(self):
        cm1 = ChronoMap()
        cm2 = ChronoMap()
        cm1.put_many({"a": 1, "b": 2})
        cm2.put_many({"a": 1, "b": 3, "c": 4})

        diff = cm1.diff(cm2)
        assert "b" in diff  # Different value
        assert "c" in diff  # Only in cm2

    def test_diff_detailed(self):
        cm1 = ChronoMap()
        cm2 = ChronoMap()
        cm1["a"] = 1
        cm2["a"] = 2

        changes = cm1.diff_detailed(cm2)
        assert len(changes) == 1
        assert changes[0] == ("a", 2, 1)

    def test_rollback_clears_cache(self):
        cm = ChronoMap(cache_size=100)

        cm["key"] = "value1"
        _ = cm["key"]  # Cache it

        snap = cm.snapshot()
        cm["key"] = "value2"
        cm.rollback(snap)

        # Cache should be cleared
        assert cm["key"] == "value1"


# ============================================================================
# Merge Tests
# ============================================================================


@pytest.mark.core
class TestMerge:
    """Test merge functionality."""

    def test_merge_timestamp_strategy(self):
        cm1 = ChronoMap()
        cm2 = ChronoMap()
        cm1.put("a", 1, timestamp=100)
        cm2.put("a", 2, timestamp=200)

        cm1.merge(cm2, strategy="timestamp")
        history = cm1.history("a")
        assert len(history) == 2
        assert history[0] == (100, 1)
        assert history[1] == (200, 2)

    def test_merge_overwrite_strategy(self):
        cm1 = ChronoMap()
        cm2 = ChronoMap()
        cm1.put("a", 1, timestamp=100)
        cm2.put("a", 2, timestamp=200)

        cm1.merge(cm2, strategy="overwrite")
        assert cm1["a"] == 2

    def test_merge_invalid_strategy(self):
        cm1 = ChronoMap()
        cm2 = ChronoMap()
        with pytest.raises(ChronoMapValueError):
            cm1.merge(cm2, strategy="invalid")

    def test_merge_invalid_type(self):
        cm = ChronoMap()
        with pytest.raises(ChronoMapTypeError):
            cm.merge({"not": "a chronomap"})

    def test_merge_with_auto_prune(self):
        cm1 = ChronoMap(max_history=5)
        cm2 = ChronoMap()

        for i in range(10):
            cm2.put("key", i, timestamp=i)

        cm1.merge(cm2, strategy="timestamp")

        # Should auto-prune to 5
        history = cm1.history("key")
        assert len(history) == 5


# ============================================================================
# Magic Methods Tests
# ============================================================================


@pytest.mark.core
class TestMagicMethods:
    """Test Pythonic magic methods."""

    def test_getitem(self):
        cm = ChronoMap()
        cm.put("key", "value")
        assert cm["key"] == "value"

    def test_getitem_raises(self):
        cm = ChronoMap()
        with pytest.raises(ChronoMapKeyError):
            _ = cm["nonexistent"]

    def test_setitem(self):
        cm = ChronoMap()
        cm["key"] = "value"
        assert cm.get("key") == "value"

    def test_delitem(self):
        cm = ChronoMap()
        cm["key"] = "value"
        del cm["key"]
        assert "key" not in cm

    def test_delitem_raises(self):
        cm = ChronoMap()
        with pytest.raises(ChronoMapKeyError):
            del cm["nonexistent"]

    def test_len(self):
        cm = ChronoMap()
        assert len(cm) == 0
        cm.put_many({"a": 1, "b": 2, "c": 3})
        assert len(cm) == 3

    def test_contains(self):
        cm = ChronoMap()
        cm["key"] = "value"
        assert "key" in cm
        assert "nonexistent" not in cm

    def test_bool(self):
        cm = ChronoMap()
        assert not cm
        cm["key"] = "value"
        assert cm

    def test_eq(self):
        cm1 = ChronoMap()
        cm2 = ChronoMap()
        cm1.put_many({"a": 1, "b": 2})
        cm2.put_many({"a": 1, "b": 2})
        assert cm1 == cm2

        cm2["b"] = 3
        assert cm1 != cm2

    def test_iter(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2, "c": 3})
        keys = list(cm)
        assert set(keys) == {"a", "b", "c"}


# ============================================================================
# Iteration Tests
# ============================================================================


@pytest.mark.core
class TestIteration:
    """Test iteration methods."""

    def test_keys(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2})
        assert set(cm.keys()) == {"a", "b"}

    def test_values(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2})
        assert set(cm.values()) == {1, 2}

    def test_items(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2})
        assert set(cm.items()) == {("a", 1), ("b", 2)}

    def test_iter_history(self):
        cm = ChronoMap()
        cm.put("key", "v1", timestamp=100)
        cm.put("key", "v2", timestamp=200)

        history = list(cm.iter_history("key"))
        assert len(history) == 2
        assert history[0] == (100, "v1")
        assert history[1] == (200, "v2")


# ============================================================================
# Utility Methods Tests
# ============================================================================


@pytest.mark.core
class TestUtilityMethods:
    """Test utility methods."""

    def test_latest(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2})
        assert cm.latest() == {"a": 1, "b": 2}

    def test_history(self):
        cm = ChronoMap()
        cm.put("key", "v1", timestamp=100)
        cm.put("key", "v2", timestamp=200)

        history = cm.history("key")
        assert len(history) == 2
        assert history[0] == (100, "v1")

    def test_get_or_set_returns_existing_without_calling_factory(self):
        cm = ChronoMap()
        cm.put("config", {"source": "stored"})
        calls = []

        value = cm.get_or_set("config", lambda: calls.append("called") or {"source": "factory"})

        assert value == {"source": "stored"}
        assert calls == []
        assert len(cm.history("config")) == 1

    def test_get_or_set_stores_missing_factory_value(self):
        cm = ChronoMap()

        value = cm.get_or_set("config", lambda: {"source": "factory"})

        assert value == {"source": "factory"}
        assert cm.get("config") == {"source": "factory"}
        assert len(cm.history("config")) == 1

    def test_get_or_set_passes_ttl_for_new_value(self):
        cm = ChronoMap(enable_ttl_cleanup=False)
        assert cm.get_or_set("session", lambda: "token", ttl=0.1) == "token"
        time.sleep(0.15)

        assert cm.get_or_set("session", lambda: "new-token") == "new-token"

    def test_get_or_default_accepts_plain_default_value(self):
        cm = ChronoMap()

        assert cm.get_or_default("feature", False) is False
        assert cm.get("feature") is False

    def test_keys_with_history_count_empty(self):
        cm = ChronoMap()
        assert cm.keys_with_history_count() == {}

    def test_keys_with_history_count(self):
        cm = ChronoMap()
        cm.put("a", 1, timestamp=1)
        cm.put("a", 2, timestamp=2)
        cm.put("b", 99, timestamp=1)

        assert cm.keys_with_history_count() == {"a": 2, "b": 1}

    def test_keys_with_history_count_respects_max_history(self):
        cm = ChronoMap(max_history=2)
        for i in range(5):
            cm.put("a", i, timestamp=i)
        cm.put("b", 1, timestamp=1)

        assert cm.keys_with_history_count() == {"a": 2, "b": 1}

    def test_keys_with_history_count_skips_expired_keys(self):
        cm = ChronoMap(enable_ttl_cleanup=False)
        cm.put("temp", "value", ttl=0.1)
        cm.put("perm", "value")
        time.sleep(0.15)

        assert cm.keys_with_history_count() == {"perm": 1}

    def test_clear(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2})
        cm.clear()
        assert len(cm) == 0
        assert cm.latest() == {}

    def test_repr(self):
        cm = ChronoMap()
        cm["key"] = "value"
        repr_str = repr(cm)
        assert "ChronoMap" in repr_str
        assert "key" in repr_str

    def test_clear_also_clears_cache(self):
        cm = ChronoMap(cache_size=100)
        cm["key"] = "value"
        _ = cm["key"]  # Cache it

        cm.clear()

        # Cache should be cleared
        stats = cm.get_stats()
        assert stats.get("cache_size", 0) == 0


# ============================================================================
# Validation Tests
# ============================================================================


@pytest.mark.core
class TestValidation:
    """Test input validation."""

    def test_unhashable_key_raises(self):
        cm = ChronoMap()
        with pytest.raises(ChronoMapTypeError):
            cm.put(["unhashable", "list"], "value")

    def test_invalid_timestamp_type_raises(self):
        cm = ChronoMap()
        with pytest.raises(ChronoMapTypeError):
            cm.put("key", "value", timestamp=1 + 2j)  # complex number

    def test_invalid_timestamp_value_raises(self):
        cm = ChronoMap()
        with pytest.raises(ChronoMapValueError):
            cm.put("key", "value", timestamp=float("inf"))

    def test_invalid_datetime_string_raises(self):
        cm = ChronoMap()
        with pytest.raises(ChronoMapValueError, match="Invalid datetime string"):
            cm.put("key", "value", timestamp="not a datetime")


# ============================================================================
# Persistence Tests
# ============================================================================


@pytest.mark.core
class TestPersistence:
    """Test serialization and persistence."""

    def test_to_dict_from_dict(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2})

        data = cm.to_dict()
        cm2 = ChronoMap.from_dict(data)

        assert cm2["a"] == 1
        assert cm2["b"] == 2

    def test_save_load_json(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2})

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test.json"
            cm.save_json(filepath)
            cm2 = ChronoMap.load_json(filepath)

            assert cm2["a"] == 1
            assert cm2["b"] == 2

    def test_history_entries_stay_tuples_after_json_round_trip(self):
        cm = ChronoMap()
        cm.put("a", 1, timestamp=100)
        cm.put("a", 2, timestamp=200)

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test.json"
            cm.save_json(filepath)
            cm2 = ChronoMap.load_json(filepath)

            loaded_history = cm2.history("a")
            assert loaded_history == [(100.0, 1), (200.0, 2)]
            assert all(isinstance(entry, tuple) for entry in loaded_history)
            # Tuples are hashable; lists aren't - this would raise TypeError
            # before the fix.
            assert len(set(loaded_history)) == 2

    def test_save_load_pickle(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2, "c": [1, 2, 3]})

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test.pkl"
            cm.save_pickle(filepath)
            cm2 = ChronoMap.load_pickle(filepath)

            assert cm2["a"] == 1
            assert cm2["c"] == [1, 2, 3]

    def test_save_preserves_max_history(self):
        cm = ChronoMap(max_history=100)
        cm["key"] = "value"

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.json"
            cm.save_json(path)
            cm2 = ChronoMap.load_json(path)

            assert cm2._max_history == 100


# ============================================================================
# Thread Safety Tests
# ============================================================================


@pytest.mark.core
class TestThreadSafety:
    """Test thread safety of operations."""

    def test_concurrent_puts(self):
        cm = ChronoMap()

        def put_values(start, count):
            for i in range(start, start + count):
                cm.put(f"key{i}", i)

        threads = []
        for i in range(5):
            t = threading.Thread(target=put_values, args=(i * 100, 100))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        assert len(cm) == 500

    def test_concurrent_reads_writes(self):
        cm = ChronoMap()
        cm.put_many({f"key{i}": i for i in range(100)})

        results = []

        def reader():
            for _ in range(100):
                results.append(cm.get("key50"))

        def writer():
            for i in range(100):
                cm.put("key50", i)

        threads = []
        for _ in range(5):
            threads.append(threading.Thread(target=reader))
            threads.append(threading.Thread(target=writer))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should complete without errors
        assert len(results) > 0

    def test_concurrent_snapshot_modify(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2})

        snapshots = []

        def take_snapshot():
            for _ in range(10):
                snapshots.append(cm.snapshot())
                time.sleep(0.001)

        def modify():
            for i in range(10):
                cm.put("a", i)
                time.sleep(0.001)

        t1 = threading.Thread(target=take_snapshot)
        t2 = threading.Thread(target=modify)

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(snapshots) == 10


# ============================================================================
# Edge Cases Tests
# ============================================================================


@pytest.mark.core
class TestEdgeCases:
    """Test edge cases and corner scenarios."""

    def test_put_same_timestamp_multiple_values(self):
        cm = ChronoMap()
        cm.put("key", "v1", timestamp=100)
        cm.put("key", "v2", timestamp=100)

        # Later put should be retrievable
        assert cm.get("key", timestamp=100) == "v2"

    def test_get_before_first_timestamp(self):
        cm = ChronoMap()
        cm.put("key", "value", timestamp=100)
        assert cm.get("key", timestamp=50) is None

    def test_empty_history(self):
        cm = ChronoMap()
        assert cm.history("nonexistent") == []

    def test_delete_nonexistent(self):
        cm = ChronoMap()
        assert cm.delete("nonexistent") is False

    def test_large_history(self):
        cm = ChronoMap()
        for i in range(1000):
            cm.put("key", i, timestamp=i)

        history = cm.history("key")
        assert len(history) == 1000
        assert cm.get("key", timestamp=500) == 500

    def test_none_as_value(self):
        cm = ChronoMap()
        cm["key"] = None
        assert cm["key"] is None
        assert "key" in cm

    def test_complex_objects_as_values(self):
        cm = ChronoMap()
        cm["dict"] = {"nested": {"value": 123}}
        cm["list"] = [1, 2, [3, 4]]

        assert cm["dict"]["nested"]["value"] == 123
        assert cm["list"][2][1] == 4


# ============================================================================
# Debug Mode Tests
# ============================================================================


@pytest.mark.core
class TestDebugMode:
    """Test debug logging."""

    def test_debug_mode_enabled(self):
        cm = ChronoMap(debug=True)
        cm.put("key", "value")
        # Should not raise, just enable logging
        assert cm.get("key") == "value"


# ============================================================================
# Event Hooks Tests
# ============================================================================


@pytest.mark.core
class TestEventHooks:
    """Test event callback functionality."""

    def test_on_change_callback(self):
        cm = ChronoMap()
        changes = []

        def track_change(key, old, new, ts):
            changes.append((key, old, new))

        cm.on_change(track_change)
        cm["key1"] = "value1"
        cm["key1"] = "value2"
        cm["key2"] = "value3"
        assert len(changes) == 3
        assert changes[0] == ("key1", None, "value1")
        assert changes[1] == ("key1", "value1", "value2")
        assert changes[2] == ("key2", None, "value3")

    def test_multiple_callbacks(self):
        cm = ChronoMap()
        results1 = []
        results2 = []
        cm.on_change(lambda k, o, n, t: results1.append(k))
        cm.on_change(lambda k, o, n, t: results2.append(n))
        cm["key"] = "value"
        assert "key" in results1
        assert "value" in results2

    def test_remove_callback(self):
        cm = ChronoMap()
        changes = []

        def callback(k, o, n, t):
            changes.append(k)

        cm.on_change(callback)
        cm["key1"] = "val1"
        assert len(changes) == 1
        cm.remove_change_callback(callback)
        cm["key2"] = "val2"
        assert len(changes) == 1

    def test_callback_exception_handling(self, caplog):
        cm = ChronoMap()

        def bad_callback(k, o, n, t):
            raise RuntimeError("Callback failed")

        cm.on_change(bad_callback)
        with caplog.at_level("ERROR"):
            cm["key"] = "value"
        assert "Error in change callback" in caplog.text

    def test_subscribe_only_fires_for_matching_key(self):
        cm = ChronoMap()
        changes = []

        cm.subscribe("app.config", lambda old, new, ts: changes.append((old, new)))

        cm["app.config"] = "v1"
        cm["other.key"] = "ignored"
        cm["app.config"] = "v2"

        assert changes == [(None, "v1"), ("v1", "v2")]

    def test_subscribe_supports_multiple_callbacks_on_same_key(self):
        cm = ChronoMap()
        first = []
        second = []

        cm.subscribe("key", lambda old, new, ts: first.append(new))
        cm.subscribe("key", lambda old, new, ts: second.append((old, new)))

        cm.put("key", "value")

        assert first == ["value"]
        assert second == [(None, "value")]

    def test_unsubscribe_removes_only_requested_callback(self):
        cm = ChronoMap()
        first = []
        second = []

        def callback_one(old, new, ts):
            first.append(new)

        def callback_two(old, new, ts):
            second.append(new)

        cm.subscribe("key", callback_one)
        cm.subscribe("key", callback_two)

        assert cm.unsubscribe("key", callback_one) is True
        cm.put("key", "value")

        assert first == []
        assert second == ["value"]
        assert cm.unsubscribe("key", callback_one) is False

    def test_subscribe_works_with_put_many(self):
        cm = ChronoMap()
        watched = []

        cm.subscribe("a", lambda old, new, ts: watched.append(("a", old, new)))
        cm.subscribe("b", lambda old, new, ts: watched.append(("b", old, new)))

        cm.put_many({"a": 1, "b": 2, "c": 3})

        assert watched == [("a", None, 1), ("b", None, 2)]

    def test_subscriber_exception_handling(self, caplog):
        cm = ChronoMap()

        def bad_callback(old, new, ts):
            raise RuntimeError("Subscriber failed")

        cm.subscribe("key", bad_callback)

        with caplog.at_level("ERROR"):
            cm.put("key", "value")

        assert "Error in key subscriber callback" in caplog.text


# ============================================================================
# Query & Analytics Tests
# ============================================================================


@pytest.mark.core
class TestQueryAnalytics:
    """Test query and analytics features."""

    def test_query_filter(self):
        cm = ChronoMap()
        cm.put_many({"a": 10, "b": 20, "c": 30, "d": 5})
        result = cm.query(lambda k, v: v > 15)
        assert result == {"b": 20, "c": 30}

    def test_query_with_key_filter(self):
        cm = ChronoMap()
        cm.put_many({"user:1": "active", "user:2": "inactive", "admin:1": "active"})
        result = cm.query(lambda k, v: k.startswith("user") and v == "active")
        assert result == {"user:1": "active"}

    def test_aggregate_sum(self):
        cm = ChronoMap()
        cm.put_many({"score1": 10, "score2": 20, "score3": 30})
        total = cm.aggregate(sum)
        assert total == 60

    def test_aggregate_average(self):
        cm = ChronoMap()
        cm.put_many({"val1": 10, "val2": 20, "val3": 30})
        avg = cm.aggregate(lambda vals: sum(vals) / len(vals))
        assert avg == 20

    def test_aggregate_specific_keys(self):
        cm = ChronoMap()
        cm.put_many({"a": 1, "b": 2, "c": 3, "d": 100})
        total = cm.aggregate(sum, keys=["a", "b", "c"])
        assert total == 6

    def test_count(self):
        cm = ChronoMap()
        cm.put_many({"a": 10, "b": 20, "c": 30})
        assert cm.count() == 3
        assert cm.count(lambda k, v: v > 15) == 2


# ============================================================================
# History Management Tests
# ============================================================================


@pytest.mark.core
class TestHistoryManagement:
    """Test history pruning and management."""

    def test_prune_history_keep_last(self):
        cm = ChronoMap()
        for i in range(100):
            cm.put("key", i, timestamp=i)
        removed = cm.prune_history("key", keep_last=10)
        assert removed == 90
        history = cm.history("key")
        assert len(history) == 10
        assert history[0][1] == 90

    def test_prune_history_older_than(self):
        cm = ChronoMap()
        cm.put("key", "old", timestamp=100)
        cm.put("key", "recent", timestamp=500)
        removed = cm.prune_history("key", older_than=300)
        assert removed == 1
        history = cm.history("key")
        assert len(history) == 1
        assert history[0][1] == "recent"

    def test_prune_history_datetime_string(self):
        cm = ChronoMap()
        dt_old = datetime(2024, 1, 1)
        dt_new = datetime(2026, 1, 1)
        cm.put("key", "old", timestamp=dt_old)
        cm.put("key", "new", timestamp=dt_new)
        removed = cm.prune_history("key", older_than="2024-06-01T00:00:00")
        assert removed == 1

    def test_prune_all_history(self):
        cm = ChronoMap()
        for key in ["a", "b", "c"]:
            for i in range(50):
                cm.put(key, i, timestamp=i)
        total_removed = cm.prune_all_history(keep_last=10)
        assert total_removed == 120


# ============================================================================
# Context Manager Tests
# ============================================================================


@pytest.mark.core
class TestContextManager:
    """Test snapshot context manager."""

    def test_snapshot_context_success(self):
        cm = ChronoMap()
        cm["key"] = "original"
        with cm.snapshot_context():
            cm["key"] = "modified"
        assert cm["key"] == "modified"

    def test_snapshot_context_rollback_on_exception(self):
        cm = ChronoMap()
        cm["key"] = "original"
        try:
            with cm.snapshot_context():
                cm["key"] = "modified"
                cm["new_key"] = "new_value"
                raise ValueError("Test exception")
        except ValueError:
            pass
        assert cm["key"] == "original"
        assert "new_key" not in cm


# ============================================================================
# Statistics Tests
# ============================================================================


@pytest.mark.core
class TestStatistics:
    """Test operation statistics tracking."""

    def test_stats_tracking(self):
        cm = ChronoMap()
        cm["a"] = 1
        cm["b"] = 2
        stats = cm.get_stats()
        assert stats["writes"] == 2
        _ = cm["a"]
        _ = cm.get("b")
        stats = cm.get_stats()
        assert stats["reads"] == 2
        del cm["a"]
        stats = cm.get_stats()
        assert stats["deletes"] == 1
        snap = cm.snapshot()
        stats = cm.get_stats()
        assert stats["snapshots"] == 1

    def test_reset_stats(self):
        cm = ChronoMap()
        cm["key"] = "value"
        _ = cm["key"]
        stats = cm.get_stats()
        assert stats["writes"] > 0
        assert stats["reads"] > 0
        cm.reset_stats()
        stats = cm.get_stats()
        assert stats["writes"] == 0
        assert stats["reads"] == 0

    def test_enhanced_stats(self):
        cm = ChronoMap(max_history=10, cache_size=100)

        for i in range(50):
            cm.put("key", i, timestamp=i)

        _ = cm["key"]  # Cache hit

        stats = cm.get_stats()

        assert "auto_prunes" in stats
        assert "total_keys" in stats
        assert "total_versions" in stats
        assert stats["total_keys"] >= 1
        assert stats["total_versions"] == 10  # Auto-pruned


# ============================================================================
# Pandas Export Tests
# ============================================================================


@pytest.mark.core
class TestPandasExport:
    """Test Pandas DataFrame export."""

    def test_to_dataframe(self):
        pytest.importorskip("pandas")
        cm = ChronoMap()
        cm.put("temp", 20, timestamp=100)
        cm.put("temp", 22, timestamp=200)
        cm.put("temp", 24, timestamp=300)
        df = cm.to_dataframe()
        assert len(df) == 3
        assert set(df.columns) >= {"key", "value", "timestamp", "datetime", "version"}
        assert df["value"].tolist() == [20, 22, 24]

    def test_to_dataframe_without_pandas(self):
        cm = ChronoMap()
        cm.put("key", "value")
        import sys

        original_pandas = sys.modules.get("pandas", None)
        sys.modules["pandas"] = None
        try:
            with pytest.raises(ImportError, match="pandas is required"):
                cm.to_dataframe()
        finally:
            if original_pandas is not None:
                sys.modules["pandas"] = original_pandas
            else:
                sys.modules.pop("pandas", None)


# ============================================================================
# Concurrency Tests (RWLock)
# ============================================================================


@pytest.mark.lock
class TestConcurrency:
    """Test concurrency with read-write locks."""

    def test_rwlock_concurrent_reads(self):
        cm = ChronoMap(use_rwlock=True)
        cm.put_many({f"key{i}": i for i in range(100)})
        results = []

        def reader():
            for _ in range(50):
                val = cm.get("key50")
                results.append(val)

        threads = [threading.Thread(target=reader) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert all(v == 50 for v in results)
        assert len(results) == 500

    def test_rwlock_prevents_corruption(self):
        cm = ChronoMap(use_rwlock=True)
        barrier = threading.Barrier(3)

        def writer(id):
            for i in range(10):
                cm.put(f"key{id}", i)
            barrier.wait()

        t1 = threading.Thread(target=writer, args=(1,))
        t2 = threading.Thread(target=writer, args=(2,))
        t1.start()
        t2.start()
        barrier.wait()
        assert len(cm.history("key1")) == 10
        assert len(cm.history("key2")) == 10
        assert cm["key1"] == 9
        assert cm["key2"] == 9

    def test_rwlock_vs_rlock_performance_hint(self):
        cm1 = ChronoMap(use_rwlock=False)
        cm2 = ChronoMap(use_rwlock=True)
        cm1["a"] = 1
        cm2["a"] = 1
        assert cm1["a"] == cm2["a"]


# ============================================================================
# AsyncChronoMap Tests
# ============================================================================


@pytest.mark.async_map
class TestAsyncChronoMap:
    """Test async version."""

    @pytest.mark.asyncio
    async def test_async_put_get(self):
        cm = AsyncChronoMap()
        await cm.put("key", "value")
        assert await cm.get("key") == "value"

    @pytest.mark.asyncio
    async def test_async_delete(self):
        cm = AsyncChronoMap()
        await cm.put("key", "value")
        existed = await cm.delete("key")
        assert existed is True
        assert await cm.get("key", default=None) is None

    @pytest.mark.asyncio
    async def test_async_snapshot(self):
        cm = AsyncChronoMap()
        await cm.put("key", "v1")
        snap = await cm.snapshot()
        await cm.put("key", "v2")
        assert await cm.get("key") == "v2"
        assert await snap.get("key") == "v1"

    @pytest.mark.asyncio
    async def test_async_on_change_sync_callback(self):
        cm = AsyncChronoMap()
        log = []
        cm.on_change(lambda k, o, n, t: log.append((k, n)))
        await cm.put("k", "v")
        assert log == [("k", "v")]

    @pytest.mark.asyncio
    async def test_async_on_change_async_callback(self):
        cm = AsyncChronoMap()
        log = []

        async def async_cb(k, o, n, t):
            log.append((k, n))

        cm.on_change(async_cb)
        await cm.put("k", "v")
        assert log == [("k", "v")]

    @pytest.mark.asyncio
    async def test_async_subscribe_only_fires_for_matching_key(self):
        cm = AsyncChronoMap()
        log = []

        cm.subscribe("config", lambda old, new, ts: log.append((old, new)))

        await cm.put("config", "v1")
        await cm.put("other", "ignored")
        await cm.put("config", "v2")

        assert log == [(None, "v1"), ("v1", "v2")]

    @pytest.mark.asyncio
    async def test_async_subscribe_accepts_async_callback_and_unsubscribes(self):
        cm = AsyncChronoMap()
        log = []

        async def callback(old, new, ts):
            log.append(new)

        cm.subscribe("key", callback)
        await cm.put_many({"key": "v1", "other": "ignored"})

        assert cm.unsubscribe("key", callback) is True
        await cm.put("key", "v2")

        assert log == ["v1"]

    @pytest.mark.asyncio
    async def test_async_keys_latest(self):
        cm = AsyncChronoMap()
        await cm.put_many({"a": 1, "b": 2})
        keys = await cm.keys()
        latest = await cm.latest()
        assert set(keys) == {"a", "b"}
        assert latest == {"a": 1, "b": 2}

    @pytest.mark.asyncio
    async def test_async_get_or_set(self):
        cm = AsyncChronoMap()
        calls = []

        missing = await cm.get_or_set("config", lambda: calls.append("called") or "created")
        existing = await cm.get_or_set("config", lambda: calls.append("called-again") or "ignored")

        assert missing == "created"
        assert existing == "created"
        assert calls == ["called"]

    @pytest.mark.asyncio
    async def test_async_keys_with_history_count(self):
        cm = AsyncChronoMap()
        await cm.put("a", 1, timestamp=1)
        await cm.put("a", 2, timestamp=2)
        await cm.put("b", 99, timestamp=1)

        assert await cm.keys_with_history_count() == {"a": 2, "b": 1}

    @pytest.mark.asyncio
    async def test_async_with_max_history(self):
        cm = AsyncChronoMap(max_history=10)

        for i in range(50):
            await cm.put("key", i, timestamp=i)

        # Should auto-prune to 10
        stats = cm.get_stats()
        assert stats["auto_prunes"] > 0


# ============================================================================
# Integration Tests
# ============================================================================


@pytest.mark.core
class TestIntegration:
    """Integration tests combining multiple features."""

    def test_full_workflow(self):
        """Test a complete workflow with multiple features."""
        cm = ChronoMap(max_history=100, cache_size=50, debug=False)
        cm.put_many({"user1": "active", "user2": "active", "user3": "inactive"})

        snap1 = cm.snapshot()
        cm["user1"] = "inactive"
        cm["user4"] = "active"

        active_users = cm.get_keys_by_value("active")
        assert set(active_users) == {"user2", "user4"}

        changed = cm.diff(snap1)
        assert "user1" in changed
        assert "user4" in changed

        cm.rollback(snap1)
        assert cm["user1"] == "active"
        assert "user4" not in cm

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "state.pkl"
            cm.save_pickle(filepath, compress="zlib")
            cm2 = ChronoMap.load_pickle(filepath)
            assert cm2 == cm

    def test_time_series_scenario(self):
        cm = ChronoMap(max_history=100)
        for hour in range(24):
            temp = 20 + (hour % 12)
            cm.put("temperature", temp, timestamp=hour * 3600)
        morning_temps = cm.get_range("temperature", start_ts=0, end_ts=12 * 3600)
        assert len(morning_temps) == 13
        full_history = cm.history("temperature")
        assert len(full_history) == 24

    def test_session_management_scenario(self):
        cm = ChronoMap(enable_ttl_cleanup=False)
        cm.put("session1", {"user": "alice"}, ttl=0.2)
        cm.put("session2", {"user": "bob"}, ttl=0.2)
        cm.put("session3", {"user": "charlie"}, ttl=0.2)
        assert len(cm) == 3
        time.sleep(0.25)
        assert len(cm) == 0

    def test_performance_optimized_workflow(self):
        """Test all performance features together."""
        cm = ChronoMap(
            max_history=50,
            cache_size=100,
            enable_ttl_cleanup=True,
            ttl_cleanup_interval=0.1,
            max_memory_mb=10,
        )

        # Batch insert
        cm.put_many({f"metric:{i}": i for i in range(200)})

        # History auto-pruned to 50
        for i in range(200):
            history = cm.history(f"metric:{i}")
            assert len(history) <= 50

        # Cache working
        _ = cm["metric:0"]
        _ = cm["metric:0"]
        stats = cm.get_stats()
        assert stats["cache_hits"] > 0

        # Stats comprehensive
        assert "auto_prunes" in stats
        assert "total_keys" in stats
        assert "cache_hit_rate" in stats


# ============================================================================
# Backward Compatibility Tests
# ============================================================================


@pytest.mark.core
class TestBackwardCompatibility:
    """Ensure old save-file formats still load correctly."""

    def test_load_v2_1_0_json(self):
        data = {"store": {"key": [(100.0, "value")]}, "ttl": {}, "snapshot_time": None, "version": "2.1.0"}
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "old.json"
            with open(path, "w") as f:
                json.dump(data, f)
            cm = ChronoMap.load_json(path)
            assert cm["key"] == "value"

    def test_load_v2_0_0_pickle(self):
        # Old format without max_history
        data = {"store": {"key": [(100.0, "value")]}, "ttl": {}, "snapshot_time": None, "version": "2.0.0"}
        cm = ChronoMap.from_dict(data)
        assert cm["key"] == "value"
        assert cm._max_history is None

    def test_repr_truncation(self):
        cm = ChronoMap()
        for i in range(20):
            cm[f"key{i}"] = i
        r = repr(cm)
        assert "..." in r
        assert len(r) < 200


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
