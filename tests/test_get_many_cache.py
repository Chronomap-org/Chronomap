# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Tests that batch reads participate in the read cache."""

import asyncio
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap.exceptions import ChronoMapKeyError


def test_get_many_uses_and_updates_the_cache():
    cm = ChronoMap(cache_size=8, enable_ttl_cleanup=False)
    cm.put("a", 1, timestamp=100)
    cm.put("b", 2, timestamp=100)
    cm.get("a", timestamp=100)

    before = cm.get_stats()
    assert cm.get_many(["a", "b"], timestamp=100) == {"a": 1, "b": 2}
    after = cm.get_stats()

    assert after["cache_hits"] == before["cache_hits"] + 1
    assert after["cache_misses"] == before["cache_misses"] + 1
    assert after["reads"] == before["reads"] + 2

    hit_before = cm.get_stats()["cache_hits"]
    assert cm.get("b", timestamp=100) == 2
    assert cm.get_stats()["cache_hits"] == hit_before + 1


def test_get_many_keeps_timestamp_cache_entries_separate():
    cm = ChronoMap(cache_size=8, enable_ttl_cleanup=False)
    cm.put("a", "old", timestamp=10)
    cm.put("a", "new", timestamp=20)

    assert cm.get_many(["a"], timestamp=20) == {"a": "new"}
    assert cm.get_many(["a"], timestamp=15) == {"a": "old"}
    assert cm.get("a", timestamp=15) == "old"


def test_get_many_skips_cached_values_for_expired_keys():
    cm = ChronoMap(cache_size=8, enable_ttl_cleanup=False)
    cm.put("k", 1, timestamp=100, ttl=0.01)
    assert cm.get("k", timestamp=100) == 1
    time.sleep(0.02)

    hits_before = cm.get_stats()["cache_hits"]
    assert cm.get_many(["k"], timestamp=100) == {}
    assert cm.get_stats()["cache_hits"] == hits_before
    assert "k" not in cm._cache._keys_by_store_key
    with pytest.raises(ChronoMapKeyError):
        cm.get_many(["k"], timestamp=100, strict=True)


def test_get_many_without_cache_still_reads_values():
    cm = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    cm.put("a", 1, timestamp=100)

    assert cm.get_many(["a", "missing"], timestamp=100) == {"a": 1}
    assert "cache_size" not in cm.get_stats()
    assert cm.get_stats()["cache_hits"] == 0


@pytest.mark.asyncio
async def test_async_get_many_uses_and_updates_the_cache():
    cm = AsyncChronoMap(cache_size=8)
    await cm.put("a", 1, timestamp=100)
    await cm.put("b", 2, timestamp=100)
    await cm.get("a", timestamp=100)

    before = cm.get_stats()
    assert await cm.get_many(["a", "b"], timestamp=100) == {"a": 1, "b": 2}
    after = cm.get_stats()

    assert after["cache_hits"] == before["cache_hits"] + 1
    assert after["cache_misses"] == before["cache_misses"] + 1
    assert after["reads"] == before["reads"] + 2

    hit_before = cm.get_stats()["cache_hits"]
    assert await cm.get("b", timestamp=100) == 2
    assert cm.get_stats()["cache_hits"] == hit_before + 1


@pytest.mark.asyncio
async def test_async_get_many_keeps_timestamp_cache_entries_separate():
    cm = AsyncChronoMap(cache_size=8)
    await cm.put("a", "old", timestamp=10)
    await cm.put("a", "new", timestamp=20)

    assert await cm.get_many(["a"], timestamp=20) == {"a": "new"}
    assert await cm.get_many(["a"], timestamp=15) == {"a": "old"}
    assert await cm.get("a", timestamp=15) == "old"


@pytest.mark.asyncio
async def test_async_get_many_skips_cached_values_for_expired_keys():
    cm = AsyncChronoMap(cache_size=8)
    await cm.put("k", 1, timestamp=100, ttl=0.01)
    assert await cm.get("k", timestamp=100) == 1
    await asyncio.sleep(0.02)

    hits_before = cm.get_stats()["cache_hits"]
    assert await cm.get_many(["k"], timestamp=100) == {}
    assert cm.get_stats()["cache_hits"] == hits_before
    assert "k" not in cm._cache._keys_by_store_key
    with pytest.raises(ChronoMapKeyError):
        await cm.get_many(["k"], timestamp=100, strict=True)


@pytest.mark.asyncio
async def test_async_get_many_without_cache_still_reads_values():
    cm = AsyncChronoMap(cache_size=0)
    await cm.put("a", 1, timestamp=100)

    assert await cm.get_many(["a", "missing"], timestamp=100) == {"a": 1}
    assert "cache_size" not in cm.get_stats()
    assert cm.get_stats()["cache_hits"] == 0
