# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Memory-guard parity tests for synchronous and asynchronous maps."""

import math

import pytest

from chronomap import AsyncChronoMap, ChronoMap, ChronoMapMemoryError, ChronoMapValueError


@pytest.mark.memory
def test_sync_memory_usage_tracks_growth_and_deletion():
    cm = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    before = cm.memory_usage()

    cm.put("payload", "x" * 10_000, ttl=3600)
    grown = cm.memory_usage()
    assert grown > before + 9_000

    cm.delete("payload")
    assert cm.memory_usage() < grown


@pytest.mark.memory
@pytest.mark.async_map
async def test_async_memory_usage_tracks_growth_and_deletion():
    cm = AsyncChronoMap(cache_size=0)
    before = await cm.memory_usage()

    await cm.put("payload", "x" * 10_000, ttl=3600)
    grown = await cm.memory_usage()
    assert grown > before + 9_000

    await cm.delete("payload")
    assert await cm.memory_usage() < grown


@pytest.mark.memory
@pytest.mark.async_map
@pytest.mark.parametrize("bad_limit", [0, -1, True, math.inf, math.nan, "1"])
def test_async_memory_limit_must_be_positive_and_finite(bad_limit):
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap(max_memory_mb=bad_limit)


@pytest.mark.memory
@pytest.mark.async_map
async def test_async_memory_limit_allows_unlimited_maps():
    cm = AsyncChronoMap(max_memory_mb=None)
    await cm.put("payload", "x" * 10_000)
    assert await cm.get("payload") == "x" * 10_000


@pytest.mark.memory
@pytest.mark.async_map
async def test_async_memory_limit_rejects_put():
    cm = AsyncChronoMap(max_memory_mb=0.001)
    with pytest.raises(ChronoMapMemoryError):
        await cm.put("payload", "x" * 10_000)


@pytest.mark.memory
@pytest.mark.async_map
async def test_async_memory_limit_rejects_put_many():
    cm = AsyncChronoMap(max_memory_mb=0.001)
    with pytest.raises(ChronoMapMemoryError):
        await cm.put_many({f"key-{index}": "x" * 1_000 for index in range(20)})


@pytest.mark.memory
@pytest.mark.async_map
async def test_async_memory_limit_rejects_get_or_set_creation():
    cm = AsyncChronoMap(max_memory_mb=0.001)

    def factory():
        return "x" * 10_000

    with pytest.raises(ChronoMapMemoryError):
        await cm.get_or_set("payload", factory)


@pytest.mark.memory
@pytest.mark.async_map
async def test_async_memory_limit_rejects_setdefault_creation():
    cm = AsyncChronoMap(max_memory_mb=0.001)
    with pytest.raises(ChronoMapMemoryError):
        await cm.setdefault("payload", "x" * 10_000)


@pytest.mark.memory
@pytest.mark.async_map
async def test_async_memory_limit_rejects_merge():
    cm = AsyncChronoMap(max_memory_mb=0.001)
    other = AsyncChronoMap(cache_size=0)
    other._store["payload"] = [(1.0, "x" * 10_000)]

    with pytest.raises(ChronoMapMemoryError):
        await cm.merge(other)
