# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import pytest

from chronomap import AsyncChronoMap, ChronoMapTypeError


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_update_accepts_a_mapping_and_forwards_timestamp_and_ttl():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)

    await cm.update({"a": 1, "b": 2}, timestamp=100, ttl=60)

    assert await cm.get("a", timestamp=100) == 1
    assert await cm.get("b", timestamp=100) == 2
    assert await cm.history("a") == [(100.0, 1)]
    assert "a" in cm._ttl
    assert "b" in cm._ttl


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_update_accepts_an_iterable_of_pairs_and_keeps_the_last_value():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)

    await cm.update([("a", 1), ("b", 2), ("a", 3)], timestamp=10)

    assert await cm.get("a") == 3
    assert await cm.get("b") == 2


@pytest.mark.async_map
@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [42, None, ["not-a-pair"]])
async def test_async_update_rejects_invalid_inputs(bad):
    cm = AsyncChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapTypeError):
        await cm.update(bad)


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_update_rejects_unhashable_keys():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapTypeError):
        await cm.update([(["not", "hashable"], 1)])
