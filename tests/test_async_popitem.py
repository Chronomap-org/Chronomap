# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import asyncio

import pytest

from chronomap import AsyncChronoMap, ChronoMapKeyError


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_popitem_removes_the_most_recent_key():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("first", 1)
    await cm.put("second", 2)

    assert await cm.popitem() == ("second", 2)
    assert await cm.popitem() == ("first", 1)
    assert await cm.size() == 0


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_popitem_skips_and_purges_expired_keys():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("live", 1)
    await cm.put("expired", 2, ttl=0.01)
    await asyncio.sleep(0.02)

    assert await cm.popitem() == ("live", 1)
    assert "expired" not in cm._store
    assert "expired" not in cm._ttl


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_popitem_returns_a_stored_none_value():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("key", None)

    key, value = await cm.popitem()

    assert key == "key"
    assert value is None


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_popitem_raises_on_an_empty_map():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapKeyError):
        await cm.popitem()
