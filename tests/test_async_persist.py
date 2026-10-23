# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import asyncio

import pytest

from chronomap import AsyncChronoMap, ChronoMapTypeError


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_persist_removes_a_live_ttl():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("session", "active", ttl=0.05)

    assert await cm.persist("session") is True
    assert "session" not in cm._ttl

    await asyncio.sleep(0.1)
    assert await cm.get("session") == "active"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_persist_returns_false_without_a_ttl():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("session", "active")

    assert await cm.persist("session") is False
    assert await cm.persist("missing") is False


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_persist_purges_an_expired_key():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("session", "active", ttl=0.01)
    await asyncio.sleep(0.02)

    assert await cm.persist("session") is False
    assert "session" not in cm._store
    assert "session" not in cm._ttl


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_persist_rejects_unhashable_keys():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapTypeError):
        await cm.persist(["not", "hashable"])
