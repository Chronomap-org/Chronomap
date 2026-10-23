# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""delete_many accepts any iterable of keys, not just lists."""

import asyncio
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap


def test_delete_many_accepts_generators_and_tuples():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put_many({"a": 1, "b": 2, "c": 3})

    assert cm.delete_many(key for key in ("a", "b", "a")) == 2
    assert set(cm.keys()) == {"c"}


def test_delete_many_accepts_sets_and_skips_expired_keys():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("a", 1)
    cm.put("b", 2)
    cm.put("expired", 3, ttl=0.01)
    time.sleep(0.02)

    assert cm.delete_many({"a", "expired"}) == 1
    assert set(cm.keys()) == {"b"}
    assert "expired" not in cm._store


@pytest.mark.asyncio
async def test_async_delete_many_accepts_generators_and_tuples():
    cm = AsyncChronoMap()
    await cm.put_many({"a": 1, "b": 2, "c": 3})

    assert await cm.delete_many(key for key in ("a", "b", "a")) == 2
    assert set(await cm.keys()) == {"c"}


@pytest.mark.asyncio
async def test_async_delete_many_accepts_sets_and_skips_expired_keys():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("a", 1)
    await cm.put("b", 2)
    await cm.put("expired", 3, ttl=0.01)
    await asyncio.sleep(0.02)

    assert await cm.delete_many({"a", "expired"}) == 1
    assert set(await cm.keys()) == {"b"}
    assert "expired" not in cm._store
