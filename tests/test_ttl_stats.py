# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""TTL summary fields reported by get_stats()."""

import asyncio
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap


def test_ttl_stats_counts_live_deadlines():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put_many({"a": 1, "b": 2, "c": 3})
    cm.expire_many(["a", "b"], ttl=3600)

    stats = cm.get_stats()
    assert stats["ttl_keys"] == 2
    assert 0 < stats["next_expiry_seconds"] <= 3600

    cm.persist("a")
    stats = cm.get_stats()
    assert stats["ttl_keys"] == 1
    assert 0 < stats["next_expiry_seconds"] <= 3600


def test_ttl_stats_without_ttls():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("plain", 1)

    stats = cm.get_stats()
    assert stats["ttl_keys"] == 0
    assert stats["next_expiry_seconds"] is None


def test_ttl_stats_ignores_unswept_expired_keys():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("short", 1, ttl=0.01)
    cm.put("long", 2, ttl=3600)
    time.sleep(0.02)

    stats = cm.get_stats()
    assert stats["expired_keys"] == 1
    assert stats["ttl_keys"] == 1
    assert 0 < stats["next_expiry_seconds"] <= 3600


@pytest.mark.asyncio
async def test_async_ttl_stats_counts_live_deadlines():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put_many({"a": 1, "b": 2, "c": 3})
    await cm.expire_many(["a", "b"], ttl=3600)

    stats = cm.get_stats()
    assert stats["ttl_keys"] == 2
    assert 0 < stats["next_expiry_seconds"] <= 3600

    await cm.persist("a")
    stats = cm.get_stats()
    assert stats["ttl_keys"] == 1
    assert 0 < stats["next_expiry_seconds"] <= 3600


@pytest.mark.asyncio
async def test_async_ttl_stats_without_ttls():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("plain", 1)

    stats = cm.get_stats()
    assert stats["ttl_keys"] == 0
    assert stats["next_expiry_seconds"] is None


@pytest.mark.asyncio
async def test_async_ttl_stats_ignores_unswept_expired_keys():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("short", 1, ttl=0.01)
    await cm.put("long", 2, ttl=3600)
    await asyncio.sleep(0.02)

    stats = cm.get_stats()
    assert stats["expired_keys"] == 1
    assert stats["ttl_keys"] == 1
    assert 0 < stats["next_expiry_seconds"] <= 3600
