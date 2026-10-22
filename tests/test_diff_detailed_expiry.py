# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""diff_detailed() reports the same expiry-state differences as diff()."""

import asyncio
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap


@pytest.mark.snapshot
def test_both_sides_expired_are_equal():
    left = ChronoMap(enable_ttl_cleanup=False)
    right = ChronoMap(enable_ttl_cleanup=False)
    left.put("k", "left", ttl=0.01)
    right.put("k", "right", ttl=0.01)
    time.sleep(0.02)

    assert left.diff(right) == set()
    assert left.diff_detailed(right) == []


@pytest.mark.snapshot
def test_expired_side_differs_from_stored_none():
    left = ChronoMap(enable_ttl_cleanup=False)
    right = ChronoMap(enable_ttl_cleanup=False)
    left.put("k", "value", ttl=0.01)
    right.put("k", None)
    time.sleep(0.02)

    assert left.diff(right) == {"k"}
    assert left.diff_detailed(right) == [("k", None, None)]


@pytest.mark.snapshot
@pytest.mark.asyncio
async def test_async_both_sides_expired_are_equal():
    left = AsyncChronoMap(enable_ttl_cleanup=False)
    right = AsyncChronoMap(enable_ttl_cleanup=False)
    await left.put("k", "left", ttl=0.01)
    await right.put("k", "right", ttl=0.01)
    await asyncio.sleep(0.02)

    assert await left.diff(right) == set()
    assert await left.diff_detailed(right) == []


@pytest.mark.snapshot
@pytest.mark.asyncio
async def test_async_expired_side_differs_from_stored_none():
    left = AsyncChronoMap(enable_ttl_cleanup=False)
    right = AsyncChronoMap(enable_ttl_cleanup=False)
    await left.put("k", "value", ttl=0.01)
    await right.put("k", None)
    await asyncio.sleep(0.02)

    assert await left.diff(right) == {"k"}
    assert await left.diff_detailed(right) == [("k", None, None)]
