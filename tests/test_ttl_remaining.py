import asyncio
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap.exceptions import ChronoMapTypeError


def test_ttl_remaining_without_key_or_ttl():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("persistent", 1)

    assert cm.ttl_remaining("missing") is None
    assert cm.ttl_remaining("persistent") is None


def test_ttl_remaining_counts_down_and_reaches_zero():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("short", 1, ttl=0.05)

    first = cm.ttl_remaining("short")
    assert first is not None
    assert 0.0 < first <= 0.05

    time.sleep(0.02)
    second = cm.ttl_remaining("short")
    assert second is not None
    assert second < first

    time.sleep(0.05)
    assert cm.ttl_remaining("short") == 0.0


def test_ttl_remaining_rejects_invalid_key():
    cm = ChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapTypeError):
        cm.ttl_remaining(["unhashable"])


@pytest.mark.asyncio
async def test_async_ttl_remaining_without_key_or_ttl():
    cm = AsyncChronoMap()
    await cm.put("persistent", 1)

    assert await cm.ttl_remaining("missing") is None
    assert await cm.ttl_remaining("persistent") is None


@pytest.mark.asyncio
async def test_async_ttl_remaining_counts_down_and_reaches_zero():
    cm = AsyncChronoMap()
    await cm.put("short", 1, ttl=0.05)

    first = await cm.ttl_remaining("short")
    assert first is not None
    assert 0.0 < first <= 0.05

    await asyncio.sleep(0.02)
    second = await cm.ttl_remaining("short")
    assert second is not None
    assert second < first

    await asyncio.sleep(0.05)
    assert await cm.ttl_remaining("short") == 0.0


@pytest.mark.asyncio
async def test_async_ttl_remaining_rejects_invalid_key():
    cm = AsyncChronoMap()

    with pytest.raises(ChronoMapTypeError):
        await cm.ttl_remaining(["unhashable"])
