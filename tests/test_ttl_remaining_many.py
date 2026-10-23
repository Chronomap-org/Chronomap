import asyncio
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap.exceptions import ChronoMapTypeError


def test_ttl_remaining_many_reports_one_snapshot():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("a", 1)
    cm.put("b", 2)
    cm.expire_many(["a", "b"], 3600)
    cm.put("plain", 3)
    cm.put("expired", 4, ttl=0.01)
    time.sleep(0.02)

    result = cm.ttl_remaining_many(["a", "b", "a", "plain", "missing", "expired"])

    assert list(result) == ["a", "b", "plain", "missing", "expired"]
    assert result["a"] == result["b"]
    assert 3599 < result["a"] <= 3600
    assert result["plain"] is None
    assert result["missing"] is None
    assert result["expired"] == 0.0
    assert cm.ttl_remaining_many([]) == {}


def test_ttl_remaining_many_validates_every_key():
    cm = ChronoMap()
    cm.put("a", 1, ttl=60)

    with pytest.raises(ChronoMapTypeError):
        cm.ttl_remaining_many(["a", ["bad"]])


@pytest.mark.asyncio
async def test_async_ttl_remaining_many_reports_one_snapshot():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("a", 1)
    await cm.put("b", 2)
    await cm.expire_many(["a", "b"], 3600)
    await cm.put("plain", 3)
    await cm.put("expired", 4, ttl=0.01)
    await asyncio.sleep(0.02)

    result = await cm.ttl_remaining_many(["a", "b", "a", "plain", "missing", "expired"])

    assert list(result) == ["a", "b", "plain", "missing", "expired"]
    assert result["a"] == result["b"]
    assert 3599 < result["a"] <= 3600
    assert result["plain"] is None
    assert result["missing"] is None
    assert result["expired"] == 0.0
    assert await cm.ttl_remaining_many([]) == {}


@pytest.mark.asyncio
async def test_async_ttl_remaining_many_validates_every_key():
    cm = AsyncChronoMap()
    await cm.put("a", 1, ttl=60)

    with pytest.raises(ChronoMapTypeError):
        await cm.ttl_remaining_many(["a", ["bad"]])
