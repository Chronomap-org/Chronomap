import asyncio
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap.exceptions import ChronoMapKeyError


def test_popitem_can_remove_oldest_first():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("first", 1)
    cm.put("second", 2)
    cm.put("third", 3)

    assert cm.popitem(last=False) == ("first", 1)
    assert cm.popitem() == ("third", 3)
    assert cm.popitem(last=False) == ("second", 2)
    with pytest.raises(ChronoMapKeyError):
        cm.popitem()


def test_popitem_oldest_skips_expired_keys():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("expired", "old", ttl=0.01)
    cm.put("live", "new")
    time.sleep(0.02)

    assert cm.popitem(last=False) == ("live", "new")
    assert "expired" not in cm


@pytest.mark.asyncio
async def test_async_popitem_can_remove_oldest_first():
    cm = AsyncChronoMap()
    await cm.put("first", 1)
    await cm.put("second", 2)
    await cm.put("third", 3)

    assert await cm.popitem(last=False) == ("first", 1)
    assert await cm.popitem() == ("third", 3)
    assert await cm.popitem(last=False) == ("second", 2)
    with pytest.raises(ChronoMapKeyError):
        await cm.popitem()


@pytest.mark.asyncio
async def test_async_popitem_oldest_skips_expired_keys():
    cm = AsyncChronoMap()
    await cm.put("expired", "old", ttl=0.01)
    await cm.put("live", "new")
    await asyncio.sleep(0.02)

    assert await cm.popitem(last=False) == ("live", "new")
    assert "expired" not in cm._store
