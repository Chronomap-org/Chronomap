import asyncio

import pytest

from chronomap import AsyncChronoMap


def test_async_repr_empty():
    assert repr(AsyncChronoMap()) == "AsyncChronoMap(keys=[])"


@pytest.mark.asyncio
async def test_async_repr_previews_live_keys():
    cm = AsyncChronoMap()
    await cm.put("a", 1)
    await cm.put("expired", 2, ttl=0.01)
    await cm.put("b", 3)
    await asyncio.sleep(0.02)

    assert "expired" not in repr(cm)
    assert repr(cm) == "AsyncChronoMap(keys=['a', 'b'])"


@pytest.mark.asyncio
async def test_async_repr_truncates_after_ten_keys():
    cm = AsyncChronoMap()
    for index in range(12):
        await cm.put(f"key{index}", index)

    rendered = repr(cm)

    assert rendered.startswith("AsyncChronoMap(keys=['key0', ")
    assert rendered.endswith("']...)")
    assert "key10" not in rendered
