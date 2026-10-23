import asyncio
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap.exceptions import ChronoMapTypeError


def test_pop_many_returns_latest_values_and_removes_keys():
    cm = ChronoMap(cache_size=4, enable_ttl_cleanup=False)
    cm.put("a", 1)
    cm.put("a", 2)
    cm.put("b", 3)
    cm.get("a")

    assert cm.pop_many(["a", "b", "missing"]) == {"a": 2, "b": 3}
    assert "a" not in cm
    assert "b" not in cm
    assert "a" not in cm._cache._keys_by_store_key
    assert "b" not in cm._cache._keys_by_store_key


def test_pop_many_skips_duplicates_and_expired_keys():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("a", 1)
    cm.put("expired", "value", ttl=0.01)
    time.sleep(0.02)

    assert cm.pop_many(["a", "a", "expired"]) == {"a": 1}
    assert "a" not in cm
    assert "expired" not in cm
    assert cm.get_stats()["deletes"] == 1


def test_pop_many_empty_and_validation():
    cm = ChronoMap(enable_ttl_cleanup=False)

    assert cm.pop_many([]) == {}
    with pytest.raises(ChronoMapTypeError):
        cm.pop_many(["ok", ["bad"]])
    assert "ok" not in cm._store


def test_pop_many_survives_wal_recovery(tmp_path):
    path = tmp_path / "state.wal"
    cm = ChronoMap(wal_path=path)
    cm.put("a", 1)
    cm.put("b", 2)
    assert cm.pop_many(["a", "b"]) == {"a": 1, "b": 2}
    cm.close()

    restored = ChronoMap(wal_path=path)

    assert len(restored) == 0


@pytest.mark.asyncio
async def test_async_pop_many_returns_latest_values_and_removes_keys():
    cm = AsyncChronoMap(cache_size=4)
    await cm.put("a", 1)
    await cm.put("a", 2)
    await cm.put("b", 3)
    await cm.get("a")

    assert await cm.pop_many(["a", "b", "missing"]) == {"a": 2, "b": 3}
    assert "a" not in cm._store
    assert "b" not in cm._store
    assert "a" not in cm._cache._keys_by_store_key
    assert "b" not in cm._cache._keys_by_store_key


@pytest.mark.asyncio
async def test_async_pop_many_skips_duplicates_and_expired_keys():
    cm = AsyncChronoMap()
    await cm.put("a", 1)
    await cm.put("expired", "value", ttl=0.01)
    await asyncio.sleep(0.02)

    assert await cm.pop_many(["a", "a", "expired"]) == {"a": 1}
    assert "a" not in cm._store
    assert "expired" not in cm._store
    assert cm.get_stats()["deletes"] == 1


@pytest.mark.asyncio
async def test_async_pop_many_empty_and_validation():
    cm = AsyncChronoMap()

    assert await cm.pop_many([]) == {}
    with pytest.raises(ChronoMapTypeError):
        await cm.pop_many(["ok", ["bad"]])
    assert "ok" not in cm._store


@pytest.mark.asyncio
async def test_async_pop_many_survives_wal_recovery(tmp_path):
    path = tmp_path / "state.wal"
    cm = AsyncChronoMap(wal_path=path)
    await cm.put("a", 1)
    await cm.put("b", 2)
    assert await cm.pop_many(["a", "b"]) == {"a": 1, "b": 2}
    await cm.aclose()

    restored = AsyncChronoMap(wal_path=path)

    assert await restored.keys() == []
