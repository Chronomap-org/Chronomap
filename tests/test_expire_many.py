import asyncio
import math
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap._journal import JournalCorruptionError
from chronomap.exceptions import ChronoMapTypeError, ChronoMapValueError


def test_expire_many_sets_one_deadline_and_skips_missing_or_expired():
    cm = ChronoMap(cache_size=4, enable_ttl_cleanup=False)
    cm.put("a", 1)
    cm.put("b", 2)
    cm.put("expired", "old", ttl=0.01)
    cm.get("a")
    cm.get("b")
    time.sleep(0.02)

    assert cm.expire_many(["a", "b", "missing", "a", "expired"], 60) == 2
    assert cm._ttl["a"] == cm._ttl["b"]
    assert "a" not in cm._cache._keys_by_store_key
    assert "b" not in cm._cache._keys_by_store_key
    assert "expired" not in cm


def test_expire_many_no_live_keys_and_validation():
    cm = ChronoMap(enable_ttl_cleanup=False)

    assert cm.expire_many([], 60) == 0
    assert cm.expire_many(["missing"], 60) == 0

    cm.put("ok", 1)
    with pytest.raises(ChronoMapTypeError):
        cm.expire_many(["ok", ["bad"]], 60)
    with pytest.raises(ChronoMapValueError):
        cm.expire_many(["ok"], 0)
    assert "ok" not in cm._ttl


def test_expire_many_survives_wal_recovery(tmp_path):
    path = tmp_path / "state.wal"
    cm = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    cm.put("a", 1)
    cm.put("b", 2)
    assert cm.expire_many(["a", "b"], 60) == 2
    cm.close()

    restored = ChronoMap(wal_path=path, enable_ttl_cleanup=False)

    assert restored._ttl["a"] == restored._ttl["b"]
    assert restored._ttl["a"] > time.time()


@pytest.mark.parametrize(
    "data",
    [
        {"keys": "not-a-list", "expires_at": time.time() + 60},
        {"keys": ["a"], "expires_at": math.nan},
    ],
)
def test_invalid_expire_many_record_is_rejected(tmp_path, data):
    path = tmp_path / "state.wal"
    cm = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    cm.put("a", 1)
    assert cm._wal is not None
    cm._wal.append("expire_many", data)
    cm.close()

    with pytest.raises(JournalCorruptionError):
        ChronoMap(wal_path=path, enable_ttl_cleanup=False)


@pytest.mark.asyncio
async def test_async_expire_many_sets_one_deadline_and_skips_missing_or_expired():
    cm = AsyncChronoMap(cache_size=4)
    await cm.put("a", 1)
    await cm.put("b", 2)
    await cm.put("expired", "old", ttl=0.01)
    await cm.get("a")
    await cm.get("b")
    await asyncio.sleep(0.02)

    assert await cm.expire_many(["a", "b", "missing", "a", "expired"], 60) == 2
    assert cm._ttl["a"] == cm._ttl["b"]
    assert "a" not in cm._cache._keys_by_store_key
    assert "b" not in cm._cache._keys_by_store_key
    assert "expired" not in cm._store


@pytest.mark.asyncio
async def test_async_expire_many_no_live_keys_and_validation():
    cm = AsyncChronoMap()

    assert await cm.expire_many([], 60) == 0
    assert await cm.expire_many(["missing"], 60) == 0

    await cm.put("ok", 1)
    with pytest.raises(ChronoMapTypeError):
        await cm.expire_many(["ok", ["bad"]], 60)
    with pytest.raises(ChronoMapValueError):
        await cm.expire_many(["ok"], 0)
    assert "ok" not in cm._ttl


@pytest.mark.asyncio
async def test_async_expire_many_survives_wal_recovery(tmp_path):
    path = tmp_path / "state.wal"
    cm = AsyncChronoMap(wal_path=path)
    await cm.put("a", 1)
    await cm.put("b", 2)
    assert await cm.expire_many(["a", "b"], 60) == 2
    await cm.aclose()

    restored = AsyncChronoMap(wal_path=path)

    assert restored._ttl["a"] == restored._ttl["b"]
    assert restored._ttl["a"] > time.time()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data",
    [
        {"keys": "not-a-list", "expires_at": time.time() + 60},
        {"keys": ["a"], "expires_at": math.nan},
    ],
)
async def test_async_invalid_expire_many_record_is_rejected(tmp_path, data):
    path = tmp_path / "state.wal"
    cm = AsyncChronoMap(wal_path=path)
    await cm.put("a", 1)
    assert cm._wal is not None
    await cm._journal_append("expire_many", data)
    await cm.aclose()

    with pytest.raises(JournalCorruptionError):
        AsyncChronoMap(wal_path=path)
