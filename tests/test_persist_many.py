import asyncio
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap._journal import JournalCorruptionError
from chronomap.exceptions import ChronoMapTypeError


def test_persist_many_removes_ttls_and_skips_missing_or_expired():
    cm = ChronoMap(cache_size=4, enable_ttl_cleanup=False)
    cm.put("a", 1, ttl=60)
    cm.put("b", 2, ttl=60)
    cm.put("plain", 3)
    cm.put("expired", 4, ttl=0.01)
    cm.get("a")
    cm.get("b")
    time.sleep(0.02)

    assert cm.persist_many(["a", "b", "a", "plain", "missing", "expired"]) == 2
    assert "a" not in cm._ttl
    assert "b" not in cm._ttl
    assert "a" not in cm._cache._keys_by_store_key
    assert "b" not in cm._cache._keys_by_store_key
    assert "expired" not in cm


def test_persist_many_no_ttls_and_validation():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("plain", 1)

    assert cm.persist_many([]) == 0
    assert cm.persist_many(["plain", "missing"]) == 0

    cm.put("ttl", 2, ttl=60)
    with pytest.raises(ChronoMapTypeError):
        cm.persist_many(["ttl", ["bad"]])
    assert "ttl" in cm._ttl


def test_persist_many_survives_wal_recovery(tmp_path):
    path = tmp_path / "state.wal"
    cm = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    cm.put("a", 1, ttl=60)
    cm.put("b", 2, ttl=60)
    assert cm.persist_many(["a", "b"]) == 2
    cm.close()

    restored = ChronoMap(wal_path=path, enable_ttl_cleanup=False)

    assert "a" not in restored._ttl
    assert "b" not in restored._ttl


def test_invalid_persist_many_record_is_rejected(tmp_path):
    path = tmp_path / "state.wal"
    cm = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    cm.put("a", 1, ttl=60)
    assert cm._wal is not None
    cm._wal.append("persist_many", {"keys": "not-a-list"})
    cm.close()

    with pytest.raises(JournalCorruptionError):
        ChronoMap(wal_path=path, enable_ttl_cleanup=False)


@pytest.mark.asyncio
async def test_async_persist_many_removes_ttls_and_skips_missing_or_expired():
    cm = AsyncChronoMap(cache_size=4)
    await cm.put("a", 1, ttl=60)
    await cm.put("b", 2, ttl=60)
    await cm.put("plain", 3)
    await cm.put("expired", 4, ttl=0.01)
    await cm.get("a")
    await cm.get("b")
    await asyncio.sleep(0.02)

    assert await cm.persist_many(["a", "b", "a", "plain", "missing", "expired"]) == 2
    assert "a" not in cm._ttl
    assert "b" not in cm._ttl
    assert "a" not in cm._cache._keys_by_store_key
    assert "b" not in cm._cache._keys_by_store_key
    assert "expired" not in cm._store


@pytest.mark.asyncio
async def test_async_persist_many_no_ttls_and_validation():
    cm = AsyncChronoMap()
    await cm.put("plain", 1)

    assert await cm.persist_many([]) == 0
    assert await cm.persist_many(["plain", "missing"]) == 0

    await cm.put("ttl", 2, ttl=60)
    with pytest.raises(ChronoMapTypeError):
        await cm.persist_many(["ttl", ["bad"]])
    assert "ttl" in cm._ttl


@pytest.mark.asyncio
async def test_async_persist_many_survives_wal_recovery(tmp_path):
    path = tmp_path / "state.wal"
    cm = AsyncChronoMap(wal_path=path)
    await cm.put("a", 1, ttl=60)
    await cm.put("b", 2, ttl=60)
    assert await cm.persist_many(["a", "b"]) == 2
    await cm.aclose()

    restored = AsyncChronoMap(wal_path=path)

    assert "a" not in restored._ttl
    assert "b" not in restored._ttl


@pytest.mark.asyncio
async def test_async_invalid_persist_many_record_is_rejected(tmp_path):
    path = tmp_path / "state.wal"
    cm = AsyncChronoMap(wal_path=path)
    await cm.put("a", 1, ttl=60)
    assert cm._wal is not None
    await cm._journal_append("persist_many", {"keys": "not-a-list"})
    await cm.aclose()

    with pytest.raises(JournalCorruptionError):
        AsyncChronoMap(wal_path=path)
