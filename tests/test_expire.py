import asyncio
import math
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap._journal import JournalCorruptionError
from chronomap.exceptions import ChronoMapTypeError, ChronoMapValueError


def test_expire_sets_and_replaces_ttl():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("key", "value", ttl=100)

    assert cm.expire("key", 1) is True
    assert cm._ttl["key"] <= time.time() + 1.1


def test_expire_returns_false_for_missing_or_expired_key():
    cm = ChronoMap(enable_ttl_cleanup=False)

    assert cm.expire("missing", 10) is False

    cm.put("expired", "value", ttl=0.01)
    time.sleep(0.02)

    assert cm.expire("expired", 10) is False
    assert "expired" not in cm


@pytest.mark.parametrize("ttl", [0, -1, math.nan, math.inf, True])
def test_expire_rejects_invalid_ttl(ttl):
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("key", "value")

    with pytest.raises((ChronoMapTypeError, ChronoMapValueError)):
        cm.expire("key", ttl)

    assert "key" not in cm._ttl


def test_expire_survives_wal_recovery(tmp_path):
    path = tmp_path / "state.wal"
    cm = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    cm.put("key", "value")
    assert cm.expire("key", 60) is True
    cm.close()

    restored = ChronoMap(wal_path=path, enable_ttl_cleanup=False)

    assert "key" in restored._ttl
    assert restored._ttl["key"] > time.time()


def test_invalid_expire_record_is_rejected(tmp_path):
    path = tmp_path / "state.wal"
    cm = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    cm.put("key", "value")
    assert cm._wal is not None
    cm._wal.append("expire", {"key": "key", "expires_at": math.nan})
    cm.close()

    with pytest.raises(JournalCorruptionError):
        ChronoMap(wal_path=path, enable_ttl_cleanup=False)


@pytest.mark.asyncio
async def test_async_expire_sets_and_replaces_ttl():
    cm = AsyncChronoMap()
    await cm.put("key", "value", ttl=100)

    assert await cm.expire("key", 1) is True
    assert cm._ttl["key"] <= time.time() + 1.1


@pytest.mark.asyncio
async def test_async_expire_returns_false_for_missing_or_expired_key():
    cm = AsyncChronoMap()

    assert await cm.expire("missing", 10) is False

    await cm.put("expired", "value", ttl=0.01)
    await asyncio.sleep(0.02)

    assert await cm.expire("expired", 10) is False
    assert "expired" not in cm._store


@pytest.mark.asyncio
async def test_async_expire_survives_wal_recovery(tmp_path):
    path = tmp_path / "state.wal"
    cm = AsyncChronoMap(wal_path=path)
    await cm.put("key", "value")
    assert await cm.expire("key", 60) is True
    await cm.aclose()

    restored = AsyncChronoMap(wal_path=path)

    assert "key" in restored._ttl
    assert restored._ttl["key"] > time.time()


@pytest.mark.asyncio
async def test_async_invalid_expire_record_is_rejected(tmp_path):
    path = tmp_path / "state.wal"
    cm = AsyncChronoMap(wal_path=path)
    await cm.put("key", "value")
    assert cm._wal is not None
    await cm._journal_append("expire", {"key": "key", "expires_at": math.nan})
    await cm.aclose()

    with pytest.raises(JournalCorruptionError):
        AsyncChronoMap(wal_path=path)
