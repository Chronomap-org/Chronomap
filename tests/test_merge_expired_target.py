# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""A live source key replaces an expired target key during merge."""

import asyncio
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap


def _merge_case(strategy):
    target = ChronoMap(enable_ttl_cleanup=False)
    source = ChronoMap(enable_ttl_cleanup=False)
    target.put("k", "old", timestamp=1, ttl=0.01)
    source.put("k", "new", timestamp=2)
    time.sleep(0.02)

    target.merge(source, strategy=strategy)

    assert target.get("k") == "new"
    assert target.ttl_remaining("k") is None


@pytest.mark.ttl
def test_timestamp_merge_replaces_expired_target_state():
    _merge_case("timestamp")


@pytest.mark.ttl
def test_overwrite_merge_replaces_expired_target_state():
    _merge_case("overwrite")


@pytest.mark.ttl
def test_merge_of_expired_target_survives_wal_replay(tmp_path):
    path = tmp_path / "state.wal"
    target = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    source = ChronoMap(enable_ttl_cleanup=False)
    target.put("k", "old", timestamp=1, ttl=0.01)
    source.put("k", "new", timestamp=2)
    time.sleep(0.02)

    target.merge(source)
    target.close()

    recovered = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    try:
        assert recovered.get("k") == "new"
        assert recovered.ttl_remaining("k") is None
    finally:
        recovered.close()


@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_merge_replaces_expired_target_state():
    for strategy in ("timestamp", "overwrite"):
        target = AsyncChronoMap(enable_ttl_cleanup=False)
        source = AsyncChronoMap(enable_ttl_cleanup=False)
        await target.put("k", "old", timestamp=1, ttl=0.01)
        await source.put("k", "new", timestamp=2)
        await asyncio.sleep(0.02)

        await target.merge(source, strategy=strategy)

        assert await target.get("k") == "new"
        assert await target.ttl_remaining("k") is None
