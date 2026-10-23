# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""count() honors timestamps when no predicate is supplied."""

import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap.exceptions import ChronoMapValueError


@pytest.mark.core
def test_count_timestamp_filters_versions():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("early", "a", timestamp=100)
    cm.put("late", "b", timestamp=200)

    assert cm.count() == 2
    assert cm.count(timestamp=50) == 0
    assert cm.count(timestamp=100) == 1
    assert cm.count(timestamp=150) == 1
    assert cm.count(timestamp=250) == 2


@pytest.mark.core
def test_count_timestamp_accepts_iso_strings():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("key", "value", timestamp="2026-01-01T00:00:00+00:00")

    assert cm.count(timestamp="2025-12-31T23:59:59+00:00") == 0
    assert cm.count(timestamp="2026-01-01T00:00:01+00:00") == 1


@pytest.mark.core
def test_count_timestamp_still_validates():
    cm = ChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapValueError):
        cm.count(timestamp="not-a-timestamp")


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_count_timestamp_matches_sync():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("early", "a", timestamp=100)
    await cm.put("late", "b", timestamp=200)

    assert await cm.count(timestamp=50) == 0
    assert await cm.count(timestamp=100) == 1
    assert await cm.count(timestamp=250) == 2
