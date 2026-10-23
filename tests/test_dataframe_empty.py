# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""to_dataframe() keeps its documented columns for empty results."""

import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap

pd = pytest.importorskip("pandas")

COLUMNS = ["key", "value", "timestamp", "datetime", "version"]


@pytest.mark.core
def test_empty_dataframe_keeps_columns():
    cm = ChronoMap(enable_ttl_cleanup=False)

    frame = cm.to_dataframe()

    assert frame.shape == (0, 5)
    assert list(frame.columns) == COLUMNS


@pytest.mark.core
def test_expired_only_dataframe_keeps_columns():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("expired", 1, ttl=0.01)
    time.sleep(0.02)

    frame = cm.to_dataframe()

    assert frame.shape == (0, 5)
    assert list(frame.columns) == COLUMNS


@pytest.mark.core
def test_non_empty_dataframe_keeps_column_order():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("key", "value", timestamp=100)

    frame = cm.to_dataframe()

    assert frame.shape == (1, 5)
    assert list(frame.columns) == COLUMNS


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_empty_dataframe_keeps_columns():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)

    frame = await cm.to_dataframe()

    assert frame.shape == (0, 5)
    assert list(frame.columns) == COLUMNS
