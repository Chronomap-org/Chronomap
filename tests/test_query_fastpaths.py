# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import pytest

from chronomap import AsyncChronoMap, ChronoMap, ChronoMapKeyError, query


@pytest.mark.query
def test_distinct_deduplicates_hashable_values():
    result = query({"a": 1, "b": 1, "c": 2}).distinct()

    assert result.items() == [("a", 1), ("c", 2)]


@pytest.mark.query
def test_distinct_deduplicates_unhashable_values():
    result = query({"a": [1], "b": [1], "c": [2]}).distinct()

    assert result.items() == [("a", [1]), ("c", [2])]


@pytest.mark.query
def test_values_in_handles_hashable_and_unhashable_values():
    assert query({"a": 1, "b": 2}).values_in([2, 3]).keys() == ["b"]
    assert query({"a": [1], "b": [2]}).values_in([[1]]).keys() == ["a"]


@pytest.mark.query
def test_values_in_falls_back_when_a_row_value_is_unhashable():
    assert query({"a": [1], "b": 2}).values_in([2]).keys() == ["b"]


@pytest.mark.core
def test_popitem_tolerates_an_empty_internal_history():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm._store["empty"] = []

    with pytest.raises(ChronoMapKeyError):
        cm.popitem()


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_popitem_tolerates_an_empty_internal_history():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    cm._store["empty"] = []

    with pytest.raises(ChronoMapKeyError):
        await cm.popitem()
