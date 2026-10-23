# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import pytest

from chronomap import ChronoMap, versions


@pytest.mark.query
def test_version_query_keys_preserves_first_seen_order():
    cm = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    cm.put("b", 1, timestamp=100)
    cm.put("a", 2, timestamp=100)
    cm.put("b", 3, timestamp=200)
    cm.put("c", 4, timestamp=200)
    cm.put("a", 5, timestamp=300)

    assert versions(cm).keys() == ["b", "a", "c"]


@pytest.mark.query
def test_version_query_keys_returns_unique_hashable_keys():
    cm = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    for index in range(100):
        cm.put(index % 10, index, timestamp=float(index))

    keys = versions(cm).keys()

    assert keys == list(range(10))
    assert len(keys) == len(set(keys))


@pytest.mark.query
def test_version_query_keys_respects_filters():
    cm = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    cm.put("a", 1, timestamp=100)
    cm.put("b", 2, timestamp=100)
    cm.put("c", 3, timestamp=200)

    assert versions(cm, start=200).keys() == ["c"]
    assert versions(cm).where_value(lambda value: value < 3).keys() == ["a", "b"]
