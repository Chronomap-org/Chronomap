# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import pytest

from chronomap import ChronoMap, QueryError, query


@pytest.fixture
def cm():
    store = ChronoMap(enable_ttl_cleanup=False)
    store.put("a", 1, timestamp=10)
    store.put("b", 2, timestamp=10)
    store.put("c", 3, timestamp=10)
    return store


@pytest.mark.query
def test_last_returns_the_final_filtered_value(cm):
    assert query(cm).last() == 3
    assert query(cm).where_value(lambda value: value < 3).last() == 2


@pytest.mark.query
def test_last_item_returns_the_final_filtered_pair(cm):
    assert query(cm).last_item() == ("c", 3)
    assert query(cm).where_key(lambda key: key != "c").last_item() == ("b", 2)


@pytest.mark.query
def test_last_supports_a_default_for_empty_queries(cm):
    empty = query(cm).where_value(lambda value: value > 100)

    assert empty.last("fallback") == "fallback"
    assert empty.last_item("fallback") == "fallback"


@pytest.mark.query
def test_last_raises_for_empty_queries_without_a_default(cm):
    empty = query(cm).where_value(lambda value: value > 100)

    with pytest.raises(QueryError):
        empty.last()
    with pytest.raises(QueryError):
        empty.last_item()
