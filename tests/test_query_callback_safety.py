# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""query() and aggregate() callbacks run without holding the map lock."""

import pytest

from chronomap import ChronoMap


@pytest.mark.core
def test_query_predicate_can_write_to_the_map():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put_many({"a": 1, "b": 2})

    result = cm.query(lambda key, value: cm.put("side", key) is None)

    assert result == {"a": 1, "b": 2}
    assert cm.get("side") == "b"


@pytest.mark.core
def test_aggregate_function_can_write_to_the_map():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put_many({"a": 1, "b": 2})

    def total(values):
        cm.put("total", sum(values))
        return sum(values)

    assert cm.aggregate(total) == 3
    assert cm.get("total") == 3


@pytest.mark.core
def test_query_predicate_exception_releases_the_lock():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("a", 1)

    def fail(key, value):
        raise RuntimeError("predicate failed")

    with pytest.raises(RuntimeError, match="predicate failed"):
        cm.query(fail)

    cm.put("after", 2)
    assert cm.get("after") == 2


@pytest.mark.core
def test_aggregate_snapshot_ignores_callback_writes():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put_many({"a": 1, "b": 2})

    def mutate(values):
        cm.put("c", 3)
        return sum(values)

    assert cm.aggregate(mutate) == 3
    assert cm.get("c") == 3
