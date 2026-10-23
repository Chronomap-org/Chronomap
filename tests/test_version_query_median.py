# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import pytest

from chronomap import ChronoMap, QueryError, versions


@pytest.mark.query
def test_version_query_median_handles_odd_and_even_counts():
    cm = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    cm.put("a", 5, timestamp=100)
    cm.put("b", 1, timestamp=100)
    cm.put("c", 3, timestamp=100)

    assert versions(cm, start=100, end=100).median() == 3

    cm.put("d", 9, timestamp=100)

    assert versions(cm, start=100, end=100).median() == 4


@pytest.mark.query
def test_version_query_median_uses_filtered_rows():
    cm = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    cm.put("a", 1, timestamp=100)
    cm.put("b", 100, timestamp=100)
    cm.put("c", 3, timestamp=100)

    result = versions(cm, start=100, end=100).where_value(lambda value: value < 50)

    assert result.median() == 2


@pytest.mark.query
def test_version_query_median_rejects_an_empty_result():
    cm = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    cm.put("a", 1, timestamp=100)

    with pytest.raises(QueryError, match="Cannot compute median of an empty version query"):
        versions(cm, start=200).median()
