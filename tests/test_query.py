# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Behavioral tests for the fluent value and version query layer."""

import time

import pytest

from chronomap import (
    ChronoMap,
    ChronoMapTypeError,
    ChronoMapValueError,
    Query,
    QueryError,
    VersionQuery,
    query,
    versions,
)


@pytest.fixture
def cm():
    store = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    store.put("alpha", 1, timestamp=100)
    store.put("alpha", 10, timestamp=300)
    store.put("beta", 2, timestamp=200)
    store.put("gamma", {"cpu": 0.5, "tags": ["x"]}, timestamp=150)
    store.put("delta", None, timestamp=120)
    store.put("future", 7, timestamp=400)
    return store


@pytest.fixture
def plain():
    return {"a": 1, "b": 2, "c": 3}


@pytest.mark.query
def test_query_mapping_source(plain):
    result = query(plain).where_value(lambda value: value > 1)

    assert result.to_dict() == {"b": 2, "c": 3}
    assert result.timestamp is None


@pytest.mark.query
def test_query_mapping_rejects_timestamp(plain):
    with pytest.raises(QueryError, match="only supported for ChronoMap"):
        Query(plain, timestamp=100)


@pytest.mark.query
def test_query_rejects_unknown_source():
    with pytest.raises(ChronoMapTypeError, match="ChronoMap or a mapping"):
        Query(["not", "a", "mapping"])


@pytest.mark.query
def test_query_accepts_iso_timestamp(cm):
    result = query(cm, timestamp="1970-01-01T00:02:05+00:00")

    assert result.to_dict() == {"alpha": 1, "delta": None}


@pytest.mark.query
def test_query_includes_stored_none(cm):
    result = query(cm, timestamp=250)

    assert "delta" in result.to_dict()
    assert result.to_dict()["delta"] is None


@pytest.mark.query
def test_query_excludes_expired_keys():
    store = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    store.put("live", 1, timestamp=100)
    store.put("gone", 2, timestamp=100, ttl=0.01)
    time.sleep(0.02)

    assert query(store).keys() == ["live"]


@pytest.mark.query
def test_query_where_and_filter_alias(cm):
    result = query(cm, timestamp=250)

    assert result.where(lambda key, value: key.startswith("a")).keys() == ["alpha"]
    assert result.filter(lambda key, value: value is None).keys() == ["delta"]


@pytest.mark.query
def test_query_where_key_and_value(cm):
    result = query(cm, timestamp=250)

    assert result.where_key(lambda key: len(key) == 4).keys() == ["beta"]
    assert result.where_value(lambda value: isinstance(value, int)).keys() == ["alpha", "beta"]


@pytest.mark.query
def test_query_where_field_skips_non_mappings_and_missing_fields(cm):
    result = query(cm, timestamp=250)

    assert result.where_field("cpu", lambda value: value >= 0.5).keys() == ["gamma"]
    assert result.where_field("missing", lambda value: True).keys() == []
    assert result.where_field("cpu", lambda value: value < 0.5).keys() == []


@pytest.mark.query
def test_query_keys_like_matches_stringified_keys(cm):
    result = query(cm, timestamp=250)

    assert result.keys_like("a*").keys() == ["alpha"]
    assert result.keys_like("?????").keys() == ["alpha", "gamma", "delta"]


@pytest.mark.query
def test_query_keys_regex_matches_stringified_keys(cm):
    result = query(cm, timestamp=250)

    assert result.keys_regex("^(al|be)").keys() == ["alpha", "beta"]


@pytest.mark.query
def test_query_pattern_validation():
    with pytest.raises(ChronoMapTypeError, match="pattern must be a string"):
        query({"a": 1}).keys_like(1)
    with pytest.raises(ChronoMapTypeError, match="pattern must be a string"):
        query({"a": 1}).keys_regex(1)
    with pytest.raises(QueryError, match="Invalid regular expression"):
        query({"a": 1}).keys_regex("[")


@pytest.mark.query
def test_query_keys_in_and_values_in(cm):
    result = query(cm, timestamp=250)

    assert result.keys_in(["alpha", "delta"]).keys() == ["alpha", "delta"]
    assert result.values_in([2, None]).keys() == ["beta", "delta"]
    assert result.values_in([[1, 2], [3]]).keys() == []


@pytest.mark.query
def test_query_keys_in_rejects_unhashable_keys(plain):
    with pytest.raises(ChronoMapTypeError, match="Key must be hashable"):
        query(plain).keys_in([["not", "hashable"]])


@pytest.mark.query
def test_query_values_between_inclusive_and_exclusive(cm):
    result = query(cm, timestamp=250)

    assert result.values_between(1, 2).keys() == ["alpha", "beta"]
    assert result.values_between(1, 2, inclusive=False).keys() == []


@pytest.mark.query
def test_query_select_field(cm):
    result = query(cm, timestamp=250).select_field("cpu")

    assert result.to_dict() == {"gamma": 0.5}


@pytest.mark.query
def test_query_transform(cm):
    result = query(cm, timestamp=250).where_key(lambda key: key == "alpha")

    assert result.transform(lambda key, value: value * 2).to_dict() == {"alpha": 2}


@pytest.mark.query
def test_query_sort_by_key(cm):
    result = query(cm, timestamp=250)

    assert result.sort_by_key().keys() == ["alpha", "beta", "delta", "gamma"]
    assert result.sort_by_key(str, reverse=True).keys() == ["gamma", "delta", "beta", "alpha"]
    assert result.sort_by_key(lambda key: len(key)).keys() == ["beta", "alpha", "gamma", "delta"]


@pytest.mark.query
def test_query_sort_by_value(cm):
    result = query(cm, timestamp=250).where_value(lambda value: isinstance(value, int))

    assert result.sort_by_value().values() == [1, 2]
    assert result.sort_by_value(lambda value: -value).values() == [2, 1]
    assert result.sort_by_value(reverse=True).values() == [2, 1]


@pytest.mark.query
def test_query_sort_by_pair(cm):
    result = query(cm, timestamp=250).where_value(lambda value: isinstance(value, int))

    assert result.sort_by(lambda key, value: (value, key)).keys() == ["alpha", "beta"]


@pytest.mark.query
def test_query_limit_and_offset(cm):
    result = query(cm, timestamp=250).sort_by_key()

    assert result.limit(2).keys() == ["alpha", "beta"]
    assert result.offset(2).keys() == ["delta", "gamma"]
    assert result.offset(2).limit(1).keys() == ["delta"]


@pytest.mark.query
def test_query_distinct_supports_unhashable_values():
    result = query({"a": [1], "b": [1], "c": [2]}).distinct()

    assert result.keys() == ["a", "c"]


@pytest.mark.query
def test_query_terminal_accessors(cm):
    result = query(cm, timestamp=250)

    assert result.items() == [
        ("alpha", 1),
        ("beta", 2),
        ("gamma", {"cpu": 0.5, "tags": ["x"]}),
        ("delta", None),
    ]
    assert result.keys() == ["alpha", "beta", "gamma", "delta"]
    assert result.values() == [1, 2, {"cpu": 0.5, "tags": ["x"]}, None]
    assert result.count() == 4
    assert len(result) == 4


@pytest.mark.query
def test_query_first_and_first_item(cm):
    result = query(cm, timestamp=250)

    assert result.first() == 1
    assert result.first_item() == ("alpha", 1)
    assert result.where_key(lambda key: key == "missing").first("fallback") == "fallback"
    assert result.where_key(lambda key: key == "missing").first_item(("x", "y")) == ("x", "y")
    with pytest.raises(QueryError, match="Query is empty"):
        result.where_key(lambda key: key == "missing").first()
    with pytest.raises(QueryError, match="Query is empty"):
        result.where_key(lambda key: key == "missing").first_item()


@pytest.mark.query
def test_query_any_and_all(cm):
    result = query(cm, timestamp=250)

    assert result.any()
    assert not result.where_key(lambda key: key == "missing").any()
    assert result.any(lambda key, value: value == 2)
    assert not result.any(lambda key, value: value == 99)
    assert result.all(lambda key, value: key != "omega")
    assert not result.all(lambda key, value: value is not None)
    assert result.where_key(lambda key: key == "missing").all(lambda key, value: False)


@pytest.mark.query
def test_query_fold(cm):
    result = query(cm, timestamp=250).where_value(lambda value: isinstance(value, int))

    assert result.fold(lambda total, value: total + value, 0) == 3


@pytest.mark.query
def test_query_group_by_and_group_items(cm):
    result = query(cm, timestamp=250)

    assert result.group_by(lambda key, value: len(key))[5] == [
        1,
        {"cpu": 0.5, "tags": ["x"]},
        None,
    ]
    assert result.group_items(lambda key, value: key[0])["a"] == [("alpha", 1)]


@pytest.mark.query
def test_query_numeric_aggregates(cm):
    result = query(cm, timestamp=250).where_value(lambda value: isinstance(value, int))

    assert result.sum() == 3
    assert result.mean() == 1.5
    assert result.median() == 1.5
    assert result.min() == 1
    assert result.max() == 2


@pytest.mark.query
def test_query_numeric_aggregate_empty_errors(cm):
    empty = query(cm, timestamp=250).where_key(lambda key: key == "missing")

    assert empty.sum() == 0
    with pytest.raises(QueryError, match="mean of an empty"):
        empty.mean()
    with pytest.raises(QueryError, match="median of an empty"):
        empty.median()
    with pytest.raises(QueryError, match="min of an empty"):
        empty.min()
    with pytest.raises(QueryError, match="max of an empty"):
        empty.max()


@pytest.mark.query
def test_query_iteration(cm):
    result = query(cm, timestamp=250).where_key(lambda key: key == "alpha")

    assert list(result) == [("alpha", 1)]


@pytest.mark.query
def test_query_at_moves_timestamp_and_keeps_filters(cm):
    result = query(cm, timestamp=250).where_key(lambda key: key == "alpha")

    assert result.to_dict() == {"alpha": 1}
    assert result.at(350).to_dict() == {"alpha": 10}


@pytest.mark.query
def test_query_at_rejects_plain_mapping(plain):
    with pytest.raises(QueryError, match="at\\(\\) requires a ChronoMap"):
        query(plain).at(1)


@pytest.mark.query
def test_query_created_without_timestamp_is_fixed_at_creation():
    store = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    store.put("key", "first", timestamp=100)
    result = query(store)
    store.put("key", "second", timestamp=50)

    assert result.to_dict() == {"key": "first"}


@pytest.mark.query
def test_query_callable_validation(cm):
    result = query(cm)

    with pytest.raises(ChronoMapTypeError, match="predicate must be callable"):
        result.where(None)
    with pytest.raises(ChronoMapTypeError, match="predicate must be callable"):
        result.where_key(None)
    with pytest.raises(ChronoMapTypeError, match="predicate must be callable"):
        result.where_value(None)
    with pytest.raises(ChronoMapTypeError, match="predicate must be callable"):
        result.where_field("field", None)
    with pytest.raises(ChronoMapTypeError, match="func must be callable"):
        result.transform(None)
    with pytest.raises(ChronoMapTypeError, match="func must be callable"):
        result.sort_by(None)
    with pytest.raises(ChronoMapTypeError, match="key must be callable"):
        result.sort_by_key(1)
    with pytest.raises(ChronoMapTypeError, match="key must be callable"):
        result.sort_by_value(1)
    with pytest.raises(ChronoMapTypeError, match="predicate must be callable"):
        result.any(1)
    with pytest.raises(ChronoMapTypeError, match="predicate must be callable"):
        result.all(1)
    with pytest.raises(ChronoMapTypeError, match="func must be callable"):
        result.fold(None, 0)
    with pytest.raises(ChronoMapTypeError, match="key_func must be callable"):
        result.group_by(None)
    with pytest.raises(ChronoMapTypeError, match="key_func must be callable"):
        result.group_items(None)


@pytest.mark.query
def test_query_limit_and_offset_validation(cm):
    result = query(cm)

    for bad in (-1, 1.5, "2", True):
        with pytest.raises(ChronoMapValueError, match="non-negative integer"):
            result.limit(bad)
        with pytest.raises(ChronoMapValueError, match="non-negative integer"):
            result.offset(bad)


@pytest.mark.query
def test_query_error_is_a_value_error():
    assert issubclass(QueryError, ChronoMapValueError)


@pytest.mark.query
def test_versions_requires_chronomap():
    with pytest.raises(ChronoMapTypeError, match="VersionQuery source must be a ChronoMap"):
        VersionQuery({"a": 1})


@pytest.mark.query
def test_versions_filters_by_range_and_keys(cm):
    result = versions(cm, start=150, end=300, keys=["alpha"])

    assert result.rows() == [("alpha", 300.0, 10)]
    assert result.start == 150
    assert result.end == 300


@pytest.mark.query
def test_versions_without_bounds_includes_future_rows(cm):
    result = versions(cm)

    assert ("future", 400.0, 7) in result.rows()
    assert result.count() == 6


@pytest.mark.query
def test_versions_rejects_invalid_key(cm):
    with pytest.raises(ChronoMapTypeError, match="Key must be hashable"):
        versions(cm, keys=[["not", "hashable"]])


@pytest.mark.query
def test_versions_excludes_expired_keys():
    store = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    store.put("live", 1, timestamp=100)
    store.put("gone", 2, timestamp=100, ttl=0.01)
    time.sleep(0.02)

    assert versions(store).rows() == [("live", 100.0, 1)]


@pytest.mark.query
def test_versions_where_variants(cm):
    result = versions(cm, start=100, end=300)

    assert result.where(lambda key, timestamp, value: timestamp == 200).rows() == [("beta", 200.0, 2)]
    assert result.where_key(lambda key: key == "delta").count() == 1
    assert result.where_value(lambda value: value == 10).rows() == [("alpha", 300.0, 10)]
    assert result.where_timestamp(lambda timestamp: timestamp >= 250).rows() == [("alpha", 300.0, 10)]


@pytest.mark.query
def test_versions_between(cm):
    result = versions(cm).between(150, 250)

    assert result.rows() == [
        ("gamma", 150.0, {"cpu": 0.5, "tags": ["x"]}),
        ("beta", 200.0, 2),
    ]
    assert result.start == 150
    assert result.end == 250


@pytest.mark.query
def test_versions_between_empty_when_reversed(cm):
    assert versions(cm).between(300, 150).rows() == []


@pytest.mark.query
def test_versions_keys_in_and_values_in(cm):
    result = versions(cm, start=100, end=300)

    assert result.keys_in(["alpha", "beta"]).count() == 3
    assert result.values_in([10]).rows() == [("alpha", 300.0, 10)]
    assert result.values_in([[1]]).rows() == []


@pytest.mark.query
def test_versions_keys_in_rejects_invalid_key(cm):
    with pytest.raises(ChronoMapTypeError, match="Key must be hashable"):
        versions(cm).keys_in([["not", "hashable"]])


@pytest.mark.query
def test_versions_sorting(cm):
    result = versions(cm, start=100, end=300)

    assert result.sort_by_timestamp().rows()[0] == ("alpha", 100.0, 1)
    assert result.sort_by_timestamp(reverse=True).rows()[0] == ("alpha", 300.0, 10)
    assert result.sort_by_key(str).keys() == ["alpha", "beta", "delta", "gamma"]
    assert result.sort_by_key(str, reverse=True).keys() == ["gamma", "delta", "beta", "alpha"]
    assert result.sort_by_value(lambda value: value if isinstance(value, int) else 0).count() == 5


@pytest.mark.query
def test_versions_limit_offset_and_iteration(cm):
    result = versions(cm, start=100, end=300).sort_by_timestamp()

    assert result.limit(2).rows() == [
        ("alpha", 100.0, 1),
        ("delta", 120.0, None),
    ]
    assert result.offset(4).count() == 1
    assert len(result) == 5
    assert list(result)[0] == ("alpha", 100.0, 1)


@pytest.mark.query
def test_versions_limit_offset_validation(cm):
    result = versions(cm)

    for bad in (-1, 1.5, "2", True):
        with pytest.raises(ChronoMapValueError, match="non-negative integer"):
            result.limit(bad)
        with pytest.raises(ChronoMapValueError, match="non-negative integer"):
            result.offset(bad)


@pytest.mark.query
def test_versions_accessors_and_records(cm):
    result = versions(cm, start=100, end=200).sort_by_timestamp()

    assert result.keys() == ["alpha", "delta", "gamma", "beta"]
    assert result.values() == [1, None, {"cpu": 0.5, "tags": ["x"]}, 2]
    assert result.count() == 4
    records = result.to_records()
    assert records[0]["key"] == "alpha"
    assert records[0]["timestamp"] == 100.0
    assert records[0]["datetime"].year == 1970
    assert records[0]["value"] == 1


@pytest.mark.query
def test_versions_latest_and_earliest_per_key(cm):
    result = versions(cm, start=100, end=300)

    assert result.latest_per_key() == {
        "alpha": 10,
        "beta": 2,
        "gamma": {"cpu": 0.5, "tags": ["x"]},
        "delta": None,
    }
    assert result.earliest_per_key() == {
        "alpha": 1,
        "beta": 2,
        "gamma": {"cpu": 0.5, "tags": ["x"]},
        "delta": None,
    }


@pytest.mark.query
def test_versions_latest_prefers_last_row_on_tied_timestamp(cm):
    result = versions(cm).where_timestamp(lambda timestamp: timestamp == 100)

    assert result.latest_per_key() == {"alpha": 1}
    assert result.earliest_per_key() == {"alpha": 1}


@pytest.mark.query
def test_versions_group_by_key(cm):
    groups = versions(cm, start=100, end=300).group_by_key()

    assert groups["alpha"] == [(100.0, 1), (300.0, 10)]
    assert groups["beta"] == [(200.0, 2)]


@pytest.mark.query
def test_versions_to_chronomap(cm):
    selected = versions(cm, start=100, end=300).where_key(lambda key: key == "alpha")
    rebuilt = selected.to_chronomap(max_history=1)

    assert rebuilt.history("alpha") == [(300.0, 10)]
    assert list(rebuilt.keys()) == ["alpha"]


@pytest.mark.query
def test_versions_first_and_last(cm):
    result = versions(cm, start=100, end=200).sort_by_timestamp()

    assert result.first() == 1
    assert result.last() == 2
    empty = versions(cm, keys=["missing"])
    assert empty.first("fallback") == "fallback"
    assert empty.last("fallback") == "fallback"
    with pytest.raises(QueryError, match="Version query is empty"):
        empty.first()
    with pytest.raises(QueryError, match="Version query is empty"):
        empty.last()


@pytest.mark.query
def test_versions_fold_and_numeric_aggregates(cm):
    result = versions(cm, start=100, end=300).where_value(lambda value: isinstance(value, int))

    assert result.fold(lambda total, value: total + value, 0) == 13
    assert result.sum() == 13
    assert result.mean() == 13 / 3
    assert result.min() == 1
    assert result.max() == 10


@pytest.mark.query
def test_versions_empty_numeric_errors(cm):
    empty = versions(cm, keys=["missing"])

    assert empty.sum() == 0
    with pytest.raises(QueryError, match="mean of an empty version query"):
        empty.mean()
    with pytest.raises(QueryError, match="min of an empty version query"):
        empty.min()
    with pytest.raises(QueryError, match="max of an empty version query"):
        empty.max()


@pytest.mark.query
def test_versions_callable_validation(cm):
    result = versions(cm)

    with pytest.raises(ChronoMapTypeError, match="predicate must be callable"):
        result.where(None)
    with pytest.raises(ChronoMapTypeError, match="predicate must be callable"):
        result.where_key(None)
    with pytest.raises(ChronoMapTypeError, match="predicate must be callable"):
        result.where_value(None)
    with pytest.raises(ChronoMapTypeError, match="predicate must be callable"):
        result.where_timestamp(None)
    with pytest.raises(ChronoMapTypeError, match="key must be callable"):
        result.sort_by_key(1)
    with pytest.raises(ChronoMapTypeError, match="key must be callable"):
        result.sort_by_value(1)
    with pytest.raises(ChronoMapTypeError, match="func must be callable"):
        result.fold(None, 0)


@pytest.mark.query
def test_query_and_versions_are_exported():
    assert Query is not None
    assert VersionQuery is not None
    assert query({"a": 1}).count() == 1
    assert versions(ChronoMap(cache_size=0, enable_ttl_cleanup=False)).count() == 0
