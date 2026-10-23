# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import math

import pytest

from chronomap import AsyncChronoMap, ChronoMapTypeError, ChronoMapValueError

pytestmark = pytest.mark.async_map


@pytest.mark.parametrize("bad", [True, "1.0", None])
def test_async_from_dict_rejects_non_numeric_version_timestamps(bad):
    data = {"store": {"key": [[bad, "value"]]}, "ttl": {}}

    with pytest.raises(ChronoMapTypeError):
        AsyncChronoMap.from_dict(data)


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_async_from_dict_rejects_non_finite_version_timestamps(bad):
    data = {"store": {"key": [[bad, "value"]]}, "ttl": {}}

    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap.from_dict(data)


def test_async_from_dict_rejects_unsorted_history():
    data = {"store": {"key": [[2.0, "new"], [1.0, "old"]]}, "ttl": {}}

    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap.from_dict(data)


def test_async_from_dict_rejects_invalid_ttl_deadlines():
    with pytest.raises(ChronoMapTypeError):
        AsyncChronoMap.from_dict({"store": {}, "ttl": {"key": "soon"}})
    with pytest.raises(ChronoMapTypeError):
        AsyncChronoMap.from_dict({"store": {}, "ttl": {"key": True}})
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap.from_dict({"store": {}, "ttl": {"key": math.inf}})


def test_async_from_dict_rejects_invalid_snapshot_time():
    with pytest.raises(ChronoMapTypeError):
        AsyncChronoMap.from_dict({"store": {}, "ttl": {}, "snapshot_time": True})
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap.from_dict({"store": {}, "ttl": {}, "snapshot_time": math.nan})


def test_async_from_dict_normalizes_valid_numeric_metadata():
    data = {
        "store": {"key": [[1, "value"]]},
        "ttl": {"key": 2_000_000_000},
        "snapshot_time": 5,
    }

    cm = AsyncChronoMap.from_dict(data)

    assert cm._store["key"] == [(1.0, "value")]
    assert cm._ttl["key"] == 2_000_000_000.0
    assert cm.snapshot_time == 5.0
