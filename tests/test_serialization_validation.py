# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import math

import pytest

from chronomap import ChronoMap, ChronoMapTypeError, ChronoMapValueError


@pytest.mark.core
@pytest.mark.parametrize("bad", [True, "1.0", None])
def test_from_dict_rejects_non_numeric_version_timestamps(bad):
    data = {"store": {"key": [[bad, "value"]]}, "ttl": {}}

    with pytest.raises(ChronoMapTypeError):
        ChronoMap.from_dict(data)


@pytest.mark.core
@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_from_dict_rejects_non_finite_version_timestamps(bad):
    data = {"store": {"key": [[bad, "value"]]}, "ttl": {}}

    with pytest.raises(ChronoMapValueError):
        ChronoMap.from_dict(data)


@pytest.mark.core
def test_from_dict_rejects_unsorted_history():
    data = {"store": {"key": [[2.0, "new"], [1.0, "old"]]}, "ttl": {}}

    with pytest.raises(ChronoMapValueError):
        ChronoMap.from_dict(data)


@pytest.mark.core
def test_from_dict_rejects_invalid_ttl_deadlines():
    with pytest.raises(ChronoMapTypeError):
        ChronoMap.from_dict({"store": {}, "ttl": {"key": "soon"}})
    with pytest.raises(ChronoMapValueError):
        ChronoMap.from_dict({"store": {}, "ttl": {"key": math.inf}})


@pytest.mark.core
def test_from_dict_rejects_invalid_snapshot_time():
    with pytest.raises(ChronoMapTypeError):
        ChronoMap.from_dict({"store": {}, "ttl": {}, "snapshot_time": True})
    with pytest.raises(ChronoMapValueError):
        ChronoMap.from_dict({"store": {}, "ttl": {}, "snapshot_time": math.nan})


@pytest.mark.core
def test_from_dict_normalizes_valid_numeric_metadata():
    data = {
        "store": {"key": [[1, "value"]]},
        "ttl": {"key": 2_000_000_000},
        "snapshot_time": 5,
    }

    cm = ChronoMap.from_dict(data, enable_ttl_cleanup=False)

    assert cm.history("key") == [(1.0, "value")]
    assert cm._ttl["key"] == 2_000_000_000.0
    assert cm.snapshot_time == 5.0
