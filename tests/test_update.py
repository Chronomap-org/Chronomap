# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import pytest

from chronomap import ChronoMap, ChronoMapTypeError


@pytest.mark.core
def test_update_accepts_a_mapping_and_forwards_timestamp_and_ttl():
    cm = ChronoMap(enable_ttl_cleanup=False)

    cm.update({"a": 1, "b": 2}, timestamp=100, ttl=60)

    assert cm.get("a", timestamp=100) == 1
    assert cm.get("b", timestamp=100) == 2
    assert cm.history("a") == [(100.0, 1)]
    assert "a" in cm._ttl
    assert "b" in cm._ttl


@pytest.mark.core
def test_update_accepts_an_iterable_of_pairs_and_keeps_the_last_value():
    cm = ChronoMap(enable_ttl_cleanup=False)

    cm.update([("a", 1), ("b", 2), ("a", 3)], timestamp=10)

    assert cm["a"] == 3
    assert cm["b"] == 2


@pytest.mark.core
@pytest.mark.parametrize("bad", [42, None, ["not-a-pair"]])
def test_update_rejects_invalid_inputs(bad):
    cm = ChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapTypeError):
        cm.update(bad)


@pytest.mark.core
def test_update_rejects_unhashable_keys():
    cm = ChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapTypeError):
        cm.update([(["not", "hashable"], 1)])
