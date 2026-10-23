# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import time

import pytest

from chronomap import ChronoMap, ChronoMapKeyError


@pytest.mark.core
def test_popitem_removes_the_most_recent_key():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("first", 1)
    cm.put("second", 2)

    assert cm.popitem() == ("second", 2)
    assert cm.popitem() == ("first", 1)
    assert len(cm) == 0


@pytest.mark.core
def test_popitem_skips_and_purges_expired_keys():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("live", 1)
    cm.put("expired", 2, ttl=0.01)
    time.sleep(0.02)

    assert cm.popitem() == ("live", 1)
    assert "expired" not in cm._store
    assert "expired" not in cm._ttl


@pytest.mark.core
def test_popitem_returns_a_stored_none_value():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("key", None)

    key, value = cm.popitem()

    assert key == "key"
    assert value is None


@pytest.mark.core
def test_popitem_raises_on_an_empty_map():
    cm = ChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapKeyError):
        cm.popitem()
