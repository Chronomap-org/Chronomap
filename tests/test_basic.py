# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Smoke tests for the package split.

This is not the full test suite yet — the original repo mentions ~140
tests that weren't in what I had to work with when I split this file up,
so these are new. Whoever picks up the original tests just needs to drop
`test_chronomap.py` in here; since the public API (`from chronomap import
ChronoMap`) didn't change, it should run against this package unmodified.
"""

import asyncio
import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap, ChronoMapKeyError


@pytest.mark.core
def test_put_get_roundtrip():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm["a"] = 1
    assert cm["a"] == 1


@pytest.mark.core
def test_history_keeps_all_versions():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("a", 1, timestamp=1.0)
    cm.put("a", 2, timestamp=2.0)
    assert cm.history("a") == [(1.0, 1), (2.0, 2)]


@pytest.mark.core
def test_time_travel():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("a", "old", timestamp=1.0)
    cm.put("a", "new", timestamp=2.0)
    assert cm.get("a", timestamp=1.5) == "old"
    assert cm.get("a", timestamp=2.5) == "new"


@pytest.mark.core
def test_strict_get_raises():
    cm = ChronoMap(enable_ttl_cleanup=False)
    with pytest.raises(ChronoMapKeyError):
        cm.get("missing", strict=True)


@pytest.mark.snapshot
def test_snapshot_and_rollback():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm["a"] = 1
    snap = cm.snapshot()
    cm["a"] = 2
    cm.rollback(snap)
    assert cm["a"] == 1


@pytest.mark.snapshot
def test_snapshot_context_rolls_back_on_exception():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm["a"] = 1
    with pytest.raises(ValueError):
        with cm.snapshot_context():
            cm["a"] = 2
            raise ValueError("boom")
    assert cm["a"] == 1


@pytest.mark.core
def test_merge_timestamp_strategy_does_not_crash_on_out_of_order_writes():
    """Regression test for the merge() bug found during the package split:
    the out-of-order insert branch referenced an undefined `value` and
    mutated the wrong list. This exercises exactly that branch.
    """
    a = ChronoMap(enable_ttl_cleanup=False)
    b = ChronoMap(enable_ttl_cleanup=False)

    a.put("k", "first", timestamp=1.0)
    a.put("k", "third", timestamp=3.0)
    b.put("k", "second", timestamp=2.0)  # lands between a's two versions

    a.merge(b, strategy="timestamp")

    assert a.history("k") == [(1.0, "first"), (2.0, "second"), (3.0, "third")]


@pytest.mark.core
def test_auto_prune_respects_max_history():
    cm = ChronoMap(max_history=3, enable_ttl_cleanup=False)
    for i in range(10):
        cm.put("k", i, timestamp=float(i))
    assert len(cm.history("k")) == 3
    assert cm.history("k")[-1] == (9.0, 9)


@pytest.mark.ttl
def test_ttl_expiry():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("session", "token", ttl=0.05)
    assert cm["session"] == "token"
    time.sleep(0.1)
    assert "session" not in cm


@pytest.mark.core
def test_query_and_aggregate():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put_many({"a": 10, "b": 20, "c": 30})
    assert cm.count(lambda k, v: v >= 20) == 2
    assert cm.aggregate(sum) == 60


@pytest.mark.core
def test_get_or_set_only_calls_factory_once():
    cm = ChronoMap(enable_ttl_cleanup=False)
    calls = []

    def factory():
        calls.append(1)
        return "created"

    assert cm.get_or_set("k", factory) == "created"
    assert cm.get_or_set("k", factory) == "created"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_async_put_get_roundtrip():
    cm = AsyncChronoMap()
    await cm.put("a", 1)
    assert await cm.get("a") == 1
