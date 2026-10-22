# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Parity and lifecycle tests for AsyncChronoMap.

The synchronous suite already covers the shared semantics; this file pins
the asyncio-specific parts: awaited locks, task-owned TTL sweeps, async
callbacks, async snapshot rollback, and non-blocking file I/O.
"""

import asyncio
import math
import pickle
import sys
import weakref
import zlib
from collections.abc import Mapping
from datetime import datetime

import pytest

from chronomap import (
    AsyncChronoMap,
    AsyncSnapshotContext,
    ChronoMapKeyError,
    ChronoMapTypeError,
    ChronoMapValueError,
)
from chronomap.asynchronous import _async_ttl_cleanup_loop


async def _expired_cache(size=10):
    cm = AsyncChronoMap(cache_size=size)
    await cm.put("k", "value", ttl=0.02)
    await asyncio.sleep(0.05)
    return cm


# ---------------------------------------------------------------------------
# Construction and validation
# ---------------------------------------------------------------------------


@pytest.mark.async_map
def test_async_constructor_rejects_invalid_limits():
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap(max_history=0)
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap(max_history=True)
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap(cache_size=-1)
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap(cache_size=True)
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap(ttl_cleanup_interval=0)
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap(ttl_cleanup_interval=math.inf)
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap(ttl_cleanup_interval=True)


@pytest.mark.async_map
def test_async_start_cleanup_outside_event_loop_is_a_noop():
    cm = AsyncChronoMap(enable_ttl_cleanup=True)
    cm.start_cleanup()
    assert cm._cleanup_task is None
    cm.stop_cleanup()


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_debug_mode_configures_logging():
    cm = AsyncChronoMap(debug=True)
    await cm.put("k", "v")
    assert await cm.get("k") == "v"
    await cm.aclose()


# ---------------------------------------------------------------------------
# Writes, reads, and timestamps
# ---------------------------------------------------------------------------


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_put_get_roundtrip_and_history_order():
    cm = AsyncChronoMap()
    await cm.put("k", "second", timestamp=200)
    await cm.put("k", "first", timestamp=100)
    assert await cm.history("k") == [(100, "first"), (200, "second")]
    assert await cm.get("k", timestamp=150) == "first"
    assert await cm.get("k", timestamp=250) == "second"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_timestamp_forms_are_accepted():
    cm = AsyncChronoMap()
    await cm.put("iso", "a", timestamp="2026-01-01T00:00:00")
    await cm.put("dt", "b", timestamp=datetime(2026, 1, 1))
    assert await cm.get("iso") == "a"
    assert await cm.get("dt") == "b"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_boolean_and_non_finite_timestamps_are_rejected():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapTypeError):
        await cm.put("k", "v", timestamp=True)
    with pytest.raises(ChronoMapTypeError):
        await cm.get("k", timestamp=False)
    with pytest.raises(ChronoMapValueError):
        await cm.put("k", "v", timestamp=math.inf)
    with pytest.raises(ChronoMapValueError):
        await cm.get("k", timestamp=math.nan)


@pytest.mark.async_map
def test_async_numeric_timestamp_validator_rejects_non_numeric_values():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm._validate_timestamp("2026-01-01")


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_get_defaults_and_strict_mode():
    cm = AsyncChronoMap()
    assert await cm.get("missing", default="fallback") == "fallback"
    with pytest.raises(ChronoMapKeyError):
        await cm.get("missing", strict=True)
    await cm.put("k", "v", timestamp=100)
    with pytest.raises(ChronoMapKeyError):
        await cm.get("k", timestamp=50, strict=True)
    assert await cm.get("k", timestamp=50, default="fallback") == "fallback"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_unhashable_key_is_rejected():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapTypeError):
        await cm.put(["bad"], "v")
    with pytest.raises(ChronoMapTypeError):
        await cm.get(["bad"])
    with pytest.raises(ChronoMapTypeError):
        await cm.contains(["bad"])


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_cache_hit_and_invalidation():
    cm = AsyncChronoMap(cache_size=10)
    await cm.put("k", "one")
    assert await cm.get("k") == "one"
    assert await cm.get("k") == "one"
    assert cm.get_stats()["cache_hits"] >= 1

    await cm.put("k", "two")
    assert await cm.get("k") == "two"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_cache_can_be_disabled():
    cm = AsyncChronoMap(cache_size=0)
    await cm.put("k", "v")
    assert await cm.get("k") == "v"
    assert "cache_size" not in cm.get_stats()


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_max_history_prunes_old_versions():
    cm = AsyncChronoMap(max_history=3)
    for ts in range(10):
        await cm.put("k", ts, timestamp=ts)
    assert await cm.history("k") == [(7, 7), (8, 8), (9, 9)]
    assert cm.get_stats()["auto_prunes"] > 0


# ---------------------------------------------------------------------------
# TTL and expiry
# ---------------------------------------------------------------------------


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_ttl_expires_and_purges_on_read():
    cm = AsyncChronoMap()
    await cm.put("k", "v", ttl=0.02)
    assert await cm.get("k") == "v"
    await asyncio.sleep(0.05)
    assert await cm.get("k") is None
    assert await cm.exists("k") is False


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_put_on_expired_key_starts_clean():
    cm = AsyncChronoMap()
    await cm.put("k", "old", ttl=0.02)
    await asyncio.sleep(0.05)
    await cm.put("k", "new")
    assert await cm.get("k") == "new"
    assert [value for _, value in await cm.history("k")] == ["new"]


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_rejected_ttl_does_not_write():
    cm = AsyncChronoMap()
    for bad in (0, -1, math.inf, math.nan, True, "60"):
        with pytest.raises((ChronoMapValueError, ChronoMapTypeError)):
            await cm.put("k", "v", ttl=bad)
    assert await cm.keys() == []


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_put_many_rejects_bad_ttl_before_writing():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapValueError):
        await cm.put_many({"a": 1, "b": 2}, ttl=0)
    assert await cm.keys() == []


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_put_many_validates_keys_before_writing():
    class BadItems(Mapping):
        def keys(self):
            return ["a", ["bad"]]

        def __getitem__(self, key):
            return key

        def __iter__(self):
            return iter(self.keys())

        def __len__(self):
            return len(self.keys())

    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapTypeError):
        await cm.put_many(BadItems())
    assert await cm.keys() == []


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_put_many_keeps_sorted_history_and_applies_ttl():
    cm = AsyncChronoMap()
    await cm.put_many({"k": "new"}, timestamp=20)
    await cm.put_many({"k": "old"}, timestamp=10)
    await cm.put_many({"k": "last"}, timestamp=30, ttl=10)

    assert await cm.history("k") == [(10, "old"), (20, "new"), (30, "last")]
    assert cm._ttl["k"] > cm._current_time()


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_ttl_cleanup_task_sweeps_in_background():
    cm = AsyncChronoMap(enable_ttl_cleanup=True, ttl_cleanup_interval=0.01)
    await cm.put("k", "v", ttl=0.02)
    await asyncio.sleep(0.1)
    assert cm.cleaned_count >= 1
    assert "k" not in cm._ttl
    await cm.aclose()


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_start_cleanup_is_idempotent_and_can_restart():
    cm = AsyncChronoMap(ttl_cleanup_interval=0.01)
    cm.start_cleanup()
    first = cm._cleanup_task
    cm.start_cleanup()
    assert cm._cleanup_task is first

    first.cancel()
    await asyncio.sleep(0)
    cm.start_cleanup()
    assert cm._cleanup_task is not first
    await cm.aclose()


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_stop_cleanup_cancels_task_and_aclose_is_safe():
    cm = AsyncChronoMap()
    cm.start_cleanup()
    task = cm._cleanup_task
    cm.stop_cleanup()
    await asyncio.sleep(0)
    assert task is not None and task.cancelled()
    cm.stop_cleanup()
    await cm.aclose()


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_context_manager_closes_cleanup_task():
    async with AsyncChronoMap(enable_ttl_cleanup=True, ttl_cleanup_interval=0.01) as cm:
        await cm.put("k", "v")
        task = cm._cleanup_task
        assert task is not None
    assert task.cancelled()


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_cleanup_loop_survives_an_exception(monkeypatch, caplog):
    cm = AsyncChronoMap(ttl_cleanup_interval=0.01)
    calls = []

    async def boom():
        calls.append(1)
        raise RuntimeError("boom")

    monkeypatch.setattr(cm, "clean_expired_keys", boom)
    cm.start_cleanup()
    await asyncio.sleep(0.05)
    assert calls
    await cm.aclose()


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_cleanup_loop_propagates_cancellation(monkeypatch):
    cm = AsyncChronoMap(ttl_cleanup_interval=0.01)

    async def cancel():
        raise asyncio.CancelledError

    monkeypatch.setattr(cm, "clean_expired_keys", cancel)
    cm.start_cleanup()
    await asyncio.sleep(0.05)

    assert cm._cleanup_task.cancelled()
    await cm.aclose()


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_cleanup_loop_exits_when_map_is_gone():
    class Dummy:
        pass

    obj = Dummy()
    ref = weakref.ref(obj)
    del obj

    task = asyncio.create_task(_async_ttl_cleanup_loop(ref, 0.01))
    await asyncio.sleep(0.05)
    assert task.done()
    assert task.exception() is None


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_clean_expired_keys_counts_and_clears_cache():
    cm = AsyncChronoMap(cache_size=10)
    await cm.put("dead", 1, ttl=0.02)
    await cm.put("live", 2)
    assert await cm.get("dead") == 1
    await asyncio.sleep(0.05)

    assert await cm.clean_expired_keys() == 1
    assert cm.cleaned_count == 1
    assert await cm.clean_expired_keys() == 0
    assert "dead" not in cm._store


# ---------------------------------------------------------------------------
# dict helpers and batch reads
# ---------------------------------------------------------------------------


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_get_many_omits_missing_and_supports_strict():
    cm = AsyncChronoMap()
    await cm.put("a", 1, timestamp=10)
    await cm.put("b", 2, timestamp=20)
    assert await cm.get_many(["a", "b"], timestamp=15) == {"a": 1}
    assert await cm.get_many(["a", "missing"], strict=False) == {"a": 1}
    with pytest.raises(ChronoMapKeyError):
        await cm.get_many(["missing"], strict=True)


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_exists_distinguishes_none_from_missing():
    cm = AsyncChronoMap()
    await cm.put("none", None, timestamp=10)
    assert await cm.exists("none", timestamp=10) is True
    assert await cm.exists("none", timestamp=5) is False
    assert await cm.exists("missing") is False


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_get_or_set_sync_and_async_factories():
    cm = AsyncChronoMap()
    calls = []

    async def factory():
        calls.append(1)
        return "created"

    assert await cm.get_or_set("k", factory) == "created"
    assert await cm.get_or_set("k", factory) == "created"
    assert calls == [1]
    assert await cm.get_or_default("plain", "fallback") == "fallback"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_get_or_set_async_factory_can_use_the_map():
    cm = AsyncChronoMap()

    async def factory():
        await cm.put("other", "value")
        return "created"

    assert await cm.get_or_set("k", factory) == "created"
    assert await cm.get("other") == "value"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_get_or_set_retries_when_another_task_wins():
    cm = AsyncChronoMap()
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_factory():
        started.set()
        await release.wait()
        return "slow"

    pending = asyncio.create_task(cm.get_or_set("k", slow_factory))
    await started.wait()
    assert await cm.get_or_set("k", lambda: "fast") == "fast"
    release.set()
    assert await pending == "fast"
    assert len(await cm.history("k")) == 1


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_get_or_set_validation_and_expired_replacement():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapTypeError):
        await cm.get_or_set("k", "not callable")
    with pytest.raises(ChronoMapValueError):
        await cm.get_or_set("k", lambda: "v", ttl=0)
    with pytest.raises(ChronoMapValueError):
        await cm.get_or_default("k", "v", ttl=-1)

    await cm.put("k", "old", ttl=0.02)
    await asyncio.sleep(0.05)
    assert await cm.get_or_set("k", lambda: "new") == "new"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_get_or_set_applies_ttl():
    cm = AsyncChronoMap()
    await cm.get_or_set("k", lambda: "v", ttl=0.02)
    assert await cm.get("k") == "v"
    await asyncio.sleep(0.05)
    assert await cm.get("k") is None


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_setdefault_existing_new_and_ttl():
    cm = AsyncChronoMap()
    await cm.put("existing", "original")
    assert await cm.setdefault("existing", "ignored") == "original"
    assert await cm.setdefault("new", "value") == "value"

    await cm.setdefault("temp", "v", ttl=0.02)
    await asyncio.sleep(0.05)
    assert await cm.get("temp") is None


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_setdefault_replaces_expired_value():
    cm = await _expired_cache()
    assert await cm.setdefault("k", "fresh") == "fresh"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_setdefault_validation():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapValueError):
        await cm.setdefault("k", "v", ttl=0)
    with pytest.raises(ChronoMapTypeError):
        await cm.setdefault(["bad"], "v")


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_pop_and_delete_semantics():
    cm = AsyncChronoMap()
    await cm.put("k", None)
    assert await cm.pop("k") is None
    assert await cm.pop("missing", "fallback") == "fallback"
    with pytest.raises(ChronoMapKeyError):
        await cm.pop("missing")

    await cm.put("live", 1)
    assert await cm.delete("live") is True
    assert await cm.delete("live") is False
    with pytest.raises(ChronoMapTypeError):
        await cm.delete(["bad"])


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_pop_and_delete_purge_expired_keys():
    cm = await _expired_cache()
    assert await cm.pop("k", "fallback") == "fallback"
    assert "k" not in cm._store

    cm = await _expired_cache()
    assert await cm.delete("k") is False
    assert "k" not in cm._ttl


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_delete_many_validates_and_skips_expired():
    cm = AsyncChronoMap()
    await cm.put("a", 1)
    await cm.put("dead", 2, ttl=0.02)
    await asyncio.sleep(0.05)

    with pytest.raises(ChronoMapTypeError):
        await cm.delete_many(["a", ["bad"]])
    assert await cm.get("a") == 1

    assert await cm.delete_many(["a", "dead", "missing"]) == 1
    assert await cm.keys() == []


# ---------------------------------------------------------------------------
# Queries and history
# ---------------------------------------------------------------------------


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_query_supports_sync_and_async_predicates():
    cm = AsyncChronoMap()
    await cm.put_many({"a": 1, "b": 2, "c": None})
    assert await cm.query(lambda key, value: value is None) == {"c": None}

    async def predicate(key, value):
        return key == "b"

    assert await cm.query(predicate) == {"b": 2}


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_query_skips_keys_hidden_by_ttl():
    cm = await _expired_cache()
    await cm.put("live", 1)

    assert await cm.query(lambda key, value: True) == {"live": 1}


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_query_predicate_can_use_the_map():
    cm = AsyncChronoMap()
    await cm.put("k", 1)

    async def predicate(key, value):
        return await cm.size() == 1

    assert await cm.query(predicate) == {"k": 1}


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_aggregate_supports_sync_and_async_functions():
    cm = AsyncChronoMap()
    await cm.put_many({"a": 1, "b": 2, "c": 3})
    assert await cm.aggregate(sum) == 6
    assert await cm.aggregate(sum, keys=["a", "c"]) == 4
    assert await cm.aggregate(lambda values: values) == [1, 2, 3]
    assert await cm.aggregate(len, keys=["missing"]) is None

    async def total(values):
        return sum(values)

    assert await cm.aggregate(total) == 6


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_count_and_none_values():
    cm = AsyncChronoMap()
    await cm.put_many({"a": None, "b": 2})
    assert await cm.count() == 2
    assert await cm.count(lambda key, value: value is None) == 1
    assert await cm.count(timestamp=0) == 0


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_history_iter_history_and_range():
    cm = AsyncChronoMap()
    for ts in range(5):
        await cm.put("k", ts, timestamp=ts)

    assert await cm.history("k") == [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]
    assert [item async for item in cm.iter_history("k")] == await cm.history("k")
    assert await cm.get_range("k", start_ts=1, end_ts=3) == [(1, 1), (2, 2), (3, 3)]
    assert await cm.get_range("k", start_ts=4, end_ts=1) == []
    assert await cm.get_range("missing") == []


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_get_range_returns_empty_for_expired_keys():
    cm = await _expired_cache()

    assert await cm.get_range("k") == []


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_range_rejects_invalid_bounds():
    cm = AsyncChronoMap()
    await cm.put("k", 1, timestamp=10)
    with pytest.raises(ChronoMapTypeError):
        await cm.get_range("k", start_ts=True)
    with pytest.raises(ChronoMapValueError):
        await cm.get_range("k", end_ts=math.nan)


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_latest_lookups_and_views():
    cm = AsyncChronoMap()
    await cm.put("a", 1, timestamp=10)
    await cm.put("b", 2, timestamp=20)
    await cm.put("dead", 3, timestamp=30, ttl=0.02)
    await asyncio.sleep(0.05)

    assert [item[0] for item in await cm.get_latest_keys(2)] == ["b", "a"]
    assert await cm.get_latest_keys(0) == []
    assert await cm.get_keys_by_value(1) == ["a"]
    assert await cm.latest() == {"a": 1, "b": 2}
    assert sorted(await cm.keys()) == ["a", "b"]
    assert sorted(await cm.values()) == [1, 2]
    assert sorted(await cm.items()) == [("a", 1), ("b", 2)]
    assert await cm.keys_with_history_count() == {"a": 1, "b": 1}
    assert await cm.size() == 2
    assert await cm.contains("a") is True


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_get_latest_keys_rejects_bad_n():
    cm = AsyncChronoMap()
    for bad in (-1, True, 1.5):
        with pytest.raises(ChronoMapValueError):
            await cm.get_latest_keys(bad)


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_history_of_expired_key_is_empty():
    cm = await _expired_cache(size=0)
    assert await cm.history("k") == []
    assert [item async for item in cm.iter_history("k")] == []


# ---------------------------------------------------------------------------
# Pruning
# ---------------------------------------------------------------------------


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_prune_history_edges():
    cm = AsyncChronoMap()
    for ts in range(5):
        await cm.put("k", ts, timestamp=ts)

    assert await cm.prune_history("k", keep_last=2) == 3
    assert await cm.history("k") == [(3, 3), (4, 4)]
    assert await cm.prune_history("k", keep_last=0) == 2
    assert "k" not in cm._store
    assert await cm.prune_history("missing") == 0


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_prune_history_validation_and_cutoff():
    cm = AsyncChronoMap()
    await cm.put("k", "old", timestamp=10)
    with pytest.raises(ChronoMapValueError):
        await cm.prune_history("k", keep_last=-1)
    with pytest.raises(ChronoMapValueError):
        await cm.prune_history("k", keep_last=True)
    with pytest.raises(ChronoMapTypeError):
        await cm.prune_history(["bad"])
    with pytest.raises(ChronoMapValueError):
        await cm.prune_history("k", older_than="nope")
    with pytest.raises(ChronoMapTypeError):
        await cm.prune_history("k", older_than=True)

    assert await cm.prune_history("k", older_than=20) == 1
    assert "k" not in cm._store


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_prune_all_history_and_cache_invalidation():
    cm = AsyncChronoMap(cache_size=10)
    for key in ("a", "b"):
        for ts in range(3):
            await cm.put(key, ts, timestamp=ts)
    assert await cm.get("a") == 2
    assert await cm.prune_all_history(keep_last=1) == 4
    assert await cm.keys_with_history_count() == {"a": 1, "b": 1}


# ---------------------------------------------------------------------------
# Snapshots, rollback, diff, merge
# ---------------------------------------------------------------------------


@pytest.mark.async_map
@pytest.mark.snapshot
@pytest.mark.asyncio
async def test_async_snapshot_is_isolated_and_timestamped():
    cm = AsyncChronoMap()
    await cm.put("k", "v1")
    snap = await cm.snapshot()
    await cm.put("k", "v2")

    assert await cm.get("k") == "v2"
    assert await snap.get("k") == "v1"
    assert cm.snapshot_time is None
    assert snap.snapshot_time is not None
    assert cm.get_stats()["snapshots"] == 1


@pytest.mark.async_map
@pytest.mark.snapshot
@pytest.mark.asyncio
async def test_async_snapshot_context_keeps_successful_writes():
    cm = AsyncChronoMap()
    await cm.put("k", "original")
    async with cm.snapshot_context() as inner:
        assert inner is cm
        await cm.put("k", "modified")
    assert await cm.get("k") == "modified"


@pytest.mark.async_map
@pytest.mark.snapshot
@pytest.mark.asyncio
async def test_async_snapshot_context_rolls_back_on_exception():
    cm = AsyncChronoMap()
    await cm.put("k", "original")
    with pytest.raises(RuntimeError):
        async with cm.snapshot_context():
            await cm.put("k", "modified")
            raise RuntimeError("boom")
    assert await cm.get("k") == "original"


@pytest.mark.async_map
@pytest.mark.snapshot
@pytest.mark.asyncio
async def test_async_snapshot_context_rejects_foreign_snapshot():
    cm = AsyncChronoMap()
    await cm.put("k", "v")
    with pytest.raises(ChronoMapTypeError):
        await cm.rollback({"not": "a map"})


@pytest.mark.async_map
@pytest.mark.snapshot
@pytest.mark.asyncio
async def test_async_diff_tracks_none_expiry_and_missing_keys():
    left = AsyncChronoMap()
    right = AsyncChronoMap()
    await left.put("same", 1)
    await right.put("same", 1)
    await left.put("none", None)

    assert await left.diff(right) == {"none"}
    assert await left.diff_detailed(right) == [("none", None, None)]

    with pytest.raises(ChronoMapTypeError):
        await left.diff({"not": "a map"})
    with pytest.raises(ChronoMapTypeError):
        await left.diff_detailed({"not": "a map"})


@pytest.mark.async_map
@pytest.mark.snapshot
@pytest.mark.asyncio
async def test_async_diff_handles_expiry_mismatch():
    left = await _expired_cache()
    right = AsyncChronoMap()
    await right.put("k", "value")

    assert await left.diff(right) == {"k"}
    assert await left.diff_detailed(right) == [("k", "value", None)]
    assert await right.diff_detailed(left) == [("k", None, "value")]

    other_expired = await _expired_cache()
    assert await left.diff(other_expired) == set()


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_merge_timestamp_and_overwrite():
    left = AsyncChronoMap()
    right = AsyncChronoMap()
    for ts in (1, 5):
        await left.put("k", f"l{ts}", timestamp=ts)
    for ts in (2, 6):
        await right.put("k", f"r{ts}", timestamp=ts)

    await left.merge(right)
    assert await left.history("k") == [(1, "l1"), (2, "r2"), (5, "l5"), (6, "r6")]

    await left.merge(right, strategy="overwrite")
    assert await left.history("k") == [(2, "r2"), (6, "r6")]


@pytest.mark.async_map
@pytest.mark.ttl
@pytest.mark.asyncio
async def test_async_merge_keeps_the_later_expiry():
    left = AsyncChronoMap()
    right = AsyncChronoMap()
    await left.put("k", "l", timestamp=1)
    await right.put("k", "r", timestamp=2)
    left._ttl["k"] = 1e12
    right._ttl["k"] = 2e12

    await left.merge(right)
    assert left._ttl["k"] == 2e12

    right._ttl["k"] = 3e12
    await left.merge(right, strategy="overwrite")
    assert left._ttl["k"] == 3e12


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_merge_validation_and_self_merge():
    cm = AsyncChronoMap()
    await cm.put("k", 1, timestamp=1)
    with pytest.raises(ChronoMapTypeError):
        await cm.merge({"not": "a map"})
    with pytest.raises(ChronoMapValueError):
        await cm.merge(AsyncChronoMap(), strategy="nope")

    await cm.merge(cm)
    assert await cm.history("k") == [(1, 1), (1, 1)]


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_merge_applies_max_history_and_clears_cache():
    left = AsyncChronoMap(max_history=2, cache_size=10)
    right = AsyncChronoMap()
    await left.put("k", "old", timestamp=1)
    assert await left.get("k") == "old"
    for ts in range(2, 6):
        await right.put("k", ts, timestamp=ts)

    await left.merge(right)
    assert await left.history("k") == [(4, 4), (5, 5)]
    assert left.get_stats()["writes"] == 5


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_merge_keeps_self_history_when_other_side_is_empty():
    left = AsyncChronoMap()
    right = AsyncChronoMap()
    await left.put("k", "left", timestamp=1)
    right._store["k"] = []

    await left.merge(right)
    assert await left.history("k") == [(1, "left")]


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_clear_removes_everything():
    cm = AsyncChronoMap(cache_size=10)
    await cm.put("k", "v")
    assert await cm.get("k") == "v"
    await cm.clear()
    assert await cm.keys() == []
    assert cm.get_stats()["total_keys"] == 0


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_callbacks_support_sync_and_async_functions():
    cm = AsyncChronoMap()
    log = []

    async def async_change(key, old, new, ts):
        log.append(("change", key, new))

    def sync_sub(old, new, ts):
        log.append(("sub", old, new))

    cm.on_change(async_change)
    cm.subscribe("k", sync_sub)
    await cm.put("k", "v")

    assert ("change", "k", "v") in log
    assert ("sub", None, "v") in log


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_callback_errors_are_logged_not_raised(caplog):
    cm = AsyncChronoMap()

    def broken(*args):
        raise RuntimeError("callback boom")

    cm.on_change(broken)
    cm.subscribe("k", broken)
    await cm.put("k", "v")
    assert await cm.get("k") == "v"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_callback_registration_validation_and_removal():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm.on_change("nope")
    with pytest.raises(ChronoMapTypeError):
        cm.subscribe("k", "nope")
    with pytest.raises(ChronoMapTypeError):
        cm.subscribe(["bad"], lambda *args: None)

    callback = lambda *args: None
    cm.on_change(callback)
    assert cm.remove_change_callback(callback) is True
    assert cm.remove_change_callback(callback) is False
    assert cm.unsubscribe("never", callback) is False

    cm.subscribe("k", callback)
    cm.subscribe("k", lambda *args: None)
    assert cm.unsubscribe("k", callback) is True
    assert cm.unsubscribe("k", callback) is False


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_put_many_callbacks_see_the_finished_batch():
    cm = AsyncChronoMap()
    observed = []

    async def callback(key, old, new, ts):
        observed.append((key, await cm.size()))

    cm.on_change(callback)
    await cm.put_many({"a": 1, "b": 2})
    assert observed == [("a", 2), ("b", 2)]


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_stats_and_reset():
    cm = AsyncChronoMap(cache_size=10, ttl_cleanup_interval=1)
    await cm.put("k", "v", ttl=10)
    await cm.get("k")
    await cm.get("k")
    await cm.get("missing")
    cm.cleaned_count = 3

    stats = cm.get_stats()
    assert stats["writes"] == 1
    assert stats["reads"] >= 1
    assert stats["cache_hits"] >= 1
    assert stats["cache_misses"] >= 1
    assert stats["total_keys"] == 1
    assert stats["ttl_cleanup_count"] == 3

    cm.reset_stats()
    reset = cm.get_stats()
    assert reset["writes"] == 0
    assert reset["ttl_cleanup_count"] == 0


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_iter_rows_streams_live_versions():
    cm = AsyncChronoMap()
    await cm.put("a", 1, timestamp=10)
    await cm.put("a", 2, timestamp=20)
    await cm.put("b", "x", timestamp=15)
    await cm.put("dead", 4, ttl=0.001)
    await asyncio.sleep(0.01)

    rows = [row async for row in cm.iter_rows()]
    assert [(row["key"], row["timestamp"], row["value"], row["version"]) for row in rows] == [
        ("a", 10.0, 1, 0),
        ("a", 20.0, 2, 1),
        ("b", 15.0, "x", 0),
    ]
    assert set(rows[0]) == {"key", "value", "timestamp", "datetime", "version"}
    assert rows[0]["datetime"] == datetime.fromtimestamp(10)


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_to_dataframe_exports_live_history():
    pytest.importorskip("pandas")
    cm = AsyncChronoMap()
    await cm.put("k", 1, timestamp=100)
    await cm.put("k", 2, timestamp=200)
    await cm.put("dead", 3, ttl=0.02)
    await asyncio.sleep(0.05)

    df = await cm.to_dataframe()
    assert df["key"].tolist() == ["k", "k"]
    assert df["value"].tolist() == [1, 2]
    assert set(df.columns) >= {"key", "value", "timestamp", "datetime", "version"}


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_to_dataframe_requires_pandas(monkeypatch):
    cm = AsyncChronoMap()
    await cm.put("k", 1)
    monkeypatch.setitem(sys.modules, "pandas", None)

    with pytest.raises(ImportError, match="pandas is required"):
        await cm.to_dataframe()


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_json_roundtrip(tmp_path):
    cm = AsyncChronoMap()
    await cm.put("k", None, timestamp=10)
    await cm.put("number", 42, timestamp=11)
    path = tmp_path / "state.json"
    await cm.save_json(path)

    loaded = await AsyncChronoMap.load_json(path)
    assert await loaded.get("k") == None  # noqa: E711 - stored None is the assertion
    assert await loaded.get("number") == 42
    assert loaded.get_stats()["total_keys"] == 2


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_load_json_accepts_missing_ttl(tmp_path):
    path = tmp_path / "old.json"
    path.write_text('{"store": {"k": [[1.0, "v"]]}}', encoding="utf-8")
    loaded = await AsyncChronoMap.load_json(path)
    assert await loaded.get("k") == "v"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_save_json_refuses_compressed_payload(monkeypatch, tmp_path):
    cm = AsyncChronoMap()
    monkeypatch.setattr(cm, "to_dict", lambda **kwargs: b"bytes")
    with pytest.raises(ChronoMapTypeError):
        await cm.save_json(tmp_path / "state.json")


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_pickle_roundtrip_uncompressed(tmp_path):
    cm = AsyncChronoMap()
    await cm.put("k", "v", timestamp=10)
    path = tmp_path / "state.pkl"
    await cm.save_pickle(path)

    loaded = await AsyncChronoMap.load_pickle(path)
    assert await loaded.get("k") == "v"


@pytest.mark.async_map
@pytest.mark.asyncio
@pytest.mark.parametrize("method", [True, "zlib", "gzip", "bz2", "lzma"])
async def test_async_pickle_roundtrip_compressed(tmp_path, method):
    cm = AsyncChronoMap()
    await cm.put("k", "v", timestamp=10)
    path = tmp_path / "state.pkl"
    await cm.save_pickle(path, compress=method)

    loaded = await AsyncChronoMap.load_pickle(path)
    assert await loaded.get("k") == "v"


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_pickle_rejects_unknown_compression(tmp_path):
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapValueError):
        await cm.save_pickle(tmp_path / "state.pkl", compress="brotli")


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_to_dict_and_from_dict_validation():
    cm = AsyncChronoMap()
    await cm.put("k", "v", timestamp=10)
    data = cm.to_dict()
    assert isinstance(data, dict)

    loaded = AsyncChronoMap.from_dict(data)
    assert await loaded.get("k") == "v"

    for bad in ([], "text", 42, None):
        with pytest.raises(ChronoMapTypeError):
            AsyncChronoMap.from_dict(bad)
    with pytest.raises(ChronoMapTypeError):
        AsyncChronoMap.from_dict({"store": [], "ttl": {}})
    with pytest.raises(ChronoMapTypeError):
        AsyncChronoMap.from_dict({"store": {}, "ttl": []})
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap.from_dict({"store": {"k": "bad"}, "ttl": {}})
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap.from_dict({"store": {"k": [(1,)]}, "ttl": {}})
    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap.from_dict({"store": {}, "ttl": {}, "max_history": 0})


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_from_dict_handles_legacy_zlib_and_unknown_markers():
    data = {"store": {"k": [(1.0, "v")]}, "ttl": {}}
    legacy = zlib.compress(pickle.dumps(data))
    loaded = AsyncChronoMap.from_dict(legacy)
    assert await loaded.get("k") == "v"

    with pytest.raises(ChronoMapValueError):
        AsyncChronoMap.from_dict(b"nope|payload")
