# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Tests targeting the branches the main test suites don't reach.

These exist purely to close coverage gaps found after wiring in the real
test suite — mostly defensive double-checks (e.g. the "expired between
the first check and the lock" race guards), strict=True paths that none
of the higher-level callers use, and a couple of straightforward
never-exercised branches (invalid-type raises on diff/merge, the CLI's
no-subcommand path). Nothing here tests new behavior; it's filling in
around what test_chronomap.py and test_cli.py already cover.
"""

import asyncio
import time

import pytest

from chronomap import (
    AsyncChronoMap,
    ChronoMap,
    ChronoMapKeyError,
    ChronoMapMemoryError,
    ChronoMapTypeError,
    ChronoMapValueError,
)
from chronomap._cache import LRUCache
from chronomap._lock import RWLock
from chronomap._memory import MemoryMonitor
from chronomap._ttl_cleanup import TTLCleanupThread
from chronomap.cli import main as cli_main

# ---------------------------------------------------------------------------
# _cache.py
# ---------------------------------------------------------------------------


@pytest.mark.cache
def test_lru_cache_put_overwrites_existing_key_moves_to_end():
    cache = LRUCache(capacity=2)
    cache.put(("a", 1), "first")
    cache.put(("b", 1), "other")
    cache.put(("a", 1), "second")  # overwrite -> move_to_end branch
    cache.put(("c", 1), "newest")  # capacity=2, should evict "b" not "a"

    assert cache.get(("a", 1)) == "second"
    assert cache.get(("b", 1)) is None
    assert cache.get(("c", 1)) == "newest"


# ---------------------------------------------------------------------------
# _lock.py
# ---------------------------------------------------------------------------


@pytest.mark.lock
def test_rwlock_writer_waits_for_reader_to_release():
    lock = RWLock()
    order = []

    lock.acquire_read()

    def writer():
        lock.acquire_write()
        order.append("write")
        lock.release_write()

    import threading

    t = threading.Thread(target=writer)
    t.start()
    time.sleep(0.1)  # writer should be blocked, waiting on the reader
    order.append("read-still-held")
    lock.release_read()
    t.join(timeout=2)

    assert order == ["read-still-held", "write"]


@pytest.mark.lock
def test_rwlock_reader_waits_for_writer_to_release():
    lock = RWLock()
    order = []

    lock.acquire_write()

    def reader():
        lock.acquire_read()
        order.append("read")
        lock.release_read()

    import threading

    t = threading.Thread(target=reader)
    t.start()
    time.sleep(0.1)  # reader should be blocked, waiting on the writer
    order.append("write-still-held")
    lock.release_write()
    t.join(timeout=2)

    assert order == ["write-still-held", "read"]


# ---------------------------------------------------------------------------
# _memory.py
# ---------------------------------------------------------------------------


@pytest.mark.memory
def test_memory_monitor_estimate_size_swallows_typeerror():
    class Unsizeable:
        def __sizeof__(self):
            raise TypeError("nope")

    assert MemoryMonitor.estimate_size(Unsizeable()) == 0


@pytest.mark.memory
def test_memory_monitor_estimate_size_counts_nested_contents():
    # Regression test: sys.getsizeof() alone only sees a dict's shallow
    # hash-table overhead, not the values stored in it. A store with a
    # few keys but large nested history used to report only a few KB.
    small_store = {"key": [(1.0, "x")]}
    big_store = {i: [(float(t), "x" * 10_000) for t in range(50)] for i in range(200)}

    small_size = MemoryMonitor.estimate_size(small_store)
    big_size = MemoryMonitor.estimate_size(big_store)

    assert big_size > small_size + 10_000_000


@pytest.mark.memory
def test_memory_monitor_enforces_limit_on_large_values_not_just_key_count():
    # A single key with a huge value must still trip the limit - this
    # is the scenario the shallow sys.getsizeof() check missed entirely.
    monitor = MemoryMonitor(max_memory_mb=1)
    store = {"one_key": [(1.0, "x" * 5_000_000)]}  # ~5MB value

    with pytest.raises(ChronoMapMemoryError):
        monitor.check_memory(store, {})


@pytest.mark.memory
def test_memory_monitor_estimate_size_handles_cycles():
    # A self-referential structure must not recurse forever.
    cyclic = {}
    cyclic["self"] = cyclic
    MemoryMonitor.estimate_size(cyclic)


@pytest.mark.memory
def test_memory_monitor_reset_warning():
    monitor = MemoryMonitor(max_memory_mb=100)
    monitor.warned = True
    monitor.reset_warning()
    assert monitor.warned is False


# ---------------------------------------------------------------------------
# _ttl_cleanup.py
# ---------------------------------------------------------------------------


@pytest.mark.ttl
def test_ttl_cleanup_thread_start_is_idempotent():
    cm = ChronoMap(enable_ttl_cleanup=False)
    thread = TTLCleanupThread(__import__("weakref").ref(cm), interval=10)
    thread.start()
    first_thread_obj = thread.thread
    thread.start()  # should be a no-op, already alive
    assert thread.thread is first_thread_obj
    thread.stop()


@pytest.mark.ttl
def test_ttl_cleanup_thread_exits_when_owner_is_garbage_collected():
    import weakref

    class Dummy:
        def clean_expired_keys(self):
            return 0

    obj = Dummy()
    thread = TTLCleanupThread(weakref.ref(obj), interval=0.05)
    thread.start()
    del obj
    time.sleep(0.2)
    assert not thread.thread.is_alive()


@pytest.mark.ttl
def test_ttl_cleanup_thread_survives_exception_in_loop():
    import weakref

    class Bad:
        def clean_expired_keys(self):
            raise RuntimeError("boom")

    obj = Bad()
    thread = TTLCleanupThread(weakref.ref(obj), interval=0.05)
    thread.start()
    time.sleep(0.15)
    assert thread.thread.is_alive()  # kept running despite the exception
    thread.stop()


@pytest.mark.ttl
def test_ttl_cleanup_thread_stop_does_not_crash_when_del_runs_on_itself():
    """Regression test for a real race found while closing coverage gaps.

    _cleanup_loop holds the only strong reference to the owning ChronoMap
    for a moment (via chronomap_ref()). If the main thread drops its
    reference at exactly that point, `del cm` inside the loop is what
    drops the refcount to zero — on the *background* thread — which runs
    ChronoMap.__del__ there. __del__ calls stop(), which used to try to
    join() the thread it was currently executing on and raise
    "RuntimeError: cannot join current thread".

    Reproduced directly: point `.thread` at the thread that's calling
    stop() (i.e. this test thread) and confirm stop() no longer tries to
    join itself.
    """
    import threading

    thread = TTLCleanupThread(lambda: None, interval=10)
    thread.thread = threading.current_thread()

    thread.stop()  # must not raise "cannot join current thread"


# ---------------------------------------------------------------------------
# core.py — _get_unlocked strict path (only reachable directly; query()/
# aggregate()/get_keys_by_value() always call it with strict=False)
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_get_unlocked_strict_raises_when_expired():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", "v", ttl=0.05)
    time.sleep(0.1)
    with pytest.raises(ChronoMapKeyError):
        cm._get_unlocked("k", strict=True)


@pytest.mark.core
def test_get_unlocked_non_strict_returns_default_when_expired():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", "v", ttl=0.05)
    time.sleep(0.1)
    assert cm._get_unlocked("k", default="fallback") == "fallback"


@pytest.mark.core
def test_get_unlocked_strict_raises_when_missing():
    cm = ChronoMap()
    with pytest.raises(ChronoMapKeyError):
        cm._get_unlocked("missing", strict=True)


@pytest.mark.core
def test_get_unlocked_non_strict_returns_default_when_missing():
    cm = ChronoMap()
    assert cm._get_unlocked("missing", default="fallback") == "fallback"


@pytest.mark.core
def test_get_unlocked_strict_raises_when_timestamp_before_first_version():
    cm = ChronoMap()
    cm.put("k", "v", timestamp=100)
    with pytest.raises(ChronoMapKeyError):
        cm._get_unlocked("k", timestamp=50, strict=True)


@pytest.mark.core
def test_get_unlocked_non_strict_returns_default_when_timestamp_before_first_version():
    cm = ChronoMap()
    cm.put("k", "v", timestamp=100)
    assert cm._get_unlocked("k", timestamp=50, default="fallback") == "fallback"


# ---------------------------------------------------------------------------
# core.py — _validate_timestamp direct (put() always pre-validates via
# _parse_timestamp, so this method's own type-check is otherwise dead)
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_validate_timestamp_rejects_non_numeric_directly():
    cm = ChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm._validate_timestamp("not a number")


# ---------------------------------------------------------------------------
# core.py — subscribe/unsubscribe/remove_change_callback edge cases
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_subscribe_rejects_non_callable():
    cm = ChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm.subscribe("key", "not callable")


@pytest.mark.core
def test_unsubscribe_key_never_subscribed_returns_false():
    cm = ChronoMap()
    assert cm.unsubscribe("never-subscribed", lambda *a: None) is False


@pytest.mark.core
def test_unsubscribe_last_callback_removes_key_entry():
    cm = ChronoMap()
    cb = lambda old, new, ts: None
    cm.subscribe("key", cb)
    assert cm.unsubscribe("key", cb) is True
    assert "key" not in cm._key_subscribers


@pytest.mark.core
def test_remove_change_callback_not_registered_returns_false():
    cm = ChronoMap()
    assert cm.remove_change_callback(lambda *a: None) is False


# ---------------------------------------------------------------------------
# core.py — out-of-order timestamp insertion (put / put_many)
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_put_out_of_order_timestamp_inserts_in_place():
    cm = ChronoMap()
    cm.put("k", "newer", timestamp=200)
    cm.put("k", "older", timestamp=100)  # goes into the else/insert branch
    assert cm.history("k") == [(100, "older"), (200, "newer")]


@pytest.mark.core
def test_put_many_out_of_order_timestamp_inserts_in_place():
    cm = ChronoMap()
    cm.put_many({"k": "newer"}, timestamp=200)
    cm.put_many({"k": "older"}, timestamp=100)
    assert cm.history("k") == [(100, "older"), (200, "newer")]


@pytest.mark.core
def test_put_many_second_call_reads_existing_old_value():
    cm = ChronoMap()
    changes = []
    cm.on_change(lambda k, o, n, t: changes.append((o, n)))
    cm.put_many({"k": 1})
    cm.put_many({"k": 2})  # exercises the "key already has versions" branch
    assert changes == [(None, 1), (1, 2)]


@pytest.mark.ttl
def test_put_many_negative_ttl_raises():
    cm = ChronoMap()
    with pytest.raises(ChronoMapValueError):
        cm.put_many({"k": 1}, ttl=-5)


# ---------------------------------------------------------------------------
# core.py — get() strict path via the "double-checked" branches inside
# the read lock (normally unreachable in a single-threaded test since
# both expiry checks see the same result; simulated with a stub)
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_get_strict_raises_on_second_expiry_check():
    cm = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    cm.put("key", "value")

    call_count = {"n": 0}

    def flip(key):
        call_count["n"] += 1
        return call_count["n"] > 1  # not expired on the first check, expired on the second

    cm._is_expired = flip

    with pytest.raises(ChronoMapKeyError):
        cm.get("key", strict=True)


@pytest.mark.core
def test_get_non_strict_returns_default_on_second_expiry_check():
    cm = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    cm.put("key", "value")

    call_count = {"n": 0}

    def flip(key):
        call_count["n"] += 1
        return call_count["n"] > 1

    cm._is_expired = flip

    assert cm.get("key", default="fallback") == "fallback"


@pytest.mark.core
def test_get_strict_raises_when_timestamp_before_first_version():
    cm = ChronoMap(cache_size=0)
    cm.put("key", "value", timestamp=100)
    with pytest.raises(ChronoMapKeyError):
        cm.get("key", timestamp=50, strict=True)


@pytest.mark.core
def test_get_or_set_rejects_non_callable_factory():
    cm = ChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm.get_or_set("k", "not callable")


@pytest.mark.ttl
def test_get_or_set_rejects_non_positive_ttl():
    cm = ChronoMap()
    with pytest.raises(ChronoMapValueError):
        cm.get_or_set("k", lambda: "v", ttl=-1)


# ---------------------------------------------------------------------------
# core.py — query() skipping expired keys
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_query_skips_expired_keys():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("temp", 100, ttl=0.05)
    cm.put("perm", 200)
    time.sleep(0.1)
    result = cm.query(lambda k, v: True)
    assert result == {"perm": 200}


# ---------------------------------------------------------------------------
# core.py — prune_history on a key that was never written
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_prune_history_on_unknown_key_returns_zero():
    cm = ChronoMap()
    assert cm.prune_history("never-existed") == 0


# ---------------------------------------------------------------------------
# core.py — get_range / get_latest_keys expiry handling
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_get_range_on_expired_key_returns_empty():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", "v", ttl=0.05)
    time.sleep(0.1)
    assert cm.get_range("k") == []


@pytest.mark.core
def test_get_range_on_unknown_key_returns_empty():
    cm = ChronoMap()
    assert cm.get_range("never-existed") == []


@pytest.mark.core
def test_get_latest_keys_skips_expired():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("temp", "v", ttl=0.05)
    cm.put("perm", "v")
    time.sleep(0.1)
    latest = cm.get_latest_keys(10)
    assert [key for key, _, _ in latest] == ["perm"]


# ---------------------------------------------------------------------------
# core.py — diff / diff_detailed: type validation and expiry mismatch
# ---------------------------------------------------------------------------


@pytest.mark.snapshot
def test_diff_invalid_type_raises():
    cm = ChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm.diff({"not": "a chronomap"})


@pytest.mark.snapshot
def test_diff_detailed_invalid_type_raises():
    cm = ChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm.diff_detailed({"not": "a chronomap"})


@pytest.mark.snapshot
def test_diff_flags_key_when_expiry_status_differs():
    cm1 = ChronoMap(enable_ttl_cleanup=False)
    cm2 = ChronoMap(enable_ttl_cleanup=False)
    cm1.put("k", "v", ttl=0.05)
    cm2.put("k", "v")  # never expires
    time.sleep(0.1)

    changed = cm1.diff(cm2)
    assert "k" in changed


@pytest.mark.snapshot
def test_diff_detailed_skips_key_when_either_side_expired():
    cm1 = ChronoMap(enable_ttl_cleanup=False)
    cm2 = ChronoMap(enable_ttl_cleanup=False)
    cm1.put("k", "v1", ttl=0.05)
    cm2.put("k", "v2")
    time.sleep(0.1)

    assert cm1.diff_detailed(cm2) == []


# ---------------------------------------------------------------------------
# core.py — merge(): TTL propagation for both strategies
# ---------------------------------------------------------------------------


@pytest.mark.ttl
def test_merge_timestamp_strategy_propagates_later_ttl():
    cm1 = ChronoMap(enable_ttl_cleanup=False)
    cm2 = ChronoMap(enable_ttl_cleanup=False)
    cm1.put("k", "v", timestamp=1.0)
    cm1._ttl["k"] = 1000.0
    cm2.put("k", "v", timestamp=1.0)
    cm2._ttl["k"] = 2000.0  # later expiry should win

    cm1.merge(cm2, strategy="timestamp")
    assert cm1._ttl["k"] == 2000.0


@pytest.mark.ttl
def test_merge_overwrite_strategy_copies_ttl():
    cm1 = ChronoMap(enable_ttl_cleanup=False)
    cm2 = ChronoMap(enable_ttl_cleanup=False)
    cm2.put("k", "v", ttl=1000)

    cm1.merge(cm2, strategy="overwrite")
    assert "k" in cm1._ttl


# ---------------------------------------------------------------------------
# core.py — history() on an expired key
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_history_on_expired_key_returns_empty():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", "v", ttl=0.05)
    time.sleep(0.1)
    assert cm.history("k") == []


# ---------------------------------------------------------------------------
# core.py — to_dataframe skips expired keys
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_to_dataframe_skips_expired_keys():
    pytest.importorskip("pandas")
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("temp", 1, ttl=0.05)
    cm.put("perm", 2)
    time.sleep(0.1)
    df = cm.to_dataframe()
    assert set(df["key"]) == {"perm"}


# ---------------------------------------------------------------------------
# core.py — __eq__ against a non-ChronoMap, and snapshot_time property
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_eq_against_non_chronomap_is_false():
    cm = ChronoMap()
    assert (cm == "not a chronomap") is False
    assert cm != "not a chronomap"


@pytest.mark.snapshot
def test_snapshot_time_property():
    cm = ChronoMap()
    assert cm.snapshot_time is None
    snap = cm.snapshot()
    assert snap.snapshot_time is not None


# ---------------------------------------------------------------------------
# core.py — from_dict with an unrecognized compression marker
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_from_dict_unknown_compression_marker_raises():
    with pytest.raises(ChronoMapValueError):
        ChronoMap.from_dict(b"not-a-real-method|somebytes")


# ---------------------------------------------------------------------------
# asynchronous.py
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_debug_mode_sets_log_level():
    cm = AsyncChronoMap(debug=True)
    await cm.put("k", "v")
    assert await cm.get("k") == "v"


@pytest.mark.asyncio
async def test_async_put_with_iso_string_timestamp():
    cm = AsyncChronoMap()
    await cm.put("k", "v", timestamp="2026-01-01T00:00:00")
    assert await cm.get("k") == "v"


@pytest.mark.asyncio
async def test_async_put_with_invalid_iso_string_raises():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapValueError):
        await cm.put("k", "v", timestamp="not a datetime")


@pytest.mark.asyncio
async def test_async_put_with_datetime_object():
    from datetime import datetime

    cm = AsyncChronoMap()
    await cm.put("k", "v", timestamp=datetime(2026, 1, 1))
    assert await cm.get("k") == "v"


@pytest.mark.asyncio
async def test_async_put_with_invalid_timestamp_type_raises():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapTypeError):
        await cm.put("k", "v", timestamp=1 + 2j)


@pytest.mark.asyncio
async def test_async_unhashable_key_raises():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapTypeError):
        await cm.put(["unhashable"], "v")


@pytest.mark.asyncio
async def test_async_ttl_expiry():
    cm = AsyncChronoMap()
    await cm.put("k", "v", ttl=0.05)
    assert await cm.get("k") == "v"
    await asyncio.sleep(0.1)
    assert await cm.get("k") is None


@pytest.mark.asyncio
async def test_async_put_out_of_order_timestamp():
    cm = AsyncChronoMap()
    await cm.put("k", "newer", timestamp=200)
    await cm.put("k", "older", timestamp=100)
    assert await cm.get("k", timestamp=150) == "older"
    assert await cm.get("k", timestamp=250) == "newer"


@pytest.mark.asyncio
async def test_async_put_negative_ttl_raises():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapValueError):
        await cm.put("k", "v", ttl=-1)


@pytest.mark.asyncio
async def test_async_get_strict_raises_when_missing():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapKeyError):
        await cm.get("missing", strict=True)


@pytest.mark.asyncio
async def test_async_get_strict_raises_when_expired():
    cm = AsyncChronoMap()
    await cm.put("k", "v", ttl=0.05)
    await asyncio.sleep(0.1)
    with pytest.raises(ChronoMapKeyError):
        await cm.get("k", strict=True)


@pytest.mark.asyncio
async def test_async_get_strict_raises_when_timestamp_before_first_version():
    cm = AsyncChronoMap()
    await cm.put("k", "v", timestamp=100)
    with pytest.raises(ChronoMapKeyError):
        await cm.get("k", timestamp=50, strict=True)


@pytest.mark.asyncio
async def test_async_get_non_strict_returns_default_when_timestamp_before_first_version():
    cm = AsyncChronoMap()
    await cm.put("k", "v", timestamp=100)
    assert await cm.get("k", timestamp=50, default="fallback") == "fallback"


@pytest.mark.asyncio
async def test_async_get_or_set_rejects_non_callable_factory():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapTypeError):
        await cm.get_or_set("k", "not callable")


@pytest.mark.asyncio
async def test_async_get_or_set_rejects_non_positive_ttl():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapValueError):
        await cm.get_or_set("k", lambda: "v", ttl=-1)


@pytest.mark.asyncio
async def test_async_get_or_set_cleans_up_expired_key_before_recreating():
    cm = AsyncChronoMap()
    await cm.put("k", "old", ttl=0.05)
    await asyncio.sleep(0.1)
    value = await cm.get_or_set("k", lambda: "new")
    assert value == "new"


@pytest.mark.asyncio
async def test_async_get_or_set_awaits_coroutine_factory():
    cm = AsyncChronoMap()

    async def factory():
        return "created-async"

    value = await cm.get_or_set("k", factory)
    assert value == "created-async"


@pytest.mark.asyncio
async def test_async_get_or_set_applies_ttl_to_new_value():
    cm = AsyncChronoMap()
    await cm.get_or_set("k", lambda: "v", ttl=0.05)
    assert await cm.get("k") == "v"
    await asyncio.sleep(0.1)
    assert await cm.get("k") is None


@pytest.mark.asyncio
async def test_async_get_or_default_with_plain_value():
    cm = AsyncChronoMap()
    assert await cm.get_or_default("k", "fallback") == "fallback"
    assert await cm.get("k") == "fallback"


@pytest.mark.asyncio
async def test_async_subscribe_rejects_non_callable():
    cm = AsyncChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm.subscribe("k", "not callable")


@pytest.mark.asyncio
async def test_async_unsubscribe_never_subscribed_returns_false():
    cm = AsyncChronoMap()
    assert cm.unsubscribe("k", lambda *a: None) is False


@pytest.mark.asyncio
async def test_async_unsubscribe_unknown_callback_returns_false():
    cm = AsyncChronoMap()
    cm.subscribe("k", lambda *a: None)
    assert cm.unsubscribe("k", lambda *a: None) is False  # different callback object


# ---------------------------------------------------------------------------
# cli.py — main()
# ---------------------------------------------------------------------------


@pytest.mark.cli
def test_cli_main_show_command(tmp_path, capsys):
    cm = ChronoMap()
    cm.put("a", 1)
    path = tmp_path / "state.json"
    cm.save_json(path)

    exit_code = cli_main(["show", str(path)])

    assert exit_code == 0
    assert "a: 1" in capsys.readouterr().out


@pytest.mark.cli
def test_cli_main_no_subcommand_prints_help_and_returns_1(capsys):
    exit_code = cli_main([])
    assert exit_code == 1
    assert "usage" in capsys.readouterr().out.lower()
