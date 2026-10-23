# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Regression tests for the core hardening pass.

Each test pins one behavior that used to be surprising: writes surviving a
rejected TTL, stored None values vanishing from queries, batch deletes that
stopped halfway, callbacks that deadlocked the writer, and merge/diff paths
that read a map without holding its lock.
"""

import math
import threading
import time
from collections.abc import Mapping

import pytest

from chronomap import (
    ChronoMap,
    ChronoMapKeyError,
    ChronoMapTypeError,
    ChronoMapValueError,
)


def _expired_map(cache_size=0):
    cm = ChronoMap(cache_size=cache_size, enable_ttl_cleanup=False)
    cm.put("k", "value", ttl=0.02)
    time.sleep(0.05)
    return cm


# ---------------------------------------------------------------------------
# Constructor and argument validation
# ---------------------------------------------------------------------------


@pytest.mark.core
@pytest.mark.parametrize("bad_history", [0, -1, True, 1.5, "10"])
def test_max_history_must_be_a_positive_integer(bad_history):
    with pytest.raises(ChronoMapValueError):
        ChronoMap(max_history=bad_history)


@pytest.mark.core
@pytest.mark.parametrize("bad_cache", [-1, True, 2.5, "100"])
def test_cache_size_must_be_a_non_negative_integer(bad_cache):
    with pytest.raises(ChronoMapValueError):
        ChronoMap(cache_size=bad_cache)


@pytest.mark.core
@pytest.mark.parametrize("bad_interval", [0, -0.1, math.inf, math.nan, True, "60"])
def test_ttl_cleanup_interval_must_be_a_positive_finite_number(bad_interval):
    with pytest.raises(ChronoMapValueError):
        ChronoMap(ttl_cleanup_interval=bad_interval)


@pytest.mark.core
@pytest.mark.parametrize("bad_limit", [0, -1, math.inf, math.nan, True, "1"])
def test_max_memory_mb_must_be_a_positive_finite_number(bad_limit):
    with pytest.raises(ChronoMapValueError):
        ChronoMap(max_memory_mb=bad_limit)


@pytest.mark.core
def test_valid_constructor_options_are_accepted():
    cm = ChronoMap(max_history=1, cache_size=0, ttl_cleanup_interval=0.5, max_memory_mb=1)
    cm.put("k", "v")
    assert cm["k"] == "v"


@pytest.mark.core
def test_boolean_timestamp_is_rejected_everywhere():
    cm = ChronoMap(enable_ttl_cleanup=False)
    with pytest.raises(ChronoMapTypeError):
        cm.put("k", "v", timestamp=True)
    with pytest.raises(ChronoMapTypeError):
        cm.get("k", timestamp=False)
    cm.put("k", "v", timestamp=10)
    with pytest.raises(ChronoMapTypeError):
        cm.get_range("k", start_ts=True)


@pytest.mark.core
def test_non_finite_timestamp_is_rejected_by_put_and_get():
    cm = ChronoMap(enable_ttl_cleanup=False)
    with pytest.raises(ChronoMapValueError):
        cm.put("k", "v", timestamp=math.inf)
    with pytest.raises(ChronoMapValueError):
        cm.get("k", timestamp=math.nan)


@pytest.mark.core
def test_validate_timestamp_rejects_boolean_directly():
    cm = ChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm._validate_timestamp(True)


# ---------------------------------------------------------------------------
# Rejected TTLs must not leave a half-written version behind
# ---------------------------------------------------------------------------


@pytest.mark.ttl
@pytest.mark.parametrize("bad_ttl", [0, -1, math.inf, math.nan, True, "60"])
def test_put_rejects_bad_ttl_before_mutating(bad_ttl):
    cm = ChronoMap(enable_ttl_cleanup=False)
    with pytest.raises((ChronoMapValueError, ChronoMapTypeError)):
        cm.put("k", "v", ttl=bad_ttl)
    assert "k" not in cm
    assert cm.history("k") == []


@pytest.mark.ttl
def test_put_keeps_previous_version_when_ttl_is_rejected():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", "old", timestamp=10)
    with pytest.raises(ChronoMapValueError):
        cm.put("k", "new", timestamp=20, ttl=0)
    assert cm.history("k") == [(10, "old")]


@pytest.mark.ttl
def test_put_many_rejects_bad_ttl_before_writing_any_item():
    cm = ChronoMap(enable_ttl_cleanup=False)
    with pytest.raises(ChronoMapValueError):
        cm.put_many({"a": 1, "b": 2}, ttl=-1)
    assert len(cm) == 0


@pytest.mark.ttl
def test_put_many_does_not_store_earlier_items_when_key_is_unhashable():
    class BadItems(Mapping):
        def keys(self):
            return ["a", ["bad"]]

        def __getitem__(self, key):
            return key

        def __iter__(self):
            return iter(self.keys())

        def __len__(self):
            return len(self.keys())

    cm = ChronoMap(enable_ttl_cleanup=False)
    with pytest.raises(ChronoMapTypeError):
        cm.put_many(BadItems())
    assert len(cm) == 0


@pytest.mark.ttl
def test_get_or_set_rejects_bad_ttl_without_storing():
    cm = ChronoMap(enable_ttl_cleanup=False)
    with pytest.raises(ChronoMapValueError):
        cm.get_or_set("k", lambda: "v", ttl=0)
    assert "k" not in cm


@pytest.mark.ttl
def test_setdefault_rejects_bad_ttl_without_storing():
    cm = ChronoMap(enable_ttl_cleanup=False)
    with pytest.raises(ChronoMapValueError):
        cm.setdefault("k", "v", ttl=-1)
    assert "k" not in cm


# ---------------------------------------------------------------------------
# Stored None values are values, not misses
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_query_includes_keys_whose_value_is_none():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put_many({"a": None, "b": 1})
    assert cm.query(lambda key, value: value is None) == {"a": None}
    assert cm.count(lambda key, value: True) == 2


@pytest.mark.core
def test_aggregate_receives_none_values():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put_many({"a": None, "b": 1})
    assert cm.aggregate(len) == 2
    assert cm.aggregate(lambda values: sum(v for v in values if v is not None)) == 1


@pytest.mark.core
def test_get_keys_by_value_finds_stored_none():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put_many({"a": None, "b": 1})
    assert cm.get_keys_by_value(None) == ["a"]


@pytest.mark.core
def test_exists_distinguishes_stored_none_from_missing():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("none", None)
    assert cm.exists("none") is True
    assert cm.exists("missing") is False


# ---------------------------------------------------------------------------
# Batch reads
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_get_many_omits_missing_keys_and_uses_one_timestamp():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("a", "old", timestamp=10)
    cm.put("a", "new", timestamp=20)
    cm.put("b", "bee", timestamp=20)

    assert cm.get_many(["a", "b"], timestamp=15) == {"a": "old"}
    assert cm.get_many(["a", "b"], timestamp=25) == {"a": "new", "b": "bee"}
    assert cm.get_many(["a", "b"], timestamp=5) == {}


@pytest.mark.core
def test_get_many_strict_raises_on_first_missing_key():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("a", 1)
    with pytest.raises(ChronoMapKeyError):
        cm.get_many(["a", "missing"], strict=True)


@pytest.mark.core
def test_get_many_rejects_unhashable_key():
    cm = ChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm.get_many([["bad"]])


@pytest.mark.core
def test_get_many_skips_expired_keys():
    cm = _expired_map()
    cm.put("live", 1)
    assert cm.get_many(["k", "live"], strict=False) == {"live": 1}
    with pytest.raises(ChronoMapKeyError):
        cm.get_many(["k", "live"], strict=True)


@pytest.mark.core
def test_get_unlocked_returns_a_stored_value_while_locked():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", "value", timestamp=10)
    cm._acquire_read()
    try:
        assert cm._get_unlocked("k", timestamp=10) == "value"
    finally:
        cm._release_read()


@pytest.mark.core
def test_exists_respects_timestamp_and_expiry():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", "v", timestamp=100)
    assert cm.exists("k", timestamp=50) is False
    assert cm.exists("k", timestamp=100) is True

    expired = _expired_map()
    assert expired.exists("k") is False


# ---------------------------------------------------------------------------
# dict-style helpers
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_setdefault_returns_existing_value_without_overwriting():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", "original")
    assert cm.setdefault("k", "fallback") == "original"
    assert cm.history("k") == [(cm.history("k")[0][0], "original")]


@pytest.mark.core
def test_setdefault_inserts_default_and_fires_callback():
    cm = ChronoMap(enable_ttl_cleanup=False)
    seen = []
    cm.on_change(lambda key, old, new, ts: seen.append((key, old, new)))
    assert cm.setdefault("k", "value") == "value"
    assert cm["k"] == "value"
    assert seen == [("k", None, "value")]


@pytest.mark.core
def test_setdefault_stores_callable_as_a_plain_value():
    cm = ChronoMap(enable_ttl_cleanup=False)
    factory = lambda: "not called"
    assert cm.setdefault("k", factory) is factory
    assert cm["k"] is factory


@pytest.mark.ttl
def test_setdefault_replaces_an_expired_value():
    cm = _expired_map(cache_size=10)
    assert cm.setdefault("k", "fresh", ttl=10) == "fresh"
    assert cm["k"] == "fresh"


@pytest.mark.ttl
def test_setdefault_applies_ttl():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.setdefault("k", "v", ttl=0.02)
    assert cm["k"] == "v"
    time.sleep(0.05)
    assert cm.get("k") is None


@pytest.mark.core
def test_pop_returns_value_and_removes_history():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", 1, timestamp=1)
    cm.put("k", 2, timestamp=2)
    assert cm.pop("k") == 2
    assert "k" not in cm
    assert cm.history("k") == []


@pytest.mark.core
def test_pop_returns_default_for_missing_key():
    cm = ChronoMap(enable_ttl_cleanup=False)
    assert cm.pop("missing", "fallback") == "fallback"
    assert cm.pop("missing", None) is None


@pytest.mark.core
def test_pop_raises_for_missing_key_without_default():
    cm = ChronoMap(enable_ttl_cleanup=False)
    with pytest.raises(ChronoMapKeyError):
        cm.pop("missing")


@pytest.mark.core
def test_pop_can_remove_a_stored_none():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", None)
    assert cm.pop("k") is None
    assert "k" not in cm


@pytest.mark.ttl
def test_pop_purges_an_expired_key_and_returns_default():
    cm = _expired_map()
    assert cm.pop("k", "fallback") == "fallback"
    assert "k" not in cm._store
    assert "k" not in cm._ttl


@pytest.mark.core
def test_delete_rejects_unhashable_key():
    cm = ChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm.delete(["bad"])


@pytest.mark.ttl
def test_delete_treats_expired_key_as_absent_and_purges_it():
    cm = _expired_map()
    assert cm.delete("k") is False
    assert "k" not in cm._store
    assert "k" not in cm._ttl


@pytest.mark.core
def test_delete_many_validates_all_keys_before_removing_anything():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put_many({"a": 1, "b": 2})
    with pytest.raises(ChronoMapTypeError):
        cm.delete_many(["a", ["bad"]])
    assert cm["a"] == 1
    assert cm["b"] == 2


@pytest.mark.ttl
def test_delete_many_skips_expired_keys_and_purges_them():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("dead", 1, ttl=0.02)
    cm.put("live", 2)
    time.sleep(0.05)

    assert cm.delete_many(["dead", "live", "missing"]) == 1
    assert "dead" not in cm._store
    assert "live" not in cm


# ---------------------------------------------------------------------------
# History pruning edge cases
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_prune_history_keep_last_zero_drops_the_key_entirely():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", 1, timestamp=1, ttl=10)
    cm.put("k", 2, timestamp=2)

    assert cm.prune_history("k", keep_last=0) == 2
    assert "k" not in cm._store
    assert "k" not in cm._ttl
    assert cm.history("k") == []


@pytest.mark.core
@pytest.mark.parametrize("bad_keep", [-1, True, 1.5, "2"])
def test_prune_history_rejects_invalid_keep_last(bad_keep):
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", 1)
    with pytest.raises(ChronoMapValueError):
        cm.prune_history("k", keep_last=bad_keep)


@pytest.mark.core
def test_prune_history_rejects_unhashable_key():
    cm = ChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm.prune_history(["bad"])


@pytest.mark.core
def test_prune_history_drops_key_when_cutoff_removes_everything():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", "old", timestamp=10, ttl=1000)

    assert cm.prune_history("k", older_than=20) == 1
    assert "k" not in cm._store
    assert "k" not in cm._ttl


@pytest.mark.core
def test_prune_history_rejects_invalid_cutoff_timestamp():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", 1, timestamp=10)
    with pytest.raises(ChronoMapValueError):
        cm.prune_history("k", older_than="not-a-date")
    with pytest.raises(ChronoMapTypeError):
        cm.prune_history("k", older_than=True)


@pytest.mark.core
def test_prune_history_invalidates_cached_reads():
    cm = ChronoMap(cache_size=10, enable_ttl_cleanup=False)
    cm.put("k", 1, timestamp=1)
    cm.put("k", 2, timestamp=2)
    assert cm["k"] == 2

    cm.prune_history("k", older_than=2)
    assert cm["k"] == 2
    assert cm.history("k") == [(2, 2)]


# ---------------------------------------------------------------------------
# Range and recency lookups
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_get_latest_keys_zero_returns_empty():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("a", 1)
    assert cm.get_latest_keys(0) == []


@pytest.mark.core
@pytest.mark.parametrize("bad_n", [-1, True, 1.5, "2"])
def test_get_latest_keys_rejects_invalid_n(bad_n):
    cm = ChronoMap()
    with pytest.raises(ChronoMapValueError):
        cm.get_latest_keys(bad_n)


@pytest.mark.core
def test_get_latest_keys_orders_by_last_version_timestamp():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("a", 1, timestamp=10)
    cm.put("b", 2, timestamp=30)
    cm.put("c", 3, timestamp=20)
    assert [item[0] for item in cm.get_latest_keys(2)] == ["b", "c"]


@pytest.mark.core
def test_get_range_returns_empty_when_start_follows_end():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", 1, timestamp=10)
    assert cm.get_range("k", start_ts=20, end_ts=10) == []


@pytest.mark.core
def test_get_range_rejects_non_finite_and_boolean_bounds():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", 1, timestamp=10)
    with pytest.raises(ChronoMapValueError):
        cm.get_range("k", start_ts=math.nan)
    with pytest.raises(ChronoMapTypeError):
        cm.get_range("k", end_ts=True)


@pytest.mark.core
def test_get_range_rejects_unhashable_key():
    cm = ChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm.get_range(["bad"])


# ---------------------------------------------------------------------------
# Snapshot diff semantics
# ---------------------------------------------------------------------------


@pytest.mark.snapshot
def test_diff_flags_stored_none_versus_missing_key():
    left = ChronoMap(enable_ttl_cleanup=False)
    right = ChronoMap(enable_ttl_cleanup=False)
    left.put("k", None)

    assert left.diff(right) == {"k"}
    assert left.diff_detailed(right) == [("k", None, None)]


@pytest.mark.snapshot
def test_diff_ignores_keys_expired_on_both_sides():
    left = _expired_map()
    right = _expired_map()
    assert left.diff(right) == set()


@pytest.mark.snapshot
def test_diff_flags_key_expired_on_only_one_side():
    left = _expired_map()
    right = ChronoMap(enable_ttl_cleanup=False)
    right.put("k", "value")
    assert left.diff(right) == {"k"}


@pytest.mark.snapshot
def test_diff_detailed_reports_added_and_removed_keys():
    left = ChronoMap(enable_ttl_cleanup=False)
    right = ChronoMap(enable_ttl_cleanup=False)
    left.put("only-left", 1)
    right.put("only-right", 2)

    changes = {(key, old, new) for key, old, new in left.diff_detailed(right)}
    assert changes == {("only-left", None, 1), ("only-right", 2, None)}


# ---------------------------------------------------------------------------
# Merge correctness and isolation
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_merge_timestamp_preserves_ties_with_self_first():
    left = ChronoMap(enable_ttl_cleanup=False)
    right = ChronoMap(enable_ttl_cleanup=False)
    left.put("k", "left", timestamp=10)
    right.put("k", "right", timestamp=10)

    left.merge(right, strategy="timestamp")
    assert left.history("k") == [(10, "left"), (10, "right")]


@pytest.mark.core
def test_merge_timestamp_interleaves_many_versions():
    left = ChronoMap(enable_ttl_cleanup=False)
    right = ChronoMap(enable_ttl_cleanup=False)
    for ts in (1, 5, 9):
        left.put("k", f"l{ts}", timestamp=ts)
    for ts in (2, 6, 10):
        right.put("k", f"r{ts}", timestamp=ts)

    left.merge(right)
    assert [ts for ts, _ in left.history("k")] == [1, 2, 5, 6, 9, 10]


@pytest.mark.core
def test_merge_does_not_alias_the_other_maps_history():
    left = ChronoMap(enable_ttl_cleanup=False)
    right = ChronoMap(enable_ttl_cleanup=False)
    right.put("k", "original", timestamp=1)

    left.merge(right, strategy="overwrite")
    right.put("k", "changed", timestamp=2)

    assert left.history("k") == [(1, "original")]


@pytest.mark.core
def test_merge_keeps_self_history_when_other_has_no_versions_for_key():
    left = ChronoMap(enable_ttl_cleanup=False)
    right = ChronoMap(enable_ttl_cleanup=False)
    left.put("k", "left", timestamp=1)
    right._store["k"] = []

    left.merge(right)
    assert left.history("k") == [(1, "left")]


@pytest.mark.core
def test_merge_applies_max_history_and_updates_write_stats():
    left = ChronoMap(max_history=2, enable_ttl_cleanup=False)
    right = ChronoMap(enable_ttl_cleanup=False)
    for ts in range(5):
        right.put("k", ts, timestamp=ts)

    left.merge(right)
    assert left.history("k") == [(3, 3), (4, 4)]
    assert left.get_stats()["writes"] == 5


@pytest.mark.core
def test_merge_overwrite_deep_copies_and_prunes():
    left = ChronoMap(max_history=2, enable_ttl_cleanup=False)
    right = ChronoMap(enable_ttl_cleanup=False)
    for ts in range(4):
        right.put("k", ts, timestamp=ts)

    left.merge(right, strategy="overwrite")
    assert left.history("k") == [(2, 2), (3, 3)]


@pytest.mark.core
def test_merge_self_is_safe_and_keeps_sorted_history():
    cm = ChronoMap(enable_ttl_cleanup=False)
    for ts in (1, 3):
        cm.put("k", ts, timestamp=ts)

    cm.merge(cm)
    assert cm.history("k") == [(1, 1), (1, 1), (3, 3), (3, 3)]


@pytest.mark.core
def test_merge_clears_cached_reads():
    left = ChronoMap(cache_size=10, enable_ttl_cleanup=False)
    right = ChronoMap(enable_ttl_cleanup=False)
    left.put("k", "old", timestamp=1)
    assert left["k"] == "old"
    right.put("k", "new", timestamp=2)

    left.merge(right)
    assert left["k"] == "new"


# ---------------------------------------------------------------------------
# Callbacks fire after the lock is released
# ---------------------------------------------------------------------------


@pytest.mark.core
def test_change_callback_can_write_back_without_deadlocking():
    cm = ChronoMap(enable_ttl_cleanup=False)

    def callback(key, old, new, ts):
        if key == "first":
            cm.put("second", "from-callback")

    cm.on_change(callback)
    writer = threading.Thread(target=lambda: cm.put("first", 1))
    writer.start()
    writer.join(timeout=2)

    assert not writer.is_alive()
    assert cm["second"] == "from-callback"


@pytest.mark.core
def test_put_many_callbacks_run_after_all_items_are_visible():
    cm = ChronoMap(enable_ttl_cleanup=False)
    observed = []
    cm.on_change(lambda key, old, new, ts: observed.append((key, len(cm))))

    cm.put_many({"a": 1, "b": 2})
    assert observed == [("a", 2), ("b", 2)]


@pytest.mark.core
def test_on_change_rejects_non_callable():
    cm = ChronoMap()
    with pytest.raises(ChronoMapTypeError):
        cm.on_change("not callable")


@pytest.mark.core
def test_remove_change_callback_is_thread_safe_and_idempotent():
    cm = ChronoMap(enable_ttl_cleanup=False)
    callback = lambda *args: None
    cm.on_change(callback)
    assert cm.remove_change_callback(callback) is True
    assert cm.remove_change_callback(callback) is False


# ---------------------------------------------------------------------------
# Cache and expiry interaction
# ---------------------------------------------------------------------------


@pytest.mark.cache
@pytest.mark.ttl
def test_cached_value_is_not_returned_after_expiry():
    cm = ChronoMap(cache_size=10, enable_ttl_cleanup=False)
    cm.put("k", "value", ttl=0.02)
    assert cm["k"] == "value"
    time.sleep(0.05)
    assert cm.get("k") is None


@pytest.mark.cache
def test_cache_hit_does_not_leak_across_explicit_timestamps():
    cm = ChronoMap(cache_size=10, enable_ttl_cleanup=False)
    cm.put("k", "old", timestamp=10)
    cm.put("k", "new", timestamp=20)

    assert cm.get("k", timestamp=15) == "old"
    assert cm.get("k", timestamp=25) == "new"
    assert cm.get("k", timestamp=15) == "old"


# ---------------------------------------------------------------------------
# Serialization validation
# ---------------------------------------------------------------------------


@pytest.mark.core
@pytest.mark.parametrize("bad_data", [[], "text", 42, None])
def test_from_dict_rejects_non_mapping_payloads(bad_data):
    with pytest.raises(ChronoMapTypeError):
        ChronoMap.from_dict(bad_data)


@pytest.mark.core
def test_from_dict_rejects_non_mapping_store_or_ttl():
    with pytest.raises(ChronoMapTypeError):
        ChronoMap.from_dict({"store": [], "ttl": {}})
    with pytest.raises(ChronoMapTypeError):
        ChronoMap.from_dict({"store": {}, "ttl": []})


@pytest.mark.core
def test_from_dict_rejects_malformed_versions():
    with pytest.raises(ChronoMapValueError):
        ChronoMap.from_dict({"store": {"k": [(1,)]}, "ttl": {}})
    with pytest.raises(ChronoMapValueError):
        ChronoMap.from_dict({"store": {"k": "not-a-sequence"}, "ttl": {}})


@pytest.mark.core
def test_from_dict_rejects_invalid_persisted_max_history():
    with pytest.raises(ChronoMapValueError):
        ChronoMap.from_dict({"store": {}, "ttl": {}, "max_history": 0})


@pytest.mark.core
def test_from_dict_normalizes_lists_to_tuples():
    cm = ChronoMap.from_dict({"store": {"k": [[1.0, "v"]]}, "ttl": {}})
    assert cm.history("k") == [(1.0, "v")]


@pytest.mark.core
def test_load_json_accepts_files_without_a_ttl_section(tmp_path):
    path = tmp_path / "old.json"
    path.write_text('{"store": {"k": [[1.0, "v"]]}}', encoding="utf-8")

    cm = ChronoMap.load_json(path)
    assert cm["k"] == "v"
    assert cm.get_stats()["total_keys"] == 1


@pytest.mark.core
def test_save_json_refuses_a_compressed_payload(monkeypatch, tmp_path):
    cm = ChronoMap()
    monkeypatch.setattr(cm, "to_dict", lambda **kwargs: b"not-a-dict")
    with pytest.raises(ChronoMapTypeError):
        cm.save_json(tmp_path / "state.json")


@pytest.mark.core
def test_save_and_load_json_roundtrip_with_none_value(tmp_path):
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("k", None, timestamp=1)
    path = tmp_path / "state.json"
    cm.save_json(path)

    loaded = ChronoMap.load_json(path)
    assert loaded.exists("k")
    assert loaded["k"] is None
