# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Tests for type-preserving, versioned ChronoMap archives."""

import json
import math
import os
import pickle
import time
from datetime import date, datetime, time as clock_time, timezone

import pytest

from chronomap import (
    ChronoMap,
    ChronoMapError,
    ChronoMapTypeError,
    CorruptArchiveError,
    CodecError,
    PersistenceError,
    UnsupportedFormatError,
    dump,
    dumps,
    inspect,
    load,
    loads,
)
from chronomap.persistence import ArchiveInfo, FORMAT_VERSION


class Sample:
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return isinstance(other, Sample) and other.value == self.value


def make_map(**kwargs):
    return ChronoMap(cache_size=0, enable_ttl_cleanup=False, **kwargs)


def items(cm):
    return dict(cm.items())


def json_payload(cm, **kwargs):
    archive = dumps(cm, **kwargs)
    return json.loads(archive.split(b"\n", 1)[1].decode("utf-8"))


def json_archive(payload):
    return b"CHRONOMAP/1 json\n" + json.dumps(payload).encode("utf-8")


def assert_corrupt(payload):
    with pytest.raises(CorruptArchiveError):
        loads(json_archive(payload), enable_ttl_cleanup=False)


@pytest.mark.persistence
def test_json_roundtrip_preserves_everything(tmp_path):
    cm = make_map()
    cm.put(
        (1, "alpha"),
        {
            "list": [1, "two", None, True],
            "tuple": (1, 2),
            "bytes": b"\x00\xff",
            "when": datetime(2024, 5, 1, 12, 30, tzinfo=timezone.utc),
            "set": {"a", "b"},
            "mapping": {(1, 2): "tuple key", 3: [4]},
        },
        timestamp=100,
    )
    cm.put(
        (1, "alpha"),
        {"replacement": True},
        timestamp=200,
    )
    cm.put(7, "integer key", timestamp=150)
    cm.put(b"bytes-key", [1, 2, 3], timestamp=160)
    cm.put(None, "none key", timestamp=170)
    cm.put(frozenset({1, 2}), "frozenset key", timestamp=180)
    cm.put(date(2020, 1, 2), "date key", timestamp=190)
    cm.put(clock_time(9, 30), "time key", timestamp=195)

    restored = loads(dumps(cm), enable_ttl_cleanup=False)

    assert items(restored) == items(cm)
    assert restored.history((1, "alpha")) == [
        (100.0, cm.history((1, "alpha"))[0][1]),
        (200.0, {"replacement": True}),
    ]
    assert restored.history(7) == [(150.0, "integer key")]
    assert restored.history(b"bytes-key") == [(160.0, [1, 2, 3])]
    assert restored.history(None) == [(170.0, "none key")]
    assert restored.history(frozenset({1, 2})) == [(180.0, "frozenset key")]
    assert restored.history(date(2020, 1, 2)) == [(190.0, "date key")]
    assert restored.history(clock_time(9, 30)) == [(195.0, "time key")]


@pytest.mark.persistence
def test_roundtrip_special_floats_and_negative_zero():
    cm = make_map()
    cm.put("nan", float("nan"))
    cm.put("inf", float("inf"))
    cm.put("neg-inf", float("-inf"))
    cm.put("zero", -0.0)

    restored = loads(dumps(cm), enable_ttl_cleanup=False)

    assert math.isnan(restored.get("nan"))
    assert restored.get("inf") == math.inf
    assert restored.get("neg-inf") == -math.inf
    assert math.copysign(1.0, restored.get("zero")) == -1.0


@pytest.mark.persistence
def test_roundtrip_nested_containers_and_bool_vs_int():
    cm = make_map()
    value = {
        "bool": True,
        "int": 1,
        "nested": [{"tuple": (1, (2, 3))}],
        "frozen": frozenset({"x"}),
    }
    cm.put("key", value, timestamp=1)

    restored = loads(dumps(cm), enable_ttl_cleanup=False).get("key")

    assert restored == value
    assert restored["bool"] is True
    assert restored["int"] == 1


@pytest.mark.persistence
def test_multiple_versions_keep_order_and_timestamps():
    cm = make_map()
    cm.put("key", "middle", timestamp=200)
    cm.put("key", "last", timestamp=300)
    cm.put("key", "first", timestamp=100)

    restored = loads(dumps(cm), enable_ttl_cleanup=False)

    assert restored.history("key") == [
        (100.0, "first"),
        (200.0, "middle"),
        (300.0, "last"),
    ]


@pytest.mark.persistence
def test_ttl_deadline_survives_roundtrip():
    cm = make_map()
    cm.put("key", "value", timestamp=100, ttl=3600)
    deadline = cm._ttl["key"]

    restored = loads(dumps(cm), enable_ttl_cleanup=False)

    assert restored._ttl == {"key": deadline}
    assert restored.get("key", timestamp=100) == "value"


@pytest.mark.persistence
def test_include_expired_false_skips_them():
    cm = make_map()
    cm.put("live", 1, timestamp=100)
    cm.put("gone", 2, timestamp=100, ttl=0.01)
    time.sleep(0.02)

    payload = json_payload(cm, include_expired=False)
    restored = loads(json_archive(payload), enable_ttl_cleanup=False)

    assert payload["entries"] == [{"key": ["str", "live"], "versions": [[100.0, ["int", 1]]]}]
    assert items(restored) == {"live": 1}


@pytest.mark.persistence
def test_drop_expired_skips_them_on_load():
    cm = make_map()
    cm.put("live", 1, timestamp=100)
    cm.put("gone", 2, timestamp=100, ttl=0.01)
    time.sleep(0.02)

    restored = loads(dumps(cm), enable_ttl_cleanup=False, drop_expired=True)
    kept = loads(dumps(cm), enable_ttl_cleanup=False)

    assert items(restored) == {"live": 1}
    assert "gone" in kept._store
    assert "gone" not in restored._store
    assert kept.get("gone") is None


@pytest.mark.persistence
def test_max_history_and_snapshot_time_survive_roundtrip():
    cm = make_map(max_history=2)
    for index in range(4):
        cm.put("key", index, timestamp=100 + index)
    snapshot = cm.snapshot()

    restored = loads(dumps(snapshot), enable_ttl_cleanup=False)
    restored.put("key", "new", timestamp=500)

    assert restored.snapshot_time == snapshot.snapshot_time
    assert snapshot.history("key") == [(102.0, 2), (103.0, 3)]
    assert restored.history("key") == [(103.0, 3), (500.0, "new")]


@pytest.mark.persistence
def test_max_history_trimming_while_loading():
    cm = make_map()
    cm.put("key", 1, timestamp=100)
    cm.put("key", 2, timestamp=200)
    cm.put("key", 3, timestamp=300)
    payload = json_payload(cm)
    payload["max_history"] = 1

    restored = loads(json_archive(payload), enable_ttl_cleanup=False)

    assert restored.history("key") == [(300.0, 3)]


@pytest.mark.persistence
def test_explicit_max_history_overrides_archive_limit():
    cm = make_map()
    cm.put("key", 1, timestamp=100)
    cm.put("key", 2, timestamp=200)
    cm.put("key", 3, timestamp=300)

    restored = loads(dumps(cm), enable_ttl_cleanup=False, max_history=1)

    assert restored._max_history == 1
    assert restored.history("key") == [(300.0, 3)]


@pytest.mark.persistence
def test_explicit_none_disables_archive_max_history():
    cm = make_map()
    cm.put("key", 1, timestamp=100)
    cm.put("key", 2, timestamp=200)
    cm.put("key", 3, timestamp=300)
    payload = json_payload(cm)
    payload["max_history"] = 2

    restored = loads(json_archive(payload), enable_ttl_cleanup=False, max_history=None)

    assert restored._max_history is None
    assert restored.history("key") == [(100.0, 1), (200.0, 2), (300.0, 3)]


@pytest.mark.persistence
def test_load_forwards_max_history_override(tmp_path):
    cm = make_map()
    cm.put("key", 1, timestamp=100)
    cm.put("key", 2, timestamp=200)
    path = tmp_path / "state.json"

    dump(cm, path)
    restored = load(path, enable_ttl_cleanup=False, max_history=1)

    assert restored.history("key") == [(200.0, 2)]


@pytest.mark.persistence
def test_empty_map_roundtrip():
    restored = loads(dumps(make_map()), enable_ttl_cleanup=False)

    assert items(restored) == {}
    assert list(restored.keys()) == []


@pytest.mark.persistence
def test_pickle_roundtrip_supports_arbitrary_objects():
    cm = make_map()
    original = Sample({"nested": [1, 2]})
    cm.put("custom", original, timestamp=100)

    data = dumps(cm, format="pickle")
    restored = loads(data, enable_ttl_cleanup=False)

    assert restored.get("custom") == original
    assert isinstance(restored.get("custom"), Sample)


@pytest.mark.persistence
def test_json_rejects_unsupported_objects():
    cm = make_map()
    cm.put("custom", Sample("value"))

    with pytest.raises(CodecError, match="use format='pickle'"):
        dumps(cm)


@pytest.mark.persistence
def test_dump_infers_format_from_suffix(tmp_path):
    cm = make_map()
    cm.put("key", {"a": (1, 2)}, timestamp=100)

    json_path = dump(cm, tmp_path / "state.json")
    pickle_path = dump(cm, tmp_path / "state.pkl")
    long_pickle_path = dump(cm, tmp_path / "state.pickle")
    no_suffix_path = dump(cm, tmp_path / "state")

    assert inspect(json_path).format == "json"
    assert inspect(pickle_path).format == "pickle"
    assert inspect(long_pickle_path).format == "pickle"
    assert inspect(no_suffix_path).format == "json"
    assert items(load(json_path, enable_ttl_cleanup=False)) == items(cm)


@pytest.mark.persistence
def test_dump_returns_path_and_writes_atomically(tmp_path):
    cm = make_map()
    cm.put("key", 1, timestamp=100)
    target = tmp_path / "archive.json"

    result = dump(cm, target)

    assert result == target
    assert target.exists()
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.persistence
def test_dump_non_atomic(tmp_path):
    cm = make_map()
    cm.put("key", 1, timestamp=100)
    target = dump(cm, tmp_path / "archive.json", atomic=False)

    assert items(load(target, enable_ttl_cleanup=False)) == {"key": 1}
    assert not any(path.suffix == ".tmp" for path in tmp_path.iterdir())


@pytest.mark.persistence
def test_dump_missing_parent_raises(tmp_path):
    cm = make_map()

    with pytest.raises(PersistenceError, match="Directory does not exist"):
        dump(cm, tmp_path / "missing" / "archive.json")


@pytest.mark.persistence
def test_non_atomic_write_error_is_wrapped(tmp_path):
    cm = make_map()

    with pytest.raises(PersistenceError, match="Could not write archive"):
        dump(cm, tmp_path, atomic=False)


@pytest.mark.persistence
def test_atomic_replace_error_cleans_up_temporary_file(tmp_path):
    cm = make_map()
    target = tmp_path / "target"
    target.mkdir()

    with pytest.raises(PersistenceError, match="Could not write archive"):
        dump(cm, target)

    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.persistence
def test_cleanup_ignores_unlink_failure(tmp_path, monkeypatch):
    cm = make_map()
    target = tmp_path / "target"
    target.mkdir()

    def fail_unlink(path):
        raise OSError("unlink failed")

    monkeypatch.setattr(os, "unlink", fail_unlink)

    with pytest.raises(PersistenceError, match="Could not write archive"):
        dump(cm, target)


@pytest.mark.persistence
def test_atomic_write_closes_descriptor_when_fdopen_fails(tmp_path, monkeypatch):
    cm = make_map()

    def fail_fdopen(descriptor, mode):
        raise OSError("fdopen failed")

    monkeypatch.setattr(os, "fdopen", fail_fdopen)

    with pytest.raises(PersistenceError, match="fdopen failed"):
        dump(cm, tmp_path / "archive.json")

    assert list(tmp_path.iterdir()) == []


@pytest.mark.persistence
def test_dump_and_load_accept_string_paths(tmp_path):
    cm = make_map()
    cm.put("key", 1, timestamp=100)
    path = str(tmp_path / "archive.json")

    dump(cm, path)

    assert items(load(path, enable_ttl_cleanup=False)) == {"key": 1}


@pytest.mark.persistence
@pytest.mark.parametrize("compression", ["zlib", "gzip", "bz2", "lzma"])
def test_compressed_archives_roundtrip(tmp_path, compression):
    cm = make_map()
    cm.put("key", {"payload": list(range(20))}, timestamp=100)
    target = tmp_path / f"archive-{compression}.json"

    dump(cm, target, compression=compression)

    assert inspect(target).compression == compression
    assert items(load(target, enable_ttl_cleanup=False)) == items(cm)


@pytest.mark.persistence
def test_compressed_pickle_roundtrip():
    cm = make_map()
    cm.put("key", Sample(1), timestamp=100)

    data = dumps(cm, format="pickle", compression="zlib")
    restored = loads(data, enable_ttl_cleanup=False)

    assert restored.get("key") == Sample(1)


@pytest.mark.persistence
def test_inspect_reports_metadata(tmp_path):
    cm = make_map(max_history=3)
    cm.put("a", 1, timestamp=100)
    cm.put("a", 2, timestamp=200)
    cm.put("b", 3, timestamp=150)
    snapshot = cm.snapshot()
    target = dump(snapshot, tmp_path / "archive.json")

    info = inspect(target)

    assert isinstance(info, ArchiveInfo)
    assert info.format == "json"
    assert info.format_version == FORMAT_VERSION
    assert info.compression is None
    assert info.key_count == 2
    assert info.version_count == 3
    assert info.max_history == 3
    assert info.snapshot_time == snapshot.snapshot_time
    assert info.created_at is not None


@pytest.mark.persistence
def test_bare_json_archive_is_accepted():
    cm = make_map()
    cm.put("key", 1, timestamp=100)
    archive = dumps(cm)
    body = archive.split(b"\n", 1)[1]

    assert items(loads(body, enable_ttl_cleanup=False)) == {"key": 1}
    assert items(loads(b"\n  " + body, enable_ttl_cleanup=False)) == {"key": 1}


@pytest.mark.persistence
def test_bare_pickle_is_rejected():
    with pytest.raises(UnsupportedFormatError, match="Unrecognized archive header"):
        loads(pickle.dumps({"format": "chronomap", "format_version": 1, "entries": []}))


@pytest.mark.persistence
def test_header_version_mismatch():
    with pytest.raises(UnsupportedFormatError, match="Unsupported archive version"):
        loads(b"CHRONOMAP/9 json\n{}")


@pytest.mark.persistence
def test_header_unknown_compression():
    with pytest.raises(UnsupportedFormatError, match="Unsupported compression"):
        loads(b"CHRONOMAP/1 json zip\n{}")


@pytest.mark.persistence
def test_requested_format_mismatch():
    data = dumps(make_map(), format="pickle")

    with pytest.raises(PersistenceError, match="Archive is pickle"):
        loads(data, format="json")


@pytest.mark.persistence
def test_load_format_is_checked_against_path(tmp_path):
    cm = make_map()
    target = dump(cm, tmp_path / "archive.pkl")

    with pytest.raises(PersistenceError, match="Archive is pickle"):
        load(target, format="json")


@pytest.mark.persistence
def test_bad_format_and_compression():
    cm = make_map()

    with pytest.raises(UnsupportedFormatError, match="Unsupported archive format"):
        dumps(cm, format="yaml")
    with pytest.raises(UnsupportedFormatError, match="Unsupported compression"):
        dumps(cm, compression="zip")
    with pytest.raises(UnsupportedFormatError, match="Unsupported archive format"):
        loads(b"anything", format="yaml")


@pytest.mark.persistence
def test_dumps_and_loads_type_checks():
    with pytest.raises(ChronoMapTypeError, match="requires a ChronoMap"):
        dumps({})
    with pytest.raises(ChronoMapTypeError, match="requires bytes"):
        loads("not bytes")


@pytest.mark.persistence
def test_missing_archive_files(tmp_path):
    missing = tmp_path / "missing.json"

    with pytest.raises(PersistenceError, match="Could not read archive"):
        load(missing)
    with pytest.raises(PersistenceError, match="Could not read archive"):
        inspect(missing)


@pytest.mark.persistence
def test_corrupt_json_and_pickle_bodies():
    with pytest.raises(CorruptArchiveError, match="Invalid JSON"):
        loads(b"CHRONOMAP/1 json\n{")
    with pytest.raises(CorruptArchiveError, match="Invalid pickle"):
        loads(b"CHRONOMAP/1 pickle\nnot a pickle")
    with pytest.raises(CorruptArchiveError, match="Could not decompress"):
        loads(b"CHRONOMAP/1 json zlib\nnot compressed")


@pytest.mark.persistence
def test_payload_must_be_an_object():
    assert_corrupt([])


@pytest.mark.persistence
def test_payload_requires_chronomap_format():
    assert_corrupt({"format": "other", "format_version": 1, "entries": []})


@pytest.mark.persistence
def test_payload_requires_supported_version():
    payload = {"format": "chronomap", "format_version": 99, "entries": []}
    with pytest.raises(UnsupportedFormatError, match="Unsupported archive version"):
        loads(json_archive(payload))


@pytest.mark.persistence
@pytest.mark.parametrize("bad", [0, -1, True, 1.5, "3"])
def test_payload_rejects_invalid_max_history(bad):
    payload = {
        "format": "chronomap",
        "format_version": 1,
        "max_history": bad,
        "entries": [],
    }
    assert_corrupt(payload)


@pytest.mark.persistence
@pytest.mark.parametrize("bad", ["soon", True, float("inf")])
def test_payload_rejects_invalid_snapshot_time(bad):
    payload = {
        "format": "chronomap",
        "format_version": 1,
        "snapshot_time": bad,
        "entries": [],
    }
    assert_corrupt(payload)


@pytest.mark.persistence
def test_payload_requires_entry_list():
    assert_corrupt({"format": "chronomap", "format_version": 1, "entries": {}})


@pytest.mark.persistence
def test_payload_entry_must_be_object():
    assert_corrupt({"format": "chronomap", "format_version": 1, "entries": ["x"]})


@pytest.mark.persistence
def test_payload_entry_requires_key():
    assert_corrupt(
        {
            "format": "chronomap",
            "format_version": 1,
            "entries": [{"versions": [[100, ["int", 1]]]}],
        }
    )


@pytest.mark.persistence
def test_payload_rejects_unhashable_key():
    assert_corrupt(
        {
            "format": "chronomap",
            "format_version": 1,
            "entries": [
                {
                    "key": ["list", [["int", 1]]],
                    "versions": [[100, ["int", 1]]],
                }
            ],
        }
    )


@pytest.mark.persistence
def test_payload_rejects_duplicate_keys():
    entry = {"key": ["str", "dupe"], "versions": [[100, ["int", 1]]]}
    assert_corrupt(
        {
            "format": "chronomap",
            "format_version": 1,
            "entries": [entry, dict(entry)],
        }
    )


@pytest.mark.persistence
@pytest.mark.parametrize("versions", [[], {}, "versions", None])
def test_payload_requires_non_empty_version_list(versions):
    assert_corrupt(
        {
            "format": "chronomap",
            "format_version": 1,
            "entries": [{"key": ["str", "key"], "versions": versions}],
        }
    )


@pytest.mark.persistence
@pytest.mark.parametrize(
    "pair",
    [
        [100],
        [100, ["int", 1], "extra"],
        {"timestamp": 100},
        "pair",
    ],
)
def test_payload_rejects_malformed_version_pairs(pair):
    assert_corrupt(
        {
            "format": "chronomap",
            "format_version": 1,
            "entries": [{"key": ["str", "key"], "versions": [pair]}],
        }
    )


@pytest.mark.persistence
@pytest.mark.parametrize("timestamp", [True, "100", float("nan")])
def test_payload_rejects_invalid_timestamps(timestamp):
    assert_corrupt(
        {
            "format": "chronomap",
            "format_version": 1,
            "entries": [
                {
                    "key": ["str", "key"],
                    "versions": [[timestamp, ["int", 1]]],
                }
            ],
        }
    )


@pytest.mark.persistence
def test_payload_rejects_unsorted_versions():
    assert_corrupt(
        {
            "format": "chronomap",
            "format_version": 1,
            "entries": [
                {
                    "key": ["str", "key"],
                    "versions": [
                        [200, ["int", 2]],
                        [100, ["int", 1]],
                    ],
                }
            ],
        }
    )


@pytest.mark.persistence
@pytest.mark.parametrize("ttl", ["soon", True, float("inf")])
def test_payload_rejects_invalid_ttl(ttl):
    assert_corrupt(
        {
            "format": "chronomap",
            "format_version": 1,
            "entries": [
                {
                    "key": ["str", "key"],
                    "versions": [[100, ["int", 1]]],
                    "ttl": ttl,
                }
            ],
        }
    )


@pytest.mark.persistence
@pytest.mark.parametrize(
    "encoded",
    [
        ["unknown"],
        ["none", "extra"],
        ["bool", "yes"],
        ["int", True],
        ["float", "bad"],
        ["str", 1],
        ["bytes", "!!!"],
        ["datetime", "not-a-date"],
        ["date", "not-a-date"],
        ["time", "not-a-time"],
        ["list", "not-a-list"],
        ["tuple", "not-a-list"],
        ["set", [["list", []]]],
        ["frozenset", [["list", []]]],
        ["dict", [["int", 1]]],
        ["dict", [[["int", 1]]]],
        ["dict", [[["list", []], ["int", 1]]]],
        ["list", [["int", 1]], "extra"],
    ],
)
def test_encoded_value_errors(encoded):
    assert_corrupt(
        {
            "format": "chronomap",
            "format_version": 1,
            "entries": [{"key": encoded, "versions": [[100, ["int", 1]]]}],
        }
    )


@pytest.mark.persistence
def test_pickle_payload_validation_matches_json():
    payload = {
        "format": "chronomap",
        "format_version": 1,
        "entries": [{"key": ("str", "not-encoded"), "versions": [[100, 1]]}],
    }
    archive = b"CHRONOMAP/1 pickle\n" + pickle.dumps(payload)

    assert items(loads(archive, enable_ttl_cleanup=False)) == {("str", "not-encoded"): 1}


@pytest.mark.persistence
def test_persistence_errors_are_chronomap_errors():
    assert issubclass(PersistenceError, ChronoMapError)
    assert issubclass(CodecError, PersistenceError)
    assert issubclass(CorruptArchiveError, PersistenceError)
    assert issubclass(UnsupportedFormatError, PersistenceError)
