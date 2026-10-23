# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Tests for the CLI TTL inspection and mutation commands."""

import json

import pytest

from chronomap import ChronoMap
from chronomap.cli import main


def _write_state(tmp_path, name="state.json", with_ttl=True, with_plain=True):
    cm = ChronoMap(enable_ttl_cleanup=False)
    if with_ttl:
        cm.put("a", 1, ttl=3600)
    if with_plain:
        cm.put("b", 2)
    path = tmp_path / name
    if path.suffix == ".json":
        cm.save_json(path)
    else:
        cm.save_pickle(path)
    return path


def _load_ttl(path, key):
    return ChronoMap.load_json(path).ttl_remaining(key)


@pytest.mark.cli
def test_ttl_lists_only_tracked_keys_by_default(tmp_path, capsys):
    path = _write_state(tmp_path)

    assert main(["ttl", str(path)]) == 0

    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 1
    assert lines[0].startswith("a: ")
    assert lines[0].endswith("s")


@pytest.mark.cli
def test_ttl_reports_requested_keys_and_preserves_order(tmp_path, capsys):
    path = _write_state(tmp_path)

    assert main(["ttl", str(path), "b", "a", "missing"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()

    assert lines[0] == "b: none"
    assert lines[1].startswith("a: ")
    assert lines[2] == "missing: none"


@pytest.mark.cli
def test_ttl_json_output_and_empty_state(tmp_path, capsys):
    path = _write_state(tmp_path)

    assert main(["ttl", str(path), "a", "b", "missing", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["path"] == str(path)
    assert payload["ttls"]["a"] == pytest.approx(3600, abs=5)
    assert payload["ttls"]["b"] is None
    assert payload["ttls"]["missing"] is None

    plain = _write_state(tmp_path, "plain.json", with_ttl=False, with_plain=False)
    assert main(["ttl", str(plain)]) == 0
    assert "No TTLs tracked" in capsys.readouterr().out
    assert main(["ttl", str(plain), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["ttls"] == {}


@pytest.mark.cli
def test_ttl_parses_numeric_keys_unless_literal(tmp_path, capsys):
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put(1, "one", ttl=60)
    path = tmp_path / "state.pkl"
    cm.save_pickle(path)

    assert main(["ttl", str(path), "1", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["ttls"]["1"] == pytest.approx(60, abs=5)

    assert main(["ttl", str(path), "1", "--literal-key", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["ttls"]["1"] is None


@pytest.mark.cli
def test_expire_sets_ttl_and_saves(tmp_path, capsys):
    path = _write_state(tmp_path, with_ttl=False)

    assert main(["expire", str(path), "b", "120"]) == 0
    assert "Set TTL for b to 120s" in capsys.readouterr().out
    assert _load_ttl(path, "b") == pytest.approx(120, abs=5)

    assert main(["expire", str(path), "b", "30", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["key"] == "b"
    assert payload["ttl"] == 30
    assert payload["expires_in"] == pytest.approx(30, abs=5)
    assert payload["path"] == str(path)


@pytest.mark.cli
def test_expire_rejects_missing_key_and_bad_ttl(tmp_path, capsys):
    path = _write_state(tmp_path, with_ttl=False)
    original = path.read_text()

    assert main(["expire", str(path), "missing", "60"]) == 1
    assert "Key not found or expired" in capsys.readouterr().err
    assert path.read_text() == original

    assert main(["expire", str(path), "b", "0"]) == 1
    assert "TTL" in capsys.readouterr().err
    assert path.read_text() == original


@pytest.mark.cli
def test_persist_removes_ttl_and_saves(tmp_path, capsys):
    path = _write_state(tmp_path)

    assert main(["persist", str(path), "a"]) == 0
    assert "Removed TTL from a" in capsys.readouterr().out
    assert _load_ttl(path, "a") is None

    assert main(["persist", str(path), "a"]) == 1
    assert "no TTL" in capsys.readouterr().err

    path = _write_state(tmp_path, "second.json")
    assert main(["persist", str(path), "a", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"key": "a", "persisted": True, "path": str(path)}


@pytest.mark.cli
def test_persist_rejects_missing_key(tmp_path, capsys):
    path = _write_state(tmp_path, with_ttl=False)

    assert main(["persist", str(path), "missing"]) == 1
    assert "missing" in capsys.readouterr().err
