# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""save_json() and save_pickle() never leave a partial destination."""

import pytest

from chronomap import AsyncChronoMap, ChronoMap


@pytest.mark.persistence
def test_save_json_keeps_existing_file_on_serialization_error(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("previous", encoding="utf-8")
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("bad", {1, 2})

    with pytest.raises(TypeError):
        cm.save_json(path)

    assert path.read_text(encoding="utf-8") == "previous"
    assert list(tmp_path.glob(".state.json.*")) == []


@pytest.mark.persistence
def test_save_pickle_keeps_existing_file_on_serialization_error(tmp_path):
    path = tmp_path / "state.pkl"
    path.write_bytes(b"previous")
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("bad", lambda: None)

    with pytest.raises(Exception):
        cm.save_pickle(path)

    assert path.read_bytes() == b"previous"
    assert list(tmp_path.glob(".state.pkl.*")) == []


@pytest.mark.persistence
def test_save_json_cleans_up_when_replace_fails(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    path.write_text("previous", encoding="utf-8")
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("key", "value")

    def fail_replace(source, destination):
        raise OSError("replace failed")

    monkeypatch.setattr("chronomap._atomic.os.replace", fail_replace)

    with pytest.raises(OSError, match="replace failed"):
        cm.save_json(path)

    assert path.read_text(encoding="utf-8") == "previous"
    assert list(tmp_path.glob(".state.json.*")) == []


@pytest.mark.persistence
@pytest.mark.asyncio
async def test_async_save_json_keeps_existing_file_on_serialization_error(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("previous", encoding="utf-8")
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("bad", {1, 2})

    with pytest.raises(TypeError):
        await cm.save_json(path)

    assert path.read_text(encoding="utf-8") == "previous"
    assert list(tmp_path.glob(".state.json.*")) == []


@pytest.mark.persistence
@pytest.mark.asyncio
async def test_async_save_pickle_keeps_existing_file_on_serialization_error(tmp_path):
    path = tmp_path / "state.pkl"
    path.write_bytes(b"previous")
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("bad", lambda: None)

    with pytest.raises(Exception):
        await cm.save_pickle(path)

    assert path.read_bytes() == b"previous"
    assert list(tmp_path.glob(".state.pkl.*")) == []
