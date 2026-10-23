# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import json

import pytest

from chronomap import AsyncChronoMap, ChronoMap, ChronoMapTypeError, ChronoMapValueError


@pytest.mark.core
def test_load_json_reads_utf8_payloads(tmp_path):
    path = tmp_path / "state.json"
    payload = {
        "store": {"café": [[1.0, "naïve"]]},
        "ttl": {},
        "snapshot_time": None,
        "max_history": None,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    cm = ChronoMap.load_json(path, enable_ttl_cleanup=False)

    assert cm["café"] == "naïve"


@pytest.mark.core
def test_save_json_roundtrips_unicode_values(tmp_path):
    path = tmp_path / "state.json"
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("café", "naïve")

    cm.save_json(path)
    restored = ChronoMap.load_json(path, enable_ttl_cleanup=False)

    assert restored["café"] == "naïve"


@pytest.mark.core
def test_load_json_rejects_missing_store(tmp_path):
    path = tmp_path / "state.json"
    path.write_text('{"ttl": {}}', encoding="utf-8")

    with pytest.raises(ChronoMapValueError):
        ChronoMap.load_json(path)


@pytest.mark.core
def test_load_json_rejects_non_object_payload(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("[]", encoding="utf-8")

    with pytest.raises(ChronoMapTypeError):
        ChronoMap.load_json(path)


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_load_json_rejects_missing_store(tmp_path):
    path = tmp_path / "state.json"
    path.write_text('{"ttl": {}}', encoding="utf-8")

    with pytest.raises(ChronoMapValueError):
        await AsyncChronoMap.load_json(path)


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_load_json_rejects_non_object_payload(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("[]", encoding="utf-8")

    with pytest.raises(ChronoMapTypeError):
        await AsyncChronoMap.load_json(path)
