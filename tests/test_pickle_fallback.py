# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import pickle

import pytest

from chronomap import AsyncChronoMap, ChronoMap

pytestmark = [pytest.mark.persistence]


def fail_on_raw_compressed_pickle(monkeypatch):
    real_loads = pickle.loads

    def loads(data):
        if data.startswith(b"gzip|"):
            raise UnicodeDecodeError("utf-8", data, 0, 1, "not UTF-8")
        return real_loads(data)

    monkeypatch.setattr(pickle, "loads", loads)


def test_load_pickle_falls_back_when_pickle_parsing_fails(monkeypatch, tmp_path):
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("key", "value", timestamp=10)
    path = tmp_path / "state.pkl"
    cm.save_pickle(path, compress="gzip")

    fail_on_raw_compressed_pickle(monkeypatch)
    restored = ChronoMap.load_pickle(path, enable_ttl_cleanup=False)
    try:
        assert restored.get("key") == "value"
    finally:
        restored.close()


@pytest.mark.async_map
async def test_async_load_pickle_falls_back_when_pickle_parsing_fails(monkeypatch, tmp_path):
    cm = AsyncChronoMap(enable_ttl_cleanup=False)
    await cm.put("key", "value", timestamp=10)
    path = tmp_path / "state.pkl"
    await cm.save_pickle(path, compress="gzip")

    fail_on_raw_compressed_pickle(monkeypatch)
    restored = await AsyncChronoMap.load_pickle(path)
    try:
        assert await restored.get("key") == "value"
    finally:
        await restored.aclose()
