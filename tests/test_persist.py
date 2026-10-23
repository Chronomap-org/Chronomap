# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import time

import pytest

from chronomap import ChronoMap, ChronoMapTypeError


@pytest.mark.core
def test_persist_removes_a_live_ttl():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("session", "active", ttl=0.05)

    assert cm.persist("session") is True
    assert "session" not in cm._ttl

    time.sleep(0.1)
    assert cm["session"] == "active"


@pytest.mark.core
def test_persist_returns_false_without_a_ttl():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("session", "active")

    assert cm.persist("session") is False
    assert cm.persist("missing") is False


@pytest.mark.core
def test_persist_purges_an_expired_key():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("session", "active", ttl=0.01)
    time.sleep(0.02)

    assert cm.persist("session") is False
    assert "session" not in cm._store
    assert "session" not in cm._ttl


@pytest.mark.core
def test_persist_rejects_unhashable_keys():
    cm = ChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapTypeError):
        cm.persist(["not", "hashable"])
