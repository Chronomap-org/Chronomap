# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""get_or_set() factories may safely use the same map."""

import pytest

from chronomap import ChronoMap


@pytest.mark.core
def test_factory_can_read_and_write_the_map():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("seed", 10)

    def factory():
        assert cm.get("seed") == 10
        cm.put("factory_side_effect", True)
        return cm.get("seed") + 1

    assert cm.get_or_set("created", factory) == 11
    assert cm.get("factory_side_effect") is True


@pytest.mark.core
def test_factory_created_target_wins():
    cm = ChronoMap(enable_ttl_cleanup=False)

    def factory():
        cm.put("key", "factory-made")
        return "discarded"

    assert cm.get_or_set("key", factory) == "factory-made"
    assert cm.history("key")[0][1] == "factory-made"


@pytest.mark.core
def test_factory_is_not_called_for_existing_key():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("key", "existing")

    def factory():
        raise AssertionError("factory should not run")

    assert cm.get_or_set("key", factory) == "existing"


@pytest.mark.core
def test_factory_result_keeps_ttl_and_change_callback():
    cm = ChronoMap(enable_ttl_cleanup=False)
    changes = []
    cm.on_change(lambda key, old, new, ts: changes.append((key, old, new)))

    assert cm.get_or_set("key", lambda: "value", ttl=60) == "value"
    assert cm.ttl_remaining("key") is not None
    assert changes == [("key", None, "value")]
