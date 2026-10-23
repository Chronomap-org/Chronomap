# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""from_dict() enforces the storage invariants used by live maps."""

import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap.exceptions import ChronoMapValueError


@pytest.mark.core
def test_from_dict_rejects_empty_history():
    with pytest.raises(ChronoMapValueError, match="must not be empty"):
        ChronoMap.from_dict({"store": {"key": []}, "ttl": {}})
    with pytest.raises(ChronoMapValueError, match="must not be empty"):
        ChronoMap.from_dict({"store": {"key": ()}, "ttl": {}})


@pytest.mark.core
def test_from_dict_rejects_ttl_without_stored_key():
    with pytest.raises(ChronoMapValueError, match="has no stored history"):
        ChronoMap.from_dict({"store": {}, "ttl": {"key": 2_000_000_000}})


@pytest.mark.core
def test_from_dict_trims_to_payload_max_history():
    cm = ChronoMap.from_dict(
        {
            "store": {"key": [[1.0, 1], [2.0, 2], [3.0, 3]]},
            "ttl": {},
            "max_history": 2,
        }
    )

    assert cm.history("key") == [(2.0, 2), (3.0, 3)]


@pytest.mark.core
def test_from_dict_explicit_max_history_overrides_and_trims():
    payload = {"store": {"key": [[1.0, 1], [2.0, 2], [3.0, 3]]}, "ttl": {}}

    cm = ChronoMap.from_dict(payload, max_history=1)
    unlimited = ChronoMap.from_dict(payload, max_history=None)

    assert cm.history("key") == [(3.0, 3)]
    assert unlimited.history("key") == [(1.0, 1), (2.0, 2), (3.0, 3)]


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_from_dict_rejects_empty_history_and_orphan_ttl():
    with pytest.raises(ChronoMapValueError, match="must not be empty"):
        AsyncChronoMap.from_dict({"store": {"key": []}, "ttl": {}})
    with pytest.raises(ChronoMapValueError, match="has no stored history"):
        AsyncChronoMap.from_dict({"store": {}, "ttl": {"key": 2_000_000_000}})


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_from_dict_trims_to_max_history():
    cm = AsyncChronoMap.from_dict(
        {
            "store": {"key": [[1.0, 1], [2.0, 2], [3.0, 3]]},
            "ttl": {},
            "max_history": 2,
        }
    )

    assert await cm.history("key") == [(2.0, 2), (3.0, 3)]
