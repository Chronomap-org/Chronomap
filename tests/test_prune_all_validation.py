# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""prune_all_history() validates arguments even when there is nothing to prune."""

import time

import pytest

from chronomap import AsyncChronoMap, ChronoMap
from chronomap.exceptions import ChronoMapTypeError, ChronoMapValueError


@pytest.mark.core
@pytest.mark.parametrize("bad_keep", [-1, True, 1.5, "2"])
def test_empty_map_rejects_invalid_keep_last(bad_keep):
    cm = ChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapValueError):
        cm.prune_all_history(keep_last=bad_keep)


@pytest.mark.core
def test_empty_map_rejects_invalid_cutoff():
    cm = ChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapValueError):
        cm.prune_all_history(older_than="not-a-date")
    with pytest.raises(ChronoMapTypeError):
        cm.prune_all_history(older_than=True)


@pytest.mark.core
def test_expired_only_map_still_validates():
    cm = ChronoMap(enable_ttl_cleanup=False)
    cm.put("expired", 1, ttl=0.01)
    time.sleep(0.02)

    with pytest.raises(ChronoMapValueError):
        cm.prune_all_history(keep_last=-1)


@pytest.mark.core
def test_valid_arguments_on_empty_map_return_zero():
    cm = ChronoMap(enable_ttl_cleanup=False)

    assert cm.prune_all_history(keep_last=1, older_than=100) == 0


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_empty_map_rejects_invalid_arguments():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)

    with pytest.raises(ChronoMapValueError):
        await cm.prune_all_history(keep_last=-1)
    with pytest.raises(ChronoMapValueError):
        await cm.prune_all_history(older_than="not-a-date")
    with pytest.raises(ChronoMapTypeError):
        await cm.prune_all_history(older_than=True)


@pytest.mark.async_map
@pytest.mark.asyncio
async def test_async_valid_arguments_on_empty_map_return_zero():
    cm = AsyncChronoMap(enable_ttl_cleanup=False)

    assert await cm.prune_all_history(keep_last=1, older_than=100) == 0
