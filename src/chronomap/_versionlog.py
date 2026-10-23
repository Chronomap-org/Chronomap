# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Low-level binary search helpers for per-key version lists.

A version list is a sorted sequence of ``(timestamp, value)`` pairs. Keeping
the search here avoids allocating a parallel timestamp list on every lookup
and lets point reads, range reads, and out-of-order inserts share one
well-tested implementation.
"""

from __future__ import annotations

from typing import Any, List, Sequence, Tuple

Version = Tuple[float, Any]


def bisect_left(versions: Sequence[Version], timestamp: float) -> int:
    """Return the first index whose timestamp is >= ``timestamp``."""
    low = 0
    high = len(versions)
    while low < high:
        middle = (low + high) // 2
        if versions[middle][0] < timestamp:
            low = middle + 1
        else:
            high = middle
    return low


def bisect_right(versions: Sequence[Version], timestamp: float) -> int:
    """Return the insertion index after any timestamp <= ``timestamp``."""
    low = 0
    high = len(versions)
    while low < high:
        middle = (low + high) // 2
        if timestamp < versions[middle][0]:
            high = middle
        else:
            low = middle + 1
    return low


def insertion_point(versions: Sequence[Version], timestamp: float) -> int:
    """Return where a new version should be inserted to stay sorted."""
    return bisect_right(versions, timestamp)


def value_at(versions: Sequence[Version], timestamp: float) -> Tuple[bool, Any]:
    """Return ``(found, value)`` for the newest version at or before ``timestamp``."""
    index = bisect_right(versions, timestamp) - 1
    if index < 0:
        return False, None
    return True, versions[index][1]


def range_versions(
    versions: Sequence[Version],
    start: float,
    end: float,
) -> List[Version]:
    """Return versions whose timestamps fall in the inclusive ``[start, end]`` range."""
    if start > end:
        return []
    return list(versions[bisect_left(versions, start) : bisect_right(versions, end)])


__all__ = ["Version", "bisect_left", "bisect_right", "insertion_point", "range_versions", "value_at"]
