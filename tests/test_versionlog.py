# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import pytest

from chronomap._versionlog import (
    bisect_left,
    bisect_right,
    insertion_point,
    range_versions,
    value_at,
)


@pytest.fixture
def versions():
    return [(10.0, "a"), (20.0, "b"), (20.0, "c"), (30.0, "d")]


@pytest.mark.core
def test_bisect_helpers_find_inclusive_boundaries(versions):
    assert bisect_left(versions, 5.0) == 0
    assert bisect_left(versions, 20.0) == 1
    assert bisect_left(versions, 25.0) == 3
    assert bisect_left(versions, 35.0) == 4

    assert bisect_right(versions, 5.0) == 0
    assert bisect_right(versions, 20.0) == 3
    assert bisect_right(versions, 25.0) == 3
    assert bisect_right(versions, 35.0) == 4

    assert insertion_point(versions, 20.0) == 3
    assert insertion_point([], 1.0) == 0


@pytest.mark.core
def test_value_at_selects_the_newest_visible_version(versions):
    assert value_at(versions, 5.0) == (False, None)
    assert value_at(versions, 20.0) == (True, "c")
    assert value_at(versions, 25.0) == (True, "c")
    assert value_at(versions, 35.0) == (True, "d")
    assert value_at([], 1.0) == (False, None)


@pytest.mark.core
def test_range_versions_returns_inclusive_slices(versions):
    assert range_versions(versions, 10.0, 10.0) == [(10.0, "a")]
    assert range_versions(versions, 20.0, 20.0) == [(20.0, "b"), (20.0, "c")]
    assert range_versions(versions, 15.0, 25.0) == [(20.0, "b"), (20.0, "c")]
    assert range_versions(versions, 25.0, 15.0) == []
    assert range_versions([], 0.0, 1.0) == []
