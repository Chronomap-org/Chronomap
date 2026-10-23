# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import json
import math

import pytest

from chronomap import ChronoMap
from chronomap.persistence import CorruptArchiveError, dumps, loads


def _archive_with_created_at(value):
    cm = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    cm.put("a", 1)
    data = dumps(cm, format="json")
    header, body = data.split(b"\n", 1)
    payload = json.loads(body)
    payload["created_at"] = value
    return header + b"\n" + json.dumps(payload).encode("utf-8")


@pytest.mark.persistence
@pytest.mark.parametrize("value", ["yesterday", True, math.inf, math.nan])
def test_loads_rejects_invalid_archive_created_at(value):
    with pytest.raises(CorruptArchiveError, match="created_at must be (numeric|finite)"):
        loads(_archive_with_created_at(value))


@pytest.mark.persistence
def test_loads_accepts_numeric_archive_created_at():
    restored = loads(_archive_with_created_at(123.5))

    assert restored["a"] == 1


@pytest.mark.persistence
def test_loads_accepts_a_missing_archive_created_at():
    restored = loads(_archive_with_created_at(None))

    assert restored["a"] == 1
