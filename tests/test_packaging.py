# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

from importlib import resources

import pytest


@pytest.mark.core
def test_chronomap_ships_a_py_typed_marker():
    assert resources.read_text("chronomap", "py.typed") == ""
