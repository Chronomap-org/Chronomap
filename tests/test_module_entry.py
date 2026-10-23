# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import importlib

import pytest


@pytest.mark.cli
def test_main_module_is_importable():
    assert importlib.import_module("chronomap.__main__").main is not None
