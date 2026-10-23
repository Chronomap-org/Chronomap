# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import importlib
import subprocess
import sys

import pytest

from chronomap import __version__


@pytest.mark.cli
def test_main_module_is_importable():
    assert importlib.import_module("chronomap.__main__").main is not None


@pytest.mark.cli
def test_python_m_chronomap_runs_the_cli():
    result = subprocess.run(
        [sys.executable, "-m", "chronomap", "--version"],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert __version__ in result.stdout
