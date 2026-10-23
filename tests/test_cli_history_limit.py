# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import pytest

from chronomap.cli import main


@pytest.mark.cli
def test_history_validates_limit_before_loading_the_file(tmp_path, capsys):
    missing = tmp_path / "missing.json"

    assert main(["history", str(missing), "alpha", "--limit", "0"]) == 1

    stderr = capsys.readouterr().err
    assert "--limit" in stderr
    assert "File not found" not in stderr
