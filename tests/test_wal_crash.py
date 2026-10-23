# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from chronomap import ChronoMap


def run_crash_writer(path, body):
    root = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    source_path = str(root / "src")
    existing_path = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = source_path + os.pathsep + existing_path if existing_path else source_path
    script = (
        "import os\n"
        "import sys\n"
        "\n"
        "from chronomap import ChronoMap\n"
        "\n"
        "cm = ChronoMap(wal_path=sys.argv[1], enable_ttl_cleanup=False)\n"
        + textwrap.dedent(body).strip()
        + "\n"
        "os._exit(0)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(path)],
        capture_output=True,
        text=True,
        env=environment,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.persistence
def test_wal_survives_an_abrupt_process_exit(tmp_path):
    path = tmp_path / "state.wal"
    run_crash_writer(
        path,
        'cm.put("acked", {"value": 1}, timestamp=10)',
    )

    recovered = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    try:
        assert recovered.get("acked") == {"value": 1}
    finally:
        recovered.close()


@pytest.mark.persistence
def test_wal_survives_an_abrupt_exit_after_checkpoint(tmp_path):
    path = tmp_path / "state.wal"
    run_crash_writer(
        path,
        'cm.put("before", 1, timestamp=10)\ncm.checkpoint()\ncm.put("after", 2, timestamp=20)',
    )

    recovered = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    try:
        assert recovered.get("before") == 1
        assert recovered.get("after") == 2
    finally:
        recovered.close()
