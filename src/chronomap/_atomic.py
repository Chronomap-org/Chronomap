# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Atomic file replacement for archive writers."""

from __future__ import annotations

import contextlib
import os
import tempfile
from pathlib import Path


def atomic_write(path: Path, data: bytes) -> None:
    """Write bytes to a temporary file, fsync it, and replace the destination."""
    temporary = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            with contextlib.suppress(OSError):
                os.unlink(temporary)
