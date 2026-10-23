# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Context manager for automatic rollback on exception.

Split into its own module for import clarity, but it's tightly coupled to
ChronoMap by nature (it calls .snapshot() and .rollback() on whatever
object it's given) so it only type-checks against ChronoMap, it doesn't
import it at runtime.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional

if TYPE_CHECKING:
    from .core import ChronoMap


class SnapshotContext:
    """Use as `with cm.snapshot_context(): ...` for auto-rollback on exception."""

    def __init__(self, chronomap: "ChronoMap") -> None:
        self.chronomap = chronomap
        self.snapshot: Optional["ChronoMap"] = None

    def __enter__(self) -> "ChronoMap":
        self.snapshot = self.chronomap.snapshot()
        return self.chronomap

    def __exit__(self, exc_type, exc_val, exc_tb) -> Literal[False]:
        if exc_type is not None:
            assert self.snapshot is not None
            self.chronomap.rollback(self.snapshot)
        return False


__all__ = ["SnapshotContext"]
