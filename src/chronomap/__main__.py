# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Allow ``python -m chronomap`` to run the command-line interface."""

from .cli import main

if __name__ == "__main__":  # pragma: no cover - exercised via subprocess
    raise SystemExit(main())
