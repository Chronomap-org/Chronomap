# Contributing to ChronoMap

Thanks for considering it. This is a small project, so the process is
intentionally light.

## Before opening a PR

- Add or update a test for whatever you changed. If you fixed a bug,
  a regression test that fails on `main` and passes on your branch is
  the single most useful thing you can include.
- Run `pytest tests/ -v`, `black --check src/ tests/`, and
  `mypy src/chronomap --ignore-missing-imports` locally.
- Keep PRs scoped to one thing. A 500-line PR that fixes a bug _and_
  reformats three unrelated files is much harder to review than two
  small PRs.

## What's useful right now

- Adding focused regression tests for edge cases in TTLs, WAL recovery,
  snapshots, and async cancellation.
- Filling out `docs/API.md` and `docs/COOKBOOK.md` with real examples.
- Anything in open issues tagged `good-first-issue`.

## Code style

- Type hints on public methods.
- Docstrings that explain _why_, not just restate the signature.
- Keep statement coverage at 100%; run
  `pytest tests/ --cov=chronomap --cov-fail-under=100`.
- No new required dependencies for the core package. Optional dependencies
  (like pandas) belong behind `pip install chronomap[extra]`.

## Reporting bugs

Open an issue with a minimal reproduction. "It doesn't work" without a
snippet is hard to act on; a 5-line script that shows the wrong behavior
gets fixed a lot faster.
