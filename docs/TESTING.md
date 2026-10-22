# Testing

ChronoMap uses pytest for behavior tests, coverage for statement coverage,
black for formatting, and mypy for type checking. The same commands run in
CI, so a clean local run is a good preview of a pull request.

## Set up the environment

From the repository root:

```bash
python -m venv .venv
```

Activate it:

```bash
# Windows PowerShell
.venv\Scripts\Activate.ps1

# macOS / Linux
source .venv/bin/activate
```

Install the package in editable mode with the development and pandas extras:

```bash
pip install -e ".[dev,pandas]"
```

`dev` installs pytest, pytest-asyncio, pytest-cov, black, and mypy. `pandas`
is optional at runtime but the test suite covers `to_dataframe()`, so CI
installs it.

To verify the interpreter:

```bash
python --version
python -c "import chronomap; print(chronomap.__version__)"
```

The project supports Python 3.8 and newer. CI runs the full suite on 3.8
through 3.14 on Linux, plus Python 3.12 on Windows and macOS.

## Run the suite

```bash
pytest tests/ -v
```

Run with the coverage report used by CI:

```bash
pytest tests/ --cov=chronomap --cov-report=term-missing
```

`main` reports **100% statement coverage**. The test count changes as features
are added; use `pytest tests/ --collect-only -q` for the current number. New
behavior comes with tests and must not lower coverage.

Useful variants while working:

```bash
pytest tests/ -x -vv              # stop on the first failure
pytest tests/ --lf                # rerun the last failed tests
pytest tests/ --ff                # failed first, then the rest
pytest tests/ -k "ttl or expired" # select by name expression
pytest tests/ -s                  # do not capture stdout/stderr
pytest tests/ --pdb               # drop into the debugger on failure
```

Run one file, class, or test:

```bash
pytest tests/test_basic.py -v
pytest tests/test_chronomap.py::TestTTL -v
pytest tests/test_basic.py::test_time_travel -vv
```

## Test layout

| File | Focus |
| --- | --- |
| `tests/test_basic.py` | Small smoke tests for core reads, writes, history, TTL, snapshots, and async basics. |
| `tests/test_chronomap.py` | The main behavior suite: storage, cache, pruning, TTL, memory, batch operations, queries, snapshots, merge, magic methods, persistence, concurrency, async, and integration. |
| `tests/test_coverage_gaps.py` | Edge and defensive branches: cache eviction, lock ordering, memory estimate edge cases, cleanup-thread lifecycle, strict reads, invalid inputs, expiry races, merge TTLs, async validation, and CLI entry paths. |
| `tests/test_core_correctness.py` | Regression coverage for validation, batch operations, delete/prune semantics, callback behavior, locking, serialization, and merge/diff edge cases. |
| `tests/test_async_parity.py` | `AsyncChronoMap` parity: validation, cache, batch/conditional operations, history, TTL, snapshots, callbacks, stats, and persistence. |
| `tests/test_async_wal.py` | `AsyncChronoMap` WAL integration: replay of every mutation, TTL purge recovery, checkpoints, auto-checkpoint failures, torn tails, corruption, `from_dict()` replacement, close/destructor behavior, and subprocess crash recovery. |
| `tests/test_async_serialization_validation.py` | Async `from_dict()` validation for timestamps, sorted histories, TTL deadlines, snapshot time, and normalization. |
| `tests/test_cli.py` | CLI parsing, formatting, color helpers, and file display. |
| `tests/test_cli_commands.py` | Expanded CLI commands: show/get/history/put/delete/keys/stats/prune/diff/merge, JSON output, and file handling. |
| `tests/test_persistence.py` | Archive formats, typed JSON codec, pickle, compression, inspection, TTL preservation, and corruption handling. |
| `tests/test_query.py` | Fluent value queries, version queries, filters, reducers, and time-travel semantics. |
| `tests/test_journal.py` | WAL framing, CRC validation, torn writes, sequence ordering, streaming record consumers, checkpoint skipping, checkpoints, and directory fsync helpers. |
| `tests/test_wal.py` | `ChronoMap` WAL integration: replay of every mutation, checkpoints, TTL recovery, corruption handling, close/context management, and `from_dict()` imports. |
| `tests/test_wal_crash.py` | Subprocess writers that call `os._exit()` immediately after a write and after a checkpoint, then verify recovery. |
| `tests/test_wal_lock.py` | Cross-process WAL ownership: sync/async contention, checkpoint-safe locks, subprocess holders, crash release, and failed-open cleanup. |
| `tests/test_pickle_fallback.py` | Compressed pickle loading when the raw file cannot be parsed as a pickle, for both map implementations. |
| `tests/test_memory_guardrails.py` | Async memory ceilings, memory-usage reporting, and parity across every guarded write path. |
| `tests/test_module_entry.py` | Importing the `chronomap.__main__` entrypoint. |

Tests use pytest fixtures such as `tmp_path` and `capsys`, not a project-wide
fixture module. Each test creates its own map, which keeps tests independent
and makes ordering irrelevant.

## Markers

`pyproject.toml` registers the semantic markers:

| Marker | Meaning |
| --- | --- |
| `core` | Core read/write/query/merge/history behavior |
| `cache` | `LRUCache` behavior |
| `lock` | `RWLock` and concurrency behavior |
| `memory` | Memory monitor and size estimation |
| `ttl` | TTL expiry and cleanup thread |
| `snapshot` | Snapshot, rollback, and diff |
| `async_map` | `AsyncChronoMap` behavior |
| `cli` | CLI behavior |
| `persistence` | Archive, codec, and WAL behavior |
| `query` | Fluent value and version queries |

Markers drive the CI subsystem job. A test can carry more than one semantic
marker, so marker selections overlap and do not partition the full suite. Use
`pytest tests/ --collect-only -q -m core` to inspect a selection.

Run one subsystem:

```bash
pytest tests/ -v -m core
pytest tests/ -v -m ttl
pytest tests/ -v -m "snapshot or lock"
```

When adding a new area, register its marker in `pyproject.toml` and add a
matching path filter in `.github/workflows/tests.yml`. Existing filters map
source files to `core`, `cache`, `lock`, `memory`, `ttl`, `snapshot`,
`async_map`, `persistence`, `cli`, and `query`.

## Async tests

`pytest-asyncio` is installed and `asyncio_mode = "auto"` is set in
`pyproject.toml`. Async tests may also use explicit `@pytest.mark.asyncio`
markers; that works in auto mode and makes the intent obvious.

```python
import pytest

from chronomap import AsyncChronoMap


@pytest.mark.asyncio
async def test_async_put_get():
    cm = AsyncChronoMap()
    await cm.put("key", "value")
    assert await cm.get("key") == "value"
```

Do not share an `AsyncChronoMap` across event loops. Create it inside the test
or inside the loop that uses it.

## Writing reliable tests

### Use explicit timestamps

Most behavior does not need wall-clock time. Pass explicit timestamps so tests
are deterministic:

```python
cm = ChronoMap(enable_ttl_cleanup=False)
cm.put("key", "old", timestamp=100)
cm.put("key", "new", timestamp=200)

assert cm.get("key", timestamp=150) == "old"
assert cm.get("key") == "new"
```

### Disable background work when it is not under test

Background TTL cleanup and lazy cache state can make assertions timing
dependent. For ordinary read/write tests, start with:

```python
cm = ChronoMap(enable_ttl_cleanup=False, cache_size=0)
```

Turn those features back on in the tests that specifically cover them.

### Test TTL with short intervals

TTL tests need real time to pass. Use a short TTL and a short sleep, or call
`clean_expired_keys()` explicitly:

```python
import time

cm = ChronoMap(enable_ttl_cleanup=False)
cm.put("temporary", "value", ttl=0.01)
time.sleep(0.02)

assert cm.get("temporary") is None
```

Prefer the smallest sleep that is reliable on CI. Do not depend on the
background thread firing at an exact moment; call `clean_expired_keys()` when
the test is about sweeping rather than scheduling.

### Test public behavior, not internals

Assert through the public API where possible. Tests in
`test_coverage_gaps.py` reach into private helpers only to exercise defensive
branches that are otherwise unreachable; new features should not need that.

### Cover the edges

A new operation should have tests for:

- the normal case
- a missing key
- an expired key, when TTL applies
- a timestamp before the first version, when history applies
- invalid inputs, if the operation validates them
- concurrency or async interleaving, if the operation shares state

### Keep regression tests honest

A regression test should fail on the old behavior and pass on the fix. If you
cannot make it fail first, the test may be asserting something weaker than the
bug.

## Formatting and type checking

Check formatting:

```bash
black --check src/ tests/
```

Apply formatting:

```bash
black src/ tests/
```

The project uses a 110-character line length and targets Python 3.8 syntax.

Run mypy:

```bash
mypy src/chronomap --ignore-missing-imports
```

`main` is clean under mypy, and CI fails the build on any new error. Run mypy
on the files you touch before opening a pull request.

## What CI runs

The test-related jobs in `.github/workflows/tests.yml` are:

1. **Detect changed subsystems**: maps changed paths to markers.
2. **Subsystem tests**: runs only the selected marker groups on Python 3.12.
3. **Full suite**: runs all tests with coverage on Python 3.8, 3.9, 3.10,
   3.11, 3.12, 3.13, and 3.14. The 3.12 run uploads `coverage.xml`.
4. **Platform suite**: runs the complete suite with a 100% coverage gate on
   Windows and macOS with Python 3.12, exercising both WAL lock backends.
5. **Formatting**: `black --check src/ tests/` is required.
6. **Type checking**: `mypy src/chronomap --ignore-missing-imports` is required.

The subsystem job is a fast feedback loop for small changes. The full-suite
job is the source of truth, especially when a change crosses modules.

## Before opening a pull request

```bash
pytest tests/ --cov=chronomap --cov-report=term-missing
black --check src/ tests/
mypy src/chronomap --ignore-missing-imports
```

Then confirm:

- the new tests fail on the old code and pass on the new code
- no unrelated files were reformatted
- no test depends on ordering, wall-clock precision, or network access
- a new marker and CI path filter were added if you introduced a subsystem
- public API changes are reflected in `docs/API.md`

See `CONTRIBUTING.md` for the review and PR conventions.
