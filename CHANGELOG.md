# Changelog

All notable changes to this project are documented here.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added
- Pytest markers (`core`, `cache`, `lock`, `memory`, `ttl`, `snapshot`,
  `async_map`, `cli`) applied across the whole test suite, so each
  subsystem's tests can be run and reported on independently
  (`pytest -m cache`, etc.) instead of only ever seeing one aggregate
  pass/fail per Python version.
- `.github/workflows/tests.yml`: new `changes` job (path-based diff
  detection) and `modules` job that runs and reports each touched
  subsystem's tests as its own named check on a PR — e.g. a PR that
  only touches `_cache.py` shows a `Subsystem: cache` check, and
  untouched subsystems simply don't run. The full cross-version suite
  (`test` job, Python 3.8–3.12) still always runs regardless, as the
  actual correctness gate — the subsystem jobs are for visibility and
  speed, not a replacement for it.
- Copyright/license header on every file under `src/` and `tests/`:
  `Copyright (c) 2026 Devansh Singh, ChronoMap contributors`, SPDX
  identifier, and a link back to the repo.
- Community health files: `SECURITY.md`, issue templates
  (`bug_report.yml`, `feature_request.yml`), `PULL_REQUEST_TEMPLATE.md`,
  and `CODEOWNERS`.
- `.github/workflows/monthly-release.yml` — scheduled monthly
  automation that cuts a dated GitHub Release (using GitHub's native
  contributor/new-contributor notes) and opens a review PR that adds a
  matching, name-free `CHANGELOG.md` entry and bumps `_version.py`.
- `scripts/build_changelog_entry.py` — builds the name-free CHANGELOG
  section from merged PRs in a date window, grouped by label
  (Added/Changed/Fixed/Documentation/Maintenance).

### Changed
- `LICENSE` copyright line updated to `Devansh Singh, ChronoMap
  contributors` to match the new file headers.
- `.github/workflows/tests.yml` — added a concurrency group (cancels
  stale runs), explicit read-only `permissions`, coverage upload, and a
  new `mypy` type-check job (non-blocking for now).
- Split the single-file `chronomap.py` module into a proper package
  (`src/chronomap/`): `core.py`, `asynchronous.py`, `_cache.py`,
  `_lock.py`, `_memory.py`, `_ttl_cleanup.py`, `_snapshot.py`,
  `exceptions.py`. Public API (`from chronomap import ChronoMap`)
  is unchanged.
- Replaced deprecated `datetime.utcnow()` calls with
  `datetime.now(timezone.utc)`.

### Added
- `chronomap/cli.py` — this was referenced in the old project structure
  and README but didn't exist in the code I had to work with. Written
  from scratch to satisfy the existing `tests/test_cli.py`: `parse_value`,
  `format_timestamp`, `colorize`, `load_and_display`, plus a small
  `show` subcommand for inspecting saved state files.
- Wired in the real `tests/test_chronomap.py` (170+ tests) and
  `tests/test_cli.py`. All pass against the restructured package
  unmodified — the public import surface didn't change.
- `tests/test_basic.py` — a small additional smoke-test file, mainly
  useful as the home for the `merge()` regression test below.
- `.github/workflows/tests.yml` — runs the full suite on Python 3.8–3.12
  on every push/PR to `main`, plus a `black --check` formatting job.
- `.github/dependabot.yml` — keeps GitHub Actions and pip dependencies
  from going stale.
- Applied `black` formatting to the whole package (`src/`, `tests/`) so
  the new formatting CI job actually passes on the first commit instead
  of failing immediately.

### Fixed
- **`ChronoMap.merge(strategy='timestamp')` was broken.** The out-of-order
  insert branch referenced an undefined variable (`value` instead of
  `val`) and inserted into the wrong list (`versions`, the *source* data
  from the other map, instead of `target_versions`). Any merge where a
  key's timestamps interleaved out of order would raise `NameError` and
  could corrupt the source map's history in the process. Added a
  regression test (`test_merge_timestamp_strategy_does_not_crash_on_out_of_order_writes`).

### Current test status
246 tests passing, 100% coverage (`pytest tests/ --cov=chronomap`). One
line in `_lock.py` is marked `# pragma: no cover` rather than covered
with a timing-dependent test — see the comment there for why.

### Fixed (found while closing coverage gaps)
- **Real race condition in the TTL cleanup thread.** `_cleanup_loop`
  briefly holds the only strong reference to its owning `ChronoMap` (via
  the weakref). If the main thread drops its own reference at exactly
  that moment, `del cm` inside the loop is what takes the refcount to
  zero — *on the background thread*. That runs `ChronoMap.__del__`
  there, which calls `stop()`, which used to try to `.join()` the
  thread it was currently executing on and raise `RuntimeError: cannot
  join current thread`. `stop()` now skips the join when it's already
  being called from the thread it would join. Regression test:
  `test_ttl_cleanup_thread_stop_does_not_crash_when_del_runs_on_itself`.
- **Dead code in `cli.py`.** The subparser was `required=True`, which
  means argparse itself exits before the code ever reaches the
  "no subcommand given" fallback (`parser.print_help(); return 1`) —
  so that branch was unreachable and untested. Changed to handle the
  missing-subcommand case in our own code instead of leaving
  argparse's default (also gives a more consistent error path).

### Added (coverage work)
- `tests/test_coverage_gaps.py` — targets branches the main suites
  don't reach: `strict=True` paths only reachable via direct private-method
  calls, double-checked-expiry race guards, TTL cleanup thread internals,
  RWLock contention, and the two bugs above.

## [2.1.0] - 2026-10-21
- Last release under the single-file layout. See git history for details
  predating this changelog.
