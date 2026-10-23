# Changelog

All notable changes to this project are documented here.
Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added
- **TTL refresh and batch removal helpers.** `ttl_remaining()` reports the
  remaining lifetime, `expire()` refreshes a live key's TTL without writing a
  new version, and `pop_many()` removes several keys under one lock and returns
  their latest values. Both map implementations expose the new methods.
- **Readable async maps.** `AsyncChronoMap.__repr__` previews up to ten live
  keys without requiring an await.
- **Memory guardrails for `AsyncChronoMap`.** The async constructor now accepts
  `max_memory_mb` with the same 80% warning and 100% `ChronoMapMemoryError`
  behavior as the synchronous map. The check covers puts, batch writes,
  `get_or_set()`/`setdefault()` creation, and merges, and runs after the
  journal append while the `asyncio.Lock` is held. Both map implementations
  also expose `memory_usage()` / `await memory_usage()` for an approximate byte
  count of stored histories and TTL metadata.
- **Durable write-ahead journal for `ChronoMap`.** `ChronoMap(wal_path=...)`
  replays an append-only WAL on startup, with `wal_sync=True` fsyncing each
  record before the in-memory mutation and `wal_auto_checkpoint_bytes`
  compacting into an atomic checkpoint (default 64 MiB). New `checkpoint()`,
  `close()`, and context-manager support; recovery discards a torn tail and
  rejects complete records that fail CRC or sequence validation.
- **WAL support for `AsyncChronoMap`.** The async map accepts the same
  `wal_path`, `wal_sync`, and `wal_auto_checkpoint_bytes` options, replays
  checkpoints and journals during construction, and journals every mutation,
  expired-key purge, rollback, merge, clear, and `from_dict()` replacement.
  Journal appends, checkpoint writes, and close run in the default executor
  while the `asyncio.Lock` preserves ordering. New `await checkpoint()`,
  `await aclose()`, and `async with` lifecycle support. Async `from_dict()`
  now validates version timestamps, sorted histories, TTL deadlines, and
  `snapshot_time` like the synchronous implementation.
- **Streaming WAL recovery.** Journal reopening can now pass
  `retain_records=False`, `skip_through=checkpoint_sequence`, and a
  `record_consumer`; records are validated and applied one at a time instead
  of being accumulated in `Journal.records`. Both `ChronoMap` and
  `AsyncChronoMap` use this path, so recovery memory is bounded by the
  restored state plus a single record. Records covered by a checkpoint are
  still validated and sequenced but are not delivered to the consumer.
- **Cross-process WAL ownership.** A `Journal` now holds an exclusive,
  OS-backed advisory lock on `<wal_path>.lock` for its lifetime, so a second
  live map on the same path raises `JournalLockError` instead of interleaving
  writes. The operating system releases the lock on close or process exit,
  including abrupt exits. The sidecar file remains on disk; it carries no
  state beyond the lock.
- `tests/test_async_wal.py` and `tests/test_async_serialization_validation.py`
  cover replay of every async mutation, TTL purge recovery, checkpoints,
  auto-checkpoint failures, torn tails, corrupt journals/checkpoints,
  `from_dict()` replacement/failure paths, closed-journal behavior, stats,
  destructor safety, and abrupt subprocess exits with and without a
  checkpoint.
- `Journal`, `JournalError`, `JournalCorruptionError`, and
  `JournalLockError` exports for callers that need the CRC-framed record
  layer directly.
- Checkpoint files are length- and CRC32-framed; truncated, extended, or
  checksum-corrupted checkpoints raise `JournalCorruptionError`. Journal
  resets use a temporary header and atomic rename, and failed automatic
  compactions are logged and leave the journal intact instead of failing the
  triggering write.
- WAL records cover puts, batch writes, deletes, TTL changes, pruning,
  merges, rollbacks, clears, and expired-key sweeps, and `from_dict(...,
  wal_path=...)` logs imported state as a replacement record. Replay validates
  prune counts and removes expired TTL state before later writes, so a crash
  cannot resurrect a stale deadline.
- `get_stats()` reports `journal_path`, `journal_bytes`, and
  `journal_sequence` for WAL-backed maps.
- `tests/test_journal.py`, `tests/test_wal.py`, `tests/test_wal_crash.py`,
  `tests/test_wal_lock.py`, and `tests/test_module_entry.py` cover framing,
  checksums, torn writes, checkpoints, replay of every mutation, TTL expiry
  edge cases, WAL validation, cross-process lock contention and release,
  abrupt process exits with and without a checkpoint, close/destructor
  behavior, and the `__main__` entrypoint. Async WAL and
  serialization-validation coverage lives in `tests/test_async_wal.py` and
  `tests/test_async_serialization_validation.py`; compressed pickle fallback
  coverage lives in `tests/test_pickle_fallback.py`.
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
  (`test` job, Python 3.8–3.14) still always runs regardless, as the
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
- `AsyncChronoMap` now mirrors the synchronous API: `get_many`,
  `exists`, `setdefault`, and `pop` for batch reads and conditional
  writes; TTL-aware `query`, `aggregate`, and `count`; history, range,
  and latest-key views; `diff`, `diff_detailed`, and `merge`;
  JSON/pickle persistence; async change callbacks; and async snapshot
  rollback through `AsyncSnapshotContext`.
- `tests/test_async_parity.py`: 84 asyncio tests covering awaited
  locking, background TTL cleanup and cancellation, async callback
  ordering, rollback, persistence errors, and parity with the
  synchronous surface.
- `ChronoMap.get_many()`, `exists()`, `setdefault()`, and `pop()` for
  dict-style batch reads and atomic conditional writes.
- Constructor validation for `max_history`, `cache_size`,
  `ttl_cleanup_interval`, and `max_memory_mb`; invalid limits now fail
  at construction instead of producing strange behavior later.
- `tests/test_core_correctness.py`: regression tests covering rejected
  TTLs, stored `None` values, batch validation, pruning, filtering,
  merge isolation, callback reentrancy, and serialization payload checks.
- `benchmarks/benchmark_core.py`: repeatable microbenchmarks for
  point reads, range reads, writes with a populated cache, and
  `query(...).distinct()`.
- `tests/test_versionlog.py` and `tests/test_query_fastpaths.py`: direct
  coverage for binary-search boundaries, duplicate timestamps,
  unhashable-value queries, and cache-backed hot paths.
- `ChronoMap.iter_rows()` and `AsyncChronoMap.iter_rows()` stream one
  dictionary per live version without holding the lock across yields.
  `to_dataframe()` now builds on the same snapshot.

### Changed
- **CI now covers Python 3.14 and fails on type errors.** The test matrix adds
  3.14, mypy runs without `continue-on-error`, every test job has a timeout,
  and the monthly release workflow is serialized to avoid overlapping runs.
- **Time-travel lookups no longer allocate a timestamp list.** Point
  reads, out-of-order writes, snapshots, and version ranges now binary
  search the `(timestamp, value)` list directly through
  `chronomap._versionlog`, removing an O(history) allocation from every
  read.
- **Range reads are now O(log n + k).** `get_range()` and
  `versions(...)` slice version histories at the requested boundaries
  instead of scanning every stored version.
- **Cache invalidation is now O(entries for one key), not O(cache
  size).** `LRUCache` maintains a per-store-key index, so writes no
  longer scan every cached timestamp.
- **Query hot paths scale better.** `values_in()` uses a set for
  hashable candidates, `distinct()` uses a hash set, and
  `get_latest_keys()` uses `heapq.nlargest()` instead of sorting the
  full map.
- **History pruning mutates in place.** `max_history` trimming no longer
  copies the retained slice.

Benchmarks on Python 3.13 for a 100k-version key improved from roughly
3.3 ms to 4.6 us per point read, 3.7 ms to 6.7 us per 10-version range
read, 3.1 ms to 5.3 us per write with a 50k-entry cache, and 669 ms to
2.5 ms for `distinct()` over 5k unique values.
- **CI now exercises Windows and macOS.** A platform matrix runs the complete
  suite with the 100% coverage gate on Python 3.12 for both operating systems,
  covering the `msvcrt` and `fcntl` WAL lock backends and platform-specific
  filesystem behavior.

### Fixed
- **`from_dict()` ignored explicit constructor options.** Explicit
  `max_history` and `enable_ttl_cleanup` arguments now override serialized
  metadata instead of colliding with it, for both map implementations.
- **`put_many()` accepted non-mapping inputs.** Both implementations now
  require a `Mapping` and reject duck-typed objects before validating keys or
  writing anything.
- **Compressed pickle loading could raise `UnicodeDecodeError`.** When a
  compressed payload happened to look like the start of a pickle, `load_pickle()`
  only caught `UnpicklingError` and `EOFError`; some gzip streams failed with a
  decoding error instead of falling back to decompression. Both map
  implementations now treat any failed raw pickle parse as compressed input.
- **Rejected TTLs used to leave a version behind.** `put()` appended the
  value before checking `ttl`, so `put("k", "v", ttl=0)` raised
  `ChronoMapValueError` *and* stored the value. TTL validation now runs
  before any mutation in `put()`, `put_many()`, `get_or_set()`, and
  `setdefault()`.
- **Stored `None` values disappeared from queries.** `query()`,
  `aggregate()`, and `get_keys_by_value()` treated `None` as a missing
  key, even though `get()` and `in` considered it present. Lookups now
  distinguish "found a `None`" from "found nothing".
- **`get()` could return the default for a fresh write.** The first
  expiry check happened before the read lock, so a write racing with
  expiration could be missed. The check and cache lookup now happen
  under the same read lock as the version lookup.
- **`prune_history(keep_last=0)` kept every version.** The slice
  `versions[-0:]` is the whole list. Zero now clears the key, and
  negative or non-integer `keep_last` values are rejected.
- **`max_history=0` kept unbounded history.** Zero and negative caps are
  now rejected at construction instead of silently disabling pruning.
- **Deleting an expired key reported success.** `delete()` and
  `delete_many()` now treat expired keys as absent, purge their stale
  TTL entries, and report only live deletions.
- **`diff()`, `diff_detailed()`, and `merge()` read the other map without
  its lock.** They now take a short visibility snapshot under the other
  map's read lock, then compare/merge outside it -- no lock nesting and
  no deadlock when two maps are diffed or merged in opposite orders.
- **`merge(strategy='timestamp')` was quadratic.** It used repeated list
  inserts; it now performs a linear `heapq.merge` over both sorted
  histories and refreshes the memory guard afterward.
- **`from_dict()` trusted stored payloads.** Non-mapping payloads,
  malformed version entries, and non-mapping `store`/`ttl` sections now
  raise typed errors. `load_json()` also accepts older files that have
  no `ttl` section.

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

### Added (Query engine)
- `src/chronomap/query.py` — a fluent query layer over `ChronoMap`.
  `query(cm, timestamp=...)` returns a lazy `Query` that filters values
  as of one instant (including stored `None` values), while
  `versions(cm, start=..., end=..., keys=...)` returns a `VersionQuery`
  over row-level `(key, timestamp, value)` history. Both support
  predicates, key globs/regexes, set membership, sorting, paging,
  distinct/grouping/folding, numeric reductions, and conversion back to
  a `ChronoMap`; `Query.at()` re-evaluates at another timestamp without
  dropping the filters already applied.
- `Query`, `VersionQuery`, `QueryError`, `query`, and `versions` are
  exported from the package root.
- `tests/test_query.py` — 59 cases covering value snapshots, stored
  `None`, expired keys, every filter/sort/reducer, empty-query errors,
  invalid arguments, version ranges, and rebuilding maps from history.
- Added the `query` pytest marker and a matching subsystem path filter
  in `.github/workflows/tests.yml`.

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

### Added (Persistence)
- `src/chronomap/persistence.py` — a versioned archive layer that fixes
  the type loss in `save_json()` (which stringifies every key).
  `dump()` / `dumps()` write a self-describing JSON or pickle archive with
  a magic header, optional `zlib`/`gzip`/`bz2`/`lzma` compression, and an
  atomic temporary-file swap. `load()` / `loads()` restore keys, values,
  TTL deadlines, history, `max_history`, and snapshot time exactly.
- The JSON codec tags scalars, bytes, datetimes/dates/times, tuples, sets,
  frozensets, and mappings with non-string keys, so round trips are
  lossless. Arbitrary Python objects can still use the pickle format.
- `inspect()` returns an `ArchiveInfo` summary without reconstructing the
  map, and `drop_expired=True` / `include_expired=False` control whether
  TTL-expired keys are carried across saves.
- `PersistenceError`, `UnsupportedFormatError`, `CorruptArchiveError`, and
  `CodecError` provide specific failure modes; all derive from
  `ChronoMapError`.
- `tests/test_persistence.py` — 88 cases covering every codec tag,
  compression mode, format inference, atomic-write failure paths, TTL
  handling, corrupt headers/bodies/payloads, and pickle objects.
- Added the `persistence` pytest marker and subsystem path filter.

### Fixed
- **Writing to a key whose TTL had expired produced an invisible value.**
  `put()` / `put_many()` (and `AsyncChronoMap.put()`) left the stale TTL
  entry in place, so the freshly written value was immediately treated as
  expired: `get()` returned `None` and `key in cm` was `False`. Writes to an
  expired key now start from a clean slate, matching what `get_or_set()`
  already did.
- **`ChronoMap.merge(strategy='timestamp')` was broken.** The out-of-order
  insert branch referenced an undefined variable (`value` instead of
  `val`) and inserted into the wrong list (`versions`, the *source* data
  from the other map, instead of `target_versions`). Any merge where a
  key's timestamps interleaved out of order would raise `NameError` and
  could corrupt the source map's history in the process. Added a
  regression test (`test_merge_timestamp_strategy_does_not_crash_on_out_of_order_writes`).

### Added (CLI)
- The CLI is no longer limited to `show`: added `get`, `history`, `put`,
  `delete`, `keys`, `stats`, `prune`, `diff`, and `merge`, each with a
  `--json` mode for scripting.
- Save files are replaced atomically through a temporary file in the
  destination directory, so a failed write leaves the previous file
  intact instead of truncating it.
- Keys are parsed as JSON by default (so `1` reads an integer key), with
  `--literal-key` for string keys that happen to look like numbers.
- `--no-color` and `NO_COLOR` disable ANSI output; colors are also
  disabled automatically when stdout is not a terminal.
- `tests/test_cli_commands.py`: 61 end-to-end cases covering every
  subcommand, JSON output, pickle paths, atomic failure handling, bad
  input, and the error/exit-code boundary.

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
