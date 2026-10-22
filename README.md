# ChronoMap

A thread-safe, time-versioned key-value store for Python. Every write is
kept, not just overwritten, so you can ask "what was this key at time T"
as well as "what is it now."

```python
from chronomap import ChronoMap

cm = ChronoMap()
cm["config"] = {"debug": True}
cm["config"] = {"debug": False}

cm.history("config")
# [(1720699200.1, {'debug': True}), (1720699241.7, {'debug': False})]

cm.get("config", timestamp="2024-07-11T10:00:00")
# the value that was visible at that moment
```

## Why this exists

Most in-memory stores (dicts, Redis) only remember the current value. Once
you overwrite a key, the old value is gone. ChronoMap keeps every version,
indexed by timestamp, so debugging "why did this config change" or
"what did this look like an hour ago" doesn't require a separate audit
log bolted on afterward.

It's not trying to replace a real database. It's an in-memory structure
for the case where you want dict-like ergonomics plus history: config
stores, session state, feature flags, small time-series, that kind of
thing.

## Installation

```bash
pip install chronomap

# with pandas export support
pip install chronomap[pandas]
```

Requires Python 3.8+. No required runtime dependencies.

## Quick start

```python
from chronomap import ChronoMap

cm = ChronoMap()
cm.put("cpu", 0.42, timestamp=100)
cm.put("cpu", 0.91, timestamp=200)
cm.put("cpu", 0.17, ttl=3600)

cm.get("cpu")                    # current value
cm.get("cpu", timestamp=150)     # 0.42, the value as of timestamp 150
cm.history("cpu")                # every stored (timestamp, value) pair
cm.get_range("cpu", 100, 200)    # versions in an inclusive time range
```

Timestamps can be Unix numbers, `datetime` objects, or ISO-8601 strings.
Expired keys are hidden from reads immediately and are removed lazily on
access (or by the optional cleanup thread).

For crash recovery, point the map at a write-ahead journal:

```python
cm = ChronoMap(wal_path="chronomap.wal")
with cm:
    cm.put("region", "us-east")
    cm.put("retries", 3)

recovered = ChronoMap(wal_path="chronomap.wal")
assert recovered.get("retries") == 3
```

Records are flushed and fsynced before the in-memory mutation by default
(`wal_sync=True`). Journals compact into an atomic checkpoint automatically
at 64 MiB, or whenever you call `cm.checkpoint()`. Recovery streams records
through a consumer instead of buffering the whole journal. `AsyncChronoMap`
accepts the same options; use `async with` or `await cm.aclose()` to close its
journal.

## Documentation

- [`docs/API.md`](docs/API.md) - method-by-method reference for the
  synchronous map, async map, exceptions, and CLI.
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) - storage layout, locking,
  caching, TTL cleanup, snapshots, and the async design.
- [`docs/COOKBOOK.md`](docs/COOKBOOK.md) - practical recipes for config,
  sessions, caches, snapshots, merges, and persistence.
- [`docs/TESTING.md`](docs/TESTING.md) - local test commands, markers,
  coverage, and CI expectations.
- [`CHANGELOG.md`](CHANGELOG.md) - what changed and why.

## Core features

- **Time travel** - `get(key, timestamp=...)` returns the value as of any
  point in history, with binary search over each key's sorted version list.
- **Snapshots and rollback** - `snapshot()` / `rollback()`, or
  `with cm.snapshot_context(): ...` for automatic rollback when a block
  raises.
- **TTL** - `put(key, value, ttl=3600)`; inspect with `ttl_remaining()`,
  refresh with `expire()`, remove with `persist()`, and sweep with
  `clean_expired_keys()` or the background cleanup thread.
- **Batch operations** - `put_many()`, `get_many()`, `pop_many()`, and
  `delete_many()` operate under a single lock acquisition.
- **Queries** - `query()`, `aggregate()`, and `count()` take ordinary
  Python callables instead of a query language.
- **Change hooks** - `on_change()` for global notifications and
  `subscribe(key, callback)` for one key.
- **Auto-pruning** - cap history length per key with `max_history`, or trim
  on demand with `prune_history()` / `prune_all_history()`.
- **Diff and merge** - compare two maps with `diff()` / `diff_detailed()`
  and combine histories with `merge(strategy=...)`.
- **Durability** - opt into an append-only, CRC-checked write-ahead journal
  with crash recovery, CRC-checked atomic checkpoints, and configurable fsync.
  Both `ChronoMap` and `AsyncChronoMap` support WAL mode.
- **Memory guardrails** - `max_memory_mb` adds an approximate ceiling and
  warning to both map implementations; `memory_usage()` reports the estimate.
- **Persistence** - checkpoint atomically with `save_json()`, `save_pickle()`,
  `to_dict()`, and the matching loaders.
- **Version export** - stream every live version with `iter_rows()`, or build
  a pandas DataFrame with `to_dataframe()`.
- **Async** - `AsyncChronoMap` is a separate asyncio-native implementation
  for use inside event loops, including TTL refresh and batch operations.
- **CLI inspection** - `python -m chronomap.cli show state.json` prints a
  saved map's current state.

## Thread safety

`ChronoMap` is safe to share between threads in one process. It uses a
reader-writer lock by default, with an `RLock` fallback if you set
`use_rwlock=False`. It does not share state across processes. A WAL-backed map
takes an OS-backed advisory lock on `<wal_path>.lock`, so opening the same
journal from a second live process raises `JournalLockError`; the lock is
released when the owner exits, including after a crash.

`AsyncChronoMap` uses an `asyncio.Lock` and should stay on one event loop.
Do not share a synchronous map and an async map as if they were the same
object.

## What it isn't

- Not a database. The optional WAL survives process crashes and torn tail
  writes for one writer, but the map is still in-memory, has no transactions
  or replication, and does not share state across processes. The WAL lock only
  prevents a second live writer; it is not distributed coordination. Snapshot
  helpers (`save_json()`, `save_pickle()`) are separate from the journal.
  JSON saves coerce keys to strings; use pickle when key types matter.
- Not process-safe. Thread-safe within one process; for multiprocessing,
  use separate instances and your own IPC. A WAL path accepts only one live
  owner and rejects a second one with `JournalLockError`.
- Not built for large-scale analytics. `query()`, `aggregate()`, and
  `count()` are O(n) scans. They are intended for hundreds to low
  thousands of keys, not warehouse-sized datasets.

## Status

The project has 800+ tests and 100% line coverage across the package.
CI runs the suite on Python 3.8 through 3.14 on Linux, plus Windows and macOS,
and checks formatting with `black`. Reproduce the coverage locally with:

```bash
pytest tests/ --cov=chronomap
```

Issues and PRs pointing out rough edges are welcome.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) and
[`docs/TESTING.md`](docs/TESTING.md). Short version: fork, branch, write a
test for the change, run the suite, and open a PR.

## License

MIT - see [`LICENSE`](LICENSE).
