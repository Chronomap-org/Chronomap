# Architecture

This document describes how the current `chronomap` package is put
together: the storage model, the modules, the locking strategy, and the
tradeoffs that fall out of those choices.

## Goals and non-goals

ChronoMap is a single-process, in-memory, time-versioned key-value store.

It is designed to answer two kinds of questions efficiently:

- What is the current value of `key`?
- What was the value of `key` at time `T`?

It deliberately does not provide:

- process-to-process replication or a network protocol
- durable storage by default
- transactions across keys
- multi-process coordination over a WAL
- automatic schema migration

Persistence is whole-map snapshot serialization, not a database. Both map
implementations can opt into a single-writer write-ahead log for crash
recovery, but ChronoMap is still in-memory and does not share state across
processes. A WAL file rejects a second live writer with `JournalLockError`;
that is ownership protection, not multi-process coordination. If you need a
durable multi-process store, use a database and keep ChronoMap as a
time-indexed cache or projection.

## Module map

| Module | Responsibility |
| --- | --- |
| `src/chronomap/core.py` | `ChronoMap`: storage, reads, writes, history, TTL, pruning, diff/merge, persistence helpers. |
| `src/chronomap/asynchronous.py` | `AsyncChronoMap`: separate asyncio-native implementation, including WAL integration. |
| `src/chronomap/_lock.py` | `RWLock`: readers-writer lock used by the sync map. |
| `src/chronomap/_cache.py` | `LRUCache`: thread-safe cache for reads at a `(key, timestamp)` pair. |
| `src/chronomap/_memory.py` | `MemoryMonitor`: approximate recursive size estimate and optional ceiling. |
| `src/chronomap/_ttl_cleanup.py` | `TTLCleanupThread`: daemon thread that periodically sweeps expired keys. |
| `src/chronomap/_snapshot.py` | `SnapshotContext`: rollback-on-exception context manager. |
| `src/chronomap/_journal.py` | `Journal`: CRC-framed WAL records and atomic checkpoint helpers. |
| `src/chronomap/exceptions.py` | The `ChronoMapError` hierarchy. |
| `src/chronomap/cli.py` | The `python -m chronomap.cli show` inspector. |
| `src/chronomap/_version.py` | Package version string. |

The synchronous and asynchronous implementations are intentionally separate.
Sharing code between `threading` and `asyncio` locks tends to make both harder
to reason about, so each implementation owns its own state machine.

## Data model

The central structure is a dictionary from a key to a list of versions:

```python
_store: Dict[Any, List[Tuple[float, Any]]]
```

Each version is `(timestamp, value)`, stored in ascending timestamp order.
The list for one key is the key's history; the last element is the newest
version.

Alongside the store, `ChronoMap` keeps:

```python
_ttl: Dict[Any, float]              # key -> absolute expiry time
_snapshot_time: Optional[float]     # timestamp of the last snapshot
_cache: Optional[LRUCache]          # optional read cache
_memory_monitor: MemoryMonitor      # optional size guardrail
_stats: Dict[str, int]              # operation counters
_change_callbacks: List[Callable]   # global write callbacks
_key_subscribers: Dict[Any, List]   # per-key callbacks
_lock                               # RWLock or threading.RLock
```

All timestamps are floating-point epoch seconds. ISO-8601 strings and
`datetime` objects are parsed before they reach the store. The store itself
only sees floats.

```text
key "cpu"
  |
  +-- (100.0, 0.42)
  +-- (200.0, 0.91)
  +-- (300.0, 0.17)
```

A read at timestamp `150` returns `0.42`. A read at `300` returns `0.17`.
A read at `50` returns the caller's default, because the key did not exist
yet.

### Why a list per key?

The alternative — one global sorted timeline, or an index of every version —
makes point reads more expensive and complicates pruning. A per-key list
gives a small, cache-friendly history for each key and makes the two hot
operations straightforward:

- `put` appends, or inserts at the correct position with `bisect`.
- `get` binary-searches the version timestamps.

The cost is that a single key with millions of versions is expensive to
scan or deep-copy. `max_history` and `prune_history()` exist to bound that.

### Same-timestamp writes

Writes at the same timestamp are allowed. A later write is inserted after
existing versions at that timestamp, and a read at that timestamp sees the
most recently inserted version. There is no merge or conflict resolution
inside a timestamp; callers that need deterministic ordering should use
distinct timestamps.

## Read and write paths

### Writes

A write does roughly this:

1. Validate the key and parse the timestamp.
2. Acquire the write lock.
3. Purge the old history if the key has expired.
4. Insert the version in timestamp order.
5. Update the TTL, prune old versions if `max_history` is set, and invalidate
   the read cache.
6. Update counters and run change callbacks.
7. Release the lock.

The important consequence is that a write is all-or-nothing with respect to
other operations on the same map: no reader can observe a half-inserted
version.

`put_many()` performs the same steps for several keys under one lock
acquisition, which is cheaper than calling `put()` in a loop when the caller
already has a batch.

### Reads

A read does roughly this:

1. Parse and validate the timestamp.
2. Check expiry.
3. Check the read cache, when enabled.
4. Acquire the read lock.
5. Binary-search the key's version list for the newest timestamp less than or
   equal to the query timestamp.
6. Return the value, or the default / `ChronoMapKeyError`.

The read cache is checked before the main lock. Cache invalidation happens on
writes, deletes, pruning, and rollback, so a cached value cannot outlive the
history it came from.

### Iteration

`keys()`, `values()`, and `items()` copy the relevant keys or values while
holding the lock, then yield from that copy. This prevents a concurrent
writer from mutating the underlying dictionary during iteration. The
tradeoff is that iteration sees a snapshot taken when the iterator was
created, not a live view.

## Concurrency model

### Synchronous map

The default lock is `RWLock`, a condition-variable based readers-writer lock:

- any number of readers may hold the lock at once
- a writer has exclusive access
- once a writer has declared intent, new readers wait

`use_rwlock=False` swaps in a plain `threading.RLock`. That serializes reads
as well as writes and is mainly useful for debugging or for environments
where the reader-writer lock is a poor fit.

The lock protects the store, TTL table, counters, and key-subscriber
registration. The LRU cache has its own lock and is designed to be safe when
checked from the read path.

Callbacks run from the writing path. An exception in a callback is logged and
does not undo the write. Callbacks are therefore not a transaction mechanism;
they are notifications. A slow callback slows down the writer.

### Async map

`AsyncChronoMap` uses a single `asyncio.Lock`. Every state access happens
inside an `async with` block. It does not use `RWLock` or the sync cache,
and it has no background cleanup thread; expiry is checked lazily on access.

Async callbacks may be plain functions or coroutine functions. The map awaits
coroutine callbacks after releasing its lock for writes, so a callback may
perform further awaits without deadlocking the map.

With `wal_path` enabled, the constructor replays the checkpoint and journal
synchronously. After construction, journal appends, checkpoint writes, and
closing the journal run through the default executor while the map holds its
`asyncio.Lock`. That preserves record order relative to in-memory mutations
without blocking the event loop. An auto-checkpoint failure is logged and
leaves the journal intact instead of failing the write. `aclose()` cancels
the optional cleanup task and closes the journal; `__del__` closes the journal
as a fallback.

The async map must be used from one event loop. It is not thread-safe and is
not a drop-in replacement for `ChronoMap` in a threaded program.

### Process model

Thread safety does not extend across processes. Two processes that load the
same archive get two independent maps. Writes to one are invisible to the
other until the state is saved and loaded again. WAL-backed maps are the
exception to accidental sharing: the journal holds an exclusive OS-backed
advisory lock on `<wal_path>.lock`, so a second live opener gets
`JournalLockError` instead of appending to the same journal.

## TTL

Every TTL is stored as an absolute expiry time:

```python
_ttl[key] = current_time() + ttl
```

A key is expired when `current_time() >= _ttl[key]`. Expiry is checked:

- on reads, where an expired key behaves as missing
- on writes, where the old history and TTL are purged first
- in bulk operations such as `keys()`, `latest()`, and `query()`

The synchronous map also starts `TTLCleanupThread`, a daemon thread that calls
`clean_expired_keys()` every `ttl_cleanup_interval` seconds. The thread holds a
weak reference to the map, so it does not keep the map alive after the owner
drops it.

There is no timer per key and no heap of expiries. Cleanup is periodic and
lazy, which keeps writes cheap but means an expired key can remain in memory
until the next access or sweep.

## History pruning

`max_history` is enforced after every write. When a key exceeds the limit,
the oldest versions are dropped and the cache for that key is invalidated.
`auto_prunes` in the statistics counts these automatic prunes.

`prune_history()` and `prune_all_history()` provide explicit control:

- `keep_last=N` keeps the newest N versions
- `older_than=T` drops versions older than T

Pruning is a write operation and takes the write lock. It is not incremental
across keys; `prune_all_history()` walks each live key.

## Read cache

`LRUCache` maps `(key, timestamp)` to a value. A timestamp of `None` in the
cache key represents the current value request. Historical reads are cached
under their concrete parsed timestamp.

The cache is bounded by `cache_size`; `0` disables it. Cache entries are
invalidated for a key whenever that key's history changes, including
deletes, automatic pruning, explicit pruning, and rollback. Merges clear the
whole cache because they can touch many keys.

The cache stores references to values, not deep copies. Mutating a returned
mutable value is the caller's responsibility and is not visible to the
cache key structure, but it can make two reads of the "same" version disagree
if one caller mutates the object in place. Treat stored values as immutable
when sharing them.

## Memory guardrail

`MemoryMonitor` recursively walks dictionaries, lists, tuples, sets, and
frozensets, using `sys.getsizeof()` for each object and a `_seen` set to avoid
double-counting shared objects and cycles.

Both `ChronoMap(max_memory_mb=...)` and `AsyncChronoMap(max_memory_mb=...)`
enforce the same rules:

- crossing 80% logs a warning once, until `reset_warning()` is called
- crossing 100% raises `ChronoMapMemoryError`

The synchronous check runs after the journaled mutation while the writer lock
is held. The async check does the same under its `asyncio.Lock`, after the
journal append completes, so a durable write is never silently rolled back by
a memory trip.

This is a tripwire, not an allocator-level limit. The recursion can miss
objects held through custom containers, and the estimate is approximate. Use
it to catch runaway histories, not as a security boundary. `memory_usage()`
exposes the current estimate on both maps for diagnostics.

The monitor runs synchronously in the write path, so very large maps make
writes more expensive when a ceiling is configured.

## Snapshots and rollback

`snapshot()` creates a new `ChronoMap` with:

- `deepcopy` of the store
- `deepcopy` of the TTL table
- a fresh `_snapshot_time`
- no cache and no cleanup thread

The result is independent of the source map. `rollback(snapshot)` restores the
store and TTL table into the current map and clears the cache.

`SnapshotContext` wraps `snapshot()` and `rollback()`:

```python
with cm.snapshot_context():
    cm.put("config", new_config)
    # commit on normal exit, rollback on exception
```

Snapshots are whole-map copies. Their cost is proportional to the number of
keys and versions, not just the number of changed keys. Use them for
short-lived transactional blocks, not as a continuous versioning strategy.

## Diff and merge

`diff(other)` compares current values for the union of both keys and returns
the keys that differ. If one map has an expired key and the other does not,
the key is reported as changed. `diff_detailed()` returns
`(key, other_value, self_value)` triples for the same comparison.

`merge(other, strategy="timestamp")` inserts the other map's versions into
this map in timestamp order and keeps the later TTL when both maps have one.
`strategy="overwrite"` replaces this map's history for keys present in the
other map and copies the other map's TTLs where they exist.

Neither operation deduplicates identical `(timestamp, value)` pairs. A merge
of two maps that share history can therefore introduce duplicate versions.
If that matters, prune or compare before merging.

## Durability

Durability is opt-in through `ChronoMap(wal_path=...)` or
`AsyncChronoMap(wal_path=...)`. Both use the same `_journal.py` record layer,
checkpoint format, and replay rules; only the locking and I/O scheduling
around them differ.

### Journal format

The journal starts with `CMWAL1\n`, followed by frames of:

```text
4-byte big-endian payload length
4-byte big-endian CRC32
pickled (sequence, operation, data) tuple
```

Sequence numbers increase monotonically. On open, the journal validates every
complete frame. An incomplete trailing frame is treated as a torn write and
truncated; a complete frame with a bad checksum or an out-of-order sequence
raises `JournalCorruptionError`. Pickle is used for records, so the journal is
trusted local state.

Before reading or truncating a journal, `Journal` opens `<wal_path>.lock` and
takes an exclusive non-blocking OS file lock (`flock` on POSIX,
`msvcrt.locking` on Windows). The sidecar is separate from the journal, so an
atomic journal reset does not drop the lock. A failed lock acquisition raises
`JournalLockError`. The kernel releases the lock when the descriptor closes or
the process exits, so a crash does not leave a stale lock behind.

Checkpoints use the same length + CRC32 framing after `CMCHK1\n`. A truncated,
extended, or checksum-corrupted checkpoint raises `JournalCorruptionError`
instead of being partially applied.

### Write path

Mutating methods append a journal record before applying the in-memory
mutation. With the default `wal_sync=True`, each append flushes and fsyncs the
file first. The records cover puts, deletes, TTL changes, pruning, merges,
rollbacks, and clears. Expired-key purges and background cleanup are logged
too, so replay does not accidentally resurrect an expired TTL on a later
write. The in-memory write lock is held across journal append and mutation, so
records from concurrent writers are ordered consistently. The synchronous map
uses its writer lock; the async map uses its `asyncio.Lock` and offloads file
I/O to the default executor while that lock is held.

### Checkpoints and recovery

`checkpoint()` serializes a deep copy of the store and TTL table to
`<wal_path>.checkpoint`, writes a length + CRC32 frame, fsyncs a temporary
file, atomically replaces the destination, then resets the journal while
retaining its sequence counter. Recovery loads the checkpoint, streams journal
records one at a time through a consumer, applies only records with higher
sequence numbers, and sets the next sequence above the checkpoint. Records are
released after replay instead of being accumulated in memory, so startup
memory is bounded by the restored map plus one journal record. A crash between
checkpoint replacement and journal reset is therefore safe: the stale journal
tail is validated and skipped. The journal reset itself
uses a temporary header plus atomic rename, so a crash can only leave the
checkpoint with either the old journal tail or a fresh empty journal.
Automatic checkpoints default to every 64 MiB of journal and never turn a
failed compaction into a failed user write.

WAL-backed maps are single-writer: the sidecar lock prevents two live
processes from opening the same path. The lock is advisory, not a distributed
coordination primitive; do not place a WAL on a filesystem whose locking
semantics you do not trust. Closing a synchronous map stops the TTL thread and
the journal; `await aclose()` does the equivalent for an async map. The
whole-map JSON/pickle persistence helpers are separate from the WAL.

## Persistence

Whole-map persistence is implemented in `core.py`:

- `to_dict()` deep-copies the store and TTL table into a plain dictionary
- `from_dict()` reconstructs a map from that shape
- JSON and pickle helpers wrap those two operations
- compression is applied to the pickle-based byte representation

The stored payload contains `store`, `ttl`, `snapshot_time`, `version`, and
`max_history`. Version metadata is written for diagnostics; loading does not
currently run migrations. Passing `wal_path` to `from_dict()` also logs the
imported state as a replacement record for both map implementations.

JSON cannot represent non-string keys. `save_json()` coerces keys with
`str()`, so loading a JSON file can change key identity. Pickle preserves key
and value types, but unpickling is code execution; only load trusted files.

## CLI

`src/chronomap/cli.py` is intentionally thin:

1. parse `show <path>`
2. choose the loader by suffix (`.json` versus `.pkl` / `.pickle`)
3. call `load_json()` or `load_pickle()`
4. print the latest keys and values

The CLI does not write files and does not expose the rest of the map API.
It is a convenience inspector, not an administration tool.

## Error handling

All package exceptions derive from `ChronoMapError`, and each subclass also
inherits the closest built-in exception:

| Exception | Built-in base | Typical cause |
| --- | --- | --- |
| `ChronoMapKeyError` | `KeyError` | strict read of a missing key |
| `ChronoMapTypeError` | `TypeError` | unhashable key, bad timestamp type |
| `ChronoMapValueError` | `ValueError` | non-positive TTL, invalid timestamp string |
| `ChronoMapMemoryError` | `MemoryError` | memory ceiling exceeded |

Catching `ChronoMapError` handles every package-specific failure. Catching a
built-in base also catches the corresponding ChronoMap error, which is useful
for code written against ordinary dictionaries.

## Testing strategy

The test suite is organized around behavior:

- core read/write/history/TTL tests
- cache and locking tests
- memory monitor tests
- async parity tests
- CLI tests
- persistence helpers

The suite runs under pytest with coverage enabled. The project targets
Python 3.8 and newer; CI exercises multiple Python versions and runs linting
and type checking alongside the tests.

## Known limitations

- History is unbounded unless `max_history` or explicit pruning is used.
- Snapshots and diff/merge are whole-map operations.
- The sync cache stores references, not deep copies.
- TTL cleanup is periodic, not exact; an expired key can occupy memory until
  the next sweep.
- The memory estimate is approximate.
- Callbacks run inline in the writer's execution path.
- The async implementation has a smaller API surface than the sync map.
- Async WAL replay runs synchronously during construction; a very large
  journal blocks the event loop until replay finishes.
- WAL checkpoints rewrite the whole store and TTL table.
- WAL ownership uses an advisory file lock; it depends on the filesystem's
  locking support and is not distributed coordination.
- JSON persistence coerces keys to strings.
- Pickle persistence and WAL/checkpoint files are unsafe for untrusted input.
- There is no cross-process shared-state or transaction coordination; the WAL
  lock only rejects a second live writer.

## Extending the store

A new synchronous operation generally belongs in `core.py` and should:

1. validate keys and timestamps with the existing helpers
2. acquire the read or write lock for the whole state access
3. update the relevant counters
4. invalidate affected cache entries when history changes
5. add focused tests for normal, missing, expired, and concurrent cases

If the operation changes the public API, update `docs/API.md` and the
`__all__` export list in `src/chronomap/__init__.py` as well.
