# Cookbook

Practical recipes built on the current `chronomap` API. Every example runs
against the synchronous `ChronoMap` unless it explicitly uses
`AsyncChronoMap`.

These examples disable the background TTL thread and sometimes the read cache
so that the behavior is deterministic. In an application, the defaults are
usually what you want.

## Keep a configuration history

Store each configuration revision at an explicit timestamp, then read the
configuration that was active at any point in time.

```python
from chronomap import ChronoMap

config = ChronoMap(enable_ttl_cleanup=False)

config.put(
    "app",
    {"debug": False, "workers": 4},
    timestamp="2024-01-01T00:00:00+00:00",
)
config.put(
    "app",
    {"debug": True, "workers": 8},
    timestamp="2024-01-02T00:00:00+00:00",
)

jan_1_noon = config.get("app", timestamp="2024-01-01T12:00:00+00:00")
assert jan_1_noon == {"debug": False, "workers": 4}

current = config.get("app")
assert current == {"debug": True, "workers": 8}

for timestamp, value in config.history("app"):
    print(timestamp, value)
```

Use ISO-8601 strings at the boundary of your application and keep the
floating-point timestamps inside ChronoMap.

## Expiring sessions

TTLs are measured from the write time. Use a short TTL for temporary data and
let the background thread or the next read remove it.

```python
import time

from chronomap import ChronoMap

sessions = ChronoMap(enable_ttl_cleanup=False)

sessions.put("session:abc", {"user": 7}, ttl=3600)
sessions.put("session:short", {"user": 8}, ttl=0.01)

assert sessions.get("session:abc") == {"user": 7}

time.sleep(0.02)
assert sessions.get("session:short") is None
assert "session:short" not in sessions
```

A write to an expired key starts a fresh history, so a new value is not born
already expired.

Inspect the remaining lifetime with `ttl_remaining()` and refresh it with
`expire()` without writing a new version:

```python
assert 0 < sessions.ttl_remaining("session:abc") <= 3600
assert sessions.expire("session:abc", 7200) is True
```

For a production service, leave `enable_ttl_cleanup=True` (the default) so
expired entries are swept periodically even if no one reads them.

To make an existing key permanent without writing a new version, call
`persist()`:

```python
sessions.put("session:long-lived", {"user": 9}, ttl=60)
assert sessions.persist("session:long-lived") is True
```

To sweep expired keys immediately — for example after a burst of short-lived
writes — call `clean_expired_keys()`:

```python
removed = sessions.clean_expired_keys()
```

## Roll back a group of writes

Use `snapshot_context()` when several writes should either commit together or
leave the map untouched.

```python
from chronomap import ChronoMap

inventory = ChronoMap(enable_ttl_cleanup=False)
inventory.put("widgets", 10)

try:
    with inventory.snapshot_context():
        inventory.put("widgets", 7)
        inventory.put("gadgets", 3)
        raise RuntimeError("payment failed")
except RuntimeError:
    pass

assert inventory.get("widgets") == 10
assert inventory.get("gadgets") is None
```

The context manager rolls back on any exception and commits on normal exit.
It takes a deep copy at entry, so the cost grows with the size of the map.

## Audit changes with callbacks

Global callbacks receive every write. Per-key subscribers receive only writes
for one key.

```python
from chronomap import ChronoMap

audit = []
cm = ChronoMap(enable_ttl_cleanup=False)

cm.on_change(lambda key, old, new, ts: audit.append(("all", key, old, new)))
cm.subscribe("cpu", lambda old, new, ts: audit.append(("cpu", old, new)))

cm.put("cpu", 0.4)
cm.put("cpu", 0.9)
cm.put("disk", 0.2)

assert ("all", "cpu", None, 0.4) in audit
assert ("cpu", 0.4, 0.9) in audit
assert ("all", "disk", None, 0.2) in audit
```

Callback exceptions are logged and do not roll back the write. Keep callbacks
short; they run in the writer's execution path in the synchronous map.

## Recover writes after a crash

Use `wal_path` when losing the latest writes is not acceptable. Each record is
fsynced before the in-memory mutation by default.

```python
import tempfile
from pathlib import Path

from chronomap import ChronoMap

with tempfile.TemporaryDirectory() as tmp:
    path = Path(tmp) / "state.wal"

    with ChronoMap(wal_path=path, enable_ttl_cleanup=False) as state:
        state.put("region", "us-east")
        state.put("retries", 3)

    recovered = ChronoMap(wal_path=path, enable_ttl_cleanup=False)
    try:
        assert recovered.get("region") == "us-east"
        assert recovered.get("retries") == 3
    finally:
        recovered.close()
```

The journal automatically compacts into an atomic checkpoint at 64 MiB. Tune
that with `wal_auto_checkpoint_bytes`, disable it with `None`, or call
`checkpoint()` explicitly. Each live map holds an exclusive advisory lock on
`<wal_path>.lock`; a second process that opens the same path raises
`JournalLockError` until the owner closes or exits. Journals and checkpoints
use trusted pickle data.

`AsyncChronoMap` takes the same WAL arguments and moves append, checkpoint, and
close operations off the event loop:

```python
import asyncio

from chronomap import AsyncChronoMap


async def main():
    async with AsyncChronoMap(wal_path="chronomap.wal") as state:
        await state.put("region", "us-east")
        await state.put("retries", 3)
        await state.checkpoint()

    recovered = AsyncChronoMap(wal_path="chronomap.wal")
    try:
        assert await recovered.get("region") == "us-east"
        assert await recovered.get("retries") == 3
    finally:
        await recovered.aclose()


asyncio.run(main())
```

## Save and restore a map

For string keys and JSON-compatible values, JSON is the portable choice.

```python
import tempfile
from pathlib import Path

from chronomap import ChronoMap

state = ChronoMap(enable_ttl_cleanup=False)
state.put("region", "us-east")
state.put("retries", 3)

with tempfile.TemporaryDirectory() as tmp:
    path = Path(tmp) / "state.json"
    state.save_json(path)

    loaded = ChronoMap.load_json(path, enable_ttl_cleanup=False, cache_size=0)
    assert loaded.get("region") == "us-east"
    assert loaded.get("retries") == 3
```

JSON has no concept of non-string keys: `save_json()` coerces keys with
`str()`. Use pickle when key or value types must survive exactly.

```python
import tempfile
from pathlib import Path

from chronomap import ChronoMap

state = ChronoMap(enable_ttl_cleanup=False)
state.put(7, {"name": "alice"})

with tempfile.TemporaryDirectory() as tmp:
    path = Path(tmp) / "state.pkl"
    state.save_pickle(path, compress="gzip")

    loaded = ChronoMap.load_pickle(path, enable_ttl_cleanup=False, cache_size=0)
    assert loaded.get(7) == {"name": "alice"}
```

Only unpickle files you trust; pickle can execute arbitrary code.

## Compare two maps and merge one into the other

Diff by current value, inspect the differences, then choose a merge strategy.

```python
from chronomap import ChronoMap

local = ChronoMap(enable_ttl_cleanup=False)
remote = ChronoMap(enable_ttl_cleanup=False)

local.put("a", 1)
local.put("only-local", "keep")
remote.put("a", 2)
remote.put("only-remote", "new")

assert local.diff(remote) == {"a", "only-local", "only-remote"}

for key, other_value, local_value in sorted(local.diff_detailed(remote)):
    print(key, other_value, local_value)

local.merge(remote, strategy="overwrite")

assert local.get("a") == 2
assert local.get("only-local") == "keep"
assert local.get("only-remote") == "new"
```

Use `strategy="timestamp"` when both maps contain partial histories that
should be interleaved rather than replaced.

TTL behavior follows the merge strategy: `"timestamp"` keeps the later
expiry, while `"overwrite"` copies the other map's TTLs where they exist.

## Use AsyncChronoMap in an asyncio service

The async map is a separate implementation with an `asyncio.Lock`. It is not
thread-safe and should be created inside the loop or used from one loop only.

```python
import asyncio

from chronomap import AsyncChronoMap


async def main():
    cm = AsyncChronoMap(max_history=100)
    counter_lock = asyncio.Lock()

    await cm.put("counter", 0)
    await cm.put("counter", 1)

    assert await cm.get("counter") == 1
    assert await cm.keys() == ["counter"]

    async def increment(times):
        for _ in range(times):
            async with counter_lock:
                value = await cm.get("counter")
                await cm.put("counter", value + 1)

    await asyncio.gather(increment(5), increment(5))

    assert await cm.get("counter") == 11


asyncio.run(main())
```

Each `get` and `put` is individually protected, but the pair is not atomic.
Without `counter_lock`, five coroutines doing `get` then `put` could interleave
and lose updates. Use an application-level lock for any read-modify-write
sequence.

## Inspect a saved file from the shell

Save a map, then use the built-in inspector:

```bash
python -m chronomap.cli show state.json
python -m chronomap.cli show state.pkl
```

The CLI prints the latest keys and values. The suite does not require the
package to be installed with a console-script entry point; `-m` works from a
source checkout as well.

## Bound history growth

If you do not need every historical version, cap history with
`max_history` or prune explicitly.

```python
from chronomap import ChronoMap

cm = ChronoMap(max_history=3, enable_ttl_cleanup=False)

for value in range(6):
    cm.put("cpu", value, timestamp=100 + value)

assert len(cm.history("cpu")) == 3

removed = cm.prune_history("cpu", keep_last=2)
assert removed == 1

removed = cm.prune_history("cpu", older_than=103)
assert removed == 0

assert cm.get("cpu") == 5
```

`prune_all_history()` applies the same rules across every live key. Pruning
is a write operation and invalidates the cache for the pruned keys.

## Replay values over a time window

`get_range()` returns the stored versions in a closed interval. This is useful
for charts, audits, and replaying a key's value changes.

```python
from chronomap import ChronoMap

metrics = ChronoMap(enable_ttl_cleanup=False)

metrics.put("cpu", 0.42, timestamp=100)
metrics.put("cpu", 0.91, timestamp=200)
metrics.put("cpu", 0.17, timestamp=300)

window = metrics.get_range("cpu", start_ts=150, end_ts=300)
assert [value for _, value in window] == [0.91, 0.17]
assert [ts for ts, _ in window] == [200.0, 300.0]
```

Omit `start_ts` to start at the beginning of the key's history. Omit `end_ts`
to end at the current time.

## Query and aggregate the current state

`query()` filters by key and value. `aggregate()` hands a list of values to a
function.

```python
from chronomap import ChronoMap

load = ChronoMap(enable_ttl_cleanup=False)
load.put_many({"cpu": 0.4, "disk": 0.7, "memory": 0.9})

busy = load.query(lambda key, value: value >= 0.7)
assert busy == {"disk": 0.7, "memory": 0.9}

total = load.aggregate(sum)
assert total == 2.0

high_count = load.count(lambda key, value: value >= 0.7)
assert high_count == 2
```

These calls scan live keys. For large maps, cache the result or maintain a
separate index rather than running them on every request.

## Use a map from multiple threads

Reads and writes are protected by the map's lock. A single map can safely be
shared by threads, as long as separate threads do not rely on a sequence of
calls being atomic.

```python
from concurrent.futures import ThreadPoolExecutor

from chronomap import ChronoMap

cm = ChronoMap(enable_ttl_cleanup=False)

with ThreadPoolExecutor(max_workers=8) as pool:
    list(pool.map(lambda i: cm.put(f"job:{i}", i), range(100)))

assert len(cm) == 100
assert all(cm.get(f"job:{i}") == i for i in range(100))
```

If you need a compound operation to be atomic, wrap it in
`cm.snapshot_context()` and retry on conflicts, or hold an application-level
lock around the whole operation.

## Watch cache and operation counters

`get_stats()` reports operation counts, current size, and cache statistics.

```python
from chronomap import ChronoMap

cm = ChronoMap(cache_size=128, enable_ttl_cleanup=False)
cm.put("cpu", 0.4)

first = cm.get("cpu")
second = cm.get("cpu")

stats = cm.get_stats()
assert first == second == 0.4
assert stats["writes"] >= 1
assert stats["cache_hits"] >= 1
assert stats["total_keys"] == 1
```

`reset_stats()` clears the counters and the read cache. It does not change
stored data.

## Export history to pandas

Install the optional dependency and convert every version into a row.

```bash
pip install "chronomap[pandas]"
```

```python
from chronomap import ChronoMap

cm = ChronoMap(enable_ttl_cleanup=False)
cm.put("cpu", 0.42, timestamp=100)
cm.put("cpu", 0.91, timestamp=200)

frame = cm.to_dataframe()
assert list(frame.columns) == ["key", "value", "timestamp", "datetime", "version"]
```

The frame has one row per version, not one row per key.

For large maps, stream the same rows instead of materializing a frame:

```python
for row in cm.iter_rows():
    print(row["key"], row["timestamp"], row["value"])
```

`iter_rows()` snapshots keys and version lists under the read lock and yields
rows afterward, so slow consumers do not block writers.

## Catch package errors precisely

Catch `ChronoMapKeyError` for strict reads, `ChronoMapValueError` for bad
values, and the base class when you want a single boundary.

```python
from chronomap import ChronoMap, ChronoMapError, ChronoMapKeyError

cm = ChronoMap(enable_ttl_cleanup=False)

try:
    cm.get("missing", strict=True)
except ChronoMapKeyError:
    print("missing key")

try:
    cm.put("bad", 1, ttl=0)
except ChronoMapError as exc:
    print(type(exc).__name__, exc)
```

The package exceptions also inherit the matching built-in exception, so code
that already catches `KeyError`, `TypeError`, `ValueError`, or `MemoryError`
continues to work.

## Guard against runaway memory

Set `max_memory_mb` when a process must not grow without bound. The check is
approximate, but it catches the common failure mode of unbounded history.

```python
from chronomap import ChronoMap, ChronoMapMemoryError

cm = ChronoMap(
    max_history=10,
    max_memory_mb=0.01,  # deliberately tiny for this example
    enable_ttl_cleanup=False,
)

try:
    cm.put("payload", "x" * 64_000)
except ChronoMapMemoryError:
    print("memory ceiling reached")
```

`AsyncChronoMap` accepts the same `max_memory_mb` option and raises the same
`ChronoMapMemoryError`. Both maps expose `memory_usage()` / `await
memory_usage()` for an approximate diagnostics reading.

In a real service, use a limit that reflects the process budget, such as
`max_memory_mb=64`, and rely on `max_history`, pruning, and TTLs to keep the
normal working set below it. The monitor is a backstop, not a substitute for
bounding history.
