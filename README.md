# ChronoMap

A thread-safe, time-versioned key-value store for Python. Every write is
kept, not just overwritten, so you can ask "what was this key at time T"
as well as "what is it now."

```python
from chronomap import ChronoMap

cm = ChronoMap()
cm['config'] = {'debug': True}
cm['config'] = {'debug': False}

cm.history('config')
# [(1720699200.1, {'debug': True}), (1720699241.7, {'debug': False})]

cm.get('config', timestamp='2024-07-11T10:00:00')
# whatever the value was at that moment
```

## Why this exists

Most in-memory stores (dicts, Redis) only remember the current value. Once
you overwrite a key, the old value is gone. ChronoMap keeps every version,
indexed by timestamp, so debugging "why did this config change" or
"what did this look like an hour ago" doesn't require a separate audit
log bolted on afterward.

It's not trying to replace a real database - it's an in-memory structure
for the case where you want dict-like ergonomics plus history: config
stores, session state, feature flags, small time-series, that kind of
thing.

## Installation

```bash
pip install chronomap

# with pandas export support
pip install chronomap[pandas]
```

Requires Python 3.8+. No required dependencies.

## Core features

- **Time travel** - `get(key, timestamp=...)` returns the value as of
  any point in history, with microsecond-precision lookups via binary
  search over each key's version list.
- **Snapshots & rollback** - `snapshot()` / `rollback()`, or
  `with cm.snapshot_context(): ...` for automatic rollback if the block
  raises.
- **TTL** - `put(key, value, ttl=3600)`; expired keys are cleared lazily
  on access and (optionally) by a background thread.
- **Queries** - `query()`, `aggregate()`, `count()` take plain Python
  predicates/functions rather than a query language.
- **Change hooks** - `on_change()` for global notifications,
  `subscribe(key, callback)` for a single key.
- **Auto-pruning** - cap history length per key with `max_history` so
  long-running processes don't grow unbounded.
- **Async** - `AsyncChronoMap` is a separate asyncio-native
  implementation with the same core surface.

Full method-by-method reference lives in [`docs/API.md`](docs/API.md).

## What it isn't

- Not a persistent database. `save_json`/`save_pickle` exist for
  checkpointing, but this is an in-memory structure, not a WAL-backed
  store.
- Not process-safe. Thread-safe within one process; if you need
  multiprocessing, use separate instances and your own IPC.
- The query/aggregate methods are O(n) linear scans. Fine for the sizes
  this is meant for (hundreds to low thousands of keys), not built for
  large-scale analytics.

## Status

This project is in the middle of being restructured from a single-file
script into a proper package (see `CHANGELOG.md`). As of this restructure:
**246 tests passing, 100% coverage** - reproducible yourself with
`pytest tests/ --cov=chronomap`, not a claim to take on faith. It hasn't
been through a broad external review yet - issues and PRs pointing out
rough edges are genuinely welcome, not just a formality.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Short version: fork, branch,
write a test for whatever you're changing, open a PR. Nothing fancier
than that.

## License

MIT - see [`LICENSE`](LICENSE).
