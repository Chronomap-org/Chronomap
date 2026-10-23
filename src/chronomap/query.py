# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Fluent queries over ChronoMap values and version history.

``query(cm)`` filters the value visible at a point in time. ``versions(cm)``
filters the individual versions in a time range. Both are snapshots: a query
reads the map once when a terminal method runs, then filters the rows it got.

Examples::

    from chronomap import ChronoMap, query, versions

    cm = ChronoMap()
    cm.put("cpu", 0.4, timestamp=100)
    cm.put("cpu", 0.9, timestamp=200)
    cm.put("disk", 0.2, timestamp=200)

    query(cm).where_value(lambda value: value > 0.5).keys()
    versions(cm, start=150).where_key(lambda key: key == "cpu").count()
"""

from __future__ import annotations

import fnmatch
import math
import re
from collections.abc import Mapping
from datetime import datetime
from statistics import mean, median
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from ._versionlog import range_versions, value_at
from .core import ChronoMap
from .exceptions import ChronoMapTypeError, ChronoMapValueError

_MISSING = object()
Item = Tuple[Any, Any]
Version = Tuple[Any, float, Any]
QueryOp = Callable[[List[Item]], List[Item]]


class QueryError(ChronoMapValueError):
    """A query was composed incorrectly or had no rows to aggregate."""


def _require_callable(value: Any, name: str) -> None:
    if not callable(value):
        raise ChronoMapTypeError(f"{name} must be callable")


def _require_non_negative(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ChronoMapValueError(f"{name} must be a non-negative integer")
    return value


def _resolve_timestamp(cm: ChronoMap, timestamp: Any) -> float:
    ts = cm._parse_timestamp(timestamp) if timestamp is not None else cm._current_time()
    cm._validate_timestamp(ts)
    return ts


def _freeze_keys(
    keys: Iterable[Any],
    validate: Optional[Callable[[Any], None]] = None,
) -> frozenset:
    frozen = set()
    for key in keys:
        if validate is not None:
            validate(key)
        try:
            frozen.add(key)
        except TypeError as exc:
            raise ChronoMapTypeError("Key must be hashable") from exc
    return frozenset(frozen)


def _snapshot(cm: ChronoMap, timestamp: float) -> Dict[Any, Any]:
    values: Dict[Any, Any] = {}
    cm._acquire_read()
    try:
        for key, versions in cm._store.items():
            if cm._is_expired(key):
                continue
            found, value = value_at(versions, timestamp)
            if found:
                values[key] = value
    finally:
        cm._release_read()
    return values


def _clone(source: Any, timestamp: Optional[float], ops: Tuple[QueryOp, ...]) -> "Query":
    clone = Query.__new__(Query)
    clone._source = source
    clone.timestamp = timestamp
    clone._ops = ops
    return clone


class Query:
    """A lazy, chainable filter over the values visible at one timestamp.

    A ``ChronoMap`` source is snapshotted when a terminal method runs. A plain
    mapping is also accepted, which makes the class useful without a map.
    """

    def __init__(
        self,
        source: Union[ChronoMap, Mapping[Any, Any]],
        timestamp: Any = None,
    ) -> None:
        if isinstance(source, ChronoMap):
            self._source: Union[ChronoMap, Mapping[Any, Any]] = source
            self.timestamp: Optional[float] = _resolve_timestamp(source, timestamp)
        elif isinstance(source, Mapping):
            if timestamp is not None:
                raise QueryError("timestamp is only supported for ChronoMap sources")
            self._source = source
            self.timestamp = None
        else:
            raise ChronoMapTypeError("Query source must be a ChronoMap or a mapping")
        self._ops: Tuple[QueryOp, ...] = ()

    def _with_op(self, op: QueryOp) -> "Query":
        return _clone(self._source, self.timestamp, self._ops + (op,))

    def _evaluate(self) -> List[Item]:
        if isinstance(self._source, ChronoMap):
            assert self.timestamp is not None
            items = list(_snapshot(self._source, self.timestamp).items())
        else:
            items = list(self._source.items())
        for op in self._ops:
            items = op(items)
        return items

    def at(self, timestamp: Any) -> "Query":
        """Move the query to another timestamp, keeping the current filters."""
        if not isinstance(self._source, ChronoMap):
            raise QueryError("at() requires a ChronoMap source")
        return _clone(
            self._source,
            _resolve_timestamp(self._source, timestamp),
            self._ops,
        )

    def where(self, predicate: Callable[[Any, Any], bool]) -> "Query":
        """Keep pairs where predicate(key, value) is truthy."""
        _require_callable(predicate, "predicate")
        return self._with_op(lambda items: [pair for pair in items if predicate(*pair)])

    def filter(self, predicate: Callable[[Any, Any], bool]) -> "Query":
        """Alias for where()."""
        return self.where(predicate)

    def where_key(self, predicate: Callable[[Any], bool]) -> "Query":
        _require_callable(predicate, "predicate")
        return self._with_op(lambda items: [pair for pair in items if predicate(pair[0])])

    def where_value(self, predicate: Callable[[Any], bool]) -> "Query":
        _require_callable(predicate, "predicate")
        return self._with_op(lambda items: [pair for pair in items if predicate(pair[1])])

    def where_field(self, field: Any, predicate: Callable[[Any], bool]) -> "Query":
        """Keep mapping values that contain field and satisfy predicate(value[field])."""
        _require_callable(predicate, "predicate")

        def op(items: List[Item]) -> List[Item]:
            kept = []
            for key, value in items:
                if isinstance(value, Mapping) and field in value and predicate(value[field]):
                    kept.append((key, value))
            return kept

        return self._with_op(op)

    def keys_like(self, pattern: str) -> "Query":
        """Match stringified keys with a shell-style glob."""
        if not isinstance(pattern, str):
            raise ChronoMapTypeError("pattern must be a string")
        return self._with_op(
            lambda items: [pair for pair in items if fnmatch.fnmatchcase(str(pair[0]), pattern)]
        )

    def keys_regex(self, pattern: str) -> "Query":
        """Match stringified keys with a regular expression search."""
        if not isinstance(pattern, str):
            raise ChronoMapTypeError("pattern must be a string")
        try:
            compiled = re.compile(pattern)
        except re.error as exc:
            raise QueryError(f"Invalid regular expression: {exc}") from exc
        return self._with_op(lambda items: [pair for pair in items if compiled.search(str(pair[0]))])

    def keys_in(self, keys: Iterable[Any]) -> "Query":
        allowed = _freeze_keys(keys)
        return self._with_op(lambda items: [pair for pair in items if pair[0] in allowed])

    def values_in(self, values: Iterable[Any]) -> "Query":
        candidates = list(values)
        try:
            allowed = set(candidates)
        except TypeError:
            allowed = None

        def matches(value: Any) -> bool:
            if allowed is not None:
                try:
                    return value in allowed
                except TypeError:
                    pass
            return any(value == candidate for candidate in candidates)

        return self._with_op(lambda items: [pair for pair in items if matches(pair[1])])

    def values_between(self, low: Any, high: Any, *, inclusive: bool = True) -> "Query":
        def op(items: List[Item]) -> List[Item]:
            kept = []
            for pair in items:
                try:
                    matches = low <= pair[1] <= high if inclusive else low < pair[1] < high
                except TypeError:
                    continue
                if matches:
                    kept.append(pair)
            return kept

        return self._with_op(op)

    def select_field(self, field: Any) -> "Query":
        """Replace each mapping value with one of its fields."""

        def op(items: List[Item]) -> List[Item]:
            selected = []
            for key, value in items:
                if isinstance(value, Mapping) and field in value:
                    selected.append((key, value[field]))
            return selected

        return self._with_op(op)

    def transform(self, func: Callable[[Any, Any], Any]) -> "Query":
        """Replace each value with func(key, value), keeping keys unchanged."""
        _require_callable(func, "func")
        return self._with_op(lambda items: [(key, func(key, value)) for key, value in items])

    def sort_by_key(
        self,
        key: Optional[Callable[[Any], Any]] = None,
        *,
        reverse: bool = False,
    ) -> "Query":
        if key is not None:
            _require_callable(key, "key")
        return self._with_op(
            lambda items: sorted(items, key=lambda pair: key(pair[0]) if key else pair[0], reverse=reverse)
        )

    def sort_by_value(
        self,
        key: Optional[Callable[[Any], Any]] = None,
        *,
        reverse: bool = False,
    ) -> "Query":
        if key is not None:
            _require_callable(key, "key")
        return self._with_op(
            lambda items: sorted(items, key=lambda pair: key(pair[1]) if key else pair[1], reverse=reverse)
        )

    def sort_by(
        self,
        func: Callable[[Any, Any], Any],
        *,
        reverse: bool = False,
    ) -> "Query":
        _require_callable(func, "func")
        return self._with_op(lambda items: sorted(items, key=lambda pair: func(*pair), reverse=reverse))

    def limit(self, count: int) -> "Query":
        _require_non_negative(count, "count")
        return self._with_op(lambda items: items[:count])

    def offset(self, count: int) -> "Query":
        _require_non_negative(count, "count")
        return self._with_op(lambda items: items[count:])

    def distinct(self) -> "Query":
        def op(items: List[Item]) -> List[Item]:
            seen_hashable = set()
            seen_unhashable: List[Any] = []
            unique: List[Item] = []
            for pair in items:
                value = pair[1]
                try:
                    if value in seen_hashable:
                        continue
                    seen_hashable.add(value)
                except TypeError:
                    if any(value == existing for existing in seen_unhashable):
                        continue
                    seen_unhashable.append(value)
                unique.append(pair)
            return unique

        return self._with_op(op)

    def items(self) -> List[Item]:
        return self._evaluate()

    def keys(self) -> List[Any]:
        return [key for key, _ in self._evaluate()]

    def values(self) -> List[Any]:
        return [value for _, value in self._evaluate()]

    def to_dict(self) -> Dict[Any, Any]:
        return dict(self._evaluate())

    def count(self) -> int:
        return len(self._evaluate())

    def first(self, default: Any = _MISSING) -> Any:
        items = self._evaluate()
        if items:
            return items[0][1]
        if default is _MISSING:
            raise QueryError("Query is empty")
        return default

    def first_item(self, default: Any = _MISSING) -> Any:
        items = self._evaluate()
        if items:
            return items[0]
        if default is _MISSING:
            raise QueryError("Query is empty")
        return default

    def last(self, default: Any = _MISSING) -> Any:
        items = self._evaluate()
        if items:
            return items[-1][1]
        if default is _MISSING:
            raise QueryError("Query is empty")
        return default

    def last_item(self, default: Any = _MISSING) -> Any:
        items = self._evaluate()
        if items:
            return items[-1]
        if default is _MISSING:
            raise QueryError("Query is empty")
        return default

    def any(self, predicate: Optional[Callable[[Any, Any], bool]] = None) -> bool:
        if predicate is None:
            return bool(self._evaluate())
        _require_callable(predicate, "predicate")
        return any(predicate(*pair) for pair in self._evaluate())

    def all(self, predicate: Callable[[Any, Any], bool]) -> bool:
        _require_callable(predicate, "predicate")
        return all(predicate(*pair) for pair in self._evaluate())

    def fold(self, func: Callable[[Any, Any], Any], initial: Any) -> Any:
        _require_callable(func, "func")
        result = initial
        for _, value in self._evaluate():
            result = func(result, value)
        return result

    def group_by(self, key_func: Callable[[Any, Any], Any]) -> Dict[Any, List[Any]]:
        _require_callable(key_func, "key_func")
        groups: Dict[Any, List[Any]] = {}
        for key, value in self._evaluate():
            groups.setdefault(key_func(key, value), []).append(value)
        return groups

    def group_items(self, key_func: Callable[[Any, Any], Any]) -> Dict[Any, List[Item]]:
        _require_callable(key_func, "key_func")
        groups: Dict[Any, List[Item]] = {}
        for key, value in self._evaluate():
            groups.setdefault(key_func(key, value), []).append((key, value))
        return groups

    def sum(self) -> Any:
        return sum(self.values(), 0)

    def mean(self) -> Any:
        values = self.values()
        if not values:
            raise QueryError("Cannot compute mean of an empty query")
        return mean(values)

    def median(self) -> Any:
        values = self.values()
        if not values:
            raise QueryError("Cannot compute median of an empty query")
        return median(values)

    def min(self) -> Any:
        values = self.values()
        if not values:
            raise QueryError("Cannot compute min of an empty query")
        return min(values)

    def max(self) -> Any:
        values = self.values()
        if not values:
            raise QueryError("Cannot compute max of an empty query")
        return max(values)

    def __len__(self) -> int:
        return self.count()

    def __iter__(self):
        return iter(self._evaluate())


def _clone_versions(
    source: ChronoMap,
    start: float,
    end: float,
    rows: List[Version],
) -> "VersionQuery":
    clone = VersionQuery.__new__(VersionQuery)
    clone._source = source
    clone.start = start
    clone.end = end
    clone._rows = rows
    return clone


class VersionQuery:
    """A chainable filter over individual versions in a time range.

    Rows are ``(key, timestamp, value)`` triples. Expired keys are excluded.
    ``start`` and ``end`` are inclusive; omitted bounds mean no bound.
    """

    def __init__(
        self,
        source: ChronoMap,
        start: Any = None,
        end: Any = None,
        keys: Optional[Iterable[Any]] = None,
    ) -> None:
        if not isinstance(source, ChronoMap):
            raise ChronoMapTypeError("VersionQuery source must be a ChronoMap")
        start_ts = _resolve_timestamp(source, start) if start is not None else -math.inf
        end_ts = _resolve_timestamp(source, end) if end is not None else math.inf
        allowed = None if keys is None else _freeze_keys(keys, source._validate_key)

        rows: List[Version] = []
        source._acquire_read()
        try:
            for key, versions in source._store.items():
                if allowed is not None and key not in allowed:
                    continue
                if source._is_expired(key):
                    continue
                rows.extend(
                    (key, timestamp, value) for timestamp, value in range_versions(versions, start_ts, end_ts)
                )
        finally:
            source._release_read()

        rows.sort(key=lambda row: row[1])
        self._source = source
        self.start = start_ts
        self.end = end_ts
        self._rows = rows

    def _with_rows(self, rows: List[Version]) -> "VersionQuery":
        return _clone_versions(self._source, self.start, self.end, rows)

    def where(self, predicate: Callable[[Any, float, Any], bool]) -> "VersionQuery":
        _require_callable(predicate, "predicate")
        return self._with_rows([row for row in self._rows if predicate(*row)])

    def where_key(self, predicate: Callable[[Any], bool]) -> "VersionQuery":
        _require_callable(predicate, "predicate")
        return self._with_rows([row for row in self._rows if predicate(row[0])])

    def where_value(self, predicate: Callable[[Any], bool]) -> "VersionQuery":
        _require_callable(predicate, "predicate")
        return self._with_rows([row for row in self._rows if predicate(row[2])])

    def where_timestamp(self, predicate: Callable[[float], bool]) -> "VersionQuery":
        _require_callable(predicate, "predicate")
        return self._with_rows([row for row in self._rows if predicate(row[1])])

    def between(self, start: Any, end: Any) -> "VersionQuery":
        start_ts = _resolve_timestamp(self._source, start)
        end_ts = _resolve_timestamp(self._source, end)
        result = self._with_rows([row for row in self._rows if start_ts <= row[1] <= end_ts])
        result.start = max(self.start, start_ts)
        result.end = min(self.end, end_ts)
        return result

    def keys_in(self, keys: Iterable[Any]) -> "VersionQuery":
        allowed = _freeze_keys(keys, self._source._validate_key)
        return self._with_rows([row for row in self._rows if row[0] in allowed])

    def values_in(self, values: Iterable[Any]) -> "VersionQuery":
        candidates = list(values)
        return self._with_rows(
            [row for row in self._rows if any(row[2] == candidate for candidate in candidates)]
        )

    def sort_by_timestamp(self, *, reverse: bool = False) -> "VersionQuery":
        return self._with_rows(sorted(self._rows, key=lambda row: row[1], reverse=reverse))

    def sort_by_key(
        self,
        key: Optional[Callable[[Any], Any]] = None,
        *,
        reverse: bool = False,
    ) -> "VersionQuery":
        if key is not None:
            _require_callable(key, "key")
        return self._with_rows(
            sorted(
                self._rows,
                key=lambda row: key(row[0]) if key else row[0],
                reverse=reverse,
            )
        )

    def sort_by_value(
        self,
        key: Optional[Callable[[Any], Any]] = None,
        *,
        reverse: bool = False,
    ) -> "VersionQuery":
        if key is not None:
            _require_callable(key, "key")
        return self._with_rows(
            sorted(
                self._rows,
                key=lambda row: key(row[2]) if key else row[2],
                reverse=reverse,
            )
        )

    def limit(self, count: int) -> "VersionQuery":
        _require_non_negative(count, "count")
        return self._with_rows(self._rows[:count])

    def offset(self, count: int) -> "VersionQuery":
        _require_non_negative(count, "count")
        return self._with_rows(self._rows[count:])

    def rows(self) -> List[Version]:
        return list(self._rows)

    def to_records(self) -> List[Dict[str, Any]]:
        return [
            {
                "key": key,
                "timestamp": timestamp,
                "datetime": datetime.fromtimestamp(timestamp),
                "value": value,
            }
            for key, timestamp, value in self._rows
        ]

    def keys(self) -> List[Any]:
        return list(dict.fromkeys(key for key, _, _ in self._rows))

    def values(self) -> List[Any]:
        return [value for _, _, value in self._rows]

    def count(self) -> int:
        return len(self._rows)

    def latest_per_key(self) -> Dict[Any, Any]:
        latest: Dict[Any, Tuple[float, Any]] = {}
        for key, timestamp, value in self._rows:
            if key not in latest or timestamp >= latest[key][0]:
                latest[key] = (timestamp, value)
        return {key: value for key, (_, value) in latest.items()}

    def earliest_per_key(self) -> Dict[Any, Any]:
        earliest: Dict[Any, Tuple[float, Any]] = {}
        for key, timestamp, value in self._rows:
            if key not in earliest or timestamp < earliest[key][0]:
                earliest[key] = (timestamp, value)
        return {key: value for key, (_, value) in earliest.items()}

    def group_by_key(self) -> Dict[Any, List[Tuple[float, Any]]]:
        groups: Dict[Any, List[Tuple[float, Any]]] = {}
        for key, timestamp, value in self._rows:
            groups.setdefault(key, []).append((timestamp, value))
        return groups

    def to_chronomap(self, max_history: Optional[int] = None) -> ChronoMap:
        """Build a new map from the selected rows."""
        cm = ChronoMap(max_history=max_history, cache_size=0, enable_ttl_cleanup=False)
        for key, timestamp, value in self._rows:
            cm.put(key, value, timestamp=timestamp)
        return cm

    def first(self, default: Any = _MISSING) -> Any:
        if self._rows:
            return self._rows[0][2]
        if default is _MISSING:
            raise QueryError("Version query is empty")
        return default

    def last(self, default: Any = _MISSING) -> Any:
        if self._rows:
            return self._rows[-1][2]
        if default is _MISSING:
            raise QueryError("Version query is empty")
        return default

    def fold(self, func: Callable[[Any, Any], Any], initial: Any) -> Any:
        _require_callable(func, "func")
        result = initial
        for _, _, value in self._rows:
            result = func(result, value)
        return result

    def sum(self) -> Any:
        return sum(self.values(), 0)

    def mean(self) -> Any:
        values = self.values()
        if not values:
            raise QueryError("Cannot compute mean of an empty version query")
        return mean(values)

    def median(self) -> Any:
        values = self.values()
        if not values:
            raise QueryError("Cannot compute median of an empty version query")
        return median(values)

    def min(self) -> Any:
        values = self.values()
        if not values:
            raise QueryError("Cannot compute min of an empty version query")
        return min(values)

    def max(self) -> Any:
        values = self.values()
        if not values:
            raise QueryError("Cannot compute max of an empty version query")
        return max(values)

    def __len__(self) -> int:
        return len(self._rows)

    def __iter__(self):
        return iter(list(self._rows))


def query(
    source: Union[ChronoMap, Mapping[Any, Any]],
    timestamp: Any = None,
) -> Query:
    """Start a value query over a map or plain mapping."""
    return Query(source, timestamp=timestamp)


def versions(
    source: ChronoMap,
    start: Any = None,
    end: Any = None,
    keys: Optional[Iterable[Any]] = None,
) -> VersionQuery:
    """Start a version query over a time range."""
    return VersionQuery(source, start=start, end=end, keys=keys)


__all__ = ["Query", "VersionQuery", "QueryError", "query", "versions"]
