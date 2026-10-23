# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Focused time-travel benchmarks for the core ChronoMap hot paths."""

from __future__ import annotations

import timeit
from typing import Callable, Dict, List, Tuple

from chronomap import ChronoMap, query

BENCHMARKS: Dict[str, Tuple[Callable[[], object], int]] = {}


def benchmark(name: str, number: int = 1) -> Callable[[Callable[[], object]], Callable[[], object]]:
    def register(function: Callable[[], object]) -> Callable[[], object]:
        BENCHMARKS[name] = (function, number)
        return function

    return register


def make_history_map(history: int) -> ChronoMap:
    chronomap = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    chronomap._store["hot"] = [(float(index), index) for index in range(history)]
    return chronomap


def make_batch_map(keys: int) -> ChronoMap:
    chronomap = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    chronomap.put_many({index: index for index in range(keys)})
    return chronomap


@benchmark("get / 100k history")
def benchmark_get():
    chronomap = make_history_map(100_000)
    return lambda: chronomap.get("hot", timestamp=50_000.0)


@benchmark("get_range 10 / 100k history")
def benchmark_get_range():
    chronomap = make_history_map(100_000)
    return lambda: chronomap.get_range("hot", 50_000.0, 50_010.0)


@benchmark("put / 50k cached keys")
def benchmark_put():
    chronomap = ChronoMap(cache_size=100_000, enable_ttl_cleanup=False)
    for index in range(50_000):
        chronomap._store[index] = [(1.0, index)]
        chronomap.get(index)
    return lambda: chronomap.put(0, "updated")


@benchmark("expire_many / 1k keys")
def benchmark_expire_many():
    chronomap = make_batch_map(1_000)
    keys = list(range(1_000))
    return lambda: chronomap.expire_many(keys, ttl=3600)


@benchmark("ttl_remaining_many / 1k keys")
def benchmark_ttl_remaining_many():
    chronomap = make_batch_map(1_000)
    keys = list(range(1_000))
    chronomap.expire_many(keys, ttl=3600)
    return lambda: chronomap.ttl_remaining_many(keys)


@benchmark("query.distinct / 5k unique values")
def benchmark_distinct():
    chronomap = ChronoMap(cache_size=0, enable_ttl_cleanup=False)
    for index in range(5_000):
        chronomap._store[index] = [(1.0, index)]
    return lambda: query(chronomap).distinct().items()


def main() -> None:
    results: List[Tuple[str, float]] = []
    for name, (factory, number) in BENCHMARKS.items():
        function = factory()
        seconds = min(timeit.repeat(function, number=number, repeat=5))
        results.append((name, seconds / number * 1_000_000))

    width = max(len(name) for name, _ in results)
    for name, microseconds in results:
        print(f"{name:<{width}}  {microseconds:>12.2f} us/op")


if __name__ == "__main__":
    main()
