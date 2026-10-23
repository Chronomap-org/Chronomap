# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Versioned, type-preserving archives for ChronoMap.

The built-in ``save_json()`` coerces every key to a string, so a map with
integer or tuple keys cannot survive a JSON round trip. This module writes a
self-describing archive instead: keys, values, TTL deadlines, history length,
and snapshot time are all preserved.

Two formats are available. ``json`` is inspectable and safe to hand to other
tools, but only supports values that the tagged codec understands: scalars,
bytes, dates, lists, tuples, sets, and mappings. ``pickle`` supports arbitrary
Python objects, but loading a pickle can execute code, so only load archives
you trust.

    from chronomap.persistence import dump, load, inspect

    dump(cm, "state.json")
    restored = load("state.json")
    print(inspect("state.json").version_count)

Writes are atomic by default: data goes to a temporary file in the target
directory, is flushed to disk, and then replaces the destination.
"""

from __future__ import annotations

import base64
import binascii
import bz2
import gzip
import json
import lzma
import math
import os
import pickle
import re
import tempfile
import time as _time
import zlib
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from .core import ChronoMap
from .exceptions import ChronoMapError, ChronoMapTypeError, ChronoMapValueError

FORMAT_NAME = "chronomap"
FORMAT_VERSION = 1

_HEADER_RE = re.compile(rb"^CHRONOMAP/(\d+) (json|pickle)(?: ([a-z0-9]+))?\n")
_FORMAT_BY_SUFFIX = {".json": "json", ".pickle": "pickle", ".pkl": "pickle"}
_SUPPORTED_FORMATS = ("json", "pickle")
_SUPPORTED_COMPRESSION = ("zlib", "gzip", "bz2", "lzma")

PathLike = Union[str, os.PathLike]


class PersistenceError(ChronoMapError):
    """Base class for archive read/write failures."""


class UnsupportedFormatError(PersistenceError):
    """The requested or detected archive format is not supported."""


class CorruptArchiveError(PersistenceError):
    """The archive is structurally valid but its contents are not."""


class CodecError(PersistenceError):
    """A value cannot be represented by the selected archive codec."""


@dataclass(frozen=True)
class ArchiveInfo:
    """Summary of an archive's header and payload."""

    format: str
    format_version: int
    compression: Optional[str]
    created_at: Optional[float]
    key_count: int
    version_count: int
    max_history: Optional[int]
    snapshot_time: Optional[float]


def _validate_format(fmt: Any) -> str:
    if fmt not in _SUPPORTED_FORMATS:
        raise UnsupportedFormatError(
            f"Unsupported archive format {fmt!r}; expected one of {_SUPPORTED_FORMATS}"
        )
    return fmt


def _validate_compression(compression: Any) -> Optional[str]:
    if compression is None:
        return None
    if compression not in _SUPPORTED_COMPRESSION:
        raise UnsupportedFormatError(
            f"Unsupported compression {compression!r}; expected one of {_SUPPORTED_COMPRESSION}"
        )
    return compression


def _format_for_path(path: Path, fmt: Optional[str]) -> str:
    if fmt is None or fmt == "auto":
        return _FORMAT_BY_SUFFIX.get(path.suffix.lower(), "json")
    return _validate_format(fmt)


def _compress(data: bytes, compression: Optional[str]) -> bytes:
    if compression == "zlib":
        return zlib.compress(data)
    if compression == "gzip":
        return gzip.compress(data)
    if compression == "bz2":
        return bz2.compress(data)
    if compression == "lzma":
        return lzma.compress(data)
    return data


def _decompress(data: bytes, compression: Optional[str]) -> bytes:
    try:
        if compression == "zlib":
            return zlib.decompress(data)
        if compression == "gzip":
            return gzip.decompress(data)
        if compression == "bz2":
            return bz2.decompress(data)
        if compression == "lzma":
            return lzma.decompress(data)
    except (OSError, EOFError, ValueError, zlib.error, lzma.LZMAError) as exc:
        raise CorruptArchiveError(f"Could not decompress {compression} payload") from exc
    return data


def _encode(value: Any) -> List[Any]:
    if value is None:
        return ["none"]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", value]
    if isinstance(value, float):
        if math.isnan(value):
            return ["float", "nan"]
        if math.isinf(value):
            return ["float", "inf" if value > 0 else "-inf"]
        return ["float", value]
    if isinstance(value, str):
        return ["str", value]
    if isinstance(value, bytes):
        return ["bytes", base64.b64encode(value).decode("ascii")]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat()]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, time):
        return ["time", value.isoformat()]
    if isinstance(value, list):
        return ["list", [_encode(item) for item in value]]
    if isinstance(value, tuple):
        return ["tuple", [_encode(item) for item in value]]
    if isinstance(value, set):
        return ["set", [_encode(item) for item in sorted(value, key=repr)]]
    if isinstance(value, frozenset):
        return ["frozenset", [_encode(item) for item in sorted(value, key=repr)]]
    if isinstance(value, Mapping):
        return ["dict", [[_encode(key), _encode(item)] for key, item in value.items()]]
    raise CodecError(f"Cannot encode {type(value).__name__} as JSON; use format='pickle'")


def _decode(node: Any) -> Any:
    if not isinstance(node, list) or not node or not isinstance(node[0], str):
        raise CorruptArchiveError("Encoded value must be a non-empty tagged list")
    tag = node[0]
    if tag == "none" and len(node) == 1:
        return None
    if tag == "bool" and len(node) == 2 and isinstance(node[1], bool):
        return node[1]
    if tag == "int" and len(node) == 2 and isinstance(node[1], int) and not isinstance(node[1], bool):
        return node[1]
    if tag == "float" and len(node) == 2:
        if node[1] == "nan":
            return math.nan
        if node[1] == "inf":
            return math.inf
        if node[1] == "-inf":
            return -math.inf
        if isinstance(node[1], (int, float)) and not isinstance(node[1], bool):
            return float(node[1])
        raise CorruptArchiveError("Invalid float payload")
    if tag == "str" and len(node) == 2 and isinstance(node[1], str):
        return node[1]
    if tag == "bytes" and len(node) == 2 and isinstance(node[1], str):
        try:
            return base64.b64decode(node[1].encode("ascii"), validate=True)
        except (ValueError, binascii.Error) as exc:
            raise CorruptArchiveError("Invalid base64 payload") from exc
    if tag in ("datetime", "date", "time") and len(node) == 2 and isinstance(node[1], str):
        try:
            if tag == "datetime":
                return datetime.fromisoformat(node[1])
            if tag == "date":
                return date.fromisoformat(node[1])
            return time.fromisoformat(node[1])
        except ValueError as exc:
            raise CorruptArchiveError(f"Invalid {tag} payload") from exc
    if tag in ("list", "tuple", "set", "frozenset") and len(node) == 2:
        if not isinstance(node[1], list):
            raise CorruptArchiveError(f"Invalid {tag} payload")
        items = [_decode(item) for item in node[1]]
        if tag == "list":
            return items
        if tag == "tuple":
            return tuple(items)
        try:
            return set(items) if tag == "set" else frozenset(items)
        except TypeError as exc:
            raise CorruptArchiveError(f"{tag} contains an unhashable value") from exc
    if tag == "dict" and len(node) == 2 and isinstance(node[1], list):
        decoded: Dict[Any, Any] = {}
        for pair in node[1]:
            if not isinstance(pair, list) or len(pair) != 2:
                raise CorruptArchiveError("Invalid dict entry")
            key = _decode(pair[0])
            try:
                decoded[key] = _decode(pair[1])
            except TypeError as exc:
                raise CorruptArchiveError("dict contains an unhashable key") from exc
        return decoded
    raise CorruptArchiveError(f"Unknown or invalid encoded tag {tag!r}")


def _collect(cm: ChronoMap, include_expired: bool) -> Dict[str, Any]:
    entries: List[Dict[str, Any]] = []
    cm._acquire_read()
    try:
        for key, versions in cm._store.items():
            if not include_expired and cm._is_expired(key):
                continue
            entry: Dict[str, Any] = {
                "key": key,
                "versions": [[timestamp, value] for timestamp, value in versions],
            }
            if key in cm._ttl:
                entry["ttl"] = cm._ttl[key]
            entries.append(entry)
        return {
            "format": FORMAT_NAME,
            "format_version": FORMAT_VERSION,
            "created_at": cm._current_time(),
            "snapshot_time": cm._snapshot_time,
            "max_history": cm._max_history,
            "entries": entries,
        }
    finally:
        cm._release_read()


def _encode_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "format": payload["format"],
        "format_version": payload["format_version"],
        "created_at": payload["created_at"],
        "snapshot_time": payload["snapshot_time"],
        "max_history": payload["max_history"],
        "entries": [
            {
                "key": _encode(entry["key"]),
                "versions": [[timestamp, _encode(value)] for timestamp, value in entry["versions"]],
                **({"ttl": entry["ttl"]} if "ttl" in entry else {}),
            }
            for entry in payload["entries"]
        ],
    }


def _require_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CorruptArchiveError(f"{field} must be numeric")
    if not math.isfinite(value):
        raise CorruptArchiveError(f"{field} must be finite")
    return float(value)


def _normalize_payload(payload: Any, encoded: bool) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise CorruptArchiveError("Archive payload must be an object")
    if payload.get("format") != FORMAT_NAME:
        raise CorruptArchiveError("Archive is not a ChronoMap archive")
    version = payload.get("format_version")
    if version != FORMAT_VERSION:
        raise UnsupportedFormatError(f"Unsupported archive version {version!r}; expected {FORMAT_VERSION}")

    max_history = payload.get("max_history")
    if max_history is not None:
        if isinstance(max_history, bool) or not isinstance(max_history, int) or max_history < 1:
            raise CorruptArchiveError("max_history must be a positive integer or null")
    created_at = payload.get("created_at")
    if created_at is not None:
        created_at = _require_number(created_at, "created_at")
    snapshot_time = payload.get("snapshot_time")
    if snapshot_time is not None:
        snapshot_time = _require_number(snapshot_time, "snapshot_time")

    raw_entries = payload.get("entries")
    if not isinstance(raw_entries, list):
        raise CorruptArchiveError("entries must be a list")

    entries: List[Dict[str, Any]] = []
    seen = set()
    for raw in raw_entries:
        if not isinstance(raw, dict):
            raise CorruptArchiveError("Each entry must be an object")
        if "key" not in raw:
            raise CorruptArchiveError("Entry is missing its key")
        key = _decode(raw["key"]) if encoded else raw["key"]
        try:
            hash(key)
        except TypeError as exc:
            raise CorruptArchiveError("Entry key must be hashable") from exc
        if key in seen:
            raise CorruptArchiveError(f"Duplicate key in archive: {key!r}")
        seen.add(key)

        raw_versions = raw.get("versions")
        if not isinstance(raw_versions, list) or not raw_versions:
            raise CorruptArchiveError(f"Key {key!r} must have at least one version")
        versions: List[Tuple[float, Any]] = []
        previous = -math.inf
        for pair in raw_versions:
            if not isinstance(pair, list) or len(pair) != 2:
                raise CorruptArchiveError(f"Invalid version for key {key!r}")
            timestamp = _require_number(pair[0], "timestamp")
            if timestamp < previous:
                raise CorruptArchiveError(f"Versions for key {key!r} are not sorted")
            previous = timestamp
            value = _decode(pair[1]) if encoded else pair[1]
            versions.append((timestamp, value))

        entry: Dict[str, Any] = {"key": key, "versions": versions}
        if "ttl" in raw:
            entry["ttl"] = _require_number(raw["ttl"], "ttl")
        entries.append(entry)

    return {
        "created_at": created_at,
        "snapshot_time": snapshot_time,
        "max_history": max_history,
        "entries": entries,
    }


def _build_map(
    payload: Dict[str, Any],
    *,
    debug: bool,
    use_rwlock: bool,
    enable_ttl_cleanup: bool,
    drop_expired: bool,
    kwargs: Dict[str, Any],
) -> ChronoMap:
    now = _time.time()
    max_history = kwargs.pop("max_history", payload["max_history"])
    cm = ChronoMap(
        debug=debug,
        use_rwlock=use_rwlock,
        max_history=max_history,
        enable_ttl_cleanup=enable_ttl_cleanup,
        **kwargs,
    )
    store: Dict[Any, List[Tuple[float, Any]]] = {}
    ttl: Dict[Any, float] = {}
    for entry in payload["entries"]:
        key = entry["key"]
        deadline = entry.get("ttl")
        if drop_expired and deadline is not None and deadline <= now:
            continue
        versions = entry["versions"]
        if max_history is not None:
            versions = versions[-max_history:]
        store[key] = versions
        if deadline is not None:
            ttl[key] = deadline
    cm._store = store
    cm._ttl = ttl
    cm._snapshot_time = payload["snapshot_time"]
    return cm


def _pack(fmt: str, compression: Optional[str], body: bytes) -> bytes:
    header = f"CHRONOMAP/{FORMAT_VERSION} {fmt}".encode("ascii")
    if compression is not None:
        header += b" " + compression.encode("ascii")
    return header + b"\n" + _compress(body, compression)


def _read_archive(
    data: bytes,
    fmt: Optional[str],
) -> Tuple[str, Optional[str], Any]:
    match = _HEADER_RE.match(data)
    if match:
        version = int(match.group(1))
        if version != FORMAT_VERSION:
            raise UnsupportedFormatError(f"Unsupported archive version {version}; expected {FORMAT_VERSION}")
        actual_format = match.group(2).decode("ascii")
        compression = match.group(3).decode("ascii") if match.group(3) else None
        if compression is not None:
            _validate_compression(compression)
        body = _decompress(data[match.end() :], compression)
    elif data.lstrip().startswith(b"{"):
        actual_format = "json"
        compression = None
        body = data
    else:
        raise UnsupportedFormatError("Unrecognized archive header; expected a ChronoMap archive")

    if fmt is not None and fmt != actual_format:
        raise PersistenceError(f"Archive is {actual_format}, but {fmt} was requested")

    if actual_format == "json":
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorruptArchiveError("Invalid JSON archive payload") from exc
        return actual_format, compression, _normalize_payload(payload, encoded=True)

    try:
        payload = pickle.loads(body)
    except (pickle.UnpicklingError, EOFError, ValueError) as exc:
        raise CorruptArchiveError("Invalid pickle archive payload") from exc
    return actual_format, compression, _normalize_payload(payload, encoded=False)


def dumps(
    cm: ChronoMap,
    *,
    format: str = "json",
    compression: Optional[str] = None,
    indent: Optional[int] = 2,
    include_expired: bool = True,
) -> bytes:
    """Serialize a map to bytes without touching the filesystem."""
    if not isinstance(cm, ChronoMap):
        raise ChronoMapTypeError("dumps() requires a ChronoMap")
    fmt = _validate_format(format)
    compression = _validate_compression(compression)
    payload = _collect(cm, include_expired)
    if fmt == "json":
        body = json.dumps(_encode_payload(payload), indent=indent).encode("utf-8")
    else:
        body = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
    return _pack(fmt, compression, body)


def dump(
    cm: ChronoMap,
    path: PathLike,
    *,
    format: Optional[str] = None,
    compression: Optional[str] = None,
    indent: Optional[int] = 2,
    include_expired: bool = True,
    atomic: bool = True,
) -> Path:
    """Write a map to an archive, inferring JSON/pickle from the suffix."""
    target = Path(path)
    fmt = _format_for_path(target, format)
    data = dumps(
        cm,
        format=fmt,
        compression=compression,
        indent=indent,
        include_expired=include_expired,
    )
    if atomic:
        _atomic_write(target, data)
    else:
        try:
            target.write_bytes(data)
        except OSError as exc:
            raise PersistenceError(f"Could not write archive {target}: {exc}") from exc
    return target


def _atomic_write(target: Path, data: bytes) -> None:
    parent = target.parent
    if not parent.exists():
        raise PersistenceError(f"Directory does not exist: {parent}")
    descriptor = -1
    temporary: Optional[str] = None
    try:
        descriptor, temporary = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(parent))
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        temporary = None
    except OSError as exc:
        raise PersistenceError(f"Could not write archive {target}: {exc}") from exc
    finally:
        if descriptor != -1:
            os.close(descriptor)
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def loads(
    data: bytes,
    *,
    format: Optional[str] = None,
    debug: bool = False,
    use_rwlock: bool = True,
    enable_ttl_cleanup: bool = True,
    drop_expired: bool = False,
    **kwargs: Any,
) -> ChronoMap:
    """Deserialize an archive from bytes.

    ``format`` only needs to be supplied to reject an unexpected archive
    type; the actual format is always read from the header.
    """
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise ChronoMapTypeError("loads() requires bytes")
    fmt = None if format is None or format == "auto" else _validate_format(format)
    _, _, payload = _read_archive(bytes(data), fmt)
    return _build_map(
        payload,
        debug=debug,
        use_rwlock=use_rwlock,
        enable_ttl_cleanup=enable_ttl_cleanup,
        drop_expired=drop_expired,
        kwargs=kwargs,
    )


def load(
    path: PathLike,
    *,
    format: Optional[str] = None,
    debug: bool = False,
    use_rwlock: bool = True,
    enable_ttl_cleanup: bool = True,
    drop_expired: bool = False,
    **kwargs: Any,
) -> ChronoMap:
    """Read an archive from disk."""
    target = Path(path)
    fmt = None if format is None else _format_for_path(target, format)
    try:
        data = target.read_bytes()
    except OSError as exc:
        raise PersistenceError(f"Could not read archive {target}: {exc}") from exc
    return loads(
        data,
        format=fmt,
        debug=debug,
        use_rwlock=use_rwlock,
        enable_ttl_cleanup=enable_ttl_cleanup,
        drop_expired=drop_expired,
        **kwargs,
    )


def inspect(path: PathLike) -> ArchiveInfo:
    """Read only an archive's metadata and row counts."""
    target = Path(path)
    try:
        data = target.read_bytes()
    except OSError as exc:
        raise PersistenceError(f"Could not read archive {target}: {exc}") from exc
    actual_format, compression, payload = _read_archive(data, None)
    key_count = len(payload["entries"])
    version_count = sum(len(entry["versions"]) for entry in payload["entries"])
    return ArchiveInfo(
        format=actual_format,
        format_version=FORMAT_VERSION,
        compression=compression,
        created_at=payload.get("created_at"),
        key_count=key_count,
        version_count=version_count,
        max_history=payload["max_history"],
        snapshot_time=payload["snapshot_time"],
    )


__all__ = [
    "FORMAT_NAME",
    "FORMAT_VERSION",
    "ArchiveInfo",
    "PersistenceError",
    "UnsupportedFormatError",
    "CorruptArchiveError",
    "CodecError",
    "dump",
    "dumps",
    "load",
    "loads",
    "inspect",
]
