# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Command-line interface for ChronoMap.

The CLI is built around save files written by ``ChronoMap.save_json`` and
``ChronoMap.save_pickle``. Read-only commands load a file, operate on the
in-memory map, and exit. Mutating commands update the map and atomically
replace the file, so an interrupted write cannot truncate a good save.

Examples::

    chronomap show state.json
    chronomap get state.json session:42
    chronomap history state.json session:42 --limit 5
    chronomap put state.json session:42 active --ttl 3600
    chronomap prune state.json --keep-last 10
    chronomap merge a.json b.json --output merged.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Sequence, Tuple, Union

from ._version import __version__
from .core import ChronoMap
from .exceptions import ChronoMapError


class Colors:
    """ANSI color codes for terminal output."""

    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"


class CLIError(Exception):
    """A command failed in a way that should be reported without a traceback."""


def colorize(text: str, color: str) -> str:
    """Wrap `text` in an ANSI color code, resetting at the end."""
    return f"{color}{text}{Colors.END}"


def _paint(text: str, color: str, enabled: bool) -> str:
    return colorize(text, color) if enabled else text


def format_timestamp(ts: float) -> str:
    """Format a Unix timestamp as a local `YYYY-MM-DD HH:MM:SS` string."""
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def parse_value(raw: str) -> Any:
    """Best-effort parse of a CLI string argument into a Python value.

    Tries int, then float, then JSON (which also covers dicts, lists,
    true/false/null), and falls back to the original string if nothing
    else matches.
    """
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        pass
    return raw


def parse_timestamp(raw: str) -> Union[float, str]:
    """Parse a numeric timestamp, leaving ISO-8601 strings for ChronoMap."""
    try:
        return float(raw)
    except ValueError:
        return raw


def _parse_key(raw: str, literal: bool) -> Any:
    return raw if literal else parse_value(raw)


def format_value(value: Any) -> str:
    """Render a stored value for human-readable CLI output."""
    if isinstance(value, str):
        return value
    if value is None:
        return "None"
    if isinstance(value, (bytes, bytearray)):
        return repr(value)
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return repr(value)


def _json_text(payload: Any) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=False, default=str)


def _key_name(key: Any) -> str:
    return key if isinstance(key, str) else repr(key)


def _color_enabled(args: argparse.Namespace) -> bool:
    if not getattr(args, "color", True):
        return False
    if "NO_COLOR" in os.environ:
        return False
    return sys.stdout.isatty()


def _fail(message: str, args: argparse.Namespace) -> int:
    print(_paint(f"Error: {message}", Colors.RED, _color_enabled(args)), file=sys.stderr)
    return 1


def _display_map(cm: ChronoMap, path: Union[str, Path], enabled: bool) -> None:
    print(_paint(f"Loaded ChronoMap from {path}", Colors.GREEN, enabled))
    latest = cm.latest()
    print(f"Keys: {len(latest)}")
    for key, value in latest.items():
        print(f"{_key_name(key)}: {format_value(value)}")


def _load(path: Union[str, Path]) -> ChronoMap:
    file_path = Path(path)
    if not file_path.exists():
        raise CLIError(f"File not found: {path}")
    if not file_path.is_file():
        raise CLIError(f"Not a file: {path}")

    suffix = file_path.suffix.lower()
    if suffix == ".json":
        loader = ChronoMap.load_json
    elif suffix in (".pkl", ".pickle"):
        loader = ChronoMap.load_pickle
    else:
        raise CLIError(f"Unsupported file type: {suffix or '<none>'}")

    try:
        return loader(file_path)
    except Exception as exc:  # noqa: BLE001 - CLI boundary, want to report any failure
        raise CLIError(f"Error loading file: {exc}") from exc


def _save(cm: ChronoMap, path: Union[str, Path]) -> None:
    file_path = Path(path)
    suffix = file_path.suffix.lower()
    if suffix not in (".json", ".pkl", ".pickle"):
        raise CLIError(f"Unsupported file type: {suffix or '<none>'}")
    if not file_path.parent.exists():
        raise CLIError(f"Output directory does not exist: {file_path.parent}")

    fd, temp_name = tempfile.mkstemp(prefix=f".{file_path.name}.", suffix=suffix, dir=str(file_path.parent))
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        if suffix == ".json":
            cm.save_json(temp_path)
        else:
            cm.save_pickle(temp_path)
        os.replace(str(temp_path), str(file_path))
    except Exception as exc:  # noqa: BLE001 - CLI boundary, want to report any failure
        raise CLIError(f"Error saving file: {exc}") from exc
    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def load_and_display(path: str) -> None:
    """Load a ChronoMap from a .json or .pkl file and print its current state."""
    try:
        cm = _load(path)
    except CLIError as exc:
        print(colorize(str(exc), Colors.RED))
        return
    _display_map(cm, path, enabled=True)


def _cmd_show(args: argparse.Namespace) -> int:
    cm = _load(args.path)
    if args.json:
        latest = cm.latest()
        print(
            _json_text(
                {
                    "path": str(args.path),
                    "count": len(latest),
                    "values": {str(key): value for key, value in latest.items()},
                }
            )
        )
    else:
        _display_map(cm, args.path, _color_enabled(args))
    return 0


def _cmd_get(args: argparse.Namespace) -> int:
    cm = _load(args.path)
    key = _parse_key(args.key, args.literal_key)
    timestamp = parse_timestamp(args.timestamp) if args.timestamp is not None else None
    sentinel = object()
    value = cm.get(key, timestamp=timestamp, default=sentinel)
    found = value is not sentinel

    if not found:
        if args.default is None:
            raise CLIError(f"Key not found: {_key_name(key)}")
        value = parse_value(args.default)

    if args.json:
        print(
            _json_text(
                {
                    "key": key,
                    "found": found,
                    "timestamp": timestamp,
                    "value": value,
                }
            )
        )
    else:
        print(format_value(value))
    return 0


def _cmd_history(args: argparse.Namespace) -> int:
    if args.limit is not None and args.limit < 1:
        raise CLIError("--limit must be at least 1")
    cm = _load(args.path)
    key = _parse_key(args.key, args.literal_key)
    history = cm.history(key)
    if not history:
        raise CLIError(f"No history for key: {_key_name(key)}")
    if args.limit is not None:
        history = history[-args.limit :]
    if args.reverse:
        history = list(reversed(history))

    if args.json:
        print(
            _json_text(
                [
                    {
                        "key": key,
                        "timestamp": ts,
                        "datetime": format_timestamp(ts),
                        "value": value,
                    }
                    for ts, value in history
                ]
            )
        )
    else:
        for ts, value in history:
            print(f"{format_timestamp(ts)}  {format_value(value)}")
    return 0


def _cmd_put(args: argparse.Namespace) -> int:
    cm = _load(args.path)
    key = _parse_key(args.key, args.literal_key)
    value = parse_value(args.value)
    timestamp = parse_timestamp(args.timestamp) if args.timestamp is not None else None
    cm.put(key, value, timestamp=timestamp, ttl=args.ttl)
    _save(cm, args.path)

    if args.json:
        print(
            _json_text(
                {
                    "key": key,
                    "value": value,
                    "timestamp": timestamp,
                    "ttl": args.ttl,
                    "path": str(args.path),
                }
            )
        )
    else:
        print(f"Wrote {_key_name(key)} = {format_value(value)}")
    return 0


def _cmd_delete(args: argparse.Namespace) -> int:
    cm = _load(args.path)
    key = _parse_key(args.key, args.literal_key)
    if not cm.delete(key):
        raise CLIError(f"Key not found: {_key_name(key)}")
    _save(cm, args.path)

    if args.json:
        print(_json_text({"key": key, "deleted": True, "path": str(args.path)}))
    else:
        print(f"Deleted {_key_name(key)}")
    return 0


def _cmd_keys(args: argparse.Namespace) -> int:
    cm = _load(args.path)
    keys = list(cm.keys())
    if args.json:
        print(_json_text({"count": len(keys), "keys": [str(key) for key in keys]}))
    else:
        for key in keys:
            print(_key_name(key))
    return 0


def _cmd_stats(args: argparse.Namespace) -> int:
    cm = _load(args.path)
    stats = cm.get_stats()
    if args.json:
        print(_json_text({"path": str(args.path), "stats": stats}))
    else:
        for name in sorted(stats):
            print(f"{name}: {stats[name]}")
    return 0


def _cmd_ttl(args: argparse.Namespace) -> int:
    cm = _load(args.path)
    if args.keys:
        keys = [_parse_key(raw, args.literal_key) for raw in args.keys]
    else:
        keys = [key for key in cm.keys() if cm.ttl_remaining(key) is not None]
        if not keys:
            if args.json:
                print(_json_text({"path": str(args.path), "ttls": {}}))
            else:
                print("No TTLs tracked")
            return 0

    ttls = {key: cm.ttl_remaining(key) for key in keys}
    if args.json:
        print(_json_text({"path": str(args.path), "ttls": {str(key): value for key, value in ttls.items()}}))
    else:
        for key, remaining in ttls.items():
            rendered = "none" if remaining is None else f"{remaining:.2f}s"
            print(f"{_key_name(key)}: {rendered}")
    return 0


def _cmd_expire(args: argparse.Namespace) -> int:
    cm = _load(args.path)
    key = _parse_key(args.key, args.literal_key)
    if not cm.expire(key, args.ttl):
        raise CLIError(f"Key not found or expired: {_key_name(key)}")
    _save(cm, args.path)

    if args.json:
        print(
            _json_text(
                {
                    "key": key,
                    "ttl": args.ttl,
                    "expires_in": cm.ttl_remaining(key),
                    "path": str(args.path),
                }
            )
        )
    else:
        print(f"Set TTL for {_key_name(key)} to {args.ttl:g}s")
    return 0


def _cmd_persist(args: argparse.Namespace) -> int:
    cm = _load(args.path)
    key = _parse_key(args.key, args.literal_key)
    if not cm.persist(key):
        raise CLIError(f"Key has no TTL or is missing: {_key_name(key)}")
    _save(cm, args.path)

    if args.json:
        print(_json_text({"key": key, "persisted": True, "path": str(args.path)}))
    else:
        print(f"Removed TTL from {_key_name(key)}")
    return 0


def _cmd_prune(args: argparse.Namespace) -> int:
    if args.keep_last is None and args.older_than is None:
        raise CLIError("Provide --keep-last, --older-than, or both")
    older_than = parse_timestamp(args.older_than) if args.older_than is not None else None
    cm = _load(args.path)

    if args.key is not None:
        key = _parse_key(args.key, args.literal_key)
        removed = cm.prune_history(key, keep_last=args.keep_last, older_than=older_than)
    else:
        removed = cm.prune_all_history(keep_last=args.keep_last, older_than=older_than)

    if not args.dry_run:
        _save(cm, args.path)

    if args.json:
        print(
            _json_text(
                {
                    "removed": removed,
                    "dry_run": args.dry_run,
                    "path": str(args.path),
                }
            )
        )
    else:
        suffix = " (dry run; file not written)" if args.dry_run else ""
        print(f"Removed {removed} version(s){suffix}")
    return 0


def _cmd_diff(args: argparse.Namespace) -> int:
    left = _load(args.left)
    right = _load(args.right)
    if args.detailed:
        changes = left.diff_detailed(right)
        if args.json:
            print(
                _json_text(
                    {
                        "count": len(changes),
                        "changes": [{"key": key, "other": old, "self": new} for key, old, new in changes],
                    }
                )
            )
        else:
            print(f"Changed keys: {len(changes)}")
            for key, old, new in changes:
                print(f"{_key_name(key)}: {format_value(old)} -> {format_value(new)}")
    else:
        changed = left.diff(right)
        if args.json:
            print(
                _json_text(
                    {
                        "count": len(changed),
                        "changes": [{"key": key} for key in sorted(changed, key=str)],
                    }
                )
            )
        else:
            print(f"Changed keys: {len(changed)}")
            for key in sorted(changed, key=str):
                print(_key_name(key))
    return 0


def _cmd_merge(args: argparse.Namespace) -> int:
    left = _load(args.left)
    right = _load(args.right)
    left.merge(right, strategy=args.strategy)
    output = args.output if args.output is not None else args.left
    _save(left, output)

    if args.json:
        print(
            _json_text(
                {
                    "strategy": args.strategy,
                    "output": str(output),
                    "keys": len(left),
                }
            )
        )
    else:
        print(f"Merged {args.right} into {output} ({args.strategy})")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chronomap",
        description="Inspect and edit ChronoMap save files from the command line.",
    )
    parser.add_argument(
        "--no-color",
        action="store_false",
        dest="color",
        help="disable ANSI colors",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command")

    show_parser = subparsers.add_parser(
        "show", help="load a .json/.pkl file and print its current key-value state"
    )
    show_parser.add_argument("path", help="path to a file written by save_json()/save_pickle()")
    show_parser.add_argument("--json", action="store_true", help="emit JSON")
    show_parser.set_defaults(func=_cmd_show)

    get_parser = subparsers.add_parser("get", help="read one key from a save file")
    get_parser.add_argument("path")
    get_parser.add_argument("key")
    get_parser.add_argument("--timestamp", help="Unix seconds or ISO-8601 timestamp")
    get_parser.add_argument("--default", help="value to print instead of failing")
    get_parser.add_argument("--literal-key", action="store_true", help="do not JSON-parse the key")
    get_parser.add_argument("--json", action="store_true", help="emit JSON")
    get_parser.set_defaults(func=_cmd_get)

    history_parser = subparsers.add_parser("history", help="print every version of a key")
    history_parser.add_argument("path")
    history_parser.add_argument("key")
    history_parser.add_argument("--limit", type=int, help="show at most N newest versions")
    history_parser.add_argument("--reverse", action="store_true", help="oldest last")
    history_parser.add_argument("--literal-key", action="store_true", help="do not JSON-parse the key")
    history_parser.add_argument("--json", action="store_true", help="emit JSON")
    history_parser.set_defaults(func=_cmd_history)

    put_parser = subparsers.add_parser("put", help="write a value and save the file")
    put_parser.add_argument("path")
    put_parser.add_argument("key")
    put_parser.add_argument("value")
    put_parser.add_argument("--timestamp", help="Unix seconds or ISO-8601 timestamp")
    put_parser.add_argument("--ttl", type=float, help="expire after N seconds")
    put_parser.add_argument("--literal-key", action="store_true", help="do not JSON-parse the key")
    put_parser.add_argument("--json", action="store_true", help="emit JSON")
    put_parser.set_defaults(func=_cmd_put)

    delete_parser = subparsers.add_parser("delete", help="remove a key and save the file")
    delete_parser.add_argument("path")
    delete_parser.add_argument("key")
    delete_parser.add_argument("--literal-key", action="store_true", help="do not JSON-parse the key")
    delete_parser.add_argument("--json", action="store_true", help="emit JSON")
    delete_parser.set_defaults(func=_cmd_delete)

    keys_parser = subparsers.add_parser("keys", help="list live keys")
    keys_parser.add_argument("path")
    keys_parser.add_argument("--json", action="store_true", help="emit JSON")
    keys_parser.set_defaults(func=_cmd_keys)

    stats_parser = subparsers.add_parser("stats", help="print operation and cache counters")
    stats_parser.add_argument("path")
    stats_parser.add_argument("--json", action="store_true", help="emit JSON")
    stats_parser.set_defaults(func=_cmd_stats)

    ttl_parser = subparsers.add_parser("ttl", help="show remaining TTLs from a save file")
    ttl_parser.add_argument("path")
    ttl_parser.add_argument("keys", nargs="*", help="keys to inspect; defaults to every key with a TTL")
    ttl_parser.add_argument("--literal-key", action="store_true", help="do not JSON-parse keys")
    ttl_parser.add_argument("--json", action="store_true", help="emit JSON")
    ttl_parser.set_defaults(func=_cmd_ttl)

    expire_parser = subparsers.add_parser("expire", help="set a key's TTL and save the file")
    expire_parser.add_argument("path")
    expire_parser.add_argument("key")
    expire_parser.add_argument("ttl", type=float, help="seconds until expiry")
    expire_parser.add_argument("--literal-key", action="store_true", help="do not JSON-parse the key")
    expire_parser.add_argument("--json", action="store_true", help="emit JSON")
    expire_parser.set_defaults(func=_cmd_expire)

    persist_parser = subparsers.add_parser("persist", help="remove a key's TTL and save the file")
    persist_parser.add_argument("path")
    persist_parser.add_argument("key")
    persist_parser.add_argument("--literal-key", action="store_true", help="do not JSON-parse the key")
    persist_parser.add_argument("--json", action="store_true", help="emit JSON")
    persist_parser.set_defaults(func=_cmd_persist)

    prune_parser = subparsers.add_parser("prune", help="drop old versions and save the file")
    prune_parser.add_argument("path")
    prune_parser.add_argument("--key", help="prune one key instead of every key")
    prune_parser.add_argument("--keep-last", type=int, help="keep at most N newest versions")
    prune_parser.add_argument("--older-than", help="drop versions older than this timestamp")
    prune_parser.add_argument("--dry-run", action="store_true", help="report without writing")
    prune_parser.add_argument("--literal-key", action="store_true", help="do not JSON-parse --key")
    prune_parser.add_argument("--json", action="store_true", help="emit JSON")
    prune_parser.set_defaults(func=_cmd_prune)

    diff_parser = subparsers.add_parser("diff", help="compare two save files")
    diff_parser.add_argument("left")
    diff_parser.add_argument("right")
    diff_parser.add_argument("--detailed", action="store_true", help="show both values")
    diff_parser.add_argument("--json", action="store_true", help="emit JSON")
    diff_parser.set_defaults(func=_cmd_diff)

    merge_parser = subparsers.add_parser("merge", help="merge two save files")
    merge_parser.add_argument("left")
    merge_parser.add_argument("right")
    merge_parser.add_argument("--output", help="write here instead of replacing left")
    merge_parser.add_argument(
        "--strategy",
        choices=("timestamp", "overwrite"),
        default="timestamp",
        help="timestamp merge or overwrite keys from the right side",
    )
    merge_parser.add_argument("--json", action="store_true", help="emit JSON")
    merge_parser.set_defaults(func=_cmd_merge)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 1

    try:
        return args.func(args)
    except CLIError as exc:
        return _fail(str(exc), args)
    except ChronoMapError as exc:
        return _fail(str(exc), args)
    except OSError as exc:
        return _fail(str(exc), args)


if __name__ == "__main__":  # pragma: no cover - covered through main() tests
    sys.exit(main())
