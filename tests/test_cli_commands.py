# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""End-to-end tests for the expanded ChronoMap command-line interface."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from chronomap import ChronoMap, __version__
from chronomap.cli import (
    CLIError,
    _load,
    _save,
    build_parser,
    format_value,
    load_and_display,
    main,
    parse_timestamp,
)


def _write_state(tmp_path, name="state.json", entries=None):
    entries = entries or [
        ("alpha", 1, 100),
        ("alpha", 2, 200),
        ("beta", "hello", 300),
    ]
    cm = ChronoMap()
    for key, value, timestamp in entries:
        cm.put(key, value, timestamp=timestamp)
    path = tmp_path / name
    if path.suffix == ".json":
        cm.save_json(path)
    else:
        cm.save_pickle(path)
    return path


def _json_output(capsys, argv):
    code = main(argv)
    captured = capsys.readouterr()
    assert code == 0, captured.err
    return json.loads(captured.out)


@pytest.mark.cli
class TestCLIParsing:
    def test_timestamp_accepts_numbers_and_iso_strings(self):
        assert parse_timestamp("100") == 100.0
        assert parse_timestamp("-2.5") == -2.5
        assert parse_timestamp("2026-01-02T03:04:05") == "2026-01-02T03:04:05"

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("hello", "hello"),
            (None, "None"),
            (42, "42"),
            ({"a": 1}, '{"a": 1}'),
            ([1, 2], "[1, 2]"),
            (b"raw", "b'raw'"),
            ({(1, 2): 3}, "{(1, 2): 3}"),
        ],
    )
    def test_format_value(self, value, expected):
        assert format_value(value) == expected

    def test_parser_exposes_every_command(self):
        parser = build_parser()
        actions = [action for action in parser._actions if action.dest == "command"]
        choices = set(actions[0].choices)
        assert choices == {
            "show",
            "get",
            "history",
            "put",
            "delete",
            "keys",
            "stats",
            "ttl",
            "expire",
            "persist",
            "prune",
            "diff",
            "merge",
        }


@pytest.mark.cli
class TestCLILoading:
    def test_load_json_and_pickle(self, tmp_path):
        json_path = _write_state(tmp_path)
        pickle_path = _write_state(tmp_path, "state.pkl")

        assert _load(json_path).get("alpha") == 2
        assert _load(pickle_path).get("beta") == "hello"

    def test_load_rejects_missing_directory_and_extension(self, tmp_path):
        with pytest.raises(CLIError, match="File not found"):
            _load(tmp_path / "missing.json")
        with pytest.raises(CLIError, match="Not a file"):
            _load(tmp_path)
        text = tmp_path / "state.txt"
        text.write_text("x")
        with pytest.raises(CLIError, match="Unsupported file type"):
            _load(text)

    def test_load_wraps_corrupt_files(self, tmp_path):
        corrupt = tmp_path / "state.json"
        corrupt.write_text("{not json")
        with pytest.raises(CLIError, match="Error loading file"):
            _load(corrupt)

        corrupt_pickle = tmp_path / "state.pkl"
        corrupt_pickle.write_bytes(b"not a pickle")
        with pytest.raises(CLIError, match="Error loading file"):
            _load(corrupt_pickle)

    def test_load_and_display_keeps_the_old_helper_contract(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        load_and_display(str(path))
        output = capsys.readouterr().out
        assert "Loaded ChronoMap from" in output
        assert "Keys: 2" in output
        assert "alpha: 2" in output

    def test_load_and_display_reports_errors_without_raising(self, tmp_path, capsys):
        load_and_display(str(tmp_path / "missing.json"))
        assert "File not found" in capsys.readouterr().out

        text = tmp_path / "state.txt"
        text.write_text("x")
        load_and_display(str(text))
        assert "Unsupported file type" in capsys.readouterr().out


@pytest.mark.cli
class TestCLISaving:
    def test_save_replaces_json_and_pickle(self, tmp_path):
        json_path = _write_state(tmp_path)
        pickle_path = _write_state(tmp_path, "state.pkl")

        cm = _load(json_path)
        cm.put("gamma", 3)
        _save(cm, json_path)
        assert _load(json_path).get("gamma") == 3

        cm.put("delta", 4)
        _save(cm, pickle_path)
        assert _load(pickle_path).get("delta") == 4

    def test_save_rejects_bad_extension_and_missing_parent(self, tmp_path):
        cm = ChronoMap()
        with pytest.raises(CLIError, match="Unsupported file type"):
            _save(cm, tmp_path / "state.txt")
        with pytest.raises(CLIError, match="Output directory does not exist"):
            _save(cm, tmp_path / "missing" / "state.json")

    def test_failed_save_leaves_the_original_file_and_no_temp(self, tmp_path, monkeypatch):
        path = _write_state(tmp_path)
        original = path.read_text()

        def boom(self, file_path):
            raise RuntimeError("disk full")

        monkeypatch.setattr(ChronoMap, "save_json", boom)
        with pytest.raises(CLIError, match="Error saving file: disk full"):
            _save(_load(path), path)

        assert path.read_text() == original
        assert list(tmp_path.glob(".state.json.*")) == []

    def test_temp_cleanup_failure_is_ignored(self, tmp_path, monkeypatch):
        path = _write_state(tmp_path)

        def save_boom(self, file_path):
            raise RuntimeError("disk full")

        def unlink_boom(self, missing_ok=False):
            raise OSError("locked")

        monkeypatch.setattr(ChronoMap, "save_json", save_boom)
        monkeypatch.setattr(Path, "unlink", unlink_boom)
        with pytest.raises(CLIError, match="disk full"):
            _save(_load(path), path)


@pytest.mark.cli
class TestCLIShowAndVersion:
    def test_show_human_and_json(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["show", str(path)]) == 0
        out = capsys.readouterr().out
        assert "Keys: 2" in out
        assert "alpha: 2" in out
        assert "beta: hello" in out

        payload = _json_output(capsys, ["show", str(path), "--json"])
        assert payload["count"] == 2
        assert payload["values"] == {"alpha": 2, "beta": "hello"}

    def test_show_errors_return_one(self, tmp_path, capsys):
        missing = tmp_path / "missing.json"
        assert main(["show", str(missing)]) == 1
        assert "File not found" in capsys.readouterr().err

        text = tmp_path / "state.txt"
        text.write_text("x")
        assert main(["show", str(text)]) == 1
        assert "Unsupported file type" in capsys.readouterr().err

    def test_no_subcommand_prints_help(self, capsys):
        assert main([]) == 1
        assert "usage" in capsys.readouterr().out.lower()

    def test_version_flag(self, capsys):
        with pytest.raises(SystemExit) as excinfo:
            main(["--version"])
        assert excinfo.value.code == 0
        assert __version__ in capsys.readouterr().out

    def test_module_entrypoint_reports_the_version(self):
        result = subprocess.run(
            [sys.executable, "-m", "chronomap.cli", "--version"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert __version__ in result.stdout

    def test_no_color_suppresses_ansi(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["--no-color", "show", str(path)]) == 0
        assert "\033[" not in capsys.readouterr().out

    def test_no_color_environment_variable(self, tmp_path, capsys, monkeypatch):
        path = _write_state(tmp_path)
        monkeypatch.setenv("NO_COLOR", "1")
        monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
        assert main(["show", str(path)]) == 0
        assert "\033[" not in capsys.readouterr().out


@pytest.mark.cli
class TestCLIGet:
    def test_get_latest_at_timestamp_and_default(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["get", str(path), "alpha"]) == 0
        assert capsys.readouterr().out.strip() == "2"

        assert main(["get", str(path), "alpha", "--timestamp", "100"]) == 0
        assert capsys.readouterr().out.strip() == "1"

        assert main(["get", str(path), "missing", "--default", "fallback"]) == 0
        assert capsys.readouterr().out.strip() == "fallback"

    def test_get_missing_and_invalid_timestamp_return_one(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["get", str(path), "missing"]) == 1
        assert "Key not found" in capsys.readouterr().err

        assert main(["get", str(path), "alpha", "--timestamp", "not-a-date"]) == 1
        assert "Error:" in capsys.readouterr().err

    def test_get_stored_none_and_json(self, tmp_path, capsys):
        path = _write_state(tmp_path, entries=[("none", None, 100)])
        assert main(["get", str(path), "none"]) == 0
        assert capsys.readouterr().out.strip() == "None"

        payload = _json_output(capsys, ["get", str(path), "none", "--json"])
        assert payload == {"key": "none", "found": True, "timestamp": None, "value": None}

    def test_get_parses_keys_unless_literal(self, tmp_path, capsys):
        path = _write_state(tmp_path, "state.pkl", entries=[(1, "number", 100), ("1", "string", 100)])
        assert main(["get", str(path), "1"]) == 0
        assert capsys.readouterr().out.strip() == "number"
        assert main(["get", str(path), "1", "--literal-key"]) == 0
        assert capsys.readouterr().out.strip() == "string"


@pytest.mark.cli
class TestCLIHistory:
    def test_history_human_limit_and_reverse(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["history", str(path), "alpha"]) == 0
        lines = capsys.readouterr().out.strip().splitlines()
        assert len(lines) == 2
        assert lines[0].endswith("  1")
        assert lines[1].endswith("  2")

        assert main(["history", str(path), "alpha", "--limit", "1"]) == 0
        assert capsys.readouterr().out.strip().endswith("  2")

        assert main(["history", str(path), "alpha", "--reverse"]) == 0
        lines = capsys.readouterr().out.strip().splitlines()
        assert lines[0].endswith("  2")

    def test_history_json_and_missing_key(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        payload = _json_output(capsys, ["history", str(path), "alpha", "--json"])
        assert [item["value"] for item in payload] == [1, 2]
        assert all("datetime" in item for item in payload)

        assert main(["history", str(path), "missing"]) == 1
        assert "No history" in capsys.readouterr().err

    def test_history_rejects_non_positive_limit(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["history", str(path), "alpha", "--limit", "0"]) == 1
        assert "--limit" in capsys.readouterr().err


@pytest.mark.cli
class TestCLIPutAndDelete:
    def test_put_parses_values_and_saves(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["put", str(path), "payload", '{"a": 1}', "--timestamp", "400"]) == 0
        assert "Wrote payload" in capsys.readouterr().out
        loaded = _load(path)
        assert loaded.get("payload", timestamp=400) == {"a": 1}

    def test_put_ttl_and_json_output(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        payload = _json_output(
            capsys,
            ["put", str(path), "temp", "value", "--ttl", "60", "--json"],
        )
        assert payload["key"] == "temp"
        assert payload["ttl"] == 60.0
        assert _load(path).get("temp") == "value"

    def test_put_pickle_file(self, tmp_path, capsys):
        path = _write_state(tmp_path, "state.pkl")
        assert main(["put", str(path), "new", "yes"]) == 0
        assert "Wrote new" in capsys.readouterr().out
        assert _load(path).get("new") == "yes"

    def test_put_invalid_ttl_does_not_touch_the_file(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        original = path.read_text()
        assert main(["put", str(path), "k", "v", "--ttl", "0"]) == 1
        assert "TTL" in capsys.readouterr().err
        assert path.read_text() == original

    def test_delete_removes_and_reports(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["delete", str(path), "alpha"]) == 0
        assert "Deleted alpha" in capsys.readouterr().out
        assert "alpha" not in _load(path)

    def test_delete_missing_returns_one(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["delete", str(path), "missing"]) == 1
        assert "Key not found" in capsys.readouterr().err

    def test_delete_json(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        payload = _json_output(capsys, ["delete", str(path), "alpha", "--json"])
        assert payload["deleted"] is True
        assert payload["key"] == "alpha"


@pytest.mark.cli
class TestCLIKeysAndStats:
    def test_keys_human_and_json(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["keys", str(path)]) == 0
        assert capsys.readouterr().out.splitlines() == ["alpha", "beta"]

        payload = _json_output(capsys, ["keys", str(path), "--json"])
        assert payload == {"count": 2, "keys": ["alpha", "beta"]}

    def test_stats_human_and_json(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["stats", str(path)]) == 0
        out = capsys.readouterr().out
        assert "total_keys: 2" in out

        payload = _json_output(capsys, ["stats", str(path), "--json"])
        assert payload["stats"]["total_keys"] == 2
        assert payload["stats"]["reads"] == 0


@pytest.mark.cli
class TestCLIPrune:
    def test_prune_all_keep_last(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["prune", str(path), "--keep-last", "1"]) == 0
        assert "Removed 1 version" in capsys.readouterr().out
        assert len(_load(path).history("alpha")) == 1

    def test_prune_one_key_older_than(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["prune", str(path), "--key", "alpha", "--older-than", "150"]) == 0
        assert "Removed 1 version" in capsys.readouterr().out
        assert [value for _, value in _load(path).history("alpha")] == [2]
        assert len(_load(path).history("beta")) == 1

    def test_prune_dry_run_does_not_write(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        original = path.read_text()
        assert main(["prune", str(path), "--keep-last", "1", "--dry-run"]) == 0
        assert "dry run" in capsys.readouterr().out
        assert path.read_text() == original

    def test_prune_requires_a_criterion(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["prune", str(path)]) == 1
        assert "Provide" in capsys.readouterr().err

    def test_prune_numeric_key_and_json(self, tmp_path, capsys):
        path = _write_state(tmp_path, "state.pkl", entries=[(1, "a", 100), (1, "b", 200)])
        payload = _json_output(
            capsys,
            ["prune", str(path), "--key", "1", "--keep-last", "1", "--json"],
        )
        assert payload["removed"] == 1
        assert payload["dry_run"] is False


@pytest.mark.cli
class TestCLIDiffAndMerge:
    def test_diff_summary_and_detailed(self, tmp_path, capsys):
        left = _write_state(tmp_path, "left.json")
        right = _write_state(
            tmp_path,
            "right.json",
            entries=[("alpha", 99, 200)],
        )

        assert main(["diff", str(left), str(right)]) == 0
        out = capsys.readouterr().out
        assert "Changed keys: 2" in out
        assert "alpha" in out
        assert "beta" in out

        assert main(["diff", str(left), str(right), "--detailed"]) == 0
        out = capsys.readouterr().out
        assert "alpha: 99 -> 2" in out
        assert "beta: None -> hello" in out

    def test_diff_json(self, tmp_path, capsys):
        left = _write_state(tmp_path, "left.json")
        right = _write_state(tmp_path, "right.json", entries=[("alpha", 2, 200)])
        payload = _json_output(capsys, ["diff", str(left), str(right), "--json"])
        assert payload["count"] == 1
        assert payload["changes"] == [{"key": "beta"}]

    def test_diff_detailed_json(self, tmp_path, capsys):
        left = _write_state(tmp_path, "left.json")
        right = _write_state(tmp_path, "right.json", entries=[("alpha", 99, 200)])
        payload = _json_output(capsys, ["diff", str(left), str(right), "--detailed", "--json"])
        assert payload["count"] == 2
        assert {"key": "alpha", "other": 99, "self": 2} in payload["changes"]

    def test_diff_same_file_is_empty(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["diff", str(path), str(path)]) == 0
        assert "Changed keys: 0" in capsys.readouterr().out

    def test_merge_timestamp_to_new_output(self, tmp_path, capsys):
        left = _write_state(tmp_path, "left.json", entries=[("k", "left", 1)])
        right = _write_state(tmp_path, "right.json", entries=[("k", "right", 2)])
        output = tmp_path / "merged.json"

        assert main(["merge", str(left), str(right), "--output", str(output)]) == 0
        assert "Merged" in capsys.readouterr().out
        assert [value for _, value in _load(output).history("k")] == ["left", "right"]

    def test_merge_overwrite_and_json(self, tmp_path, capsys):
        left = _write_state(tmp_path, "left.json", entries=[("k", "left", 1)])
        right = _write_state(tmp_path, "right.json", entries=[("k", "right", 2)])
        payload = _json_output(
            capsys,
            [
                "merge",
                str(left),
                str(right),
                "--strategy",
                "overwrite",
                "--json",
            ],
        )
        assert payload["strategy"] == "overwrite"
        assert payload["keys"] == 1
        assert [value for _, value in _load(left).history("k")] == ["right"]

    def test_merge_bad_strategy_is_an_argparse_error(self, tmp_path):
        left = _write_state(tmp_path, "left.json")
        with pytest.raises(SystemExit) as excinfo:
            main(["merge", str(left), str(left), "--strategy", "nope"])
        assert excinfo.value.code == 2

    def test_merge_pickle_output(self, tmp_path, capsys):
        left = _write_state(tmp_path, "left.json")
        right = _write_state(tmp_path, "right.json")
        output = tmp_path / "merged.pkl"
        assert main(["merge", str(left), str(right), "--output", str(output)]) == 0
        assert capsys.readouterr().out
        assert _load(output).get("alpha") == 2


@pytest.mark.cli
class TestCLIErrorBoundary:
    def test_corrupt_file_in_command_returns_one(self, tmp_path, capsys):
        path = tmp_path / "state.json"
        path.write_text("{broken")
        assert main(["keys", str(path)]) == 1
        assert "Error loading file" in capsys.readouterr().err

    def test_unsupported_output_returns_one(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["merge", str(path), str(path), "--output", str(tmp_path / "x.txt")]) == 1
        assert "Unsupported file type" in capsys.readouterr().err

    def test_invalid_prune_timestamp_returns_one(self, tmp_path, capsys):
        path = _write_state(tmp_path)
        assert main(["prune", str(path), "--older-than", "not-a-date"]) == 1
        assert "Error:" in capsys.readouterr().err

    def test_save_error_is_reported(self, tmp_path, capsys, monkeypatch):
        path = _write_state(tmp_path)

        def boom(self, file_path):
            raise RuntimeError("disk full")

        monkeypatch.setattr(ChronoMap, "save_json", boom)
        assert main(["delete", str(path), "alpha"]) == 1
        assert "disk full" in capsys.readouterr().err

    def test_uncaught_os_error_is_reported(self, monkeypatch, capsys):
        def boom(args):
            raise OSError("permission denied")

        monkeypatch.setattr("chronomap.cli._cmd_keys", boom)
        assert main(["keys", "anything.json"]) == 1
        assert "permission denied" in capsys.readouterr().err


@pytest.mark.cli
class TestCLIHelpersStillWork:
    def test_parse_value_and_colorize(self, capsys):
        from chronomap.cli import Colors, colorize, parse_value

        assert parse_value("42") == 42
        assert parse_value("true") is True
        assert colorize("x", Colors.RED) == f"{Colors.RED}x{Colors.END}"

    def test_load_and_display_pickle(self, tmp_path, capsys):
        path = _write_state(tmp_path, "state.pkl")
        load_and_display(str(path))
        assert "Loaded ChronoMap from" in capsys.readouterr().out
