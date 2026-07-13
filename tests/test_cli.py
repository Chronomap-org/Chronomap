# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""
Unit tests for ChronoMap CLI helper functions and commands.
"""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime
from chronomap.cli import (
    parse_value,
    format_timestamp,
    colorize,
    load_and_display,
    Colors,
)
from chronomap import ChronoMap


@pytest.mark.cli
class TestCLIHelpers:
    """Test CLI helper functions parse_value, format_timestamp, and colorize."""

    def test_parse_value_int(self):
        """Test parse_value with integer strings."""
        assert parse_value("42") == 42
        assert isinstance(parse_value("42"), int)

    def test_parse_value_float(self):
        """Test parse_value with float strings."""
        assert parse_value("3.14") == pytest.approx(3.14)
        assert isinstance(parse_value("3.14"), float)

    def test_parse_value_string(self):
        """Test parse_value with generic strings."""
        assert parse_value("hello") == "hello"
        assert isinstance(parse_value("hello"), str)

    def test_parse_value_json_dict(self):
        """Test parse_value with valid JSON dictionary strings."""
        parsed = parse_value('{"a": 1}')
        assert parsed == {"a": 1}
        assert isinstance(parsed, dict)

    def test_parse_value_json_list(self):
        """Test parse_value with valid JSON list strings."""
        parsed = parse_value("[1, 2, 3]")
        assert parsed == [1, 2, 3]
        assert isinstance(parsed, list)

    def test_parse_value_boolean(self):
        """Test parse_value with boolean strings."""
        assert parse_value("true") is True
        assert parse_value("false") is False

    def test_parse_value_null(self):
        """Test parse_value with null/None strings."""
        assert parse_value("null") is None

    def test_format_timestamp(self):
        """Test format_timestamp formats Unix epoch to localized date string correctly."""
        expected = datetime.fromtimestamp(0).strftime("%Y-%m-%d %H:%M:%S")
        assert format_timestamp(0) == expected

    def test_colorize(self):
        """Test colorize successfully wraps text with terminal colors."""
        text = "hello"
        color = Colors.RED
        assert colorize(text, color) == f"{color}{text}{Colors.END}"


@pytest.mark.cli
class TestCLILoadDisplay:
    """Test loading and displaying files via CLI load_and_display function."""

    def test_load_and_display_nonexistent(self, capsys):
        """Test load_and_display behavior on a nonexistent file."""
        load_and_display("nonexistent_file.json")
        captured = capsys.readouterr()
        assert "not found" in (captured.out + captured.err)

    def test_load_and_display_unsupported(self, capsys):
        """Test load_and_display on an unsupported file format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "test.txt"
            temp_file.touch()
            load_and_display(str(temp_file))
            captured = capsys.readouterr()
            assert "Unsupported file type" in (captured.out + captured.err)

    def test_load_and_display_json(self, capsys):
        """Test load_and_display successfully loads and prints ChronoMap state from JSON."""
        cm = ChronoMap()
        cm.put("a", 1)
        cm.put("b", 2)

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "test.json"
            cm.save_json(str(temp_file))

            load_and_display(str(temp_file))
            captured = capsys.readouterr()
            output = captured.out + captured.err
            assert "Loaded ChronoMap from" in output
            assert "Keys: 2" in output
            assert "a: 1" in output
            assert "b: 2" in output

    def test_load_and_display_pickle(self, capsys):
        """Test load_and_display successfully loads and prints ChronoMap state from pickle."""
        cm = ChronoMap()
        cm.put("x", "hello")
        cm.put("y", "world")

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "test.pkl"
            cm.save_pickle(str(temp_file))

            load_and_display(str(temp_file))
            captured = capsys.readouterr()
            output = captured.out + captured.err
            assert "Loaded ChronoMap from" in output
            assert "Keys: 2" in output
            assert "x: hello" in output
            assert "y: world" in output

    def test_load_and_display_invalid_format(self, capsys):
        """Test load_and_display error handling when loading corrupt data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_file = Path(tmpdir) / "corrupt.json"
            temp_file.write_text("invalid json content")

            load_and_display(str(temp_file))
            captured = capsys.readouterr()
            assert "Error loading file:" in (captured.out + captured.err)
