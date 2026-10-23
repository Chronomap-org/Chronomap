import importlib.util
import json
import urllib.error
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_script():
    path = ROOT / "scripts" / "build_changelog_entry.py"
    spec = importlib.util.spec_from_file_location("build_changelog_entry", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    def __init__(self, payload, headers):
        self._payload = json.dumps(payload).encode("utf-8")
        self._headers = headers
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._closed = True
        return False

    def read(self):
        return self._payload

    @property
    def headers(self):
        if self._closed:
            raise urllib.error.URLError("response is closed")
        return self._headers


def test_fetch_merged_prs_reads_link_header_before_closing(monkeypatch):
    module = _load_script()
    responses = iter(
        [
            FakeResponse(
                {"items": [{"title": "one"}]},
                {"Link": '<https://api.github.com/search/issues?page=2>; rel="next"'},
            ),
            FakeResponse({"items": [{"title": "two"}]}, {}),
        ]
    )
    requested = []

    def fake_urlopen(request):
        requested.append(request.full_url)
        return next(responses)

    monkeypatch.setattr(module.urllib.request, "urlopen", fake_urlopen)

    prs = module.fetch_merged_prs("owner/repo", "token", "2026-01-01", "2026-02-01")

    assert [pr["title"] for pr in prs] == ["one", "two"]
    assert len(requested) == 2
    assert "page=2" in requested[1]


def test_categorize_uses_deterministic_label_precedence():
    module = _load_script()

    assert module.categorize({"labels": [{"name": "bug"}]}) == "Fixed"
    assert module.categorize({"labels": [{"name": "documentation"}]}) == "Documentation"
    assert module.categorize({"labels": [{"name": "bug"}, {"name": "feature"}]}) == "Added"
    assert module.categorize({"labels": []}) == "Changed"


def test_sanitize_title_removes_mentions():
    module = _load_script()

    assert module.sanitize_title("Fix cache @alice and @bob-dev") == "Fix cache  and"


def test_build_entry_groups_and_orders_sections():
    module = _load_script()

    entry = module.build_entry(
        "3.2.0",
        [
            {"title": "Fix crash", "labels": [{"name": "bug"}]},
            {"title": "Add expire", "labels": [{"name": "feature"}]},
            {"title": "Bump deps", "labels": [{"name": "dependencies"}]},
            {"title": "Unlabeled", "labels": []},
        ],
        "2026-02-01",
    )

    assert entry.startswith("## [3.2.0] - 2026-02-01")
    assert entry.index("### Added") < entry.index("### Changed")
    assert entry.index("### Changed") < entry.index("### Fixed")
    assert entry.index("### Fixed") < entry.index("### Maintenance")
    assert "- Add expire" in entry
    assert "- Unlabeled" in entry


def test_build_entry_handles_empty_input():
    module = _load_script()

    entry = module.build_entry("3.2.0", [], "2026-02-01")

    assert "_No user-facing changes this cycle._" in entry
