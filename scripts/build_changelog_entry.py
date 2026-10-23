# Copyright (c) 2026 Devansh Singh, ChronoMap contributors
# SPDX-License-Identifier: MIT


"""Build a name-free CHANGELOG.md section from PRs merged in a date window.

Used by .github/workflows/monthly-release.yml. Deliberately excludes PR
authors and any @mentions — CHANGELOG.md describes *what* changed
(feature/fix/docs/chore), not *who* changed it. The "who did what" side
lives in the GitHub Release notes instead (see that workflow), which
uses GitHub's native contributor/author info.

Usage:
    python scripts/build_changelog_entry.py \
        --repo owner/name --token $GITHUB_TOKEN \
        --since 2026-06-01 --until 2026-07-01 --version 3.1.0
"""

from __future__ import annotations

import argparse
import re
import sys
import urllib.request
import urllib.parse
import json
from datetime import datetime, timezone

CATEGORY_LABELS = {
    "feature": "Added",
    "enhancement": "Added",
    "bug": "Fixed",
    "fix": "Fixed",
    "changed": "Changed",
    "refactor": "Changed",
    "documentation": "Documentation",
    "chore": "Maintenance",
    "ci": "Maintenance",
    "dependencies": "Maintenance",
}
CATEGORY_ORDER = ["Added", "Changed", "Fixed", "Documentation", "Maintenance"]
CATEGORY_PRECEDENCE = [
    "feature",
    "enhancement",
    "bug",
    "fix",
    "changed",
    "refactor",
    "documentation",
    "chore",
    "ci",
    "dependencies",
]

MENTION_RE = re.compile(r"@[\w-]+")


def fetch_merged_prs(repo: str, token: str, since: str, until: str) -> list[dict]:
    """Pull merged PRs against main, merged within [since, until)."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    query = f"repo:{repo} is:pr is:merged base:main merged:{since}..{until}"
    url = "https://api.github.com/search/issues?q=" + urllib.parse.quote(query)
    prs: list[dict] = []
    while url:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as resp:
            data = json.load(resp)
            link = resp.headers.get("Link", "")
        prs.extend(data.get("items", []))
        next_url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip().strip("<>")
        url = next_url
    return prs


def categorize(pr: dict) -> str:
    labels = {lbl["name"] for lbl in pr.get("labels", [])}
    for label in CATEGORY_PRECEDENCE:
        if label in labels:
            return CATEGORY_LABELS[label]
    return "Changed"


def sanitize_title(title: str) -> str:
    """Strip @mentions from a PR title so no names leak into CHANGELOG.md."""
    return MENTION_RE.sub("", title).strip()


def build_entry(version: str, prs: list[dict], release_date: str) -> str:
    buckets: dict[str, list[str]] = {c: [] for c in CATEGORY_ORDER}
    for pr in prs:
        buckets[categorize(pr)].append(sanitize_title(pr["title"]))

    lines = [f"## [{version}] - {release_date}"]
    any_content = False
    for category in CATEGORY_ORDER:
        items = buckets[category]
        if not items:
            continue
        any_content = True
        lines.append(f"\n### {category}")
        for item in items:
            lines.append(f"- {item}")
    if not any_content:
        lines.append("\n_No user-facing changes this cycle._")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, help="owner/name")
    parser.add_argument("--token", required=True)
    parser.add_argument("--since", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--until", required=True, help="YYYY-MM-DD, exclusive")
    parser.add_argument("--version", required=True)
    args = parser.parse_args()

    prs = fetch_merged_prs(args.repo, args.token, args.since, args.until)
    release_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    entry = build_entry(args.version, prs, release_date)

    changelog_path = "CHANGELOG.md"
    with open(changelog_path, "r", encoding="utf-8") as f:
        content = f.read()

    marker = "## [Unreleased]"
    if marker in content:
        head, _, rest = content.partition(marker)
        new_content = f"{head}{marker}\n\n{entry}\n{rest.lstrip(chr(10))}"
    else:
        new_content = f"{content.rstrip()}\n\n{entry}\n"

    with open(changelog_path, "w", encoding="utf-8") as f:
        f.write(new_content)

    print(entry)
    return 0


if __name__ == "__main__":
    sys.exit(main())
