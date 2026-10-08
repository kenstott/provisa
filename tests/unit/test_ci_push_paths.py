# Copyright (c) 2026 Kenneth Stott
# Canary: c5ba82e7-9684-430d-83b7-c8666ce3f628
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A push that changes only the website or the documentation does not start the container suite.

Every push to a branch starts a suite run that replaces the branch's run in flight. On a day of
site and docs pushes no run on main reached the end of its lanes, so nothing merged that day was
proven by the suite. Such a push now starts no run, and so cancels none."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
_SUITE = REPO / ".github" / "workflows" / "integration-suite.yml"


def _triggers() -> dict:
    return yaml.safe_load(_SUITE.read_text())[True]  # PyYAML reads the key `on` as True


def _matches(pattern: str, path: str) -> bool:
    """GitHub's path filter for the patterns this workflow uses: `**` crosses directories,
    `*` does not."""
    regex = re.escape(pattern).replace(r"\*\*/", "(?:.*/)?").replace(r"\*\*", ".*")
    regex = regex.replace(r"\*", "[^/]*")
    return re.fullmatch(regex, path) is not None


def _starts_a_run(changed: list[str]) -> bool:
    """Whether a push changing ``changed`` starts the workflow: it does when at least one file
    is left in after every pattern has been applied in order (a later pattern wins)."""
    patterns = _triggers()["push"]["paths"]

    def _kept(path: str) -> bool:
        kept = False
        for pattern in patterns:
            if pattern.startswith("!"):
                if _matches(pattern[1:], path):
                    kept = False
            elif _matches(pattern, path):
                kept = True
        return kept

    return any(_kept(path) for path in changed)


def test_a_site_or_docs_only_push_starts_no_run():
    assert not _starts_a_run(["site/index.html", "site/assets/app.js"])
    assert not _starts_a_run(["docs/sources.md", "docs/arch/requirements.md"])
    assert not _starts_a_run(["docs/arch/requirements.yaml"])
    assert not _starts_a_run(["README.md", "provisa-ui/README.md", "CHANGELOG.md"])
    assert not _starts_a_run([".github/workflows/docs.yml"])
    assert not _starts_a_run(["site/a.html", "docs/b.md", "README.md"])


def test_a_push_that_touches_code_starts_a_run_whatever_else_it_touches():
    assert _starts_a_run(["provisa/api/app.py"])
    assert _starts_a_run(["tests/integration/test_x.py"])
    assert _starts_a_run(["site/index.html", "provisa/core/models.py"])
    assert _starts_a_run(["mkdocs.yml"])  # read by a lane; not under docs/
    assert _starts_a_run([".github/workflows/integration-suite-lanes.yml"])
    assert _starts_a_run(["pyproject.toml"])
    assert _starts_a_run(["tests/features/REQ-001.feature"])


def test_the_documentation_file_a_lane_reads_still_starts_a_run():
    """tests/steps/steps_metadata_export_docs.py reads docs/metadata-export.md."""
    step = (REPO / "tests" / "steps" / "steps_metadata_export_docs.py").read_text()
    assert '"docs" / "metadata-export.md"' in step
    assert _starts_a_run(["docs/metadata-export.md"])


def test_no_other_documentation_file_is_read_by_the_container_suite():
    """If a suite test comes to read another file under docs/ or site/, that file must be kept
    in the push filter, or a change to it is never run against the test."""
    reads = re.compile(
        r"""["'](?:docs|site)["']\s*/\s*["']([\w./-]+)["']|["']((?:docs|site)/[\w./-]+)["']"""
    )
    read: set[str] = set()
    for root in ("tests/integration", "tests/steps", "tests/e2e"):
        for path in (REPO / root).rglob("*.py"):
            for a, b in reads.findall(path.read_text(encoding="utf-8")):
                read.add(f"docs/{a}" if a else b)
    unfiltered = sorted(p for p in read if (REPO / p).is_file() and not _starts_a_run([p]))
    assert unfiltered == []


def test_only_pushes_are_filtered():
    triggers = _triggers()
    assert "paths" not in (triggers.get("workflow_dispatch") or {})
    assert triggers["schedule"] == [{"cron": "0 6 * * *"}]
    assert triggers["push"]["branches"] == ["integration", "main"]
