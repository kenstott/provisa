# Copyright (c) 2026 Kenneth Stott
# Canary: 9f541809-e2cc-448a-a3a6-a2e41dfd195d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The release notes generator, on a fixture repository's range between two tags."""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
CHANGELOG = """# Changelog

All notable changes to Provisa are documented in this file.

## [Unreleased]

### Phase A: Infrastructure
- Docker Compose setup
"""
MKDOCS = """site_name: Provisa
nav:
  - Home: index.md
  - Architecture:
      - Overview: architecture.md

exclude_docs: |
  exports/
"""


def _generator():
    spec = importlib.util.spec_from_file_location(
        "gen_release_notes", REPO / "scripts" / "gen_release_notes.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["gen_release_notes"] = module
    spec.loader.exec_module(module)
    return module


gen = _generator()


def _req(req_id: str, status: str, category: str, description: str) -> dict:
    return {"id": req_id, "status": status, "category": category, "description": description}


class Fixture:
    def __init__(self, root: Path):
        self.root = root
        self.git("init", "-q", "-b", "main")
        (root / "docs" / "arch").mkdir(parents=True)
        (root / "scripts").mkdir()
        (root / "CHANGELOG.md").write_text(CHANGELOG)
        (root / "mkdocs.yml").write_text(MKDOCS)
        self.settings(None)

    def git(self, *args: str, date: str = "2026-10-01T12:00:00") -> str:
        env = {
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.com",
            "GIT_AUTHOR_DATE": date,
            "GIT_COMMITTER_DATE": date,
            "HOME": str(self.root),
            "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
            "GIT_CONFIG_NOSYSTEM": "1",
        }
        done = subprocess.run(
            ["git", "-c", "core.hooksPath=/dev/null", "-C", str(self.root), *args],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        return done.stdout

    def settings(self, start: str | None) -> None:
        (self.root / "scripts" / "release_notes.yaml").write_text(
            f"start: {start}\n" if start else "start:\n"
        )

    def requirements(self, entries: list[dict]) -> None:
        (self.root / "docs" / "arch" / "requirements.yaml").write_text(yaml.safe_dump(entries))

    def commit(self, subject: str, body: str = "", date: str = "2026-10-01T12:00:00") -> None:
        self.git("add", "-A", date=date)
        self.git("commit", "-q", "--allow-empty", "-m", subject, "-m", body, date=date)

    def tag(self, name: str, date: str = "2026-10-01T12:00:00") -> None:
        self.git("tag", name, date=date)

    def run(self, prev: str, new: str) -> int:
        return gen.main(["--from", prev, "--to", new, "--repo", str(self.root)])

    def notes(self) -> dict | None:
        return json.loads((self.root / gen.UI_JSON).read_text())

    def page(self, version: str) -> Path:
        return self.root / "docs" / "releases" / f"{version}.md"


@pytest.fixture
def repo(tmp_path: Path) -> Fixture:
    """v1 → v2: one requirement becomes complete, one stays proposed; commits of every kind."""
    fx = Fixture(tmp_path)
    fx.requirements(
        [
            _req("REQ-010", "in-progress", "Replication", "A failed build is retried. Wait grows."),
            _req("REQ-011", "proposed", "Replication", "Builds run in parallel."),
            _req("REQ-012", "complete", "Security", "Already shipped before."),
        ]
    )
    fx.commit("feat(core): the first release")
    fx.tag("v1")
    fx.requirements(
        [
            _req(
                "REQ-010",
                "complete",
                "Replication",
                "A failed build is retried with a growing wait (see REQ-011). It never gives up, e.g. on a timeout.",
            ),
            _req("REQ-011", "proposed", "Replication", "Builds run in parallel."),
            _req("REQ-012", "complete", "Security", "Already shipped before."),
        ]
    )
    fx.commit("reqs: REQ-010 -- complete")
    fx.commit("feat(replication): a failed build is retried (REQ-010)")
    fx.commit("fix(replication): the retry wait is capped", body="Part of REQ-009..010.")
    fx.commit("feat(ui): the replica page shows its next try (REQ-011)")
    fx.commit("fix(api): a float subclass is encoded as its number")
    fx.commit("fix(tests): the retry test uses an injected clock")
    fx.commit("test(replication): a retry is counted")
    fx.commit("chore(replication): canary stamps")
    fx.commit("feat(replication): canary stamps of the retry loop")
    fx.commit("ci(release): wait for the wheel")
    fx.commit("docs(replication): the retry policy")
    fx.commit("refactor(replication): one retry function")
    fx.commit("feat: strawberry-graphql is upgraded")
    fx.tag("v2")
    return fx


def test_a_requirement_that_became_complete_is_a_feature_in_its_category(repo: Fixture):
    assert repo.run("v1", "v2") == 0
    notes = repo.notes()
    assert notes is not None
    assert [g["category"] for g in notes["features"]] == ["Replication"]
    [entry] = notes["features"][0]["entries"]
    # Its description's first sentence, with no requirement id; "e.g." does not end it.
    assert entry["text"] == "A failed build is retried with a growing wait."


def test_a_commit_citing_a_completed_requirement_is_folded_under_it_not_listed_again(
    repo: Fixture,
):
    repo.run("v1", "v2")
    notes = repo.notes()
    assert notes is not None
    [entry] = notes["features"][0]["entries"]
    # Cited in the subject, and in the body by a range.
    assert entry["changes"] == ["A failed build is retried", "The retry wait is capped"]
    listed = [text for g in notes["changes"] for text in g["entries"]]
    assert "A failed build is retried" not in listed
    assert "The retry wait is capped" not in listed


def test_changes_are_feat_and_user_visible_fix_commits_by_scope(repo: Fixture):
    repo.run("v1", "v2")
    notes = repo.notes()
    assert notes is not None
    assert notes["changes"] == [
        {"scope": "api", "entries": ["A float subclass is encoded as its number"]},
        # A requirement not completed in this release does not fold its commit.
        {"scope": "ui", "entries": ["The replica page shows its next try"]},
        # No scope; a name keeps its own spelling.
        {"scope": "", "entries": ["strawberry-graphql is upgraded"]},
    ]


def test_test_chore_ci_docs_refactor_and_canary_commits_are_left_out(repo: Fixture):
    repo.run("v1", "v2")
    text = repo.page("v2").read_text() + (repo.root / "CHANGELOG.md").read_text()
    for left_out in (
        "injected clock",
        "a retry is counted",
        "Canary stamps",
        "canary stamps",
        "wait for the wheel",
        "the retry policy",
        "one retry function",
        "the first release",
    ):
        assert left_out not in text, left_out


def test_no_requirement_id_in_any_output(repo: Fixture):
    repo.run("v1", "v2")
    for path in (
        repo.root / gen.UI_JSON,
        repo.page("v2"),
        repo.root / "CHANGELOG.md",
        repo.root / "mkdocs.yml",
    ):
        assert not re.search(r"REQ-?\d", path.read_text(), re.IGNORECASE), path


def test_a_requirement_id_the_generator_cannot_remove_fails_the_run(repo: Fixture):
    # A requirement that becomes complete in v3, its description citing one in a spelling the
    # generator does not remove.
    repo.requirements([_req("REQ-013", "complete", "Replication", "Retried as REQ010 says.")])
    repo.commit("feat(replication): the wording")
    repo.tag("v3")
    assert repo.run("v2", "v3") == 2


def test_the_outputs_the_docs_page_the_nav_and_the_changelog(repo: Fixture):
    assert repo.run("v1", "v2") == 0
    page = repo.page("v2").read_text()
    assert page.startswith("# v2\n\nReleased 2026-10-01.\n\n## Features\n\n### Replication\n\n")
    assert (
        "- A failed build is retried with a growing wait.\n    - A failed build is retried\n"
        in page
    )
    assert "## Changes\n\n### api\n\n- A float subclass is encoded as its number\n" in page
    assert "### General\n\n- strawberry-graphql is upgraded\n" in page
    mkdocs = (repo.root / "mkdocs.yml").read_text()
    assert "  - Release Notes:\n      - v2: releases/v2.md\n\nexclude_docs" in mkdocs
    changelog = (repo.root / "CHANGELOG.md").read_text()
    assert "## [Unreleased]" not in changelog
    assert changelog.index("## v2\n") < changelog.index("## History\n")
    assert changelog.index("## History\n") < changelog.index("### Phase A: Infrastructure")
    assert "### Features\n\n#### Replication\n" in changelog


def test_a_second_run_and_a_later_release_keep_one_section_each_newest_first(repo: Fixture):
    repo.run("v1", "v2")
    repo.run("v1", "v2")
    repo.commit("feat(ui): a later page", date="2026-10-05T12:00:00")
    repo.tag("v3", date="2026-10-05T12:00:00")
    assert repo.run("v2", "v3") == 0
    changelog = (repo.root / "CHANGELOG.md").read_text()
    assert changelog.count("## v2\n") == 1
    assert changelog.index("## v3\n") < changelog.index("## v2\n") < changelog.index("## History")
    mkdocs = (repo.root / "mkdocs.yml").read_text()
    assert "  - Release Notes:\n      - v3: releases/v3.md\n      - v2: releases/v2.md\n" in mkdocs
    notes = repo.notes()
    assert notes is not None and notes["version"] == "v3"


def _all_changes(repo: Fixture) -> list[str]:
    notes = repo.notes()
    assert notes is not None
    return [text for g in notes["changes"] for text in g["entries"]]


def test_a_commit_reverted_before_the_release_is_left_out_with_its_revert(repo: Fixture):
    (repo.root / "page.txt").write_text("a page\n")  # a revert needs a change to undo
    repo.commit("feat(ui): a page that did not last", date="2026-10-03T12:00:00")
    repo.git("revert", "--no-edit", "HEAD", date="2026-10-04T12:00:00")
    repo.commit("feat(ui): a page that stayed", date="2026-10-05T12:00:00")
    repo.tag("v3", date="2026-10-05T12:00:00")
    assert repo.run("v2", "v3") == 0
    changes = _all_changes(repo)
    assert changes == ["A page that stayed"]
    assert "did not last" not in repo.page("v3").read_text()


def test_a_revert_of_an_earlier_release_s_commit_is_left_out(repo: Fixture):
    # The commit it reverts shipped in v2; the revert in v3 is not a change of its own.
    (repo.root / "page.txt").write_text("a page\n")  # a revert needs a change to undo
    repo.commit("feat(ui): a page", date="2026-10-02T12:00:00")
    repo.tag("v2.1", date="2026-10-02T12:00:00")
    earlier = repo.git("rev-parse", "HEAD").strip()
    repo.git("revert", "--no-edit", earlier, date="2026-10-04T12:00:00")
    repo.commit("feat(ui): a later page", date="2026-10-05T12:00:00")
    repo.tag("v3", date="2026-10-05T12:00:00")
    assert repo.run("v2.1", "v3") == 0
    assert _all_changes(repo) == ["A later page"]


def test_a_commit_the_settings_exclude_is_left_out(repo: Fixture):
    sha = repo.git("log", "-1", "--format=%H", "--grep=float subclass").strip()
    (repo.root / "scripts" / "release_notes.yaml").write_text(
        f"start:\nexclude:\n  - sha: {sha[:9]}\n    reason: reverted by hand before the release\n"
    )
    assert repo.run("v1", "v2") == 0
    assert "A float subclass is encoded as its number" not in _all_changes(repo)
    assert "float subclass" not in repo.page("v2").read_text()


@pytest.mark.parametrize(
    "entry",
    ["  - sha: 0000000000\n    reason: no such commit\n", "  - sha: HEAD\n"],
    ids=["unknown sha", "no reason"],
)
def test_an_exclude_entry_naming_no_commit_or_no_reason_fails_the_run(repo: Fixture, entry: str):
    (repo.root / "scripts" / "release_notes.yaml").write_text(f"start:\nexclude:\n{entry}")
    assert repo.run("v1", "v2") == 2


def test_an_unknown_ref_fails_the_run(repo: Fixture):
    assert repo.run("v1", "no-such-tag") == 2


def _later_release(repo: Fixture) -> None:
    repo.run("v1", "v2")
    repo.commit("feat(ui): a later page", date="2026-11-02T12:00:00")
    repo.tag("v3", date="2026-11-02T12:00:00")


def test_with_no_start_every_release_is_shown(repo: Fixture):
    _later_release(repo)
    assert repo.run("v2", "v3") == 0
    assert repo.page("v2").exists() and repo.page("v3").exists()
    assert "## v2\n" in (repo.root / "CHANGELOG.md").read_text()


@pytest.mark.parametrize("start", ["2026-11-01", "v3"])
def test_a_release_before_the_start_is_left_out_of_every_output(repo: Fixture, start: str):
    _later_release(repo)
    repo.settings(start)
    # The release before the start writes nothing of its own, and the UI holds no notes.
    assert repo.run("v1", "v2") == 0
    assert repo.notes() is None
    assert not repo.page("v2").exists()
    # The release from the start is shown; what an earlier run wrote for v2 is removed.
    assert repo.run("v2", "v3") == 0
    notes = repo.notes()
    assert notes is not None and notes["version"] == "v3"
    assert repo.page("v3").exists() and not repo.page("v2").exists()
    changelog = (repo.root / "CHANGELOG.md").read_text()
    assert "## v3\n" in changelog and "## v2\n" not in changelog
    mkdocs = (repo.root / "mkdocs.yml").read_text()
    assert "releases/v3.md" in mkdocs and "releases/v2.md" not in mkdocs


def test_a_start_that_is_no_tag_fails_the_run(repo: Fixture):
    repo.settings("v9")
    assert repo.run("v1", "v2") == 2


@pytest.mark.parametrize(
    ("subject", "scope"),
    [
        ("fix(REQ-039): fail closed when a table is absent", ""),
        ("fix(REQ-039, REQ-2167): a qualified call is a call", ""),
        ("fix(REQ-005,REQ-883,REQ-2190): a catalog statement is answered whole", ""),
        ("fix(R508g3): a synthetic env is built in full", ""),
        ("fix(r508): the steps fake holds node ids", ""),
        ("fix(J1185): the last pg differences", ""),
        ("fix(pgwire): a catalog statement is answered whole", "pgwire"),
    ],
)
def test_a_scope_of_requirement_or_work_item_ids_is_no_scope(subject: str, scope: str):
    # Requirement ids and work item ids never reach the notes, the scope included (REQ-1989).
    change = gen._change(subject)
    assert change is not None
    assert change[0] == scope
