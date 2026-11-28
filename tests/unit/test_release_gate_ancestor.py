# Copyright (c) 2026 Kenneth Stott
# Canary: 21b07b8c-db6a-4d26-9556-94967fda13ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The release gate takes a suite's green run from an earlier commit of the release when every
commit since needs no rerun (REQ-1989, amended 2026-11-27): documentation only, a clean merge, a
change to the accepts record alone, or a fix listed in .github/release-gate-accepts.json whose
targeted runs all succeeded on the commit it names. Anything else is still not green, and a beta
or production cut is still refused by it.

History is a real temporary Git repository; the GitHub lookups are replaced, so nothing reaches
the network."""

# Requirements: REQ-1989
from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_ALPHA = "v0.1.0-alpha.9"
_BETA = "v0.2.0-beta.1"


def _gate():
    spec = importlib.util.spec_from_file_location(
        "release_gate_ancestor_under_test", _ROOT / "scripts" / "release_gate.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def _path(name: str) -> str:
    """A keyword name as a path: ``__`` is ``/`` and a trailing ``_md``/``_py``/``_yml`` the
    extension (``_json`` too), so ``docs__guide_md`` is ``docs/guide.md``."""
    path = name.replace("__", "/")
    for ext in ("md", "py", "yml", "json"):
        if path.endswith(f"_{ext}"):
            return f"{path[: -len(ext) - 1]}.{ext}"
    return path


def _commit(repo: Path, subject: str, **files: str) -> str:
    for name, text in files.items():
        path = repo / _path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", subject)
    return _git(repo, "rev-parse", "HEAD")


class _Release:
    """A release branch whose suites ran green on ``base``, with GitHub replaced."""

    def __init__(self, tmp_path: Path, monkeypatch):
        self.repo = tmp_path / "repo"
        self.repo.mkdir()
        _git(self.repo, "init", "-q", "-b", "release/v")
        _git(self.repo, "config", "user.email", "t@example.com")
        _git(self.repo, "config", "user.name", "t")
        self.base = _commit(self.repo, "base", app__core_py="x = 1\n")
        self.gate = _gate()
        self.accepts: dict[str, dict] = {}
        self.views: dict[int, dict] = {}
        self.tip_red: set[str] = set()
        self.green_at: dict[str, list[str]] = {}
        self.viewed: list[int] = []
        gate = self.gate
        monkeypatch.setattr(gate, "ROOT", self.repo)
        monkeypatch.setattr(gate, "load_accepts", lambda repo: self.accepts)
        monkeypatch.setattr(gate, "run_state", self._run_state)
        monkeypatch.setattr(gate, "successful_runs", self._successful_runs)
        monkeypatch.setattr(gate, "run_view", self._run_view)

    def _run_state(self, workflow: str, branch: str, sha: str) -> str:
        assert branch == "release/v"
        if workflow in self.tip_red:
            return "never run  "
        return "completed success http://tip"

    def _successful_runs(self, workflow: str, branch: str) -> list[dict]:
        assert branch == "release/v"
        return [
            {"databaseId": 100 + i, "headSha": sha, "url": f"http://run/{workflow}/{i}"}
            for i, sha in enumerate(self.green_at.get(workflow, []))
        ]

    def _run_view(self, run_id: int) -> dict:
        self.viewed.append(run_id)
        return self.views[run_id]

    def suites_ran_on_base(self, *workflows: str) -> None:
        for workflow in workflows:
            self.tip_red.add(workflow)
            self.green_at[workflow] = [self.base]

    def cut(self, version: str, tmp_path: Path) -> tuple[int, Path]:
        tip = _git(self.repo, "rev-parse", "HEAD")
        out = tmp_path / "gate.md"
        args = ["--version", version, "--branch", "release/v", "--sha", tip, "--out", str(out)]
        return self.gate.main(args), out

    def view(self, run_id: int, sha: str, conclusion: str = "success") -> None:
        self.views[run_id] = {
            "status": "completed",
            "conclusion": conclusion,
            "headSha": sha,
            "url": f"http://targeted/{run_id}",
            "workflowName": "Unit Tests",
        }


@pytest.fixture
def release(tmp_path, monkeypatch) -> _Release:
    return _Release(tmp_path, monkeypatch)


def test_a_documentation_only_commit_needs_no_rerun(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml", "integration-suite.yml")
    docs = _commit(
        release.repo, "docs", docs__guide_md="# Guide\n", CHANGELOG_md="x\n", mkdocs_yml="nav: []\n"
    )
    code, out = release.cut(_ALPHA, tmp_path)
    assert code == 0
    listing = out.read_text()
    assert "Runs accepted from an earlier commit" in listing
    assert "`unit-tests.yml`: http://run/unit-tests.yml/0" in listing
    assert f"`{docs[:10]}`: documentation only" in listing
    assert "Known failures" not in listing


def test_a_previous_cuts_notes_commit_needs_no_rerun(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml", "integration-suite.yml")
    notes = _commit(
        release.repo,
        "chore(release): notes",
        CHANGELOG_md="x\n",
        docs__releases__v1_md="# v1\n",
        **{"provisa-ui__src__generated__releaseNotes_json": "{}\n"},
    )
    code, out = release.cut(_ALPHA, tmp_path)
    assert code == 0
    assert f"`{notes[:10]}`: documentation only" in out.read_text()


def test_a_listed_fix_is_accepted_when_its_targeted_runs_are_green(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml")
    fix = _commit(release.repo, "fix", app__core_py="x = 2\n")
    release.accepts = {fix: {"tested_sha": fix, "runs": [7, 8], "reason": "the fix's own tests"}}
    release.view(7, fix)
    release.view(8, fix)
    code, out = release.cut(_ALPHA, tmp_path)
    assert code == 0
    listing = out.read_text()
    assert f"`{fix[:10]}`: targeted runs 7, 8 green on {fix[:10]}: the fix's own tests" in listing
    assert "Known failures" not in listing


def test_a_fix_proved_on_a_release_commit_that_contains_it_is_accepted(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml")
    fix = _commit(release.repo, "fix", app__core_py="x = 2\n")
    merged = _commit(release.repo, "notes", docs__n_md="n\n")
    release.accepts = {fix: {"tested_sha": merged, "runs": [7], "reason": "unit at the merge"}}
    release.view(7, merged)
    code, out = release.cut(_ALPHA, tmp_path)
    assert code == 0
    assert f"targeted runs 7 green on {merged[:10]}" in out.read_text()


@pytest.mark.parametrize("conclusion", ["failure", "cancelled"])
def test_a_listed_fix_whose_targeted_run_is_not_green_is_not_accepted(
    release, tmp_path, conclusion
):
    release.suites_ran_on_base("unit-tests.yml")
    fix = _commit(release.repo, "fix", app__core_py="x = 2\n")
    release.accepts = {fix: {"tested_sha": fix, "runs": [7, 8], "reason": "r"}}
    release.view(7, fix)
    release.view(8, fix, conclusion)
    code, out = release.cut(_ALPHA, tmp_path)
    assert code == 0
    listing = out.read_text()
    assert "Runs accepted" not in listing
    assert "Known failures" in listing
    assert f"targeted run http://targeted/8 is completed {conclusion}" in listing


def test_a_listed_fix_whose_run_tested_another_commit_is_not_accepted(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml")
    fix = _commit(release.repo, "fix", app__core_py="x = 2\n")
    release.accepts = {fix: {"tested_sha": fix, "runs": [7], "reason": "r"}}
    release.view(7, release.base)
    code, out = release.cut(_ALPHA, tmp_path)
    assert code == 0
    assert f"ran on {release.base[:10]}, not {fix[:10]}" in out.read_text()


def test_a_listed_fix_with_no_runs_is_not_accepted(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml")
    fix = _commit(release.repo, "fix", app__core_py="x = 2\n")
    release.accepts = {fix: {"tested_sha": fix, "runs": [], "reason": "r"}}
    code, out = release.cut(_ALPHA, tmp_path)
    assert "entry lists no runs" in out.read_text()


def test_an_unlisted_code_commit_is_refused(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml")
    _commit(release.repo, "docs", docs__a_md="a\n")
    code_commit = _commit(release.repo, "code", app__core_py="x = 3\n", docs__b_md="b\n")
    code, out = release.cut(_ALPHA, tmp_path)
    assert code == 0
    listing = out.read_text()
    assert "Runs accepted" not in listing
    assert "- `unit-tests.yml`: never run" in listing
    assert f"{code_commit[:10]} changes code and is not listed" in listing


def test_an_alpha_whose_dry_run_build_is_only_on_a_refused_ancestor_is_not_cut(
    release, tmp_path, capsys
):
    release.suites_ran_on_base("build-dmg.yml")
    _commit(release.repo, "code", app__core_py="x = 3\n")
    code, out = release.cut(_ALPHA, tmp_path)
    assert code == 2
    assert "::error::build-dmg.yml" in capsys.readouterr().out
    assert not out.exists()


def test_an_alphas_dry_run_build_is_accepted_from_an_ancestor_under_the_same_rule(
    release, tmp_path
):
    release.suites_ran_on_base("build-dmg.yml")
    _commit(release.repo, "docs", docs__a_md="a\n")
    code, out = release.cut(_ALPHA, tmp_path)
    assert code == 0
    assert "`build-dmg.yml`: http://run/build-dmg.yml/0" in out.read_text()


def test_a_clean_merge_is_judged_by_the_commits_it_brings_in(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml")
    _git(release.repo, "checkout", "-q", "-b", "side")
    _commit(release.repo, "side docs", docs__side_md="s\n")
    _git(release.repo, "checkout", "-q", "release/v")
    _commit(release.repo, "main docs", docs__main_md="m\n")
    _git(release.repo, "merge", "-q", "--no-ff", "-m", "merge side", "side")
    code, out = release.cut(_ALPHA, tmp_path)
    assert code == 0
    assert "clean merge" in out.read_text()


def test_a_merge_that_brings_in_unlisted_code_is_refused(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml")
    _git(release.repo, "checkout", "-q", "-b", "side")
    side = _commit(release.repo, "side code", app__other_py="y = 1\n")
    _git(release.repo, "checkout", "-q", "release/v")
    _commit(release.repo, "main docs", docs__main_md="m\n")
    _git(release.repo, "merge", "-q", "--no-ff", "-m", "merge side", "side")
    code, out = release.cut(_ALPHA, tmp_path)
    assert f"{side[:10]} changes code and is not listed" in out.read_text()


def test_a_change_to_the_accepts_record_alone_needs_no_rerun(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml")
    record = json.dumps({"commits": {}})
    _commit(release.repo, "record", **{".github__release-gate-accepts.json": record})
    code, out = release.cut(_ALPHA, tmp_path)
    assert code == 0
    assert "changes only .github/release-gate-accepts.json" in out.read_text()


def test_an_older_green_run_on_the_cut_commit_does_not_hide_its_newer_failure(release, tmp_path):
    # The latest run on the cut commit is what counts there; only an ancestor's run is taken.
    tip = _commit(release.repo, "docs", docs__a_md="a\n")
    release.tip_red.add("unit-tests.yml")
    release.green_at["unit-tests.yml"] = [tip]
    code, out = release.cut(_ALPHA, tmp_path)
    assert f"no green run on an ancestor of {tip[:10]}" in out.read_text()


def test_a_run_on_a_commit_outside_the_history_is_not_taken(release, tmp_path):
    release.tip_red.add("unit-tests.yml")
    release.green_at["unit-tests.yml"] = ["f" * 40]
    _commit(release.repo, "docs", docs__a_md="a\n")
    code, out = release.cut(_ALPHA, tmp_path)
    assert "no green run on an ancestor" in out.read_text()


def test_the_newest_ancestor_run_is_the_one_judged(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml")
    _commit(release.repo, "code", app__core_py="x = 3\n")
    later = _commit(release.repo, "docs", docs__a_md="a\n")
    release.green_at["unit-tests.yml"] = [later, release.base]
    _commit(release.repo, "more docs", docs__b_md="b\n")
    code, out = release.cut(_ALPHA, tmp_path)
    listing = out.read_text()
    assert f"on `{later[:10]}`" in listing
    assert "Known failures" not in listing


def test_a_beta_cut_is_refused_by_an_unlisted_code_commit(release, tmp_path, capsys):
    release.suites_ran_on_base("integration-suite.yml")
    _commit(release.repo, "code", app__core_py="x = 3\n")
    code, out = release.cut(_BETA, tmp_path)
    assert code == 2
    assert "::error::integration-suite.yml" in capsys.readouterr().out
    assert not out.exists()


def test_a_beta_cut_is_refused_by_a_listed_fix_whose_run_failed(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml")
    fix = _commit(release.repo, "fix", app__core_py="x = 2\n")
    release.accepts = {fix: {"tested_sha": fix, "runs": [7], "reason": "r"}}
    release.view(7, fix, "failure")
    code, out = release.cut(_BETA, tmp_path)
    assert code == 2
    assert not out.exists()


def test_a_beta_still_needs_every_suite_green_somewhere(release, tmp_path):
    release.tip_red.add("ui-e2e-core.yml")
    _commit(release.repo, "docs", docs__a_md="a\n")
    code, out = release.cut(_BETA, tmp_path)
    assert code == 2
    assert not out.exists()


def test_a_beta_cut_names_what_it_accepted(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml")
    docs = _commit(release.repo, "docs", docs__a_md="a\n")
    code, out = release.cut(_BETA, tmp_path)
    assert code == 0
    assert f"`{docs[:10]}`: documentation only" in out.read_text()


def test_a_targeted_run_is_looked_up_once_for_every_suite(release, tmp_path):
    release.suites_ran_on_base("unit-tests.yml", "integration-suite.yml", "lint.yml")
    fix = _commit(release.repo, "fix", app__core_py="x = 2\n")
    release.accepts = {fix: {"tested_sha": fix, "runs": [7], "reason": "r"}}
    release.view(7, fix)
    code, _ = release.cut(_ALPHA, tmp_path)
    assert code == 0
    assert release.viewed == [7]


def test_the_committed_accepts_record_names_every_run_by_id():
    record = json.loads((_ROOT / ".github" / "release-gate-accepts.json").read_text())
    for sha, entry in record["commits"].items():
        assert len(sha) == 40 and len(entry["tested_sha"]) == 40
        assert entry["runs"] and all(isinstance(r, int) for r in entry["runs"])
        assert entry["reason"]
