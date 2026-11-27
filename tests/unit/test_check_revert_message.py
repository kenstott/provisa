# Copyright (c) 2026 Kenneth Stott
# Canary: 751ae52c-5eee-4e83-b6c7-265e72fb550a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The commit-msg check: a commit that reverts names the commit it undoes, in git's own line."""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


def _check():
    spec = importlib.util.spec_from_file_location(
        "check_revert_message", REPO / "scripts" / "check_revert_message.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_revert_message"] = module
    spec.loader.exec_module(module)
    return module


check = _check()


@pytest.fixture
def sha(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """A repository with one commit, as the working directory; its sha."""
    # On NixOS, git lives at /run/current-system/sw/bin/git (not in the FHS
    # paths below).  Discover git's actual location before we override PATH so
    # the fixture works on every distro without hard-coding distro paths.
    # REQ-1900: the NixOS CI workflow must pass its unit-test leg.
    _git = shutil.which("git") or "/usr/bin/git"
    _git_dir = str(Path(_git).parent)
    env = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
        "HOME": str(tmp_path),
        "PATH": f"/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:{_git_dir}",
        "GIT_CONFIG_NOSYSTEM": "1",
    }

    def run(*args: str) -> str:
        return subprocess.run(
            ["git", "-c", "core.hooksPath=/dev/null", *args],
            cwd=tmp_path,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    run("init", "-q")
    run("commit", "-q", "--allow-empty", "-m", "feat(ui): a page")
    monkeypatch.chdir(tmp_path)
    return run("rev-parse", "HEAD").strip()


@pytest.mark.parametrize("subject", ["revert: a page", "revert(ui): a page", "revert(ui)!: a page"])
def test_a_revert_without_git_s_line_is_refused_and_told_what_to_add(sha: str, subject: str):
    reason = check.problem(f"{subject}\n\nThe page is withdrawn.\n")
    assert reason is not None
    assert "This reverts commit <sha" in reason


def test_a_revert_naming_its_commit_passes(sha: str):
    assert check.problem(f"revert(ui): a page\n\nThis reverts commit {sha}.\n") is None
    assert check.problem(f"revert: a page\n\nThis reverts commit {sha[:9]}.\n") is None


def test_a_revert_naming_no_commit_that_exists_is_refused(sha: str):
    reason = check.problem("revert(ui): a page\n\nThis reverts commit 0123456789abcdef.\n")
    assert reason is not None and "0123456789abcdef" in reason and "names no commit" in reason


def test_other_commits_and_git_s_own_comment_lines_are_not_held(sha: str):
    assert check.problem("feat(ui): revert button on the page\n") is None
    assert check.problem('Revert "feat(ui): a page"\n\nThis reverts commit ' + sha + ".\n") is None
    # A line git adds as a comment is not part of the message.
    assert (
        check.problem(f"# revert: a comment\nfeat(ui): a page\n# This reverts commit {sha}\n")
        is None
    )


def test_the_hook_exits_non_zero_with_the_reason_on_a_refused_message(
    sha: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    message = tmp_path / "COMMIT_EDITMSG"
    message.write_text("revert(ui): a page\n")
    assert check.main([str(message)]) == 1
    assert "commit refused" in capsys.readouterr().err
    message.write_text(f"revert(ui): a page\n\nThis reverts commit {sha}.\n")
    assert check.main([str(message)]) == 0


def test_the_hook_runs_at_the_commit_msg_stage_and_the_others_stay_at_pre_commit():
    import yaml

    config = yaml.safe_load((REPO / ".pre-commit-config.yaml").read_text())
    assert config["default_stages"] == ["pre-commit"]
    hooks = {h["id"]: h for r in config["repos"] for h in r["hooks"]}
    assert hooks["revert-names-commit"]["stages"] == ["commit-msg"]
    assert "scripts/check_revert_message.py" in hooks["revert-names-commit"]["entry"]
    delegate = (REPO / ".githooks" / "commit-msg").read_text()
    assert "--hook-stage commit-msg" in delegate and '--commit-msg-filename "$1"' in delegate
