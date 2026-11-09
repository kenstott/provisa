# Copyright (c) 2026 Kenneth Stott
# Canary: 732dd5e4-3169-4bab-96ac-ea9771914f8c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Merge a published release back to main, and delete its branch.

Run by the lead, after the release is published:

    python scripts/release_merge.py --version v0.1.0-alpha.480

It works in a temporary worktree, so any clone will do and its checkouts are left alone. It uses
no CI secret: the push is the lead's. It prints what it did and stops, exiting 2 with the reason
and pushing nothing, on:

- a release branch that moved after the tag: deleting it would lose those commits;
- a merge conflict into main, naming the files.

main and the branch's deletion go in one atomic push, so a refused push (a landing on main that
got in first) changes nothing and the script is run again. Nothing is ever forced.
"""

# Requirements: REQ-1989
from __future__ import annotations

import argparse
import subprocess  # nosec B404 - argv-list git invocations only, never shell=True
import sys
import tempfile
from pathlib import Path


class MergeRefused(Exception):
    pass


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(  # nosec B603 B607 - fixed git argv, no shell
        ["git", "-C", str(repo), *args], capture_output=True, text=True
    )


def _required(repo: Path, *args: str) -> str:
    done = _git(repo, *args)
    if done.returncode != 0:
        raise MergeRefused(f"git {' '.join(args)}: {done.stderr.strip()}")
    return done.stdout.strip()


def _merged_into(repo: Path, target: str, version: str) -> str:
    """The merge commit of the release on origin's ``target``, made in ``repo``'s detached HEAD."""
    _required(repo, "checkout", "-q", "--detach", f"refs/remotes/origin/{target}")
    subject = f"chore(release): merge {version} into {target}"
    if _git(repo, "merge", "-q", "--no-ff", "-m", subject, f"refs/tags/{version}").returncode != 0:
        conflicts = _required(repo, "diff", "--name-only", "--diff-filter=U").split()
        _required(repo, "merge", "--abort")
        raise MergeRefused(f"merging into {target} conflicts in: {', '.join(conflicts)}")
    return _required(repo, "rev-parse", "HEAD")


def _short(sha: str) -> str:
    return sha[:12]


def merge(repo: Path, version: str) -> None:
    branch = f"release/{version}"
    _required(repo, "fetch", "-q", "--tags", "origin", "+refs/heads/*:refs/remotes/origin/*")
    tagged = _required(repo, "rev-parse", "--verify", f"refs/tags/{version}^{{commit}}")
    tip = _required(repo, "rev-parse", "--verify", f"refs/remotes/origin/{branch}")
    if tip != tagged:
        raise MergeRefused(f"{branch} has commits after the tag {version}: {tagged}..{tip}")
    old_main = _required(repo, "rev-parse", "refs/remotes/origin/main")
    with tempfile.TemporaryDirectory(prefix="release-merge-") as parent:
        tree = Path(parent) / "tree"
        _required(repo, "worktree", "add", "-q", "--detach", str(tree), tagged)
        try:
            new_main = _merged_into(tree, "main", version)
            _required(
                tree,
                "push",
                "-q",
                "--atomic",
                "origin",
                f"{new_main}:refs/heads/main",
                f":refs/heads/{branch}",
            )
        finally:
            _git(repo, "worktree", "remove", "--force", str(tree))
    print(f"main: {_short(old_main)} -> {_short(new_main)} (merged {version})")
    print(f"deleted {branch}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Merge a published release back to main.")
    parser.add_argument("--version", required=True, help="the release's tag")
    parser.add_argument("--repo", type=Path, default=Path("."), help="the repository (default: .)")
    args = parser.parse_args(argv)
    try:
        merge(args.repo, args.version)
    except MergeRefused as refused:
        print(f"ERROR: {refused}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
