# Copyright (c) 2026 Kenneth Stott
# Canary: 732dd5e4-3169-4bab-96ac-ea9771914f8c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Merge a published release to main and to dev, and delete its branch.

Run by the release's last job (.github/workflows/release-publish.yml), from a clone holding
origin's refs. It stops, exiting 2 with the reason and pushing nothing, on:

- a release branch that moved after the tag: deleting it would lose those commits;
- a merge conflict, into main or into dev, naming the files;
- a main that would not match the release afterwards: main only ever takes a release.

main, dev and the branch's deletion go in one atomic push, so a refused push (a landing on dev
that got in first) changes nothing and the job is re-run. Nothing is ever forced.
"""

# Requirements: REQ-1989
from __future__ import annotations

import argparse
import subprocess  # nosec B404 - argv-list git invocations only, never shell=True
import sys
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


def _merged_into(repo: Path, target: str, version: str) -> None:
    """A merge commit of the release on a local ``target`` taken from origin's."""
    _required(repo, "checkout", "-q", "-B", target, f"refs/remotes/origin/{target}")
    subject = f"chore(release): merge {version} into {target}"
    if _git(repo, "merge", "-q", "--no-ff", "-m", subject, f"refs/tags/{version}").returncode == 0:
        return
    conflicts = _required(repo, "diff", "--name-only", "--diff-filter=U").split()
    _required(repo, "merge", "--abort")
    raise MergeRefused(f"merging into {target} conflicts in: {', '.join(conflicts)}")


def merge(repo: Path, version: str) -> None:
    branch = f"release/{version}"
    _required(repo, "fetch", "-q", "origin", "+refs/heads/*:refs/remotes/origin/*")
    tagged = _required(repo, "rev-parse", "--verify", f"refs/tags/{version}^{{commit}}")
    tip = _required(repo, "rev-parse", "--verify", f"refs/remotes/origin/{branch}")
    if tip != tagged:
        raise MergeRefused(f"{branch} has commits after the tag {version}: {tagged}..{tip}")
    _merged_into(repo, "main", version)
    if _git(repo, "diff", "--quiet", f"refs/tags/{version}", "main").returncode != 0:
        raise MergeRefused(
            f"main would not match {version} after the merge: it holds something no release brought"
        )
    _merged_into(repo, "dev", version)
    _required(repo, "push", "--atomic", "origin", "main", "dev", f":refs/heads/{branch}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Merge a published release to main and dev.")
    parser.add_argument("--version", required=True, help="the release's tag")
    parser.add_argument("--repo", type=Path, default=Path("."), help="the repository (default: .)")
    args = parser.parse_args(argv)
    try:
        merge(args.repo, args.version)
    except MergeRefused as refused:
        print(f"ERROR: {refused}", file=sys.stderr)
        return 2
    print(f"{args.version} merged to main and dev; release/{args.version} deleted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
