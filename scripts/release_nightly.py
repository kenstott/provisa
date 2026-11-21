# Copyright (c) 2026 Kenneth Stott
# Canary: 0ed0c2b4-1f2b-4fbc-aea8-2853d1b0563f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The nightly alpha's start (REQ-1989): a release branch cut from main's newest commit.

Run by .github/workflows/alpha-nightly.yml, from a clone that can push. In order:

1. A scheduled run goes on only if its cron is the one for New York's current UTC offset: the
   workflow holds one cron for each offset, because a cron is in UTC and 17:00 in New York is not
   one hour of it all year.
2. One alpha at a time: an alpha is open until it ships or is abandoned, and while one is open the
   night is skipped and the open branch is named in the log. release_merge.py deletes a release's
   branch in the same push that merges it to main, and abandoning an alpha deletes its branch, so
   any release/<alpha> branch still on origin is open, tagged or not.
3. With no alpha open, the last alpha is the highest tag. When it already holds main, or matches
   main (a shipped release merged back, or a fix cascaded to main, adds a commit and changes
   nothing), main has not moved and the night is skipped.
4. The next alpha number is taken above that tag.
5. release/<version> is pushed at origin/main's commit, everything on main to that point. It is
   a cut: no commit is made and nothing is ever forced.

Prints ``version=<tag>`` or ``skip=true`` for the step's outputs, which is why the open branch is
named on stderr: stdout is appended to $GITHUB_OUTPUT. Exits 2, pushing nothing, on a refused
push, on a release above the last alpha (alpha numbering ends at the first beta release), or when
no alpha can be found to number from.
"""

# Requirements: REQ-1989
from __future__ import annotations

import argparse
import re
import subprocess  # nosec B404 - argv-list git invocations only, never shell=True
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_NEW_YORK = ZoneInfo("America/New_York")
_HOUR = 17
_RELEASE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)(?:-(alpha|beta|rc)\.(\d+))?$")
_STAGES = {"alpha": 0, "beta": 1, "rc": 2, None: 3}


class NightRefused(Exception):
    pass


def due(schedule: str, now: datetime) -> bool:
    """Whether ``schedule``, the cron that fired, is the one for New York's offset at ``now``."""
    offset = now.astimezone(_NEW_YORK).utcoffset()
    assert offset is not None
    return schedule == f"0 {_HOUR - int(offset.total_seconds() // 3600)} * * *"


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(  # nosec B603 B607 - fixed git argv, no shell
        ["git", "-C", str(repo), *args], capture_output=True, text=True
    )


def _required(repo: Path, *args: str) -> str:
    done = _git(repo, *args)
    if done.returncode != 0:
        raise NightRefused(f"git {' '.join(args)}: {done.stderr.strip()}")
    return done.stdout.strip()


def _order(version: str) -> tuple[int, int, int, int, int]:
    match = _RELEASE.match(version)
    assert match is not None, version
    major, minor, patch, stage, number = match.groups()
    return int(major), int(minor), int(patch), _STAGES[stage], int(number or 0)


def _tags(repo: Path) -> list[str]:
    return [t for t in _required(repo, "tag", "--list", "v*").split() if _RELEASE.match(t)]


def _branches(repo: Path) -> list[str]:
    """The versions with a release branch on origin."""
    return [
        b
        for b in _required(
            repo,
            "for-each-ref",
            "--format=%(refname:strip=4)",
            "refs/remotes/origin/release/",
        ).split()
        if _RELEASE.match(b)
    ]


def _last_alpha(releases: list[str]) -> str:
    """The highest alpha among ``releases``, refused when a release stands above it."""
    alphas = [v for v in releases if "-alpha." in v]
    if not alphas:
        raise NightRefused("no alpha tag or open alpha branch to number the next alpha from")
    last = max(alphas, key=_order)
    above = [v for v in releases if _order(v) > _order(last)]
    if above:
        raise NightRefused(
            f"alpha numbering ends at the first beta release, and {max(above, key=_order)} "
            f"is above the last alpha {last}"
        )
    return last


def _main_moved(repo: Path, ref: str) -> bool:
    main = "refs/remotes/origin/main"
    if _git(repo, "merge-base", "--is-ancestor", main, ref).returncode == 0:
        return False
    return _git(repo, "diff", "--quiet", ref, main).returncode != 0


def _next(version: str) -> str:
    stem, number = version.rsplit(".", 1)
    return f"{stem}.{int(number) + 1}"


def _cut(repo: Path, version: str) -> None:
    """release/<version> is main at its newest commit."""
    _required(
        repo, "push", "-q", "origin", f"refs/remotes/origin/main:refs/heads/release/{version}"
    )


def start(repo: Path) -> str | None:
    """The version started tonight, or None when an alpha is open or main has not moved."""
    _required(
        repo, "fetch", "-q", "--prune", "--tags", "origin", "+refs/heads/*:refs/remotes/origin/*"
    )
    tags = _tags(repo)
    branches = _branches(repo)
    last = _last_alpha(tags + branches)
    open_alphas = sorted((b for b in branches if "-alpha." in b), key=_order)
    if open_alphas:
        names = ", ".join(f"release/{b}" for b in open_alphas)
        print(f"skipping the night: an alpha is still open: {names}", file=sys.stderr)
        return None
    if not _main_moved(repo, f"refs/tags/{last}"):
        return None
    version = _next(last)
    _cut(repo, version)
    return version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Start the nightly alpha.")
    parser.add_argument("--repo", type=Path, default=Path("."), help="the repository (default: .)")
    parser.add_argument("--schedule", default="", help="the cron that fired; empty on a dispatch")
    args = parser.parse_args(argv)
    if args.schedule and not due(args.schedule, datetime.now(_NEW_YORK)):
        print("skip=true")
        return 0
    try:
        version = start(args.repo)
    except NightRefused as refused:
        print(f"ERROR: {refused}", file=sys.stderr)
        return 2
    print(f"version={version}" if version else "skip=true")
    return 0


if __name__ == "__main__":
    sys.exit(main())
