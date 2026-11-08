# Copyright (c) 2026 Kenneth Stott
# Canary: 0ed0c2b4-1f2b-4fbc-aea8-2853d1b0563f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The nightly alpha's start (REQ-1989): a release branch from main with all of dev merged.

Run by .github/workflows/alpha-nightly.yml, from a clone that can push. In order:

1. A scheduled run goes on only if its cron is the one for New York's current UTC offset: the
   workflow holds one cron for each offset, because a cron is in UTC and 17:00 in New York is not
   one hour of it all year.
2. The last alpha is the highest one, tagged or still an open release branch. When it already
   holds dev, or matches dev (a shipped release merged to dev adds a commit and changes nothing),
   dev has not moved and the night is skipped.
3. The next alpha number is taken above every tag and every open branch, so an alpha nobody cut
   keeps its number.
4. release/<version> is made from origin/main, origin/dev is merged into it, and it is pushed.
   A conflict stops the night, naming the files, with nothing pushed. Nothing is ever forced.

Prints ``version=<tag>`` or ``skip=true`` for the step's outputs. Exits 2, pushing nothing, on a
conflict, on a release above the last alpha (alpha numbering ends at the first beta release), or
when no alpha can be found to number from.
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


def _last_alpha(repo: Path) -> tuple[str, str]:
    """The highest alpha, tagged or open, as (version, the ref holding it)."""
    tags = [t for t in _required(repo, "tag", "--list", "v*").split() if _RELEASE.match(t)]
    branches = [
        b
        for b in _required(
            repo,
            "for-each-ref",
            "--format=%(refname:strip=4)",
            "refs/remotes/origin/release/",
        ).split()
        if _RELEASE.match(b)
    ]
    releases = tags + branches
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
    ref = f"refs/tags/{last}" if last in tags else f"refs/remotes/origin/release/{last}"
    return last, ref


def _dev_moved(repo: Path, ref: str) -> bool:
    dev = "refs/remotes/origin/dev"
    if _git(repo, "merge-base", "--is-ancestor", dev, ref).returncode == 0:
        return False
    return _git(repo, "diff", "--quiet", ref, dev).returncode != 0


def _next(version: str) -> str:
    stem, number = version.rsplit(".", 1)
    return f"{stem}.{int(number) + 1}"


def _make_branch(repo: Path, version: str) -> None:
    """release/<version> is main with all of dev merged."""
    branch = f"release/{version}"
    _required(repo, "checkout", "-q", "--detach", "refs/remotes/origin/main")
    subject = f"chore(release): merge dev into {branch}"
    if _git(repo, "merge", "-q", "--no-ff", "-m", subject, "refs/remotes/origin/dev").returncode:
        conflicts = _required(repo, "diff", "--name-only", "--diff-filter=U").split()
        _required(repo, "merge", "--abort")
        raise NightRefused(f"merging dev into {branch} conflicts in: {', '.join(conflicts)}")
    _required(repo, "push", "-q", "origin", f"HEAD:refs/heads/{branch}")


def start(repo: Path) -> str | None:
    """The version started tonight, or None when dev has not moved."""
    _required(
        repo, "fetch", "-q", "--prune", "--tags", "origin", "+refs/heads/*:refs/remotes/origin/*"
    )
    last, ref = _last_alpha(repo)
    if not _dev_moved(repo, ref):
        return None
    version = _next(last)
    _make_branch(repo, version)
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
