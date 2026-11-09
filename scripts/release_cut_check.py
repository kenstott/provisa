# Copyright (c) 2026 Kenneth Stott
# Canary: 8830694f-2fb6-4fef-8ba8-25e9c4039dac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Whether a release can be cut at HEAD, and from which release its notes start.

Run by the "Release: cut" workflow (.github/workflows/release-cut.yml) before anything is written.
With ``--ref`` it refuses any ref but the release's own branch, release/<version>. Without it, it
refuses, exiting 2 with the reason:

- a version not shaped as a release tag (v0.1.0, v0.1.0-alpha.480, -beta.N, -rc.N);
- a tag that already exists;
- a docs/releases/<version>.md that someone wrote: the notes generator would overwrite it. A
  page this same cut wrote, on an earlier attempt that did not finish, is its own and is
  written again;
- no earlier release tag to start the notes from;
- an earlier release that is not in HEAD's history. A branch started while that release was
  publishing merges its tag first: its notes would repeat that release, and its merge to main
  would conflict once it is published.

The previous release is the highest release tag below this version, by version and not by what
is reachable: the tag is made on its own release branch.

Run again with ``--written`` after the generator, it refuses a hand-written page under
docs/releases that the generator changed or removed.

On success it prints ``previous=<tag>`` for the workflow's step outputs.
"""

# Requirements: REQ-1989
from __future__ import annotations

import argparse
import re
import subprocess  # nosec B404 - argv-list git invocations only, never shell=True
import sys
from pathlib import Path

VERSION = re.compile(r"^v(\d+)\.(\d+)\.(\d+)(?:-(alpha|beta|rc)\.(\d+))?$")
_STAGES = {"alpha": 0, "beta": 1, "rc": 2, None: 3}  # a pre-release sorts below its release


def _order(tag: str) -> tuple[int, int, int, int, int]:
    """SemVer order of a tag shaped as ``VERSION``."""
    match = VERSION.match(tag)
    assert match is not None, tag
    major, minor, patch, stage, number = match.groups()
    return int(major), int(minor), int(patch), _STAGES[stage], int(number or 0)


def cut_subject(version: str) -> str:
    """The subject of the commit the cut makes: how a page it wrote is told from a hand-written
    one."""
    return f"chore(release): {version} notes"


class CutRefused(Exception):
    pass


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(  # nosec B603 B607 - fixed git argv, no shell
        ["git", "-C", str(repo), *args], capture_output=True, text=True
    )


def check_ref(ref: str, version: str) -> None:
    """A release is cut only from its own branch."""
    own = f"refs/heads/release/{version}"
    if ref != own:
        raise CutRefused(f"a release is cut from its own branch, {own}; this run is on {ref}")


def _previous(repo: Path, version: str) -> str:
    """The highest release tag below ``version``, which HEAD must already hold."""
    tags = _git(repo, "tag", "--list", "v*").stdout.split()
    earlier = [tag for tag in tags if VERSION.match(tag) and _order(tag) < _order(version)]
    if not earlier:
        raise CutRefused(
            f"HEAD has no earlier release tag to start the notes from (below {version})"
        )
    previous = max(earlier, key=_order)
    if _git(repo, "merge-base", "--is-ancestor", previous, "HEAD").returncode != 0:
        raise CutRefused(
            f"the previous release {previous} is not in this branch's history; bring it in with "
            f"`git merge {previous}` and let the suites run on the result"
        )
    return previous


def check(repo: Path, version: str) -> str:
    """The previous release's tag, when ``version`` can be cut at ``repo``'s HEAD."""
    if not VERSION.match(version):
        raise CutRefused(
            f"{version!r} is not a release tag (v0.1.0, or with -alpha.N, -beta.N or -rc.N)"
        )
    if _git(repo, "rev-parse", "-q", "--verify", f"refs/tags/{version}").returncode == 0:
        raise CutRefused(f"the tag {version} already exists")
    page = Path("docs/releases") / f"{version}.md"
    if (repo / page).exists():
        last = _git(repo, "log", "-1", "--format=%s", "--", str(page)).stdout.strip()
        if last != cut_subject(version):
            raise CutRefused(
                f"{page} is already in the tree (last written by {last!r}), and the notes "
                "generator would overwrite it; move or rename the hand-written page first"
            )
    return _previous(repo, version)


def check_written(repo: Path, version: str) -> None:
    """After the generator ran: every page it changed under docs/releases is this release's, or
    one an earlier cut wrote for its own release (the generator drops those of releases before
    the first one shown). A hand-written page it changed or removed refuses the cut by name."""
    changed = _git(repo, "status", "--porcelain", "--", "docs/releases").stdout.splitlines()
    own = f"docs/releases/{version}.md"
    foreign = []
    for line in changed:
        path = line[3:].strip()
        if path == own:
            continue
        stem = Path(path).stem
        last = _git(repo, "log", "-1", "--format=%s", "--", path).stdout.strip()
        if last != cut_subject(stem):
            foreign.append(f"{path} ({line[:2].strip()}; last written by {last!r})")
    if foreign:
        raise CutRefused(
            "the notes generator changed hand-written release pages: " + "; ".join(foreign)
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check a release can be cut at HEAD.")
    parser.add_argument("--version", required=True, help="the release tag to cut")
    parser.add_argument("--repo", type=Path, default=Path("."), help="the repository (default: .)")
    parser.add_argument(
        "--written",
        action="store_true",
        help="after the notes were generated: check no hand-written page was changed",
    )
    parser.add_argument("--ref", help="check only that this ref is the release's own branch")
    args = parser.parse_args(argv)
    try:
        if args.ref:
            check_ref(args.ref, args.version)
            return 0
        if args.written:
            check_written(args.repo, args.version)
            return 0
        previous = check(args.repo, args.version)
    except CutRefused as refused:
        print(f"ERROR: {refused}", file=sys.stderr)
        return 2
    print(f"previous={previous}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
