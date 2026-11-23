# Copyright (c) 2026 Kenneth Stott
# Canary: f2f80702-ab2a-469e-ab5c-bcf5abf88b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The release gate of a cut (REQ-1989): which runs a release needs on its commit, and what a
run that is not green does to it.

Run by "Release: cut" (.github/workflows/release-cut.yml). A run counts only on this commit of
this branch, whatever event started it (a push or a dispatch): failed, cancelled, running or never run is not green.

- A beta or production release (any version without ``-alpha.``) ships at zero bugs: one run not
  green refuses the cut, by name, exiting 2.
- An alpha ships at the end of its time box with what is left listed: each run not green is
  reported and written to ``--out``, which the cut appends to the release's notes. Its dry-run
  build is the exception: the tag is made only after packaging, so an alpha that did not package
  is refused like any other release.

With ``--list`` it prints the workflows of the version, one a line: the nightly starts those.

The nightly on main (REQ-1989, amended 2026-11-22) is gated here too. ``--nightly-dispatch``
starts every lane of ``NIGHTLY_LANES`` on ``nightly/main``, the branch .github/workflows/
nightly-main.yml pins at main's commit for the night, the packaging dry run built for the next
release's number and publishing nothing. ``--ready --sha <commit>`` is the release-ready summary
(.github/workflows/release-ready.yml): it prints each lane's runs on that one commit and exits 0
only when every one is green, naming the commit a release may be cut from; anything else exits 1.
"""

# Requirements: REQ-1989
from __future__ import annotations

import argparse
import subprocess  # nosec B404 - argv-list gh invocations only, never shell=True
import sys
from pathlib import Path

_CORE = (
    "unit-tests.yml",
    "ui-unit-tests.yml",
    "lint.yml",
    "cve-audit.yml",
    "docker-image.yml",
    "build-dmg.yml",
)
# What a release branch runs on dispatch (a push to it starts only lint and requirements-ci). Beta
# and production need them green; an alpha lists what is left of them.
_FULL_SUITES = (
    "integration-suite.yml",
    "ui-e2e-core.yml",
    "ui-e2e-trino.yml",
    "ui-e2e-swap-amd64.yml",
    "conformance-bolt.yml",
    "conformance-cypher.yml",
    "conformance-drivers.yml",
    "conformance-flight-sql.yml",
    "conformance-functions.yml",
    "conformance-graphql.yml",
    "conformance-pgwire.yml",
    "conformance-sparql.yml",
)
_PACKAGING = "build-dmg.yml"

#: The branch the nightly on main pins at main's commit for the night. A dispatch runs on a ref,
#: not a commit, and main moves while the night's lanes start; on this branch every lane tests the
#: same commit, and no push to main cancels one of them.
NIGHTLY_BRANCH = "nightly/main"
#: What runs each night on that one commit, by lane: a release is cut only from a night where
#: every lane is green (REQ-1989, amended 2026-11-22). Packaging is the whole release build as a
#: dry run -- build-dmg.yml's macOS, Windows and Linux AppImage legs -- and the DuckDB extensions.
NIGHTLY_LANES: dict[str, tuple[str, ...]] = {
    "unit": ("unit-tests.yml", "ui-unit-tests.yml"),
    "lint": ("lint.yml",),
    "requirements": ("requirements-ci.yml",),
    "integration": ("integration-suite.yml",),
    "e2e": ("ui-e2e-core.yml", "ui-e2e-trino.yml", "ui-e2e-swap-amd64.yml"),
    "packaging": ("build-dmg.yml", "build-duckdb-extensions.yml"),
}
_GREEN = "completed success "
_JQ = '.[0] // {} | "\\(.status // "never run") \\(.conclusion // "") \\(.url // "")"'


class LookupFailed(Exception):
    pass


def is_alpha(version: str) -> bool:
    return "-alpha." in version


def gates(version: str) -> tuple[str, ...]:
    """The workflows ``version`` is gated on. requirements-ci is out of an alpha's: it is red on
    requirements that lack a unit test, which is no defect of the alpha being shipped."""
    if is_alpha(version):
        return _CORE + _FULL_SUITES
    return _CORE + _FULL_SUITES + ("requirements-ci.yml",)


def run_state(workflow: str, branch: str, sha: str) -> str:
    """``<status> <conclusion> <url>`` of the workflow's latest run on this commit of the branch."""
    done = subprocess.run(  # nosec B603 B607 - fixed gh argv, no shell
        [
            "gh",
            "run",
            "list",
            "--workflow",
            workflow,
            "--branch",
            branch,
            "--commit",
            sha,
            "--limit",
            "1",
            "--json",
            "status,conclusion,url",
            "--jq",
            _JQ,
        ],
        capture_output=True,
        text=True,
    )
    if done.returncode != 0:
        raise LookupFailed(f"gh run list --workflow {workflow}: {done.stderr.strip()}")
    return done.stdout.strip()


def _dispatch_inputs(workflow: str, version: str) -> list[str]:
    """A packaging workflow is built for ``version`` and publishes nothing; no other takes input."""
    if workflow == "build-dmg.yml":
        return ["-f", f"version={version}", "-f", "publish=false"]
    if workflow == "build-duckdb-extensions.yml":
        return ["-f", "publish=false"]
    return []


def nightly_dispatches(version: str) -> list[list[str]]:
    """The ``gh`` argv starting each nightly lane's workflows on the pinned branch."""
    return [
        [
            "gh",
            "workflow",
            "run",
            workflow,
            "--ref",
            NIGHTLY_BRANCH,
            *_dispatch_inputs(workflow, version),
        ]
        for workflows in NIGHTLY_LANES.values()
        for workflow in workflows
    ]


def dispatch_nightly(version: str) -> int:
    started = 0
    for argv in nightly_dispatches(version):
        done = subprocess.run(argv, capture_output=True, text=True)  # nosec B603 B607 - fixed gh argv
        if done.returncode != 0:
            print(f"::error::{' '.join(argv)}: {done.stderr.strip()}")
            return 1
        started += 1
    print(f"started {started} workflows on {NIGHTLY_BRANCH} for {version}")
    return 0


def nightly_ready(sha: str) -> tuple[bool, list[str]]:
    """Whether every nightly lane is green on ``sha`` of the pinned branch, and a table of them."""
    rows = ["| lane | workflow | run |", "| --- | --- | --- |"]
    green = True
    for lane, workflows in NIGHTLY_LANES.items():
        for workflow in workflows:
            state = run_state(workflow, NIGHTLY_BRANCH, sha)
            green = green and state.startswith(_GREEN)
            rows.append(f"| {lane} | `{workflow}` | {state} |")
    return green, rows


def ready(sha: str) -> int:
    try:
        green, rows = nightly_ready(sha)
    except LookupFailed as failed:
        print(f"::error::{failed}")
        return 1
    if green:
        print(f"## Release-ready: {sha}\n\nEvery nightly lane is green on this main commit.\n")
    else:
        print(f"## Not release-ready: {sha}\n\nA nightly lane is not green on this commit.\n")
    print("\n".join(rows))
    return 0 if green else 1


def _listing(failures: list[tuple[str, str]], sha: str) -> str:
    lines = "".join(f"- `{workflow}`: {state}\n" for workflow, state in failures)
    return f"\n## Known failures\n\nThis alpha shipped with these not green on {sha}:\n\n{lines}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gate a release on its runs.")
    parser.add_argument("--version", help="the release tag being cut, or the next one's number")
    parser.add_argument("--list", action="store_true", help="print the version's workflows")
    parser.add_argument("--branch", help="the release branch")
    parser.add_argument("--sha", help="the commit being cut")
    parser.add_argument("--out", type=Path, help="where an alpha's failures are written")
    parser.add_argument(
        "--nightly-dispatch", action="store_true", help="start the nightly lanes on main's pin"
    )
    parser.add_argument("--ready", action="store_true", help="every nightly lane green on --sha")
    args = parser.parse_args(argv)
    if args.ready:
        if not args.sha:
            parser.error("--ready needs --sha")
        return ready(args.sha)
    if not args.version:
        parser.error("--version is required unless --ready is given")
    if args.nightly_dispatch:
        return dispatch_nightly(args.version)
    if args.list:
        print("\n".join(gates(args.version)))
        return 0
    if not (args.branch and args.sha and args.out):
        parser.error("--branch, --sha and --out are required unless --list is given")
    try:
        states = [(w, run_state(w, args.branch, args.sha)) for w in gates(args.version)]
    except LookupFailed as failed:
        print(f"::error::{failed}")
        return 1
    failures = [(w, state) for w, state in states if not state.startswith(_GREEN)]
    if not failures:
        return 0
    where = f"on {args.branch} at {args.sha}"
    if is_alpha(args.version) and all(workflow != _PACKAGING for workflow, _ in failures):
        for workflow, state in failures:
            print(f"::warning::{workflow} is not green {where}: {state}")
        args.out.write_text(_listing(failures, args.sha))
        return 0
    for workflow, state in failures:
        print(f"::error::{workflow} is not green {where}: {state}")
    print(
        f"A dry run is started from Actions -> Build Provisa Packages on {args.branch}, "
        f"version {args.version}, publish unchecked."
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
