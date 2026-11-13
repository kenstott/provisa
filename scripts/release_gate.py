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
)
_PACKAGING = "build-dmg.yml"
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


def _listing(failures: list[tuple[str, str]], sha: str) -> str:
    lines = "".join(f"- `{workflow}`: {state}\n" for workflow, state in failures)
    return f"\n## Known failures\n\nThis alpha shipped with these not green on {sha}:\n\n{lines}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gate a release on its runs.")
    parser.add_argument("--version", required=True, help="the release tag being cut")
    parser.add_argument("--list", action="store_true", help="print the version's workflows")
    parser.add_argument("--branch", help="the release branch")
    parser.add_argument("--sha", help="the commit being cut")
    parser.add_argument("--out", type=Path, help="where an alpha's failures are written")
    args = parser.parse_args(argv)
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
