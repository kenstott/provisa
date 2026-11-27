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

Run by "Release: cut" (.github/workflows/release-cut.yml). A run counts on this commit of this
branch, whatever event started it (a push or a dispatch): failed, cancelled, running or never run is
not green. A workflow with no green run on this commit takes the newest green run of it on this
branch at an ancestor commit A, when every commit in A..this commit is one of (REQ-1989, amended
2026-11-27: a documentation-only change needs no retest, a fix runs only its own targeted tests):

- documentation only (docs/**, *.md, mkdocs.yml, and the release notes the UI bundles,
  provisa-ui/src/generated/releaseNotes.json, which the cut generates with them), or a change to ``ACCEPTS_PATH`` alone, which
  records proof and runs nothing;
- a clean merge, whose own combined diff is empty: the commits it brings in are judged one by one;
- listed in ``ACCEPTS_PATH`` with the targeted runs that proved it, every one of them completed
  successfully on the commit the entry names (the commit itself, a release commit containing it,
  or the fix branch's own commit the release took in).

Anything else leaves the workflow not green. Each accepted run is named in ``--out`` with its
commit and why every later commit is covered; the cut appends that file to the release's notes.

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
import json
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
    "conformance-grpc.yml",
    "conformance-jsonapi.yml",
    "conformance-mcp.yml",
    "conformance-pgwire.yml",
    "conformance-rest.yml",
    "conformance-sparql.yml",
)
_PACKAGING = "build-dmg.yml"

#: The branch the nightly on main pins at main's commit for the night. A dispatch runs on a ref,
#: not a commit, and main moves while the night's lanes start; on this branch every lane tests the
#: same commit, and no push to main cancels one of them.
NIGHTLY_BRANCH = "nightly/main"
#: What runs each night on that one commit, by lane: a release is cut only from a night where
#: every lane is green (REQ-1989, amended 2026-11-22). Packaging is the whole release build as a
#: dry run -- build-dmg.yml's macOS, Windows and Linux AppImage legs, and its install of those
#: assets on a clean Linux host by scripts/install-host.sh, smoke-checked (REQ-2269) -- and the
#: DuckDB extensions.
NIGHTLY_LANES: dict[str, tuple[str, ...]] = {
    "unit": ("unit-tests.yml", "ui-unit-tests.yml"),
    "lint": ("lint.yml",),
    "requirements": ("requirements-ci.yml",),
    "integration": ("integration-suite.yml",),
    "e2e": ("ui-e2e-core.yml", "ui-e2e-trino.yml", "ui-e2e-swap-amd64.yml"),
    "packaging": ("build-dmg.yml", "build-duckdb-extensions.yml"),
}
_GREEN = "completed success "
#: The repository the cut runs in: its history decides which commits an accepted run covers.
ROOT = Path(__file__).resolve().parents[1]
#: Each code commit of a release since its suites last ran, mapped to the targeted runs that
#: proved it: ``{"commits": {<sha>: {"tested_sha": <sha>, "runs": [<id>...], "reason": <text>}}}``.
ACCEPTS_PATH = ".github/release-gate-accepts.json"
#: The cut's own notes commit writes releaseNotes.json beside CHANGELOG.md; a re-cut must not
#: count the previous cut's notes as code (513 re-cut refused on 3fdfbc681b, 2026-11-27).
_DOC_FILES = ("mkdocs.yml", "provisa-ui/src/generated/releaseNotes.json")
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


def _gh_json(argv: list[str]) -> object:
    done = subprocess.run(argv, capture_output=True, text=True)  # nosec B603 B607 - fixed gh argv
    if done.returncode != 0:
        raise LookupFailed(f"{' '.join(argv[:3])}: {done.stderr.strip()}")
    return json.loads(done.stdout)


def successful_runs(workflow: str, branch: str) -> list[dict]:
    """The workflow's successful runs on the branch, newest first."""
    runs = _gh_json(
        [
            "gh",
            "run",
            "list",
            "--workflow",
            workflow,
            "--branch",
            branch,
            "--status",
            "success",
            "--limit",
            "100",
            "--json",
            "databaseId,headSha,url",
        ]
    )
    assert isinstance(runs, list)
    return runs


def run_view(run_id: int) -> dict:
    view = _gh_json(
        ["gh", "run", "view", str(run_id), "--json", "status,conclusion,headSha,url,workflowName"]
    )
    assert isinstance(view, dict)
    return view


def _git(repo: Path, *args: str) -> str:
    done = subprocess.run(  # nosec B603 B607 - fixed git argv, no shell
        ["git", "-C", str(repo), *args], capture_output=True, text=True
    )
    if done.returncode != 0:
        raise LookupFailed(f"git {' '.join(args)}: {done.stderr.strip()}")
    return done.stdout


def _is_ancestor(repo: Path, ancestor: str, sha: str) -> bool:
    """Whether ``ancestor`` is in ``sha``'s history. The cut checks out the whole history, so a
    commit this checkout lacks (a run on a commit since rewritten away) is in no history of it."""
    known = subprocess.run(  # nosec B603 B607 - fixed git argv, no shell
        ["git", "-C", str(repo), "cat-file", "-e", f"{ancestor}^{{commit}}"], capture_output=True
    )
    if known.returncode != 0:
        return False
    done = subprocess.run(  # nosec B603 B607 - fixed git argv, no shell
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", ancestor, sha],
        capture_output=True,
        text=True,
    )
    if done.returncode not in (0, 1):
        raise LookupFailed(f"git merge-base --is-ancestor {ancestor} {sha}: {done.stderr.strip()}")
    return done.returncode == 0


def load_accepts(repo: Path) -> dict[str, dict]:
    return json.loads((repo / ACCEPTS_PATH).read_text())["commits"]


def _is_doc(path: str) -> bool:
    return path.startswith("docs/") or path.endswith(".md") or path in _DOC_FILES


def _changed(repo: Path, commit: str) -> tuple[bool, list[str]]:
    """Whether ``commit`` is a merge, and the files it changes itself: a merge's combined diff,
    which is empty when it only brings in other commits."""
    parents = _git(repo, "rev-list", "--parents", "-n", "1", commit).split()[1:]
    if len(parents) > 1:
        return True, _git(repo, "show", "--cc", "--format=", "--name-only", commit).splitlines()
    files = _git(repo, "diff-tree", "--no-commit-id", "--name-only", "-r", "--root", commit)
    return False, files.splitlines()


def commit_cover(
    repo: Path, commit: str, accepts: dict[str, dict], views: dict[int, dict]
) -> tuple[bool, str]:
    """Whether ``commit`` needs no rerun of a suite that last ran before it, and why (or why not)."""
    merge, files = _changed(repo, commit)
    files = [f for f in files if f]
    if merge and not files:
        return True, "clean merge: the commits it brings in are judged one by one"
    if all(_is_doc(f) for f in files):
        return True, "documentation only"
    if all(_is_doc(f) or f == ACCEPTS_PATH for f in files):
        return True, f"changes only {ACCEPTS_PATH} (and documentation)"
    entry = accepts.get(commit)
    if entry is None:
        return False, f"changes code and is not listed in {ACCEPTS_PATH}"
    tested = entry["tested_sha"]
    if not entry["runs"]:
        return False, f"its {ACCEPTS_PATH} entry lists no runs"
    for run_id in entry["runs"]:
        if run_id not in views:
            views[run_id] = run_view(run_id)
        view = views[run_id]
        if (view["status"], view["conclusion"]) != ("completed", "success"):
            return False, f"targeted run {view['url']} is {view['status']} {view['conclusion']}"
        if view["headSha"] != tested:
            return (
                False,
                f"targeted run {view['url']} ran on {view['headSha'][:10]}, not {tested[:10]}",
            )
    ids = ", ".join(str(r) for r in entry["runs"])
    return True, f"targeted runs {ids} green on {tested[:10]}: {entry['reason']}"


def ancestor_run(
    repo: Path,
    workflow: str,
    branch: str,
    sha: str,
    accepts: dict[str, dict],
    views: dict[int, dict],
) -> tuple[dict | None, str]:
    """The newest green run of ``workflow`` on ``branch`` at a proper ancestor of ``sha`` whose
    later commits all need no rerun, with each commit's reason; or None and why not."""
    for run in successful_runs(workflow, branch):
        base = run["headSha"]
        if base == sha or not _is_ancestor(repo, base, sha):
            continue
        covered = []
        for commit in _git(repo, "rev-list", "--reverse", f"{base}..{sha}").split():
            ok, why = commit_cover(repo, commit, accepts, views)
            if not ok:
                return None, f"newest green run {run['url']} is on {base[:10]}; {commit[:10]} {why}"
            covered.append((commit, why))
        return {"url": run["url"], "sha": base, "commits": covered}, ""
    return None, f"no green run on an ancestor of {sha[:10]} on {branch}"


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


def _accepted_listing(accepted: list[tuple[str, dict]], sha: str) -> str:
    lines = ""
    for workflow, found in accepted:
        lines += f"- `{workflow}`: {found['url']} on `{found['sha'][:10]}`\n"
        lines += "".join(f"  - `{c[:10]}`: {why}\n" for c, why in found["commits"])
    return (
        "\n## Runs accepted from an earlier commit\n\n"
        f"These last ran green on an earlier commit than {sha}; every commit since is covered "
        f"(REQ-1989):\n\n{lines}"
    )


def _judge(
    version: str, branch: str, sha: str
) -> tuple[list[tuple[str, dict]], list[tuple[str, str]]]:
    """Each gated workflow green on ``sha``, accepted from an ancestor, or failed with why."""
    accepted: list[tuple[str, dict]] = []
    failures: list[tuple[str, str]] = []
    accepts: dict[str, dict] | None = None
    views: dict[int, dict] = {}
    for workflow in gates(version):
        state = run_state(workflow, branch, sha)
        if state.startswith(_GREEN):
            continue
        if accepts is None:
            accepts = load_accepts(ROOT)
        found, why = ancestor_run(ROOT, workflow, branch, sha, accepts, views)
        if found is None:
            failures.append((workflow, f"{state}; {why}"))
        else:
            accepted.append((workflow, found))
    return accepted, failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gate a release on its runs.")
    parser.add_argument("--version", help="the release tag being cut, or the next one's number")
    parser.add_argument("--list", action="store_true", help="print the version's workflows")
    parser.add_argument("--branch", help="the release branch")
    parser.add_argument("--sha", help="the commit being cut")
    parser.add_argument(
        "--out", type=Path, help="where accepted runs and an alpha's failures are written"
    )
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
        accepted, failures = _judge(args.version, args.branch, args.sha)
    except LookupFailed as failed:
        print(f"::error::{failed}")
        return 1
    for workflow, found in accepted:
        print(f"::notice::{workflow} accepted from {found['url']} on {found['sha'][:10]}")
        for commit, why in found["commits"]:
            print(f"  {commit[:10]}: {why}")
    accepted_text = _accepted_listing(accepted, args.sha) if accepted else ""
    if not failures:
        if accepted:
            args.out.write_text(accepted_text)
        return 0
    where = f"on {args.branch} at {args.sha}"
    if is_alpha(args.version) and all(workflow != _PACKAGING for workflow, _ in failures):
        for workflow, state in failures:
            print(f"::warning::{workflow} is not green {where}: {state}")
        args.out.write_text(accepted_text + _listing(failures, args.sha))
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
