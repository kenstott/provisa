# Copyright (c) 2026 Kenneth Stott
# Canary: b3d84a17-5e20-4c69-9f03-71ac6e2d8e15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every failing test of a verification, by name, per suite, for the run's job summary.

Reads what the run's jobs uploaded, each artifact under ``<dir>/<artifact>/``: JUnit XML (the unit
shards and the container lanes), the conformance reports (a failing case there is data and does
not fail its job, so this is where it is seen), vitest's and Playwright's JSON reports. A job that
failed and left no failing test is named too: nothing that failed goes unlisted.

Usage: verify_summary.py <artifacts-dir> <jobs.json> [<run-url>]
       (<jobs.json>: the workflow's `needs` context, {job: {"result": ...}})
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lane_report  # noqa: E402


def _junit(artifact: Path) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for junit in sorted(artifact.rglob("*junit.xml")):
        result = lane_report.read_lane(junit.parent.name, junit)
        if result.problems:
            found.setdefault(artifact.name, []).extend(result.problems)
    return found


def _conformance(artifact: Path) -> dict[str, list[str]]:
    found: list[str] = []
    for report in sorted(artifact.glob("conformance-*.json")):
        doc = json.loads(report.read_text())
        # pgwire's report keeps its failures in its parts; every other's are at the top.
        parts = [doc, *(doc[k] for k, v in doc.items() if isinstance(v, dict) and "failures" in v)]
        found += [
            f"FAILED {f['feature']} :: {f['scenario']}: {f.get('reason', '')[:200]}"
            for part in parts
            for f in part.get("failures", ())
        ]
    return {artifact.name: found} if found else {}


def _vitest(artifact: Path) -> dict[str, list[str]]:
    found: list[str] = []
    for report in sorted(artifact.rglob("vitest-results.json")):
        for file in json.loads(report.read_text()).get("testResults", ()):
            failed = [a for a in file.get("assertionResults", ()) if a["status"] == "failed"]
            found += [f"FAILED {file['name']} :: {a['fullName']}" for a in failed]
            # A file that did not load has no assertions to name.
            if file.get("status") == "failed" and not failed:
                found.append(f"FAILED {file['name']}: {(file.get('message') or '')[:200]}")
    return {"ui-unit": found} if found else {}


def _playwright_specs(suite: dict, path: tuple[str, ...]):
    here = (*path, suite["title"])
    for spec in suite.get("specs", ()):
        yield here, spec
    for child in suite.get("suites", ()):
        yield from _playwright_specs(child, here)


def _playwright(artifact: Path) -> dict[str, list[str]]:
    found: list[str] = []
    for report in sorted(artifact.rglob("playwright-results.json")):
        for suite in json.loads(report.read_text()).get("suites", ()):
            for path, spec in _playwright_specs(suite, ()):
                if not spec.get("ok", True):
                    found.append(f"FAILED {' › '.join((*path, spec['title']))}")
    return {artifact.name: found} if found else {}


def failures(artifacts: Path) -> dict[str, list[str]]:
    """{suite: its failing tests}."""
    found: dict[str, list[str]] = {}
    for artifact in sorted(p for p in artifacts.iterdir() if p.is_dir()):
        name = artifact.name
        if name.startswith(("lane-", "unit-junit-")):
            read = _junit(artifact)
        elif name.startswith("conformance-"):
            read = _conformance(artifact)
        elif name.startswith("vitest-results-"):
            read = _vitest(artifact)
        elif name.startswith("playwright-report-"):
            read = _playwright(artifact)
        else:
            continue
        for suite, tests in read.items():
            found.setdefault(suite, []).extend(tests)
    return found


def report(artifacts: Path, jobs: dict[str, dict], run_url: str = "") -> str:
    found = failures(artifacts)
    lines = ["## Verification", "", "| job | result |", "| --- | --- |"]
    lines += [f"| {job} | {body['result']} |" for job, body in sorted(jobs.items())]
    total = sum(len(tests) for tests in found.values())
    lines += ["", f"**{total} failing test(s)**" if total else "No failing test."]
    for suite, tests in sorted(found.items()):
        lines += ["", f"### {suite}: {len(tests)}", *[f"- {t}" for t in tests]]
    red = [j for j, body in sorted(jobs.items()) if body["result"] in ("failure", "cancelled")]
    if red:
        lines += ["", "### Jobs that did not pass", *[f"- {j}" for j in red]]
    if run_url:
        lines += ["", f"Run: {run_url}"]
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    jobs = json.loads(Path(argv[1]).read_text())
    sys.stdout.write(report(Path(argv[0]), jobs, argv[2] if len(argv) > 2 else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
