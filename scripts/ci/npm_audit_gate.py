# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The npm audit gate for the UI's packages (cve-audit.yml's npm-audit job).

``npm_audit_gate.py PACKAGE_DIR...`` audits each package's lockfile twice: everything, and with
``--omit=dev``. A high or critical advisory fails the run:

- reached through a production dependency, it always fails. ALLOW does not apply to it;
- reached only through dev dependencies, it fails unless ALLOW names it with the reason it
  cannot be fixed yet.

An ALLOW entry that matches no dev-only advisory fails too, so the list never outlives the
advisories it excuses. Moderate and low advisories are reported by ``npm audit`` itself and do
not fail the gate.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

FAILING = frozenset({"high", "critical"})

# Dev-only advisories that cannot be fixed yet: GHSA id -> why (which package carries it, and
# what has to be released upstream before it can be fixed). Reviewed by
# tests/unit/test_ci_workflow_structure.py. A production advisory never belongs here.
ALLOW: dict[str, str] = {}


def audit(package_dir: Path, omit_dev: bool) -> dict:
    cmd = ["npm", "audit", "--json", *(["--omit=dev"] if omit_dev else [])]
    run = subprocess.run(cmd, cwd=package_dir, capture_output=True, text=True, check=False)
    report = json.loads(run.stdout)
    # npm audit exits 1 both when it finds advisories and when the audit itself failed (no
    # lockfile, registry unreachable); only the report says which.
    if "error" in report or "vulnerabilities" not in report:
        raise SystemExit(f"npm audit could not audit {package_dir}: {run.stdout}{run.stderr}")
    return report


def failing_advisories(report: dict) -> dict[str, str]:
    """GHSA id -> 'package severity title' for every high or critical advisory in a report."""
    found: dict[str, str] = {}
    for vuln in report["vulnerabilities"].values():
        for via in vuln["via"]:
            if isinstance(via, dict) and via["severity"] in FAILING:
                ghsa = via["url"].rstrip("/").rsplit("/", 1)[-1]
                found[ghsa] = f"{via['name']} {via['severity']}: {via['title']}"
    return found


def violations(reports: list[tuple[str, dict, dict]], allow: dict[str, str]) -> list[str]:
    """Each report is (package dir, full audit, --omit=dev audit)."""
    problems: list[str] = []
    dev_only: set[str] = set()
    for package, full, prod in reports:
        prod_found = failing_advisories(prod)
        for ghsa, what in sorted(prod_found.items()):
            problems.append(f"{package}: production {ghsa} {what}")
        for ghsa, what in sorted(failing_advisories(full).items()):
            if ghsa in prod_found:
                continue
            dev_only.add(ghsa)
            if ghsa not in allow:
                problems.append(f"{package}: dev-only {ghsa} {what} (not in ALLOW)")
    for ghsa in sorted(set(allow) - dev_only):
        problems.append(f"ALLOW names {ghsa}, which no dev-only advisory matches: remove it")
    return problems


def main(argv: list[str]) -> int:
    if not argv:
        raise SystemExit("usage: npm_audit_gate.py PACKAGE_DIR...")
    reports = [(d, audit(Path(d), False), audit(Path(d), True)) for d in argv]
    problems = violations(reports, ALLOW)
    for package, full, _ in reports:
        print(f"{package}: {full['metadata']['vulnerabilities']}")
    for problem in problems:
        print(f"FAIL {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
