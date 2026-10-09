#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: eff1e93e-7dd2-4024-96b9-365f960ac371
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Name every test a Playwright lane did NOT execute (lane.yml `after`).

A skipped test is not a passed one. Playwright's summary counts skips in one number beside the
passes; this reads the run's JSON report and lists each test that did not run by name, with the
reason its skip gave, in the log and in the job's summary, under its own count: NOT EXECUTED.

    playwright_not_executed.py <report.json>

Exits 0: the list is a report, not a verdict (the lane's own result stands). A report that is
missing or unreadable is an error -- an absent list must not read as "none".
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _tests(suite: dict, path: tuple[str, ...] = ()):
    title = suite.get("title") or ""
    here = (*path, title) if title else path
    for spec in suite.get("specs") or []:
        for test in spec.get("tests") or []:
            yield (*here, spec["title"]), spec.get("file") or suite.get("file") or "", test
    for child in suite.get("suites") or []:
        yield from _tests(child, here)


def not_executed(report: dict) -> list[tuple[str, str]]:
    """(test, reason) for every test that did not run: skipped by its own condition, or never
    started (a serial group whose earlier test failed)."""
    found = []
    for suite in report.get("suites") or []:
        for titles, file, test in _tests(suite):
            results = test.get("results") or []
            skipped = test.get("status") == "skipped" or (
                bool(results) and all(r.get("status") == "skipped" for r in results)
            )
            if results and not skipped:
                continue
            reasons = [
                a.get("description") or ""
                for a in test.get("annotations") or []
                if a.get("type") in ("skip", "fixme")
            ]
            if not results:
                reasons.append("did not run")
            name = " › ".join(t for t in (file, *(t for t in titles if t != file)) if t)
            reason = "; ".join(r for r in reasons if r) or "no reason given"
            found.append((name, reason))
    return sorted(found)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    report = json.loads(Path(argv[1]).read_text())
    missed = not_executed(report)
    lines = [f"NOT EXECUTED: {len(missed)} test(s)"]
    lines += [f"- {name} -- {reason}" for name, reason in missed]
    print("\n".join(lines))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as out:
            out.write(f"### Not executed: {len(missed)}\n\n")
            out.write("These tests did not run. They are not counted as passed.\n\n")
            out.writelines(f"- `{name}` -- {reason}\n" for name, reason in missed)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
