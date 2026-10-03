# Copyright (c) 2026 Kenneth Stott
# Canary: 8571ed37-30ed-4769-a287-b60c9c8cf963
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One table of results for a CI run of the container-backed suite, a row per lane.

Reads every lane's JUnit XML (``<dir>/<lane>/junit.xml``, as each job uploads it) and writes a
Markdown table: lane, passed, failed, errors, skipped, and the first line of each failure, so a
failure can be routed without opening every job. A skip counts against its lane: a lane with any
skip is not green. Exits non-zero when any lane is not green.

Usage: lane_report.py <artifacts-dir> [<run-url>]
"""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET  # noqa: S405 - parsing our own jobs' JUnit output
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class LaneResult:
    lane: str
    passed: int = 0
    failed: int = 0
    errors: int = 0
    skipped: int = 0
    problems: list[str] = field(default_factory=list)

    @property
    def green(self) -> bool:
        return self.failed == 0 and self.errors == 0 and self.skipped == 0


def read_lane(lane: str, junit: Path) -> LaneResult:
    result = LaneResult(lane)
    root = ET.parse(junit).getroot()  # noqa: S314 - our own jobs' JUnit output
    for case in root.iter("testcase"):
        name = f"{case.get('classname')}::{case.get('name')}"
        outcome = next((c for c in case if c.tag in ("failure", "error", "skipped")), None)
        if outcome is None:
            result.passed += 1
            continue
        message = (outcome.get("message") or (outcome.text or "")).strip().splitlines()
        first = message[0][:200] if message else ""
        if outcome.tag == "failure":
            result.failed += 1
        elif outcome.tag == "error":
            result.errors += 1
        else:
            result.skipped += 1
        result.problems.append(f"{outcome.tag.upper()} {name}: {first}")
    return result


def report(artifacts: Path, run_url: str = "") -> tuple[str, bool]:
    lanes = sorted(p for p in artifacts.iterdir() if (p / "junit.xml").is_file())
    results = [read_lane(p.name, p / "junit.xml") for p in lanes]
    lines = [
        "| lane | passed | failed | errors | skipped | |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for r in results:
        mark = "ok" if r.green else "**not green**"
        lines.append(f"| {r.lane} | {r.passed} | {r.failed} | {r.errors} | {r.skipped} | {mark} |")
    for r in results:
        if r.problems:
            lines += ["", f"### {r.lane}", *[f"- {p}" for p in r.problems[:50]]]
            if len(r.problems) > 50:
                lines.append(f"- ... and {len(r.problems) - 50} more")
    if run_url:
        lines += ["", f"Run: {run_url}"]
    return "\n".join(lines) + "\n", all(r.green for r in results) and bool(results)


def main(argv: list[str]) -> int:
    text, green = report(Path(argv[0]), argv[1] if len(argv) > 1 else "")
    sys.stdout.write(text)
    return 0 if green else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
