# Copyright (c) 2026 Kenneth Stott
# Canary: 3c82de67-5f6e-435c-8644-04d9a710bc42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pgwire conformance run's results: pgspec's `go test -json` stream, summarised (REQ-2167).

Reads the event stream of one or more `go test -json -tags pgspec` invocations and pgspec's own
source (pgspec_test.go), and writes:

* a JSON document (total, passed, failed, skipped, pass rate, and every failed test grouped by
  pgspec section), uploaded as the run's ``conformance-pgwire.json``;
* the same as Markdown, for the job summary.

A test is a leaf: a subtest, or a top-level test that has none. The leaves are those the source
declares (every ``t.Run("Name"`` inside a ``func TestX``) together with every one the stream
reports, so a test the binary never reached -- it exited on an earlier test's panic -- is counted
as failed and named "not reached" rather than missing from the total. The section of a test is the
``// Section N: Title`` comment its top-level function sits under in the source.

The exit status is non-zero when the suite did not run (no leaf passed or failed), and, while
pgwire is at its baseline (by_design.AT_BASELINE), when a test failed that no by-design group of
the pgwire surface excludes (by_design/pgwire.yaml): a regression. A test failed by
design is left out of the counts, in the group naming its decision.

Usage: pgspec_summary.py <pgspec_test.go> <results.jsonl> <out.json> <out.md> [<version>]
"""

# Requirements: REQ-2167

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import by_design

_SECTION = re.compile(r"^//\s*Section\s+(\d+):\s*(.+?)\s*$")
_FUNC = re.compile(r"^func\s+(Test\w+)\s*\(\s*t\s+\*testing\.T\s*\)")
_RUN = re.compile(r"\bt\.Run\(\s*\"([^\"]+)\"")
_TERMINAL = {"pass": "passed", "fail": "failed", "skip": "skipped"}


@dataclass
class Declared:
    """What pgspec's source declares: each top-level test's section and its subtests."""

    section_of: dict[str, str] = field(default_factory=dict)
    subtests: dict[str, list[str]] = field(default_factory=dict)

    def leaves(self) -> list[str]:
        out: list[str] = []
        for top, subs in self.subtests.items():
            if subs:
                out.extend(f"{top}/{s.replace(' ', '_')}" for s in subs)
            else:
                out.append(top)
        return out


def declared_tests(source: str) -> Declared:
    declared = Declared()
    section = "Unsectioned"
    current: str | None = None
    for line in source.splitlines():
        if m := _SECTION.match(line):
            section = f"Section {m.group(1)}: {m.group(2)}"
            continue
        if m := _FUNC.match(line):
            name: str = m.group(1)
            declared.section_of[name] = section
            declared.subtests[name] = []
            current = name
            continue
        if line.startswith("func "):
            current = None
            continue
        if current is not None and (m := _RUN.search(line)):
            declared.subtests[current].append(m.group(1))
    return declared


def read_events(path: Path) -> list[dict]:
    events = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line.startswith("{"):
            events.append(json.loads(line))
    return events


def outcomes(events: list[dict]) -> tuple[dict[str, str], dict[str, list[str]], set[str]]:
    """Each test's terminal outcome, its output lines, and every test name the stream ran."""
    outcome: dict[str, str] = {}
    output: dict[str, list[str]] = {}
    seen: set[str] = set()
    for ev in events:
        name = ev.get("Test")
        if not name:
            continue
        seen.add(name)
        action = ev.get("Action")
        if action == "output":
            output.setdefault(name, []).append(ev.get("Output", ""))
        elif action in _TERMINAL:
            outcome[name] = _TERMINAL[action]
    return outcome, output, seen


def _first_failure_line(lines: list[str]) -> str:
    for line in lines:
        text = line.strip()
        if not text or text.startswith(("=== ", "--- ")):
            continue
        return text[:300]
    return ""


def summarise(source: str, events: list[dict], version: str = "") -> dict:
    declared = declared_tests(source)
    outcome, output, seen = outcomes(events)
    leaves = set(declared.leaves())
    # A top-level test with declared subtests is never itself a leaf, even when the stream saw
    # none of its subtests (it failed before the first one): those subtests count instead.
    leaves |= {
        n
        for n in seen
        if not declared.subtests.get(n) and not any(o.startswith(n + "/") for o in seen)
    }
    counts = {"passed": 0, "failed": 0, "skipped": 0}
    failed_by_section: dict[str, list[dict]] = {}
    not_reached: list[str] = []
    declared_groups = by_design.groups("pgwire")
    excluded: list[dict] = []
    for name in sorted(leaves):
        result = outcome.get(name)
        if result is None:
            result = "failed"
            not_reached.append(name)
            reason = "not reached (the test binary exited before it ran)"
        else:
            reason = _first_failure_line(output.get(name, [])) if result == "failed" else ""
        top = name.split("/", 1)[0]
        if result == "failed":
            failure = {"feature": top, "scenario": name.partition("/")[2], "reason": reason}
            left, by_design_hit = by_design.classify([failure], declared_groups)
            if not left:
                excluded = _merged(excluded, by_design_hit)
                continue
            section = declared.section_of.get(top, "Unsectioned")
            failed_by_section.setdefault(section, []).append({"test": name, "reason": reason})
        counts[result] += 1
    total = sum(counts.values())
    return {
        "suite": "pgspec",
        "module": "github.com/goceleris/celeris/test/pgspec",
        "version": version,
        "total": total,
        "passed": counts["passed"],
        "failed": counts["failed"],
        "skipped": counts["skipped"],
        "not_reached": not_reached,
        "pass_rate": round(counts["passed"] / total, 4) if total else 0.0,
        "not_runnable": sum(g["count"] for g in excluded),
        "by_design": excluded,
        "failed_by_section": dict(sorted(failed_by_section.items(), key=_section_key)),
    }


def _merged(excluded: list[dict], hits: list[dict]) -> list[dict]:
    """The by-design groups with one more case each of ``hits`` excluded."""
    by_name = {g["group"]: dict(g) for g in excluded}
    for hit in hits:
        kept = by_name.setdefault(hit["group"], {**hit, "count": 0})
        kept["count"] += 1
    return list(by_name.values())


def _section_key(item: tuple[str, list]) -> tuple[int, str]:
    m = re.match(r"Section (\d+)", item[0])
    return (int(m.group(1)) if m else 10**6, item[0])


def markdown(summary: dict) -> str:
    rate = f"{summary['pass_rate'] * 100:.1f}%"
    lines = [
        f"## pgwire conformance: pgspec {summary['version']}".rstrip(),
        "",
        "| total | passed | failed | skipped | pass rate |",
        "|---:|---:|---:|---:|---:|",
        f"| {summary['total']} | {summary['passed']} | {summary['failed']} "
        f"| {summary['skipped']} | {rate} |",
        "",
    ]
    if summary["by_design"]:
        lines += [
            f"Not runnable by design: {summary['not_runnable']}.",
            "",
            *by_design.table(summary["by_design"]),
            "",
        ]
    for section, failures in summary["failed_by_section"].items():
        lines.append(f"### {section} ({len(failures)} failed)")
        lines.append("")
        for f in failures:
            reason = f["reason"].replace("|", "\\|")
            lines.append(f"- `{f['test']}`" + (f": {reason}" if reason else ""))
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    if len(argv) not in (5, 6):
        print(__doc__, file=sys.stderr)
        return 2
    source, results, out_json, out_md = (Path(a) for a in argv[1:5])
    version = argv[5] if len(argv) == 6 else ""
    events = read_events(results) if results.is_file() else []
    summary = summarise(source.read_text(), events, version)
    out_json.write_text(json.dumps(summary, indent=2) + "\n")
    out_md.write_text(markdown(summary) + "\n")
    print(markdown(summary))
    if summary["passed"] + summary["failed"] - len(summary["not_reached"]) == 0:
        print("::error::pgspec did not run: no test passed or failed", file=sys.stderr)
        return 1
    # REQ-2167: at its baseline, a failure no by-design group excludes fails the job (each is
    # named in the summary above).
    return 1 if by_design.baseline_failed("pgwire", summary["failed"]) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
