# Copyright (c) 2026 Kenneth Stott
# Canary: 2718205d-30c4-4fe7-92fe-80daa6385a91
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A PostgreSQL driver's own test suite against Provisa's pgwire, against its expected-failure
list (REQ-2203).

The driver's suite is run as its own CI runs it, pointed at a Provisa test instance, and writes
JUnit XML. Each test is then one of four counts:

* passed: the test passed;
* skipped by design: the test failed, and the driver's list names it with a skipped-by-design
  reason (a requirement and its decision);
* failed: the test failed, and the list names it with an open defect, or does not name it;
* not tested: the suite skipped the test itself (a server feature it gates on, a module not
  installed).

The list is ``drivers/<driver>.yaml``, seeded from a real run, every entry naming its reason:

    driver: pgjdbc
    version: REL42.7.14            # the pinned upstream version the list was seeded against
    reasons:
      writes:                      # a skipped-by-design decision
        requirement: REQ-2203
        decision: Tables are the model's; DDL is refused by name.
      cursor-hold:                 # an open defect
        defect: WITH HOLD cursors answer "portal does not exist".
    expected:
      "org.postgresql.test.jdbc2.BlobTest::testSet": writes

The run fails only when a test not on the list fails (an unexpected failure), or a listed test
passes (an unexpected pass: correct the list), or no test ran at all. A listed test the suite did
not run is reported, never counted.

Log lines, printed after the files are written, so the result is readable from the job log:

    CONFORMANCE-COUNTS surface=driver-<driver> passed=<n> by_design=<n> failed=<n> not_tested=<n> total=<n>
    DRIVER-UNEXPECTED-FAILURE <test>
    DRIVER-UNEXPECTED-PASS <test>

Usage:
    driver_suite.py summarise <driver> <version> <junit.xml or dir>[,...] <out.json> <out.md> <suite>
    driver_suite.py fill-docs <conformance-driver-<driver>.json> [...]
"""

# Requirements: REQ-2203

from __future__ import annotations

import json
import re
import sys
import xml.etree.ElementTree as ET  # nosec B405 -- the XML is the CI job's own test output
from dataclasses import dataclass
from pathlib import Path

import yaml

LISTS = Path(__file__).resolve().parent / "drivers"
DOCS = Path(__file__).resolve().parents[2] / "docs" / "conformance"
PAGE = DOCS / "drivers.md"

#: The four counts every driver's report gives (REQ-2203), each with the label a page gives it.
COUNTS = {
    "passed": "passed",
    "by_design": "skipped by design",
    "failed": "failed",
    "not_tested": "not tested",
}

#: How long a suite's message is kept, per test.
MESSAGE_CHARS = 300


@dataclass(frozen=True)
class Reason:
    """One reason of a driver's list: a skipped-by-design decision citing its requirement, or an
    open defect."""

    key: str
    requirement: str = ""
    decision: str = ""
    defect: str = ""

    @property
    def by_design(self) -> bool:
        return not self.defect


@dataclass(frozen=True)
class Expected:
    driver: str
    version: str
    reasons: dict[str, Reason]
    tests: dict[str, str]  # test id -> reason key


def load(driver: str, lists: Path | None = None) -> Expected:
    """The driver's expected-failure list, refused when an entry has no reason, a reason is
    neither a decision nor a defect, or a reason names no test."""
    path = (lists or LISTS) / f"{driver}.yaml"
    doc = yaml.safe_load(path.read_text()) or {}
    if doc.get("driver") != driver:
        raise ValueError(f"{path.name}: names driver {doc.get('driver')!r}, not {driver!r}")
    if not doc.get("version"):
        raise ValueError(f"{path.name}: names no version it was seeded against")
    reasons: dict[str, Reason] = {}
    for key, body in (doc.get("reasons") or {}).items():
        body = body or {}
        if "defect" in body and ("requirement" in body or "decision" in body):
            raise ValueError(f"{path.name}: reason {key!r} is both a decision and a defect")
        if "defect" in body:
            if not str(body["defect"]).strip():
                raise ValueError(f"{path.name}: defect {key!r} says nothing")
            reasons[key] = Reason(key, defect=str(body["defect"]).strip())
            continue
        if not re.fullmatch(r"REQ-\d+(, REQ-\d+)*", str(body.get("requirement") or "")):
            raise ValueError(
                f"{path.name}: reason {key!r} names no requirement; a failure with no design "
                "decision behind it is an open defect"
            )
        if not str(body.get("decision") or "").strip():
            raise ValueError(f"{path.name}: reason {key!r} states no decision")
        if re.search(r"REQ-\d", str(body["decision"])):
            raise ValueError(
                f"{path.name}: reason {key!r} names a requirement in its decision; a published "
                "page names none, so it goes in requirement"
            )
        reasons[key] = Reason(
            key, requirement=body["requirement"], decision=" ".join(str(body["decision"]).split())
        )
    tests = {str(t): str(r) for t, r in (doc.get("expected") or {}).items()}
    unknown = sorted({r for r in tests.values() if r not in reasons})
    if unknown:
        raise ValueError(f"{path.name}: tests name reasons not declared: {unknown}")
    unused = sorted(set(reasons) - set(tests.values()))
    if unused:
        raise ValueError(f"{path.name}: reasons no test names: {unused}")
    return Expected(driver, str(doc["version"]), reasons, tests)


def _junit_files(spec: str) -> list[Path]:
    """``spec``: JUnit XML files or directories of them, comma-separated (Gradle writes one file
    per test class)."""
    files: list[Path] = []
    for part in filter(None, spec.split(",")):
        path = Path(part)
        files.extend(sorted(path.rglob("*.xml")) if path.is_dir() else [path])
    return files


def read_results(spec: str) -> dict[str, dict]:
    """Each test's outcome: ``{test id: {"outcome": passed|failed|skipped, "message": ...}}``.

    A test id is ``<classname>::<name>``. A test reported more than once (a parameterised name a
    runner repeats) is failed when any report failed, else passed when any passed, else skipped."""
    rank = {"failed": 2, "passed": 1, "skipped": 0}
    results: dict[str, dict] = {}
    for path in _junit_files(spec):
        root = ET.parse(path).getroot()  # nosec B314 -- see above
        for case in root.iter("testcase"):
            test = f"{case.get('classname', '')}::{case.get('name', '')}"
            mark = next((c for c in case if c.tag in ("failure", "error", "skipped")), None)
            if mark is None:
                outcome, message = "passed", ""
            else:
                outcome = "skipped" if mark.tag == "skipped" else "failed"
                lines = (mark.get("message") or mark.text or mark.tag).strip().splitlines()
                message = " ".join((lines[0] if lines else mark.tag).split())[:MESSAGE_CHARS]
            seen = results.get(test)
            if seen is None or rank[outcome] > rank[seen["outcome"]]:
                results[test] = {"outcome": outcome, "message": message}
    return results


def summarise(expected: Expected, version: str, results: dict[str, dict], suite: str) -> dict:
    """The run's four counts against the driver's list, and what the gate reads."""
    if version != expected.version:
        raise ValueError(
            f"{expected.driver}: the run is at {version}, the list was seeded at "
            f"{expected.version}: re-seed the list from a run at the new version"
        )
    by_reason: dict[str, list[dict]] = {k: [] for k in expected.reasons}
    passed = 0
    not_tested: list[dict] = []
    unexpected_failures: list[dict] = []
    unexpected_passes: list[dict] = []
    for test, result in sorted(results.items()):
        listed = expected.tests.get(test)
        if result["outcome"] == "skipped":
            not_tested.append({"test": test, "reason": result["message"]})
        elif result["outcome"] == "passed":
            passed += 1
            if listed is not None:
                unexpected_passes.append({"test": test, "listed": listed})
        elif listed is None:
            unexpected_failures.append({"test": test, "message": result["message"]})
        else:
            by_reason[listed].append({"test": test, "message": result["message"]})
    groups = [
        {
            "reason": r.key,
            "requirement": r.requirement,
            "decision": r.decision,
            "count": len(by_reason[r.key]),
            "tests": by_reason[r.key],
        }
        for r in expected.reasons.values()
        if r.by_design and by_reason[r.key]
    ]
    defects = [
        {
            "reason": r.key,
            "defect": r.defect,
            "count": len(by_reason[r.key]),
            "tests": by_reason[r.key],
        }
        for r in expected.reasons.values()
        if not r.by_design and by_reason[r.key]
    ]
    by_design = sum(g["count"] for g in groups)
    failed = sum(d["count"] for d in defects) + len(unexpected_failures)
    return {
        "surface": f"driver-{expected.driver}",
        "driver": expected.driver,
        "suite": suite,
        "version": version,
        "total": passed + by_design + failed + len(not_tested),
        "passed": passed,
        "by_design": by_design,
        "failed": failed,
        "not_tested": len(not_tested),
        "by_design_groups": groups,
        "defects": defects,
        "unexpected_failures": unexpected_failures,
        "unexpected_passes": unexpected_passes,
        "not_tested_tests": not_tested,
        "listed_not_run": sorted(set(expected.tests) - set(results)),
    }


def counts_line(doc: dict) -> str:
    said = ", ".join(f"{doc[key]} {label}" for key, label in COUNTS.items())
    return f"{said}, of {doc['total']} tests"


def gate_failed(doc: dict) -> bool:
    """Whether the run fails its job: no test ran, a test not on the list failed, or a listed
    test passed (said as workflow errors)."""
    errors = []
    if not doc["total"]:
        errors.append(f"{doc['driver']}: no test ran")
    for f in doc["unexpected_failures"]:
        errors.append(f"{doc['driver']}: {f['test']} failed and is not on the list: {f['message']}")
    for p in doc["unexpected_passes"]:
        errors.append(
            f"{doc['driver']}: {p['test']} passed but is listed ({p['listed']}): correct "
            f"scripts/conformance/drivers/{doc['driver']}.yaml"
        )
    for error in errors:
        print(f"::error::{error}", file=sys.stderr)
    return bool(errors)


def _cell(text: str) -> str:
    return " ".join(str(text).split()).replace("|", "/")


def _details(title: str, rows: list[tuple[str, str]], header: tuple[str, str]) -> list[str]:
    return [
        f"<details><summary>{title} ({len(rows)})</summary>\n",
        f"| {header[0]} | {header[1]} |",
        "| --- | --- |",
        *(f"| {_cell(a)} | {_cell(b)} |" for a, b in rows),
        "\n</details>\n",
    ]


def section(doc: dict, requirement: bool = False) -> str:
    """The driver's results as Markdown: the four counts, then each by-design reason, each open
    defect, the tests the suite did not run, and, for the job's own summary (``requirement``),
    what the gate failed on. A published page names no requirement."""
    out = [
        f"## {doc['driver']}",
        "",
        f"**Suite:** {doc['suite']} {doc['version']}.",
        "",
        f"**Results:** {counts_line(doc)}.",
        "",
        "### Skipped by design",
        "",
    ]
    if not doc["by_design_groups"]:
        out.append("None.")
    else:
        if requirement:
            out += ["| Reason | Count | Requirement | Decision |", "| --- | --- | --- | --- |"]
            out += [
                f"| {g['reason']} | {g['count']} | {g['requirement']} | {_cell(g['decision'])} |"
                for g in doc["by_design_groups"]
            ]
        else:
            out += ["| Reason | Count | Decision |", "| --- | --- | --- |"]
            out += [
                f"| {g['reason']} | {g['count']} | {_cell(g['decision'])} |"
                for g in doc["by_design_groups"]
            ]
        out.append("")
        for g in doc["by_design_groups"]:
            rows = [(t["test"], t["message"]) for t in g["tests"]]
            out += _details(g["reason"], rows, ("Test", "Message"))
    out += ["", "### Failed: open defects", ""]
    if not doc["defects"]:
        out.append("None.")
    else:
        out += ["| Defect | Count | What fails |", "| --- | --- | --- |"]
        out += [f"| {d['reason']} | {d['count']} | {_cell(d['defect'])} |" for d in doc["defects"]]
        out.append("")
        for d in doc["defects"]:
            rows = [(t["test"], t["message"]) for t in d["tests"]]
            out += _details(d["reason"], rows, ("Test", "Message"))
    out += ["", "### Not tested", ""]
    if not doc["not_tested_tests"]:
        out.append("None.")
    else:
        rows = [(t["test"], t["reason"]) for t in doc["not_tested_tests"]]
        out += _details("Skipped by the suite", rows, ("Test", "Suite's reason"))
    if requirement:
        out += ["", "### Unexpected", ""]
        out += [
            f"- failed, not listed: {f['test']}: {f['message']}" for f in doc["unexpected_failures"]
        ]
        out += [f"- passed, listed as {p['listed']}: {p['test']}" for p in doc["unexpected_passes"]]
        out += [f"- listed, not run: {t}" for t in doc["listed_not_run"]]
        if not (doc["unexpected_failures"] or doc["unexpected_passes"] or doc["listed_not_run"]):
            out.append("None.")
    return "\n".join(out) + "\n"


def log_lines(doc: dict) -> list[str]:
    return [
        f"CONFORMANCE-COUNTS surface={doc['surface']} passed={doc['passed']} "
        f"by_design={doc['by_design']} failed={doc['failed']} not_tested={doc['not_tested']} "
        f"total={doc['total']}",
        *(f"DRIVER-UNEXPECTED-FAILURE {f['test']}" for f in doc["unexpected_failures"]),
        *(f"DRIVER-UNEXPECTED-PASS {p['test']}" for p in doc["unexpected_passes"]),
    ]


def _markers(driver: str) -> tuple[str, str]:
    return f"<!-- driver-results:{driver}:begin -->", f"<!-- driver-results:{driver}:end -->"


def fill(page: str, doc: dict) -> str:
    """``page`` with the driver's section between its markers, which the page must have."""
    begin, end = _markers(doc["driver"])
    if begin not in page or end not in page:
        raise ValueError(f"the page has no {doc['driver']} markers to put the results between")
    start, stop = page.index(begin), page.index(end) + len(end)
    return page[:start] + f"{begin}\n{section(doc)}{end}" + page[stop:]


def main(argv: list[str]) -> int:
    command, *args = argv
    if command == "summarise":
        driver, version, junit, out_json, out_md, suite = args
        doc = summarise(load(driver), version, read_results(junit), suite)
        Path(out_json).write_text(json.dumps(doc, indent=2) + "\n")
        Path(out_md).write_text(section(doc, requirement=True))
        print("\n".join(log_lines(doc)), flush=True)
        return 1 if gate_failed(doc) else 0
    if command == "fill-docs":
        for path in args:
            doc = json.loads(Path(path).read_text())
            PAGE.write_text(fill(PAGE.read_text(), doc))
            print(f"{PAGE}: {doc['driver']}: {counts_line(doc)}")
        return 0
    raise SystemExit(f"unknown command {command!r}: summarise or fill-docs")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
