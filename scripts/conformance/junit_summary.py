# Copyright (c) 2026 Kenneth Stott
# Canary: 1c7e4a92-8d35-4b60-a2f9-5e3b8d6c1a74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A third-party test suite's JUnit XML, as a conformance report (REQ-2167).

For a suite run with pytest (``--junitxml``) against a Provisa endpoint -- a driver's own
integration tests -- writes the JSON the conformance pages are filled from
(scripts/conformance/fill_docs.py) and the same as Markdown for the job summary. Each test is
passed, skipped by design or failed (REQ-2167, amended 2026-10-28); a test the suite skips is
failed unless a by-design group names it. A failing test is data. The exit status is non-zero when
the run has no test, and, for a surface at its baseline (by_design.AT_BASELINE), when a test
failed.

Usage: junit_summary.py <junit.xml>[,<junit.xml>...] <surface> <suite> <version> <tests run> <out.json> <out.md>
"""

# Requirements: REQ-2167

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET  # nosec B405 -- the XML is the CI job's own pytest output
from pathlib import Path

import by_design


def summarise(junit: str, surface: str, suite: str, version: str, tests: str) -> dict:
    """``junit``: the run's junit files, comma-separated -- one per pytest call of the run."""
    cases = [
        case
        for path in junit.split(",")
        for case in ET.parse(path).getroot().iter("testcase")  # nosec B314 -- see above
    ]
    skipped: list[dict] = []
    outcomes: list[dict] = []
    for case in cases:
        outcome = next((c for c in case if c.tag in ("failure", "error", "skipped")), None)
        if outcome is not None and outcome.tag == "skipped":
            # Named with the suite's reason, so what it did not run is decided case by case.
            skipped.append(
                {
                    "feature": case.get("classname", ""),
                    "scenario": case.get("name", ""),
                    "reason": (outcome.get("message") or outcome.text or "").strip()[:300],
                }
            )
            continue
        message = (
            []
            if outcome is None
            else ((outcome.get("message") or outcome.text or "").strip().splitlines())
        )
        outcomes.append(
            {
                "feature": case.get("classname", ""),
                "scenario": case.get("name", ""),
                "reason": "" if outcome is None else (message[0] if message else outcome.tag)[:300],
                "outcome": "passed" if outcome is None else "failed",
            }
        )
    passed = sum(o["outcome"] == "passed" for o in outcomes)
    failures = [
        {k: o[k] for k in ("feature", "scenario", "reason")}
        for o in outcomes
        if o["outcome"] == "failed"
    ]
    declared = by_design.groups(surface)
    # REQ-2167 (amended 2026-10-27 and 2026-10-28): every case is passed, skipped by design (a
    # group of by_design/<surface>.yaml citing its requirement and decision), or failed. A case
    # the suite skipped did not pass: it is skipped by design when a group names the decision
    # behind it (matched on the suite's own reason), and failed otherwise, saying the suite
    # skipped it. A client group says why some failed cases failed -- the driver did not send
    # what they need -- and they stay failed; an adapted case counts, marked with what its setup
    # became.
    failures, excluded = by_design.classify(failures + skipped, declared)
    suite_skipped = {(c["feature"], c["scenario"]) for c in skipped}
    failures = [
        {**f, "reason": f"skipped by the suite: {f['reason']}"}
        if (f["feature"], f["scenario"]) in suite_skipped
        else f
        for f in failures
    ]
    return {
        "surface": surface,
        "suite": suite,
        "version": version,
        "tests": tests,
        **by_design.outcomes(passed, excluded, failures),
        "not_sent_by_client": by_design.not_sent(failures, declared),
        "adapted": by_design.adapted(outcomes, declared),
    }


def markdown(doc: dict) -> str:
    return (
        "\n".join(
            [
                f"## {doc['surface']} conformance: {doc['suite']} {doc['version']}",
                "",
                f"Tests: {doc['tests']}. {by_design.counts_line(doc)}",
                "",
                "### Skipped by design",
                "",
                *by_design.table(doc["by_design_groups"], requirement=True),
                "",
                "### Failed: not sent by the client",
                "",
                *by_design.not_sent_table(doc["not_sent_by_client"]),
                "",
                "### Adapted",
                "",
                *by_design.adapted_lines(doc["adapted"]),
                "",
                "### Failed",
                "",
                "| Test | Case | Reason |",
                "| --- | --- | --- |",
                *(
                    f"| {f['feature']} | {f['scenario']} | {f['reason'].replace('|', '/')} |"
                    for f in doc["failures"]
                ),
            ]
        )
        + "\n"
    )


def main(argv: list[str]) -> int:
    junit, surface, suite, version, tests, out_json, out_md = argv
    doc = summarise(junit, surface, suite, version, tests)
    Path(out_json).write_text(json.dumps(doc, indent=2) + "\n")
    Path(out_md).write_text(markdown(doc))
    if not doc["total"]:
        return 1
    # REQ-2167: at its baseline, a failed case fails the job.
    return 1 if by_design.baseline_failed(surface, doc["failed"]) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
