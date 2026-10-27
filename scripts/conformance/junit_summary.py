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
(scripts/conformance/fill_docs.py) and the same as Markdown for the job summary. A failing test
is data; a skipped one is counted apart. The exit status is non-zero only when no test ran.

Usage: junit_summary.py <junit.xml> <surface> <suite> <version> <tests run> <out.json> <out.md>
"""

# Requirements: REQ-2167

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET  # nosec B405 -- the XML is the CI job's own pytest output
from pathlib import Path


def summarise(junit: str, surface: str, suite: str, version: str, tests: str) -> dict:
    root = ET.parse(junit).getroot()  # nosec B314 -- see above
    passed, skipped, failures = 0, 0, []
    for case in root.iter("testcase"):
        outcome = next((c for c in case if c.tag in ("failure", "error", "skipped")), None)
        if outcome is None:
            passed += 1
        elif outcome.tag == "skipped":
            skipped += 1
        else:
            message = (outcome.get("message") or outcome.text or "").strip().splitlines()
            failures.append(
                {
                    "feature": case.get("classname", ""),
                    "scenario": case.get("name", ""),
                    "reason": (message[0] if message else outcome.tag)[:300],
                }
            )
    ran = passed + len(failures)
    return {
        "surface": surface,
        "suite": suite,
        "version": version,
        "tests": tests,
        "total": ran + skipped,
        "ran": ran,
        "passed": passed,
        "failed": len(failures),
        "skipped": skipped,
        # A test the suite skips is one for a feature the endpoint does not claim -- creating its
        # own tables, for one -- so it is not runnable by design (the pages count it apart).
        "not_runnable": skipped,
        "pass_rate": round(passed / ran, 4) if ran else None,
        "failures": failures,
    }


def markdown(doc: dict) -> str:
    rate = "n/a" if doc["pass_rate"] is None else f"{doc['pass_rate']:.1%}"
    return (
        "\n".join(
            [
                f"## {doc['surface']} conformance: {doc['suite']} {doc['version']}",
                "",
                f"Tests: {doc['tests']}. Passed {doc['passed']} of {doc['ran']} ({rate}); "
                f"{doc['skipped']} skipped by the suite.",
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
    return 0 if doc["ran"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
