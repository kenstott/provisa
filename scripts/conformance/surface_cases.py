# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e51f7-8a2d-4b6e-9f1c-7d4a2e6b8c05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The case runner and report the REST/OpenAPI, JSON:API, gRPC and MCP conformance runs share
(REQ-2167).

Each of those surfaces' scenario sets is a fixed list of cases, each derived from a normative
statement of its specification and named for the section it comes from. A case passes, is skipped
by design (a group of scripts/conformance/by_design/<surface>.yaml excludes its failure), or fails:
an exception its check raises is its failure, with the exception's text as the reason. The report
has the shape every conformance artifact has (by_design.outcomes), so baseline_gate.py judges it
and fill_docs.py writes its docs/conformance/<surface>.md page from it.
"""

# Requirements: REQ-2167

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

import by_design


class CaseFailed(AssertionError):
    """A case's check found the server answering otherwise than the specification says."""


def expect(condition: bool, reason: str) -> None:
    """Fail the case with ``reason`` unless ``condition`` holds."""
    if not condition:
        raise CaseFailed(reason)


@dataclass(frozen=True)
class Case:
    feature: str  # the specification section the case comes from
    scenario: str  # the normative statement it checks
    check: Callable[[], None]


@dataclass(frozen=True)
class Outcome:
    feature: str
    scenario: str
    reason: str | None  # None when it passed

    @property
    def passed(self) -> bool:
        return self.reason is None


def run(cases: Iterable[Case]) -> list[Outcome]:
    """Each case's outcome, in order. A case whose check raises fails with what it raised."""
    out = []
    for case in cases:
        try:
            case.check()
        except Exception as exc:  # noqa: BLE001 - every failure of a case is that case's outcome
            reason = str(exc).strip() or type(exc).__name__
            out.append(Outcome(case.feature, case.scenario, f"{type(exc).__name__}: {reason}"))
        else:
            out.append(Outcome(case.feature, case.scenario, None))
    return out


def report(surface: str, suite: str, version: str, outcomes: list[Outcome]) -> dict:
    """The run's artifact: the three counts, the by-design groups and the failures."""
    failures = [
        {"feature": o.feature, "scenario": o.scenario, "reason": o.reason}
        for o in outcomes
        if not o.passed
    ]
    left, excluded = by_design.classify(failures, by_design.groups(surface))
    passed = sum(1 for o in outcomes if o.passed)
    return {
        "surface": surface,
        "suite": suite,
        "version": version,
        **by_design.outcomes(passed, excluded, left),
    }


def markdown(title: str, doc: dict) -> str:
    """The job summary: the counts line, the by-design groups and the failed cases."""
    lines = [f"## {title} conformance: {doc['suite']} {doc['version']}", ""]
    lines += [by_design.counts_line(doc), ""]
    if doc["by_design_groups"]:
        lines += ["### Skipped by design", "", *by_design.table(doc["by_design_groups"]), ""]
    lines += ["### Failed", "", "| Section | Case | Reason |", "| --- | --- | --- |"]
    lines += [
        f"| {f['feature']} | {f['scenario']} | {' '.join(f['reason'].split()).replace('|', '/')} |"
        for f in doc["failures"]
    ]
    return "\n".join(lines) + "\n"


def write(doc: dict, title: str, out_json: Path, out_md: Path) -> int:
    """Write the artifact and the summary and print the log lines; non-zero only when no case
    ran (a failure is data: baseline_gate.py judges it)."""
    out_json.write_text(json.dumps(doc, indent=2) + "\n")
    out_md.write_text(markdown(title, doc))
    print("\n".join(by_design.log_lines(doc)))
    return 0 if doc["total"] else 1
