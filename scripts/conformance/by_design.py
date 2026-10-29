# Copyright (c) 2026 Kenneth Stott
# Canary: 151a9da7-c3e7-4544-9803-5ea7f9e5c825
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every conformance case is passed, skipped by design, or failed (REQ-2167, amended 2026-10-27
and 2026-10-28).

A suite's case that cannot pass because of a designed limitation of Provisa is skipped by design,
in a group that names the design decision (its requirement), and left out of the pass rate; it is
never counted as failed. Every other case that did not pass -- including one the suite or its
client could not run -- is failed. The groups are declared per surface in
``by_design/<surface>.yaml``:

    - group: Unbounded variable-length patterns
      requirement: REQ-348
      decision: Maximum hop depth is enforced at compile time; unbounded `[*]` is rejected.
      reason: "Unbounded variable-length pattern"  # a regex the failure's reason matches, and/or
      case: "VarLengthAcceptance"                  # a regex its feature::scenario matches

A group with no requirement is refused: a case with no design decision behind it stays failed.

Two further kinds are declared in the same file, each with ``kind:`` and a ``reason`` and/or
``case`` pattern:

    - kind: adapted          # setup that creates objects is adapted, not excluded: the case
      group: Fixture tables  # runs and counts, and the page marks it with what its setup became
      became: the table is declared in the fixture model, seeded by the case's setup.
      case: "TestQuery::test_query\\["

    - kind: client           # the client driver does not send what the case needs -- it
      group: Bind with no Prepare  # refuses before any request, or drops what it was given:
      client: adbc-driver-flightsql 1.12 (Go)  # the case stays failed; the group says why
      behavior: refuses Bind ("must set IngestTargetTable before bulk ingestion")
      evidence: with Bind and no Prepare, the driver sends no RPC.
      workaround: prepare the statement before binding.
      reason: "must set IngestTargetTable"

Neither names a requirement: neither is a design decision of Provisa's, so neither takes a case
out of passed or failed."""

# Requirements: REQ-2167

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

RULES = Path(__file__).resolve().parent / "by_design"

#: REQ-2167 (amended 2026-10-27): the surfaces at their conformance baseline, every case passing. On
#: one of them a case that fails and no group of the surface excludes is a regression, and fails
#: the job; on any other surface a failure is data.
AT_BASELINE = frozenset({"pgwire", "graphql"})


def at_baseline(surface: str) -> bool:
    return surface in AT_BASELINE


def baseline_failed(surface: str, failed: int) -> bool:
    """Whether the run fails its job: ``failed`` cases no group excludes on a surface at its
    baseline (said as a workflow error)."""
    if not failed or not at_baseline(surface):
        return False
    print(
        f"::error::{surface}: {failed} case(s) failed that no by-design group of the surface "
        f"excludes (scripts/conformance/by_design/{surface}.yaml); its baseline is every case "
        "passing",
        file=sys.stderr,
    )
    return True


#: Each kind's fields, besides group and the patterns.
FIELDS = {
    "design": ("requirement", "decision"),
    "adapted": ("became",),
    "client": ("client", "behavior", "evidence", "workaround"),
}


@dataclass
class Group:
    group: str
    kind: str = "design"
    requirement: str = ""
    decision: str = ""
    details: dict[str, str] = field(default_factory=dict)  # the kind's other fields
    reason: re.Pattern[str] | None = None
    case: re.Pattern[str] | None = None
    cases: list[str] = field(default_factory=list)

    def matches(self, failure: dict) -> bool:
        if self.reason is not None and not self.reason.search(failure["reason"]):
            return False
        named = f"{failure['feature']}::{failure['scenario']}"
        return self.case is None or bool(self.case.search(named))


def groups(surface: str) -> list[Group]:
    """The surface's groups of every kind: a design group with its requirement."""
    path = RULES / f"{surface}.yaml"
    if not path.exists():
        return []
    out = []
    for entry in yaml.safe_load(path.read_text()) or []:
        kind = entry.get("kind", "design")
        if kind not in FIELDS:
            raise ValueError(f"{path.name}: the group {entry.get('group')!r} is of no kind: {kind}")
        if kind == "design" and not re.fullmatch(
            r"REQ-\d+(, REQ-\d+)*", str(entry.get("requirement") or "")
        ):
            raise ValueError(
                f"{path.name}: the group {entry.get('group')!r} names no requirement; an "
                "exclusion with no design decision behind it stays a failure"
            )
        # What a group says is published on the surface's page, which names no requirement:
        # its requirement is its own field.
        said = [f for f in ("group", *FIELDS[kind]) if f != "requirement"]
        named = [f for f in said if re.search(r"REQ-\d", str(entry.get(f) or ""))]
        if named:
            raise ValueError(
                f"{path.name}: the group {entry.get('group')!r} names a requirement in {named}; "
                "a published page names none, so it goes in requirement"
            )
        missing = [f for f in FIELDS[kind] if not entry.get(f)]
        if missing:
            raise ValueError(
                f"{path.name}: the {kind} group {entry.get('group')!r} has no {missing}"
            )
        if not entry.get("reason") and not entry.get("case"):
            raise ValueError(f"{path.name}: the group {entry['group']!r} matches nothing")
        out.append(
            Group(
                group=entry["group"],
                kind=kind,
                requirement=entry.get("requirement", ""),
                decision=entry.get("decision", ""),
                details={f: entry[f] for f in FIELDS[kind] if kind != "design"},
                reason=re.compile(entry["reason"]) if entry.get("reason") else None,
                case=re.compile(entry["case"]) if entry.get("case") else None,
            )
        )
    return out


#: REQ-2167 (amended 2026-10-28, EVERY CASE IS PASSED, BY DESIGN OR FAILED): the three outcomes a
#: case has, each with the label a summary or page gives it. Nothing else.
OUTCOMES = {"passed": "passed", "by_design": "skipped by design", "failed": "failed"}


def outcomes(passed: int, groups: list[dict], failures: list[dict]) -> dict:
    """A suite's (or a suite part's) three counts, the same in every report (REQ-2167, amended
    2026-10-28): ``passed``; ``by_design``, the cases skipped by design, each in one of
    ``by_design_groups`` citing its requirement and decision; and ``failed``, the ``failures``.
    ``total`` is their sum; the pass rate is passed over passed plus failed, and ``ran`` is that
    denominator."""
    skipped = sum(g["count"] for g in groups)
    ran = passed + len(failures)
    return {
        "total": ran + skipped,
        "passed": passed,
        "by_design": skipped,
        "failed": len(failures),
        "ran": ran,
        "pass_rate": round(passed / ran, 4) if ran else None,
        "by_design_groups": groups,
        "failures": failures,
    }


def counts_line(doc: dict, noun: str = "cases") -> str:
    """The three counts as a summary says them: never a by-design case as failed."""
    rate = "n/a" if doc["pass_rate"] is None else f"{doc['pass_rate']:.1%}"
    return (
        f"{doc['total']} {noun}: {doc['passed']} {OUTCOMES['passed']}, {doc['by_design']} "
        f"{OUTCOMES['by_design']}, {doc['failed']} {OUTCOMES['failed']}. Pass rate {rate} "
        "(passed over passed and failed)."
    )


def classify(failures: list[dict], declared: list[Group]) -> tuple[list[dict], list[dict]]:
    """(the failures no group excludes, each group that excluded any: its name, requirement,
    decision, count and cases -- the cases skipped by design)."""
    declared = [g for g in declared if g.kind == "design"]
    left = []
    for failure in failures:
        hit = next((g for g in declared if g.matches(failure)), None)
        if hit is None:
            left.append(failure)
        else:
            hit.cases.append(f"{failure['feature']}::{failure['scenario']}")
    excluded = [
        {
            "group": g.group,
            "requirement": g.requirement,
            "decision": g.decision,
            "count": len(g.cases),
            "cases": list(g.cases),
        }
        for g in declared
        if g.cases
    ]
    return left, excluded


def not_sent(failures: list[dict], declared: list[Group]) -> list[dict]:
    """Each client group that names any of ``failures``: its fields, count and cases. What such a
    case needs was not sent by the client; with no design decision of Provisa's behind it, it stays
    failed (REQ-2167, amended 2026-10-28) -- the group only says why, and its workaround."""
    clients = [g for g in declared if g.kind == "client"]
    for failure in failures:
        hit = next((g for g in clients if g.matches(failure)), None)
        if hit is not None:
            hit.cases.append(f"{failure['feature']}::{failure['scenario']}")
    return [
        {"group": g.group, **g.details, "count": len(g.cases), "cases": list(g.cases)}
        for g in clients
        if g.cases
    ]


def adapted(ran: list[dict], declared: list[Group]) -> list[dict]:
    """Each adapted group that names any case run (``ran``: feature, scenario, reason and
    outcome, "passed" or "failed"): what the cases' setup became and each case's outcome."""
    marks = []
    for group in (g for g in declared if g.kind == "adapted"):
        cases = [
            {"case": f"{o['feature']}::{o['scenario']}", "outcome": o["outcome"]}
            for o in ran
            if group.matches(o)
        ]
        if cases:
            marks.append({"group": group.group, **group.details, "cases": cases})
    return marks


def table(excluded: list[dict], requirement: bool = False) -> list[str]:
    """The by-design groups as a Markdown table: each with its count and decision, and, for the
    job's own summary (``requirement``), the requirement it cites. A published page names no
    requirement: there it stays in the artifact."""
    if not excluded:
        return ["None."]
    if requirement:
        return [
            "| Skipped by design | Count | Requirement | Decision |",
            "| --- | --- | --- | --- |",
            *(
                f"| {g['group']} | {g['count']} | {g['requirement']} "
                f"| {g['decision'].replace('|', '/')} |"
                for g in excluded
            ),
        ]
    return [
        "| Skipped by design | Count | Decision |",
        "| --- | --- | --- |",
        *(f"| {g['group']} | {g['count']} | {g['decision'].replace('|', '/')} |" for g in excluded),
    ]


def not_sent_table(groups: list[dict]) -> list[str]:
    """The client groups as a Markdown table: why some failed cases failed."""
    if not groups:
        return ["None."]
    return [
        "| Group | Count | Client | What it does | Evidence | Workaround |",
        "| --- | --- | --- | --- | --- | --- |",
        *(
            "| "
            + " | ".join(
                str(g[k]).replace("|", "/")
                for k in ("group", "count", "client", "behavior", "evidence", "workaround")
            )
            + " |"
            for g in groups
        ),
    ]


def adapted_lines(marks: list[dict]) -> list[str]:
    """The adapted groups as Markdown: what each setup became, and each case with its outcome."""
    if not marks:
        return ["None."]
    out = []
    for mark in marks:
        passed = sum(c["outcome"] == "passed" for c in mark["cases"])
        out.append(
            f"<details><summary>{mark['group']} ({passed} of {len(mark['cases'])} passed)"
            "</summary>\n"
        )
        out.append(f"Setup became: {mark['became']}\n")
        out.append("| Case | Outcome |")
        out.append("| --- | --- |")
        out.extend(f"| {c['case']} | {c['outcome']} |" for c in mark["cases"])
        out.append("\n</details>\n")
    return out
