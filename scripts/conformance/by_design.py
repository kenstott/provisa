# Copyright (c) 2026 Kenneth Stott
# Canary: 151a9da7-c3e7-4544-9803-5ea7f9e5c825
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""By-design exclusions are not failures (REQ-2167, amended 2026-10-27).

A suite's test that fails because of a designed limitation of Provisa is counted as not runnable
by design, in a group that names the design decision (its requirement), and left out of the pass
rate. The groups are declared per surface in ``by_design/<surface>.yaml``:

    - group: DETACH DELETE
      requirement: REQ-671
      decision: DETACH DELETE is not supported; a client deletes each table in dependency order.
      reason: "Cypher write pattern 'DETACH'"     # a regex the failure's reason matches, and/or
      case: "test_readme"                          # a regex its feature::scenario matches

A group with no requirement is refused: an exclusion with no design decision behind it stays a
failure.

Two further kinds are declared in the same file, each with ``kind:`` and a ``reason`` and/or
``case`` pattern:

    - kind: adapted          # setup that creates objects is adapted, not excluded: the case
      group: Fixture tables  # runs and counts, and the page marks it with what its setup became
      became: the table is declared in the fixture model, seeded by the case's setup.
      case: "TestQuery::test_query\\["

    - kind: client           # the client driver refuses before any request reaches Provisa:
      group: Bind before Prepare   # left out of the pass rate, in its own group
      client: adbc-driver-flightsql 1.12 (Go)
      refusal: "must set IngestTargetTable before bulk ingestion"
      evidence: with Bind and no Prepare, the driver sends no RPC.
      workaround: prepare the statement before binding.
      reason: "must set IngestTargetTable"

Neither names a requirement: neither is a design decision of Provisa's."""

# Requirements: REQ-2167

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

RULES = Path(__file__).resolve().parent / "by_design"


#: Each kind's fields, besides group and the patterns.
FIELDS = {
    "design": ("requirement", "decision"),
    "adapted": ("became",),
    "client": ("client", "refusal", "evidence", "workaround"),
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


def classify(failures: list[dict], declared: list[Group]) -> tuple[list[dict], list[dict]]:
    """(the failures no group excludes, each group that excluded any: its name, requirement,
    decision and count)."""
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
        }
        for g in declared
        if g.cases
    ]
    return left, excluded


def refused(failures: list[dict], declared: list[Group]) -> tuple[list[dict], list[dict]]:
    """(the failures no client group names, each client group that named any: its fields and
    count) -- refused by the client before any request, left out of the pass rate."""
    clients = [g for g in declared if g.kind == "client"]
    left = []
    for failure in failures:
        hit = next((g for g in clients if g.matches(failure)), None)
        if hit is None:
            left.append(failure)
        else:
            hit.cases.append(f"{failure['feature']}::{failure['scenario']}")
    return left, [
        {"group": g.group, **g.details, "count": len(g.cases)} for g in clients if g.cases
    ]


def adapted(outcomes: list[dict], declared: list[Group]) -> list[dict]:
    """Each adapted group that names any case run (``outcomes``: feature, scenario, reason and
    outcome, "passed" or "failed"): what the cases' setup became and each case's outcome."""
    marks = []
    for group in (g for g in declared if g.kind == "adapted"):
        cases = [
            {"case": f"{o['feature']}::{o['scenario']}", "outcome": o["outcome"]}
            for o in outcomes
            if group.matches(o)
        ]
        if cases:
            marks.append({"group": group.group, **group.details, "cases": cases})
    return marks


def table(excluded: list[dict]) -> list[str]:
    """The by-design groups as a Markdown table: each with its count, decision and requirement."""
    if not excluded:
        return ["None."]
    return [
        "| Group | Count | Decision | Requirement |",
        "| --- | --- | --- | --- |",
        *(
            f"| {g['group']} | {g['count']} | {g['decision'].replace('|', '/')} | "
            f"{g['requirement']} |"
            for g in excluded
        ),
    ]


def refused_table(refusals: list[dict]) -> list[str]:
    """The client-refused groups as a Markdown table."""
    if not refusals:
        return ["None."]
    return [
        "| Group | Count | Client | Its refusal | Evidence | Workaround |",
        "| --- | --- | --- | --- | --- | --- |",
        *(
            "| "
            + " | ".join(
                str(g[k]).replace("|", "/")
                for k in ("group", "count", "client", "refusal", "evidence", "workaround")
            )
            + " |"
            for g in refusals
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
