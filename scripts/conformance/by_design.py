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
never counted as failed. So is a case the client library never sends, or one the suite skips for
every server, each in its own group citing its requirement and the driver's or suite's evidence.
Every other case that did not pass is failed. Each case skipped by design keeps its recorded
reason. The groups are declared per surface in ``by_design/<surface>.yaml``:

    - group: Unbounded variable-length patterns
      requirement: REQ-348
      decision: Maximum hop depth is enforced at compile time; unbounded `[*]` is rejected.
      reason: "Unbounded variable-length pattern"  # a regex the failure's reason matches, and/or
      case: "VarLengthAcceptance"                  # a regex its feature::scenario matches

A group with no requirement is refused: a case with no design decision behind it stays failed.

Three further kinds are declared in the same file, each with ``kind:`` and a ``reason`` and/or
``case`` pattern:

    - kind: adapted          # setup that creates objects is adapted, not excluded: the case
      group: Fixture tables  # runs and counts, and the page marks it with what its setup became
      became: the table is declared in the fixture model, seeded by the case's setup.
      case: "TestQuery::test_query\\["

    - kind: client           # the client library never sends what the case needs -- it
      group: Bind with no Prepare  # refuses before any request, or drops what it was given:
      requirement: REQ-2167  # skipped by design, its decision citing the driver's source
      decision: "flightsql_statement.go:526-531 (ADBC 1.12.0) refuses before any RPC."
      client: adbc-driver-flightsql 1.12 (Go)
      behavior: refuses Bind ("must set IngestTargetTable before bulk ingestion")
      evidence: with Bind and no Prepare, the driver sends no RPC.
      workaround: prepare the statement before binding.
      reason: "must set IngestTargetTable"

    - kind: suite            # the suite skips the case for every server: skipped by design,
      group: Kerberos example  # citing its skip marker; matches only a skipped case
      requirement: REQ-2167
      decision: "test_kerberos_auth_example.py:40 calls pytest.skip unconditionally."
      suite: neo4j-python-driver integration tests
      evidence: the example is marked skip, "Currently no way to test Kerberos auth".
      case: "test_kerberos_auth_example::test_example$"

A design or client group may say ``skipped: true`` to name only a case the suite skipped (the suite
gates it on a feature the client cannot claim). An adapted group names no requirement: its cases
run and count."""

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
    "client": ("requirement", "decision", "client", "behavior", "evidence", "workaround"),
    "suite": ("requirement", "decision", "suite", "evidence"),
}

#: The kinds whose cases are skipped by design (REQ-2167, amended 2026-10-28 and 2026-10-29), each
#: citing its requirement and, in its decision, the evidence itself: a design decision of
#: Provisa's; a case the client library refuses or drops itself, citing the driver's source (file
#: and line, at its pinned version); a case the suite skips for every server, citing the suite's
#: skip marker (file and line). A case with no such evidence stays failed.
BY_DESIGN = ("design", "client", "suite")


@dataclass
class Group:
    group: str
    kind: str = "design"
    requirement: str = ""
    decision: str = ""
    details: dict[str, str] = field(default_factory=dict)  # the kind's other fields
    reason: re.Pattern[str] | None = None
    case: re.Pattern[str] | None = None
    cases: list[dict] = field(default_factory=list)  # each case named, with its recorded reason
    skipped: bool = False  # names only a case the suite skipped (always so for a suite group)

    def matches(self, failure: dict) -> bool:
        if (self.skipped or self.kind == "suite") and not failure.get("skipped"):
            return False
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
        if kind in BY_DESIGN and not re.fullmatch(
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
                details={f: entry[f] for f in FIELDS[kind] if f not in ("requirement", "decision")},
                reason=re.compile(entry["reason"]) if entry.get("reason") else None,
                case=re.compile(entry["case"]) if entry.get("case") else None,
                skipped=bool(entry.get("skipped")),
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
    """(the failures no group excludes, each group that excluded any: its name, kind,
    requirement, decision, the kind's evidence, count and cases -- the cases skipped by design,
    each with its recorded reason)."""
    declared = [g for g in declared if g.kind in BY_DESIGN]
    left = []
    for failure in failures:
        hit = next((g for g in declared if g.matches(failure)), None)
        if hit is None:
            left.append(failure)
        else:
            hit.cases.append(
                {
                    "case": f"{failure['feature']}::{failure['scenario']}",
                    "reason": failure["reason"],
                }
            )
    excluded = [
        {
            "group": g.group,
            "kind": g.kind,
            "requirement": g.requirement,
            "decision": g.decision,
            **g.details,
            "count": len(g.cases),
            "cases": list(g.cases),
        }
        for g in declared
        if g.cases
    ]
    return left, excluded


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


def _cited(group: dict) -> str:
    """A group's decision, with the evidence a client or suite group cites."""
    said = group["decision"]
    if group.get("kind") == "client":
        said += f" Workaround: {group['workaround']}"
    return said.replace("|", "/")


def cases_lines(excluded: list[dict]) -> list[str]:
    """Each case skipped by design with its recorded reason, per group."""
    out: list[str] = []
    for g in excluded:
        out.append(f"<details><summary>{g['group']} ({g['count']})</summary>\n")
        out.append("| Case | Reason |")
        out.append("| --- | --- |")
        out.extend(f"| {c['case']} | {c['reason'].replace('|', '/')} |" for c in g["cases"])
        out.append("\n</details>\n")
    return out


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
                f"| {g['group']} | {g['count']} | {g['requirement']} | {_cited(g)} |"
                for g in excluded
            ),
        ]
    return [
        "| Skipped by design | Count | Decision |",
        "| --- | --- | --- |",
        *(f"| {g['group']} | {g['count']} | {_cited(g)} |" for g in excluded),
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
