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
failure."""

# Requirements: REQ-2167

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

RULES = Path(__file__).resolve().parent / "by_design"


@dataclass
class Group:
    group: str
    requirement: str
    decision: str
    reason: re.Pattern[str] | None = None
    case: re.Pattern[str] | None = None
    cases: list[str] = field(default_factory=list)

    def matches(self, failure: dict) -> bool:
        if self.reason is not None and not self.reason.search(failure["reason"]):
            return False
        named = f"{failure['feature']}::{failure['scenario']}"
        return self.case is None or bool(self.case.search(named))


def groups(surface: str) -> list[Group]:
    """The surface's by-design groups, each with its requirement."""
    path = RULES / f"{surface}.yaml"
    if not path.exists():
        return []
    out = []
    for entry in yaml.safe_load(path.read_text()) or []:
        if not re.fullmatch(r"REQ-\d+(, REQ-\d+)*", str(entry.get("requirement") or "")):
            raise ValueError(
                f"{path.name}: the group {entry.get('group')!r} names no requirement; an "
                "exclusion with no design decision behind it stays a failure"
            )
        if not entry.get("reason") and not entry.get("case"):
            raise ValueError(f"{path.name}: the group {entry['group']!r} matches nothing")
        out.append(
            Group(
                group=entry["group"],
                requirement=entry["requirement"],
                decision=entry["decision"],
                reason=re.compile(entry["reason"]) if entry.get("reason") else None,
                case=re.compile(entry["case"]) if entry.get("case") else None,
            )
        )
    return out


def classify(failures: list[dict], declared: list[Group]) -> tuple[list[dict], list[dict]]:
    """(the failures no group excludes, each group that excluded any: its name, requirement,
    decision and count)."""
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
