# Copyright (c) 2026 Kenneth Stott
# Canary: 8b3f6d21-7c49-4e15-a0d8-2f5b9e1c6a37
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fill a docs/conformance/<surface>.md page from its conformance job's artifact (REQ-2167).

The page's **Suite:** and **Pass rate:** lines and its failing cases come from the JSON a
conformance-<surface> workflow uploads, never typed: download the artifact of a run and point this
at its JSON. Every case is passed, skipped by design or failed (REQ-2167, amended 2026-10-28), and
the page gives the three counts and the pass rate, passed over passed plus failed. On pgwire they
are the whole run's: a pg_catalog function not served is failed, grouped by family.

    .venv/bin/python3 scripts/conformance/fill_docs.py conformance-cypher.json [more.json ...]

Each artifact names its surface: ``surface`` (the TCK-style runners), or the shape the pgwire job
writes (``pgspec`` beside the catalog comparison). The failing cases are written between the
page's ``conformance-results`` markers, added under the pass rate the first time.
"""

# Requirements: REQ-2167

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import by_design

DOCS = Path(__file__).resolve().parents[2] / "docs" / "conformance"
BEGIN = "<!-- conformance-results:begin -->"
END = "<!-- conformance-results:end -->"

#: The parts of a pgwire run beside pgspec, each with its own three counts.
COMPARED = ("catalog_relations", "catalog_functions", "type_conversions")


def _rate(doc: dict) -> str:
    """The pass rate (passed over passed plus failed) and the three counts (REQ-2167, amended
    2026-10-28): a case skipped by design is never counted as failed."""
    head = "no case passed or failed" if doc["pass_rate"] is None else f"{doc['pass_rate']:.1%}"
    return (
        f"{head}: {doc['passed']} passed, {doc['by_design']} skipped by design, "
        f"{doc['failed']} failed, of {doc['total']} cases"
    )


def _designed(groups: list[dict]) -> str:
    """Each by-design group, its count and the decision behind it, and its cases."""
    return "\n".join(
        ["## Skipped by design", "", *by_design.table(groups), "", *by_design.cases_lines(groups)]
    )


def _cases(groups: dict[str, list[tuple[str, str]]]) -> str:
    out = ["## Failed cases", ""]
    if not groups:
        return "\n".join([*out, "None."])
    for group, cases in groups.items():
        out.append(f"<details><summary>{group} ({len(cases)})</summary>\n")
        out.append("| Case | Reason |")
        out.append("| --- | --- |")
        out.extend(f"| {c} | {' '.join(r.split()).replace('|', '/')} |" for c, r in cases)
        out.append("\n</details>\n")
    return "\n".join(out)


def read(artifact: dict) -> tuple[str, str, str, str]:
    """(surface, suite line, pass rate line, cases sections) for an artifact."""
    if "pgspec" in artifact:
        spec = artifact["pgspec"]
        groups = {
            section: [(c["test"], c["reason"]) for c in cases]
            for section, cases in spec["failed_by_section"].items()
        }
        parts = [spec, *(artifact[p] for p in COMPARED if artifact.get(p) is not None)]
        for part in parts[1:]:
            for f in part["failures"]:
                # REQ-2167 (PG_CATALOG FUNCTIONS ARE GAPS): functions fail by family.
                group = f"Catalog function: {f['family']}" if "family" in f else f["feature"]
                groups.setdefault(group, []).append((f["scenario"], f["reason"]))
        suite = f"pgspec {spec['version']}, against PostgreSQL {artifact.get('postgres_version')}"
        sections = [_designed([g for p in parts for g in p["by_design_groups"]]), _cases(groups)]
        return "pgwire", suite, _rate(artifact), "\n\n".join(sections)
    groups: dict[str, list[tuple[str, str]]] = {}
    for f in artifact["failures"]:
        groups.setdefault(f["feature"], []).append((f["scenario"], f["reason"]))
    suite = f"{artifact['suite']} {artifact['version']}"
    if artifact.get("tests"):  # which of the suite's tests ran (a driver's own test directory)
        suite += f" ({artifact['tests']})"
    # REQ-2167 (amended 2026-10-27 and 2026-10-28): each by-design group, its count and the
    # decision behind it -- with the driver's or the suite's evidence, and a workaround, for a
    # client or suite group -- and its cases with their reasons; adapted cases marked with what
    # their setup became -- on the surfaces that have any.
    sections = [_designed(artifact["by_design_groups"])]
    if "adapted" in artifact:
        sections.append(
            "\n".join(["## Adapted cases", "", *by_design.adapted_lines(artifact["adapted"])])
        )
    return artifact["surface"], suite, _rate(artifact), "\n\n".join([*sections, _cases(groups)])


def fill(page: str, suite: str, rate: str, cases: str) -> str:
    page = re.sub(r"^\*\*Suite:\*\*.*$", f"**Suite:** {suite}.", page, count=1, flags=re.M)
    page = re.sub(r"^\*\*Pass rate:\*\*.*$", f"**Pass rate:** {rate}.", page, count=1, flags=re.M)
    block = f"{BEGIN}\n{cases}\n{END}"
    if BEGIN in page:
        start, end = page.index(BEGIN), page.index(END) + len(END)
        return page[:start] + block + page[end:]
    at = re.search(r"^\*\*Pass rate:\*\*.*$", page, flags=re.M)
    if at is None:
        raise ValueError("the page has no **Pass rate:** line to put the results under")
    return page[: at.end()] + "\n\n" + block + page[at.end() :]


def main(argv: list[str]) -> int:
    for path in argv:
        surface, suite, rate, cases = read(json.loads(Path(path).read_text()))
        doc = DOCS / f"{surface}.md"
        doc.write_text(fill(doc.read_text(), suite, rate, cases))
        print(f"{doc}: {suite}, {rate}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
