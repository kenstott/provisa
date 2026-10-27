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
at its JSON.

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

DOCS = Path(__file__).resolve().parents[2] / "docs" / "conformance"
BEGIN = "<!-- conformance-results:begin -->"
END = "<!-- conformance-results:end -->"


def _rate(passed: int, ran: int) -> str:
    return f"{passed / ran:.1%} ({passed} of {ran})" if ran else "no case ran"


def _cases(groups: dict[str, list[tuple[str, str]]]) -> str:
    out = ["## Failing cases", ""]
    if not groups:
        return "\n".join([*out, "None."])
    for group, cases in groups.items():
        out.append(f"<details><summary>{group} ({len(cases)})</summary>\n")
        out.append("| Case | Reason |")
        out.append("| --- | --- |")
        out.extend(f"| {c} | {r.replace('|', '/')} |" for c, r in cases)
        out.append("\n</details>\n")
    return "\n".join(out)


def read(artifact: dict) -> tuple[str, str, str, str]:
    """(surface, suite line, pass rate line, failing cases section) for an artifact."""
    if "pgspec" in artifact:
        spec = artifact["pgspec"]
        groups = {
            section: [(c["test"], c["reason"]) for c in cases]
            for section, cases in spec["failed_by_section"].items()
        }
        ran = spec["passed"] + spec["failed"]
        suite = f"pgspec {spec['version']}, against PostgreSQL {artifact.get('postgres_version')}"
        return "pgwire", suite, _rate(spec["passed"], ran), _cases(groups)
    groups: dict[str, list[tuple[str, str]]] = {}
    for f in artifact["failures"]:
        groups.setdefault(f["feature"], []).append((f["scenario"], f["reason"]))
    rate = _rate(artifact["passed"], artifact["ran"])
    if artifact.get("not_runnable"):
        rate += f"; {artifact['not_runnable']} cases not runnable by design"
    if "recommendations" in artifact:  # the audit's SHOULD and MAY checks, counted apart
        rate = (
            f"{rate} of the requirements (MUST); recommendations met "
            f"{artifact['recommendations_met']} of {artifact['recommendations']}"
        )
    suite = f"{artifact['suite']} {artifact['version']}"
    if artifact.get("tests"):  # which of the suite's tests ran (a driver's own test directory)
        suite += f" ({artifact['tests']})"
    return artifact["surface"], suite, rate, _cases(groups)


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
