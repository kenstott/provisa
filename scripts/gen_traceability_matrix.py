#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: d6f9c245-3b8g-4f2e-g063-ae2h7d5f4c39
#
# This source code is licensed under the Business Source License 1.1
"""Generate docs/exports/traceability_matrix.csv for auditor-facing REQ↔test mapping.

Includes accepted, in-progress, and complete requirements. Columns:
  ID, Group, Category, Priority, Status, Description, Code Paths, Test Files

Usage:
  python scripts/gen_traceability_matrix.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from provisa.tools.req_schema import RequirementsFile, Status  # type: ignore[import]

YAML_PATH = Path("docs/arch/requirements.yaml")
OUT_PATH = Path("docs/exports/traceability_matrix.csv")

_COVERED_STATUSES = {Status.accepted, Status.in_progress, Status.complete}


def main() -> int:
    try:
        rf = RequirementsFile.load(YAML_PATH)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    rows = [r for r in rf.requirements if r.status in _COVERED_STATUSES]

    with OUT_PATH.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "ID",
                "Group",
                "Category",
                "Priority",
                "Status",
                "Description",
                "Code Paths",
                "Test Files",
                "Has Tests",
            ]
        )
        for req in rows:
            writer.writerow(
                [
                    req.id,
                    req.group,
                    req.category,
                    req.priority.value,
                    req.status.value,
                    req.description.replace("\n", " ").strip(),
                    "; ".join(req.code) if req.code else "",
                    "; ".join(req.tests) if req.tests else "",
                    "Yes" if req.tests else "No",
                ]
            )

    tested = sum(1 for r in rows if r.tests)
    print(f"Written: {OUT_PATH} ({len(rows)} requirements, {tested} with tests)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
