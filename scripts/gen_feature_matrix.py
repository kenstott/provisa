#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: c798cb2f-66aa-4b26-aa53-2d421234aece
# Canary: c5e8b134-2a7f-4e1d-f952-9d1g6c4e3b28
#
# This source code is licensed under the Business Source License 1.1
"""Generate docs/exports/feature_matrix.csv for buyer-facing feature evaluation.

Includes all non-rejected requirements. Columns:
  ID, Group, Category, Priority, Status, Description, Use Case

Usage:
  python scripts/gen_feature_matrix.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from provisa.tools.req_schema import RequirementsFile, Status  # type: ignore[import]

YAML_PATH = Path("docs/arch/requirements.yaml")
OUT_PATH = Path("docs/exports/feature_matrix.csv")

_STATUS_LABEL = {
    "complete": "Available",
    "in-progress": "In Development",
    "proposed": "Planned",
    "accepted": "Accepted",
    "rejected": "Not Planned",
}


def main() -> int:
    try:
        rf = RequirementsFile.load(YAML_PATH)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    rows = [r for r in rf.requirements if r.status != Status.rejected]

    with OUT_PATH.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["ID", "Group", "Category", "Priority", "Status", "Description", "Use Case"]
        )
        for req in rows:
            writer.writerow(
                [
                    req.id,
                    req.group,
                    req.category,
                    req.priority.value,
                    _STATUS_LABEL.get(req.status.value, req.status.value),
                    req.description.replace("\n", " ").strip(),
                    (req.use_case or "").replace("\n", " ").strip(),
                ]
            )

    print(f"Written: {OUT_PATH} ({len(rows)} features)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
