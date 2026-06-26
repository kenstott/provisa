#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: e7g0d356-4c9h-5g3f-h174-bf3i8e6g5d40
#
# This source code is licensed under the Business Source License 1.1
"""Generate docs/exports/roadmap.md — proposed and accepted requirements by target quarter.

Usage:
  python scripts/gen_roadmap.py
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from provisa.tools.req_schema import RequirementsFile, Status  # type: ignore[import]

YAML_PATH = Path("docs/arch/requirements.yaml")
OUT_PATH = Path("docs/exports/roadmap.md")

_ROADMAP_STATUSES = {Status.proposed, Status.accepted, Status.in_progress}


def main() -> int:
    try:
        rf = RequirementsFile.load(YAML_PATH)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    by_quarter: dict[str, list] = defaultdict(list)
    no_target: list = []

    for req in rf.requirements:
        if req.status not in _ROADMAP_STATUSES:
            continue
        if req.target:
            by_quarter[req.target].append(req)
        else:
            no_target.append(req)

    lines = [
        "# Provisa Roadmap",
        "",
        "> Generated from `docs/arch/requirements.yaml`. Do not hand-edit.",
        "",
    ]

    for quarter in sorted(by_quarter):
        reqs = by_quarter[quarter]
        lines += [f"## {quarter}", ""]
        for req in reqs:
            status_tag = f"[{req.status.value}]"
            priority_tag = f"[{req.priority.value}]"
            desc = req.description.replace("\n", " ").strip()
            if len(desc) > 140:
                desc = desc[:137] + "…"
            lines.append(f"- **{req.id}** {priority_tag} {status_tag} {req.category} — {desc}")
        lines.append("")

    if no_target:
        lines += ["## Unscheduled", ""]
        for req in no_target:
            desc = req.description.replace("\n", " ").strip()
            if len(desc) > 140:
                desc = desc[:137] + "…"
            lines.append(f"- **{req.id}** [{req.priority.value}] {req.category} — {desc}")
        lines.append("")

    OUT_PATH.write_text("\n".join(lines))
    total = sum(len(v) for v in by_quarter.values()) + len(no_target)
    print(f"Written: {OUT_PATH} ({total} roadmap items across {len(by_quarter)} quarters)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
