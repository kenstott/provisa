#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 869490cb-a265-4a1f-a69a-0ee3556d7d6c
# Canary: e7g0d356-4c9h-5g3f-h174-bf3i8e6g5d40
#
# This source code is licensed under the Business Source License 1.1
"""Generate docs/exports/roadmap.md: the open requirements (proposed, accepted, in progress), grouped
under the feature they serve (docs/arch/features.yaml), features in their published order.
Internal features are left out.

Usage:
  python scripts/gen_roadmap.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from provisa.tools.feature_schema import FeaturesFile  # type: ignore[import]
from provisa.tools.req_schema import RequirementsFile, Status  # type: ignore[import]

YAML_PATH = Path("docs/arch/requirements.yaml")
OUT_PATH = Path("docs/exports/roadmap.md")

_ROADMAP_STATUSES = {Status.proposed, Status.accepted, Status.in_progress}


_SUPERSEDED_SPAN = re.compile(r"\[SUPERSEDED\b.*?\[END SUPERSEDED BLOCK\]", re.S)


def _brief(text: str) -> str:
    """The live wording only: amendment history kept inline is not what the roadmap promises."""
    desc = " ".join(_SUPERSEDED_SPAN.sub(" ", text).split())
    return desc if len(desc) <= 140 else desc[:137] + "…"


def main() -> int:
    try:
        rf = RequirementsFile.load(YAML_PATH)
        ff = FeaturesFile.load()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    owner = ff.paragraph_owner()
    by_feature: dict[str, list] = {}
    for req in rf.requirements:
        if req.status in _ROADMAP_STATUSES:
            by_feature.setdefault(owner[req.feature or ""].id, []).append(req)

    lines = [
        "# Provisa Roadmap",
        "",
        "> Generated from `docs/arch/requirements.yaml` and `docs/arch/features.yaml`."
        " Do not hand-edit.",
        "",
    ]
    total = 0
    section = None
    for feat in ff.features:
        reqs = by_feature.get(feat.id)
        if not feat.published or not reqs:
            continue
        if feat.section != section:
            section = feat.section
            lines += [f"## {section}", ""]
        lines += [f"### {feat.name}", ""]
        for req in reqs:
            target = f" [{req.target}]" if req.target else ""
            lines.append(
                f"- **{req.id}** [{req.priority.value}] [{req.status.value}]{target}"
                f" {req.category} — {_brief(req.description)}"
            )
        lines.append("")
        total += len(reqs)

    OUT_PATH.write_text("\n".join(lines))
    print(f"Written: {OUT_PATH} ({total} roadmap items)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
