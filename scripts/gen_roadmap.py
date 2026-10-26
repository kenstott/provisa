#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 869490cb-a265-4a1f-a69a-0ee3556d7d6c
# Canary: e7g0d356-4c9h-5g3f-h174-bf3i8e6g5d40
#
# This source code is licensed under the Business Source License 1.1
"""Generate docs/exports/roadmap.md: the open requirements (proposed, accepted, in progress), grouped
under the feature they serve (docs/arch/features.yaml), features in their published order.
Internal features are left out.

Also generates docs/roadmap.md: the public roadmap, containing only requirements that have a
`value` sentence, grouped by target quarter then by feature area. No requirement IDs, statuses
or priorities appear in that file.

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
PUBLIC_OUT_PATH = Path("docs/roadmap.md")

_ROADMAP_STATUSES = {Status.proposed, Status.accepted, Status.in_progress}

_QUARTER_ORDER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}

_SUPERSEDED_SPAN = re.compile(r"\[SUPERSEDED\b.*?\[END SUPERSEDED BLOCK\]", re.S)


def _brief(text: str) -> str:
    """The live wording only: amendment history kept inline is not what the roadmap promises."""
    desc = " ".join(_SUPERSEDED_SPAN.sub(" ", text).split())
    return desc if len(desc) <= 140 else desc[:137] + "…"


def _quarter_sort_key(target: str | None) -> tuple:
    """Return a sortable tuple for a target string such as '2027-Q1' or None."""
    if not target:
        return (9999, 9)
    parts = target.split("-")
    if len(parts) == 2 and parts[1] in _QUARTER_ORDER:
        return (int(parts[0]), _QUARTER_ORDER[parts[1]])
    # ISO date or other format: sort after quarters
    return (9998, 0)


def _write_internal(rf: RequirementsFile, ff: FeaturesFile) -> int:
    """Write docs/exports/roadmap.md (internal, with REQ IDs)."""
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
    return total


def _write_public(rf: RequirementsFile, ff: FeaturesFile) -> int:
    """Write docs/roadmap.md (public, no REQ IDs, value sentences only).

    Groups by target quarter (earliest first), then by feature area (from features.yaml,
    using published names and order). Each item is its value sentence.
    """
    owner = ff.paragraph_owner()

    # Collect requirements that have a value, are in a roadmap status, and have a target quarter
    # after the first launch (2026-Q4 and earlier items ship at launch or are v1 work).
    valued: list = []
    for req in rf.requirements:
        if (
            req.status in _ROADMAP_STATUSES
            and req.target
            and req.value  # type: ignore[attr-defined]
            and _quarter_sort_key(req.target) > _quarter_sort_key("2026-Q4")
        ):
            valued.append(req)

    if not valued:
        PUBLIC_OUT_PATH.write_text("# Roadmap\n\nNo items.\n")
        print(f"Written: {PUBLIC_OUT_PATH} (0 public items)")
        return 0

    # Collect the set of quarters present, in order.
    quarters: list[str] = sorted(
        {r.target for r in valued if r.target},
        key=_quarter_sort_key,
    )

    # Feature ordering: follow features.yaml publication order.
    feat_order: dict[str, int] = {f.id: i for i, f in enumerate(ff.features) if f.published}

    intro = (
        "This roadmap shows where Provisa is headed. "
        "Groupings and timing reflect current plans and may shift as priorities develop. "
        "Items listed here are things a user will see—internal engineering work is not shown."
    )

    lines: list[str] = [
        "# Roadmap",
        "",
        intro,
        "",
    ]

    total = 0
    for quarter in quarters:
        lines += [f"## {quarter}", ""]

        # Reqs in this quarter, sorted by feature order then by REQ id for stability.
        quarter_reqs = [r for r in valued if r.target == quarter]
        quarter_reqs.sort(
            key=lambda r: (
                feat_order.get(
                    owner.get(r.feature or "", owner.get("", None)) and owner[r.feature or ""].id,
                    9999,
                ),  # type: ignore[arg-type]
                r.id,
            )
        )

        # Group by feature (published name).
        seen_feat: str | None = None
        for req in quarter_reqs:
            feat = owner.get(req.feature or "")
            feat_name = feat.name if feat and feat.published else None
            if feat_name is None:
                continue
            if feat_name != seen_feat:
                if seen_feat is not None:
                    lines.append("")
                lines += [f"### {feat_name}", ""]
                seen_feat = feat_name
            lines.append(f"- {req.value}")  # type: ignore[attr-defined]
            total += 1

        lines.append("")

    PUBLIC_OUT_PATH.write_text("\n".join(lines))
    print(f"Written: {PUBLIC_OUT_PATH} ({total} public items)")
    return total


def main() -> int:
    try:
        rf = RequirementsFile.load(YAML_PATH)
        ff = FeaturesFile.load()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    _write_internal(rf, ff)
    _write_public(rf, ff)
    return 0


if __name__ == "__main__":
    sys.exit(main())
