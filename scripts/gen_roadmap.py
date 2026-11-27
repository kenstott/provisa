#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 869490cb-a265-4a1f-a69a-0ee3556d7d6c
# Canary: e7g0d356-4c9h-5g3f-h174-bf3i8e6g5d40
#
# This source code is licensed under the Business Source License 1.1
"""Generate the roadmap from the release plan, docs/arch/release-plan.json.

docs/exports/roadmap.md (attached to the GitHub Release): the plan release by release: date,
status and notes, then each topic's features with requirement id, priority, state and business
benefit; then the decisions, the requirements off the release train, the already-shipped appendix
and the planning basis.

docs/roadmap.md (the public page): releases, then topics, then each feature with its benefit.
No requirement ids, statuses or priorities appear in that file.

Usage:
  python scripts/gen_roadmap.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

PLAN_PATH = Path("docs/arch/release-plan.json")
OUT_PATH = Path("docs/exports/roadmap.md")
PUBLIC_OUT_PATH = Path("docs/roadmap.md")

VERSION_PREFIX = "v0.1.0-alpha."

# A split requirement's name carries where its remainder went; the public page names no requirement.
_REMAINDER_NOTE = re.compile(r"\s*\(delivered rules; the rest is REQ-\d+\)")
_REQ_ID = re.compile(r"\bREQ-\d+")

PUBLIC_INTRO = (
    "This roadmap shows where Provisa is headed, one release at a time; a release ships every "
    "two days. Groupings and timing reflect current plans and may shift as priorities develop. "
    "Items listed here are things a user will see—internal engineering work is not shown."
)


def load_plan(path: Path) -> dict:
    return json.loads(path.read_text())


def _features(release: dict) -> list[dict]:
    return [f for topic in release["topics"] for f in topic["features"]]


def _rules_cell(release: dict) -> str:
    """Planned releases carry their planned rule count; shipped ones list what their code ships."""
    return str(release["planned_rules"]) if "planned_rules" in release else "shipped"


def _release_title(release: dict) -> str:
    return f"{VERSION_PREFIX}{release['version']}"


def render_internal(plan: dict) -> str:
    releases = plan["releases"]
    lines = [
        "# Provisa Release Plan",
        "",
        "> Generated from `docs/arch/release-plan.json` by `scripts/gen_roadmap.py`."
        " Do not hand-edit.",
        "",
        f"One release every 2 days, from {_release_title(releases[0])} to"
        f" {_release_title(releases[-1])}. Dates are publish dates; planned releases are not"
        " committed.",
        "",
        "| Release | Date | Status | Requirements | Product rules |",
        "|---|---|---|---|---|",
    ]
    for rel in releases:
        lines.append(
            f"| {rel['version']} | {rel['date']} | {rel['status']} | {len(_features(rel))}"
            f" | {_rules_cell(rel)} |"
        )
    lines.append("")

    for rel in releases:
        lines += [f"## {_release_title(rel)}: {rel['date']} ({rel['status']})", ""]
        for note in rel.get("notes", []):
            lines.append(f"> {note}")
        if rel.get("notes"):
            lines.append("")
        for topic in rel["topics"]:
            lines += [f"### {topic['topic']}", ""]
            for f in topic["features"]:
                lines.append(
                    f"- **{f['req']}** {f['feature']} [{f['priority']}] [{f['state']}]"
                    f" — {f['benefit']}"
                )
            lines.append("")

    lines += ["## Decisions applied", ""]
    lines += [f"- **{d['req']}** {d['decision']}" for d in plan["decisions"]]
    lines += ["", "## Off the release train", "", ", ".join(plan["off_train"]) + ".", ""]

    lines += ["## Appendix: already shipped", ""]
    for item in plan["already_shipped"]:
        detail = f"{item['evidence']}; now {item['state_after_edit']}"
        if item["remainder"]:
            detail += f"; remainder {item['remainder']}"
        lines.append(f"- **{item['req']}** {item['title']} ({detail})")

    lines += [
        "",
        "## Planning basis",
        "",
        f"Capacity: about {plan['capacity_per_release']} {plan['capacity_unit']} per release.",
        "",
        plan["basis"],
        "",
    ]
    return "\n".join(lines)


def public_feature_name(name: str) -> str:
    return _REMAINDER_NOTE.sub("", name)


def render_public(plan: dict) -> str:
    lines = ["# Roadmap", "", PUBLIC_INTRO, ""]
    for rel in plan["releases"]:
        lines += [f"## {_release_title(rel)} ({rel['date']})", ""]
        for topic in rel["topics"]:
            lines += [f"### {topic['topic']}", ""]
            lines += [
                f"- **{public_feature_name(f['feature'])}.** {f['benefit']}"
                for f in topic["features"]
            ]
            lines.append("")
    text = "\n".join(lines)
    leaked = _REQ_ID.findall(text)
    if leaked:
        raise ValueError(f"public roadmap would name requirements: {sorted(set(leaked))}")
    return text


def main() -> int:
    plan = load_plan(PLAN_PATH)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(render_internal(plan))
    count = sum(len(_features(r)) for r in plan["releases"])
    print(f"Written: {OUT_PATH} ({len(plan['releases'])} releases, {count} features)")
    PUBLIC_OUT_PATH.write_text(render_public(plan))
    print(f"Written: {PUBLIC_OUT_PATH} ({count} public items)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
