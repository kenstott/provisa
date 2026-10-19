#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2f4a71-6c3b-4d95-b1a8-0f7d3e5c9b26
#
# This source code is licensed under the Business Source License 1.1
"""Generate docs/features.md from docs/arch/features.yaml and docs/arch/requirements.yaml.

One section per published feature (internal features are left out): its paragraphs, each anchored by its id, and a collapsed "Requirements" block listing each
paragraph with the requirements that serve it, linked to their sections in arch/requirements.md.

Usage:
  python scripts/gen_features_doc.py            # write the file
  python scripts/gen_features_doc.py --check    # exit 1 if the file is out of date
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from provisa.tools.feature_schema import FeaturesFile  # type: ignore[import]
from provisa.tools.req_schema import RequirementsFile, Status  # type: ignore[import]

REQS_YAML = Path("docs/arch/requirements.yaml")
OUT_PATH = Path("docs/features.md")


def _label(text: str) -> str:
    """A paragraph's first sentence, as its link text in the trace."""
    first = re.split(r"(?<=[.!?])\s", " ".join(text.split()), maxsplit=1)[0]
    return first if len(first) <= 90 else first[:87].rstrip() + "..."


def render(ff: FeaturesFile, rf: RequirementsFile) -> str:
    reqs = [r for r in rf.requirements if r.status != Status.rejected]
    by_paragraph: dict[str, list[str]] = {}
    for r in reqs:
        by_paragraph.setdefault(r.feature or "", []).append(r.id)
    lines = [
        "# Features",
        "",
        "<!-- Generated from docs/arch/features.yaml and docs/arch/requirements.yaml"
        " by scripts/gen_features_doc.py. Do not hand-edit. -->",
        "",
        "What Provisa does, feature by feature. The Requirements block under each feature lists"
        " the detailed requirements behind each paragraph.",
        "",
    ]
    section = None
    for f in ff.features:
        if not f.published:
            continue
        if f.section != section:
            section = f.section
            lines += [f"## {section}", ""]
        lines += [f"### {f.name}", ""]
        for p in f.paragraphs:
            lines += [" ".join(p.text.split()), f"{{ #{p.id} }}", ""]
        lines += ['??? note "Requirements"', ""]
        for p in f.paragraphs:
            ids = sorted(by_paragraph.get(p.id, []), key=lambda i: int(i[4:]))
            links = ", ".join(f"[{i}](arch/requirements.md#{i})" for i in ids)
            lines.append(f"    - [{_label(p.text)}](#{p.id}): {links}")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="exit 1 if the file is out of date")
    args = parser.parse_args()
    text = render(FeaturesFile.load(), RequirementsFile.load(REQS_YAML))
    if args.check:
        if not OUT_PATH.exists() or OUT_PATH.read_text() != text:
            print(f"{OUT_PATH} is out of date: run scripts/gen_features_doc.py", file=sys.stderr)
            return 1
        return 0
    OUT_PATH.write_text(text)
    print(f"Written: {OUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
