#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2f4a71-6c3b-4d95-b1a8-0f7d3e5c9b26
#
# This source code is licensed under the Business Source License 1.1
"""Generate docs/features.md from docs/arch/features.yaml.

One section per published feature (internal features are left out): its paragraphs, each anchored
by its id. The page is read by customers, so it names no requirement: which requirements serve a
paragraph is each requirement's ``feature`` in docs/arch/requirements.yaml, shown on the
requirements page.

Usage:
  python scripts/gen_features_doc.py            # write the file
  python scripts/gen_features_doc.py --check    # exit 1 if the file is out of date
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from provisa.tools.feature_schema import FeaturesFile  # type: ignore[import]

OUT_PATH = Path("docs/features.md")


def render(ff: FeaturesFile) -> str:
    lines = [
        "# Features",
        "",
        "<!-- Generated from docs/arch/features.yaml by scripts/gen_features_doc.py."
        " Do not hand-edit. -->",
        "",
        "What Provisa does, feature by feature.",
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
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="exit 1 if the file is out of date")
    args = parser.parse_args()
    text = render(FeaturesFile.load())
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
