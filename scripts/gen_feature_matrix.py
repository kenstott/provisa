#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: c798cb2f-66aa-4b26-aa53-2d421234aece
# Canary: c5e8b134-2a7f-4e1d-f952-9d1g6c4e3b28
#
# This source code is licensed under the Business Source License 1.1
"""Generate docs/exports/feature_matrix.csv for buyer-facing feature evaluation.

One row per published feature (docs/arch/features.yaml; internal features are left out). Columns:
  Feature, Section, Audience, Kind, Description, Paragraphs

Description is the feature's paragraphs. Paragraphs lists each paragraph id with the requirements
that serve it ("paragraph-id: REQ-1 REQ-2; ...")

Usage:
  python scripts/gen_feature_matrix.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from provisa.tools.feature_schema import FeaturesFile  # type: ignore[import]
from provisa.tools.req_schema import RequirementsFile, Status  # type: ignore[import]

YAML_PATH = Path("docs/arch/requirements.yaml")
OUT_PATH = Path("docs/exports/feature_matrix.csv")


def main() -> int:
    try:
        rf = RequirementsFile.load(YAML_PATH)
        ff = FeaturesFile.load()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    reqs = [r for r in rf.requirements if r.status != Status.rejected]
    by_paragraph: dict[str, list[str]] = {}
    for r in reqs:
        by_paragraph.setdefault(r.feature or "", []).append(r.id)
    features = [f for f in ff.features if f.published]

    with OUT_PATH.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Feature", "Section", "Audience", "Kind", "Description", "Paragraphs"])
        for feat in features:
            writer.writerow(
                [
                    feat.name,
                    feat.section,
                    feat.audience.value if feat.audience else "",
                    feat.visibility.value,
                    "\n\n".join(" ".join(p.text.split()) for p in feat.paragraphs),
                    "; ".join(
                        f"{p.id}: {' '.join(by_paragraph.get(p.id, []))}" for p in feat.paragraphs
                    ),
                ]
            )

    print(f"Written: {OUT_PATH} ({len(features)} features)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
