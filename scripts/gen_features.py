#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: a9c2e574-3f1d-4b8a-c317-5d7e0f2a9b63
#
# This source code is licensed under the Business Source License 1.1
"""Generate tests/features/REQ-NNN.feature from docs/arch/requirements.yaml.

One .feature file per behavioral requirement that has a scenario.

Usage:
  python scripts/gen_features.py            # write files
  python scripts/gen_features.py --check    # exit 1 if any file differs or is missing
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from provisa.tools.req_schema import RequirementsFile, ReqType, Status  # type: ignore[import]

YAML_PATH = Path("docs/arch/requirements.yaml")
FEATURES_DIR = Path("tests/features")

_SKIP_STATUSES = {Status.proposed, Status.rejected}


def feature_content(req_id: str, category: str, description: str, scenario: str) -> str:
    desc_short = description[:120].replace("\n", " ").strip()
    if len(description) > 120:
        desc_short += "…"
    lines = [
        "# Generated from docs/arch/requirements.yaml. Do not hand-edit.",
        f"Feature: {req_id} — {category}",
        f"  # {desc_short}",
        "",
        f"  Scenario: {req_id} default behaviour",
    ]
    for line in scenario.rstrip().splitlines():
        lines.append(f"    {line}" if line.strip() else "")
    lines.append("")
    return "\n".join(lines)


def generate_all(rf: RequirementsFile) -> dict[Path, str]:
    files: dict[Path, str] = {}
    for req in rf.requirements:
        if req.type != ReqType.behavioral:
            continue
        if req.status in _SKIP_STATUSES:
            continue
        if not req.scenario:
            continue
        path = FEATURES_DIR / f"{req.id}.feature"
        files[path] = feature_content(req.id, req.category, req.description, req.scenario)
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Exit 1 if output differs from committed files"
    )
    args = parser.parse_args()

    try:
        rf = RequirementsFile.load(YAML_PATH)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    files = generate_all(rf)
    errors: list[str] = []

    if args.check:
        for path, content in files.items():
            if not path.exists():
                errors.append(f"MISSING: {path}")
            elif path.read_text() != content:
                errors.append(f"DRIFT: {path}")
        if errors:
            for e in errors:
                print(e, file=sys.stderr)
            print("Run: python scripts/gen_features.py", file=sys.stderr)
            return 1
        print(f"OK: {len(files)} feature files up to date")
        return 0

    FEATURES_DIR.mkdir(parents=True, exist_ok=True)
    for path, content in files.items():
        path.write_text(content)
    print(f"Written: {len(files)} feature files in {FEATURES_DIR}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
