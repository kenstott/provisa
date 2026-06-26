#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: f3a1d892-7b4e-4c09-a215-6e8f0d3c1b47
#
# This source code is licensed under the Business Source License 1.1
"""Generate docs/arch/requirements.md from docs/arch/requirements.yaml.

Usage:
  python scripts/gen_requirements_md.py            # write file
  python scripts/gen_requirements_md.py --check    # diff only, exit 1 if drift
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from provisa.tools.req_schema import RequirementsFile  # type: ignore[import]

YAML_PATH = Path("docs/arch/requirements.yaml")
MD_PATH = Path("docs/arch/requirements.md")

STATUS_BADGE = {
    "complete": "",
    "in-progress": " ⚙",
    "proposed": " 💡",
    "accepted": " ✓",
    "rejected": " ✗",
}


def generate(rf: RequirementsFile) -> str:
    lines = [
        "# Requirements",
        "",
        "> Generated from `docs/arch/requirements.yaml`. Do not hand-edit.",
        "",
        "| Group | # | Category | Description | Use Case | Code | Test |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]

    for req in rf.requirements:
        badge = STATUS_BADGE.get(req.status.value, "")
        req_id = f"{req.id}{badge}"
        desc = req.description.replace("\n", " ").strip()
        use_case = (req.use_case or "").replace("\n", " ").strip()
        code = ", ".join(req.code) if req.code else ""
        tests = ", ".join(req.tests) if req.tests else ""
        lines.append(
            f"| {req.group} | {req_id} | {req.category} | {desc} | {use_case} | {code} | {tests} |"
        )

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Exit 1 if output differs from committed file"
    )
    args = parser.parse_args()

    try:
        rf = RequirementsFile.load(YAML_PATH)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    output = generate(rf)

    if args.check:
        if not MD_PATH.exists():
            print(f"FAIL: {MD_PATH} does not exist", file=sys.stderr)
            return 1
        committed = MD_PATH.read_text()
        if output != committed:
            print(f"FAIL: {MD_PATH} is out of sync with {YAML_PATH}", file=sys.stderr)
            print("Run: python scripts/gen_requirements_md.py", file=sys.stderr)
            return 1
        print(f"OK: {MD_PATH} is up to date")
        return 0

    MD_PATH.write_text(output)
    print(f"Written: {MD_PATH} ({len(rf.requirements)} requirements)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
