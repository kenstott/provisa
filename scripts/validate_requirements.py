#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: b82e1f47-9c3d-4a18-d561-7e4f0b3c5d29
#
# This source code is licensed under the Business Source License 1.1
"""Validate docs/arch/requirements.yaml against the Pydantic schema.

Exit codes:
  0 — valid
  1 — validation errors
  2 — file not found or YAML parse error

Usage:
  python scripts/validate_requirements.py
  python scripts/validate_requirements.py --coverage-check
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running from repo root without install
sys.path.insert(0, str(Path(__file__).parent.parent))

from provisa.tools.req_schema import Priority, RequirementsFile, ReqType, Status  # type: ignore[import]

REQUIREMENTS_YAML = Path("docs/arch/requirements.yaml")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--coverage-check",
        action="store_true",
        help="Fail if any MUST complete behavioral/constraint REQ has no tests",
    )
    args = parser.parse_args()

    if not REQUIREMENTS_YAML.exists():
        print(f"ERROR: {REQUIREMENTS_YAML} not found", file=sys.stderr)
        return 2

    try:
        rf = RequirementsFile.load(REQUIREMENTS_YAML)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    errors: list[str] = []

    if args.coverage_check:
        for req in rf.requirements:
            if (
                req.priority == Priority.MUST
                and req.status == Status.complete
                and req.type in {ReqType.behavioral, ReqType.constraint}
                and not req.tests
            ):
                errors.append(f"{req.id}: MUST complete {req.type.value} has no tests")

    if errors:
        for e in errors:
            print(f"FAIL: {e}", file=sys.stderr)
        return 1

    total = len(rf.requirements)
    by_status = {}
    for req in rf.requirements:
        by_status[req.status.value] = by_status.get(req.status.value, 0) + 1

    print(f"OK: {total} requirements valid")
    for status, count in sorted(by_status.items()):
        print(f"  {status}: {count}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
