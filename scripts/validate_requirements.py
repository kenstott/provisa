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
  python scripts/validate_requirements.py --orphan-check
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
        help="Fail if any MUST/SHOULD complete behavioral/constraint REQ has no tests",
    )
    parser.add_argument(
        "--orphan-check",
        action="store_true",
        help="Fail if any test file under tests/ or provisa-ui/e2e/ is not referenced by any requirement",
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
                req.priority not in {Priority.MUST, Priority.SHOULD}
                or req.status != Status.complete
            ):
                continue
            if req.type not in {ReqType.behavioral, ReqType.constraint}:
                continue
            tests = req.tests or []
            unit = [t for t in tests if t.startswith("tests/unit/")]
            integration = [t for t in tests if t.startswith("tests/integration/")]
            e2e = [t for t in tests if t.startswith("provisa-ui/e2e/")]
            if not unit and req.unit_test is not False:
                errors.append(f"{req.id}: MUST complete {req.type.value} has no unit test")
            if (
                req.integration_test
                and req.integration_test.value == "required"
                and not integration
            ):
                errors.append(f"{req.id}: integration_test=required but no tests/integration/ path")
            if req.e2e and not e2e:
                errors.append(f"{req.id}: e2e=true but no provisa-ui/e2e/ path")

    if args.orphan_check:
        referenced: set[str] = set()
        for req in rf.requirements:
            for t in req.tests or []:
                referenced.add(t)

        test_roots = [Path("tests"), Path("provisa-ui/e2e")]
        for root in test_roots:
            if not root.exists():
                continue
            for f in (
                sorted(root.rglob("test_*.py"))
                if root.name == "tests"
                else sorted(root.rglob("*.spec.ts"))
            ):
                rel = str(f)
                if rel not in referenced:
                    errors.append(f"ORPHAN: {rel}")

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
