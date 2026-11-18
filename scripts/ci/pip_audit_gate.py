# Copyright (c) 2026 Kenneth Stott
# Canary: 69eca12f-173b-444e-b0ab-c26b27f6942c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The coverage gate on a pip-audit JSON report of the locked dependency set.

``pip_audit_gate.py REPORT.json`` fails when the report audited nothing or skipped any
dependency. pip-audit exits 0 on a skipped dependency (one it could not look up, such as a
local version like ``torch==2.15.0+cpu`` against PyPI's JSON API), so without this gate a
package drops out of the audit silently. Its own non-zero exit already fails on a finding.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def violations(report: dict) -> list[str]:
    deps = report["dependencies"]
    if not deps:
        return ["the report audited no dependencies"]
    return [f"{d['name']}: skipped ({d['skip_reason']})" for d in deps if "skip_reason" in d]


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        raise SystemExit("usage: pip_audit_gate.py REPORT.json")
    report = json.loads(Path(argv[0]).read_text())
    problems = violations(report)
    audited = [d for d in report["dependencies"] if "skip_reason" not in d]
    print(f"audited {len(audited)} dependencies")
    for problem in problems:
        print(f"FAIL {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
