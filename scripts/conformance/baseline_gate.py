#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Whether a conformance report whose runner is not Python fails its job (REQ-2167, amended
2026-10-27): the report's ``failed`` cases on a surface at its baseline (by_design.AT_BASELINE).
For the GraphQL over HTTP audit, whose report (graphql_http_audit.mjs) counts failed MUST audits.

Usage: baseline_gate.py <surface> <report.json>
"""

# Requirements: REQ-2167

from __future__ import annotations

import json
import sys
from pathlib import Path

import by_design


def main(argv: list[str]) -> int:
    surface, report = argv
    doc = json.loads(Path(report).read_text())
    return 1 if by_design.baseline_failed(surface, doc["failed"]) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
