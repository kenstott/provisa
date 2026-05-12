#!/usr/bin/env python3
# PostToolUse — Bash
# Blocks continuation when test output contains failures.
# "Pre-existing" is not a valid disposition — every failure must be
# investigated, fixed, or filed as a GitHub issue with a skip referencing it.

import json
import re
import sys

d = json.load(sys.stdin)
output = d.get("tool_response", {})
if isinstance(output, dict):
    output = output.get("output", "") or output.get("stdout", "")
if not isinstance(output, str):
    output = str(output)

FAILURE_PATTERNS = [
    r"\d+ failed",
    r"^FAILED ",
    r"\d+ error[s]? during collection",
    r"^\s*\d+ failed,",
    r"✗ ",
    r"\d+ failed\b.*playwright",
]

if any(re.search(p, output, re.MULTILINE | re.IGNORECASE) for p in FAILURE_PATTERNS):
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PostToolUse",
                    "permissionDecision": "block",
                    "permissionDecisionReason": (
                        "Test failures detected. Investigate and resolve every failure "
                        "before continuing. Options: fix the code, fix the test, install "
                        "the missing dependency, or file a GitHub issue and add "
                        "pytest.mark.skip(reason='GH#<n>'). 'Pre-existing' is not a valid "
                        "disposition."
                    ),
                }
            }
        )
    )
    sys.exit(2)
