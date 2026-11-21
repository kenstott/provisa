# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e6d20-c4a1-4f7e-8d52-a06f1e94c7b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ``pattern`` a reader of ``/debug/logs`` filters the log buffer with.

The pattern comes from the request, so it is never run by ``re``, which backtracks without limit:
it runs on the bounded matcher (:mod:`provisa.security.bounded_pattern`), refused by name when it
could backtrack catastrophically or uses what that matcher does not run, and cut off, refused by
name, when a line takes it past its step limit.
"""

# Requirements: REQ-2087

from __future__ import annotations

from provisa.api.errors import ApiError
from provisa.security.bounded_pattern import PatternRefused, StepLimitExceeded, compile_bounded


def lines_matching(pattern: str, lines: list[str]) -> list[str]:
    """The ``lines`` ``pattern`` matches anywhere in, in order."""
    try:
        compiled = compile_bounded(pattern)
    except PatternRefused as exc:
        raise ApiError(
            400, "debug_logs.pattern_refused", f"the pattern is refused: {exc}", why=str(exc)
        ) from exc
    try:
        return [line for line in lines if compiled.search(line)]
    except StepLimitExceeded as exc:
        raise ApiError(
            400,
            "debug_logs.pattern_exceeded",
            f"the pattern is refused: {exc}",
            steps=exc.steps,
            length=exc.length,
        ) from exc
