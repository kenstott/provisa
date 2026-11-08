# Copyright (c) 2026 Kenneth Stott
# Canary: 9b2e6c41-7d18-4f35-a0c9-3e5d8b1f2a76
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A VOID command (REQ-2001, amended 2026-10-28): one that declares no outputs, its
``output_columns`` an empty list. Calling it runs it and answers an empty result with no columns,
as a Cypher VOID procedure does, on every surface that calls commands. Outputs left undeclared
(``output_columns`` None) are not VOID: that command's output is unvalidated (REQ-1159)."""

# Requirements: REQ-2001

from __future__ import annotations

import json
from typing import Any


def is_void(command: dict) -> bool:
    """Whether ``command`` declares no outputs."""
    declared: Any = command.get("output_columns")
    if isinstance(declared, str):  # a JSON column read as its text
        declared = json.loads(declared)
    return declared is not None and len(declared) == 0


def check(name: str, output_columns: list | None, returns: str, return_schema: Any) -> None:
    """A VOID command names no return besides: a ``returns`` table or a ``return_schema`` would
    say it answers rows."""
    if output_columns is None or output_columns:
        return
    if returns or return_schema:
        named = "returns" if returns else "return_schema"
        raise ValueError(
            f"command {name!r} is VOID (it declares no output_columns) and also names a "
            f"{named}: a VOID command answers no rows"
        )
