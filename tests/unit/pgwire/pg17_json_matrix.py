# Copyright (c) 2026 Kenneth Stott
# Canary: b6a1f0d3-52c7-4e19-8d3b-7c04e2a91f65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What each engine does with the statements of pg17_json_answers: replayed, never derived. A
statement not listed answers as PostgreSQL 17 did. REFUSED is by name; DIFFERS is an answer PostgreSQL
did not give; ERROR is the engine's own refusal of what PostgreSQL answered, or of what it refused."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from collections import Counter
from typing import Any

SERVED, REFUSED, DIFFERS, ERROR = "served", "refused", "differs", "error"


def outcome(recorded: tuple, rows: list[tuple] | None, refused: bool) -> str:
    """One statement's outcome: ``rows`` its answer as text, None for an engine error. Rows compare
    as a multiset: no statement has an ORDER BY of its own to hold them to."""
    if refused:
        return REFUSED
    if recorded[0] == "ERROR":
        return ERROR if rows is None else DIFFERS
    if rows is None:
        return ERROR
    return SERVED if Counter(map(repr, rows)) == Counter(map(repr, recorded[1])) else DIFFERS


def text_of(value: Any) -> str | None:
    """A cell of an engine's driver as PostgreSQL's text."""
    if value is None or isinstance(value, str):
        return value
    return {True: "t", False: "f"}[value] if isinstance(value, bool) else str(value)


MATRIX: dict[str, dict[str, str]] = {"duckdb": {}, "trino": {}, "clickhouse": {}}
