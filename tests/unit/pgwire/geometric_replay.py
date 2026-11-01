# Copyright (c) 2026 Kenneth Stott
# Canary: 8e41c7a3-6d20-4f1b-9c75-2ab0e6d4f193
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Replays the recorded answers of the geometric family (REQ-2167, REQ-2171): an engine's answer is
PostgreSQL 17's, or PostgreSQL's own error, or a refusal by name. Nothing else passes."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.pg_forms_types import REFUSED
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.pg17_geometric_answers import RECORDED

_CALLED = re.compile(r"SELECT (\w+)\(")


def function_of(sql: str) -> str:
    found = _CALLED.match(sql)
    assert found is not None, sql
    return found.group(1)


def same(got: Any, want: str | None) -> bool:
    """An engine's value against PostgreSQL's text: a double compares as its number."""
    if isinstance(got, bool):
        return ("t" if got else "f") == want
    if isinstance(got, float):
        return want is not None and got == float(want)
    return (None if got is None else str(got)) == want


def outcome(sql: str, dialect: str, run: Callable[[str], Any]) -> str:
    """ "ok", "refused", or what differs from PostgreSQL's recorded answer."""
    want = RECORDED[sql][1]
    raises = (want or "").startswith("ERROR ")
    try:
        got = run(transpile(sql, dialect))
    except Exception as error:  # noqa: BLE001 -- each engine's client raises its own
        if isinstance(error, FunctionRefused) or REFUSED in str(error):
            return "refused"
        if raises and f"{(want or '').split()[1]}:" in str(error):
            return "ok"
        return f"{sql}: raised {str(error)[:300]}, PostgreSQL {want}"
    if raises:
        return f"{sql}: answered {got}, PostgreSQL {want}"
    return "ok" if same(got, want) else f"{sql}: {got!r}, PostgreSQL {want}"


#: The statements of each function: one engine takes minutes over all of them.
BY_FUNCTION: dict[str, list[str]] = {}
for _sql in RECORDED:
    BY_FUNCTION.setdefault(function_of(_sql), []).append(_sql)


def problems(function: str, dialect: str, run: Callable[[str], Any]) -> tuple[list[str], list[str]]:
    """What differs among a function's statements, and those that were refused."""
    said = {sql: outcome(sql, dialect, run) for sql in BY_FUNCTION[function]}
    return (
        [s for s in said.values() if s not in ("ok", "refused")],
        [sql for sql, s in said.items() if s == "refused"],
    )
