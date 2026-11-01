# Copyright (c) 2026 Kenneth Stott
# Canary: 5d08e2b7-1f94-4c36-b7a2-c06e93d1f485
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Replays the recorded answers of the text search family (REQ-2167, REQ-2171): an engine's answer
is PostgreSQL 17's, or PostgreSQL's own error, or a refusal by name. Nothing else passes."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import re
import struct
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from provisa.core.pg_error import PostgresError, from_engine
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.pg_forms_types import REFUSED
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.pg17_text_search_answers import RECORDED

_NO_FORM = "has no form on"
_BOOL, _FLOAT4 = 16, 700
_NEEDS_QUOTES = re.compile(r'[\s,{}"\\]')
_CALLED = re.compile(r'pg_catalog\."(\w+)"')


def function_of(sql: str) -> str:
    found = _CALLED.search(sql)
    assert found is not None, sql
    return found.group(1)


def _item(value: Any) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    if text == "" or text.upper() == "NULL" or _NEEDS_QUOTES.search(text):
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def _float4(value: float) -> str:
    """float4out: the fewest digits that read back as the same float4."""
    held = struct.unpack("f", struct.pack("f", value))[0]
    for digits in range(1, 10):
        written = f"{held:.{digits}g}"
        if struct.unpack("f", struct.pack("f", float(written)))[0] == held:
            return written
    return f"{held:.9g}"


def _text(value: Any, oid: int) -> str | None:
    """A value as the wire writes one of the type ``oid``."""
    if value is None:
        return None
    if oid == _BOOL:
        return "t" if value in (True, 1) else "f"
    if isinstance(value, (list, tuple)):
        return "{" + ",".join(_item(v) for v in value) + "}"
    if oid == _FLOAT4:
        return _float4(float(value))
    return str(value)


def _refused(error: Exception) -> bool:
    return isinstance(error, FunctionRefused) or REFUSED in str(error) or _NO_FORM in str(error)


def outcome(sql: str, dialect: str, run: Callable[[str], Iterable[Sequence[Any]]]) -> str:
    """ "ok", "refused", or what differs from PostgreSQL's recorded answer."""
    kind, oid, second = RECORDED[sql]
    try:
        rows = run(transpile(sql, dialect))
    except Exception as error:  # noqa: BLE001 -- each engine's client raises its own
        if _refused(error):
            return "refused"
        said = error if isinstance(error, PostgresError) else from_engine(error)
        if kind == "err" and said is not None and said.sqlstate == oid and str(said) == second:
            return "ok"
        return f"{sql}: raised {str(error)[:300]}, PostgreSQL {kind} {oid} {str(second)[:120]}"
    if kind == "err":
        return f"{sql}: answered, PostgreSQL raised {oid} {second}"
    got = sorted((tuple(_text(c, oid) for c in row) for row in rows), key=str)
    want = sorted(second, key=str)
    return "ok" if got == want else f"{sql}: {got[:3]}, PostgreSQL {want[:3]}"


def problems(
    dialect: str, run: Callable[[str], Iterable[Sequence[Any]]]
) -> tuple[list[str], dict[str, int]]:
    """What differs, and how many statements of each function were refused."""
    wrong: list[str] = []
    refused: dict[str, int] = {}
    for sql in RECORDED:
        said = outcome(sql, dialect, run)
        if said == "refused":
            name = function_of(sql)
            refused[name] = refused.get(name, 0) + 1
        elif said != "ok":
            wrong.append(said)
    return wrong, refused
