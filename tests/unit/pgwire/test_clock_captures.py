# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a61d2-7c4e-4b19-a0d5-92e6b17c4f08
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No captured answer compares a clock function's value, so none goes stale as the clock moves on,
as age(timestamp) did: a clock's answer is compared by its type and form only (REQ-2236)."""

# Requirements: REQ-2167, REQ-2236

from __future__ import annotations

import datetime
import importlib
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.unit.pgwire.pg17_forms_answers import RECORDED
from tests.unit.pgwire.pg17_forms_matrix import compared, text_of

_HERE = Path(__file__).parent
_CAPTURES = sorted(
    [*_HERE.glob("pg17_*answers.py"), _HERE / "session_zone_answers.py"],
)

#: The functions whose answer is the moment it is asked at.
_CLOCK = re.compile(
    r'(?<![\w.])(?:pg_catalog\.)?"?(?:now|statement_timestamp|clock_timestamp|transaction_timestamp'
    r'|timeofday)"?\s*\(|(?<![\w."])(?:current_timestamp|current_date|current_time|localtime'
    r"|localtimestamp)\b",
    re.IGNORECASE,
)
_AGE = re.compile(r'(?<![\w.])(?:pg_catalog\.)?"?age"?\s*\(', re.IGNORECASE)
_LITERAL = re.compile(r"'(?:[^']|'')*'")

_STATEMENT_TIMESTAMP = 'SELECT pg_catalog."statement_timestamp"() AS v'
_TIMESTAMPTZ = 1184


def _one_argument(sql: str, start: int) -> bool:
    """Whether the call whose argument list opens at ``start`` takes a single argument."""
    depth = 0
    for char in sql[start:]:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return True
        elif char == "," and depth == 1:
            return False
    raise AssertionError(f"unbalanced call in {sql!r}")


def reads_the_clock(sql: str) -> bool:
    """Whether ``sql``, outside its string literals, calls a clock function or age() of one
    argument, which is age from today."""
    bare = _LITERAL.sub("''", sql)
    if _CLOCK.search(bare):
        return True
    return any(_one_argument(bare, m.end() - 1) for m in _AGE.finditer(bare))


def _keys(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        for key, inner in value.items():
            if isinstance(key, str):
                yield key
            yield from _keys(inner)


def _statements(path: Path) -> Iterator[tuple[str, str]]:
    module = importlib.import_module(f"tests.unit.pgwire.{path.stem}")
    for name, value in vars(module).items():
        if name.isupper():
            for key in _keys(value):
                yield path.name, key


def test_the_scan_sees_each_clock_form():
    assert reads_the_clock(_STATEMENT_TIMESTAMP)
    assert reads_the_clock("SELECT now()")
    assert reads_the_clock("SELECT CURRENT_DATE")
    assert reads_the_clock('SELECT pg_catalog."age"(NULL::timestamp with time zone) AS v')
    assert not reads_the_clock("SELECT age(a, b)")
    assert not reads_the_clock("SELECT pg_catalog.\"regprocin\"('now'::cstring) AS v")
    assert not reads_the_clock("SELECT query_to_xmlschema('SELECT now()::time i', true, false, '')")


def test_every_captured_clock_answer_is_compared_by_form():
    clocked = [
        (name, sql) for path in _CAPTURES for name, sql in _statements(path) if reads_the_clock(sql)
    ]
    by_value = [
        (name, sql) for name, sql in clocked if name != "pg17_forms_answers.py" or RECORDED[sql][2]
    ]
    assert clocked, "the scan found no clock call: it reads nothing"
    assert by_value == []


@pytest.mark.parametrize("days", [0, 1, 400])
def test_statement_timestamp_answers_on_any_day(days):
    """The captured statement_timestamp() answers as PostgreSQL's at any later moment: its value
    was 2026-11-01's, and only its type and form are held."""
    oids, want, values = RECORDED[_STATEMENT_TIMESTAMP]
    moment = datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=days)
    served = [(text_of(moment, _TIMESTAMPTZ),)]
    assert served != want
    assert compared(oids, want, served, values, _STATEMENT_TIMESTAMP)


def test_statement_timestamp_form_is_still_held():
    oids, want, values = RECORDED[_STATEMENT_TIMESTAMP]
    assert not compared(oids, want, [("2026-11-02",)], values, _STATEMENT_TIMESTAMP)
    assert not compared(oids, want, [(None,)], values, _STATEMENT_TIMESTAMP)
