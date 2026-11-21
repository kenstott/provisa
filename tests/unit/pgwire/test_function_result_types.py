# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7e1c95-8d24-4a6f-b0e3-5f19c2a7d846
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A result column that calls a pg_catalog function is described by the type the catalog says the
function returns, of the overload its arguments pick, and its value written by that type's output
function (REQ-2167, PGWIRE CATALOG AND TYPES): an engine that writes JSON compactly (Trino) answers
a jsonb function as jsonb_out spaces it. A cast is described by its type; a function the catalog
does not know is described by the engine's type."""

# Requirements: REQ-2167

from __future__ import annotations

from typing import Any, Callable

import pytest
from buenavista.core import BVType

from provisa.pgwire.server import _describe_casts
from provisa.transpiler.pg_forms_json import jsonb_out
from tests.unit.pgwire.text_search_replay import outcome

_HEADLINE = (
    """SELECT pg_catalog."ts_headline"('{"a": 1, "b": [1, 2]}'::jsonb, 'cat'::tsquery) AS v"""
)


def _described(
    sql: str, served: BVType, engine_type: str
) -> tuple[BVType, int | None, list[Callable[[Any], Any]]]:
    types, oids, typmods = [served], [None], [-1]
    coerce: list[tuple[int, Callable[[Any], Any]]] = []
    _describe_casts(sql, types, oids, typmods, coerce, (engine_type,))  # type: ignore[arg-type]
    return types[0], oids[0], [c for _, c in coerce]


def _written(value: Any, coercers: list[Callable[[Any], Any]]) -> Any:
    for coercer in coercers:
        value = coercer(value)
    return value


@pytest.mark.parametrize(
    "sql",
    [
        _HEADLINE,
        """SELECT ts_headline('{"a": 1, "b": [1, 2]}'::jsonb, 'cat'::tsquery)""",
        """SELECT pg_catalog.jsonb_strip_nulls('{"a":1,"b":[1,2]}'::jsonb) AS v""",
    ],
)
def test_a_jsonb_function_from_compact_json_is_described_as_jsonb_and_spaced(sql: str):
    bvtype, oid, coercers = _described(sql, BVType.JSON, "json")
    assert (bvtype, oid) == (BVType.JSONB, 3802)
    assert _written('{"a":1,"b":[1,2]}', coercers) == '{"a": 1, "b": [1, 2]}'


def test_the_overload_the_arguments_pick_describes_the_column():
    sql = "SELECT pg_catalog.ts_headline('a fat cat', 'cat'::tsquery) AS v"
    bvtype, oid, coercers = _described(sql, BVType.TEXT, "varchar")
    assert (bvtype, oid, coercers) == (BVType.TEXT, 25, [])


def test_a_cast_column_is_described_by_its_cast():
    bvtype, oid, coercers = _described("""SELECT '{"a":1}'::jsonb AS v""", BVType.JSON, "json")
    assert (bvtype, oid, coercers) == (BVType.JSONB, 3802, [jsonb_out])


def test_a_function_the_catalog_does_not_know_is_described_by_the_engine():
    bvtype, oid, coercers = _described("SELECT no_such_function('{}') AS v", BVType.JSON, "json")
    assert (bvtype, oid, coercers) == (BVType.JSON, None, [])


def test_the_replay_writes_a_jsonb_answer_as_the_wire_does():
    """The engine replays compare the engine's rows as pgwire writes them (text_search_replay)."""
    assert outcome(_HEADLINE, "trino", lambda _: [('{"a":1,"b":[1,2]}',)]) == "ok"
