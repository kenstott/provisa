# Copyright (c) 2026 Kenneth Stott
# Canary: 8d41c7a2-5e0b-4f6c-9a37-2b1e60d4c9f5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A to_json of a to_json of ... on DuckDB grows with its depth, not as a power of it (REQ-2171).

The form that tells an instant from the rest names its value more than once; written around each
level of a nested statement it multiplied the text, and the ontology reads (five levels of
to_json(MAP(...))) ran past pytest's 300 seconds."""

# Requirements: REQ-2171

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.transpiler import pg_functions
from provisa.transpiler.pg_forms_json import json_encoded
from provisa.transpiler.transpile import transpile_to_duckdb


def _nested(depth: int) -> str:
    inner = "TO_JSON(x.a)"
    for _ in range(depth):
        inner = f"TO_JSON(MAP(ARRAY['a', 'b'], ARRAY[TO_JSON(x.a), {inner}]))"
    return f"SELECT {inner} FROM x"


def test_a_nested_to_json_grows_by_a_constant_per_level():
    sizes = [len(pg_functions.after(_nested(depth))) for depth in range(1, 8)]
    steps = [b - a for a, b in zip(sizes, sizes[1:], strict=False)]
    assert max(steps) == min(steps), sizes


def test_a_to_json_the_json_form_already_tells_apart_is_not_told_again():
    said = json_encoded(sqlglot.parse_one("a", read="duckdb")).sql(dialect="duckdb")
    sql = f"SELECT {said} FROM x"
    assert pg_functions.after(sql).count("TYPEOF") == 1


_MAP_LEVEL = "TO_JSON(MAP(ARRAY['a', 'b'], ARRAY[TO_JSON(x.t), {}]))"
_LIST_OF_MAP_LEVEL = "TO_JSON(ARRAY(SELECT MAP(ARRAY['a', 'b'], ARRAY[TO_JSON(x.t), {}])))"


def _statement(level: str, depth: int) -> str:
    inner = "TO_JSON(x.t)"
    for _ in range(depth):
        inner = level.format(inner)
    return f"SELECT {inner} AS v FROM x"


@pytest.mark.parametrize("sql", ["MAP(ARRAY['a'], ARRAY[x.t])", "ARRAY(SELECT x.t)", "ARRAY[x.t]"])
def test_a_value_that_is_a_map_or_a_list_is_encoded_once(sql):
    """Neither is an instant, so the form that tells one apart would name it three times for
    nothing: at each level of a nested statement that multiplied its text."""
    encoded = json_encoded(sqlglot.parse_one(sql, read="postgres")).sql(dialect="duckdb")
    assert "TYPEOF" not in encoded.upper()
    assert encoded.upper().count("TO_JSON") == 1


@pytest.mark.parametrize("level", [_MAP_LEVEL, _LIST_OF_MAP_LEVEL])
def test_a_nested_map_s_statement_grows_by_a_constant_per_level(level):
    sizes = [len(transpile_to_duckdb(_statement(level, depth))) for depth in range(1, 7)]
    steps = [b - a for a, b in zip(sizes, sizes[1:], strict=False)]
    assert max(steps) == min(steps), sizes


def test_no_map_is_named_by_the_form_that_tells_an_instant():
    written = transpile_to_duckdb(_statement(_MAP_LEVEL, 3)).upper()
    assert "TYPEOF(MAP(" not in written


@pytest.mark.parametrize(
    ("level", "expected"),
    [
        (
            _MAP_LEVEL,
            '{"a":"2026-07-01T08:00:00","b":{"a":"2026-07-01T08:00:00","b":"2026-07-01T08:00:00"}}',
        ),
        (
            _LIST_OF_MAP_LEVEL,
            '[{"a":"2026-07-01T08:00:00","b":'
            '[{"a":"2026-07-01T08:00:00","b":"2026-07-01T08:00:00"}]}]',
        ),
    ],
)
def test_an_instant_under_nested_maps_reads_as_postgresql_writes_it(level, expected):
    con = duckdb.connect()
    con.execute("CREATE TABLE x AS SELECT TIMESTAMP '2026-07-01 08:00:00' AS t")
    answer = con.execute(transpile_to_duckdb(_statement(level, 2))).fetchone()
    assert answer is not None
    assert answer[0] == expected


def _to_json_of_to_json(depth: int) -> str:
    inner = "x.t"
    for _ in range(depth):
        inner = f"TO_JSON({inner})"
    return f"SELECT {inner} AS v FROM x"


def test_a_to_json_of_a_to_json_grows_by_a_constant_per_level():
    sizes = [len(transpile_to_duckdb(_to_json_of_to_json(depth))) for depth in range(2, 8)]
    steps = [b - a for a, b in zip(sizes, sizes[1:], strict=False)]
    assert max(steps) == min(steps), sizes


def test_an_instant_under_a_to_json_of_a_to_json_reads_as_postgresql_writes_it():
    con = duckdb.connect()
    con.execute("CREATE TABLE x AS SELECT TIMESTAMP '2026-07-01 08:00:00' AS t")
    answer = con.execute(transpile_to_duckdb(_to_json_of_to_json(3))).fetchone()
    assert answer is not None
    assert answer[0] == '"2026-07-01T08:00:00"'
