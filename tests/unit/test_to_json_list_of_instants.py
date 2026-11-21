# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1e7a42-9c3d-4f60-8a17-2d4e6b9c0f13
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""to_json of a list of instants on DuckDB reads as PostgreSQL's does (REQ-2171): each element the
ISO form with a whole offset, ``["2024-01-01T10:00:00+00:00"]``, where DuckDB writes
``["2024-01-01 10:00:00+00"]``. Run on the native engine; a list of anything else is left alone."""

# Requirements: REQ-2171

from __future__ import annotations

import duckdb
import pytest

from provisa.transpiler.transpile import transpile_to_duckdb

_TZ = "'2024-01-01 10:00:00+00'::timestamptz"
_HALF = "'2024-01-01 10:00:00.5+00'::timestamptz"


def _run(sql: str) -> str:
    conn = duckdb.connect()
    conn.execute("SET TimeZone = 'UTC'")
    return conn.execute(transpile_to_duckdb(sql)).fetchone()[0]


@pytest.mark.parametrize(
    "sql, rendered",
    [
        (
            f"SELECT to_json(array_agg(t)) FROM (SELECT {_TZ} AS t) s",
            '["2024-01-01T10:00:00+00:00"]',
        ),
        (f"SELECT to_json(ARRAY[{_TZ}])", '["2024-01-01T10:00:00+00:00"]'),
        (f"SELECT to_json(ARRAY[{_TZ}, NULL])", '["2024-01-01T10:00:00+00:00",null]'),
        (f"SELECT to_json(ARRAY[{_HALF}])", '["2024-01-01T10:00:00.5+00:00"]'),
        ("SELECT to_json(ARRAY['2024-01-01 10:00:00'::timestamp])", '["2024-01-01T10:00:00"]'),
        (f"SELECT to_json(json_build_object('a', {_TZ}))", '{"a":"2024-01-01T10:00:00+00:00"}'),
        (f"SELECT to_json({_TZ})", '"2024-01-01T10:00:00+00:00"'),
    ],
)
def test_instants_in_a_list_or_object_are_written_as_postgresql_writes_them(sql, rendered):
    assert _run(sql) == rendered


@pytest.mark.parametrize(
    "sql, rendered",
    [
        ("SELECT to_json(ARRAY['a', 'b'])", '["a","b"]'),
        ("SELECT to_json(ARRAY[1, 2])", "[1,2]"),
        ("SELECT to_json(ARRAY['2024-01-01 10:00:00 +00'])", '["2024-01-01 10:00:00 +00"]'),
    ],
)
def test_a_list_of_anything_else_is_left_alone(sql, rendered):
    assert _run(sql) == rendered
