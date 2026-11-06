# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pg_timezone_names on the catalog: its table macro reads the engine's own zones
(system.main.pg_timezone_names()), and DuckDB 2.0 cannot bind that call once a scalar macro of the
same name exists. The catalog creates the table macros first, so both forms answer."""

# Requirements: REQ-2167

from __future__ import annotations

import duckdb

from provisa.pgwire.catalog_server_answers import macros


def _zone_macros() -> list[str]:
    return [m for m in macros() if m.startswith("CREATE MACRO pg_timezone_names(")]


def test_the_table_form_is_created_before_the_value_form():
    statements = _zone_macros()
    assert [" AS TABLE " in m for m in statements] == [True, False]


def test_both_forms_answer_on_duckdb_in_the_catalogs_order():
    con = duckdb.connect()
    try:
        for statement in _zone_macros():
            con.execute(statement)
        (rows,) = con.execute("SELECT count(*) FROM pg_timezone_names()").fetchall()[0]
        (utc,) = con.execute(
            "SELECT abbrev FROM pg_timezone_names() WHERE name = 'UTC'"
        ).fetchall()[0]
        (value,) = con.execute("SELECT count(*) FROM (SELECT pg_timezone_names() AS z)").fetchall()[
            0
        ]
    finally:
        con.close()
    assert rows > 400
    assert utc == "UTC"
    assert value == rows


def test_the_value_form_first_is_what_duckdb_refuses():
    # The order the catalog avoids: proves the test above guards something.
    value, table = sorted(_zone_macros(), key=lambda m: " AS TABLE " in m)
    con = duckdb.connect()
    try:
        con.execute(value)
        try:
            con.execute(table)
        except duckdb.BinderException as e:
            assert "pg_timezone_names" in str(e)
        else:
            raise AssertionError("DuckDB bound the table macro after the value macro")
    finally:
        con.close()
