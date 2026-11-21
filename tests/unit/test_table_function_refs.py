# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3f6c28-1e47-4b5d-8c02-d7e5b1a49f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A function in FROM, its schema named, is no table reference (REQ-641, REQ-2167): the table
references are qualified and quoted around it, and it is left as written, its column definition
list with it. Rebuilt as a table, ``pg_catalog.json_to_recordset('[]') AS t(v integer)`` was
``"pg_catalog"."" AS "t"``, which the engine refused ("zero-length delimited identifier")."""

# Requirements: REQ-641, REQ-2167

from __future__ import annotations

import pytest

from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.sql_rewrite import normalize_table_refs


def _context() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "orders": TableMeta(
            table_id=1,
            field_name="orders",
            type_name="Orders",
            source_id="sales-pg",
            catalog_name="sales_pg",
            schema_name="public",
            table_name="orders",
            domain_id="sales",
        )
    }
    return ctx


@pytest.mark.parametrize(
    "call",
    [
        """pg_catalog."JSON_TO_RECORDSET"('[]') AS t(v INT)""",
        """pg_catalog."JSONB_TO_RECORDSET"('[]') AS t(v INT, w TEXT)""",
        "pg_catalog.UNNEST(ARRAY[1, 2]) AS u(n)",
    ],
)
def test_a_schema_named_function_in_from_is_left_as_written(call: str):
    written = normalize_table_refs(f"SELECT * FROM {call}", _context())
    assert written == f"SELECT * FROM {call}"


def test_a_table_beside_the_function_is_still_qualified():
    written = normalize_table_refs(
        """SELECT o.id, t.v FROM orders AS o, pg_catalog."JSON_TO_RECORDSET"('[]') AS t(v INT)""",
        _context(),
    )
    assert written == (
        'SELECT "o".id, t.v FROM "public"."orders" AS "o", '
        """pg_catalog."JSON_TO_RECORDSET"('[]') AS t(v INT)"""
    )
