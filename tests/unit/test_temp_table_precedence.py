# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session's temporary table named by its own name is the session's, ahead of a model table of
the same name, at every step a statement's table is resolved (REQ-615) -- as PostgreSQL's pg_temp
comes first and as the validator already reads it; a model table is still reached by its schema.
Before, the validator took the bare name for the temporary table while the resolver and the
physical rewrite took it for the model's, so the statement read the model table."""

# Requirements: REQ-615

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from provisa.compiler import temp_tables
from provisa.compiler.naming import table_id_of
from provisa.compiler.sql_rewrite import normalize_table_refs


def _table(sql: str) -> exp.Table:
    return next(sqlglot.parse_one(sql, read="postgres").find_all(exp.Table))


def _in_session_holding(name: str):
    session = temp_tables.TempSession()
    session.tables[name] = temp_tables.TempTable(name, [("id", "bigint")])
    return temp_tables.bind(session)


_MAP = {"orders": 1, "sales.orders": 1}


def test_a_bare_name_the_session_holds_is_its_temporary_table():
    token = _in_session_holding("orders")
    try:
        assert table_id_of(_table("SELECT * FROM orders"), _MAP) is None
        assert table_id_of(_table("SELECT * FROM sales.orders"), _MAP) == 1
    finally:
        temp_tables.unbind(token)
    assert table_id_of(_table("SELECT * FROM orders"), _MAP) == 1  # outside the session


def test_the_physical_rewrite_leaves_the_temporary_table_named_by_its_own_name():
    from tests.unit.test_flightsql_catalog import _context  # orders and customers, in sales

    ctx = _context()
    token = _in_session_holding("orders")
    try:
        held = normalize_table_refs("SELECT id FROM orders", ctx)
        model = normalize_table_refs("SELECT id FROM sales.orders", ctx)
    finally:
        temp_tables.unbind(token)
    assert '"public"' not in held
    assert '"public"."orders"' in model
    assert '"public"."orders"' in normalize_table_refs("SELECT id FROM orders", ctx)
