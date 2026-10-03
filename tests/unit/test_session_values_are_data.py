# Copyright (c) 2026 Kenneth Stott
# Canary: 797cface-c4d9-4fd8-9015-8744b74239f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A request's session value is data in the row-level filter it fills.

A row filter reads a session value through ``current_setting('provisa.<name>')``; on an engine
without that function the value is written into the engine's statement. On an engine that reads
a backslash inside a string as an escape (ClickHouse, Snowflake, BigQuery, Databricks), a value
holding a backslash and a quote stays one value: the filter keeps exactly its one comparison."""

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.federation.engine import build_engine
from provisa.pgwire._pipeline import _resolve_session_settings

GOVERNED = 'SELECT "id" FROM "orders" WHERE "tenant" = current_setting(\'provisa.tenant\', true)'
HOSTILE = ["x\\' OR 1=1 --", "x' OR 1=1 --", "it's", "back\\slash"]


@pytest.mark.parametrize("engine", ["clickhouse", "snowflake", "duckdb", "trino"])
@pytest.mark.parametrize("value", HOSTILE)
def test_a_session_value_is_one_value_of_its_one_comparison(engine, value):
    backend = build_engine(engine).backend
    physical = backend.transpile_physical(GOVERNED)
    resolved = _resolve_session_settings(physical, {"tenant": value}, backend.dialect)
    (statement,) = [s for s in sqlglot.parse(resolved, read=backend.dialect) if s is not None]
    where = statement.args["where"].this
    assert isinstance(where, exp.EQ), resolved
    assert where.expression.this == value, resolved
