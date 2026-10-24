# Copyright (c) 2026 Kenneth Stott
# Canary: df8dc75d-fb75-42f8-b230-ad6c40f33c20
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's json_build_object / jsonb_build_object reach DuckDB as its json_object.

DuckDB registers neither PostgreSQL spelling; written SQL that builds JSON objects (the SQL
explorer, NL's SQL) failed with "Scalar Function with name jsonb_build_object does not exist".
"""

# Requirements: REQ-066

import duckdb
import pytest

from provisa.transpiler.transpile import transpile_to_duckdb


@pytest.mark.parametrize("fn", ["json_build_object", "jsonb_build_object"])
def test_a_postgres_json_object_constructor_runs_on_duckdb(fn: str) -> None:
    sql = transpile_to_duckdb(
        f"SELECT json_agg({fn}('name', t.name, 'n', t.n)) AS j "
        "FROM (VALUES ('a', 1), ('b', 2)) AS t(name, n)"
    )
    assert fn.upper() not in sql.upper()
    (value,) = duckdb.sql(sql).fetchone()
    assert '"name":"a"' in value.replace(" ", "") and '"n":2' in value.replace(" ", "")


def test_a_nested_constructor_is_rewritten_too() -> None:
    sql = transpile_to_duckdb(
        "SELECT jsonb_build_object('outer', jsonb_build_object('inner', 1)) AS j"
    )
    assert "BUILD_OBJECT" not in sql.upper()
    (value,) = duckdb.sql(sql).fetchone()
    assert value.replace(" ", "") == '{"outer":{"inner":1}}'
