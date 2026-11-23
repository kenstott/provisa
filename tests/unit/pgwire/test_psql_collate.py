# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4b2e61-7a3f-4c85-b0e2-6f1d8a5c3e97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2204: a collation after COLLATE is a collation's name, with PostgreSQL's meaning. psql's
pattern statements (\\dt, \\dv, \\di, \\df, \\d+: the BI replay's group G1, statements 4, 5, 6, 8,
9, 17 and 29 of scripts/conformance/bi_tools/psql.yaml) end each pattern in
``COLLATE pg_catalog.default``, which governance refused as a column naming no relation. On an
engine with no collations, the default collation and "C" mean no change."""

# Requirements: REQ-2204

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot
import yaml
from sqlglot import exp

import provisa.transpiler  # noqa: F401 -- pg_syntax's reading of PostgreSQL
from provisa.compiler.sql_validator import _check_column_visibility
from provisa.compiler.stage2 import GovernanceContext
from provisa.transpiler.transpile import transpile

_PSQL = Path(__file__).resolve().parents[3] / "scripts/conformance/bi_tools/psql.yaml"
_G1 = (4, 5, 6, 8, 9, 17, 29)
_STATEMENTS = {s["id"]: s["sql"] for s in yaml.safe_load(_PSQL.read_text())["statements"]}
_ENGINES = ("duckdb", "trino", "clickhouse")


def _psql(statement: int) -> str:
    sql = _STATEMENTS[statement]
    assert "COLLATE pg_catalog.default" in sql
    return sql


@pytest.mark.parametrize("statement", _G1)
def test_psql_collation_is_no_column(statement: int) -> None:
    tree = sqlglot.parse_one(_psql(statement), read="postgres")
    collations = list(tree.find_all(exp.Collate))
    assert collations
    for collate in collations:
        assert not list(collate.expression.find_all(exp.Column))
        assert collate.expression.sql(dialect="postgres") == "pg_catalog.default"


@pytest.mark.parametrize("statement", _G1)
def test_psql_collation_names_no_relation_refused(statement: int) -> None:
    tree = sqlglot.parse_one(_psql(statement), read="postgres")
    gov = GovernanceContext(visible_columns={}, table_map={}, all_columns={})
    refused = [v.message for v in _check_column_visibility(tree, gov)]
    assert not [m for m in refused if "pg_catalog.default" in m or "no relation in scope" in m]


@pytest.mark.parametrize("statement", _G1)
def test_psql_statement_writes_back_its_collation_on_postgres(statement: int) -> None:
    written = transpile(_psql(statement), "postgres")
    assert written.count("COLLATE pg_catalog.default") == _psql(statement).count(
        "COLLATE pg_catalog.default"
    )


@pytest.mark.parametrize("engine", _ENGINES)
@pytest.mark.parametrize("statement", _G1)
def test_psql_default_collation_is_no_change_on_an_engine(statement: int, engine: str) -> None:
    sql = _psql(statement)
    written = transpile(sql, engine)
    assert "COLLATE" not in written.upper()
    assert transpile(sql.replace(" COLLATE pg_catalog.default", ""), engine) == written


@pytest.mark.parametrize(
    ("collation", "postgres"),
    [
        ("pg_catalog.default", "pg_catalog.default"),
        ('pg_catalog."default"', 'pg_catalog."default"'),
        ('pg_catalog."C"', 'pg_catalog."C"'),
        ('"C"', '"C"'),
        ('"POSIX"', '"POSIX"'),
        ("C", "C"),
    ],
)
def test_collation_name_read_and_written(collation: str, postgres: str) -> None:
    sql = f"SELECT a COLLATE {collation} FROM t WHERE b = 'x' COLLATE {collation} ORDER BY 1"
    tree = sqlglot.parse_one(sql, read="postgres")
    assert not [c for c in tree.find_all(exp.Column) if c.name not in ("a", "b")]
    assert transpile(sql, "postgres") == (
        f"SELECT a COLLATE {postgres} FROM t WHERE b = 'x' COLLATE {postgres} ORDER BY 1"
    )
    for engine in _ENGINES:
        assert transpile(sql, engine) == transpile(
            "SELECT a FROM t WHERE b = 'x' ORDER BY 1", engine
        )


def test_other_collation_left_for_the_engine() -> None:
    sql = 'SELECT a COLLATE "de_DE" FROM t'
    for engine in _ENGINES:
        assert 'COLLATE "de_DE"' in transpile(sql, engine)


def test_collation_of_another_schema_left_as_written() -> None:
    sql = 'SELECT a COLLATE myschema."C" FROM t'
    assert not list(sqlglot.parse_one(sql, read="postgres").find_all(exp.Column))[1:]
    for engine in _ENGINES:
        assert 'COLLATE myschema."C"' in transpile(sql, engine)
