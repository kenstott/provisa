# Copyright (c) 2026 Kenneth Stott
# Canary: f4863de7-7ed2-4adc-b4d5-8c72fe483dcb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-778 (amended 2026-10-24): what a Cypher read cannot answer is answered with a code.

- ``toInteger``, ``toFloat`` and ``toBoolean`` of a value that is no such thing are null, as
  Neo4j's are: the semantic SQL keeps TRY_CAST through every rewrite, and each engine writes its
  own null-on-failure cast (a PostgreSQL target, ``pg_input_is_valid``).
- An answer holding a number JSON cannot carry (Infinity, NaN) is a 400 naming it.
- A statement the engine refused -- a bound value it cannot convert, among others -- is a 400
  cypher.engine_refused on every engine, not only Trino; a lost engine is a 503."""

# Requirements: REQ-778

from __future__ import annotations

import json
import math

import duckdb
import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.cypher.parser import parse_cypher
from provisa.gql.label_map import CypherLabelMap
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile, transpile_to_duckdb
from tests.unit.test_cypher_junction_relationships import _term_node


@pytest.fixture
def terms():
    lm = CypherLabelMap(nodes={"GlossaryTerm": _term_node()}, relationships={})
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS provisa_admin")
    con.execute("CREATE SCHEMA provisa_admin.public")
    con.execute("CREATE TABLE provisa_admin.public.glossary_terms (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO provisa_admin.public.glossary_terms VALUES (1, 'ab'), (2, '12')")
    return lm, con


def _semantic(lm, cypher: str) -> str:
    """The read's semantic SQL as the pipeline holds it: rendered, then governed (re-rendered)."""
    sql_ast, _, _ = cypher_to_sql(parse_cypher(cypher), lm, {})
    return apply_governance(sql_ast.sql(dialect="postgres"), GovernanceContext(), None, [])


def _rows(terms, cypher: str) -> list[tuple]:
    lm, con = terms
    return sorted(con.execute(transpile_to_duckdb(_semantic(lm, cypher))).fetchall(), key=repr)


@pytest.mark.parametrize(
    "fn, expected",
    [
        ("toInteger(t.name) + 1", [(13,), (None,)]),
        ("toFloat(t.name)", [(12.0,), (None,)]),
        ("toBoolean(t.name)", [(None,), (None,)]),
    ],
)
def test_a_conversion_of_a_value_that_is_no_such_thing_is_null(terms, fn, expected):
    assert _rows(terms, f"MATCH (t:GlossaryTerm) RETURN {fn} AS v") == expected


def test_the_semantic_sql_keeps_its_try_cast_through_governance(terms):
    lm, _ = terms
    assert "TRY_CAST(" in _semantic(lm, "MATCH (t:GlossaryTerm) RETURN toInteger(t.name) AS v")


@pytest.mark.parametrize(
    "target, written",
    [
        ("trino", "TRY_CAST(t.name AS BIGINT)"),
        ("duckdb", "TRY_CAST(t.name AS BIGINT)"),
        ("bigquery", "SAFE_CAST(t.name AS INT64)"),
        (
            "postgres",
            "CASE WHEN PG_INPUT_IS_VALID(CAST(t.name AS TEXT), 'BIGINT') "
            "THEN CAST(t.name AS BIGINT) END",
        ),
    ],
)
def test_each_target_writes_its_own_null_on_failure_cast(target, written):
    assert written in transpile("SELECT TRY_CAST(t.name AS BIGINT) FROM t", target)


_CASTS = (
    "SELECT TRY_CAST(t.name AS BIGINT), TRY_CAST(t.name AS DOUBLE PRECISION), "
    "TRY_CAST(t.name AS BOOLEAN), TRY_CAST(t.age AS VARCHAR) FROM t"
)


@pytest.mark.parametrize(
    "target, written",
    [
        # Snowflake's TRY_CAST takes text only: the value is read as text first.
        (
            "snowflake",
            'SELECT TRY_CAST(CAST("t"."name" AS VARCHAR) AS BIGINT), '
            'TRY_CAST(CAST("t"."name" AS VARCHAR) AS DOUBLE), '
            'TRY_CAST(CAST("t"."name" AS VARCHAR) AS BOOLEAN), '
            'TRY_CAST(CAST("t"."age" AS VARCHAR) AS VARCHAR) FROM "t"',
        ),
        # MySQL has none, and its CAST of 'ab' is 0: the value is converted where it reads as
        # one, else null.
        (
            "mysql",
            "SELECT CASE WHEN REGEXP_LIKE(TRIM(t.name), '^[+-]?[0-9]+$') "
            "THEN CAST(t.name AS SIGNED) END, "
            "CASE WHEN REGEXP_LIKE(TRIM(t.name), '^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?$') "
            "THEN CAST(t.name AS DOUBLE) END, "
            "CASE LOWER(TRIM(t.name)) WHEN 'true' THEN TRUE WHEN 'false' THEN FALSE END, "
            "CAST(t.age AS CHAR) FROM t",
        ),
        (
            "clickhouse",
            "SELECT accurateCastOrNull(t.name, 'Int64'), accurateCastOrNull(t.name, 'Float64'), "
            "accurateCastOrNull(t.name, 'Bool'), accurateCastOrNull(t.age, 'String') FROM t",
        ),
        (
            "spark",
            "SELECT TRY_CAST(t.name AS BIGINT), TRY_CAST(t.name AS DOUBLE), "
            "TRY_CAST(t.name AS BOOLEAN), TRY_CAST(t.age AS STRING) FROM t",
        ),
        (
            "tsql",
            "SELECT TRY_CAST(t.name AS BIGINT), TRY_CAST(t.name AS FLOAT), "
            "TRY_CAST(t.name AS BIT), TRY_CAST(t.age AS VARCHAR) FROM t",
        ),
    ],
)
def test_a_target_without_a_null_on_failure_cast_of_its_own_still_answers_null(target, written):
    assert transpile(_CASTS, target) == written


def test_mysql_s_null_on_failure_casts_answer_null_where_the_value_does_not_convert():
    # The MySQL form's tests, run by DuckDB's own regexp (the same POSIX classes).
    con = duckdb.connect()
    for value, integer, real, boolean in [
        ("12", 12, 12.0, None),
        (" -7 ", -7, -7.0, None),
        ("1.5e3", None, 1500.0, None),
        ("ab", None, None, None),
        ("TRUE", None, None, True),
    ]:
        assert con.execute(
            "SELECT CASE WHEN regexp_full_match(TRIM(?), '[+-]?[0-9]+') THEN CAST(TRIM(?) AS BIGINT) END, "
            "CASE WHEN regexp_full_match(TRIM(?), '[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?') "
            "THEN CAST(TRIM(?) AS DOUBLE) END, "
            "CASE LOWER(TRIM(?)) WHEN 'true' THEN TRUE WHEN 'false' THEN FALSE END",
            [value] * 5,
        ).fetchone() == (integer, real, boolean)


def test_an_answer_holding_a_number_json_cannot_carry_is_400_naming_it():
    from provisa.api.rest.cypher_router import _serialize_rows

    rows = [{"v": 1.5}, {"v": math.inf}]
    refused = _serialize_rows(rows, {}, lambda r, _graph_vars: r, lambda r: r)

    assert not isinstance(refused, tuple) and refused.status_code == 400
    assert json.loads(bytes(refused.body)) == {
        "detail": "The answer holds a number JSON cannot carry: v is Infinity",
        "code": "cypher.non_finite_number",
        "params": {"column": "v", "value": "Infinity"},
    }


def test_a_division_by_zero_reaches_the_answer_as_infinity(terms):
    # What the engine answers t.id / 0 with -- the value the 400 above names.
    assert _rows(terms, "MATCH (t:GlossaryTerm) RETURN t.id / 0 AS v") == [
        (math.inf,),
        (math.inf,),
    ]


def _backend(name: str):
    from provisa.federation import (
        bigquery_backend,
        clickhouse_backend,
        databricks_backend,
        duckdb_backend,
        mssql_warehouse_backend,
        pg_backend,
        snowflake_backend,
        sqlalchemy_backend,
    )

    return {
        "duckdb": duckdb_backend.DuckDBBackend,
        "pg": pg_backend.PgBackend,
        "clickhouse": clickhouse_backend.ClickHouseBackend,
        "bigquery": bigquery_backend.BigQueryBackend,
        "databricks": databricks_backend.DatabricksBackend,
        "snowflake": snowflake_backend.SnowflakeBackend,
        "fabric": mssql_warehouse_backend.FabricBackend,
        "synapse": mssql_warehouse_backend.SynapseBackend,
        "sqlalchemy": sqlalchemy_backend.SqlAlchemyBackend,
    }[name](None)


def _errors() -> list[tuple[str, BaseException, str]]:
    import clickhouse_connect.driver.exceptions as ch
    import databricks.sql.exc as dbx
    import google.api_core.exceptions as gapi
    import psycopg2
    import pyodbc
    import snowflake.connector.errors as sf
    import sqlalchemy.exc as sa

    return [
        (
            "duckdb",
            duckdb.ConversionException("Could not convert string 'seven' to INT32"),
            "query",
        ),
        ("duckdb", duckdb.BinderException("Referenced column x not found"), "query"),
        ("duckdb", duckdb.IOException("Could not connect to the source"), "connection"),
        ("pg", psycopg2.DataError("invalid input syntax for type integer"), "query"),
        ("pg", psycopg2.ProgrammingError("syntax error"), "query"),
        ("pg", psycopg2.OperationalError("server closed the connection"), "connection"),
        ("clickhouse", ch.DatabaseError("Code: 6. Cannot parse string 'seven'"), "query"),
        ("clickhouse", ch.OperationalError("Connection refused"), "connection"),
        ("bigquery", gapi.BadRequest("Bad int64 value: seven"), "query"),
        ("bigquery", gapi.ServiceUnavailable("unavailable"), "connection"),
        ("databricks", dbx.ServerOperationError("CAST_INVALID_INPUT"), "query"),
        ("databricks", dbx.RequestError("connection reset"), "connection"),
        ("snowflake", sf.ProgrammingError("Numeric value 'seven' is not recognized"), "query"),
        ("snowflake", sf.OperationalError("connection lost"), "connection"),
        ("fabric", pyodbc.DataError("22018", "Conversion failed"), "query"),
        ("synapse", pyodbc.ProgrammingError("42000", "Incorrect syntax"), "query"),
        ("fabric", pyodbc.OperationalError("08S01", "Communication link failure"), "connection"),
        ("sqlalchemy", sa.DataError("SELECT", {}, Exception("bad value")), "query"),
        ("sqlalchemy", sa.OperationalError("SELECT", {}, Exception("gone")), "connection"),
    ]


@pytest.mark.parametrize(
    "engine, error, kind", _errors(), ids=[f"{e}-{type(x).__name__}" for e, x, _ in _errors()]
)
def test_each_native_engine_says_which_errors_are_the_statement_s(engine, error, kind):
    assert _backend(engine).classify_error(error) == kind


def test_an_error_no_engine_raises_is_none_of_its_kinds():
    assert _backend("duckdb").classify_error(ValueError("not the engine's")) is None


def test_a_null_on_failure_cast_on_a_postgresql_older_than_16_is_refused_by_name():
    import psycopg2.errors

    refusal = _backend("pg").refusal_of(
        psycopg2.errors.UndefinedFunction(
            "function pg_input_is_valid(text, unknown) does not exist"
        )
    )

    assert refusal is not None
    assert refusal.code == "cypher.try_cast_needs_postgres_16"
    assert str(refusal) == (
        "toInteger, toFloat and toBoolean of a value that may not convert need PostgreSQL 16 or "
        "later on this engine"
    )
    assert (
        _backend("pg").refusal_of(psycopg2.errors.UndefinedFunction("function f() does not exist"))
        is None
    )


def _run(monkeypatch, error: BaseException, engine: str):
    import asyncio
    from types import SimpleNamespace

    import provisa.pgwire._pipeline as pipeline
    from provisa.api.rest.cypher_router import _run_plan

    async def _refused(_plan, _state):
        raise error

    monkeypatch.setattr(pipeline, "_execute_plan", _refused)
    monkeypatch.setattr(pipeline, "require_governed_plan", lambda _plan: None)
    plan = SimpleNamespace(physical_sql="SELECT 1", exec_sql=None)
    return asyncio.run(_run_plan(plan, SimpleNamespace(federation_engine=_backend(engine))))


def test_a_bound_value_the_engine_cannot_convert_is_400_engine_refused(monkeypatch):
    answered = _run(
        monkeypatch,
        duckdb.ConversionException("Could not convert string 'seven' to INT32"),
        "duckdb",
    )

    assert answered.status_code == 400
    assert json.loads(bytes(answered.body))["code"] == "cypher.engine_refused"


def test_a_null_on_failure_cast_on_an_old_postgresql_is_refused_by_name_at_execution(monkeypatch):
    import psycopg2.errors

    from provisa.federation.pg_backend import TryCastNeedsPostgres16

    with pytest.raises(TryCastNeedsPostgres16):
        _run(
            monkeypatch,
            psycopg2.errors.UndefinedFunction(
                "function pg_input_is_valid(text, unknown) does not exist"
            ),
            "pg",
        )


def test_the_pg_engine_runs_its_statement_with_postgresql_s_null_on_failure_cast():
    from types import SimpleNamespace

    from provisa.federation.pg_backend import PgBackend

    engine = SimpleNamespace(native_store="postgres", name="postgres")
    physical = PgBackend(engine).transpile_physical("SELECT TRY_CAST(t.name AS BIGINT) FROM t")
    assert "PG_INPUT_IS_VALID(" in physical and "TRY_CAST" not in physical


def _bound(tree, name: str) -> list:
    """Every value ``name`` is bound to in ``tree`` (a plain or a tuple-unpacking assignment)."""
    import ast

    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            names = target.elts if isinstance(target, ast.Tuple) else [target]
            if any(isinstance(t, ast.Name) and t.id == name for t in names):
                found.append(node.value)
    return found


def test_every_direct_route_runs_a_statement_transpile_wrote_for_its_source():
    """The semantic SQL keeps TRY_CAST, which PostgreSQL (and MySQL) do not have: a route that sent
    it to a source as it is would fail there. The statement a direct route runs (``sql_to_run``)
    is only ever ``_direct`` -- or that wrapped as its EXPLAIN -- and ``_direct`` is only ever
    what ``transpile`` wrote for the source's dialect, kept or newly made."""
    import ast
    from pathlib import Path

    import provisa.pgwire._pipeline as pipeline

    tree = ast.parse(Path(pipeline.__file__).read_text())
    runs = _bound(tree, "sql_to_run")
    assert runs, "the direct routes bind the statement they run as sql_to_run"
    for value in runs:
        held = value.id if isinstance(value, ast.Name) else None
        explained = isinstance(value, ast.Call) and getattr(value.func, "id", "") == "wrap_explain"
        assert held == "_direct" or explained, ast.unparse(value)

    made = _bound(tree, "_direct")
    assert made
    for value in made:
        if isinstance(value, ast.Constant) or ast.unparse(value).startswith(("None if", "memo")):
            continue  # not yet made, or the one kept with the governed statement
        call = value.elts[0] if isinstance(value, ast.Tuple) else value
        assert isinstance(call, ast.Call) and getattr(call.func, "id", "") == "transpile", (
            ast.unparse(value)
        )
