# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6b9d14-73ea-4c08-a1d7-5e80c3b6f92a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The generated rule of pg_catalog's operator-named functions (REQ-2171, A REWRITE IS A SQLGLOT EXPANSION,
ALWAYS POSTGRESQL TO THE TARGET, AN ANSWER THAT DIFFERS IS REFUSED; REQ-2167): each statement run as
a client runs it, over pgwire on the default engine, and answered as PostgreSQL 17.11 answered it,
compared by answer and never by text."""

# Requirements: REQ-2167, REQ-2171
# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

from typing import Any

import duckdb
import pytest
import sqlglot
from psycopg import pq
from sqlglot import exp

from provisa.transpiler.function_macros import (
    MACROS,
    OVERLOADS,
    FunctionRefused,
    Pg,
    outcome,
    overload,
)
from provisa.transpiler.pg_forms_types import (
    ARRAY_OPERATORS,
    DUCKDB_TWEAKS,
    OPERATORS,
    OVERFLOW_UNCHECKED,
    REFUSED,
)
from provisa.transpiler.transpile import transpile, transpile_to_duckdb
from tests.unit.pgwire.pg17_function_rule_answers import RECORDED
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401


def _run(conn: Any, sql: str) -> Any:
    return conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)


def _rows(res: Any) -> list[Any]:
    return [None if (v := res.get_value(i, 0)) is None else v.decode() for i in range(res.ntuples)]


def _refused_by_name(res: Any) -> bool:
    """AN ANSWER THAT DIFFERS IS REFUSED: a value a form cannot answer as PostgreSQL does."""
    said = res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b""
    return said.decode().endswith(REFUSED)


def _problem(conn: Any, sql: str) -> str | None:
    res = _run(conn, f"{sql} AS v")
    if res.status == pq.ExecStatus.FATAL_ERROR and _refused_by_name(res):
        return None
    kind, first, second = RECORDED[sql]
    if kind == "err":
        if res.status != pq.ExecStatus.FATAL_ERROR:
            return f"answered {_rows(res)}"
        said = (
            (res.error_field(pq.DiagnosticField.SQLSTATE) or b"").decode(),
            (res.error_field(pq.DiagnosticField.MESSAGE_PRIMARY) or b"").decode(),
        )
        return None if said == (first, second) else str(said)
    if res.status != pq.ExecStatus.TUPLES_OK:
        return (res.error_message or b"").decode()[:200]
    if res.ftype(0) != first:
        return f"type {res.ftype(0)}, rows {_rows(res)}"
    rows = _rows(res)
    return None if rows == second or _same_order(sql, rows, second) else str(rows)


def _same_order(sql: str, rows: list[Any], want: list[Any]) -> bool:
    """A cmp function promises its sign: PostgreSQL's magnitude is memcmp's and strcoll's."""
    if "cmp" not in sql or None in (*rows, *want):
        return False
    return [(int(v) > 0) - (int(v) < 0) for v in rows] == [
        (int(v) > 0) - (int(v) < 0) for v in want
    ]


def test_every_statement_answers_as_postgresql_17_answered(conn: Any):
    problems = {}
    for sql in RECORDED:
        problem = _problem(conn, sql)
        if problem is not None:
            problems[sql] = problem
            if conn.conn.pgconn.status != pq.ConnStatus.OK:
                break
    assert not problems


_FORMS = (*OPERATORS, *ARRAY_OPERATORS)
#: Rows PostgreSQL has no name for, reached through an overload of another.
_OF_AN_OVERLOAD = frozenset(row for (_, _, _), row in OVERLOADS.items())


def _called(form: Any) -> str:
    return f'pg_catalog."{form.name}"('


def test_every_form_is_proved_with_a_value_and_a_null():
    for form in _FORMS:
        if form.name in _OF_AN_OVERLOAD:
            continue
        statements = [sql for sql in RECORDED if _called(form) in sql]
        assert any("NULL" in sql for sql in statements), form.name
        assert any("NULL" not in sql for sql in statements), form.name


@pytest.mark.parametrize("form", _FORMS, ids=lambda form: form.name)
def test_a_form_is_postgresql_syntax(form: Any):
    macro = MACROS[(form.name, len(form.params.split(", ")))]
    native = form in ARRAY_OPERATORS or "trino" in macro.differs_on
    _, written = macro.resolved("duckdb" if native else "trino")
    assert isinstance(written, Pg)
    tree = sqlglot.parse_one(written, read="postgres")
    assert not list(tree.find_all(exp.Lambda))
    assert "provisa_" not in written.lower()
    assert "error(" not in written.lower().replace("postgresql-error", "")


def test_only_a_replay_put_an_engine_tweak_beside_a_form():
    assert set(DUCKDB_TWEAKS) == {"int8div"}


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        ("int8div(9007199254740993, 1)", 9007199254740993),
        ("int8div(-9223372036854775807, 3)", -3074457345618258602),
        ("int8mod(9007199254740993, 2)", 1),
        ("int8pl(9223372036854775806, 1)", 9223372036854775807),
    ],
)
@pytest.mark.parametrize("write", [transpile_to_duckdb, lambda sql: transpile(sql, "duckdb")])
def test_a_bigint_keeps_its_low_bits_on_either_path(sql: str, want: int, write: Any):
    assert duckdb.connect().execute(write(f"SELECT {sql} AS v")).fetchone()[0] == want


@pytest.fixture
def rows():
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE t AS SELECT * FROM (VALUES (1, 2, 'a', DATE '2024-02-29'), "
        "(-7, 2, 'B', DATE '2000-01-01'), (NULL, 2, NULL, NULL)) v(x, y, s, d)"
    )
    return connection


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        ("int4pl(x, y)", [3, -5, None]),
        ("int4div(x, y)", [0, -3, None]),
        ("int4mod(x, y)", [1, -1, None]),
        ("int4lt(x, y)", [True, True, None]),
        ("int4larger(x, y)", [2, 2, None]),
        ("int4um(x)", [-1, 7, None]),
        ("int4shr(x, 1)", [0, -4, None]),
        ("texteq(s, 'a')", [True, False, None]),
        ("date_mi(d, DATE '2000-01-01')", [8825, 0, None]),
    ],
)
def test_a_form_reads_a_column_as_it_reads_a_value(rows: Any, sql: str, want: list[Any]):
    written = transpile_to_duckdb(f"SELECT {sql} AS v FROM t")
    assert [row[0] for row in rows.execute(written).fetchall()] == want


def test_a_form_never_writes_a_volatile_argument_twice():
    written = transpile_to_duckdb(
        "SELECT int4larger(CAST(x + random() * 1000000 AS INTEGER), y) AS v FROM t"
    )
    assert written.lower().count("random()") == 1


def test_a_form_naming_an_argument_once_keeps_its_own_shape():
    written = transpile_to_duckdb("SELECT int4eq(CAST(random() * 9 AS INTEGER), y) AS v FROM t")
    assert written.lower().count("random()") == 1
    assert "SELECT CAST(" not in written.split("FROM")[0].split("SELECT", 2)[-1]


def test_a_volatile_argument_that_is_held_differs_from_row_to_row(rows: Any):
    written = transpile_to_duckdb(
        "SELECT int4larger(CAST(g + random() * 1000000 AS INTEGER), 0) AS v FROM range(200) r(g)"
    )
    assert len({row[0] for row in rows.execute(written).fetchall()}) > 100


def test_a_statement_with_one_row_holds_a_volatile_argument_once(rows: Any):
    written = transpile_to_duckdb("SELECT int4larger(CAST(random() * 9 AS INTEGER), 0) AS v")
    assert written.lower().count("random()") == 1
    assert rows.execute(written).fetchone()[0] >= 0


def test_a_volatile_argument_no_row_correlates_is_refused_by_name():
    with pytest.raises(FunctionRefused, match=r"int4larger\(\) has no form"):
        transpile_to_duckdb("SELECT int4larger(CAST(random() * 9 AS INTEGER), 0) AS v FROM t")


@pytest.mark.parametrize(
    ("sql", "row"),
    [
        ("set_masklen('10.0.0.1/8'::inet, 16)", "set_masklen"),
        ("set_masklen('10.0.0.0/8'::cidr, 16)", "cidr_set_masklen"),
        ("set_masklen(x, 16)", "set_masklen"),
    ],
)
def test_an_overload_of_one_name_is_chosen_by_the_type_said(sql: str, row: str):
    call = sqlglot.parse_one(sql, read="postgres")
    assert overload(call.name.lower(), list(call.expressions)) == row


@pytest.mark.parametrize(
    "sql",
    [
        "set_masklen(c, 16)",
        "set_masklen(network(c), 16)",
        "set_masklen(network('10.1.2.3/8'::inet), 16)",
        "set_masklen(c::text, 16)",
    ],
)
def test_set_masklen_of_a_value_of_no_said_network_type_is_refused_by_name(sql: str):
    """A cidr column and an inet column are the same text: the inet row would keep the host bits
    the cidr row clears, so set_masklen(c, 8) of 10.1.0.0/16 would answer 10.1.0.0/8."""
    with pytest.raises(FunctionRefused, match=r"set_masklen\(\) has no form on duckdb"):
        transpile_to_duckdb(f"SELECT {sql} AS v FROM t")


def test_set_masklen_of_a_column_answers_once_the_statement_says_its_type():
    connection = duckdb.connect()
    connection.execute("CREATE TABLE t AS SELECT '10.1.0.0/16' AS c")
    for said, want in (("inet", "10.1.0.0/8"), ("cidr", "10.0.0.0/8")):
        written = transpile_to_duckdb(f"SELECT set_masklen(c::{said}, 8) AS v FROM t")
        assert connection.execute(written).fetchone()[0] == want


def test_set_masklen_of_an_unknown_literal_is_inet_as_postgresql_resolves_it():
    written = transpile_to_duckdb("SELECT set_masklen('10.1.0.0/16', 8) AS v")
    assert duckdb.connect().execute(written).fetchone()[0] == "10.1.0.0/8"


_NO_CHECKED_CAST = ("mysql", "sqlite", "spark", "databricks")
_CHECKED_CAST = (
    "duckdb", "trino", "presto", "athena", "snowflake", "bigquery", "clickhouse", "tsql",
    "redshift", "oracle",
)  # fmt: skip


@pytest.mark.parametrize("dialect", _NO_CHECKED_CAST)
def test_a_row_that_must_raise_is_refused_where_a_cast_cannot_raise(dialect: str):
    for form in OPERATORS:
        wanted = (
            "emulated" if not form.raises and form.name not in OVERFLOW_UNCHECKED else "refused"
        )
        assert outcome(form.name, len(form.params.split(", ")), dialect) == wanted, form.name


@pytest.mark.parametrize("dialect", _NO_CHECKED_CAST)
@pytest.mark.parametrize(
    "call",
    ["int4pl(a, b)", "int8mul(a, b)", "float8pl(a, b)", "numeric_add(a, b)", "interval_pl(a, b)"],
)
def test_an_overflow_that_could_answer_a_wrong_value_is_refused_by_name(dialect: str, call: str):
    with pytest.raises(
        FunctionRefused, match=rf"{call.split('(')[0]}\(\) has no form on {dialect}"
    ):
        transpile(f"SELECT {call} AS v FROM t", dialect)


@pytest.mark.parametrize("dialect", _CHECKED_CAST)
def test_a_row_that_must_raise_has_a_form_where_a_cast_raises(dialect: str):
    for form in OPERATORS:
        arity = len(form.params.split(", "))
        if dialect in MACROS[(form.name, arity)].differs_on:
            continue  # REQ-2171, AN ANSWER THAT DIFFERS IS REFUSED: replay decided it there
        assert outcome(form.name, arity, dialect) == "emulated", form.name
