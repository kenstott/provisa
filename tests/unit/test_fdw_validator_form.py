# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e4a92-3b5d-4f08-9a6e-2d8f1b0c5e47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""postgresql_fdw_validator on Trino, DuckDB and PostgreSQL raises 42601 for an option outside the
catalog's list, as PostgreSQL does, instead of answering t. ClickHouse 24.3's throwIf takes only a
constant message, so there such a call is refused by name, per row (REQ-2236, a stated limitation).
An array holding a NULL element is 22004 on every engine, as PostgreSQL raises it."""

# Requirements: REQ-2236

from __future__ import annotations

import chdb
import pytest
import sqlglot

from sqlglot import exp

from provisa.core.pg_error import postgres_error
from provisa.transpiler.metadata_facts import CatalogFacts
from provisa.transpiler.metadata_functions import expand
from provisa.transpiler.metadata_server_forms import FDW_OPTION_REFUSAL
from provisa.transpiler.transpile import transpile

pytestmark = pytest.mark.unit


def _written(options: str, catalog: int, dialect: str) -> str:
    sql = (
        f"SELECT pg_catalog.\"postgresql_fdw_validator\"('{options}'::text[], {catalog}::oid) AS v"
    )
    tree = sqlglot.parse_one(sql, read="postgres")
    return transpile(expand(tree, CatalogFacts("analyst")).sql("postgres"), dialect)


@pytest.mark.parametrize("dialect", ["trino", "postgres"])
def test_the_form_is_written_for_each_engine_and_raises_42601(dialect):
    written = _written('{"",a}', 12345, dialect)
    assert "42601" in written
    assert "invalid option" in written


def test_clickhouse_writes_the_refusal_and_not_the_message_it_cannot_raise():
    written = _written('{"",a}', 12345, "clickhouse")
    assert FDW_OPTION_REFUSAL in written
    assert "invalid option" not in written


@pytest.mark.parametrize("options", ['{"",a}', "{host,NULL}", "{host,b,c}"])
def test_clickhouse_throws_only_constant_messages(options):
    """ClickHouse 24.3 (CI's, run 44961655222) raises ILLEGAL_COLUMN for a throwIf whose message is
    no constant; chdb's newer ClickHouse takes one, so the message is checked as written."""
    tree = sqlglot.parse_one(_written(options, 1417, "clickhouse"), read="clickhouse")
    throws = [n for n in tree.find_all(exp.Anonymous) if n.name.lower() == "throwif"]
    assert throws
    for throw in throws:
        message = throw.expressions[1]
        assert message.find(exp.Lambda, exp.Column) is None, message.sql("clickhouse")


@pytest.mark.parametrize(
    ("options", "catalog"), [("{}", 12345), ("{host,port=1}", 1417), ("{user}", 1418)]
)
def test_clickhouse_answers_true_for_options_the_catalog_lists(options, catalog):
    assert str(chdb.query(_written(options, catalog, "clickhouse"), "CSV")).strip() == "true"


@pytest.mark.parametrize(
    ("options", "catalog", "name"),
    [('{"",a}', 12345, ""), ("{host,user}", 1417, "user"), ("{a=1}", 1418, "a")],
)
def test_clickhouse_refuses_an_option_the_catalog_does_not_list_by_name(options, catalog, name):
    with pytest.raises(Exception, match=f"postgresql-error 0A000: {FDW_OPTION_REFUSAL}"):
        chdb.query(_written(options, catalog, "clickhouse"), "CSV")


def _over_rows(rows: str) -> str:
    sql = (
        'SELECT pg_catalog."postgresql_fdw_validator"(t.o, 1417::oid) AS v '
        f"FROM (VALUES {rows}) AS t(o)"
    )
    tree = sqlglot.parse_one(sql, read="postgres")
    return transpile(expand(tree, CatalogFacts("analyst")).sql("postgres"), "clickhouse")


def test_clickhouse_refuses_per_row_only_where_a_row_has_an_unlisted_option():
    listed = _over_rows("('{host}'::text[]), ('{port=1}'::text[])")
    assert str(chdb.query(listed, "CSV")).split() == ["true", "true"]
    with pytest.raises(Exception, match=f"postgresql-error 0A000: {FDW_OPTION_REFUSAL}"):
        chdb.query(_over_rows("('{host}'::text[]), ('{b}'::text[])"), "CSV")


#: PostgreSQL's error for an option array holding a NULL element (arrayfuncs.c deconstruct_array,
#: called by untransformRelOptions with no nulls array), whatever the element's place.
NULL_ELEMENT = "22004: null array element not allowed in this context"


@pytest.mark.parametrize("options", ["{host,NULL}", "{b,NULL}", "{NULL}"])
def test_clickhouse_raises_22004_for_a_null_option(options):
    sql = _written(options, 1417, "clickhouse")
    with pytest.raises(Exception, match=f"postgresql-error {NULL_ELEMENT}"):
        chdb.query(sql, "CSV")


@pytest.mark.parametrize("dialect", ["trino", "postgres"])
def test_the_form_raises_22004_for_a_null_option(dialect):
    written = _written("{host,NULL}", 1417, dialect)
    assert "22004" in written
    assert "null array element not allowed in this context" in written
    assert "of a NULL option" not in written


def _duckdb(options: str, catalog: int):
    import duckdb

    from provisa.pgwire.catalog_server_answers import macros

    con = duckdb.connect()
    for statement in macros():
        if statement.startswith("CREATE MACRO postgresql_fdw_validator("):
            con.execute(statement)
    return con.execute(f"SELECT postgresql_fdw_validator({options}, {catalog})").fetchone()[0]


@pytest.mark.parametrize(
    ("options", "catalog"),
    [("[]::VARCHAR[]", 12345), ("['host', 'port=1']", 1417), ("['user']", 1418)],
)
def test_duckdb_answers_true_for_options_the_catalog_lists(options, catalog):
    assert _duckdb(options, catalog) is True


@pytest.mark.parametrize(
    ("options", "catalog", "name"),
    [
        ("['', 'a']", 12345, ""),
        ("['host', 'user']", 1417, "user"),
        ("['a=1']", 1418, "a"),
        ("['host', 'b', 'c']", 1417, "b"),
    ],
)
def test_duckdb_raises_42601_naming_the_first_option_the_catalog_does_not_list(
    options, catalog, name
):
    with pytest.raises(Exception, match=rf'42601.*invalid option "{name}"'):
        _duckdb(options, catalog)


@pytest.mark.parametrize("options", ["['host', NULL]", "['b', NULL]", "[NULL]::VARCHAR[]"])
def test_duckdb_raises_22004_for_a_null_option(options):
    with pytest.raises(Exception, match=NULL_ELEMENT):
        _duckdb(options, 1417)


def test_duckdb_answers_null_for_a_null_argument():
    assert _duckdb("NULL::VARCHAR[]", 1417) is None


def test_a_trino_error_ends_its_message_before_the_query_id():
    from provisa.core.pg_error import from_engine

    raised = Exception(
        'TrinoUserError(type=USER_ERROR, name=GENERIC_USER_ERROR, message="postgresql-error '
        '42601: invalid option "user"", query_id=20261121_160523_00007_4ebgz)'
    )
    error = from_engine(raised)
    assert (error.sqlstate, str(error)) == ("42601", 'invalid option "user"')


class _Diagnostics:
    sqlstate = "42601"
    message_primary = 'invalid option "b"'


class _Psycopg2Error(Exception):
    """psycopg2's error (the pg engine's driver): its SQLSTATE is on its diagnostics alone."""

    pgcode = "42601"
    diag = _Diagnostics()


def test_a_psycopg2_error_is_read_as_postgresqls_own():
    error = postgres_error(_Psycopg2Error('invalid option "b"\nCONTEXT:  PL/pgSQL function'))
    assert error is not None
    assert (error.sqlstate, str(error)) == ("42601", 'invalid option "b"')


def test_postgres_subscripts_the_refused_options_in_parentheses():
    written = _written("{host,NULL}", 1417, "postgres")
    assert "END)[1]" in written
    assert "END[1]" not in written
    sqlglot.parse_one(written, read="postgres")


def test_clickhouse_null_element_does_not_assume_not_null_on_the_element():
    written = _written("{host,NULL}", 1417, "clickhouse")
    assert "assumeNotNull(COALESCE(_o, ''))" in written
    assert "assumeNotNull(_o)" not in written


def _row():
    from provisa.transpiler.polyfill import GRID

    (row,) = [f for f in GRID.features if f.name == "postgresql_fdw_validator"]
    return GRID, row


def test_the_grid_row_is_a_family_of_its_own():
    grid, row = _row()
    assert row.family == "foreign-data wrapper options"
    assert [f.name for f in grid.features if f.family == row.family] == [row.name]


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "presto", "athena", "clickhouse"])
def test_the_grid_row_states_no_null_option_limitation(dialect):
    _, row = _row()
    for limitation in row.read.limitations.get(dialect, ()):
        assert "NULL" not in limitation.text


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "presto", "athena"])
def test_the_grid_row_is_exact_where_the_form_runs(dialect):
    from provisa.transpiler.polyfill.grid import Status

    grid, row = _row()
    assert grid.status(row, dialect) is Status.NATIVE


#: (options PostgreSQL's untransformRelOptions deconstructs, the catalog, the option its 42601
#: names): deconstruct_array reads a multidimensional array's elements in storage order, so a
#: two-dimensional options array is read as the one-dimensional array of its elements.
_DIMENSIONS = [
    ("{a,b,c,d}", 12345, "a"),
    ("{{a,b},{c,d}}", 12345, "a"),
    ("{{host,port},{dbname,user}}", 1417, "user"),
]


@pytest.mark.parametrize(("options", "catalog", "name"), _DIMENSIONS)
def test_trino_reads_a_two_dimensional_options_array_as_its_elements(options, catalog, name):
    """On Trino a two-dimensional text[] is an array(array(varchar)), which the form's text
    functions do not take (TYPE_MISMATCH in COALESCE, run 45042021798): the form reads its elements
    in storage order, as one dimension."""
    written = _written(options, catalog, "trino")
    elements = options.replace("{", "").replace("}", "").split(",")
    flat = "ARRAY[" + ", ".join(f"'{e}'" for e in elements) + "]"
    assert f"CAST({flat} AS ARRAY(VARCHAR))" in written
    assert written == _written("{" + ",".join(elements) + "}", catalog, "trino")
    assert "ARRAY[ARRAY[" not in written
    assert "ARRAY(ARRAY(" not in written
    assert "invalid option" in written


@pytest.mark.parametrize(
    ("options", "catalog", "name"),
    [*_DIMENSIONS, ("{{host,port},{dbname,port=1}}", 1417, None)],
)
def test_clickhouse_reads_a_two_dimensional_options_array_as_its_elements(options, catalog, name):
    sql = _written(options, catalog, "clickhouse")
    if name is None:
        assert str(chdb.query(sql, "CSV")).strip() == "true"
        return
    with pytest.raises(Exception, match=f"postgresql-error 0A000: {FDW_OPTION_REFUSAL}"):
        chdb.query(sql, "CSV")


def test_a_two_dimensional_options_array_with_a_null_element_is_22004():
    with pytest.raises(Exception, match=f"postgresql-error {NULL_ELEMENT}"):
        chdb.query(_written("{{host,port},{b,NULL}}", 1417, "clickhouse"), "CSV")
