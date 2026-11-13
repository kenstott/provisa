# Copyright (c) 2026 Kenneth Stott
# Canary: 5a96179e-f02a-4590-a84f-5292fd55029b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two sets of pg_catalog calls on the engines that are not PostgreSQL (REQ-2236, REQ-2171).

The first: the calls a tool sends that were refused as having no form on Trino and ClickHouse
(DataGrip's ``SELECT age(0::varchar::xid)``, mxid_age and pg_char_to_encoding), each now a row of
the polyfill grid answering as PostgreSQL does. The second: the last engine errors the function
support measure found on Trino (run 43337203459), each statement the one it sent.

A Trino form's values are read by running it on DuckDB, written there by sqlglot: Trino's own
engine does not run here."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import duckdb
import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.transpile import transpile, transpile_to_duckdb, transpile_to_trino

_TRINO = ("trino", "presto", "athena")
_ENGINES = (*_TRINO, "clickhouse")


def _sent(sql: str, dialect: str = "trino") -> str:
    return transpile_to_trino(sql) if dialect == "trino" else transpile(sql, dialect)


def _tree(sql: str, dialect: str = "trino") -> exp.Expr:
    read = "trino" if dialect in _TRINO else dialect
    return sqlglot.parse_one(_sent(sql, dialect), read=read)


def _calls(tree: exp.Expr) -> set[str]:
    names = {node.name.lower() for node in tree.find_all(exp.Anonymous)}
    names |= {
        type(node).sql_names()[0].lower()
        for node in tree.find_all(exp.Func)
        if not isinstance(node, exp.Anonymous)
    }
    return names


def _octet_length(node: exp.Expr) -> exp.Expr:
    """Trino's length of a value's UTF-8 bytes, as DuckDB counts them."""
    if isinstance(node, exp.Length) and isinstance(node.this, exp.Encode):
        return exp.Anonymous(this="octet_length", expressions=[node.this])
    return node


def _on_duckdb(trino_sql: str) -> list[tuple]:
    """What Trino's statement answers, run on DuckDB: its fail() is DuckDB's error()."""
    tree = sqlglot.parse_one(trino_sql, read="trino").transform(_octet_length)
    con = duckdb.connect()
    con.execute("CREATE MACRO fail(m) AS error(m)")
    return con.execute(tree.sql(dialect="duckdb")).fetchall()


def _value(sql: str) -> object:
    return _on_duckdb(_sent(sql))[0][0]


def _refused(sql: str, function: str, dialect: str = "trino") -> RowRefused:
    with pytest.raises(RowRefused) as refused:
        _sent(sql, dialect)
    assert refused.value.sqlstate == "0A000"
    assert str(refused.value) == f"{function}() has no form on {dialect}"
    return refused.value


# --- set 1: age(xid) ---------------------------------------------------------------------------

#: DataGrip's own statement.
_DATAGRIP_AGE = "SELECT age(0::varchar::xid)"


@pytest.mark.parametrize("dialect", _ENGINES)
def test_datagrips_age_of_xid_is_answered(dialect):
    """xid 0 is no normal transaction id: PostgreSQL's xid_age answers INT_MAX whatever the
    current transaction is."""
    tree = _tree(_DATAGRIP_AGE, dialect)
    assert "age" not in _calls(tree)
    assert any(lit.this == "2147483647" for lit in tree.find_all(exp.Literal))


@pytest.mark.parametrize("dialect", _ENGINES)
def test_age_of_a_null_xid_is_answered(dialect):
    assert "age" not in _calls(_tree('SELECT pg_catalog."age"(NULL::xid) AS v', dialect))


@pytest.mark.parametrize(
    ("xid", "answer"), [("0", 2147483647), ("1", 2147483647), ("2", 2147483647)]
)
def test_age_of_a_special_xid_is_int_max(xid, answer):
    assert _value(f"SELECT age('{xid}'::xid) AS v") == answer


def test_age_of_a_null_xid_is_null():
    assert _value("SELECT age(NULL::xid) AS v") is None


def test_age_of_a_normal_xid_is_refused_when_it_runs():
    """A normal xid's age is the current transaction id less it: Trino holds no transaction id."""
    with pytest.raises(duckdb.Error, match="0A000: age does not read this value"):
        _value("SELECT age('5'::xid) AS v")


@pytest.mark.parametrize("dialect", _ENGINES)
def test_age_of_a_timestamp_is_still_refused_by_name(dialect):
    with pytest.raises(FunctionRefused, match=rf"^age\(\) has no form on {dialect}$"):
        _sent("SELECT age(NULL::timestamp) AS v", dialect)


def test_age_of_xid_on_duckdb_is_unchanged():
    assert "TXID_CURRENT" in transpile_to_duckdb(_DATAGRIP_AGE).upper()


# --- set 1: mxid_age ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("xid", "answer"),
    [
        ("0", 2147483647),  # no multixact: infinitely old
        ("1", 0),
        ("5", -4),  # the next multixact is 1: none was ever made
        ("-1", 2),  # xid's input reads -1 as 4294967295
        ("2147483649", -2147483648),
        ("2147483650", 2147483647),  # (int32)(1 - x) wraps
        ("4294967295", 2),
    ],
)
def test_mxid_age_answers_as_postgresql(xid, answer):
    assert _value(f"SELECT mxid_age('{xid}'::xid) AS v") == answer


def test_mxid_age_of_null_is_null():
    assert _value('SELECT pg_catalog."mxid_age"(NULL::xid) AS v') is None


@pytest.mark.parametrize("dialect", _ENGINES)
def test_mxid_age_has_a_form(dialect):
    assert "mxid_age" not in _calls(_tree('SELECT pg_catalog."mxid_age"(NULL::xid) AS v', dialect))


def test_mxid_age_on_clickhouse_wraps_and_refuses_an_xid_past_its_range():
    sent = _sent("SELECT mxid_age('5'::xid) AS v", "clickhouse")
    assert "4294967296" in sent
    assert "throwIf" in sent


# --- set 1: pg_char_to_encoding ----------------------------------------------------------------

#: PostgreSQL's pg_encname_tbl (src/common/encnames.c, PostgreSQL 16.11), each name with the number
#: of its encoding (pg_enc, src/include/mb/pg_wchar.h).
_PG_ENCODINGS = {
    "abc": 19, "alt": 20, "big5": 36, "euccn": 2, "eucjis2004": 5, "eucjp": 1, "euckr": 3,
    "euctw": 4, "gb18030": 39, "gbk": 37, "iso88591": 8, "iso885910": 13, "iso885913": 14,
    "iso885914": 15, "iso885915": 16, "iso885916": 17, "iso88592": 9, "iso88593": 10,
    "iso88594": 11, "iso88595": 25, "iso88596": 26, "iso88597": 27, "iso88598": 28,
    "iso88599": 12, "johab": 40, "koi8": 22, "koi8r": 22, "koi8u": 34, "latin1": 8, "latin10": 17,
    "latin2": 9, "latin3": 10, "latin4": 11, "latin5": 12, "latin6": 13, "latin7": 14,
    "latin8": 15, "latin9": 16, "mskanji": 35, "muleinternal": 7, "shiftjis": 35,
    "shiftjis2004": 41, "sjis": 35, "sqlascii": 0, "tcvn": 19, "tcvn5712": 19, "uhc": 38,
    "unicode": 6, "utf8": 6, "vscii": 19, "win": 23, "win1250": 29, "win1251": 23, "win1252": 24,
    "win1253": 30, "win1254": 31, "win1255": 32, "win1256": 18, "win1257": 33, "win1258": 19,
    "win866": 20, "win874": 21, "win932": 35, "win936": 37, "win949": 38, "win950": 36,
    "windows1250": 29, "windows1251": 23, "windows1252": 24, "windows1253": 30,
    "windows1254": 31, "windows1255": 32, "windows1256": 18, "windows1257": 33,
    "windows1258": 19, "windows866": 20, "windows874": 21, "windows932": 35, "windows936": 37,
    "windows949": 38, "windows950": 36,
}  # fmt: skip
#: A name as a client writes it, cleaned as clean_encoding_name cleans it.
_WRITTEN = {
    "UTF-8": 6, "ISO_8859-1": 8, "Windows-1252": 24, "Shift_JIS": 35, "EUC_JIS_2004": 5,
    "MULE_INTERNAL": 7, "SQL_ASCII": 0, "KOI8-U": 34, "": -1, "utf9": -1, "LATIN11": -1,
}  # fmt: skip


def _encoding_statement(names: list[str]) -> str:
    rows = " UNION ALL ".join(f"SELECT {i} AS i, '{n}'::name AS n" for i, n in enumerate(names))
    return f"SELECT pg_char_to_encoding(n) AS v FROM ({rows}) AS t ORDER BY i"


@pytest.mark.parametrize("table", [_PG_ENCODINGS, _WRITTEN], ids=["pg_encname_tbl", "written"])
def test_pg_char_to_encoding_answers_postgresqls_number_on_trino(table):
    names = list(table)
    answered = [r[0] for r in _on_duckdb(_sent(_encoding_statement(names)))]
    assert answered == [table[n] for n in names]


@pytest.mark.parametrize("table", [_PG_ENCODINGS, _WRITTEN], ids=["pg_encname_tbl", "written"])
def test_pg_char_to_encoding_answers_postgresqls_number_on_duckdb(table):
    names = list(table)
    sql = transpile_to_duckdb(_encoding_statement(names))
    answered = [r[0] for r in duckdb.connect().execute(sql).fetchall()]
    assert answered == [table[n] for n in names]


def test_pg_char_to_encoding_of_null_is_null():
    assert _value('SELECT pg_catalog."pg_char_to_encoding"(NULL::name) AS v') is None


def test_pg_char_to_encoding_of_a_name_past_namedatalen_is_refused():
    """A name holds 63 bytes: a longer text is cut before it is read, which no form here does."""
    with pytest.raises(duckdb.Error, match="0A000: pg_char_to_encoding does not read this value"):
        _value(f"SELECT pg_char_to_encoding('{'u' * 64}') AS v")


@pytest.mark.parametrize("dialect", _ENGINES)
def test_pg_char_to_encoding_has_a_form(dialect):
    tree = _tree('SELECT pg_catalog."pg_char_to_encoding"(NULL::name) AS v', dialect)
    assert "pg_char_to_encoding" not in _calls(tree)


def test_pg_char_to_encoding_on_clickhouse_reads_postgresqls_table():
    sent = _sent("SELECT pg_char_to_encoding('UTF8') AS v", "clickhouse")
    assert "replaceRegexpAll" in sent
    assert "'windows950'" in sent and "'mskanji'" in sent


@pytest.mark.parametrize("dialect", ["snowflake", "mysql"])
def test_pg_char_to_encoding_is_refused_by_name_on_a_source(dialect):
    _refused("SELECT pg_char_to_encoding('UTF8') AS v", "pg_char_to_encoding", dialect)


# --- set 2: a multirange of a VARIADIC array of ranges -----------------------------------------

_VARIADIC = [
    ("int4", '\'{"[1,5)","[7,9)"}\'', ["'[1,5)'", "'[7,9)'"]),
    ("int8", '\'{"[1,5)","[7,9)"}\'', ["'[1,5)'", "'[7,9)'"]),
    ("num", "'{\"[1.5,2.5)\"}'", ["'[1.5,2.5)'"]),
    (
        "date",
        '\'{"[2024-01-01,2024-02-01)","[2024-03-01,2024-04-01)"}\'',
        ["'[2024-01-01,2024-02-01)'", "'[2024-03-01,2024-04-01)'"],
    ),
    (
        "ts",
        "'{\"[2024-01-01 00:00,2024-02-01 00:00)\"}'",
        ["'[2024-01-01 00:00,2024-02-01 00:00)'"],
    ),
    (
        "tstz",
        "'{\"[2024-01-01 00:00+00,2024-02-01 00:00+00)\"}'",
        ["'[2024-01-01 00:00+00,2024-02-01 00:00+00)'"],
    ),
]


@pytest.mark.parametrize(("base", "array", "ranges"), _VARIADIC)
@pytest.mark.parametrize("dialect", [*_TRINO, "clickhouse"])
def test_a_variadic_array_of_ranges_is_the_ranges_it_holds(base, array, ranges, dialect):
    """PostgreSQL's VARIADIC array is the arguments it holds: the multirange of each range."""
    variadic = f'SELECT pg_catalog."{base}multirange"(VARIADIC {array}::{base}range[]) AS v'
    each = ", ".join(f"{r}::{base}range" for r in ranges)
    assert _sent(variadic, dialect) == _sent(f"SELECT {base}multirange({each}) AS v", dialect)


def test_a_variadic_array_of_ranges_holding_null_is_refused_as_postgresql_refuses_it():
    """Where it runs, as PostgreSQL's constructor refuses a NULL range of its arguments."""
    sent = _sent("SELECT int4multirange(VARIADIC '{\"[1,5)\",NULL}'::int4range[]) AS v")
    assert "ARRAY(" not in sent.upper()
    assert "22004: multirange values cannot contain null members" in sent


@pytest.mark.parametrize("dialect", [*_TRINO, "clickhouse"])
def test_a_variadic_array_of_ranges_the_rows_hold_is_refused_by_name(dialect):
    _refused(
        "SELECT int4multirange(VARIADIC a::int4range[]) AS v FROM t", "int4multirange", dialect
    )


# --- set 2: range_minus and range_intersect_agg of NULL ----------------------------------------


def test_range_minus_of_null_ranges_is_a_null_text():
    """range_minus is strict: Trino reads an untyped NULL concatenated as no one operator."""
    sent = _sent('SELECT pg_catalog."range_minus"(NULL::int4range, NULL::int4range) AS v')
    tree = sqlglot.parse_one(sent, read="trino")
    assert all(isinstance(null.parent, exp.Cast) for null in tree.find_all(exp.Null)), sent
    assert _on_duckdb(sent) == [(None,)]


def test_range_minus_of_one_null_range_is_null():
    sql = "SELECT range_minus('[1,5)'::int4range, NULL::int4range) AS v"
    assert _on_duckdb(_sent(sql)) == [(None,)]


def test_range_intersect_agg_of_a_null_range_is_an_aggregate_of_null():
    sent = _sent('SELECT pg_catalog."range_intersect_agg"(NULL::int4range) AS v')
    tree = sqlglot.parse_one(sent, read="trino")
    assert tree.find(exp.AggFunc) is not None
    assert len(sent) < 200, sent
    assert _on_duckdb(sent) == [(None,)]


# --- set 2: json_agg of an integer, and an object's key that is no text ------------------------


@pytest.mark.parametrize("dialect", _TRINO)
def test_json_agg_of_an_integer_is_its_json_number(dialect):
    tree = _tree('SELECT pg_catalog."json_agg"(NULL::integer) AS v', dialect)
    assert tree.find(exp.ParseJSON) is None
    assert "json_parse" not in _calls(tree)
    assert "json_format" in _calls(tree)


def test_json_agg_of_integers_aggregates_them_as_an_array_cast_to_json():
    sql = "SELECT json_agg(x::integer) AS v FROM t"
    collected = _tree(sql).find(exp.ArrayAgg)
    assert collected is not None
    assert isinstance(collected.this, exp.Cast) and collected.this.this.name == "x"
    assert isinstance(collected.parent, exp.Cast) and collected.parent.to.is_type("json")


def test_json_agg_of_a_column_of_no_said_type_keeps_its_form():
    assert "JSON_PARSE" in _sent("SELECT json_agg(x) AS v FROM t").upper()


@pytest.mark.parametrize("function", ["json_build_object", "jsonb_build_object"])
@pytest.mark.parametrize("dialect", _TRINO)
def test_an_integer_key_is_its_text(function, dialect):
    """PostgreSQL casts a key to text: Trino's JSON_OBJECT takes a text key only."""
    tree = _tree(f'SELECT pg_catalog."{function}"(1::integer, 2::integer) AS v', dialect)
    pair = tree.find(exp.JSONKeyValue)
    assert pair is not None
    assert isinstance(pair.this, exp.Cast) and pair.this.to.is_type(*exp.DataType.TEXT_TYPES)


def test_a_boolean_key_is_its_text():
    pair = _tree("SELECT json_build_object(true, 1) AS v").find(exp.JSONKeyValue)
    assert pair is not None
    assert isinstance(pair.this, exp.Cast) and pair.this.to.is_type(*exp.DataType.TEXT_TYPES)


def test_a_text_key_is_as_written():
    pair = _tree("SELECT json_build_object('a', 1) AS v").find(exp.JSONKeyValue)
    assert pair is not None
    assert isinstance(pair.this, exp.Literal) and pair.this.this == "a"


@pytest.mark.parametrize("key", ["1.5::float8", "'2024-01-01'::date", "1.5"])
def test_a_key_whose_text_trino_may_write_otherwise_is_refused_by_name(key):
    _refused(f"SELECT json_build_object({key}, 1) AS v", "json_build_object")


def test_an_integer_key_beside_a_call_is_refused_by_name():
    """The call's own form would not be written inside the object: refused, not sent."""
    _refused("SELECT json_build_object(1, array_dims(a)) AS v FROM t", "json_build_object")


def test_an_odd_count_of_arguments_is_refused_as_postgresql_refuses_it():
    with pytest.raises(RowRefused) as refused:
        _sent("SELECT json_build_object('a', 1, 'b') AS v")
    assert refused.value.sqlstate == "22023"
    assert str(refused.value) == "argument list must have even number of elements"


# --- set 2: unnest of a tsvector ---------------------------------------------------------------


def test_unnest_of_a_tsvector_is_refused_by_name_on_trino():
    _refused('SELECT pg_catalog."unnest"(NULL::tsvector) AS v', "unnest")


def test_unnest_of_a_tsvector_is_refused_by_name_on_clickhouse():
    _refused('SELECT pg_catalog."unnest"(NULL::tsvector) AS v', "unnest", "clickhouse")


def test_unnest_of_a_multirange_on_clickhouse_is_still_its_ranges():
    assert "arrayJoin" in _sent("SELECT unnest('{[1,2)}'::int4multirange) AS v", "clickhouse")


@pytest.mark.parametrize("value", ["'1'::text", "x::text", "x::varchar"])
def test_json_agg_of_a_said_text_is_refused_by_name(value):
    """Trino's form parses each value as JSON: the text '1' would be the number 1."""
    _refused(f"SELECT json_agg({value}) AS v FROM t", "json_agg")
