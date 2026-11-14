# Copyright (c) 2026 Kenneth Stott
# Canary: 4b8e2d17-9a3c-4f60-b5e1-7c2d9f0a6e38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A bytea's functions on ClickHouse (REQ-2236; REQ-2167, AN ANSWER WITHIN A STATED BOUND COMES
BEFORE A REFUSAL): held as ClickHouse's String, its bytes, each answers PostgreSQL 17's answer, an
index outside the value raises PostgreSQL's SQLSTATE, and what ClickHouse would not hold as those
bytes keeps its refusal by name.

The NULL statements are the function support run's (43490334080); the answers of the others are
PostgreSQL 17's (bytea.c, like_match.c, encode.c)."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.core.pg_error import PostgresError, from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.polyfill import GRID, bytea, trino
from provisa.transpiler.transpile import transpile


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _held(value: Any) -> Any:
    """A bytea's answer as its bytes: the runtime reads a String that is valid UTF-8 as text."""
    return value.encode() if isinstance(value, str) else value


def _one(runtime: ClickHouseFederationRuntime, sql: str) -> Any:
    ((value,),) = list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    return value


def _raised(runtime: ClickHouseFederationRuntime, sql: str) -> PostgresError:
    with pytest.raises(Exception) as raised:
        list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    error = from_engine(raised.value)
    assert error is not None, str(raised.value)[:300]
    return error


def _b(digits: str) -> str:
    return f"'\\x{digits}'::bytea"


def _call(function: str, *args: str) -> str:
    return f'SELECT pg_catalog."{function}"({", ".join(args)}) AS v'


# --- the run's statements: each NULL call answers NULL -------------------------------------------


@pytest.mark.parametrize(
    "args",
    [
        ("octet_length", "NULL::bytea"),
        ("length", "NULL::bytea"),
        ("bit_length", "NULL::bytea"),
        ("bit_count", "NULL::bytea"),
        ("byteacat", "NULL::bytea", "NULL::bytea"),
        ("btrim", "NULL::bytea", "NULL::bytea"),
        ("ltrim", "NULL::bytea", "NULL::bytea"),
        ("rtrim", "NULL::bytea", "NULL::bytea"),
        ("get_bit", "NULL::bytea", "NULL::bigint"),
        ("set_bit", "NULL::bytea", "NULL::bigint", "NULL::integer"),
        ("get_byte", "NULL::bytea", "NULL::integer"),
        ("set_byte", "NULL::bytea", "NULL::integer", "NULL::integer"),
        ("like", "NULL::bytea", "NULL::bytea"),
        ("notlike", "NULL::bytea", "NULL::bytea"),
        ("bytealike", "NULL::bytea", "NULL::bytea"),
        ("byteanlike", "NULL::bytea", "NULL::bytea"),
        ("like_escape", "NULL::bytea", "NULL::bytea"),
        ("overlay", "NULL::bytea", "NULL::bytea", "NULL::integer"),
        ("overlay", "NULL::bytea", "NULL::bytea", "NULL::integer", "NULL::integer"),
        ("position", "NULL::bytea", "NULL::bytea"),
        ("substr", "NULL::bytea", "NULL::integer"),
        ("substr", "NULL::bytea", "NULL::integer", "NULL::integer"),
        ("substring", "NULL::bytea", "NULL::integer"),
        ("substring", "NULL::bytea", "NULL::integer", "NULL::integer"),
        ("encode", "NULL::bytea", "NULL::text"),
    ],
)
def test_the_run_s_null_call_answers_null(runtime, args):
    assert _one(runtime, _call(*args)) is None


# --- values: PostgreSQL 17's answers -------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        (_call("octet_length", _b("0102ff")), 3),
        (_call("length", _b("0102ff")), 3),
        (_call("octet_length", "'abc'::bytea"), 3),
        (_call("octet_length", "'é'::bytea"), 2),  # its UTF-8 bytes
        (_call("bit_length", _b("0102ff")), 24),
        (_call("bit_count", _b("0103ff")), 11),
        (_call("get_bit", _b("0102"), "0"), 1),
        (_call("get_bit", _b("0102"), "8"), 0),
        (_call("get_bit", _b("0102"), "9"), 1),
        (_call("get_byte", _b("01ff"), "1"), 255),
        (_call("like", _b("0102ff"), _b("0125")), True),  # \x01%
        (_call("like", _b("0102ff"), _b("0225")), False),
        (_call("like", _b("0102ff"), _b("5f02ff")), True),  # _\x02\xff
        (_call("like", _b("01255f"), _b("015c255f")), True),  # \x01\%_: an escaped %
        (_call("like", _b("012345"), _b("015c255f")), False),
        (_call("like", "''::bytea", _b("25")), True),
        (_call("bytealike", _b("0102ff"), _b("2502ff")), True),
        (_call("notlike", _b("0102ff"), _b("015fff")), False),
        (_call("byteanlike", _b("0102ff"), _b("02")), True),
        (f"SELECT {_b('0102ff')} LIKE {_b('255fff')} AS v", True),
        (_call("position", _b("0102ff03"), _b("ff03")), 3),
        (_call("position", _b("0102ff03"), _b("09")), 0),
        (_call("position", _b("01"), "''::bytea"), 1),
        (f"SELECT position({_b('ff03')} IN {_b('0102ff03')}) AS v", 3),
        (_call("encode", _b("00ff5c41"), "'hex'"), "00ff5c41"),
        (_call("encode", _b("00ff5c41"), "'HEX'"), "00ff5c41"),
        (_call("encode", _b("00ff5c41"), "'escape'"), "\\000\\377\\\\A"),
        (_call("encode", _b("00ff5c41"), "'base64'"), "AP9cQQ=="),
        (_call("encode", _b("41" * 60), "'base64'"), bytea.base64_lines(b"A" * 60)),
        (_call("encode", "''::bytea", "'base64'"), ""),
    ],
)  # fmt: skip
def test_a_function_of_bytes_answers_postgresql_on_clickhouse(runtime, sql, answer):
    assert _one(runtime, sql) == answer


@pytest.mark.parametrize(
    ("sql", "answer"),
    [
        (_call("byteacat", _b("01"), _b("ff")), b"\x01\xff"),
        (_call("byteacat", _b("01"), "''::bytea"), b"\x01"),
        (_call("btrim", _b("ff01ff02ff"), _b("ff")), b"\x01\xff\x02"),
        (_call("ltrim", _b("ff01ff02ff"), _b("ff")), b"\x01\xff\x02\xff"),
        (_call("rtrim", _b("ff01ff02ff"), _b("ff")), b"\xff\x01\xff\x02"),
        (_call("btrim", _b("ffff"), _b("ff")), b""),
        (_call("btrim", _b("0102"), "''::bytea"), b"\x01\x02"),
        (_call("btrim", _b("01020301"), _b("0301")), b"\x02"),
        (f"SELECT trim(both {_b('ff')} from {_b('ff01ff')}) AS v", b"\x01"),
        (_call("set_bit", _b("0102"), "3", "1"), b"\x09\x02"),
        (_call("set_bit", _b("0102"), "9", "0"), b"\x01\x00"),
        (_call("set_byte", _b("0102"), "0", "511"), b"\xff\x02"),  # its low 8 bits
        (_call("set_byte", _b("0102"), "1", "-1"), b"\x01\xff"),
        (_call("like_escape", _b("41235f"), _b("23")), b"A\\_"),  # # as the escape: \_
        (_call("like_escape", _b("415c"), "''::bytea"), b"A\\\\"),  # no escape: \ doubled
        (_call("like_escape", _b("415c25"), _b("5c")), b"A\\%"),  # \ as the escape: as it is
        (_call("like_escape", _b("23235c"), _b("23")), b"\\#\\\\"),
        (_call("overlay", _b("01020304"), _b("ff"), "2"), b"\x01\xff\x03\x04"),
        (_call("overlay", _b("01020304"), _b("ff"), "2", "2"), b"\x01\xff\x04"),
        (_call("overlay", _b("01020304"), _b("ff"), "2", "0"), b"\x01\xff\x02\x03\x04"),
        (_call("overlay", _b("01020304"), _b("ff"), "5"), b"\x01\x02\x03\x04\xff"),
        (_call("overlay", _b("01020304"), _b("ff"), "2", "-1"), b"\x01\xff\x01\x02\x03\x04"),
        (_call("substr", _b("01020304"), "2"), b"\x02\x03\x04"),
        (_call("substr", _b("01020304"), "5"), b""),
        (_call("substr", _b("01020304"), "-3"), b"\x01\x02\x03\x04"),
        (_call("substr", _b("01020304"), "0", "3"), b"\x01\x02"),
        (_call("substr", _b("01020304"), "2", "100"), b"\x02\x03\x04"),
        (_call("substr", _b("01020304"), "-5", "3"), b""),
        (_call("substring", _b("01020304"), "-1", "3"), b"\x01"),
        (f"SELECT substring({_b('01020304')} from 2 for 2) AS v", b"\x02\x03"),
        # A bytea a row computes is read as one.
        (_call("substr", f'pg_catalog."byteacat"({_b("01")}, {_b("ff")})', "2"), b"\xff"),
        ('SELECT pg_catalog."octet_length"(pg_catalog."btrim"(' f"{_b('ff01ff')}, {_b('ff')})) AS v", 1),
    ],
)  # fmt: skip
def test_a_bytea_answer_is_postgresql_s_bytes(runtime, sql, answer):
    assert _held(_one(runtime, sql)) == answer


def test_a_column_of_values_reads_each_row(runtime):
    sql = (
        'SELECT pg_catalog."octet_length"(c) AS v FROM '
        f"(VALUES ({_b('0102')}), ('abc'::bytea), (NULL::bytea)) AS t(c)"
    )
    rows = list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    # ClickHouse reads a VALUES list as a union of its rows, in no order.
    assert sorted(rows, key=repr) == sorted([(2,), (3,), (None,)], key=repr)


# --- outside the value: PostgreSQL's SQLSTATE ---------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "sqlstate", "message"),
    [
        (_call("get_bit", _b("0102"), "16"), "2202E", "index out of valid range"),
        (_call("get_bit", _b("0102"), "-1"), "2202E", "index out of valid range"),
        (_call("get_byte", _b("0102"), "2"), "2202E", "index out of valid range"),
        (_call("set_bit", _b("0102"), "16", "1"), "2202E", "index out of valid range"),
        (_call("set_byte", _b("0102"), "-1", "1"), "2202E", "index out of valid range"),
        # The index first, as PostgreSQL reads it, then the bit.
        (_call("set_bit", _b("0102"), "3", "2"), "22023", "new bit must be 0 or 1"),
        (_call("set_bit", _b("0102"), "16", "2"), "2202E", "index out of valid range"),
        (_call("substr", _b("0102"), "1", "-1"), "22011", "negative substring length not allowed"),
        (_call("overlay", _b("0102"), _b("03"), "0"), "22011", "negative substring length not allowed"),
        (_call("overlay", _b("0102"), _b("03"), "2", "2147483647"), "22003", "integer out of range"),
        (_call("like_escape", _b("41"), _b("2323")), "22025", "invalid escape string"),
        (_call("encode", _b("41"), "'nope'"), "22023", 'unrecognized encoding: "nope"'),
    ],
)  # fmt: skip
def test_outside_the_value_raises_postgresql_s_sqlstate(runtime, sql, sqlstate, message):
    error = _raised(runtime, sql)
    assert (error.sqlstate, str(error)) == (sqlstate, message)


# --- what ClickHouse would not hold as the bytes: refused by name -------------------------------


@pytest.mark.parametrize(
    ("sql", "function"),
    [
        # The escape form is written as its text on ClickHouse (transpile._bytea_literal).
        (_call("octet_length", "'a\\001'::bytea"), "octet_length"),
        (_call("btrim", "'a\\\\'::bytea", _b("61")), "trim"),
        # A pattern a row computes, and one that ends in its escape.
        ('SELECT pg_catalog."like"(' f"{_b('01')}, p) AS v FROM (VALUES ({_b('25')})) AS t(p)", "like"),
        (_call("like", _b("01"), _b("015c")), "like"),
        # A length a row computes, beside a start function_macros reads from the end.
        ('SELECT substring(' f"{_b('0102')}, 0, n) AS v FROM (VALUES (1)) AS t(n)", "substring"),
        # A format a row computes.
        ('SELECT pg_catalog."encode"(' f"{_b('01')}, f) AS v FROM (VALUES ('hex'::text)) AS t(f)", "encode"),
    ],
)  # fmt: skip
def test_what_clickhouse_would_not_hold_as_the_bytes_is_refused_by_name(sql, function):
    with pytest.raises(FunctionRefused) as refused:
        transpile(sql, "clickhouse")
    assert str(refused.value) == f"{function}() has no form on clickhouse"


def test_a_text_keeps_its_own_form():
    # The rows read a bytea; a text's call is the old table's still.
    written = transpile("SELECT pg_catalog.\"btrim\"('xax'::text, 'x'::text) AS v", "clickhouse")
    assert "splitByString" not in written


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "snowflake"])
def test_every_other_dialect_keeps_what_it_had(dialect):
    assert len(bytea.FEATURES) == 21
    for feature in bytea.FEATURES:
        for rule in feature.read.rules:
            assert rule.unlisted_keep
            if dialect in rule.forms:
                assert rule.forms[dialect] is trino.KEPT
    dot = sqlglot.parse_one(_call("byteacat", _b("01"), _b("02")), read="postgres").find(exp.Dot)
    assert dot is not None
    applied = GRID.read(dot.expression, dialect)
    assert applied is None or applied.form in (None, trino.KEPT)


def test_each_row_states_its_limitation():
    for feature in bytea.FEATURES:
        (stated,) = feature.read.limitations["clickhouse"]
        assert "bytes" in stated.text and "{" not in stated.text
