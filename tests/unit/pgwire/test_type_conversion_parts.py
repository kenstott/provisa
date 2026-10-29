# Copyright (c) 2026 Kenneth Stott
# Canary: 6f9b2d40-1e7a-4c35-b8d2-a3e5c7f91b04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The parts pgwire's type conversions are made of (REQ-2167, PGWIRE CATALOG AND TYPES), each
against PostgreSQL 17's documented behaviour: array_in's text form, byteain's hex form, an exact
numeric literal, an untyped NULL, a cast's typmod, bpchar's padding, the ISO date style's offsets
and fractions, DuckDB's INET values, format_type's modifiers and the object identifier types."""

# Requirements: REQ-2167

from __future__ import annotations

import datetime
import ipaddress
from decimal import Decimal
from unittest.mock import MagicMock

import duckdb
import pytest

from provisa.pgwire.cast_types import result_casts
from provisa.transpiler.pg_literals import array_elements
from provisa.transpiler.transpile import transpile_to_duckdb


def _duck(sql: str):
    con = duckdb.connect()
    try:
        return con.execute(transpile_to_duckdb(sql)).fetchone()
    finally:
        con.close()


@pytest.mark.parametrize(
    ("text", "elements"),
    [
        ("{1,2}", ["1", "2"]),
        ("{}", []),
        ("{ 1 , 2 }", ["1", "2"]),
        ('{"a b","c\\"d"}', ["a b", 'c"d']),
        ('{NULL,"NULL",null}', [None, "NULL", None]),
        ("{{1,2},{3,4}}", [["1", "2"], ["3", "4"]]),
        ("{a\\,b}", ["a,b"]),
    ],
)
def test_an_array_literal_is_read_as_array_in_reads_it(text, elements):
    assert array_elements(text) == elements


@pytest.mark.parametrize("text", ["{1,2", "1,2", "{1,,2}", "{1} x", '{a"b}'])
def test_a_string_that_is_no_array_literal_is_refused(text):
    with pytest.raises(ValueError):
        array_elements(text)


def test_array_literals_reach_the_engine_as_arrays():
    assert _duck("""SELECT '{"a b",NULL}'::text[] AS v""") == (["a b", None],)
    assert _duck("SELECT '{{1,2},{3,4}}'::int[][] AS v") == ([[1, 2], [3, 4]],)
    assert _duck("SELECT '{}'::int4[] AS v") == ([],)


def test_a_bytea_hex_literal_is_its_bytes():
    assert _duck("SELECT '\\xdeadbeef'::bytea AS v") == (b"\xde\xad\xbe\xef",)
    assert _duck("SELECT '\\x'::bytea AS v") == (b"",)


def test_an_exact_numeric_literal_keeps_every_digit():
    assert _duck("SELECT '-0.001'::numeric AS v") == (Decimal("-0.001"),)
    assert _duck("SELECT 12345.6789::numeric AS v") == (Decimal("12345.6789"),)
    # A sized numeric keeps the size it was given.
    assert _duck("SELECT '12345.6789'::numeric(10,2) AS v") == (Decimal("12345.68"),)


def test_an_untyped_null_result_column_is_text_and_only_there():
    con = duckdb.connect()
    try:
        con.execute(transpile_to_duckdb("SELECT NULL AS v"))
        assert con.description[0][1] == "VARCHAR"
        # A set operation's NULL takes the other branch's type, as PostgreSQL resolves it.
        con.execute(transpile_to_duckdb("SELECT NULL AS v UNION ALL SELECT 1"))
        assert con.description[0][1] == "INTEGER"
    finally:
        con.close()


def test_a_cast_carries_its_typmod():
    def typing(sql: str):
        (cast,) = result_casts(sql) or (None,)
        assert cast is not None
        return cast.oid, cast.typmod

    assert typing("SELECT 'x'::char AS v") == (1042, 5)  # character is character(1)
    assert typing("SELECT 'x'::character varying(20) AS v") == (1043, 24)
    assert typing("SELECT 1::numeric(5) AS v") == (1700, (5 << 16) + 4)
    assert typing("SELECT 1::numeric AS v") == (1700, -1)
    assert result_casts("SELECT 1 AS v, 'x'::text AS w") == (None, None)
    assert result_casts("SELECT * FROM t") is None


def test_a_character_value_is_cut_and_padded_to_its_length():
    (cast,) = result_casts("SELECT 'abcd'::char(3) AS v") or (None,)
    assert cast is not None and cast.coerce is not None
    assert cast.coerce("abcd") == "abc"
    assert cast.coerce("a") == "a  "


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (datetime.datetime(2024, 6, 15, 12, 30, 45, tzinfo=datetime.UTC), "2024-06-15 12:30:45+00"),
        (
            datetime.datetime(
                2024,
                6,
                15,
                12,
                30,
                45,
                100000,
                tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
            ),
            "2024-06-15 12:30:45.1+05:30",
        ),
        (
            datetime.datetime(
                2024, 6, 15, 1, 2, 3, tzinfo=datetime.timezone(-datetime.timedelta(hours=3))
            ),
            "2024-06-15 01:02:03-03",
        ),
        (datetime.datetime(2024, 6, 15, 12, 30, 45, 123456), "2024-06-15 12:30:45.123456"),
    ],
)
def test_a_timestamp_is_written_in_the_iso_date_style(value, text):
    from buenavista.postgres import _timestamp_text

    assert _timestamp_text(value) == text


def test_a_time_drops_trailing_zeros_of_its_fraction():
    from buenavista.core import BVType
    from buenavista.postgres import BVTYPE_TO_PGTYPE

    assert BVTYPE_TO_PGTYPE[BVType.TIME][1](datetime.time(12, 30, 45, 500000)) == "12:30:45.5"
    tz = datetime.timezone(datetime.timedelta(hours=2))
    assert BVTYPE_TO_PGTYPE[BVType.TIMETZ][1](datetime.time(12, 30, 45, tzinfo=tz)) == (
        "12:30:45+02"
    )


def test_duckdb_inet_values_are_their_addresses():
    from provisa.pgwire.server import _duckdb_inet

    con = duckdb.connect()
    try:
        con.execute("INSTALL inet; LOAD inet")
        for written in ("192.168.0.1", "10.0.0.0/8", "::1", "2001:db8::1/64", "ffff::1"):
            ((value,),) = con.execute(f"SELECT '{written}'::INET").fetchall()
            assert _duckdb_inet(value) == ipaddress.ip_interface(written)
    finally:
        con.close()


def _state():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.multitenancy = False
    state.contexts = {"testrole": ctx}
    state.schema_build_cache = {"column_types": {}}
    return state


def _answer(sql: str):
    from provisa.pgwire.catalog import answer, classify

    assert classify(sql) == "INTERCEPT", sql
    return answer(sql, "testrole", _state())


@pytest.mark.parametrize(
    ("call", "name"),
    [
        ("format_type(1042, 9)", "character(5)"),
        ("format_type(1042, -1)", "bpchar"),
        ("format_type(1042, NULL)", "character"),
        ("format_type(1043, NULL)", "character varying"),
        ("format_type(1700, 655366)", "numeric(10,2)"),
        ("format_type(1114, 3)", "timestamp(3) without time zone"),
        ("format_type(1184, 0)", "timestamp(0) with time zone"),
        ("format_type(1560, 8)", "bit(8)"),
        ("format_type(23, 4)", "integer"),
    ],
)
def test_format_type_writes_the_modifier_its_typmod_gives(call, name):
    assert _answer(f"SELECT pg_catalog.{call} AS v").rows == [(name,)]


def test_an_object_named_by_no_relation_type_or_schema_is_an_error():
    for sql, message in (
        ("SELECT 'nope'::regclass AS v", 'relation "nope" does not exist'),
        ("SELECT 'nope'::regtype AS v", 'type "nope" does not exist'),
        ("SELECT 'nope'::regnamespace AS v", 'schema "nope" does not exist'),
    ):
        with pytest.raises(Exception, match=message):
            _answer(sql)


def test_a_reg_value_naming_no_object_is_its_number():
    assert _answer("SELECT 5::regclass AS v").rows == [({"oid": 5, "name": "5"},)]
    assert _answer("SELECT 5::regclass::text AS v").rows == [("5",)]
    assert _answer("SELECT 'int4'::regtype::text AS v").rows == [("integer",)]
    assert _answer("SELECT 'pg_catalog'::regnamespace::text AS v").rows == [("pg_catalog",)]


def test_quote_ident_quotes_only_what_needs_it():
    assert _answer(
        "SELECT pg_catalog.quote_ident('x'), pg_catalog.quote_ident('Mixed'), "
        "pg_catalog.quote_ident('a\"b')"
    ).rows == [("x", '"Mixed"', '"a""b"')]
