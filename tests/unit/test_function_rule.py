# Copyright (c) 2026 Kenneth Stott
# Canary: aeb6ff2d-4446-4767-a0b9-0cee659d6459
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""THE RULE FOR TYPES AND FUNCTIONS (REQ-2171): one place, in one order: the dialect's own function
under any name, the functions it has, its inline operators, else refused by name. What a type's
functions answered as Python UDFs they answer as forms, on no UDF."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

from decimal import Decimal

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from provisa.core import pg_money as core_money
from provisa.core.pg_error import from_engine
from provisa.federation.duckdb_pg_functions import register
from provisa.transpiler import function_macros
from provisa.transpiler.function_macros import (
    MACROS,
    MAPPED,
    FunctionRefused,
    Macro,
    Step,
    overload,
)
from provisa.transpiler.pg_xml import XmlFunctionRefused
from provisa.transpiler.transpile import transpile, transpile_to_duckdb

_ON = frozenset({"somewhere"})


def _forms() -> Macro:
    return Macro(
        "f",
        ("a",),
        forms={_ON: "g(a)"},
        own={_ON: "h(a)"},
        operators={_ON: "a + 1"},
    )


def test_step_one_the_dialects_own_function_comes_first():
    assert _forms().resolved("somewhere") == (Step.OWN, "h(a)")


def test_step_one_postgresql_has_every_function_itself():
    assert _forms().resolved("postgres") == (Step.OWN, MAPPED)


def test_step_two_the_functions_it_has_come_before_its_operators():
    macro = Macro("f", ("a",), forms={_ON: "g(a)"}, operators={_ON: "a + 1"})
    assert macro.resolved("somewhere") == (Step.FUNCTIONS, "g(a)")


def test_step_three_inline_operators_answer_where_no_function_does():
    assert Macro("f", ("a",), operators={_ON: "a + 1"}).resolved("somewhere") == (
        Step.OPERATORS,
        "a + 1",
    )


def test_step_four_what_it_can_express_neither_way_is_refused_by_name():
    assert Macro("f", ("a",)).resolved("somewhere") == (Step.REFUSED, None)
    with pytest.raises(FunctionRefused, match=r"area\(\) has no form on mysql"):
        transpile("SELECT area('(1,1),(0,0)'::box) AS v", "mysql")


@pytest.mark.parametrize("dialect", ["duckdb", "trino", "snowflake", "mysql"])
def test_only_a_refusal_or_sqlglots_own_writing_has_no_form(dialect):
    for macro in MACROS.values():
        step, form = macro.resolved(dialect)
        if form is None:
            assert step in (Step.REFUSED, Step.OWN)


@pytest.mark.parametrize(
    ("sql", "row"),
    [
        ("area('(1,1),(0,0)'::box)", "box_area"),
        ("area('((0,0),(1,1))'::path)", "path_area"),
        ("area('<(0,0),1>'::circle)", "circle_area"),
        ("center('<(0,0),1>'::circle)", "circle_center"),
        ("abbrev('10.0.0.0/8'::cidr)", "cidr_abbrev"),
        ("abbrev('10.0.0.1'::inet)", "inet_abbrev"),
        ("strip('a:1'::tsvector)", "tsvector_strip"),
        ("ishorizontal('{0,1,0}'::line)", "line_horizontal"),
        ("area(c)", "area"),
    ],
)
def test_an_overload_is_chosen_by_the_type_the_statement_says(sql, row):
    call = sqlglot.parse_one(sql, read="postgres")
    assert overload(call.name.lower(), list(call.expressions)) == row


@pytest.mark.parametrize(
    "sql",
    [
        "area('(1,1),(0,0)'::box)",
        "area('((0,0),(1,0),(1,1))'::path)",
        "center('(1,1),(0,0)'::box)",
        "diagonal('(1,1),(0,0)'::box)",
        "height('(1,1),(0,0)'::box)",
        "width('(1,1),(0,0)'::box)",
        "length('[(0,0),(1,1)]'::lseg)",
        "length('[(0,0),(1,1)]'::path)",
        "pt_contained_poly('(1,1)'::point, '((0,0),(2,0),(2,2))'::polygon)",
        "pt_contained_circle('(1,1)'::point, '<(0,0),2>'::circle)",
    ],
)
def test_a_shape_on_trino_is_trinos_geometry_not_inline_arithmetic(sql):
    written = transpile(f"SELECT {sql} AS v", "trino")
    assert "ST_" in written.upper()
    assert "SQRT" not in written.upper()
    sqlglot.parse_one(written, read="trino")


@pytest.mark.parametrize(
    "sql",
    [
        "area('<(0,0),1>'::circle)",
        "abbrev('10.0.0.0/8'::cidr)",
        "abbrev('10.0.0.1'::inet)",
        "strip('a:1'::tsvector)",
        "length('a:1'::tsvector)",
        "isparallel('[(0,0),(1,1)]'::lseg, '[(0,1),(1,2)]'::lseg)",
    ],
)
def test_a_type_trino_holds_as_text_reaches_it_with_no_cast_to_the_type(sql):
    tree = sqlglot.parse_one(transpile(f"SELECT {sql} AS v", "trino"), read="trino")
    held = {"box", "path", "circle", "cidr", "inet", "tsvector", "lseg"}
    assert not {c.to.sql(dialect="trino").lower() for c in tree.find_all(exp.Cast)} & held


def test_an_unaliased_overload_keeps_the_column_postgresql_names():
    assert transpile("SELECT area('(1,1),(0,0)'::box)", "trino").endswith(" AS area")


# --- XML: each dialect what it can do, the rest by name -----------------------------------------


@pytest.mark.parametrize("dialect", ["trino", "mysql", "snowflake", "bigquery", "spark"])
def test_xmltext_is_any_dialects_replace(dialect):
    written = transpile("SELECT xmltext(x) AS v FROM t", dialect)
    assert "&amp;" in written and "provisa" not in written.lower()


def test_a_comment_on_trino_refuses_what_postgresql_refuses():
    written = transpile("SELECT xmlcomment(x) AS v FROM t", "trino")
    assert "invalid XML comment" in written


def test_well_formed_documents_are_snowflakes_own_check():
    assert "CHECK_XML" in transpile("SELECT xml_is_well_formed_document(x) FROM t", "snowflake")


def test_xpath_stays_refused_where_no_dialect_answers_as_postgresql():
    with pytest.raises(XmlFunctionRefused, match="xpath .*trino"):
        transpile("SELECT xpath('/a', x) FROM t", "trino")


# --- the answers the UDFs gave, with no UDF ---------------------------------------------------


@pytest.fixture(scope="module")
def con():
    connection = duckdb.connect()
    register(connection)
    return connection


def _literal(value: object) -> str:
    return "NULL" if value is None else exp.Literal.string(str(value)).sql()


def _value(con, row: str, *args: object):
    """A row's form for literal arguments. A form names a parameter as often as it needs it, so a
    prepared statement's placeholder would not bind."""
    called = f"SELECT {row}({', '.join(map(_literal, args))}) AS v"
    tree = function_macros.inline(sqlglot.parse_one(called, read="postgres"), "duckdb")
    return con.execute(tree.sql(dialect="duckdb")).fetchone()[0]


def _money(con, row: str, cents: int):
    value = f"{'-' if cents < 0 else ''}{abs(cents) // 100}.{abs(cents) % 100:02d}"
    called = f"SELECT {row}(CAST({value} AS DECIMAL(21,4))) AS v"
    tree = function_macros.inline(sqlglot.parse_one(called, read="postgres"), "duckdb")
    return con.execute(tree.sql(dialect="duckdb")).fetchone()[0]


@pytest.mark.parametrize(
    ("row", "args", "want"),
    [
        ("area", ("(2,2),(0,0)",), 4.0),
        ("area", ("<(1,2),3>",), 28.274333882308138),
        ("area", ("[(0,0),(1,0)]",), None),
        ("area", ("((0,0),(1,0),(1,1),(0,1))",), 1.0),
        ("area", (None,), None),
        ("path_area", ("((0,0),(2,0),(2,2))",), 2.0),
        ("center", ("(1.5,2.5),(0,0)",), "(0.75,1.25)"),
        ("center", ("<(1,2),3>",), "(1,2)"),
        ("center", ("(1234567,0),(1234567,0)",), "(1.23457e+06,0)"),
        ("diagonal", ("(0,0),(2,2)",), "[(2,2),(0,0)]"),
        ("lseg_length", ("[(0,0),(3,4)]",), 5.0),
        ("path_length", ("((0,0),(3,0),(3,4))",), 12.0),
        ("path_length", ("[(0,0),(3,0),(3,4)]",), 7.0),
        ("ishorizontal", ("{0,1,-2}",), True),
        ("ishorizontal", ("[(0,1),(2,1)]",), True),
        ("isvertical", ("{1,0,-2}",), True),
        ("isparallel", ("{1,1,1}", "{1,1,5}"), True),
        ("isparallel", ("[(0,0),(1,1)]", "[(0,1),(1,2)]"), True),
        ("isperp", ("{1,1,1}", "{1,-1,5}"), True),
        ("ishorizontal", ("(0,1)", "(5,1)"), True),
        ("isvertical", ("(1,0)", "(1,5)"), True),
        ("pt_contained_circle", ("(1,1)", "<(0,0),2>"), True),
        ("pt_contained_circle", ("(3,3)", "<(0,0),2>"), False),
        ("pt_contained_poly", ("(1,1)", "((0,0),(2,0),(2,2),(0,2))"), True),
        ("pt_contained_poly", ("(2,1)", "((0,0),(2,0),(2,2),(0,2))"), True),  # on an edge
        ("pt_contained_poly", ("(3,1)", "((0,0),(2,0),(2,2),(0,2))"), False),
        ("tsvector_strip", ("fat:2,4 cat:3 rat:5A",), "'cat' 'fat' 'rat'"),
        ("tsvector_strip", ("'it''s':1 b",), "'b' 'it''s'"),
        ("tsvector_length", ("fat:2,4 cat:3",), 2),
        ("inet_abbrev", ("10.1.2.3/8",), "10.1.2.3/8"),
        ("inet_abbrev", ("::1/128",), "::1"),
        ("inet_abbrev", ("10.1.0.1/32",), "10.1.0.1"),
        ("cidr_abbrev", ("10.0.0.0/8",), "10/8"),
        ("cidr_abbrev", ("192.168.1.0/24",), "192.168.1/24"),
    ],
)
def test_a_form_answers_as_the_udf_it_replaced(con, row, args, want):
    assert _value(con, row, *args) == want


def test_a_polygons_level_edge_divides_by_nothing(con):
    """The pipeline's division raises on zero, and an AND in a lambda guards nothing: a horizontal
    edge, where the crossing's divisor is zero, is held off by a CASE."""
    written = transpile_to_duckdb(
        "SELECT pt_contained_poly('(-5,0)'::point, '((0,0),(0,8.376),(8,0))'::polygon) AS v"
    )
    assert con.execute(written).fetchone()[0] is False


def test_a_null_box_keeps_the_type_its_diagonal_is_described_by(con):
    written = transpile_to_duckdb("SELECT diagonal(NULL) AS v")
    assert con.execute(f"SELECT typeof(v) FROM ({written})").fetchone()[0] == "VARCHAR"


def test_a_bound_money_input_runs_as_a_form_of_its_parameter(con):
    written = transpile_to_duckdb("SELECT $1::money AS v")
    assert con.execute(written, ["$1,234.50"]).fetchone()[0] == Decimal("1234.5000")


def test_unnest_of_a_tsvector_is_its_lexemes_in_order(con):
    written = transpile_to_duckdb("SELECT unnest('cat:3 fat:2,4 rat:5A'::tsvector) AS v")
    assert [r[0] for r in con.execute(written).fetchall()] == [
        {"lexeme": "cat", "positions": [3], "weights": ["D"]},
        {"lexeme": "fat", "positions": [2, 4], "weights": ["D", "D"]},
        {"lexeme": "rat", "positions": [5], "weights": ["A"]},
    ]


@pytest.mark.parametrize(
    "row",
    [
        "money_text", "money_words", "money_bytes", "money_input", "tsvector_strip", "inet_abbrev",
        "cidr_abbrev", "area", "center", "diagonal", "path_length", "lseg_length",
    ],
)  # fmt: skip
def test_a_form_is_strict_as_the_udf_was(con, row):
    assert _value(con, row, None) is None


_CENTS = (0, 1, 99, 100, 101, 110, 120, 2000, 2001, 10000, 10001, 12345, 100000, 123456789,
          100000000000, 1000000000000000000, 9223372036854775807, -1, -100, -123456,
          -9223372036854775808)  # fmt: skip


@pytest.mark.parametrize("cents", _CENTS)
def test_money_text_is_cash_out(con, cents):
    assert _money(con, "money_text", cents) == core_money.cash_out(cents)


@pytest.mark.parametrize("cents", _CENTS)
def test_money_words_are_cash_words(con, cents):
    assert _money(con, "money_words", cents) == core_money.cash_words(cents)


@pytest.mark.parametrize("cents", _CENTS)
def test_money_bytes_are_the_int8(con, cents):
    assert _money(con, "money_bytes", cents) == cents.to_bytes(8, "big", signed=True)


@pytest.mark.parametrize(
    "text",
    [
        "1", "$1", "$1,234.56", " $ 1.5", "-1", "(1.00)", "1.00-", "$-1", "-$1", "+1", "1.235",
        "1.234", "1.2", ".5", "1,0,0", "1.2,3", "1.999", "", "$", "92233720368547758.07",
        "-92233720368547758.08", "0001", "1.00 ", "1)", "1+", "1$",
    ],
)  # fmt: skip
def test_money_input_is_cash_in(con, text):
    want = core_money.cash_in(text)
    got = _value(con, "money_input", text)
    assert int(got * 100) == want


@pytest.mark.parametrize(
    "text", ["abc", "1x", "1.2.3", "1 2", "--1", "5 +5", ") 5", "92233720368547758.08", "1" * 30]
)
def test_money_input_refuses_what_cash_in_refuses(con, text):
    with pytest.raises(Exception) as expected:
        core_money.cash_in(text)
    with pytest.raises(duckdb.Error) as raised:
        _value(con, "money_input", text)
    refused = from_engine(raised.value)
    assert refused is not None
    assert (refused.sqlstate, str(refused)) == (expected.value.sqlstate, str(expected.value))


@pytest.mark.parametrize(
    "udf",
    ["provisa_cash_out", "provisa_area", "provisa_tsvector_strip", "provisa_abbrev"],
)
def test_the_type_functions_are_no_udf_on_the_engine(con, udf):
    names = {
        r[0]
        for r in con.execute("SELECT DISTINCT function_name FROM duckdb_functions()").fetchall()
    }
    assert udf not in names
