# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7d1e60-94af-4c13-8e7b-5f06a3c9d214
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The per-engine function conformance run (REQ-2167, REQ-2171): which overload is called how, how an
answer compares, how the form of one that cannot compare is checked, one outcome per engine, and the
guaranteed list the merge writes."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml
from psycopg.sql import Composable

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "conformance"))

import fill_docs  # noqa: E402
import function_report as report  # noqa: E402
import function_support as support  # noqa: E402
import pgwire_catalog_types as catalog  # noqa: E402
from pgwire_function_probe import Function, call, refused_by_name  # noqa: E402

ABS = Function("abs", "integer", argtypes=("integer",), volatility="i", rettype="integer")


class Fake:
    """A server answering by statement; a statement it does not know is refused."""

    def __init__(self, answers: dict[str, dict[str, Any]]) -> None:
        self.answers = answers

    def answer(self, sql: str) -> dict[str, Any]:
        return self.answers.get(sql, {"ok": False, "error": "ERROR:  no such function"})


class Refusing(Fake):
    """As ``Fake``, refusing a statement it does not know with one SQLSTATE and message."""

    def __init__(self, answers: dict[str, dict[str, Any]], *, sqlstate: str, error: str) -> None:
        super().__init__(answers)
        self.refusal = {"ok": False, "error": error, "sqlstate": sqlstate}

    def answer(self, sql: str) -> dict[str, Any]:
        return self.answers.get(sql, self.refusal)


def _ok(*values: Any, oid: int = 23) -> dict[str, Any]:
    return {"ok": True, "fields": [oid], "rows": [(v,) for v in values]}


def _route(_sql: str) -> str:
    return "PASS_THROUGH"


def _both(fn: Function, family: str, want: dict[str, Any], got: dict[str, Any]) -> dict[str, Any]:
    """``fn`` measured where every call of it answers ``want`` and ``got``."""
    sqls = [c.sql for c in support.calls(fn, family)]
    return support.measure_overload(
        fn, family, Fake({s: want for s in sqls}), Fake({s: got for s in sqls}), _route, engine="pg"
    )


# --- which calls an overload gets ---------------------------------------------------------------


def test_an_immutable_function_is_called_with_null_and_with_three_rows_of_values():
    planned = support.calls(ABS, "Numeric")
    assert planned[0].sql == call(ABS) and not planned[0].answers and planned[0].values
    assert [c.sql for c in planned[1:]] == [
        'SELECT pg_catalog."abs"(3::integer) AS v',
        'SELECT pg_catalog."abs"((-7)::integer) AS v',
        'SELECT pg_catalog."abs"(42::integer) AS v',
    ]
    assert all(c.answers for c in planned[1:])


@pytest.mark.parametrize(
    ("fn", "family", "why"),
    [
        (
            Function("setval", "regclass, bigint", argtypes=("regclass", "bigint")),
            "System",
            "volatile",
        ),
        (
            Function("pg_terminate_backend", "integer", argtypes=("integer",), volatility="v"),
            "System",
            "volatile",
        ),
        (
            Function("pg_stat_get_db_xact_commit", "oid", argtypes=("oid",), volatility="s"),
            "System",
            "answers with its own server's state",
        ),
        (
            Function("age", "xid", argtypes=("xid",), volatility="s"),
            "System",
            "answers with its own server's state",
        ),
        (
            Function("now", "", volatility="s", rettype="timestamp with time zone"),
            "Date",
            "answers from the clock",
        ),
        (
            Function("current_user", "", volatility="s", rettype="name"),
            "System",
            "by design (REQ-588): names the role the session acts as",
        ),
        (
            Function("gtsvectorout", "gtsvector", argtypes=("gtsvector",), volatility="i"),
            "Type operators",
            "PostgreSQL takes no literal of gtsvector",
        ),
        (
            Function("f", "polygon[]", argtypes=("polygon[]",), volatility="i"),
            "Geometric",
            "no sample of polygon[]",
        ),
    ],
)
def test_a_function_values_cannot_judge_or_must_not_run_gets_none(fn, family, why):
    assert support.unchecked(fn, family) == why
    assert support.value_calls(fn, family) == []


def test_a_stable_catalog_or_session_function_is_given_values_like_any_other():
    setting = Function("current_setting", "text", argtypes=("text",), volatility="s")
    assert [c.sql for c in support.value_calls(setting, "System")] == [
        f"""SELECT pg_catalog."current_setting"({name}::text) AS v"""
        for name in ("'TimeZone'", "'DateStyle'", "'IntervalStyle'")
    ]
    age = Function("age", "timestamp", argtypes=("timestamp without time zone",), volatility="s")
    assert len(support.value_calls(age, "Date")) == 3


def test_an_input_function_reads_its_types_samples_as_cstring():
    int4in = Function("int4in", "cstring", argtypes=("cstring",), volatility="i")
    assert [c.sql for c in support.value_calls(int4in, "Type operators")] == [
        f"""SELECT pg_catalog."int4in"({v}::cstring) AS v""" for v in ("'3'", "'-7'", "'42'")
    ]
    numeric_in = Function(
        "numeric_in",
        "cstring, oid, integer",
        argtypes=("cstring", "oid", "integer"),
        volatility="i",
    )
    assert support.value_calls(numeric_in, "Type operators")[0].sql == (
        """SELECT pg_catalog."numeric_in"('2.5'::cstring, 'numeric'::regtype::oid, """
        "(-1)::integer) AS v"
    )


@pytest.mark.parametrize(
    ("argtype", "first"),
    [
        ("tid", "'(0,1)'::tid"),
        ("xid8", "'3'::xid8"),
        ("record", "ROW(1, 'a'::text)::record"),
        ("anyenum", "'a'::conformance.probe_enum"),
        ("oidvector", "'1 2 3'::oidvector"),
        ("regclass", "'pg_catalog.pg_class'::regclass"),
        ("aclitem", "'pg_monitor=r/pg_monitor'::aclitem"),
    ],
)
def test_a_type_no_column_holds_has_samples_postgresql_accepts(argtype, first):
    fn = Function("f", argtype, argtypes=(argtype,), volatility="i")
    assert support.value_calls(fn, "Other")[0].sql == f"""SELECT pg_catalog."f"({first}) AS v"""


def test_a_privilege_function_asks_of_a_role_an_object_and_a_privilege():
    fn = Function(
        "has_column_privilege",
        "name, oid, smallint, text",
        argtypes=("name", "oid", "smallint", "text"),
        volatility="s",
    )
    assert support.value_calls(fn, "System")[0].sql == (
        """SELECT pg_catalog."has_column_privilege"('pg_monitor'::name, """
        "'pg_catalog.pg_class'::regclass::oid, 2::smallint, 'SELECT'::text) AS v"
    )
    two = Function("has_schema_privilege", "text, text", argtypes=("text", "text"), volatility="s")
    assert support.value_calls(two, "System")[1].sql == (
        """SELECT pg_catalog."has_schema_privilege"('public'::text, 'CREATE'::text) AS v"""
    )


def test_a_raw_literal_is_written_as_it_stands():
    fn = Function("json_build_object", 'VARIADIC "any"', argtypes=('"any"',), volatility="s")
    assert support.value_calls(fn, "JSON")[0].sql == (
        """SELECT pg_catalog."json_build_object"(1::integer, 2::integer) AS v"""
    )


def test_no_volatile_function_is_ever_given_a_value():
    # A real value given to one runs it for real (pgwire_function_probe.ARGUMENTS).
    lo_unlink = Function("lo_unlink", "oid", argtypes=("oid",), volatility="v")
    assert [c.answers for c in support.calls(lo_unlink, "Large object")] == [False]


def test_the_null_call_of_a_function_answering_its_servers_own_compares_its_type_alone():
    now = Function("now", "", volatility="s", rettype="timestamp with time zone")
    assert [(c.values, c.answers, c.form) for c in support.calls(now, "Date")] == [
        (False, False, True)
    ]
    stat = Function("pg_stat_get_db_xact_commit", "oid", argtypes=("oid",), volatility="s")
    assert [(c.values, c.answers) for c in support.calls(stat, "System")][0] == (False, False)
    setting = Function("current_setting", "text", argtypes=("text",), volatility="s")
    assert support.calls(setting, "System")[0].values


def test_a_function_argument_takes_the_literal_its_type_cannot_give():
    trunc = Function("date_trunc", "text, date", argtypes=("text", "date"), volatility="i")
    first = support.value_calls(trunc, "Date")[0].sql
    assert first == """SELECT pg_catalog."date_trunc"('day'::text, '2024-06-15'::date) AS v"""


def test_a_literal_named_for_one_first_argument_leaves_the_other_overloads_alone():
    text = Function("timezone", "text, date", argtypes=("text", "date"), volatility="i")
    interval = Function("timezone", "interval, date", argtypes=("interval", "date"), volatility="i")
    assert "'UTC'::text" in support.value_calls(text, "Date")[0].sql
    assert "'1 day 02:03:04'::interval" in support.value_calls(interval, "Date")[0].sql


def test_a_pseudo_type_argument_takes_its_concrete_type():
    length = Function(
        "array_length", "anyarray, integer", argtypes=("anyarray", "integer"), volatility="i"
    )
    assert "'{1,2,3}'::integer[]" in support.value_calls(length, "Array")[0].sql


def test_a_variadic_array_is_passed_as_variadic():
    fn = Function(
        "concat_ws", "text, VARIADIC text[]", argtypes=("text", "text[]"), variadic=True,
        volatility="i",
    )  # fmt: skip
    assert "VARIADIC '{a,b,c}'::text[]" in support.value_calls(fn, "String")[0].sql


def test_an_aggregate_reads_its_values_as_rows_of_a_relation_that_is_no_table():
    fn = Function("sum", "integer", kind="a", argtypes=("integer",), volatility="i")
    [one] = support.value_calls(fn, "Aggregate and window")
    assert one.sql == (
        'SELECT pg_catalog."sum"(c1) AS v FROM '
        "(VALUES (3::integer), ((-7)::integer), (42::integer)) AS t(c1)"
    )


def test_an_order_keeping_aggregate_is_given_its_order():
    fn = Function("array_agg", "anynonarray", kind="a", argtypes=("anynonarray",), volatility="i")
    assert '"array_agg"(c1 ORDER BY c1)' in support.value_calls(fn, "Array")[0].sql


def test_an_ordered_set_aggregate_takes_a_direct_argument_within_group():
    fn = Function(
        "percentile_cont", "double precision ORDER BY double precision", kind="a", aggkind="o",
        direct=1, argtypes=("double precision", "double precision"), volatility="i",
    )  # fmt: skip
    assert support.value_calls(fn, "Aggregate and window")[0].sql == (
        'SELECT pg_catalog."percentile_cont"(0.25::double precision) WITHIN GROUP (ORDER BY c1) AS v '
        "FROM (VALUES (0.5::double precision), ((-2.25)::double precision), "
        "(100::double precision)) AS t(c1)"
    )


def test_a_hypothetical_set_aggregate_ranks_its_direct_argument():
    fn = Function(
        "rank", 'VARIADIC "any" ORDER BY VARIADIC "any"', kind="a", aggkind="h", direct=1,
        argtypes=('"any"',), volatility="i",
    )  # fmt: skip
    assert support.value_calls(fn, "Aggregate and window")[0].sql == (
        'SELECT pg_catalog."rank"(3::integer) WITHIN GROUP (ORDER BY c1) AS v '
        "FROM (VALUES (1), (2), (3)) AS t(c1)"
    )


def test_a_window_function_runs_over_rows_in_a_stated_order():
    fn = Function("lag", "anyelement", kind="w", argtypes=("anyelement",), volatility="i")
    assert support.value_calls(fn, "Aggregate and window")[0].sql == (
        'SELECT pg_catalog."lag"(c2) OVER (ORDER BY c1) AS v FROM '
        "(VALUES (1, 3::integer), (2, (-7)::integer), (3, 42::integer)) AS t(c1, c2)"
    )


def test_a_function_of_no_argument_that_is_immutable_has_one_call_and_it_is_an_answer():
    fn = Function("pi", "", volatility="i", rettype="double precision")
    assert [(c.answers, c.values) for c in support.calls(fn, "Numeric")] == [(True, True)]


def test_an_internal_argument_has_no_call():
    fn = Function("int4_accum", "internal", argtypes=("internal",), volatility="i")
    assert support.calls(fn, "Aggregate support") == []


# --- how an answer compares ---------------------------------------------------------------------


def _floats(*values: str | None, oid: int = 701) -> dict[str, Any]:
    return _ok(*values, oid=oid)


def test_floats_compare_within_one_part_in_ten_to_the_fourteen():
    near = _floats("1.0000000000000005")
    assert support.difference(_floats("1"), near, values=True, sql="SELECT 1") is None
    far = _floats("1.00000000000002")
    assert (
        support.difference(_floats("1"), far, values=True, sql="SELECT 1")
        == "'1.00000000000002', PostgreSQL '1'"
    )


def test_a_numeric_compares_exactly():
    want, got = _ok("1.0000000000000001", oid=1700), _ok("1", oid=1700)
    assert (
        support.difference(want, got, values=True, sql="SELECT 1")
        == "'1', PostgreSQL '1.0000000000000001'"
    )


def test_float_arrays_compare_element_by_element():
    a, b = _ok("{1,2}", oid=1022), _ok("{1.0000000000000002,2}", oid=1022)
    assert support.difference(a, b, values=True, sql="SELECT 1") is None
    assert support.difference(a, _ok("{1,3}", oid=1022), values=True, sql="SELECT 1") is not None
    assert support.difference(a, _ok("{1}", oid=1022), values=True, sql="SELECT 1") is not None


def test_infinity_and_nan_compare_as_written():
    assert (
        support.difference(_floats("Infinity"), _floats("Infinity"), values=True, sql="SELECT 1")
        is None
    )
    assert support.difference(_floats("NaN"), _floats("1"), values=True, sql="SELECT 1") is not None


@pytest.mark.parametrize("oid", [114, 3802])
@pytest.mark.parametrize(
    ("want", "got", "differs"),
    [
        ('{"a": 1.50}', '{"a": 1.5}', False),
        ("[1e2, 1.0, -0]", "[100, 1, 0]", False),
        ("1.50", "1.5", False),
        ("0.1", "0.10000000000000001", False),
        ("1.50", "1.51", True),
        ("[12345678901234567890]", "[12345678901234567891]", True),
        ('{"a": 1.50}', '{"a":1.5}', False),  # spacing is no difference
        ('{"b": 1, "a": 2}', '{"a": 2, "b": 1}', False),  # nor is key order
        ('{"a": {"y": 1, "b": [1]}}', '{"a":{"b":[1.0],"y":1}}', False),
        ('{"a": 1}', '{"a": 1, "b": 2}', True),
        ('{"a": 1}', '{"b": 1}', True),
        ('["1.50"]', '["1.5"]', True),  # a string is its text
        ("[1, 2]", "[2, 1]", True),  # an array stated as a value keeps its order
        ("[1, [2, 3]]", "[1, [3, 2]]", True),
        ("[true]", "[1]", True),  # a boolean is no number
        ("[null]", "[0]", True),
        ('["1"]', "[1]", True),
        ("[1]", "[1", True),  # one side is not JSON: text, and so different
        ("not json", "not  json", True),
    ],
)
def test_json_compares_as_parsed_json(oid: int, want: str, got: str, differs: bool):
    a = {"ok": True, "fields": [oid], "rows": [(want,)]}
    b = {"ok": True, "fields": [oid], "rows": [(got,)]}
    assert (support.difference(a, b, values=True, sql="SELECT 1") is not None) is differs


_AGGREGATE = "SELECT json_agg(x) FROM t"


@pytest.mark.parametrize(
    ("sql", "want", "got", "differs"),
    [
        (_AGGREGATE, "[1, 2, 2]", "[2, 1.0, 2]", False),
        (_AGGREGATE, "[1, 2, 2]", "[1, 1, 2]", True),
        (_AGGREGATE, "[1, 2]", "[1, 2, 2]", True),
        (_AGGREGATE, "[1, 2]", "[2, 1, 3]", True),
        (_AGGREGATE, "[[1, 2], [3]]", "[[3], [2, 1]]", True),  # each element is a stated value
        ('SELECT pg_catalog."jsonb_agg_strict"(x) FROM t', "[1, 2]", "[2, 1]", False),
        ("SELECT to_json(array_agg(x)) FROM t", "[1, 2]", "[2, 1]", False),
        ("SELECT json_agg(x ORDER BY x) FROM t", "[1, 2]", "[2, 1]", True),
        ("SELECT json_agg(x) FROM (SELECT x FROM t order  by x) s", "[1, 2]", "[2, 1]", True),
        ("SELECT json_object_agg(k, x) FROM t", "[1, 2]", "[2, 1]", True),
        ("SELECT '[1, 2]'::json", "[1, 2]", "[2, 1]", True),
    ],
)
def test_only_an_aggregate_with_no_order_by_leaves_its_array_open(
    sql: str, want: str, got: str, differs: bool
):
    a, b = _ok(want, oid=114), _ok(got, oid=114)
    assert (support.difference(a, b, values=True, sql=sql) is not None) is differs


@pytest.mark.parametrize(
    ("sql", "want", "got", "differs"),
    [
        ("SELECT d ->> 0", "1.50", "1.5", False),
        ("SELECT d #>> '{a}'", '{"b": 1, "a": [1.50]}', '{"a":[1.5],"b":1}', False),
        ('SELECT pg_catalog."json_array_element_text"(d, 0)', "1.50", "1.5", False),
        ("SELECT jsonb_extract_path_text(d, 'a')", "[1]", "[ 1.0 ]", False),
        ("SELECT d ->> 0", "1.50", "1.51", True),
        ("SELECT d ->> 0", "[1, 2]", "[2, 1]", True),
        ("SELECT d ->> 0", "true", "TRUE", True),
        ("""SELECT '["1.50"]'::json ->> 0""", "1.50", "1.5", True),  # a json string
        ("SELECT 'x'::text", "1.50", "1.5", True),  # no read of json
        ("SELECT array_to_text(d)", "1.50", "1.5", True),  # nor is this one
    ],
)
def test_a_read_of_json_as_text_compares_as_json_but_for_a_string(
    sql: str, want: str, got: str, differs: bool
):
    a, b = _ok(want, oid=25), _ok(got, oid=25)
    assert (support.difference(a, b, values=True, sql=sql) is not None) is differs


def test_a_read_cast_to_another_type_is_that_type():
    # numeric's rule, by value and so equivalent, never JSON's: its scale is still not exact.
    a, b = _ok("1.50", oid=1700), _ok("1.5", oid=1700)
    compared = support.compare(a, b, values=True, sql="SELECT (d ->> 0)::numeric")
    assert (compared.difference, set(compared.rules)) == (None, {report.NUMBER_VALUE})
    other = _ok("1.6", oid=1700)
    assert support.difference(a, other, values=True, sql="SELECT (d ->> 0)::numeric") is not None


@pytest.mark.parametrize("oid", [114, 3802])
def test_json_rows_are_a_multiset_of_parsed_json_not_of_text(oid: int):
    want = _ok('{"b": 1, "a": 2}', '{"a": 9}', "1.50", oid=oid)
    got = _ok("1.5", '{"a":2,"b":1.0}', '{ "a": 9 }', oid=oid)
    assert support.difference(want, got, values=True, sql="SELECT 1") is None


def test_rows_of_floats_are_a_multiset_of_values():
    want, got = _floats("1", "1.0000000000000002", "3"), _floats("1.0000000000000001", "3", "1")
    assert support.difference(want, got, values=True, sql="SELECT 1") is None


def test_a_different_type_differs_whatever_the_text():
    differs = support.difference(_ok("3", oid=23), _ok("3", oid=20), values=True, sql="SELECT 1")
    assert differs == "result type oids [20], PostgreSQL [23]"


def test_rows_compare_as_a_multiset():
    assert support.difference(_ok("1", "2"), _ok("2", "1"), values=True, sql="SELECT 1") is None
    assert support.difference(_ok("1", "1"), _ok("1", "2"), values=True, sql="SELECT 1") is not None
    assert (
        support.difference(_ok("1", "2"), _ok("1"), values=True, sql="SELECT 1")
        == "1 rows, PostgreSQL 2"
    )


def test_a_null_is_no_value():
    assert (
        support.difference(_ok(None), _ok("1"), values=True, sql="SELECT 1")
        == "'1', PostgreSQL None"
    )


def test_a_type_only_comparison_ignores_the_rows():
    assert support.difference(_ok("1"), _ok("2"), values=False, sql="SELECT 1") is None


# --- one outcome per overload -------------------------------------------------------------------


def test_every_call_equal_is_a_pass_with_its_answers_checked():
    entry = _both(ABS, "Numeric", _ok("3"), _ok("3"))
    assert entry["outcome"] == "pass"
    assert entry["answers_checked"] and entry["answers_compared"] == 3
    assert "unchecked" not in entry


def test_a_call_with_nothing_compared_is_not_checked_and_says_why():
    fn = Function("f", "polygon[]", argtypes=("polygon[]",), volatility="i")
    entry = _both(fn, "Geometric", _ok(None), _ok(None))
    assert entry["outcome"] == "not_checked"
    assert not entry["answers_checked"] and not entry["type_and_form_checked"]
    assert entry["unchecked"] == "no sample of polygon[]"


def test_a_null_postgresql_answers_holds_provisa_to_null_and_is_no_pass():
    fn = Function("setseed", "double precision", argtypes=("double precision",), volatility="v")
    entry = _both(fn, "Numeric", _ok(None), _ok("0"))
    assert entry["outcome"] == "wrong_answer"
    assert entry["detail"]["difference"] == "'0', PostgreSQL None"


def test_a_value_call_that_differs_is_a_wrong_answer_even_when_null_agrees():
    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    want = {sqls[0]: _ok(None), sqls[1]: _ok("3"), sqls[2]: _ok("7"), sqls[3]: _ok("42")}
    got = {**want, sqls[2]: _ok("-7")}
    entry = support.measure_overload(ABS, "Numeric", Fake(want), Fake(got), _route, engine="pg")
    assert entry["outcome"] == "wrong_answer"
    assert entry["detail"] == {"statement": sqls[2], "difference": "'-7', PostgreSQL '7'"}


def test_a_wrong_answer_outranks_an_error():
    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    want = {s: _ok("1") for s in sqls}
    got = {sqls[1]: _ok("2")}
    entry = support.measure_overload(ABS, "Numeric", Fake(want), Fake(got), _route, engine="pg")
    assert entry["outcome"] == "wrong_answer"


def test_an_error_keeps_the_engines_message():
    entry = _both(ABS, "Numeric", _ok("3"), {"ok": False, "error": "ERROR:  Scalar Function"})
    assert entry["outcome"] == "error"
    assert entry["detail"]["error"] == "ERROR:  Scalar Function"


def test_a_value_call_postgresql_refuses_compares_nothing():
    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    want = {sqls[0]: _ok(None), sqls[1]: _ok("3")}
    got = {**want, sqls[2]: _ok("anything")}
    entry = support.measure_overload(ABS, "Numeric", Fake(want), Fake(got), _route, engine="pg")
    assert entry["outcome"] == "pass" and entry["answers_compared"] == 1


def test_every_value_call_refused_leaves_it_not_checked():
    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    want = {sqls[0]: _ok(None)}
    entry = support.measure_overload(ABS, "Numeric", Fake(want), Fake(want), _route, engine="pg")
    assert entry["outcome"] == "not_checked" and not entry["answers_checked"]
    assert entry["unchecked"] == "PostgreSQL refused every value call"


def test_postgresql_refusing_every_call_is_not_callable():
    entry = support.measure_overload(ABS, "Numeric", Fake({}), Fake({}), _route, engine="pg")
    assert entry["outcome"] == "not_callable"
    assert entry["detail"] == "PostgreSQL refuses the call: ERROR:  no such function"


def test_an_overload_with_no_call_is_not_callable_for_that_reason():
    fn = Function("int4_accum", "internal", argtypes=("internal",), volatility="i")
    entry = support.measure_overload(
        fn, "Aggregate support", Fake({}), Fake({}), _route, engine="pg"
    )
    assert entry["outcome"] == "not_callable"
    assert entry["detail"] == "takes an internal argument, which no SQL expression can supply"


def test_what_the_catalog_db_answers_is_marked_so():
    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    want = {s: _ok("3") for s in sqls}
    entry = support.measure_overload(
        ABS, "Numeric", Fake(want), Fake(want), lambda _s: "INTERCEPT", engine="pg"
    )
    assert entry["served_by"] == "catalog"


def test_a_hung_statement_costs_its_overload_not_the_leg(monkeypatch):
    class Hung:
        def answer(self, _sql: str) -> dict[str, Any]:
            return {"ok": False, "error": "no answer in 30s"}

    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    entry = support.measure_overload(
        ABS, "Numeric", Fake({s: _ok("3") for s in sqls}), Hung(), _route, engine="pg"
    )
    assert entry["outcome"] == "error" and entry["detail"]["error"] == "no answer in 30s"


# --- the form of an answer that cannot be compared -----------------------------------------------


NOW = Function("now", "", volatility="s", rettype="timestamp with time zone")
UUID = Function("gen_random_uuid", "", volatility="v", rettype="uuid")
RANDOM = Function("random", "", volatility="v", rettype="double precision")
PID = Function("pg_backend_pid", "", volatility="s", rettype="integer")
USER = Function("current_user", "", volatility="s", rettype="name")
VERSION = Function("version", "", volatility="s", rettype="text")
TIMEOFDAY = Function("timeofday", "", volatility="v", rettype="text")
SETTINGS_ROWS = Function(
    "pg_show_all_settings", "OUT name text, OUT setting text", volatility="s", rettype="record"
)
_V4 = "dfe9efb2-a1e6-472d-835e-a79e9f1d8463"
_THEN = "2026-10-31 17:20:18.01903+00"


@pytest.mark.parametrize(
    ("fn", "oid", "pg", "provisa"),
    [
        (UUID, 2950, _V4, "00000000-0000-4000-8000-000000000000"),
        (RANDOM, 701, "0.8675929329186349", "1.2345e-05"),
        (RANDOM, 701, "0.5", "0"),
        # Run 42527458223: random(-2.25, 5.125)'s form is a numeric of scale 3, of either sign.
        (RANDOM, 1700, "-0.803", "1.023"),
        (RANDOM, 1700, "2.525", "-0.400"),
        (PID, 23, "96517", "1"),
        (USER, 19, "postgres", "conformance"),
        (VERSION, 25, "PostgreSQL 17.11 (Debian) on x86_64, 64-bit", "PostgreSQL 17.11 on Provisa"),
    ],
)
def test_an_answer_of_the_form_postgresql_answers_in_is_a_pass_type_and_form_checked(
    fn, oid, pg, provisa
):
    entry = _both(fn, "System", _ok(pg, oid=oid), _ok(provisa, oid=oid))
    assert entry["outcome"] == "pass"
    assert entry["type_and_form_checked"] and not entry["answers_checked"]
    assert entry["unchecked"]  # the value itself was still not compared


@pytest.mark.parametrize(
    ("fn", "oid", "pg", "provisa"),
    [
        (UUID, 2950, _V4, "dfe9efb2-a1e6-172d-835e-a79e9f1d8463"),  # version 1
        (RANDOM, 701, "0.5", "1.5"),
        (RANDOM, 701, "0.5", "-0.5"),
        (RANDOM, 1700, "-0.803", "1.02"),
        (RANDOM, 1700, "2.525", "-0.4754636315882823"),
        (PID, 23, "96517", "0"),
        (USER, 19, "postgres", "two words"),
        (VERSION, 25, "PostgreSQL 17.11 on x86_64, 64-bit", "Provisa 1.0"),
    ],
)
def test_an_answer_of_another_form_is_a_wrong_answer_saying_so(fn, oid, pg, provisa):
    entry = _both(fn, "System", _ok(pg, oid=oid), _ok(provisa, oid=oid))
    assert entry["outcome"] == "wrong_answer"
    assert entry["detail"]["difference"] == (f"{provisa!r} is not the form of PostgreSQL's {pg!r}")


@pytest.mark.parametrize(
    ("oid", "good", "bad"),
    [
        (1082, "2024-06-15", "06/15/2024"),
        (1083, "12:30:45.5", "12:30"),
        (1186, "1 year 2 mons 3 days 04:05:06.5", "P1Y"),
        (1186, "-1 days", ""),
        (1186, "00:00:00", "soon"),
        (869, "192.168.0.1/24", "localhost"),
        (869, "::1", "1"),
        (869, "2001:db8::1/64", "2001"),
        (20, "-7", "7.5"),
        (1700, "-0.803", "1.0234"),
        (1700, "1.023", "-0.80"),
        (1700, "1000", "1000.0"),
        (1700, "-7", "7.5"),
        (16, "t", "true"),
        (3220, "0/14A2520", "14A2520"),
        (5038, "730:730:", "730"),
        (5038, "730:735:731,733", "730:735"),
    ],
)
def test_a_type_with_a_form_of_its_own_is_held_to_it(oid, good, bad):
    form = support.value_form("any", oid, good)
    assert support.re.fullmatch(form, good)
    assert support.re.fullmatch(form, bad) is None


def test_a_functions_own_form_holds_for_the_overload_answering_its_type_only():
    assert support.value_form("random", 701, "0.5") == support.FUNCTION_FORMS["random"]
    assert support.re.fullmatch(support.value_form("random", 23, "7"), "2")
    assert support.re.fullmatch(support.value_form("random", 1700, "1.53"), "2.23")


def test_a_string_with_no_form_of_its_own_takes_the_shape_of_postgresqls_answer():
    query = 'SELECT pg_catalog."current_query"() AS v'
    form = support.value_form("current_query", 25, query)
    assert support.re.fullmatch(form, query)
    assert support.re.fullmatch(form, 'SELECT "current_query"() AS v') is None
    hex_or_not = support.value_form("pg_export_snapshot", 25, "00000003-00000040-1")
    assert support.re.fullmatch(hex_or_not, "00000003-0000003A-1")


def test_a_record_is_held_to_its_number_of_fields():
    sample = '("","SELECT 1, ""x""",f,"2026-10-31 17:20:18+00")'
    form = support.value_form("pg_cursor", 2249, sample)
    assert support.re.fullmatch(form, sample)
    assert support.re.fullmatch(form, "(a,b,,)")
    assert support.re.fullmatch(form, "(a,b,c)") is None
    assert support.re.fullmatch(form, "(a,b,c,d,e)") is None


def test_a_stable_function_only_called_with_null_arguments_is_not_checked():
    # Its answer is its server's own, and no real value is given it: the NULL call alone.
    fn = Function("pg_stat_get_buf_alloc", "text", argtypes=("text",), volatility="s")
    entry = _both(fn, "System", _ok("whatever", oid=25), _ok("no form at all", oid=25))
    assert entry["outcome"] == "not_checked"
    assert not entry["type_and_form_checked"] and not entry["answers_checked"]


def test_a_volatile_function_only_called_with_null_arguments_has_its_type_checked():
    """REQ-2167, amended 2026-11-04 (4): a volatile overload is never left not tested for being
    volatile: its NULL call's result type and NULLs are PostgreSQL's."""
    fn = Function("f", "double precision", argtypes=("double precision",), volatility="v")
    entry = _both(fn, "System", _ok("whatever", oid=25), _ok("no form at all", oid=25))
    assert entry["outcome"] == "pass" and entry["type_and_form_checked"]
    assert not entry["answers_checked"]
    assert _both(fn, "System", _ok("whatever", oid=25), _ok("1", oid=23))["outcome"] == (
        "wrong_answer"
    )


def test_a_null_from_postgresql_leaves_no_form_to_check_and_holds_provisa_to_null():
    # Volatile: its type and its NULL are PostgreSQL's (REQ-2167, amended 2026-11-04 (4)).
    entry = _both(UUID, "System", _ok(None, oid=2950), _ok(None, oid=2950))
    assert entry["outcome"] == "pass" and entry["type_and_form_checked"]
    entry = _both(UUID, "System", _ok(None, oid=2950), _ok(_V4, oid=2950))
    assert entry["outcome"] == "wrong_answer"


def test_a_void_answer_has_no_form_and_is_tested_by_its_side_effect():
    fn = Function("pg_stat_clear_snapshot", "", volatility="v", rettype="void")
    entry = _both(fn, "System", _ok("", oid=2278), _ok("", oid=2278))
    assert not entry["type_and_form_checked"]
    assert entry["outcome"] == "by_design" and entry["group"] == support.NOT_TESTABLE


def test_null_where_postgresql_answers_a_value_is_a_wrong_answer():
    entry = _both(UUID, "System", _ok(_V4, oid=2950), _ok(None, oid=2950))
    assert entry["outcome"] == "wrong_answer"
    assert entry["detail"]["difference"] == f"None, PostgreSQL {_V4!r}"


def test_the_rows_of_a_set_are_the_servers_own():
    want = {"ok": True, "fields": [2249], "rows": [("(a,b)",), ("(c,d)",)]}
    none = {"ok": True, "fields": [2249], "rows": []}
    entry = _both(SETTINGS_ROWS, "System", want, none)
    assert entry["outcome"] == "not_checked" and not entry["type_and_form_checked"]
    other = {"ok": True, "fields": [2249], "rows": [("(x,y)",), ("(z,w)",), ("(p,q)",)]}
    assert _both(SETTINGS_ROWS, "System", want, other)["type_and_form_checked"]
    short = {"ok": True, "fields": [2249], "rows": [("(x,y)",), ("(z)",)]}
    assert _both(SETTINGS_ROWS, "System", want, short)["outcome"] == "wrong_answer"


def test_a_different_type_is_a_wrong_answer_before_any_form():
    entry = _both(NOW, "Date", _ok(_THEN, oid=1184), _ok(_THEN, oid=1114))
    assert entry["outcome"] == "wrong_answer"
    assert entry["detail"]["difference"] == "result type oids [1114], PostgreSQL [1184]"


class Zoned:
    """A server writing timeofday under the session's TimeZone: ``writes`` names the zone."""

    def __init__(self, writes: Any) -> None:
        self.zone = "UTC"
        self.writes = writes

    def answer(self, sql: str) -> dict[str, Any]:
        if sql.startswith("SET TimeZone"):
            self.zone = sql.split("'")[1]
            return {"ok": True, "fields": [], "rows": []}
        return _ok(f"Sat Oct 31 17:20:18.213779 2026 {self.writes(self.zone)}", oid=25)


_ABBREVIATION = {"UTC": "UTC", support.ZONE: "JST"}


def test_timeofday_is_exact_on_a_session_off_utc():
    pg = Zoned(_ABBREVIATION.__getitem__)
    entry = support.measure_overload(
        TIMEOFDAY, "Date", pg, Zoned(_ABBREVIATION.__getitem__), _route, engine="pg"
    )
    assert entry["outcome"] == "pass" and entry["type_and_form_checked"]
    assert pg.zone == "UTC"


def test_timeofday_writing_the_zone_name_is_a_wrong_answer():
    entry = support.measure_overload(
        TIMEOFDAY,
        "Date",
        Zoned(_ABBREVIATION.__getitem__),
        Zoned(lambda zone: zone),
        _route,
        engine="pg",
    )
    assert entry["outcome"] == "wrong_answer"
    assert support.ZONE in entry["detail"]["difference"]


def test_timeofday_ignoring_the_sessions_zone_is_a_wrong_answer():
    entry = support.measure_overload(
        TIMEOFDAY,
        "Date",
        Zoned(_ABBREVIATION.__getitem__),
        Zoned(lambda _zone: "UTC"),
        _route,
        engine="pg",
    )
    assert entry["outcome"] == "wrong_answer"


SHUFFLE = Function("array_shuffle", "anyarray", argtypes=("anyarray",), volatility="v")


def test_a_volatile_function_that_acts_on_nothing_has_its_type_and_form_compared():
    planned = support.calls(SHUFFLE, "Array")
    assert [c.sql for c in planned[1:]] == [
        f"""SELECT pg_catalog."array_shuffle"({a}::integer[]) AS v"""
        for a in ("'{1,2,3}'", "'{}'", "'{4,5,6}'")
    ]
    assert all(c.form and not c.answers for c in planned[1:])
    pg = {c.sql: _ok("{3,1,2}", oid=1007) for c in planned}
    pg[planned[0].sql] = _ok(None, oid=1007)
    got = {**pg, planned[1].sql: _ok("{2,3,1}", oid=1007)}
    entry = support.measure_overload(SHUFFLE, "Array", Fake(pg), Fake(got), _route, engine="pg")
    assert entry["outcome"] == "pass" and entry["type_and_form_checked"]
    got[planned[1].sql] = _ok("{2,3}", oid=1007)
    entry = support.measure_overload(SHUFFLE, "Array", Fake(pg), Fake(got), _route, engine="pg")
    assert entry["outcome"] == "wrong_answer"


def test_a_clock_function_has_its_type_and_form_compared_never_its_value():
    entry = _both(NOW, "Date", _ok(_THEN, oid=1184), _ok("2024-01-02 03:04:05+00", oid=1184))
    assert entry["outcome"] == "pass" and entry["type_and_form_checked"]
    assert entry["unchecked"] == "answers from the clock"
    entry = _both(NOW, "Date", _ok(_THEN, oid=1184), _ok("2026-10-31T17:20:18Z", oid=1184))
    assert entry["outcome"] == "wrong_answer"


# --- side effects -------------------------------------------------------------------------------


class Effects:
    """A server answering by statement, ``answers``, recording what it ran in ``ran``."""

    def __init__(self, answers: dict[str, dict[str, Any]]) -> None:
        self.answers = answers
        self.ran: list[str] = []

    def answer(self, sql: str) -> dict[str, Any]:
        self.ran.append(sql)
        return self.answers.get(sql, _ok("", oid=2278))


SET_CONFIG = Function(
    "set_config",
    "text, text, boolean",
    argtypes=("text", "text", "boolean"),
    volatility="v",
    rettype="text",
)
LOCK = Function("pg_advisory_lock", "bigint", argtypes=("bigint",), volatility="v", rettype="void")
SLEEP = Function(
    "pg_sleep", "double precision", argtypes=("double precision",), volatility="v", rettype="void"
)
STAT_RESET = Function("pg_stat_reset", "", volatility="v", rettype="void")


@pytest.mark.parametrize(
    ("fn", "acts"),
    [
        (SET_CONFIG, True),
        (LOCK, True),
        (STAT_RESET, True),
        (Function("pg_try_advisory_lock", "bigint", argtypes=("bigint",), rettype="boolean"), True),
        (Function("lo_unlink", "oid", argtypes=("oid",), rettype="integer"), True),
        (
            Function(
                "fmgr_sql_validator", "oid", argtypes=("oid",), volatility="s", rettype="void"
            ),
            True,
        ),
        (RANDOM, False),
        (SHUFFLE, False),
        (ABS, False),
    ],
)
def test_a_function_returning_void_or_acting_is_a_side_effect_function(fn, acts):
    assert support.side_effect(fn) is acts


def test_a_side_effect_is_read_back_through_the_same_client_and_compared():
    effect = support.EFFECTS["set_config(text, text, boolean)"]
    seen = {effect.observe: _ok("on", oid=25)}
    pg, provisa = Effects(dict(seen)), Effects(dict(seen))
    entry = support.measure_overload(SET_CONFIG, "System", pg, provisa, _route, engine="pg")
    assert entry["outcome"] == "pass" and entry["side_effect_checked"]
    assert not entry["answers_checked"] and not entry["type_and_form_checked"]
    assert provisa.ran[-3:] == [effect.call, effect.observe, *effect.teardown]
    wrong = Effects({effect.observe: _ok("", oid=25)})
    entry = support.measure_overload(
        SET_CONFIG, "System", Effects(dict(seen)), wrong, _route, engine="pg"
    )
    assert entry["outcome"] == "wrong_answer"
    assert entry["detail"]["difference"].startswith(f"after it, {effect.observe}:")


def test_a_lock_is_seen_in_pg_locks_and_released_whatever_the_check_found():
    effect = support.EFFECTS["pg_advisory_lock(bigint)"]
    held = _ok("ExclusiveLock", oid=25)
    provisa = Effects({effect.observe: {"ok": False, "error": "ERROR:  boom"}})
    entry = support.measure_overload(
        LOCK, "System", Effects({effect.observe: held}), provisa, _route, engine="pg"
    )
    assert entry["outcome"] == "error"
    assert entry["detail"] == {"statement": effect.observe, "error": "ERROR:  boom"}
    assert provisa.ran[-1] == "SELECT pg_catalog.pg_advisory_unlock_all()"


class Sleeper(Effects):
    def __init__(self, seconds: float) -> None:
        super().__init__({})
        self.seconds = seconds

    def answer(self, sql: str) -> dict[str, Any]:
        if "pg_sleep(" in sql and "NULL" not in sql:
            support.time.sleep(self.seconds)
        return super().answer(sql)


def test_a_sleep_takes_at_least_what_it_was_asked_for():
    entry = support.measure_overload(
        SLEEP, "System", Sleeper(support.SLEEP_S), Sleeper(support.SLEEP_S), _route, engine="pg"
    )
    assert entry["outcome"] == "pass" and entry["side_effect_checked"]
    entry = support.measure_overload(
        SLEEP, "System", Sleeper(support.SLEEP_S), Sleeper(0), _route, engine="pg"
    )
    assert entry["outcome"] == "wrong_answer"
    assert "asked for 0.2s" in entry["detail"]["difference"]


def test_a_side_effect_no_client_can_see_is_not_testable_by_design():
    entry = support.measure_overload(
        STAT_RESET, "System", Effects({}), Effects({}), _route, engine="pg"
    )
    assert entry["outcome"] == "by_design"
    assert entry["group"] == support.NOT_TESTABLE and entry["requirement"] == "REQ-2167"


def test_a_side_effect_function_off_the_postgresql_engine_is_skipped_by_design_uncalled_for_effect():
    provisa = Effects({})
    entry = support.measure_overload(
        SET_CONFIG, "System", Effects({}), provisa, _route, engine="duckdb"
    )
    assert entry["outcome"] == "by_design" and entry["group"] == support.SIDE_EFFECT_GROUP
    assert support.EFFECTS["set_config(text, text, boolean)"].call not in provisa.ran


def test_a_side_effect_postgresql_does_not_produce_is_not_checked():
    effect = support.EFFECTS["set_config(text, text, boolean)"]
    pg = Effects({effect.call: {"ok": False, "error": "ERROR:  permission denied"}})
    entry = support.measure_overload(SET_CONFIG, "System", pg, Effects({}), _route, engine="pg")
    assert entry["outcome"] == "not_checked"
    assert entry["unchecked"].startswith("PostgreSQL did not produce the side effect")


def test_a_refusal_a_decision_makes_is_skipped_by_design_under_it():
    fn = Function("nextval", "regclass", argtypes=("regclass",), volatility="v", rettype="bigint")
    refusal = {"ok": False, "error": "ERROR:  nextval() has no form on duckdb"}
    sql = support.calls(fn, "System")[0].sql
    entry = support.measure_overload(
        fn, "System", Fake({sql: _ok(None, oid=20)}), Fake({sql: refusal}), _route, engine="duckdb"
    )
    assert entry["outcome"] == "by_design" and entry["group"] == support.SEQUENCE_GROUP


def test_every_effect_is_of_a_side_effect_function_and_every_lock_is_undone():
    for key, effect in support.EFFECTS.items():
        assert "(" in key and effect.call.startswith("SELECT ")
        if "advisory_lock" in effect.call and "xact" not in effect.call:
            assert effect.teardown == ("SELECT pg_catalog.pg_advisory_unlock_all()",)


# --- the merged data file ------------------------------------------------------------------------


def _entry(
    name: str, outcome: str, *, checked: bool = True, prop: bool = False, family: str = "Numeric"
) -> dict:
    """An overload's measurement: a pass is answers checked, or property checked with ``prop``."""
    return {
        "function": name,
        "overload": "integer",
        "family": family,
        "kind": "f",
        "volatility": "i",
        "outcome": outcome,
        "answers_checked": checked and not prop,
        "type_and_form_checked": prop,
        "calls": 4,
        "served_by": "engine",
    }


def _report(engine: str, *entries: dict) -> dict[str, Any]:
    return {
        "engine": engine,
        "postgres_version": "17.11",
        "server_gone": None,
        "counts": support.counts(list(entries)),
        "overloads": list(entries),
    }


def test_the_guaranteed_list_is_what_passes_on_every_engine():
    doc = support.merged(
        {
            "a": _report(
                "a",
                _entry("abs", "pass"),
                _entry("sqrt", "pass"),
                _entry("exp", "not_checked", checked=False),
            ),
            "b": _report(
                "b",
                _entry("abs", "pass"),
                _entry("sqrt", "wrong_answer"),
                _entry("exp", "not_checked", checked=False),
            ),
        }
    )
    assert doc["guaranteed"]["answers_checked"] == ["abs(integer)"]
    assert doc["guaranteed"]["type_and_form_checked"] == []
    assert set(doc["guaranteed"]) == {
        "answers_checked",
        "type_and_form_checked",
        "side_effect_checked",
        "functions",
    }
    assert doc["guaranteed"]["functions"] == ["abs"]
    by = {f["function"]: f for f in doc["functions"]}
    assert by["sqrt"]["outcomes"]["b"]["outcome"] == "wrong_answer"
    assert by["abs"]["family"] == "Numeric"


def test_the_guaranteed_list_is_answers_checked_and_type_and_form_checked_on_every_engine():
    now = _entry("now", "pass", prop=True)
    exp = {**_entry("exp", "not_checked", checked=False), "unchecked": "volatile"}
    doc = support.merged(
        {
            "a": _report("a", _entry("abs", "pass"), now, exp),
            "b": _report("b", _entry("abs", "pass"), now, exp),
        }
    )
    assert doc["guaranteed"]["answers_checked"] == ["abs(integer)"]
    assert doc["guaranteed"]["type_and_form_checked"] == ["now(integer)"]
    assert doc["guaranteed"]["functions"] == ["abs", "now"]
    assert doc["counts"]["a"]["pass_type_and_form_checked"] == 1
    assert doc["counts"]["a"]["not_checked"] == 1
    assert doc["not_checked_reasons"]["a"] == {"volatile": 1}
    text = support.merged_markdown(doc)
    assert "| a | 3 | 2 | 0 | 0 | 1 | 1 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |" in text
    assert (
        "| Numeric | 3 | 1 + 1 + 0, 1 not checked, of 3 | 1 + 1 + 0, 1 not checked, of 3 | 2 |"
        in text
    )
    assert (
        "**Guaranteed list:** 1 overloads with answers compared, 1 with type and form compared"
        in text
    )
    assert "| volatile | 1 |" in text


def test_an_overload_type_and_form_checked_on_one_engine_and_not_checked_on_another_is_off_the_list():
    doc = support.merged(
        {
            "a": _report("a", _entry("now", "pass", prop=True)),
            "b": _report("b", _entry("now", "not_checked", checked=False)),
        }
    )
    assert doc["guaranteed"]["type_and_form_checked"] == []
    assert doc["guaranteed"]["functions"] == []
    assert doc["functions"][0]["outcomes"]["b"]["result"] == "not_checked"


def test_one_not_checked_overload_keeps_its_function_off_the_list():
    other = {**_entry("abs", "not_checked", checked=False), "overload": "bigint"}
    doc = support.merged({"e": _report("e", _entry("abs", "pass"), other)})
    assert doc["guaranteed"]["functions"] == []


def test_an_engine_with_no_result_guarantees_nothing():
    doc = support.merged({"a": _report("a", _entry("abs", "pass")), "b": None})
    assert doc["guaranteed"]["answers_checked"] == []
    assert doc["engines_without_a_result"] == ["b"]
    assert doc["engines_incomplete"] == ["b"]
    assert doc["functions"][0]["outcomes"]["b"]["outcome"] == "not_reached"


def test_a_not_callable_overload_does_not_keep_a_function_off_the_list():
    a = {
        **_entry("abs", "not_callable", checked=False),
        "overload": "internal",
        "detail": support.function_probe.NO_CALL,
    }
    doc = support.merged({"e": _report("e", _entry("abs", "pass"), a)})
    assert doc["guaranteed"]["functions"] == ["abs"]


def test_one_failing_overload_keeps_its_function_off_the_list():
    other = {**_entry("abs", "error"), "overload": "bigint"}
    doc = support.merged({"e": _report("e", _entry("abs", "pass"), other)})
    assert doc["guaranteed"]["answers_checked"] == ["abs(integer)"]
    assert doc["guaranteed"]["functions"] == []


def test_counts_split_a_pass_by_what_was_checked_and_keep_not_checked_apart():
    c = support.counts(
        [
            _entry("a", "pass"),
            _entry("b", "pass", prop=True),
            _entry("c", "not_checked", checked=False),
            _entry("d", "error"),
        ]
    )
    assert (c["pass"], c["pass_answers_checked"], c["pass_type_and_form_checked"]) == (2, 1, 1)
    assert (c["not_checked"], c["error"], c["total"]) == (1, 1, 4)
    assert "pass_only_called" not in c and "pass_form_checked" not in c


def test_the_summary_gives_every_engine_and_every_family():
    doc = support.merged(
        {
            "a": _report("a", _entry("abs", "pass"), _entry("x", "error", family="String")),
            "b": _report("b", _entry("abs", "pass"), _entry("x", "pass", family="String")),
        }
    )
    text = support.merged_markdown(doc)
    assert "| a | 2 | 1 | 0 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 |" in text
    assert "| b | 2 | 2 | 0 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |" in text
    assert (
        "| Numeric | 1 | 1 + 0 + 0, 0 not checked, of 1 | 1 + 0 + 0, 0 not checked, of 1 | 1 |"
        in text
    )
    assert (
        "| String | 1 | 0 + 0 + 0, 0 not checked, of 1 | 1 + 0 + 0, 0 not checked, of 1 | 0 |"
        in text
    )
    assert "1 overloads with answers compared" in text


def test_merge_reads_a_leg_from_wherever_its_artifact_landed_and_fails_on_a_missing_one(tmp_path):
    leg = tmp_path / "legs" / "function-support-a"
    leg.mkdir(parents=True)
    (leg / "function-support-a.json").write_text(json.dumps(_report("a", _entry("abs", "pass"))))
    out, md = tmp_path / "out.json", tmp_path / "out.md"
    assert support.merge(["a"], tmp_path / "legs", out, md) == 0
    assert json.loads(out.read_text())["guaranteed"]["functions"] == ["abs"]
    assert support.merge(["a", "b"], tmp_path / "legs", out, md) == 1
    assert json.loads(out.read_text())["guaranteed"]["functions"] == []


def test_a_leg_whose_server_went_away_is_incomplete():
    report = {**_report("a", _entry("abs", "pass")), "server_gone": "Provisa stopped answering"}
    assert support.merged({"a": report})["engines_incomplete"] == ["a"]


# --- the model and the workflow -----------------------------------------------------------------


def test_the_model_runs_on_the_engine_the_job_names(monkeypatch):
    monkeypatch.setattr(catalog, "registrable_columns", lambda _dsn: [])
    monkeypatch.delenv("PROVISA_ENGINE", raising=False)
    plain = catalog.provisa_config(5432, "dsn")
    assert plain["federation_engine"] == "duckdb"
    assert "federation_hints" not in plain["sources"][0]
    monkeypatch.setenv("PROVISA_ENGINE", "clickhouse")
    on_clickhouse = catalog.provisa_config(5432, "dsn")
    assert on_clickhouse["federation_engine"] == "clickhouse"
    assert on_clickhouse["sources"][0]["federation_hints"] == {"schema": "conformance"}


WORKFLOW = REPO / ".github" / "workflows" / "conformance-functions.yml"


def _workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text())


def test_the_workflow_runs_by_dispatch_only_and_is_no_gate_on_main():
    doc = _workflow()
    assert doc["name"] == "Conformance (functions)"
    triggers = doc[True]  # YAML reads the key `on` as true
    assert set(triggers) == {"workflow_dispatch"}
    assert doc["permissions"] == {"contents": "read"}
    for path in (REPO / ".github" / "workflows").glob("*.yml"):
        if path != WORKFLOW:
            assert "conformance-functions" not in path.read_text(), path


def test_the_merge_names_every_engine_of_the_matrix():
    jobs = _workflow()["jobs"]
    engines = [leg["engine"] for leg in jobs["probe"]["strategy"]["matrix"]["include"]]
    assert engines == ["duckdb", "pg", "clickhouse", "trino"]
    assert jobs["probe"]["strategy"]["fail-fast"] is False
    merge = next(s for s in jobs["merge"]["steps"] if s.get("name") == "Merge the legs")
    assert f"--engines {','.join(engines)} " in merge["run"]
    assert jobs["merge"]["if"] == "${{ !cancelled() }}"


def test_the_workflow_carries_no_secret():
    assert "secrets." not in WORKFLOW.read_text()


def test_a_leg_boots_the_shared_instance_on_its_own_engine():
    probe = _workflow()["jobs"]["probe"]
    assert probe["env"]["PROVISA_ENGINE"] == "${{ matrix.engine }}"
    uses = [s.get("uses") for s in probe["steps"]]
    assert "./.github/actions/conformance-instance" in uses
    trino = next(s for s in probe["steps"] if s.get("name") == "Start the Trino stack")
    assert trino["if"] == "matrix.engine == 'trino'"
    assert "scripts/ci/lanes/trino-stack-prepare.sh placeholder-cert" in trino["run"]


def test_the_instance_keeps_its_embedded_control_plane_unless_it_is_given_one():
    action = yaml.safe_load(
        (REPO / ".github" / "actions" / "conformance-instance" / "action.yml").read_text()
    )
    assert action["inputs"]["control-plane-url"]["default"] == ""
    assert action["inputs"]["boot-attempts"]["default"] == "90"


# --- results by engine (REQ-2171, EXACT OR EQUIVALENT and RESULTS ARE BY ENGINE) --------------


def _compare(want: dict[str, Any], got: dict[str, Any], sql: str = "SELECT 1", **given: Any) -> Any:
    return support.compare(want, got, values=True, sql=sql, **given)


def _rules_of(
    want: dict[str, Any], got: dict[str, Any], sql: str = "SELECT 1", **given: Any
) -> Any:
    compared = _compare(want, got, sql, **given)
    assert compared.difference is None
    return set(compared.rules)


def test_the_same_text_is_exact_under_no_rule():
    assert _rules_of(_ok("1", "2"), _ok("1", "2")) == set()


@pytest.mark.parametrize(
    ("want", "got", "oid", "rule"),
    [
        ('{"a": 1}', '{"a":1}', 3802, report.JSON_SPACING),
        ('(b,"[1, 2]")', '(b,"[1,2]")', 2249, report.JSON_SPACING),
        ('{"a": 1.50}', '{"a": 1.5}', 3802, report.NUMBER_VALUE),
        ("2.50", "2.5", 1700, report.NUMBER_VALUE),
        ("{1.50,2}", "{1.5,2.0}", 1231, report.NUMBER_VALUE),
        ("100", "100.0", 701, report.NUMBER_VALUE),
        ("0.1", "0.10000000000000001", 701, report.NUMBER_VALUE),
        ("1.00000000000000012", "1.00000000000000011", 1700, None),
        ("6.9077552789821371", "6.907755278982137", 1700, None),
        ("3.141592653589793", "3.1415926535897936", 701, report.FLOAT_CLOSE),
        ('{"a": 1, "b": 2}', '{"b": 2, "a": 1}', 3802, report.JSON_KEY_ORDER),
    ],
)
def test_a_rule_names_what_made_two_texts_the_same(want, got, oid, rule):
    if rule is None:
        assert _compare(_ok(want, oid=oid), _ok(got, oid=oid)).difference is not None
    else:
        assert rule in _rules_of(_ok(want, oid=oid), _ok(got, oid=oid))


def test_an_aggregate_s_array_with_no_order_by_compares_in_any_order():
    sql = "SELECT json_agg(c1) FROM t"
    rules = _rules_of(_ok("[1, 2]", oid=114), _ok("[2,1]", oid=114), sql)
    assert report.ARRAY_ORDER in rules and report.JSON_SPACING in rules
    ordered = "SELECT json_agg(c1 ORDER BY c1) FROM t"
    assert _compare(_ok("[1,2]", oid=114), _ok("[2,1]", oid=114), ordered).difference is not None


def test_rows_in_another_order_are_equivalent_and_only_then():
    assert _rules_of(_ok("1", "2"), _ok("2", "1")) == {report.ROW_ORDER}
    assert _compare(_ok("1", "2"), _ok("1", "3")).difference == "'3', PostgreSQL '2'"


def test_an_enum_label_sent_as_text_is_equivalent_and_no_other_type_is():
    enums = frozenset({16400, 16399})
    want, got = _ok("red", oid=16400), _ok("red", oid=25)
    assert _rules_of(want, got, enums=enums) == {report.ENUM_LABEL}
    assert _rules_of(_ok("{a,b}", oid=16399), _ok("{a,b}", oid=1009), enums=enums) == {
        report.ENUM_LABEL
    }
    # The label must still match, and the oid must be an enum's.
    assert _compare(want, _ok("blue", oid=25), enums=enums).difference is not None
    assert _compare(_ok("7", oid=23), _ok("7", oid=25), enums=enums).difference is not None
    assert _compare(want, _ok("red", oid=23), enums=enums).difference is not None


def test_an_answer_about_the_database_is_equivalent_for_those_functions_only():
    schema = "<xsd:schema>{}</xsd:schema>"
    want, got = _ok(schema.format("pg"), oid=142), _ok(schema.format("model"), oid=142)
    rules = _rules_of(want, got, function="database_to_xmlschema")
    assert rules == {report.MODEL_ANSWER}
    assert _compare(want, got, function="xmlcomment").difference is not None
    garbled = _ok("not xml", oid=142)
    assert _compare(want, garbled, function="database_to_xmlschema").difference is not None


def test_a_record_compares_json_only_in_its_json_fields():
    assert _compare(_ok('(a,"[1]")', oid=2249), _ok('(b,"[1]")', oid=2249)).difference
    assert _compare(_ok('(a,"[1,2]")', oid=2249), _ok('(a,"[1,3]")', oid=2249)).difference
    # A text field holding digits is text.
    assert _compare(_ok("(a,1.50)", oid=2249), _ok("(a,1.5)", oid=2249)).difference
    quoted = _ok('(a,"{""k"": ""x,y""}")', oid=2249), _ok('(a,"{""k"":""x,y""}")', oid=2249)
    assert _rules_of(*quoted) == {report.JSON_SPACING}


def test_a_number_that_differs_in_value_stays_a_wrong_answer():
    assert _compare(_ok("2.5", oid=1700), _ok("2", oid=1700)).difference == "'2', PostgreSQL '2.5'"


@pytest.mark.parametrize(
    "refusal",
    [
        lambda: __import__("provisa.transpiler.function_macros", fromlist=["x"]).FunctionRefused(
            "int4", "trino"
        ),
        lambda: __import__(
            "provisa.transpiler.metadata_functions", fromlist=["x"]
        ).MetadataFunctionRefused("nextval"),
        lambda: __import__("provisa.transpiler.pg_xml", fromlist=["x"]).XmlFunctionRefused(
            "xpath", "clickhouse"
        ),
        lambda: __import__(
            "provisa.transpiler.native_subtype", fromlist=["x"]
        ).SubtypeOperationRefused("money", "cast to text", "clickhouse"),
        lambda: __import__(
            "provisa.transpiler.metadata_settings", fromlist=["x"]
        ).CatalogViewRefused("pg_settings"),
    ],
)
def test_each_refusal_the_product_raises_is_refused_by_name(refusal):
    assert refused_by_name(f"ERROR:  {refusal()}")


@pytest.mark.parametrize(
    "error",
    [
        "ERROR:  to_char: a format that is not a literal is not supported; each of its elements",
        "ERROR:  to_char: the format element 'Q' is not supported; it is not answered by a guess",
        "ERROR:  encode: a format that is not a literal is not supported; it is chosen where",
        "ERROR:  inetmi does not read this value",
        'ERROR:  [V006] Table function "JSON_TO_RECORDSET" is not a governed relation',
    ],
)
def test_the_refusals_written_as_text_are_refused_by_name(error):
    assert refused_by_name(error)


@pytest.mark.parametrize(
    "error",
    [
        "ERROR:  Code: 50. DB::Exception: Unknown data type family: CSTRING.",
        "ERROR:  Catalog Error: Scalar Function with name ts_headline does not exist!",
        "ERROR:  Expected TYPE after CAST. Line 1, Col: 38.",
        "ERROR:  FederationError(type=USER_ERROR, name=NOT_SUPPORTED, message='x', query_id=q)",
        "ERROR:  'NoneType' object has no attribute 'name'",
        "no answer in 30s",
        "connection: consuming input failed",
    ],
)
def test_an_engine_s_own_error_is_no_refusal(error):
    assert not refused_by_name(error)


def test_measure_keeps_a_named_refusal_apart_from_an_engine_error():
    named = {"ok": False, "error": "ERROR:  abs() has no form on trino"}
    entry = _both(ABS, "Numeric", _ok("3"), named)
    assert entry["outcome"] == "refused"
    assert entry["detail"]["error"].endswith("has no form on trino")
    raw = {"ok": False, "error": "ERROR:  Code: 46. DB::Exception: no function"}
    assert _both(ABS, "Numeric", _ok("3"), raw)["outcome"] == "error"


def test_a_wrong_answer_on_one_call_outranks_a_refusal_on_another():
    calls_ = [c.sql for c in support.calls(ABS, "Numeric")]
    got = {
        calls_[0]: {"ok": False, "error": "ERROR:  abs() has no form on trino"},
        **{sql: _ok("9") for sql in calls_[1:]},
    }
    want = {sql: _ok("3") for sql in calls_}
    entry = support.measure_overload(ABS, "Numeric", Fake(want), Fake(got), _route, engine="pg")
    assert entry["outcome"] == "wrong_answer"


def test_a_pass_keeps_the_rules_it_took():
    entry = _both(ABS, "Numeric", _ok("2.50", oid=1700), _ok("2.5", oid=1700))
    assert (entry["outcome"], entry["rules"]) == ("pass", [report.NUMBER_VALUE])


def _results(*by_engine: tuple[str, str, list[str]]) -> dict[str, Any]:
    """The engines' one overload of abs, each an outcome and its rules."""
    entries = {
        engine: {**_entry("abs", outcome), "rules": rules, "detail": {"difference": "x"}}
        for engine, outcome, rules in by_engine
    }
    return support.merged(
        {engine: _report(engine, entry) for engine, entry in entries.items()},
    )


def test_each_overload_has_a_result_per_engine():
    doc = _results(
        ("pg", "pass", []),
        ("duckdb", "pass", [report.ROW_ORDER]),
        ("trino", "refused", []),
        ("clickhouse", "wrong_answer", []),
    )
    outcomes = doc["functions"][0]["outcomes"]
    assert {e: o["result"] for e, o in outcomes.items()} == {
        "pg": "exact",
        "duckdb": "equivalent",
        "trino": "refused",
        "clickhouse": "differs",
    }
    assert outcomes["duckdb"]["rules"] == [report.ROW_ORDER]
    assert outcomes["pg"]["check"] == "answers"


def test_the_counts_are_per_engine_in_stable_keys_and_nothing_adds_the_engines():
    doc = _results(("pg", "pass", []), ("duckdb", "error", []))
    assert set(doc["by_engine"]) == {"pg", "duckdb"}
    assert tuple(doc["by_engine"]["pg"]) == report.RESULT_KEYS + report.COLUMN_KEYS
    assert doc["by_engine"]["pg"]["exact"] == 1
    assert doc["by_engine"]["duckdb"]["differs"] == 1
    assert doc["by_engine"]["duckdb"]["differs_engine_error"] == 1
    assert doc["by_engine"]["duckdb"]["differs_wrong_answer"] == 0
    assert "failed" not in doc and "total" not in doc


def test_an_engine_that_left_no_result_has_none_for_each_overload():
    doc = support.merged({"pg": _report("pg", _entry("abs", "pass")), "trino": None})
    assert doc["functions"][0]["outcomes"]["trino"]["result"] == "not_reached"
    assert doc["by_engine"]["trino"]["not_reached"] == 1
    assert "**Incomplete engines:** Trino." in report.pgwire_block(doc)


def test_what_no_engine_serves_is_counted_by_family():
    both = {"a": ("sqrt", "refused"), "b": ("sqrt", "wrong_answer")}
    doc = support.merged(
        {
            engine: _report(
                engine,
                _entry("abs", "pass" if engine == "a" else "refused"),
                _entry(*both[engine]),
                {
                    **_entry("lo_in", "not_callable"),
                    "family": "Other",
                    "detail": support.function_probe.NO_CALL,
                },
            )
            for engine in ("a", "b")
        }
    )
    assert doc["no_engine"] == {
        "overloads": 1,
        "functions": 1,
        "by_family": {"Numeric": {"overloads": 1, "functions": 1}},
    }


def test_a_rule_is_a_footnote_linked_from_every_test_it_covers():
    doc = _results(("pg", "pass", []), ("duckdb", "pass", [report.ROW_ORDER, report.NUMBER_VALUE]))
    page = report.pgwire_block(doc)
    number = next(r["footnote"] for r in doc["rules"] if r["key"] == report.ROW_ORDER)
    assert f"[{number}](#rule-row-order)" in page
    assert '<a id="rule-row-order"></a>' in page
    assert "| `abs(integer)` | exact | equivalent " in page
    covered = next(r for r in doc["rules"] if r["key"] == report.ROW_ORDER)["overloads"]
    assert covered == {"duckdb": 1, "pg": 0}
    assert "Overloads covered: Postgres 0, DuckDB 1." in page


def test_the_rules_are_the_ones_the_requirement_and_the_brief_name():
    assert [rule.key for rule in report.RULES] == [
        report.JSON_SPACING,
        report.ENUM_LABEL,
        report.ROW_ORDER,
        report.NUMBER_VALUE,
        report.MODEL_ANSWER,
        report.FLOAT_CLOSE,
        report.JSON_KEY_ORDER,
        report.ARRAY_ORDER,
        report.CLOCK_FIELD,
        report.ZONE_PACKAGING,
    ]


def test_the_report_fails_per_engine_never_overall_and_keeps_a_wrong_answer_in_view():
    doc = _results(("pg", "wrong_answer", []), ("duckdb", "refused", []))
    for text in (report.pgwire_block(doc), report.overview_block(doc)):
        assert "| Engine | Passed | Skipped by design | Failed | Not tested |" in text
        assert "| Postgres | 0 | 0 | 1 | 0 |" in text and "| DuckDB | 0 | 0 | 1 | 0 |" in text
        assert "| Total" not in text and "| All" not in text
    assert "| `abs(integer)` | x |" in report.pgwire_block(doc)


@pytest.mark.parametrize(
    ("entry", "verdict"),
    [
        ({"outcome": "pass"}, "passed"),
        ({"outcome": "by_design"}, "skipped_by_design"),
        (
            {"outcome": "not_callable", "detail": support.function_probe.NO_CALL},
            "skipped_by_design",
        ),
        (
            {
                "outcome": "not_callable",
                "detail": "PostgreSQL refuses the call: ERROR:  must be called as trigger",
                "refused_for": "the call",
            },
            "skipped_by_design",
        ),
        (
            {
                "outcome": "not_callable",
                "detail": "PostgreSQL refuses the call: ERROR:  must be called as trigger",
            },
            "failed",
        ),
        (
            {
                "outcome": "not_callable",
                "detail": "PostgreSQL refuses the call: ERROR:  recovery is not in progress",
            },
            "failed",
        ),
        ({"outcome": "not_checked"}, "not_tested"),
        ({"outcome": "refused"}, "failed"),
        ({"outcome": "wrong_answer"}, "failed"),
        ({"outcome": "error"}, "failed"),
        ({"outcome": "not_reached"}, "not_tested"),
    ],
)
def test_every_overload_is_passed_skipped_by_design_failed_or_not_tested(entry, verdict):
    assert report.verdict(entry) == verdict


def test_the_four_counts_add_up_to_every_overload():
    outcomes = [
        {"outcome": o}
        for o in ("pass", "by_design", "not_checked", "not_reached", "refused", "wrong_answer")
    ]
    c = report.result_counts(outcomes)
    assert (c["passed"], c["skipped_by_design"], c["failed"], c["not_tested"]) == (1, 1, 2, 2)
    assert c["passed"] + c["skipped_by_design"] + c["failed"] + c["not_tested"] == c["total"]


def test_a_cell_says_what_was_checked_of_a_pass_and_a_call_alone_is_none():
    assert report.cell({"outcome": "pass", "check": "answers"}) == "exact"
    assert (
        report.cell({"outcome": "pass", "check": "type_and_form"}) == "exact (type and form only)"
    )
    assert report.cell({"outcome": "not_checked"}) == "not checked"
    assert report.result_of("not_checked") == report.NOT_CHECKED
    assert report.cell({"outcome": "error"}) == "differs (error)"
    assert report.cell({"outcome": "not_callable"}) == "n/a"


def test_both_pages_are_filled_from_the_results_between_their_markers(tmp_path):
    doc = _results(("pg", "pass", []), ("duckdb", "refused", []))
    begin, end = fill_docs.FUNCTIONS_BEGIN, fill_docs.FUNCTIONS_END
    for name in ("pgwire.md", "overview.md"):
        (tmp_path / name).write_text(f"# Page\n\n{begin}\nstale text\n{end}\n\nAfter.\n")
    fill_docs.fill_function_pages(doc, tmp_path)
    for name in ("pgwire.md", "overview.md"):
        text = (tmp_path / name).read_text()
        assert "stale text" not in text and "After." in text
        assert "| Postgres | 1 | 0 | 0 |" in text and "| DuckDB | 0 | 0 | 1 |" in text
        assert "| Postgres | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |" in text
        assert "| DuckDB | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 0 | 0 |" in text


def test_a_page_without_markers_is_refused():
    with pytest.raises(ValueError, match="no function-results markers"):
        fill_docs.fill_functions("# Page\n", "x")


def test_the_published_pages_have_the_markers():
    for name in ("pgwire.md", "overview.md"):
        text = (fill_docs.DOCS / name).read_text()
        assert fill_docs.FUNCTIONS_BEGIN in text and fill_docs.FUNCTIONS_END in text


def test_the_merge_job_regenerates_the_pages_and_uploads_them():
    merge = _workflow()["jobs"]["merge"]
    merged_step = next(s for s in merge["steps"] if s.get("name") == "Merge the legs")
    assert "fill_docs.py scripts/conformance/function_support.json" in merged_step["run"]
    upload = next(s for s in merge["steps"] if "pages" in s.get("name", ""))
    assert "docs/conformance/pgwire.md" in upload["with"]["path"]
    assert "docs/conformance/overview.md" in upload["with"]["path"]


# --- literals and columns -------------------------------------------------------------------------

TO_TSVECTOR = Function(
    "to_tsvector", "text", argtypes=("text",), volatility="s", rettype="tsvector"
)
REFUSED_ON_COLUMN = "ERROR:  to_tsvector() has no form on duckdb"


class Served(Fake):
    """A server answering every value call ``answer``, and a column call as ``column`` says."""

    def __init__(self, answer: dict[str, Any], column: dict[str, Any]) -> None:
        super().__init__({})
        self.literal, self.column = answer, column

    def answer(self, sql: str) -> dict[str, Any]:
        reads_a_column = " FROM " in sql
        return self.column if reads_a_column else self.literal


def _measured(column: dict[str, Any]) -> dict[str, Any]:
    pg = Served(_ok("cat", oid=3614), _ok("cat", oid=3614))
    provisa = Served(_ok("cat", oid=3614), column)
    return support.measure_overload(TO_TSVECTOR, "Text search", pg, provisa, _route, engine="pg")


def _doc(entry: dict[str, Any]) -> dict[str, Any]:
    return support.merged({"duckdb": _report("duckdb", entry)})


def test_a_sampled_argument_reads_its_column_row_by_row():
    assert [c.sql for c in support.column_calls(ABS, "Numeric")] == [
        'SELECT pg_catalog."abs"(c_integer) AS v FROM conformance.function_columns WHERE id = 1',
        'SELECT pg_catalog."abs"(c_integer) AS v FROM conformance.function_columns WHERE id = 2',
        'SELECT pg_catalog."abs"(c_integer) AS v FROM conformance.function_columns WHERE id = 3',
    ]


def test_every_sample_has_a_column_of_its_own():
    columns = [support.sample_column(t) for t in support.SAMPLES]
    assert len(set(columns)) == len(columns)
    assert all(c.replace("_", "").isalnum() for c in columns)
    assert support.sample_column("integer[]") == "c_integer_array"
    assert support.sample_column('"char"') == "c_char"
    assert support.sample_column("character") == "c_character"


def test_a_literal_a_function_needs_stays_a_literal():
    fn = Function(
        "date_trunc",
        "text, date",
        argtypes=("text", "date"),
        volatility="i",
    )
    assert support.column_calls(fn, "Date")[0].sql == (
        "SELECT pg_catalog.\"date_trunc\"('day'::text, c_date) AS v "
        "FROM conformance.function_columns WHERE id = 1"
    )


def test_an_overload_of_given_literals_alone_reads_its_first_from_values_of_that_row():
    fn = Function(
        "to_timestamp",
        "double precision",
        argtypes=("double precision",),
        volatility="i",
    )
    assert [c.sql for c in support.column_calls(fn, "Date")][2] == (
        'SELECT pg_catalog."to_timestamp"(c1) AS v '
        "FROM (VALUES (3, '1999'::double precision)) AS t(id, c1) WHERE id = 3"
    )


def test_a_variadic_array_column_is_passed_as_variadic():
    fn = Function(
        "concat_ws",
        "text, VARIADIC text[]",
        argtypes=("text", "text[]"),
        variadic=True,
        volatility="s",
    )
    assert "(c_text, VARIADIC c_text_array)" in support.column_calls(fn, "String")[0].sql


def test_an_aggregate_reads_every_row_of_its_column_in_a_stated_order():
    fn = Function("string_agg", "text, text", kind="a", argtypes=("text", "text"), volatility="i")
    assert [c.sql for c in support.column_calls(fn, "String")] == [
        'SELECT pg_catalog."string_agg"(c_text, c_text ORDER BY id) AS v '
        "FROM conformance.function_columns"
    ]


def test_an_ordered_set_aggregate_keeps_its_direct_argument_a_literal():
    fn = Function(
        "percentile_disc",
        "double precision ORDER BY anyelement",
        kind="a",
        aggkind="o",
        direct=1,
        argtypes=("double precision", "anyelement"),
        volatility="i",
    )
    assert support.column_calls(fn, "Aggregate and window")[0].sql == (
        'SELECT pg_catalog."percentile_disc"(0.25::double precision) '
        "WITHIN GROUP (ORDER BY c_integer) AS v FROM conformance.function_columns"
    )


def test_a_hypothetical_set_aggregate_orders_by_the_column_it_ranks_a_literal_in():
    fn = Function(
        "rank",
        'VARIADIC "any" ORDER BY VARIADIC "any"',
        kind="a",
        aggkind="h",
        direct=1,
        argtypes=('"any"',),
        volatility="i",
    )
    assert support.column_calls(fn, "Aggregate and window")[0].sql == (
        'SELECT pg_catalog."rank"(3::integer) WITHIN GROUP (ORDER BY c_integer) AS v '
        "FROM conformance.function_columns"
    )


def test_a_window_function_reads_its_column_over_a_stated_order():
    fn = Function("lag", "anyelement", kind="w", argtypes=("anyelement",), volatility="i")
    assert support.column_calls(fn, "Aggregate and window")[0].sql == (
        'SELECT pg_catalog."lag"(c_integer) OVER (ORDER BY id) AS v '
        "FROM conformance.function_columns"
    )


@pytest.mark.parametrize(
    "fn",
    [
        Function("pi", "", volatility="i"),
        Function("setval", "text", argtypes=("text",), volatility="v"),
        Function("gtsvectorout", "gtsvector", argtypes=("gtsvector",), volatility="i"),
    ],
)
def test_an_overload_with_no_sample_value_has_no_column_call(fn):
    assert support.column_calls(fn, "Numeric") == []


def test_a_form_that_serves_literals_and_refuses_columns_is_reported_literal_only():
    entry = _measured({"ok": False, "error": REFUSED_ON_COLUMN})
    assert entry["outcome"] == "pass"
    assert entry["column"]["outcome"] == "refused"
    doc = _doc(entry)
    engine = doc["by_engine"]["duckdb"]
    assert engine["pass_literal_only"] == 1
    assert engine["pass_literal_only_overloads"] == ["to_tsvector(text)"]
    assert engine["pass_literal_and_column"] == 0
    assert engine["pass_literal_and_column_overloads"] == []
    assert engine["exact"] == 1 and engine["refused_by_name"] == 0
    assert doc["functions"][0]["outcomes"]["duckdb"]["result"] == report.EXACT
    assert doc["guaranteed"]["answers_checked"] == ["to_tsvector(text)"]


def test_a_form_that_serves_both_passes_on_literals_and_columns():
    doc = _doc(_measured(_ok("cat", oid=3614)))
    engine = doc["by_engine"]["duckdb"]
    assert engine["pass_literal_and_column"] == 1
    assert engine["pass_literal_and_column_overloads"] == ["to_tsvector(text)"]
    assert engine["pass_literal_only"] == 0


@pytest.mark.parametrize(
    ("column", "outcome"),
    [
        ({"ok": False, "error": "ERROR:  Binder Error: no such thing"}, "error"),
        (_ok("dog", oid=3614), "wrong_answer"),
        (_ok("cat", oid=25), "wrong_answer"),
    ],
)
def test_any_column_call_that_fails_is_literal_only(column, outcome):
    entry = _measured(column)
    assert (entry["outcome"], entry["column"]["outcome"]) == ("pass", outcome)
    assert _doc(entry)["by_engine"]["duckdb"]["pass_literal_only"] == 1


def test_a_column_rule_is_the_columns_own_and_leaves_the_result_exact():
    pg = Served(_ok("cat", oid=1700), _ok("2.50", oid=1700))
    provisa = Served(_ok("cat", oid=1700), _ok("2.5", oid=1700))
    entry = support.measure_overload(TO_TSVECTOR, "Text search", pg, provisa, _route, engine="pg")
    assert entry["rules"] == []
    assert entry["column"] == {"outcome": "pass", "rules": [report.NUMBER_VALUE]}
    assert _doc(entry)["by_engine"]["duckdb"]["exact"] == 1


def test_a_failing_overload_is_on_neither_list_whatever_its_column_does():
    pg = Served(_ok("cat", oid=3614), _ok("cat", oid=3614))
    provisa = Served({"ok": False, "error": REFUSED_ON_COLUMN}, _ok("cat", oid=3614))
    entry = support.measure_overload(TO_TSVECTOR, "Text search", pg, provisa, _route, engine="pg")
    assert entry["outcome"] == "refused"
    engine = _doc(entry)["by_engine"]["duckdb"]
    assert engine["refused_by_name"] == 1
    assert engine["pass_literal_and_column"] == engine["pass_literal_only"] == 0


def test_the_column_call_compared_is_the_first_postgresql_answers():
    planned = [c.sql for c in support.column_calls(ABS, "Numeric")]
    pg = Fake({planned[0]: {"ok": False, "error": "ERROR:  out of range"}, planned[1]: _ok("7")})
    provisa = Fake({planned[1]: _ok("7")})
    assert support.measure_column(ABS, "Numeric", pg, provisa) == {"outcome": "pass", "rules": []}


def test_a_column_call_postgresql_refuses_leaves_no_column_result():
    assert support.measure_column(ABS, "Numeric", Fake({}), Fake({})) is None
    doc = _doc({**_entry("abs", "pass"), "rules": []})
    assert doc["by_engine"]["duckdb"]["pass_no_column_call"] == 1
    assert doc["by_engine"]["duckdb"]["pass_literal_only"] == 0


def test_a_leg_whose_model_lacks_the_table_stops_rather_than_lists_every_overload():
    with pytest.raises(SystemExit, match="function_columns is not served"):
        support.require_column_table(Fake({}))
    rows = _ok(1, 2, 3)
    support.require_column_table(Fake({f"SELECT id FROM {support.COLUMN_RELATION}": rows}))


def test_the_results_a_floor_reads_keep_their_keys():
    assert report.RESULT_KEYS == (
        "passed",
        "skipped_by_design",
        "failed",
        "not_tested",
        "exact",
        "equivalent",
        "not_checked",
        "by_design",
        "refused_by_name",
        "differs",
        "differs_wrong_answer",
        "differs_engine_error",
        "not_callable",
        "not_reached",
        "total",
    )
    assert not set(report.RESULT_KEYS) & set(report.COLUMN_KEYS)


def test_the_overview_and_the_summary_count_and_list_literal_only_by_engine():
    doc = support.merged(
        {
            "duckdb": _report("duckdb", _measured({"ok": False, "error": REFUSED_ON_COLUMN})),
            "pg": _report("pg", _measured(_ok("cat", oid=3614))),
        }
    )
    assert "| Engine | Literals and columns | Literals only | No column call |" in (
        report.overview_block(doc)
    )
    for block in (report.overview_block(doc), report.summary_block(doc), report.pgwire_block(doc)):
        assert "| DuckDB | 0 | 1 | 0 |" in block
        assert "| Postgres | 1 | 0 | 0 |" in block
        assert "<details><summary>DuckDB (1)</summary>" in block
        assert "| `to_tsvector(text)` | ERROR: to_tsvector() has no form on duckdb |" in block
        assert "<summary>Postgres" not in block.split("Every overload")[0]


def test_a_leg_s_summary_says_how_many_pass_on_literals_only():
    leg = _report("duckdb", _measured({"ok": False, "error": REFUSED_ON_COLUMN}))
    leg["postgres_version"] = "17.11"
    assert "On literals only: 1." in support.engine_markdown(leg)


def test_the_column_table_seeds_each_sample_and_registers_as_registration_types(
    tmp_path, monkeypatch
):
    statements: list[str] = []

    class Conn:
        def __enter__(self) -> Any:
            return self

        def __exit__(self, *_: Any) -> None:
            return None

        def execute(self, query: Composable) -> None:
            statements.append(query.as_string())

    monkeypatch.setattr(support.psycopg, "connect", lambda *_a, **_k: Conn())
    monkeypatch.setattr(catalog, "_sized_numeric", lambda *_a: {"data_type": "numeric(38,3)"})
    out = tmp_path / "tables.yaml"
    support.seed_columns("dsn", out)
    inserts = [s for s in statements if s.startswith("INSERT")]
    assert len(inserts) == support.VALUE_ROWS
    assert inserts[0].startswith(
        'INSERT INTO "conformance"."function_columns" VALUES (1, 3::smallint'
    )
    assert "'cat:1 fat:2'::tsvector" in inserts[0] and "'rat:3A'::tsvector" in inserts[2]
    [table] = yaml.safe_load(out.read_text())
    assert table["table"] == "function_columns" and table["source_id"] == "conformance-pg"
    columns = {c["name"]: c for c in table["columns"]}
    assert set(columns) == {"id", *map(support.sample_column, support.SAMPLES)}
    assert columns["c_tsvector"]["data_type"] == "tsvector"
    assert columns["c_numeric"]["data_type"] == "numeric(38,3)"
    assert all(c["visible_to"] == ["org_admin"] for c in table["columns"])


def test_a_leg_seeds_the_column_table_and_gives_it_to_the_instance_after_the_engine_is_up():
    steps = _workflow()["jobs"]["probe"]["steps"]
    names = [s.get("name") for s in steps]
    assert names.index("Start the Trino stack") < names.index("Seed the table column calls read")
    assert names.index("Seed the table column calls read") < names.index(
        "Start the Provisa instance"
    )
    seed = steps[names.index("Seed the table column calls read")]["run"]
    assert "seed-columns" in seed and '"$RUNNER_TEMP/function-columns.yaml"' in seed
    instance = steps[names.index("Start the Provisa instance")]["with"]
    assert instance["extra-tables"] == "${{ runner.temp }}/function-columns.yaml"


# --- real values for what cannot be compared by value (REQ-2167, only compared answers pass) ---


LO_GET = Function("lo_get", "oid", argtypes=("oid",), volatility="v", rettype="bytea")
READ_FILE = Function("pg_read_file", "text", argtypes=("text",), volatility="v", rettype="text")
AGE_XID = Function("age", "xid", argtypes=("xid",), volatility="s", rettype="integer")
DB_COMMITS = Function(
    "pg_stat_get_db_xact_commit", "oid", argtypes=("oid",), volatility="s", rettype="bigint"
)
BACKEND_START = Function(
    "pg_stat_get_backend_start",
    "integer",
    argtypes=("integer",),
    volatility="s",
    rettype="timestamp with time zone",
)


def _per_call(fn: Function, answers: dict[int, dict[str, Any]], other: dict[str, Any]) -> Fake:
    """A server answering ``fn``'s call ``i`` with ``answers[i]``, and every other with ``other``."""
    planned = [c.sql for c in support.calls(fn, "System")]
    return Fake({sql: answers.get(i, other) for i, sql in enumerate(planned)})


def test_a_volatile_function_that_reads_an_object_is_given_one_and_held_to_its_form():
    planned = support.calls(LO_GET, "Large object")
    assert [c.sql for c in planned[1:]] == [
        f'SELECT pg_catalog."lo_get"({support.FIXTURE_LARGE_OBJECT}::oid) AS v'
    ] * support.VALUE_ROWS
    assert all(c.form and not c.answers for c in planned[1:])
    files = [c.sql for c in support.calls(READ_FILE, "System")[1:]]
    assert files[0] == """SELECT pg_catalog."pg_read_file"('PG_VERSION'::text) AS v"""


def test_a_read_answering_the_form_postgresql_answers_passes_type_and_form_checked():
    pg = _per_call(LO_GET, {0: _ok(None, oid=17)}, _ok("\\x616263", oid=17))
    entry = support.measure_overload(
        LO_GET,
        "System",
        pg,
        _per_call(LO_GET, {0: _ok(None, oid=17)}, _ok("\\x616264", oid=17)),
        _route,
        engine="pg",
    )
    assert entry["outcome"] == "pass" and entry["type_and_form_checked"]
    assert entry["unchecked"] == "volatile"
    other = _per_call(LO_GET, {0: _ok(None, oid=17)}, _ok("no such thing", oid=17))
    entry = support.measure_overload(LO_GET, "System", pg, other, _route, engine="pg")
    assert entry["outcome"] == "wrong_answer"


def test_a_server_file_read_is_skipped_by_design_whatever_it_answers():
    """REQ-2167, amended 2026-11-03 (3): no function reads the server's files, so pg_read_file is
    no longer held to the form of PostgreSQL's file."""
    pg = _per_call(READ_FILE, {0: _ok(None, oid=25)}, _ok("17\n", oid=25))
    for answer in ("18\n", "no such thing"):
        got = _per_call(READ_FILE, {0: _ok(None, oid=25)}, _ok(answer, oid=25))
        entry = support.measure_overload(READ_FILE, "System", pg, got, _route, engine="pg")
        assert entry["outcome"] == "by_design" and entry["group"] == support.SERVER_FILES_GROUP


def test_a_statistic_of_a_database_or_a_backend_is_asked_of_the_sessions_own():
    commits = support.calls(DB_COMMITS, "System")
    assert commits[0].sql == call(DB_COMMITS) and not commits[0].form
    assert [c.sql for c in commits[1:]] == [
        f'SELECT pg_catalog."pg_stat_get_db_xact_commit"({oid}) AS v'
        for oid in support._OBJECTS["database"][1]
    ]
    assert all(c.form and not c.values for c in commits[1:])
    started = support.calls(BACKEND_START, "System")[1]
    assert "pg_catalog.pg_stat_get_backend_idset()" in started.sql
    assert "pg_catalog.pg_backend_pid()" in started.sql


def test_a_volatile_function_off_the_list_still_gets_no_value_whatever_its_prefix():
    reset = Function("pg_stat_get_db_reset", "oid", argtypes=("oid",), volatility="v")
    assert support.form_calls(reset) == []


def test_a_value_of_the_servers_own_is_resolved_from_postgresql_before_it_is_given(monkeypatch):
    monkeypatch.setattr(support, "FIXTURE", {})
    assert support.form_calls(AGE_XID) == []
    pg = Fake(
        {
            support.FIXTURE_QUERIES["xid"]: _ok("779", oid=28),
            support.FIXTURE_QUERIES["multixact"]: _ok(None, oid=28),
        }
    )
    resolved = support.resolve_fixture(pg)
    assert resolved == {"xid": "779"}
    monkeypatch.setattr(support, "FIXTURE", resolved)
    assert [c.sql for c in support.form_calls(AGE_XID)] == [
        """SELECT pg_catalog."age"('779'::xid) AS v"""
    ] * support.VALUE_ROWS
    members = Function("pg_get_multixact_members", "xid", argtypes=("xid",), volatility="v")
    assert support.form_calls(members) == []


@pytest.mark.parametrize(
    ("fn", "reads"),
    [
        (
            Function(
                "pg_node_tree_out", "pg_node_tree", argtypes=("pg_node_tree",), volatility="i"
            ),
            "pg_catalog.pg_attrdef",
        ),
        (
            Function(
                "pg_ndistinct_out", "pg_ndistinct", argtypes=("pg_ndistinct",), volatility="i"
            ),
            "d.stxdndistinct FROM pg_catalog.pg_statistic_ext_data",
        ),
        (
            Function("pg_mcv_list_send", "pg_mcv_list", argtypes=("pg_mcv_list",), volatility="s"),
            "d.stxdmcv FROM pg_catalog.pg_statistic_ext_data",
        ),
    ],
)
def test_a_type_postgresql_takes_no_literal_of_is_read_from_its_catalog_and_compared(fn, reads):
    assert support.unchecked(fn, "Type operators") is None
    valued = support.value_calls(fn, "Type operators")
    assert len(valued) == support.VALUE_ROWS and all(c.answers for c in valued)
    assert reads in valued[0].sql


def test_what_the_catalog_values_read_is_what_the_fixture_makes():
    made = " ".join(support.FIXTURE_SQL)
    for name in (*support.STATISTICS, support.STATISTICS_TABLE, support.PARTITIONED_TABLE):
        assert f"{catalog.FIXTURE_SCHEMA}.{name} " in made
    assert f"{support.PARTITIONED_TABLE}_0" in made
    trees = support.DERIVED_SAMPLES["pg_node_tree"]
    assert f"'{support.STATISTICS_TABLE}'" in trees[0]
    assert f"'{support.PARTITIONED_TABLE}_0'" in trees[1]
    assert f"lo_from_bytea({support.FIXTURE_LARGE_OBJECT}" in made
    assert f"'{support.REPLICATION_SLOT}'" in made
    assert f"CREATE SERVER {support.FOREIGN_SERVER}" in made


def test_a_foreign_data_wrapper_or_server_privilege_asks_of_the_fixtures_own():
    fdw = Function(
        "has_foreign_data_wrapper_privilege",
        "text, text",
        argtypes=("text", "text"),
        volatility="s",
    )
    assert [c.sql for c in support.value_calls(fdw, "System")][0] == (
        f"""SELECT pg_catalog."has_foreign_data_wrapper_privilege"("""
        f"""'{support.FOREIGN_DATA_WRAPPER}'::text, 'USAGE'::text) AS v"""
    )
    server = Function(
        "has_server_privilege", "name, oid, text", argtypes=("name", "oid", "text"), volatility="s"
    )
    sql = support.value_calls(server, "System")[0].sql
    assert f"srvname = '{support.FOREIGN_SERVER}'" in sql and "'pg_monitor'::name" in sql


def test_a_record_of_no_declared_type_is_called_with_a_column_definition_list():
    fn = Function("json_to_record", "json", argtypes=("json",), volatility="s", rettype="record")
    valued = support.value_calls(fn, "JSON")
    assert [c.sql for c in valued] == list(support.VALUE_STATEMENTS["json_to_record(json)"])
    assert all(c.answers for c in valued)
    assert valued[0].sql.endswith("AS t(a integer, b text)")


def test_a_cursor_is_opened_in_a_transaction_around_the_call_and_rolled_back():
    fn = Function(
        "cursor_to_xmlschema",
        "cursor refcursor, nulls boolean, tableforest boolean, targetns text",
        argtypes=("refcursor", "boolean", "boolean", "text"),
        volatility="v",
        rettype="xml",
    )
    [first, *_] = support.form_calls(fn)
    assert first.setup == support._CURSOR_SETUP and first.teardown == ("ROLLBACK",)
    assert f"'{support._CURSOR}'::refcursor" in first.sql
    server = Effects({s: _ok("x", oid=142) for s in (*first.setup, first.sql, "ROLLBACK")})
    assert support._answer(server, first)["rows"] == [("x",)]
    assert server.ran == [*first.setup, first.sql, "ROLLBACK"]
    no_cursors = {"ok": False, "error": "ERROR:  DECLARE CURSOR is not supported"}
    refused = Effects({first.setup[1]: no_cursors})
    assert not support._answer(refused, first)["ok"]
    assert refused.ran == [*first.setup, "ROLLBACK"]


def test_real_values_postgresql_refuses_or_answers_null_to_say_so_and_stay_not_checked():
    own = "answers with its own server's state"
    null_call = {0: _ok(None, oid=20)}
    refusing = _per_call(DB_COMMITS, null_call, {"ok": False, "error": "ERROR:  no such database"})
    entry = support.measure_overload(DB_COMMITS, "System", refusing, refusing, _route, engine="pg")
    assert entry["outcome"] == "not_checked"
    assert entry["unchecked"] == f"{own}; {support._FORM_REFUSED}"
    nulls = _per_call(DB_COMMITS, null_call, _ok(None, oid=20))
    entry = support.measure_overload(DB_COMMITS, "System", nulls, nulls, _route, engine="pg")
    assert entry["unchecked"] == f"{own}; {support._FORMLESS}"


def test_a_volatile_read_postgresql_refuses_real_values_of_is_checked_by_its_null_call():
    """REQ-2167, amended 2026-11-04 (4): never not tested for being volatile."""
    null_call = {0: _ok(None, oid=17)}
    refusing = _per_call(LO_GET, null_call, {"ok": False, "error": "ERROR:  no such large object"})
    entry = support.measure_overload(
        LO_GET, "Large object", refusing, refusing, _route, engine="pg"
    )
    assert entry["outcome"] == "pass" and entry["type_and_form_checked"]
    assert entry["unchecked"] == "volatile"


def test_the_fixture_makes_a_multixact_of_two_share_locks_held_at_once(monkeypatch):
    ran: list[tuple[int, str]] = []

    class Conn:
        def __init__(self, number: int) -> None:
            self.number = number

        def __enter__(self) -> Any:
            return self

        def __exit__(self, *_: Any) -> None:
            return None

        def execute(self, sql: bytes) -> None:
            ran.append((self.number, sql.decode()))

        def commit(self) -> None:
            ran.append((self.number, "COMMIT"))

    made = iter(range(3))
    monkeypatch.setattr(support.psycopg, "connect", lambda *_a, **_k: Conn(next(made)))
    support.seed_fixture("dsn")
    assert [sql for n, sql in ran if n == 0] == list(support.FIXTURE_SQL)
    lock = f"SELECT id FROM conformance.{support.MULTIXACT_TABLE} FOR SHARE"
    assert ran[len(support.FIXTURE_SQL) :] == [(1, lock), (2, lock), (1, "COMMIT"), (2, "COMMIT")]


def test_seeding_the_column_table_also_seeds_the_fixture(monkeypatch, tmp_path):
    seeded: list[str] = []
    monkeypatch.setattr(support, "seed_columns", lambda dsn, _out: seeded.append(f"columns {dsn}"))
    monkeypatch.setattr(support, "seed_fixture", lambda dsn: seeded.append(f"fixture {dsn}"))
    out = str(tmp_path / "t.yaml")
    assert support.main(["function_support.py", "seed-columns", "--pg", "dsn", "--out", out]) == 0
    assert seeded == ["columns dsn", "fixture dsn"]


def test_lastval_is_measured_after_the_insert_its_answer_needs():
    """REQ-2167 (amended 2026-11-02): lastval answers the session's own INSERT, which runs on
    the same connection just before the call."""
    import pgwire_function_probe as function_probe

    lastval = Function("lastval", "", rettype="bigint", volatility="v")
    (planned,) = support.calls(lastval, "Sequence")
    assert planned.before == function_probe.BEFORE["lastval()"]

    class _Connection(Fake):
        def __init__(self) -> None:
            super().__init__({planned.before: {"ok": True, "fields": [], "rows": []}})
            self.ran: list[str] = []

        def answer(self, sql: str) -> dict[str, Any]:
            self.ran.append(sql)
            if sql == planned.sql and planned.before in self.ran:
                return _ok(1, oid=20)
            return super().answer(sql)

    server = _Connection()
    assert support._answer(server, planned) == _ok(1, oid=20)  # pyright: ignore[reportArgumentType]
    assert server.ran == [planned.before, planned.sql]


# --- decisions of 2026-11-03 (REQ-2167) and the catalog's model (REQ-2190) ----------------------


@pytest.mark.parametrize(
    "name,sql,difference,group",
    [
        (
            "has_table_privilege",
            "SELECT pg_catalog.\"has_table_privilege\"('pg_catalog.pg_namespace'::text, "
            "'INSERT'::text) AS v",
            "'f', PostgreSQL 't'",
            support.SYSTEM_CATALOG_WRITE_GROUP,
        ),
        (
            "has_column_privilege",
            "SELECT pg_catalog.\"has_column_privilege\"('pg_catalog.pg_namespace'::regclass::oid, "
            "'nspname'::text, 'UPDATE'::text) AS v",
            "'f', PostgreSQL 't'",
            support.SYSTEM_CATALOG_WRITE_GROUP,
        ),
        (
            "regclasssend",
            "SELECT pg_catalog.\"regclasssend\"('conformance.function_columns'::regclass) AS v",
            "'\\\\x0000410b', PostgreSQL '\\\\x00004002'",
            support.CATALOG_IS_THE_MODEL_GROUP,
        ),
        (
            "table_to_xml",
            "SELECT pg_catalog.\"table_to_xml\"('pg_catalog.pg_class'::regclass, true::boolean, "
            "true::boolean, 'abc'::text) AS v",
            "'<pg_class ...', PostgreSQL '<pg_class ...'",
            support.CATALOG_IS_THE_MODEL_GROUP,
        ),
    ],
)
def test_a_difference_a_decision_makes_is_that_decisions(name, sql, difference, group):
    assert support.decided_difference(name, sql, difference) == group


@pytest.mark.parametrize(
    "name,sql,difference",
    [
        # A read of a catalog, or a write on the model's own table, is no decision's.
        (
            "has_table_privilege",
            "SELECT pg_catalog.\"has_table_privilege\"('pg_catalog.pg_class'::text, "
            "'SELECT'::text) AS v",
            "'f', PostgreSQL 't'",
        ),
        (
            "has_table_privilege",
            "SELECT pg_catalog.\"has_table_privilege\"('conformance.function_columns'::text, "
            "'INSERT'::text) AS v",
            "'f', PostgreSQL 't'",
        ),
        ("abs", 'SELECT pg_catalog."abs"(3::integer) AS v', "'4', PostgreSQL '3'"),
    ],
)
def test_any_other_difference_stays_a_wrong_answer(name, sql, difference):
    assert support.decided_difference(name, sql, difference) is None


@pytest.mark.parametrize(
    "name,error,group",
    [
        (
            "lo_create",
            "ERROR:  cannot execute lo_create() in a read-only transaction",
            support.LARGE_OBJECT_GROUP,
        ),
        (
            "lo_put",
            "ERROR:  cannot execute lo_from_bytea() in a read-only transaction",
            support.LARGE_OBJECT_GROUP,
        ),
        (
            "has_sequence_privilege",
            'ERROR:  relation "conformance.probe_sequence" does not exist',
            support.SEQUENCE_GROUP,
        ),
        # REQ-2167, amended 2026-11-02, Sequence functions are not supported.
        (
            "pg_sequence_parameters",
            'ERROR:  relation "conformance.probe_sequence" does not exist',
            support.SEQUENCE_GROUP,
        ),
        # REQ-2167, amended 2026-10-27 and 2026-11-03: no function reads the server's files.
        (
            "pg_ls_dir",
            'ERROR:  could not open directory "base": No such file or directory\nCONTEXT:  '
            "PL/pgSQL function provisa_raise(text,text) line 1 at RAISE",
            support.SERVER_FILES_GROUP,
        ),
        (
            "pg_stat_file",
            'ERROR:  could not stat file "PG_VERSION": No such file or directory',
            support.SERVER_FILES_GROUP,
        ),
        # REQ-2167, amended 2026-11-03, Storage, fixture objects and server files (2).
        (
            "lo_get",
            "ERROR:  large object 424243 does not exist",
            support.CATALOG_IS_THE_MODEL_GROUP,
        ),
        (
            "pg_ls_replslotdir",
            'ERROR:  replication slot "conformance_slot" does not exist',
            support.CATALOG_IS_THE_MODEL_GROUP,
        ),
        (
            "pg_xact_status",
            "ERROR:  transaction ID 753 is in the future",
            support.CATALOG_IS_THE_MODEL_GROUP,
        ),
        (
            "txid_status",
            "ERROR:  transaction ID 753 is in the future",
            support.CATALOG_IS_THE_MODEL_GROUP,
        ),
        (
            "pg_extension_update_paths",
            'ERROR:  extension "hstore" is not available',
            support.CATALOG_IS_THE_MODEL_GROUP,
        ),
        # REQ-2167, amended 2026-11-03, Storage, fixture objects and server files (1): Provisa
        # holds no tuple ids.
        (
            "currtid2",
            'ERROR:  relation "conformance.function_columns" does not exist',
            support.STORAGE_GROUP,
        ),
    ],
)
def test_an_error_a_decision_makes_is_that_decisions(name, error, group):
    assert support.decided_error(name, error) == group
    assert support.decided_error("abs", error) is None


def test_listen_and_notify_are_refused_by_name():
    assert refused_by_name(
        "ERROR:  LISTEN is refused by name: Provisa's pgwire has no notification channel"
    )
    assert support.DECIDED_REFUSALS["pg_notify"] == support.NOTIFICATION_GROUP


LO_CREATE = Function("lo_create", "oid", argtypes=("oid",), volatility="v", rettype="oid")


def test_a_large_object_write_is_skipped_by_design_under_the_read_only_decision():
    effect = support.EFFECTS["lo_create(oid)"]
    refusal = {
        "ok": False,
        "error": "ERROR:  cannot execute lo_create() in a read-only transaction",
    }
    provisa = Effects({effect.call: refusal})
    pg = Effects({effect.observe: _ok("1", oid=20)})
    entry = support.measure_overload(LO_CREATE, "Large object", pg, provisa, _route, engine="pg")
    assert entry["outcome"] == "by_design" and entry["group"] == support.LARGE_OBJECT_GROUP


# --- decisions of 2026-11-03: storage, fixture objects, server files, server administration ----


def _named(name: str, argtypes: tuple[str, ...], rettype: str, volatility: str = "v") -> Function:
    return Function(
        name, ", ".join(argtypes), argtypes=argtypes, volatility=volatility, rettype=rettype
    )


@pytest.mark.parametrize(
    "fn,group",
    [
        # REQ-2167, amended 2026-11-03, Storage, fixture objects and server files (1).
        (_named("pg_relation_size", ("regclass",), "bigint"), support.STORAGE_GROUP),
        (_named("pg_tablespace_size", ("oid",), "bigint"), support.STORAGE_GROUP),
        (_named("pg_relation_filepath", ("regclass",), "text", "s"), support.STORAGE_GROUP),
        # (3): no function reads the server's files.
        (_named("pg_read_file", ("text", "boolean"), "text"), support.SERVER_FILES_GROUP),
        (_named("pg_read_binary_file", ("text",), "bytea"), support.SERVER_FILES_GROUP),
        # pg_stat_file with missing_ok answers as a server without the file: NULL, where the
        # fixture server has it (2026-11-04).
        (_named("pg_stat_file", ("text", "boolean"), "record"), support.SERVER_FILES_GROUP),
        # REQ-579: a collation's version is the server's.
        (_named("pg_collation_actual_version", ("oid",), "text"), support.SERVER_VERSION_GROUP),
        (
            _named("pg_database_collation_actual_version", ("oid",), "text"),
            support.SERVER_VERSION_GROUP,
        ),
    ],
)
def test_a_decided_function_s_null_answer_is_skipped_by_design(fn, group):
    entry = _both(fn, "System", _ok("114688", oid=20), _ok(None, oid=20))
    assert entry["outcome"] == "by_design" and entry["group"] == group
    assert entry["requirement"] == support.GROUP_REQUIREMENTS.get(group, "REQ-2167")


def test_pg_stat_file_of_one_argument_is_not_decided_whatever_it_answers():
    """Without missing_ok, a server without the file refuses: that refusal is the decision
    (_DECIDED_ERRORS), and any other answer is measured."""
    fn = _named("pg_stat_file", ("text",), "record")
    entry = _both(fn, "System", _ok("(4096)", oid=2249), _ok(None, oid=2249))
    assert entry["outcome"] != "by_design"


def test_a_collation_s_version_cites_the_server_version_decision():
    assert support.GROUP_REQUIREMENTS[support.SERVER_VERSION_GROUP] == "REQ-579"


def test_a_server_file_read_provisa_refuses_is_skipped_by_design():
    fn = _named("pg_read_file", ("text",), "text")
    entry = _both(
        fn,
        "System",
        _ok("17\n", oid=25),
        {"ok": False, "error": 'ERROR:  could not open file "PG_VERSION" for reading'},
    )
    assert entry["outcome"] == "by_design" and entry["group"] == support.SERVER_FILES_GROUP


@pytest.mark.parametrize(
    "name,refusal",
    [
        ("pg_wal_replay_pause", "ERROR:  recovery is not in progress"),
        ("pg_logical_slot_peek_binary_changes", 'ERROR:  logical decoding requires "wal_level"'),
        ("pg_sync_replication_slots", "ERROR:  replication slots can only be synchronized"),
        ("pg_last_committed_xact", "ERROR:  could not get commit timestamp data"),
        ("pg_ls_logdir", 'ERROR:  could not open directory "log"'),
        ("pg_replication_origin_session_reset", "ERROR:  no replication origin is configured"),
        ("pg_stop_making_pinned_objects", "ERROR:  cannot advance OID counter anymore"),
    ],
)
def test_a_server_administration_function_is_skipped_by_design_with_postgresql_s_refusal(
    name, refusal
):
    """REQ-2167, amended 2026-11-03, Server administration functions: on every engine, PostgreSQL's
    refusal named as the reason."""
    fn = _named(name, (), "void")
    refused = {"ok": False, "error": refusal}
    for engine in ("pg", "duckdb"):
        sqls = [c.sql for c in support.calls(fn, "System")]
        entry = support.measure_overload(
            fn,
            "System",
            Fake(dict.fromkeys(sqls, refused)),
            Fake({}),
            _route,
            engine=engine,
        )
        assert entry["outcome"] == "by_design", engine
        assert entry["group"] == support.SERVER_ADMINISTRATION_GROUP
        assert refusal in entry["reason"]
        assert support.function_report.verdict(entry) == "skipped_by_design"
        assert support.by_design_groups([entry])[support.SERVER_ADMINISTRATION_GROUP] == [
            f"{name}(): PostgreSQL refuses the call: {refusal}"
        ]


@pytest.mark.parametrize(
    "name,sql,difference",
    [
        (
            "has_foreign_data_wrapper_privilege",
            'SELECT pg_catalog."has_foreign_data_wrapper_privilege"((SELECT oid FROM '
            "pg_catalog.pg_foreign_data_wrapper WHERE fdwname = 'conformance_fdw'), "
            "'USAGE'::text) AS v",
            "None, PostgreSQL 't'",
        ),
        (
            "has_server_privilege",
            'SELECT pg_catalog."has_server_privilege"((SELECT oid FROM '
            "pg_catalog.pg_foreign_server WHERE srvname = 'conformance_server'), 'USAGE'::text) AS v",
            "None, PostgreSQL 't'",
        ),
        (
            "pg_ndistinct_send",
            'SELECT pg_catalog."pg_ndistinct_send"((SELECT d.stxdndistinct FROM '
            "pg_catalog.pg_statistic_ext_data AS d JOIN pg_catalog.pg_statistic_ext AS s ON "
            "s.oid = d.stxoid WHERE s.stxname = 'stats_fixture_ab')::pg_ndistinct) AS v",
            "None, PostgreSQL '\\\\xa4bf'",
        ),
        (
            "pg_get_expr",
            'SELECT pg_catalog."pg_get_expr"((SELECT relpartbound FROM pg_catalog.pg_class '
            "WHERE relname = 'hash_fixture_0')::pg_node_tree, 0::oid) AS v",
            "None, PostgreSQL 'FOR VALUES WITH (modulus 2, remainder 0)'",
        ),
        (
            "satisfies_hash_partition",
            'SELECT pg_catalog."satisfies_hash_partition"((SELECT oid FROM pg_catalog.pg_class '
            "WHERE relname = 'hash_fixture'), 2::integer, 0::integer, 42::integer) AS v",
            "'f', PostgreSQL 't'",
        ),
        (
            "pg_get_replication_slots",
            'SELECT pg_catalog."pg_get_replication_slots"() AS v',
            "0 rows, PostgreSQL 1",
        ),
        (
            "pg_stat_get_db_stat_reset_time",
            'SELECT pg_catalog."pg_stat_get_db_stat_reset_time"(5::oid) AS v',
            "None, PostgreSQL '2026-11-03 12:23:32.570024+00'",
        ),
        # REQ-2190: an oid that is no collation of PostgreSQL's is one of the model's catalog.
        (
            "pg_collation_is_visible",
            'SELECT pg_catalog."pg_collation_is_visible"(12345::oid) AS v',
            "'t', PostgreSQL None",
        ),
    ],
)
def test_an_object_only_the_postgresql_fixture_makes_is_the_catalog_s_model(name, sql, difference):
    """REQ-2167, amended 2026-11-03, Storage, fixture objects and server files (2)."""
    assert support.decided_difference(name, sql, difference) == support.CATALOG_IS_THE_MODEL_GROUP


@pytest.mark.parametrize("name", ["pg_node_tree_out", "pg_node_tree_send"])
def test_a_system_views_rule_is_the_catalog_s_model(name):
    """REQ-2167, CATALOG IS THE MODEL'S: the model's pg_rewrite holds no rule of a system view."""
    sql = (
        f'SELECT pg_catalog."{name}"((SELECT r.ev_action FROM pg_catalog.pg_rewrite AS r '
        "JOIN pg_catalog.pg_class AS c ON c.oid = r.ev_class WHERE c.relname = 'pg_tables' "
        "AND c.relnamespace = 'pg_catalog'::regnamespace)::pg_node_tree) AS v"
    )
    difference = "None, PostgreSQL '({QUERY :commandType 1'"
    assert support.decided_difference(name, sql, difference) == support.CATALOG_IS_THE_MODEL_GROUP
    assert support.decided_difference(name, sql, "'x', PostgreSQL 'y'") is None


def test_a_fixture_free_call_of_a_fixture_function_stays_a_wrong_answer():
    sql = 'SELECT pg_catalog."pg_get_expr"(NULL::pg_node_tree, 0::oid) AS v'
    assert support.decided_difference("pg_get_expr", sql, "'x', PostgreSQL None") is None


def test_the_column_table_grants_the_probe_s_role_writes_on_every_column(tmp_path, monkeypatch):
    """REQ-2167, amended 2026-11-03 (4): the privilege checks compare like with like."""

    class Conn:
        def __enter__(self) -> Any:
            return self

        def __exit__(self, *_: Any) -> None:
            return None

        def execute(self, _sql: str) -> None:
            return None

    monkeypatch.setattr(support.psycopg, "connect", lambda *_a, **_k: Conn())
    monkeypatch.setattr(catalog, "_sized_numeric", lambda *_a: {"data_type": "numeric(38,3)"})
    out = tmp_path / "tables.yaml"
    support.seed_columns("dsn", out)
    [table] = yaml.safe_load(out.read_text())
    names = [c["name"] for c in table["columns"]]
    grants = {g["operation"]: g for g in table["write_grants"]}
    assert set(grants) == {"insert", "update", "delete"}
    assert all(g["role"] == "org_admin" for g in grants.values())
    assert grants["insert"]["columns"] == names and grants["update"]["columns"] == names
    from provisa.core.models import WriteGrant

    for grant in table["write_grants"]:
        WriteGrant(**grant)


def test_pg_cursor_s_creation_time_is_compared_by_its_form():
    """REQ-2167, WHAT GUARANTEED MEANS: the clock's field of pg_cursor cannot be compared."""
    sql = 'SELECT pg_catalog."pg_cursor"() AS v'
    want = _ok('("",x,f,f,f,"2026-11-03 12:52:57.771655+00")', oid=2249)
    got = _ok('("",x,f,f,f,"2026-11-03 12:52:57.772824+00")', oid=2249)
    compared = support.compare(want, got, values=True, sql=sql, function="pg_cursor")
    assert compared.difference is None
    assert report.CLOCK_FIELD in compared.rules
    other = _ok('("",y,f,f,f,"2026-11-03 12:52:57.772824+00")', oid=2249)
    assert support.compare(want, other, values=True, sql=sql, function="pg_cursor").difference
    unformed = _ok('("",x,f,f,f,"yesterday")', oid=2249)
    assert support.compare(want, unformed, values=True, sql=sql, function="pg_cursor").difference


# --- decisions of 2026-11-03: time zones, REFERENCES, a table's XML (REQ-2167) --------------------

_ZONES_SQL = 'SELECT pg_catalog."pg_timezone_names"() AS v'


def _zones(*names: str) -> dict[str, Any]:
    return _ok(*(f"({name},UTC,00:00:00,f)" for name in names), oid=2249)


def _compared_zones(want: dict[str, Any], got: dict[str, Any]) -> Any:
    return support.compare(want, got, values=True, sql=_ZONES_SQL, function="pg_timezone_names")


def test_the_zones_compare_by_the_names_both_lists_hold():
    """REQ-2167, amended 2026-11-03, Time zones follow upstream tzdata: Debian's tzdata drops the
    backward links (US/Eastern, a link to America/New_York) and adds localtime and posixrules."""
    want = _zones("America/New_York", "UTC", "localtime", "posixrules")
    got = _zones("America/New_York", "US/Eastern", "UTC")
    compared = _compared_zones(want, got)
    assert compared.difference is None
    assert report.ZONE_PACKAGING in compared.rules


def test_a_posix_rule_zone_tzdata_made_a_backward_link_is_packaging():
    """tzdata 2024b made CST6CDT and its kin backward links, which Debian drops; the pinned release
    compiles each apart from the zone it links to (run 42935119044)."""
    want = _zones("America/Chicago", "America/New_York", "UTC")
    got = _zones("America/Chicago", "America/New_York", "CST6CDT", "EST5EDT", "UTC")
    compared = _compared_zones(want, got)
    assert compared.difference is None
    assert report.ZONE_PACKAGING in compared.rules
    # Its zone not listed by PostgreSQL either: a difference.
    lone = _compared_zones(_zones("UTC"), _zones("UTC", "PST8PDT"))
    assert lone.difference is not None and "PST8PDT" in lone.difference


def test_the_same_zones_need_no_packaging():
    compared = _compared_zones(_zones("UTC", "Asia/Tokyo"), _zones("Asia/Tokyo", "UTC"))
    assert compared.difference is None
    assert report.ZONE_PACKAGING not in compared.rules


def test_a_zone_both_lists_hold_compares_its_row():
    got = _ok("(UTC,UCT,00:00:00,f)", oid=2249)
    compared = _compared_zones(_zones("UTC", "localtime"), got)
    assert compared.difference == "'(UTC,UCT,00:00:00,f)', PostgreSQL '(UTC,UTC,00:00:00,f)'"


@pytest.mark.parametrize(
    "want,got,named",
    [
        # A zone PostgreSQL lists that is no distribution's own.
        (("UTC", "Mars/Olympus"), ("UTC",), "Mars/Olympus"),
        # A zone of the pinned release that is no link to one both lists hold.
        (("UTC",), ("UTC", "Asia/Tokyo"), "Asia/Tokyo"),
        # A link whose zone PostgreSQL does not list either.
        (("UTC",), ("UTC", "US/Eastern"), "US/Eastern"),
    ],
)
def test_a_zone_one_list_holds_for_any_other_reason_differs(want, got, named):
    compared = _compared_zones(_zones(*want), _zones(*got))
    assert compared.difference is not None and named in compared.difference


@pytest.mark.parametrize(
    "name,sql",
    [
        (
            "has_table_privilege",
            "SELECT pg_catalog.\"has_table_privilege\"('conformance.function_columns'::text, "
            "'REFERENCES'::text) AS v",
        ),
        (
            "has_column_privilege",
            'SELECT pg_catalog."has_column_privilege"('
            "'conformance.function_columns'::regclass::oid, 1::smallint, 'REFERENCES'::text) AS v",
        ),
        (
            "has_any_column_privilege",
            "SELECT pg_catalog.\"has_any_column_privilege\"('conformance.function_columns'::text, "
            "'REFERENCES'::text) AS v",
        ),
    ],
)
def test_references_is_never_granted(name, sql):
    """REQ-2167, amended 2026-11-03, REFERENCES is never granted: 'f' is Provisa's answer."""
    assert support.decided_difference(name, sql, "'f', PostgreSQL 't'") == support.REFERENCES_GROUP
    assert support.GROUPS[support.REFERENCES_GROUP]


def test_another_privilege_on_the_model_s_table_stays_a_wrong_answer():
    sql = (
        "SELECT pg_catalog.\"has_table_privilege\"('conformance.function_columns'::text, "
        "'SELECT'::text) AS v"
    )
    assert support.decided_difference("has_table_privilege", sql, "'f', PostgreSQL 't'") is None


_TABLE_XML_SQL = (
    "SELECT pg_catalog.\"table_to_xml_and_xmlschema\"('conformance.function_columns'::regclass, "
    "true::boolean, true::boolean, '12'::text) AS v"
)


def _document(database: str, arrays: tuple[str, str], tail: str = "") -> dict[str, Any]:
    integers, texts = arrays
    return _ok(
        f'<xsd:simpleType name="UDT.{database}.pg_catalog._int4">\n'
        f'<xsd:complexType name="RowType.{database}.conformance.function_columns">\n'
        f"  <c_integer_array>{integers}</c_integer_array>\n"
        f"  <c_text_array>{texts}</c_text_array>\n"
        f"  <c_int4range>(,4)</c_int4range>{tail}\n",
        oid=142,
    )


def test_a_table_s_xml_differing_in_its_arrays_and_database_alone_is_by_design():
    """REQ-1965: an array column is the JSON it is registered as; REQ-588: the database is the
    served one."""
    want = _document("conformance", ("<element>-7</element><element>42</element>", ""))
    got = _document("provisa", ("[-7, null, 42]", "[]"))
    differs = support.compare(want, got, values=True, sql=_TABLE_XML_SQL).difference
    assert differs is not None
    group = support.decided_answer("table_to_xml_and_xmlschema", _TABLE_XML_SQL, want, got)
    assert group == support.TABLE_XML_GROUP
    assert support.GROUP_REQUIREMENTS[group] == "REQ-1965, REQ-588"


def test_a_table_s_xml_differing_in_anything_else_stays_a_wrong_answer():
    want = _document("conformance", ("<element>-7</element><element>42</element>", ""))
    for got in (
        _document("provisa", ("[-7, null, 42]", "[]"), tail="x"),
        _document("provisa", ("[-7, null, 43]", "[]")),
    ):
        assert support.decided_answer("table_to_xml", _TABLE_XML_SQL, want, got) is None
    assert support.decided_answer("abs", _TABLE_XML_SQL, want, want) is None


# REQ-2167, amended 2026-11-04: the server state the client tools read is Provisa's own, and
# compared by its type and form, never skipped by design.
_TOOL_SERVER_STATE = (
    (Function("pg_stat_get_numscans", "oid", argtypes=("oid",), volatility="s"), 20, "1", "0"),
    (Function("pg_tablespace_location", "oid", argtypes=("oid",), volatility="s"), 25, "", ""),
    (
        Function("pg_relation_is_publishable", "regclass", argtypes=("regclass",), volatility="s"),
        16,
        "t",
        "f",
    ),
)


@pytest.mark.parametrize(("fn", "oid", "pg", "provisa"), _TOOL_SERVER_STATE)
def test_server_state_a_client_tool_reads_is_compared_by_type_and_form(fn, oid, pg, provisa):
    assert support.own_answer(fn) is not None
    assert fn.name not in support.DECIDED_FUNCTIONS
    assert len(support.form_calls(fn)) == support.VALUE_ROWS
    entry = _both(fn, "System", _ok(pg, oid=oid), _ok(provisa, oid=oid))
    assert entry["outcome"] == "pass"
    assert entry["type_and_form_checked"] and not entry["answers_checked"]


@pytest.mark.parametrize("name", ["pg_relation_size", "pg_total_relation_size"])
def test_relation_sizes_stay_skipped_by_design_as_provisa_holds_no_storage(name):
    assert support.DECIDED_FUNCTIONS[name] == support.STORAGE_GROUP


# --- REQ-2167, amended 2026-11-03 and 2026-11-04: server state, internal types, call shapes ----

_DECIDED_2026_11_04 = [
    # Backend session statistics are the engine's (amended 2026-11-03).
    (_named("pg_stat_get_backend_activity", ("integer",), "text", "s"), "SERVER_ADMINISTRATION"),
    (_named("pg_stat_get_backend_subxact", ("integer",), "record", "s"), "SERVER_ADMINISTRATION"),
    (_named("pg_stat_get_backend_idset", (), "integer", "s"), "SERVER_ADMINISTRATION"),
    # (1) PostgreSQL's own server state, the transaction counters beside it.
    (_named("pg_stat_get_xact_numscans", ("oid",), "bigint"), "SERVER_STATE"),
    (_named("pg_stat_get_xact_function_calls", ("oid",), "bigint"), "SERVER_STATE"),
    (_named("pg_current_logfile", ("text",), "text"), "SERVER_STATE"),
    (_named("pg_hba_file_rules", (), "record"), "SERVER_STATE"),
    (_named("pg_get_shmem_allocations", (), "record"), "SERVER_STATE"),
    (_named("pg_lock_status", (), "record"), "SERVER_STATE"),
    (_named("pg_stat_get_slru", (), "record", "s"), "SERVER_STATE"),
    (_named("pg_stat_get_wal_senders", (), "record", "s"), "SERVER_STATE"),
    (_named("pg_ls_tmpdir", ("oid",), "record"), "SERVER_STATE"),
    (_named("pg_prepared_xact", (), "record"), "SERVER_STATE"),
    # (2) the session's cursors, a source's sequences, the server's extensions, LATIN encodings.
    (_named("pg_cursor", (), "record", "s"), "SESSION_CURSORS"),
    (_named("pg_sequence_last_value", ("regclass",), "bigint"), "SEQUENCE"),
    (_named("pg_extension_update_paths", ("name",), "record", "s"), "SERVER_EXTENSIONS"),
    (_named("to_ascii", ("text",), "text", "i"), "SINGLE_BYTE_DATABASE"),
    # (3) the types no SQL value can have.
    (
        _named("anycompatiblenonarray_out", ("anycompatiblenonarray",), "cstring", "i"),
        "NO_SQL_VALUE",
    ),
    (_named("shell_out", ("void",), "cstring", "i"), "NO_SQL_VALUE"),
    (_named("pg_mcv_list_in", ("cstring",), "pg_mcv_list", "s"), "NO_SQL_VALUE"),
    (_named("brin_bloom_summary_send", ("pg_brin_bloom_summary",), "bytea", "s"), "NO_SQL_VALUE"),
    (_named("gtsvectorout", ("gtsvector",), "cstring", "i"), "NO_SQL_VALUE"),
]


@pytest.mark.parametrize(("fn", "group"), _DECIDED_2026_11_04)
def test_a_function_a_decision_names_is_skipped_by_design_whatever_it_answers(fn, group):
    group = getattr(support, f"{group}_GROUP")
    assert support.decided_function(fn) == group
    for got in (_ok("1"), {"ok": False, "error": "ERROR:  pg_cursor() has no form on pg"}):
        entry = _both(fn, "System", _ok("1"), got)
        assert entry["outcome"] == "by_design" and entry["group"] == group
        assert entry["detail"] == support.GROUPS[group]
        assert support.function_report.verdict(entry) == "skipped_by_design"


def test_a_backend_statistic_provisa_refuses_by_name_is_skipped_by_design():
    """REQ-2167, amended 2026-11-03, Backend session statistics are the engine's."""
    refused = {"ok": False, "error": "ERROR:  pg_stat_get_backend_pid() has no form on pg"}
    fn = _named("pg_stat_get_backend_pid", ("integer",), "integer", "s")
    entry = _both(fn, "System", _ok("42"), refused)
    assert entry["group"] == support.SERVER_ADMINISTRATION_GROUP
    assert "pg_stat_get_backend_* and pg_stat_get_backend_idset" in entry["detail"]


def test_the_twenty_five_pseudo_type_and_eight_summary_io_functions_are_decided():
    decided = {
        name for name, g in support.DECIDED_FUNCTIONS.items() if g == support.NO_SQL_VALUE_GROUP
    }
    summaries = {
        "brin_bloom_summary_in",
        "brin_bloom_summary_out",
        "brin_bloom_summary_send",
        "brin_minmax_multi_summary_in",
        "brin_minmax_multi_summary_out",
        "brin_minmax_multi_summary_send",
        "gtsvectorin",
        "gtsvectorout",
    }
    assert summaries <= decided and len(decided - summaries) == 25
    # The forms a value can reach stay compared: an array's output, a recv's internal argument.
    assert "anyarray_out" not in decided and "anyarray_recv" not in decided


def test_only_the_one_argument_to_ascii_is_decided():
    assert support.decided_function(_named("to_ascii", ("text", "integer"), "text", "i")) is None
    assert support.decided_function(_named("to_ascii", ("text", "name"), "text", "i")) is None


def test_a_function_no_decision_names_is_measured():
    for fn in (
        ABS,
        _named("pg_stat_get_db_xact_commit", ("oid",), "bigint", "s"),
        _named("pg_current_wal_lsn", (), "pg_lsn"),
        _named("pg_partition_tree", ("regclass",), "record"),
    ):
        assert support.decided_function(fn) is None


def test_unknownin_s_refusal_proves_it_internal_and_skipped_by_design():
    """REQ-2167, amended 2026-11-04 (2): the parser's internal type, which PostgreSQL itself cannot
    call from SQL."""
    import pgwire_function_probe as function_probe

    refusal = "ERROR:  failed to find conversion function from unknown to text"
    fn = _named("unknownin", ("cstring",), "unknown", "i")
    assert function_probe.refused_for(fn, "XX000", refusal) == function_probe.THE_CALL
    pg = Refusing({}, sqlstate="XX000", error=refusal)
    entry = support.measure_overload(fn, "Type operators", pg, Fake({}), _route, engine="pg")
    assert entry["outcome"] == "not_callable"
    assert entry["detail"] == f"PostgreSQL refuses the call: {refusal}"
    assert support.function_report.verdict(entry) == "skipped_by_design"


def test_a_volatile_function_answering_a_null_where_postgresql_answers_a_value_is_wrong():
    """REQ-2167, amended 2026-11-04 (4): its type and form, NULLs too, are PostgreSQL's."""
    fn = Function("f", "integer", argtypes=("integer",), volatility="v", rettype="text")
    entry = _both(fn, "System", _ok("x", oid=25), _ok(None, oid=25))
    assert entry["outcome"] == "wrong_answer"
    assert entry["detail"]["difference"] == "None, PostgreSQL 'x'"


def test_a_volatile_set_provisa_answers_empty_has_its_type_checked():
    tree = _named("pg_partition_tree", ("regclass",), "record")
    want = {"ok": True, "fields": [2249], "rows": [("(a,,t,0)",), ("(b,a,t,1)",)]}
    none = {"ok": True, "fields": [2249], "rows": []}
    entry = _both(tree, "System", want, none)
    assert entry["outcome"] == "pass" and entry["type_and_form_checked"]
    other = {"ok": True, "fields": [25], "rows": []}
    assert _both(tree, "System", want, other)["outcome"] == "wrong_answer"


def test_a_volatile_function_that_acts_is_tested_by_its_side_effect_not_its_type():
    fn = _named("pg_cancel_backend", ("integer",), "boolean")
    entry = _both(fn, "System", _ok(None, oid=16), _ok(None, oid=16))
    assert not entry["type_and_form_checked"]
    assert entry["outcome"] == "by_design" and entry["group"] == support.NOT_TESTABLE


@pytest.mark.parametrize(
    "name,typ", [("json_to_recordset", "json"), ("jsonb_to_recordset", "jsonb")]
)
def test_a_recordset_is_called_in_from_with_a_column_definition_list(name, typ):
    """REQ-2167, amended 2026-11-04 (5)."""
    fn = Function(name, typ, argtypes=(typ,), volatility="s", rettype="record")
    assert support.unchecked(fn, "JSON") is None
    planned = support.value_calls(fn, "JSON")
    assert [c.sql for c in planned][0] == (
        f'SELECT * FROM pg_catalog."{name}"(\'[{{"a": 1, "b": "x"}}, {{"a": 2}}]\'::{typ}) '
        "AS t(a integer, b text)"
    )
    assert len(planned) == support.VALUE_ROWS and all(c.answers for c in planned)


@pytest.mark.parametrize(
    "argtypes",
    [
        ("double precision[]", "double precision"),
        ("double precision[]", "interval"),
        ("double precision[]", "anyelement"),
    ],
)
def test_a_percentile_of_an_array_of_fractions_is_within_group(argtypes):
    for name in ("percentile_cont", "percentile_disc"):
        fn = Function(name, "x", kind="a", aggkind="o", direct=1, argtypes=argtypes, volatility="i")
        sql = support.value_calls(fn, "Numeric")[0].sql
        assert sql.startswith(
            f"SELECT pg_catalog.\"{name}\"('{{0.25,0.5}}'::double precision[]) "
            "WITHIN GROUP (ORDER BY c1) AS v FROM (VALUES "
        )


@pytest.mark.parametrize(
    "name,modifier",
    [
        *(
            (f"{t}typmodin", "'{10}'")
            for t in ("bit", "bpchar", "timestamp", "timestamptz", "time", "timetz", "varbit")
        ),
        ("varchartypmodin", "'{10}'"),
        ("numerictypmodin", "'{12,2}'"),
        # PostgreSQL refuses a bare number for interval: its fields and precision, INTERVAL(3).
        ("intervaltypmodin", "'{32767,3}'"),
    ],
)
def test_a_typmodin_reads_a_cstring_array_modifier(name, modifier):
    fn = Function(name, "cstring[]", argtypes=("cstring[]",), volatility="i", rettype="integer")
    assert support.unchecked(fn, "Type operators") is None
    assert [c.sql for c in support.value_calls(fn, "Type operators")] == [
        f'SELECT pg_catalog."{name}"({modifier}::cstring[]) AS v'
    ] * support.VALUE_ROWS


def test_interval_in_reads_intervals_with_its_type_oid_and_no_typmod():
    fn = Function(
        "interval_in",
        "cstring, oid, integer",
        argtypes=("cstring", "oid", "integer"),
        volatility="s",
        rettype="interval",
    )
    assert support.value_calls(fn, "Type operators")[0].sql == (
        "SELECT pg_catalog.\"interval_in\"('1 day 02:03:04'::cstring, 'interval'::regtype::oid, "
        "(-1)::integer) AS v"
    )


def test_domain_in_reads_the_fixture_s_domain(monkeypatch):
    fn = Function(
        "domain_in",
        "cstring, oid, integer",
        argtypes=("cstring", "oid", "integer"),
        volatility="s",
        rettype='"any"',
    )
    monkeypatch.setattr(support, "FIXTURE", {})
    assert support.value_calls(fn, "Type operators") == []
    monkeypatch.setattr(support, "FIXTURE", {"domain": "16393"})
    assert [c.sql for c in support.value_calls(fn, "Type operators")][:3] == [
        f"SELECT pg_catalog.\"domain_in\"({v}::cstring, '16393'::oid, (-1)::integer) IS NULL AS v"
        for v in ("'5'", "NULL", "'1'")
    ]
    made = " ".join(support.FIXTURE_SQL)
    assert f"CREATE DOMAIN conformance.{support.FIXTURE_DOMAIN} AS integer" in made
    assert support.FIXTURE_DOMAIN in support.FIXTURE_QUERIES["domain"]


# --- a call PostgreSQL cannot make (REQ-2167, amended 2026-11-04) -------------------------------

import pgwire_function_probe as function_probe  # noqa: E402

OID_ARG = Function("f", "oid", argtypes=("oid",), volatility="i", rettype="boolean")
TEXT_ARG = Function("f", "text", argtypes=("text",), volatility="i", rettype="boolean")


@pytest.mark.parametrize(
    ("fn", "sqlstate", "error", "kind"),
    [
        (OID_ARG, "0A000", "ERROR:  aclinsert is no longer supported", "the call"),
        (OID_ARG, "0A000", "ERROR:  cannot display a value of type any", "the call"),
        (OID_ARG, "42809", "ERROR:  x is not a table", "the call"),
        (OID_ARG, "39P01", 'ERROR:  function "x" was not called by trigger manager', "the call"),
        (
            OID_ARG,
            "39P03",
            "ERROR:  x() can only be called in an event trigger function",
            "the call",
        ),
        (OID_ARG, "42P22", "ERROR:  could not determine which collation to use", "the call"),
        (TEXT_ARG, "42P22", "ERROR:  could not determine which collation to use", "its arguments"),
        (OID_ARG, "42883", "ERROR:  function f(text) does not exist", "its arguments"),
        (OID_ARG, "42804", "ERROR:  argument of WHERE must be type boolean", "its arguments"),
        (OID_ARG, "42846", "ERROR:  cannot cast type integer to point", "its arguments"),
        (OID_ARG, "22P02", 'ERROR:  invalid input syntax for type integer: "x"', "its arguments"),
        (OID_ARG, "22003", "ERROR:  integer out of range", "its arguments"),
        # XX000 and 55P02 are each PostgreSQL's code for both kinds: the message tells them apart.
        (
            OID_ARG,
            "XX000",
            "ERROR:  aggregate function called in non-aggregate context",
            "the call",
        ),
        (OID_ARG, "XX000", "ERROR:  cache lookup failed for type 1", None),
        (
            OID_ARG,
            "55P02",
            "ERROR:  function can only be called when server is in binary upgrade mode",
            "the call",
        ),
        (OID_ARG, "55P02", 'ERROR:  parameter "x" cannot be changed now', None),
        (OID_ARG, "55000", "ERROR:  recovery is not in progress", None),
        (OID_ARG, "", "ERROR:  no such function", None),
    ],
)
def test_a_refusal_is_of_the_call_or_of_its_arguments_by_its_sqlstate(fn, sqlstate, error, kind):
    assert function_probe.refused_for(fn, sqlstate, error) == kind


#: The seven overloads the amendment names, each with PostgreSQL 17.11's refusal of its value calls.
SEVEN = [
    (
        Function(
            "int4_avg_combine",
            "bigint[], bigint[]",
            argtypes=("bigint[]", "bigint[]"),
            volatility="i",
            rettype="bigint[]",
        ),
        "XX000",
        "ERROR:  aggregate function called in non-aggregate context",
    ),
    (
        Function(
            "range_intersect_agg_transfn",
            "anyrange, anyrange",
            argtypes=("anyrange", "anyrange"),
            volatility="i",
            rettype="anyrange",
        ),
        "XX000",
        "ERROR:  range_intersect_agg_transfn called in non-aggregate context",
    ),
    (
        Function(
            "multirange_intersect_agg_transfn",
            "anymultirange, anymultirange",
            argtypes=("anymultirange", "anymultirange"),
            volatility="i",
            rettype="anymultirange",
        ),
        "XX000",
        "ERROR:  multirange_intersect_agg_transfn called in non-aggregate context",
    ),
    (
        Function("btvarstrequalimage", "oid", argtypes=("oid",), volatility="i", rettype="boolean"),
        "42P22",
        "ERROR:  could not determine which collation to use for string comparison",
    ),
    (
        Function(
            "aclinsert",
            "aclitem[], aclitem",
            argtypes=("aclitem[]", "aclitem"),
            volatility="i",
            rettype="aclitem[]",
        ),
        "0A000",
        "ERROR:  aclinsert is no longer supported",
    ),
    (
        Function(
            "aclremove",
            "aclitem[], aclitem",
            argtypes=("aclitem[]", "aclitem"),
            volatility="i",
            rettype="aclitem[]",
        ),
        "0A000",
        "ERROR:  aclremove is no longer supported",
    ),
    (
        Function(
            "xmlvalidate", "xml, text", argtypes=("xml", "text"), volatility="i", rettype="boolean"
        ),
        "0A000",
        "ERROR:  xmlvalidate is not implemented",
    ),
]


@pytest.mark.parametrize(("fn", "sqlstate", "error"), SEVEN, ids=[f.name for f, _, _ in SEVEN])
def test_an_overload_postgresql_refuses_every_real_call_of_is_skipped_by_design(
    fn, sqlstate, error
):
    """Its NULL call answers NULL without running it (it is strict); every call with a value is
    refused for the call itself."""
    planned = support.calls(fn, "Aggregate support")
    assert any(c.answers for c in planned)
    null = {planned[0].sql: _ok(None)}
    pg = Refusing(null, sqlstate=sqlstate, error=error)
    entry = support.measure_overload(fn, "Aggregate support", pg, Fake(null), _route, engine="pg")
    assert entry["outcome"] == "not_callable"
    assert entry["refused_for"] == "the call" and entry["refusal_sqlstates"] == [sqlstate]
    assert entry["detail"] == f"PostgreSQL refuses the call: {error}"
    assert report.verdict(entry) == "skipped_by_design"


def test_one_refusal_of_the_arguments_leaves_the_overload_not_tested():
    """Not callable only when every call was refused for the call itself: one refused for the
    values the check gave it is a gap in the samples."""
    planned = support.calls(ABS, "Numeric")
    pg = Fake(
        {
            planned[0].sql: _ok(None),
            planned[1].sql: {"ok": False, "error": "ERROR:  not supported", "sqlstate": "0A000"},
            planned[2].sql: {"ok": False, "error": "ERROR:  bad", "sqlstate": "22P02"},
            planned[3].sql: {"ok": False, "error": "ERROR:  not supported", "sqlstate": "0A000"},
        }
    )
    entry = support.measure_overload(
        ABS, "Numeric", pg, Fake({planned[0].sql: _ok(None)}), _route, engine="pg"
    )
    assert entry["outcome"] == "not_checked" and report.verdict(entry) == "not_tested"


def test_every_call_refused_for_its_arguments_is_not_tested_not_failed():
    pg = Refusing({}, sqlstate="42883", error="ERROR:  function abs(text) does not exist")
    entry = support.measure_overload(ABS, "Numeric", pg, Fake({}), _route, engine="pg")
    assert entry["outcome"] == "not_checked" and entry["refused_for"] == "its arguments"
    assert entry["unchecked"] == (
        "PostgreSQL refused every call for its arguments: ERROR:  function abs(text) does not exist"
    )
    assert report.verdict(entry) == "not_tested"


def test_every_call_refused_for_the_server_s_state_still_fails():
    pg = Refusing({}, sqlstate="55000", error="ERROR:  recovery is not in progress")
    entry = support.measure_overload(ABS, "Numeric", pg, Fake({}), _route, engine="pg")
    assert entry["outcome"] == "not_callable" and "refused_for" not in entry
    assert report.verdict(entry) == "failed"


def test_an_overload_postgresql_answers_a_value_call_of_is_callable():
    planned = support.calls(ABS, "Numeric")
    answered = {planned[0].sql: _ok(None), planned[1].sql: _ok("3")}
    pg = Refusing(answered, sqlstate="0A000", error="ERROR:  not supported")
    entry = support.measure_overload(ABS, "Numeric", pg, Fake(answered), _route, engine="pg")
    assert entry["outcome"] == "pass"


def test_a_null_call_answering_a_value_makes_the_overload_callable():
    planned = support.calls(ABS, "Numeric")
    answered = {planned[0].sql: _ok("0")}
    pg = Refusing(answered, sqlstate="0A000", error="ERROR:  not supported")
    entry = support.measure_overload(ABS, "Numeric", pg, Fake(answered), _route, engine="pg")
    assert entry["outcome"] != "not_callable" and "refused_for" not in entry


def test_the_data_file_keeps_why_postgresql_refused_the_call():
    entry = {
        "outcome": "not_callable",
        "detail": "PostgreSQL refuses the call: ERROR:  xmlvalidate is not implemented",
        "refused_for": "the call",
        "refusal_sqlstates": ["0A000"],
    }
    kept = support._outcome(entry)
    assert kept["refused_for"] == "the call" and kept["refusal_sqlstates"] == ["0A000"]
    assert report.verdict(kept) == "skipped_by_design"


def test_a_server_s_refusal_keeps_its_sqlstate():
    class Res:
        status = support.pq.ExecStatus.FATAL_ERROR
        error_message = b'ERROR:  value for domain d violates check constraint "d_check"\n'

        @staticmethod
        def error_field(field: Any) -> bytes | None:
            assert field == support.pq.DiagnosticField.SQLSTATE
            return b"23514"

    assert support._result(Res()) == {
        "ok": False,
        "error": 'ERROR:  value for domain d violates check constraint "d_check"',
        "sqlstate": "23514",
    }


@pytest.mark.parametrize(
    "name",
    [
        "pg_config",
        "pg_available_extensions",
        "pg_available_extension_versions",
        "pg_current_xact_id_if_assigned",
        "txid_current_if_assigned",
        "pg_stat_get_activity",
        "pg_stat_get_db_checksum_failures",
        "pg_stat_get_db_checksum_last_failure",
        "pg_stat_get_snapshot_timestamp",
        "pg_stat_get_progress_info",
    ],
)
def test_more_of_postgresql_s_own_server_state_is_skipped_by_design(name):
    assert support.decided_function(_named(name, (), "record", "s")) == (support.SERVER_STATE_GROUP)


# --- domain_in: whether its answer IS NULL, and the refusal its domain's CHECK makes --------------

DOMAIN_IN = Function(
    "domain_in",
    "cstring, oid, integer",
    argtypes=("cstring", "oid", "integer"),
    volatility="s",
    rettype='"any"',
)
CHECK_REFUSAL = {
    "ok": False,
    "error": "ERROR:  value for domain conformance.fixture_domain violates check constraint "
    '"fixture_domain_check"',
    "sqlstate": "23514",
}


def _domain_in(monkeypatch, provisa_refusal: dict[str, Any]) -> dict[str, Any]:
    monkeypatch.setattr(support, "FIXTURE", {"domain": "16393"})
    planned = support.calls(DOMAIN_IN, "Type operators")
    answers = {planned[0].sql: _ok(None, oid=2276)}
    for c in planned[1:]:
        answers[c.sql] = (
            CHECK_REFUSAL if c.refusal else _ok("t" if "NULL::cstring" in c.sql else "f", oid=16)
        )
    got = {**answers, planned[-1].sql: provisa_refusal}
    return support.measure_overload(
        DOMAIN_IN, "Type operators", Fake(answers), Fake(got), _route, engine="pg"
    )


def test_domain_in_is_compared_by_whether_its_answer_is_null(monkeypatch):
    monkeypatch.setattr(support, "FIXTURE", {"domain": "16393"})
    planned = support.value_calls(DOMAIN_IN, "Type operators")
    assert all(c.sql.endswith(" IS NULL AS v") and c.answers for c in planned)
    assert [c.refusal for c in planned] == [False, False, False, True]
    assert planned[-1].sql == (
        "SELECT pg_catalog.\"domain_in\"('-1'::cstring, '16393'::oid, (-1)::integer) IS NULL AS v"
    )


def test_domain_in_passes_on_postgresql_s_check_refusal(monkeypatch):
    entry = _domain_in(monkeypatch, CHECK_REFUSAL)
    assert entry["outcome"] == "pass" and entry["answers_checked"]


def test_domain_in_refused_with_another_sqlstate_is_a_wrong_answer(monkeypatch):
    other = {**CHECK_REFUSAL, "sqlstate": "22P02"}
    entry = _domain_in(monkeypatch, other)
    assert entry["outcome"] == "wrong_answer"
    assert entry["detail"]["difference"].startswith("refused: 22P02 ")


def test_domain_in_answering_a_value_breaking_its_check_is_a_wrong_answer(monkeypatch):
    entry = _domain_in(monkeypatch, _ok("f", oid=16))
    assert entry["outcome"] == "wrong_answer"
    assert entry["detail"]["difference"].startswith("answered [('f',)], PostgreSQL refused: 23514 ")


# --- REQ-2167, amended 2026-11-13, AN INTERNAL TYPE IS BY DESIGN ---------------------------------

ENGINES = ("clickhouse", "trino", "duckdb")
NOT_READ = "does not read this value"


def _on(fn: Function, engine: str, provisa: dict[str, Any] | None) -> dict[str, Any]:
    """``fn`` on ``engine`` where PostgreSQL answers every call and Provisa refuses each with
    ``provisa``'s error, or answers as PostgreSQL does when ``provisa`` is None."""
    sqls = [c.sql for c in support.calls(fn, fn_family(fn))]
    want = {s: _ok("1") for s in sqls}
    got = want if provisa is None else {s: provisa for s in sqls}
    return support.measure_overload(fn, fn_family(fn), Fake(want), Fake(got), _route, engine=engine)


def fn_family(fn: Function) -> str:
    return "Aggregate and window" if fn.kind == "a" else "System"


def _refusal(fn: Function, text: str, engine: str) -> dict[str, Any]:
    said = f"{fn.name}() has no form on {engine}" if text == "no form" else f"{fn.name} {text}"
    return {"ok": False, "error": f"ERROR:  {said}"}


REGCLASSIN = Function(
    "regclassin", "cstring", argtypes=("cstring",), rettype="regclass", volatility="s"
)

#: One overload of each internal type's own functions, and the refusal text it had in the baseline
#: run: the type decides, whatever the text.
OWN_FUNCTIONS = [
    (REGCLASSIN, NOT_READ),
    (Function("regclasssend", "regclass", argtypes=("regclass",), rettype="bytea"), "no form"),
    (Function("regprocout", "regproc", argtypes=("regproc",), rettype="cstring"), NOT_READ),
    (Function("record_out", "record", argtypes=("record",), rettype="cstring"), NOT_READ),
    (
        Function("btrecordcmp", "record, record", argtypes=("record", "record"), rettype="integer"),
        NOT_READ,
    ),
    (
        Function(
            "hash_record_extended",
            "record, bigint",
            argtypes=("record", "bigint"),
            rettype="bigint",
        ),
        NOT_READ,
    ),
    (Function("aclitemin", "cstring", argtypes=("cstring",), rettype="aclitem"), NOT_READ),
    (
        Function(
            "aclitemeq", "aclitem, aclitem", argtypes=("aclitem", "aclitem"), rettype="boolean"
        ),
        "no form",
    ),
    (
        Function(
            "aclcontains",
            "aclitem[], aclitem",
            argtypes=("aclitem[]", "aclitem"),
            rettype="boolean",
        ),
        "no form",
    ),
    (
        Function(
            "aclexplode",
            "acl aclitem[], OUT grantor oid, OUT grantee oid, OUT privilege_type text, "
            "OUT is_grantable boolean",
            argtypes=("aclitem[]",),
            rettype="record",
        ),
        NOT_READ,
    ),
    (
        Function(
            "array_in",
            "cstring, oid, integer",
            argtypes=("cstring", "oid", "integer"),
            rettype="anyarray",
        ),
        NOT_READ,
    ),
    (
        Function(
            "domain_in",
            "cstring, oid, integer",
            argtypes=("cstring", "oid", "integer"),
            rettype='"any"',
        ),
        NOT_READ,
    ),
    (
        Function(
            "record_in",
            "cstring, oid, integer",
            argtypes=("cstring", "oid", "integer"),
            rettype="record",
        ),
        NOT_READ,
    ),
    (Function("xid8in", "cstring", argtypes=("cstring",), rettype="xid8"), NOT_READ),
    (Function("xid8eq", "xid8, xid8", argtypes=("xid8", "xid8"), rettype="boolean"), "no form"),
    (
        Function("xid8_larger", "xid8, xid8", argtypes=("xid8", "xid8"), rettype="xid8"),
        "no form",
    ),
    (Function("xid", "xid8", argtypes=("xid8",), rettype="xid"), "no form"),
    (Function("age", "xid", argtypes=("xid",), rettype="integer", volatility="s"), NOT_READ),
    (Function("int8_avg", "bigint[]", argtypes=("bigint[]",), rettype="numeric"), NOT_READ),
    (
        Function(
            "int8inc_float8_float8",
            "bigint, double precision, double precision",
            argtypes=("bigint", "double precision", "double precision"),
            rettype="bigint",
        ),
        NOT_READ,
    ),
]

#: Refused overloads that stay refused by name: a PostgreSQL-only value type (class B), a value
#: type an engine does not hold yet (class C), and a user-level function that merely takes or
#: returns an internal type. Each is a gap.
GAPS = [
    (Function("jsonpath_in", "cstring", argtypes=("cstring",), rettype="jsonpath"), NOT_READ),
    (Function("tsqueryout", "tsquery", argtypes=("tsquery",), rettype="cstring"), NOT_READ),
    (Function("xml_in", "cstring", argtypes=("cstring",), rettype="xml"), NOT_READ),
    (
        Function(
            "numeric_div", "numeric, numeric", argtypes=("numeric", "numeric"), rettype="numeric"
        ),
        NOT_READ,
    ),
    (Function("network_eq", "inet, inet", argtypes=("inet", "inet"), rettype="boolean"), NOT_READ),
    (Function("interval_hash", "interval", argtypes=("interval",), rettype="integer"), NOT_READ),
    # A type with a typmod reads it by the same shape: its own input, not a type's input by oid.
    (
        Function(
            "interval_in",
            "cstring, oid, integer",
            argtypes=("cstring", "oid", "integer"),
            rettype="interval",
        ),
        NOT_READ,
    ),
    (
        Function(
            "numeric_in",
            "cstring, oid, integer",
            argtypes=("cstring", "oid", "integer"),
            rettype="numeric",
        ),
        NOT_READ,
    ),
    (Function("row_to_json", "record", argtypes=("record",), rettype="json"), "no form"),
    (
        Function("record_eq", "record, record", argtypes=("record", "record"), rettype="boolean"),
        "no form",
    ),
    (
        Function(
            "record_image_lt",
            "record, record",
            argtypes=("record", "record"),
            rettype="boolean",
        ),
        "no form",
    ),
    (
        Function(
            "ts_headline",
            "regconfig, text, tsquery, text",
            argtypes=("regconfig", "text", "tsquery", "text"),
            rettype="text",
        ),
        "no form",
    ),
    (
        Function("min", "xid8", kind="a", argtypes=("xid8",), rettype="xid8", volatility="i"),
        "no form",
    ),
    (
        Function(
            "pg_xact_commit_timestamp",
            "xid",
            argtypes=("xid",),
            rettype="timestamp with time zone",
        ),
        "no form",
    ),
    (
        Function(
            "pg_visible_in_snapshot",
            "xid8, pg_snapshot",
            argtypes=("xid8", "pg_snapshot"),
            rettype="boolean",
        ),
        "no form",
    ),
    (Function("pg_xact_status", "xid8", argtypes=("xid8",), rettype="text"), "no form"),
]


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize(("fn", "text"), OWN_FUNCTIONS, ids=lambda v: getattr(v, "signature", v))
def test_an_internal_types_own_function_refused_is_by_design_on_each_engine(fn, text, engine):
    entry = _on(fn, engine, _refusal(fn, text, engine))
    assert entry["outcome"] == "by_design", entry
    assert entry["group"] == support.INTERNAL_TYPE_GROUP
    assert entry["requirement"] == "REQ-2167"
    assert entry["detail"] == support.GROUPS[support.INTERNAL_TYPE_GROUP]


@pytest.mark.parametrize("engine", ENGINES)
@pytest.mark.parametrize(("fn", "text"), GAPS, ids=lambda v: getattr(v, "signature", v))
def test_a_gap_with_the_same_refusal_text_stays_refused_by_name(fn, text, engine):
    entry = _on(fn, engine, _refusal(fn, text, engine))
    assert entry["outcome"] == "refused", entry
    assert report.result_of(entry["outcome"], ()) == report.REFUSED


@pytest.mark.parametrize("engine", ENGINES)
def test_an_internal_types_own_function_the_engine_answers_is_never_moved(engine):
    entry = _on(REGCLASSIN, engine, None)
    assert entry["outcome"] == "pass"


@pytest.mark.parametrize("engine", ENGINES)
def test_an_internal_types_own_function_with_an_engine_error_stays_a_defect(engine):
    raw = {"ok": False, "error": "ERROR:  Code: 46. DB::Exception: Unknown function"}
    assert _on(REGCLASSIN, engine, raw)["outcome"] == "error"


@pytest.mark.parametrize("engine", ENGINES)
def test_per_engine_the_totals_still_add_up(engine):
    entries = [
        *(_on(fn, engine, _refusal(fn, text, engine)) for fn, text in OWN_FUNCTIONS),
        *(_on(fn, engine, _refusal(fn, text, engine)) for fn, text in GAPS),
        _on(REGCLASSIN, engine, None),
    ]
    c = support.counts(entries)
    assert c["total"] == len(entries)
    assert c["passed"] + c["skipped_by_design"] + c["failed"] + c["not_tested"] == c["total"]
    assert sum(c[o] for o in support.OUTCOMES) == c["total"]
    assert (c["by_design"], c["refused"], c["pass"]) == (len(OWN_FUNCTIONS), len(GAPS), 1)
    groups = support.by_design_groups(entries)
    assert len(groups[support.INTERNAL_TYPE_GROUP]) == len(OWN_FUNCTIONS)
    results = report.result_counts(
        support._outcome(e)
        for e in entries  # noqa: SLF001 - the data file's own shape
    )
    assert results["refused_by_name"] == len(GAPS)


def test_the_internal_types_are_named_in_one_place():
    assert set(support.INTERNAL_TYPES) == {
        "regclass",
        "regcollation",
        "regconfig",
        "regdictionary",
        "regnamespace",
        "regoper",
        "regoperator",
        "regproc",
        "regprocedure",
        "regrole",
        "regtype",
        "record",
        "aclitem",
        "xid",
        "xid8",
    }
    for fn, _text in OWN_FUNCTIONS:
        assert support.internal_types_own(fn), fn.signature
    for fn, _text in GAPS:
        assert not support.internal_types_own(fn), fn.signature
