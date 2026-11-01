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

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "conformance"))

import function_support as support  # noqa: E402
import pgwire_catalog_types as catalog  # noqa: E402
from pgwire_function_probe import Function, call  # noqa: E402

ABS = Function("abs", "integer", argtypes=("integer",), volatility="i", rettype="integer")


class Fake:
    """A server answering by statement; a statement it does not know is refused."""

    def __init__(self, answers: dict[str, dict[str, Any]]) -> None:
        self.answers = answers

    def answer(self, sql: str) -> dict[str, Any]:
        return self.answers.get(sql, {"ok": False, "error": "ERROR:  no such function"})


def _ok(*values: Any, oid: int = 23) -> dict[str, Any]:
    return {"ok": True, "fields": [oid], "rows": [(v,) for v in values]}


def _route(_sql: str) -> str:
    return "PASS_THROUGH"


def _both(fn: Function, family: str, want: dict[str, Any], got: dict[str, Any]) -> dict[str, Any]:
    """``fn`` measured where every call of it answers ``want`` and ``got``."""
    sqls = [c.sql for c in support.calls(fn, family)]
    return support.measure_overload(
        fn, family, Fake({s: want for s in sqls}), Fake({s: got for s in sqls}), _route
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
            Function("current_setting", "text", argtypes=("text",), volatility="s"),
            "System",
            "stable, of family System",
        ),
        (
            Function("age", "timestamp", argtypes=("timestamp without time zone",), volatility="s"),
            "Date",
            "answers from the clock",
        ),
        (
            Function("int4in", "cstring", argtypes=("cstring",), volatility="i"),
            "Type operators",
            "no sample of cstring",
        ),
    ],
)
def test_a_function_values_cannot_judge_or_must_not_run_gets_none(fn, family, why):
    assert support.unchecked(fn, family) == why
    assert support.value_calls(fn, family) == []


def test_no_volatile_function_is_ever_given_a_value():
    # A real value given to one runs it for real (pgwire_function_probe.ARGUMENTS).
    lo_unlink = Function("lo_unlink", "oid", argtypes=("oid",), volatility="v")
    assert [c.answers for c in support.calls(lo_unlink, "Large object")] == [False]


def test_the_null_call_of_a_stable_function_compares_its_type_alone():
    now = Function("now", "", volatility="s", rettype="timestamp with time zone")
    assert [(c.values, c.answers) for c in support.calls(now, "Date")] == [(False, False)]


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
    a, b = _ok("1.50", oid=1700), _ok("1.5", oid=1700)
    assert support.difference(a, b, values=True, sql="SELECT (d ->> 0)::numeric") is not None


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


def test_a_pass_with_no_value_call_is_only_called_and_says_why():
    fn = Function("setseed", "double precision", argtypes=("double precision",), volatility="v")
    entry = _both(fn, "Numeric", _ok(None), _ok(None))
    assert entry["outcome"] == "pass"
    assert not entry["answers_checked"]
    assert entry["unchecked"] == "volatile"


def test_a_value_call_that_differs_is_a_wrong_answer_even_when_null_agrees():
    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    want = {sqls[0]: _ok(None), sqls[1]: _ok("3"), sqls[2]: _ok("7"), sqls[3]: _ok("42")}
    got = {**want, sqls[2]: _ok("-7")}
    entry = support.measure_overload(ABS, "Numeric", Fake(want), Fake(got), _route)
    assert entry["outcome"] == "wrong_answer"
    assert entry["detail"] == {"statement": sqls[2], "difference": "'-7', PostgreSQL '7'"}


def test_a_wrong_answer_outranks_an_error():
    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    want = {s: _ok("1") for s in sqls}
    got = {sqls[1]: _ok("2")}
    entry = support.measure_overload(ABS, "Numeric", Fake(want), Fake(got), _route)
    assert entry["outcome"] == "wrong_answer"


def test_an_error_keeps_the_engines_message():
    entry = _both(ABS, "Numeric", _ok("3"), {"ok": False, "error": "ERROR:  Scalar Function"})
    assert entry["outcome"] == "error"
    assert entry["detail"]["error"] == "ERROR:  Scalar Function"


def test_a_value_call_postgresql_refuses_compares_nothing():
    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    want = {sqls[0]: _ok(None), sqls[1]: _ok("3")}
    got = {**want, sqls[2]: _ok("anything")}
    entry = support.measure_overload(ABS, "Numeric", Fake(want), Fake(got), _route)
    assert entry["outcome"] == "pass" and entry["answers_compared"] == 1


def test_every_value_call_refused_leaves_a_pass_only_called():
    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    want = {sqls[0]: _ok(None)}
    entry = support.measure_overload(ABS, "Numeric", Fake(want), Fake(want), _route)
    assert entry["outcome"] == "pass" and not entry["answers_checked"]
    assert entry["unchecked"] == "PostgreSQL refused every value call"


def test_postgresql_refusing_every_call_is_not_callable():
    entry = support.measure_overload(ABS, "Numeric", Fake({}), Fake({}), _route)
    assert entry["outcome"] == "not_callable"
    assert entry["detail"] == "PostgreSQL refuses the call: ERROR:  no such function"


def test_an_overload_with_no_call_is_not_callable_for_that_reason():
    fn = Function("int4_accum", "internal", argtypes=("internal",), volatility="i")
    entry = support.measure_overload(fn, "Aggregate support", Fake({}), Fake({}), _route)
    assert entry["outcome"] == "not_callable"
    assert entry["detail"] == "takes an internal argument, which no SQL expression can supply"


def test_what_the_catalog_db_answers_is_marked_so():
    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    want = {s: _ok("3") for s in sqls}
    entry = support.measure_overload(ABS, "Numeric", Fake(want), Fake(want), lambda _s: "INTERCEPT")
    assert entry["served_by"] == "catalog"


def test_a_hung_statement_costs_its_overload_not_the_leg(monkeypatch):
    class Hung:
        def answer(self, _sql: str) -> dict[str, Any]:
            return {"ok": False, "error": "no answer in 30s"}

    sqls = [c.sql for c in support.calls(ABS, "Numeric")]
    entry = support.measure_overload(
        ABS, "Numeric", Fake({s: _ok("3") for s in sqls}), Hung(), _route
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
CURSORS = Function(
    "pg_cursor", "OUT name text, OUT statement text", volatility="s", rettype="record"
)
_V4 = "dfe9efb2-a1e6-472d-835e-a79e9f1d8463"
_THEN = "2026-10-31 17:20:18.01903+00"


@pytest.mark.parametrize(
    ("fn", "oid", "pg", "provisa"),
    [
        (NOW, 1184, _THEN, "2024-01-02 03:04:05+00"),
        (UUID, 2950, _V4, "00000000-0000-4000-8000-000000000000"),
        (RANDOM, 701, "0.8675929329186349", "1.2345e-05"),
        (RANDOM, 701, "0.5", "0"),
        (PID, 23, "96517", "1"),
        (USER, 19, "postgres", "conformance"),
        (VERSION, 25, "PostgreSQL 17.11 (Debian) on x86_64, 64-bit", "PostgreSQL 17.11 on Provisa"),
    ],
)
def test_an_answer_of_the_form_postgresql_answers_in_is_a_pass_form_checked(fn, oid, pg, provisa):
    entry = _both(fn, "System", _ok(pg, oid=oid), _ok(provisa, oid=oid))
    assert entry["outcome"] == "pass"
    assert entry["form_checked"] and not entry["answers_checked"]
    assert entry["unchecked"]  # the value itself was still not compared


@pytest.mark.parametrize(
    ("fn", "oid", "pg", "provisa"),
    [
        (NOW, 1184, _THEN, "2026-10-31T17:20:18Z"),
        (UUID, 2950, _V4, "dfe9efb2-a1e6-172d-835e-a79e9f1d8463"),  # version 1
        (RANDOM, 701, "0.5", "1.5"),
        (RANDOM, 701, "0.5", "-0.5"),
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


def test_a_function_only_called_with_null_arguments_stays_called_only():
    fn = Function("pg_sleep", "double precision", argtypes=("double precision",), volatility="v")
    entry = _both(fn, "System", _ok("whatever", oid=25), _ok("no form at all", oid=25))
    assert entry["outcome"] == "pass"
    assert not entry["form_checked"] and not entry["answers_checked"]


def test_a_null_from_postgresql_leaves_no_form_to_check():
    entry = _both(NOW, "Date", _ok(None, oid=1184), _ok("not a time", oid=1184))
    assert entry["outcome"] == "pass" and not entry["form_checked"]


def test_a_void_answer_has_no_form():
    fn = Function("pg_stat_clear_snapshot", "", volatility="v", rettype="void")
    entry = _both(fn, "System", _ok("", oid=2278), _ok("", oid=2278))
    assert entry["outcome"] == "pass" and not entry["form_checked"]


def test_null_where_postgresql_answers_a_value_is_a_wrong_answer():
    entry = _both(NOW, "Date", _ok(_THEN, oid=1184), _ok(None, oid=1184))
    assert entry["outcome"] == "wrong_answer"
    assert entry["detail"]["difference"] == f"None, PostgreSQL {_THEN!r}"


def test_the_rows_of_a_set_are_the_servers_own():
    want = {"ok": True, "fields": [2249], "rows": [("(a,b)",), ("(c,d)",)]}
    none = {"ok": True, "fields": [2249], "rows": []}
    entry = _both(CURSORS, "System", want, none)
    assert entry["outcome"] == "pass" and not entry["form_checked"]
    other = {"ok": True, "fields": [2249], "rows": [("(x,y)",), ("(z,w)",), ("(p,q)",)]}
    assert _both(CURSORS, "System", want, other)["form_checked"]
    short = {"ok": True, "fields": [2249], "rows": [("(x,y)",), ("(z)",)]}
    assert _both(CURSORS, "System", want, short)["outcome"] == "wrong_answer"


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
        TIMEOFDAY, "Date", pg, Zoned(_ABBREVIATION.__getitem__), _route
    )
    assert entry["outcome"] == "pass" and entry["form_checked"]
    assert pg.zone == "UTC"


def test_timeofday_writing_the_zone_name_is_a_wrong_answer():
    entry = support.measure_overload(
        TIMEOFDAY, "Date", Zoned(_ABBREVIATION.__getitem__), Zoned(lambda zone: zone), _route
    )
    assert entry["outcome"] == "wrong_answer"
    assert support.ZONE in entry["detail"]["difference"]


def test_timeofday_ignoring_the_sessions_zone_is_a_wrong_answer():
    entry = support.measure_overload(
        TIMEOFDAY, "Date", Zoned(_ABBREVIATION.__getitem__), Zoned(lambda _zone: "UTC"), _route
    )
    assert entry["outcome"] == "wrong_answer"


# --- the merged data file ------------------------------------------------------------------------


def _entry(
    name: str, outcome: str, *, checked: bool = True, form: bool = False, family: str = "Numeric"
) -> dict:
    return {
        "function": name,
        "overload": "integer",
        "family": family,
        "kind": "f",
        "volatility": "i",
        "outcome": outcome,
        "answers_checked": checked,
        "form_checked": form,
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
                _entry("exp", "pass", checked=False),
            ),
            "b": _report(
                "b",
                _entry("abs", "pass"),
                _entry("sqrt", "wrong_answer"),
                _entry("exp", "pass", checked=False),
            ),
        }
    )
    assert doc["guaranteed"]["answers_checked"] == ["abs(integer)"]
    assert doc["guaranteed"]["form_checked"] == []
    assert doc["guaranteed"]["only_called"] == ["exp(integer)"]
    assert doc["guaranteed"]["functions"] == ["abs"]
    by = {f["function"]: f for f in doc["functions"]}
    assert by["sqrt"]["outcomes"]["b"]["outcome"] == "wrong_answer"
    assert by["abs"]["family"] == "Numeric"


def test_the_guaranteed_list_is_answers_checked_and_form_checked_on_every_engine():
    now = _entry("now", "pass", checked=False, form=True)
    doc = support.merged(
        {
            "a": _report("a", _entry("abs", "pass"), now, _entry("exp", "pass", checked=False)),
            "b": _report("b", _entry("abs", "pass"), now, _entry("exp", "pass", checked=False)),
        }
    )
    assert doc["guaranteed"]["answers_checked"] == ["abs(integer)"]
    assert doc["guaranteed"]["form_checked"] == ["now(integer)"]
    assert doc["guaranteed"]["only_called"] == ["exp(integer)"]
    assert doc["guaranteed"]["functions"] == ["abs", "now"]
    assert doc["counts"]["a"]["pass_form_checked"] == 1
    text = support.merged_markdown(doc)
    assert "| a | 3 | 1 | 1 | 1 | 0 | 0 | 0 | 0 |" in text
    assert "| Numeric | 3 | 1 + 1 + 1 of 3 | 1 + 1 + 1 of 3 | 3 |" in text
    assert "**Guaranteed list:** 1 overloads with answers compared and 1 form checked" in text
    assert "1 more only called" in text


def test_an_overload_form_checked_on_one_engine_and_only_called_on_another_is_only_called():
    doc = support.merged(
        {
            "a": _report("a", _entry("now", "pass", checked=False, form=True)),
            "b": _report("b", _entry("now", "pass", checked=False)),
        }
    )
    assert doc["guaranteed"]["form_checked"] == []
    assert doc["guaranteed"]["only_called"] == ["now(integer)"]
    assert doc["guaranteed"]["functions"] == []


def test_one_only_called_overload_keeps_its_function_off_the_list():
    other = {**_entry("abs", "pass", checked=False), "overload": "bigint"}
    doc = support.merged({"e": _report("e", _entry("abs", "pass"), other)})
    assert doc["guaranteed"]["functions"] == []


def test_an_engine_with_no_result_guarantees_nothing():
    doc = support.merged({"a": _report("a", _entry("abs", "pass")), "b": None})
    assert doc["guaranteed"]["answers_checked"] == []
    assert doc["engines_without_a_result"] == ["b"]
    assert doc["engines_incomplete"] == ["b"]
    assert doc["functions"][0]["outcomes"]["b"]["outcome"] == "not_reached"


def test_a_not_callable_overload_does_not_keep_a_function_off_the_list():
    a = {**_entry("abs", "not_callable", checked=False), "overload": "internal"}
    doc = support.merged({"e": _report("e", _entry("abs", "pass"), a)})
    assert doc["guaranteed"]["functions"] == ["abs"]


def test_one_failing_overload_keeps_its_function_off_the_list():
    other = {**_entry("abs", "error"), "overload": "bigint"}
    doc = support.merged({"e": _report("e", _entry("abs", "pass"), other)})
    assert doc["guaranteed"]["answers_checked"] == ["abs(integer)"]
    assert doc["guaranteed"]["functions"] == []


def test_counts_split_a_pass_by_what_was_checked():
    c = support.counts(
        [_entry("a", "pass"), _entry("b", "pass", checked=False), _entry("c", "error")]
    )
    assert (c["pass"], c["pass_answers_checked"], c["pass_only_called"]) == (2, 1, 1)
    assert c["pass_form_checked"] == 0
    assert (c["error"], c["total"]) == (1, 3)


def test_the_summary_gives_every_engine_and_every_family():
    doc = support.merged(
        {
            "a": _report("a", _entry("abs", "pass"), _entry("x", "error", family="String")),
            "b": _report("b", _entry("abs", "pass"), _entry("x", "pass", family="String")),
        }
    )
    text = support.merged_markdown(doc)
    assert "| a | 2 | 1 | 0 | 0 | 0 | 1 | 0 | 0 |" in text
    assert "| b | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |" in text
    assert "| Numeric | 1 | 1 + 0 + 0 of 1 | 1 + 0 + 0 of 1 | 1 |" in text
    assert "| String | 1 | 0 + 0 + 0 of 1 | 1 + 0 + 0 of 1 | 0 |" in text
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


def test_the_workflow_is_dispatched_by_hand_and_is_no_gate():
    doc = _workflow()
    assert doc["name"] == "Conformance (functions)"
    assert set(doc[True]) == {"workflow_dispatch"}  # YAML reads the key `on` as true
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
