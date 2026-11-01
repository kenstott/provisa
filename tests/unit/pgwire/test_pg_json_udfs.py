# Copyright (c) 2026 Kenneth Stott
# Canary: d2b7094e-5f1a-4c36-a8e9-3c60f7e1b582
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The DuckDB UDFs of PostgreSQL's json and jsonb functions (REQ-2167, REQ-2171): each function of
the conformance probe's calls reaches an answer, and the answers the PostgreSQL 17 documentation
prints (functions-json, tables 9.46 to 9.51). tests/integration/test_json_udfs_against_postgres.py
compares them to a live server, and pg17_json_answers to a recording."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import duckdb
import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.duckdb_pg_functions import register
from provisa.federation.duckdb_pg_json import exists_all, exists_any
from provisa.transpiler.function_macros import FunctionRefused
from provisa.transpiler.transpile import transpile_to_duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts" / "conformance"))

from pgwire_function_probe import ROW_TYPE  # noqa: E402

_NOT_NAMED = {
    "array_to_json(NULL::integer[], NULL::boolean)": None,
    "row_to_json(NULL::record, NULL::boolean)": None,
}
_J, _B, _P = "NULL::json", "NULL::jsonb", "NULL::jsonpath"
_T, _TA = "NULL::text", "NULL::text[]"
_FLAG = "NULL::boolean"
_PATH_CALL = f"{_B}, {_P}, {_B}, {_FLAG}"
#: The call the conformance probe makes of each function that had no form.
_PROBED = (
    *_NOT_NAMED,
    "json_agg_strict(NULL::integer)",
    "jsonb_agg_strict(NULL::integer)",
    f"json_array_elements({_J})",
    f"json_array_elements_text({_J})",
    f"jsonb_array_elements({_B})",
    f"jsonb_array_elements_text({_B})",
    "json_build_array()",
    "json_build_array(VARIADIC NULL::text[])",
    f"json_each_text({_J})",
    f"jsonb_each({_B})",
    f"jsonb_each_text({_B})",
    f"json_extract_path({_J}, VARIADIC {_TA})",
    f"json_extract_path_text({_J}, VARIADIC {_TA})",
    f"jsonb_extract_path({_B}, VARIADIC {_TA})",
    f"jsonb_extract_path_text({_B}, VARIADIC {_TA})",
    *(
        f"{kind}_object_agg{suffix}('k'::text, NULL::integer)"
        for kind in ("json", "jsonb")
        for suffix in ("_strict", "_unique", "_unique_strict")
    ),
    f"json_object_keys({_J})",
    f"jsonb_object_keys({_B})",
    f"json_populate_record(NULL::{ROW_TYPE}, {_J}, {_FLAG})",
    f"json_populate_recordset(NULL::{ROW_TYPE}, {_J}, {_FLAG})",
    f"jsonb_populate_record(NULL::{ROW_TYPE}, {_B})",
    f"jsonb_populate_record_valid(NULL::{ROW_TYPE}, {_B})",
    f"jsonb_populate_recordset(NULL::{ROW_TYPE}, {_B})",
    f"json_strip_nulls({_J})",
    f"jsonb_strip_nulls({_B})",
    f"json_to_record({_J})",
    f"jsonb_to_record({_B})",
    f"jsonb_cmp({_B}, {_B})",
    f"jsonb_concat({_B}, {_B})",
    f"jsonb_contained({_B}, {_B})",
    f"jsonb_contains({_B}, {_B})",
    f"jsonb_delete({_B}, VARIADIC {_TA})",
    f"jsonb_delete({_B}, NULL::integer)",
    f"jsonb_delete({_B}, {_T})",
    f"jsonb_delete_path({_B}, {_TA})",
    *(f"jsonb_{name}({_B}, {_B})" for name in ("eq", "ne", "lt", "le", "gt", "ge")),
    f"jsonb_exists({_B}, {_T})",
    f"jsonb_exists_all({_B}, {_TA})",
    f"jsonb_exists_any({_B}, {_TA})",
    f"jsonb_hash({_B})",
    f"jsonb_hash_extended({_B}, NULL::bigint)",
    f"jsonb_insert({_B}, {_TA}, {_B}, {_FLAG})",
    f"jsonb_object({_TA})",
    f"jsonb_object({_TA}, {_TA})",
    *(
        f"jsonb_path_{name}{suffix}({_PATH_CALL})"
        for name in ("exists", "match", "query", "query_array", "query_first")
        for suffix in ("", "_tz")
    ),
    f"jsonb_path_exists_opr({_B}, {_P})",
    f"jsonb_path_match_opr({_B}, {_P})",
    f"jsonb_pretty({_B})",
    f"jsonb_set({_B}, {_TA}, {_B}, {_FLAG})",
    f"jsonb_set_lax({_B}, {_TA}, {_B}, {_FLAG}, {_T})",
)


@pytest.fixture(scope="module")
def db() -> Any:
    connection = duckdb.connect()
    register(connection)
    return connection


def _one(db: Any, call: str) -> Any:
    return db.execute(transpile_to_duckdb(f"SELECT {call}")).fetchall()[0][0]


@pytest.mark.parametrize("call", _PROBED)
def test_each_probed_function_reaches_an_answer(db: Any, call: str):
    written = transpile_to_duckdb(f"SELECT pg_catalog.{call} AS v")
    db.execute(written).fetchall()


@pytest.mark.parametrize(
    ("aggregate", "want"),
    [
        ("json_agg(x)", [1, 3]),
        ("jsonb_agg(x)", [1, 3]),
        ("json_agg(x ORDER BY x DESC)", [3, 1]),
        ("jsonb_agg(x ORDER BY x DESC)", [3, 1]),
        ("json_agg_strict(y)", [1]),
        ("jsonb_agg_strict(y)", [1]),
        ("json_object_agg(k, x)", {"a": 1, "c": 3}),
        ("jsonb_object_agg(k, x)", {"a": 1, "c": 3}),
        ("json_object_agg_strict(k, y)", {"a": 1}),
        ("json_object_agg_unique(k, x)", {"a": 1, "c": 3}),
    ],
)
def test_a_filter_keeps_out_of_a_json_aggregate_what_it_excludes(
    db: Any, aggregate: str, want: Any
):
    import json

    rows = "(VALUES ('a', 1, 1), ('b', 2, 2), ('c', 3, NULL)) t(k, x, y)"
    written = transpile_to_duckdb(f"SELECT {aggregate} FILTER (WHERE x <> 2) FROM {rows}")
    assert json.loads(db.execute(written).fetchall()[0][0]) == want


_WHEN = "TIMESTAMP '1996-07-01 09:00:00'"
_SPELLED = "1996-07-01T09:00:00"


@pytest.mark.parametrize(
    ("value", "reads"),
    [
        ("to_json(t)", lambda v: v),
        ("json_build_object('k', t)", lambda v: v["k"]),
        ("jsonb_build_object('k', t)", lambda v: v["k"]),
        ("json_object(KEY 'k' VALUE t)", lambda v: v["k"]),
        ("json_build_array(t)", lambda v: v[0]),
        ("json_agg(t)", lambda v: v[0]),
        ("jsonb_agg(t)", lambda v: v[0]),
        ("json_agg(t ORDER BY t)", lambda v: v[0]),
        ("json_agg(DISTINCT t)", lambda v: v[0]),
        ("json_agg(t) FILTER (WHERE t IS NOT NULL)", lambda v: v[0]),
        ("json_agg_strict(t)", lambda v: v[0]),
        ("json_object_agg('k', t)", lambda v: v["k"]),
        ("json_object_agg_strict('k', t)", lambda v: v["k"]),
        (
            "json_agg(json_object(KEY 'a' VALUE json_build_object('k', t)))",
            lambda v: v[0]["a"]["k"],
        ),
    ],
)
def test_a_timestamp_in_json_is_written_alike_by_each_constructor(db: Any, value: str, reads: Any):
    import json

    written = transpile_to_duckdb(f"SELECT {value} FROM (SELECT {_WHEN} AS t) AS s")
    assert reads(json.loads(db.execute(written).fetchall()[0][0])) == _SPELLED


@pytest.mark.parametrize(
    "literal",
    [
        "TIMESTAMP '1996-07-01 09:00:00'",
        "TIMESTAMP '1996-07-01 09:00:00.5'",
        "TIMESTAMP '1996-07-01 09:00:00.123456'",
        "TIMESTAMPTZ '1996-07-01 09:00:00+00'",
        "TIMESTAMPTZ '1996-07-01 09:00:00.25+00'",
    ],
)
def test_a_timestamp_answers_the_same_at_any_depth(db: Any, literal: str):
    import json

    from provisa.executor.serialize import _convert_value

    nested = transpile_to_duckdb(
        f"SELECT json_build_object('k', t), t FROM (SELECT {literal} AS t) s"
    )
    text, top = db.execute(nested).fetchall()[0]
    assert json.loads(text)["k"] == _convert_value(top)


def test_a_json_value_keeps_its_type_beside_a_timestamp(db: Any):
    import json

    written = transpile_to_duckdb(
        "SELECT json_build_object('n', 1.50, 's', s, 'j', '{\"a\": 1}'::json, 'x', NULL, 'b', true)"
        " FROM (SELECT '{\"a\": 1}' AS s) AS v"
    )
    assert json.loads(db.execute(written).fetchall()[0][0]) == {
        "n": 1.5,
        "s": '{"a": 1}',
        "j": {"a": 1},
        "x": None,
        "b": True,
    }


def test_nesting_does_not_multiply_the_statement():
    def nested(depth: int) -> str:
        inner = "json_build_object('k', c)"
        for _ in range(depth):
            inner = f"json_build_object('k', c, 'n', (SELECT json_agg({inner}) FROM t))"
        return transpile_to_duckdb(f"SELECT {inner} FROM t")

    one, two, three = len(nested(1)), len(nested(2)), len(nested(3))
    assert three - two == two - one


@pytest.mark.parametrize("table", ("json_to_recordset", "jsonb_to_recordset"))
def test_a_definition_list_gives_the_columns_of_no_rows(db: Any, table: str):
    written = transpile_to_duckdb(f"""SELECT * FROM pg_catalog."{table}"('[]') AS t(v integer)""")
    assert db.execute(written).fetchall() == []


@pytest.mark.parametrize(
    ("call", "want"),
    [
        ("""jsonb_path_query_array('[1,2,3,4,5]', '$[*] ? (@ > 2)')""", [3, 4, 5]),
        ("""jsonb_path_query_first('[1,2,3,4,5]', '$[*] ? (@ > 2)')""", 3),
        ("""jsonb_path_exists('{"a":[1,2,3,4,5]}', '$.a[*] ? (@ >= 2 && @ <= 4)')""", True),
        ("""jsonb_path_match('{"a":1}', '$.a == 1')""", True),
        ("""jsonb_path_query_array('[1,2]', '$[*] + 1')""", "error"),
        ("""jsonb_path_query_array('{"a":[1,2]}', '$.a[last]')""", [2]),
        ("""jsonb_path_query_array('{"x":[2,3]}', '$.x[*] * 2')""", "error"),
        ("""jsonb_path_query_first('{"h":{"a":1}}', '$.h.*.type()')""", "number"),
        ("""jsonb_path_query_first('[1,2]', '$.size()')""", 2),
        ("""jsonb_path_query_first('1.5', '$.floor()')""", 1),
        ("""jsonb_path_query_first('1.5', '$.ceiling()')""", 2),
        ("""jsonb_path_query_first('-1.5', '$.abs()')""", 1.5),
        ("""jsonb_path_query_first('"1.5"', '$.double()')""", 1.5),
        ("""jsonb_path_query_first('12', '$.string()')""", "12"),
        ("""jsonb_path_query_first('"42"', '$.integer()')""", 42),
        ("""jsonb_path_query_first('"42"', '$.bigint()')""", 42),
        ("""jsonb_path_query_first('"yes"', '$.boolean()')""", True),
        ("""jsonb_path_query_first('1.234', '$.decimal(3, 1)')""", 1.2),
        ("""jsonb_path_query_first('"1.5"', '$.number()')""", 1.5),
    ],
)
def test_jsonpath_answers_as_the_documentation_prints(db: Any, call: str, want: Any):
    import json

    if want == "error":
        with pytest.raises(duckdb.Error):
            _one(db, call)
        return
    answer = _one(db, call)
    assert (json.loads(answer) if isinstance(answer, str) else answer) == want


@pytest.mark.parametrize(
    ("call", "function"),
    [
        ("""jsonb_path_query_first('"2024-01-02"', '$.datetime()')""", "jsonpath_datetime"),
        ("""jsonb_path_query_first('"2024-01-02"', '$.date()')""", "jsonpath_date"),
        ("""jsonb_path_query_first('{"a":{"b":1}}', '$.a.keyvalue()')""", "jsonpath_keyvalue"),
    ],
)
def test_what_a_path_cannot_answer_is_refused_by_name(db: Any, call: str, function: str):
    with pytest.raises(duckdb.Error, match=f"{function}\\(\\) has no form on duckdb"):
        _one(db, call)


@pytest.mark.parametrize(
    "sql",
    [
        f"""SELECT jsonb_populate_record(NULL::{ROW_TYPE}, '{{"a":1}}')""",
        f"""SELECT * FROM jsonb_populate_record(NULL::{ROW_TYPE}, '{{"a":1}}')""",
        "SELECT * FROM jsonb_to_record('{}')",
    ],
)
def test_a_row_the_engine_has_no_columns_of_is_refused_by_name(sql: str):
    with pytest.raises(FunctionRefused):
        transpile_to_duckdb(sql)


def test_a_record_base_with_a_definition_list_is_answered(db: Any):
    written = transpile_to_duckdb(
        """SELECT * FROM jsonb_populate_record(NULL::record, '{"a":1}') AS t(a int, b text)"""
    )
    assert db.execute(written).fetchall() == [(1, None)]


def test_a_fraction_for_an_integer_column_is_postgresqls_error_not_rounded(db: Any):
    written = transpile_to_duckdb("""SELECT * FROM json_to_record('{"a":1.5}') AS t(a int)""")
    with pytest.raises(duckdb.Error) as refused:
        db.execute(written).fetchall()
    error = from_engine(refused.value)
    assert error is not None and error.sqlstate == "22P02"


def test_a_column_is_read_by_the_udfs_row_by_row(db: Any):
    written = transpile_to_duckdb(
        """SELECT jsonb_set(d, '{a}', '9') FROM (VALUES ('{"a":1}'::jsonb), ('{"b":2}'::jsonb)) t(d)"""
    )
    assert {str(row[0]).replace(" ", "") for row in db.execute(written).fetchall()} == {
        '{"a":9}',
        '{"a":9,"b":2}',
    }


@pytest.mark.parametrize(
    ("call", "want"),
    [
        ("""jsonb_concat('{}', '[1]')""", "[{}, 1]"),
        ("""jsonb_concat('[]', '{"a":1}')""", '[{"a": 1}]'),
        ("""jsonb_concat('[]', '[1]')""", "[1]"),
        ("""jsonb_concat('{}', '{"a":1}')""", '{"a":1}'),
    ],
)
def test_an_empty_value_is_dropped_only_beside_one_of_its_kind(db: Any, call: str, want: str):
    assert _one(db, call) == want


def test_a_null_key_is_skipped():
    assert exists_all('{"a":1}', ["a", None]) is True
    assert exists_any('{"a":1}', [None]) is False


def test_a_deeply_nested_call_is_rewritten_once():
    """25 levels: a rewrite of each call's arguments twice is 2**25, and sqlglot's parser stops
    near 40."""
    nested = "x"
    for _ in range(25):
        nested = f"abs({nested})"
    written = transpile_to_duckdb(f"SELECT jsonb_typeof(a), {nested} FROM t")
    assert written.upper().count("ABS(") == 25
