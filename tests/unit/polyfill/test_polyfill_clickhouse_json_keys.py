# Copyright (c) 2026 Kenneth Stott
# Canary: 979bc3ef-f31b-4556-a465-c72ebce07c05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""ClickHouse's rows of a json or jsonb value's keys (REQ-2236; REQ-2171): jsonb_exists,
jsonb_exists_any, jsonb_exists_all, jsonb_object_keys and json_object_keys, each refused by name on
the base, answered on the embedded engine (chdb) against the embedded PostgreSQL 16.11's answer:
the measure's own calls of each overload and the edges the rows read."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import pytest

from provisa.core.pg_error import from_engine
from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.transpile import transpile

ANSWERED = [
    ('SELECT pg_catalog."jsonb_exists"(NULL::jsonb, NULL::text) AS v', None),
    ('SELECT pg_catalog."jsonb_exists"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'abc\'::text) AS v', "f"),
    (
        "SELECT pg_catalog.\"jsonb_exists\"('[1, \"two\", null]'::jsonb, 'Hello World'::text) AS v",
        "f",
    ),
    ("SELECT pg_catalog.\"jsonb_exists\"('3'::jsonb, '12'::text) AS v", "f"),
    ("SELECT pg_catalog.\"jsonb_exists\"(NULL::jsonb, 'abc'::text) AS v", None),
    ('SELECT pg_catalog."jsonb_exists"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::text) AS v', None),
    ("SELECT pg_catalog.\"jsonb_exists\"('{}'::jsonb, 'abc'::text) AS v", "f"),
    ('SELECT pg_catalog."jsonb_exists"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'\'::text) AS v', "f"),
    ("SELECT pg_catalog.\"jsonb_exists\"('[]'::jsonb, 'abc'::text) AS v", "f"),
    (
        'SELECT pg_catalog."jsonb_exists"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'日本語テキスト😀\'::text) AS v',
        "f",
    ),
    ("SELECT pg_catalog.\"jsonb_exists\"('\"日本語😀\"'::jsonb, 'abc'::text) AS v", "f"),
    ('SELECT pg_catalog."jsonb_exists"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'été\'::text) AS v', "f"),
    ('SELECT pg_catalog."jsonb_exists_any"(NULL::jsonb, NULL::text[]) AS v', None),
    (
        'SELECT pg_catalog."jsonb_exists_any"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[]) AS v',
        "t",
    ),
    ("SELECT pg_catalog.\"jsonb_exists_any\"('[1, \"two\", null]'::jsonb, '{}'::text[]) AS v", "f"),
    ("SELECT pg_catalog.\"jsonb_exists_any\"('3'::jsonb, '{x,NULL,z}'::text[]) AS v", "f"),
    ("SELECT pg_catalog.\"jsonb_exists_any\"(NULL::jsonb, '{a,b,c}'::text[]) AS v", None),
    (
        'SELECT pg_catalog."jsonb_exists_any"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::text[]) AS v',
        None,
    ),
    ("SELECT pg_catalog.\"jsonb_exists_any\"('{}'::jsonb, '{a,b,c}'::text[]) AS v", "f"),
    (
        'SELECT pg_catalog."jsonb_exists_any"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"",日本語😀,é}\'::text[]) AS v',
        "f",
    ),
    ("SELECT pg_catalog.\"jsonb_exists_any\"('[]'::jsonb, '{a,b,c}'::text[]) AS v", "f"),
    (
        'SELECT pg_catalog."jsonb_exists_any"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{NULL}\'::text[]) AS v',
        "f",
    ),
    ("SELECT pg_catalog.\"jsonb_exists_any\"('\"日本語😀\"'::jsonb, '{a,b,c}'::text[]) AS v", "f"),
    (
        'SELECT pg_catalog."jsonb_exists_any"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{{a,b},{c,d}}\'::text[]) AS v',
        "t",
    ),
    ('SELECT pg_catalog."jsonb_exists_all"(NULL::jsonb, NULL::text[]) AS v', None),
    (
        'SELECT pg_catalog."jsonb_exists_all"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{a,b,c}\'::text[]) AS v',
        "f",
    ),
    ("SELECT pg_catalog.\"jsonb_exists_all\"('[1, \"two\", null]'::jsonb, '{}'::text[]) AS v", "t"),
    ("SELECT pg_catalog.\"jsonb_exists_all\"('3'::jsonb, '{x,NULL,z}'::text[]) AS v", "f"),
    ("SELECT pg_catalog.\"jsonb_exists_all\"(NULL::jsonb, '{a,b,c}'::text[]) AS v", None),
    (
        'SELECT pg_catalog."jsonb_exists_all"(\'{"a": 1, "b": [1, 2]}\'::jsonb, NULL::text[]) AS v',
        None,
    ),
    ("SELECT pg_catalog.\"jsonb_exists_all\"('{}'::jsonb, '{a,b,c}'::text[]) AS v", "f"),
    (
        'SELECT pg_catalog."jsonb_exists_all"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{"",日本語😀,é}\'::text[]) AS v',
        "f",
    ),
    ("SELECT pg_catalog.\"jsonb_exists_all\"('[]'::jsonb, '{a,b,c}'::text[]) AS v", "f"),
    (
        'SELECT pg_catalog."jsonb_exists_all"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{NULL}\'::text[]) AS v',
        "t",
    ),
    ("SELECT pg_catalog.\"jsonb_exists_all\"('\"日本語😀\"'::jsonb, '{a,b,c}'::text[]) AS v", "f"),
    (
        'SELECT pg_catalog."jsonb_exists_all"(\'{"a": 1, "b": [1, 2]}\'::jsonb, \'{{a,b},{c,d}}\'::text[]) AS v',
        "f",
    ),
    ("SELECT jsonb_exists('{\"a\\\"b\":1}'::jsonb, 'a\"b')", "t"),
    ("SELECT jsonb_exists('{\"é\":1}'::jsonb, 'é')", "t"),
    ('SELECT jsonb_exists(\'  ["x", {"y":1}, "y"]\'::jsonb, \'y\')', "t"),
    ("SELECT jsonb_exists('\"x\"'::jsonb, 'x')", "t"),
    ("SELECT jsonb_exists('{\"a\":{\"b\":1}}'::jsonb, 'b')", "f"),
    ("SELECT jsonb_exists_any('{\"a\":1}'::jsonb, ARRAY['x','a'])", "t"),
    ("SELECT jsonb_exists_all('[\"a\",\"b\"]'::jsonb, ARRAY['a','b'])", "t"),
]
# A text[] written as a literal of unknown type, which PostgreSQL reads as the parameter's text[]
# (PostgreSQL 17's answers, tests/unit/pgwire/pg17_json_answers.py).
UNTYPED = [
    ("SELECT jsonb_exists_all('{\"a\":1}', '{a,b}')", "f"),
    ("SELECT jsonb_exists_any('{\"a\":1}', '{a,b}')", "t"),
    ("SELECT jsonb_exists_all('{\"a\":1}', '{\"a\",NULL}')", "t"),
    ("SELECT jsonb_exists_any('{\"a\": 1, \"b\": [1, 2]}', '{{a,b},{c,d}}')", "t"),
    ("SELECT jsonb_exists_all('{\"a\": 1, \"b\": [1, 2]}', '{{a,b},{c,d}}')", "f"),
    ("SELECT jsonb_exists_any('{\"a\":1}', '{}')", "f"),
    ("SELECT jsonb_exists_all('{\"a\":1}', '{}')", "t"),
]
RAISED = [
    ('SELECT pg_catalog."jsonb_object_keys"(\'[1, "two", null]\'::jsonb) AS v', "22023"),
    ("SELECT pg_catalog.\"jsonb_object_keys\"('3'::jsonb) AS v", "22023"),
    ("SELECT pg_catalog.\"jsonb_object_keys\"('[]'::jsonb) AS v", "22023"),
    ('SELECT pg_catalog."jsonb_object_keys"(\'"日本語😀"\'::jsonb) AS v', "22023"),
    (
        "SELECT pg_catalog.\"jsonb_object_keys\"('12345678901234567890123456789'::jsonb) AS v",
        "22023",
    ),
    ('SELECT pg_catalog."json_object_keys"(\'[1, "two", null]\'::json) AS v', "22023"),
    ("SELECT pg_catalog.\"json_object_keys\"('3'::json) AS v", "22023"),
    ("SELECT pg_catalog.\"json_object_keys\"('[]'::json) AS v", "22023"),
    ('SELECT pg_catalog."json_object_keys"(\'"日本語😀"\'::json) AS v', "22023"),
    ("SELECT pg_catalog.\"json_object_keys\"('12345678901234567890123456789'::json) AS v", "22023"),
]


@pytest.fixture(scope="module")
def runtime() -> ClickHouseFederationRuntime:
    return ClickHouseFederationRuntime.embedded()


def _answers(runtime: ClickHouseFederationRuntime, sql: str) -> list[object]:
    return [row[0] for row in runtime.run_sync(transpile(sql, "clickhouse")).iter_rows()]


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "t" if value else "f"
    return str(value)


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_clickhouse_answers_postgresql_s(runtime, sql, want):
    assert [_text(v) for v in _answers(runtime, sql)] == [want]


@pytest.mark.parametrize(("sql", "want"), UNTYPED)
def test_clickhouse_reads_an_untyped_array_literal_as_text_array(runtime, sql, want):
    assert [_text(v) for v in _answers(runtime, sql)] == [want]


@pytest.mark.parametrize("name", ["jsonb_exists_any", "jsonb_exists_all"])
def test_an_untyped_literal_not_an_array_is_refused_by_name(name):
    with pytest.raises(FunctionRefused):
        transpile(f"SELECT {name}('{{\"a\":1}}', 'abc')", "clickhouse")


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_clickhouse_raises_postgresql_s_error(runtime, sql, state):
    with pytest.raises(Exception) as raised:
        _answers(runtime, sql)
    if isinstance(raised.value, RowRefused):
        assert raised.value.sqlstate == state
        return
    error = from_engine(raised.value)
    assert error is not None and error.sqlstate == state, str(raised.value)[:300]


@pytest.mark.parametrize(
    ("sql", "want"),
    [
        (
            'SELECT jsonb_object_keys(\'{"bb":1, "a":2, "bb":3, "ccc":0, "é":1, "z":1}\'::jsonb)',
            ["a", "z", "bb", "é", "ccc"],
        ),
        ('SELECT json_object_keys(\'{"bb":1, "a":2, "bb":3}\'::json)', ["bb", "a", "bb"]),
        ('SELECT pg_catalog."jsonb_object_keys"(\'{"a": 1, "b": [1, 2]}\'::jsonb)', ["a", "b"]),
        ('SELECT pg_catalog."jsonb_object_keys"(NULL::jsonb)', []),
        ("SELECT pg_catalog.\"jsonb_object_keys\"('{}'::jsonb)", []),
    ],
)
def test_object_keys_answers_each_key_in_postgresql_s_order(runtime, sql, want):
    assert _answers(runtime, sql) == want


def test_trino_keeps_its_refusal():
    with pytest.raises(Exception):
        transpile("SELECT jsonb_exists('{\"a\":1}'::jsonb, 'a')", "trino")
