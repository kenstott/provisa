# Copyright (c) 2026 Kenneth Stott
# Canary: 0d7a5e2c-9f41-4b38-a6c1-3e8b2f7d5a96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A call's type is read once per typing (REQ-2236, REQ-2167): the five ClickHouse calls the
functions measure listed as engine errors ("no answer in 30s") were the translator typing a range
form's nested calls again for each of the four readings of its literals, a cost exponential in
their nesting (a row of polyfill.clickhouse types each operand it reads). Each now answers
PostgreSQL's value (the embedded PostgreSQL the unit tests boot), its calls typed a bounded number of
times."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

import re

import pytest

from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.pgwire import function_types
from provisa.transpiler.transpile import transpile

#: Each statement and PostgreSQL's answer.
LISTED = [
    ("SELECT pg_catalog.\"int4range\"(NULL::integer, NULL::integer, '[]'::text) AS v", "(,)"),
    ("SELECT pg_catalog.\"int8range\"(NULL::bigint, NULL::bigint, '[]'::text) AS v", "(,)"),
    ('SELECT pg_catalog."range_agg"(NULL::int4multirange) AS v', None),
    ('SELECT pg_catalog."range_agg"(NULL::int4range) AS v', None),
    (
        'SELECT pg_catalog."range_intersect_agg"(c1 ORDER BY c1) AS v FROM '
        "(VALUES ('[1,5)'::int4range), ('empty'::int4range), ('(,3]'::int4range)) AS t(c1)",
        "empty",
    ),
]
#: More readings of calls than any of these statements needs, once each call is typed once.
_MOST_TYPINGS = 200_000


class _TypedTooOften(Exception):
    pass


@pytest.fixture
def counted(monkeypatch):
    calls = [0]
    typed = function_types._call_result_type

    def counting(expression):
        calls[0] += 1
        if calls[0] > _MOST_TYPINGS:
            raise _TypedTooOften
        return typed(expression)

    monkeypatch.setattr(function_types, "_call_result_type", counting)
    return calls


@pytest.mark.parametrize(("sql", "want"), LISTED)
def test_a_nested_call_is_typed_once_per_typing(counted, sql, want):
    transpile(sql, "clickhouse")
    assert counted[0] <= _MOST_TYPINGS


@pytest.mark.parametrize(("sql", "want"), LISTED)
def test_the_listed_engine_errors_answer_postgresql_s_value(sql, want):
    runtime = ClickHouseFederationRuntime.embedded()
    ((got,),) = list(runtime.run_sync(transpile(sql, "clickhouse")).iter_rows())
    assert got == want


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT pg_catalog."range_agg"(c1 ORDER BY c1) AS v FROM (VALUES '
        "('[1,5)'::int4range), ('empty'::int4range), ('(,3]'::int4range)) AS t(c1)",
        'SELECT pg_catalog."range_agg"(NULL::int4range) AS v',
    ],
)
def test_a_range_fold_names_its_item_a_few_times(sql):
    """ClickHouse's analysis of a lambda costs as the square of the times its body names its
    variables: range_agg's fold named its item 973 times (each read of a bound re-read the item's
    text) and took ClickHouse's analyzer longer than the measure's 30 s a call. Each item is now
    read as a range before the fold, a tuple whose parts the step names once each."""
    written = transpile(sql, "clickhouse")
    folds = re.findall(r"arrayFold\(\((_s\d+), (_x\d+)\)", written)
    assert folds
    for _, item in folds:
        assert len(re.findall(rf"\b{item}\b", written)) < 100
