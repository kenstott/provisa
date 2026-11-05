# Copyright (c) 2026 Kenneth Stott
# Canary: d368dd54-4356-44d4-9128-219895fa66d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cypher orders null as the largest value (REQ-2135): last ascending, first descending. Answered
on DuckDB, where the statement's null order becomes a sort key of its own (#294), for Cypher as
written and for a GraphQL read lowered to it, whose own SQL orders nulls the same way."""

# Requirements: REQ-2135, REQ-863

import pytest

from tests.unit import permits_on_duckdb as permits

#: Valuations, two of them null, in no order.
_ROWS = [{"valuation": v} for v in (100.0, None, 300.0, None, 200.0)]
_DESC = [None, None, 300.0, 200.0, 100.0]
_ASC = [100.0, 200.0, 300.0, None, None]


@pytest.fixture(scope="module")
def model():
    return permits.model()


@pytest.mark.parametrize(
    ("direction", "limit", "expected"),
    [
        ("DESC", None, _DESC),
        ("ASC", None, _ASC),
        ("DESC", 3, _DESC[:3]),
        ("ASC", 3, _ASC[:3]),
    ],
)
def test_authored_cypher_orders_null_as_the_largest_value(model, direction, limit, expected):
    text = f"MATCH (p:Permits) RETURN p.valuation AS v ORDER BY p.valuation {direction}"
    if limit is not None:
        text += f" LIMIT {limit}"
    assert permits.cypher_answer(text, model, _ROWS) == expected


@pytest.mark.parametrize(
    ("direction", "limit", "expected"),
    [
        ("desc", None, _DESC),
        ("asc", None, _ASC),
        ("desc", 3, _DESC[:3]),
        ("asc", 3, _ASC[:3]),
    ],
)
def test_a_lowered_read_orders_nulls_as_its_graphql_sql_does(model, direction, limit, expected):
    from graphql import parse

    from provisa.compiler.sql_gen import compile_query
    from provisa.cypher.printer import print_cypher
    from provisa.graphql_front.lower import lower_read

    ctx, lm, _view_sql = model
    args = f"order_by: [{{valuation: {direction}}}]" + (f", limit: {limit}" if limit else "")
    document = parse(f"query {{ c__permits({args}) {{ valuation }} }}")
    [own] = compile_query(document, ctx, {})
    assert permits.answer(own.sql, own.params, model, _ROWS) == expected
    [root] = lower_read(document, ctx, lm, {})
    assert permits.cypher_answer(print_cypher(root.statement), model, _ROWS) == expected
