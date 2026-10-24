# Copyright (c) 2026 Kenneth Stott
# Canary: ca37ea72-1aab-46c8-a109-dcc3486b27ee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A GraphQL aggregate root lowered to the graph IR (REQ-2135, REQ-863): printed as Cypher and
read back by the Cypher front end, it answers on DuckDB what the GraphQL compiler's own
statements (the aggregate's and its nodes') answer, as the GraphQL endpoint serializes them."""

# Requirements: REQ-2135, REQ-863

import json

import pytest

from tests.unit import permits_on_duckdb as permits

_ROWS = [
    {"address": "1 Oak", "status": "active", "valuation": 100.0},
    {"address": "2 Oak", "status": "active", "valuation": 250.5},
    {"address": "3 Elm", "status": "expired", "valuation": None},
    {"address": "4 Elm", "status": "active", "valuation": 75.0},
]


@pytest.fixture(scope="module")
def model():
    return permits.model()


def _answer_rows(sql: str, params: list, model) -> list[tuple]:
    """Every column of ``sql``'s answer, a row a tuple."""
    return [tuple(r.values()) for r in permits.answer_rows(sql, params, model, _ROWS)]


@pytest.mark.parametrize(
    "selection",
    [
        "aggregate { count }",
        "aggregate { count sum { valuation } avg { valuation } min { address } max { valuation } }",
        "aggregate { stddev { valuation } }",
        "aggregate { total: sum { v: valuation } }",
        "agg: aggregate { count }",
        "aggregate { count max { valuation } } nodes { address v: valuation }",
    ],
)
@pytest.mark.parametrize("where", ["", '(where: {status: {eq: "active"}})'])
def test_a_lowered_aggregate_root_answers_as_its_sql_does(model, selection, where):
    from graphql import parse

    from provisa.compiler.sql_gen import compile_query
    from provisa.cypher.printer import print_cypher
    from provisa.executor.serialize import serialize_aggregate
    from provisa.graphql_front.lower import lower_read

    ctx, lm, _view_sql = model
    document = parse(f"query {{ c__permitsAggregate{where} {{ {selection} }} }}")
    [own] = compile_query(document, ctx, {})
    # One statement: its nodes, when selected, a column of its one row.
    expected = serialize_aggregate(
        _answer_rows(own.sql, own.params, model), own.columns, own.root_field, own.agg_alias
    )["data"][own.root_field]

    [root] = lower_read(document, ctx, lm, {})
    cypher = print_cypher(root.statement)
    [row] = permits.cypher_answer_rows(cypher, model, _ROWS)
    got = {k: _decoded(v) for k, v in row.items()}
    for answer in (expected, got):
        if "nodes" in answer:
            answer["nodes"] = sorted(answer["nodes"], key=lambda n: json.dumps(n, sort_keys=True))
    assert _numbers(got) == _numbers(expected), cypher


def _decoded(value):
    """A Cypher cell as JSON holds it: a map's values are JSON text."""
    if isinstance(value, dict):
        return {k: json.loads(v) if isinstance(v, str) else v for k, v in value.items()}
    if isinstance(value, list):
        return [_decoded(v) for v in value]
    return value


def _numbers(value):
    """``value`` with every number a float rounded to 9 places: a decimal and a JSON number of
    the same value compare equal."""
    if isinstance(value, dict):
        return {k: _numbers(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_numbers(v) for v in value]
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return round(float(value), 9)
    return value
