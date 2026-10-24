# Copyright (c) 2026 Kenneth Stott
# Canary: 382be3f3-5db0-47a7-bd0c-63c6dd7121f2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the benchmark-features e2e calls a correct answer (REQ-1911), checked without a stack:
every returned row is one of the expected rows, as many as the limit allows, with the requested
columns; a masked column null; a join's every pair joins, flat or nested. And each bench client's
``rows()`` reads its route's answer into column names and one dict per row."""

# Requirements: REQ-1911

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import contract_model as cm  # noqa: E402
import optimistic_load as ol  # noqa: E402
from queries import Query  # noqa: E402
from request_mix import RequestSpec  # noqa: E402

from tests.integration import test_bench_features_e2e as features  # noqa: E402


def _col(name: str, **spelled: str) -> cm.Column:
    names = {"sql": name, "graphql": spelled.get("graphql", name), "grpc": name, "rest": name}
    return cm.Column(name, None, names)


def _table(name: str, base: str, *cols: cm.Column) -> cm.Table:
    return cm.Table(
        name=name,
        schema="public",
        weight=1.0,
        base_column=base,
        sql=f"perf_bench.{name}",
        cypher_label=None,
        cypher_var=None,
        graphql_field=name,
        grpc_type_name=None,
        rest_path=None,
        jsonapi_path=None,
        jsonapi_type=None,
        columns=cols,
    )


ORDERS = _table(
    "orders",
    "order_id",
    _col("order_id", graphql="orderId"),
    _col("status"),
    _col("amount"),
)
ITEMS = _table("order_items", "item_id", _col("item_id", graphql="itemId"), _col("order_id"))


class _Oracle:
    tables = {
        ("pg", "orders"): [
            {"order_id": 1, "status": "paid", "amount": "10.50"},
            {"order_id": 2, "status": "paid", "amount": "3.00"},
            {"order_id": 3, "status": "new", "amount": "7.25"},
        ],
        ("pg", "order_items"): [
            {"item_id": 11, "order_id": 1},
            {"item_id": 12, "order_id": 1},
            {"item_id": 31, "order_id": 3},
        ],
    }

    def rows(self, source: str, table: str) -> list[dict[str, Any]]:
        return self.tables[(source, table)]


ORACLE: Any = _Oracle()


def _spec(cols=("order_id", "status"), rows=10, filters=(), joins=()) -> RequestSpec:
    return RequestSpec("pg", "orders", tuple(cols), rows, False, tuple(filters), tuple(joins))


def test_a_subset_of_the_matching_rows_as_many_as_the_limit_is_correct():
    spec = _spec(rows=2, filters=[("status", "paid")])
    rows = [{"order_id": 2, "status": "paid"}, {"order_id": "1", "status": "paid"}]
    features.check_single("data_sql", ORDERS, spec, ["order_id", "status"], rows, ORACLE)


def test_a_row_the_source_does_not_hold_is_refused():
    spec = _spec(rows=1, filters=[("status", "paid")])
    with pytest.raises(AssertionError, match="rows not in"):
        features.check_single(
            "data_sql",
            ORDERS,
            spec,
            ["order_id", "status"],
            [{"order_id": 3, "status": "paid"}],
            ORACLE,
        )


def test_fewer_rows_than_the_limit_allows_is_refused():
    spec = _spec(rows=3)
    with pytest.raises(AssertionError):
        features.check_single(
            "data_sql",
            ORDERS,
            spec,
            ["order_id", "status"],
            [{"order_id": 1, "status": "paid"}],
            ORACLE,
        )


def test_columns_are_matched_by_any_transports_spelling_and_grpc_may_answer_more():
    spec = _spec(cols=("order_id",), rows=3)
    rows = [{"orderId": i} for i in (1, 2, 3)]
    features.check_single("graphql", ORDERS, spec, ["orderId"], rows, ORACLE)
    with pytest.raises(AssertionError):  # an unasked column, outside gRPC
        features.check_single(
            "graphql",
            ORDERS,
            spec,
            ["orderId", "status"],
            [{**r, "status": "x"} for r in rows],
            ORACLE,
        )
    grpc_rows = [{"order_id": str(i), "status": "x", "amount": "1"} for i in (1, 2, 3)]
    features.check_single("grpc", ORDERS, spec, ["order_id", "status", "amount"], grpc_rows, ORACLE)


def test_a_masked_column_is_expected_null():
    spec = _spec(cols=("order_id", "amount"), rows=3)
    rows = [{"order_id": i, "amount": None} for i in (1, 2, 3)]
    features.check_single(
        "data_sql", ORDERS, spec, ["order_id", "amount"], rows, ORACLE, {"amount"}
    )
    with pytest.raises(AssertionError):
        features.check_single(
            "data_sql",
            ORDERS,
            spec,
            ["order_id", "amount"],
            [{"order_id": 1, "amount": "10.50"}],
            ORACLE,
            {"amount"},
        )


_JOIN = cm.JoinStep(
    parent=0,
    kind="same",
    child_source="pg",
    child_table="order_items",
    parent_column="order_id",
    child_column="order_id",
    forward=True,
    graphql_field="items",
    cypher_rel="HAS_ITEM",
)
SETUP: Any = SimpleNamespace(sources={"pg": SimpleNamespace(tables=(ORDERS, ITEMS))})


def test_a_flat_join_answer_pairs_that_join():
    spec = _spec(cols=("order_id",), joins=[_JOIN])
    good = [{"order_id": 1, "order_items_item_id": 11}, {"order_id": 3, "order_items_item_id": 31}]
    features.check_join("data_sql", SETUP, spec, good, ORACLE)
    with pytest.raises(AssertionError, match="do not join"):
        features.check_join(
            "data_sql", SETUP, spec, [{"order_id": 3, "order_items_item_id": 11}], ORACLE
        )


def test_a_nested_graphql_join_answer_pairs_that_join():
    spec = _spec(cols=("order_id",), joins=[_JOIN])
    rows = [{"orderId": 1, "items": [{"itemId": 11}, {"itemId": 12}]}, {"orderId": 2, "items": []}]
    features.check_join("graphql", SETUP, spec, rows, ORACLE)
    with pytest.raises(AssertionError, match="do not join"):
        features.check_join(
            "graphql", SETUP, spec, [{"orderId": 2, "items": [{"itemId": 31}]}], ORACLE
        )


# ---- each client's rows()


def _client(cls: Any, handler: Any) -> Any:
    client = cls.__new__(cls)
    client._cred = None
    client._http = httpx.Client(base_url="http://bench", transport=httpx.MockTransport(handler))
    return client


QUERY = Query(
    id="q",
    category="c",
    description="d",
    sql="SELECT order_id FROM orders LIMIT 2",
    cypher="MATCH (o:Order) RETURN o.order_id AS order_id LIMIT 2",
    graphql="{ orders(limit: 2) { orderId } }",
    rest={"path": "/data/rest/perf-bench/orders", "params": {}, "headers": {}},
    jsonapi={"path": "/data/jsonapi/perf-bench/orders", "params": {}, "headers": {}},
)


@pytest.mark.parametrize(
    ("cls", "body", "columns"),
    [
        (
            ol.DataSqlClient,
            {
                "data": {"sql": [{"order_id": 1}, {"order_id": 2}]},
                "columns": [{"name": "order_id"}],
            },
            ["order_id"],
        ),
        (ol.GraphqlClient, {"data": {"orders": [{"orderId": 1}, {"orderId": 2}]}}, ["orderId"]),
        (
            ol.CypherHttpClient,
            {"rows": [{"order_id": 1}, {"order_id": 2}], "columns": ["order_id"]},
            ["order_id"],
        ),
        (ol.RestClient, {"data": [{"order_id": 1}, {"order_id": 2}]}, ["order_id"]),
        (
            ol.JsonApiClient,
            {
                "data": [
                    {"id": "1", "attributes": {"order_id": 1}},
                    {"id": "2", "attributes": {"order_id": 2}},
                ]
            },
            ["order_id"],
        ),
    ],
    ids=["data_sql", "graphql", "cypher_http", "rest", "jsonapi"],
)
def test_rows_reads_each_http_routes_answer(cls, body, columns):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body, headers={"x-provisa-cache": "HIT"})

    got_columns, rows, _hit = _client(cls, handler).rows(QUERY, "org_admin")
    assert got_columns == columns
    assert [list(r.values())[0] for r in rows] == [1, 2]
    # call() still counts what the same answer holds
    n, ncols, _ = _client(cls, handler).call(QUERY, "org_admin")
    assert (n, ncols) == (2, 1)


def test_rows_reads_the_mcp_tools_answer():
    client = ol.McpClient.__new__(ol.McpClient)
    answer = {"rows": [[1], [2]], "columns": ["order_id"]}
    client._rpc = lambda method, params: {"content": [{"text": json.dumps(answer)}]}
    columns, rows, hit = client.rows(QUERY, "org_admin")
    assert (columns, rows, hit) == (["order_id"], [{"order_id": 1}, {"order_id": 2}], None)
