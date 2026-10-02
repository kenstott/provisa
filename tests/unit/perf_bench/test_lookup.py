# Copyright (c) 2026 Kenneth Stott
# Canary: 601217e8-ff68-46fd-96b0-a1cf4648eb3d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-1911: every transport's spelling of a table comes from the deployment, not the contract.

FIXTURE: the deployment's answers are recorded shapes (deployment_fixture.py); the real lookup is
proven by the local integration run."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import deployment_fixture as fx  # noqa: E402
import lookup  # noqa: E402

ORDERS = lookup.TableIdentity(
    "bench-postgresql", "public", "orders", ("order_id", "customer_id", "region")
)
ITEMS = lookup.TableIdentity("bench-postgresql", "public", "order_items", ("item_id", "order_id"))
EVENTS = lookup.TableIdentity(
    "bench-clickhouse", "default", "order_events", ("event_id", "order_id")
)
DOCS = lookup.TableIdentity(
    "bench-mongodb", "provisa_bench", "order_docs", ("order_id", "shipping_carrier")
)
NODE = lookup.TableIdentity("bench-neo4j", "neo4j", "bench_order_node", ("order_id",))


def _resolve(*tables: lookup.TableIdentity, joins: tuple = (), **kw: object) -> lookup.Resolved:
    return lookup.resolve(list(tables), list(joins), fx.build(**kw))  # type: ignore[arg-type]


def test_a_table_is_spelled_on_every_transport() -> None:
    names = _resolve(ORDERS).tables[ORDERS.key]
    assert (names.sql, names.graphql_field, names.cypher_label, names.grpc_type) == (
        "perf_bench.orders",
        "pb__orders",
        "PerfBench:Orders",
        "PbOrders",
    )
    assert (names.rest_path, names.jsonapi_path, names.jsonapi_type) == (
        "/data/rest/perf-bench/orders",
        "/data/jsonapi/perf-bench/orders",
        "orders",
    )
    assert names.columns["order_id"] == {
        "sql": "order_id",
        "graphql": "orderId",
        "cypher": "orderId",
        "grpc": "order_id",
        "rest": "orderId",
        "jsonapi": "order_id",
    }
    assert names.columns["region"]["graphql"] == "region"


def test_an_aliased_table_is_matched_by_its_alias() -> None:
    names = _resolve(NODE).tables[NODE.key]
    assert (
        names.cypher_label == "PerfBench:Order"
        and names.jsonapi_path == "/data/jsonapi/perf-bench/Order"
    )
    assert names.jsonapi_type == "Order" and names.sql == "perf_bench.bench_order_node"


@pytest.mark.parametrize(
    "lang,field",
    [
        ("rest", "rest_path"),
        ("jsonapi", "jsonapi_path"),
        ("cypher", "cypher_label"),
        ("grpc", "grpc_type"),
        ("graphql", "graphql_field"),
    ],
)
def test_a_transport_that_does_not_expose_a_table_is_a_fact(lang: str, field: str) -> None:
    names = _resolve(EVENTS, drop={"order_events": {lang}}).tables[EVENTS.key]
    assert getattr(names, field) is None
    assert all(lang not in spelled for spelled in names.columns.values())
    assert names.sql == "perf_bench.order_events"  # the others are untouched


def test_a_column_a_transport_does_not_expose_is_left_out() -> None:
    tables = fx.TABLES
    tables = [dict(t) for t in tables]
    orders = tables[0]
    orders["columns"] = [c for c in orders["columns"]]
    raw = fx.build(tables=tables)
    # remove one REST enum entry only
    enum = next(iter(raw.rest_spec["components"]["schemas"].values()))["enum"]
    enum.remove("customerId")
    names = lookup.resolve([ORDERS], [], raw).tables[ORDERS.key]
    assert (
        "rest" not in names.columns["customer_id"]
        and names.columns["order_id"]["rest"] == "orderId"
    )


def test_a_table_the_deployment_does_not_register() -> None:
    ghost = lookup.TableIdentity("bench-postgresql", "public", "ghost", ("a",))
    with pytest.raises(
        lookup.LookupError_, match="bench-postgresql/public.ghost is not registered"
    ):
        _resolve(ghost)


def test_a_column_the_deployment_does_not_register_is_named_with_what_is() -> None:
    bad = lookup.TableIdentity("bench-postgresql", "public", "orders", ("order_id", "nope"))
    with pytest.raises(
        lookup.LookupError_,
        match="column 'nope' of bench-postgresql/public.orders is not registered",
    ):
        _resolve(bad)


def test_an_ambiguous_match_is_refused_not_guessed() -> None:
    raw = fx.build()
    raw.graph_schema["node_labels"].append(
        {
            "label": "Other:Orders",
            "domain_id": "perf-bench",
            "table_label": "Orders",
            "properties": [],
        }
    )
    with pytest.raises(lookup.LookupError_, match="all match table 'orders'"):
        lookup.resolve([ORDERS], [], raw)


# ------------------------------------------------------------------ joins


def _join(
    left: lookup.TableIdentity, right: lookup.TableIdentity, col: str = "order_id"
) -> lookup.JoinIdentity:
    return lookup.JoinIdentity(left, col, right, col)


def test_a_join_is_a_registered_relationship_with_its_nested_field_and_type() -> None:
    r = _resolve(
        ORDERS,
        ITEMS,
        EVENTS,
        DOCS,
        joins=(_join(ORDERS, ITEMS), _join(ORDERS, EVENTS), _join(ORDERS, DOCS)),
    )
    got = {
        k.split("->")[1].split("/")[0] + k.split("->")[1].split(".")[1]: (
            j.forward,
            j.graphql_field,
            j.cypher_rel,
        )
        for k, j in r.joins.items()
    }
    assert got == {
        "bench-postgresqlorder_items": (True, "orderItems", "HAS_ITEM"),
        "bench-clickhouseorder_events": (True, "orderEvents", "HAS_EVENT"),
        "bench-mongodborder_docs": (True, "orderDoc", "HAS_DOC"),
    }


def test_a_join_written_the_other_way_round_is_oriented_to_the_relationship() -> None:
    r = _resolve(ORDERS, ITEMS, joins=(_join(ITEMS, ORDERS),))
    (j,) = r.joins.values()
    assert j.forward is False and j.graphql_field == "orderItems" and j.cypher_rel == "HAS_ITEM"


def test_a_join_that_is_not_registered_is_refused() -> None:
    with pytest.raises(
        lookup.LookupError_, match="is not a registered relationship in the deployment"
    ):
        _resolve(
            ORDERS, ITEMS, joins=(lookup.JoinIdentity(ORDERS, "customer_id", ITEMS, "order_id"),)
        )


def test_a_join_transport_that_does_not_expose_it_is_a_fact() -> None:
    r = _resolve(
        ORDERS, ITEMS, joins=(_join(ORDERS, ITEMS),), drop={"order_items": {"cypher", "graphql"}}
    )
    (j,) = r.joins.values()
    assert (j.graphql_field, j.cypher_rel) == (None, None)


def test_two_graphql_fields_reaching_the_target_are_refused() -> None:
    raw = fx.build()
    orders_type = next(
        t for t in raw.introspection["data"]["__schema"]["types"] if t["name"] == "PbOrders"
    )
    orders_type["fields"].append({"name": "items2", "type": fx._list_of("PbOrderItems")})
    with pytest.raises(lookup.LookupError_, match="all reach the target type"):
        lookup.resolve([ORDERS, ITEMS], [_join(ORDERS, ITEMS)], raw)


def test_replication_facts_are_carried() -> None:
    r = _resolve(ORDERS, EVENTS)
    assert r.tables[ORDERS.key].replicate is None
    assert (r.tables[EVENTS.key].replicate, r.tables[EVENTS.key].cache_ttl) == (0, 300)
    assert r.sources["bench-clickhouse"] == {
        "type": "clickhouse",
        "replicate": 0,
        "cache_enabled": True,
        "cache_ttl": 300,
    }


# ------------------------------------------------------------------ the resolved-names file


def test_resolved_names_round_trip(tmp_path: Path) -> None:
    r = _resolve(ORDERS, ITEMS, joins=(_join(ORDERS, ITEMS),))
    path = tmp_path / "resolved.json"
    lookup.write_resolved(r, path)
    assert lookup.read_resolved(path) == r


def test_the_raw_answers_round_trip() -> None:
    raw = fx.build()
    assert lookup.raw_from_json(lookup.raw_to_json(raw)) == raw


# ------------------------------------------------------------------ fetch


def _server(raw: lookup.RawLookup, seen: list[httpx.Request]) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        p = request.url.path
        if p == "/admin/graphql":
            return httpx.Response(200, json=raw.admin)
        if p == "/data/introspection":
            return httpx.Response(200, json=raw.introspection)
        if p == "/data/graph-schema":
            return httpx.Response(200, json=raw.graph_schema)
        if p == "/data/rest/openapi.json":
            return httpx.Response(200, json=raw.rest_spec)
        if p == "/data/jsonapi/openapi.json":
            return httpx.Response(200, json=raw.jsonapi_spec)
        if p == "/data/proto/org_admin":
            return httpx.Response(200, text=raw.proto)
        return httpx.Response(404)

    return httpx.Client(base_url="http://x", transport=httpx.MockTransport(handler))


def test_fetch_reads_each_endpoint_as_the_role() -> None:
    raw, seen = fx.build(), []
    got = lookup.fetch(_server(raw, seen), "org_admin")
    assert got == raw
    assert [r.url.path for r in seen] == [
        "/admin/graphql",
        "/data/introspection",
        "/data/graph-schema",
        "/data/rest/openapi.json",
        "/data/jsonapi/openapi.json",
        "/data/proto/org_admin",
    ]
    assert all(r.headers["x-provisa-role"] == "org_admin" for r in seen)
    assert json.loads(seen[0].content)["query"] == lookup.ADMIN_QUERY
    assert seen[3].url.params["role"] == "org_admin"


def test_fetch_reports_an_admin_error() -> None:
    client = httpx.Client(
        base_url="http://x",
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, json={"errors": [{"message": "no"}]})
        ),
    )
    with pytest.raises(lookup.LookupError_, match="/admin/graphql"):
        lookup.fetch(client, "org_admin")
