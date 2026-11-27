# Copyright (c) 2026 Kenneth Stott
# Canary: 9a2f6c83-1d4e-47b5-8c90-e5b3a7d12f46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: GraphQL relationship fields on the engine swap (#362). One PostgreSQL source of
customers and orders, read over POST /data/graphql by a real server on each in-process engine --
ClickHouse (embedded chdb) and DuckDB: a many-to-one field is an object, null where no row
matches; a one-to-many field a list, [] for none; a nested pair reads both ways; and a capped list
holds at most its cap. Every engine answers the same JSON."""

# Requirements: REQ-047, REQ-048, REQ-2236

from __future__ import annotations

import httpx
import pytest

pytestmark = [pytest.mark.integration]

_ROLE = "org_admin"


def _config(port: int, engine: str) -> dict:
    from tests.embedded_pg import DATABASE, PASSWORD, ROLE

    def column(name: str, data_type: str) -> dict:
        return {"name": name, "data_type": data_type, "visible_to": [_ROLE]}

    return {
        "federation_engine": engine,
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "shop", "description": "Shop"}],
        "roles": [],
        "sources": [
            {
                "id": "shop-pg",
                "type": "postgresql",
                "host": "127.0.0.1",
                "port": port,
                "database": DATABASE,
                "username": ROLE,
                "password": PASSWORD,
            }
        ],
        "tables": [
            {
                "source_id": "shop-pg",
                "domain_id": "shop",
                "schema": "public",
                "table": "customers",
                "columns": [column("id", "integer"), column("name", "varchar")],
            },
            {
                "source_id": "shop-pg",
                "domain_id": "shop",
                "schema": "public",
                "table": "orders",
                "columns": [
                    column("id", "integer"),
                    column("customer_id", "integer"),
                    column("total", "numeric"),
                ],
            },
        ],
        "relationships": [
            {
                "id": "orders-customer",
                "source_table_id": "orders",
                "target_table_id": "customers",
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
                "graphql_alias": "customer",
            },
            {
                "id": "customer-orders",
                "source_table_id": "customers",
                "target_table_id": "orders",
                "source_column": "id",
                "target_column": "customer_id",
                "cardinality": "one-to-many",
                "graphql_alias": "orders",
            },
        ],
    }


@pytest.fixture(scope="module")
def source():
    """customers 1 (ada, two orders), 2 (bo, one), 3 (cy, none); order 13 names no customer."""
    import psycopg

    from tests.embedded_pg import start_local_postgres

    pg = start_local_postgres()
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        conn.execute("CREATE TABLE customers (id integer PRIMARY KEY, name text)")
        conn.execute("INSERT INTO customers VALUES (1, 'ada'), (2, 'bo'), (3, 'cy')")
        conn.execute(
            "CREATE TABLE orders (id integer PRIMARY KEY, customer_id integer, total numeric(10,2))"
        )
        conn.execute(
            "INSERT INTO orders VALUES (10, 1, 12.50), (11, 1, 3.00), (12, 2, 7.25), (13, 9, 1.00)"
        )
    try:
        yield pg
    finally:
        pg.stop()


@pytest.fixture(scope="module", params=["clickhouse", "duckdb"])
def server(request, source, tmp_path_factory):
    import asyncio

    import yaml

    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    engine = request.param
    org = f"relfields_{engine}"
    path = tmp_path_factory.mktemp(org) / "config.yaml"
    path.write_text(yaml.safe_dump(_config(source.port, engine)))
    srv = IsolatedServer(org, engine=engine, config=str(path))
    srv.start(timeout=300)
    try:
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(org))


def _graphql(srv, query: str) -> dict:
    resp = httpx.post(
        f"{srv.base_url}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ROLE},
        timeout=srv.request_timeout + 120,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    return body["data"]


def test_a_many_to_one_field_is_an_object_or_null(server):
    orders = _graphql(server, "{ orders(order_by: {id: asc}) { id customer { id name } } }")
    assert orders["orders"] == [
        {"id": 10, "customer": {"id": 1, "name": "ada"}},
        {"id": 11, "customer": {"id": 1, "name": "ada"}},
        {"id": 12, "customer": {"id": 2, "name": "bo"}},
        {"id": 13, "customer": None},
    ]


def test_a_one_to_many_field_is_a_list_or_empty(server):
    customers = _graphql(server, "{ customers(order_by: {id: asc}) { id orders { id total } } }")
    by_id = {c["id"]: sorted(o["id"] for o in c["orders"]) for c in customers["customers"]}
    assert by_id == {1: [10, 11], 2: [12], 3: []}
    totals = {o["id"]: o["total"] for c in customers["customers"] for o in c["orders"]}
    assert {k: float(v) for k, v in totals.items()} == {10: 12.5, 11: 3.0, 12: 7.25}


def test_nested_fields_read_both_ways(server):
    data = _graphql(
        server,
        "{ orders(order_by: {id: asc}) { id customer { name orders { id } } } }",
    )
    got = {
        o["id"]: None
        if o["customer"] is None
        else (o["customer"]["name"], sorted(x["id"] for x in o["customer"]["orders"]))
        for o in data["orders"]
    }
    assert got == {10: ("ada", [10, 11]), 11: ("ada", [10, 11]), 12: ("bo", [12]), 13: None}


def test_a_capped_list_holds_at_most_its_cap(server):
    customers = _graphql(
        server, "{ customers(order_by: {id: asc}) { id orders(limit: 1) { id } } }"
    )
    assert [len(c["orders"]) for c in customers["customers"]] == [1, 1, 0]
