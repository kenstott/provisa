# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2033: the named demo ``streaming`` answers its question from what it ships, with no broker
or server: the orders producer.py publishes fit the table they land in and join the catalog
db/init.sql seeds, and the view's own SQL, run over those tables, answers which regions placed the
most orders in the last five minutes."""

# Requirements: REQ-2033

from __future__ import annotations

import importlib.util
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import graphql
import pytest
import yaml

_DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "streaming"
_CONFIG = yaml.safe_load((_DEMO / "config.yaml").read_text())
_ORDERS = next(t for t in _CONFIG["tables"] if t["table"] == "orders")
_VIEW = next(v for v in _CONFIG["views"] if v["id"] == "orders_last_five_minutes")
_LOCAL_DEFAULT = re.compile(r"^\$\{env:\w+:-(localhost|127\.0\.0\.1)\}$")


@pytest.fixture
def producer(monkeypatch):
    monkeypatch.setenv("ORDERS_BOOTSTRAP", "broker:9092")
    monkeypatch.setenv("TOPIC_SUFFIX", "_ns")
    spec = importlib.util.spec_from_file_location("streaming_producer", _DEMO / "producer.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def shop():
    """The shop's tables in the engine the demo federates on (DuckDB, UTC): the catalog as
    db/init.sql seeds it, and an empty orders table with the columns config.yaml lands."""
    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone='UTC'")
    con.execute("CREATE SCHEMA shop; USE shop")
    con.execute((_DEMO / "db" / "init.sql").read_text())
    columns = ", ".join(f'"{c["name"]}" {c["data_type"]}' for c in _ORDERS["columns"])
    con.execute(f'CREATE TABLE "orders" ({columns})')
    yield con
    con.close()


def _land(con, *orders: tuple[str, int, str, int, timedelta]) -> None:
    """Land orders of (id, product, region, quantity, how long ago)."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    con.executemany(
        'INSERT INTO "orders" VALUES (?, ?, ?, ?, ?)',
        [(oid, product, region, qty, now - ago) for oid, product, region, qty, ago in orders],
    )


def _operation(file: str) -> graphql.OperationDefinitionNode:
    document = graphql.parse((_DEMO / "queries" / file).read_text())
    operation = document.definitions[0]
    assert isinstance(operation, graphql.OperationDefinitionNode)
    return operation


def test_an_order_has_exactly_the_columns_of_the_table_it_lands_in(producer):
    declared = {c["name"] for c in _ORDERS["columns"]}
    for _ in range(200):
        order = producer._order()
        assert set(order) == declared
        assert isinstance(order["product_id"], int) and isinstance(order["quantity"], int)
        assert order["quantity"] >= 1
        # Stamped in UTC: the view compares it with the current time, also in UTC.
        assert datetime.fromisoformat(order["ordered_at"]).utcoffset() == timedelta(0)


def test_every_order_joins_a_product_and_a_region_of_the_catalog(producer, shop):
    """An order naming an id the catalog lacks would drop out of the view's inner joins."""
    products = {p for (p,) in shop.execute("SELECT product_id FROM products").fetchall()}
    regions = {r for (r,) in shop.execute("SELECT region_id FROM regions").fetchall()}
    assert set(producer._PRODUCTS) == products
    assert set(producer._WEIGHTS) == regions
    assert len(producer._PRODUCTS) == len(producer._PRODUCT_WEIGHTS)


def test_the_producer_publishes_to_the_topic_the_table_lands_from(producer):
    topic = _ORDERS["live"]["kafka"]["topic"]
    assert _ORDERS["live"]["strategy"] == "kafka"
    assert topic == "orders${env:STREAMING_KAFKA_NS}"
    assert producer.TOPIC == "orders_ns"


def test_the_view_answers_the_last_five_minutes_per_region_priced_from_the_catalog(shop):
    _land(
        shop,
        ("a", 1, "na-east", 2, timedelta(minutes=1)),  # 2 x 24.00
        ("b", 4, "na-east", 1, timedelta(minutes=4)),  # 1 x 129.00
        ("c", 5, "eu-west", 3, timedelta(seconds=10)),  # 3 x 6.50
        ("d", 1, "apac", 9, timedelta(minutes=6)),  # older than the window: not counted
        ("e", 1, "na-east", 9, timedelta(hours=1)),  # nor is this one of a counted region
    )
    rows = shop.execute(
        f"SELECT region, orders, items, revenue FROM ({_VIEW['sql']}) ORDER BY orders DESC"
    ).fetchall()
    assert rows == [
        ("North America East", 2, 3, Decimal("177.00")),
        ("Europe West", 1, 3, Decimal("19.50")),
    ]


def test_the_view_refreshes_as_orders_land_and_is_what_the_question_reads():
    """Materialized over the landed table, so each landed batch refreshes it; the example that
    answers the question reads it busiest first, from the response cache."""
    assert _VIEW["materialize"] is True
    assert '"shop"."orders"' in _VIEW["sql"]
    query = _operation("1-busiest-regions-now.graphql")
    assert [d.name.value for d in query.directives] == ["cached"]
    [field] = query.selection_set.selections
    assert isinstance(field, graphql.FieldNode)
    assert field.name.value == "s__viewOrdersLastFiveMinutes"
    assert graphql.print_ast(field.arguments[0].value) == "[{orders: desc}]"


def test_the_live_step_is_a_subscription_on_the_landed_orders():
    operation = _operation("3-watch-the-orders.graphql")
    assert operation.operation == graphql.OperationType.SUBSCRIPTION
    [field] = operation.selection_set.selections
    assert isinstance(field, graphql.FieldNode) and field.name.value == "s__orders"


def test_no_source_reaches_outside_the_demo_s_own_containers():
    sources = _CONFIG["sources"]
    assert {s["id"] for s in sources} == {"catalog", "orders"}
    for source in sources:
        assert _LOCAL_DEFAULT.match(source["host"]), source["host"]
