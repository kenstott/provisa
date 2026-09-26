# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Graph store bench source (sales dept relationship shape): (:Customer)-[:PLACED]->(:Order)
-[:CONTAINS]->(:Product), each node type carrying realistic width (Order 16 props, Customer 10,
Product 9). order_id/customer_id are the cross-source join keys shared with
generate_postgres.py/generate_clickhouse.py/generate_mongo.py (same domain: 1..--orders from
generate_postgres.py). Loaded via the HTTP transaction API in UNWIND batches (same transport as
demo/sources/neo4j/prime.py) so no bolt driver dependency is required.

Usage: python generate_neo4j.py [--orders N] [--items-per-order N]
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from datetime import datetime, timedelta

import httpx

HOST = os.environ.get("PROVISA_BENCH_NEO4J_HOST", "localhost")
PORT = int(os.environ.get("PROVISA_BENCH_NEO4J_HTTP_PORT", "27974"))
BASE = f"http://{HOST}:{PORT}"
TX = f"{BASE}/db/neo4j/tx/commit"
REGIONS = ["NA", "EMEA", "APAC", "LATAM"]
TIERS = ["bronze", "silver", "gold", "platinum"]
INDUSTRIES = ["retail", "manufacturing", "healthcare", "finance", "tech", "logistics"]
CATEGORIES = ["electronics", "apparel", "home", "industrial", "office", "food", "automotive"]
STATUSES = ["open", "shipped", "delivered", "cancelled", "returned"]
CHANNELS = ["web", "mobile", "phone", "reseller", "marketplace"]
BATCH_ROWS = 20_000
START = datetime(2020, 1, 1)


def _run(client: httpx.Client, statement: str, params: dict | None = None) -> dict:
    resp = client.post(
        TX,
        json={"statements": [{"statement": statement, "parameters": params or {}}]},
        timeout=120,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("errors"):
        raise SystemExit(f"neo4j tx failed: {body['errors']}")
    return body


def _connect() -> httpx.Client:
    client = httpx.Client()
    deadline = time.monotonic() + 120
    while True:
        try:
            _run(client, "RETURN 1")
            return client
        except (httpx.HTTPError, SystemExit):
            if time.monotonic() > deadline:
                print(f"neo4j at {BASE} did not become ready", file=sys.stderr)
                raise
            time.sleep(2)


def _customers(rng: random.Random, n: int):
    for customer_id in range(1, n + 1):
        yield {
            "customer_id": customer_id,
            "name": f"Customer {customer_id}",
            "tier": TIERS[rng.randrange(len(TIERS))],
            "region": REGIONS[rng.randrange(len(REGIONS))],
            "industry": INDUSTRIES[rng.randrange(len(INDUSTRIES))],
            "employees": rng.randint(1, 50000),
            "lifetime_value": round(rng.uniform(50, 500000), 2),
            "signup_year": rng.randint(2015, 2025),
            "churn_risk": round(rng.uniform(0, 1), 3),
            "primary_contact_email": f"contact{customer_id}@example.com",
        }


def _products(rng: random.Random, n: int):
    for product_id in range(1, n + 1):
        yield {
            "product_id": product_id,
            "sku": f"SKU-{product_id:06d}",
            "name": f"Product {product_id}",
            "category": CATEGORIES[rng.randrange(len(CATEGORIES))],
            "unit_cost": round(rng.uniform(1, 300), 2),
            "list_price": round(rng.uniform(2, 500), 2),
            "supplier_id": rng.randint(1, 500),
            "active": rng.random() > 0.05,
            "launch_year": rng.randint(2010, 2025),
        }


def _orders(rng: random.Random, n_orders: int, n_customers: int):
    for order_id in range(1, n_orders + 1):
        order_date = START + timedelta(days=rng.randrange(0, 2000))
        yield {
            "order_id": order_id,
            "customer_id": rng.randint(1, n_customers),
            "region": REGIONS[rng.randrange(len(REGIONS))],
            "channel": CHANNELS[rng.randrange(len(CHANNELS))],
            "status": STATUSES[rng.randrange(len(STATUSES))],
            "amount": round(rng.uniform(5, 5000), 2),
            "discount": round(rng.uniform(0, 500), 2),
            "tax": round(rng.uniform(0, 400), 2),
            "shipping_cost": round(rng.uniform(0, 50), 2),
            "currency": "USD",
            "payment_method": "credit_card",
            "priority": rng.choice(["low", "standard", "high", "rush"]),
            "warehouse_id": rng.randint(1, 40),
            "order_date": order_date.isoformat(),
            "created_at": order_date.isoformat(),
            "fulfillment_days": rng.randint(1, 5),
        }


def _order_lines(rng: random.Random, n_orders: int, n_products: int, items_per_order: int):
    for order_id in range(1, n_orders + 1):
        for _ in range(rng.randrange(1, items_per_order * 2)):
            yield {
                "order_id": order_id,
                "product_id": rng.randint(1, n_products),
                "quantity": rng.randint(1, 20),
                "unit_price": round(rng.uniform(1, 500), 2),
            }


def _batched(it, size):
    batch = []
    for row in it:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _load(client: httpx.Client, label: str, rows_it, cypher: str) -> int:
    t0 = time.monotonic()
    n = 0
    for batch in _batched(rows_it, BATCH_ROWS):
        _run(client, cypher, {"rows": batch})
        n += len(batch)
    print(f"{label}: {n:,} in {time.monotonic() - t0:.0f}s")
    return n


def main() -> int:
    parser = argparse.ArgumentParser()
    # Smaller default than the other 3 generate_*.py scripts on purpose — see seed.py's
    # PROVISA_BENCH_NEO4J_ORDERS docstring: this is a replica-only source (materialize/cache
    # pattern), so the materialize-cost benchmark needs realistic table SIZE, not full order_id
    # domain coverage matching the live sources.
    parser.add_argument("--orders", type=int, default=2_000_000)
    parser.add_argument("--items-per-order", type=int, default=4)
    args = parser.parse_args()
    n_customers = max(1, args.orders // 8)
    n_products = 50_000

    client = _connect()
    _run(client, "MATCH (n) DETACH DELETE n")
    _run(client, "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE")
    _run(client, "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE")
    _run(client, "CREATE CONSTRAINT IF NOT EXISTS FOR (o:Order) REQUIRE o.order_id IS UNIQUE")

    rng = random.Random(5)
    _load(
        client,
        "Customer nodes",
        _customers(rng, n_customers),
        "UNWIND $rows AS row CREATE (c:Customer) SET c = row",
    )
    _load(
        client,
        "Product nodes",
        _products(rng, n_products),
        "UNWIND $rows AS row CREATE (p:Product) SET p = row",
    )
    _load(
        client,
        "Order nodes",
        _orders(random.Random(6), args.orders, n_customers),
        "UNWIND $rows AS row CREATE (o:Order) SET o = row",
    )
    _load(
        client,
        "PLACED relationships",
        _orders(random.Random(6), args.orders, n_customers),
        """
        UNWIND $rows AS row
        MATCH (c:Customer {customer_id: row.customer_id}), (o:Order {order_id: row.order_id})
        CREATE (c)-[:PLACED]->(o)
        """,
    )
    _load(
        client,
        "CONTAINS relationships",
        _order_lines(random.Random(7), args.orders, n_products, args.items_per_order),
        """
        UNWIND $rows AS row
        MATCH (o:Order {order_id: row.order_id}), (p:Product {product_id: row.product_id})
        CREATE (o)-[:CONTAINS {quantity: row.quantity, unit_price: row.unit_price}]->(p)
        """,
    )

    counts = _run(
        client,
        "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS n ORDER BY label",
    )["results"][0]["data"]
    for row in counts:
        label, n = row["row"]
        print(f"{label}: {n:,} nodes")
    print(f"neo4j bench source primed at {BASE}")
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
