# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""RDB bench source (sales dept OLTP shape): orders (26 cols) + order_items (16 cols).
order_id and customer_id are the cross-source join keys shared with generate_clickhouse.py,
generate_mongo.py, and generate_neo4j.py. Loads via COPY FROM STDIN (fastest bulk path) streamed
from a generator, so memory use stays flat regardless of row count.

Usage: python generate_postgres.py [--orders N] [--items-per-order N]
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from datetime import date, timedelta
from io import StringIO

import psycopg2

HOST = os.environ.get("PROVISA_BENCH_POSTGRESQL_HOST", "localhost")
PORT = int(os.environ.get("PROVISA_BENCH_POSTGRESQL_PORT", "25632"))
REGIONS = ["NA", "EMEA", "APAC", "LATAM"]
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "JP", "AU", "BR", "IN", "MX"]
STATUSES = ["open", "shipped", "delivered", "cancelled", "returned"]
CHANNELS = ["web", "mobile", "phone", "reseller", "marketplace"]
PAYMENT_METHODS = ["credit_card", "ach", "wire", "invoice", "paypal"]
PAYMENT_STATUSES = ["paid", "pending", "failed", "refunded"]
SEGMENTS = ["enterprise", "mid_market", "smb", "consumer"]
DEVICE_TYPES = ["desktop", "mobile", "tablet", "pos"]
PRIORITIES = ["low", "standard", "high", "rush"]
CATEGORIES = ["electronics", "apparel", "home", "industrial", "office", "food", "automotive"]
SUBCATEGORIES = ["a", "b", "c", "d", "e"]
SKUS = [f"SKU-{i:06d}" for i in range(50_000)]
BATCH_ROWS = 200_000
START_DATE = date(2020, 1, 1)


def _connect() -> psycopg2.extensions.connection:
    deadline = time.monotonic() + 60
    while True:
        try:
            return psycopg2.connect(
                host=HOST,
                port=PORT,
                user="provisa",
                password="provisa",
                dbname="provisa_bench",
                connect_timeout=5,
            )
        except psycopg2.Error:
            if time.monotonic() > deadline:
                print(f"postgresql at {HOST}:{PORT} did not become ready", file=sys.stderr)
                raise
            time.sleep(2)


def _pick(rng: random.Random, choices: list[str]) -> str:
    return choices[rng.randrange(len(choices))]


def _orders_batches(n: int):
    rng = random.Random(1)
    buf = StringIO()
    count = 0
    for order_id in range(1, n + 1):
        customer_id = rng.randint(1, max(1, n // 8))
        sales_rep_id = rng.randint(1, 2000)
        warehouse_id = rng.randint(1, 40)
        order_date = START_DATE + timedelta(days=rng.randrange(0, 2000))
        ship_date = order_date + timedelta(days=rng.randrange(0, 5))
        delivery_date = ship_date + timedelta(days=rng.randrange(1, 10))
        amount = rng.uniform(5, 5000)
        discount = round(amount * rng.uniform(0, 0.3), 2)
        tax = round(amount * 0.08, 2)
        shipping_cost = round(rng.uniform(0, 50), 2)
        cols = [
            order_id,
            customer_id,
            sales_rep_id,
            warehouse_id,
            _pick(rng, REGIONS),
            _pick(rng, COUNTRIES),
            order_date,
            ship_date,
            delivery_date,
            _pick(rng, CHANNELS),
            f"{amount:.2f}",
            f"{discount:.2f}",
            f"{tax:.2f}",
            f"{shipping_cost:.2f}",
            "USD",
            _pick(rng, PAYMENT_METHODS),
            _pick(rng, PAYMENT_STATUSES),
            _pick(rng, STATUSES),
            _pick(rng, PRIORITIES),
            _pick(rng, SEGMENTS),
            _pick(rng, DEVICE_TYPES),
            f"PROMO{rng.randint(0, 200)}" if rng.random() < 0.2 else "\\N",
            order_date,
            order_date,
            rng.randint(1, 5),
            "\\N",
        ]
        buf.write("\t".join(str(c) for c in cols) + "\n")
        count += 1
        if count >= BATCH_ROWS:
            buf.seek(0)
            yield buf
            buf = StringIO()
            count = 0
    if count:
        buf.seek(0)
        yield buf


def _items_batches(n_orders: int, items_per_order: int):
    rng = random.Random(2)
    buf = StringIO()
    count = 0
    item_id = 0
    for order_id in range(1, n_orders + 1):
        for _ in range(rng.randrange(1, items_per_order * 2)):
            item_id += 1
            unit_price = rng.uniform(1, 500)
            quantity = rng.randint(1, 20)
            discount_pct = round(rng.uniform(0, 0.25), 4)
            line_total = round(unit_price * quantity * (1 - discount_pct), 2)
            cost_of_goods = round(unit_price * rng.uniform(0.4, 0.75), 2)
            margin = round(line_total - cost_of_goods * quantity, 2)
            cols = [
                item_id,
                order_id,
                _pick(rng, SKUS),
                f"Product {item_id % 50_000}",
                _pick(rng, CATEGORIES),
                _pick(rng, SUBCATEGORIES),
                quantity,
                f"{unit_price:.2f}",
                f"{discount_pct:.4f}",
                f"{line_total:.2f}",
                f"{cost_of_goods:.2f}",
                f"{margin:.2f}",
                rng.randint(1, 500),
                rng.randint(1, 40),
                "t" if rng.random() < 0.03 else "f",
                rng.randint(1, 10),
            ]
            buf.write("\t".join(str(c) for c in cols) + "\n")
            count += 1
            if count >= BATCH_ROWS:
                buf.seek(0)
                yield buf
                buf = StringIO()
                count = 0
    if count:
        buf.seek(0)
        yield buf


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=20_000_000)
    parser.add_argument("--items-per-order", type=int, default=4)
    args = parser.parse_args()

    conn = _connect()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS order_items")
            cur.execute("DROP TABLE IF EXISTS orders")
            cur.execute(
                """
                CREATE TABLE orders (
                    order_id BIGINT PRIMARY KEY,
                    customer_id BIGINT NOT NULL,
                    sales_rep_id BIGINT NOT NULL,
                    warehouse_id INT NOT NULL,
                    region TEXT NOT NULL,
                    country TEXT NOT NULL,
                    order_date DATE NOT NULL,
                    ship_date DATE NOT NULL,
                    delivery_date DATE NOT NULL,
                    channel TEXT NOT NULL,
                    amount NUMERIC(12,2) NOT NULL,
                    discount NUMERIC(12,2) NOT NULL,
                    tax NUMERIC(12,2) NOT NULL,
                    shipping_cost NUMERIC(10,2) NOT NULL,
                    currency TEXT NOT NULL,
                    payment_method TEXT NOT NULL,
                    payment_status TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    customer_segment TEXT NOT NULL,
                    device_type TEXT NOT NULL,
                    promo_code TEXT,
                    created_at DATE NOT NULL,
                    updated_at DATE NOT NULL,
                    fulfillment_days INT NOT NULL,
                    notes TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE order_items (
                    item_id BIGINT PRIMARY KEY,
                    order_id BIGINT NOT NULL,
                    sku TEXT NOT NULL,
                    product_name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    subcategory TEXT NOT NULL,
                    quantity INT NOT NULL,
                    unit_price NUMERIC(10,2) NOT NULL,
                    discount_pct NUMERIC(6,4) NOT NULL,
                    line_total NUMERIC(12,2) NOT NULL,
                    cost_of_goods NUMERIC(10,2) NOT NULL,
                    margin NUMERIC(12,2) NOT NULL,
                    supplier_id INT NOT NULL,
                    warehouse_id INT NOT NULL,
                    backorder_flag BOOLEAN NOT NULL,
                    lead_time_days INT NOT NULL
                )
                """
            )
        conn.commit()

        t0 = time.monotonic()
        with conn.cursor() as cur:
            for batch in _orders_batches(args.orders):
                cur.copy_expert("COPY orders FROM STDIN WITH (FORMAT text)", batch)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute("CREATE INDEX ON orders (customer_id)")
        conn.commit()
        print(f"orders: {args.orders:,} rows in {time.monotonic() - t0:.0f}s")

        t0 = time.monotonic()
        with conn.cursor() as cur:
            for batch in _items_batches(args.orders, args.items_per_order):
                cur.copy_expert("COPY order_items FROM STDIN WITH (FORMAT text)", batch)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("CREATE INDEX ON order_items (order_id)")
            cur.execute("CREATE INDEX ON order_items (sku)")
            cur.execute("SELECT count(*) FROM order_items")
            row = cur.fetchone()
            n_items = row[0] if row else 0
        conn.commit()
        print(f"order_items: {n_items:,} rows in {time.monotonic() - t0:.0f}s")

        with conn.cursor() as cur:
            cur.execute("SELECT pg_size_pretty(pg_database_size('provisa_bench'))")
            row = cur.fetchone()
            size = row[0] if row else "?"
            print(f"provisa_bench size: {size} at {HOST}:{PORT}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
