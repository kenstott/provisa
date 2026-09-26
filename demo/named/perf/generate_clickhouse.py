# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""DW bench source (sales dept analytics shape): order_events fact table, 22 columns. order_id
is the cross-source join key shared with generate_postgres.py/generate_mongo.py/generate_neo4j.py
(same domain: 1..--orders from generate_postgres.py). Loaded via clickhouse-connect batched
insert() into a MergeTree table ordered by (order_id, event_ts) for join-friendly scans.

Usage: python generate_clickhouse.py [--orders N] [--events-per-order N]
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from datetime import datetime, timedelta

import clickhouse_connect

HOST = os.environ.get("PROVISA_BENCH_CLICKHOUSE_HOST", "localhost")
PORT = int(os.environ.get("PROVISA_BENCH_CLICKHOUSE_PORT", "28623"))
EVENT_TYPES = [
    "viewed",
    "cart_add",
    "checkout_start",
    "payment_captured",
    "shipped",
    "delivered",
    "returned",
]
CHANNELS = ["web", "mobile", "phone", "reseller", "marketplace"]
DEVICES = ["desktop", "mobile", "tablet", "pos"]
BROWSERS = ["chrome", "safari", "firefox", "edge", "app"]
OS_NAMES = ["macos", "windows", "ios", "android", "linux"]
GEO_COUNTRIES = ["US", "CA", "GB", "DE", "FR", "JP", "AU", "BR", "IN", "MX"]
STATUS_CODES = [200, 200, 200, 200, 201, 400, 404, 500]
BATCH_ROWS = 200_000
COLUMNS = [
    "event_id",
    "order_id",
    "customer_id",
    "event_type",
    "event_ts",
    "channel",
    "campaign_id",
    "device",
    "browser",
    "os",
    "session_id",
    "page_url",
    "referrer",
    "geo_country",
    "geo_region",
    "user_agent",
    "response_time_ms",
    "status_code",
    "revenue_impact",
    "warehouse_id",
    "etl_batch_id",
    "ingested_at",
]
START = datetime(2020, 1, 1)


def _connect():
    deadline = time.monotonic() + 60
    while True:
        try:
            client = clickhouse_connect.get_client(
                host=HOST, port=PORT, username="default", password="provisa"
            )
            client.command("SELECT 1")
            return client
        except Exception:  # noqa: BLE001 - readiness probe, any error means "not yet"
            if time.monotonic() > deadline:
                print(f"clickhouse at {HOST}:{PORT} did not become ready", file=sys.stderr)
                raise
            time.sleep(2)


def _rows(n_orders: int, events_per_order: int):
    rng = random.Random(3)
    event_id = 0
    for order_id in range(1, n_orders + 1):
        customer_id = rng.randint(1, max(1, n_orders // 8))
        session_id = f"sess-{order_id}-{rng.randint(0, 999)}"
        for _ in range(rng.randrange(1, events_per_order * 2)):
            event_id += 1
            ts = START + timedelta(days=rng.randrange(0, 2000), seconds=rng.randrange(0, 86400))
            yield (
                event_id,
                order_id,
                customer_id,
                EVENT_TYPES[rng.randrange(len(EVENT_TYPES))],
                ts,
                CHANNELS[rng.randrange(len(CHANNELS))],
                rng.randint(1, 500),
                DEVICES[rng.randrange(len(DEVICES))],
                BROWSERS[rng.randrange(len(BROWSERS))],
                OS_NAMES[rng.randrange(len(OS_NAMES))],
                session_id,
                f"/orders/{order_id}",
                "https://ref.example.com" if rng.random() < 0.4 else "",
                GEO_COUNTRIES[rng.randrange(len(GEO_COUNTRIES))],
                f"R{rng.randint(1, 50)}",
                "Mozilla/5.0 (bench-data synthetic)",
                rng.randint(5, 3000),
                STATUS_CODES[rng.randrange(len(STATUS_CODES))],
                round(rng.uniform(-50, 500), 2),
                rng.randint(1, 40),
                rng.randint(1, 5000),
                ts,
            )


def _batched(it, size):
    batch = []
    for row in it:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=20_000_000)
    parser.add_argument("--events-per-order", type=int, default=3)
    args = parser.parse_args()

    client = _connect()
    client.command("DROP TABLE IF EXISTS order_events")
    client.command(
        """
        CREATE TABLE order_events (
            event_id UInt64,
            order_id UInt64,
            customer_id UInt64,
            event_type LowCardinality(String),
            event_ts DateTime,
            channel LowCardinality(String),
            campaign_id UInt32,
            device LowCardinality(String),
            browser LowCardinality(String),
            os LowCardinality(String),
            session_id String,
            page_url String,
            referrer String,
            geo_country LowCardinality(String),
            geo_region String,
            user_agent String,
            response_time_ms UInt32,
            status_code UInt16,
            revenue_impact Decimal(12,2),
            warehouse_id UInt32,
            etl_batch_id UInt32,
            ingested_at DateTime
        ) ENGINE = MergeTree ORDER BY (order_id, event_ts)
        """
    )
    # ORDER BY only sparse-indexes (order_id, event_ts); customer_id lookups need their own
    # skip index since MergeTree has no traditional secondary index.
    client.command(
        "ALTER TABLE order_events ADD INDEX customer_id_idx customer_id TYPE bloom_filter GRANULARITY 4"
    )

    t0 = time.monotonic()
    n = 0
    for batch in _batched(_rows(args.orders, args.events_per_order), BATCH_ROWS):
        client.insert("order_events", batch, column_names=COLUMNS)
        n += len(batch)
    client.command("OPTIMIZE TABLE order_events")
    print(f"order_events: {n:,} rows in {time.monotonic() - t0:.0f}s")

    size = client.command(
        "SELECT formatReadableSize(total_bytes) FROM system.tables WHERE name='order_events'"
    )
    print(f"order_events size: {size} at {HOST}:{PORT}")
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
