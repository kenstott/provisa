# Copyright (c) 2026 Kenneth Stott
# Canary: c0ea42fe-a4ab-4452-8646-82188c825336
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Doc store bench source (sales dept case-file shape): order_docs collection, one wide nested
document per order (customer profile, addresses, line items, payment, audit log, ~20 top-level
fields plus nested arrays/subdocuments). order_id is the cross-source join key shared with
generate_postgres.py/generate_clickhouse.py/generate_neo4j.py (same domain: 1..--orders from
generate_postgres.py). Loaded via batched insert_many(ordered=False).

Usage: python generate_mongo.py [--orders N]
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time
from datetime import datetime, timedelta

from pymongo import MongoClient
from pymongo.errors import PyMongoError

HOST = os.environ.get("PROVISA_BENCH_MONGO_HOST", "localhost")
PORT = int(os.environ.get("PROVISA_BENCH_MONGO_PORT", "27317"))
REGIONS = ["NA", "EMEA", "APAC", "LATAM"]
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "JP", "AU", "BR", "IN", "MX"]
STATUSES = ["open", "shipped", "delivered", "cancelled", "returned"]
TIERS = ["bronze", "silver", "gold", "platinum"]
CARRIERS = ["ups", "fedex", "usps", "dhl"]
SKUS = [f"SKU-{i:06d}" for i in range(50_000)]
AUDIT_EVENTS = ["created", "payment_captured", "packed", "shipped", "delivered", "note_added"]
BATCH_DOCS = 20_000
START = datetime(2020, 1, 1)


def _connect() -> MongoClient:
    deadline = time.monotonic() + 60
    while True:
        try:
            # directConnection=True: skip replica-set topology discovery and talk to exactly
            # HOST:PORT. Needed because the single-node replica set (docker-compose.yml, added
            # for change-stream support) registers its member under the Compose service name
            # (mongodb:27017) — a hostname only in-network containers can resolve. Without this,
            # a driver doing normal discovery (e.g. from the host machine over the published
            # port) would learn "mongodb:27017" from the handshake and fail to reconnect to it.
            client: MongoClient = MongoClient(
                host=HOST, port=PORT, serverSelectionTimeoutMS=5000, directConnection=True
            )
            client.admin.command("ping")
            return client
        except PyMongoError:
            if time.monotonic() > deadline:
                print(f"mongodb at {HOST}:{PORT} did not become ready", file=sys.stderr)
                raise
            time.sleep(2)


def _doc(rng: random.Random, order_id: int, n_orders: int) -> dict:
    customer_id = rng.randint(1, max(1, n_orders // 8))
    order_date = START + timedelta(days=rng.randrange(0, 2000))
    n_lines = rng.randrange(1, 6)
    line_items = [
        {
            "sku": SKUS[rng.randrange(len(SKUS))],
            "quantity": rng.randint(1, 10),
            "unit_price": round(rng.uniform(1, 500), 2),
            "gift_wrap": rng.random() < 0.05,
        }
        for _ in range(n_lines)
    ]
    n_events = rng.randrange(1, 5)
    audit_log = [
        {
            "event": AUDIT_EVENTS[rng.randrange(len(AUDIT_EVENTS))],
            "at": order_date + timedelta(hours=rng.randrange(0, 200)),
            "actor": f"agent-{rng.randint(1, 200)}",
        }
        for _ in range(n_events)
    ]
    return {
        "_id": order_id,
        "order_id": order_id,
        "customer_id": customer_id,
        "customer_profile": {
            "tier": TIERS[rng.randrange(len(TIERS))],
            "email_domain": f"customer{customer_id % 10000}.example.com",
            "lifetime_value": round(rng.uniform(50, 50000), 2),
            "signup_year": rng.randint(2015, 2025),
        },
        "shipping_address": {
            "line1": f"{rng.randint(1, 9999)} Bench St",
            "city": f"City{rng.randint(1, 5000)}",
            "region": REGIONS[rng.randrange(len(REGIONS))],
            "country": COUNTRIES[rng.randrange(len(COUNTRIES))],
            "postal_code": f"{rng.randint(10000, 99999)}",
        },
        "billing_address": {
            "line1": f"{rng.randint(1, 9999)} Ledger Ave",
            "city": f"City{rng.randint(1, 5000)}",
            "region": REGIONS[rng.randrange(len(REGIONS))],
            "country": COUNTRIES[rng.randrange(len(COUNTRIES))],
            "postal_code": f"{rng.randint(10000, 99999)}",
        },
        "line_items": line_items,
        "payment": {
            "method": "credit_card",
            "last4": f"{rng.randint(1000, 9999)}",
            "authorized_at": order_date,
            "amount": round(sum(li["unit_price"] * li["quantity"] for li in line_items), 2),
        },
        "shipping": {
            "carrier": CARRIERS[rng.randrange(len(CARRIERS))],
            "tracking_number": f"TRK{order_id:012d}",
            "estimated_delivery": order_date + timedelta(days=rng.randrange(2, 10)),
        },
        "audit_log": audit_log,
        "status": STATUSES[rng.randrange(len(STATUSES))],
        "tags": rng.sample(
            ["priority", "gift", "b2b", "backorder", "loyalty"], k=rng.randrange(0, 3)
        ),
        "order_date": order_date,
        "created_at": order_date,
        "updated_at": order_date + timedelta(hours=rng.randrange(0, 500)),
        "notes": None,
        "metadata": {"source_system": "bench-data", "version": 1},
    }


def _batched(n_orders: int):
    rng = random.Random(4)
    batch = []
    for order_id in range(1, n_orders + 1):
        batch.append(_doc(rng, order_id, n_orders))
        if len(batch) >= BATCH_DOCS:
            yield batch
            batch = []
    if batch:
        yield batch


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=20_000_000)
    args = parser.parse_args()

    client = _connect()
    db = client.provisa_bench
    db.drop_collection("order_docs")
    coll = db.order_docs
    coll.create_index("order_id")
    coll.create_index("customer_id")

    t0 = time.monotonic()
    n = 0
    for batch in _batched(args.orders):
        coll.insert_many(batch, ordered=False)
        n += len(batch)
    print(f"order_docs: {n:,} docs in {time.monotonic() - t0:.0f}s")

    stats = db.command("collstats", "order_docs")
    size_gb = stats["size"] / (1024**3)
    print(f"order_docs size: {size_gb:.2f} GB at {HOST}:{PORT}")
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
