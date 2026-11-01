# Copyright (c) 2026 Kenneth Stott
# Canary: 2e9c7b14-58d3-4f06-b1a7-6f4e0d9a3c82
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""What a seed image holds against what its compose defaults state (REQ-1911, ../METHODOLOGY.md).

The seed hash says which inputs made an image. This says the image holds what they make: on device
3, after the first boot from it, every store is counted against the seeder's scale settings. Any
difference fails the session before a unit runs.

    python seed_counts.py --compose ../docker-compose.yml --host 10.90.0.4 --out ~/seed-counts.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import httpx
import psycopg
import yaml

# Each generator's rule: orders // 8 customers, one document an order, a mean of 3 events an order.
CUSTOMER_DIVISOR = 8
EVENTS_PER_ORDER = 3
# Items and events per order are drawn from 1..2n-1, so n is their mean, not each order's count.
PER_ORDER_TOLERANCE = 0.02


def _default(value: str) -> int:
    """``${NAME:-20000000}`` to 20000000."""
    return int(value.rsplit(":-", 1)[1].rstrip("}"))


def stated(compose_text: str) -> dict[str, int]:
    """The seeder's scale settings at their compose defaults."""
    env = yaml.safe_load(compose_text)["services"]["seeder"]["environment"]
    return {
        "orders": _default(env["PROVISA_BENCH_ORDERS"]),
        "neo4j_orders": _default(env["PROVISA_BENCH_NEO4J_ORDERS"]),
        "items_per_order": _default(env["PROVISA_BENCH_ITEMS_PER_ORDER"]),
    }


def differences(scale: Mapping[str, int], counts: Mapping[str, Any]) -> list[str]:
    """Each count that is not what the scale settings make."""
    orders, neo4j_orders = scale["orders"], scale["neo4j_orders"]
    exact = {
        "postgres_orders": orders,
        "mongo_documents": orders,
        "neo4j_orders": neo4j_orders,
        "neo4j_customers": neo4j_orders // CUSTOMER_DIVISOR,
    }
    out = [
        f"{name}: {counts[name]}, stated {want}"
        for name, want in exact.items()
        if counts[name] != want
    ]
    if counts["postgres_max_customer"] > orders // CUSTOMER_DIVISOR:
        out.append(
            f"postgres_max_customer: {counts['postgres_max_customer']}, "
            f"stated at most {orders // CUSTOMER_DIVISOR}"
        )
    means = (
        ("items", counts["postgres_items"], scale["items_per_order"]),
        ("events", counts["clickhouse_events"], EVENTS_PER_ORDER),
    )
    for name, total, want in means:
        per_order = total / counts["postgres_orders"]
        if abs(per_order - want) > want * PER_ORDER_TOLERANCE:
            out.append(f"{name} per order: {per_order:.3f}, stated {want}")
    return out


def count(host: str) -> dict[str, Any]:
    """Every store's count, asked of the store."""
    url = f"host={host} port=25632 user=provisa password=provisa dbname=provisa_bench"
    with psycopg.connect(url, autocommit=True) as pg:
        row = pg.execute("SELECT count(*), max(customer_id) FROM orders").fetchone()
        items = pg.execute("SELECT count(*) FROM order_items").fetchone()[0]
    clickhouse = httpx.post(
        f"http://{host}:28623/",
        params={"user": "default", "password": "provisa"},
        content="SELECT count() FROM order_events",
        timeout=600,
    ).raise_for_status()
    neo4j = httpx.post(
        f"http://{host}:27974/db/neo4j/tx/commit",
        json={
            "statements": [
                {"statement": "MATCH (o:Order) RETURN count(o)"},
                {"statement": "MATCH (c:Customer) RETURN count(c)"},
            ]
        },
        timeout=600,
    ).raise_for_status()
    answers = [r["data"][0]["row"][0] for r in neo4j.json()["results"]]
    mongo = subprocess.run(
        [
            "sudo",
            "docker",
            "exec",
            "perf-mongodb-1",
            "mongosh",
            "--quiet",
            "--eval",
            'db.getSiblingDB("provisa_bench").order_docs.estimatedDocumentCount()',
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return {
        "postgres_orders": row[0],
        "postgres_max_customer": row[1],
        "postgres_items": items,
        "clickhouse_events": int(clickhouse.text),
        "mongo_documents": int(mongo.stdout.strip()),
        "neo4j_orders": answers[0],
        "neo4j_customers": answers[1],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="A seed image's counts against its compose")
    parser.add_argument("--compose", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    scale = stated(Path(args.compose).read_text())
    counts = count(args.host)
    wrong = differences(scale, counts)
    result = {"stated": scale, "counts": counts, "differences": wrong}
    Path(args.out).expanduser().write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    return 1 if wrong else 0


if __name__ == "__main__":
    raise SystemExit(main())
