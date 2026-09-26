# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Seeds all 4 perf-demo sources as part of `docker compose up` (the `seeder` service in
docker-compose.yml runs this once the other 4 services report healthy). Idempotent via a marker
file on the bind-mounted ./data volume: multi-GB generation must never re-run on a container
restart, unlike the toy demo/sources/<name>/prime.py scripts, which re-seed every start because
their datasets are a handful of rows.

Row counts are configurable via PROVISA_BENCH_ORDERS (shared order_id domain across postgres/
clickhouse/mongodb — the 3 LIVE sources, default 20,000,000) and PROVISA_BENCH_ITEMS_PER_ORDER
(default 4) so a smaller/larger perf run doesn't require editing the generate_*.py scripts.

Neo4j's order count is DELIBERATELY separate (PROVISA_BENCH_NEO4J_ORDERS, default 2,000,000):
it's a replica-only source (materialize/cache pattern — see fragment.yaml's header and REQ-1858),
never queried live at request time the way the other 3 are, so what the materialize-cost
benchmark needs is a table of realistic SIZE, not full order_id-domain coverage — testing that
cost at 1/10th the scale takes a fraction of the wall-clock time (Neo4j's HTTP-transaction-batch
loading is the slowest of the 4 steps by far). Trade-off: bench-neo4j's order_id domain is a
PREFIX of the other 3 sources' (1..NEO4J_ORDERS, not 1..ORDERS), so cross-engine joins against it
only match within that prefix — intentional, not a bug, for the reason above.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
MARKER = Path("/data-marker/.seeded")
ORDERS = os.environ.get("PROVISA_BENCH_ORDERS", "20000000")
NEO4J_ORDERS = os.environ.get("PROVISA_BENCH_NEO4J_ORDERS", "2000000")
ITEMS_PER_ORDER = os.environ.get("PROVISA_BENCH_ITEMS_PER_ORDER", "4")

STEPS = [
    ["generate_postgres.py", "--orders", ORDERS, "--items-per-order", ITEMS_PER_ORDER],
    ["generate_clickhouse.py", "--orders", ORDERS],
    ["generate_mongo.py", "--orders", ORDERS],
    ["generate_neo4j.py", "--orders", NEO4J_ORDERS, "--items-per-order", ITEMS_PER_ORDER],
]


def main() -> int:
    if MARKER.exists():
        print(f"perf demo already seeded ({MARKER.read_text().strip()}); skipping")
        return 0

    t0 = time.monotonic()
    for step in STEPS:
        script = HERE / step[0]
        print(f"=== {step[0]} ===", flush=True)
        subprocess.run([sys.executable, str(script), *step[1:]], check=True)

    MARKER.parent.mkdir(parents=True, exist_ok=True)
    MARKER.write_text(
        f"seeded in {time.monotonic() - t0:.0f}s, orders={ORDERS}, neo4j_orders={NEO4J_ORDERS}\n"
    )
    print(f"perf demo seed complete in {time.monotonic() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
