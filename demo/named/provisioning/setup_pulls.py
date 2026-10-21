# Copyright (c) 2026 Kenneth Stott
# Canary: 618a7b71-78c1-4325-9179-8d932dde4713
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
#
"""The provisioning demo's setup client (setup.yaml; scripts/run_demo_setup.py runs it against the
server the setup is run on): one notice and one pull for each team, recorded in the warehouse, so a
start the consumers do not run on -- a hosted sandbox -- still shows what a pull is.

It waits until the month of orders has landed and the dataset is built from it, writes a notice
for each team as the scheduled triggers do, and runs the consumer (consumer.py --notices 2) until
it has pulled for both, as of each notice's time.

Environment: PROVISA_URL and PROVISA_FLIGHT (the server, from run_demo_setup.py); the stores'
addresses as config.yaml reads them (PROVISIONING_PG_*, PROVISIONING_KAFKA_*).
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import httpx

URL = os.environ["PROVISA_URL"]
ADMIN = {"X-Provisa-Role": "org_admin"}
#: The dataset is ready to pull once it holds this many orders of the month's history.
READY_ROWS = 10000
WAIT_S = 900


def _sql(client: httpx.Client, sql: str) -> list[dict]:
    response = client.post("/data/sql", json={"sql": sql})
    if response.status_code != 200:
        raise SystemExit(f"{sql}: HTTP {response.status_code}: {response.text}")
    return response.json()["data"]["sql"]


def _dataset_built(client: httpx.Client) -> bool:
    """Whether order_margins is built and holds the month's history. It is refreshed by deltas, and
    a delta refresh does not know the view's whole count (mvList's rowCount stays empty), so the
    rows are counted by reading the view once it is fresh."""
    response = client.post("/admin/graphql", json={"query": "{ mvList { id status } }"})
    response.raise_for_status()
    [view] = [m for m in response.json()["data"]["mvList"] if m["id"] == "view-view_order_margins"]
    if view["status"] != "fresh":
        return False
    [held] = _sql(client, 'SELECT COUNT(*) AS n FROM "provisioning"."view_order_margins"')
    return held["n"] >= READY_ROWS


def main() -> None:
    with httpx.Client(base_url=URL, headers=ADMIN, timeout=300) as client:
        deadline = time.monotonic() + WAIT_S
        while not _dataset_built(client):
            if time.monotonic() > deadline:
                raise SystemExit(f"order_margins did not reach {READY_ROWS} rows in {WAIT_S}s")
            time.sleep(5)
        for team in ("finance", "data_science"):
            _sql(
                client,
                'INSERT INTO "provisioning"."notices" (dataset, consumer) '
                f"VALUES ('order_margins', '{team}')",
            )
    # The stores where config.yaml reads them: its variables, else its own defaults (a local start
    # sets none; the bundle build sets the ports it ran the stores on).
    pg_host = os.environ.get("PROVISIONING_PG_HOST", "localhost")
    pg_port = os.environ.get("PROVISIONING_PG_PORT", "28032")
    kafka_host = os.environ.get("PROVISIONING_KAFKA_HOST", "127.0.0.1")
    kafka_port = os.environ.get("PROVISIONING_KAFKA_PORT", "29192")
    env = {
        **os.environ,
        "FEEDS_BOOTSTRAP": f"{kafka_host}:{kafka_port}",
        "CONSUMERS": "finance,data_science",
        "PULLS_DSN": f"postgresql://consumer:consumer@{pg_host}:{pg_port}/provisioning",
        # The topics' suffix, as the run compose hands it the consumer: its broker's namespace,
        # which every runner sets (store_namespaces; empty on the demo's own broker).
        "TOPIC_SUFFIX": os.environ["PROVISIONING_KAFKA_NS"],
    }
    consumer = Path(__file__).with_name("consumer.py")
    subprocess.run(
        [sys.executable, str(consumer), "--notices", "2"], env=env, check=True, timeout=WAIT_S
    )


if __name__ == "__main__":
    main()
