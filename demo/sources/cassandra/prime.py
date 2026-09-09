# Copyright (c) 2026 Kenneth Stott
# Canary: 7f43d238-ae43-41d3-9dfd-af41258cd064
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Cassandra demo source: wait for the node, create the shelter_ops keyspace
and its intake_events table, replace the rows. Idempotent. Uses the cassandra-driver the
`cassandra` extra installs — the same reader the native engine uses (REQ-1676).
"""

from __future__ import annotations

import os
import sys
import time

PORT = int(os.environ.get("PROVISA_DEMO_CASSANDRA_PORT", "29042"))
KEYSPACE = "shelter_ops"
TABLE = "intake_events"
EVENTS = [
    (1, "2025-03-01", "intake", "Buddy", "dog"),
    (2, "2025-03-02", "vet_visit", "Buddy", "dog"),
    (3, "2025-03-02", "intake", "Mittens", "cat"),
    (4, "2025-03-04", "adoption", "Buddy", "dog"),
    (5, "2025-03-05", "intake", "Rex", "dog"),
    (6, "2025-03-06", "vet_visit", "Mittens", "cat"),
    (7, "2025-03-08", "adoption", "Mittens", "cat"),
]


def main() -> int:
    from cassandra.cluster import Cluster, NoHostAvailable

    deadline = time.monotonic() + 240
    while True:
        try:
            cluster = Cluster(contact_points=["localhost"], port=PORT, connect_timeout=10)
            session = cluster.connect()
            break
        except NoHostAvailable:
            if time.monotonic() > deadline:
                print(f"cassandra at localhost:{PORT} did not become ready", file=sys.stderr)
                return 1
            time.sleep(5)
    try:
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        session.execute(
            f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} "
            "(event_id int PRIMARY KEY, event_date text, event_type text, animal_name text, "
            "species text)"
        )
        session.execute(f"TRUNCATE {KEYSPACE}.{TABLE}")
        for row in EVENTS:
            session.execute(
                f"INSERT INTO {KEYSPACE}.{TABLE} (event_id, event_date, event_type, animal_name, "
                "species) VALUES (%s, %s, %s, %s, %s)",
                row,
            )
    finally:
        cluster.shutdown()
    print(
        f"cassandra demo source primed: {len(EVENTS)} events in {KEYSPACE}.{TABLE} at localhost:{PORT}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
