# Copyright (c) 2026 Kenneth Stott
# Canary: e71ca380-88ef-4297-b890-1ff16ce8d88d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Redis demo source: wait for the server, wipe the demo prefixes, write the
support-desk hashes. Idempotent. Keys follow the connector convention <table>:<id>, so each prefix
is a table in Register Table (REQ-1675).
"""

from __future__ import annotations

import os
import sys
import time

import redis

PORT = int(os.environ.get("PROVISA_DEMO_REDIS_PORT", "26379"))

AGENTS = [
    (1, "Ann Lee", "tier1", "day"),
    (2, "Bo Chen", "tier1", "night"),
    (3, "Cy Park", "tier2", "day"),
    (4, "Dee Ruiz", "tier2", "night"),
    (5, "Eli Stone", "escalations", "day"),
]
STATUS = [
    (1, "available", "0"),
    (2, "busy", "3"),
    (3, "available", "1"),
    (4, "away", "0"),
    (5, "busy", "5"),
]


def main() -> int:
    deadline = time.monotonic() + 60
    client = redis.Redis(host="localhost", port=PORT, decode_responses=True, socket_timeout=5)
    while True:
        try:
            client.ping()
            break
        except redis.RedisError:
            if time.monotonic() > deadline:
                print(f"redis at localhost:{PORT} did not become ready", file=sys.stderr)
                return 1
            time.sleep(2)
    for prefix in ("support_agent", "agent_status"):
        keys = list(client.scan_iter(match=f"{prefix}:*"))
        if keys:
            client.delete(*keys)
    for agent_id, name, team, shift in AGENTS:
        client.hset(
            f"support_agent:{agent_id}",
            mapping={"agent_id": str(agent_id), "name": name, "team": team, "shift": shift},
        )
    for agent_id, status, open_tickets in STATUS:
        client.hset(
            f"agent_status:{agent_id}",
            mapping={"agent_id": str(agent_id), "status": status, "open_tickets": open_tickets},
        )
    print(f"redis demo source primed: {len(AGENTS)} agents at localhost:{PORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
