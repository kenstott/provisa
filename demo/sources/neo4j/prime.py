# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Neo4j demo source: wait for the container, wipe, apply seed.cypher.

Run by start-ui-install.sh --demo --source=neo4j after the compose project is healthy. Idempotent:
the wipe makes every run land exactly the seed graph. Talks to the HTTP transaction API the same
way provisa-ui/e2e/neo4j-docker-export.spec.ts does.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import httpx

PORT = int(os.environ.get("PROVISA_DEMO_NEO4J_HTTP_PORT", "27474"))
BASE = f"http://localhost:{PORT}"
TX = f"{BASE}/db/neo4j/tx/commit"
SEED = Path(__file__).with_name("seed.cypher")


def _run(client: httpx.Client, *statements: str) -> None:
    resp = client.post(TX, json={"statements": [{"statement": s} for s in statements]})
    resp.raise_for_status()
    errors = resp.json().get("errors", [])
    if errors:
        raise SystemExit(f"neo4j seed failed: {errors}")


def main() -> int:
    deadline = time.monotonic() + 120
    with httpx.Client(timeout=30) as client:
        while True:
            try:
                _run(client, "RETURN 1")
                break
            except (httpx.HTTPError, SystemExit):
                if time.monotonic() > deadline:
                    print(f"neo4j at {BASE} did not become ready", file=sys.stderr)
                    return 1
                time.sleep(2)
        _run(client, "MATCH (n) DETACH DELETE n")
        seed = "\n".join(
            line for line in SEED.read_text().splitlines() if not line.startswith("//")
        )
        _run(client, seed)
        resp = client.post(
            TX,
            json={"statements": [{"statement": "MATCH (a:Adopter) RETURN count(a) AS n"}]},
        )
        n = resp.json()["results"][0]["data"][0]["row"][0]
    print(f"neo4j demo source primed: {n} adopters at {BASE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
