# Copyright (c) 2026 Kenneth Stott
# Canary: 03fb39f4-5964-43e3-891e-0a97c38cd135
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional SPARQL demo source: wait for Fuseki, clear the dataset, load the volunteer
graph through SPARQL Update. Idempotent. The same endpoint the native reader queries (REQ-1683).
"""

from __future__ import annotations

import os
import sys
import time

import httpx

PORT = int(os.environ.get("PROVISA_DEMO_SPARQL_PORT", "23030"))
BASE = f"http://localhost:{PORT}/provisa"
PREFIX = "PREFIX s: <http://provisa.dev/shelter#>"
VOLUNTEERS = [
    ("V-01", "Grace Hall", "adoption", "2023-02-14"),
    ("V-02", "Omar Reyes", "fostering", "2023-05-01"),
    ("V-03", "Priya Nair", "transport", "2023-09-18"),
    ("V-04", "Leo Baptiste", "events", "2024-01-09"),
    ("V-05", "Mia Sato", "adoption", "2024-03-22"),
    ("V-06", "Noah Bryce", "fostering", "2024-07-30"),
]


def _triples() -> str:
    parts = []
    for vid, name, program, since in VOLUNTEERS:
        parts.append(
            f'<http://provisa.dev/volunteer/{vid}> a s:Volunteer ; s:id "{vid}" ; '
            f's:name "{name}" ; s:program "{program}" ; s:since "{since}" .'
        )
    return "\n".join(parts)


def main() -> int:
    deadline = time.monotonic() + 90
    with httpx.Client(timeout=30) as client:
        while True:
            try:
                if client.get(f"{BASE}/query", params={"query": "ASK {}"}).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                print(f"fuseki at {BASE} did not become ready", file=sys.stderr)
                return 1
            time.sleep(2)
        for update in ("CLEAR ALL", f"{PREFIX} INSERT DATA {{ {_triples()} }}"):
            resp = client.post(f"{BASE}/update", data={"update": update})
            if resp.status_code not in (200, 204):
                raise SystemExit(f"sparql update failed: HTTP {resp.status_code}: {resp.text}")
    print(f"sparql demo source primed: {len(VOLUNTEERS)} volunteers at {BASE}/query")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
