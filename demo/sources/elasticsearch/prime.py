# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Elasticsearch demo source: wait for the node, recreate the index with an
explicit mapping, bulk-load the tickets. Idempotent (delete + create). Same shape as the seed in
tests/integration/test_elasticsearch_pipeline_e2e.py.
"""

from __future__ import annotations

import json
import os
import sys
import time

import httpx

PORT = int(os.environ.get("PROVISA_DEMO_ES_PORT", "29200"))
BASE = f"http://localhost:{PORT}"
INDEX = "support_tickets"
MAPPING = {
    "mappings": {
        "properties": {
            "ticket_id": {"type": "keyword"},
            "subject": {"type": "text"},
            "status": {"type": "keyword"},
            "priority": {"type": "integer"},
            "opened_at": {"type": "date"},
        }
    }
}
TICKETS = [
    ("T-1001", "Adoption paperwork not received", "open", 2, "2025-03-01T09:15:00Z"),
    ("T-1002", "Wrong microchip number on record", "open", 1, "2025-03-02T14:40:00Z"),
    ("T-1003", "Question about vaccination schedule", "closed", 3, "2025-03-03T11:05:00Z"),
    ("T-1004", "Enclosure booking overlaps", "pending", 2, "2025-03-04T16:20:00Z"),
    ("T-1005", "Refund for cancelled adoption fee", "closed", 1, "2025-03-05T10:00:00Z"),
    ("T-1006", "Cannot log in to volunteer portal", "open", 2, "2025-03-06T08:30:00Z"),
]


def main() -> int:
    deadline = time.monotonic() + 180
    with httpx.Client(timeout=60) as client:
        while True:
            try:
                if client.get(f"{BASE}/_cluster/health").status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                print(f"elasticsearch at {BASE} did not become ready", file=sys.stderr)
                return 1
            time.sleep(3)
        client.delete(f"{BASE}/{INDEX}")
        resp = client.put(f"{BASE}/{INDEX}", json=MAPPING)
        if resp.status_code != 200:
            raise SystemExit(f"create index: HTTP {resp.status_code}: {resp.text}")
        lines: list[str] = []
        for ticket_id, subject, status, priority, opened_at in TICKETS:
            lines.append('{"index":{}}')
            lines.append(
                json.dumps(
                    {
                        "ticket_id": ticket_id,
                        "subject": subject,
                        "status": status,
                        "priority": priority,
                        "opened_at": opened_at,
                    }
                )
            )
        resp = client.post(
            f"{BASE}/{INDEX}/_bulk?refresh=wait_for",
            content="\n".join(lines) + "\n",
            headers={"Content-Type": "application/x-ndjson"},
        )
        if resp.status_code != 200 or resp.json().get("errors"):
            raise SystemExit(f"bulk: HTTP {resp.status_code}: {resp.text}")
    print(f"elasticsearch demo source primed: {len(TICKETS)} tickets in {INDEX} at {BASE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
