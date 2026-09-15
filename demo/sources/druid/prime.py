# Copyright (c) 2026 Kenneth Stott
# Canary: 4f3a5b6a-2e6f-4e4e-8f1a-0d5c9b4f7a2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Prime the optional Druid demo source: ingest 3 rows into the `widgets` datasource via Druid's
native batch ingestion REST API (submitted to the overlord, i.e. the coordinator with
asOverlord.enabled=true), then wait for the segment to be loaded and queryable through the broker.

No python druid client needed — the REST API is plain JSON over HTTP (stdlib urllib), mirroring
tests/integration/test_druid_source_e2e.py's `_seed_druid()` exactly (that test is this fixture's
proof it works end to end through Trino's druid connector).

``PROVISA_DEMO_DRUID_COORD_PORT``/``PROVISA_DEMO_DRUID_BROKER_PORT`` are the host-published ports
this compose file's own env vars publish (see compose.yml's module comment for why they exist —
seeding only, Trino never uses them).
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
from os import environ

_COORD_PORT = int(environ.get("PROVISA_DEMO_DRUID_COORD_PORT", "23221"))
_BROKER_PORT = int(environ.get("PROVISA_DEMO_DRUID_BROKER_PORT", "23222"))
_DATASOURCE = "widgets"
_WIDGETS = [("1", "Widget A"), ("2", "Widget B"), ("3", "Widget C")]


def _http_json(url: str, payload: dict | None = None) -> dict | list:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(  # noqa: S310 - fixed localhost URL to this demo's own container
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST" if data is not None else "GET",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - localhost only
        body = resp.read().decode()
    return json.loads(body) if body else {}


def _druid_sql(query: str) -> list:
    url = f"http://localhost:{_BROKER_PORT}/druid/v2/sql"
    try:
        result = _http_json(url, {"query": query})
    except urllib.error.HTTPError:
        return []  # datasource not queryable yet (segments not loaded)
    return result if isinstance(result, list) else []


def main() -> int:
    coord = f"http://localhost:{_COORD_PORT}"
    rows = "\n".join(
        json.dumps({"ts": "2020-01-01T00:00:00Z", "id": wid, "name": name})
        for wid, name in _WIDGETS
    )
    task = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": _DATASOURCE,
                "timestampSpec": {"column": "ts", "format": "iso"},
                "dimensionsSpec": {"dimensions": ["id", "name"]},
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "day",
                    "queryGranularity": "none",
                    "rollup": False,
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {"type": "inline", "data": rows},
                "inputFormat": {"type": "json"},
            },
            "tuningConfig": {"type": "index_parallel", "maxRowsInMemory": 25000},
        },
    }
    submit = _http_json(f"{coord}/druid/indexer/v1/task", task)
    assert isinstance(submit, dict)
    task_id = submit["task"]  # KeyError here = the overlord rejected the task; no silent fallback

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        status = _http_json(f"{coord}/druid/indexer/v1/task/{task_id}/status")
        assert isinstance(status, dict)
        code = status["status"]["statusCode"]
        if code == "SUCCESS":
            break
        if code == "FAILED":
            print(f"Druid ingestion task {task_id} FAILED: {status!r}", file=sys.stderr)
            return 1
        time.sleep(5)
    else:
        print(f"Druid ingestion task {task_id} did not finish within 180s", file=sys.stderr)
        return 1

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        rows_out = _druid_sql(f"SELECT COUNT(*) AS c FROM {_DATASOURCE}")
        if rows_out and rows_out[0].get("c") == len(_WIDGETS):
            print(f"druid demo source primed: {len(_WIDGETS)} widgets in {_DATASOURCE}")
            return 0
        time.sleep(3)
    print("Druid datasource never became queryable within 120s after ingestion", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
