# Copyright (c) 2026 Kenneth Stott
# Canary: 77a6430d-b86b-461e-9242-467af58c28ee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Elasticsearch read over HTTP against a live node (REQ-1672): what Register Table lists and
types on a native engine, and the rows the landing loader produces — no Trino anywhere."""

from __future__ import annotations

import os
from types import SimpleNamespace

import httpx
import pytest

from provisa.elasticsearch import fetch as es
from provisa.events.source_loader import make_elasticsearch_loader

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_elasticsearch,
    pytest.mark.asyncio(loop_scope="session"),
]

_INDEX = "native_fetch_tickets"
_TICKETS = [("T-1", "open", 2, "Ann"), ("T-2", "closed", 1, "Bo"), ("T-3", "open", 3, "Cy")]


def _port() -> int:
    return int(os.environ["ELASTICSEARCH_PORT"])


@pytest.fixture(autouse=True)
def _seed():
    base = f"http://localhost:{_port()}"
    with httpx.Client(timeout=60) as c:
        c.delete(f"{base}/{_INDEX}")
        resp = c.put(
            f"{base}/{_INDEX}",
            json={
                "mappings": {
                    "properties": {
                        "ticket_id": {"type": "keyword"},
                        "status": {"type": "keyword"},
                        "priority": {"type": "integer"},
                        "reporter": {"properties": {"name": {"type": "text"}}},
                    }
                }
            },
        )
        assert resp.status_code == 200, resp.text
        lines = []
        for tid, status, prio, who in _TICKETS:
            lines.append('{"index":{}}')
            lines.append(
                f'{{"ticket_id":"{tid}","status":"{status}","priority":{prio},'
                f'"reporter":{{"name":"{who}"}}}}'
            )
        resp = c.post(
            f"{base}/{_INDEX}/_bulk",
            params={"refresh": "wait_for"},
            content="\n".join(lines) + "\n",
            headers={"Content-Type": "application/x-ndjson"},
        )
        assert resp.status_code == 200 and not resp.json()["errors"], resp.text
        yield
        c.delete(f"{base}/{_INDEX}")


async def test_indices_and_mapping_columns_come_from_the_node():
    conn = es.ESConnection.build("localhost", _port())
    assert _INDEX in es.list_indices(conn)
    cols = {c["name"]: (c["type"], c["sourcePath"]) for c in es.index_columns(conn, _INDEX)}
    assert cols["priority"] == ("INTEGER", "priority")
    assert cols["reporter_name"] == ("VARCHAR", "reporter.name")


async def test_loader_lands_every_document_with_nested_paths_resolved():
    source = SimpleNamespace(
        id="es_native", host="localhost", port=_port(), username="", password="", mapping={}
    )
    table = SimpleNamespace(
        table_name=_INDEX,
        columns=[
            SimpleNamespace(name=n, native_filter_type=None)
            for n in ("ticket_id", "status", "priority", "reporter_name")
        ],
    )
    rows = await make_elasticsearch_loader()(source, table)
    rows.sort(key=lambda r: r["ticket_id"])
    assert [
        (r["ticket_id"], r["status"], r["priority"], r["reporter_name"]) for r in rows
    ] == _TICKETS
