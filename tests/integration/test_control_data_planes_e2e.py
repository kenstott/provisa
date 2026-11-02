# Copyright (c) 2026 Kenneth Stott
# Canary: 5aa96a36-6a48-417d-af38-6736b83119b2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One control node and one data node, on one deployment (REQ-1916).

Two server processes on the same control plane and the same deployment key: a control node
(``PROVISA_MODE=control``, its plan listener on a port of its own) and a data node
(``PROVISA_MODE=data``, pointed at that listener). A GraphQL query and a pgwire query sent to the
data node are answered there: the data node asks the control node for each plan over the plane
socket, runs it, encodes the rows itself, and reports the statement's counters back, which the
control node writes to the audit log.

The control process never receives a row: its own data surfaces refuse every request by name, and
its plan listener is a Flight server that answers actions only (a plan, a description, a report,
a ping) -- it has no stream to receive or send rows on, which is asserted against the live
listener. The rows the data node answered are counted in the audit row the control node wrote
from the data node's report, and nowhere else.
"""

# Requirements: REQ-1916

from __future__ import annotations

import asyncio
import base64
import os
import secrets
import time

import httpx
import pytest

from tests.integration.isolated_server import IsolatedServer, drop_org_schema, free_port

pytestmark = [pytest.mark.integration]

_ORG = "control_data_planes"
_CONFIG = "tests/fixtures/sample_config.yaml"
_ROLE = "sales_analytics_analyst"


@pytest.fixture(scope="module")
def planes():
    key = (
        os.environ.get("PROVISA_ENCRYPTION_KEY")
        or base64.b64encode(secrets.token_bytes(32)).decode()
    )
    plane_port = free_port()
    control = IsolatedServer(
        _ORG,
        enable_pgwire=True,
        config=_CONFIG,
        env={
            "PROVISA_MODE": "control",
            "PROVISA_PLANE_PORT": str(plane_port),
            "PROVISA_ENCRYPTION_KEY": key,
        },
    )
    data = IsolatedServer(
        _ORG,
        enable_pgwire=True,
        config=_CONFIG,
        env={
            "PROVISA_MODE": "data",
            "PROVISA_CONTROL": f"127.0.0.1:{plane_port}",
            "PROVISA_ENCRYPTION_KEY": key,
        },
    )
    control.start()
    try:
        data.start()
        try:
            yield control, data, plane_port
        finally:
            data.stop_process()
    finally:
        control.stop_process()
        asyncio.run(drop_org_schema(_ORG))


def _client(srv: IsolatedServer) -> httpx.Client:
    return httpx.Client(base_url=srv.base_url, timeout=srv.request_timeout + 15)


def _pgwire(srv: IsolatedServer):
    import psycopg2

    return psycopg2.connect(
        host="127.0.0.1", port=srv.pgwire_port, dbname="provisa", user=_ROLE, password="provisa"
    )


def _orders_field(srv: IsolatedServer) -> str:
    with _client(srv) as c:
        resp = c.post(
            "/data/graphql",
            json={"query": "{ __schema { queryType { fields { name } } } }"},
            headers={"x-provisa-role": _ROLE},
        )
    assert resp.status_code == 200, resp.text
    names = [f["name"] for f in resp.json()["data"]["__schema"]["queryType"]["fields"]]
    (orders,) = [n for n in names if n.endswith("orders")]
    return orders


def _audited(srv: IsolatedServer, source: str, since: float) -> list[tuple]:
    """The audit rows written for ``source`` since ``since`` (epoch seconds), waiting for the
    writer (it inserts in batches)."""
    import sqlalchemy as sa

    from provisa.core.database import create_engine_from_url

    engine = create_engine_from_url(os.environ["TENANT_DATABASE_URL"])
    query = sa.text(
        f"SELECT route, row_count, model_stamp FROM org_{srv.org_id}.query_audit_log "
        "WHERE source = :source AND status_code = 200 AND logged_at >= to_timestamp(:since) "
        "ORDER BY id"
    )
    deadline = time.monotonic() + 60
    try:
        while True:
            with engine.connect() as conn:
                rows = [tuple(r) for r in conn.execute(query, {"source": source, "since": since})]
            if rows or time.monotonic() > deadline:
                return rows
            time.sleep(0.5)
    finally:
        engine.dispose()


def test_a_graphql_query_is_answered_by_the_data_node(planes):
    control, data, _ = planes
    since = time.time()
    field = _orders_field(data)
    with _client(data) as c:
        resp = c.post(
            "/data/graphql",
            json={"query": f"{{ {field} {{ id region }} }}"},
            headers={"x-provisa-role": _ROLE},
        )
    assert resp.status_code == 200, resp.text
    rows = resp.json()["data"][field]
    assert rows and all({"id", "region"} <= set(r) for r in rows)
    # The control node wrote the request's audit row from the data node's report: its counters.
    audited = _audited(control, "http", since)
    assert any(row_count == len(rows) for _route, row_count, _stamp in audited), audited


def test_a_pgwire_query_is_answered_by_the_data_node(planes):
    control, data, _ = planes
    since = time.time()
    conn = _pgwire(data)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, region FROM sa__orders ORDER BY id")
            rows = cur.fetchall()
    finally:
        conn.close()
    assert rows
    audited = _audited(control, "pgwire", since)
    assert any(row_count == len(rows) for _route, row_count, _stamp in audited), audited
    # REQ-1914: the plan carried the model stamp it was governed under into the audit row.
    assert all(stamp is not None for _route, _count, stamp in audited), audited


def test_the_control_node_refuses_data_requests(planes):
    control, _, _ = planes
    with _client(control) as c:
        resp = c.post(
            "/data/graphql", json={"query": "{ __typename }"}, headers={"x-provisa-role": _ROLE}
        )
    assert resp.status_code == 503 and resp.json()["code"] == "node.control_serves_no_data"
    import psycopg2

    with pytest.raises(psycopg2.Error, match="control mode"):
        conn = _pgwire(control)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM sa__orders")
        finally:
            conn.close()


def test_the_plan_listener_carries_no_rows(planes):
    """The control node's plan listener answers actions and nothing else: there is no stream on
    it to carry a row in either direction."""
    from pyarrow import flight

    _, _, plane_port = planes
    client = flight.FlightClient(f"grpc://127.0.0.1:{plane_port}")
    actions = {a.type for a in client.list_actions()}
    assert actions == {
        "govern_and_route",
        "govern_and_route_compiled",
        "describe",
        "report",
        "ping",
    }
    with pytest.raises(NotImplementedError):
        list(client.list_flights())
    with pytest.raises(NotImplementedError):
        client.do_get(flight.Ticket(b"sa__orders")).read_all()
