# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1d0a2e-91f4-4c77-8f3a-2d5e0c9a71bb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1159 cross-surface contract: an INLINE-composed command works on EVERY raw-SQL surface.

REQ-1159 requires that a registered command composed inline within a larger statement
(joined/sub-queried, not a standalone ``SELECT * FROM fn(args)``) works across ALL
client-facing surfaces, routed through the ONE shared inline-localization pass
(``_localize_inline_commands`` in provisa/pgwire/_pipeline.py).

The surfaces that route SQL through the ONE governed pipeline (_govern_and_route →
_execute_plan) are pinned here against ONE server + dataset, running the SAME composed
JOIN over ``enrich_orders`` and asserting the same enriched rows:

  * pgwire       — SQL over the Postgres wire (psycopg2)
  * flight       — SQL over the app's OWN governed Arrow Flight server
  * rest_sql     — SQL over HTTP (POST /data/sql) — now on the one pipeline (the drifted
                   _compile_govern_execute has been deleted)

  * MCP run_sql  — same ``_govern_and_route`` path as pgwire; unit-pinned in
                   tests/unit/test_mcp_server.py::test_run_sql_composed_command_routes_to_pipeline

Surfaces with no raw-SQL inline-composition entry point (N/A for REQ-1159):

  * gRPC         — lowers a typed proto request to SQL via _govern_and_route_compiled;
                   no raw-SQL string entry, so inline composition cannot be expressed
  * Bolt/Cypher  — speaks Cypher; a command is invoked standalone via CALL (REQ-1156),
                   there is no syntax to compose it inline into a SQL SELECT
  * GraphQL      — commands surface as generated fields, not raw SQL
"""

from __future__ import annotations

import json

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration]

_ORG = "cmd_compose_surfaces"
_ROLE = "admin"

# Known seed rows of public.orders (db/init.sql): id -> region.
_ORDER_REGION = {1: "us-east", 2: "us-east", 3: "us-west", 4: "eu-west", 5: "us-east"}

# The composed statement — the command JOINed INLINE with the source table (the point of REQ-1159).
_COMPOSED_SQL = (
    "SELECT o.id, e.score, e.region_label "
    "FROM sa__orders o JOIN enrich_orders('sales-pg.public.orders') e ON o.id = e.id "
    "ORDER BY o.id LIMIT 3"
)


def _score(order_id: int) -> float:
    return round(((order_id * 37) % 100) / 100.0, 2)


def _expected() -> list[tuple]:
    return [(oid, _score(oid), f"R-{_ORDER_REGION[oid]}") for oid in (1, 2, 3)]


@pytest_asyncio.fixture(scope="module")
async def server():
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    srv = IsolatedServer(
        _ORG,
        engine="trino",
        enable_pgwire=True,
        await_flight=True,
        config="tests/fixtures/sample_config.yaml",
        control_plane="sqlite",
    )
    srv.start()
    try:
        yield srv
    finally:
        srv.stop_process()
        await drop_org_schema(_ORG)


# --------------------------------------------------------------------------- #
# Per-surface readers — each submits _COMPOSED_SQL and returns normalized rows.
# --------------------------------------------------------------------------- #
def _read_pgwire(srv) -> list[tuple]:
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1", port=srv.pgwire_port, dbname="provisa", user=_ROLE, password="provisa"
    )
    try:
        cur = conn.cursor()
        cur.execute(_COMPOSED_SQL)
        return [(int(r[0]), float(r[1]), r[2]) for r in cur.fetchall()]
    finally:
        conn.close()


def _read_flight(srv) -> list[tuple]:
    import pyarrow.flight as flight

    client = flight.FlightClient(f"grpc://127.0.0.1:{srv.flight_port}")
    ticket = flight.Ticket(json.dumps({"query": _COMPOSED_SQL, "role": _ROLE}).encode())
    try:
        table = client.do_get(ticket).read_all()
    finally:
        client.close()
    rows = table.to_pylist()
    return [(int(r["id"]), float(r["score"]), r["region_label"]) for r in rows]


def _read_rest(srv) -> list[tuple]:
    import httpx

    with httpx.Client(base_url=srv.base_url, timeout=60.0) as c:
        resp = c.post("/data/sql", json={"sql": _COMPOSED_SQL}, headers={"x-provisa-role": _ROLE})
    assert resp.status_code == 200, f"rest_sql: HTTP {resp.status_code} {resp.text[:300]}"
    body = resp.json()
    # /data/sql returns {"data": {"sql": [rowdict, ...]}}.
    data = body["data"] if isinstance(body, dict) and "data" in body else body
    rowdicts = next(iter(data.values())) if isinstance(data, dict) else data
    return [(int(r["id"]), float(r["score"]), r["region_label"]) for r in rowdicts]


_READERS = {"pgwire": _read_pgwire, "flight": _read_flight, "rest_sql": _read_rest}


@pytest.mark.parametrize("surface", list(_READERS), ids=list(_READERS))
def test_inline_composed_command_on_every_raw_sql_surface(server, surface):
    """The inline-composed command yields identical enriched rows on every raw-SQL surface.

    A surface that returns different rows — or hands the composed statement to the
    federation engine as an unknown table function (a silent fall-through) — fails here.
    """
    rows = _READERS[surface](server)
    assert rows == _expected(), f"{surface}: inline command composition diverged"
