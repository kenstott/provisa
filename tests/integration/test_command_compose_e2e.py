# Copyright (c) 2026 Kenneth Stott
# Canary: 9f4c1e07-3b62-4d58-8a09-2e6d1f7c5b40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: an inline-composed command through the REAL Provisa server + engine (REQ-1159).

Starts a full Provisa server (Trino engine, pgwire enabled) on the sample-analytics config, which
registers the ``enrich_orders`` python command with an IR-typed input/output contract. Over the
pgwire wire we run a COMPOSED statement — a source table JOINed inline with the command — and assert
the joined+enriched rows. This exercises the whole path end to end: pgwire -> _govern_and_route ->
inline command localization -> the shared governed executor (input projection + contract validation +
python transform + output validation) -> typed VALUES substitution -> engine execution of the join.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration]

psycopg2 = pytest.importorskip("psycopg2")

_ORG = "cmd_compose_test"

# Known seed rows of public.orders (db/init.sql): id -> region.
_ORDER_REGION = {1: "us-east", 2: "us-east", 3: "us-west", 4: "eu-west", 5: "us-east"}


def _score(order_id: int) -> float:
    return round(((order_id * 37) % 100) / 100.0, 2)


@pytest_asyncio.fixture(scope="module")
async def server():
    from tests.integration.isolated_server import IsolatedServer, drop_org_schema

    srv = IsolatedServer(
        _ORG,
        engine="trino",
        enable_pgwire=True,
        config="tests/fixtures/sample_config.yaml",
    )
    srv.start()
    try:
        yield srv
    finally:
        srv.stop_process()
        await drop_org_schema(_ORG)


def _query(srv, sql: str) -> list[tuple]:
    conn = psycopg2.connect(
        host="127.0.0.1", port=srv.pgwire_port, dbname="provisa", user="admin", password="provisa"
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def test_standalone_command_returns_enriched_set(server):
    # The standalone short-circuit returns the command's full set (outer LIMIT is not pushed into it),
    # so assert the first rows in id order.
    rows = _query(server, "SELECT id, score, region_label FROM enrich_orders('sales-pg.public.orders')")
    by_id = {r[0]: r for r in rows}
    for oid in (1, 2, 3):
        assert by_id[oid] == (oid, _score(oid), f"R-{_ORDER_REGION[oid]}")


def test_composed_command_joined_inline(server):
    # The command is composed INLINE with the source table — the whole point of REQ-1159.
    rows = _query(
        server,
        "SELECT o.id, e.score, e.region_label "
        "FROM sa__orders o JOIN enrich_orders('sales-pg.public.orders') e ON o.id = e.id "
        "ORDER BY o.id LIMIT 3",
    )
    # pgwire returns text-format cells; coerce and compare the values (correctness, not wire type)
    typed = [(int(r[0]), float(r[1]), r[2]) for r in rows]
    assert typed == [
        (1, _score(1), f"R-{_ORDER_REGION[1]}"),
        (2, _score(2), f"R-{_ORDER_REGION[2]}"),
        (3, _score(3), f"R-{_ORDER_REGION[3]}"),
    ]
