# Copyright (c) 2026 Kenneth Stott
# Canary: 43e1b200-3aad-4e78-8e53-4b046a4f2ef8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: PgFederationRuntime federates a Postgres source in place via postgres_fdw (REQ-904).

Drives the runtime object directly — the NativeEngineBackend protocol (attach_source, run/run_sync,
connection, ensure_materialize_attached) — against a stock embedded PG with the contrib FDWs built in
(reuses the pgserver + FDW-build helpers proven by test_embedded_pg_fdw_engine_e2e). A green run proves
the pg engine's runtime, not just its connectors: a loopback Postgres source is imported as a foreign
schema, wrapped in a physical-named view, and a federated query returns its rows.
"""

from __future__ import annotations

import sys
import tempfile
from types import SimpleNamespace

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

asyncpg = pytest.importorskip("asyncpg")
pgserver = pytest.importorskip("pgserver")
psycopg2 = pytest.importorskip("psycopg2")

from provisa.federation.pg_runtime import PgFederationRuntime  # noqa: E402
from tests.integration.test_embedded_pg_fdw_engine_e2e import (  # noqa: E402
    _have_build_tools,
    _install_fdws_into_pgserver,
    _loopback_source_params,
)


@pytest.fixture(scope="session")
def pg_with_fdw():
    if sys.platform not in ("darwin", "linux") or not _have_build_tools():
        pytest.skip("needs a C toolchain (cc/make/tar + curl/wget) to build the contrib FDWs")
    base = tempfile.mkdtemp(prefix="provisa_pg_rt_")
    server = pgserver.get_server(base)
    _install_fdws_into_pgserver()
    yield server


async def test_pg_runtime_federates_postgres_source(pg_with_fdw):
    server = pg_with_fdw
    dsn = server.get_uri()

    # A loopback "remote" the pg runtime will attach via postgres_fdw: orders in demo_remote.
    conn = await asyncpg.connect(dsn=dsn)
    try:
        await conn.execute("CREATE SCHEMA IF NOT EXISTS demo_remote")
        await conn.execute("DROP TABLE IF EXISTS demo_remote.orders")
        await conn.execute("CREATE TABLE demo_remote.orders(id int, amount numeric)")
        await conn.execute("INSERT INTO demo_remote.orders VALUES (1,10.0),(2,20.0),(3,30.0)")
    finally:
        await conn.close()

    lp = _loopback_source_params(server)
    rt = PgFederationRuntime(engine_dsn=dsn)
    try:
        src = SimpleNamespace(
            id="ord",
            type=SimpleNamespace(value="postgresql"),
            host=lp["host"],
            port=lp["port"],
            database=lp["database"],
            username=lp["username"],
            password="",
            federation_hints={"schema": "demo_remote"},
            schema_name="sales",
            table_name="orders",
        )
        rt.attach_source(src)

        res = rt.run_sync('SELECT "id", "amount" FROM "sales"."orders" ORDER BY "id"')
        assert res.column_names == ["id", "amount"]
        assert len(res.rows) == 3
        assert [r[0] for r in res.rows] == [1, 2, 3]

        # the materialization-store reference for a pg engine is its own database name
        assert rt.ensure_materialize_attached()  # non-empty
    finally:
        rt.close()
