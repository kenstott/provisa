# Copyright (c) 2026 Kenneth Stott
# Canary: e86f07e0-4732-4d3a-b996-27be5c83ad25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: SqlAlchemyFederationRuntime executes governed SQL against its self-only store (REQ-905).

Drives the runtime object directly — the NativeEngineBackend execution protocol (attach_source,
run/run_sync, ensure_materialize_attached) — against an embedded PostgreSQL reached via SQLAlchemy.
A green run proves the self-only engine's runtime: a landed table in the store is queried through the
runtime, and attach_source is the expected no-op (self-only lands, never attaches in place).
"""

from __future__ import annotations

import tempfile
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

pgserver = pytest.importorskip("pgserver")
pytest.importorskip("sqlalchemy")
pytest.importorskip("psycopg2")

from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime  # noqa: E402


@pytest.fixture(scope="session")
def sa_store():
    base = tempfile.mkdtemp(prefix="provisa_sa_")
    yield pgserver.get_server(base)


def _sa_url(server) -> str:
    u = urlparse(server.get_uri())
    q = parse_qs(u.query)
    host = q["host"][0] if "host" in q else (u.hostname or "")
    db = u.path.lstrip("/") or "postgres"
    user = u.username or "postgres"
    return f"postgresql+psycopg2://{user}@/{db}?host={host}"


async def test_sqlalchemy_runtime_executes_on_store(sa_store):
    rt = SqlAlchemyFederationRuntime(url=_sa_url(sa_store))
    try:
        rt.run_sync('CREATE SCHEMA IF NOT EXISTS "sales"')
        rt.run_sync('DROP TABLE IF EXISTS "sales"."orders"')
        rt.run_sync('CREATE TABLE "sales"."orders"(id int, amount numeric)')
        rt.run_sync('INSERT INTO "sales"."orders" VALUES (1,10),(2,20),(3,30)')

        res = rt.run_sync('SELECT "id" FROM "sales"."orders" ORDER BY "id"')
        assert [r[0] for r in res.rows] == [1, 2, 3]

        # self-only: attach is a no-op (must not raise); the store name is the cache reference.
        rt.attach_source(SimpleNamespace(id="x", schema_name="sales", table_name="orders"))
        assert rt.ensure_materialize_attached()
    finally:
        rt.close()
