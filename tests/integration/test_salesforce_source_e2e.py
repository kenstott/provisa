# Copyright (c) 2026 Kenneth Stott
# Canary: 6d0e3a59-4f17-4b82-9c5d-a2e8f1b7c360
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live Salesforce source (REQ-1946): read and written through its pgwire server, read through
Trino's ``salesforce`` catalog.

Salesforce has no direct driver of its own. A ``Source`` row (type=salesforce) carries:

  - ``base_url`` (or ``host``) -> the org's My Domain login URL
  - ``username`` / ``password`` -> the connected app's consumer key / consumer secret
  - ``mapping.auth_type`` -> ``CLIENT_CREDENTIALS`` (default), ``USERNAME_PASSWORD`` or
    ``ACCESS_TOKEN``, with that set's own mapping keys
  - ``mapping.api_version`` -> optional

``provisa.federation.pgwire_replica`` writes the server's model from those fields and starts the
bundled ``pgwire-salesforce`` server from the source's own state directory; every engine but Trino
attaches it, and every engine's writes run on it. ``TrinoSalesforceConnector.details()`` builds the
``salesforce`` catalog Trino reads through.

Credentials
-----------
There is no Salesforce emulator: a real org and a connected app with the client-credentials flow
enabled are required, so this runs in the warehouse lane. It reads SF_LOGIN_URL, SF_CONSUMER_KEY
and SF_CONSUMER_SECRET from the lane's environment. The write test creates, changes and deletes
one Account whose name carries a random suffix; it leaves nothing behind when it passes.

The first start of the server describes every sObject of the org before it listens (minutes on a
large org); the describe cache in the source's state directory makes later starts quick.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_salesforce,
    pytest.mark.requires_warehouse,
]

_aio = pytest.mark.asyncio(loop_scope="session")

_SOURCE_ID = "salesforce-itest"
_SCHEMA = _SOURCE_ID.replace("-", "_")
# The describe of a large org runs for minutes before the server listens.
_SERVER_READY_SECONDS = 900


def _source():
    from provisa.core.models import Source, SourceType

    return Source(
        id=_SOURCE_ID,
        type=SourceType.salesforce,
        base_url=os.environ["SF_LOGIN_URL"],
        username=os.environ["SF_CONSUMER_KEY"],
        password=os.environ["SF_CONSUMER_SECRET"],
    )


@pytest.fixture(scope="module")
def endpoint(tmp_path_factory):
    """The source's pgwire server, started from a state directory of this run's own."""
    from provisa.federation import pgwire_replica as pr

    previous = os.environ.get("PROVISA_DATA_DIR")
    os.environ["PROVISA_DATA_DIR"] = str(tmp_path_factory.mktemp("salesforce-state"))
    try:
        yield pr.ensure_endpoint(_source(), timeout=_SERVER_READY_SECONDS)
    finally:
        pr.stop_endpoint(_SOURCE_ID)
        if previous is None:
            del os.environ["PROVISA_DATA_DIR"]
        else:
            os.environ["PROVISA_DATA_DIR"] = previous


@_aio
async def test_an_sobject_is_read_through_the_pgwire_server(endpoint):
    from provisa.federation import pgwire_replica as pr

    conn = await pr._pg_connect(endpoint.calcite_child_host, endpoint.pgwire_port)
    try:
        rows = await conn.fetch(f'SELECT "Id", "Name" FROM {_SCHEMA}."Account" LIMIT 5')
    finally:
        await conn.close()
    assert all(r["Id"] for r in rows)


@_aio
async def test_the_describe_cache_is_kept_in_the_sources_state_directory(endpoint):
    from pathlib import Path

    from provisa.federation import pgwire_replica as pr

    del endpoint
    state = Path(os.environ["PROVISA_DATA_DIR"]) / "pgwire"
    assert list(state.glob(f"*/{_SOURCE_ID}/{pr.DESCRIBE_CACHE_DIR_NAME}/*"))


@_aio
async def test_an_account_is_inserted_updated_and_deleted_on_the_write_route(endpoint):
    """The DIRECT terminal's own driver and pool (``api/data/pgwire_write.ensure_write_pool``),
    against the live server: the three statements a governed mutation lowers to."""
    from types import SimpleNamespace

    from provisa.api.admin import schema_query
    from provisa.api.data.pgwire_write import ensure_write_pool
    from provisa.executor.pool import SourcePool

    del endpoint
    name = f"provisa-itest-{uuid.uuid4().hex[:12]}"
    table = f'{_SCHEMA}."Account"'
    state = SimpleNamespace(source_types={_SOURCE_ID: "salesforce"}, source_pools=SourcePool())

    async def _registered(source_id: str):
        return _source()

    original = schema_query._source_for_introspection
    schema_query._source_for_introspection = _registered
    try:
        await ensure_write_pool(state, _SOURCE_ID)
        pool = state.source_pools

        async def _industry() -> list[str]:
            result = await pool.execute(
                _SOURCE_ID, f'SELECT "Industry" FROM {table} WHERE "Name" = $1', [name]
            )
            return [r[0] for r in result.rows]

        try:
            await pool.execute(
                _SOURCE_ID,
                f'INSERT INTO {table} ("Name", "Industry") VALUES ($1, $2)',
                [name, "Energy"],
            )
            assert await _industry() == ["Energy"]
            await pool.execute(
                _SOURCE_ID,
                f'UPDATE {table} SET "Industry" = $1 WHERE "Name" = $2',
                ["Banking", name],
            )
            assert await _industry() == ["Banking"]
        finally:
            await pool.execute(_SOURCE_ID, f'DELETE FROM {table} WHERE "Name" = $1', [name])
        assert await _industry() == []
    finally:
        schema_query._source_for_introspection = original
        await state.source_pools.close_all()


def test_trino_reads_the_org_through_its_salesforce_catalog():
    """``create_catalog`` builds the catalog from ``TrinoSalesforceConnector.details()`` and
    issues CREATE CATALOG against the live coordinator; SHOW TABLES then enumerates the org's
    sObjects through the plugin, which is what proves the credential set (schema names alone are
    answered without a Salesforce call)."""
    trino_dbapi = pytest.importorskip("trino.dbapi")
    import trino.exceptions

    from provisa.core.catalog import create_catalog

    conn = trino_dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="itest",
        catalog="system",
    )
    cur = conn.cursor()
    catalog = "salesforce_itest"

    def _drop() -> None:
        cur.execute(f"DROP CATALOG IF EXISTS {catalog}")
        cur.fetchall()

    _drop()
    try:
        create_catalog(conn, _source(), os.environ["SF_CONSUMER_SECRET"])
        tables: set[str] = set()
        deadline = time.monotonic() + _SERVER_READY_SECONDS
        while time.monotonic() < deadline:
            try:
                cur.execute(f"SHOW TABLES FROM {catalog}.{_SCHEMA}")
                tables = {r[0].lower() for r in cur.fetchall()}
            except trino.exceptions.TrinoExternalError:
                tables = set()  # the plugin is still describing the org
            if tables:
                break
            time.sleep(5)
        assert "account" in tables, f"no Account among {sorted(tables)[:20]}"
        cur.execute(f"SELECT id, name FROM {catalog}.{_SCHEMA}.account LIMIT 5")
        assert all(r[0] for r in cur.fetchall())
    finally:
        _drop()
        conn.close()
