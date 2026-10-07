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
and SF_CONSUMER_SECRET from the lane's environment. Each write test creates, changes and deletes
one Account of its own, named ``provisa-itest-`` plus a random suffix, and removes it by that name
whether it passes or fails; it touches no record it did not create.

API calls
---------
The org has a daily API limit, and the first start of the server describes every sObject of the
org before it listens (about 1,200 calls, minutes on a large org). The server's state directory
is therefore one this machine keeps between runs (under the runtime-deps cache root, beside the
bundle), so the describe cache there is read by the next run and a run within the cache's day
costs the statements below and no describe. The Trino plugin keeps its own describe results under
/data/trino/cache/salesforce-describe in the coordinator, which the test stack binds to a
directory this machine keeps too (docker-compose.itest.yml), so the same holds for the Trino test.
Both caches are kept by source id, and the id carries a digest of the login URL.
"""

from __future__ import annotations

import hashlib
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

# The describe of a large org runs for minutes before the server listens.
_SERVER_READY_SECONDS = 900


def _source_id() -> str:
    """Named for the org: both describe caches are kept between runs under the source's id, and
    one org's describe results must never be read as another's."""
    org = hashlib.sha256(os.environ["SF_LOGIN_URL"].encode()).hexdigest()[:8]
    return f"salesforce-itest-{org}"


def _schema() -> str:
    return _source_id().replace("-", "_")


def _table() -> str:
    return f'{_schema()}."Account"'


def _source():
    from provisa.core.models import Source, SourceType

    return Source(
        id=_source_id(),
        type=SourceType.salesforce,
        base_url=os.environ["SF_LOGIN_URL"],
        username=os.environ["SF_CONSUMER_KEY"],
        password=os.environ["SF_CONSUMER_SECRET"],
    )


def _state_root():
    """The directory the source's server state is kept in between runs (see the module docstring:
    the describe cache must outlive the run)."""
    from provisa.runtime_deps.pgwire_bundles import default_cache_root

    return default_cache_root() / "salesforce-itest-state"


@pytest.fixture(scope="module")
def endpoint():
    """The source's pgwire server, started from the state directory this machine keeps."""
    from provisa.federation import pgwire_replica as pr

    previous = os.environ.get("PROVISA_DATA_DIR")
    os.environ["PROVISA_DATA_DIR"] = str(_state_root())
    try:
        yield pr.ensure_endpoint(_source(), timeout=_SERVER_READY_SECONDS)
    finally:
        pr.stop_endpoint(_source_id())
        if previous is None:
            del os.environ["PROVISA_DATA_DIR"]
        else:
            os.environ["PROVISA_DATA_DIR"] = previous


@pytest.fixture
async def write_pool(endpoint):
    """The DIRECT terminal's own driver and pool (``api/data/pgwire_write.ensure_write_pool``),
    against the live server."""
    from types import SimpleNamespace

    from provisa.api.admin import schema_query
    from provisa.api.data.pgwire_write import ensure_write_pool
    from provisa.executor.pool import SourcePool

    del endpoint
    state = SimpleNamespace(source_types={_source_id(): "salesforce"}, source_pools=SourcePool())

    async def _registered(source_id: str):
        return _source()

    original = schema_query._source_for_introspection
    schema_query._source_for_introspection = _registered
    try:
        await ensure_write_pool(state, _source_id())
        yield state.source_pools
    finally:
        schema_query._source_for_introspection = original
        await state.source_pools.close_all()


@_aio
async def test_an_sobject_is_read_through_the_pgwire_server(endpoint):
    from provisa.federation import pgwire_replica as pr

    conn = await pr._pg_connect(endpoint.calcite_child_host, endpoint.pgwire_port)
    try:
        rows = await conn.fetch(f'SELECT "Id", "Name" FROM {_schema()}."Account" LIMIT 5')
    finally:
        await conn.close()
    assert all(r["Id"] for r in rows)


@_aio
async def test_the_describe_cache_is_kept_in_the_sources_state_directory(endpoint):
    from pathlib import Path

    from provisa.federation import pgwire_replica as pr

    del endpoint
    state = Path(os.environ["PROVISA_DATA_DIR"]) / "pgwire"
    assert list(state.glob(f"*/{_source_id()}/{pr.DESCRIBE_CACHE_DIR_NAME}/*"))


def _own_account_name() -> str:
    return f"provisa-itest-{uuid.uuid4().hex[:12]}"


async def _industry(pool, name: str) -> list[str]:
    result = await pool.execute(
        _source_id(), f'SELECT "Industry" FROM {_table()} WHERE "Name" = $1', [name]
    )
    return [r[0] for r in result.rows]


@_aio
async def test_an_account_is_inserted_updated_and_deleted_on_the_write_route(write_pool):
    """The three statements a governed mutation lowers to."""
    pool = write_pool
    name = _own_account_name()
    try:
        await pool.execute(
            _source_id(),
            f'INSERT INTO {_table()} ("Name", "Industry") VALUES ($1, $2)',
            [name, "Energy"],
        )
        assert await _industry(pool, name) == ["Energy"]
        await pool.execute(
            _source_id(),
            f'UPDATE {_table()} SET "Industry" = $1 WHERE "Name" = $2',
            ["Banking", name],
        )
        assert await _industry(pool, name) == ["Banking"]
    finally:
        await pool.execute(_source_id(), f'DELETE FROM {_table()} WHERE "Name" = $1', [name])
    assert await _industry(pool, name) == []


@_aio
async def test_each_write_returns_the_rows_it_wrote(write_pool):
    """``RETURNING`` on INSERT, UPDATE and DELETE hands back the written Account: what a GraphQL
    mutation field's selection is answered from (``writable._ROUTE_RETURNS_WRITTEN_ROWS``)."""
    pool = write_pool
    name = _own_account_name()
    try:
        inserted = await pool.execute(
            _source_id(),
            f'INSERT INTO {_table()} ("Name", "Industry") VALUES ($1, $2) '
            'RETURNING "Id", "Name", "Industry"',
            [name, "Energy"],
        )
        assert [(r[1], r[2]) for r in inserted.rows] == [(name, "Energy")]
        account_id = inserted.rows[0][0]
        assert account_id
        updated = await pool.execute(
            _source_id(),
            f'UPDATE {_table()} SET "Industry" = $1 WHERE "Name" = $2 RETURNING "Id", "Industry"',
            ["Banking", name],
        )
        assert [tuple(r) for r in updated.rows] == [(account_id, "Banking")]
        deleted = await pool.execute(
            _source_id(), f'DELETE FROM {_table()} WHERE "Name" = $1 RETURNING "Id"', [name]
        )
        assert [r[0] for r in deleted.rows] == [account_id]
    finally:
        # Whatever failed above, the Account this test created does not outlive it.
        await pool.execute(_source_id(), f'DELETE FROM {_table()} WHERE "Name" = $1', [name])
    assert await _industry(pool, name) == []


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
    catalog = _schema()

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
                cur.execute(f"SHOW TABLES FROM {catalog}.{_schema()}")
                tables = {r[0].lower() for r in cur.fetchall()}
            except trino.exceptions.TrinoExternalError:
                tables = set()  # the plugin is still describing the org
            if tables:
                break
            time.sleep(5)
        assert "account" in tables, f"no Account among {sorted(tables)[:20]}"
        cur.execute(f"SELECT id, name FROM {catalog}.{_schema()}.account LIMIT 5")
        assert all(r[0] for r in cur.fetchall())
    finally:
        _drop()
        conn.close()
