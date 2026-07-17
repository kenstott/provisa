# Copyright (c) 2026 Kenneth Stott
# Canary: b2380efb-e3ed-45bd-81a4-af8571899b22
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Cassandra as a connector-source, read through the Provisa federation engine (REQ-1097).

THIS IS THE REFERENCE TEST for the Trino-connector-source bucket (druid, pinot, exasol, hive,
elasticsearch, mongodb, redis, …) — types that have NO direct driver in
provisa/executor/drivers/registry.py and are reachable ONLY through the federation engine's Trino
layer. Copy this pattern for the rest of that bucket; only the seed/query specifics change.

How a connector-source e2e works (the catalog seam)
-----------------------------------------------------
A "connector source" has an entry in provisa/federation/trino_connectors.py::TRINO_CONNECTORS but
NOT in provisa/executor/drivers/registry.py. There is no SourcePool for it (that machinery is for
direct-driver types like mariadb/oracle) — the ONLY way Provisa reaches it is by projecting it as a
live Trino catalog and querying through Trino. The seam, end to end:

  1. A `Source` row (provisa.core.models.Source) declares type=<connector type> + connection details
     (host/port/mapping). This is the same row the registry API would persist; the test builds it
     in-process instead of going through the HTTP layer.
  2. `provisa.core.catalog.create_catalog(conn, source, resolved_password)` is the real registration
     path (REQ-012/250/251/842): it looks up the source's type in TRINO_CONNECTORS (the single
     source of truth for "is this reachable by Trino, and with which connector"), builds the
     catalog .properties dict via `connector.details(source)`, and issues a dynamic
     `CREATE CATALOG ... USING <connector> WITH (...)` against the live Trino coordinator (REST
     catalog API under the hood — see trino_lifecycle.reload_catalog for the sibling reload path
     used by the settings router). No restart, no on-disk .properties file — this is the same path
     `provisa.federation.engine.EngineRuntime.on_asset_create`/`reconcile`/`ensure_entry` drive when
     a source is registered through the real API (REQ-843): resolve() -> connector.catalog_entry(),
     then the catalog projects into Trino via this same create_catalog call.
  3. Once the catalog exists, querying it via any Trino client (`trino.dbapi.connect(...)`) against
     `<catalog>.<schema>.<table>` IS querying "through the engine" — Trino's cassandra connector
     does the live read from the source, no data is landed/copied into Provisa's own store.

The crux for any connector whose source lives in the SAME docker-compose network as Trino: the
`Source.host` handed to `create_catalog` must be the compose SERVICE NAME (e.g. "cassandra"), not
"localhost" and not the host-published ephemeral port — Trino resolves that host from INSIDE its
own container, on the isolated stack's private network (see tests/conftest.py::_ITEST_COMPOSE_ARGS).
The host-published `${CASSANDRA_PORT}` exists only so the TEST PROCESS itself (running on the host)
can reach Cassandra for seeding via `docker exec ... cqlsh` — Trino never uses it.

Cassandra specifics
--------------------
- provisa/federation/trino_connectors.py:181 TrinoCassandraConnector emits cassandra.contact-points
  (source.host), cassandra.native-protocol-port (source.port), a fixed local-dc "datacenter1", and
  consistency-level ONE — all of which must match how the `cassandra` compose service actually
  advertises itself (single-node, GossipingPropertyFileSnitch, datacenter1 — see
  docker-compose.test.yml).
- No `cassandra-driver` Python package in this venv (checked: not installed, and not added as a
  dependency here — seeding a test fixture is not a reason to add a runtime dependency). Seeding
  goes through `docker exec <container> cqlsh -e "<CQL>"` instead — the container already ships
  cqlsh, so this needs nothing beyond the docker CLI the test harness already requires.
"""

from __future__ import annotations

import os
import subprocess
import time

import pytest
import trino.dbapi
import trino.exceptions

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
_ITEST_PROJECT = os.environ.get("PROVISA_ITEST_PROJECT", "provisa-itest")

_KEYSPACE = "provisa"
_TABLE = "widgets"
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


@pytest.fixture(scope="module", autouse=True)
def _wait_for_trino():
    """Wait for Trino to finish initializing before running Trino tests."""
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            conn = trino.dbapi.connect(
                host=_TRINO_HOST, port=_TRINO_PORT, user="itest", catalog="system"
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            conn.close()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Trino did not become ready within 120s")


def _trino_cursor():
    conn = trino.dbapi.connect(host=_TRINO_HOST, port=_TRINO_PORT, user="itest", catalog="system")
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchall()
    return conn, cur


def _drop(cur, name):
    try:
        cur.execute(f"DROP CATALOG {name}")
        cur.fetchall()
    except Exception:
        pass


def _cassandra_container_id() -> str:
    """The running `cassandra` service's container id, found by compose labels (no fixed name —
    the isolated stack never uses container_name, so it never collides with a parallel run)."""
    out = subprocess.run(
        [
            "docker", "ps", "-q",
            "--filter", f"label=com.docker.compose.project={_ITEST_PROJECT}",
            "--filter", "label=com.docker.compose.service=cassandra",
        ],  # fmt: skip
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    ids = out.splitlines()
    if not ids:
        raise RuntimeError(f"No running cassandra container for project {_ITEST_PROJECT!r}")
    return ids[0]


def _cqlsh(container_id: str, statement: str) -> None:
    """Run one CQL statement inside the cassandra container via cqlsh (no python driver installed;
    the container already ships cqlsh — see module docstring)."""
    subprocess.run(
        ["docker", "exec", container_id, "cqlsh", "-e", statement],
        capture_output=True,
        text=True,
        check=True,
    )


def _seed_cassandra() -> None:
    """Create the provisa.widgets keyspace/table and insert 3 rows, retrying while gossip/schema
    agreement settles right after the healthcheck first passes."""
    container_id = _cassandra_container_id()
    ddl = (
        f"CREATE KEYSPACE IF NOT EXISTS {_KEYSPACE} "
        "WITH replication = {'class':'SimpleStrategy','replication_factor':1}"
    )
    deadline = time.monotonic() + 60
    last_err: subprocess.CalledProcessError | None = None
    while time.monotonic() < deadline:
        try:
            _cqlsh(container_id, ddl)
            break
        except subprocess.CalledProcessError as e:
            last_err = e
            time.sleep(3)
    else:
        raise RuntimeError(f"cassandra keyspace creation never succeeded: {last_err!r}")

    _cqlsh(container_id, f"CREATE TABLE IF NOT EXISTS {_KEYSPACE}.{_TABLE} (id int PRIMARY KEY, name text)")
    for wid, name in _WIDGETS:
        _cqlsh(container_id, f"INSERT INTO {_KEYSPACE}.{_TABLE} (id, name) VALUES ({wid}, '{name}')")


@pytest.mark.requires_cassandra
async def test_cassandra_catalog_created_and_queryable():
    """Register a cassandra Source, project it as a live Trino catalog, query it end-to-end.

    Drives the REAL registration path: provisa.core.catalog.create_catalog builds the catalog
    properties from TrinoCassandraConnector.details() and issues CREATE CATALOG against the live
    Trino coordinator — the same seam EngineRuntime.on_asset_create/reconcile/ensure_entry drive
    when a source is registered through the actual API (REQ-843). host="cassandra" is the compose
    service name — Trino resolves it from inside its own container on the isolated stack's private
    network, NOT the host-published ephemeral ${CASSANDRA_PORT} (see module docstring).
    """
    pytest.importorskip("trino")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    _seed_cassandra()

    conn, cur = _trino_cursor()

    catalog = "cassandra_itest"
    _drop(cur, catalog)
    src = Source(id="cassandra-itest", type=SourceType.cassandra, host="cassandra", port=9042)
    try:
        create_catalog(conn, src, "")

        # The catalog now exists and exposes the seeded keyspace as a Trino schema.
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = {r[0] for r in cur.fetchall()}
        assert _KEYSPACE in schemas

        cur.execute(f"SHOW TABLES FROM {catalog}.{_KEYSPACE}")
        tables = {r[0] for r in cur.fetchall()}
        assert _TABLE in tables

        # Querying <catalog>.<keyspace>.<table> through Trino IS reading through the federation
        # engine — Trino's cassandra connector reads live from the source; nothing is landed.
        rows: list = []
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                cur.execute(f"SELECT id, name FROM {catalog}.{_KEYSPACE}.{_TABLE} ORDER BY id")
                rows = cur.fetchall()
            except trino.exceptions.TrinoExternalError:
                rows = []  # catalog freshly created; connector may not be warm yet
            if len(rows) == len(_WIDGETS):
                break
            time.sleep(2)

        assert sorted((r[0], r[1]) for r in rows) == _WIDGETS
    finally:
        _drop(cur, catalog)
        conn.close()
