# Copyright (c) 2026 Kenneth Stott
# Canary: 0073a502-d8cb-4152-acb6-c1756e3c86a0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Exasol as a connector-source, read through the Provisa federation engine (REQ-1097).

Same connector-bucket pattern as ``test_cassandra_source_e2e.py`` (THE reference test for this
bucket — read its module docstring for the full catalog-seam explanation). Exasol has NO direct
driver in ``provisa/executor/drivers/registry.py``; it is reachable ONLY through the federation
engine's Trino ``exasol`` catalog:

  1. A ``Source`` row (type=exasol, host=<compose service name>, port=8563) declares the connection.
  2. ``provisa.core.catalog.create_catalog`` looks up the type in
     ``provisa.federation.trino_connectors.TRINO_CONNECTORS``, builds the catalog ``.properties``
     via ``connector.details(source)``, and issues ``CREATE CATALOG ... USING exasol WITH (...)``
     against the live Trino coordinator.
  3. Querying ``<catalog>.<schema>.<table>`` via ``trino.dbapi`` then reads live from Exasol through
     Trino's exasol JDBC connector — no data is landed in Provisa's own store.

Exasol specifics
-----------------
- ``provisa/federation/trino_connectors.py``: exasol is NOT one of the connectors with its own
  ``_TrinoConnector`` subclass (unlike cassandra/mongodb/redis/...). It is registered generically
  via ``_TRINO_JDBC_TYPES = {..., "exasol": "exasol"}`` (line ~124), which builds a
  ``Trino_exasol_Connector(_TrinoJdbcConnector)`` class at import time. ``_TrinoJdbcConnector.details()``
  (line ~54) builds its catalog properties entirely from ``source.jdbc_url(host, port)``.
- Trino's official ``exasol`` connector plugin (bundled in ``trinodb/trino:481``, the image
  ``docker-compose.core.yml`` uses — verified: ``/usr/lib/trino/plugin/exasol`` ships
  ``io.trino_trino-exasol-481.jar`` + the Exasol JDBC driver jar) needs a JDBC
  ``connection-url`` shaped ``jdbc:exa:<host>:<port>`` in its catalog properties.
- ``pyexasol`` is NOT installed in this venv and is NOT added as a dependency here (seeding a test
  fixture is not a reason to add a runtime dependency — same rule the cassandra reference test
  documents for ``cassandra-driver``). Seeding instead goes through
  ``docker exec <container> exaplus -c <host>:8563 -u sys -p exasol -sql "<SQL>"`` — the
  ``exasol/docker-db`` image ships the ``exaplus`` CLI.

REQ-1097 product bug FIXED alongside this test:

``provisa/core/models.py::Source.jdbc_url()`` previously omitted ``exasol`` (and ``redshift``) from
its JDBC-prefix table, so it returned ``""`` for an exasol source regardless of host/port. That made
``_TrinoJdbcConnector.details()`` (``trino_connectors.py`` line ~54) return ``{}`` and
``create_catalog`` (``core/catalog.py`` line ~118: ``if not props: return``) SILENTLY skip catalog
creation — the catalog never got created and downstream queries failed with "catalog not found".
Fixed by adding ``"exasol": "jdbc:exa"`` (colon-delimited, no db in URL) and ``"redshift":
"jdbc:redshift"`` to the prefix table; regression covered in
``tests/unit/test_wire_compatible_rdbs.py::test_exasol_and_redshift_jdbc_urls_present``. This test
asserts the working end state (catalog created + queryable) against the fixed source.
"""

from __future__ import annotations

import os
import platform
import subprocess
import time

import pytest
import trino.dbapi
import trino.exceptions

# exasol/docker-db is published linux/amd64 only; under QEMU on arm64 (Apple Silicon) its
# EXAStorage boot never completes, so the container can't become healthy. Runs for real on an
# amd64 host/CI. Gate on the machine arch — a documented platform gap, not a dodge.
_AMD64 = platform.machine() in ("x86_64", "amd64")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="session"),
    pytest.mark.skipif(not _AMD64, reason="exasol/docker-db is amd64-only; unbootable under arm64 emulation"),
]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
_ITEST_PROJECT = os.environ.get("PROVISA_ITEST_PROJECT", "provisa-itest")

_EXASOL_USER = "sys"
_EXASOL_PASSWORD = "exasol"  # image default (verified: exasol/docker-db, github.com/exasol/docker-db)
_SCHEMA = "PROVISA"
_TABLE = "WIDGETS"
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


def _exasol_container_id() -> str:
    """The running `exasol` service's container id, found by compose labels (no fixed name — the
    isolated stack never uses container_name, so it never collides with a parallel run)."""
    out = subprocess.run(
        [
            "docker", "ps", "-q",
            "--filter", f"label=com.docker.compose.project={_ITEST_PROJECT}",
            "--filter", "label=com.docker.compose.service=exasol",
        ],  # fmt: skip
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    ids = out.splitlines()
    if not ids:
        raise RuntimeError(f"No running exasol container for project {_ITEST_PROJECT!r}")
    return ids[0]


def _exaplus(container_id: str, statement: str) -> None:
    """Run one SQL statement inside the exasol container via exaplus (no python driver installed;
    the container already ships exaplus — see module docstring)."""
    subprocess.run(
        [
            "docker", "exec", container_id,
            "exaplus", "-c", "localhost:8563", "-u", _EXASOL_USER, "-p", _EXASOL_PASSWORD,
            "-sql", statement,
        ],  # fmt: skip
        capture_output=True,
        text=True,
        check=True,
    )


def _seed_exasol() -> None:
    """Create the PROVISA.WIDGETS schema/table and insert 3 rows, retrying while the engine
    finishes booting right after the healthcheck first passes."""
    container_id = _exasol_container_id()
    deadline = time.monotonic() + 90
    last_err: subprocess.CalledProcessError | None = None
    while time.monotonic() < deadline:
        try:
            _exaplus(container_id, f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA}")
            break
        except subprocess.CalledProcessError as e:
            last_err = e
            time.sleep(3)
    else:
        raise RuntimeError(f"exasol schema creation never succeeded: {last_err!r}")

    _exaplus(
        container_id,
        f"CREATE TABLE IF NOT EXISTS {_SCHEMA}.{_TABLE} (ID DECIMAL(18,0), NAME VARCHAR(64))",
    )
    for wid, name in _WIDGETS:
        _exaplus(container_id, f"INSERT INTO {_SCHEMA}.{_TABLE} (ID, NAME) VALUES ({wid}, '{name}')")


@pytest.mark.requires_exasol
async def test_exasol_catalog_created_and_queryable():
    """Register an exasol Source, project it as a live Trino catalog, query it end-to-end.

    Drives the REAL registration path: provisa.core.catalog.create_catalog builds the catalog
    properties from the generic ``_TrinoJdbcConnector.details()`` (exasol has no dedicated
    connector class — see module docstring) and issues CREATE CATALOG against the live Trino
    coordinator. host="exasol" is the compose service name — Trino resolves it from inside its own
    container on the isolated stack's private network, NOT the host-published ${EXASOL_PORT}.

    The ``Source.jdbc_url()`` exasol gap that used to make this silently no-op is now fixed (see
    module docstring), so this asserts the catalog IS created and queryable end-to-end.
    """
    pytest.importorskip("trino")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    _seed_exasol()

    conn, cur = _trino_cursor()

    catalog = "exasol_itest"
    _drop(cur, catalog)
    src = Source(id="exasol-itest", type=SourceType.exasol, host="exasol", port=8563)
    try:
        create_catalog(conn, src, "")

        # The catalog now exists and exposes the seeded schema as a Trino schema.
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = {r[0] for r in cur.fetchall()}
        assert _SCHEMA.lower() in schemas or _SCHEMA in schemas

        cur.execute(f"SHOW TABLES FROM {catalog}.{_SCHEMA.lower()}")
        tables = {r[0] for r in cur.fetchall()}
        assert _TABLE.lower() in tables or _TABLE in tables

        # Querying <catalog>.<schema>.<table> through Trino IS reading through the federation
        # engine — Trino's exasol connector reads live from the source; nothing is landed.
        rows: list = []
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                cur.execute(f"SELECT id, name FROM {catalog}.{_SCHEMA.lower()}.{_TABLE.lower()} ORDER BY id")
                rows = cur.fetchall()
            except trino.exceptions.TrinoExternalError:
                rows = []  # catalog freshly created; connector may not be warm yet
            if len(rows) == len(_WIDGETS):
                break
            time.sleep(2)

        assert sorted((int(r[0]), r[1]) for r in rows) == _WIDGETS
    finally:
        _drop(cur, catalog)
        conn.close()
