# Copyright (c) 2026 Kenneth Stott
# Canary: 7d4e1f9a-2c68-4b1e-9d3a-58e6f1a0b7c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Redshift as a connector-source, read through the Provisa federation engine (REQ-1097).

Same connector-bucket pattern as ``test_cassandra_source_e2e.py`` / ``test_exasol_source_e2e.py``
(read the cassandra module docstring for the full catalog-seam explanation). Redshift has NO direct
driver in ``provisa/executor/drivers/registry.py``; it is reachable ONLY through the federation
engine's Trino ``redshift`` catalog:

  1. A ``Source`` row (type=redshift, host/port/database/user/password) declares the connection.
  2. ``provisa.core.catalog.create_catalog`` looks up the type in
     ``provisa.federation.trino_connectors.TRINO_CONNECTORS``, builds the catalog ``.properties``
     via ``connector.details(source)`` (generic ``_TrinoJdbcConnector`` — redshift has no dedicated
     connector class, see ``trino_connectors.py`` line ~117 ``_TRINO_JDBC_TYPES["redshift"] =
     "redshift"``), and issues ``CREATE CATALOG ... USING redshift WITH (...)`` against the live
     Trino coordinator.
  3. Querying ``<catalog>.<schema>.<table>`` via ``trino.dbapi`` then reads live from Redshift
     through Trino's redshift connector — no data is landed in Provisa's own store.

Why this is credential-gated, not docker-gated
-------------------------------------------------
Redshift is AWS-only — there is no local/OSS image to self-provision (unlike cassandra/exasol,
which run as docker-compose services on this host). A real Redshift cluster is required to exercise
this end to end, so the test is unconditionally skipped unless ALL of
``REDSHIFT_HOST``/``REDSHIFT_PORT``/``REDSHIFT_DATABASE``/``REDSHIFT_USER``/``REDSHIFT_PASSWORD``
are set in the environment — no AWS/Redshift creds exist in this repo's ``.env`` today, so this
test SKIPS here. Not added to ``tests/conftest.py::_MARKER_SERVICES`` — there is no docker service
for the provisioner to bring up; the isolated stack's core Trino (always started for `integration`
tests) is reused as-is and simply reaches out to the real AWS endpoint over the network.

Seeding uses ``psycopg2`` (already a project dependency) rather than the AWS ``redshift_connector``
package: Redshift's leader node speaks the Postgres wire protocol for ordinary DDL/DML, so a plain
libpq client is sufficient to seed a scratch table without adding a new dependency for test setup.
"""

from __future__ import annotations

import os
import time

import pytest
import trino.dbapi
import trino.exceptions

pytestmark = [pytest.mark.integration, pytest.mark.requires_redshift]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

_ENV = ("REDSHIFT_HOST", "REDSHIFT_PORT", "REDSHIFT_DATABASE", "REDSHIFT_USER", "REDSHIFT_PASSWORD")
_HAVE_CREDS = all(os.environ.get(v) for v in _ENV)
pytestmark.append(
    pytest.mark.skipif(
        not _HAVE_CREDS,
        reason=(
            "No AWS Redshift cluster available (AWS-only, not self-provisionable on this host); "
            "set REDSHIFT_HOST/REDSHIFT_PORT/REDSHIFT_DATABASE/REDSHIFT_USER/REDSHIFT_PASSWORD to "
            "run against a real cluster"
        ),
    )
)

_SCHEMA = "public"
_TABLE = "provisa_widgets_e2e"
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


def _seed_redshift() -> None:
    """Create a scratch table on the real cluster and insert 3 rows via psycopg2 (Redshift's
    leader node speaks the Postgres wire protocol for ordinary DDL/DML — see module docstring)."""
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.environ["REDSHIFT_PORT"]),
        dbname=os.environ["REDSHIFT_DATABASE"],
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {_SCHEMA}.{_TABLE}")
        cur.execute(f"CREATE TABLE {_SCHEMA}.{_TABLE} (id INTEGER, name VARCHAR(64))")
        for wid, name in _WIDGETS:
            cur.execute(f"INSERT INTO {_SCHEMA}.{_TABLE} (id, name) VALUES (%s, %s)", (wid, name))
        cur.close()
    finally:
        conn.close()


def _drop_redshift_table() -> None:
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.environ["REDSHIFT_PORT"]),
        dbname=os.environ["REDSHIFT_DATABASE"],
        user=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
    )
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {_SCHEMA}.{_TABLE}")
        cur.close()
    finally:
        conn.close()


def test_redshift_catalog_created_and_queryable():
    """Register a redshift Source, project it as a live Trino catalog, query it end-to-end.

    Drives the REAL registration path: provisa.core.catalog.create_catalog builds the catalog
    properties from the generic ``_TrinoJdbcConnector.details()`` (redshift has no dedicated
    connector class) via ``Source.jdbc_url()`` (``jdbc:redshift://host:port/db``), and issues
    CREATE CATALOG against the live Trino coordinator.
    """
    pytest.importorskip("trino")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    _seed_redshift()

    conn, cur = _trino_cursor()

    catalog = "redshift_itest"
    _drop(cur, catalog)
    src = Source(
        id="redshift-itest",
        type=SourceType.redshift,
        host=os.environ["REDSHIFT_HOST"],
        port=int(os.environ["REDSHIFT_PORT"]),
        database=os.environ["REDSHIFT_DATABASE"],
        username=os.environ["REDSHIFT_USER"],
        password=os.environ["REDSHIFT_PASSWORD"],
    )
    try:
        create_catalog(conn, src, os.environ["REDSHIFT_PASSWORD"])

        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = {r[0] for r in cur.fetchall()}
        assert _SCHEMA in schemas

        cur.execute(f"SHOW TABLES FROM {catalog}.{_SCHEMA}")
        tables = {r[0] for r in cur.fetchall()}
        assert _TABLE in tables

        # Querying <catalog>.<schema>.<table> through Trino IS reading through the federation
        # engine — Trino's redshift connector reads live from the source; nothing is landed.
        rows: list = []
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                cur.execute(f"SELECT id, name FROM {catalog}.{_SCHEMA}.{_TABLE} ORDER BY id")
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
        _drop_redshift_table()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
