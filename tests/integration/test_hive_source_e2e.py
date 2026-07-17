# Copyright (c) 2026 Kenneth Stott
# Canary: 9c1b7e42-3a5d-4f81-b6c0-2e9d47a1f5be
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Hive (local warehouse) as a connector-source, read through the Provisa federation engine
(REQ-1097).

A connector-source (see test_cassandra_source_e2e.py for the full explanation of the catalog seam):
``hive`` has an entry in provisa/federation/trino_connectors.py::TRINO_CONNECTORS but NO direct driver
in provisa/executor/drivers/registry.py — the ONLY way Provisa reaches it is by projecting it as a
live Trino catalog and querying through Trino.

Hive specifics
--------------
- Trino's hive connector is NOT a JDBC connector: it needs hive.metastore.uri (thrift://…), not a
  connection-url. Before REQ-1097 hive/hive_s3 routed through the generic JDBC connector, whose
  jdbc_url() is empty for hive, so create_catalog() silently produced no catalog. The dedicated
  TrinoHiveConnector emits hive.metastore=thrift + hive.metastore.uri (Source.host:Source.port,
  default 9083) + fs.hadoop.enabled=true.
- The compose service ``hive-metastore`` IS the Thrift metastore, reached in-network at
  hive-metastore:9083 (the host handed to create_catalog is that compose service name — Trino resolves
  it inside its own container). Table data lives on a warehouse volume SHARED with Trino at the same
  path (/opt/hive/data/warehouse); Trino's Hadoop-native filesystem reads the file:/ locations the
  metastore records. No beeline is needed: the ``widgets`` table is CREATEd + INSERTed THROUGH Trino
  (the hive connector supports writes), which is itself the federation-engine write path.
"""

from __future__ import annotations

import os
import time

import pytest
import trino.dbapi
import trino.exceptions

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

_SCHEMA = "wh"
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


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


def _seed_hive(cur, catalog: str) -> None:
    """Create the schema + widgets table and insert rows THROUGH Trino's hive connector (the
    federation-engine write path). Retries the schema create while the freshly-created catalog's
    metastore connection warms up."""
    deadline = time.monotonic() + 60
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            _exec(cur, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{_SCHEMA}")
            break
        except trino.exceptions.TrinoQueryError as exc:
            last_exc = exc
            time.sleep(3)
    else:
        raise RuntimeError(f"hive CREATE SCHEMA never succeeded: {last_exc!r}")

    _exec(cur, f"DROP TABLE IF EXISTS {catalog}.{_SCHEMA}.{_TABLE}")
    _exec(
        cur,
        f"CREATE TABLE {catalog}.{_SCHEMA}.{_TABLE} (id integer, name varchar) "
        "WITH (format = 'PARQUET')",
    )
    values = ", ".join(f"({wid}, '{name}')" for wid, name in _WIDGETS)
    _exec(cur, f"INSERT INTO {catalog}.{_SCHEMA}.{_TABLE} (id, name) VALUES {values}")


@pytest.mark.requires_hive
async def test_hive_catalog_created_and_queryable():
    """Register a hive Source, project it as a live Trino catalog, seed + query it end-to-end.

    Drives the REAL registration path: provisa.core.catalog.create_catalog builds the catalog
    properties from TrinoHiveConnector.details() (hive.metastore.uri from Source.host:port —
    REQ-1097) and issues CREATE CATALOG against the live Trino coordinator. host="hive-metastore" is
    the compose service name of the Thrift metastore; Trino resolves it inside its own container on
    the isolated stack's private network (thrift://hive-metastore:9083).
    """
    pytest.importorskip("trino")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    conn, cur = _trino_cursor()

    catalog = "hive_itest"
    _drop(cur, catalog)
    src = Source(id="hive-itest", type=SourceType.hive, host="hive-metastore", port=9083)
    try:
        create_catalog(conn, src, "")

        _seed_hive(cur, catalog)

        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = {r[0] for r in cur.fetchall()}
        assert _SCHEMA in schemas

        cur.execute(f"SHOW TABLES FROM {catalog}.{_SCHEMA}")
        tables = {r[0] for r in cur.fetchall()}
        assert _TABLE in tables

        # Querying <catalog>.<schema>.<table> through Trino IS reading through the federation engine —
        # Trino's hive connector reads the warehouse files the metastore points at; nothing is landed.
        cur.execute(f"SELECT id, name FROM {catalog}.{_SCHEMA}.{_TABLE} ORDER BY id")
        rows = cur.fetchall()
        assert sorted((r[0], r[1]) for r in rows) == _WIDGETS
    finally:
        _drop(cur, catalog)
        conn.close()
