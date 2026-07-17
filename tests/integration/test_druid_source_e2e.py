# Copyright (c) 2026 Kenneth Stott
# Canary: c920acf4-0aca-485d-88ce-ffd916e203a2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Druid as a connector-source, read through the Provisa federation engine (REQ-1097).

A connector-source (see test_cassandra_source_e2e.py for the full explanation of the seam): druid
has an entry in provisa/federation/trino_connectors.py::TRINO_CONNECTORS (``"druid": "druid"`` in
_TRINO_JDBC_TYPES) but NO direct driver in provisa/executor/drivers/registry.py. The ONLY way
Provisa reaches it is by projecting it as a live Trino catalog and querying through Trino.

Druid specifics
---------------
- Trino's druid connector wraps the Avatica JDBC driver and attaches to the Druid BROKER's Avatica
  endpoint. provisa.core.models.Source.jdbc_url() emits
  ``jdbc:avatica:remote:url=http://<broker>:<port>/druid/v2/sql/avatica/`` for a druid source; the
  compose service named ``druid`` IS the broker, reached in-network at ``druid:8082`` (the host
  handed to create_catalog is that compose service name, NOT localhost — Trino resolves it inside its
  own container). Trino's schema for a druid catalog is always ``druid``; the table is the datasource.
- The official apache/druid image has no perl, so bin/supervise (the single-container quickstart)
  cannot run; Druid is split into its normal processes + external ZooKeeper + Postgres metadata (see
  docker-compose.test.yml). Seeding uses Druid's native batch ingestion REST API against the overlord
  (coordinator asOverlord, host-published ${DRUID_COORD_PORT}); readiness is polled against the
  broker (${DRUID_BROKER_PORT}) until the segment is loaded and queryable. No python druid client is
  needed — the REST API is plain JSON over HTTP (stdlib urllib).
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

import pytest
import trino.dbapi
import trino.exceptions

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

_DRUID_HOST = os.environ.get("DRUID_HOST", "localhost")
_DRUID_BROKER_PORT = int(os.environ.get("DRUID_BROKER_PORT", "8082"))
_DRUID_COORD_PORT = int(os.environ.get("DRUID_COORD_PORT", "8081"))

_DATASOURCE = "widgets"
# Druid stores dimensions as strings; the ingestion rows below define these values.
_WIDGETS = [("1", "Widget A"), ("2", "Widget B"), ("3", "Widget C")]


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


def _http_json(url: str, payload: dict | None = None) -> dict | list:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(  # noqa: S310 - fixed localhost URL to the itest druid container
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST" if data is not None else "GET",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 - localhost only
        body = resp.read().decode()
    return json.loads(body) if body else {}


def _druid_sql(query: str) -> list:
    """Run a Druid SQL query against the broker (host-published for readiness polling only)."""
    url = f"http://{_DRUID_HOST}:{_DRUID_BROKER_PORT}/druid/v2/sql"
    try:
        result = _http_json(url, {"query": query})
    except urllib.error.HTTPError:
        return []  # datasource not queryable yet (segments not loaded)
    return result if isinstance(result, list) else []


def _seed_druid() -> None:
    """Ingest 3 rows into the ``widgets`` datasource via Druid's native batch REST API and wait for
    the segment to be loaded on the historical (queryable through the broker)."""
    coord = f"http://{_DRUID_HOST}:{_DRUID_COORD_PORT}"
    rows = "\n".join(
        json.dumps({"ts": "2020-01-01T00:00:00Z", "id": wid, "name": name})
        for wid, name in _WIDGETS
    )
    task = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": _DATASOURCE,
                "timestampSpec": {"column": "ts", "format": "iso"},
                "dimensionsSpec": {"dimensions": ["id", "name"]},
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "day",
                    "queryGranularity": "none",
                    "rollup": False,
                },
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {"type": "inline", "data": rows},
                "inputFormat": {"type": "json"},
            },
            "tuningConfig": {"type": "index_parallel", "maxRowsInMemory": 25000},
        },
    }
    submit = _http_json(f"{coord}/druid/indexer/v1/task", task)
    task_id = submit["task"]  # KeyError here = the overlord rejected the task; no silent fallback

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        status = _http_json(f"{coord}/druid/indexer/v1/task/{task_id}/status")
        code = status["status"]["statusCode"]
        if code == "SUCCESS":
            break
        if code == "FAILED":
            raise RuntimeError(f"Druid ingestion task {task_id} FAILED: {status!r}")
        time.sleep(5)
    else:
        raise RuntimeError(f"Druid ingestion task {task_id} did not finish within 180s")

    # Wait for the segment to be loaded and the datasource queryable through the broker.
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        rows_out = _druid_sql(f"SELECT COUNT(*) AS c FROM {_DATASOURCE}")
        if rows_out and rows_out[0].get("c") == len(_WIDGETS):
            return
        time.sleep(3)
    raise RuntimeError("Druid datasource never became queryable within 120s after ingestion")


@pytest.mark.requires_druid
async def test_druid_catalog_created_and_queryable():
    """Register a druid Source, project it as a live Trino catalog, query it end-to-end.

    Drives the REAL registration path: provisa.core.catalog.create_catalog builds the catalog
    properties from the druid JDBC connector's details() (connection-url from Source.jdbc_url(), which
    must emit the broker Avatica URL — REQ-1097) and issues CREATE CATALOG against the live Trino
    coordinator. host="druid" is the compose service name of the BROKER; Trino resolves it inside its
    own container on the isolated stack's private network (Avatica at druid:8082).
    """
    pytest.importorskip("trino")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    _seed_druid()

    conn, cur = _trino_cursor()

    catalog = "druid_itest"
    _drop(cur, catalog)
    src = Source(id="druid-itest", type=SourceType.druid, host="druid", port=8082)
    try:
        create_catalog(conn, src, "")

        # Druid's Trino catalog exposes a single fixed schema "druid"; the datasource is the table.
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = {r[0] for r in cur.fetchall()}
        assert "druid" in schemas

        cur.execute(f"SHOW TABLES FROM {catalog}.druid")
        tables = {r[0] for r in cur.fetchall()}
        assert _DATASOURCE in tables

        # Querying <catalog>.druid.<datasource> through Trino IS reading through the federation
        # engine — Trino's druid connector reads live from the broker; nothing is landed.
        rows: list = []
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                cur.execute(f"SELECT id, name FROM {catalog}.druid.{_DATASOURCE}")
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
