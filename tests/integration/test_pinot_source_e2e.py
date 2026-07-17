# Copyright (c) 2026 Kenneth Stott
# Canary: 80c87941-f60e-418b-8da8-92052a1fdc90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Pinot as a connector-source, read through the Provisa federation engine (REQ-1097).

A "connector source" has an entry in provisa/federation/trino_connectors.py::TRINO_CONNECTORS but
NOT in provisa/executor/drivers/registry.py — the ONLY way Provisa reaches it is by projecting it as
a live Trino catalog and querying through Trino. See test_cassandra_source_e2e.py for the full
description of the catalog seam; this file follows the same pattern.

Pinot specifics
---------------
- provisa/federation/trino_connectors.py TrinoPinotConnector emits pinot.controller-urls
  (source.host:source.port) — the Pinot controller's REST endpoint. Trino's pinot connector
  discovers brokers/servers from the controller.
- The `pinot` compose service runs `QuickStart -type batch`, a single container (zookeeper +
  controller + broker + server) preloaded with the classic sample offline table `airlineStats`.
  Trino reaches the controller in-network at pinot:9000; the host-published ${PINOT_CONTROLLER_PORT}
  is used only by this test process to wait for the controller to be healthy.
- Pinot tables live in Trino's implicit `default` schema. No manual seed — QuickStart is the seed.
"""

from __future__ import annotations

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
_PINOT_CONTROLLER_PORT = int(os.environ.get("PINOT_CONTROLLER_PORT", "9000"))

_SCHEMA = "default"
_TABLE = "airlineStats"


@pytest.fixture(scope="module", autouse=True)
def _wait_for_trino():
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


def _wait_for_pinot_table() -> None:
    """Wait until the QuickStart batch load has published the airlineStats offline table (the
    controller lists it) so the Trino query below is not racing the sample ingestion."""
    url = f"http://localhost:{_PINOT_CONTROLLER_PORT}/tables"
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310 - localhost test probe
                body = resp.read().decode()
            if _TABLE in body:
                return
        except (urllib.error.URLError, TimeoutError, OSError):
            pass
        time.sleep(3)
    raise RuntimeError(f"Pinot never published table {_TABLE!r} within 180s")


@pytest.mark.requires_pinot
async def test_pinot_catalog_created_and_queryable():
    """Register a pinot Source, project it as a live Trino catalog, query it end-to-end.

    Drives the real registration path: provisa.core.catalog.create_catalog builds the catalog
    properties from TrinoPinotConnector.details() and issues CREATE CATALOG against the live Trino
    coordinator. host="pinot" is the compose service name — Trino resolves it from inside its own
    container on the isolated stack's private network.
    """
    pytest.importorskip("trino")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    _wait_for_pinot_table()

    conn, cur = _trino_cursor()

    catalog = "pinot_itest"
    _drop(cur, catalog)
    src = Source(id="pinot-itest", type=SourceType.pinot, host="pinot", port=9000)
    try:
        create_catalog(conn, src, "")

        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = {r[0] for r in cur.fetchall()}
        assert _SCHEMA in schemas

        # Trino lowercases identifiers; the QuickStart table surfaces as airlinestats.
        tables: set[str] = set()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            cur.execute(f'SHOW TABLES FROM {catalog}."{_SCHEMA}"')
            tables = {r[0] for r in cur.fetchall()}
            if _TABLE.lower() in tables:
                break
            time.sleep(3)
        assert _TABLE.lower() in tables

        # Querying <catalog>.default.<table> through Trino IS reading through the federation engine —
        # Trino's pinot connector reads live from the cluster; nothing is landed.
        count = 0
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                cur.execute(f'SELECT count(*) FROM {catalog}."{_SCHEMA}"."{_TABLE.lower()}"')
                count = cur.fetchall()[0][0]
            except trino.exceptions.TrinoExternalError:
                count = 0
            if count > 0:
                break
            time.sleep(3)
        assert count > 0
    finally:
        _drop(cur, catalog)
        conn.close()
