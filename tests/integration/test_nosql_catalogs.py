# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-251: NoSQL/non-relational catalog generation verified against live Trino.

Drives the real create_catalog path (catalog_properties_for -> dynamic CREATE
CATALOG) for a Prometheus source and queries a metric end-to-end. Prometheus is
the connector that needs no on-disk table-description files, so it is fully
verifiable against the running stack.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
# Reachable from the Trino container's network (compose service name).
_PROM_URL = os.environ.get("PROM_INTERNAL_URL", "http://prometheus:9090")


def _trino_cursor():
    import trino

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


async def test_prometheus_catalog_created_and_queryable():
    pytest.importorskip("trino")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    try:
        conn, cur = _trino_cursor()
    except Exception:
        pytest.skip("Trino not reachable on localhost:8080")

    catalog = "prom_itest"
    _drop(cur, catalog)
    src = Source(id="prom-itest", type=SourceType.prometheus, mapping={"url": _PROM_URL})
    try:
        # Real path: builds props via catalog_properties_for, issues CREATE CATALOG.
        create_catalog(conn, src, "")

        # The catalog now exists and exposes the prometheus 'default' schema.
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = {r[0] for r in cur.fetchall()}
        assert "default" in schemas

        # The 'up' metric is always present; querying it proves data flows through.
        cur.execute(f"SELECT value FROM {catalog}.default.up LIMIT 1")
        rows = cur.fetchall()
        assert len(rows) >= 1
    finally:
        _drop(cur, catalog)
