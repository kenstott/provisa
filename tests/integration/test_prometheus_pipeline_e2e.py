# Copyright (c) 2026 Kenneth Stott
# Canary: b5e08f37-1c62-4d94-a7f8-3096ce2b41d7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for Prometheus as a connector-source: registration through to a served query.

See ``connector_source_harness`` for why reachability and this are different claims.

Prometheus is a time series server, not a table store, so this test cannot assert rows. The data
is whatever the container has scraped: ``observability/prometheus.yml`` targets ``trino:8080``,
which resolves inside the stack's project network, so the ``up`` metric accumulates one sample per
scrape interval and its row count changes continuously. The assertion is therefore an aggregate,
as with Pinot — still a full trip through the settled pipeline, and ``COUNT(*)`` still requires
``registerTable`` to have resolved real types from the catalog's ``information_schema``.

``up`` is the metric to use because Prometheus itself generates it for every configured target: no
seeding is possible or needed, and the test never touches Prometheus directly (the service
publishes no host port — Trino reaches it at ``prometheus:9090`` on the project network).

The registered columns are ``timestamp`` and ``value``. The connector also exposes a ``labels``
column typed ``map(varchar, varchar)``; it is omitted because a map column asserts JSON encoding of
a Trino map rather than anything about the registration path.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from tests.integration.connector_source_harness import (
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
    create_domain,
    create_source,
    query_semantic,
    rebuild_schemas,
    register_table,
)

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

# Hyphen-free id, empty database — keeps the recorded catalog name and the physical one identical.
_SOURCE_ID = "prometheus_pipeline"
_DOMAIN = "prom_e2e"

# The connector's fixed schema; each metric name is a table.
_SCHEMA = "default"
_METRIC = "up"
_COLUMNS = ["timestamp", "value"]

# One scrape every 15s (observability/prometheus.yml), plus Prometheus and Trino start-up.
_SETTLE_SECONDS = 180


@pytest.mark.requires_prometheus
async def test_prometheus_registers_and_serves_semantic_query(connector_client):  # noqa: F811
    """createSource → registerTable (types introspected) → rebuildSchemas → semantic aggregate."""
    await create_source(
        connector_client,
        source_id=_SOURCE_ID,
        source_type="prometheus",
        host="prometheus",
        port=9090,
        mapping={"tables": []},
    )
    await create_domain(connector_client, _DOMAIN)
    await register_table(
        connector_client,
        source_id=_SOURCE_ID,
        domain_id=_DOMAIN,
        schema_name=_SCHEMA,
        table_name=_METRIC,
        alias=_METRIC,
        columns=_COLUMNS,
    )
    await rebuild_schemas(connector_client)

    # Registration is expected to succeed on the first attempt; only the sample count is timing
    # dependent, so only the query is retried.
    sql = f'SELECT COUNT(*) AS samples FROM "{_DOMAIN}"."{_METRIC}"'
    deadline = time.monotonic() + _SETTLE_SECONDS
    while True:
        rows = await query_semantic(connector_client, sql)
        if (rows and rows[0]["samples"] > 0) or time.monotonic() >= deadline:
            break
        await asyncio.sleep(5)

    assert rows, f"semantic aggregate over the {_METRIC} metric returned no rows: {rows!r}"
    assert rows[0]["samples"] > 0, f"no scraped samples within {_SETTLE_SECONDS}s: {rows!r}"
