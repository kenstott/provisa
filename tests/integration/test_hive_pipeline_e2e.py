# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7e5a94-0b6d-41f3-8a27-9e4b13d0c765
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for Hive (HDFS-less local warehouse) as a connector-source.

``test_hive_source_e2e.py`` proves Trino can reach the metastore. This proves Provisa can — see
``connector_source_harness`` for why the two are different claims.

Hive is the reason the harness has a post-createSource seed hook. There is no table in the
warehouse until one is written *through* the catalog, so the data cannot exist before the source
is created, and ``registerTable`` cannot introspect a table that has not been written yet. The
seed therefore runs between the two, over a plain Trino cursor pointed at the catalog Provisa just
created — which incidentally re-asserts that the catalog name Provisa recorded is the one that
physically exists.
"""

from __future__ import annotations

import time

import pytest
import trino.dbapi

from tests.integration.connector_source_harness import (
    assert_registration_and_query,
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
)
from tests.integration.test_hive_source_e2e import (
    _SCHEMA,
    _TABLE,
    _TRINO_HOST,
    _TRINO_PORT,
    _WIDGETS,
    _seed_hive,
)

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

# Hyphen-free id, empty database — keeps the recorded catalog name and the physical one identical.
_SOURCE_ID = "hive_pipeline"
_DOMAIN = "hive_e2e"


def _trino_cursor():
    """A cursor on the isolated stack's coordinator, waiting for it to answer at all."""
    deadline = time.monotonic() + 120
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = trino.dbapi.connect(
                host=_TRINO_HOST, port=_TRINO_PORT, user="itest", catalog="system"
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            return conn, cur
        except Exception as exc:
            last_exc = exc
            time.sleep(2)
    raise RuntimeError(f"Trino did not become ready within 120s: {last_exc!r}")


@pytest.mark.requires_hive
async def test_hive_registers_and_serves_semantic_query(connector_client):  # noqa: F811
    """createSource → seed through the catalog → registerTable → rebuildSchemas → semantic SELECT."""
    conn, cur = _trino_cursor()

    def _seed(catalog: str) -> None:
        _seed_hive(cur, catalog)

    try:
        await assert_registration_and_query(
            connector_client,
            source_id=_SOURCE_ID,
            source_type="hive",
            host="hive-metastore",
            port=9083,
            domain_id=_DOMAIN,
            schema_name=_SCHEMA,
            table_name=_TABLE,
            columns=["id", "name"],
            order_by="id",
            expected_rows=[{"id": wid, "name": name} for wid, name in _WIDGETS],
            seed_after_create_source=_seed,
        )
    finally:
        conn.close()
