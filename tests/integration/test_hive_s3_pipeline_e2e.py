# Copyright (c) 2026 Kenneth Stott
# Canary: 5d80a3e1-9c47-42b6-8f13-6e2907ab4d59
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for Hive-on-S3 as a connector-source: registration through to a served query.

``test_hive_s3_source_e2e.py`` proves Trino can reach the metastore and MinIO. This proves Provisa
can — see ``connector_source_harness`` for why the two are different claims.

The distinguishing detail for the registration path is the mapping: ``createSource`` carries the
S3 endpoint and credentials as ``mappingJson``, and those have to survive persistence and reach
the catalog properties intact, or introspection reads a table whose files it cannot open. The
plain hive test cannot cover that — it has no mapping at all.
"""

from __future__ import annotations

import pytest

from tests.integration.connector_source_harness import (
    assert_registration_and_query,
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
)
from tests.integration.test_hive_pipeline_e2e import _trino_cursor
from tests.integration.test_hive_s3_source_e2e import (
    _S3_MAPPING,
    _SCHEMA,
    _TABLE,
    _WIDGETS,
    _ensure_bucket,
    _seed_hive_s3,
)

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

# Hyphen-free id, empty database — keeps the recorded catalog name and the physical one identical.
_SOURCE_ID = "hive_s3_pipeline"
_DOMAIN = "hive_s3_e2e"


@pytest.mark.requires_hive_s3
async def test_hive_s3_registers_and_serves_semantic_query(connector_client):  # noqa: F811
    """createSource → seed through the catalog → registerTable → rebuildSchemas → semantic SELECT.

    The bucket is created first, from the host: S3A writes keys under an existing bucket and never
    creates one, so the warehouse write in the seed hook would fail without it.
    """
    _ensure_bucket()
    conn, cur = _trino_cursor()

    def _seed(catalog: str) -> None:
        _seed_hive_s3(cur, catalog)

    try:
        await assert_registration_and_query(
            connector_client,
            source_id=_SOURCE_ID,
            source_type="hive_s3",
            host="hive-s3-metastore",
            port=9083,
            mapping=dict(_S3_MAPPING),
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
