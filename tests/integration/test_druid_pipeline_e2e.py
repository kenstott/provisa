# Copyright (c) 2026 Kenneth Stott
# Canary: 8b4f9d26-7e10-4a3c-b592-1d7c60ea3f88
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for Druid as a connector-source: registration through to a served query.

``test_druid_source_e2e.py`` proves Trino can reach Druid. This proves Provisa can — see
``connector_source_harness`` for why the two are different claims.

Druid's Trino catalog exposes one fixed schema, ``druid``, with each datasource as a table. Both
seeded dimensions are strings: Druid's native batch ingestion declares ``id``/``name`` as
dimensions without types, so ``id`` arrives as a VARCHAR and is asserted as one rather than being
coerced to look tidier.
"""

from __future__ import annotations

import pytest

from tests.integration.connector_source_harness import (
    assert_registration_and_query,
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
)
from tests.integration.test_druid_source_e2e import _DATASOURCE, _WIDGETS, _seed_druid

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

# Hyphen-free id, empty database — keeps the recorded catalog name and the physical one identical.
_SOURCE_ID = "druid_pipeline"
_DOMAIN = "druid_e2e"


@pytest.mark.requires_druid
async def test_druid_registers_and_serves_semantic_query(connector_client):  # noqa: F811
    """createSource → registerTable (types introspected) → rebuildSchemas → semantic SELECT.

    ``_seed_druid`` already blocks until the ingestion task succeeds and the broker answers a
    ``COUNT(*)``, so no settle window is needed once it returns. ``host="druid"`` is the compose
    service name of the broker; Trino reaches it at ``druid:8082`` on the stack's private network.
    """
    _seed_druid()

    await assert_registration_and_query(
        connector_client,
        source_id=_SOURCE_ID,
        source_type="druid",
        host="druid",
        port=8082,
        domain_id=_DOMAIN,
        schema_name="druid",
        table_name=_DATASOURCE,
        columns=["id", "name"],
        order_by="id",
        expected_rows=[{"id": wid, "name": name} for wid, name in _WIDGETS],
    )
