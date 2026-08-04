# Copyright (c) 2026 Kenneth Stott
# Canary: 91e6c47a-3b28-4d05-9f6c-84a2b7e01d33
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for Pinot as a connector-source: registration through to a served query.

``test_pinot_source_e2e.py`` proves Trino can reach Pinot. This proves Provisa can — see
``connector_source_harness`` for why the two are different claims.

Pinot is the one source in this bucket whose data the test does not author: the container's
QuickStart loads the ``airlineStats`` sample table, thousands of rows wide and deep. So this test
uses the harness's individual steps rather than ``assert_registration_and_query`` and asserts on an
aggregate — a row-for-row expectation over vendor sample data would assert the sample's contents
rather than Provisa's behaviour, and would break whenever the image's fixture changes.

The aggregate is still a full trip through the settled pipeline: ``COUNT(*)`` and ``GROUP BY`` are
compiled and governed exactly like a projection, and the ``GROUP BY`` requires the registered
column to have resolved a real type during introspection.
"""

from __future__ import annotations

import pytest

from tests.integration.connector_source_harness import (
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
    create_domain,
    create_source,
    query_semantic,
    rebuild_schemas,
    register_table,
)
from tests.integration.test_pinot_source_e2e import _SCHEMA, _TABLE, _wait_for_pinot_table

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

# Hyphen-free id, empty database — keeps the recorded catalog name and the physical one identical.
_SOURCE_ID = "pinot_pipeline"
_DOMAIN = "pinot_e2e"
_ALIAS = "airline_stats"

# Two columns of the QuickStart schema: a dimension and a metric-shaped int. Both must resolve a
# type from information_schema for registration to be accepted.
#
# Lowercase because that is the column's identity in the engine, not a spelling choice: the Trino
# Pinot connector lowercases every Pinot column name, so ``information_schema.columns`` for
# airlinestats reports ``carrier``/``arrdelay`` and registering the Pinot-cased ``Carrier``/
# ``ArrDelay`` is refused with schema.column_types_unresolved — the same lowercasing the table name
# already goes through above.
_COLUMNS = ["carrier", "arrdelay"]


@pytest.mark.requires_pinot
async def test_pinot_registers_and_serves_semantic_query(connector_client):  # noqa: F811
    """createSource → registerTable (types introspected) → rebuildSchemas → semantic aggregate."""
    _wait_for_pinot_table()

    await create_source(
        connector_client,
        source_id=_SOURCE_ID,
        source_type="pinot",
        host="pinot",
        port=9000,
    )
    await create_domain(connector_client, _DOMAIN)
    await register_table(
        connector_client,
        source_id=_SOURCE_ID,
        domain_id=_DOMAIN,
        # Trino lowercases the Pinot table name; the schema is Pinot's fixed "default".
        schema_name=_SCHEMA,
        table_name=_TABLE.lower(),
        alias=_ALIAS,
        columns=_COLUMNS,
    )
    await rebuild_schemas(connector_client)

    rows = await query_semantic(
        connector_client,
        f'SELECT "carrier", COUNT(*) AS flights FROM "{_DOMAIN}"."{_ALIAS}" '
        'GROUP BY "carrier" ORDER BY flights DESC',
    )
    assert rows, "semantic aggregate over airlineStats returned no rows"
    assert sum(r["flights"] for r in rows) > 0
    assert all(r["carrier"] for r in rows), f"null carrier in {rows!r}"
