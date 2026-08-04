# Copyright (c) 2026 Kenneth Stott
# Canary: 3f9b47c1-52a6-4d08-b7e3-0c1d86af5920
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for MongoDB as a connector-source: registration through to a served query.

See ``connector_source_harness`` for why reachability and this are different claims.

MongoDB is the only source in this bucket with no fixed schema of its own, so
``information_schema`` for the catalog is synthesised entirely from the ``_schema`` collection
Trino's connector reads (``mongodb.schema-collection``, set unconditionally by
``TrinoMongoConnector.details``, trino_connectors.py:200). That makes untyped registration the
whole test: if ``_schema`` is not found or not honoured, every column resolves no type and
``registerTable`` refuses. ``db/mongo-init.js`` seeds both the documents and the matching
``_schema`` entry when the container's volume is first created, so no seeding runs here.

``comment`` and ``created_at`` are left out of the projection deliberately — three columns are
enough to prove type resolution, and the ``date`` column would only assert JSON date formatting.
"""

from __future__ import annotations

import pytest

from tests.integration.connector_source_harness import (
    assert_registration_and_query,
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
)

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]

# Hyphen-free id, empty database — keeps the recorded catalog name and the physical one identical.
_SOURCE_ID = "mongodb_pipeline"
_DOMAIN = "mongo_e2e"

# The Mongo database is the Trino schema; the collection is the table.
_SCHEMA = "provisa"
_TABLE = "product_reviews"
_COLUMNS = ["product_id", "reviewer", "rating"]

# db/mongo-init.js, in reviewer order.
_REVIEWS = [
    (1, "alice", 5),
    (1, "bob", 4),
    (2, "carol", 3),
    (3, "david", 5),
    (3, "eve", 4),
    (4, "frank", 2),
    (5, "grace", 5),
    (5, "henry", 4),
    (6, "iris", 3),
    (7, "jack", 1),
]


@pytest.mark.requires_mongodb
async def test_mongodb_registers_and_serves_semantic_query(connector_client):  # noqa: F811
    """createSource → registerTable (types introspected) → rebuildSchemas → semantic SELECT."""
    await assert_registration_and_query(
        connector_client,
        source_id=_SOURCE_ID,
        source_type="mongodb",
        host="mongodb",
        port=27017,
        domain_id=_DOMAIN,
        schema_name=_SCHEMA,
        table_name=_TABLE,
        columns=_COLUMNS,
        order_by="reviewer",
        expected_rows=[
            {"product_id": pid, "reviewer": who, "rating": stars} for pid, who, stars in _REVIEWS
        ],
    )
