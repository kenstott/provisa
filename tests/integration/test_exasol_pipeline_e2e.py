# Copyright (c) 2026 Kenneth Stott
# Canary: 6a2d0b17-4c93-4f58-a1e7-2b8d95f30c41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""True E2E for Exasol as a connector-source: registration through to a served query.

``test_exasol_source_e2e.py`` proves Trino can reach Exasol. This proves Provisa can — see
``connector_source_harness`` for why the two are different claims.

Exasol folds unquoted identifiers to UPPER CASE, so the schema, table, and columns are all
uppercase here. That is the interesting part of this source for the registration path: every
identifier travels from ``registerTable`` into an ``information_schema.columns`` lookup, and a
case mismatch anywhere resolves zero column types and refuses the registration.
"""

from __future__ import annotations

import pytest

from tests.integration.connector_source_harness import (
    assert_registration_and_query,
    connector_client,  # noqa: F401 — imported for pytest fixture discovery
)
from tests.integration.test_exasol_source_e2e import (
    _AMD64,
    _EXASOL_PASSWORD,
    _EXASOL_USER,
    _WIDGETS,
    _seed_exasol,
)

# Same platform gate as test_exasol_source_e2e: exasol/docker-db ships linux/amd64 only, and under
# QEMU on arm64 EXAStorage never finishes booting — cored comes up, /exa/data/storage stays empty,
# and the DB instance is never started, so the container can never turn healthy. Without the gate
# the per-test heavy provisioning (_heavy_db_service) fails the `docker compose up --wait` and the
# test ERRORs at setup instead of reporting the platform gap.
_UNBOOTABLE = "exasol/docker-db is amd64-only; unbootable under arm64 emulation"

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.asyncio(loop_scope="session"),
    pytest.mark.skipif(not _AMD64, reason=_UNBOOTABLE),
]

_SCHEMA = "PROVISA"
_TABLE = "WIDGETS"

# Hyphen-free id, empty database — keeps the recorded catalog name and the physical one identical.
_SOURCE_ID = "exasol_pipeline"
_DOMAIN = "exasol_e2e"


@pytest.mark.requires_exasol
async def test_exasol_registers_and_serves_semantic_query(connector_client):  # noqa: F811
    """createSource → registerTable (types introspected) → rebuildSchemas → semantic SELECT.

    ``ID`` is ``DECIMAL(18,0)`` on the Exasol side and arrives as a JSON number, so the integer
    literals in ``_WIDGETS`` compare equal to it without any coercion in the assertion.
    """
    _seed_exasol()

    await assert_registration_and_query(
        connector_client,
        source_id=_SOURCE_ID,
        source_type="exasol",
        host="exasol",
        port=8563,
        username=_EXASOL_USER,
        password=_EXASOL_PASSWORD,
        domain_id=_DOMAIN,
        schema_name=_SCHEMA,
        table_name=_TABLE,
        alias="widgets",
        columns=["ID", "NAME"],
        order_by="ID",
        expected_rows=[{"ID": wid, "NAME": name} for wid, name in _WIDGETS],
    )
