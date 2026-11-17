# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1d6b2a-8c47-4e09-a5b3-71e0c9d4f286
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The platform plane's secret search reads only the platform registry (REQ-2087, REQ-1922).

The search reads the platform plane through the registry handle (``admin_db``), which refuses
the deployment's operating state (``platform_state_db``). ``running_sinks`` (REQ-812) is the first
platform-state table that carries an ``org_id``, and the search took every ``org_id`` table of the
platform metadata: the registry handle refused the statement, so loading an org's redaction rules
failed and every request that first used the org answered 500. Operating state holds what the
deployment is doing, not a stored value resolved as a secret reference, so it is not searched."""

# Requirements: REQ-2087, REQ-1922, REQ-812

from __future__ import annotations

import pytest
from sqlalchemy import insert

from provisa.core import schema_admin, secret_references
from provisa.core.database import Database
from provisa.core.platform_state import TABLES as PLATFORM_STATE

pytestmark = [pytest.mark.unit]


def _registry_handle(control_plane) -> Database:
    """The platform registry's handle as the app builds it (control_plane.bring_up_platform):
    it refuses org and platform-state tables."""
    return Database(control_plane.engine(), name="platform", holds="platform_admin")


def test_an_org_scoped_platform_state_table_exists():
    # The case under test: an operating-state table the org_id scope would otherwise take.
    assert any("org_id" in schema_admin.metadata.tables[t].c for t in PLATFORM_STATE)


@pytest.mark.asyncio
async def test_referenced_names_reads_through_the_registry_handle(control_plane):
    model, admin = control_plane.org(), _registry_handle(control_plane)
    async with admin.acquire() as conn:
        await conn.execute_core(
            insert(schema_admin.environments).values(
                org_id="default",
                name="dev",
                created_by="${secret:on_platform}",
                parent="prod",
                data_mode="inherit",
            )
        )

    found = await secret_references.referenced_names(admin, "default", environments={"prod": model})

    assert found == {"on_platform"}


@pytest.mark.asyncio
async def test_references_reads_through_the_registry_handle(control_plane):
    model, admin = control_plane.org(), _registry_handle(control_plane)
    async with admin.acquire() as conn:
        await conn.execute_core(
            insert(schema_admin.environments).values(
                org_id="default",
                name="dev",
                created_by="${secret:on_platform}",
                parent="prod",
                data_mode="inherit",
            )
        )

    found = await secret_references.references(
        admin, "default", "on_platform", environments={"prod": model}
    )

    assert [(r.table, r.environment) for r in found] == [("environments", None)]
