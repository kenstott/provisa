# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e9b41-05af-4c38-9e6b-1c84f0a7d3e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ``environments`` table's own referential integrity: a branch names a real parent, in the
same org, and a parent still named by a branch cannot be deleted out from under it."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BASE = "dev"
BRANCH = "fix"
DEEPER = "fixfix"
OWNER = "uid-admin"
DEVELOPER = "uid-developer"


@pytest.fixture
async def planes(docker_postgres):
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema
    from provisa.core.environments import org_schema
    from provisa.core.schema_admin import environments, init_registry_schema

    org_id = f"envreg{uuid.uuid4().hex[:8]}"
    url = (
        f"postgresql+asyncpg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    admin_engine = create_engine_from_url(url, pool_size=2)
    tenant_engine = create_engine_from_url(url, pool_size=2)
    admin_db = Database(admin_engine, name="admin")
    tenant_db = Database(tenant_engine, name="org", search_path=org_schema(org_id))
    await init_registry_schema(admin_db, org_id)

    schema_sql = (Path(__file__).parents[2] / "provisa" / "core" / "schema.sql").read_text()
    for env in (None, BASE, BRANCH, DEEPER):
        await init_schema(tenant_db, schema_sql, org_id=org_id, env=env)

    async with admin_db.acquire() as conn:
        await conn.execute_core(
            environments.insert().values(
                [
                    {
                        "org_id": org_id,
                        "name": BASE,
                        "created_by": OWNER,
                        "branched_from": None,
                    },
                    {
                        "org_id": org_id,
                        "name": BRANCH,
                        "created_by": DEVELOPER,
                        "branched_from": BASE,
                    },
                    {
                        "org_id": org_id,
                        "name": DEEPER,
                        "created_by": DEVELOPER,
                        "branched_from": BRANCH,
                    },
                ]
            )
        )

    yield type("Planes", (), {"org_id": org_id, "admin": admin_db, "tenant": tenant_db})
    await admin_engine.dispose()
    await tenant_engine.dispose()


class TestRegistry:
    async def test_a_base_cannot_be_deleted_while_a_branch_still_resolves_through_it(self, planes):
        from sqlalchemy.exc import IntegrityError

        from provisa.core.schema_admin import environments as env_table

        with pytest.raises(IntegrityError):
            async with planes.admin.acquire() as conn:
                await conn.execute_core(
                    env_table.delete().where(
                        env_table.c.org_id == planes.org_id, env_table.c.name == BASE
                    )
                )

    async def test_a_branch_cannot_name_an_environment_of_another_org(self, planes):
        from sqlalchemy.exc import IntegrityError

        from provisa.core.schema_admin import environments

        with pytest.raises(IntegrityError):
            async with planes.admin.acquire() as conn:
                await conn.execute_core(
                    environments.update()
                    .where(environments.c.org_id == planes.org_id, environments.c.name == BASE)
                    .values(branched_from="does-not-exist")
                )
