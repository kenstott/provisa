# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e9b41-05af-4c38-9e6b-1c84f0a7d3e5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a branch reaches when it never entered a credential (REQ-1529, REQ-1491, REQ-1492).

Two real schemas and a real registry, because the whole claim is about resolution across them: the
branch's own ``sources`` row is the unbound row a copy produces, and the connection values come from
the base's schema at the moment they are read. A double could not tell the difference between that
and a copy, which is the one difference these tests exist to observe.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import MetaData, select, update

from provisa.core import env_bindings as eb
from provisa.core.env_copy import REPLACE, copy_model
from provisa.core.environments import org_schema
from provisa.core.schema_admin import environments, init_registry_schema
from provisa.core.schema_org import sources

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BASE = "dev"
BRANCH = "fix"
DEEPER = "fixfix"
OWNER = "uid-admin"
DEVELOPER = "uid-developer"


def _scoped(table, schema):
    return table.to_metadata(MetaData(schema=schema), schema=schema)


@pytest.fixture
async def planes(docker_postgres):
    """A registry plus three schemas: prod, a base branched from it, and a branch of the base."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema

    org_id = f"envbind{uuid.uuid4().hex[:8]}"
    url = (
        f"postgresql+asyncpg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    # Separate engines: the tenant Database sets ``search_path`` on the session it checks out, and
    # a shared pool would hand that session back to the registry, whose tables are not on it.
    admin_engine = create_engine_from_url(url, pool_size=2)
    tenant_engine = create_engine_from_url(url, pool_size=2)
    admin_db = Database(admin_engine, name="admin")
    tenant_db = Database(tenant_engine, name="org", search_path=org_schema(org_id))
    await init_registry_schema(admin_db, org_id)

    schema_sql = (Path(__file__).parents[2] / "provisa" / "core" / "schema.sql").read_text()
    for env in (None, BASE, BRANCH, DEEPER):
        await init_schema(tenant_db, schema_sql, org_id=org_id, env=env)

    # prod arrives with the org (REQ-1487) and is a base by construction: it branched from nothing.
    async with admin_db.acquire() as conn:
        await conn.execute_core(
            environments.insert().values(
                [
                    {
                        "org_id": org_id,
                        "name": BASE,
                        "created_by": OWNER,
                        # Named on every row: a multi-row VALUES takes its columns from the first
                        # dict, so a key absent there is absent from all of them.
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

    async def add_source(env, **values):
        scoped = _scoped(sources, org_schema(org_id, env))
        async with tenant_db.acquire() as conn:
            await conn.execute_core(scoped.insert().values(**values))

    async def set_source(env, key, **values):
        scoped = _scoped(sources, org_schema(org_id, env))
        async with tenant_db.acquire() as conn:
            await conn.execute_core(scoped.update().where(scoped.c.id == key).values(**values))

    async def row(env, key):
        scoped = _scoped(sources, org_schema(org_id, env))
        async with tenant_db.acquire() as conn:
            result = await conn.execute_core(select(scoped).where(scoped.c.id == key))
            return dict(result.fetchone()._mapping)

    yield type(
        "Planes",
        (),
        {
            "org_id": org_id,
            "admin": admin_db,
            "tenant": tenant_db,
            "add_source": staticmethod(add_source),
            "set_source": staticmethod(set_source),
            "row": staticmethod(row),
        },
    )
    await admin_engine.dispose()
    await tenant_engine.dispose()


@pytest.fixture
async def bound(planes):
    """The base holds a bound warehouse; the branch holds the unbound row a copy produces."""
    await planes.add_source(
        BASE,
        id="warehouse",
        type="postgres",
        host="dev-db.internal",
        port=5432,
        database="scratch",
        username="dev_reader",
        dialect="postgresql",
        description="the warehouse",
    )
    await copy_model(planes.tenant, planes.org_id, BASE, BRANCH, mode=REPLACE)
    await copy_model(planes.tenant, planes.org_id, BRANCH, DEEPER, mode=REPLACE)
    return planes


class TestLineage:
    async def test_a_base_is_its_own_root(self, planes):
        assert await eb.base_of(planes.admin, planes.org_id, BASE) == BASE

    async def test_a_branch_resolves_to_the_base_it_came_from(self, planes):
        assert await eb.lineage(planes.admin, planes.org_id, BRANCH) == [BRANCH, BASE]

    async def test_branching_a_branch_still_ends_at_the_base(self, planes):
        assert await eb.base_of(planes.admin, planes.org_id, DEEPER) == BASE

    async def test_an_unknown_environment_is_refused(self, planes):
        with pytest.raises(eb.BindingError, match="no environment"):
            await eb.lineage(planes.admin, planes.org_id, "nowhere")


class TestResolution:
    async def test_a_branch_reaches_the_bases_connection_without_holding_it(self, bound):
        # The whole point: the branch's own row says nothing about where the source is.
        own = await bound.row(BRANCH, "warehouse")
        assert (own["host"], own["database"], own["bound"]) == ("", "", False)

        resolved_from, values = await eb.resolve(
            bound.admin, bound.tenant, bound.org_id, BRANCH, "sources", "warehouse"
        )
        assert resolved_from == BASE
        assert (values["host"], values["database"], values["username"]) == (
            "dev-db.internal",
            "scratch",
            "dev_reader",
        )

    async def test_rotating_the_base_moves_every_branch_with_it(self, bound):
        # Nothing was materialized, so nothing has to be re-copied for the change to take.
        await bound.set_source(BASE, "warehouse", host="dev-db-2.internal")
        _, values = await eb.resolve(
            bound.admin, bound.tenant, bound.org_id, DEEPER, "sources", "warehouse"
        )
        assert values["host"] == "dev-db-2.internal"

    async def test_a_branch_of_a_branch_resolves_through_the_one_that_bound_it(self, bound):
        await bound.set_source(BRANCH, "warehouse", host="own-db.internal", bound=True)
        resolved_from, values = await eb.resolve(
            bound.admin, bound.tenant, bound.org_id, DEEPER, "sources", "warehouse"
        )
        assert (resolved_from, values["host"]) == (BRANCH, "own-db.internal")

    async def test_an_environment_that_bound_it_resolves_to_itself(self, bound):
        resolved_from, _ = await eb.resolve(
            bound.admin, bound.tenant, bound.org_id, BASE, "sources", "warehouse"
        )
        assert resolved_from == BASE

    async def test_a_source_nobody_bound_is_a_refusal_and_not_an_empty_connection(self, planes):
        # An empty host is read as localhost:5432 by the connection builder, so resolving to one
        # would be worse than failing: it would silently point at whatever is on that port.
        # ``bound`` defaults true — a row inserted by hand is a row somebody bound — so the
        # state under test is written explicitly rather than left to the default.
        await planes.add_source(BASE, id="unbound", type="postgres", bound=False)
        await copy_model(planes.tenant, planes.org_id, BASE, BRANCH, mode=REPLACE)
        with pytest.raises(eb.BindingError, match="unbound in"):
            await eb.resolve(
                planes.admin, planes.tenant, planes.org_id, BRANCH, "sources", "unbound"
            )

    async def test_only_an_identity_only_table_holds_bindings(self, planes):
        with pytest.raises(eb.BindingError, match="IDENTITY_ONLY"):
            await eb.resolve(planes.admin, planes.tenant, planes.org_id, BRANCH, "domains", "sales")


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

        with pytest.raises(IntegrityError):
            async with planes.admin.acquire() as conn:
                await conn.execute_core(
                    update(environments)
                    .where(environments.c.org_id == planes.org_id, environments.c.name == BASE)
                    .values(branched_from="does-not-exist")
                )
