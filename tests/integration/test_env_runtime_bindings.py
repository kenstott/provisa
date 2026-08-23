# Copyright (c) 2026 Kenneth Stott
# Canary: 1f7d3c58-90b4-4e26-a3f1-8c25be40d719
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The source map an environment's RUNTIME is built from (REQ-1529, REQ-1491).

``env_bindings.resolve`` is covered elsewhere; what is covered here is the one seam between it and
the runtime -- ``_overlay_env_bindings``, which turns the rows a branch actually holds into the
connection the query path dials. It runs against real schemas because the interesting claims are
about rows the branch does NOT hold: an unbound source must arrive still unbound (an invented host
dials localhost), and a credential rotated on the base must reach the branch with no act of its own,
which is only observable when the base's row is the one being read at that moment.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import MetaData, select

from provisa.api import app as app_module
from provisa.api.app import _overlay_env_bindings
from provisa.core.env_copy import REPLACE, copy_model
from provisa.core.environments import org_schema
from provisa.core.schema_admin import environments, init_registry_schema
from provisa.core.schema_org import sources

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BASE = "dev"
BRANCH = "fix"
OWNER = "uid-admin"
DEVELOPER = "uid-developer"


def _scoped(table, schema):
    return table.to_metadata(MetaData(schema=schema), schema=schema)


@pytest.fixture
async def runtime(docker_postgres, monkeypatch):
    """A base holding a bound warehouse and an unbound lake, and a branch copied from it."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema

    org_id = f"envrt{uuid.uuid4().hex[:8]}"
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
    for env in (None, BASE, BRANCH):
        await init_schema(tenant_db, schema_sql, org_id=org_id, env=env)

    async with admin_db.acquire() as conn:
        await conn.execute_core(
            environments.insert().values(
                [
                    {"org_id": org_id, "name": BASE, "created_by": OWNER, "branched_from": None},
                    {
                        "org_id": org_id,
                        "name": BRANCH,
                        "created_by": DEVELOPER,
                        "branched_from": BASE,
                    },
                ]
            )
        )

    scoped_base = _scoped(sources, org_schema(org_id, BASE))
    async with tenant_db.acquire() as conn:
        await conn.execute_core(
            scoped_base.insert().values(
                id="warehouse",
                type="postgres",
                host="base-db.internal",
                port=5432,
                database="scratch",
                username="base_reader",
                dialect="postgresql",
            )
        )
        await conn.execute_core(
            scoped_base.insert().values(
                # Explicitly unbound: ``bound`` defaults to true, and a base that never entered a
                # credential for this source is the case the branch must not be handed a
                # connection for.
                id="lake",
                type="postgres",
                dialect="postgresql",
                host="",
                database="",
                bound=False,
            )
        )
    await copy_model(tenant_db, org_id, BASE, BRANCH, mode=REPLACE)

    monkeypatch.setattr(app_module.state, "admin_db", admin_db)
    monkeypatch.setattr(app_module.state, "tenant_db", tenant_db)

    async def branch_rows():
        """The branch's own rows, exactly as the runtime builder reads them from the DB."""
        scoped = _scoped(sources, org_schema(org_id, BRANCH))
        async with tenant_db.acquire() as conn:
            result = await conn.execute_core(select(scoped))
            return {r._mapping["id"]: dict(r._mapping) for r in result.fetchall()}

    async def rebind_base(**values):
        async with tenant_db.acquire() as conn:
            await conn.execute_core(
                scoped_base.update().where(scoped_base.c.id == "warehouse").values(**values)
            )

    yield type(
        "Runtime",
        (),
        {
            "org_id": org_id,
            "branch_rows": staticmethod(branch_rows),
            "rebind_base": staticmethod(rebind_base),
        },
    )
    await admin_engine.dispose()
    await tenant_engine.dispose()


class TestTheSourceMapABranchRunsOn:
    async def test_the_branch_dials_the_bases_connection(self, runtime):
        rows = await runtime.branch_rows()
        assert rows["warehouse"]["host"] == ""  # the unbound row a copy produces (REQ-1491)
        overlaid, supplier = await _overlay_env_bindings(rows, runtime.org_id, BRANCH)
        assert overlaid["warehouse"]["host"] == "base-db.internal"
        assert overlaid["warehouse"]["username"] == "base_reader"
        assert supplier["warehouse"] == BASE

    async def test_the_overlaid_source_is_marked_bound(self, runtime):
        overlaid, _supplier = await _overlay_env_bindings(
            await runtime.branch_rows(), runtime.org_id, BRANCH
        )
        assert overlaid["warehouse"]["bound"] is True

    async def test_identity_and_governance_are_the_branchs_own(self, runtime):
        rows = await runtime.branch_rows()
        overlaid, _supplier = await _overlay_env_bindings(rows, runtime.org_id, BRANCH)
        assert overlaid["warehouse"]["id"] == "warehouse"
        assert overlaid["warehouse"]["type"] == rows["warehouse"]["type"]

    async def test_a_rotated_credential_reaches_the_branch_with_no_act_of_its_own(self, runtime):
        await runtime.rebind_base(host="rotated-db.internal", username="rotated_reader")
        overlaid, _supplier = await _overlay_env_bindings(
            await runtime.branch_rows(), runtime.org_id, BRANCH
        )
        assert overlaid["warehouse"]["host"] == "rotated-db.internal"
        assert overlaid["warehouse"]["username"] == "rotated_reader"


class TestWhatIsNotInvented:
    async def test_a_source_nobody_bound_arrives_still_unbound(self, runtime):
        rows = await runtime.branch_rows()
        overlaid, supplier = await _overlay_env_bindings(rows, runtime.org_id, BRANCH)
        assert overlaid["lake"] == rows["lake"]
        assert overlaid["lake"]["bound"] is False
        assert "lake" not in supplier

    async def test_an_unbound_source_never_gains_a_host(self, runtime):
        overlaid, _supplier = await _overlay_env_bindings(
            await runtime.branch_rows(), runtime.org_id, BRANCH
        )
        assert overlaid["lake"]["host"] == ""

    async def test_every_source_survives_the_overlay(self, runtime):
        rows = await runtime.branch_rows()
        overlaid, _supplier = await _overlay_env_bindings(rows, runtime.org_id, BRANCH)
        assert set(overlaid) == set(rows) == {"warehouse", "lake"}
