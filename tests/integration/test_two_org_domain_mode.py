# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1d9b74-2e58-4a03-9f61-5d7c08ab3e12
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1349: two orgs, one process, opposite domain modes — neither disturbs the other.

The unit tests cover the scope key in isolation. What only a live control plane can show is that
the override each org's mode is PERSISTED in lives in that org's own schema, and that the policy
the query path reads under a bound org is the one that org wrote — the single-domain org keeps
single-domain while the namespaced org keeps namespaced, in the same interpreter, with the real
``current_org`` ContextVar driving the resolution.
"""

from __future__ import annotations

import os

import pytest

from provisa.api.org_runtime import current_org, reset_current_org, set_current_org
from provisa.core import domain_policy
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.org_settings import read_org_overrides, write_org_overrides

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ORGS = ("req1349a", "req1349b")

_SCHEMA_SQL = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "provisa", "core", "schema.sql")
)


async def _drop(db: Database, org_id: str) -> None:
    async with db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS org_{org_id} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS org_{org_id}_mv_cache CASCADE")


@pytest.fixture
async def tenant_dbs():
    engine = create_engine_from_url(_ASYNC_URL)
    dbs = {}
    with open(_SCHEMA_SQL, encoding="utf-8") as fh:
        schema_sql = fh.read()
    for org_id in _ORGS:
        db = Database(engine, name="tenant", search_path=f"org_{org_id}")
        await _drop(db, org_id)
        await init_schema(db, schema_sql, org_id=org_id)
        dbs[org_id] = db
    yield dbs
    for org_id, db in dbs.items():
        await _drop(db, org_id)
    await engine.dispose()


@pytest.fixture
def scoped_policy():
    # The resolver the API layer installs at startup. Without it every org would read and write the
    # one deployment-wide policy, which is exactly the collision this test exists to rule out.
    domain_policy.set_scope_resolver(current_org.get)
    yield
    domain_policy.set_scope_resolver(None)
    domain_policy.reset_all()


async def test_two_orgs_hold_opposite_domain_modes(tenant_dbs, scoped_policy):
    a, b = _ORGS
    await write_org_overrides(
        tenant_dbs[a],
        {"naming": {"use_domains": False, "default_domain": "sales"}},
        updated_by="alice",
    )
    await write_org_overrides(tenant_dbs[b], {"naming": {"use_domains": True}}, updated_by="bob")

    # Each org's override is in its OWN schema: neither read sees the other's row.
    assert await read_org_overrides(tenant_dbs[a]) == {
        "naming": {"use_domains": False, "default_domain": "sales"}
    }
    assert await read_org_overrides(tenant_dbs[b]) == {"naming": {"use_domains": True}}

    # Apply each org's mode the way build_org_runtime does — bound org, then configure.
    for org_id, use, default in ((a, False, "sales"), (b, True, "default")):
        token = set_current_org(org_id)
        try:
            domain_policy.configure(use, default)
        finally:
            reset_current_org(token)

    # Configuring the second org did not move the first. Read back under each binding.
    token = set_current_org(a)
    try:
        assert domain_policy.single_domain() is True
        assert domain_policy.default_domain() == "sales"
    finally:
        reset_current_org(token)

    token = set_current_org(b)
    try:
        assert domain_policy.single_domain() is False
        assert domain_policy.use_domains() is True
    finally:
        reset_current_org(token)

    # An org that configured nothing still inherits the deployment-wide policy, not a neighbour's.
    token = set_current_org("req1349c")
    try:
        assert domain_policy.use_domains() is None
    finally:
        reset_current_org(token)
