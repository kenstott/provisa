# Copyright (c) 2026 Kenneth Stott
# Canary: 21f034e8-fbce-4cfe-82d7-bed18fe97fe1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The domain-policy switch is refused while the org has a catalog (REQ-1919).

Every registered table's domain is bound to the policy. The switch used to wipe the org's
catalog as a side effect; it now answers with a refusal counting the tables, sources and domains
that exist, and removes nothing. With no catalog it is applied.

The scenarios run here on a SQLite control plane, and unchanged on PostgreSQL in
``tests/integration/test_domain_policy_switch_refused_pg.py``.
"""

# Requirements: REQ-1919, REQ-165

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

import provisa.api.app as appmod
from provisa.api.admin import settings_router
from provisa.api.errors import ApiError
from provisa.core import domain_policy
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.org_settings import read_org_overrides
from provisa.core.schema_org import domains, registered_tables, sources


@pytest.fixture
async def db() -> Database:
    plane = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="policy-test")
    await _init_schema_portable(plane)
    return plane


@pytest.fixture(autouse=True)
def wired(db, monkeypatch) -> list[int]:
    """The route's surroundings: the acting org's control plane, a caller who may change org
    settings, a deployment with no naming block, and a record of each schema rebuild."""
    rebuilds: list[int] = []

    async def _rebuild() -> None:
        rebuilds.append(1)

    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", appmod.state.tenant_db, raising=False)
    monkeypatch.setattr(appmod, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(settings_router, "require_org_settings", lambda request: None)
    monkeypatch.setattr(settings_router, "read_config", lambda: {})
    before = domain_policy.snapshot()
    yield rebuilds
    domain_policy.configure(*before)


def _request(body: dict[str, Any]) -> Any:
    async def _json() -> dict[str, Any]:
        return body

    return types.SimpleNamespace(
        json=_json, state=types.SimpleNamespace(identity=types.SimpleNamespace(user_id="ada"))
    )


async def _count(db: Database, table) -> int:
    async with db.acquire() as conn:
        return (await conn.execute_core(select(func.count()).select_from(table))).scalar_one()


async def _a_catalog(db: Database) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql", origin="admin"))
        await conn.execute_core(insert(domains).values(id="sales", origin="admin"))
        await conn.execute_core(insert(domains).values(id="lab", origin="config"))
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="pg",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                origin="admin",
            )
        )


async def test_the_switch_is_refused_while_a_catalog_exists_and_removes_nothing(db, wired):
    await _a_catalog(db)
    kept = [await _count(db, t) for t in (sources, domains, registered_tables)]

    with pytest.raises(ApiError) as err:
        await settings_router.set_domain_policy(
            _request({"use_domains": False, "default_domain": "main"})
        )

    # One of the domains is a config's: the operator is sent to the file, not told to empty a
    # catalog the next load would restore.
    assert (err.value.status_code, err.value.code) == (
        409,
        "settings.domain_policy_catalog_in_config",
    )
    assert "Set the policy in the config file" in err.value.detail
    assert err.value.params == {"tables": 1, "sources": 1, "domains": 2}
    assert [await _count(db, t) for t in (sources, domains, registered_tables)] == kept
    assert "naming" not in await read_org_overrides(db)
    assert wired == []


@pytest.mark.parametrize(
    ("leftover", "counts"),
    [
        ("source", {"tables": 0, "sources": 1, "domains": 0}),
        ("domain", {"tables": 0, "sources": 0, "domains": 1}),
    ],
)
async def test_one_source_or_one_domain_is_enough_to_refuse(db, leftover, counts):
    async with db.acquire() as conn:
        if leftover == "source":
            await conn.execute_core(
                insert(sources).values(id="pg", type="postgresql", origin="admin")
            )
        else:
            await conn.execute_core(insert(domains).values(id="sales", origin="admin"))
    with pytest.raises(ApiError) as err:
        await settings_router.set_domain_policy(_request({"use_domains": True}))
    # Made through the admin: the operator deletes it and switches then.
    assert err.value.code == "settings.domain_policy_catalog_exists"
    assert "Delete them first" in err.value.detail
    assert err.value.params == counts


async def test_what_the_deployment_keeps_is_not_a_catalog(db):
    """The seeded domains and the built-in sources do not stand in the way."""
    async with db.acquire() as conn:
        for source_id in ("provisa-admin", "provisa-otel", "__derived__"):
            await conn.execute_core(
                insert(sources).values(id=source_id, type="postgresql", origin="seed")
            )
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="provisa-admin",
                domain_id="meta",
                schema_name="public",
                table_name="registered_tables",
                origin="seed",
            )
        )
    answer = await settings_router.set_domain_policy(_request({"use_domains": True}))
    assert answer == {"success": True, "use_domains": True}


async def test_with_no_catalog_the_switch_is_applied(db, wired):
    answer = await settings_router.set_domain_policy(
        _request({"use_domains": False, "default_domain": "main"})
    )

    assert answer == {"success": True, "use_domains": False}
    assert (await read_org_overrides(db))["naming"] == {
        "use_domains": False,
        "default_domain": "main",
    }
    assert domain_policy.single_domain() and domain_policy.default_domain() == "main"
    # The single domain is seeded for the first registration to sit in.
    async with db.acquire() as conn:
        row = (
            await conn.execute_core(select(domains.c.origin).where(domains.c.id == "main"))
        ).fetchone()
    assert row is not None and row[0] == "seed"
    assert wired == [1]
