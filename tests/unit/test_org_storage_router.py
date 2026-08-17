# Copyright (c) 2026 Kenneth Stott
# Canary: 2f7a5c19-4b83-4d06-9e27-51cb6a0f38d2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1046/1048/1049: the admin surface for an org's storage footprint and its own store.

Two things this surface must never do: report a store DSN back to a caller (it is a credential to
a system the platform does not own), and accept a DSN that cannot be opened — an unusable one taken
here surfaces as a failed refresh hours later with nothing pointing at the setting that caused it.
"""

# Requirements: REQ-1046, REQ-1048, REQ-1049

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import StaticPool

from provisa.api.admin import org_storage_router as router_mod
from provisa.api.admin.org_storage_router import (
    OrgStorageBody,
    get_org_storage,
    put_org_storage,
)
from provisa.api.errors import ApiError
from provisa.api.org_runtime import OrgRegistry, OrgRuntime
from provisa.core.database import Database
from provisa.core.schema_admin import metadata, orgs

_ORG_STORE = "postgresql+asyncpg://acme:secret@acme-own-host/store"
_REQUEST = SimpleNamespace()


@pytest.fixture
async def admin_db():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        yield Database(engine, "test")
    finally:
        await engine.dispose()


@pytest.fixture
def app_state(admin_db, monkeypatch):
    registry = OrgRegistry()
    registry.set("acme", OrgRuntime("acme"))
    rebuilt: list[str] = []

    async def _rebuild(org_id, builder):
        rebuilt.append(org_id)

    registry.rebuild = _rebuild  # type: ignore[method-assign]
    state = SimpleNamespace(
        admin_db=admin_db,
        org_registry=registry,
        federation_engine=SimpleNamespace(
            materialize_store_dsn=lambda: "postgresql://platform/store"
        ),
        rebuilt=rebuilt,
    )
    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    # The guard and the acting-org resolution belong to the request pipeline, not to this surface.
    monkeypatch.setattr(router_mod, "require_org_settings", lambda request: None)
    monkeypatch.setattr(router_mod, "require_active_org_id", lambda request: "acme")

    async def _flags(org_id):
        return SimpleNamespace(
            seeded_demo=False,
            isolated_engine=False,
            external_engine=False,
            engine_kind=None,
            engine_url=None,
            shard=None,
            storage_url=None,
        )

    monkeypatch.setattr("provisa.api.app._read_org_flags", _flags, raising=False)
    return state


async def _org(admin_db: Database, org_id: str = "acme") -> None:
    async with admin_db.acquire() as conn:
        await conn.execute_core(orgs.insert().values(id=org_id, name=org_id))


async def _stored_dsn(admin_db: Database, org_id: str = "acme") -> bytes | None:
    async with admin_db.acquire() as conn:
        result = await conn.execute_core(
            select(orgs.c.storage_url_enc).where(orgs.c.id == org_id)
        )
        return result.fetchone()[0]


class TestGet:
    async def test_an_unknown_org_is_a_404_before_the_store_is_probed(self, app_state):
        with pytest.raises(ApiError) as exc:
            await get_org_storage(_REQUEST)
        assert exc.value.status_code == 404
        assert exc.value.code == "org_storage.org_not_found"

    async def test_the_report_is_returned_for_a_known_org(self, app_state, admin_db, monkeypatch):
        await _org(admin_db)

        async def _report(org_id):
            return {"org_id": org_id, "byo": False, "used_bytes": 7, "ceiling_bytes": 100}

        monkeypatch.setattr("provisa.storage.quota.storage_report", _report)
        assert await get_org_storage(_REQUEST) == {
            "org_id": "acme",
            "byo": False,
            "used_bytes": 7,
            "ceiling_bytes": 100,
        }


class TestPut:
    async def test_registering_a_store_encrypts_it_and_rebuilds_the_runtime(
        self, app_state, admin_db
    ):
        await _org(admin_db)
        result = await put_org_storage(OrgStorageBody(storage_url=_ORG_STORE), _REQUEST)

        # The response says only that one is set — never what it is.
        assert result == {"success": True, "org_id": "acme", "storage_url_set": True}
        assert "secret" not in repr(result)
        assert await _stored_dsn(admin_db) is not None
        # The store is resolved off the built runtime, so the change reaches the write paths only
        # once the runtime is rebuilt.
        assert app_state.rebuilt == ["acme"]

    async def test_clearing_returns_the_org_to_the_platform_store(self, app_state, admin_db):
        await _org(admin_db)
        await put_org_storage(OrgStorageBody(storage_url=_ORG_STORE), _REQUEST)

        result = await put_org_storage(OrgStorageBody(storage_url=None), _REQUEST)
        assert result["storage_url_set"] is False
        assert await _stored_dsn(admin_db) is None

    @pytest.mark.parametrize("bad", ["not a dsn", "://missing-scheme", "   "])
    async def test_an_unusable_dsn_is_refused_at_the_setting_not_at_the_first_write(
        self, app_state, admin_db, bad
    ):
        await _org(admin_db)
        try:
            result = await put_org_storage(OrgStorageBody(storage_url=bad), _REQUEST)
        except ApiError as exc:
            assert exc.status_code == 400
            assert exc.code == "org_storage.invalid_url"
            assert await _stored_dsn(admin_db) is None
            return
        # Whitespace-only is not a DSN at all; it clears rather than registering garbage.
        assert bad.strip() == ""
        assert result["storage_url_set"] is False
