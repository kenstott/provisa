# Copyright (c) 2026 Kenneth Stott
# Canary: 8a1f34c7-5d6b-4e29-9c08-b7f2a4e61d35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1600: an environment can be given away for a span of USE rather than a span of time.

REQ-1523's expiry is a fixed deadline: an environment is never reaped for being idle, because a
quiet pre-prod is not an abandoned one. An environment handed to a visitor by an invitation is the
opposite case -- it is worth keeping exactly as long as somebody is working in it -- so it carries
an idle TTL, and the request that reaches it pushes the deadline out.
"""

# Requirements: REQ-1600, REQ-1523

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import StaticPool

from provisa.core.database import Database
from provisa.core.env_store import renew_idle_expiry
from provisa.core.schema_admin import REGISTRY_TABLES, environments, metadata, orgs


@pytest.fixture
async def admin_db():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(lambda sc: metadata.create_all(sc, tables=REGISTRY_TABLES))
    db = Database(engine, "test")
    async with db.acquire() as conn:
        await conn.execute_core(orgs.insert().values(id="acme", name="Acme"))
    try:
        yield db
    finally:
        await engine.dispose()


async def _write(db, name, *, expires_at, idle_ttl_seconds):
    async with db.acquire() as conn:
        await conn.execute_core(
            environments.insert().values(
                org_id="acme",
                name=name,
                expires_at=expires_at,
                idle_ttl_seconds=idle_ttl_seconds,
            )
        )


async def _expiry(db, name) -> datetime | None:
    async with db.acquire() as conn:
        result = await conn.execute_core(
            select(environments.c.expires_at).where(
                environments.c.org_id == "acme", environments.c.name == name
            )
        )
        return result.scalar()


def _aware(moment: datetime | None) -> datetime:
    assert moment is not None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)


HOUR = 3600


class TestRenewIdleExpiry:
    async def test_a_fixed_expiry_is_never_moved_by_use(self, admin_db):
        # REQ-1523's deadline is a promise about time, and being used is no argument against it.
        deadline = datetime.now(tz=timezone.utc) + timedelta(seconds=90)
        await _write(admin_db, "dev_1", expires_at=deadline, idle_ttl_seconds=None)
        await renew_idle_expiry(admin_db, "acme", "dev_1")
        assert _aware(await _expiry(admin_db, "dev_1")) == deadline

    async def test_a_deadline_inside_the_last_half_is_pushed_a_full_ttl_out(self, admin_db):
        await _write(
            admin_db,
            "sandbox_a",
            expires_at=datetime.now(tz=timezone.utc) + timedelta(seconds=600),
            idle_ttl_seconds=HOUR,
        )
        await renew_idle_expiry(admin_db, "acme", "sandbox_a")
        renewed = _aware(await _expiry(admin_db, "sandbox_a"))
        assert (renewed - datetime.now(tz=timezone.utc)).total_seconds() == pytest.approx(
            HOUR, abs=30
        )

    async def test_a_deadline_still_far_out_is_left_alone(self, admin_db):
        # The renewal only has to keep the deadline ahead of the work. Writing a row per request
        # would put an UPDATE on the serving path and buy no further guarantee.
        deadline = datetime.now(tz=timezone.utc) + timedelta(seconds=HOUR - 60)
        await _write(admin_db, "sandbox_b", expires_at=deadline, idle_ttl_seconds=HOUR)
        await renew_idle_expiry(admin_db, "acme", "sandbox_b")
        assert _aware(await _expiry(admin_db, "sandbox_b")) == deadline

    async def test_an_absent_environment_is_not_an_error(self, admin_db):
        # The routing renews after it has served, and a reap between the two is a race nobody loses.
        await renew_idle_expiry(admin_db, "acme", "gone_9")


class TestTheServingPathRenews:
    """The renewal happens where the routing last knows the environment was actually reached."""

    async def test_being_served_pushes_the_deadline_out(self, monkeypatch):
        from provisa.api.env_routing import select_environment

        row = {
            "org_id": "acme",
            "name": "sandbox_a",
            "expires_at": datetime.now(tz=timezone.utc) + timedelta(seconds=60),
            "idle_ttl_seconds": HOUR,
        }

        async def _get_env(_db, _org_id, _name):
            return row

        renewed: list[tuple[str, str]] = []

        async def _renew(_db, org_id, name):
            renewed.append((org_id, name))

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        monkeypatch.setattr("provisa.core.env_store.renew_idle_expiry", _renew)
        assert await select_environment(object(), "acme", "sandbox_a", None, None) == "sandbox_a"
        assert renewed == [("acme", "sandbox_a")]

    async def test_an_expired_environment_is_refused_rather_than_renewed(self, monkeypatch):
        # Otherwise the deadline could never arrive: the request that came in one second late would
        # be the one that moved it.
        from provisa.api.env_routing import EnvironmentSelectionError, select_environment

        row = {
            "org_id": "acme",
            "name": "sandbox_a",
            "expires_at": datetime.now(tz=timezone.utc) - timedelta(seconds=1),
            "idle_ttl_seconds": HOUR,
        }

        async def _get_env(_db, _org_id, _name):
            return row

        renewed: list[tuple[str, str]] = []

        async def _renew(_db, org_id, name):
            renewed.append((org_id, name))

        monkeypatch.setattr("provisa.core.env_store.get_env", _get_env)
        monkeypatch.setattr("provisa.core.env_store.renew_idle_expiry", _renew)
        with pytest.raises(EnvironmentSelectionError):
            await select_environment(object(), "acme", "sandbox_a", None, None)
        assert renewed == []
