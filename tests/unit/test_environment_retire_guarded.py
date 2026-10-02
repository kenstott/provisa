# Copyright (c) 2026 Kenneth Stott
# Canary: 911f2364-7118-4e08-9250-3d93289c92ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment is retired only when nothing still refers to it (REQ-1918).

A membership pinned to it, an invitation that can still seat or deploy a redeemer from it, and
an environment branched from it each block, are each named, and nothing is removed. The one
exception is stated in the inventory: an environment minted for a visitor takes the memberships
pinned to it WITH it, because a visitor's account exists only inside that environment. The check
is in ``retire_environment`` itself, so the delete door, a merge that retires its source and the
expiry sweep are the same act.

The scenarios run here on a SQLite platform plane, and unchanged on PostgreSQL in
``tests/integration/test_environment_retire_guarded_pg.py``.
"""

# Requirements: REQ-1918, REQ-1596, REQ-1542, REQ-1523

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import insert, select

from provisa.core import env_reaper, env_retire, schema_admin
from provisa.core.database import Database, create_engine_from_url
from provisa.core.env_retire import (
    DEPENDENT,
    ENVIRONMENT_REFERENCES,
    PART,
    EnvironmentInUse,
    RetirementError,
    env_dependents,
    is_visitor_environment,
    retire_environment,
)
from provisa.core.schema_admin import (
    environments,
    org_invites,
    orgs,
    user_org_memberships,
    user_profiles,
)

ORG = "acme"
OTHER = "globex"
SOON = datetime.now(tz=timezone.utc) + timedelta(days=1)
PAST = datetime.now(tz=timezone.utc) - timedelta(days=1)


@pytest.fixture
async def admin(tmp_path) -> Database:
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'platform.db'}")
    with engine.begin() as conn:
        schema_admin.metadata.create_all(conn)
    plane = Database(engine, name="admin")
    await seed(plane)
    return plane


async def seed(plane: Database) -> None:
    """Two orgs, each with prod and the same set of other environments by name."""
    async with plane.acquire() as conn:
        for org_id in (ORG, OTHER):
            await conn.execute_core(insert(orgs).values(id=org_id, name=org_id))
            for name in ("prod", "staging", "portal", "sandbox_abc123def456", "ephemeral_1a2b3c4d"):
                await conn.execute_core(insert(environments).values(org_id=org_id, name=name))


@pytest.fixture(autouse=True)
def stores(monkeypatch) -> list[tuple]:
    """What the retirement removes outside the platform plane — schemas, files, the store's
    schema, the branch — recorded rather than done."""
    done: list[tuple] = []

    async def _deprovision(pool, org_id, redis_url=None, env=None):
        done.append(("schemas", org_id, env))

    async def _drop_store(org_id, name):
        done.append(("store", org_id, name))
        return None

    monkeypatch.setattr("provisa.core.org_provisioning.deprovision_org", _deprovision)
    monkeypatch.setattr(env_retire, "discard_file_sources", lambda org_id, name: 0)
    monkeypatch.setattr(env_retire, "_drop_store_schema", _drop_store)
    monkeypatch.setattr(env_retire, "delete_branch", lambda org_id, name: True)
    monkeypatch.setattr("provisa.core.redis_location.redis_url", lambda: None)
    return done


async def _pin(admin: Database, user_id: str, env: str, *, org: str = ORG) -> None:
    async with admin.acquire() as conn:
        await conn.execute_core(
            insert(user_org_memberships).values(user_id=user_id, org_id=org, env_name=env)
        )


async def _member(admin: Database, user_id: str, org: str) -> None:
    async with admin.acquire() as conn:
        await conn.execute_core(insert(user_org_memberships).values(user_id=user_id, org_id=org))


async def _invite(
    admin: Database,
    token: str,
    env: str,
    *,
    expires=SOON,
    uses=0,
    max_uses: int | None = 1,
    email=None,
    **kw,
) -> None:
    async with admin.acquire() as conn:
        await conn.execute_core(
            insert(org_invites).values(
                token=token,
                org_id=kw.get("org", ORG),
                created_by="alice",
                expires_at=expires,
                uses=uses,
                max_uses=max_uses,
                email=email,
                env_policy=kw.get("policy", "shared"),
                env_ttl_seconds=kw.get("ttl"),
                env_name=env,
            )
        )


async def _envs(admin: Database, org: str = ORG) -> set[str]:
    async with admin.acquire() as conn:
        rows = await conn.execute_core(
            select(environments.c.name).where(environments.c.org_id == org)
        )
        return {r[0] for r in rows.fetchall()}


async def _retire(admin: Database, name: str, *, org: str = ORG) -> dict:
    return await retire_environment(admin, admin, org, name, drop_branch=False)


def _named(refused: EnvironmentInUse) -> list[tuple]:
    return sorted((d.kind, d.name, tuple(d.as_dict()["via"])) for d in refused.dependents)


# --- the inventory ----------------------------------------------------------------------------


def test_the_inventory_says_what_blocks_and_what_goes_with_a_visitors_environment():
    standing = {(r.table, r.column): (r.visitor, r.otherwise) for r in ENVIRONMENT_REFERENCES}
    assert standing == {
        ("user_org_memberships", "env_name"): (PART, DEPENDENT),
        ("org_invites", "env_name"): (DEPENDENT, DEPENDENT),
        ("environments", "branched_from"): (DEPENDENT, DEPENDENT),
    }


def test_the_inventory_covers_every_column_that_names_an_environment():
    """A column added to the platform plane that names an environment has to be given a standing."""
    naming = {
        (table.name, column.name)
        for table in schema_admin.metadata.tables.values()
        for column in table.columns
        if column.name in ("env_name", "branched_from")
    }
    assert naming == {(r.table, r.column) for r in ENVIRONMENT_REFERENCES}


def test_a_visitors_environment_is_known_by_how_it_was_minted():
    assert is_visitor_environment("sandbox_abc123def456")
    assert is_visitor_environment("ephemeral_1a2b3c4d")
    assert not is_visitor_environment("staging")
    assert not is_visitor_environment("prod")


# --- what blocks ------------------------------------------------------------------------------


async def test_a_pinned_membership_blocks_an_ordinary_environment(admin, stores):
    await _pin(admin, "viv", "portal")
    await _pin(admin, "someone-else", "portal", org=OTHER)  # another org's is not ours

    with pytest.raises(EnvironmentInUse) as err:
        await _retire(admin, "portal")

    assert _named(err.value) == [("membership", "viv", ("user_org_memberships.env_name",))]
    assert "Environment 'portal' is still referred to by: membership 'viv'" in str(err.value)
    assert "portal" in await _envs(admin) and stores == []


async def test_an_invitation_that_can_still_be_redeemed_blocks_and_a_spent_one_does_not(admin):
    await _invite(admin, "tok-open", "portal", email="guest@example.com")
    await _invite(admin, "tok-link", "portal", max_uses=None, policy="per_visitor", ttl=3600)
    await _invite(admin, "tok-expired", "portal", expires=PAST)
    await _invite(admin, "tok-used", "portal", uses=1, max_uses=1)

    with pytest.raises(EnvironmentInUse) as err:
        await _retire(admin, "portal")

    assert _named(err.value) == [
        ("invitation", "guest@example.com", ("org_invites.env_name",)),
        ("invitation", "per_visitor link", ("org_invites.env_name",)),
    ]
    # The token is the invitation's secret: a refusal never carries one.
    assert "tok-" not in str([d.as_dict() for d in err.value.dependents]) + str(err.value)


async def test_an_environment_branched_from_it_blocks(admin):
    async with admin.acquire() as conn:
        await conn.execute_core(
            insert(environments).values(org_id=ORG, name="feature-x", branched_from="staging")
        )
    with pytest.raises(EnvironmentInUse) as err:
        await _retire(admin, "staging")
    assert _named(err.value) == [("environment", "feature-x", ("environments.branched_from",))]


async def test_every_dependent_is_named_in_the_one_refusal(admin):
    await _pin(admin, "viv", "staging")
    await _invite(admin, "tok", "staging", email="guest@example.com")
    async with admin.acquire() as conn:
        await conn.execute_core(
            insert(environments).values(org_id=ORG, name="feature-x", branched_from="staging")
        )
    with pytest.raises(EnvironmentInUse) as err:
        await _retire(admin, "staging")
    assert [d.kind for d in err.value.dependents] == ["membership", "invitation", "environment"]


async def test_prod_is_still_refused_as_before(admin):
    with pytest.raises(RetirementError) as err:
        await _retire(admin, "prod")
    assert not isinstance(err.value, EnvironmentInUse)


# --- what goes ---------------------------------------------------------------------------------


async def test_an_environment_nothing_refers_to_is_retired(admin, stores):
    outcome = await _retire(admin, "staging")
    assert outcome["retired"] == "staging" and outcome["members_removed"] == []
    assert "staging" not in await _envs(admin)
    assert "staging" in await _envs(admin, OTHER)
    assert ("schemas", ORG, "staging") in stores and ("store", ORG, "staging") in stores


@pytest.mark.parametrize("name", ["ephemeral_1a2b3c4d", "sandbox_abc123def456"])
async def test_a_visitors_environment_takes_its_pinned_memberships_with_it(admin, name):
    """The visitor's account existed only inside the environment: the membership goes, and so
    does the profile of a person left with no membership anywhere. A person who also belongs to
    another org keeps their profile."""
    async with admin.acquire() as conn:
        for user_id in ("visitor", "member-elsewhere"):
            await conn.execute_core(
                insert(user_profiles).values(user_id=user_id, email=f"{user_id}@x")
            )
    await _pin(admin, "visitor", name)
    await _pin(admin, "member-elsewhere", name)
    await _member(admin, "member-elsewhere", OTHER)
    assert await env_dependents(admin, ORG, name) == []

    outcome = await _retire(admin, name)

    assert outcome["members_removed"] == ["member-elsewhere", "visitor"]
    async with admin.acquire() as conn:
        left = (
            await conn.execute_core(
                select(user_org_memberships.c.user_id, user_org_memberships.c.org_id)
            )
        ).fetchall()
        profiles = (await conn.execute_core(select(user_profiles.c.user_id))).fetchall()
    assert sorted(tuple(r) for r in left) == [("member-elsewhere", OTHER)]
    assert [r[0] for r in profiles] == ["member-elsewhere"]
    assert name not in await _envs(admin)


async def test_a_visitors_environment_is_still_blocked_by_what_is_branched_from_it(admin):
    async with admin.acquire() as conn:
        await conn.execute_core(
            insert(environments).values(org_id=ORG, name="kept", branched_from="ephemeral_1a2b3c4d")
        )
    await _pin(admin, "visitor", "ephemeral_1a2b3c4d")
    with pytest.raises(EnvironmentInUse) as err:
        await _retire(admin, "ephemeral_1a2b3c4d")
    assert _named(err.value) == [("environment", "kept", ("environments.branched_from",))]
    # Nothing was removed: the visitor is still pinned.
    async with admin.acquire() as conn:
        pinned = (await conn.execute_core(select(user_org_memberships.c.user_id))).fetchall()
    assert [r[0] for r in pinned] == ["visitor"]


# --- the route's refusal -----------------------------------------------------------------------


async def test_the_delete_door_answers_with_each_dependent(admin):
    from provisa.api.admin import environments_router

    await _pin(admin, "viv", "portal")
    with pytest.raises(EnvironmentInUse) as err:
        await _retire(admin, "portal")
    refusal = environments_router._in_use(err.value)
    assert (refusal.status_code, refusal.code) == (409, "environments.in_use")
    assert refusal.params == {
        "org": ORG,
        "env": "portal",
        "count": 1,
        "dependents": [
            {
                "kind": "membership",
                "id": "viv",
                "name": "viv",
                "via": ["user_org_memberships.env_name"],
            }
        ],
    }


# --- the expiry sweep -------------------------------------------------------------------------


async def test_the_sweep_leaves_an_expired_environment_that_is_still_referred_to(admin, caplog):
    """An expiry does not override what still refers to the environment. The sweep reaps what it
    may and raises nothing for the rest; it says what it kept and why in ONE warning per sweep,
    by kind and count, naming nobody."""
    await _pin(admin, "viv", "portal")
    await _invite(admin, "tok-a", "portal", email="guest@example.com")
    await _invite(admin, "tok-b", "portal", max_uses=None, policy="per_visitor", ttl=3600)
    await _pin(admin, "zed", "portal", org=OTHER)
    async with admin.acquire() as conn:
        for org_id, name in ((ORG, "portal"), (ORG, "staging"), (OTHER, "portal")):
            await conn.execute_core(
                environments.update()
                .where(environments.c.org_id == org_id, environments.c.name == name)
                .values(expires_at=PAST)
            )

    with caplog.at_level("INFO", logger=env_reaper.__name__):
        outcomes = await env_reaper.reap_expired(admin, admin)

    assert [o["retired"] for o in outcomes] == ["staging"]
    assert {"portal", "prod"} <= await _envs(admin) and "staging" not in await _envs(admin)
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert [r.getMessage() for r in warnings] == [
        "2 expired environment(s) kept because something still refers to them: "
        "acme/portal (invitation: 2, membership: 1); globex/portal (membership: 1)"
    ]
    assert "viv" not in warnings[0].getMessage() and "guest@" not in warnings[0].getMessage()


async def test_the_environments_listing_says_which_expired_rows_are_kept_and_why(
    admin, monkeypatch
):
    """The same fact an operator needs without reading logs: per row, the kinds and counts of
    what keeps an expired environment standing; empty for every other row."""
    import types

    from provisa.api.admin import environments_router

    await _pin(admin, "viv", "portal")
    await _pin(admin, "ann", "staging")  # pinned, but staging has not expired
    async with admin.acquire() as conn:
        await conn.execute_core(
            environments.update()
            .where(environments.c.org_id == ORG, environments.c.name == "portal")
            .values(expires_at=PAST)
        )
        await conn.execute_core(
            environments.update()
            .where(environments.c.org_id == ORG, environments.c.name == "staging")
            .values(expires_at=SOON)
        )

    async def _allowed(request, org_id, *rights):
        return "alice"

    async def _not_pinned(request, org_id):
        return None

    monkeypatch.setattr(environments_router, "_admin_pool", lambda: admin)
    monkeypatch.setattr(environments_router, "_member", _allowed)
    monkeypatch.setattr(environments_router, "_pinned_env", _not_pinned)
    monkeypatch.setattr(environments_router, "_with_history", lambda org_id, row: dict(row))

    answer = await environments_router.list_environments(types.SimpleNamespace(), ORG)

    kept = {e["name"]: e["expired_kept_by"] for e in answer["environments"]}
    assert kept["portal"] == {"membership": 1}
    assert kept["staging"] == {} and kept["prod"] == {}
