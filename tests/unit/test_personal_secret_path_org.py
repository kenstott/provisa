# Copyright (c) 2026 Kenneth Stott
# Canary: bf2f6f5a-bcb2-43ef-b828-21cade2a0435
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A personal secret is addressed under an organization the caller belongs to (REQ-1560).

The personal endpoints take their owner from the authenticated identity; the organization comes
from the path. Under an organization the caller is not a member of, every personal endpoint
answers exactly as under one that does not exist, and nothing is read or written.
"""

# Requirements: REQ-1560

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select

from provisa.api.admin import secrets_router as sr
from provisa.api.errors import ApiError
from provisa.core.org_membership import JOINED_VIA_INVITE, grant_membership
from provisa.core.schema_admin import (
    deployment_encryption_key,
    metadata,
    orgs,
    secrets_store,
    user_org_memberships,
)
from provisa.encryption.runtime import reset_encryption

pytestmark = pytest.mark.asyncio

MINE, OTHER, MISSING, PERSON = "mine", "other", "nosuchorg", "uid-dev"


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    from provisa.core.secrets_runtime import configure_secrets, reset_secrets

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("PROVISA_ENCRYPTION_KEY", raising=False)
    monkeypatch.setitem(sys.modules, "keyring", None)
    reset_encryption()
    configure_secrets("provisa")
    yield
    reset_secrets()
    reset_encryption()


@pytest.fixture
async def plane(tmp_path, monkeypatch):
    """A platform control plane holding two organizations and one person, who belongs to the
    first only. The deployment is multi-tenant."""
    from provisa.core.database import Database, create_engine_from_url

    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'platform.db'}")
    with engine.begin() as conn:
        metadata.create_all(
            conn, tables=[orgs, user_org_memberships, secrets_store, deployment_encryption_key]
        )
    db = Database(engine, name="admin")
    async with db.acquire() as conn:
        for org_id in (MINE, OTHER):
            await conn.execute_core(insert(orgs).values(id=org_id, name=org_id))
    await grant_membership(db, PERSON, MINE, joined_via=JOINED_VIA_INVITE)

    async def _no_audit(*_args, **_kwargs) -> None:
        return None

    monkeypatch.setattr(sr, "_admin_pool", lambda: db)
    monkeypatch.setattr(sr, "_audit", _no_audit)  # the audit trail is the org's tenant plane
    monkeypatch.setattr(sr, "_deployment", lambda: (True, MINE))
    yield db
    engine.dispose()


def _request(person: str | None = PERSON) -> SimpleNamespace:
    return SimpleNamespace(state=SimpleNamespace(identity=SimpleNamespace(user_id=person)))


async def _stored(db) -> list[tuple[str, str, str]]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(
            select(secrets_store.c.org_id, secrets_store.c.owner_id, secrets_store.c.name)
        )
        return sorted(tuple(r) for r in rows.fetchall())


def _calls(org_id: str):
    request = _request()
    return {
        "list": lambda: sr.list_my_secrets(request, org_id),
        "put": lambda: sr.put_my_secret(request, org_id, "GIT_TOKEN", sr.SecretBody(value="v")),
        "delete": lambda: sr.delete_my_secret(request, org_id, "GIT_TOKEN"),
    }


async def _refusals(org_id: str) -> dict[str, tuple]:
    """What each personal endpoint answers under ``org_id``, with the organization's own id
    taken out so two organizations' answers can be compared."""
    answers = {}
    for name, call in _calls(org_id).items():
        with pytest.raises(ApiError) as refused:
            await call()
        error = refused.value
        answers[name] = (
            error.status_code,
            error.code,
            str(error.detail).replace(org_id, "<org>"),
            {k: str(v).replace(org_id, "<org>") for k, v in error.params.items()},
            error.headers,
        )
    return answers


async def test_a_member_keeps_a_personal_secret_under_their_own_organization(plane):
    request = _request()
    await sr.put_my_secret(request, MINE, "GIT_TOKEN", sr.SecretBody(value="v"))
    listed = await sr.list_my_secrets(request, MINE)
    assert [s["name"] for s in listed["secrets"]] == ["GIT_TOKEN"]
    assert await _stored(plane) == [(MINE, PERSON, "GIT_TOKEN")]
    assert await sr.delete_my_secret(request, MINE, "GIT_TOKEN") == {"deleted": "GIT_TOKEN"}


async def test_under_an_organization_the_caller_is_not_in_every_personal_endpoint_is_not_found(
    plane,
):
    answers = await _refusals(OTHER)
    assert {a[:2] for a in answers.values()} == {(404, "secrets.org_not_found")}
    assert await _stored(plane) == []  # nothing was written under the other organization


async def test_an_organization_that_does_not_exist_answers_exactly_the_same(plane):
    """Status, code, message, parameters and headers: an organization the caller is not in cannot
    be told from one that is not there."""
    assert await _refusals(OTHER) == await _refusals(MISSING)


async def test_a_secret_held_before_membership_ended_is_not_reachable_after(plane):
    from sqlalchemy import delete

    request = _request()
    await sr.put_my_secret(request, MINE, "GIT_TOKEN", sr.SecretBody(value="v"))
    async with plane.acquire() as conn:
        await conn.execute_core(
            delete(user_org_memberships).where(user_org_memberships.c.user_id == PERSON)
        )
    answers = await _refusals(MINE)
    assert {a[:2] for a in answers.values()} == {(404, "secrets.org_not_found")}
    assert await _stored(plane) == [(MINE, PERSON, "GIT_TOKEN")]  # not deleted by the refusal


async def test_an_organization_still_awaiting_its_subscription_answers_the_same(plane):
    """A reserved organization cannot be worked in, whoever holds a membership of it."""
    from provisa.core.schema_admin import AWAITING_CHECKOUT

    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(orgs).values(
                id="reserved", name="reserved", provisioning_state=AWAITING_CHECKOUT
            )
        )
    await grant_membership(plane, PERSON, "reserved", joined_via=JOINED_VIA_INVITE)
    assert await _refusals("reserved") == await _refusals(MISSING)
    assert await _stored(plane) == []


async def test_a_deployment_of_one_organization_serves_that_organization_only(plane, monkeypatch):
    """Without multi-tenancy the deployment has one organization and records no memberships:
    its people keep personal secrets under it, and no other organization id answers."""
    monkeypatch.setattr(sr, "_deployment", lambda: (False, "default"))
    request = _request("uid-with-no-membership")
    await sr.put_my_secret(request, "default", "GIT_TOKEN", sr.SecretBody(value="v"))
    assert await _stored(plane) == [("default", "uid-with-no-membership", "GIT_TOKEN")]
    with pytest.raises(ApiError) as refused:
        await sr.list_my_secrets(request, MINE)
    assert (refused.value.status_code, refused.value.code) == (404, "secrets.org_not_found")


async def test_without_an_identity_the_answer_is_still_identity_required(plane):
    with pytest.raises(ApiError) as refused:
        await sr.list_my_secrets(_request(None), MINE)
    assert (refused.value.status_code, refused.value.code) == (403, "secrets.identity_required")
