# Copyright (c) 2026 Kenneth Stott
# Canary: 7a3e1c5b-8d92-4f04-b6a1-c2e9d7f05b38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A developer's route to a dev-mode code source's services (REQ-2106, amended 2026-11-22;
REQ-2112): ``/admin/orgs/{org}/code-sources/{source}/services/{token}/{service}/{call}``.

A valid personal access token reaches a services call answered under its source's grant, as its
role; the call is in the org's trail. Another source, an unknown token, a revoked one and an
expired one are each refused by name. The auth middleware lets the path through without a
credential, since the token is the path's."""

# Requirements: REQ-2106, REQ-2112

from __future__ import annotations

import datetime
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import insert, update

from provisa.api.admin import developer_services_router
from provisa.auth.pat import PersonalAccessTokenStore, hash_token
from provisa.core import schema_admin
from provisa.core.database import Database, create_engine_from_url
from provisa.core.schema_admin import orgs, personal_access_tokens
from provisa.security.rights import Capability

pytestmark = pytest.mark.asyncio

ORG = "acme"
SOURCE = "rates"
_DEV = {
    "runtime": "python",
    "code": {"kind": "upload", "upload": "u1"},
    "secrets": {"api_key": None},
    "settings": {"region": "EU"},
    "mode": "dev",
}


def _source(source_id: str, source_type: str, mapping: dict) -> SimpleNamespace:
    return SimpleNamespace(id=source_id, type=SimpleNamespace(value=source_type), mapping=mapping)


@pytest.fixture
async def world(monkeypatch):
    """A control plane holding the org, its sources and its developer's tokens; the trail kept."""
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="dev-services")
    with db.engine.begin() as conn:
        schema_admin.metadata.create_all(conn)
    async with db.acquire() as conn:
        for org_id in (ORG, "globex"):
            await conn.execute_core(insert(orgs).values(id=org_id, name=org_id))
    state = SimpleNamespace(
        admin_db=db,
        roles={
            "developer": {"capabilities": [Capability.SOURCE_REGISTRATION.value]},
            "analyst": {"capabilities": []},
        },
        udf_egress_allowlist=["api.example.com"],
    )
    sources = [
        _source(SOURCE, "python", _DEV),
        _source("ledger", "python", {**_DEV, "mode": "built"}),
        _source("warehouse", "postgresql", {}),
    ]
    trail: list[dict] = []

    async def ensure_org_runtime(org_id):
        return None

    async def registered_sources(_state):
        return sources

    async def sign_in_roles(_state, identity, org_id):
        scopes = identity.raw_claims["scopes"]
        return scopes[0], scopes

    async def org_model_db(org_id):
        return f"model:{org_id}"

    async def record_admin_action(model_db, **entry):
        trail.append({"model_db": model_db, **entry})

    monkeypatch.setattr("provisa.api.app.state", state, raising=False)
    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", ensure_org_runtime, raising=False)
    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", registered_sources)
    monkeypatch.setattr("provisa.auth.held_assignments.sign_in_roles", sign_in_roles)
    monkeypatch.setattr("provisa.api.admin.orgs_router._org_model_db", org_model_db)
    monkeypatch.setattr("provisa.core.org_membership.record_admin_action", record_admin_action)
    app = FastAPI()
    app.include_router(developer_services_router.router)
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    yield SimpleNamespace(db=db, pats=PersonalAccessTokenStore(db), client=client, trail=trail)
    await client.aclose()


async def _token(world, *, org_id: str = ORG, scopes: tuple[str, ...] = ("developer",)) -> str:
    secret, _row = await world.pats.issue(
        user_id="ada", org_id=org_id, env_name="prod", name="vscode", scopes=list(scopes)
    )
    return secret


def _address(token: str, source: str = SOURCE, org_id: str = ORG) -> str:
    return f"/admin/orgs/{org_id}/code-sources/{source}/services/{token}"


async def _set(world, token: str, **values) -> None:
    async with world.db.acquire() as conn:
        await conn.execute_core(
            update(personal_access_tokens)
            .where(personal_access_tokens.c.token_hash == hash_token(token))
            .values(**values)
        )


async def test_a_valid_token_reaches_its_sources_services_and_the_call_is_audited(world):
    token = await _token(world)
    answered = await world.client.get(f"{_address(token)}/settings/region")
    assert answered.status_code == 200, answered.text
    assert answered.json() == {"value": "EU"}  # the source's own declared setting
    (entry,) = world.trail
    assert entry["model_db"] == f"model:{ORG}"
    assert entry["org_id"] == ORG
    assert entry["action"] == "code_source.services.developer_call"
    assert entry["actor_id"] == "ada"
    assert entry["subject_id"] == "settings/region"
    assert entry["detail"] == {
        "source": SOURCE,
        "service": "settings",
        "token": "vscode",
        "role": "developer",
        "status": 200,
    }
    assert token not in repr(world.trail)


async def test_the_call_is_answered_under_the_sources_grant_as_the_tokens_role(world, monkeypatch):
    token = await _token(world)
    seen: list = []

    def answer_grant(grant, method, service, sub, body, role):
        seen.append((grant, method, service, sub, body, role))
        return 200, "application/json", b"{}"

    monkeypatch.setattr("provisa.codesource.services.answer_grant", answer_grant)
    answered = await world.client.put(f"{_address(token)}/cache/rates", content=b'{"value": 1}')
    assert answered.status_code == 200
    ((grant, method, service, sub, body, role),) = seen
    assert (method, service, sub, body, role) == (
        "PUT",
        "cache",
        "rates",
        b'{"value": 1}',
        "developer",
    )
    assert (grant.org_id, grant.source_id, grant.role) == (ORG, SOURCE, "developer")
    assert dict(grant.secrets) == {"api_key": None}
    assert dict(grant.settings) == {"region": "EU"}
    assert grant.egress == ("api.example.com",)


async def test_another_source_is_refused_by_name_and_audited(world):
    token = await _token(world)
    for source, status, named in (
        ("missing", 404, "'acme' has no source 'missing'"),
        ("ledger", 403, "the source 'ledger' is not in dev mode"),
        ("warehouse", 403, "the source 'warehouse' is not a code source"),
    ):
        answered = await world.client.get(f"{_address(token, source)}/settings/region")
        assert answered.status_code == status, answered.text
        assert named in answered.json()["error"]["message"]
    assert [(e["detail"]["source"], e["detail"]["status"]) for e in world.trail] == [
        ("missing", 404),
        ("ledger", 403),
        ("warehouse", 403),
    ]


async def test_another_organizations_source_is_refused_by_name(world):
    token = await _token(world, org_id="globex")
    answered = await world.client.get(f"{_address(token)}/settings/region")
    assert answered.status_code == 403
    message = answered.json()["error"]["message"]
    assert "'vscode' is another organization's" in message and "'acme'" in message
    assert world.trail == []


async def test_an_unknown_token_is_refused_by_name(world):
    answered = await world.client.get(f"{_address('provisa_pat_' + 'x' * 43)}/settings/region")
    assert answered.status_code == 401
    assert answered.json()["error"]["message"] == "the personal access token is not known here"
    assert world.trail == []


async def test_a_path_carrying_no_token_is_refused_by_name(world):
    answered = await world.client.get(f"{_address('tok-123')}/secrets/api_key")
    assert answered.status_code == 401
    assert answered.text == "the services address carries no personal access token"


async def test_a_revoked_token_is_refused_by_name(world):
    token = await _token(world)
    await _set(world, token, revoked_at=datetime.datetime.now(datetime.timezone.utc))
    answered = await world.client.get(f"{_address(token)}/settings/region")
    assert answered.status_code == 401
    assert answered.json()["error"]["message"] == "the personal access token 'vscode' is revoked"
    assert world.trail == []


async def test_an_expired_token_is_refused_by_name(world):
    token = await _token(world)
    past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=1)
    await _set(world, token, expires_at=past)
    answered = await world.client.get(f"{_address(token)}/settings/region")
    assert answered.status_code == 401
    assert answered.json()["error"]["message"] == "the personal access token 'vscode' has expired"


async def test_a_token_without_source_registration_is_refused_by_name(world):
    token = await _token(world, scopes=("analyst",))
    answered = await world.client.get(f"{_address(token)}/settings/region")
    assert answered.status_code == 403
    assert (
        "'vscode' does not carry source registration in 'acme'"
        in (answered.json()["error"]["message"])
    )


async def test_every_sign_in_still_says_only_that_the_token_is_not_valid(world):
    token = await _token(world)
    await _set(world, token, revoked_at=datetime.datetime.now(datetime.timezone.utc))
    with pytest.raises(ValueError, match="^Invalid token$"):
        await world.pats.validate(token)


async def test_the_auth_middleware_lets_the_path_through_without_a_credential():
    from provisa.auth.middleware import _DEVELOPER_SERVICES_PATH

    assert _DEVELOPER_SERVICES_PATH.match(
        "/admin/orgs/acme/code-sources/rates/services/provisa_pat_abc/secrets/api_key"
    )
    assert not _DEVELOPER_SERVICES_PATH.match("/admin/orgs/acme/code-sources/rates/builds")
    assert not _DEVELOPER_SERVICES_PATH.match("/admin/orgs/acme/members")
