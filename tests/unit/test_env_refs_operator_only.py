# Copyright (c) 2026 Kenneth Stott
# Canary: 0f6f6c0e-6a0b-4f0e-9a63-6e3a1f0f7c31
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A member of a held organization neither writes nor edits a reference to the server's
environment (REQ-1557): refused at the save and where entered text is resolved before a save,
the variable never read. Stored text resolves at use as before, so a demo's own sources work."""

# Requirements: REQ-557, REQ-1557

from __future__ import annotations

import os

import pytest
import strawberry
from sqlalchemy import insert, select, update

from provisa.core import schema_org
from provisa.core.database import Database, create_engine_from_url
from provisa.core.env_reference_guard import (
    EnvReferenceEditRefused,
    EnvReferenceRefused,
    refuse_entered,
)
from provisa.core.request_context import (
    current_acting_role,
    reset_current_org,
    set_current_org,
)
from provisa.core.schema_org import org_settings, scheduled_triggers, sources, tracked_webhooks
from provisa.core.secrets import resolve_secrets

pytestmark = pytest.mark.asyncio

_VARIABLE = "PROVISA_TEST_SERVER_ONLY_VARIABLE"
_VALUE = "the-servers-own-value-7f3a"
_REF = f"${{env:{_VARIABLE}}}"
_SANDBOX = "sbx0123456789ab"
_TENANT = "acme"
_DEPLOYMENT = "default"


@pytest.fixture
def server(monkeypatch):
    """A server whose environment holds one variable; say whether it holds many organizations.
    Every read of the environment through the resolver is recorded in ``server.read``."""
    from provisa.core import secrets, secrets_providers
    from provisa.security import sandbox_egress

    # The API layer says what the deployment is when it is imported; it is imported first, so
    # what this test says after it is what holds.
    import provisa.api.app  # noqa: F401

    monkeypatch.setenv(_VARIABLE, _VALUE)
    read: list[str] = []
    resolve = secrets.EnvSecretsProvider.resolve

    def _recorded(self, reference: str) -> str:
        read.append(reference)
        return resolve(self, reference)

    monkeypatch.setattr(secrets.EnvSecretsProvider, "resolve", _recorded)

    def _say(many: bool) -> None:
        monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: many)
        monkeypatch.setattr(sandbox_egress, "_deployment_org", lambda: _DEPLOYMENT)

    _say.read = read  # type: ignore[attr-defined]
    return _say


class _Acting:
    """Bind an organization, and a role when a person acts in it."""

    def __init__(self, org_id: str, role: str | None) -> None:
        self._org_id, self._role = org_id, role

    def __enter__(self) -> None:
        self._org = set_current_org(self._org_id)
        self._acting = current_acting_role.set(self._role)

    def __exit__(self, *exc: object) -> None:
        current_acting_role.reset(self._acting)
        reset_current_org(self._org)


def member(org_id: str) -> _Acting:
    return _Acting(org_id, "source_admin")


def operator(org_id: str) -> _Acting:
    """The operator's own writes: a boot, a seed, a sandbox's creation. No person acts."""
    return _Acting(org_id, None)


@pytest.fixture
async def store() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="env-refs")
    from provisa.core import container_registries, package_registries

    tables = [sources, tracked_webhooks, scheduled_triggers, org_settings]
    registries = [container_registries._t, package_registries._t]
    with db.engine.begin() as conn:
        schema_org.metadata.create_all(conn, tables=tables)
        for table in registries:
            table.create(conn)
    return db


def _source(**written) -> dict:
    return {"id": "theirs", "type": "postgresql", "host": "db.example.com", **written}


async def _stored_sources(db: Database) -> dict[str, tuple[str, str | None]]:
    async with db.acquire() as conn:
        rows = await conn.execute_core(select(sources.c.id, sources.c.host, sources.c.password_ref))
        return {r[0]: (r[1], r[2]) for r in rows.fetchall()}


# --- (a) a member's new source ---------------------------------------------------------------


@pytest.mark.parametrize(
    "written",
    [
        {"host": _REF},
        {"password_ref": _REF},
        {"federation_hints": {"account": _REF}},
    ],
    ids=["host", "password", "hint"],
)
async def test_a_members_new_source_is_refused_at_save_and_the_variable_is_never_read(
    server, store, written
):
    server(True)
    with member(_TENANT):
        async with store.acquire() as conn:
            with pytest.raises(EnvReferenceRefused) as refused:
                await conn.execute_core(insert(sources).values(**_source(**written)))
    assert refused.value.code == "secrets.env_reference_refused"
    assert refused.value.params == {"name": _VARIABLE}
    assert _VALUE not in str(refused.value)
    assert await _stored_sources(store) == {}
    assert server.read == []


@strawberry.type
class _Query:
    @strawberry.field
    def ping(self) -> str:
        return "pong"


@strawberry.input
class SourceInput:
    host: str
    password: str = ""
    federation_hints_json: str = ""


@strawberry.type
class _Mutation:
    @strawberry.mutation
    def create_source(self, input: SourceInput) -> str:
        # As the admin's createSource does: dial what was entered before saving it.
        return resolve_secrets(input.host) + resolve_secrets(input.password)


def _admin_schema() -> strawberry.Schema:
    from provisa.api.admin.env_reference_check import EnteredEnvReference

    return strawberry.Schema(query=_Query, mutation=_Mutation, extensions=[EnteredEnvReference])


_CREATE = "mutation($input: SourceInput!) { createSource(input: $input) }"


@pytest.mark.parametrize(
    "entered",
    [
        {"host": _REF},
        {"host": "db.example.com", "password": _REF},
        {"host": "db.example.com", "federationHintsJson": '{"account": "\\u0024{env:%s}"}'},
    ],
    ids=["host", "password", "hint-json-escaped"],
)
async def test_what_a_member_enters_is_refused_before_the_operation_runs(server, entered):
    server(True)
    if "federationHintsJson" in entered:
        entered = {**entered, "federationHintsJson": entered["federationHintsJson"] % _VARIABLE}
    with member(_TENANT):
        result = await _admin_schema().execute(_CREATE, variable_values={"input": entered})
    assert result.data is None
    (error,) = result.errors or []
    assert error.extensions == {
        "code": "secrets.env_reference_refused",
        "params": {"name": _VARIABLE},
    }
    assert _VALUE not in str(error)
    assert server.read == []


async def test_a_reference_written_inline_under_a_graphql_escape_is_refused(server):
    server(True)
    inline = 'mutation { createSource(input: {host: "\\u0024{env:%s}"}) }' % _VARIABLE
    with member(_TENANT):
        result = await _admin_schema().execute(inline)
    assert result.data is None and result.errors
    assert server.read == []


# --- (b) the same over every entry point -----------------------------------------------------


def _registry(table, **written):
    return table, {"org_id": _TENANT, "id": "r1", "address": "registry.example.com", **written}


def _entry_points():
    from provisa.core import container_registries, package_registries

    return {
        "webhook-url": (tracked_webhooks, {"name": "hook", "url": f"https://x.example/{_REF}"}),
        "trigger-url": (
            scheduled_triggers,
            {"id": "t1", "name": "t", "kind": "webhook", "url": f"https://x.example/?k={_REF}"},
        ),
        "sparql-endpoint": (sources, _source(type="sparql", host=f"https://{_REF}/sparql")),
        "code-source-secret": (
            sources,
            _source(type="python", mapping={"secrets": {"TOKEN": _REF}}),
        ),
        "elasticsearch": (sources, _source(type="elasticsearch", password_ref=_REF)),
        "container-registry": _registry(container_registries._t, credential=_REF),
        "package-registry": _registry(package_registries._t, credential=_REF),
        "ai-endpoint-key-variable": (
            org_settings,
            {
                "key": "ai_endpoints",
                "value": [
                    {
                        "id": "mine",
                        "style": "openai",
                        "base_url": "https://collector.example",
                        "api_key_env": _VARIABLE,
                    }
                ],
            },
        ),
        "vector-model-key-variable": (
            org_settings,
            {"key": "vector_models", "value": [{"id": "v", "api_key_env": _VARIABLE}]},
        ),
    }


@pytest.mark.parametrize("entry", sorted(_entry_points()))
@pytest.mark.parametrize("held_org", [_TENANT, _SANDBOX], ids=["tenant", "sandbox"])
async def test_every_entry_point_refuses_a_members_reference(server, store, entry, held_org):
    server(True)
    table, values = _entry_points()[entry]
    with member(held_org):
        async with store.acquire() as conn:
            with pytest.raises(EnvReferenceRefused) as refused:
                await conn.execute_core(insert(table).values(**values))
            with pytest.raises(EnvReferenceRefused):
                await conn.upsert(table, values, index_elements=list(values)[:1])
            with pytest.raises(EnvReferenceRefused):
                await conn.execute_core_many(insert(table), [values])
            with pytest.raises(EnvReferenceRefused):
                await conn.bulk_copy(table, [values])
    assert refused.value.params == {"name": _VARIABLE}
    assert server.read == []


async def test_a_key_variable_given_as_a_secret_reference_is_saved(server, store):
    server(True)
    endpoint = {
        "id": "mine",
        "style": "openai",
        "base_url": "https://a",
        "api_key_env": "${secret:K}",
    }
    with member(_TENANT):
        async with store.acquire() as conn:
            await conn.upsert(
                org_settings, {"key": "ai_endpoints", "value": [endpoint]}, index_elements=["key"]
            )


async def test_a_raw_write_is_a_write_like_any_other(server, store):
    server(True)
    with member(_TENANT):
        async with store.acquire() as conn:
            with pytest.raises(EnvReferenceRefused):
                await conn.execute(
                    "INSERT INTO sources (id, type, host) VALUES ($1, $2, $3)", "s", "sparql", _REF
                )
            with pytest.raises(EnvReferenceRefused):
                await conn.executemany(
                    "INSERT INTO sources (id, type, host) VALUES ($1, $2, $3)",
                    [("s", "sparql", _REF)],
                )
    assert await _stored_sources(store) == {}


async def test_text_no_one_resolves_may_hold_the_literal(server, store):
    """A query's own text in the audit log is evidence, not a reference anyone resolves."""
    from provisa.core.env_reference_guard import check_values
    from provisa.core.schema_org import query_audit_log

    server(True)
    with member(_TENANT):
        check_values(query_audit_log, {"query_text": f"SELECT '{_REF}'"})


# --- (c) a member's edit of the operator's object --------------------------------------------


async def _seed_demo_source(db: Database, org_id: str) -> None:
    with operator(org_id):
        async with db.acquire() as conn:
            await conn.execute_core(
                insert(sources).values(**_source(id="demo", host="demo-db", password_ref=_REF))
            )


@pytest.mark.parametrize(
    "edit",
    [
        {"host": "collector.example.com"},
        {"description": "mine now"},
        {"host": "collector.example.com", "password_ref": ""},
    ],
    ids=["host-only", "description-only", "host-and-cleared-password"],
)
async def test_a_members_edit_of_a_demo_source_is_refused(server, store, edit):
    server(True)
    await _seed_demo_source(store, _SANDBOX)
    with member(_SANDBOX):
        async with store.acquire() as conn:
            with pytest.raises(EnvReferenceEditRefused) as refused:
                await conn.execute_core(
                    update(sources).where(sources.c.id == "demo").values(**edit)
                )
            with pytest.raises((EnvReferenceEditRefused, EnvReferenceRefused)):
                await conn.upsert(
                    sources,
                    _source(id="demo", password_ref=_REF, **{"host": "collector.example.com"}),
                    index_elements=["id"],
                )
    assert refused.value.code == "secrets.env_reference_object_locked"
    assert refused.value.params == {"object": "sources"}
    assert await _stored_sources(store) == {"demo": ("demo-db", _REF)}
    assert server.read == []


async def test_a_member_edits_their_own_source_beside_the_demos(server, store):
    server(True)
    await _seed_demo_source(store, _SANDBOX)
    with member(_SANDBOX):
        async with store.acquire() as conn:
            await conn.execute_core(insert(sources).values(**_source(password_ref="${secret:PW}")))
            await conn.execute_core(
                update(sources).where(sources.c.id == "theirs").values(host="other.example.com")
            )
    assert (await _stored_sources(store))["theirs"] == ("other.example.com", "${secret:PW}")


async def test_deleting_the_operators_object_is_not_restricted(server, store):
    from sqlalchemy import delete

    server(True)
    await _seed_demo_source(store, _SANDBOX)
    with member(_SANDBOX):
        async with store.acquire() as conn:
            await conn.execute_core(delete(sources).where(sources.c.id == "demo"))
    assert await _stored_sources(store) == {}


# --- (d) the demo's own source still resolves at use ------------------------------------------


@pytest.mark.parametrize("many", [True, False], ids=["many-organizations", "one-organization"])
async def test_a_demos_own_source_in_a_sandbox_is_seeded_and_resolves_at_use(server, store, many):
    server(many)
    await _seed_demo_source(store, _SANDBOX)
    with member(_SANDBOX):
        host, password_ref = (await _stored_sources(store))["demo"]
        assert resolve_secrets(password_ref or "") == _VALUE
        assert resolve_secrets(f"postgres://u:{password_ref}@{host}/d") == (
            f"postgres://u:{_VALUE}@demo-db/d"
        )


async def test_a_copy_of_stored_rows_with_no_person_acting_is_not_held(server, store):
    """An environment's creation and a sandbox's build write the model as a whole."""
    from provisa.core import model_change

    server(True)
    with member(_SANDBOX):
        async with model_change.committed_by_caller(), store.acquire() as conn:
            await conn.execute_core(insert(sources).values(**_source(password_ref=_REF)))
    assert (await _stored_sources(store))["theirs"][1] == _REF


# --- (e) the deployment's own organization, and an install of one organization ---------------


@pytest.mark.parametrize(
    ("many", "org_id"),
    [(True, _DEPLOYMENT), (False, _DEPLOYMENT), (False, _TENANT)],
    ids=["deployment-org-among-many", "one-organization", "one-organization-other-id"],
)
async def test_an_organization_that_is_not_held_saves_edits_and_resolves_as_before(
    server, store, many, org_id
):
    server(many)
    with member(org_id):
        async with store.acquire() as conn:
            await conn.execute_core(insert(sources).values(**_source(password_ref=_REF)))
            await conn.execute_core(
                update(sources).where(sources.c.id == "theirs").values(host=f"{_REF}.example.com")
            )
        refuse_entered({"host": _REF})
        assert resolve_secrets(_REF) == _VALUE
    assert (await _stored_sources(store))["theirs"] == (f"{_REF}.example.com", _REF)


# --- (f) a sandbox on an install of one organization is held ----------------------------------


async def test_a_sandbox_on_a_single_organization_install_is_held(server, store):
    server(False)
    with member(_SANDBOX):
        async with store.acquire() as conn:
            with pytest.raises(EnvReferenceRefused):
                await conn.execute_core(insert(sources).values(**_source(host=_REF)))
        with pytest.raises(EnvReferenceRefused):
            refuse_entered({"password": _REF})
    assert server.read == [] and _VARIABLE in os.environ
