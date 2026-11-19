# Copyright (c) 2026 Kenneth Stott
# Canary: 9b3e5d21-7c4a-4f8e-a1d6-3e2b7c9f0a54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command registered from a remote GraphQL source's root query field that requires an
argument is a function returning rows (REQ-1924, REQ-1986), saved through the create endpoint
the Commands form calls. Saving it never answers an internal error: it saves, or it is refused
by name."""

# Requirements: REQ-1924, REQ-1986

from __future__ import annotations

import types
from typing import Any, Optional

import pytest
import strawberry
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import actions_router
from provisa.api.errors import ApiError
from provisa.core import domain_policy
from provisa.core.database import Database
from provisa.core.models import GraphQLRemoteConfig
from provisa.core.schema_org import domains, registered_tables, sources, tracked_functions
from provisa.graphql_remote.introspect import INTROSPECTION_QUERY
from tests.unit.gate_identity import serve_every_column

_SOURCE = "gql"
_RIGHTS = ["table_registration", "column_grant"]
_ROLES = {"steward": {"id": "steward", "capabilities": _RIGHTS, "domain_access": ["*"]}}


@strawberry.type
class Breed:
    name: str
    species: str


@strawberry.type
class Keeper:
    id: int
    name: str


@strawberry.type
class Query:
    @strawberry.field(description="Every breed")
    def animal_breeds(self) -> list[Breed]:
        return []

    @strawberry.field(description="Care requirements for one breed")
    def animal_breed(self, name: str) -> Optional[Breed]:
        return None

    @strawberry.field(description="One keeper")
    def keeper(self, id: int) -> Optional[Keeper]:
        return None


@strawberry.type
class Mutation:
    @strawberry.mutation(description="Admit a breed")
    def admit_breed(self, name: str) -> Breed:
        return Breed(name=name, species="dog")


def _introspected() -> dict:
    result = strawberry.Schema(query=Query, mutation=Mutation).execute_sync(INTROSPECTION_QUERY)
    assert result.errors is None and result.data is not None
    return result.data["__schema"]


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)


@pytest.fixture
async def plane(monkeypatch, control_plane) -> Database:
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(sources).values(id=_SOURCE, type="graphql_remote", cache_ttl=300)
        )
        await conn.execute_core(insert(domains).values(id="shelter"))
    state = appmod.state
    monkeypatch.setattr(state, "tenant_db", db, raising=False)
    monkeypatch.setattr(state, "model_db", db, raising=False)
    monkeypatch.setattr(state, "roles", _ROLES, raising=False)
    monkeypatch.setattr(state, "source_types", {_SOURCE: "graphql_remote"}, raising=False)
    monkeypatch.setattr(
        state,
        "graphql_remote_sources",
        {
            _SOURCE: {
                "source_id": _SOURCE,
                "url": "http://gql.invalid/graphql",
                "namespace": "",
                "schema": _introspected(),
                "tables": [],
                "functions": [],
                "relationships": [],
            }
        },
        raising=False,
    )
    monkeypatch.setattr(
        state,
        "config",
        types.SimpleNamespace(
            graphql_remote=GraphQLRemoteConfig(), mail=types.SimpleNamespace(provider="")
        ),
        raising=False,
    )
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)

    async def _rebuild() -> None:
        return None

    monkeypatch.setattr(appmod, "_rebuild_schemas", _rebuild)
    return db


def _request() -> Any:
    identity = types.SimpleNamespace(user_id="u1", roles=["steward"], acting_roles=["steward"])
    return types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id="a", role="steward")
    )


def _form(name: str, operation: str) -> actions_router.FunctionInput:
    """What the Commands form posts for a "DB Function" whose implementation is the source
    operation ``operation`` of the remote GraphQL source."""
    return actions_router.FunctionInput(
        name=name,
        sourceId=_SOURCE,
        schemaName="graphql",
        functionName=operation,
        domainId="shelter",
        implKind="source_operation",
        kind="mutation",
        arguments=[],
        returns="",
        returnSchema=None,
        # The form sends the output columns it holds; a source operation's form hides them.
        outputColumns=[],
    )


async def _stored(db: Database, name: str) -> dict | None:
    async with db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(tracked_functions).where(tracked_functions.c.name == name)
            )
        ).fetchone()
    return None if row is None else dict(row._mapping)


@pytest.mark.asyncio
async def test_a_read_whose_plain_list_is_registered_saves_returning_its_rows(plane):
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(registered_tables).values(
                source_id=_SOURCE,
                domain_id="shelter",
                schema_name="graphql",
                table_name="animal_breeds",
            )
        )
    reply = await actions_router.create_function(_request(), _form("xxx", "animal_breed"))
    assert reply == {"success": True, "name": "xxx"}
    stored = await _stored(plane, "xxx")
    assert stored is not None
    assert (stored["kind"], stored["impl_kind"], stored["returns"]) == (
        "query",
        "source_operation",
        "graphql.animal_breeds",
    )
    assert stored["exposed_as_table"] is False
    assert [(a["name"], a["type"]) for a in stored["arguments"]] == [("name", "text")]
    # Its rows are the plain list's: it declares no output of its own, so it is not VOID.
    assert stored["output_columns"] is None


@pytest.mark.asyncio
async def test_a_read_whose_plain_list_is_unregistered_is_refused_naming_it(plane):
    with pytest.raises(ApiError) as refused:
        await actions_router.create_function(_request(), _form("xxx", "animal_breed"))
    assert refused.value.code == "functions.plain_list_unregistered"
    assert refused.value.params["lists"] == "animal_breeds"
    assert await _stored(plane, "xxx") is None


@pytest.mark.asyncio
async def test_a_read_with_no_plain_list_saves_returning_the_table_derived_from_it(plane):
    reply = await actions_router.create_function(_request(), _form("one_keeper", "keeper"))
    assert reply == {"success": True, "name": "one_keeper"}
    stored = await _stored(plane, "one_keeper")
    assert stored is not None
    assert stored["exposed_as_table"] is True
    assert stored["returns"] == "graphql.keeper"
    async with plane.acquire() as conn:
        derived = (
            await conn.execute_core(
                select(registered_tables.c.table_name).where(
                    registered_tables.c.source_id == _SOURCE
                )
            )
        ).fetchall()
    assert [r.table_name for r in derived] == ["keeper"]


@pytest.mark.asyncio
async def test_a_write_saves_answering_what_the_remote_answers(plane):
    reply = await actions_router.create_function(_request(), _form("admit", "admitBreed"))
    assert reply == {"success": True, "name": "admit"}
    stored = await _stored(plane, "admit")
    assert stored is not None
    assert (stored["kind"], stored["returns"], stored["output_columns"]) == ("mutation", "", None)
