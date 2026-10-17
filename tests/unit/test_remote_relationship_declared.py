# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A relationship a remote source's schema offers, which a declared relationship already is, is
not kept beside it (REQ-313, REQ-194).

A remote GraphQL source's mapper offers a relationship for each object field it can key
(``gql_remote__<source>__<table>__<field>``). The graphql-demo source offers assignments' employee
on ``employee_id -> employees.id``, which the demo's config declares too
(``shelter-assignments-to-employees``): two relationships of one type under one field, which the
schema build refuses (RelationshipFieldTaken), so every request of the deployment was answered
500. They join the same columns: they are one relationship, and the declared one is it."""

# Requirements: REQ-313, REQ-194, REQ-019

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from provisa.core import model_change
from provisa.core.database import Database
from provisa.core.models import Cardinality, Relationship
from provisa.core.repositories import relationship as rel_repo
from provisa.core.schema_org import domains, registered_tables, relationships, sources

_OFFERED = "gql_remote__graphql-demo__assignments__employee"
_DECLARED = "shelter-assignments-to-employees"


@pytest.fixture
async def plane(control_plane) -> Database:
    db = control_plane.org()
    async with model_change.scope("tables"), db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="graphql-demo", type="graphql_remote"))
        await conn.execute_core(insert(domains).values(id="shelter"))
        for name in ("assignments", "employees"):
            await conn.execute_core(
                insert(registered_tables).values(
                    source_id="graphql-demo",
                    domain_id="shelter",
                    schema_name="public",
                    table_name=name,
                )
            )
    return db


def _rel(rel_id: str, source_column: str = "employee_id") -> Relationship:
    return Relationship(
        id=rel_id,
        source_table_id="assignments",
        target_table_id="employees",
        source_column=source_column,
        target_column="id",
        cardinality=Cardinality("many-to-one"),
    )


async def _save(db: Database, *rels: Relationship) -> list[str]:
    for rel in rels:
        async with model_change.scope(rel.id), db.acquire() as conn:
            await rel_repo.upsert(conn, rel)
    async with db.acquire() as conn:
        rows = await conn.execute_core(select(relationships.c.id).order_by(relationships.c.id))
        return [r[0] for r in rows]


async def test_a_relationship_offered_after_the_declared_one_is_not_kept(plane):
    assert await _save(plane, _rel(_DECLARED), _rel(_OFFERED)) == [_DECLARED]


async def test_the_declared_relationship_replaces_one_offered_before_it(plane):
    assert await _save(plane, _rel(_OFFERED), _rel(_DECLARED)) == [_DECLARED]


async def test_an_offered_relationship_on_other_columns_is_kept(plane):
    """Not the same relationship: each is kept, and the schema build names their clash."""
    kept = await _save(plane, _rel(_DECLARED), _rel(_OFFERED, source_column="breed_name"))
    assert kept == [_OFFERED, _DECLARED]


@pytest.mark.asyncio
async def test_a_model_the_schema_build_refuses_is_answered_naming_why():
    """The build of a role's surface runs as a request is authenticated, so a model the build
    refuses is the answer to every request, /auth/me among them: it names the two relationships
    and what to do, never only the catch-all's "Internal server error"."""
    import json
    from types import SimpleNamespace

    from provisa.api.app import create_app
    from provisa.compiler.context import RelationshipFieldTaken

    refused = RelationshipFieldTaken("S__Assignments", "employee", _OFFERED, _DECLARED)
    handler = create_app().exception_handlers[Exception]
    request = SimpleNamespace(method="GET", url=SimpleNamespace(path="/auth/me"))
    response = await handler(request, refused)  # type: ignore[arg-type]
    assert response.status_code == 500
    assert json.loads(response.body) == {
        "detail": str(refused),
        "code": "model.relationship_field_taken",
        "params": {
            "type": "S__Assignments",
            "field": "employee",
            "held_by": _OFFERED,
            "refused": _DECLARED,
        },
    }
