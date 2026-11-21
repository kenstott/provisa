# Copyright (c) 2026 Kenneth Stott
# Canary: b231eff4-8e54-41a8-ab27-8d693772ca0a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model's history names each changed object by its model URI, never a serial id (REQ-1524,
amended 2026-11-14; the identifier is the model name, REQ-1385).

Each change runs in a change scope against a control plane bound as acme/prod, with the commit
captured: what is asserted is the message the commit is asked to carry. The org segment follows
the deployment's tenancy as the metadata export writes it (REQ-697).
"""

# Requirements: REQ-1524, REQ-1385

from __future__ import annotations

import re
from collections.abc import Iterator

import pytest
from sqlalchemy import insert, update

from provisa.core import model_change
from provisa.core.database import Database
from provisa.core.env_deploy import PROJECTED
from provisa.core.model_change import ModelPlane
from provisa.core.models import Cardinality, Relationship, Tag
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import relationship as relationship_repo
from provisa.core.repositories import table as table_repo
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import registered_tables, table_columns
from tests.unit.test_every_kind_records_no_origin import plane  # noqa: F401 — the fixture

PLANE = ModelPlane("acme", "prod")


def _commits(uri_org) -> Iterator[list[str]]:
    asked: list[str] = []

    async def _write_through(conn, admin_db, org_id, env, schema, message, actor):
        asked.append(message)
        return "sha"

    model_change.attach(
        object(),  # type: ignore[arg-type]
        commit=_write_through,
        projected=PROJECTED,
        uri_org=uri_org,
    )
    yield asked
    model_change.detach()


@pytest.fixture
def commits() -> Iterator[list[str]]:
    """The messages of every commit asked for, in a multitenant deployment: URIs name the org."""
    yield from _commits(lambda org_id: org_id)


@pytest.fixture
def single_tenant_commits() -> Iterator[list[str]]:
    """The same in a single-tenant deployment: URIs name no org."""
    yield from _commits(lambda org_id: None)


@pytest.fixture
async def model(plane) -> Database:  # noqa: F811 — the kinds' plane
    plane.model = PLANE
    return plane


def _serials(message: str) -> list[str]:
    """The bare integers in a message: a serial id would be one."""
    return re.findall(r"(?<![\w%])\d+(?![\w%])", message)


async def test_a_term_retire_names_the_term_by_its_uri(model, commits):
    async with model.acquire() as conn:
        async with model_change.committed_by_caller():
            term_id = await glossary_repo.create_abstract_term(conn, "Revenue", domains=set())
    async with model_change.scope("POST /admin/graphql"):
        async with model.acquire() as conn:
            await glossary_repo.retire_term(
                conn, term_id, reason="merged", successor=None, by="pat"
            )
    assert commits == ["retire provisa://acme/terms/revenue"]
    assert str(term_id) not in _serials(commits[0])


async def test_a_table_retire_names_the_table_by_its_uri(model, commits):
    async with model_change.scope("DELETE table"):
        async with model.acquire() as conn:
            assert await table_repo.delete(conn, 1)
    assert commits == ["retire provisa://acme/sales/tables/orders"]
    assert _serials(commits[0]) == []


async def test_a_table_is_named_by_its_alias_not_its_physical_name(model, commits):
    async with model.acquire() as conn:
        async with model_change.committed_by_caller():
            await conn.execute_core(
                update(registered_tables).where(registered_tables.c.id == 2).values(alias="clients")
            )
    async with model_change.scope("DELETE table"):
        async with model.acquire() as conn:
            assert await table_repo.delete(conn, 2)
    assert commits == ["retire provisa://acme/sales/tables/clients"]


async def test_a_relationship_retire_names_it_at_its_source_table(model, commits):
    async with model.acquire() as conn:
        async with model_change.committed_by_caller():
            await relationship_repo.upsert(
                conn,
                Relationship(
                    id="orders-to-customers",
                    source_table_id="orders",
                    target_table_id="customers",
                    source_column="customer_id",
                    target_column="id",
                    cardinality=Cardinality.many_to_one,
                    alias="customer",
                ),
            )
    async with model_change.scope("DELETE relationship"):
        async with model.acquire() as conn:
            assert await relationship_repo.delete(conn, "orders-to-customers")
    assert commits == ["retire provisa://acme/sales/tables/orders#rel:customer"]
    assert _serials(commits[0]) == []


async def test_a_tag_retire_names_the_tag_by_its_uri(model, commits):
    async with model.acquire() as conn:
        async with model_change.committed_by_caller():
            await tag_repo.upsert(conn, Tag(id="finance"))
    async with model_change.scope("DELETE tag"):
        async with model.acquire() as conn:
            assert await tag_repo.delete(conn, "finance")
    assert commits == ["retire provisa://acme/tags/finance"]


async def test_a_single_tenant_history_names_no_org(model, single_tenant_commits):
    async with model_change.scope("DELETE table"):
        async with model.acquire() as conn:
            assert await table_repo.delete(conn, 1)
    assert single_tenant_commits == ["retire provisa://sales/tables/orders"]


# REQ-2248: a delete retires the object, and a restore brings the same object back; each is
# recorded in model history under the object's address.


async def test_a_table_restore_names_the_table_by_its_uri(model, commits):
    async with model.acquire() as conn:
        async with model_change.committed_by_caller():
            assert await table_repo.delete(conn, 1)
    async with model_change.scope("restoreModelObject"):
        async with model.acquire() as conn:
            assert await table_repo.restore(conn, 1)
    assert commits == ["restore provisa://acme/sales/tables/orders"]
    assert _serials(commits[0]) == []


async def test_a_column_restore_names_the_column_at_its_table(model, commits):
    async with model.acquire() as conn:
        async with model_change.committed_by_caller():
            await conn.execute_core(
                insert(table_columns).values(
                    table_id=1, column_name="amount", data_type="numeric", retired=True
                )
            )
    async with model_change.scope("restoreModelObject"):
        async with model.acquire() as conn:
            assert await table_repo.restore_column(conn, 1, "amount")
    assert commits == ["restore provisa://acme/sales/tables/orders#field:amount"]
    assert _serials(commits[0]) == []


async def test_a_relationship_restore_names_it_at_its_source_table(model, commits):
    async with model.acquire() as conn:
        async with model_change.committed_by_caller():
            await relationship_repo.upsert(
                conn,
                Relationship(
                    id="orders-to-customers",
                    source_table_id="orders",
                    target_table_id="customers",
                    source_column="customer_id",
                    target_column="id",
                    cardinality=Cardinality.many_to_one,
                    alias="customer",
                ),
            )
            assert await relationship_repo.delete(conn, "orders-to-customers")
    async with model_change.scope("restoreModelObject"):
        async with model.acquire() as conn:
            assert await relationship_repo.restore(conn, "orders-to-customers")
    assert commits == ["restore provisa://acme/sales/tables/orders#rel:customer"]


async def test_a_tag_restore_names_the_tag_by_its_uri(model, commits):
    async with model.acquire() as conn:
        async with model_change.committed_by_caller():
            await tag_repo.upsert(conn, Tag(id="finance"))
            assert await tag_repo.delete(conn, "finance")
    async with model_change.scope("restoreModelObject"):
        async with model.acquire() as conn:
            assert await tag_repo.restore(conn, "finance")
    assert commits == ["restore provisa://acme/tags/finance"]
