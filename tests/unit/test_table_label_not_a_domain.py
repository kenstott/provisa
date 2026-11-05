# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8e1a7d-2f63-4b95-8d0e-9a3b5c7f1e26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No table label may equal a domain label (REQ-572, NO TABLE LABEL MAY EQUAL A DOMAIN LABEL).

A label never means both a domain and a table. Registering a table, renaming it or giving it an
alias is refused by name when its resolved Cypher label equals an org domain's label; creating a
domain is refused by name when its label equals a registered table's. The check is on the write
path the admin and config load share, so a config carrying such a collision is refused on load,
naming both objects."""

# Requirements: REQ-572

from __future__ import annotations

import contextlib
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app  # noqa: F401  (says whether the deployment holds many organizations)
from provisa.api.admin import schema_mutation
from provisa.api.admin.types import DomainInput
from provisa.compiler.naming import LabelClash
from provisa.core import secrets_store
from provisa.core.config_loader import apply_config, parse_config_dict
from provisa.core.models import Column, Domain, Table
from provisa.core.repositories import domain as domain_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, registered_tables, sources

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


@pytest.fixture
async def org_plane(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        for domain in ("sales", "returns", "sales-ops"):
            await conn.execute_core(insert(domains).values(id=domain))
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
    return db


def _table(name: str, *, domain: str = "sales", alias: str | None = None) -> Table:
    return Table(
        source_id="pg",
        domain_id=domain,
        schema_name="public",
        table_name=name,
        alias=alias,
        columns=[Column(name="id", data_type="integer", visible_to=["admin"])],
    )


async def _table_names(db) -> set[str]:
    async with db.acquire() as conn:
        rows = (await conn.execute_core(select(registered_tables.c.table_name))).fetchall()
    return {r.table_name for r in rows}


# --- the table side ------------------------------------------------------------------------------


async def test_a_table_whose_label_is_a_domains_is_refused_naming_both(org_plane):
    async with org_plane.acquire() as conn:
        with pytest.raises(LabelClash) as refused:
            await table_repo.upsert(conn, _table("returns"))
    assert (refused.value.label, refused.value.domain_id, refused.value.table) == (
        "Returns",
        "returns",
        "pg.public.returns",
    )
    assert "pg.public.returns" in str(refused.value) and "'returns'" in str(refused.value)
    assert "returns" not in await _table_names(org_plane)


async def test_the_label_is_compared_after_the_naming_convention(org_plane):
    # sales_ops and sales-ops are spelled apart, and both are the label SalesOps.
    async with org_plane.acquire() as conn:
        with pytest.raises(LabelClash) as refused:
            await table_repo.upsert(conn, _table("sales_ops", domain="returns"))
    assert (refused.value.label, refused.value.domain_id) == ("SalesOps", "sales-ops")


async def test_an_alias_that_is_a_domains_label_is_refused(org_plane):
    async with org_plane.acquire() as conn:
        with pytest.raises(LabelClash) as refused:
            await table_repo.upsert(conn, _table("refunds", alias="returns"))
    assert refused.value.label == "Returns"


async def test_renaming_a_table_to_a_domains_label_is_refused(org_plane):
    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _table("orders"))
        with pytest.raises(LabelClash):
            await table_repo.upsert(conn, _table("orders", alias="sales"))


async def test_a_table_whose_label_is_no_domains_registers(org_plane):
    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _table("orders"))
        await table_repo.upsert(conn, _table("orders"))  # saved again: still no clash
    assert "orders" in await _table_names(org_plane)


# --- the domain side -----------------------------------------------------------------------------


async def test_a_domain_whose_label_is_a_tables_is_refused_naming_both(org_plane):
    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _table("orders"))
        with pytest.raises(LabelClash) as refused:
            await domain_repo.upsert(conn, Domain(id="orders"))
    assert (refused.value.label, refused.value.domain_id, refused.value.table) == (
        "Orders",
        "orders",
        "pg.public.orders",
    )
    async with org_plane.acquire() as conn:
        assert await domain_repo.get(conn, "orders") is None


async def test_a_domain_matching_a_tables_alias_is_refused(org_plane):
    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _table("ord_v2", alias="order_lines"))
        with pytest.raises(LabelClash):
            await domain_repo.upsert(conn, Domain(id="order-lines"))


async def test_saving_a_domain_again_is_not_a_clash(org_plane):
    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _table("orders"))
        await domain_repo.upsert(conn, Domain(id="sales", description="again"))


async def test_creating_a_domain_through_the_admin_api_is_refused_by_name(org_plane, monkeypatch):
    async def _pool():
        return org_plane

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    from provisa.api.admin import capabilities

    monkeypatch.setattr(capabilities, "require_capability", lambda _info, _cap: None)
    monkeypatch.setattr(capabilities, "require_right_in_domains", lambda _i, _r, _d: None)
    async with org_plane.acquire() as conn:
        await table_repo.upsert(conn, _table("orders"))
    mutation: Any = schema_mutation.Mutation()
    result = await mutation.create_domain(None, DomainInput(id="orders"))
    assert (result.success, result.code, result.params) == (
        False,
        "schema.label_is_a_domain",
        {"label": "Orders", "domain": "orders", "table": "pg.public.orders"},
    )


# --- config load ---------------------------------------------------------------------------------


def _config(*, tables: list[dict]) -> dict:
    return {
        "sources": [{"id": "pg", "type": "postgresql", "host": "h", "port": 5432, "database": "d"}],
        "domains": [{"id": "sales"}, {"id": "returns"}],
        "roles": [
            {
                "id": "seller",
                "description": "Test role.",
                "capabilities": [],
                "domain_access": ["sales"],
            }
        ],
        "tables": [
            {
                "source_id": "pg",
                "domain_id": "sales",
                "schema": "public",
                "columns": [{"name": "id", "data_type": "integer", "visible_to": ["seller"]}],
                **t,
            }
            for t in tables
        ],
    }


async def test_a_config_carrying_a_collision_is_refused_on_load_naming_both(control_plane):
    db = control_plane.org()
    with pytest.raises(LabelClash) as refused:
        async with db.acquire() as conn:
            await apply_config(parse_config_dict(_config(tables=[{"table": "returns"}])), conn)
    told = str(refused.value)
    assert "pg.public.returns" in told and "'returns'" in told and "Returns" in told


async def test_a_config_with_no_collision_loads(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await apply_config(parse_config_dict(_config(tables=[{"table": "orders"}])), conn)
    assert "orders" in await _table_names(db)
