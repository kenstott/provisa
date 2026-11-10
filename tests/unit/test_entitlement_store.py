# Copyright (c) 2026 Kenneth Stott
# Canary: 8d21c7a4-3b5e-4f60-a1d9-6c2e0b7f9354
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219: the Provisa-held rows, a role's default entitlements, a product's creator."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select

from provisa.core.models import DataProduct, Role
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import role as role_repo
from provisa.core.schema_org import data_products, domains, entitlements
from provisa.entitlements import store

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def conn(control_plane):
    db = control_plane.org()
    async with db.acquire() as c:
        yield c


async def _ids(conn, principal):
    held = await store.load_held(conn)
    return {(r.role_id, r.kind): r.identifiers for r in held[principal].lists}


async def test_a_set_list_is_read_back(conn):
    await store.set_list(conn, "alice", "viewer", "tables", ["sales.*"], set_by="ops", reason="r")
    assert await _ids(conn, "alice") == {("viewer", "tables"): ("sales.*",)}


async def test_setting_a_list_appends_and_the_last_row_wins(conn):
    await store.set_list(conn, "alice", "viewer", "tables", ["a.b"], set_by="ops", reason="r")
    await store.set_list(conn, "alice", "viewer", "tables", ["c.d"], set_by="ops", reason="r2")
    assert await _ids(conn, "alice") == {("viewer", "tables"): ("c.d",)}
    rows = await conn.execute_core(select(entitlements).where(entitlements.c.principal == "alice"))
    assert len(rows.fetchall()) == 2


async def test_removing_a_list_ends_it_and_keeps_the_history(conn):
    await store.set_list(conn, "alice", "viewer", "tables", ["a.b"], set_by="ops", reason="r")
    await store.remove_list(conn, "alice", "viewer", "tables", set_by="ops", reason="gone")
    assert "alice" not in await store.load_held(conn)
    rows = await conn.execute_core(select(entitlements).where(entitlements.c.principal == "alice"))
    assert len(rows.fetchall()) == 2


async def test_an_expiry_rule_is_read_back(conn):
    at = datetime(2027, 1, 1, tzinfo=UTC)
    await store.add_expiry(
        conn, "alice", "viewer", at, {"read": ["finance"]}, set_by="ops", reason="temp"
    )
    held = (await store.load_held(conn))["alice"]
    assert held.expiries[0].ends_at == at
    assert held.expiries[0].ends == {"read": ("finance",)}


async def test_a_product_creator_holds_rw_on_it(conn):
    await store.record_product_creator(conn, "alice", "steward", "sales-eu", "p1")
    row = (await store.load_held(conn))["alice"].lists[0]
    assert row.kind == "data_products"
    assert row.capability == "data_product_rw"
    assert row.identifiers == ("sales_eu.p1",)
    assert row.role_id == "steward"


def _role(**over):
    return Role(
        id="custom",
        description="A role for the test",
        capabilities=[],
        domain_access=["sales"],
        **over,
    )


async def test_setting_domains_writes_default_entitlements(conn):
    await role_repo.upsert(conn, _role(), org_id="default")
    stored = await role_repo.get(conn, "custom")
    assert stored["default_entitlements"] == {"domains": ["sales"]}


async def test_a_narrowed_default_survives_a_save_that_keeps_the_domains(conn):
    await role_repo.upsert(conn, _role(), org_id="default")
    await role_repo.upsert(conn, _role(default_entitlements={"domains": []}), org_id="default")
    await role_repo.upsert(conn, _role(), org_id="default")
    assert (await role_repo.get(conn, "custom"))["default_entitlements"] == {"domains": []}


async def test_changing_the_domains_rewrites_the_defaults(conn):
    await role_repo.upsert(conn, _role(default_entitlements={"domains": []}), org_id="default")
    changed = Role(
        id="custom", description="A role for the test", capabilities=[], domain_access=["hr"]
    )
    await role_repo.upsert(conn, changed, org_id="default")
    assert (await role_repo.get(conn, "custom"))["default_entitlements"] == {"domains": ["hr"]}


async def test_a_plain_default_is_the_role_itself():
    from provisa.entitlements.runtime import role_defaults

    role = {"domain_access": ["sales"], "default_entitlements": {"domains": ["sales"]}}
    assert role_defaults(role) == {}
    deny = {"domain_access": ["sales"], "default_entitlements": {"domains": []}}
    assert role_defaults(deny) == {"domains": []}


async def test_created_by_is_set_on_insert_only(conn):
    await conn.execute_core(insert(domains).values(id="sales", description="d"))
    product = DataProduct(id="p1", domain_id="sales", name="P", created_by="alice")
    await data_product_repo.upsert(conn, product)
    await data_product_repo.upsert(
        conn, DataProduct(id="p1", domain_id="sales", name="P2", created_by="mallory")
    )
    rows = await conn.execute_core(select(data_products).where(data_products.c.id == "p1"))
    row = rows.fetchone()
    assert (row.name, row.created_by) == ("P2", "alice")


async def test_product_refs_list_every_product(conn):
    await conn.execute_core(insert(domains).values(id="sales", description="d"))
    await data_product_repo.upsert(conn, DataProduct(id="p1", domain_id="sales", name="P"))
    assert await data_product_repo.refs(conn) == (("p1", "sales"),)


async def test_a_created_product_records_its_creator(monkeypatch, control_plane):
    from provisa.api.admin import capabilities, schema_mutation
    from provisa.api.admin.schema_mutation import DataProductInput, _save_data_product

    db = control_plane.org()
    async with db.acquire() as c:
        await c.execute_core(insert(domains).values(id="sales", description="d"))

    async def _pool():
        return db

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(capabilities, "require_right_in_domains", lambda _i, _r, _d: None)
    info = SimpleNamespace(
        context={
            "request": SimpleNamespace(
                state=SimpleNamespace(identity=SimpleNamespace(user_id="alice"), role="steward")
            )
        }
    )
    product = DataProductInput(id="p1", domain_id="sales", name="P")
    result = await _save_data_product(info, product, creating=True)
    assert result.success, result.message
    async with db.acquire() as c:
        row = (await c.execute_core(select(data_products))).fetchone()
        assert row.created_by == "alice"
        held = (await store.load_held(c))["alice"].lists[0]
    assert (held.role_id, held.identifiers, held.capability) == (
        "steward",
        ("sales.p1",),
        "data_product_rw",
    )
