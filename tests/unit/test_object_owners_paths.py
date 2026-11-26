# Copyright (c) 2026 Kenneth Stott
# Canary: 9e4a2c71-0b3f-4d58-8a6e-1c7f5b2d9e03
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""object_owners is the owners helper's answer after every path that changes owner rows
(REQ-2219, amended 2026-11-23).

The meta views read who owns each data product and relationship, and who stewards each domain,
from object_owners. Every path that changes what the helper answers rewrites it: an Owner or
Steward field's save, a person's effective rows recomputed (write_person) or every holder's
(write_every_holder), the expiry run, and an object deleted or restored. Each test changes the
rows by that path alone and holds object_owners to ``holders`` for every kind."""

# Requirements: REQ-609, REQ-2219

from __future__ import annotations

import types
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import domain as domain_repo
from provisa.core.repositories import relationship as relationship_repo
from provisa.core.schema_org import (
    data_products,
    domains,
    model_names,
    object_owners,
    registered_tables,
    relationships,
    roles,
    sources,
    user_role_assignments,
)
from provisa.entitlements import effective_store, expiry_run, store
from provisa.entitlements.owners import (
    DOMAINS,
    PRODUCTS,
    RELATIONSHIPS,
    domain_identifier,
    holders,
    product_identifier,
    set_domain_stewards,
    set_product_owners,
    set_relationship_owners,
)
from tests.pg_control_plane import ORG

pytestmark = pytest.mark.unit

ANA, BEN = "ana", "ben"
REL = "orders_customer"
REL_IDENTIFIER = "sales.orders_customer"
_KIND = {PRODUCTS: "data_product", DOMAINS: "domain", RELATIONSHIPS: "relationship"}


@pytest.fixture
async def db(monkeypatch, control_plane):
    """Domains sales and finance; product c360 in sales; relationship orders_customer from
    sales.orders, its names stored. ana and ben hold a role carrying all three owner
    capabilities with no list, so each owns or stewards only what a row of theirs names."""
    org = control_plane.org()
    async with org.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(insert(domains).values(id="finance"))
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        for tid, name in ((1, "orders"), (2, "customers")):
            await conn.execute_core(
                insert(registered_tables).values(
                    id=tid, source_id="pg", domain_id="sales", schema_name="public", table_name=name
                )
            )
            await conn.execute_core(
                insert(model_names).values(
                    object_kind="table",
                    object_id=str(tid),
                    surface="sql",
                    name=name,
                    physical_name=name,
                    label=f"table {name}",
                    domain_id="sales",
                    table_id=tid,
                )
            )
        await conn.execute_core(
            insert(relationships).values(
                id=REL,
                source_table_id=1,
                target_table_id=2,
                source_column="customer_id",
                target_column="id",
                cardinality="many-to-one",
                graphql_field="customer",
            )
        )
        await conn.execute_core(
            insert(data_products).values(id="c360", domain_id="sales", name="C360")
        )
        await conn.execute_core(
            insert(roles).values(
                id="owners",
                capabilities=["data_product_owner", "domain_steward", "relationship_owner"],
                domain_access=["*"],
                parents=[],
            )
        )
        for user in (ANA, BEN):
            await conn.execute_core(
                insert(user_role_assignments).values(user_id=user, role_id="owners", domain_id="*")
            )

    async def _pool():
        return org

    monkeypatch.setattr(appmod.state, "tenant_db", org, raising=False)
    monkeypatch.setattr(appmod.state, "names_table", None, raising=False)
    monkeypatch.setattr(appmod.state, "held_entitlements", {}, raising=False)
    monkeypatch.setattr(appmod.state, "data_product_refs", (), raising=False)
    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    return org


def _info() -> Any:
    request = types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))
    return types.SimpleNamespace(context={"request": request})


async def _derived(db) -> dict[tuple[str, str], tuple[str, ...]]:
    async with db.acquire() as conn:
        rows = (await conn.execute_core(select(object_owners))).fetchall()
    out: dict[tuple[str, str], list[str]] = {}
    for r in rows:
        out.setdefault((r.object_kind, r.object_id), []).append(r.principal)
    return {k: tuple(sorted(v)) for k, v in out.items()}


async def _helper(db) -> dict[tuple[str, str], tuple[str, ...]]:
    async with db.acquire() as conn:
        return {
            (name, oid): people
            for kind, name in _KIND.items()
            for oid, people in (await holders(conn, kind)).items()
        }


async def _held_to_the_helper(db) -> dict[tuple[str, str], tuple[str, ...]]:
    derived = await _derived(db)
    assert derived == await _helper(db)
    return derived


async def _raw_row(db, principal: str, kind: str, capability: str, ids: list[str]) -> None:
    """A Provisa-held owner row written straight to the store, as the entitlements API writes it,
    without any refresh of object_owners."""
    async with db.acquire() as conn:
        await store.set_list(
            conn,
            principal,
            "owners",
            kind,
            ids,
            set_by="admin",
            reason="test",
            capability=capability,
        )


# --- a field's save -----------------------------------------------------------------------------


async def test_a_steward_field_save(db):
    assert (await schema_mutation.Mutation().set_domain_stewards(_info(), "sales", [ANA])).success
    assert (await _held_to_the_helper(db))[("domain", "sales")] == (ANA,)
    assert (await schema_mutation.Mutation().set_domain_stewards(_info(), "sales", [])).success
    assert ("domain", "sales") not in await _held_to_the_helper(db)


async def test_an_owner_field_save_of_a_product_and_of_a_relationship(db):
    async with db.acquire() as conn:
        await set_product_owners(conn, "c360", "sales", [BEN], set_by=None)
        changed = await set_relationship_owners(conn, REL, [ANA], set_by=None)
        await set_domain_stewards(conn, "finance", [BEN], set_by=None)
        from provisa.entitlements.owners import reload_owner_rows

        await reload_owner_rows(conn, appmod.state, changed | {BEN})
    derived = await _held_to_the_helper(db)
    assert derived[("data_product", "c360")] == (BEN,)
    assert derived[("relationship", REL)] == (ANA,)
    assert derived[("domain", "finance")] == (BEN,)


# --- a person's rows recomputed, or every holder's ----------------------------------------------


async def test_write_person(db):
    await _raw_row(db, ANA, "domains", "domain_steward", [domain_identifier("finance")])
    assert await _derived(db) == {}  # written straight to the store: nothing refreshed yet
    async with db.acquire() as conn:
        await effective_store.write_person(conn, appmod.state, ANA)
    assert (await _held_to_the_helper(db))[("domain", "finance")] == (ANA,)


async def test_write_every_holder(db):
    await _raw_row(
        db, BEN, "data_products", "data_product_owner", [product_identifier("c360", "sales")]
    )
    await _raw_row(db, ANA, "relationships", "relationship_owner", [REL_IDENTIFIER])
    async with db.acquire() as conn:
        await effective_store.write_every_holder(conn, appmod.state)
    derived = await _held_to_the_helper(db)
    assert derived[("data_product", "c360")] == (BEN,)
    assert derived[("relationship", REL)] == (ANA,)


# --- the expiry run -----------------------------------------------------------------------------


async def test_the_expiry_run(db, monkeypatch):
    monkeypatch.setattr(expiry_run, "notice_window", lambda: timedelta(days=7))
    async with db.acquire() as conn:
        await set_domain_stewards(conn, "sales", [ANA], set_by=None)
        await effective_store.write_person(conn, appmod.state, ANA)
        await store.add_expiry(
            conn,
            ANA,
            "owners",
            datetime.now(UTC) - timedelta(minutes=5),
            {"*": (domain_identifier("sales"),)},
            set_by="admin",
            reason="stewardship ends",
            list_kind="domains",
        )
    # The rule has passed: the helper no longer counts ana, while object_owners still says she is.
    assert (await _derived(db))[("domain", "sales")] == (ANA,)
    run = await expiry_run.run(db, ORG, datetime.now(UTC), None)
    assert len(run.expired) == 1
    assert ("domain", "sales") not in await _held_to_the_helper(db)


# --- an object deleted, or restored --------------------------------------------------------------


async def _owned_everything(db) -> None:
    async with db.acquire() as conn:
        changed = await set_domain_stewards(conn, "finance", [ANA], set_by=None)
        await set_product_owners(conn, "c360", "sales", [ANA], set_by=None)
        changed |= await set_relationship_owners(conn, REL, [BEN], set_by=None)
        # The fields' save refresh, as every field save runs it: ana's and ben's rows, then the
        # whole table.
        from provisa.entitlements.owners import reload_owner_rows

        await reload_owner_rows(conn, appmod.state, changed | {ANA})
    assert len(await _held_to_the_helper(db)) == 3


async def test_a_domain_deleted_and_restored(db):
    await _owned_everything(db)
    async with db.acquire() as conn:
        assert await domain_repo.delete(conn, "finance")
    assert ("domain", "finance") not in await _held_to_the_helper(db)
    async with db.acquire() as conn:
        key = (
            await conn.execute_core(select(domains.c.id).where(domains.c.retired_id == "finance"))
        ).scalar_one()
        assert await domain_repo.restore(conn, key)
    assert (await _held_to_the_helper(db))[("domain", "finance")] == (ANA,)


async def test_a_relationship_deleted_and_restored(db):
    await _owned_everything(db)
    async with db.acquire() as conn:
        assert await relationship_repo.delete(conn, REL)
    assert ("relationship", REL) not in await _held_to_the_helper(db)
    async with db.acquire() as conn:
        assert await relationship_repo.restore(conn, REL)
    assert (await _held_to_the_helper(db))[("relationship", REL)] == (BEN,)


async def test_a_product_deleted(db):
    await _owned_everything(db)
    async with db.acquire() as conn:
        assert await data_product_repo.delete(conn, "c360")
    assert ("data_product", "c360") not in await _held_to_the_helper(db)
