# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e5d14-6a2f-4c97-b1e0-7d4f9a2c6e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Owner and Steward fields name roles as well as people, and only an org admin or a current
holder of the object changes them (REQ-2219, amended 2026-11-23).

Each kind alike: a data product's Owner field, a domain's Steward field, a relationship's Owner
field. Naming a role writes the kind's capability row on the role, scoped to the object, so every
holder of the role holds it; the role must hold the capability. A field whose people or roles
change is refused unless the person saving it is an org admin (org_settings) or a current holder
of that object, who may also hand it over."""

# Requirements: REQ-609, REQ-2219

from __future__ import annotations

import types
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import capabilities, schema_mutation, schema_query
from provisa.core.schema_org import (
    data_products,
    domains,
    entitlements,
    model_names,
    object_owners,
    registered_tables,
    relationships,
    roles,
    sources,
    user_role_assignments,
)
from provisa.entitlements import store
from provisa.entitlements.lists import ROLE
from provisa.entitlements.owners import (
    DOMAINS,
    PRODUCTS,
    RELATIONSHIPS,
    Changer,
    Named,
    OwnedKind,
    OwnerChangeRefused,
    OwnerRoleRefused,
    field_of,
    holders,
    save_domain_stewards,
    save_owner_field,
    save_relationship_owners,
    write_object_owners,
)
from provisa.entitlements.runtime import entitlements_scope, owned_products

pytestmark = pytest.mark.unit

ANA, BEN, CARL, DANA, EVE = "ana", "ben", "carl", "dana", "eve"
REL = "orders_customer"
ADMIN = Changer("root", org_admin=True)

#: Each kind's object in the fixture, and the name object_owners gives the kind.
OBJECT = {PRODUCTS: "c360", DOMAINS: "sales", RELATIONSHIPS: REL}
KIND_NAME = {PRODUCTS: "data_product", DOMAINS: "domain", RELATIONSHIPS: "relationship"}
KINDS = pytest.mark.parametrize("kind", [PRODUCTS, DOMAINS, RELATIONSHIPS], ids=lambda k: k.noun)

_OWNER_CAPS = ["data_product_owner", "domain_steward", "relationship_owner"]


@pytest.fixture
async def db(monkeypatch, control_plane):
    """Domain sales; product c360 in it; relationship orders_customer from sales.orders, its names
    stored. Role ``owners`` (ana, ben) and role ``team`` (carl, dana) hold every owner capability
    with no list; role ``reader`` (eve) holds none."""
    org = control_plane.org()
    async with org.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales"))
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
        for role_id, caps in (("owners", _OWNER_CAPS), ("team", _OWNER_CAPS), ("reader", [])):
            await conn.execute_core(
                insert(roles).values(id=role_id, capabilities=caps, domain_access=["*"], parents=[])
            )
        for user, role_id in (
            (ANA, "owners"),
            (BEN, "owners"),
            (CARL, "team"),
            (DANA, "team"),
            (EVE, "reader"),
        ):
            await conn.execute_core(
                insert(user_role_assignments).values(user_id=user, role_id=role_id, domain_id="*")
            )

    async def _pool():
        return org

    monkeypatch.setattr(appmod.state, "tenant_db", org, raising=False)
    monkeypatch.setattr(appmod.state, "names_table", None, raising=False)
    monkeypatch.setattr(appmod.state, "held_entitlements", {}, raising=False)
    monkeypatch.setattr(appmod.state, "role_owner_rows", {}, raising=False)
    monkeypatch.setattr(appmod.state, "data_product_refs", (), raising=False)
    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_query, "_get_pool", _pool)
    return org


Save = Callable[..., Awaitable[set[str]]]


def _save(kind: OwnedKind) -> Save:
    """The field's save for ``kind``, every one called as (conn, people, roles, changer)."""

    async def product(conn, people, roles_, changer):
        return await save_owner_field(
            conn,
            "c360",
            "sales",
            people,
            roles=roles_,
            stored_domain_id="sales",
            changer=changer,
            set_by=changer.user_id,
        )

    async def domain(conn, people, roles_, changer):
        return await save_domain_stewards(
            conn, "sales", people, roles=roles_, changer=changer, set_by=changer.user_id
        )

    async def relationship(conn, people, roles_, changer):
        return await save_relationship_owners(
            conn, REL, people, roles=roles_, changer=changer, set_by=changer.user_id
        )

    return {PRODUCTS: product, DOMAINS: domain, RELATIONSHIPS: relationship}[kind]


async def _set(db, kind: OwnedKind, people, roles_=None, changer: Changer = ADMIN) -> set[str]:
    async with db.acquire() as conn:
        async with conn.transaction():
            return await _save(kind)(conn, people, roles_, changer)


async def _holders(db, kind: OwnedKind) -> tuple[str, ...]:
    async with db.acquire() as conn:
        return (await holders(conn, kind)).get(OBJECT[kind], ())


async def _field(db, kind: OwnedKind) -> Named:
    async with db.acquire() as conn:
        return await field_of(conn, kind, OBJECT[kind])


async def _row_count(db) -> int:
    async with db.acquire() as conn:
        return len((await conn.execute_core(select(entitlements.c.id))).fetchall())


# --- (b) roles as owners --------------------------------------------------------------------------


@KINDS
async def test_naming_a_role_makes_every_holder_of_it_a_holder(db, kind):
    changed = await _set(db, kind, [ANA], ["team"])
    assert await _holders(db, kind) == (ANA, CARL, DANA)
    assert await _field(db, kind) == Named((ANA,), ("team",))
    assert changed == {ANA, CARL, DANA}
    async with db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(entitlements).where(
                    entitlements.c.source == ROLE, entitlements.c.role_id == "team"
                )
            )
        ).fetchall()
    assert [(r.principal, r.capability, r.kind) for r in rows] == [
        (None, kind.capability, kind.list_kind)
    ]


@KINDS
async def test_a_role_taken_off_the_field_ends_its_row(db, kind):
    await _set(db, kind, [], ["team"])
    await _set(db, kind, None, [])
    assert await _holders(db, kind) == ()
    assert await _field(db, kind) == Named((), ())


@KINDS
async def test_a_role_without_the_capability_is_refused_and_nothing_is_written(db, kind):
    before = await _row_count(db)
    with pytest.raises(OwnerRoleRefused) as refused:
        await _set(db, kind, [ANA], ["reader"])
    noun = kind.noun.replace(" ", "_")
    assert refused.value.code == f"{noun}.{kind.holder}_role_not_eligible"
    assert refused.value.role_id == "reader"
    assert await _row_count(db) == before


@KINDS
async def test_a_role_that_is_no_role_is_refused(db, kind):
    with pytest.raises(OwnerRoleRefused):
        await _set(db, kind, [], ["ghost"])


@KINDS
async def test_people_left_as_held_when_only_roles_change(db, kind):
    await _set(db, kind, [BEN])
    await _set(db, kind, None, ["team"])
    assert await _field(db, kind) == Named((BEN,), ("team",))


async def test_a_model_build_leaves_a_roles_owner_rows_alone(db):
    await _set(db, PRODUCTS, [], ["team"])
    async with db.acquire() as conn:
        await store.write_role_rows(conn)
        role_rows = await store.load_role_rows(conn)
    assert await _field(db, PRODUCTS) == Named((), ("team",))
    # Nor are they the role's default lists.
    assert "data_products" not in role_rows["team"].defaults


async def test_object_owners_names_the_members_of_a_role_named(db):
    await _set(db, PRODUCTS, [], ["team"])
    await _set(db, DOMAINS, [ANA], ["team"])
    async with db.acquire() as conn:
        await write_object_owners(conn)
        rows = (await conn.execute_core(select(object_owners))).fetchall()
    got = sorted(
        (r.object_kind, r.object_id, r.principal_kind, r.principal, r.named, r.holds) for r in rows
    )
    assert got == [
        ("data_product", "c360", "person", CARL, False, True),
        ("data_product", "c360", "person", DANA, False, True),
        ("data_product", "c360", "role", "team", True, False),
        ("domain", "sales", "person", ANA, True, True),
        ("domain", "sales", "person", CARL, False, True),
        ("domain", "sales", "person", DANA, False, True),
        ("domain", "sales", "role", "team", True, False),
    ]


@KINDS
async def test_the_owners_view_lists_the_field_as_entered_beside_its_expansion(db, kind):
    """REQ-2219 (amended 2026-11-24): an Owner (or Steward) field naming "team, ana" lists both
    principals, each with its kind, through the object's owners relationship, beside the people
    the role expands to; rights still come from the expansion alone."""
    from sqlalchemy import text

    from provisa.api._meta_views import _META_TABLE_VIEWS
    from provisa.api.startup_seed import _adapt_view_ddl

    view = {PRODUCTS: "data_product_owners", DOMAINS: "domain_stewards"}.get(
        kind, "relationship_owners"
    )
    key = {PRODUCTS: "product_id", DOMAINS: "domain_id"}.get(kind, "relationship_id")
    await _set(db, kind, [ANA], ["team"])
    async with db.acquire() as conn:
        await write_object_owners(conn)
        await conn.execute(_adapt_view_ddl(_META_TABLE_VIEWS[view]))
        rows = (
            await conn.execute_core(
                text(
                    f"SELECT principal_kind, principal, named, holds FROM {view}_meta "
                    f"WHERE {key} = :oid ORDER BY id"
                ),
                {"oid": OBJECT[kind]},
            )
        ).fetchall()
    listed = [tuple(r) for r in rows]
    assert [(k, p) for k, p, named, _ in listed if named] == [("person", ANA), ("role", "team")]
    assert sorted(p for k, p, _, holds in listed if holds) == [ANA, CARL, DANA]
    assert all(k == "person" for k, _, _, holds in listed if holds)
    # The rights are the expansion's, unchanged.
    assert await _holders(db, kind) == (ANA, CARL, DANA)


async def test_a_person_acting_as_a_role_named_owns_the_product_at_runtime(db):
    await _set(db, PRODUCTS, [], ["team"])
    async with db.acquire() as conn:
        role_rows = await store.load_role_owner_rows(conn)
    state = types.SimpleNamespace(
        held_entitlements={},
        role_owner_rows=role_rows,
        roles={rid: {"id": rid, "capabilities": _OWNER_CAPS} for rid in ("team", "owners")},
        role_chains={"team": ["team"], "owners": ["owners"]},
        data_product_refs=(("c360", "sales"),),
    )

    def _identity(user: str, role_id: str) -> Any:
        return types.SimpleNamespace(
            user_id=user, raw_claims={}, entitlements_claim=None, acting_roles=[role_id]
        )

    with entitlements_scope(_identity(CARL, "team")):
        assert owned_products(state) == ("c360",)
    with entitlements_scope(_identity(ANA, "owners")):
        assert owned_products(state) == ()


async def test_the_owner_field_query_answers_people_and_roles(db):
    await _set(db, PRODUCTS, [ANA], ["team"])
    got = await schema_mutation_query().owner_field(_info(None), "data_product", "c360")
    assert (got.people, got.roles) == ([ANA], ["team"])


# --- (a) who may change the field -----------------------------------------------------------------


@KINDS
async def test_a_change_by_someone_neither_admin_nor_holder_is_refused(db, kind):
    await _set(db, kind, [ANA])
    before = await _row_count(db)
    with pytest.raises(OwnerChangeRefused) as refused:
        await _set(db, kind, [ANA, EVE], changer=Changer(EVE, org_admin=False))
    noun = kind.noun.replace(" ", "_")
    assert refused.value.code == f"{noun}.{kind.holder}s_change_refused"
    with pytest.raises(OwnerChangeRefused):
        await _set(db, kind, None, ["team"], changer=Changer(BEN, org_admin=False))
    assert await _row_count(db) == before
    assert await _holders(db, kind) == (ANA,)


@KINDS
async def test_an_unchanged_field_needs_neither(db, kind):
    await _set(db, kind, [ANA], ["team"])
    await _set(db, kind, [ANA], ["team"], changer=Changer(EVE, org_admin=False))
    await _set(db, kind, None, None, changer=Changer(EVE, org_admin=False))
    assert await _field(db, kind) == Named((ANA,), ("team",))


@KINDS
async def test_an_org_admin_changes_it(db, kind):
    await _set(db, kind, [ANA])
    await _set(db, kind, [BEN], ["team"], changer=Changer(EVE, org_admin=True))
    assert await _field(db, kind) == Named((BEN,), ("team",))


@KINDS
async def test_a_current_holder_adds_and_removes_holders(db, kind):
    await _set(db, kind, [ANA])
    await _set(db, kind, [ANA, BEN], ["team"], changer=Changer(ANA, org_admin=False))
    assert await _field(db, kind) == Named((ANA, BEN), ("team",))
    await _set(db, kind, [ANA], [], changer=Changer(ANA, org_admin=False))
    assert await _field(db, kind) == Named((ANA,), ())


@KINDS
async def test_a_current_holder_hands_it_over(db, kind):
    await _set(db, kind, [ANA])
    await _set(db, kind, [BEN], changer=Changer(ANA, org_admin=False))
    assert await _holders(db, kind) == (BEN,)
    # ana handed it over, so is no longer one who may change it.
    with pytest.raises(OwnerChangeRefused):
        await _set(db, kind, [ANA], changer=Changer(ANA, org_admin=False))


@KINDS
async def test_a_holder_through_a_role_named_changes_it(db, kind):
    await _set(db, kind, [], ["team"])
    await _set(db, kind, [BEN], ["team"], changer=Changer(CARL, org_admin=False))
    assert await _field(db, kind) == Named((BEN,), ("team",))


async def test_creating_a_product_with_owners_takes_an_org_admin(db):
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(data_products).values(id="fresh", domain_id="sales", name="Fresh")
        )
        with pytest.raises(OwnerChangeRefused):
            await save_owner_field(
                conn,
                "fresh",
                "sales",
                [ANA],
                stored_domain_id=None,
                changer=Changer(ANA, org_admin=False),
                set_by=ANA,
            )
        assert (
            await save_owner_field(
                conn,
                "fresh",
                "sales",
                [],
                stored_domain_id=None,
                changer=Changer(ANA, False),
                set_by=ANA,
            )
            == set()
        )


def _info(user: str | None) -> Any:
    identity = None if user is None else types.SimpleNamespace(user_id=user, roles=[])
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id="a")
    )
    return types.SimpleNamespace(context={"request": request})


def schema_mutation_query() -> Any:
    return schema_query.Query()


async def test_the_steward_mutation_refuses_a_non_holder_by_name(db, monkeypatch):
    assert (
        await schema_mutation.Mutation().set_domain_stewards(_info(None), "sales", [ANA])
    ).success
    monkeypatch.setattr(capabilities, "has_capability", lambda info, cap: False)
    result = await schema_mutation.Mutation().set_domain_stewards(_info(EVE), "sales", [EVE])
    assert not result.success
    assert result.code == "domain.stewards_change_refused"
    assert result.params == {"domain": "sales"}
    # ana, a current steward, hands it to the team.
    result = await schema_mutation.Mutation().set_domain_stewards(
        _info(ANA), "sales", [], steward_roles=["team"]
    )
    assert result.success, result.message
    assert await _holders(db, DOMAINS) == (CARL, DANA)


async def test_the_steward_mutation_refuses_a_role_without_the_capability(db):
    result = await schema_mutation.Mutation().set_domain_stewards(
        _info(None), "sales", [], steward_roles=["reader"]
    )
    assert not result.success
    assert result.code == "domain.steward_role_not_eligible"
    assert result.params == {"domain": "sales", "role": "reader"}


def test_the_refusal_codes_are_named_in_every_locale():
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[2] / "provisa-ui" / "src" / "i18n" / "locales"
    locales = sorted(p.name for p in root.iterdir() if p.is_dir())
    assert len(locales) == 13
    for lng in locales:
        errors = json.loads((root / lng / "serverErrors.json").read_text())["serverErrors"]
        for noun, holder in (
            ("data_product", "owner"),
            ("relationship", "owner"),
            ("domain", "steward"),
        ):
            assert errors[noun][f"{holder}_role_not_eligible"], (lng, noun)
            assert errors[noun][f"{holder}s_change_refused"], (lng, noun)
