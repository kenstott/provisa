# Copyright (c) 2026 Kenneth Stott
# Canary: 8e41b7c6-2d90-4a53-9f17-c3a6d05e7b21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Owner field names people, the row rule enforces (REQ-2188, REQ-2219 amended 2026-11-23).

Saving a product's Owner field writes or ends its people's data_product_owner rows. A role holding
data_product_owner holds RW on data products with generated U and D rules reading the products the
person acting owns: those their own rows name. A role holding the capability with no row owns
nothing by it. Nobody writes the rule and Security edits none: it is generated at each build, and an
org admin, or a role an org admin gave RW of its own, is not narrowed by it. These run the scenario
through the admin API, on a model store, with the build's own folds."""

# Requirements: REQ-2188, REQ-2219, REQ-2155, REQ-1634, REQ-1944

from __future__ import annotations

import contextlib
import types
from collections.abc import Generator
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import roles_router, schema_helpers, schema_mutation
from provisa.api.admin.capabilities import right_domain_refusal
from provisa.api.admin.types import DataProductInput
from provisa.core import domain_policy, model_row_rules
from provisa.core.model_row_rules import ModelRowRefused
from provisa.core.models import RLSRule
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import rls as rls_repo
from provisa.core.request_context import current_acting_role, current_org
from provisa.core.schema_org import (
    data_products,
    domains,
    glossary_terms,
    registered_tables,
    roles,
    sources,
    user_role_assignments,
)
from provisa.entitlements import store
from provisa.entitlements.owners import product_owners, set_product_owners
from provisa.entitlements.runtime import entitlements_scope
from provisa.security.inheritance import flatten_role_dicts, parent_map, role_chains
from provisa.security.row_filters import (
    OWNER_RULE,
    acting_model_rule,
    model_write_rules,
    owner_held_roles,
    resolve_rules_by_right,
    with_owner_grant,
)
from tests.unit.gate_identity import serve_every_column
from tests.rules import holds

_META_TABLE_ID = 9001
_TERMS_TABLE_ID = 9002

#: Who acts, and the role they act as. pam owns pets_p and sam sales_p, both through ``owners``
#: (data_product_owner); lee holds ``owners_lead``, a child of it, and owns nothing until named.
PAM, SAM, LEE, RITA, OLGA, PAT, CORA = "pam", "sam", "lee", "rita", "olga", "pat", "cora"
_ACTS_AS = {
    PAM: "owners",
    SAM: "owners",
    LEE: "owners_lead",
    RITA: "reader",
    OLGA: "org_admin",
    PAT: "product_admin",
    CORA: "curator",
}


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)


class _Runtime:
    """The parts of a runtime build the store and the gates read, from the build's own folds. What
    it holds of the entitlement store is the application's, as a save leaves it."""

    def __init__(self, db) -> None:
        self.db = db
        self.roles: dict[str, dict] = {}
        self.meta_roles: dict[str, tuple[str, ...]] = {}
        self.role_build_inputs: dict[str, Any] = {}
        self.role_chains: dict[str, list[str]] = {}
        self.builds = 0

    @property
    def held_entitlements(self) -> dict[str, Any]:
        return appmod.state.held_entitlements

    @property
    def data_product_refs(self) -> tuple[tuple[str, str], ...]:
        return appmod.state.data_product_refs

    async def rebuild(self) -> None:
        self.builds += 1
        async with self.db.acquire() as conn:
            rows = [dict(r._mapping) for r in (await conn.execute_core(select(roles))).fetchall()]
            stored_rules = await rls_repo.list_all(conn)
            appmod.state.held_entitlements = await store.load_held(conn)
            appmod.state.data_product_refs = await data_product_repo.refs(conn)
        tables = [
            {"id": _META_TABLE_ID, "domain_id": "meta", "table_name": "data_products"},
            {"id": _TERMS_TABLE_ID, "domain_id": "ontology", "table_name": "_terms"},
        ]
        chains = role_chains(rows)
        held = with_owner_grant(rows)
        folded = flatten_role_dicts(held)
        resolved = resolve_rules_by_right(
            stored_rules, tables, parent_map(rows), [], chains, [], {}
        )
        self.roles.clear()
        for role in folded:
            self.roles[role["id"]] = {**role, "acting_chain": chains[role["id"]]}
        self.role_chains.clear()
        self.role_chains.update(chains)
        self.role_build_inputs = {
            "model_write_rules": model_write_rules(
                resolved, tables, [r["id"] for r in folded if r.get("owner_held")]
            ),
            "physical_table_map": {("provisa-admin", "data_products"): "data_products_meta"},
        }


@pytest.fixture
async def plane(monkeypatch, control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="provisa-admin", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="pets"))
        for table_id, domain, name in (
            (_META_TABLE_ID, "meta", "data_products"),
            (_TERMS_TABLE_ID, "ontology", "_terms"),
        ):
            await conn.execute_core(
                insert(registered_tables).values(
                    id=table_id,
                    source_id="provisa-admin",
                    domain_id=domain,
                    schema_name="meta",
                    table_name=name,
                )
            )
        for role, parents, capabilities in (
            ("owners", [], ["data_product_read", "data_product_owner"]),
            ("owners_lead", ["owners"], []),  # inherits the capability
            ("reader", [], ["data_product_read"]),  # R only, cannot own
            ("product_admin", [], ["data_product_read", "data_product_rw"]),  # RW of its own
            ("curator", [], ["glossary_read", "glossary_rw"]),
        ):
            await conn.execute_core(
                insert(roles).values(
                    id=role, capabilities=capabilities, domain_access=["*"], parents=parents
                )
            )
        for user, role in _ACTS_AS.items():
            await conn.execute_core(
                insert(user_role_assignments).values(user_id=user, role_id=role, domain_id="*")
            )
        for product, owner in (("pets_p", PAM), ("sales_p", SAM)):
            await conn.execute_core(
                insert(data_products).values(id=product, domain_id="pets", name=product)
            )
            await set_product_owners(conn, product, "pets", [owner], set_by=None)
    runtime = _Runtime(db)

    async def _pool():
        return db

    async def _refresh() -> None:
        return None

    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", runtime.roles, raising=False)
    monkeypatch.setattr(appmod.state, "role_chains", runtime.role_chains, raising=False)
    monkeypatch.setattr(appmod.state, "held_entitlements", {}, raising=False)
    monkeypatch.setattr(appmod.state, "idp_entitlements", {}, raising=False)
    monkeypatch.setattr(appmod.state, "data_product_refs", (), raising=False)
    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_helpers, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", runtime.rebuild)
    monkeypatch.setattr(schema_helpers, "_rebuild_schemas", runtime.rebuild)
    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _refresh)
    monkeypatch.setattr(
        model_row_rules,
        "_provider",
        lambda role, table, right: acting_model_rule(runtime, role, table, right),
    )
    await runtime.rebuild()
    return runtime


def _identity(user: str, role: str, idp: dict | None = None) -> Any:
    return types.SimpleNamespace(
        user_id=user,
        roles=[role],
        acting_roles=[role],
        raw_claims={"entitlements": idp or {}},
        entitlements_claim="entitlements",
    )


def _request(user: str, role: str | None = None) -> Any:
    role = role or _ACTS_AS[user]
    return types.SimpleNamespace(
        state=types.SimpleNamespace(identity=_identity(user, role), active_org_id="acme", role=role)
    )


def _info(user: str) -> Any:
    return types.SimpleNamespace(context={"request": _request(user)})


@contextlib.contextmanager
def _acting(user: str, idp: dict | None = None) -> Generator[None]:
    """``user`` signed in and acting as their role, as a transport binds them."""
    role = _ACTS_AS[user]
    token = current_acting_role.set(role)
    try:
        with entitlements_scope(_identity(user, role, idp)):
            yield
    finally:
        current_acting_role.reset(token)


M = schema_mutation.Mutation


def _input(product: str, name: str, owners: list[str] | None) -> DataProductInput:
    return DataProductInput(id=product, domain_id="pets", name=name, owners=owners)


async def _update(user: str, product: str, name: str, owners: list[str] | None = None) -> Any:
    with _acting(user):
        return await M().update_data_product(_info(user), _input(product, name, owners))


async def _create(user: str, product: str, owners: list[str] | None = None) -> Any:
    with _acting(user):
        return await M().create_data_product(_info(user), _input(product, product, owners))


async def _delete(user: str, product: str) -> Any:
    with _acting(user):
        return await M().delete_data_product(_info(user), product)


async def _name(plane, product: str) -> str | None:
    async with plane.db.acquire() as conn:
        row = await data_product_repo.get(conn, product)
    return None if row is None else row["name"]


async def _owners(plane) -> dict[str, tuple[str, ...]]:
    async with plane.db.acquire() as conn:
        return await product_owners(conn)


def _holds_rw(plane, role: str) -> bool:
    return "data_product_rw" in plane.roles[role]["capabilities"]


# --- the scenario --------------------------------------------------------------------------------


async def test_an_owner_by_entitlement_edits_their_product_and_no_other(plane):
    assert (await _update(PAM, "pets_p", "renamed")).success
    assert await _name(plane, "pets_p") == "renamed"

    with pytest.raises(ModelRowRefused, match="owners"):
        await _update(PAM, "sales_p", "stolen")
    assert await _name(plane, "sales_p") == "sales_p"


async def test_a_holder_of_the_capability_with_no_row_naming_it_is_no_owner(plane):
    """lee's role holds data_product_owner (inherited) with no row: he owns nothing by it."""
    assert _holds_rw(plane, "owners_lead")
    with pytest.raises(ModelRowRefused, match="owners_lead"):
        await _update(LEE, "pets_p", "by the lead")
    assert await _name(plane, "pets_p") == "pets_p"
    assert LEE not in {o for owners in (await _owners(plane)).values() for o in owners}


async def test_an_org_admin_with_no_rule_edits_either_product(plane):
    assert (await _update(OLGA, "pets_p", "one")).success
    assert (await _update(OLGA, "sales_p", "two")).success
    assert await _name(plane, "pets_p") == "one"
    assert await _name(plane, "sales_p") == "two"


async def test_a_role_with_r_only_and_a_u_rule_is_refused(plane):
    async with plane.db.acquire() as conn:
        await rls_repo.upsert(
            conn,
            RLSRule(
                table_id="data_products",
                role_id="reader",
                filter="1 = 1",
                description="gives nothing",
                rights=["U"],
            ),
        )
    await plane.rebuild()
    with pytest.raises(PermissionError, match="data_product_rw"):
        await _update(RITA, "pets_p", "no")
    assert await _name(plane, "pets_p") == "pets_p"


async def test_a_person_named_on_a_child_role_edits_what_they_are_named_owner_of(plane):
    assert (await _update(OLGA, "pets_p", "pets_p", [PAM, LEE])).success
    assert (await _update(LEE, "pets_p", "by the lead")).success
    assert await _name(plane, "pets_p") == "by the lead"

    with pytest.raises(ModelRowRefused, match="owners_lead"):
        await _update(LEE, "sales_p", "stolen")
    assert await _name(plane, "sales_p") == "sales_p"


async def test_an_owner_deletes_and_publishes_their_own_product_only(plane):
    with _acting(PAM):
        assert (await M().publish_data_product(_info(PAM), "pets_p")).success
        with pytest.raises(ModelRowRefused):
            await M().publish_data_product(_info(PAM), "sales_p")
    with pytest.raises(ModelRowRefused):
        await _delete(PAM, "sales_p")
    assert (await _delete(PAM, "pets_p")).success
    assert await _name(plane, "pets_p") is None
    assert await _name(plane, "sales_p") == "sales_p"


async def test_the_mcp_tool_is_held_to_the_same_rule(plane, monkeypatch):
    from provisa.api.mcp import tools

    # The role's built schema is not what this asks about; its rights and rows are.
    monkeypatch.setattr(tools, "require_role", lambda _role, _state: None)
    request = _request(PAM)
    with _acting(PAM):
        await tools.create_data_product(appmod.state, "owners", request, "pets_p", "pets", "by mcp")
        with pytest.raises(ModelRowRefused):
            await tools.create_data_product(
                appmod.state, "owners", request, "sales_p", "pets", "stolen"
            )
    assert await _name(plane, "pets_p") == "by mcp"
    assert await _name(plane, "sales_p") == "sales_p"


async def test_the_mcp_tool_names_owners_as_the_form_does(plane, monkeypatch):
    from provisa.api.mcp import tools

    monkeypatch.setattr(tools, "require_role", lambda _role, _state: None)
    with _acting(OLGA):
        await tools.create_data_product(
            appmod.state, "org_admin", _request(OLGA), "pets_p", "pets", "pets_p", owners=[SAM]
        )
    assert (await _owners(plane))["pets_p"] == (SAM,)
    assert (await _update(SAM, "pets_p", "sams now")).success


# --- the Owner field writes and ends rows ----------------------------------------------------------


async def test_naming_a_person_writes_their_row_and_they_edit_at_once(plane):
    assert (await _update(OLGA, "sales_p", "sales_p", [SAM, PAM])).success
    assert (await _owners(plane))["sales_p"] == (PAM, SAM)
    async with plane.db.acquire() as conn:
        held = (await store.load_held(conn))[PAM]
    (row,) = [r for r in held.lists if r.capability == "data_product_owner"]
    assert row.role_id == "owners" and set(row.identifiers) == {"pets.pets_p", "pets.sales_p"}
    assert (await _update(PAM, "sales_p", "pams too")).success


async def test_unnaming_a_person_ends_their_row_and_their_edit(plane):
    assert (await _update(OLGA, "pets_p", "pets_p", [SAM])).success
    assert (await _owners(plane))["pets_p"] == (SAM,)
    async with plane.db.acquire() as conn:
        held = await store.load_held(conn)
    assert not [r for r in held.get(PAM, store.HeldEntitlements()).lists if r.identifiers]
    with pytest.raises(ModelRowRefused):
        await _update(PAM, "pets_p", "no")
    assert (await _update(SAM, "pets_p", "taken")).success
    assert await _name(plane, "pets_p") == "taken"


async def test_a_person_who_cannot_own_is_refused_and_nothing_is_saved(plane):
    result = await _update(OLGA, "pets_p", "renamed", [RITA])
    assert not result.success
    assert result.code == "data_product.owner_not_eligible"
    assert result.params == {"data_product": "pets_p", "owner": RITA}
    assert await _name(plane, "pets_p") == "pets_p"
    assert (await _owners(plane))["pets_p"] == (PAM,)


async def test_a_save_that_leaves_the_owners_out_keeps_them(plane):
    assert (await _update(PAM, "pets_p", "renamed")).success
    assert (await _owners(plane))["pets_p"] == (PAM,)


async def test_owners_named_on_create_edit_the_new_product(plane):
    assert (await _create(OLGA, "pets_new", [SAM])).success
    assert (await _owners(plane))["pets_new"] == (SAM,)
    assert (await _update(SAM, "pets_new", "sams")).success


async def test_deleting_a_product_ends_every_row_naming_it(plane):
    assert (await _delete(OLGA, "pets_p")).success
    assert "pets_p" not in await _owners(plane)
    async with plane.db.acquire() as conn:
        held = await store.load_held(conn)
    assert PAM not in held


async def test_a_save_builds_nothing(plane):
    before = plane.builds
    assert (await _update(OLGA, "pets_p", "renamed", [SAM])).success
    assert plane.builds == before


async def test_an_identity_providers_list_names_an_owner(plane):
    """An identity provider's data_products list on a role holding the capability is an owner's
    row as much as a Provisa-held one; on a role without it, it owns nothing."""
    with _acting(LEE, {"owners_lead": {"data_products": ["pets.sales_p"]}}):
        assert (
            await M().update_data_product(_info(LEE), _input("sales_p", "by idp", None))
        ).success
    assert await _name(plane, "sales_p") == "by idp"


# --- the capability is how the grant changes -------------------------------------------------------


def test_a_role_holding_the_capability_holds_the_grant_and_the_rules():
    rows = [
        {"id": "owners", "capabilities": ["data_product_owner"], "parents": []},
        {"id": "lead", "capabilities": [], "parents": ["owners"]},
        {"id": "reader", "capabilities": ["data_product_read"], "parents": []},
    ]
    assert owner_held_roles(rows) == {"owners", "lead"}
    rules = model_write_rules([], [], owner_held_roles(rows))
    assert rules[("owners", "data_products", "U")] == OWNER_RULE
    assert rules[("lead", "data_products", "D")] == OWNER_RULE


def test_the_owner_rule_reads_the_products_owned_not_the_roles_acted_as():
    assert holds(OWNER_RULE, "current_setting('provisa.owned_products')")
    assert not holds(OWNER_RULE, "current_setting('provisa.acting_roles')")
    assert OWNER_RULE.this.name == "id"


async def test_rw_includes_create_which_has_no_row_to_narrow(plane):
    assert (await _create(PAM, "pets_new")).success
    assert await _name(plane, "pets_new") == "pets_new"


async def test_a_rule_written_by_hand_does_not_replace_the_owners_rule(plane):
    async with plane.db.acquire() as conn:
        await rls_repo.upsert(
            conn,
            RLSRule(
                table_id="data_products",
                role_id="owners",
                filter="1 = 1",
                description="an attempt to widen it",
                rights=["U", "D"],
            ),
        )
    await plane.rebuild()
    with pytest.raises(ModelRowRefused):
        await _update(PAM, "sales_p", "stolen")


async def test_no_generated_rule_is_stored_for_security_to_edit(plane):
    await _update(OLGA, "pets_p", "pets_p", [SAM])
    async with plane.db.acquire() as conn:
        assert await rls_repo.list_all(conn) == []
        stored = (
            await conn.execute_core(select(roles.c.capabilities).where(roles.c.id == "owners"))
        ).scalar_one()
    assert "data_product_rw" not in stored


async def test_a_client_cannot_say_which_products_it_owns(plane, monkeypatch):
    from provisa.core.request_context import set_session_vars, reset_session_vars

    token = set_session_vars({"owned_products": "sales_p"})
    try:
        with pytest.raises(ModelRowRefused):
            await _update(PAM, "sales_p", "stolen")
    finally:
        reset_session_vars(token)


async def test_the_roles_listing_shows_what_the_capability_gives(plane):
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(
                user_id=OLGA, roles=["org_admin"], acting_roles=["org_admin"]
            ),
            active_org_id="acme",
        ),
        app=types.SimpleNamespace(state=types.SimpleNamespace()),
    )
    listed = {r["id"]: r for r in await roles_router.list_roles(request)}
    assert "data_product_rw" in listed["owners"]["effectiveCapabilities"]
    assert "data_product_rw" not in listed["owners"]["capabilities"]
    assert "data_product_rw" in listed["owners_lead"]["effectiveCapabilities"]
    assert "data_product_rw" not in listed["reader"]["effectiveCapabilities"]


# --- an org admin may widen it ---------------------------------------------------------------------


async def test_naming_an_org_admin_does_not_narrow_them(plane):
    assert (await _update(OLGA, "pets_p", "pets_p", [OLGA])).success
    assert (await _update(OLGA, "sales_p", "still mine")).success
    assert await _name(plane, "sales_p") == "still mine"
    assert not any(k[0] == "org_admin" for k in plane.role_build_inputs["model_write_rules"])


async def test_a_role_with_rw_of_its_own_is_not_narrowed(plane):
    assert (await _update(PAT, "sales_p", "any")).success
    assert (await _update(PAT, "pets_p", "still any")).success
    assert await _name(plane, "pets_p") == "still any"


async def test_a_role_with_rw_of_its_own_keeps_a_rule_an_org_admin_gave_it(plane):
    async with plane.db.acquire() as conn:
        await rls_repo.upsert(
            conn,
            RLSRule(
                table_id="data_products",
                role_id="product_admin",
                filter="id = 'pets_p'",
                description="a data product administrator narrowed by hand",
                rights=["U"],
            ),
        )
    await plane.rebuild()
    assert (await _update(PAT, "pets_p", "hers")).success
    with pytest.raises(ModelRowRefused):
        await _update(PAT, "sales_p", "not hers")


def test_a_child_holding_rw_of_its_own_is_not_narrowed_by_a_parent_holding_the_capability():
    rows = [
        {"id": "team", "capabilities": ["data_product_owner"], "parents": []},
        {"id": "lead", "capabilities": ["data_product_rw"], "parents": ["team"]},
        {"id": "member", "capabilities": [], "parents": ["team"]},
    ]
    assert owner_held_roles(rows) == {"team", "member"}


def test_a_role_without_the_capability_is_held_to_nothing():
    rows = [{"id": "team", "capabilities": [], "parents": []}]
    assert with_owner_grant(rows) == rows


def test_the_grant_leaves_its_input_alone():
    row = {"id": "team", "capabilities": ["data_product_owner"], "parents": []}
    assert with_owner_grant([row])[0]["capabilities"] == ["data_product_owner", "data_product_rw"]
    assert row == {"id": "team", "capabilities": ["data_product_owner"], "parents": []}


async def test_an_owners_edit_stays_within_its_reach(plane):
    plane.roles["owners"]["domain_access"] = ["sales"]
    identity = types.SimpleNamespace(user_id=PAM, acting_roles=["owners"], env_capabilities=set())
    refusal = right_domain_refusal(identity, appmod.state, "data_product_rw", {"pets"})
    assert refusal is not None and "pets" in refusal


# --- meaning: glossary terms ---------------------------------------------------------------------


_NAMED = "name = 'held'"


async def _term_rule(plane, role: str, rights: list[str]) -> None:
    async with plane.db.acquire() as conn:
        await rls_repo.upsert(
            conn,
            RLSRule(
                table_id="_terms",
                role_id=role,
                filter=_NAMED,
                description="only the held term",
                rights=rights,
            ),
        )
        for name in ("held", "other"):
            await conn.execute_core(insert(glossary_terms).values(name=name, definition=name))
    await plane.rebuild()


async def _term_id(plane, name: str) -> int:
    async with plane.db.acquire() as conn:
        return (
            await conn.execute_core(
                select(glossary_terms.c.id).where(glossary_terms.c.name == name)
            )
        ).scalar_one()


async def _definition(plane, term_id: int) -> str:
    async with plane.db.acquire() as conn:
        return (
            await conn.execute_core(
                select(glossary_terms.c.definition).where(glossary_terms.c.id == term_id)
            )
        ).scalar_one()


async def test_a_u_rule_on_terms_holds_a_terms_update(plane):
    await _term_rule(plane, "curator", ["U"])
    held, other = await _term_id(plane, "held"), await _term_id(plane, "other")
    token = current_acting_role.set("curator")
    try:
        async with plane.db.acquire() as conn:
            assert await glossary_repo.set_definition(conn, held, "edited")
            with pytest.raises(ModelRowRefused, match="glossary_terms"):
                await glossary_repo.set_definition(conn, other, "stolen")
    finally:
        current_acting_role.reset(token)
    assert await _definition(plane, held) == "edited"
    assert await _definition(plane, other) == "other"


async def test_a_d_rule_on_terms_holds_a_terms_delete(plane):
    await _term_rule(plane, "curator", ["D"])
    held, other = await _term_id(plane, "held"), await _term_id(plane, "other")
    token = current_acting_role.set("curator")
    try:
        async with plane.db.acquire() as conn:
            with pytest.raises(ModelRowRefused):
                await glossary_repo.delete_term(conn, other)
            assert await glossary_repo.delete_term(conn, held)
    finally:
        current_acting_role.reset(token)
    assert await _definition(plane, other) == "other"


async def test_a_role_holding_only_r_on_terms_is_held_by_no_rule(plane):
    """A rule gives nothing and holds nothing where the role has no RW: the right's own gate
    refuses its writes, and a write its other rights make of a term is not narrowed by it."""
    await _term_rule(plane, "reader", ["U"])
    assert ("reader", "glossary_terms", "U") in plane.role_build_inputs["model_write_rules"]
    assert acting_model_rule(plane, "reader", "glossary_terms", "U") is None


def test_a_rule_on_a_data_table_named_like_terms_holds_no_model_write():
    tables = [{"id": 1, "domain_id": "sales", "table_name": "_terms"}]
    resolved = [{"role_id": "a", "table_id": 1, "filter_expr": "x", "right": "U"}]
    assert model_write_rules(resolved, tables) == {}


def test_a_terms_rule_is_read_through_its_definition_view():
    state = types.SimpleNamespace(
        role_build_inputs={
            "model_write_rules": {("a", "glossary_terms", "U"): "status = 'draft'"},
            "physical_table_map": {("provisa-admin", "_terms"): "_terms_ontology"},
        },
        meta_roles={},
        roles={"a": {"id": "a", "capabilities": ["glossary_rw"], "session_vars": {}}},
    )
    rule = acting_model_rule(state, "a", "glossary_terms", "U")
    assert rule is not None and rule.view == "_terms_ontology"


def test_the_store_asks_the_runtime_about_a_term(monkeypatch):
    from provisa.api.app import _model_rule
    from provisa.api.org_runtime import runtime_key

    asked = []
    monkeypatch.setattr(appmod, "acting_model_rule", lambda *args: asked.append(args[1:]))
    monkeypatch.setattr(
        appmod.state, "org_registry", {runtime_key("acme", None): object()}, raising=False
    )
    token = current_org.set("acme")
    try:
        _model_rule("curator", "glossary_terms", "U")
    finally:
        current_org.reset(token)
    assert asked == [("curator", "glossary_terms", "U")]


# --- a set of roles --------------------------------------------------------------------------------


async def test_a_set_of_roles_edits_what_its_person_owns(plane):
    plane.meta_roles["meta:owners+reader"] = ("owners", "reader")
    plane.roles["meta:owners+reader"] = {
        "id": "meta:owners+reader",
        "capabilities": ["data_product_rw"],
        "acting_chain": ["owners", "reader"],
    }
    with entitlements_scope(_identity(PAM, "meta:owners+reader")):
        rule = acting_model_rule(plane, "meta:owners+reader", "data_products", "U")
    assert rule is not None and holds(rule.predicate, "current_setting('provisa.owned_products')")
    assert rule.session_vars["owned_products"] == "pets_p"


async def test_a_set_with_a_member_holding_rw_of_its_own_is_not_narrowed(plane):
    plane.meta_roles["meta:owners+product_admin"] = ("owners", "product_admin")
    assert acting_model_rule(plane, "meta:owners+product_admin", "data_products", "U") is None


async def test_a_retired_products_owner_still_owns_it(plane):
    async with plane.db.acquire() as conn:
        await data_product_repo.retire(conn, "pets_p", reason="done", successor=None, by=OLGA)
    assert (await _owners(plane))["pets_p"] == (PAM,)


# --- the configuration carries no owner_role -------------------------------------------------------


def test_a_config_product_naming_an_owner_role_is_refused_by_name():
    """REQ-2219 (amended 2026-11-23): a product's owners are people, held as entitlement rows; a
    config that still carries owner_role is refused, never read past."""
    from pydantic import ValidationError

    from provisa.core.models import DataProduct

    with pytest.raises(ValidationError, match="owner_role is no longer a setting"):
        DataProduct.model_validate(
            {"id": "pets_p", "domain_id": "pets", "name": "Pets", "owner_role": "pets_team"}
        )


def test_a_config_product_with_no_owner_role_loads():
    from provisa.core.models import DataProduct

    product = DataProduct.model_validate({"id": "pets_p", "domain_id": "pets", "name": "Pets"})
    assert "owner_role" not in product.model_dump()
