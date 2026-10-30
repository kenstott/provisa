# Copyright (c) 2026 Kenneth Stott
# Canary: 8e41b7c6-2d90-4a53-9f17-c3a6d05e7b21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Owner field decides, the row rule enforces (REQ-2188).

Naming a role in a product's Owner field gives it RW on data products with generated U and D rules
reading the product's owner role. Nobody writes one and Security edits none: they are worked out
from the Owner fields at each build, so a role that owns no product any more holds nothing, and an
org admin, or a role an org admin gave RW of its own, is not narrowed by being named. These run the
scenario through the admin API, on a model store, with the build's own folds."""

# Requirements: REQ-2188, REQ-2155, REQ-1634, REQ-1944

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert, select, update

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
)
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

_META_TABLE_ID = 9001
_TERMS_TABLE_ID = 9002


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    serve_every_column(monkeypatch)
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)


class _Runtime:
    """The parts of a runtime build the store and the gates read, from the build's own folds."""

    def __init__(self, db) -> None:
        self.db = db
        self.roles: dict[str, dict] = {}
        self.meta_roles: dict[str, tuple[str, ...]] = {}
        self.role_build_inputs: dict[str, Any] = {}
        self.role_chains: dict[str, list[str]] = {}
        self.builds = 0

    async def rebuild(self) -> None:
        self.builds += 1
        async with self.db.acquire() as conn:
            rows = [dict(r._mapping) for r in (await conn.execute_core(select(roles))).fetchall()]
            owners = await data_product_repo.owner_roles(conn)
            stored_rules = await rls_repo.list_all(conn)
        tables = [
            {"id": _META_TABLE_ID, "domain_id": "meta", "table_name": "data_products"},
            {"id": _TERMS_TABLE_ID, "domain_id": "ontology", "table_name": "_terms"},
        ]
        chains = role_chains(rows)
        held = with_owner_grant(rows, owners)
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
            "physical_table_map": {"data_products": "data_products_meta"},
        }


@pytest.fixture
async def plane(monkeypatch, control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="provisa-admin", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="pets"))
        await conn.execute_core(insert(domains).values(id="meta"))
        await conn.execute_core(insert(domains).values(id="ontology"))
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
            ("pets_team", [], ["data_product_read"]),
            ("sales_team", [], ["data_product_read"]),
            ("pets_lead", ["pets_team"], []),  # a child of the owner role
            ("reader", [], ["data_product_read"]),  # R only
            ("product_admin", [], ["data_product_read", "data_product_rw"]),  # RW of its own
            ("curator", [], ["glossary_read", "glossary_rw"]),
        ):
            await conn.execute_core(
                insert(roles).values(
                    id=role, capabilities=capabilities, domain_access=["*"], parents=parents
                )
            )
        for product, owner in (("pets_p", "pets_team"), ("sales_p", "sales_team")):
            await conn.execute_core(
                insert(data_products).values(
                    id=product, domain_id="pets", name=product, owner_role=owner
                )
            )
    runtime = _Runtime(db)

    async def _pool():
        return db

    async def _refresh() -> None:
        return None

    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", runtime.roles, raising=False)
    monkeypatch.setattr(appmod.state, "role_chains", runtime.role_chains, raising=False)
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


def _request(role: str) -> Any:
    identity = types.SimpleNamespace(user_id="u1", roles=[role], acting_roles=[role])
    return types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id="acme", role=role)
    )


def _info(role: str) -> Any:
    return types.SimpleNamespace(context={"request": _request(role)})


M = schema_mutation.Mutation


def _input(product: str, name: str, owner: str | None) -> DataProductInput:
    return DataProductInput(id=product, domain_id="pets", name=name, owner_role=owner)


async def _update(role: str, product: str, name: str, owner: str | None) -> Any:
    token = current_acting_role.set(role)
    try:
        return await M().update_data_product(_info(role), _input(product, name, owner))
    finally:
        current_acting_role.reset(token)


async def _create(role: str, product: str, owner: str | None) -> Any:
    token = current_acting_role.set(role)
    try:
        return await M().create_data_product(_info(role), _input(product, product, owner))
    finally:
        current_acting_role.reset(token)


async def _delete(role: str, product: str) -> Any:
    token = current_acting_role.set(role)
    try:
        return await M().delete_data_product(_info(role), product)
    finally:
        current_acting_role.reset(token)


async def _name(plane, product: str) -> str | None:
    async with plane.db.acquire() as conn:
        row = await data_product_repo.get(conn, product)
    return None if row is None else row["name"]


def _holds_rw(plane, role: str) -> bool:
    return "data_product_rw" in plane.roles[role]["capabilities"]


# --- the scenario --------------------------------------------------------------------------------


async def test_a_named_owner_edits_its_product_and_no_other(plane):
    assert (await _update("pets_team", "pets_p", "renamed", "pets_team")).success
    assert await _name(plane, "pets_p") == "renamed"

    with pytest.raises(ModelRowRefused, match="pets_team"):
        await _update("pets_team", "sales_p", "stolen", "sales_team")
    assert await _name(plane, "sales_p") == "sales_p"


async def test_an_org_admin_with_no_rule_edits_either_product(plane):
    assert (await _update("org_admin", "pets_p", "one", "pets_team")).success
    assert (await _update("org_admin", "sales_p", "two", "sales_team")).success
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
        await _update("reader", "pets_p", "no", "pets_team")
    assert await _name(plane, "pets_p") == "pets_p"


async def test_a_child_of_the_owner_role_edits_what_the_parent_owns_and_nothing_else(plane):
    assert (await _update("pets_lead", "pets_p", "by the lead", "pets_team")).success
    assert await _name(plane, "pets_p") == "by the lead"

    with pytest.raises(ModelRowRefused, match="pets_lead"):
        await _update("pets_lead", "sales_p", "stolen", "sales_team")
    assert await _name(plane, "sales_p") == "sales_p"


async def test_an_owner_deletes_and_publishes_its_own_product_only(plane):
    token = current_acting_role.set("pets_team")
    try:
        assert (await M().publish_data_product(_info("pets_team"), "pets_p")).success
        with pytest.raises(ModelRowRefused):
            await M().publish_data_product(_info("pets_team"), "sales_p")
    finally:
        current_acting_role.reset(token)
    with pytest.raises(ModelRowRefused):
        await _delete("pets_team", "sales_p")
    assert (await _delete("pets_team", "pets_p")).success
    assert await _name(plane, "pets_p") is None
    assert await _name(plane, "sales_p") == "sales_p"


async def test_the_mcp_tool_is_held_to_the_same_rule(plane):
    from provisa.api.mcp import tools

    request = _request("pets_team")
    state = types.SimpleNamespace(
        model_db=plane.db, roles=plane.roles, meta_roles={}, role_chains=plane.role_chains
    )
    token = current_acting_role.set("pets_team")
    try:
        await tools.create_data_product(
            state, "pets_team", request, "pets_p", "pets", "by mcp", owner_role="pets_team"
        )
        with pytest.raises(ModelRowRefused):
            await tools.create_data_product(
                state, "pets_team", request, "sales_p", "pets", "stolen", owner_role="sales_team"
            )
    finally:
        current_acting_role.reset(token)
    assert await _name(plane, "pets_p") == "by mcp"
    assert await _name(plane, "sales_p") == "sales_p"


# --- the Owner field is how the grant changes ------------------------------------------------------


async def test_naming_a_role_gives_it_the_grant_and_the_rules(plane):
    assert not _holds_rw(plane, "reader")
    assert (await _update("org_admin", "pets_p", "pets_p", "reader")).success
    assert _holds_rw(plane, "reader")
    rules = plane.role_build_inputs["model_write_rules"]
    assert rules[("reader", "data_products", "U")] == OWNER_RULE
    assert rules[("reader", "data_products", "D")] == OWNER_RULE


async def test_a_role_that_owns_no_product_any_more_loses_the_grant(plane):
    assert _holds_rw(plane, "pets_team")
    assert (await _update("org_admin", "pets_p", "pets_p", "sales_team")).success
    assert not _holds_rw(plane, "pets_team")
    assert not _holds_rw(plane, "pets_lead")
    with pytest.raises(PermissionError, match="data_product_rw"):
        await _update("pets_team", "pets_p", "no", "pets_team")


async def test_a_role_keeps_the_grant_while_it_owns_another_product(plane):
    await _create("org_admin", "pets_q", "pets_team")
    assert (await _delete("org_admin", "pets_p")).success
    assert _holds_rw(plane, "pets_team")
    assert (await _delete("org_admin", "pets_q")).success
    assert not _holds_rw(plane, "pets_team")


async def test_a_save_that_leaves_the_owner_builds_nothing(plane):
    before = plane.builds
    assert (await _update("pets_team", "pets_p", "renamed", "pets_team")).success
    assert plane.builds == before


async def test_handing_a_product_to_another_role_ends_the_old_owners_edit(plane):
    assert (await _update("pets_team", "pets_p", "handed", "sales_team")).success
    assert not _holds_rw(plane, "pets_team")
    assert (await _update("sales_team", "pets_p", "taken", "sales_team")).success
    assert await _name(plane, "pets_p") == "taken"


async def test_rw_includes_create_which_has_no_row_to_narrow(plane):
    assert (await _create("pets_team", "pets_new", "pets_team")).success
    assert await _name(plane, "pets_new") == "pets_new"


async def test_a_rule_written_by_hand_does_not_replace_the_owners_rule(plane):
    async with plane.db.acquire() as conn:
        await rls_repo.upsert(
            conn,
            RLSRule(
                table_id="data_products",
                role_id="pets_team",
                filter="1 = 1",
                description="an attempt to widen it",
                rights=["U", "D"],
            ),
        )
    await plane.rebuild()
    with pytest.raises(ModelRowRefused):
        await _update("pets_team", "sales_p", "stolen", "sales_team")


async def test_no_generated_rule_is_stored_for_security_to_edit(plane):
    await _update("org_admin", "pets_p", "pets_p", "reader")
    async with plane.db.acquire() as conn:
        assert await rls_repo.list_all(conn) == []
        stored = (
            await conn.execute_core(select(roles.c.capabilities).where(roles.c.id == "reader"))
        ).scalar_one()
    assert "data_product_rw" not in stored


async def test_the_roles_listing_shows_what_the_owner_field_gives(plane):
    org = current_org.set("acme")
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="u1", roles=["org_admin"]),
            active_org_id="acme",
        ),
        app=types.SimpleNamespace(state=types.SimpleNamespace()),
    )
    try:
        listed = {r["id"]: r for r in await roles_router.list_roles(request)}
    finally:
        current_org.reset(org)
    assert "data_product_rw" in listed["pets_team"]["effectiveCapabilities"]
    assert "data_product_rw" not in listed["pets_team"]["capabilities"]
    assert "data_product_rw" in listed["pets_lead"]["effectiveCapabilities"]
    assert "data_product_rw" not in listed["reader"]["effectiveCapabilities"]


# --- an org admin may widen it ---------------------------------------------------------------------


async def test_naming_org_admin_does_not_narrow_it(plane):
    assert (await _update("org_admin", "pets_p", "pets_p", "org_admin")).success
    assert (await _update("org_admin", "sales_p", "still mine", "sales_team")).success
    assert await _name(plane, "sales_p") == "still mine"


async def test_org_admin_inherits_no_rule_from_a_parent_named_owner(plane):
    assert (await _update("org_admin", "pets_p", "pets_p", "developer")).success
    assert (await _update("org_admin", "pets_p", "pets_p", "data_product_owner")).success
    assert (await _update("org_admin", "sales_p", "still mine", "sales_team")).success
    assert await _name(plane, "sales_p") == "still mine"
    assert not any(k[0] == "org_admin" for k in plane.role_build_inputs["model_write_rules"])


async def test_a_role_with_rw_of_its_own_is_not_narrowed_by_being_named(plane):
    assert (await _update("product_admin", "sales_p", "any", "sales_team")).success
    await _update("org_admin", "pets_p", "pets_p", "product_admin")
    assert (await _update("product_admin", "sales_p", "still any", "sales_team")).success
    assert await _name(plane, "sales_p") == "still any"


async def test_a_role_with_rw_of_its_own_keeps_a_rule_an_org_admin_gave_it(plane):
    async with plane.db.acquire() as conn:
        await rls_repo.upsert(
            conn,
            RLSRule(
                table_id="data_products",
                role_id="product_admin",
                filter="owner_role = 'pets_team'",
                description="a data product administrator narrowed by hand",
                rights=["U"],
            ),
        )
    await plane.rebuild()
    assert (await _update("product_admin", "pets_p", "hers", "pets_team")).success
    with pytest.raises(ModelRowRefused):
        await _update("product_admin", "sales_p", "not hers", "sales_team")


async def test_a_child_holding_rw_of_its_own_is_not_narrowed_by_a_parent_named_owner():
    rows = [
        {"id": "team", "capabilities": [], "parents": []},
        {"id": "lead", "capabilities": ["data_product_rw"], "parents": ["team"]},
        {"id": "member", "capabilities": [], "parents": ["team"]},
    ]
    assert owner_held_roles(rows, {"team"}) == {"team", "member"}


def test_a_role_named_by_no_product_is_held_to_nothing():
    rows = [{"id": "team", "capabilities": [], "parents": []}]
    assert with_owner_grant(rows, set()) == rows


def test_the_grant_leaves_its_input_alone():
    row = {"id": "team", "capabilities": ["usage"], "parents": []}
    assert with_owner_grant([row], {"team"})[0]["capabilities"] == ["usage", "data_product_rw"]
    assert row == {"id": "team", "capabilities": ["usage"], "parents": []}


async def test_an_owners_edit_stays_within_its_reach(plane):
    plane.roles["pets_team"]["domain_access"] = ["sales"]
    identity = types.SimpleNamespace(
        user_id="u", acting_roles=["pets_team"], env_capabilities=set()
    )
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
            "physical_table_map": {"_terms": "_terms_ontology"},
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


async def test_a_set_of_two_owners_edits_what_either_owns(plane):
    plane.meta_roles["meta:pets_team+sales_team"] = ("pets_team", "sales_team")
    plane.roles["meta:pets_team+sales_team"] = {
        "id": "meta:pets_team+sales_team",
        "capabilities": ["data_product_rw"],
        "acting_chain": ["pets_team", "sales_team"],
    }
    rule = acting_model_rule(plane, "meta:pets_team+sales_team", "data_products", "U")
    assert rule is not None and "current_setting('provisa.acting_roles')" in rule.predicate
    assert rule.session_vars["acting_roles"] == "pets_team,sales_team"


async def test_a_set_with_a_member_holding_rw_of_its_own_is_not_narrowed(plane):
    plane.meta_roles["meta:pets_team+product_admin"] = ("pets_team", "product_admin")
    assert acting_model_rule(plane, "meta:pets_team+product_admin", "data_products", "U") is None


async def test_a_retired_products_owner_keeps_the_grant(plane):
    async with plane.db.acquire() as conn:
        await conn.execute_core(
            update(data_products).where(data_products.c.id == "pets_p").values(status="retired")
        )
    await plane.rebuild()
    assert _holds_rw(plane, "pets_team")
