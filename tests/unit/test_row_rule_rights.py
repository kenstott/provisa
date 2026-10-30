# Copyright (c) 2026 Kenneth Stott
# Canary: 3e9c1f7a-6b42-4d18-a0c5-7f2e8d1b6a94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A row rule has a right: R, U or D, one of each per role and model (REQ-2188).

R or RW on a model is the ceiling. A rule only narrows: it gives no right the role does not hold,
and a role with no rule for a right is not narrowed. Reads read R rules alone; a data write ANDs
the role's U or D rule in beside its grant; the model store applies them to every write of a
semantic object, by whatever route it came, and a write refused so changes nothing."""

# Requirements: REQ-2188, REQ-2074, REQ-1919, REQ-041

from __future__ import annotations

import types

import pytest
from sqlalchemy import insert, select, update

from provisa.auth.attributes import session_constant_problem
from provisa.auth.derived_roles import principal_session_vars
from provisa.compiler.rls import build_rls_context
from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.compiler.write_admission import WriteNotAdmitted
from provisa.core import model_row_rules
from provisa.core.model_row_rules import ModelRowRefused, ModelRule
from provisa.core.models import DataProduct, RLSRule
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import rls as rls_repo
from provisa.core.request_context import current_acting_role
from provisa.core.schema_org import (
    data_products,
    domains,
    registered_tables,
    rls_rules,
    roles,
    sources,
)
from provisa.security.inheritance import materialize_rls
from provisa.security.row_filters import (
    acting_model_rule,
    model_write_rules,
    resolve_rules_by_right,
    resolve_table_rules,
)

OWNS = "owner_role IN (current_setting('provisa.acting_roles'))"
_ORDERS = 1


# --- the column ----------------------------------------------------------------------------------


def _rule(role="sales", filter_expr="region = 'eu'", **fields):
    return {"role_id": role, "table_id": 1, "filter_expr": filter_expr, **fields}


def test_a_rule_saved_with_no_right_is_an_r_rule():
    assert RLSRule(table_id="orders", role_id="r", filter="1 = 1", description="d").right == "R"


def test_a_rule_refuses_a_right_that_is_not_r_u_or_d():
    with pytest.raises(ValueError, match="right"):
        RLSRule(table_id="orders", role_id="r", filter="1 = 1", description="d", right="X")


def test_an_action_rule_has_no_row_to_update():
    with pytest.raises(ValueError, match="action"):
        RLSRule(action_name="a", role_id="r", filter="1 = 1", description="d", right="U")


# --- reads ---------------------------------------------------------------------------------------


def test_read_compilation_reads_only_r_rules():
    rules = [
        _rule(right="R"),
        _rule(filter_expr="region = 'u'", right="U"),
        _rule(filter_expr="region = 'd'", right="D"),
    ]
    ctx = build_rls_context(rules, "sales")
    assert ctx.rules == {1: "region = 'eu'"}
    assert ctx.write_rules == {"update": {1: "region = 'u'"}, "delete": {1: "region = 'd'"}}


def test_a_rule_row_with_no_right_stays_a_read_rule():
    assert build_rls_context([_rule()], "sales").rules == {1: "region = 'eu'"}


def test_a_u_rule_alone_narrows_no_read():
    assert build_rls_context([_rule(right="U")], "sales").rules == {}


# --- reach: inheritance, domain and tag, per right -----------------------------------------------


_TABLES = [{"id": 1, "domain_id": "sales", "source_id": "pg", "table_name": "orders"}]


def test_each_right_resolves_on_its_own():
    rules = [
        {"role_id": "a", "table_id": 1, "filter_expr": "x = 1", "right": "R"},
        {"role_id": "a", "table_id": 1, "filter_expr": "x = 2", "right": "U"},
        {"role_id": "a", "domain_id": "sales", "filter_expr": "x = 3", "right": "D"},
    ]
    out = resolve_rules_by_right(rules, _TABLES, {"a": []}, [], {"a": ["a"]}, [], {})
    by_right = {r["right"]: r["filter_expr"] for r in out if r.get("table_id") == 1}
    assert by_right == {"R": "x = 1", "U": "x = 2", "D": "x = 3"}


def test_a_domain_u_rule_reaches_the_tables_of_the_domain_as_an_r_rule_does():
    rules = [{"role_id": "a", "domain_id": "sales", "filter_expr": "x = 3", "right": "U"}]
    out = resolve_rules_by_right(rules, _TABLES, {"a": []}, [], {"a": ["a"]}, [], {})
    ctx = build_rls_context(out, "a")
    assert ctx.write_rules["update"] == {1: "x = 3"}
    assert ctx.rules == {}


def test_a_tag_d_rule_reaches_the_tables_carrying_the_tag():
    rules = [{"role_id": "a", "tag_id": "regional", "filter_expr": "x = 4", "right": "D"}]
    tagged = [{"object_type": "table", "table_id": 1, "base_tag_id": "regional"}]
    out = resolve_rules_by_right(rules, _TABLES, {"a": []}, [], {"a": ["a"]}, tagged, {})
    assert build_rls_context(out, "a").write_rules["delete"] == {1: "x = 4"}


def test_a_child_inherits_its_parents_u_rule_and_not_as_a_read_rule():
    rules = [{"role_id": "p", "table_id": 1, "filter_expr": "x = 5", "right": "U"}]
    chains = {"p": ["p"], "c": ["c", "p"]}
    out = resolve_rules_by_right(rules, _TABLES, {"p": [], "c": ["p"]}, [], chains, [], {})
    ctx = build_rls_context(out, "c")
    assert ctx.write_rules["update"] == {1: "x = 5"}
    assert ctx.rules == {}


def test_folding_only_r_rules_is_unchanged_from_before_rights():
    rules = [
        {"role_id": "a", "table_id": 1, "filter_expr": "x = 1"},
        {"role_id": "a", "domain_id": "sales", "filter_expr": "y = 1"},
    ]
    before = resolve_table_rules(
        materialize_rls(rules, _TABLES, {"a": []}), _TABLES, {"a": ["a"]}, [], {}
    )
    after = resolve_rules_by_right(rules, _TABLES, {"a": []}, [], {"a": ["a"]}, [], {})
    assert [r["filter_expr"] for r in after] == [r["filter_expr"] for r in before]


# --- data writes ---------------------------------------------------------------------------------


_COLUMNS = [("id", "integer"), ("region", "varchar"), ("owner_role", "varchar")]


def _g(operation, columns=("region",), rows=None):
    return {
        "role": "sales",
        "operation": operation,
        "columns": list(columns),
        "rows": rows,
        "check": None,
        "presets": [],
    }


def _gov(*grants, rls=None, update=None, delete=None):
    gov = GovernanceContext()
    gov.role_id = "sales"
    gov.can_write = True
    gov.table_map = {"public.orders": _ORDERS, "orders": _ORDERS}
    gov.all_columns = {_ORDERS: list(_COLUMNS)}
    gov.write_grants = {_ORDERS: {g["operation"]: g for g in grants}}
    gov.rls_rules = {_ORDERS: rls} if rls else {}
    gov.write_rules = {
        "update": {_ORDERS: update} if update else {},
        "delete": {_ORDERS: delete} if delete else {},
    }
    gov.write_ops = {_ORDERS: frozenset({"insert", "update", "delete"})}
    gov.write_returns_rows = {_ORDERS: True}
    gov.write_refused_forms = {_ORDERS: frozenset()}
    return gov


def test_a_u_rule_is_anded_into_an_update_beside_the_grant():
    sql = apply_governance(
        "UPDATE public.orders SET region = 'x' WHERE id = 1",
        _gov(_g("update", rows="id > 0"), update="region = 'eu'"),
    )
    assert '"id" > 0' in sql and "\"region\" = 'eu'" in sql


def test_a_u_rule_leaves_a_grant_that_states_no_rows_taking_the_read_filter():
    sql = apply_governance(
        "UPDATE public.orders SET region = 'x' WHERE id = 1",
        _gov(_g("update"), rls="id > 5", update="region = 'eu'"),
    )
    assert '"id" > 5' in sql and "'eu'" in sql


def test_a_d_rule_is_anded_into_a_delete():
    sql = apply_governance(
        "DELETE FROM public.orders WHERE id = 1",
        _gov(_g("delete", rows="TRUE"), delete="region = 'eu'"),
    )
    assert "'eu'" in sql


def test_a_u_rule_does_not_touch_a_delete_nor_a_d_rule_an_update():
    delete = apply_governance(
        "DELETE FROM public.orders WHERE id = 1",
        _gov(_g("delete", rows="TRUE"), update="region = 'eu'"),
    )
    assert "'eu'" not in delete
    written = apply_governance(
        "UPDATE public.orders SET region = 'x' WHERE id = 1",
        _gov(_g("update", rows="TRUE"), delete="region = 'eu'"),
    )
    assert "'eu'" not in written


def test_a_role_with_only_r_gets_nothing_from_a_u_or_d_rule():
    gov = _gov(update="region = 'eu'", delete="region = 'eu'")
    with pytest.raises(WriteNotAdmitted, match="holds no update grant"):
        apply_governance("UPDATE public.orders SET region = 'x' WHERE id = 1", gov)
    with pytest.raises(WriteNotAdmitted, match="holds no delete grant"):
        apply_governance("DELETE FROM public.orders WHERE id = 1", gov)


def test_a_truncate_cannot_carry_a_d_rule():
    with pytest.raises(WriteNotAdmitted, match="Use DELETE"):
        apply_governance("TRUNCATE public.orders", _gov(_g("delete", rows="TRUE"), delete="id = 1"))


def test_a_merge_cannot_carry_a_u_rule():
    every = [c for c, _ in _COLUMNS]
    gov = _gov(_g("insert", every, rows="TRUE"), _g("update", every, rows="TRUE"), update="id = 1")
    gov.write_grants[_ORDERS]["insert"]["check"] = "TRUE"
    gov.write_grants[_ORDERS]["update"]["check"] = "TRUE"
    with pytest.raises(WriteNotAdmitted, match="MERGE"):
        apply_governance(
            "MERGE INTO public.orders USING s ON public.orders.id = s.id "
            "WHEN MATCHED THEN UPDATE SET region = s.region",
            gov,
        )


def test_an_on_conflict_update_cannot_carry_a_u_rule():
    gov = _gov(
        _g("insert", ("id", "region")),
        _g("update", ("region",), rows="TRUE"),
        update="region = 'eu'",
    )
    gov.write_grants[_ORDERS]["update"]["check"] = "TRUE"
    with pytest.raises(WriteNotAdmitted, match="ON CONFLICT"):
        apply_governance(
            "INSERT INTO public.orders (id, region) VALUES (1, 'x') "
            "ON CONFLICT (id) DO UPDATE SET region = 'y'",
            gov,
        )


# --- the session variable ------------------------------------------------------------------------


def test_one_role_binds_itself_as_its_acting_roles():
    assert principal_session_vars("u", {}, "pets_team")["acting_roles"] == "pets_team"


def test_a_meta_role_binds_every_member():
    bound = principal_session_vars("u", {}, "meta:pets_team+sales_team")
    assert bound["acting_roles"] == "pets_team,sales_team"


def test_a_client_cannot_supply_the_acting_roles():
    bound = principal_session_vars("u", {"acting_roles": "org_admin"}, None)
    assert "acting_roles" not in bound


def test_acting_roles_cannot_be_a_roles_constant():
    assert session_constant_problem("acting_roles") is not None


def test_a_rule_reads_the_acting_roles_with_in():
    sql = apply_governance(
        "UPDATE public.orders SET region = 'x' WHERE id = 1",
        _gov(_g("update", rows="TRUE"), update=OWNS),
        session_vars={"acting_roles": "pets_team,sales_team"},
    )
    assert "IN (CAST('pets_team' AS VARCHAR), CAST('sales_team' AS VARCHAR))" in sql


def test_a_rule_with_no_acting_roles_bound_matches_nothing():
    sql = apply_governance(
        "UPDATE public.orders SET region = 'x' WHERE id = 1",
        _gov(_g("update", rows="TRUE"), update=OWNS),
        session_vars={},
    )
    # Left for the pipeline, which resolves an unbound variable to NULL (REQ-1682).
    assert "CURRENT_SETTING('provisa.acting_roles')" in sql


def test_acting_roles_in_a_read_rule_is_expanded_too():
    gov = _gov()
    gov.visible_columns = {_ORDERS: None}
    gov.rls_rules = {_ORDERS: "region IN (current_setting('provisa.acting_roles'))"}
    sql = apply_governance(
        "SELECT id FROM public.orders", gov, session_vars={"acting_roles": "a,b"}
    )
    assert "'a'" in sql and "'b'" in sql


# --- which rule the store applies ----------------------------------------------------------------


_META_TABLES = [
    {"id": 9, "domain_id": "meta", "source_id": "provisa-admin", "table_name": "data_products"},
    {"id": 2, "domain_id": "sales", "source_id": "pg", "table_name": "orders"},
]


def test_only_u_and_d_rules_on_model_tables_reach_the_store():
    resolved = [
        {"role_id": "a", "table_id": 9, "filter_expr": "r", "right": "R"},
        {"role_id": "a", "table_id": 9, "filter_expr": "u", "right": "U"},
        {"role_id": "a", "table_id": 2, "filter_expr": "d", "right": "D"},
    ]
    assert model_write_rules(resolved, _META_TABLES) == {("a", "data_products", "U"): "u"}


def _model_state(rules, role_capabilities):
    return types.SimpleNamespace(
        role_build_inputs={
            "model_write_rules": rules,
            "physical_table_map": {"data_products": "data_products_meta"},
        },
        meta_roles={"meta:a+b": ("a", "b")},
        roles={
            role: {"id": role, "capabilities": caps, "session_vars": {}}
            for role, caps in role_capabilities.items()
        },
    )


def test_a_role_with_a_rule_gets_it_with_the_view_its_columns_are_read_through():
    state = _model_state({("a", "data_products", "U"): OWNS}, {"a": ["data_product_rw"]})
    rule = acting_model_rule(state, "a", "data_products", "U")
    assert rule is not None and rule.predicate == OWNS and rule.view == "data_products_meta"


def test_a_role_with_no_rule_is_not_narrowed():
    state = _model_state({}, {"a": ["data_product_rw"]})
    assert acting_model_rule(state, "a", "data_products", "U") is None


def test_a_set_of_roles_ors_its_members_rules():
    state = _model_state(
        {("a", "data_products", "U"): "x = 1", ("b", "data_products", "U"): "y = 2"},
        {"a": ["data_product_rw"], "b": ["data_product_rw"]},
    )
    rule = acting_model_rule(state, "meta:a+b", "data_products", "U")
    assert rule is not None and "x = 1" in rule.predicate and "y = 2" in rule.predicate


def test_a_member_holding_rw_with_no_rule_lifts_the_set():
    state = _model_state(
        {("a", "data_products", "U"): "x = 1"},
        {"a": ["data_product_rw"], "b": ["data_product_rw"]},
    )
    assert acting_model_rule(state, "meta:a+b", "data_products", "U") is None


def test_a_member_with_only_r_lifts_nothing():
    state = _model_state(
        {("a", "data_products", "U"): "x = 1"},
        {"a": ["data_product_rw"], "b": ["data_product_read"]},
    )
    rule = acting_model_rule(state, "meta:a+b", "data_products", "U")
    assert rule is not None and rule.predicate == "(x = 1)"


# --- the model store -----------------------------------------------------------------------------


@pytest.fixture
async def plane(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="pets"))
        for role in ("pets_team", "sales_team", "rule_holder"):  # org_admin is seeded
            await conn.execute_core(
                insert(roles).values(id=role, capabilities=[], domain_access=["*"])
            )
        for product, owner in (("pets_p", "pets_team"), ("sales_p", "sales_team")):
            await conn.execute_core(
                insert(data_products).values(
                    id=product, domain_id="pets", name=product, owner_role=owner
                )
            )
    return db


@pytest.fixture
def rights(monkeypatch):
    """pets_team holds RW on data_products with U and D rules naming the product's owner role;
    org_admin holds RW with none."""
    held = {
        ("pets_team", "data_products", "U"): OWNS,
        ("pets_team", "data_products", "D"): OWNS,
    }

    def provider(role, table, right):
        predicate = held.get((role, table, right))
        if predicate is None:
            return None
        return ModelRule(predicate=predicate, session_vars={"acting_roles": role})

    monkeypatch.setattr(model_row_rules, "_provider", provider)


def _acting(role):
    return current_acting_role.set(role)


def _product(product, name):
    owner = "pets_team" if product == "pets_p" else "sales_team"
    return DataProduct(id=product, domain_id="pets", name=name, owner_role=owner)


async def _name(db, product):
    async with db.acquire() as conn:
        row = await data_product_repo.get(conn, product)
    return None if row is None else row["name"]


async def test_req_2188_scenario(plane, rights):
    token = _acting("pets_team")
    try:
        async with plane.acquire() as conn:
            await data_product_repo.upsert(conn, _product("pets_p", "pets renamed"))
            with pytest.raises(ModelRowRefused, match="pets_team") as refused:
                await data_product_repo.upsert(conn, _product("sales_p", "stolen"))
    finally:
        current_acting_role.reset(token)
    assert "data_products" in str(refused.value)
    assert await _name(plane, "pets_p") == "pets renamed"
    assert await _name(plane, "sales_p") == "sales_p"

    token = _acting("org_admin")
    try:
        async with plane.acquire() as conn:
            await data_product_repo.upsert(conn, _product("pets_p", "admin one"))
            await data_product_repo.upsert(conn, _product("sales_p", "admin two"))
    finally:
        current_acting_role.reset(token)
    assert await _name(plane, "pets_p") == "admin one"
    assert await _name(plane, "sales_p") == "admin two"


def test_req_2188_a_role_with_r_only_and_a_u_rule_is_refused_at_the_right():
    """The ceiling is the role's right: a U rule neither gives data_product_rw nor touches the
    gate that reads it."""
    from provisa.api.admin.capabilities import data_product_edit_refusal

    state = types.SimpleNamespace(
        roles={"rule_holder": {"capabilities": ["data_product_read"], "domain_access": ["*"]}},
        role_chains={"rule_holder": ["rule_holder"]},
    )
    identity = types.SimpleNamespace(
        user_id="u", acting_roles=["rule_holder"], env_capabilities=set()
    )
    stored = {"id": "p", "domain_id": "pets", "owner_role": "rule_holder"}
    assert data_product_edit_refusal(identity, state, stored, {"pets"}) is not None


async def test_a_refused_delete_leaves_the_product(plane, rights):
    token = _acting("pets_team")
    try:
        async with plane.acquire() as conn:
            with pytest.raises(ModelRowRefused, match="D rule"):
                await data_product_repo.delete(conn, "sales_p")
            assert await data_product_repo.delete(conn, "pets_p") is True
    finally:
        current_acting_role.reset(token)
    assert await _name(plane, "sales_p") == "sales_p"
    assert await _name(plane, "pets_p") is None


async def test_every_status_write_is_narrowed(plane, rights):
    token = _acting("pets_team")
    try:
        async with plane.acquire() as conn:
            with pytest.raises(ModelRowRefused):
                await data_product_repo.publish(conn, "sales_p", by="u")
            await data_product_repo.publish(conn, "pets_p", by="u")
    finally:
        current_acting_role.reset(token)
    async with plane.acquire() as conn:
        assert (await data_product_repo.get(conn, "sales_p"))["status"] == "draft"
        assert (await data_product_repo.get(conn, "pets_p"))["status"] == "live"


async def test_a_refused_retirement_moves_no_member(plane, rights):
    async with plane.acquire() as conn:
        await conn.execute_core(
            update(data_products).where(data_products.c.id == "sales_p").values(status="live")
        )
        await conn.execute_core(
            insert(data_products).values(
                id="sales_next",
                domain_id="pets",
                name="n",
                owner_role="sales_team",
                status="live",
            )
        )
        await conn.execute_core(
            insert(registered_tables).values(
                source_id="pg",
                domain_id="pets",
                schema_name="public",
                table_name="m",
                product_id="sales_p",
            )
        )
    token = _acting("pets_team")
    try:
        async with plane.acquire() as conn:
            with pytest.raises(ModelRowRefused):
                await data_product_repo.retire(
                    conn, "sales_p", reason="r", successor="sales_next", by="u"
                )
    finally:
        current_acting_role.reset(token)
    async with plane.acquire() as conn:
        assert await data_product_repo.member_count(conn, "sales_p") == 1


async def test_a_new_product_has_no_row_to_narrow(plane, rights):
    token = _acting("pets_team")
    try:
        async with plane.acquire() as conn:
            await data_product_repo.upsert(conn, _product("sales_new", "new"))
    finally:
        current_acting_role.reset(token)
    assert await _name(plane, "sales_new") == "new"


async def test_a_statement_touching_one_refused_row_changes_none(plane, rights):
    token = _acting("pets_team")
    try:
        async with plane.acquire() as conn:
            with pytest.raises(ModelRowRefused):
                await conn.execute_core(update(data_products).values(name="all"))
    finally:
        current_acting_role.reset(token)
    assert await _name(plane, "pets_p") == "pets_p"


async def test_a_write_with_no_row_is_no_refusal(plane, rights):
    token = _acting("pets_team")
    try:
        async with plane.acquire() as conn:
            result = await conn.execute_core(
                update(data_products).where(data_products.c.id == "nope").values(name="x")
            )
    finally:
        current_acting_role.reset(token)
    assert result.rowcount == 0


async def test_no_acting_person_is_not_narrowed(plane, rights):
    assert current_acting_role.get() is None
    async with plane.acquire() as conn:
        await data_product_repo.upsert(conn, _product("sales_p", "by the system"))
    assert await _name(plane, "sales_p") == "by the system"


async def test_config_load_is_not_narrowed_even_inside_a_request(plane, rights):
    token = _acting("pets_team")
    try:
        with model_row_rules.unnarrowed():
            async with plane.acquire() as conn:
                await data_product_repo.upsert(conn, _product("sales_p", "by config"))
    finally:
        current_acting_role.reset(token)
    assert await _name(plane, "sales_p") == "by config"


async def test_a_commit_of_a_whole_tree_is_not_narrowed(plane, rights):
    from provisa.core import model_change

    token = _acting("pets_team")
    try:
        async with model_change.committed_by_caller():
            async with plane.acquire() as conn:
                await data_product_repo.upsert(conn, _product("sales_p", "from the tree"))
    finally:
        current_acting_role.reset(token)
    assert await _name(plane, "sales_p") == "from the tree"


async def test_a_rule_column_only_the_view_holds_resolves_through_the_view(plane, monkeypatch):
    async with plane.acquire() as conn:
        await conn.execute(
            "CREATE VIEW data_products_meta AS "
            "SELECT id, owner_role, 'x' AS derived FROM data_products"
        )

    def provider(role, table, right):
        return ModelRule(
            predicate="derived = 'x' AND owner_role = 'pets_team'",
            session_vars={},
            view="data_products_meta",
        )

    monkeypatch.setattr(model_row_rules, "_provider", provider)
    token = _acting("pets_team")
    try:
        async with plane.acquire() as conn:
            await data_product_repo.upsert(conn, _product("pets_p", "through the view"))
            with pytest.raises(ModelRowRefused):
                await data_product_repo.upsert(conn, _product("sales_p", "no"))
    finally:
        current_acting_role.reset(token)
    assert await _name(plane, "pets_p") == "through the view"


async def test_a_rule_column_nothing_holds_is_refused_by_name(plane, monkeypatch):
    monkeypatch.setattr(
        model_row_rules,
        "_provider",
        lambda role, table, right: ModelRule(predicate="nowhere = 1", session_vars={}),
    )
    token = _acting("pets_team")
    try:
        async with plane.acquire() as conn:
            with pytest.raises(ModelRowRefused, match="nowhere"):
                await data_product_repo.upsert(conn, _product("pets_p", "x"))
    finally:
        current_acting_role.reset(token)
    assert await _name(plane, "pets_p") == "pets_p"


async def test_a_rule_with_a_literal_holding_a_colon_runs(plane, monkeypatch):
    monkeypatch.setattr(
        model_row_rules,
        "_provider",
        lambda role, table, right: ModelRule(
            predicate=OWNS, session_vars={"acting_roles": "meta:x+y,pets_team"}
        ),
    )
    token = _acting("pets_team")
    try:
        async with plane.acquire() as conn:
            await data_product_repo.upsert(conn, _product("pets_p", "colon"))
    finally:
        current_acting_role.reset(token)
    assert await _name(plane, "pets_p") == "colon"


# --- the rule store ------------------------------------------------------------------------------


async def test_one_rule_of_each_right_per_role_and_model(plane):
    async with plane.acquire() as conn:
        for right, expr in (("R", "1 = 1"), ("U", "2 = 2"), ("D", "3 = 3")):
            await rls_repo.upsert(
                conn,
                RLSRule(
                    domain_id="pets",
                    role_id="rule_holder",
                    filter=expr,
                    description="d",
                    right=right,
                ),
            )
        # The same right again replaces its own rule and leaves the others.
        await rls_repo.upsert(
            conn,
            RLSRule(
                domain_id="pets", role_id="rule_holder", filter="4 = 4", description="d", right="U"
            ),
        )
        rows = await rls_repo.list_for_role(conn, "rule_holder")
    assert {r["right"]: r["filter_expr"] for r in rows} == {
        "R": "1 = 1",
        "U": "4 = 4",
        "D": "3 = 3",
    }


async def test_deleting_a_rule_of_one_right_leaves_the_others(plane):
    async with plane.acquire() as conn:
        for right in ("R", "U"):
            await rls_repo.upsert(
                conn,
                RLSRule(
                    domain_id="pets",
                    role_id="rule_holder",
                    filter="1 = 1",
                    description="d",
                    right=right,
                ),
            )
        assert await rls_repo.delete(conn, "rule_holder", domain_id="pets", right="U") is True
        rows = (await conn.execute_core(select(rls_rules.c.right))).fetchall()
    assert [r[0] for r in rows] == ["R"]
