# Copyright (c) 2026 Kenneth Stott
# Canary: 2bb9e8fa-78a0-4c0f-92f1-28c34a53fccd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1682: a role's configured session values are the constants underneath; the request's
bindings overlay them, and the node's own facts (region, org) are set over both."""

from __future__ import annotations

import types
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import insert, select

from provisa.core.models import Role, flatten_roles
from provisa.core.request_context import (
    ORG_VAR,
    REGION_VAR,
    current_org,
    reset_session_vars,
    session_vars_for,
    set_session_vars,
)
from provisa.pgwire._pipeline import _resolve_session_settings
from provisa.security.inheritance import flatten_role_dicts


def _role(**kw: Any) -> Role:
    return Role(
        id=kw.pop("id", "seller"),
        description="Test role.",
        capabilities=[],
        domain_access=["sales"],
        **kw,
    )


# --- the model ------------------------------------------------------------------------------------


def test_a_role_holds_session_values_by_their_one_spelling():
    assert _role().session_vars == {}
    assert _role(session_vars={"Tier-Level": "gold"}).session_vars == {"tier_level": "gold"}


@pytest.mark.parametrize("name", ["user_id", "role", "User-Id", REGION_VAR, ORG_VAR])
def test_a_name_the_request_or_the_node_binds_is_refused_by_name(name):
    with pytest.raises(ValidationError, match="cannot be a role's session value") as err:
        _role(session_vars={name: "x"})
    assert repr(name.lower().replace("-", "_")) in str(err.value)


@pytest.mark.parametrize(("name", "said"), [("1tier", "'1tier'"), ("ti er", "'ti er'")])
def test_a_name_a_predicate_cannot_spell_is_refused_by_name(name, said):
    with pytest.raises(ValidationError, match="letters, digits and underscores") as err:
        _role(session_vars={name: "x"})
    assert said in str(err.value)


def test_two_names_of_one_spelling_are_refused():
    with pytest.raises(ValidationError, match="'tier_level' is named twice"):
        _role(session_vars={"tier-level": "a", "tier_level": "b"})


# --- what a predicate reads -----------------------------------------------------------------------

_RULE = "tier = current_setting('provisa.tier')"


def test_a_constant_binds_the_predicate_when_the_request_binds_nothing():
    token = set_session_vars({})
    try:
        values = session_vars_for({"session_vars": {"tier": "gold"}})
    finally:
        reset_session_vars(token)
    assert _resolve_session_settings(_RULE, values, "postgres") == "tier = 'gold'"


def test_the_requests_binding_of_the_same_name_wins():
    token = set_session_vars({"tier": "platinum"})
    try:
        values = session_vars_for({"session_vars": {"tier": "gold"}})
    finally:
        reset_session_vars(token)
    assert _resolve_session_settings(_RULE, values, "postgres") == "tier = 'platinum'"


def test_the_nodes_region_and_org_are_set_over_any_constant(monkeypatch):
    """Configuring them is refused, and a role dict carrying them anyway (one built by hand) still
    resolves to the node's facts: session_vars_for sets them last."""
    from provisa.core import process_region

    monkeypatch.setattr(process_region, "_region", "eu")
    token = set_session_vars({})
    org_token = current_org.set("acme")
    try:
        values = session_vars_for({"session_vars": {REGION_VAR: "us", ORG_VAR: "other"}})
    finally:
        current_org.reset(org_token)
        reset_session_vars(token)
    assert (values[REGION_VAR], values[ORG_VAR]) == ("eu", "acme")


# --- inheritance ----------------------------------------------------------------------------------


def test_a_child_takes_its_ancestors_constants_under_its_own():
    rows = [
        {"id": "base", "parents": [], "session_vars": {"tier": "silver", "zone": "a"}},
        {"id": "mid", "parents": ["base"], "session_vars": {"tier": "gold"}},
        {"id": "leaf", "parents": ["mid"], "session_vars": {"desk": "7"}},
    ]
    flat = {r["id"]: r for r in flatten_role_dicts(rows)}
    assert flat["base"]["session_vars"] == {"tier": "silver", "zone": "a"}
    assert flat["mid"]["session_vars"] == {"tier": "gold", "zone": "a"}
    assert flat["leaf"]["session_vars"] == {"tier": "gold", "zone": "a", "desk": "7"}
    assert rows[2]["session_vars"] == {"desk": "7"}  # the input rows are not modified


def test_the_model_flattening_merges_constants_alike():
    flat = {
        r.id: r
        for r in flatten_roles(
            [
                _role(id="base", session_vars={"tier": "silver", "zone": "a"}),
                _role(id="leaf", parents=["base"], session_vars={"tier": "gold"}),
            ]
        )
    }
    assert flat["leaf"].session_vars == {"tier": "gold", "zone": "a"}


def test_a_meta_role_inlines_each_members_inherited_constants(monkeypatch):
    """A member's constants, inherited ones included (the build flattens them before the
    meta-role reads them), are written into that member's own term."""
    from provisa.compiler.rls import RLSContext
    from provisa.security.meta_role import ensure_meta_role
    from tests.role_state import RoleState

    monkeypatch.setattr(
        "provisa.api.app_loaders.register_role_surface",
        lambda state, role, rls: state.rls_contexts.__setitem__(role["id"], rls),
    )
    stored = [
        {
            "description": "Test role.",
            "id": "base",
            "capabilities": [],
            "domain_access": ["sales"],
            "parents": [],
            "max_rows": None,
            "rate_limit": None,
            "session_vars": {"tier": "gold"},
        },
        {
            "description": "Test role.",
            "id": "east",
            "capabilities": [],
            "domain_access": ["sales"],
            "parents": ["base"],
            "max_rows": None,
            "rate_limit": None,
            "session_vars": {},
        },
        {
            "description": "Test role.",
            "id": "west",
            "capabilities": [],
            "domain_access": ["sales"],
            "parents": [],
            "max_rows": None,
            "rate_limit": None,
            "session_vars": {},
        },
    ]
    roles = {r["id"]: r for r in flatten_role_dicts(stored)}
    tables = [{"id": 1, "source_id": "pg", "domain_id": "sales", "columns": []}]
    state = RoleState(
        roles=dict(roles),
        rls_contexts={
            "east": RLSContext(rules={1: _RULE}),
            "west": RLSContext(rules={1: _RULE}),
        },
        contexts={
            r: types.SimpleNamespace(tables={"1": types.SimpleNamespace(table_id=1)})
            for r in ("east", "west")
        },
        masking_rules={},
        masked_columns=[],
        tracked_functions={},
        tracked_webhooks={},
        meta_roles={},
        meta_resolved_at={},
        role_build_inputs={
            "role_chains": {"base": ["base"], "east": ["east", "base"], "west": ["west"]},
            "tables": tables,
            "metrics": [],
            "functions": [],
            "webhooks": [],
        },
    )
    meta = ensure_meta_role(state, ["east", "west"])
    assert state.rls_contexts[meta].rules[1] == (
        "(tier = 'gold') OR (tier = current_setting('provisa.tier'))"
    )


# --- the configuration round trip ----------------------------------------------------------------


async def test_the_store_writes_a_roles_constants_into_the_configuration(monkeypatch):
    from unittest.mock import AsyncMock

    from provisa.core import store_config
    from provisa.core.repositories import role as role_repo

    row = {
        "description": "Test role.",
        "id": "seller",
        "capabilities": ["usage"],
        "domain_access": ["sales"],
        "session_vars": {"tier": "gold"},
    }
    monkeypatch.setattr(role_repo, "list_all", AsyncMock(return_value=[row]))
    written = await store_config._roles(object())  # type: ignore[arg-type]
    assert written[0]["session_vars"] == {"tier": "gold"}
    assert Role.model_validate(written[0]).session_vars == {"tier": "gold"}


async def test_a_saved_roles_constants_are_stored_and_read_back(control_plane):
    from provisa.core.repositories import role as role_repo

    db = control_plane.org()
    async with db.acquire() as conn:
        await role_repo.upsert(conn, _role(session_vars={"tier": "gold"}), org_id=None)
        assert (await role_repo.get(conn, "seller"))["session_vars"] == {"tier": "gold"}
        await role_repo.upsert(conn, _role(), org_id=None)
        assert (await role_repo.get(conn, "seller"))["session_vars"] == {}


# --- the admin mutation ---------------------------------------------------------------------------


def _info(role_id: str) -> Any:
    identity = types.SimpleNamespace(user_id="u1", roles=[role_id], acting_roles=[role_id])
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id="acme")
    )
    return types.SimpleNamespace(context={"request": request})


@pytest.fixture
async def plane(monkeypatch, control_plane):
    import provisa.api.app as appmod
    from provisa.api.admin import schema_mutation
    from provisa.core.schema_org import domains

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(domains).values(id="sales"))
    admin = {
        "id": "admin",
        "description": "Test role.",
        "capabilities": ["user_management"],
        "domain_access": ["*"],
    }
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", {"admin": admin}, raising=False)
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)

    async def _pool():
        return db

    async def _rebuild() -> None:
        return None

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    return db


async def _save(session_vars: list) -> Any:
    from provisa.api.admin import schema_mutation
    from provisa.api.admin.types import RoleInput, SessionVarInput

    return await schema_mutation.Mutation().create_role(
        _info("admin"),  # type: ignore[arg-type]
        RoleInput(
            id="seller",
            description="Test role.",
            capabilities=["usage"],
            domain_access=["sales"],
            session_vars=[SessionVarInput(name=n, value=v) for n, v in session_vars],
        ),
    )


async def _stored(db) -> dict | None:
    from provisa.core.schema_org import roles

    async with db.acquire() as conn:
        row = (await conn.execute_core(select(roles).where(roles.c.id == "seller"))).fetchone()
    return None if row is None else dict(row._mapping)


async def test_the_mutation_saves_a_roles_constants(plane):
    result = await _save([("Tier", "gold")])
    assert result.success is True
    assert (await _stored(plane))["session_vars"] == {"tier": "gold"}


@pytest.mark.parametrize(
    ("name", "code"),
    [
        ("user_id", "schema.role_session_var_name_reserved"),
        (REGION_VAR, "schema.role_session_var_name_reserved"),
        (ORG_VAR, "schema.role_session_var_name_reserved"),
        ("9lives", "schema.role_session_var_name_invalid"),
        ("", "schema.role_session_var_name_missing"),
    ],
)
async def test_the_mutation_refuses_a_name_by_name_and_writes_nothing(plane, name, code):
    result = await _save([(name, "x")])
    assert (result.success, result.code, result.params) == (
        False,
        code,
        {"role": "seller", "name": name},
    )
    assert await _stored(plane) is None


async def test_the_mutation_refuses_a_name_given_twice(plane):
    result = await _save([("tier", "a"), ("Tier", "b")])
    assert (result.success, result.code, result.params) == (
        False,
        "schema.role_session_var_name_repeated",
        {"role": "seller", "name": "tier"},
    )
    assert await _stored(plane) is None


def test_the_role_type_carries_its_constants_as_name_value_pairs():
    from provisa.api.admin._row_mappers import _role_from_row

    out = _role_from_row(
        {
            "description": "Test role.",
            "id": "seller",
            "capabilities": [],
            "domain_access": ["sales"],
            "demonstrated": [],
            "residency_values": [],
            "session_vars": {"zone": "a", "tier": "gold"},
            "inactivity_days": None,  # REQ-2077: a stored role always carries it
            "parents": [],  # REQ-2155: a stored role always carries it
        }
    )
    assert [(p.name, p.value) for p in out.session_vars] == [("tier", "gold"), ("zone", "a")]
    assert (
        _role_from_row({"id": "seller", "description": "Test role."}, detail=False).session_vars
        is None
    )


@pytest.mark.parametrize("value", ["${env:PROVISA_ZONE_CODE}", "${secret:zone_code}"])
async def test_the_mutation_saves_a_reference_as_written(plane, value):
    # REQ-2086: the model keeps the reference; it is resolved when a query uses it.
    result = await _save([("zone", value)])
    assert result.success is True
    assert (await _stored(plane))["session_vars"] == {"zone": value}
