# Copyright (c) 2026 Kenneth Stott
# Canary: 9e4a1c7b-3d52-4f86-a0e9-6b18c2d4f735
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A governance right is exercised only in the domains of the roles that carry it (REQ-1944).

The standard data_steward role holds the governance rights and nothing operational. Every edit
those rights allow -- a row rule, a column grant, a mask, a sensitive tag, a fake on a sensitive
column, a data product, a command's reclassification -- is checked against the domain of what it
changes, and refused naming the domain when the steward's role does not reach it. A role is only a
collection of rights within domains: the same rights under another name behave identically.
"""

# Requirements: REQ-1944, REQ-1943, REQ-1531, REQ-870

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import schema_mutation
from provisa.api.errors import ApiError
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.schema_org import (
    data_products,
    domains,
    roles,
    sources,
    tag_assignments,
    tags,
    tracked_functions,
)

STEWARD_RIGHTS = [
    "access_config",
    "column_grant",
    "masking_config",
    "sensitive_data",
    "glossary_read",
    "glossary_rw",
    "data_product_read",
    "data_product_rw",
    "usage",
    "query_development",
    "full_results",
    "view_governance",
]


def _roles(steward_id: str) -> dict[str, dict]:
    return {
        # The steward under test: the seeded rights, copied to sales.
        steward_id: {"id": steward_id, "capabilities": STEWARD_RIGHTS, "domain_access": ["sales"]},
        # The seeded reach: every domain.
        "steward_everywhere": {
            "id": "steward_everywhere",
            "capabilities": STEWARD_RIGHTS,
            "domain_access": ["*"],
        },
        # A read-only role in finance: holding it beside the steward reaches finance for reading,
        # never for governing (the rights are paired with the domains of the roles carrying them).
        "finance_reader": {
            "id": "finance_reader",
            "capabilities": ["usage", "query_development"],
            "domain_access": ["finance"],
        },
        # A table editor reaching every domain but holding no governance right.
        "editor": {
            "id": "editor",
            "capabilities": ["table_registration"],
            "domain_access": ["*"],
        },
        "admin_sales": {
            "id": "admin_sales",
            "capabilities": ["user_management"],
            "domain_access": ["sales"],
        },
    }


# The seeded role id and another name with the same rights and domains: identical behaviour.
STEWARD_IDS = ["data_steward", "governance_lead"]


@pytest.fixture(params=STEWARD_IDS)
async def plane(request, monkeypatch):
    steward_id = request.param
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="steward-test")
    await _init_schema_portable(db)
    from provisa.core.models import Column, Table
    from provisa.core.repositories import table as table_repo

    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        for domain_id in ("sales", "finance"):
            await conn.execute_core(insert(domains).values(id=domain_id))
        for table_name, domain_id in (("orders", "sales"), ("invoices", "finance")):
            ids[table_name] = await table_repo.upsert(
                conn,
                Table(
                    source_id="pg",
                    domain_id=domain_id,
                    schema_name="public",
                    table_name=table_name,
                    governance="pre-approved",
                    columns=[
                        Column(name="id", visible_to=[], data_type="varchar"),
                        Column(name="email", visible_to=[], data_type="varchar"),
                        Column(name="amount", visible_to=[], data_type="integer"),
                    ],
                ),
            )
        await conn.execute_core(
            insert(tags).values(id="mnpi", applies_to=["column"], sensitive=True)
        )
        for table_name in ("orders", "invoices"):
            await conn.execute_core(
                insert(tag_assignments).values(
                    tag_id="pii",
                    base_tag_id="pii",
                    object_type="column",
                    table_id=ids[table_name],
                    column_name="email",
                    object_key=f"column:{ids[table_name]}:email",
                )
            )
        for name, domain_id in (("of_sales", "sales"), ("of_finance", "finance")):
            await conn.execute_core(
                insert(tracked_functions).values(name=name, domain_id=domain_id, kind="mutation")
            )
        for role_id, domain_access in (("sales_role", ["sales"]), ("finance_role", ["finance"])):
            await conn.execute_core(
                insert(roles).values(
                    id=role_id, capabilities=["usage"], domain_access=domain_access
                )
            )
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "model_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", _roles(steward_id), raising=False)
    from provisa.core.models import ProvisaConfig

    monkeypatch.setattr(
        appmod.state,
        "config",
        ProvisaConfig.model_validate({"sources": [], "domains": [], "tables": [], "roles": []}),
        raising=False,
    )
    from provisa.core import domain_policy

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)

    async def _pool():
        return db

    async def _rebuild() -> None:
        return None

    async def _refresh() -> None:
        return None

    from provisa.api.admin import schema_helpers

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_helpers, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(schema_mutation, "_refresh_config_tags", _refresh)
    monkeypatch.setattr(appmod, "_rebuild_schemas", _rebuild)
    return types.SimpleNamespace(db=db, ids=ids, steward=steward_id)


def _request(*role_ids: str) -> Any:
    identity = types.SimpleNamespace(user_id="u1", roles=list(role_ids))
    return types.SimpleNamespace(
        state=types.SimpleNamespace(identity=identity, active_org_id="acme")
    )


def _info(*role_ids: str) -> Any:
    return types.SimpleNamespace(context={"request": _request(*role_ids)})


M = schema_mutation.Mutation


# --- the paired helper ---------------------------------------------------------------------------


def test_a_right_reaches_only_the_domains_of_the_roles_carrying_it(plane):
    from provisa.api.admin.capabilities import right_domain_refusal

    identity = _request(plane.steward, "finance_reader").state.identity
    state = appmod.state
    assert right_domain_refusal(identity, state, "masking_config", {"sales"}) is None
    refusal = right_domain_refusal(identity, state, "masking_config", {"finance"})
    assert refusal is not None and "'finance'" in refusal
    assert "every domain" in (right_domain_refusal(identity, state, "sensitive_data", {"*"}) or "")
    everywhere = _request("steward_everywhere").state.identity
    assert right_domain_refusal(everywhere, state, "sensitive_data", {"*"}) is None


# --- row rules (masking_config) ------------------------------------------------------------------


def _rls(table_id: str) -> Any:
    from provisa.api.admin.types import RLSRuleInput

    return RLSRuleInput(table_id=table_id, role_id="sales_role", filter_expr="1 = 1")


async def test_a_row_rule_in_the_stewards_domain_is_saved(plane):
    result = await M().upsert_rls_rule(_info(plane.steward), _rls("orders"))
    assert result.success is True, result.message


async def test_a_row_rule_outside_the_stewards_domain_is_refused_by_name(plane):
    with pytest.raises(PermissionError, match="'finance'"):
        await M().upsert_rls_rule(_info(plane.steward, "finance_reader"), _rls("invoices"))


async def test_deleting_a_row_rule_outside_the_stewards_domain_is_refused(plane):
    with pytest.raises(PermissionError, match="'finance'"):
        await M().delete_rls_rule(
            _info(plane.steward), "sales_role", table_id=plane.ids["invoices"]
        )


# --- data products (data_product_rw) -------------------------------------------------------------


def _product(domain_id: str, product_id: str = "p1") -> Any:
    from provisa.api.admin.types import DataProductInput

    return DataProductInput(id=product_id, domain_id=domain_id, name=product_id)


async def _product_ids(db: Database) -> set[str]:
    async with db.acquire() as conn:
        return {r[0] for r in (await conn.execute_core(select(data_products.c.id))).fetchall()}


async def test_a_data_product_in_the_stewards_domain_is_created_and_deleted(plane):
    assert (await M().create_data_product(_info(plane.steward), _product("sales"))).success
    assert (await M().delete_data_product(_info(plane.steward), "p1")).success
    assert await _product_ids(plane.db) == set()


async def test_a_data_product_in_another_domain_is_refused(plane):
    with pytest.raises(PermissionError, match="'finance'"):
        await M().create_data_product(_info(plane.steward), _product("finance"))
    assert await _product_ids(plane.db) == set()


async def test_another_domains_product_can_be_neither_moved_in_nor_deleted(plane):
    assert (await M().create_data_product(_info("steward_everywhere"), _product("finance"))).success
    with pytest.raises(PermissionError, match="'finance'"):
        await M().create_data_product(_info(plane.steward), _product("sales"))
    with pytest.raises(PermissionError, match="'finance'"):
        await M().delete_data_product(_info(plane.steward), "p1")
    assert await _product_ids(plane.db) == {"p1"}


async def test_the_mcp_data_product_tools_are_scoped_alike(plane, monkeypatch):
    from provisa.api.mcp import tools

    # The role's built schema is not what this asks about; its rights and domains are.
    monkeypatch.setattr(tools, "require_role", lambda _role, _state: None)
    request = _request(plane.steward)
    with pytest.raises(ApiError, match="'finance'"):
        await tools.create_data_product(
            appmod.state, plane.steward, request, id="p2", domain_id="finance", name="p2"
        )
    assert (
        await tools.create_data_product(
            appmod.state, plane.steward, request, id="p2", domain_id="sales", name="p2"
        )
    )["id"] == "p2"
