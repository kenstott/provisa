# Copyright (c) 2026 Kenneth Stott
# Canary: 1ba06cb2-bace-42e7-9191-bbdd03b58d99
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every command and webhook sits in a domain (REQ-1531).

Saving one with an empty domain is refused by every path that saves one: the REST create and
update, and the model store's own upserts, which the config loader and the remote registrations
use. The caller must reach the domain it saves into, and on an update the domain it moves the
command out of. A stored row that names no domain is shown to no role: the same fail-closed
reading as a role that lists no domain.
"""

# Requirements: REQ-1531, REQ-1530

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.api.admin import actions_router
from provisa.api.errors import ApiError
from provisa.compiler.actions_schema import _build_action_fields
from provisa.compiler.schema_types import SchemaInput
from provisa.core import domain_policy
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.models import Function, Webhook
from provisa.core.repositories import function as function_repo
from provisa.core.schema_org import domains, sources, tracked_functions, tracked_webhooks

RIGHTS = ["table_registration"]
ROLES = {
    "sales_admin": {"id": "sales_admin", "capabilities": RIGHTS, "domain_access": ["sales"]},
    "everywhere": {"id": "everywhere", "capabilities": RIGHTS, "domain_access": ["*"]},
}


@pytest.fixture
async def plane(monkeypatch) -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="command-domain-test")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql", origin="admin"))
        for domain_id in ("sales", "finance"):
            await conn.execute_core(insert(domains).values(id=domain_id, origin="admin"))
        for table in (tracked_functions, tracked_webhooks):
            await conn.execute_core(
                insert(table).values(origin="admin", name="in_finance", domain_id="finance")
            )
    monkeypatch.setattr(appmod.state, "tenant_db", db, raising=False)
    monkeypatch.setattr(appmod.state, "roles", ROLES, raising=False)
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)

    async def _rebuild() -> None:
        return None

    monkeypatch.setattr(appmod, "_rebuild_schemas", _rebuild)
    return db


def _request(role_id: str | None) -> Any:
    identity = None if role_id is None else types.SimpleNamespace(user_id="u1", roles=[role_id])
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=identity, active_org_id="a"))


async def _domain_of(db: Database, table, name: str) -> str | None:
    async with db.acquire() as conn:
        row = (
            await conn.execute_core(select(table.c.domain_id).where(table.c.name == name))
        ).fetchone()
    return None if row is None else row[0]


# --- the rule ------------------------------------------------------------------------------------


def test_an_empty_domain_is_refused_and_a_named_one_is_kept(monkeypatch):
    assert domain_policy.command_domain_id("sales", "refund") == "sales"
    for empty in ("", None):
        with pytest.raises(ValueError, match="'refund' names no domain"):
            domain_policy.command_domain_id(empty, "refund")


async def test_the_model_store_refuses_a_command_or_webhook_with_no_domain(plane):
    """The write path the config loader and the remote registrations use."""
    async with plane.acquire() as conn:
        with pytest.raises(ValueError, match="'refund' names no domain"):
            await function_repo.upsert_function(
                conn,
                Function(
                    name="refund", source_id="pg", function_name="refund", returns="", domain_id=""
                ),
                origin="admin",
            )
        with pytest.raises(ValueError, match="'notify' names no domain"):
            await function_repo.upsert_webhook(
                conn, Webhook(name="notify", url="http://x", domain_id=""), origin="admin"
            )
        await function_repo.upsert_function(
            conn,
            Function(
                name="refund", source_id="pg", function_name="refund", returns="", domain_id="sales"
            ),
            origin="admin",
        )
    assert await _domain_of(plane, tracked_functions, "refund") == "sales"
    assert await _domain_of(plane, tracked_webhooks, "notify") is None


# --- REST ----------------------------------------------------------------------------------------


def _function(name: str, domain: str) -> actions_router.FunctionInput:
    return actions_router.FunctionInput(
        name=name, sourceId="pg", functionName=name, domainId=domain
    )


def _webhook(name: str, domain: str) -> actions_router.WebhookInput:
    return actions_router.WebhookInput(name=name, url="http://x", domainId=domain)


SAVES = [
    pytest.param(
        lambda req, name, dom: actions_router.create_function(req, _function(name, dom)),
        lambda req, name, dom: actions_router.update_function(req, name, _function(name, dom)),
        tracked_functions,
        id="command",
    ),
    pytest.param(
        lambda req, name, dom: actions_router.create_webhook(req, _webhook(name, dom)),
        lambda req, name, dom: actions_router.update_webhook(req, name, _webhook(name, dom)),
        tracked_webhooks,
        id="webhook",
    ),
]


@pytest.mark.parametrize(("create", "update", "table"), SAVES)
async def test_creating_one_with_no_domain_is_refused(plane, create, update, table):
    for caller in ("everywhere", None):  # None: no auth provider — the rule is not a gate
        with pytest.raises(ApiError) as err:
            await create(_request(caller), "new_one", "")
        assert (err.value.status_code, err.value.code) == (422, "actions.domain_required")
    assert await _domain_of(plane, table, "new_one") is None


@pytest.mark.parametrize(("create", "update", "table"), SAVES)
async def test_updating_one_to_no_domain_is_refused(plane, create, update, table):
    with pytest.raises(ApiError) as err:
        await update(_request("everywhere"), "in_finance", "")
    assert (err.value.status_code, err.value.code) == (422, "actions.domain_required")
    assert await _domain_of(plane, table, "in_finance") == "finance"


@pytest.mark.parametrize(("create", "update", "table"), SAVES)
async def test_the_caller_reaches_the_domain_it_saves_into(plane, create, update, table):
    with pytest.raises(ApiError) as err:
        await create(_request("sales_admin"), "new_one", "finance")
    assert (err.value.status_code, err.value.code) == (403, "auth.domain_denied")
    assert await _domain_of(plane, table, "new_one") is None


@pytest.mark.parametrize(("create", "update", "table"), SAVES)
async def test_moving_one_needs_the_domain_it_is_moved_out_of(plane, create, update, table):
    with pytest.raises(ApiError) as err:
        await update(_request("sales_admin"), "in_finance", "sales")
    assert (err.value.status_code, err.value.code) == (403, "auth.domain_denied")
    assert await _domain_of(plane, table, "in_finance") == "finance"

    await update(_request("everywhere"), "in_finance", "sales")
    assert await _domain_of(plane, table, "in_finance") == "sales"


# --- the schema build: a stored row with no domain is shown to nobody ----------------------------


def _fields(role: dict, items: list[dict]) -> set[str]:
    si = SchemaInput(
        tables=[],
        relationships=[],
        column_types={},
        naming_rules=[],
        role=role,
        domains=[{"id": "sales"}, {"id": "finance"}],
        domain_prefix=False,
        functions=items,
        webhooks=[],
    )
    query, mutation = _build_action_fields(si, {}, [])
    return set(query) | set(mutation)


def _command(name: str, domain: str) -> dict:
    return {
        "name": name,
        "function_name": name,
        "returns": "",
        "arguments": [],
        "visible_to": [],
        "writable_by": [],
        "domain_id": domain,
        "kind": "mutation",
    }


@pytest.mark.parametrize("access", [["sales"], ["*"]], ids=["one-domain", "all-domains"])
def test_a_stored_command_with_no_domain_is_shown_to_no_role(access):
    role = {"id": "r", "capabilities": ["usage"], "domain_access": access}
    items = [_command("in_sales", "sales"), _command("in_none", "")]
    shown = _fields(role, items)
    assert any("in_sales" in f or "inSales" in f for f in shown), shown
    assert not any("in_none" in f or "inNone" in f for f in shown), shown


# --- an OpenAPI registration whose spec declares commands ----------------------------------------


def _spec_with_a_mutation() -> dict:
    ok = {
        "200": {
            "description": "ok",
            "content": {"application/json": {"schema": {"type": "object"}}},
        }
    }
    listing = {
        "200": {
            "description": "ok",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {"type": "object", "properties": {"id": {"type": "integer"}}},
                    }
                }
            },
        }
    }
    return {
        "openapi": "3.0.0",
        "info": {"title": "t", "version": "1"},
        "paths": {
            "/pets": {
                "get": {"operationId": "listPets", "responses": listing},
                "post": {"operationId": "createPet", "responses": ok},
            }
        },
    }


async def test_a_registration_with_commands_and_no_domain_is_refused_before_anything_is_written(
    plane,
):
    from provisa.core.schema_org import registered_tables
    from provisa.openapi.register import CommandsNeedDomain, auto_register_openapi_source

    async with plane.acquire() as conn:
        with pytest.raises(CommandsNeedDomain) as err:
            await auto_register_openapi_source("pg", _spec_with_a_mutation(), conn, "")
        assert (err.value.source_id, err.value.commands) == ("pg", 1)
        assert "declares 1 command(s) and names no domain" in str(err.value)
        # Not half-registered: neither its table nor its command landed.
        assert (await conn.execute_core(select(registered_tables.c.id))).fetchall() == []
        assert await _domain_of(plane, tracked_functions, "create_pet") is None

        tables, commands, _ = await auto_register_openapi_source(
            "pg", _spec_with_a_mutation(), conn, "sales", base_url="http://x"
        )
    assert (tables, commands) == (1, 1)
