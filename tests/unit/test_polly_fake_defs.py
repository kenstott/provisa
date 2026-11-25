# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e2c71-4d93-4a6f-9e10-c27a3f8d64b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly declares fakes for the columns a user names (REQ-1944, REQ-2053, REQ-1838).

A maintainer asked Polly to "create fake defs for: employee.contact, employees.contact,
get_user_by_name.email, get_user_by_name.phone, users.email, users.phone" on the standard demo;
propose_fakes and set_column_fake failed and the turn ended "(The model returned no answer)". The
demo's own tables (config/provisa.yaml: pet-store's users and get_user_by_name, shelter's employee
and employees, with their sources' settings) are registered here in a control plane of the test's
own, and the turn runs against it with a scripted model and an org admin's request.
"""

# Requirements: REQ-1944, REQ-2053, REQ-1838

from __future__ import annotations

import contextlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import yaml
from sqlalchemy import insert, select

from provisa.api.mcp import chat
from provisa.core import secrets_store
from tests.unit.gate_identity import serve_every_column
from tests.unit.test_mcp_chat import _block, _install_fake_anthropic

pytestmark = pytest.mark.asyncio

_ROOT = Path(__file__).resolve().parents[2]
_CONFIG = _ROOT / "config" / "provisa.yaml"
_TABLES = {
    ("pet-store", "users"),
    ("pet-store", "get_user_by_name"),
    ("shelter", "employees"),
    ("shelter", "employee"),
}
_SOURCES = {"petstore-api", "inquiries-sqlite", "graphql-demo"}


def _demo() -> dict:
    return yaml.safe_load(_CONFIG.read_text())


@pytest.fixture(autouse=True)
def no_vault(monkeypatch):
    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)


@pytest.fixture(autouse=True)
def _a_configured_vendor_key(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")


@pytest.fixture
async def demo(control_plane, monkeypatch):
    """The demo's four tables, registered as config/provisa.yaml declares them; an org admin."""
    from provisa.api.app import state
    from provisa.core.db import STANDARD_ROLE_RIGHTS
    from provisa.core.models import GraphQLRemoteConfig, Table
    from provisa.core.repositories import table as table_repo
    from provisa.core.schema_org import api_sources, domains, sources

    raw = _demo()
    db = control_plane.org()
    async with db.acquire() as conn:
        for domain in ("pet-store", "shelter"):
            await conn.execute_core(insert(domains).values(id=domain))
        for source in raw["sources"]:
            if source["id"] in _SOURCES:
                await conn.execute_core(
                    insert(sources).values(
                        id=source["id"], type=source["type"], cache_ttl=source.get("cache_ttl")
                    )
                )
        # The OpenAPI source as its registration stores it: the API source its endpoints hang on.
        await conn.execute_core(
            insert(api_sources).values(
                id="petstore-api", type="openapi", base_url="http://localhost:18080/api/v3"
            )
        )
        for table in raw["tables"]:
            if (table["domain_id"], table["table"]) in _TABLES:
                await table_repo.upsert(conn, Table.model_validate(table))
    monkeypatch.setattr(state, "model_db", db, raising=False)
    monkeypatch.setattr(
        state,
        "roles",
        {
            "org_admin": {
                "capabilities": sorted(STANDARD_ROLE_RIGHTS["org_admin"]),
                "domain_access": ["*"],
                "own_domain_access": ["*"],
            }
        },
        raising=False,
    )
    monkeypatch.setattr(
        state, "contexts", {"org_admin": SimpleNamespace(tables={}, joins={})}, raising=False
    )
    monkeypatch.setattr(
        state, "config", SimpleNamespace(graphql_remote=GraphQLRemoteConfig()), raising=False
    )
    # The petstore spec, loaded at boot from the demo's petstore server (demo/petstore_server):
    # a save of an OpenAPI table writes its endpoint from it.
    spec = json.loads((_ROOT / "demo" / "petstore_server" / "openapi.json").read_text())
    monkeypatch.setattr(
        state,
        "openapi_specs",
        {"petstore-api": {"spec": spec, "base_url": "http://localhost:18080/api/v3"}},
        raising=False,
    )
    monkeypatch.setattr("provisa.core.request_context.require_current_org", lambda: "default")
    # Which columns a caller is served is REQ-2152's own tests; the org admin reads them all.
    serve_every_column(monkeypatch)
    return state


def _request():
    identity = SimpleNamespace(user_id="u1", roles=["org_admin"], acting_roles=["org_admin"])
    return SimpleNamespace(
        headers={},
        state=SimpleNamespace(identity=identity, active_org_id="default", role="org_admin"),
    )


async def _tool(state, name: str, args: dict):
    return await chat._execute_tool(state, "org_admin", name, args, scope=None, request=_request())


async def _fakes(state) -> dict[str, str | None]:
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import table_columns as tc

    async with state.model_db.acquire() as conn:
        rows = (
            await conn.execute_core(
                select(rt.c.table_name, tc.c.column_name, tc.c.fake).join(
                    tc, tc.c.table_id == rt.c.id
                )
            )
        ).fetchall()
    return {f"{r.table_name}.{r.column_name}": r.fake for r in rows if r.fake is not None}


async def test_find_table_id_finds_each_named_table_and_get_user_by_name_is_one(demo):
    """get_user_by_name is a registered table (an OpenAPI operation registered as one, aliased
    user_by_name), not a command: found by its name or its alias in its domain's id."""
    by_name = await _tool(
        demo, "find_table_id", {"domain": "pet-store", "table": "get_user_by_name"}
    )
    by_alias = await _tool(demo, "find_table_id", {"domain": "pet-store", "table": "user_by_name"})
    assert [t["table"] for t in by_name] == ["get_user_by_name"]
    assert by_alias == by_name and by_name[0]["alias"] == "user_by_name"
    for domain, table in sorted(_TABLES):
        found = await _tool(demo, "find_table_id", {"domain": domain, "table": table})
        assert [t["table"] for t in found] == [table]


async def test_propose_fakes_on_an_unprofiled_demo_table_is_refused_by_name(demo):
    """What Polly met: no demo table has a profile run, so propose_fakes is refused; the guidance
    (test_mcp_model_tools) makes it no prerequisite for named columns."""
    [users] = await _tool(demo, "find_table_id", {"domain": "pet-store", "table": "users"})
    with pytest.raises(Exception) as refused:
        await _tool(demo, "propose_fakes", {"table_id": users["id"]})
    assert refused.value.code == "fakes.no_profile_run"
    assert "table 'users' has no succeeded profile run" in str(refused.value)


async def test_a_json_column_refuses_a_method_fake_and_its_family_says_so_first(demo):
    """employee.contact is jsonb (phone, email, ssn fields): its family is other, which no fake
    method fits; set_column_fake is refused naming the column, and a field of it is no column."""
    [employee] = await _tool(demo, "find_table_id", {"domain": "shelter", "table": "employee"})
    fakes = await _tool(demo, "get_table_fakes", {"table_id": employee["id"]})
    assert {c["column"]: c["family"] for c in fakes["columns"]}["contact"] == "other"
    kinds = await _tool(demo, "list_fake_kinds", {})
    assert not [m["name"] for m in kinds["methods"] if "other" in m["families"]]
    with pytest.raises(Exception) as refused:
        await _tool(
            demo,
            "set_column_fake",
            {"table_id": employee["id"], "column": "contact", "fake": "email()"},
        )
    assert refused.value.code == "schema.fake_refused"
    assert "employee.contact: email() makes str values" in str(refused.value)
    with pytest.raises(ValueError, match="table 'employee' has no column 'contact.email'"):
        await _tool(
            demo,
            "set_column_fake",
            {"table_id": employee["id"], "column": "contact.email", "fake": "email()"},
        )


def _round(*calls: tuple[str, dict]):
    return SimpleNamespace(
        content=[
            _block(type="tool_use", name=name, input=args, id=f"t{i}")
            for i, (name, args) in enumerate(calls)
        ],
        stop_reason="tool_use",
    )


async def test_the_whole_request_saves_four_fakes_and_states_the_two_refusals(demo, monkeypatch):
    """The maintainer's request, scripted: the model finds the four tables, reads the kinds, saves
    each named column's fake with set_column_fake, and then answers nothing. The four text columns
    are saved; the two jsonb contact columns are refused, and the turn ends naming that refusal
    and its reason instead of only "please ask again"."""
    ids = {}
    for domain, table in sorted(_TABLES):
        [found] = await _tool(demo, "find_table_id", {"domain": domain, "table": table})
        ids[table] = found["id"]
    asked = [
        ("users", "email", "email()"),
        ("users", "phone", "phone_number()"),
        ("get_user_by_name", "email", "email()"),
        ("get_user_by_name", "phone", "phone_number()"),
        ("employee", "contact", "email()"),
        ("employees", "contact", "email()"),
    ]
    _install_fake_anthropic(
        monkeypatch,
        [
            _round(
                *[
                    ("find_table_id", {"domain": domain, "table": table})
                    for domain, table in sorted(_TABLES)
                ],
                ("list_fake_kinds", {}),
            ),
            _round(
                *[
                    (
                        "set_column_fake",
                        {"table_id": ids[table], "column": column, "fake": fake},
                    )
                    for table, column, fake in asked
                ]
            ),
            SimpleNamespace(content=[_block(type="text", text="")], stop_reason="end_turn"),
        ],
    )
    question = (
        "create fake defs for: employee.contact, employees.contact, get_user_by_name.email, "
        "get_user_by_name.phone, users.email, users.phone"
    )
    with (
        # A save rebuilds the org's schemas; this control plane has no engine to build them on.
        patch("provisa.api.admin.schema_mutation._rebuild_schemas", new=AsyncMock()),
        patch("provisa.api.admin.schema_mutation._remove_view_mv"),
    ):
        events = [
            ev
            async for ev in chat.run_chat(
                demo,
                "org_admin",
                [{"role": "user", "content": question}],
                request=_request(),
            )
        ]
    results = [(e["name"], e["is_error"]) for e in events if e["type"] == "tool_result"]
    assert results == [
        *[("find_table_id", False)] * 4,
        ("list_fake_kinds", False),
        *[("set_column_fake", False)] * 4,
        *[("set_column_fake", True)] * 2,
    ]
    assert await _fakes(demo) == {
        "users.email": "email()",
        "users.phone": "phone_number()",
        "get_user_by_name.email": "email()",
        "get_user_by_name.phone": "phone_number()",
    }
    [text] = [e["text"] for e in events if e["type"] == "text"]
    assert text == (
        "\n\n(The model returned no answer. These tool calls failed:\n"
        "- set_column_fake: Error: 422: employee.contact: email() makes str values, which a other "
        "column cannot hold\n"
        "- set_column_fake: Error: 422: employees.contact: email() makes str values, which a other "
        "column cannot hold)"
    )
