# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e9b14-5c3a-4f81-9e06-b3a1c8f47d25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1105, AN MCP CALL RUNS AS A PRINCIPAL WITH ITS ENTITLEMENTS and A LOCAL MCP SERVER ACTS FOR
A PRINCIPAL ITS OPERATOR NAMES.

A remote call runs as its token's principal; a local one acts for the principal the operator names
in ``mcp.principal``. Either way the principal's entitlement lists (REQ-2219) narrow the role the
call acts as, read where the one SQL pipeline reads them (``govern_for_request``): a column outside
the person's list is not returned. A local server naming no principal refuses every call by the
setting's name, and one naming someone not in the org refuses every call too."""

# Requirements: REQ-1105, REQ-2219

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import insert

from provisa.api.mcp import server as mcp_server
from provisa.api.mcp.server import build_mcp_server
from provisa.auth.models import AuthIdentity
from provisa.core.schema_admin import local_users, user_org_memberships
from provisa.core.schema_org import user_directory
from provisa.entitlements import runtime
from provisa.entitlements.mask import MaskCache
from provisa.executor.result import QueryResult
from provisa.pgwire import _pipeline
from tests.role_state import RoleState
from tests.unit.test_entitlement_lists import _ctx, _names
from tests.unit.test_entitlement_rule import _governance

pytestmark = pytest.mark.asyncio

_EVERY_COLUMN = "SELECT * FROM revenue"
_REVENUE = 1  # its table id in the entitlement fixtures
#: alice may read only revenue.id within viewer: amount is outside her list.
_ALICES_LISTS = {"viewer": {"columns": ("finance.revenue.id",)}}


def _state(**over) -> RoleState:
    base = dict(
        contexts={"viewer": _ctx()},
        roles={"viewer": {"capabilities": [], "domain_access": ["*"], "default_entitlements": {}}},
        names_table=_names(),
        entitlement_masks=MaskCache(),
        schema_boot_id="b",
        schema_version=1,
        data_product_refs=(),
        idp_entitlements={},
        auth_config={"default_role": "viewer", "role_mapping": []},
        multitenancy=False,
        org_id="default",  # the deployment's org, the one the unit tests run bound to
        admin_db=None,
    )
    base.update(over)
    return RoleState(**base)


@pytest.fixture
def executed(monkeypatch):
    """The pipeline's governance step, as ``governed_plan`` takes it, for the role the call acts
    as: the bound principal's mask narrows the role's governance, and ``SELECT *`` reads the
    narrowed columns of ``revenue`` (as ``test_entitlement_rule`` shows the star does). The
    columns of each executed statement are recorded."""
    monkeypatch.setattr(runtime, "_role_context", lambda _state, _role: _ctx())
    seen: list[list[str]] = []

    async def _batch(sql, role, state, **_kw):
        governance = runtime.govern_for_request(_governance(), state, role)
        visible = governance.visible_columns[_REVENUE]
        names = [c for c, _t in governance.all_columns[_REVENUE] if visible is None or c in visible]
        seen.append(names)
        return QueryResult(rows=[tuple(range(len(names)))], column_names=names)

    monkeypatch.setattr(_pipeline, "execute_sql_batch", _batch)
    return seen


def _columns(result) -> list[str]:
    blocks = result[0] if isinstance(result, tuple) else result
    return json.loads(blocks[0].text)["columns"]


# -- remote: the token's principal ------------------------------------------------------------


async def _over_http(state, sql: str):
    """``sql`` sent through run_sql over the Streamable HTTP transport with a bearer token (which
    the ``remote`` fixture validates to a person)."""
    mcp = build_mcp_server(state)
    answered: list = []

    async def _app(_scope, _receive, _send):
        answered.append(await mcp.call_tool("run_sql", {"sql": sql}))

    async def _send(_message):
        raise AssertionError(f"the transport refused the call: {_message}")

    middleware = mcp_server._wrap_role_auth(_app, state, require_token=True)
    scope = {"type": "http", "headers": [(b"authorization", b"Bearer t")]}
    await middleware(scope, None, _send)
    (result,) = answered
    return result


@pytest.fixture
def remote(monkeypatch):
    from provisa.security import role_probe

    monkeypatch.setattr("provisa.api.app.ensure_serving_runtime", AsyncMock())
    monkeypatch.setattr(role_probe, "suspended", AsyncMock(return_value=False))

    def _token_of(identity):
        monkeypatch.setattr(mcp_server, "_validate_mcp_token", AsyncMock(return_value=identity))

    return _token_of


def _person(user_id: str, claims: dict) -> AuthIdentity:
    return AuthIdentity(
        user_id=user_id, email=None, display_name=None, roles=["viewer"], raw_claims=claims
    )


async def test_a_remote_query_runs_as_the_tokens_principal_narrowed_by_its_list(remote, executed):
    remote(_person("alice", {"groups": ["provisa:viewer:columns:finance.revenue.id"]}))
    result = await _over_http(_state(), _EVERY_COLUMN)
    assert _columns(result) == ["id"]
    assert executed == [["id"]]


async def test_another_principal_of_the_same_role_reads_what_the_role_reads(remote, executed):
    remote(_person("bob", {}))
    result = await _over_http(_state(), _EVERY_COLUMN)
    assert _columns(result) == ["id", "amount"]


# -- local: the principal the operator names ---------------------------------------------------


@pytest.fixture
async def org(control_plane, monkeypatch):
    """A single-org deployment's control plane, with alice's local account, and alice's lists
    as the store mirrors them from her last sign-in."""
    admin = control_plane.platform
    async with admin.acquire() as conn:
        await conn.execute_core(
            insert(local_users).values(
                id="alice", username="alice", password_hash="x", roles=["viewer"]
            )
        )
    state = _state(
        admin_db=admin,
        model_db=control_plane.org(),
        idp_entitlements={"alice": _ALICES_LISTS},
    )
    monkeypatch.setattr("provisa.api.app.state", state)
    monkeypatch.setenv("PROVISA_MCP_ROLE", "viewer")
    return state


async def test_a_local_query_acts_for_the_named_principal_narrowed_by_its_list(
    org, executed, monkeypatch
):
    monkeypatch.setenv("PROVISA_MCP_PRINCIPAL", "alice")
    result = await build_mcp_server(org).call_tool("run_sql", {"sql": _EVERY_COLUMN})
    assert _columns(result) == ["id"]
    assert executed == [["id"]]


async def test_a_local_principal_with_no_list_reads_what_the_role_reads(org, executed, monkeypatch):
    async with org.model_db.acquire() as conn:
        await conn.execute_core(insert(user_directory).values(user_id="carol"))
    monkeypatch.setenv("PROVISA_MCP_PRINCIPAL", "carol")
    result = await build_mcp_server(org).call_tool("run_sql", {"sql": _EVERY_COLUMN})
    assert _columns(result) == ["id", "amount"]


#: Tools across the server: data, catalog, graph and role-free ones alike.
_CALLS = [
    ("run_sql", {"sql": _EVERY_COLUMN}),
    ("explain_sql", {"sql": _EVERY_COLUMN}),
    ("list_schemas", {}),
    ("describe_role", {}),
    ("run_cypher", {"cypher": "MATCH (n) RETURN n"}),
    ("clear_graph_session", {}),
    ("render_graph", {"elements": {"nodes": [], "edges": []}}),
]


@pytest.mark.parametrize(("tool", "args"), _CALLS, ids=[c[0] for c in _CALLS])
async def test_a_local_server_naming_no_principal_refuses_every_call_by_name(
    org, executed, monkeypatch, tool, args
):
    monkeypatch.delenv("PROVISA_MCP_PRINCIPAL", raising=False)
    with pytest.raises(Exception, match=r"mcp\.principal \(PROVISA_MCP_PRINCIPAL\) is not set"):
        await build_mcp_server(org).call_tool(tool, args)
    assert executed == []


@pytest.mark.parametrize(("tool", "args"), _CALLS, ids=[c[0] for c in _CALLS])
async def test_an_unknown_principal_is_refused(org, executed, monkeypatch, tool, args):
    monkeypatch.setenv("PROVISA_MCP_PRINCIPAL", "mallory")
    with pytest.raises(Exception, match=r"names 'mallory', who is not in org"):
        await build_mcp_server(org).call_tool(tool, args)
    assert executed == []


async def test_an_inactive_local_account_is_not_in_the_org(org, executed, monkeypatch):
    async with org.admin_db.acquire() as conn:
        await conn.execute_core(
            insert(local_users).values(
                id="dave", username="dave", password_hash="x", roles=[], is_active=False
            )
        )
    monkeypatch.setenv("PROVISA_MCP_PRINCIPAL", "dave")
    with pytest.raises(Exception, match=r"names 'dave', who is not in org"):
        await build_mcp_server(org).call_tool("run_sql", {"sql": _EVERY_COLUMN})


async def test_under_multitenancy_only_a_member_of_the_org_is_in_it(org, executed, monkeypatch):
    """A local account of the deployment is not thereby in every org: membership is the rule
    the wire protocols bind an org by."""
    org.multitenancy = True
    monkeypatch.setenv("PROVISA_MCP_PRINCIPAL", "alice")
    with pytest.raises(Exception, match=r"names 'alice', who is not in org"):
        await build_mcp_server(org).call_tool("run_sql", {"sql": _EVERY_COLUMN})
    from provisa.core.request_context import require_current_org

    async with org.admin_db.acquire() as conn:
        await conn.execute_core(
            insert(user_org_memberships).values(user_id="alice", org_id=require_current_org())
        )
    result = await build_mcp_server(org).call_tool("run_sql", {"sql": _EVERY_COLUMN})
    assert _columns(result) == ["id"]


async def test_the_principal_setting_follows_the_operators_configuration(monkeypatch):
    from provisa.api.mcp import local_principal

    monkeypatch.setenv("PROVISA_MCP_PRINCIPAL", " alice ")
    assert local_principal.configured_principal() == "alice"
    monkeypatch.delenv("PROVISA_MCP_PRINCIPAL")
    with pytest.raises(PermissionError, match=r"mcp\.principal \(PROVISA_MCP_PRINCIPAL\)"):
        local_principal.configured_principal()


async def test_a_local_call_is_recorded_as_its_principals(org, monkeypatch):
    """The statements of a local call are the named principal's, as a remote call's are its
    token's (REQ-074)."""
    from provisa.audit.context import current_audit_identity

    monkeypatch.setenv("PROVISA_MCP_PRINCIPAL", "alice")
    seen: list = []

    async def _batch(sql, role, state, **_kw):
        seen.append(current_audit_identity())
        return QueryResult(rows=[], column_names=[])

    monkeypatch.setattr(_pipeline, "execute_sql_batch", _batch)
    await build_mcp_server(org).call_tool("run_sql", {"sql": "SELECT 1"})
    assert [(i.user_id, i.surface) for i in seen] == [("alice", "mcp")]
