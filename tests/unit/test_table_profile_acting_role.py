# Copyright (c) 2026 Kenneth Stott
# Canary: 7c4b9e02-5d16-4a83-b3f7-1e9a6d0c2f58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Profiling a view samples it as the role the request runs as (REQ-273, REQ-452).

The auth layer resolves ``X-Provisa-Role`` before the handler runs: a comma-separated set of held
roles becomes their meta-role, and a control-plane role resolves to the caller's data-plane role.
The sample is governed as THAT role. Read from the raw header instead, a set reached the pipeline
as one unknown role named ``a,b`` and a remapped role as the one the auth layer had replaced.
"""

# Requirements: REQ-273, REQ-452

from __future__ import annotations

import types

import pytest

from provisa.api.admin import table_profile_router as router_mod
from provisa.api.errors import ApiError


@pytest.fixture
def governed(monkeypatch) -> list[str]:
    """The roles the table's sample was read as."""
    seen: list[str] = []

    async def _read(_state, table_id, role_id, *, limit=None, request=None):
        seen.append(role_id)
        return types.SimpleNamespace(column_names=["id"], rows=[(1,)])

    monkeypatch.setattr(router_mod, "read_as_caller", _read)
    monkeypatch.setattr(
        router_mod, "table_for_steward", lambda request, state, table_id: {"draft": False}
    )
    state = types.SimpleNamespace(model_db=object(), federation_engine=object(), roles={})
    monkeypatch.setattr(router_mod, "state", state)
    monkeypatch.setattr(router_mod, "require_role", lambda roles, role_id: {"id": role_id})
    monkeypatch.setattr(router_mod, "role_max_query_time_ms", lambda role: None)
    monkeypatch.setattr(router_mod, "request_timeout_for", lambda transport: 30.0)
    return seen


def _request(role: str | None):
    state = types.SimpleNamespace()
    if role is not None:
        state.role = role
    return types.SimpleNamespace(state=state)


@pytest.mark.parametrize(
    "header, established",
    [
        ("analyst,org_admin", "meta:analyst+org_admin"),  # a set acts as its meta-role
        ("platform_admin", "org_admin"),  # a control-plane role resolved to the data-plane one
        ("analyst", "analyst"),
    ],
)
async def test_the_sample_is_governed_as_the_established_role(governed, header, established):
    out = await router_mod.profile_table(_request(established), 7, x_provisa_role=header)
    assert governed == [established]
    assert out == {"columns": ["id"], "rows": [{"id": 1}], "rowCount": 1, "draft": False}


async def test_with_no_auth_layer_the_header_names_the_role(governed):
    await router_mod.profile_table(_request(None), 7, x_provisa_role="analyst")
    assert governed == ["analyst"]


async def test_no_role_at_all_is_refused(governed):
    with pytest.raises(ApiError) as err:
        await router_mod.profile_table(_request(None), 7, x_provisa_role=None)
    assert (err.value.status_code, err.value.code) == (400, "profile.role_header_required")
    assert governed == []


async def test_the_read_runs_inside_the_roles_own_time(monkeypatch):
    """What a preview that waits on its copy's build answers before (ReplicaBuilding)."""
    from provisa.api.admin.viewer_statement import ViewerRequest
    from provisa.core import request_deadline

    deadlines: list = []

    async def _read(_state, table_id, role_id, *, limit=None, request=None):
        deadlines.append(request_deadline.remaining())
        return types.SimpleNamespace(column_names=["id"], rows=[])

    monkeypatch.setattr(router_mod, "read_as_caller", _read)
    monkeypatch.setattr(router_mod, "table_for_steward", lambda *a: {"draft": True})
    monkeypatch.setattr(router_mod, "header_role", lambda *a: "r")
    monkeypatch.setattr(router_mod, "require_role", lambda roles, role_id: {"id": role_id})
    monkeypatch.setattr(router_mod, "role_max_query_time_ms", lambda role: 5000)
    monkeypatch.setattr(router_mod, "request_timeout_for", lambda transport: 30.0)
    monkeypatch.setattr(
        router_mod,
        "state",
        types.SimpleNamespace(model_db=object(), federation_engine=object(), roles={}),
    )
    await router_mod._rows(types.SimpleNamespace(), 9, "r", page=ViewerRequest())
    assert deadlines[0] is not None and deadlines[0] <= 5.0


async def test_a_read_whose_copy_is_still_building_is_answered_by_code():
    """On any route: the table and how long, by the code a statement on /data/sql is answered
    with -- not the bare timeout answer."""
    import json

    from provisa.api.app import create_app
    from provisa.federation.replica_state import ReplicaBuilding

    handler = create_app().exception_handlers[ReplicaBuilding]
    resp = await handler(types.SimpleNamespace(), ReplicaBuilding("mail.public.messages", 41.6))
    body = json.loads(resp.body)
    assert resp.status_code == 504
    assert body["code"] == "data.replica_building"
    assert body["params"] == {"replica": "mail.public.messages", "seconds": 42}


async def test_a_governance_refusal_is_answered_403_naming_it_never_500():
    """A view reading a column the role may not see is refused by governance (one semantic
    statement). The route catches nothing; the app answers the refusal 403 with its words."""
    import json

    from provisa.api.app import create_app

    app = create_app()
    handler = app.exception_handlers[PermissionError]
    refusal = PermissionError("[V003] Column 'price' is not visible to this role")
    resp = await handler(types.SimpleNamespace(), refusal)
    assert resp.status_code == 403
    assert json.loads(resp.body) == {"detail": "[V003] Column 'price' is not visible to this role"}


async def test_an_approval_denial_keeps_its_code_and_reason():
    import json

    from provisa.api.app import create_app
    from provisa.pgwire._pipeline import ApprovalDenied

    handler = create_app().exception_handlers[PermissionError]
    resp = await handler(types.SimpleNamespace(), ApprovalDenied("outside hours"))
    assert resp.status_code == 403
    assert json.loads(resp.body) == {
        "detail": "Approval denied: outside hours",
        "code": "data.approval_denied",
        "params": {"reason": "outside hours"},
    }


async def test_a_role_served_no_table_is_answered_403_naming_it():
    # REQ-2154: a role no column is granted to is refused naming it, never a 500.
    import json

    from provisa.api.app import create_app
    from provisa.api.role_surface import RoleServedNothing

    handler = create_app().exception_handlers[PermissionError]
    resp = await handler(types.SimpleNamespace(), RoleServedNothing("loose"))
    assert resp.status_code == 403
    assert json.loads(resp.body) == {
        "detail": "Role 'loose' is served no tables: no column of any table is granted to it",
        "code": "data.role_served_nothing",
        "params": {"role_id": "loose"},
    }
