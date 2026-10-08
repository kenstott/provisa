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
from provisa.core.models import DERIVED_SOURCE_ID


class _Conn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute_core(self, _stmt):
        row = types.SimpleNamespace(
            _mapping={
                "source_id": DERIVED_SOURCE_ID,
                "schema_name": "sales",
                "table_name": "big_orders",
                "view_sql": "SELECT id FROM sales.orders;",
            }
        )
        return types.SimpleNamespace(fetchone=lambda: row)


@pytest.fixture
def governed(monkeypatch) -> list[str]:
    """The roles the view's sample was governed as."""
    seen: list[str] = []

    async def _govern(sql, role_id):
        seen.append(role_id)
        return object()

    async def _execute(_plan, _state):
        return types.SimpleNamespace(column_names=["id"], rows=[(1,)])

    monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route", _govern)
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _execute)
    monkeypatch.setattr(router_mod, "require_capability_request", lambda request, cap: None)
    state = types.SimpleNamespace(
        model_db=types.SimpleNamespace(acquire=lambda: _Conn()), federation_engine=object()
    )
    monkeypatch.setattr(router_mod, "state", state)
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
    assert out == {"columns": ["id"], "rows": [{"id": 1}], "rowCount": 1}


async def test_with_no_auth_layer_the_header_names_the_role(governed):
    await router_mod.profile_table(_request(None), 7, x_provisa_role="analyst")
    assert governed == ["analyst"]


async def test_no_role_at_all_is_refused(governed):
    with pytest.raises(ApiError) as err:
        await router_mod.profile_table(_request(None), 7, x_provisa_role=None)
    assert (err.value.status_code, err.value.code) == (400, "profile.role_header_required")
    assert governed == []
