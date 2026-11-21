# Copyright (c) 2026 Kenneth Stott
# Canary: 4c6fde58-bdf4-4256-9339-70184ec2f73a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1921: no query language reaches a withdrawn table, on the GraphQL and REST surfaces as on the
SQL ones, for any role -- org_admin, a domain steward, a role inheriting the right, a meta role,
or one holding no right. No role's schema or REST paths offer it, no catalog lists it, the ways to
reach a table name it for no role, and a request naming it is refused, never routed. Its stewards
read it through the table's preview alone (admin/table_read).

The real role build, the real endpoint and router, with routing stood in for."""

# Requirements: REQ-1921

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api.role_surface import role_surface
from tests.unit.test_table_read_as_caller import (
    _REFUNDS,
    multi_domain,  # noqa: F401  # pyright: ignore[reportUnusedImport]
    routing,  # noqa: F401  # pyright: ignore[reportUnusedImport]
)
from tests.unit.test_withdrawn_reached_by_no_statement import state  # noqa: F401  # pyright: ignore[reportUnusedImport]

_QUERY = "{ refunds { id note } }"


def _meta(state_: Any, members: list[str]) -> str:
    from provisa.security.meta_role import ensure_meta_role

    return ensure_meta_role(state_, members)


def _every_role(state_: Any) -> list[str]:
    return [
        "org_admin",
        "steward",
        "sales_steward",
        "r",
        _meta(state_, ["org_admin", "r"]),
        _meta(state_, ["r", "s"]),
        _meta(state_, ["finance_registrar", "r"]),
    ]


def test_no_roles_schema_or_rest_paths_offer_a_withdrawn_table(state):  # noqa: F811
    for role_id in _every_role(state):
        surface = role_surface(state, role_id)
        assert surface is not None
        assert "refunds" not in surface.schema.query_type.fields, role_id
        assert all(m["table_id"] != _REFUNDS for m in surface.table_path_map.values()), role_id
        assert "refunds" not in surface.context.tables, role_id


# ---- GraphQL ------------------------------------------------------------------------------ #


def _graphql(state_: Any, role_id: str, query: str, monkeypatch) -> Any:
    from provisa.api.data import endpoint

    async def _awake(_state: Any) -> None:
        return None

    monkeypatch.setattr(endpoint, "ensure_engine_awake", _awake)
    raw = SimpleNamespace(
        state=SimpleNamespace(role=role_id, tenant_id=None), headers={}, method="POST"
    )

    async def _ask() -> Any:
        return await endpoint.graphql_endpoint(
            raw,
            endpoint.GraphQLRequest(query=query, role=role_id),
            x_provisa_role=role_id,
            accept=None,
            x_provisa_redirect=None,
            x_provisa_redirect_threshold=None,
            x_provisa_redirect_format=None,
            x_provisa_stats=None,
            x_provisa_normalized=None,
            x_provisa_trace=None,
        )

    return asyncio.run(_ask())


def _refused(call: Any) -> Any:
    """What a refused request comes back as: an error raised, or a response naming errors and
    holding no data."""
    import json

    try:
        response = call()
    except Exception as refused:  # allow-ble: the test reads whichever form the refusal took
        return refused
    body = json.loads(response.body)
    assert body.get("errors") and not body.get("data"), body
    return body


def test_every_roles_graphql_query_naming_it_is_refused(state, routing, monkeypatch):  # noqa: F811
    for role_id in _every_role(state):
        _refused(lambda: _graphql(state, role_id, _QUERY, monkeypatch))
    routing.assert_not_awaited()


def test_a_withdrawn_table_granted_a_write_takes_none(state, routing, monkeypatch):  # noqa: F811
    """Granted a write of the withdrawn table, org_admin is offered no mutation of it, and a
    mutation naming it is refused, never routed."""
    # Before any surface is built: the lazy build reads these inputs when the role is first read.
    (withdrawn,) = state.role_build_inputs["withdrawn_tables"]
    withdrawn.update(
        write_ops=["delete"],
        write_returns_rows=True,
        write_grants=[
            {"role": "org_admin", "operation": "delete", "row_rule": None, "columns": ["id"]}
        ],
    )
    surface = role_surface(state, "org_admin")
    assert surface is not None
    mutations = surface.schema.mutation_type
    assert mutations is None or not any("refunds" in name.lower() for name in mutations.fields)
    _refused(
        lambda: _graphql(
            state,
            "org_admin",
            "mutation { deleteRefunds(where: {id: {eq: 1}}) { affectedRows } }",
            monkeypatch,
        )
    )
    routing.assert_not_awaited()


# ---- REST --------------------------------------------------------------------------------- #


def _rest(state_: Any, role_id: str) -> Any:
    from fastapi import FastAPI
    from starlette.testclient import TestClient

    from provisa.api.rest.generator import create_rest_router

    app = FastAPI()

    @app.middleware("http")
    async def _as_role(request, call_next):  # pyright: ignore[reportUnusedFunction]
        request.state.role = role_id
        return await call_next(request)

    app.include_router(create_rest_router(state_))
    return TestClient(app).get("/data/rest/sales/refunds", params={"limit": 5})


def test_every_roles_rest_read_of_it_is_not_found(state, routing):  # noqa: F811
    for role_id in ("org_admin", "steward", "sales_steward", "r"):
        assert _rest(state, role_id).status_code == 404, role_id
    routing.assert_not_awaited()


# ---- No catalog lists it ------------------------------------------------------------------ #


def test_no_catalog_listing_names_a_withdrawn_table(state):  # noqa: F811
    """The pgwire catalog and the table list MCP and Flight SQL read are the role's context, which
    holds no withdrawn table. The preview's own view, which its steward's read runs in, would list
    it: the control that the listing reads the context."""
    from provisa.api.admin.table_read import previewing
    from provisa.api.flight.catalog import role_visibility
    from provisa.pgwire.catalog_populate import role_catalog_index

    (withdrawn,) = state.role_build_inputs["withdrawn_tables"]
    for role_id in ("org_admin", "steward", _meta(state, ["org_admin", "r"])):
        _ctx, index, _metrics = role_catalog_index(state, role_id)
        assert _REFUNDS not in index.table_id_to_oid
        assert _REFUNDS not in role_visibility(state, role_id)
        seen = asyncio.run(previewing(state, role_id, [withdrawn]))
        _ctx, in_view, _metrics = role_catalog_index(seen, role_id)
        assert _REFUNDS in in_view.table_id_to_oid
        assert _REFUNDS in role_visibility(seen, role_id)


# ---- The ways to reach it (admin tableAccess) --------------------------------------------- #


def _endpoints() -> Any:
    from provisa.api.metadata_export.model import ServingEndpoints

    return ServingEndpoints(
        hostname="data.example.com",
        database="provisa",
        http_origin="https://data.example.com",
        pgwire_port=5433,
        http_paths={"GraphQL": "/data/graphql", "REST": "/data/rest"},
    )


def _access(state_: Any, role_id: str) -> Any:
    from provisa.api.admin._table_access import table_access

    return asyncio.run(table_access(state_, role_id, _REFUNDS, _endpoints()))


def test_no_role_is_told_how_to_reach_a_withdrawn_table(state):  # noqa: F811
    for role_id in _every_role(state):
        with pytest.raises(ValueError, match=f"table {_REFUNDS} is not served to role"):
            _access(state, role_id)
