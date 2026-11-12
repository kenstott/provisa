# Copyright (c) 2026 Kenneth Stott
# Canary: 4c6fde58-bdf4-4256-9339-70184ec2f73a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1921, A DRAFT IS SERVED TO ORG_ADMIN (decided 2026-11-12), on the GraphQL and REST surfaces:
the schema and REST paths of a role holding table registration in a draft's domain -- org_admin, a
domain steward, a role inheriting the right, or a meta role one of whose members carries it
there -- offer that draft, marked as a draft; a request of that role naming one is
compiled and governed in the drafts' own view (admin/table_read.reading_drafts), the path a SQL
statement naming one takes, nothing of it kept and recorded as a draft preview. Every other role's
schema holds no draft and its request naming one is refused as a draft, as before.

The real role build, the real endpoint and router, with routing stood in for."""

# Requirements: REQ-1921

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api.app_loaders import DRAFT_MARK
from provisa.api.role_surface import role_surface
from tests.unit.test_draft_read_by_org_admin import state  # noqa: F401  # pyright: ignore[reportUnusedImport]
from tests.unit.test_table_read_as_caller import (
    _REFUNDS,
    _Routed,
    multi_domain,  # noqa: F401  # pyright: ignore[reportUnusedImport]
    routing,  # noqa: F401  # pyright: ignore[reportUnusedImport]
)

_QUERY = "{ refunds { id note } }"


def _meta(state_: Any, members: list[str]) -> str:
    from provisa.security.meta_role import ensure_meta_role

    return ensure_meta_role(state_, members)


def _offered(state_: Any, role_id: str) -> bool:
    surface = role_surface(state_, role_id)
    assert surface is not None
    return "refunds" in surface.schema.query_type.fields


@pytest.mark.parametrize("role_id", ["org_admin", "steward", "sales_steward"])
def test_the_schema_of_a_role_holding_org_admin_offers_the_draft_marked(state, role_id):  # noqa: F811
    surface = role_surface(state, role_id)
    assert surface is not None
    assert surface.schema.query_type.fields["refunds"]
    assert surface.schema.get_type("Refunds").description.startswith(DRAFT_MARK)
    (path,) = [m for m in surface.table_path_map.values() if m["table_id"] == _REFUNDS]
    assert path["table_description"].startswith(DRAFT_MARK)
    # Its context keeps it a draft: only the drafts' own view reads it.
    assert "refunds" not in surface.context.tables
    assert _REFUNDS in surface.context.draft_ids.values()


def test_a_meta_role_with_org_admin_among_its_members_is_offered_it(state):  # noqa: F811
    assert _offered(state, _meta(state, ["org_admin", "r"]))


def test_no_other_role_is_offered_a_draft(state):  # noqa: F811
    for role_id in ("r", "s", _meta(state, ["r", "s"]), _meta(state, ["finance_registrar", "r"])):
        surface = role_surface(state, role_id)
        assert surface is not None
        assert not _offered(state, role_id)
        assert all(m["table_id"] != _REFUNDS for m in surface.table_path_map.values())


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


@pytest.mark.parametrize("role_id", ["org_admin", "steward", "sales_steward"])
def test_a_graphql_query_of_a_role_holding_org_admin_reads_the_draft(
    state,  # noqa: F811
    routing,  # noqa: F811
    monkeypatch,
    role_id,
):
    with pytest.raises(_Routed) as routed:
        _graphql(state, role_id, _QUERY, monkeypatch)
    routing.assert_awaited_once()
    assert "refunds" in routed.value.sql


def test_a_meta_role_with_org_admin_reads_it_by_graphql(state, routing, monkeypatch):  # noqa: F811
    with pytest.raises(_Routed):
        _graphql(state, _meta(state, ["org_admin", "r"]), _QUERY, monkeypatch)
    routing.assert_awaited_once()


def test_the_graphql_read_is_governed_in_the_drafts_view_and_recorded_as_a_preview(
    state,  # noqa: F811
    monkeypatch,
):
    from provisa.pgwire import _pipeline

    governed_in: list = []

    async def _govern(sql: str, role_id: str, **kw: Any) -> Any:
        governed_in.append(kw["state"])
        raise _Routed(sql)

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    with pytest.raises(_Routed):
        _graphql(state, "org_admin", _QUERY, monkeypatch)
    (seen,) = governed_in
    assert seen.previewed_draft_ids == {_REFUNDS}
    assert seen is not state and seen.compiled_query_cache is not state.compiled_query_cache


def test_the_graphql_read_keeps_nothing(state, routing, monkeypatch):  # noqa: F811
    surface_before = role_surface(state, "org_admin")
    assert surface_before is not None
    kept_before = len(state.compiled_query_cache)
    with pytest.raises(_Routed):
        _graphql(state, "org_admin", _QUERY, monkeypatch)
    assert role_surface(state, "org_admin") is surface_before
    assert "refunds" not in surface_before.context.tables
    assert len(state.compiled_query_cache) == kept_before
    assert not hasattr(state, "previewed_draft_ids")


def test_every_other_roles_graphql_query_naming_it_is_refused_as_a_draft(
    state,  # noqa: F811
    routing,  # noqa: F811
    monkeypatch,
):
    from provisa.compiler.definitions import TableIsDraft

    for role_id in ("r", _meta(state, ["r", "s"]), _meta(state, ["finance_registrar", "r"])):
        with pytest.raises(TableIsDraft):
            _graphql(state, role_id, _QUERY, monkeypatch)
    routing.assert_not_awaited()


def test_a_draft_is_offered_to_be_read_and_takes_no_write(state, routing, monkeypatch):  # noqa: F811
    """Granted a write of the draft, org_admin is still offered no mutation of it, and a mutation
    naming it is refused, never routed."""
    from fastapi import HTTPException

    from provisa.compiler.definitions import TableIsDraft

    # Before any surface is built: the lazy build reads these inputs when the role is first read.
    (draft,) = state.role_build_inputs["draft_tables"]
    draft.update(
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
    with pytest.raises((TableIsDraft, HTTPException)):
        _graphql(
            state,
            "org_admin",
            "mutation { deleteRefunds(where: {id: {eq: 1}}) { affectedRows } }",
            monkeypatch,
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


@pytest.mark.parametrize("role_id", ["org_admin", "steward", "sales_steward"])
def test_a_rest_read_of_a_role_holding_org_admin_reads_the_draft(
    state,  # noqa: F811
    monkeypatch,
    role_id,
):
    from provisa.executor.result import QueryResult
    from provisa.pgwire import _pipeline

    governed_in: list = []
    ran_in: list = []

    async def _govern(sql: str, role_id_: str, **kw: Any) -> Any:
        governed_in.append((sql, kw["state"]))
        return "plan"

    async def _execute(plan: Any, state_: Any) -> Any:
        ran_in.append(state_)
        return QueryResult(rows=[], column_names=[])

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    kept_before = len(state.compiled_query_cache)
    response = _rest(state, role_id)
    assert response.status_code == 200, response.text
    ((sql, seen),) = governed_in
    assert "refunds" in sql
    assert seen.previewed_draft_ids == {_REFUNDS} and ran_in == [seen]
    assert len(state.compiled_query_cache) == kept_before  # its plan is the view's alone


def test_every_other_roles_rest_read_of_it_is_not_found(state, routing):  # noqa: F811
    response = _rest(state, "r")
    assert response.status_code == 404
    routing.assert_not_awaited()


# ---- What still omits a draft ------------------------------------------------------------- #


def test_no_catalog_listing_names_a_draft_for_a_role_holding_org_admin(state):  # noqa: F811
    """The pgwire catalog and the table list MCP and Flight SQL read are the role's context, which
    holds no draft: a draft is read by naming it, and listed nowhere. The drafts' own view, which
    a read naming one runs in, would list it: the control that the listing reads the context."""
    from provisa.api.admin.table_read import reading_drafts
    from provisa.api.flight.catalog import role_visibility
    from provisa.pgwire.catalog_populate import role_catalog_index

    (draft,) = state.role_build_inputs["draft_tables"]
    for role_id in ("org_admin", "steward", _meta(state, ["org_admin", "r"])):
        _ctx, index, _metrics = role_catalog_index(state, role_id)
        assert _REFUNDS not in index.table_id_to_oid
        assert _REFUNDS not in role_visibility(state, role_id)
        seen = asyncio.run(reading_drafts(state, role_id, [draft]))
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


@pytest.mark.parametrize("role_id", ["org_admin", "steward", "sales_steward", "meta"])
def test_a_role_holding_org_admin_is_told_how_to_reach_the_draft_marked_draft(state, role_id):  # noqa: F811
    """Named as its surface names it: the field its schema offers, the REST path that answers it,
    and the SQL its statements name it by -- each of which reads the draft (above)."""
    if role_id == "meta":
        role_id = _meta(state, ["org_admin", "r"])
    access = _access(state, role_id)
    assert access.draft is True
    assert access.graphql == "query {\n  refunds(limit: 10) {\n    id\n    note\n  }\n}"
    assert access.sql == 'SELECT * FROM "sales"."refunds" LIMIT 10'
    assert access.rest == "GET https://data.example.com/data/rest/sales/refunds?limit=20"
    assert json.loads(access.mcp) == {"tool": "run_sql", "arguments": {"sql": access.sql}}
    assert access.pgwire is not None and access.pgwire.endswith("search_path%3Dsales")


def test_every_other_role_is_not_told_how_to_reach_a_draft(state):  # noqa: F811
    for role_id in ("r", _meta(state, ["r", "s"]), _meta(state, ["finance_registrar", "r"])):
        with pytest.raises(ValueError, match=f"table {_REFUNDS} is not served to role"):
            _access(state, role_id)
