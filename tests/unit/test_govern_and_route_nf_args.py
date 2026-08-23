# Copyright (c) 2026 Kenneth Stott
# Canary: 1b8c3d5e-7f9a-4c2e-9d1b-6e3a8f5c2d7b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regression test: the raw-SQL governance path (_govern_and_route, used by pgwire and
the admin /data/sql endpoint that GovernedTableViewer/TablePreviewModal call) must
extract _nf_* WHERE conditions (e.g. _nf_petId) and pass them through to
_optimize_and_route as nf_args, exactly like the compiled GraphQL/Cypher path
(_govern_and_route_compiled) already does.

ROOT CAUSE: _govern_and_route never called extract_nf_args, so nf_args was always None.
_optimize_and_route -> _materialize_api_to_engine_cache -> _mat_api_ep_table could not
resolve a required path param (e.g. petId for an openapi get_pet_by_id endpoint) from an
empty nf_args, so materialization was silently skipped. The unmaterialized table then
reached the engine unchanged, including the literal `"_nf_petId" = '1'` predicate, and
failed with "no such table: default.get_pet_by_id" (or a masked-column error, since the
predicate itself was never a real column).

FIX: _govern_and_route now calls extract_nf_args on the catalog-physical SQL before
_optimize_and_route, same as the compiled path, and forwards the extracted nf_args.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_types import TableMeta

pytestmark = pytest.mark.asyncio

TABLE_ID = 99
SOURCE_ID = "petstore-api"
DOMAIN_ID = "petstore"

_PETS_COLUMNS = [
    {"column_name": "id", "data_type": "varchar", "visible_to": ["analyst"]},
    {"column_name": "petId", "data_type": "varchar", "visible_to": ["analyst"]},
]

_PETS_TABLE_DICT = {
    "id": TABLE_ID,
    "source_id": SOURCE_ID,
    "schema_name": "public",
    "table_name": "get_pet_by_id",
    "domain_id": DOMAIN_ID,
    "columns": _PETS_COLUMNS,
}


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "get_pet_by_id": TableMeta(
            table_id=TABLE_ID,
            field_name="get_pet_by_id",
            type_name="GetPetById",
            source_id=SOURCE_ID,
            catalog_name=SOURCE_ID,
            schema_name="public",
            table_name="get_pet_by_id",
            domain_id=DOMAIN_ID,
        )
    }
    return ctx


def _fake_state():
    return SimpleNamespace(
        contexts={"analyst": _ctx()},
        rls_contexts={},
        roles={"analyst": {"capabilities": [], "domain_access": ["*"]}},
        masking_rules={},
        tables=[_PETS_TABLE_DICT],
        relationships=[],
        source_types={SOURCE_ID: "openapi"},
        source_catalogs={},
        federation_engine=None,
        view_sql_map=None,
        security_high=False,
        metrics={},
    )


async def test_nf_condition_extracted_and_forwarded_as_nf_args(monkeypatch):
    """FAILS before fix: _optimize_and_route is called with nf_args=None even though the
    SQL carries a `_nf_petId` predicate. PASSES after fix: nf_args == {"petId": "1"}."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", _fake_state(), raising=False)

    captured: dict = {}

    async def _fake_optimize_and_route(exec_sql, governed_sql, gov_ctx, ctx, state, **kwargs):
        captured["exec_sql"] = exec_sql
        captured["nf_args"] = kwargs.get("nf_args")
        from provisa.transpiler.router import Route, RouteDecision

        return (
            exec_sql,
            RouteDecision(route=Route.DIRECT, source_id=SOURCE_ID, dialect=None, reason="test"),
            SOURCE_ID,
            False,
            {SOURCE_ID},
            (),
        )

    with patch.object(
        _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_fake_optimize_and_route)
    ):
        await _pipeline._govern_and_route(
            "SELECT * FROM get_pet_by_id WHERE \"_nf_petId\" = '1'", "analyst"
        )

    assert captured["nf_args"] == {"petId": "1"}
    assert "_nf_petId" not in captured["exec_sql"]
