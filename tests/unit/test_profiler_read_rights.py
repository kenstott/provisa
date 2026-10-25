# Copyright (c) 2026 Kenneth Stott
# Canary: 7742d77d-7b18-4af7-8a91-e22433802b73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The profiler's surfaces answer only for a table its caller is served (REQ-2152).

``table_registration`` opens the profiler; it does not widen what the caller reads. Every
profiler route reads its table through ``profiler_router._member``, which answers a table the
caller is served no column of exactly as it answers one that does not exist.
"""

# Requirements: REQ-2152

from __future__ import annotations

import types

import pytest
from sqlalchemy import insert

from provisa.api.admin.catalog_scope import CatalogScope
from provisa.api.admin.profiler_router import _member
from provisa.api.errors import ApiError
from provisa.core.models import Column, Table
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def member(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="hr"))
        table_id = await table_repo.upsert(
            conn,
            Table(
                source_id="pg",
                domain_id="hr",
                schema_name="public",
                table_name="payroll",
                profiler_source_id="pg",
                columns=[Column(name="salary", visible_to=["hr_role"], data_type="integer")],
            ),
        )
    assert table_id is not None
    return db, table_id


def _serving(monkeypatch, served: dict[int, set[str]], compiled: frozenset[int]) -> None:
    scope = CatalogScope(
        whole=False, reach=None, served=served, governance=False, role="r", compiled=compiled
    )
    monkeypatch.setattr("provisa.api.admin.catalog_scope.catalog_scope", lambda info: scope)


async def test_a_table_the_caller_is_not_served_is_answered_as_missing(member, monkeypatch):
    db, table_id = member
    _serving(monkeypatch, {}, frozenset({table_id}))
    request = types.SimpleNamespace(state=types.SimpleNamespace())
    async with db.acquire() as conn:
        with pytest.raises(ApiError) as unserved:
            await _member(conn, table_id, request)
        with pytest.raises(ApiError) as missing:
            await _member(conn, 4242, request)
    assert (unserved.value.status_code, unserved.value.code) == (404, "profile.table_not_found")
    assert str(unserved.value).replace(str(table_id), "N") == str(missing.value).replace(
        "4242", "N"
    )


async def test_a_served_table_is_answered_with_what_the_caller_is_served(member, monkeypatch):
    db, table_id = member
    _serving(monkeypatch, {table_id: {"salary"}}, frozenset({table_id}))
    request = types.SimpleNamespace(state=types.SimpleNamespace())
    async with db.acquire() as conn:
        found = await _member(conn, table_id, request)
    assert found["table_name"] == "payroll" and found["served"] == {"salary"}


def test_a_constraint_decision_is_answered_only_when_every_column_it_names_is_served():
    from provisa.api.admin.profiler_checks_router import readable_decisions

    decided = [
        {"id": "a", "physical_column": "id", "physical_other_column": None},
        {"id": "b", "physical_column": "id", "physical_other_column": "salary"},
        {"id": "c", "physical_column": "salary", "physical_other_column": None},
    ]
    assert [d["id"] for d in readable_decisions(decided, {"id"})] == ["a"]
    assert [d["id"] for d in readable_decisions(decided, None)] == ["a", "b", "c"]


def test_a_classification_on_a_column_the_caller_is_not_served_is_answered_as_unknown(
    monkeypatch,
):
    from provisa.api.admin import profiler_classifications_router as router

    monkeypatch.setattr(
        "provisa.api.admin.profiler_checks_router._physical_names",
        lambda table_id: {"id": "id", "salary": "salary"},
    )
    assert router._physical(7, "id", {"id"}) == "id"
    with pytest.raises(ApiError) as unserved:
        router._physical(7, "salary", {"id"})
    with pytest.raises(ApiError) as unknown:
        router._physical(7, "nope", {"id"})
    assert unserved.value.code == unknown.value.code == "profile.column_not_found"
    assert str(unserved.value).replace("salary", "X") == str(unknown.value).replace("nope", "X")


async def test_a_finding_on_a_column_the_caller_is_not_served_is_not_answered(monkeypatch):
    """REQ-2152: a judgment finding names its column; one the caller is not served is left out
    of the list and answered as missing when addressed."""
    from provisa.api.admin import judgments_router
    from provisa.judgments.resolution import finding_column

    on_salary = {"object_type": "column", "object_key": "7:salary"}
    on_tag = {"object_type": "tag_assignment", "object_key": "pii@column:7:id"}
    on_role = {"object_type": "role", "object_key": "analyst"}
    assert finding_column(on_salary) == (7, "salary")
    assert finding_column(on_tag) == (7, "id")
    assert finding_column(on_role) is None

    async def _served_columns(conn, request, table_id):
        return {"id"}

    monkeypatch.setattr("provisa.api.admin.read_rights.served_columns_request", _served_columns)
    assert not await judgments_router._served(None, None, on_salary)
    assert await judgments_router._served(None, None, on_tag)
    assert await judgments_router._served(None, None, on_role)
