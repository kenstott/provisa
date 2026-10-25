# Copyright (c) 2026 Kenneth Stott
# Canary: 675c96e4-a19c-4913-8fb7-925eea4d31ee
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table or view leaves a domain only by someone who may act in it (REQ-1531).

Saving a table or view under an address that already holds one in another domain checks the
caller's ``table_registration`` right in the domain it is in, as well as in the domain sent --
through ``updateTable`` and through a ``registerTable`` that hits the existing address, for a
physical table and for a view.
"""

# Requirements: REQ-1531, REQ-1944

from __future__ import annotations

import copy
from typing import Any

import pytest
from sqlalchemy import insert, select

import provisa.api.app as appmod
from provisa.core.schema_org import registered_tables, sources
from tests.unit.test_data_steward_domain_scope import (  # noqa: F401  (plane is a fixture)
    M,
    _info,
    _input,
    plane,
)
from tests.unit.gate_identity import serve_every_column

_EDIT_RIGHTS = ["table_registration", "create_view"]


@pytest.fixture(autouse=True)
def _served(monkeypatch):
    """Every column is served here: which ones a caller may read is REQ-2152's own tests."""
    serve_every_column(monkeypatch)


@pytest.fixture
async def two_domains(plane, monkeypatch):  # noqa: F811  (the fixture imported above)
    """``orders`` (a physical table) and ``sales_view`` (a view) in sales; a caller with the
    registering rights in finance only, and one with them in both domains."""
    from provisa.core import process_region
    from provisa.core.models import Column, Table
    from provisa.core.repositories import table as table_repo

    roles = dict(appmod.state.roles)
    roles["finance_editor"] = {
        "id": "finance_editor",
        "capabilities": _EDIT_RIGHTS,
        "domain_access": ["finance"],
    }
    roles["sales_finance_editor"] = {
        "id": "sales_finance_editor",
        "capabilities": _EDIT_RIGHTS,
        "domain_access": ["sales", "finance"],
    }
    monkeypatch.setattr(appmod.state, "roles", roles, raising=False)
    # A platform that declares no regions.
    monkeypatch.setattr(process_region, "_region", process_region.DEFAULT_REGION)
    monkeypatch.setattr(process_region, "_platform_regions", ())
    async with plane.db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="__derived__", type="postgresql"))
        # The view reads no governed table, so the view gate's read half passes for any caller
        # and the save reaches the domain check under test.
        plane.ids["sales_view"] = await table_repo.upsert(
            conn,
            Table(
                source_id="__derived__",
                domain_id="sales",
                schema_name="sales",
                table_name="sales_view",
                governance="pre-approved",
                view_sql="SELECT 1 AS id",
                columns=[Column(name="id", visible_to=[], data_type="integer")],
            ),
        )
    return plane


async def _domain(saved: Any, name: str) -> str:
    async with saved.db.acquire() as conn:
        row = (
            await conn.execute_core(
                select(registered_tables.c.domain_id).where(
                    registered_tables.c.id == saved.ids[name]
                )
            )
        ).fetchone()
    return row[0]


async def _sent_to_finance(saved: Any, name: str) -> Any:
    edited = await _input(saved.ids[name])
    edited.domain_id = "finance"
    return edited


async def _save(op: str, role: str, edited: Any) -> Any:
    if op == "update":
        return await M().update_table(_info(role), edited)
    return await M().register_table(_info(role), copy.deepcopy(edited))


@pytest.mark.parametrize("op", ["update", "register"])
@pytest.mark.parametrize("name", ["orders", "sales_view"])
async def test_a_table_or_view_leaves_a_domain_only_by_someone_who_may_act_in_it(
    two_domains, op, name
):
    edited = await _sent_to_finance(two_domains, name)
    with pytest.raises(PermissionError, match="sales"):
        await _save(op, "finance_editor", edited)
    assert await _domain(two_domains, name) == "sales"


@pytest.mark.parametrize("op", ["update", "register"])
async def test_a_view_leaves_a_domain_by_someone_who_may_act_in_both(two_domains, op):
    edited = await _sent_to_finance(two_domains, "sales_view")
    result = await _save(op, "sales_finance_editor", edited)
    assert result.success is True, result.message
    assert await _domain(two_domains, "sales_view") == "finance"


async def test_a_save_that_keeps_the_domain_checks_only_that_domain(two_domains):
    edited = await _input(two_domains.ids["invoices"])
    edited.description = "changed"
    result = await M().update_table(_info("finance_editor"), edited)
    assert result.success is True, result.message
