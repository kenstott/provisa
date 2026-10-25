# Copyright (c) 2026 Kenneth Stott
# Canary: b35c80e3-0800-4e23-bc6d-3233311d62b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A refused delete names only what its caller may read, and never how many (REQ-1918, REQ-2152).

A delete is refused while anything refers to the object, and the refusal stands whatever the
caller may read. It names the dependents the caller may read; when it may read none of them it
says they are outside its grants; it never counts them.

The model: source ``pg`` holds ``orders`` (served to the reader) and ``payroll`` (not).
"""

# Requirements: REQ-1918, REQ-2152

from __future__ import annotations

import pytest
from sqlalchemy import insert

from provisa.api.admin.read_rights import (
    SERVICE_READER,
    DependentsReader,
    column_drop_text,
    refusal,
)
from provisa.core.models import Column, Table
from provisa.core.repositories import source as source_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def model(control_plane):
    db = control_plane.org()
    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        for name in ("orders", "payroll"):
            table_id = await table_repo.upsert(
                conn,
                Table(
                    source_id="pg",
                    domain_id="sales",
                    schema_name="public",
                    table_name=name,
                    columns=[Column(name="id", visible_to=[], data_type="integer")],
                ),
            )
            assert table_id is not None
            ids[name] = table_id
        with pytest.raises(source_repo.SourceDeleteRefused) as refused:
            await source_repo.delete(conn, "pg")
    return db, ids, refused.value


def _serving(*table_ids: int) -> DependentsReader:
    return DependentsReader(
        column=lambda table_id, column: table_id in table_ids,
        metric=lambda row: False,
        term=lambda domains: False,
        command=lambda name: False,
    )


async def test_a_refusal_names_only_the_dependents_its_caller_may_read(model):
    db, ids, refused = model
    async with db.acquire() as conn:
        said, named = await refusal(conn, _serving(ids["orders"]), refused)
    assert [d["id"] for d in named] == [ids["orders"]]
    assert "orders" in said and "payroll" not in said
    assert "2" not in said, "no count"


async def test_a_refusal_whose_dependents_are_all_unreadable_still_stands(model):
    db, _ids, refused = model
    async with db.acquire() as conn:
        said, named = await refusal(conn, _serving(), refused)
    assert named == []
    assert said == "Source 'pg' is still referred to by objects outside your grants"


async def test_provisas_own_identity_is_told_every_dependent(model):
    db, ids, refused = model
    async with db.acquire() as conn:
        _said, named = await refusal(conn, SERVICE_READER, refused)
    assert sorted(d["id"] for d in named) == sorted(ids.values())


def test_a_column_drop_names_per_column_only_what_is_readable():
    report = {
        "total": [{"kind": "table", "id": 3, "name": "big_orders", "via": []}],
        "secret": [],
    }
    assert column_drop_text("orders", report) == (
        "Table 'orders' cannot drop column(s) that are still referred to: "
        "total (referred to by: table big_orders); "
        "secret (referred to by objects outside your grants)"
    )
