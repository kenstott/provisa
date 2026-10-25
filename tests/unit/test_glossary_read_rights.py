# Copyright (c) 2026 Kenneth Stott
# Canary: baedee14-ebe9-4d35-99db-28df13509f7e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The glossary names and binds only columns and tables its caller reads (REQ-2152).

A term is prose its domains' people read, but the columns and tables it is bound to are model
names: a term's refs, its counts and the tables it grounds are answered as the caller is served
them, and a ref or a grounding on a column or table the caller is not served is answered as one
that does not exist.

The model: ``sales.orders`` (served: id) and ``hr.payroll`` (not served); the term ``Key`` is
bound to orders.id and payroll.salary.
"""

# Requirements: REQ-2152

from __future__ import annotations

import types
from typing import Any, cast

import pytest
from sqlalchemy import insert

from provisa.api.admin import glossary_router
from provisa.api.admin.read_rights import DependentsReader
from provisa.api.errors import ApiError
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, sources

pytestmark = pytest.mark.asyncio

_REQUEST = cast(Any, types.SimpleNamespace(state=types.SimpleNamespace()))


@pytest.fixture
async def model(control_plane, monkeypatch):
    db = control_plane.org()
    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        for domain_id in ("sales", "hr"):
            await conn.execute_core(insert(domains).values(id=domain_id))
        for key, domain_id, column in (("orders", "sales", "id"), ("payroll", "hr", "salary")):
            table_id = await table_repo.upsert(
                conn,
                Table(
                    source_id="pg",
                    domain_id=domain_id,
                    schema_name="public",
                    table_name=key,
                    columns=[Column(name=column, visible_to=[], data_type="integer")],
                ),
            )
            assert table_id is not None
            ids[key] = table_id
        term = await glossary_repo.get_term_by_ref(conn, ids["orders"], "id")
        assert term is not None
        ids["term"] = term["id"]
        await glossary_repo.move_ref(conn, ids["payroll"], "salary", ids["term"])

    async def _pool():
        return db

    reader = DependentsReader(
        column=lambda table_id, column: table_id == ids["orders"] and column in (None, "id"),
        metric=lambda row: True,
        term=lambda term_domains: True,
        command=lambda name: True,
    )
    monkeypatch.setattr(glossary_router, "_pool", _pool)
    monkeypatch.setattr(glossary_router, "_require_glossary_read", lambda request: None)
    monkeypatch.setattr(glossary_router, "_require_glossary_rw", lambda request: None)
    monkeypatch.setattr(glossary_router, "_require_table_registration", lambda request: None)
    monkeypatch.setattr(glossary_router, "_require_domain", lambda request, domain: None)
    monkeypatch.setattr(glossary_router, "require_active_org_id", lambda request: "acme")
    monkeypatch.setattr(glossary_router, "_authority", lambda request: None)
    monkeypatch.setattr("provisa.api.admin.read_rights.reader_for_request", lambda r: reader)
    return db, ids


async def test_a_term_names_and_counts_only_the_refs_its_caller_is_served(model):
    _db, ids = model
    term = await glossary_router.get_term(_REQUEST, ids["term"])
    assert [(r["table_name"], r["column_name"]) for r in term["refs"]] == [("orders", "id")]
    assert term["ref_count"] == 1


async def test_the_term_list_counts_only_the_refs_its_caller_is_served(model):
    _db, ids = model
    listed = {
        t["id"]: t
        for t in await glossary_router.list_terms(
            _REQUEST, q=None, include_deprecated=True, domains=None
        )
    }
    assert listed[ids["term"]]["ref_count"] == 1


async def test_a_ref_the_caller_is_not_served_is_answered_as_missing(model):
    _db, ids = model
    found = await glossary_router.term_for_ref(_REQUEST, ids["orders"], "id")
    assert found["id"] == ids["term"]
    with pytest.raises(ApiError) as unserved:
        await glossary_router.term_for_ref(_REQUEST, ids["payroll"], "salary")
    assert unserved.value.code == "glossary.ref_not_found"


async def test_a_table_the_caller_is_not_served_is_not_grounded(model):
    db, ids = model
    async with db.acquire() as conn:
        with pytest.raises(ApiError) as unserved:
            await glossary_router.ground_term_table(conn, ids["term"], ids["payroll"], _REQUEST)
    assert unserved.value.code == "glossary.table_not_found"
