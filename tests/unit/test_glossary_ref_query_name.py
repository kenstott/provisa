# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9d51a7-8e24-4f0b-a6d2-71b5e0c84f19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product's consumer view finds a column's glossary term by the name it is queried by.

A contract names each column by its SQL alias where one is set, otherwise its column name
(REQ-2158, amended 2026-11-25). The glossary is keyed on the physical column, so the hover's
lookup resolves the consumer's name to its column first.

The model: ``sales.orders`` with ``cust_id`` aliased ``customer``, ``total`` unaliased, and
``customer`` aliased ``buyer`` -- its physical name is another column's query name.
"""

# Requirements: REQ-2158, REQ-1387

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
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        table_id = await table_repo.upsert(
            conn,
            Table(
                source_id="pg",
                domain_id="sales",
                schema_name="public",
                table_name="orders",
                columns=[
                    Column(name="cust_id", alias="customer", visible_to=[], data_type="integer"),
                    Column(name="total", visible_to=[], data_type="integer"),
                    Column(name="customer", alias="buyer", visible_to=[], data_type="text"),
                ],
            ),
        )
        assert table_id is not None
        terms = {}
        for column in ("cust_id", "total", "customer"):
            term = await glossary_repo.get_term_by_ref(conn, table_id, column)
            assert term is not None
            terms[column] = term["id"]

    async def _pool():
        return db

    reader = DependentsReader(
        column=lambda t, column: t == table_id,
        metric=lambda row: True,
        term=lambda term_domains: True,
        command=lambda name: True,
    )
    monkeypatch.setattr(glossary_router, "_pool", _pool)
    monkeypatch.setattr(glossary_router, "_require_table_registration", lambda request: None)
    monkeypatch.setattr(glossary_router, "_require_domain", lambda request, domain: None)
    monkeypatch.setattr(glossary_router, "require_active_org_id", lambda request: "acme")
    monkeypatch.setattr("provisa.api.admin.read_rights.reader_for_request", lambda r: reader)
    return db, table_id, terms


async def test_a_query_name_resolves_to_the_column_it_names(model):
    db, table_id, _terms = model
    async with db.acquire() as conn:
        assert await glossary_repo.column_queried_as(conn, table_id, "customer") == "cust_id"
        assert await glossary_repo.column_queried_as(conn, table_id, "total") == "total"
        assert await glossary_repo.column_queried_as(conn, table_id, "buyer") == "customer"
        assert await glossary_repo.column_queried_as(conn, table_id, "cust_id") is None
        assert await glossary_repo.column_queried_as(conn, table_id, "nope") is None


async def test_the_hover_finds_an_aliased_columns_term_by_its_query_name(model):
    _db, table_id, terms = model
    found = await glossary_router.term_for_ref(_REQUEST, table_id, query_name="customer")
    assert found["id"] == terms["cust_id"]
    found = await glossary_router.term_for_ref(_REQUEST, table_id, query_name="buyer")
    assert found["id"] == terms["customer"]
    found = await glossary_router.term_for_ref(_REQUEST, table_id, query_name="total")
    assert found["id"] == terms["total"]


async def test_the_physical_name_still_finds_its_own_term(model):
    _db, table_id, terms = model
    found = await glossary_router.term_for_ref(_REQUEST, table_id, column_name="customer")
    assert found["id"] == terms["customer"]


async def test_a_physical_name_hidden_behind_an_alias_is_no_query_name(model):
    _db, table_id, _terms = model
    with pytest.raises(ApiError) as missing:
        await glossary_router.term_for_ref(_REQUEST, table_id, query_name="cust_id")
    assert missing.value.code == "glossary.ref_not_found"


async def test_exactly_one_name_is_given(model):
    _db, table_id, _terms = model
    for kwargs in ({}, {"column_name": "total", "query_name": "total"}):
        with pytest.raises(ApiError) as bad:
            await glossary_router.term_for_ref(_REQUEST, table_id, **kwargs)
        assert bad.value.code == "glossary.invalid"
