# Copyright (c) 2026 Kenneth Stott
# Canary: 9c4d2a7e-51b8-46f3-bd0a-3e7c8f1a6b94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1591: which domains a glossary term belongs to, and who may therefore reach it.

A rooted term's domains are DERIVED from the tables its refs point at; an abstract term
DECLARES them. Both are answered by one repository function, and one ANY predicate decides
reachability for reading and for curating alike. The stamp is the interesting case: a rooted
term's domains stop being derivable the moment its last ref departs, so the departing domains
are recorded at that moment rather than lost.
"""

# Requirements: REQ-1591

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.glossary import within_domains
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    glossary_term_domains,
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_refs,
    glossary_terms,
    registered_tables,
    roles,
    table_columns,
)


_TABLES = [
    registered_tables,
    table_columns,
    roles,
    glossary_terms,
    glossary_term_refs,
    glossary_term_edges,
    glossary_term_experts,
    glossary_term_domains,
]


@asynccontextmanager
async def _conn(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'gd.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: registered_tables.metadata.create_all(s, tables=_TABLES))
    try:
        async with Database(engine, name="gd").acquire() as conn:
            yield conn
    finally:
        await engine.dispose()


def _tbl(domain: str, name: str, columns: list[str]) -> Table:
    return Table(
        source_id="__derived__",
        domain_id=domain,
        schema_name="s",
        table_name=name,
        columns=[Column(name=c, data_type="text", visible_to=[]) for c in columns],
        view_sql="SELECT 1",
    )


async def _id(conn, name: str) -> int:
    terms = await glossary_repo.list_terms(conn)
    return next(t["id"] for t in terms if t["name"] == name)


def test_within_domains_is_any_not_all():
    # The deliberate asymmetry with REQ-1531's require_domains: either domain of a shared term
    # suffices, for reading and for curating alike.
    assert within_domains(frozenset({"sales"}), {"sales", "pet-store"}) is True
    assert within_domains(frozenset({"pet-store"}), {"sales", "pet-store"}) is True
    assert within_domains(frozenset({"hr"}), {"sales", "pet-store"}) is False
    # None is an unlimited caller, not a missing value; an unscoped term is reachable by all.
    assert within_domains(None, {"sales"}) is True
    assert within_domains(frozenset(), set()) is True
    assert within_domains(frozenset(), {"sales"}) is False


@pytest.mark.asyncio
async def test_a_rooted_terms_domains_are_its_refs(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("sales", "orders", ["customer_id"]))
        scope = await glossary_repo.term_domains(conn)
        assert scope[await _id(conn, "customer")] == {"sales"}


@pytest.mark.asyncio
async def test_one_term_spans_two_domains_rather_than_splitting(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("sales", "orders", ["customer_id"]))
        await table_repo.upsert(conn, _tbl("pet-store", "visits", ["customer_id"]))
        customer = await _id(conn, "customer")
        # ONE term scoped to both, not two terms — _find_or_create_term matches by name.
        assert (await glossary_repo.term_domains(conn))[customer] == {"sales", "pet-store"}
        rows = await glossary_repo.list_terms(conn, domains=frozenset({"pet-store"}))
        assert [r["name"] for r in rows if r["id"] == customer] == ["customer"]
        detail = await glossary_repo.get_term(conn, customer)
        assert detail is not None
        assert detail["domains"] == ["pet-store", "sales"]


@pytest.mark.asyncio
async def test_an_abstract_terms_domains_are_declared(tmp_path):
    async with _conn(tmp_path) as conn:
        party = await glossary_repo.create_abstract_term(
            conn, "party", definition="Any actor.", domains={"sales"}
        )
        assert (await glossary_repo.term_domains(conn))[party] == {"sales"}
        # A declaration is replaced wholesale, not merged.
        await glossary_repo.set_declared_domains(conn, party, {"hr"})
        assert (await glossary_repo.term_domains(conn))[party] == {"hr"}


@pytest.mark.asyncio
async def test_the_list_narrows_by_domain(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("sales", "orders", ["order_dt"]))
        await table_repo.upsert(conn, _tbl("hr", "staff", ["hire_dt"]))
        names = {r["name"] for r in await glossary_repo.list_terms(conn, domains=frozenset({"hr"}))}
        assert names == {"hire date"}
        # None narrows nothing — unlimited, distinct from limited-to-nothing.
        assert {r["name"] for r in await glossary_repo.list_terms(conn, domains=None)} == {
            "order date",
            "hire date",
        }
        assert await glossary_repo.list_terms(conn, domains=frozenset()) == []


@pytest.mark.asyncio
async def test_search_narrows_by_the_same_rule(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("sales", "orders", ["customer_id"]))
        customer = await _id(conn, "customer")
        await glossary_repo.set_definition(conn, customer, "The buyer.")
        assert await glossary_repo.search_terms(conn, "customer", domains=frozenset({"hr"})) == []
        hits = await glossary_repo.search_terms(conn, "customer", domains=frozenset({"sales"}))
        assert [h["name"] for h in hits] == ["customer"]


@pytest.mark.asyncio
async def test_the_departing_domains_are_stamped_when_the_last_ref_leaves(tmp_path):
    async with _conn(tmp_path) as conn:
        tid = await table_repo.upsert(conn, _tbl("sales", "orders", ["customer_id"]))
        assert tid is not None
        customer = await _id(conn, "customer")
        await glossary_repo.set_definition(conn, customer, "The buyer.")
        # The table departs: the term is deprecated rather than removed (it carries a definition),
        # and its domains are no longer derivable — so they must have been recorded.
        await table_repo.delete(conn, tid)
        detail = await glossary_repo.get_term(conn, customer)
        assert detail is not None
        assert bool(detail["deprecated"]) is True
        assert detail["refs"] == []
        assert detail["domains"] == ["sales"]
        rows = await glossary_repo.list_terms(conn, domains=frozenset({"sales"}))
        assert [r["name"] for r in rows] == ["customer"]
