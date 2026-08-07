# Copyright (c) 2026 Kenneth Stott
# Canary: 2f8a6c14-9e5b-4d7a-8b3f-6c1d9e0a4f22
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1387: glossary term lifecycle derived from semantic-layer membership.

A real SQLite tenant DB through the table repository — the lifecycle under test IS the
table upsert/delete choke point: registration creates/links terms, departure applies the
remove-or-deprecate rule, and an abstract term is never left dangling.
"""

# Requirements: REQ-1387

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from provisa.core.database import Database
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
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
]


@asynccontextmanager
async def _conn(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'g.db'}")
    async with engine.begin() as c:
        await c.run_sync(lambda s: registered_tables.metadata.create_all(s, tables=_TABLES))
    try:
        async with Database(engine, name="g").acquire() as conn:
            yield conn
    finally:
        await engine.dispose()


def _tbl(name: str, columns: list[str]) -> Table:
    return Table(
        source_id="__derived__",
        domain_id="d",
        schema_name="s",
        table_name=name,
        columns=[Column(name=c, visible_to=[]) for c in columns],
        view_sql="SELECT 1",
    )


async def _term(conn, name: str) -> dict | None:
    terms = await glossary_repo.list_terms(conn)
    return next((t for t in terms if t["name"] == name), None)


@pytest.mark.asyncio
async def test_generic_columns_qualify_with_the_table_and_machinery_derives_nothing(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(
            conn,
            Table(
                source_id="__derived__",
                domain_id="d",
                schema_name="s",
                table_name="employees",
                columns=[
                    Column(name="first_name", visible_to=[]),
                    Column(name="id", visible_to=[]),
                    # Native-filter pseudo-columns are query machinery, not business fields.
                    Column(name="_nf_region", visible_to=[], native_filter_type="query_param"),
                ],
                view_sql="SELECT 1",
            ),
        )
        terms = await glossary_repo.list_terms(conn)
        names = {t["name"] for t in terms}
    # employees.id collapses to the concept it identifies — same term FK columns
    # (employee_id elsewhere) land on.
    assert names == {"employee first name", "employee"}


@pytest.mark.asyncio
async def test_registration_creates_and_dedups_terms(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id", "order_dt"]))
        await table_repo.upsert(conn, _tbl("customers", ["customerId", "CUSTOMER_KEY"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        assert customer["ref_count"] == 3
        order_date = await _term(conn, "order date")
        assert order_date is not None and order_date["ref_count"] == 1


@pytest.mark.asyncio
async def test_column_departure_removes_term_without_abstract_dependents(tmp_path):
    async with _conn(tmp_path) as conn:
        tid = await table_repo.upsert(conn, _tbl("orders", ["cust_id", "order_dt"]))
        assert tid is not None
        await table_repo.upsert(conn, _tbl("orders", ["cust_id"]))
        assert await _term(conn, "order date") is None
        assert await _term(conn, "customer") is not None


@pytest.mark.asyncio
async def test_last_ref_removal_deprecates_when_abstract_term_would_dangle(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["order_dt"]))
        rooted = await _term(conn, "order date")
        assert rooted is not None
        abstract_id = await glossary_repo.create_abstract_term(conn, "business date")
        await glossary_repo.add_edge(conn, abstract_id, rooted["id"], "KIND_OF")
        await table_repo.upsert(conn, _tbl("orders", ["placed_ts"]))
        kept = await _term(conn, "order date")
        assert kept is not None and kept["deprecated"] and kept["ref_count"] == 0
        assert (await _term(conn, "business date")) is not None


@pytest.mark.asyncio
async def test_abstract_term_with_alternative_root_path_does_not_block_removal(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["order_dt", "ship_dt"]))
        order_date = await _term(conn, "order date")
        ship_date = await _term(conn, "ship date")
        assert order_date is not None and ship_date is not None
        abstract_id = await glossary_repo.create_abstract_term(conn, "business date")
        await glossary_repo.add_edge(conn, abstract_id, order_date["id"], "KIND_OF")
        await glossary_repo.add_edge(conn, abstract_id, ship_date["id"], "KIND_OF")
        await table_repo.upsert(conn, _tbl("orders", ["ship_dt"]))
        # The abstract term still reaches a rooted term through ship date, so order date
        # is removed outright rather than deprecated.
        assert await _term(conn, "order date") is None
        assert (await _term(conn, "business date")) is not None


@pytest.mark.asyncio
async def test_relink_revives_deprecated_term(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["order_dt"]))
        rooted = await _term(conn, "order date")
        assert rooted is not None
        abstract_id = await glossary_repo.create_abstract_term(conn, "business date")
        await glossary_repo.add_edge(conn, abstract_id, rooted["id"], "KIND_OF")
        await table_repo.upsert(conn, _tbl("orders", []))
        deprecated = await _term(conn, "order date")
        assert deprecated is not None and deprecated["deprecated"]
        await table_repo.upsert(conn, _tbl("orders", ["order_dt"]))
        revived = await _term(conn, "order date")
        assert revived is not None
        assert revived["deprecated"] is False or revived["deprecated"] == 0
        assert revived["ref_count"] == 1
        assert revived["id"] == rooted["id"]  # same term row: definition/edges survive


@pytest.mark.asyncio
async def test_table_delete_sweeps_terms(tmp_path):
    async with _conn(tmp_path) as conn:
        tid = await table_repo.upsert(conn, _tbl("orders", ["order_dt"]))
        assert tid is not None
        assert await _term(conn, "order date") is not None
        await table_repo.delete(conn, tid)
        assert await _term(conn, "order date") is None


@pytest.mark.asyncio
async def test_move_ref_settles_the_losing_term(tmp_path):
    async with _conn(tmp_path) as conn:
        tid = await table_repo.upsert(conn, _tbl("orders", ["cust_id", "buyer_id"]))
        customer = await _term(conn, "customer")
        buyer = await _term(conn, "buyer")
        assert tid is not None and customer is not None and buyer is not None
        moved = await glossary_repo.move_ref(conn, tid, "buyer_id", customer["id"])
        assert moved
        assert await _term(conn, "buyer") is None  # lost its last ref, nothing dangles
        winner = await _term(conn, "customer")
        assert winner is not None and winner["ref_count"] == 2


@pytest.mark.asyncio
async def test_edge_types_are_a_closed_set(tmp_path):
    async with _conn(tmp_path) as conn:
        a = await glossary_repo.create_abstract_term(conn, "party")
        b = await glossary_repo.create_abstract_term(conn, "person")
        with pytest.raises(ValueError):
            await glossary_repo.add_edge(conn, a, b, "LOOSELY_EVOKES")
        with pytest.raises(ValueError):
            await glossary_repo.add_edge(conn, a, a, "KIND_OF")
        await glossary_repo.add_edge(conn, b, a, "KIND_OF")


@pytest.mark.asyncio
async def test_rooted_term_cannot_be_deleted_by_hand(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        with pytest.raises(ValueError):
            await glossary_repo.delete_term(conn, customer["id"])


@pytest.mark.asyncio
async def test_curation_round_trip(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        assert await glossary_repo.rename_term(conn, customer["id"], "client")
        assert await glossary_repo.set_definition(conn, customer["id"], "A paying party.")
        await glossary_repo.add_expert(conn, customer["id"], "alice", kind="author")
        detail = await glossary_repo.get_term(conn, customer["id"])
        assert detail is not None
        assert detail["name"] == "client"
        assert detail["definition"] == "A paying party."
        assert detail["experts"] == [{"user_id": "alice", "kind": "author"}]
        assert detail["refs"][0]["column_name"] == "cust_id"
        assert await glossary_repo.remove_expert(conn, customer["id"], "alice")


@pytest.mark.asyncio
async def test_search_terms_matches_name_and_definition(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id", "order_dt"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        await glossary_repo.set_definition(conn, customer["id"], "The buying organization.")
        by_name = await glossary_repo.search_terms(conn, "custom")
        assert [t["name"] for t in by_name] == ["customer"]
        by_definition = await glossary_repo.search_terms(conn, "buying organization")
        assert [t["name"] for t in by_definition] == ["customer"]


@pytest.mark.asyncio
async def test_export_graph_shape(tmp_path):
    async with _conn(tmp_path) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        abstract_id = await glossary_repo.create_abstract_term(conn, "party")
        await glossary_repo.add_edge(conn, customer["id"], abstract_id, "KIND_OF")
        await glossary_repo.add_expert(conn, customer["id"], "alice")
        graph = await glossary_repo.export_graph(conn)
        assert {t["name"] for t in graph["terms"]} == {"customer", "party"}
        assert graph["refs"] == [
            {
                "term_id": customer["id"],
                "column_name": "cust_id",
                "source_id": "__derived__",
                "schema_name": "s",
                "table_name": "orders",
            }
        ]
        assert graph["edges"] == [
            {"from_term_id": customer["id"], "to_term_id": abstract_id, "rel_type": "KIND_OF"}
        ]
        assert graph["experts"] == [
            {"term_id": customer["id"], "user_id": "alice", "kind": "expert"}
        ]
