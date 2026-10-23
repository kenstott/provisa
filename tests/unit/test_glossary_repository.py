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
from sqlalchemy import insert, select

from provisa.api.admin.column_dependents import _glossary_dependents

from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, registered_tables, sources


@asynccontextmanager
async def _conn(control_plane):
    with control_plane.engine().begin() as c:
        c.exec_driver_sql('SET LOCAL search_path TO "org_default"')
        c.execute(insert(domains).values(id="d", description=""))
        c.execute(insert(sources).values(id="__derived__", type="postgresql"))
    async with control_plane.org().acquire() as conn:
        yield conn


def _tbl(name: str, columns: list[str]) -> Table:
    return Table(
        source_id="__derived__",
        domain_id="d",
        schema_name="s",
        table_name=name,
        columns=[Column(name=c, data_type="text", visible_to=[]) for c in columns],
        view_sql="SELECT 1",
    )


async def _term(conn, name: str) -> dict | None:
    terms = await glossary_repo.list_terms(conn)
    return next((t for t in terms if t["name"] == name), None)


@pytest.mark.asyncio
async def test_generic_columns_qualify_with_the_table_and_machinery_derives_nothing(control_plane):
    async with _conn(control_plane) as conn:
        await table_repo.upsert(
            conn,
            Table(
                source_id="__derived__",
                domain_id="d",
                schema_name="s",
                table_name="employees",
                columns=[
                    Column(name="first_name", data_type="text", visible_to=[]),
                    Column(name="id", data_type="integer", visible_to=[]),
                    # Native-filter pseudo-columns are query machinery, not business fields.
                    Column(
                        name="_nf_region",
                        data_type="text",
                        visible_to=[],
                        native_filter_type="query_param",
                        native_filter_required=False,
                    ),
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
async def test_registration_creates_and_dedups_terms(control_plane):
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id", "order_dt"]))
        await table_repo.upsert(conn, _tbl("customers", ["customerId", "CUSTOMER_KEY"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        assert customer["ref_count"] == 3
        order_date = await _term(conn, "order date")
        assert order_date is not None and order_date["ref_count"] == 1


@pytest.mark.asyncio
async def test_column_departure_removes_term_without_abstract_dependents(control_plane):
    async with _conn(control_plane) as conn:
        tid = await table_repo.upsert(conn, _tbl("orders", ["cust_id", "order_dt"]))
        assert tid is not None
        await table_repo.upsert(conn, _tbl("orders", ["cust_id"]))
        assert await _term(conn, "order date") is None
        assert await _term(conn, "customer") is not None


@pytest.mark.asyncio
async def test_last_ref_removal_deprecates_when_abstract_term_would_dangle(control_plane):
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["order_dt"]))
        rooted = await _term(conn, "order date")
        assert rooted is not None
        abstract_id = await glossary_repo.create_abstract_term(conn, "business date", domains=set())
        await glossary_repo.add_edge(conn, abstract_id, rooted["id"], "KIND_OF")
        await table_repo.upsert(conn, _tbl("orders", ["placed_ts"]))
        kept = await _term(conn, "order date")
        assert kept is not None and kept["deprecated"] and kept["ref_count"] == 0
        assert (await _term(conn, "business date")) is not None


@pytest.mark.asyncio
async def test_create_abstract_term_lowercases_the_name(control_plane):  # REQ-1844
    # Every term in this catalog is lowercase — auto-derived ones already are (normalize_term
    # case-folds); a manually-created one that wasn't would be the one inconsistent entry, and
    # would silently invite a same-word duplicate differing only in case.
    async with _conn(control_plane) as conn:
        term_id = await glossary_repo.create_abstract_term(conn, "Sepal Length", domains=set())
        term = await glossary_repo.get_term(conn, term_id)
        assert term is not None
        assert term["name"] == "sepal length"


@pytest.mark.asyncio
async def test_create_abstract_term_rejects_a_case_only_duplicate(control_plane):  # REQ-1844
    async with _conn(control_plane) as conn:
        await glossary_repo.create_abstract_term(conn, "sepal length", domains=set())
        with pytest.raises(ValueError):
            await glossary_repo.create_abstract_term(conn, "Sepal Length", domains=set())


@pytest.mark.asyncio
async def test_rename_term_lowercases_the_new_name(control_plane):  # REQ-1844
    async with _conn(control_plane) as conn:
        term_id = await glossary_repo.create_abstract_term(conn, "party", domains=set())
        await glossary_repo.rename_term(conn, term_id, "Legal Entity")
        term = await glossary_repo.get_term(conn, term_id)
        assert term is not None
        assert term["name"] == "legal entity"


@pytest.mark.asyncio
async def test_abstract_term_with_alternative_root_path_does_not_block_removal(control_plane):
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["order_dt", "ship_dt"]))
        order_date = await _term(conn, "order date")
        ship_date = await _term(conn, "ship date")
        assert order_date is not None and ship_date is not None
        abstract_id = await glossary_repo.create_abstract_term(conn, "business date", domains=set())
        await glossary_repo.add_edge(conn, abstract_id, order_date["id"], "KIND_OF")
        await glossary_repo.add_edge(conn, abstract_id, ship_date["id"], "KIND_OF")
        await table_repo.upsert(conn, _tbl("orders", ["ship_dt"]))
        # The curator wired order date into the abstract term, so losing its column
        # deprecates it rather than deleting the relationship out from under them —
        # and the abstract term keeps its own grounding through ship date regardless.
        stale = await _term(conn, "order date")
        assert stale is not None and bool(stale["deprecated"]) is True
        assert (await _term(conn, "business date")) is not None


@pytest.mark.asyncio
async def test_relink_revives_deprecated_term(control_plane):
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["order_dt"]))
        rooted = await _term(conn, "order date")
        assert rooted is not None
        abstract_id = await glossary_repo.create_abstract_term(conn, "business date", domains=set())
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
async def test_table_delete_sweeps_terms(control_plane):
    async with _conn(control_plane) as conn:
        tid = await table_repo.upsert(conn, _tbl("orders", ["order_dt"]))
        assert tid is not None
        assert await _term(conn, "order date") is not None
        await table_repo.delete(conn, tid)
        assert await _term(conn, "order date") is None


@pytest.mark.asyncio
async def test_move_ref_settles_the_losing_term(control_plane):
    async with _conn(control_plane) as conn:
        tid = await table_repo.upsert(conn, _tbl("orders", ["cust_id", "buyer_id"]))
        customer = await _term(conn, "customer")
        buyer = await _term(conn, "buyer")
        assert tid is not None and customer is not None and buyer is not None
        moved = await glossary_repo.move_ref(conn, tid, "buyer_id", customer["id"])
        # The losing term's fate is reported so the caller does not re-read a deleted term.
        assert moved == {"source_term_removed": True}
        assert await _term(conn, "buyer") is None  # lost its last ref, nothing dangles
        winner = await _term(conn, "customer")
        assert winner is not None and winner["ref_count"] == 2


@pytest.mark.asyncio
async def test_move_ref_reports_a_surviving_source_term_and_a_missing_ref(control_plane):
    async with _conn(control_plane) as conn:
        tid = await table_repo.upsert(conn, _tbl("orders", ["cust_id", "buyer_id"]))
        a = await table_repo.upsert(conn, _tbl("invoices", ["cust_id"]))
        buyer = await _term(conn, "buyer")
        assert tid is not None and a is not None and buyer is not None
        # "customer" still holds invoices.cust_id afterwards, so it survives the settle.
        customer = await _term(conn, "customer")
        assert customer is not None
        moved = await glossary_repo.move_ref(conn, tid, "cust_id", buyer["id"])
        assert moved == {"source_term_removed": False}
        assert await _term(conn, "customer") is not None
        assert await glossary_repo.move_ref(conn, tid, "no_such_col", buyer["id"]) is None


@pytest.mark.asyncio
async def test_edge_types_are_a_closed_set(control_plane):
    async with _conn(control_plane) as conn:
        a = await glossary_repo.create_abstract_term(conn, "party", domains=set())
        b = await glossary_repo.create_abstract_term(conn, "person", domains=set())
        with pytest.raises(ValueError):
            await glossary_repo.add_edge(conn, a, b, "LOOSELY_EVOKES")
        with pytest.raises(ValueError):
            await glossary_repo.add_edge(conn, a, a, "KIND_OF")
        await glossary_repo.add_edge(conn, b, a, "KIND_OF")


@pytest.mark.asyncio
async def test_rooted_term_cannot_be_deleted_by_hand(control_plane):
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        with pytest.raises(ValueError):
            await glossary_repo.delete_term(conn, customer["id"])


@pytest.mark.asyncio
async def test_curation_round_trip(control_plane):
    async with _conn(control_plane) as conn:
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
async def test_search_terms_matches_name_and_definition(control_plane):
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id", "order_dt"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        await glossary_repo.set_definition(conn, customer["id"], "The buying organization.")
        by_name = await glossary_repo.search_terms(conn, "custom")
        assert [t["name"] for t in by_name] == ["customer"]
        by_definition = await glossary_repo.search_terms(conn, "buying organization")
        assert [t["name"] for t in by_definition] == ["customer"]


@pytest.mark.asyncio
async def test_retired_and_deprecated_terms_are_withheld_from_agent_search(control_plane):
    """A retired term keeps its refs but must be unreachable through the MCP term search —
    that surface is how an agent binds a question to a column."""
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id", "order_dt"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        # Derived and still undefined: a proposal, not vocabulary an agent may ground on.
        assert await glossary_repo.search_terms(conn, "custom") == []
        await glossary_repo.set_definition(conn, customer["id"], "The buyer.")
        assert [t["name"] for t in await glossary_repo.search_terms(conn, "custom")] == ["customer"]

        # REQ-2144: retiring moves its column to the successor; the term is kept as a record.
        client = await glossary_repo.create_abstract_term(
            conn, "client", definition="One who buys.", domains=set()
        )
        await glossary_repo.retire_term(
            conn, customer["id"], reason="renamed", successor=client, by=None
        )
        assert await glossary_repo.search_terms(conn, "custom") == []
        detail = await glossary_repo.get_term(conn, customer["id"])
        assert detail is not None and detail["refs"] == [] and detail["status"] == "retired"
        assert [r["column_name"] for r in (await glossary_repo.get_term(conn, client))["refs"]] == [
            "cust_id"
        ]

        # Restored, it comes back with its record and nothing attached: a draft Concept.
        assert await glossary_repo.restore_term(conn, customer["id"], by=None)
        assert (await glossary_repo.get_term(conn, customer["id"]))["status"] == "draft"
        assert await glossary_repo.search_terms(conn, "custom") == []


@pytest.mark.asyncio
async def test_abstract_term_is_offered_only_while_a_chain_reaches_a_physical_ref(control_plane):
    """An abstract term names data only through its relationships: every chain must terminate
    at a term holding a physical ref, or it answers nothing and the search surface withholds
    it. Connectivity ignores definitions — the undefined rooted term still grounds it — but
    out-of-service terms do not conduct."""
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["order_dt"]))
        rooted = await _term(conn, "order date")
        assert rooted is not None
        abstract_id = await glossary_repo.create_abstract_term(
            conn, "business date", definition="A date the business reasons about.", domains=set()
        )
        # Defined, in service, but wired to nothing: no chain, no data, not offered.
        assert await glossary_repo.search_terms(conn, "business date") == []

        await glossary_repo.add_edge(conn, abstract_id, rooted["id"], "KIND_OF")
        assert [t["name"] for t in await glossary_repo.search_terms(conn, "business date")] == [
            "business date"
        ]

        # Retiring the only term on the chain breaks the grounding: a withdrawn term must not
        # keep its dependents reachable (its column goes to the successor; its edges stay).
        successor = await glossary_repo.create_abstract_term(conn, "placed on", domains=set())
        await glossary_repo.retire_term(
            conn, rooted["id"], reason="split", successor=successor, by=None
        )
        assert await glossary_repo.search_terms(conn, "business date") == []


@pytest.mark.asyncio
async def test_column_removal_warning_states_each_terms_fate(control_plane):
    """The pre-save warning (REQ-1484) must say what removing the column does to each bound
    term, because the settle rule has already decided it: kept, kept-out-of-service, deleted."""
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id", "order_dt", "ship_dt"]))
        table_id = (await conn.execute_core(select(registered_tables.c.id))).fetchone().id
        order_date = await _term(conn, "order date")
        customer = await _term(conn, "customer")
        assert order_date is not None and customer is not None
        await glossary_repo.set_definition(conn, order_date["id"], "When the order was placed.")
        # customer is bound to cust_id alone and blank; ship date is blank too but the term is
        # shared with nothing, so each column tells a different story.
        fates = {
            d.name: d.detail
            for col in ("cust_id", "order_dt")
            for d in await _glossary_dependents(conn, table_id, col)
        }
        assert fates == {
            "customer": "bound term, derived and blank — deleted with the column",
            "order date": "bound term, curated — kept but taken out of service",
        }

        # A term bound to a second column loses nothing at all.
        await table_repo.upsert(conn, _tbl("invoices", ["cust_id"]))
        again = await _glossary_dependents(conn, table_id, "cust_id")
        assert [d.detail for d in again] == ["bound term, keeps its other columns"]


@pytest.mark.asyncio
async def test_retirement_survives_re_registration(control_plane):
    """Re-registering the table leaves the retirement alone: nothing in the derived lifecycle
    writes ``retired``, unlike ``deprecated``, which _find_or_create_term clears whenever a
    matching column reappears. The column stays on the curated successor it moved to."""
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id", "order_dt"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        client = await glossary_repo.create_abstract_term(
            conn, "client", definition="One who buys.", domains=set()
        )
        await glossary_repo.retire_term(
            conn, customer["id"], reason="renamed", successor=client, by=None
        )
        await table_repo.upsert(conn, _tbl("orders", ["cust_id", "order_dt"]))
        still = await _term(conn, "customer")
        assert still is not None and still["id"] == customer["id"]
        assert bool(still["retired"]) is True and bool(still["deprecated"]) is False
        assert still["ref_count"] == 0
        assert await glossary_repo.search_terms(conn, "custom") == []


@pytest.mark.asyncio
async def test_delete_term_names_retire_as_the_remedy(control_plane):
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        with pytest.raises(ValueError, match="retire it"):
            await glossary_repo.delete_term(conn, customer["id"])


@pytest.mark.asyncio
async def test_export_graph_shape(control_plane):
    async with _conn(control_plane) as conn:
        await table_repo.upsert(conn, _tbl("orders", ["cust_id"]))
        customer = await _term(conn, "customer")
        assert customer is not None
        abstract_id = await glossary_repo.create_abstract_term(conn, "party", domains=set())
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


def test_a_search_is_its_quoted_phrases_and_its_words():
    assert glossary_repo.search_items('"shift coverage" clinic') == ["shift coverage", "clinic"]
    assert glossary_repo.search_items("  Shift   CLINIC ") == ["shift", "clinic"]
    # A stray quote mark is dropped; an empty phrase is nothing.
    assert glossary_repo.search_items('shift" "" clinic') == ["shift", "clinic"]
    assert glossary_repo.search_items("") == []


@pytest.mark.asyncio
async def test_a_search_admits_a_term_matching_any_of_its_items(control_plane):
    async with _conn(control_plane) as conn:
        for name in ("shift coverage", "shift start", "clinic", "referral"):
            await glossary_repo.create_abstract_term(conn, name, domains=set())

        async def names(q: str) -> set[str]:
            return {t["name"] for t in await glossary_repo.list_terms(conn, q=q)}

        assert await names("shift clinic") == {"shift coverage", "shift start", "clinic"}
        # A quoted phrase is one item, spaces and all, beside a bare word.
        assert await names('"shift coverage" clinic') == {"shift coverage", "clinic"}
