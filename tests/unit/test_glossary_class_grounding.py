# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1e9d47-3a2c-4f85-9e17-c40d2b8a5f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2130: a class term grounds whole tables and views.

A table has at most one class term and a term may ground many. Only a table with a primary key is
grounded; a table another term already grounds is refused, naming that term. The grounding is
curated under the column refs' rights, read from the meta domain, and carried by an environment's
tree with the table it grounds.
"""

# Requirements: REQ-2130

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

import pytest
from sqlalchemy import insert, select

if TYPE_CHECKING:
    from fastapi import Request

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.api.admin import glossary_router
from provisa.api.errors import ApiError
from provisa.core.env_deploy import _decompose, _existing_ids
from provisa.core.env_merge import requirements
from provisa.core.env_project import project
from provisa.core.models import Column, Table
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import (
    domains,
    glossary_term_table_refs,
    glossary_terms,
    registered_tables,
    sources,
)

SCHEMA = "org_default"


def _request() -> "Request":
    fake = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="alice", roles=[], acting_roles=[]),
            active_org_id="acme",
        )
    )
    return cast("Request", fake)


def _with_json(request: "Request", body: dict) -> "Request":
    async def _json():
        return body

    cast(Any, request).json = _json
    return request


def _tbl(name: str, *, keyed: bool) -> Table:
    return Table(
        source_id="__derived__",
        domain_id="sales",
        schema_name="s",
        table_name=name,
        columns=[
            Column(name="id", data_type="integer", visible_to=[], is_primary_key=keyed),
            Column(name="note", data_type="text", visible_to=[]),
        ],
        view_sql="SELECT 1",
    )


@asynccontextmanager
async def _surface(control_plane, monkeypatch, *, authority=None):
    """Three tables in ``sales`` -- two keyed, one not -- and two abstract terms."""
    with control_plane.engine().begin() as c:
        c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
        c.execute(insert(domains).values(id="sales", description=""))
        c.execute(insert(sources).values(id="__derived__", type="derived"))
    db = control_plane.org()
    notified: list[str] = []

    async def _pool():
        return db

    async def _notify(org_id: str, reason: str) -> None:
        notified.append(reason)

    monkeypatch.setattr(glossary_router, "_pool", _pool)
    monkeypatch.setattr(glossary_router, "_notify", _notify)
    # Which right each endpoint names is asserted below from the router's own source; here the
    # capability half must simply not block the handler. The domain half is the case's own.
    monkeypatch.setattr(glossary_router, "_require_glossary_read", lambda request: None)
    monkeypatch.setattr(glossary_router, "_require_glossary_rw", lambda request: None)
    monkeypatch.setattr(glossary_router, "_authority", lambda request: authority)
    monkeypatch.setattr(glossary_router, "_curate_authority", lambda request: authority)
    monkeypatch.setattr(glossary_router, "_is_org_curator", lambda request: False)
    ids: dict[str, int] = {}
    async with db.acquire() as conn:
        for name, keyed in (("customers", True), ("accounts", True), ("notes", False)):
            await table_repo.upsert(conn, _tbl(name, keyed=keyed))
            row = (
                await conn.execute_core(
                    select(registered_tables.c.id).where(registered_tables.c.table_name == name)
                )
            ).fetchone()
            ids[name] = row.id
        # Each keyed table proposes its own Class on sync (REQ-2141); grounding a curated term
        # replaces the proposal, so these cases ground into the tables as they are.
        for name in ("party", "ledger"):
            ids[name] = await glossary_repo.create_abstract_term(conn, name, domains={"sales"})
    yield db, ids, notified


async def _ground(term_id: int, table_id: int) -> dict:
    return await glossary_router.add_term_table(
        _with_json(_request(), {"table_id": table_id}), term_id
    )


@pytest.mark.asyncio
async def test_a_term_grounds_many_tables_and_lists_them_by_address(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, ids, notified):
        assert await _ground(ids["party"], ids["customers"]) == {
            "ok": True,
            "term_id": ids["party"],
            "table_id": ids["customers"],
        }
        await _ground(ids["party"], ids["accounts"])
        listed = await glossary_router.list_term_tables(_request(), ids["party"])
        assert [(t["table_name"], t["source_id"], t["domain_id"]) for t in listed] == [
            ("accounts", "__derived__", "sales"),
            ("customers", "__derived__", "sales"),
        ]
        assert notified == ["ontology table grounded", "ontology table grounded"]


@pytest.mark.asyncio
async def test_a_table_without_a_primary_key_is_refused_by_name(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, ids, notified):
        with pytest.raises(ApiError) as err:
            await _ground(ids["party"], ids["notes"])
        assert (err.value.status_code, err.value.code) == (400, "glossary.table_no_primary_key")
        assert err.value.params == {"table": "__derived__.s.notes"}
        assert "__derived__.s.notes has no primary key" in err.value.detail
        assert notified == []


@pytest.mark.asyncio
async def test_a_table_has_one_class_term_and_the_refusal_names_it(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, ids, _notified):
        await _ground(ids["party"], ids["customers"])
        for term in ("ledger", "party"):  # another term, and the same one again
            with pytest.raises(ApiError) as err:
                await _ground(ids[term], ids["customers"])
            assert (err.value.status_code, err.value.code) == (
                409,
                "glossary.table_already_grounded",
            )
            assert err.value.params == {
                "table": "__derived__.s.customers",
                "term_id": ids["party"],
                "term_name": "party",
            }
            assert "already grounded by term 'party'" in err.value.detail


@pytest.mark.asyncio
async def test_ungrounding_withdraws_it_and_a_missing_grounding_is_404(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, ids, notified):
        await _ground(ids["party"], ids["customers"])
        assert await glossary_router.remove_term_table(
            _request(), ids["party"], ids["customers"]
        ) == {"ok": True}
        assert await glossary_router.list_term_tables(_request(), ids["party"]) == []
        with pytest.raises(ApiError) as err:
            await glossary_router.remove_term_table(_request(), ids["party"], ids["customers"])
        assert (err.value.status_code, err.value.code) == (404, "glossary.table_ref_not_found")
        # Ungrounded, the table may take another class term.
        await _ground(ids["ledger"], ids["customers"])
        assert notified[-2:] == ["ontology table ungrounded", "ontology table grounded"]


@pytest.mark.asyncio
async def test_a_missing_term_or_table_is_404(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, ids, _notified):
        with pytest.raises(ApiError) as err:
            await _ground(99999, ids["customers"])
        assert (err.value.status_code, err.value.code) == (404, "glossary.term_not_found")
        with pytest.raises(ApiError) as err:
            await _ground(ids["party"], 99999)
        assert (err.value.status_code, err.value.code) == (404, "glossary.table_not_found")
        with pytest.raises(ApiError) as err:
            await glossary_router.list_term_tables(_request(), 99999)
        assert (err.value.status_code, err.value.code) == (404, "glossary.term_not_found")


@pytest.mark.asyncio
async def test_deleting_the_term_or_the_table_takes_the_grounding(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        await _ground(ids["party"], ids["customers"])
        await _ground(ids["ledger"], ids["accounts"])
        async with db.acquire() as conn:
            await glossary_repo.delete_term(conn, ids["party"])
            await table_repo.delete(conn, ids["accounts"])
            assert await glossary_repo.term_tables(conn, ids["ledger"]) == []
        await _ground(ids["ledger"], ids["customers"])


# --- rights (REQ-1590/1591/1592: the column refs' own) ----------------------------------------


def test_listing_is_gated_on_read_and_curating_on_rw():
    from tests.unit.test_glossary_rights import _endpoint_gates

    gates = _endpoint_gates()
    assert gates["GET /terms/{term_id}/tables"] == "glossary_read"
    assert gates["POST /terms/{term_id}/tables"] == "glossary_rw"
    assert gates["DELETE /terms/{term_id}/tables/{table_id}"] == "glossary_rw"


@pytest.mark.asyncio
async def test_a_table_outside_the_callers_domains_is_refused(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch, authority=frozenset({"hr"})) as (
        _db,
        ids,
        notified,
    ):
        with pytest.raises(ApiError) as err:
            await _ground(ids["party"], ids["customers"])
        assert (err.value.status_code, err.value.code) == (403, "auth.domain_denied")
        with pytest.raises(ApiError) as err:
            await glossary_router.list_term_tables(_request(), ids["party"])
        assert err.value.status_code == 403
        assert notified == []


@pytest.mark.asyncio
async def test_a_term_its_author_claimed_is_refused_to_another_curator(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        async with db.acquire() as conn:
            await glossary_repo.add_expert(conn, ids["party"], "bob", kind="author")
        with pytest.raises(ApiError) as err:
            await _ground(ids["party"], ids["customers"])
        assert (err.value.status_code, err.value.code) == (403, "glossary.term_claimed")


# --- the meta domain --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_meta_view_resolves_each_grounding_and_a_table_names_its_class_term(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        await _ground(ids["party"], ids["customers"])
        async with db.acquire() as conn:
            for name in ("glossary_term_domains", "glossary_term_table_refs", "registered_tables"):
                await conn.execute(_META_TABLE_VIEWS[name])
            rows = await conn.fetch(
                "SELECT term_id, table_id, source_id, schema_name, table_name, domain_id,"
                " term_domains FROM glossary_term_table_refs_meta WHERE table_name = 'customers'"
            )
            assert [tuple(r) for r in rows] == [
                (
                    ids["party"],
                    ids["customers"],
                    "__derived__",
                    "s",
                    "customers",
                    "sales",
                    "|sales|",
                )
            ]
            classes = dict(
                tuple(r)
                for r in await conn.fetch(
                    "SELECT table_name, class_term_id FROM registered_tables_meta"
                )
            )
            [account] = [
                t["id"] for t in await glossary_repo.list_terms(conn) if t["name"] == "account"
            ]
            # accounts keeps its proposal; notes, with no primary key, has no class
            assert classes == {"customers": ids["party"], "accounts": account, "notes": None}


def test_the_meta_schema_reaches_a_terms_tables_and_a_tables_class_term():
    from provisa.api._meta_seed import META_RELATIONSHIPS

    edges = {r[0]: r for r in META_RELATIONSHIPS}
    tables = edges["meta:glossary_terms:tables"]
    assert (tables[2], tables[5], tables[7], tables[9]) == (
        "glossary_terms",
        "one-to-many",
        "glossary_term_table_refs",
        "tables",
    )
    assert edges["meta:glossary_term_table_refs:registered_tables"][9] == "table"
    assert edges["meta:glossary_term_table_refs:term"][9] == "term"
    class_term = edges["meta:registered_tables:class_term"]
    assert (class_term[3], class_term[5], class_term[7], class_term[9]) == (
        "class_term_id",
        "many-to-one",
        "glossary_terms",
        "classTerm",
    )


# --- environments -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_environment_tree_carries_a_tables_class_term_and_deploys_it_back(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        await _ground(ids["party"], ids["customers"])
        async with db.acquire() as conn:
            tree = await project(conn, SCHEMA)
            existing = await _existing_ids(conn, SCHEMA)
        [path] = [p for p, b in tree.items() if b.get("table_name") == "customers"]
        term_path = next(p for p, b in tree.items() if b.get("name") == "party")
        assert tree[path]["class_term"] == [{"term": term_path}]
        assert term_path in requirements(path, tree[path], tree)
        rows = _decompose(tree, existing, frozenset())
        assert [
            r for r in rows["glossary_term_table_refs"] if r["table_id"] == ids["customers"]
        ] == [{"table_id": ids["customers"], "term_id": ids["party"], "derived": False}]


# --- grounded to a table (REQ-1387): the admission rule and the derived domains ---------------


@pytest.mark.asyncio
async def test_a_term_grounded_only_in_a_table_is_grounded_and_live_once_defined(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        async with db.acquire() as conn:
            assert ids["party"] not in await glossary_repo.grounded_ids(conn)
            await glossary_repo.add_edge(conn, ids["ledger"], ids["party"], "KIND_OF")
            await glossary_repo.set_definition(conn, ids["ledger"], "What the books record.")
        await _ground(ids["party"], ids["customers"])
        async with db.acquire() as conn:
            grounded = await glossary_repo.grounded_ids(conn)
            # Grounded through the table, and conducted over the edge to its neighbour.
            assert {ids["party"], ids["ledger"]} <= grounded
            live = await glossary_repo.live_ids(conn)
            assert ids["ledger"] in live and ids["party"] not in live  # party has no definition
            # retired, party's table goes to its successor and party conducts no more (REQ-2144)
            client = await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
            await glossary_repo.retire_term(
                conn, ids["party"], reason="renamed", successor=client, by=None
            )
            assert ids["ledger"] not in await glossary_repo.grounded_ids(conn)


@pytest.mark.asyncio
async def test_a_class_terms_domains_derive_from_the_tables_it_grounds(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        with control_plane.engine().begin() as c:
            c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
            c.execute(insert(domains).values(id="hr", description=""))
        async with db.acquire() as conn:
            await table_repo.upsert(
                conn, _tbl("staff", keyed=True).model_copy(update={"domain_id": "hr"})
            )
            staff = (
                await conn.execute_core(
                    select(registered_tables.c.id).where(registered_tables.c.table_name == "staff")
                )
            ).fetchone()
            # Declared {sales}; grounded in an hr table, the term's domains are derived from it.
            assert (await glossary_repo.term_domains(conn))[ids["party"]] == {"sales"}
            [proposed] = [
                t["id"] for t in await glossary_repo.list_terms(conn) if t["name"] == "staff"
            ]
            assert await glossary_repo.unground_table(conn, proposed, staff.id)
            await glossary_repo.ground_table(conn, ids["party"], staff.id)
            assert (await glossary_repo.term_domains(conn))[ids["party"]] == {"hr"}
            await conn.execute(_META_TABLE_VIEWS["glossary_term_domains"])
            rows = await conn.fetch(
                "SELECT domain_id FROM glossary_term_domains_meta WHERE term_id = $1",
                ids["party"],
            )
            assert [r[0] for r in rows] == ["hr"]


@pytest.mark.asyncio
async def test_the_meta_terms_view_counts_a_table_as_grounding(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        async with db.acquire() as conn:
            await glossary_repo.set_definition(conn, ids["party"], "Someone we deal with.")
        await _ground(ids["party"], ids["customers"])
        async with db.acquire() as conn:
            for name in ("glossary_term_domains", "glossary_terms"):
                await conn.execute(_META_TABLE_VIEWS[name])
            rows = await conn.fetch(
                "SELECT name, is_rooted, grounded, live FROM glossary_terms_meta"
                " WHERE name IN ('party', 'ledger') ORDER BY name"
            )
            assert [tuple(r) for r in rows] == [
                ("ledger", False, False, False),
                ("party", True, True, True),
            ]


@pytest.mark.asyncio
async def test_a_term_that_owns_column_refs_cannot_ground_a_table(control_plane, monkeypatch):
    """REQ-2141: a term is a Class or a Property, never both -- a Property (it owns column refs)
    is refused a table grounding, naming its columns and why."""
    async with _surface(control_plane, monkeypatch) as (db, ids, notified):
        async with db.acquire() as conn:
            note = await glossary_repo.get_term_by_ref(conn, ids["customers"], "note")
        assert note is not None
        with pytest.raises(ApiError) as err:
            await _ground(note["id"], ids["customers"])
        assert (err.value.status_code, err.value.code) == (409, "glossary.term_is_property")
        assert err.value.params == {
            "term": "note",
            # notes.id is the notes table's "note" too (its business name in its table's context)
            "columns": "__derived__.s.accounts.note, __derived__.s.customers.note, "
            "__derived__.s.notes.id, __derived__.s.notes.note",
        }
        assert "a term is either a Class, grounded in tables, or a Property" in err.value.detail
        assert notified == []
        async with db.acquire() as conn:
            assert await glossary_repo.term_tables(conn, note["id"]) == []


@pytest.mark.asyncio
async def test_a_column_ref_cannot_move_into_a_term_grounded_in_tables(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        await _ground(ids["party"], ids["customers"])
        body = {"table_id": ids["accounts"], "column_name": "note", "to_term_id": ids["party"]}
        with pytest.raises(ApiError) as err:
            await glossary_router.move_ref(_with_json(_request(), body))
        assert (err.value.status_code, err.value.code) == (409, "glossary.term_is_class")
        assert err.value.params == {"term": "party", "tables": "__derived__.s.customers"}
        assert "cannot own a column ref" in err.value.detail
        async with db.acquire() as conn:
            kept = await glossary_repo.get_term_by_ref(conn, ids["accounts"], "note")
        assert kept is not None and kept["name"] == "note"


@pytest.mark.asyncio
async def test_a_column_named_for_a_class_is_a_property_of_its_own_table(
    control_plane, monkeypatch
):
    """A column with no declared relationship is no key: named like a Class, it is not the
    Class's -- it derives a Property qualified with its own table's concept."""
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        await _ground(ids["party"], ids["customers"])
        named = _tbl("accounts", keyed=True).model_copy(
            update={
                "columns": [
                    Column(name="id", data_type="integer", visible_to=[], is_primary_key=True),
                    Column(name="party", data_type="text", visible_to=[]),
                ]
            }
        )
        async with db.acquire() as conn:
            await table_repo.upsert(conn, named)
            term = await glossary_repo.get_term_by_ref(conn, ids["accounts"], "party")
        assert term is not None and term["name"] == "account party"


@pytest.mark.asyncio
async def test_the_check_reports_a_class_holding_non_key_refs_and_changes_nothing(
    control_plane, monkeypatch
):
    """Data from before the rule: a term both grounding a table and holding non-key column refs
    is reported with its tables and those columns, and left as it is."""
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        await _ground(ids["party"], ids["accounts"])  # a Class holding its keys is no conflict
        async with db.acquire() as conn:
            note = await glossary_repo.get_term_by_ref(conn, ids["customers"], "note")
            assert note is not None
            await conn.execute_core(  # planted past the rules: note made customers' class
                glossary_term_table_refs.update()
                .where(glossary_term_table_refs.c.table_id == ids["customers"])
                .values(term_id=note["id"])
            )
        found = await glossary_router.role_conflicts(_request())
        assert found == {
            "terms": [
                {
                    "term_id": note["id"],
                    "term": "note",
                    "tables": ["__derived__.s.customers"],
                    "columns": [
                        "__derived__.s.accounts.note",
                        "__derived__.s.customers.note",
                        "__derived__.s.notes.id",
                        "__derived__.s.notes.note",
                    ],
                }
            ],
            "tables": [],
            "tags": [],
        }
        assert await glossary_router.role_conflicts(_request()) == found  # nothing changed


@pytest.mark.asyncio
async def test_a_table_whose_concept_is_a_property_gets_no_class_and_is_reported(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        with control_plane.engine().begin() as c:
            c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
            c.execute(insert(domains).values(id="hr", description=""))
        async with db.acquire() as conn:
            await table_repo.upsert(
                conn,
                _tbl("notes", keyed=True).model_copy(
                    update={"schema_name": "t", "domain_id": "hr"}
                ),
            )
            keyed = (
                await conn.execute_core(
                    select(registered_tables.c.id).where(registered_tables.c.schema_name == "t")
                )
            ).fetchone()
            assert (
                await glossary_repo.term_tables(
                    conn,
                    ids_note := (
                        await glossary_repo.get_term_by_ref(conn, ids["customers"], "note")
                    )["id"],
                )
                == []
            )
        found = await glossary_router.role_conflicts(_request())
        [table] = found["tables"]
        assert (table["table_id"], table["table"], table["concept"], table["term_id"]) == (
            keyed.id,
            "__derived__.t.notes",
            "note",
            ids_note,
        )
        assert "__derived__.s.customers.note" in table["columns"]


# --- the export admits a class term by the tables it publishes --------------------------------


def test_the_export_roots_a_class_term_by_a_published_table():
    from provisa.api.metadata_export.builder import _glossary_assets

    orders = _tbl("orders", keyed=True)
    glossary = {
        "terms": [
            {
                "id": 1,
                "name": "booking",
                "definition": "A sale.",
                "is_abstract": True,
                "deprecated": False,
                "deprecation_asserted": False,
                "status_reason": None,
                "retire_on": None,
                "retired": False,
            },
            {
                "id": 2,
                "name": "refund",
                "definition": "A return.",
                "is_abstract": True,
                "deprecated": False,
                "deprecation_asserted": False,
                "status_reason": None,
                "retire_on": None,
                "retired": False,
            },
        ],
        "refs": [],
        "table_refs": [
            {"term_id": 1, "source_id": "__derived__", "schema_name": "s", "table_name": "orders"},
            # Its table does not publish, so it roots nothing here.
            {"term_id": 2, "source_id": "__derived__", "schema_name": "s", "table_name": "gone"},
        ],
        "edges": [],
        "experts": [],
    }
    terms, _edges = _glossary_assets(glossary, [orders], set(), "acme")
    assert [t.name for t in terms] == ["booking"]


# --- the declarative model (REQ-2141): config load and the store's export ----------------------


def _config(*, class_term: str | None, keyed: bool = True):
    from provisa.core.config_loader import parse_config_dict

    column = {"name": "id", "data_type": "integer", "visible_to": ["seller"]}
    if keyed:
        column["is_primary_key"] = True
    table = {
        "source_id": "cfg",
        "domain_id": "sales",
        "schema": "public",
        "table": "orders",
        "columns": [column],
    }
    if class_term is not None:
        table["class_term"] = class_term
    return parse_config_dict(
        {
            "sources": [
                {"id": "cfg", "type": "postgresql", "host": "h", "port": 5432, "database": "d"}
            ],
            "domains": [{"id": "sales"}],
            "roles": [
                {
                    "id": "seller",
                    "description": "Test role.",
                    "capabilities": [],
                    "domain_access": ["sales"],
                }
            ],
            "tables": [table],
            "glossary_terms": [
                {"name": "Booking", "domains": ["sales"], "definition": "A sale."},
                {"name": "Sale", "domains": ["sales"], "definition": "A sale, again."},
            ],
        }
    )


@pytest.fixture
async def cfg_db(control_plane, monkeypatch):
    import contextlib

    from provisa.core import secrets_providers, secrets_store

    # One organisation, as the API layer says at import (provisa.api.app); and no request org to
    # bind a secret to -- this config names none.
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)

    @contextlib.asynccontextmanager
    async def _unbound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _unbound)
    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="__derived__", type="postgresql"))
    return db


async def _class_term_of_orders(conn) -> str | None:
    from provisa.core.store_config import store_model

    [orders] = [t for t in (await store_model(conn))["tables"] if t["table"] == "orders"]
    return orders.get("class_term")


@pytest.mark.asyncio
async def test_a_config_states_a_tables_class_term_and_the_store_exports_it(cfg_db):
    from provisa.core.config_loader import apply_config

    async with cfg_db.acquire() as conn:
        await apply_config(_config(class_term="Booking"), conn)
        assert await _class_term_of_orders(conn) == "booking"
        # Loading it again changes nothing; stating another term replaces it.
        await apply_config(_config(class_term="Booking"), conn)
        assert await _class_term_of_orders(conn) == "booking"
        await apply_config(_config(class_term="sale"), conn)
        assert await _class_term_of_orders(conn) == "sale"
        [sale] = [t for t in await glossary_repo.list_terms(conn) if t["name"] == "sale"]
        assert sale["live"]


@pytest.mark.asyncio
async def test_a_config_class_term_naming_nothing_or_on_an_unkeyed_table_is_refused(cfg_db):
    from provisa.core.config_loader import apply_config

    async with cfg_db.acquire() as conn:
        with pytest.raises(ValueError, match="names 'Nobody' as its class term"):
            await apply_config(_config(class_term="Nobody"), conn)
    async with cfg_db.acquire() as conn:
        with pytest.raises(ValueError, match="cfg.public.orders has no primary key"):
            await apply_config(_config(class_term="Booking", keyed=False), conn)


@pytest.mark.asyncio
async def test_a_config_class_term_that_owns_column_refs_is_refused(cfg_db):
    """REQ-2141: the orders table's ``booking`` column makes ``booking`` a Property, so the same
    config naming it the table's class term is refused, saying why."""
    from provisa.core.config_loader import apply_config

    config = _config(class_term="Booking")
    [orders] = config.tables
    config.tables[0] = orders.model_copy(
        update={
            "columns": [
                *orders.columns,
                orders.columns[0].model_copy(update={"name": "booking", "is_primary_key": False}),
            ]
        }
    )
    async with cfg_db.acquire() as conn:
        with pytest.raises(ValueError, match="cfg.public.orders cannot have a class term") as err:
            await apply_config(config, conn)
    assert "term 'booking' owns column refs (cfg.public.orders.booking)" in str(err.value)


@pytest.mark.asyncio
async def test_the_term_list_and_detail_count_its_tables_beside_its_columns(
    control_plane, monkeypatch
):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        await _ground(ids["party"], ids["customers"])
        await _ground(ids["party"], ids["accounts"])
        listed = {t["name"]: t for t in await glossary_router.list_terms(_request(), domains=None)}
        # its tables' primary keys are its own refs (REQ-2141, KEYS ON THE CLASS)
        assert (listed["party"]["table_count"], listed["party"]["ref_count"]) == (2, 2)
        assert listed["ledger"]["table_count"] == 0
        async with db.acquire() as conn:
            note = await glossary_repo.get_term_by_ref(conn, ids["customers"], "note")
        assert note is not None and note["table_count"] == 0 and note["ref_count"] >= 1
        detail = await glossary_router.get_term(_request(), ids["party"])
        assert (detail["table_count"], detail["ref_count"]) == (2, 2)


# --- abstract is derived from grounding: the role is the only classification (REQ-2141) ------


@pytest.mark.asyncio
async def test_abstract_is_derived_for_each_role(control_plane, monkeypatch):
    """A Concept grounds nothing and is abstract; a Property (column refs) and a Class (tables)
    are not -- in the list, the detail and the meta view alike."""
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        await _ground(ids["party"], ids["customers"])  # party: a Class; ledger: a Concept
        async with db.acquire() as conn:
            note = await glossary_repo.get_term_by_ref(conn, ids["customers"], "note")
            assert note is not None  # note: a Property
            listed = {t["name"]: t["is_abstract"] for t in await glossary_repo.list_terms(conn)}
            detail = {
                name: (await glossary_repo.get_term(conn, ids.get(name, note["id"])))["is_abstract"]
                for name in ("party", "ledger", "note")
            }
            exported = {
                t["name"]: t["is_abstract"]
                for t in (await glossary_repo.export_graph(conn))["terms"]
            }
        expected = {"party": False, "ledger": True, "note": False}
        assert {n: listed[n] for n in expected} == expected
        assert detail == expected
        assert {n: exported[n] for n in expected} == expected
        # ungrounded, the Class is a Concept again
        await glossary_router.remove_term_table(_request(), ids["party"], ids["customers"])
        async with db.acquire() as conn:
            party = await glossary_repo.get_term(conn, ids["party"])
        assert party is not None and party["is_abstract"] is True


@pytest.mark.asyncio
async def test_the_api_refuses_to_set_abstract(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (_db, ids, notified):
        for call in (
            lambda: glossary_router.create_abstract_term(
                _with_json(_request(), {"name": "thing", "domains": ["sales"], "is_abstract": True})
            ),
            lambda: glossary_router.update_term(
                _with_json(_request(), {"is_abstract": False}), ids["ledger"]
            ),
        ):
            with pytest.raises(ApiError) as err:
                await call()
            assert (err.value.status_code, err.value.code) == (400, "glossary.abstract_is_derived")
            assert "abstract is derived from grounding" in err.value.detail
        assert notified == []


def test_config_refuses_to_declare_abstract():
    from provisa.core.models import GlossaryTermConfig

    with pytest.raises(ValueError, match="'buyer': abstract is derived from grounding"):
        GlossaryTermConfig.model_validate(
            {"name": "buyer", "domains": ["sales"], "is_abstract": True}
        )
    assert GlossaryTermConfig.model_validate({"name": "buyer", "domains": ["sales"]}).name


@pytest.mark.asyncio
async def test_declared_domains_apply_to_a_concept_only(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        with control_plane.engine().begin() as c:
            c.exec_driver_sql(f'SET LOCAL search_path TO "{SCHEMA}"')
            c.execute(insert(domains).values(id="finance", description=""))
        await glossary_router.update_term(
            _with_json(_request(), {"domains": ["finance"]}), ids["ledger"]
        )
        async with db.acquire() as conn:
            assert (await glossary_repo.term_domains(conn))[ids["ledger"]] == {"finance"}
            note = await glossary_repo.get_term_by_ref(conn, ids["customers"], "note")
        assert note is not None
        await _ground(ids["party"], ids["customers"])
        for term_id in (note["id"], ids["party"]):  # a Property, then a Class
            with pytest.raises(ApiError) as err:
                await glossary_router.update_term(
                    _with_json(_request(), {"domains": ["finance"]}), term_id
                )
            assert (err.value.status_code, err.value.code) == (400, "glossary.invalid")
            assert "only a Concept's domains can be set" in err.value.detail


# --- table proposals and keys on the class (REQ-2141) ------------------------------------------


def _keyed(name: str, *columns: str, keys: "tuple[str, ...]" = ("id",)) -> Table:
    return Table(
        source_id="__derived__",
        domain_id="sales",
        schema_name="m",
        table_name=name,
        columns=[
            Column(name=c, data_type="text", visible_to=[], is_primary_key=c in keys)
            for c in columns
        ],
        view_sql="SELECT 1",
    )


@asynccontextmanager
async def _model(control_plane, monkeypatch):
    async with _surface(control_plane, monkeypatch) as (db, _ids, _notified):
        async with db.acquire() as conn:
            for table in (
                _keyed("buyers", "id", "name"),
                _keyed("buyer", "id", "since"),  # the same concept, singular
                _keyed("employees", "id", "name", "manager_id"),
                _keyed("users", "email", "nickname", keys=("email",)),
                _keyed("orders", "id", "buyer_id", "total"),
                _keyed("order_lines", "order_id", "line_no", "qty", keys=("order_id", "line_no")),
                _keyed("shipments", "id", "order_id", "line_no"),
                _keyed("buyer_profiles", "buyer_id", "bio", keys=("buyer_id",)),
            ):
                await table_repo.upsert(conn, table)
        yield db


async def _term_of(conn, table: str, column: str) -> str:
    row = (
        await conn.execute_core(
            select(registered_tables.c.id).where(
                (registered_tables.c.table_name == table) & (registered_tables.c.schema_name == "m")
            )
        )
    ).fetchone()
    term = await glossary_repo.get_term_by_ref(conn, row.id, column)
    assert term is not None
    return term["name"]


async def _class_of(conn, table: str) -> "str | None":
    row = (
        await conn.execute_core(
            select(glossary_terms.c.name)
            .join(
                glossary_term_table_refs, glossary_term_table_refs.c.term_id == glossary_terms.c.id
            )
            .join(registered_tables, glossary_term_table_refs.c.table_id == registered_tables.c.id)
            .where(
                (registered_tables.c.table_name == table) & (registered_tables.c.schema_name == "m")
            )
        )
    ).fetchone()
    return None if row is None else row.name


async def _relate(conn, rel_id, source, column, target, target_column="id"):
    from provisa.core.models import Cardinality, Relationship
    from provisa.core.repositories import relationship as relationship_repo

    await relationship_repo.upsert(
        conn,
        Relationship(
            id=rel_id,
            source_table_id=source,
            target_table_id=target,
            source_column=column,
            target_column=target_column,
            cardinality=Cardinality.many_to_one,
        ),
    )


@pytest.mark.asyncio
async def test_a_keyed_table_proposes_its_class_and_its_key_is_the_class(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as db:
        async with db.acquire() as conn:
            assert await _class_of(conn, "employees") == "employee"
            assert await _term_of(conn, "employees", "id") == "employee"
            employee = next(
                t for t in await glossary_repo.list_terms(conn) if t["name"] == "employee"
            )
            assert not (employee["definition"] or "")  # a draft, as a derived column term is
            # a primary key of any name is its table's class, a natural key included
            assert await _class_of(conn, "users") == "user"
            assert await _term_of(conn, "users", "email") == "user"
            assert await _term_of(conn, "users", "nickname") == "nickname"


@pytest.mark.asyncio
async def test_tables_of_one_concept_share_one_class(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as db:
        async with db.acquire() as conn:
            assert await _class_of(conn, "buyers") == await _class_of(conn, "buyer") == "buyer"
            [customer] = [t for t in await glossary_repo.list_terms(conn) if t["name"] == "buyer"]
            assert customer["table_count"] == 2


@pytest.mark.asyncio
async def test_a_role_named_foreign_key_lands_on_its_targets_class_and_leaves_with_it(
    control_plane, monkeypatch
):
    """manager_id -> employees: the role is the relationship's, the ref the employee class's."""
    from provisa.core.repositories import relationship as relationship_repo

    async with _model(control_plane, monkeypatch) as db:
        async with db.acquire() as conn:
            assert await _term_of(conn, "employees", "manager_id") == "manager"
            await _relate(conn, "employee-manager", "employees", "manager_id", "employees")
            assert await _term_of(conn, "employees", "manager_id") == "employee"
            assert await relationship_repo.delete(conn, "employee-manager")
            assert await _term_of(conn, "employees", "manager_id") == "manager"


@pytest.mark.asyncio
async def test_a_foreign_key_by_name_alone_stays_a_property_until_it_is_declared(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as db:
        async with db.acquire() as conn:
            # named for the buyer class, but no relationship says so
            assert await _term_of(conn, "orders", "buyer_id") == "order buyer"
            await _relate(conn, "order-buyer", "orders", "buyer_id", "buyers")
            assert await _term_of(conn, "orders", "buyer_id") == "buyer"
            names = {t["name"] for t in await glossary_repo.list_terms(conn)}
            assert "order buyer" not in names  # the stand-in Property, blank, went


@pytest.mark.asyncio
async def test_every_part_of_a_composite_key_is_on_the_class(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as db:
        async with db.acquire() as conn:
            assert await _class_of(conn, "order_lines") == "order line"
            await _relate(
                conn,
                "shipment-line",
                "shipments",
                "order_id,line_no",
                "order_lines",
                "order_id,line_no",
            )
            assert await _term_of(conn, "shipments", "order_id") == "order line"
            assert await _term_of(conn, "shipments", "line_no") == "order line"


@pytest.mark.asyncio
async def test_a_primary_key_that_is_a_foreign_key_is_on_the_parents_class(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as db:
        async with db.acquire() as conn:
            await _relate(conn, "profile-buyer", "buyer_profiles", "buyer_id", "buyers")
            assert await _term_of(conn, "buyer_profiles", "buyer_id") == "buyer"
            # the extension table still proposes its own class
            assert await _class_of(conn, "buyer_profiles") == "buyer profile"
            await _relate(conn, "line-order", "order_lines", "order_id", "orders")
            assert await _term_of(conn, "order_lines", "order_id") == "order"
            assert await _term_of(conn, "order_lines", "line_no") == "order line"


@pytest.mark.asyncio
async def test_sync_never_re_points_a_curated_class(control_plane, monkeypatch):
    async with _model(control_plane, monkeypatch) as db:
        async with db.acquire() as conn:
            client = await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
            [employee] = [
                t["id"] for t in await glossary_repo.list_terms(conn) if t["name"] == "employee"
            ]
            employees = (
                await conn.execute_core(
                    select(registered_tables.c.id).where(
                        registered_tables.c.table_name == "employees"
                    )
                )
            ).fetchone()
            assert await glossary_repo.unground_table(conn, employee, employees.id)
            await glossary_repo.ground_table(conn, client, employees.id)
            assert await _term_of(conn, "employees", "id") == "client"
            await table_repo.upsert(conn, _keyed("employees", "id", "name", "manager_id"))
            assert await _class_of(conn, "employees") == "client"
            assert await _term_of(conn, "employees", "id") == "client"


@pytest.mark.asyncio
async def test_a_key_column_is_not_moved_by_hand_whatever_the_target(control_plane, monkeypatch):
    """A key's ref is placed by derivation alone: moving it anywhere is refused, naming its class."""
    async with _surface(control_plane, monkeypatch) as (db, ids, _notified):
        body = {"table_id": ids["customers"], "column_name": "id", "to_term_id": ids["ledger"]}
        with pytest.raises(ApiError) as err:
            await glossary_router.move_ref(_with_json(_request(), body))
        assert (err.value.status_code, err.value.code) == (409, "glossary.key_column_derived")
        assert err.value.params == {
            "column": "id",
            "table": "__derived__.s.customers",
            "class": "customer",
        }
        async with db.acquire() as conn:
            assert (await glossary_repo.get_term_by_ref(conn, ids["customers"], "id"))[
                "name"
            ] == "customer"


@pytest.mark.asyncio
async def test_grounding_carries_the_keys_and_ungrounding_returns_them_to_the_proposal(
    control_plane, monkeypatch
):
    async with _model(control_plane, monkeypatch) as db:
        async with db.acquire() as conn:
            await _relate(conn, "order-buyer", "orders", "buyer_id", "buyers")
            client = await glossary_repo.create_abstract_term(conn, "client", domains={"sales"})
            buyers = (
                await conn.execute_core(
                    select(registered_tables.c.id).where(registered_tables.c.table_name == "buyers")
                )
            ).fetchone()
            # the proposal gives way; the table's key and the key referring to it move at once
            await glossary_repo.ground_table(conn, client, buyers.id)
            assert await _class_of(conn, "buyers") == "client"
            assert await _term_of(conn, "buyers", "id") == "client"
            assert await _term_of(conn, "orders", "buyer_id") == "client"
            # ungrounded, they go back to the table's proposed class, as a sync would put them
            assert await glossary_repo.unground_table(conn, client, buyers.id)
            assert await _class_of(conn, "buyers") == "buyer"
            assert await _term_of(conn, "buyers", "id") == "buyer"
            assert await _term_of(conn, "orders", "buyer_id") == "buyer"
            assert (await glossary_repo.get_term(conn, client))["is_abstract"] is True


@pytest.mark.asyncio
async def test_a_declared_class_named_for_its_tables_concept_is_curated(cfg_db):
    """A configuration declaring orders' class as "order" -- the very name sync proposes -- states
    a curated grounding: the export keeps it, and deleting the table keeps the term."""
    from provisa.core.config_loader import apply_config

    async with cfg_db.acquire() as conn:
        await apply_config(_config(class_term="Order"), conn)
        assert await _class_term_of_orders(conn) == "order"
        [order] = [t for t in await glossary_repo.list_terms(conn) if t["name"] == "order"]
        orders = (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == "orders")
            )
        ).fetchone()
        await table_repo.delete(conn, orders.id)
        kept = await glossary_repo.get_term(conn, order["id"])
    assert kept is not None and kept["is_abstract"] is True


@pytest.mark.asyncio
async def test_a_proposal_is_not_stated_by_the_export_and_goes_with_its_table(cfg_db):
    from provisa.core.config_loader import apply_config

    async with cfg_db.acquire() as conn:
        await apply_config(_config(class_term=None), conn)
        assert await _class_term_of_orders(conn) is None  # proposed "order", derived
        [order] = [t for t in await glossary_repo.list_terms(conn) if t["name"] == "order"]
        assert order["table_count"] == 1
        orders = (
            await conn.execute_core(
                select(registered_tables.c.id).where(registered_tables.c.table_name == "orders")
            )
        ).fetchone()
        await table_repo.delete(conn, orders.id)
        assert await glossary_repo.get_term(conn, order["id"]) is None
