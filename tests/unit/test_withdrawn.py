# Copyright (c) 2026 Kenneth Stott
# Canary: 1b70a083-0644-45d3-bcf3-d78cdebaefa5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Withdrawn takes a table or a source out of production (REQ-1921, amended 2026-11-16).

A withdrawn table -- itself, or through its source -- is left out of the data plane's one load
(api.admin.db_queries), so no role's surface offers it and a statement naming it names nothing;
it keeps its definition, grants and relationships. A relationship is withdrawn while either end
(or its junction) is withdrawn or retired: derived, stored nowhere, in no compiled graph.
Relationship discovery never scopes it. A withdrawn source is attached by no engine and has no
pool. The ERD's lists and the graph schema leave withdrawn tables out, and show them, marked, only
to a person who may preview them and asks to. Withdraw and put back are one action each."""

# Requirements: REQ-1921

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import insert, select, update

from provisa.core import model_change
from provisa.core.models import Cardinality, Column, Relationship, Table
from provisa.core.repositories import relationship as rel_repo
from provisa.core.repositories import source as source_repo
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import domains, registered_tables, relationships, roles, sources

READER = "seller"
STEWARD = "steward"
_ROLE_ROWS = [
    {
        "id": READER,
        "description": "Reads sales.",
        "capabilities": ["usage"],
        "domain_access": ["sales"],
    },
    {
        "id": STEWARD,
        "description": "Registers sales tables.",
        "capabilities": ["usage", "table_registration"],
        "domain_access": ["sales"],
    },
]


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    """A deployment of one organisation, as the API layer says at import (provisa.api.app)."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


def _table(name: str, *columns: str, source: str = "crm") -> Table:
    return Table(
        source_id=source,
        domain_id="sales",
        schema_name="public",
        table_name=name,
        columns=[
            Column(name=c, visible_to=[READER, STEWARD], data_type="integer") for c in columns
        ],
    )


_ORDERS_CUSTOMER = Relationship(
    id="orders_customer",
    source_table_id="orders",
    target_table_id="customers",
    source_column="customer_id",
    target_column="id",
    cardinality=Cardinality.many_to_one,
)


@pytest.fixture
async def plane(control_plane):
    db = control_plane.org()
    async with model_change.scope("save", actor=lambda: "ada"), db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="crm", type="postgresql"))
        await conn.execute_core(insert(sources).values(id="erp", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        for row in _ROLE_ROWS:
            await conn.execute_core(
                insert(roles).values(
                    id=row["id"],
                    capabilities=row["capabilities"],
                    domain_access=row["domain_access"],
                )
            )
        await table_repo.upsert(conn, _table("orders", "id", "customer_id"))
        await table_repo.upsert(conn, _table("customers", "id"))
        await table_repo.upsert(conn, _table("invoices", "id", source="erp"))
        await rel_repo.upsert(conn, _ORDERS_CUSTOMER)
    return db


async def _id(conn, name: str) -> int:
    return (
        await conn.execute_core(
            select(registered_tables.c.id).where(registered_tables.c.table_name == name)
        )
    ).scalar_one()


async def _withdraw_table(conn, name: str, withdrawn: bool = True) -> int:
    table_id = await _id(conn, name)
    await conn.execute_core(
        update(registered_tables)
        .where(registered_tables.c.id == table_id)
        .values(withdrawn=withdrawn)
    )
    return table_id


# --- the data plane's one load --------------------------------------------------------------------


async def test_a_withdrawn_table_is_out_of_the_load_and_kept_for_its_preview(plane):
    from provisa.api.admin.db_queries import fetch_tables

    async with plane.acquire() as conn:
        await _withdraw_table(conn, "customers")
        served = {t["table_name"] for t in await fetch_tables(conn)}
        withdrawn = await fetch_tables(conn, withdrawn=True)
    assert served == {"orders", "invoices"}
    (customers,) = withdrawn
    assert customers["table_name"] == "customers"
    assert (customers["withdrawn"], customers["source_withdrawn"]) == (True, False)
    # It keeps its definition: its columns are all there.
    assert [c["column_name"] for c in customers["columns"]] == ["id"]


async def test_a_withdrawn_sources_tables_are_out_of_the_load_and_marked(plane):
    from provisa.api.admin.db_queries import fetch_tables

    async with plane.acquire() as conn:
        assert await source_repo.set_withdrawn(conn, "erp", True)
        served = {t["table_name"] for t in await fetch_tables(conn)}
        withdrawn = await fetch_tables(conn, withdrawn=True)
        assert await source_repo.set_withdrawn(conn, "erp", False)
        back = {t["table_name"] for t in await fetch_tables(conn)}
    assert served == {"orders", "customers"}
    assert [(t["table_name"], t["withdrawn"], t["source_withdrawn"]) for t in withdrawn] == [
        ("invoices", True, True)
    ]
    assert back == {"orders", "customers", "invoices"}


async def test_a_relationship_is_withdrawn_while_an_end_is_and_back_when_it_is_put_back(plane):
    from provisa.api.admin.db_queries import fetch_relationships

    async with plane.acquire() as conn:
        assert [r["id"] for r in await fetch_relationships(conn)] == ["orders_customer"]
        assert await fetch_relationships(conn, withdrawn=True) == []
        await _withdraw_table(conn, "customers")
        assert await fetch_relationships(conn) == []
        assert [r["id"] for r in await fetch_relationships(conn, withdrawn=True)] == [
            "orders_customer"
        ]
        await _withdraw_table(conn, "customers", withdrawn=False)
        assert [r["id"] for r in await fetch_relationships(conn)] == ["orders_customer"]
        # Through a source: orders' source withdrawn withdraws the relationship it holds.
        assert await source_repo.set_withdrawn(conn, "crm", True)
        assert await fetch_relationships(conn) == []
        assert await source_repo.set_withdrawn(conn, "crm", False)
        # A retired end retires it from both loads: it is not merely withdrawn.
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.table_name == "customers")
            .values(retired=True, withdrawn=True)
        )
        assert await fetch_relationships(conn) == []
        assert await fetch_relationships(conn, withdrawn=True) == []


async def test_relationship_discovery_never_scopes_a_withdrawn_table(plane):
    from provisa.discovery.gather import scope_table_ids

    async with plane.acquire() as conn:
        everything = await scope_table_ids(conn, ["sales"], None)
        customers = await _withdraw_table(conn, "customers")
        assert await source_repo.set_withdrawn(conn, "erp", True)
        invoices = await _id(conn, "invoices")
        orders = await _id(conn, "orders")
        assert set(everything) == {orders, customers, invoices}
        assert await scope_table_ids(conn, ["sales"], None) == [orders]
        # Named, it is not scoped either.
        assert await scope_table_ids(conn, [], [orders, customers, invoices]) == [orders]


async def test_a_withdrawn_source_is_attached_by_no_engine_and_still_listed(plane):
    from provisa.api.model_reload import registered_source_rows

    async with plane.acquire() as conn:
        assert await source_repo.set_withdrawn(conn, "erp", True)
        attached = set(await registered_source_rows(conn))
        listed = {s["id"]: s["withdrawn"] for s in await source_repo.list_all(conn)}
        withdrawn = await source_repo.withdrawn_ids(conn)
    assert "erp" not in attached and "crm" in attached
    assert listed["erp"] is True and listed["crm"] is False
    assert withdrawn == frozenset({"erp"})


async def test_withdrawing_an_unknown_source_says_so(plane):
    async with plane.acquire() as conn:
        assert not await source_repo.set_withdrawn(conn, "nope", True)


# --- the actions --------------------------------------------------------------------------------


@pytest.fixture
def actions(plane, monkeypatch):
    """The withdraw / put back mutations on the plane, the rebuild recorded."""
    from provisa.api.admin import schema_mutation

    done: list[tuple] = []

    async def _pool():
        return plane

    async def _rebuild():
        done.append(("rebuild",))

    async def _readable(_info, _table_id):
        return None

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)
    monkeypatch.setattr(schema_mutation, "_rebuild_schemas", _rebuild)
    monkeypatch.setattr(schema_mutation, "_table_unread", _readable)
    return done


def _info() -> Any:
    request = SimpleNamespace(state=SimpleNamespace(identity=None, role=STEWARD))
    return SimpleNamespace(context={"request": request})


async def _withdrawn_flag(db, name: str) -> bool:
    async with db.acquire() as conn:
        return bool(
            (
                await conn.execute_core(
                    select(registered_tables.c.withdrawn).where(
                        registered_tables.c.table_name == name
                    )
                )
            ).scalar_one()
        )


async def test_a_table_is_withdrawn_and_put_back_by_one_action_each(plane, actions):
    from provisa.api.admin.schema_mutation import _set_table_withdrawn

    async with plane.acquire() as conn:
        customers = await _id(conn, "customers")
    async with model_change.scope("save", actor=lambda: "ada"):
        out = await _set_table_withdrawn(_info(), customers, True, None)
    assert (out.success, out.code) == (True, "schema.table_withdrawn")
    assert await _withdrawn_flag(plane, "customers") is True
    async with model_change.scope("save", actor=lambda: "ada"):
        back = await _set_table_withdrawn(_info(), customers, False, None)
    assert (back.success, back.code) == (True, "schema.table_put_back")
    assert await _withdrawn_flag(plane, "customers") is False
    assert actions == [("rebuild",), ("rebuild",)]
    missing = await _set_table_withdrawn(_info(), 999_999, True, None)
    assert (missing.success, missing.code) == (False, "schema.table_not_found")


async def test_withdrawing_a_table_needs_table_registration_in_its_domain(
    plane, actions, monkeypatch
):
    from provisa.api.admin import schema_mutation

    asked: list[tuple] = []

    def _refuse(_info, capability, domain_id=None):
        asked.append((capability, domain_id))
        raise PermissionError(f"Missing capability: {capability!r}")

    monkeypatch.setattr(schema_mutation, "require_capability", _refuse)
    async with plane.acquire() as conn:
        customers = await _id(conn, "customers")
    with pytest.raises(PermissionError):
        await schema_mutation._set_table_withdrawn(_info(), customers, True, None)
    assert asked == [("table_registration", "sales")]
    assert await _withdrawn_flag(plane, "customers") is False


async def test_a_source_is_withdrawn_and_put_back_by_one_action_each(plane, actions):
    """Each worker's engine detaches or attaches it when its model reload reads the row
    (model_reload.reconcile_sources), this worker's included: the action writes the row and
    rebuilds."""
    from provisa.api.admin.schema_mutation import _set_source_withdrawn

    async with model_change.scope("save", actor=lambda: "ada"):
        out = await _set_source_withdrawn(_info(), "erp", True, None)
        assert "erp" in await _withdrawn_source_ids(plane)
        back = await _set_source_withdrawn(_info(), "erp", False, None)
        missing = await _set_source_withdrawn(_info(), "nope", True, None)
    assert (out.success, out.code) == (True, "schema.source_withdrawn")
    assert (back.success, back.code) == (True, "schema.source_put_back")
    assert (missing.success, missing.code) == (False, "schema.source_not_found")
    assert "erp" not in await _withdrawn_source_ids(plane)
    assert actions == [("rebuild",), ("rebuild",)]


async def _withdrawn_source_ids(db) -> frozenset[str]:
    async with db.acquire() as conn:
        return await source_repo.withdrawn_ids(conn)


async def test_putting_back_what_is_not_withdrawn_is_refused_by_name(plane, actions):
    """Putting back names a region: on a table or source in production it would move it past
    the origin region's holder, so it is refused."""
    from provisa.api.admin.schema_mutation import _set_source_withdrawn, _set_table_withdrawn

    async with plane.acquire() as conn:
        customers = await _id(conn, "customers")
    table = await _set_table_withdrawn(_info(), customers, False, None)
    assert (table.success, table.code, table.params) == (
        False,
        "schema.table_not_withdrawn",
        {"table": "customers"},
    )
    source = await _set_source_withdrawn(_info(), "erp", False, None)
    assert (source.success, source.code) == (False, "schema.source_not_withdrawn")
    assert actions == []


async def test_put_back_without_a_region_is_refused_naming_what_to_give(plane, actions):
    import strawberry

    from provisa.api.admin.schema_mutation import Mutation

    async with plane.acquire() as conn:
        customers = await _id(conn, "customers")
    table = await Mutation().put_back_table(_info(), customers, strawberry.UNSET)  # pyright: ignore[reportCallIssue]
    assert (table.code, table.params) == (
        "schema.put_back_needs_region",
        {"object": f"table {customers}"},
    )
    source = await Mutation().put_back_source(_info(), "erp", strawberry.UNSET)  # pyright: ignore[reportCallIssue]
    assert (source.code, source.params) == (
        "schema.put_back_needs_region",
        {"object": "source erp"},
    )
    assert actions == []


# --- a move between regions passes through withdrawn, by a holder for each region -----------------

EU_HOLDER, US_HOLDER = "eu_holder", "us_holder"


@pytest.fixture
def regions(plane, actions, monkeypatch):
    """A platform declaring the regions eu and us, and two people: one holding data_residency
    for eu, one for us. The org selects both regions (the stores each reads are not this test's)."""
    from provisa.api import app as appmod
    from provisa.api.admin import schema_mutation

    monkeypatch.setattr("provisa.api.admin.region_defaults.connected_region", lambda: "eu")
    monkeypatch.setattr(
        "provisa.core.repositories.region.require_table_region", _any_region_selected
    )
    monkeypatch.setattr("provisa.core.repositories.region.require_selected", _any_region_selected)
    monkeypatch.setattr(schema_mutation, "require_capability", lambda *a, **kw: None)
    holders = {
        holder: {
            "id": holder,
            "capabilities": ["data_residency", "table_registration", "source_registration"],
            "domain_access": ["*"],
            "residency_values": [region],
        }
        for holder, region in ((EU_HOLDER, "eu"), (US_HOLDER, "us"))
    }
    monkeypatch.setattr(appmod.state, "roles", holders, raising=False)


async def _any_region_selected(_conn, _what, _region) -> None:
    return None


def _as(holder: str) -> Any:
    identity = SimpleNamespace(user_id=holder, roles=[holder], acting_roles=[holder])
    request = SimpleNamespace(state=SimpleNamespace(identity=identity, role=holder))
    return SimpleNamespace(context={"request": request})


async def _region_of(db, name: str) -> str | None:
    async with db.acquire() as conn:
        return (
            await conn.execute_core(
                select(registered_tables.c.region).where(registered_tables.c.table_name == name)
            )
        ).scalar_one()


async def test_a_table_moves_between_regions_by_a_holder_for_each(plane, regions):
    """eu's holder withdraws a table kept in eu; us's holder puts it back into us. Neither
    needed the other's region."""
    from provisa.api.admin.schema_mutation import _set_table_withdrawn

    async with plane.acquire() as conn:
        customers = await _id(conn, "customers")
        await conn.execute_core(
            update(registered_tables).where(registered_tables.c.id == customers).values(region="eu")
        )
    async with model_change.scope("save", actor=lambda: "ada"):
        out = await _set_table_withdrawn(_as(EU_HOLDER), customers, True, None)
    assert (out.success, out.code) == (True, "schema.table_withdrawn")
    assert await _withdrawn_flag(plane, "customers") is True
    async with model_change.scope("save", actor=lambda: "ada"):
        back = await _set_table_withdrawn(_as(US_HOLDER), customers, False, "us")
    assert (back.success, back.code) == (True, "schema.table_put_back")
    assert await _withdrawn_flag(plane, "customers") is False
    assert await _region_of(plane, "customers") == "us"


async def test_only_the_origin_regions_holder_withdraws(plane, regions):
    from provisa.api.admin.schema_mutation import _set_table_withdrawn

    async with plane.acquire() as conn:
        customers = await _id(conn, "customers")
        await conn.execute_core(
            update(registered_tables).where(registered_tables.c.id == customers).values(region="eu")
        )
    refused = await _set_table_withdrawn(_as(US_HOLDER), customers, True, None)
    assert (refused.success, refused.code, refused.params) == (
        False,
        "security.withdraw_residency_refused",
        {"object": "table customers", "value": "eu"},
    )
    assert await _withdrawn_flag(plane, "customers") is False


async def test_putting_back_names_a_region_the_holders_grant_covers(plane, regions):
    from provisa.api.admin.schema_mutation import _set_table_withdrawn

    async with plane.acquire() as conn:
        customers = await _id(conn, "customers")
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.id == customers)
            .values(region="eu", withdrawn=True)
        )
    refused = await _set_table_withdrawn(_as(EU_HOLDER), customers, False, "us")
    assert (refused.success, refused.code, refused.params) == (
        False,
        "security.data_residency_refused",
        {"object": "table customers", "value": "us"},
    )
    # Not put back, and still where it was.
    assert await _withdrawn_flag(plane, "customers") is True
    assert await _region_of(plane, "customers") == "eu"


async def test_a_source_moves_by_a_holder_for_each_region(plane, regions):
    from provisa.api.admin.schema_mutation import _set_source_withdrawn

    async with plane.acquire() as conn:
        await conn.execute_core(update(sources).where(sources.c.id == "erp").values(region="eu"))
    refused = await _set_source_withdrawn(_as(US_HOLDER), "erp", True, None)
    assert refused.code == "security.withdraw_residency_refused"
    async with model_change.scope("save", actor=lambda: "ada"):
        out = await _set_source_withdrawn(_as(EU_HOLDER), "erp", True, None)
        refused_back = await _set_source_withdrawn(_as(EU_HOLDER), "erp", False, "us")
        back = await _set_source_withdrawn(_as(US_HOLDER), "erp", False, "us")
    assert (out.success, refused_back.code, back.success) == (
        True,
        "security.data_residency_refused",
        True,
    )
    async with plane.acquire() as conn:
        row = await source_repo.get(conn, "erp")
    assert row is not None and (row["withdrawn"], row["region"]) == (False, "us")


# --- every surface, built from the shared load --------------------------------------------------


@pytest.fixture
async def surfaces(plane, monkeypatch):
    """The role surfaces built as the app builds them, from the shared load, with customers
    withdrawn."""
    import provisa.api.app as appmod
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables
    from provisa.api.app_loaders import _build_and_register_schemas
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.core import domain_policy

    async with plane.acquire() as conn:
        await _withdraw_table(conn, "customers")
        tables = await fetch_tables(conn)
        withdrawn = await fetch_tables(conn, withdrawn=True)
        relationships = await fetch_relationships(conn)
        withdrawn_relationships = await fetch_relationships(conn, withdrawn=True)
    for t in [*tables, *withdrawn]:
        t.update(
            governance="pre-approved",
            write_ops=[],
            write_returns_rows=False,
            write_refused_forms=[],
        )
    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    state = appmod.state
    for name in ("roles", "schemas", "contexts", "rls_contexts", "table_path_maps", "proto_files"):
        monkeypatch.setattr(state, name, {}, raising=False)
    monkeypatch.setattr(state, "graphql_remote_sources", {}, raising=False)
    monkeypatch.setattr(
        state, "source_types", {"crm": "postgresql", "erp": "postgresql"}, raising=False
    )
    monkeypatch.setattr(state, "source_catalogs", {"crm": "crm", "erp": "erp"}, raising=False)
    monkeypatch.setattr(state, "model_db", plane, raising=False)
    col_types = {
        t["id"]: [
            ColumnMetadata(column_name=c["column_name"], data_type="integer", is_nullable=True)
            for c in t["columns"]
        ]
        for t in tables
    }
    _build_and_register_schemas(
        roles=[dict(r) for r in _ROLE_ROWS],
        tables=tables,
        relationships=relationships,
        col_types_converted=col_types,
        naming_rules=[],
        domains=[{"id": "sales"}],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        withdrawn_tables=withdrawn,
        withdrawn_relationships=withdrawn_relationships,
    )
    monkeypatch.setattr(
        state,
        "schema_build_cache",
        {"tables": tables, "relationships": relationships, "column_types": col_types},
        raising=False,
    )
    return state


@pytest.mark.parametrize("role", [READER, STEWARD])
def test_no_role_is_offered_a_withdrawn_table_or_its_relationship(surfaces, role):
    """REQ-1921: withdrawn serves no role -- not even one that may preview it."""
    from provisa.compiler.parser import GraphQLValidationError, parse_query

    schema = surfaces.schemas[role]
    fields = set(schema.query_type.fields)
    assert "orders" in fields
    assert not any(f.startswith("customers") for f in fields)
    # The relationship it is an end of is in no compiled graph.
    orders_type = schema.get_type("Orders")
    assert not any("Customers" in str(f.type) for f in orders_type.fields.values())
    with pytest.raises(GraphQLValidationError):
        parse_query(schema, "{ customers { id } }", ctx=surfaces.contexts[role])


@pytest.mark.parametrize("role", [READER, STEWARD])
def test_sql_names_a_withdrawn_table_as_unknown(surfaces, role):
    import sqlglot

    from provisa.compiler.sql_validator import _check_registered_relations

    gov: Any = SimpleNamespace(table_map={}, physical_map={})
    assert [
        v.code
        for v in _check_registered_relations(
            sqlglot.parse_one("SELECT id FROM customers"), gov, surfaces.contexts[role]
        )
    ] == ["V006"]


def _graph_schema(role: str, include_withdrawn: bool) -> dict:
    from provisa.api.rest.cypher_router import graph_schema

    request = SimpleNamespace(state=SimpleNamespace(role=role))
    response = asyncio.run(graph_schema(request, include_withdrawn=include_withdrawn))  # type: ignore[arg-type]
    return json.loads(bytes(response.body))


@pytest.mark.parametrize("role", [READER, STEWARD])
def test_the_graph_schema_leaves_a_withdrawn_table_and_its_relationships_out(surfaces, role):
    body = _graph_schema(role, include_withdrawn=False)
    assert {n["table_label"] for n in body["node_labels"]} == {"Orders", "Invoices"}
    assert body["relationship_types"] == []
    assert not any(n["withdrawn"] for n in body["node_labels"])


def test_the_graph_schema_shows_them_marked_to_a_steward_who_asks(surfaces):
    body = _graph_schema(STEWARD, include_withdrawn=True)
    nodes = {n["table_label"]: n["withdrawn"] for n in body["node_labels"]}
    assert nodes == {"Orders": False, "Invoices": False, "Customers": True}
    (rel,) = body["relationship_types"]
    assert rel["source"].endswith("Orders") and rel["target"].endswith("Customers")
    assert rel["withdrawn"] is True


def test_the_graph_schema_flag_shows_nothing_to_a_role_that_may_not_preview(surfaces):
    body = _graph_schema(READER, include_withdrawn=True)
    assert {n["table_label"] for n in body["node_labels"]} == {"Orders", "Invoices"}
    assert body["relationship_types"] == []


# --- the ERD's lists ------------------------------------------------------------------------------

#: The resolver's info, read only by preview_reach, which these tests replace.
_NO_INFO: Any = None


async def test_a_relationship_list_leaves_a_withdrawn_one_out_unless_a_steward_asks(
    plane, monkeypatch
):
    from provisa.api.admin import schema_query, withdrawn

    async with plane.acquire() as conn:
        await _withdraw_table(conn, "customers")
        rows = [
            dict(r._mapping) for r in (await conn.execute_core(select(relationships))).fetchall()
        ]

        monkeypatch.setattr(withdrawn, "preview_reach", lambda _info: frozenset({"sales"}))
        default = await schema_query._with_withdrawn(conn, _NO_INFO, False, rows)
        asked = await schema_query._with_withdrawn(conn, _NO_INFO, True, rows)
        monkeypatch.setattr(withdrawn, "preview_reach", lambda _info: frozenset())
        refused = await schema_query._with_withdrawn(conn, _NO_INFO, True, rows)
    assert default == []
    assert [(r.id, r.withdrawn) for r in asked] == [("orders_customer", True)]
    assert refused == []


def test_who_may_preview_a_withdrawn_table():
    from provisa.api.admin.withdrawn import previews

    assert previews(None, "sales")  # every domain
    assert previews(frozenset({"sales"}), "sales")
    assert not previews(frozenset({"finance"}), "sales")
    assert not previews(frozenset(), "sales")


# --- the steward's preview ----------------------------------------------------------------------


def test_a_withdrawn_table_is_found_for_its_stewards_preview_and_named_by_no_statement(surfaces):
    from provisa.api.admin import table_read

    withdrawn = surfaces.role_build_inputs["withdrawn_tables"]
    (customers,) = withdrawn
    assert table_read.withdrawn_table(surfaces, customers["id"]) is customers
    # A statement a caller sends never reaches it, a steward's included: there is no path that
    # names a withdrawn table to a statement.
    assert not hasattr(table_read, "drafts_a_statement_names")
    from provisa.api.role_surface import role_surface

    surface = role_surface(surfaces, STEWARD)
    assert surface is not None and "customers" not in surface.context.tables


def test_a_preview_of_a_withdrawn_sources_table_is_refused_saying_why(surfaces):
    from provisa.api.admin.table_read import _require_previewable
    from provisa.api.errors import ApiError

    table = {
        "table_name": "invoices",
        "source_id": "erp",
        "withdrawn": True,
        "source_withdrawn": True,
    }
    with pytest.raises(ApiError) as refused:
        _require_previewable(surfaces, table, None)
    assert refused.value.code == "schema.withdrawn_source_not_previewable"
    assert refused.value.params == {"table": "invoices", "source": "erp"}
