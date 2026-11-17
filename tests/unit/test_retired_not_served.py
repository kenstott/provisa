# Copyright (c) 2026 Kenneth Stott
# Canary: 3e8b1f62-7a4c-4d09-b5e1-6c2a9f0d8e47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A retired model object is not served anywhere, its name stays taken, and it can be restored
(REQ-2248).

The data plane's one load (``api.admin.db_queries``: fetch_tables, fetch_relationships) leaves a
retired table, column and relationship out, and the tag and metric registries leave a retired tag
or metric out; every surface is built from them, so none offers a retired object and a query
naming one is refused as naming nothing. The SQL catalog and meta views, which read the control
plane themselves, leave them out too. A registration at a retired name is refused naming the
retired object; a restore brings the same object back, served again."""

# Requirements: REQ-2248

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import insert, select, update

from provisa.core import model_change
from provisa.core.models import (
    Cardinality,
    Column,
    Metric,
    Relationship,
    Table,
    Tag,
    TagAssignment,
)
from provisa.core.repositories import metric as metric_repo
from provisa.core.repositories import relationship as rel_repo
from provisa.core.repositories import table as table_repo
from provisa.core.repositories import tag as tag_repo
from provisa.core.repositories.retirement import RetiredNameTaken
from provisa.core.schema_org import domains, registered_tables, roles, sources, table_columns

ROLE = "seller"
_ROLE_ROW = {
    "id": ROLE,
    "description": "Reads sales.",
    "capabilities": ["usage"],
    "domain_access": ["sales"],
}


@pytest.fixture(autouse=True)
def _one_organisation(monkeypatch):
    """A deployment of one organisation, as the API layer says at import (provisa.api.app)."""
    from provisa.core import secrets_providers

    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


def _table(name: str, *columns: str, alias: str | None = None):
    return Table(
        source_id="crm",
        domain_id="sales",
        schema_name="public",
        table_name=name,
        alias=alias,
        columns=[Column(name=c, visible_to=[ROLE], data_type="integer") for c in columns],
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
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(roles).values(id=ROLE, capabilities=["usage"], domain_access=["sales"])
        )
        await table_repo.upsert(conn, _table("orders", "id", "amount", "customer_id"))
        await table_repo.upsert(conn, _table("customers", "id"))
        await rel_repo.upsert(conn, _ORDERS_CUSTOMER)
        await tag_repo.upsert(conn, Tag(id="audited", description="Audited", applies_to=["column"]))
        orders = await _id(conn, "orders")
        await tag_repo.assign(
            conn,
            TagAssignment(
                tag_id="audited", object_type="column", table_id=orders, column_name="id"
            ),
        )
        await metric_repo.upsert(
            conn,
            Metric(name="revenue", expression="COUNT(orders.customer_id)", visible_to=[ROLE]),
        )
    return db


async def _id(conn, name: str) -> int:
    return (
        await conn.execute_core(
            select(registered_tables.c.id).where(registered_tables.c.table_name == name)
        )
    ).scalar_one()


async def _retire_customers_and_amount(conn) -> int:
    """Retire the customers table (its relationship first: a relationship holds a table it
    joins) and the amount column of orders. Returns the customers table's id."""
    customers = await _id(conn, "customers")
    assert await rel_repo.delete(conn, "orders_customer", reason="no longer joined")
    assert await table_repo.delete(conn, customers, reason="moved to crm2")
    await table_repo.upsert(conn, _table("orders", "id", "customer_id"))
    return customers


# --- the shared load, each kind -----------------------------------------------------------------


async def test_a_retired_table_and_column_are_absent_from_the_data_planes_load(plane):
    from provisa.api.admin.db_queries import fetch_tables

    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        await _retire_customers_and_amount(conn)
        served = {t["table_name"]: t for t in await fetch_tables(conn)}
        drafts = await fetch_tables(conn, draft=True)
    assert set(served) == {"orders"}
    assert [c["column_name"] for c in served["orders"]["columns"]] == ["id", "customer_id"]
    assert drafts == []


async def test_a_retired_relationship_and_one_reaching_a_retired_table_are_not_traversable(plane):
    from provisa.api.admin.db_queries import fetch_relationships

    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        assert [r["id"] for r in await fetch_relationships(conn)] == ["orders_customer"]
        assert await rel_repo.delete(conn, "orders_customer")
        assert await fetch_relationships(conn) == []
        assert await rel_repo.restore(conn, "orders_customer")
        assert [r["id"] for r in await fetch_relationships(conn)] == ["orders_customer"]
        # A live relationship whose target is retired (as a merge can leave one) is not served.
        await conn.execute_core(
            update(registered_tables)
            .where(registered_tables.c.table_name == "customers")
            .values(retired=True)
        )
        assert await fetch_relationships(conn) == []


@pytest.mark.parametrize(("table", "column"), [("orders", "customer_id"), ("customers", "id")])
async def test_a_relationship_joining_a_retired_column_of_a_live_table_is_not_traversable(
    plane, table, column
):
    from provisa.api.admin.db_queries import fetch_relationships

    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        # Both tables stay in service; only the joined column is retired (as a merge can leave
        # it), at the relationship's source end or its target end.
        table_id = await _id(conn, table)
        await conn.execute_core(
            update(table_columns)
            .where(table_columns.c.table_id == table_id, table_columns.c.column_name == column)
            .values(retired=True)
        )
        assert await fetch_relationships(conn) == []
        assert await table_repo.restore_column(conn, table_id, column)
        assert [r["id"] for r in await fetch_relationships(conn)] == ["orders_customer"]


async def test_a_retired_tag_is_not_offered_and_its_assignments_are_not_applied(plane):
    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        assert "audited" in {t["id"] for t in await tag_repo.list_all(conn)}
        assert [a["tag_id"] for a in await tag_repo.list_assignments(conn)] == ["audited"]
        assert await tag_repo.delete(conn, "audited", reason="unused")
        assert "audited" not in {t["id"] for t in await tag_repo.list_all(conn)}
        assert await tag_repo.list_assignments(conn) == []
        assert [t["id"] for t in await tag_repo.list_all(conn, retired=True)] == ["audited"]


async def test_a_tag_on_a_retired_column_is_not_applied(plane):
    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        await table_repo.upsert(conn, _table("orders", "amount", "customer_id"))
        assert await tag_repo.list_assignments(conn) == []


async def test_a_retired_metric_is_not_offered(plane):
    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        assert await metric_repo.delete(conn, "revenue", reason="replaced")
        assert await metric_repo.list_all(conn) == []
        retired = await metric_repo.list_all(conn, retired=True)
    assert [(m["name"], m["status_by"], m["status_reason"]) for m in retired] == [
        ("revenue", "ada", "replaced")
    ]


async def test_a_retired_tables_api_endpoint_is_not_served(plane):
    from provisa.api_source.loader import load_api_sources
    from provisa.core.schema_org import api_endpoints, api_sources

    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        await conn.execute_core(insert(api_sources).values(id="crm", type="openapi"))
        await conn.execute_core(
            insert(api_endpoints).values(
                source_id="crm", path="/customers", method="GET", table_name="customers", columns=[]
            )
        )
        served, _ = await load_api_sources(conn, {})
        assert ("crm", "customers") in served
        await _retire_customers_and_amount(conn)
        served, _ = await load_api_sources(conn, {})
    assert ("crm", "customers") not in served


# --- every surface, built from the shared load --------------------------------------------------


@pytest.fixture
async def surfaces(plane, monkeypatch):
    """The role surfaces built as the app builds them, from the shared load, with the customers
    table and the amount column of orders retired."""
    import provisa.api.app as appmod
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables
    from provisa.api.app_loaders import _build_and_register_schemas
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.core import domain_policy

    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        await _retire_customers_and_amount(conn)
        tables = await fetch_tables(conn)
        relationships = await fetch_relationships(conn)
    for t in tables:
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
    monkeypatch.setattr(state, "source_types", {"crm": "postgresql"}, raising=False)
    monkeypatch.setattr(state, "source_catalogs", {"crm": "crm"}, raising=False)
    monkeypatch.setattr(state, "model_db", plane, raising=False)
    _build_and_register_schemas(
        roles=[dict(_ROLE_ROW)],
        tables=tables,
        relationships=relationships,
        col_types_converted={
            t["id"]: [
                ColumnMetadata(column_name=c["column_name"], data_type="integer", is_nullable=True)
                for c in t["columns"]
            ]
            for t in tables
        },
        naming_rules=[],
        domains=[{"id": "sales"}],
        domain_prefix=False,
        kafka_physical={},
        tracked_functions=[],
        tracked_webhooks=[],
        gql_object_cols={},
        rls_rules=[],
        metrics=[],
        draft_tables=[],
        withdrawn_tables=[],
        withdrawn_relationships=[],
    )
    return state


def test_graphql_offers_no_retired_table_or_column_and_refuses_a_query_naming_one(surfaces):
    from provisa.compiler.parser import GraphQLValidationError, parse_query

    schema = surfaces.schemas[ROLE]
    ctx = surfaces.contexts[ROLE]
    fields = set(schema.query_type.fields)
    assert "orders" in fields
    assert not any(f.startswith("customers") for f in fields)
    assert "amount" not in schema.get_type("Orders").fields
    for query in ("{ customers { id } }", "{ orders { amount } }"):
        with pytest.raises(GraphQLValidationError):
            parse_query(schema, query, ctx=ctx)


def test_sql_refuses_a_statement_naming_a_retired_table_as_unknown(surfaces):
    import sqlglot

    from provisa.compiler.sql_validator import _check_registered_relations

    ctx = surfaces.contexts[ROLE]
    gov: Any = SimpleNamespace(table_map={}, physical_map={})
    assert [
        v.code
        for v in _check_registered_relations(
            sqlglot.parse_one("SELECT id FROM customers"), gov, ctx
        )
    ] == ["V006"]
    orders = next(m for m in ctx.tables.values() if m.table_name == "orders")
    assert (orders.table_id, "amount") not in ctx.physical_to_sql
    assert (orders.table_id, "id") in ctx.physical_to_sql


def _label_map(state):
    from provisa.gql.label_map import CypherLabelMap

    return CypherLabelMap.from_schema(state.contexts[ROLE], ["sales"])


def test_cypher_and_bolt_offer_no_retired_label_property_or_relationship(surfaces):
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.translator import cypher_to_sql

    lm = _label_map(surfaces)
    tables = {nm.table_name: nm for nm in lm.nodes.values()}
    assert set(tables) == {"orders"}
    assert "amount" not in tables["orders"].properties.values()
    assert lm.relationships == {}
    with pytest.raises(Exception, match="(?i)customers"):
        cypher_to_sql(parse_cypher("MATCH (c:Customers) RETURN c", lm), lm, {})


def test_sparql_has_no_class_or_predicate_for_a_retired_table_or_column(surfaces):
    from provisa.sparql_query.vocabulary import Grounding, build_vocabulary, column_types_of

    lm = _label_map(surfaces)
    base = "https://example.test/org"
    vocab = build_vocabulary(lm, base, column_types_of(lm, surfaces.contexts[ROLE]), Grounding())
    iris = repr(vocab)
    assert "/orders" in iris
    assert "customers" not in iris
    assert "amount" not in iris


def test_rest_and_its_openapi_have_no_path_for_a_retired_table_or_column(surfaces):
    from provisa.api.rest.openapi_spec import generate_rest_openapi_spec

    spec = generate_rest_openapi_spec(surfaces, ROLE)
    text = repr(spec)
    assert any("orders" in p for p in spec["paths"])
    assert not any("customers" in p for p in spec["paths"])
    assert "amount" not in text


def test_grpc_has_no_message_or_field_for_a_retired_table_or_column(surfaces):
    proto = surfaces.proto_files[ROLE]
    assert "Orders" in proto
    assert "Customers" not in proto and "customers" not in proto
    assert "amount" not in proto
    assert "Customers" not in surfaces.wire_proto and "amount" not in surfaces.wire_proto


async def test_flight_sql_and_mcp_catalog_list_no_retired_table_or_column(surfaces):
    from provisa.api.flight.catalog import _build_catalog_tables_async

    for role in (None, ROLE):
        listed = {t.table_name: t for t in await _build_catalog_tables_async(surfaces, role)}
        assert set(listed) == {"orders"}, role
        assert "amount" not in {c.name for c in listed["orders"].columns}, role


# --- the SQL catalog (pgwire, Flight) and the meta views ----------------------------------------


_SCHEMA = "org_default"


@pytest.fixture
def catalog(embedded_postgres, control_plane_template):
    from provisa.api._meta_views import _META_TABLE_VIEWS
    from provisa.core import schema_org as t
    from provisa.transpiler.metadata_views import CATALOG_VIEW_NAMES, NEUTRAL_VIEW_NAMES
    from tests.pg_control_plane import clone

    held = clone(embedded_postgres, control_plane_template)
    engine = held.engine(f"{held.url}?options=-csearch_path%3D{_SCHEMA},public")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA meta")
        conn.execute(t.sources.insert().values(id="crm", type="postgresql"))
        conn.execute(t.domains.insert().values(id="sales", description="Sales"))
        for table_id, name, retired in ((1, "orders", False), (2, "customers", True)):
            conn.execute(
                t.registered_tables.insert().values(
                    id=table_id,
                    source_id="crm",
                    domain_id="sales",
                    schema_name="public",
                    table_name=name,
                    retired=retired,
                )
            )
        for table_id, name, retired in ((1, "id", False), (1, "amount", True), (2, "id", False)):
            conn.execute(
                t.table_columns.insert().values(
                    table_id=table_id, column_name=name, data_type="integer", retired=retired
                )
            )
        for rel_id, retired in (("orders_self", False), ("orders_customer", True)):
            conn.execute(
                t.relationships.insert().values(
                    id=rel_id,
                    source_table_id=1,
                    target_table_id=1 if rel_id == "orders_self" else 2,
                    source_column="id",
                    target_column="id",
                    cardinality="many-to-one",
                    retired=retired,
                )
            )
        conn.execute(t.metrics.insert().values(name="revenue", expression="SUM(x)", retired=True))
        conn.execute(t.metrics.insert().values(name="orders_count", expression="COUNT(*)"))
        for ddl in _META_TABLE_VIEWS.values():
            conn.exec_driver_sql(ddl.replace("%", "%%"))
        for name in (*NEUTRAL_VIEW_NAMES, *CATALOG_VIEW_NAMES):
            conn.exec_driver_sql(f"CREATE VIEW meta.{name} AS SELECT * FROM {_SCHEMA}.{name}")
    yield engine
    held.close()


def _read(conn, sql: str) -> list[tuple]:
    return [tuple(r) for r in conn.exec_driver_sql(sql.replace("%", "%%")).fetchall()]


def test_the_sql_catalog_lists_no_retired_table_column_or_metric(catalog):
    from provisa.transpiler.metadata_oids import table_oid
    from provisa.transpiler.metadata_settings import set_statement, statement_settings

    with catalog.begin() as conn:
        sql, values = set_statement(statement_settings(ROLE, True, "provisa", False, session={}))
        for n, value in reversed(list(enumerate(values, 1))):
            sql = sql.replace(f"${n}", "'" + value.replace("'", "''") + "'")
        conn.exec_driver_sql(sql)
        relations = {n for (n,) in _read(conn, "SELECT relname FROM meta.catalog_pg_class")}
        columns = _read(
            conn,
            f"SELECT attname FROM meta.catalog_pg_attribute WHERE attrelid = {table_oid(1)} "
            "AND attnum > 0 ORDER BY attnum",
        )
    assert "orders" in relations and "orders_count" in relations
    assert "customers" not in relations and "revenue" not in relations
    assert ("amount",) not in columns and ("id",) in columns


def test_the_meta_views_list_no_retired_table_column_or_metric(catalog):
    with catalog.begin() as conn:
        tables = _read(conn, "SELECT table_name FROM registered_tables_meta ORDER BY id")
        columns = _read(conn, "SELECT column_name FROM table_columns_meta ORDER BY id")
        metrics = _read(conn, "SELECT name FROM metric_definitions ORDER BY name")
    assert tables == [("orders",)]
    assert columns == [("id",)]
    assert metrics == [("orders_count",)]


def test_the_meta_relationships_list_no_retired_relationship(catalog):
    with catalog.begin() as conn:
        listed = _read(conn, "SELECT id FROM relationships_meta ORDER BY id")
    assert listed == [("orders_self",)]


# --- the write path: a retired name stays taken -------------------------------------------------


async def test_registering_a_table_at_a_retired_name_is_refused_naming_it(plane):
    from provisa.api.admin._retired_objects import retired_name_result

    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        await _retire_customers_and_amount(conn)
        for again in (
            _table("customers", "id"),  # the same physical table
            # another physical table (another schema) under its model name
            _table("customers", "id").model_copy(update={"schema_name": "archive"}),
            _table("people", "id", alias="customers"),  # another one aliased to it
        ):
            with pytest.raises(RetiredNameTaken) as refused:
                await table_repo.upsert(conn, again)
            assert (refused.value.kind, refused.value.name) == ("table", "customers")
    said = retired_name_result(refused.value)
    assert said.success is False and said.code == "schema.retired_name_taken"
    assert said.message == (
        "the name 'customers' belongs to a retired table; restore that table to use it again"
    )


async def test_saving_a_relationship_tag_or_metric_at_a_retired_name_is_refused(plane):
    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        assert await rel_repo.delete(conn, "orders_customer")
        assert await tag_repo.delete(conn, "audited")
        assert await metric_repo.delete(conn, "revenue")
        for kind, save in (
            ("relationship", lambda: rel_repo.upsert(conn, _ORDERS_CUSTOMER)),
            ("tag", lambda: tag_repo.upsert(conn, Tag(id="audited", applies_to=["column"]))),
            (
                "metric",
                lambda: metric_repo.upsert(
                    conn, Metric(name="revenue", expression="COUNT(*)", visible_to=[ROLE])
                ),
            ),
        ):
            with pytest.raises(RetiredNameTaken, match="restore that " + kind) as refused:
                await save()
            assert refused.value.kind == kind


# --- restore ------------------------------------------------------------------------------------


async def test_restoring_returns_the_same_object_served_again(plane):
    from provisa.api.admin.db_queries import fetch_relationships, fetch_tables

    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        customers = await _retire_customers_and_amount(conn)
        orders = await _id(conn, "orders")
        assert await tag_repo.delete(conn, "audited")
        assert await metric_repo.delete(conn, "revenue")

        assert await table_repo.restore(conn, customers)
        assert await table_repo.restore_column(conn, orders, "amount")
        assert await rel_repo.restore(conn, "orders_customer")
        assert await tag_repo.restore(conn, "audited")
        assert await metric_repo.restore(conn, "revenue")
        assert not await table_repo.restore(conn, customers)  # not retired any more

        served = {t["table_name"]: t for t in await fetch_tables(conn)}
        assert served["customers"]["id"] == customers
        # Back in its table; registration order puts it after the columns saved since.
        assert [c["column_name"] for c in served["orders"]["columns"]] == [
            "id",
            "customer_id",
            "amount",
        ]
        assert [r["id"] for r in await fetch_relationships(conn)] == ["orders_customer"]
        assert "audited" in {t["id"] for t in await tag_repo.list_all(conn)}
        assert [a["tag_id"] for a in await tag_repo.list_assignments(conn)] == ["audited"]
        assert [m["name"] for m in await metric_repo.list_all(conn)] == ["revenue"]
        row = await table_repo.get(conn, customers)
    assert row is not None
    assert (row["retired"], row["status_by"], row["status_reason"]) == (False, None, None)


# --- the admin: lists, the retired listing, the table form ---------------------------------------


def _info() -> Any:
    """An admin request in dev mode (no identity): every right and domain."""
    return SimpleNamespace(context={"request": SimpleNamespace(state=SimpleNamespace())})


async def test_the_admin_lists_leave_retired_objects_out_and_the_retired_listing_has_them(plane):
    from provisa.api.admin._retired_objects import restore, retired_objects

    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        customers = await _retire_customers_and_amount(conn)
        assert await tag_repo.delete(conn, "audited", reason="unused")
        assert await metric_repo.delete(conn, "revenue", reason="replaced")
        assert [t["table_name"] for t in await table_repo.list_all(conn)] == ["orders"]
        assert await rel_repo.list_all(conn) == []
        orders = await _id(conn, "orders")
        listed = await retired_objects(_info(), conn)
        said = await restore(_info(), conn, "column", f"{orders}.amount")
    assert [(o.kind, o.key, o.name, o.retired_by, o.reason) for o in listed] == [
        ("table", str(customers), "customers", "ada", "moved to crm2"),
        (
            "column",
            f"{orders}.amount",
            "orders.amount",
            "ada",
            "dropped from the table's registration",
        ),
        ("relationship", "orders_customer", "orders_customer", "ada", "no longer joined"),
        ("tag", "audited", "audited", "ada", "unused"),
        ("metric", "revenue", "revenue", "ada", "replaced"),
    ]
    assert all(o.retired_at is not None for o in listed)
    assert (said.success, said.code) == (True, "schema.restored")


async def test_the_table_form_round_trip_of_a_table_with_a_retired_column_saves(plane):
    """The form reads the table as the admin lists it and saves what it read: the retired
    column is not in it, so the save leaves it retired rather than naming it again."""
    async with model_change.scope("save", actor=lambda: "ada"), plane.acquire() as conn:
        await table_repo.upsert(conn, _table("orders", "id", "customer_id"))
        orders = await _id(conn, "orders")
        stored = await table_repo.get(conn, orders)
        assert stored is not None
        form = _table("orders", *(c["column_name"] for c in stored["columns"])).model_copy(
            update={"description": "Every order"}
        )
        assert await table_repo.upsert(conn, form) == orders
        retired = await table_repo.retired_columns(conn, orders)
    assert [c["column_name"] for c in stored["columns"]] == ["id", "customer_id"]
    assert [(c["column_name"], c["retired"]) for c in retired] == [("amount", True)]
