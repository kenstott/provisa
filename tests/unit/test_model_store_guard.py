# Copyright (c) 2026 Kenneth Stott
# Canary: 7edf976c-165a-4fa5-a4b8-de2e0967fb84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The model store's dependency guard and its inventory (REQ-1917, REQ-1918, REQ-1919).

Two halves. The inventory is checked against the schema: every declared foreign key is listed
and every listed column exists. Then ``guard``, ``parts``, ``remove_parts`` and ``circle_of`` are
run against rows on a seeded control plane, for each kind's parts and dependents as REQ-1918
states them, and for the shapes that could look like a circle.
"""

# Requirements: REQ-1917, REQ-1918, REQ-1919

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import func, insert, select

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.repositories import integrity
from provisa.core.repositories.integrity import Match, ObjectRef, Standing
from provisa.core.schema_org import metadata

# --- the inventory against the schema ------------------------------------------------------------


def test_every_declared_foreign_key_is_in_the_inventory():
    listed = {(r.table, r.column, integrity.KINDS[r.to].table) for r in integrity.REFERENCES}
    missing = [
        f"{table.name}.{column.name} -> {fk.column.table.name}"
        for table in metadata.sorted_tables
        for column in table.columns
        for fk in column.foreign_keys
        if (table.name, column.name, fk.column.table.name) not in listed
    ]
    assert missing == [], "a foreign key the inventory does not declare a part or a dependent"


def test_every_reference_names_columns_that_exist():
    for r in integrity.REFERENCES:
        table = metadata.tables[r.table]
        for name in (r.column, *r.ends, *([r.owner] if r.owner else [])):
            assert name in table.c, f"{r.table}.{name}"
        kind = integrity.KINDS[r.to]
        assert r.by in ("key", "view_mv_id", "schema_table") or (r.by == "name" and kind.name), (
            r.table,
            r.column,
        )
    for kind in integrity.KINDS.values():
        table = metadata.tables[kind.table]
        assert kind.key in table.c and (kind.name is None or kind.name in table.c), kind


def test_a_dependent_names_the_object_that_blocks_and_a_part_is_found_by_equality():
    for r in integrity.REFERENCES:
        if r.standing is Standing.DEPENDENT:
            assert r.of in integrity.KINDS and r.owner is not None, (r.table, r.column)
        else:
            assert r.match is Match.EQUALS, (r.table, r.column)
            assert (r.of is None) == (r.owner is None), (r.table, r.column)


def test_no_reference_is_declared_twice():
    seen = [(r.table, r.column, r.to) for r in integrity.REFERENCES]
    assert len(seen) == len(set(seen))


def test_sql_text_that_does_not_parse_is_an_error_not_nothing():
    with pytest.raises(ValueError, match="could not be parsed"):
        integrity.names_in_sql("SELECT FROM WHERE (")


def test_the_names_a_sql_text_reads():
    relations, metric_names = integrity.names_in_sql(
        "WITH recent AS (SELECT * FROM orders o) "
        "SELECT r.id, m.total FROM recent r JOIN metrics.revenue m ON m.id = r.id"
    )
    assert (relations, metric_names) == ({"orders"}, {"revenue"})
    assert integrity.names_in_sql("SUM(orders.amount) - SUM(refunds.amount)")[0] == {
        "orders",
        "refunds",
    }


# --- rows ----------------------------------------------------------------------------------------


class Plane:
    """A seeded SQLite control plane and short ways to put rows on it and ask the guard."""

    def __init__(self, db: Database) -> None:
        self.db = db

    async def add(self, table: str, **values: Any) -> Any:
        tbl = metadata.tables[table]
        async with self.db.acquire() as conn:
            result = await conn.execute_core(insert(tbl).values(**values))
            return result.inserted_primary_key[0] if result.inserted_primary_key else None

    async def table(self, name: str, domain: str = "sales", **values: Any) -> ObjectRef:
        table_id = await self.add(
            "registered_tables",
            source_id="pg",
            domain_id=domain,
            schema_name="public",
            table_name=name,
            origin="admin",
            **values,
        )
        return ObjectRef("table", table_id)

    async def relationship(self, rel_id: str, source: ObjectRef, target: ObjectRef) -> ObjectRef:
        await self.add(
            "relationships",
            id=rel_id,
            source_table_id=source.id,
            target_table_id=target.id,
            source_column="id",
            target_column="id",
            cardinality="many-to-one",
            origin="admin",
        )
        return ObjectRef("relationship", rel_id)

    async def blocking(self, ref: ObjectRef) -> set[ObjectRef]:
        async with self.db.acquire() as conn:
            return {d.ref for d in await integrity.guard(conn, ref)}

    async def via(self, ref: ObjectRef) -> dict[ObjectRef, tuple[str, ...]]:
        async with self.db.acquire() as conn:
            return {d.ref: d.via for d in await integrity.guard(conn, ref)}

    async def going(self, ref: ObjectRef) -> dict[str, int]:
        async with self.db.acquire() as conn:
            return await integrity.parts(conn, ref)

    async def circle(self, ref: ObjectRef) -> list[ObjectRef]:
        async with self.db.acquire() as conn:
            return await integrity.circle_of(conn, ref)

    async def remove(self, ref: ObjectRef) -> None:
        """What a kind's delete does once the guard returns nothing: its parts, then its row."""
        kind = integrity.KINDS[ref.kind]
        table = metadata.tables[kind.table]
        async with self.db.acquire() as conn:
            async with conn.transaction():
                assert await integrity.guard(conn, ref) == []
                await integrity.remove_parts(conn, ref)
                await conn.execute_core(table.delete().where(table.c[kind.key] == ref.id))

    async def count(self, table: str) -> int:
        async with self.db.acquire() as conn:
            return (
                await conn.execute_core(select(func.count()).select_from(metadata.tables[table]))
            ).scalar_one()


@pytest.fixture
async def plane() -> Plane:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="guard-test")
    await _init_schema_portable(db)
    p = Plane(db)
    await p.add("sources", id="pg", type="postgresql", origin="admin")
    for domain_id in ("sales", "finance"):
        await p.add("domains", id=domain_id, origin="admin")
    return p


async def test_asking_about_an_object_that_is_not_there_is_a_lookup_error(plane):
    with pytest.raises(LookupError, match="no table 999"):
        await plane.blocking(ObjectRef("table", 999))


# --- a domain is blocked by everything that refers to it (REQ-1917) ------------------------------


async def test_everything_that_refers_to_a_domain_blocks_it(plane):
    orders = await plane.table("orders")
    await plane.add("table_columns", table_id=orders.id, column_name="id", domain_id="sales")
    await plane.add("data_products", id="dp", domain_id="sales", name="Sales", origin="admin")
    await plane.add(
        "roles", id="seller", capabilities=[], domain_access=["sales", "finance"], origin="admin"
    )
    await plane.add("roles", id="everywhere", capabilities=[], domain_access=["*"], origin="admin")
    async with plane.db.acquire() as conn:
        src = metadata.tables["sources"]
        await conn.execute_core(
            src.update().where(src.c.id == "pg").values(allowed_domains=["sales"])
        )
    rule = await plane.add(
        "rls_rules", role_id="seller", domain_id="sales", filter_expr=b"1=1", origin="admin"
    )
    await plane.add("tracked_functions", name="refund", domain_id="sales", origin="admin")
    await plane.add("tracked_webhooks", name="notify", domain_id="sales", origin="admin")
    assignment = await plane.add(
        "user_role_assignments", user_id="u1", role_id="seller", domain_id="sales"
    )
    term = await plane.add("glossary_terms", name="Order", origin="admin")
    await plane.add("glossary_term_domains", term_id=term, domain_id="sales")

    sales = ObjectRef("domain", "sales")
    assert await plane.blocking(sales) == {
        orders,
        ObjectRef("data_product", "dp"),
        ObjectRef("role", "seller"),
        ObjectRef("source", "pg"),
        ObjectRef("row_filter", rule),
        ObjectRef("command", "refund"),
        ObjectRef("webhook", "notify"),
        ObjectRef("role_assignment", assignment),
        ObjectRef("glossary_term", term),
    }
    # A domain removes only itself: it has no parts.
    assert await plane.going(sales) == {}
    # The table blocks through two columns, reported once.
    assert (await plane.via(sales))[orders] == (
        "registered_tables.domain_id",
        "table_columns.domain_id",
    )
    assert await plane.blocking(ObjectRef("domain", "finance")) == {ObjectRef("role", "seller")}


async def test_a_domain_nothing_refers_to_may_go(plane):
    assert await plane.blocking(ObjectRef("domain", "finance")) == set()
    await plane.remove(ObjectRef("domain", "finance"))


# --- a source is blocked by its registered tables ------------------------------------------------


async def test_a_source_is_blocked_by_its_tables_and_takes_its_registrations_with_it(plane):
    await plane.add("sources", id="api", type="openapi", origin="admin")
    await plane.add("api_sources", id="api", type="openapi", base_url="http://x")
    await plane.add("api_endpoints", source_id="api", path="/p", table_name="t", columns=[])
    table = await plane.table("t")
    async with plane.db.acquire() as conn:
        tbl = metadata.tables["registered_tables"]
        await conn.execute_core(tbl.update().where(tbl.c.id == table.id).values(source_id="api"))
    api = ObjectRef("source", "api")
    assert await plane.blocking(api) == {table}
    await plane.remove(table)
    assert await plane.blocking(api) == set()
    assert await plane.going(api) == {"api_sources.id": 1}
    await plane.remove(api)
    # The registration row went, and its own endpoints with it.
    assert await plane.count("api_sources") == 0 and await plane.count("api_endpoints") == 0


# --- a table: what it takes with it, and what blocks it ------------------------------------------


async def test_a_table_is_blocked_by_what_reads_it_and_by_every_relationship_it_is_in(plane):
    orders, customers = await plane.table("orders"), await plane.table("customers")
    regions = await plane.table("regions")
    out = await plane.relationship("orders_customers", orders, customers)
    incoming = await plane.relationship("regions_orders", regions, orders)
    view = await plane.table("open_orders", view_sql="SELECT o.id FROM orders o WHERE o.open")
    await plane.add("metrics", name="revenue", expression="SUM(orders.amount)", origin="admin")
    await plane.add(
        "materialized_views",
        id="mv-daily",
        source_tables=["orders"],
        target_catalog="c",
        target_schema="s",
        target_table="daily",
    )
    await plane.add(
        "tracked_functions", name="recent_orders", returns="public.orders", origin="admin"
    )
    await plane.add(
        "tracked_webhooks", name="notify", url="http://x", returns="public.orders", origin="admin"
    )
    await plane.add("tracked_functions", name="elsewhere", returns="archive.orders", origin="admin")
    assert await plane.blocking(orders) == {
        out,
        incoming,
        view,
        ObjectRef("metric", "revenue"),
        ObjectRef("materialized_view", "mv-daily"),
        ObjectRef("command", "recent_orders"),
        ObjectRef("webhook", "notify"),
    }
    assert await plane.blocking(customers) == {out}
    # Nothing refers to a relationship, so it can always go first.
    assert await plane.blocking(out) == set()


async def test_a_table_takes_its_columns_row_filters_and_tags_with_it(plane):
    orders = await plane.table("orders")
    for column in ("id", "amount"):
        await plane.add("table_columns", table_id=orders.id, column_name=column)
    await plane.add("roles", id="seller", capabilities=[], domain_access=["sales"], origin="admin")
    await plane.add(
        "rls_rules", role_id="seller", table_id=orders.id, filter_expr=b"1=1", origin="admin"
    )
    await plane.add(
        "tag_assignments",
        tag_id="pii",
        base_tag_id="pii",
        object_type="table",
        object_key="orders",
        table_id=orders.id,
        origin="admin",
    )
    other = await plane.table("customers")
    await plane.add("table_columns", table_id=other.id, column_name="id")

    assert await plane.blocking(orders) == set()
    assert await plane.going(orders) == {
        "rls_rules.table_id": 1,
        "table_columns.table_id": 2,
        "tag_assignments.table_id": 1,
    }
    await plane.remove(orders)
    assert await plane.count("table_columns") == 1  # the other table's column
    assert await plane.count("rls_rules") == 0 and await plane.count("tag_assignments") == 0


async def test_a_views_storage_row_and_its_log_go_with_the_view(plane):
    view = await plane.table("open_orders", view_sql="SELECT 1 AS id", materialize=True)
    await plane.add(
        "materialized_views",
        id="view-open_orders",
        source_tables=[],
        target_catalog="c",
        target_schema="s",
        target_table="open_orders",
    )
    await plane.add("mv_refresh_log", mv_id="view-open_orders", status="success")
    assert await plane.blocking(view) == set()
    assert await plane.going(view) == {"materialized_views.id": 1}
    await plane.remove(view)
    assert await plane.count("materialized_views") == 0 and await plane.count("mv_refresh_log") == 0


# --- a role is blocked by anyone who holds it and every grant that names it ----------------------


async def test_a_role_is_blocked_by_its_holders_its_heirs_and_every_grant_naming_it(plane):
    orders = await plane.table("orders")
    column = await plane.add(
        "table_columns", table_id=orders.id, column_name="id", visible_to=["seller", "analyst"]
    )
    await plane.add("roles", id="seller", capabilities=[], domain_access=["sales"], origin="admin")
    await plane.add(
        "roles",
        id="junior",
        capabilities=[],
        domain_access=[],
        parent_role_id="seller",
        origin="admin",
    )
    assignment = await plane.add(
        "user_role_assignments", user_id="u1", role_id="seller", domain_id="*"
    )
    await plane.add(
        "rls_rules", role_id="seller", table_id=orders.id, filter_expr=b"1=1", origin="admin"
    )
    await plane.add(
        "metrics",
        name="revenue",
        expression="SUM(orders.amount)",
        visible_to=["seller"],
        origin="admin",
    )
    await plane.add("tracked_functions", name="refund", visible_to=["seller"], origin="admin")
    await plane.add(
        "data_products",
        id="dp",
        domain_id="sales",
        name="Sales",
        owner_role="seller",
        origin="admin",
    )
    async with plane.db.acquire() as conn:
        dom = metadata.tables["domains"]
        await conn.execute_core(dom.update().where(dom.c.id == "finance").values(steward="seller"))

    seller = ObjectRef("role", "seller")
    assert await plane.via(seller) == {
        ObjectRef("role", "junior"): ("roles.parent_role_id",),
        ObjectRef("role_assignment", assignment): ("user_role_assignments.role_id",),
        ObjectRef("column", column): ("table_columns.visible_to",),
        ObjectRef("metric", "revenue"): ("metrics.visible_to",),
        ObjectRef("command", "refund"): ("tracked_functions.visible_to",),
        ObjectRef("data_product", "dp"): ("data_products.owner_role",),
        ObjectRef("domain", "finance"): ("domains.steward",),
    }
    # Its row filters go with it.
    assert await plane.going(seller) == {"rls_rules.role_id": 1}


# --- a data product is blocked by its members; a tag takes its assignments -----------------------


async def test_a_data_product_is_blocked_by_its_members(plane):
    await plane.add("data_products", id="dp", domain_id="sales", name="Sales", origin="admin")
    member = await plane.table("orders", product_id="dp")
    await plane.add("tracked_functions", name="refund", product_id="dp", origin="admin")
    assert await plane.blocking(ObjectRef("data_product", "dp")) == {
        member,
        ObjectRef("command", "refund"),
    }


async def test_a_tag_takes_its_assignments_and_values_with_it(plane):
    await plane.add("tags", id="pii", origin="admin")
    await plane.add("tag_param_values", tag_id="pii", value="high")
    orders = await plane.table("orders")
    for column in ("email", "phone"):
        await plane.add(
            "tag_assignments",
            tag_id="pii",
            base_tag_id="pii",
            object_type="column",
            object_key=f"orders.{column}",
            table_id=orders.id,
            column_name=column,
            origin="admin",
        )
    pii = ObjectRef("tag", "pii")
    assert await plane.blocking(pii) == set()
    # The count of objects that lose the tag, for the confirmation.
    assert await plane.going(pii) == {
        "tag_assignments.base_tag_id": 2,
        "tag_param_values.tag_id": 1,
    }
    await plane.remove(pii)
    assert await plane.count("tag_assignments") == 0 and await plane.count("tag_param_values") == 0


async def test_a_glossary_term_is_blocked_by_its_refs_and_takes_its_edges_with_it(plane):
    """Something that points a term at real data is a dependent; its edges, domain links and
    experts are its parts."""
    orders = await plane.table("orders")
    t1 = await plane.add("glossary_terms", name="Order", origin="admin")
    t2 = await plane.add("glossary_terms", name="Sale", origin="admin")
    await plane.add("glossary_term_edges", from_term_id=t1, to_term_id=t2, rel_type="RELATED_TO")
    await plane.add("glossary_term_edges", from_term_id=t2, to_term_id=t1, rel_type="RELATED_TO")
    await plane.add("glossary_term_domains", term_id=t1, domain_id="sales")
    await plane.add("glossary_term_refs", term_id=t1, table_id=orders.id, column_name="id")
    term = ObjectRef("glossary_term", t1)
    assert await plane.via(term) == {orders: ("glossary_term_refs.term_id",)}
    assert await plane.circle(term) == []

    async with plane.db.acquire() as conn:
        refs = metadata.tables["glossary_term_refs"]
        await conn.execute_core(refs.delete())
    assert await plane.blocking(term) == set()
    await plane.remove(term)
    assert await plane.count("glossary_term_edges") == 0
    assert await plane.count("glossary_term_domains") == 0
    assert await plane.count("glossary_terms") == 1


# --- no circle: something can always go first ----------------------------------------------------


async def test_a_table_related_to_itself_is_not_blocked_by_that(plane):
    employees = await plane.table("employees")
    await plane.relationship("manager", employees, employees)
    assert await plane.blocking(employees) == set()
    assert await plane.going(employees) == {
        "relationships.source_table_id": 1,
        "relationships.target_table_id": 1,
    }
    await plane.remove(employees)
    assert await plane.count("relationships") == 0


async def test_two_tables_each_related_to_the_other_are_not_a_circle(plane):
    """REQ-1918's scenario: each table is refused listing the two relationships; once both
    relationships are gone, each table can go."""
    a, b = await plane.table("a"), await plane.table("b")
    a_b = await plane.relationship("a_b", a, b)
    b_a = await plane.relationship("b_a", b, a)
    assert await plane.blocking(a) == {a_b, b_a} and await plane.blocking(b) == {a_b, b_a}
    assert await plane.circle(a) == []
    await plane.remove(a_b)
    await plane.remove(b_a)
    await plane.remove(a)
    await plane.remove(b)
    assert await plane.count("registered_tables") == 0


async def test_a_relationship_to_a_view_that_reads_its_source_table_is_not_a_circle(plane):
    a = await plane.table("a")
    view = await plane.table("a_summary", view_sql="SELECT id FROM a")
    rel = await plane.relationship("a_summary_rel", a, view)
    assert await plane.blocking(a) == {view, rel}
    assert await plane.blocking(view) == {rel}
    assert await plane.circle(a) == []
    for ref in (rel, view, a):
        await plane.remove(ref)


async def test_a_domain_its_role_and_the_roles_assignment_are_not_a_circle(plane):
    await plane.add("roles", id="seller", capabilities=[], domain_access=["sales"], origin="admin")
    assignment = await plane.add(
        "user_role_assignments", user_id="u1", role_id="seller", domain_id="sales"
    )
    assert await plane.circle(ObjectRef("domain", "sales")) == []
    for ref in (
        ObjectRef("role_assignment", assignment),
        ObjectRef("role", "seller"),
        ObjectRef("domain", "sales"),
    ):
        await plane.remove(ref)


# --- a damaged control plane: the circle is named ------------------------------------------------


async def test_views_that_read_each_other_are_named_as_a_circle(plane):
    """Such views are refused when saved; these rows are written directly, as a control plane
    saved before that refusal holds them."""
    v1 = await plane.table("v1", view_sql="SELECT * FROM v2")
    v2 = await plane.table("v2", view_sql="SELECT * FROM v3")
    v3 = await plane.table("v3", view_sql="SELECT * FROM v1")
    assert await plane.blocking(v1) == {v3}
    assert await plane.circle(v1) == [v2, v3]
    assert await plane.circle(v2) == [v1, v3]


async def test_a_view_that_reads_itself_is_not_blocked_by_that(plane):
    v = await plane.table("v", view_sql="SELECT * FROM v")
    assert await plane.blocking(v) == set() and await plane.circle(v) == []


async def test_a_role_parent_loop_is_named_as_a_circle(plane):
    await plane.add("roles", id="r1", capabilities=[], domain_access=["*"], origin="admin")
    await plane.add(
        "roles", id="r2", capabilities=[], domain_access=["*"], parent_role_id="r1", origin="admin"
    )
    async with plane.db.acquire() as conn:
        roles = metadata.tables["roles"]
        await conn.execute_core(
            roles.update().where(roles.c.id == "r1").values(parent_role_id="r2")
        )
    assert await plane.circle(ObjectRef("role", "r1")) == [ObjectRef("role", "r2")]


# --- a dependent is named as an operator knows it -------------------------------------------------


async def test_each_dependent_carries_the_name_an_operator_knows_it_by(plane):
    orders = await plane.table("orders")
    column = await plane.add(
        "table_columns", table_id=orders.id, column_name="amount", visible_to=["seller"]
    )
    await plane.add("roles", id="seller", capabilities=[], domain_access=["sales"], origin="admin")
    assignment = await plane.add(
        "user_role_assignments", user_id="alice", role_id="seller", domain_id="*"
    )
    rule = await plane.add(
        "rls_rules", role_id="seller", domain_id="sales", filter_expr=b"1=1", origin="admin"
    )
    term = await plane.add("glossary_terms", name="Order", origin="admin")
    await plane.add("glossary_term_domains", term_id=term, domain_id="sales")
    await plane.add(
        "data_products", id="dp", domain_id="sales", name="Sales product", origin="admin"
    )

    async with plane.db.acquire() as conn:
        of_role = {d.ref: d.name for d in await integrity.guard(conn, ObjectRef("role", "seller"))}
        of_domain = {
            d.ref: d.name for d in await integrity.guard(conn, ObjectRef("domain", "sales"))
        }
        reported = (await integrity.guard(conn, ObjectRef("role", "seller")))[0].as_dict()

    assert of_role == {
        ObjectRef("column", column): "orders.amount",
        ObjectRef("role_assignment", assignment): "alice holds seller",
    }
    assert of_domain[orders] == "orders"
    assert of_domain[ObjectRef("row_filter", rule)] == "seller on sales"
    assert of_domain[ObjectRef("glossary_term", term)] == "Order"
    assert of_domain[ObjectRef("data_product", "dp")] == "Sales product"
    assert of_domain[ObjectRef("role", "seller")] == "seller"
    assert reported == {
        "kind": "column",
        "id": column,
        "name": "orders.amount",
        "via": ["table_columns.visible_to"],
        # The table a column grant is changed through (revokeRoleFromTable).
        "table_id": orders.id,
    }
