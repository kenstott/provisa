# Copyright (c) 2026 Kenneth Stott
# Canary: 29a24b37-ac01-441c-bf4d-7db19df7ffe7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The one inventory of what refers to what, and the three answers read from it (REQ-1918).

Two halves. The inventory is checked against the schema: every declared foreign key is listed,
every listed column exists, and an open choice has no default. Then ``dependents``, ``parts`` and
``cycle_of`` are run against rows on a seeded control plane, including every cycle the model
permits today. An open choice is exercised under BOTH of its alternatives.
"""

# Requirements: REQ-1917, REQ-1918

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.core import dependents as dep
from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.dependents import ObjectRef, PendingChoice, Standing
from provisa.core.schema_org import metadata

PART, DEPENDENT = Standing.PART, Standing.DEPENDENT
ALL_PARTS = {choice: PART for choice in dep.PENDING_CHOICES}
ALL_DEPENDENTS = {choice: DEPENDENT for choice in dep.PENDING_CHOICES}


# --- the inventory against the schema ------------------------------------------------------------


def test_every_declared_foreign_key_is_in_the_inventory():
    listed = {(r.table, r.column, dep.KINDS[r.to].table) for r in dep.REFERENCES}
    missing = [
        f"{table.name}.{column.name} -> {fk.column.table.name}"
        for table in metadata.sorted_tables
        for column in table.columns
        for fk in column.foreign_keys
        if (table.name, column.name, fk.column.table.name) not in listed
    ]
    assert missing == [], "a foreign key the inventory does not declare a part or a dependent"


def test_every_reference_names_columns_that_exist():
    for r in dep.REFERENCES:
        table = metadata.tables[r.table]
        for name in (r.column, *r.ends, *([r.owner] if r.owner else [])):
            assert name in table.c, f"{r.table}.{name}"
        kind = dep.KINDS[r.to]
        assert r.by in ("key", "view_mv_id") or (r.by == "name" and kind.name), (r.table, r.column)
    for kind in dep.KINDS.values():
        table = metadata.tables[kind.table]
        assert kind.key in table.c and (kind.name is None or kind.name in table.c), kind
    for kind, (owner_kind, owner_column, choice) in dep.OWNED.items():
        assert owner_column in metadata.tables[dep.KINDS[kind].table].c
        assert owner_kind in dep.KINDS and (choice is None or choice in dep.PENDING_CHOICES)


def test_a_blocking_reference_names_the_object_that_blocks():
    for r in dep.REFERENCES:
        if r.standing is PART:
            assert r.choice is None, (r.table, r.column)
            continue
        assert r.of in dep.KINDS and r.owner is not None, (r.table, r.column)


def test_an_open_choice_is_named_and_a_decided_reference_carries_none():
    used = {r.choice for r in dep.REFERENCES if r.standing is Standing.UNDECIDED}
    used |= {choice for _, _, choice in dep.OWNED.values() if choice is not None}
    assert used == set(dep.PENDING_CHOICES), (
        "every open choice is listed, and every listed one is open"
    )
    for r in dep.REFERENCES:
        assert (r.choice is not None) == (r.standing is Standing.UNDECIDED), (r.table, r.column)


def test_no_reference_is_declared_twice():
    seen = [(r.table, r.column, r.to) for r in dep.REFERENCES]
    assert len(seen) == len(set(seen))


def test_sql_text_that_does_not_parse_is_an_error_not_nothing():
    with pytest.raises(ValueError, match="could not be parsed"):
        dep.names_in_sql("SELECT FROM WHERE (")


def test_the_names_a_sql_text_reads():
    relations, metric_names = dep.names_in_sql(
        "WITH recent AS (SELECT * FROM orders o) "
        "SELECT r.id, m.total FROM recent r JOIN metrics.revenue m ON m.id = r.id"
    )
    assert (relations, metric_names) == ({"orders"}, {"revenue"})
    assert dep.names_in_sql("SUM(orders.amount) - SUM(refunds.amount)")[0] == {"orders", "refunds"}


# --- rows ----------------------------------------------------------------------------------------


class Plane:
    """A seeded SQLite control plane and short ways to put rows on it."""

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
            **values,
        )
        return ObjectRef("table", table_id)

    async def relationship(
        self, rel_id: str, source: ObjectRef, target: ObjectRef, **kw
    ) -> ObjectRef:
        await self.add(
            "relationships",
            id=rel_id,
            source_table_id=source.id,
            target_table_id=target.id,
            source_column="id",
            target_column="id",
            cardinality="many-to-one",
            **kw,
        )
        return ObjectRef("relationship", rel_id)

    async def blocking(self, ref: ObjectRef, rulings=dep.NO_RULINGS) -> set[ObjectRef]:
        async with self.db.acquire() as conn:
            return {d.ref for d in await dep.dependents(conn, ref, rulings)}

    async def going(self, ref: ObjectRef, rulings=dep.NO_RULINGS) -> dict[str, int]:
        async with self.db.acquire() as conn:
            return {f"{p.table}.{p.column}": p.count for p in await dep.parts(conn, ref, rulings)}

    async def cycle(self, ref: ObjectRef, rulings=dep.NO_RULINGS) -> list[ObjectRef]:
        async with self.db.acquire() as conn:
            return await dep.cycle_of(conn, ref, rulings)


@pytest.fixture
async def plane() -> Plane:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="dependents-test")
    await _init_schema_portable(db)
    p = Plane(db)
    await p.add("sources", id="pg", type="postgresql")
    for domain_id in ("sales", "finance"):
        await p.add("domains", id=domain_id)
    return p


async def test_asking_about_an_object_that_is_not_there_is_a_lookup_error(plane):
    with pytest.raises(LookupError, match="no table 999"):
        await plane.blocking(ObjectRef("table", 999))


# --- a domain removes only itself: everything in it blocks (REQ-1917) ----------------------------


async def test_everything_that_refers_to_a_domain_blocks_it(plane):
    orders = await plane.table("orders")
    await plane.add("table_columns", table_id=orders.id, column_name="id", domain_id="sales")
    await plane.add("data_products", id="dp", domain_id="sales", name="Sales")
    await plane.add("roles", id="seller", capabilities=[], domain_access=["sales", "finance"])
    await plane.add("roles", id="everywhere", capabilities=[], domain_access=["*"])
    async with plane.db.acquire() as conn:
        src = metadata.tables["sources"]
        await conn.execute_core(
            src.update().where(src.c.id == "pg").values(allowed_domains=["sales"])
        )
    rule = await plane.add("rls_rules", role_id="seller", domain_id="sales", filter_expr=b"1=1")
    await plane.add("tracked_functions", name="refund", domain_id="sales")
    await plane.add("tracked_webhooks", name="notify", domain_id="sales")
    assignment = await plane.add(
        "user_role_assignments", user_id="u1", role_id="seller", domain_id="sales"
    )
    term = await plane.add("glossary_terms", name="Order")
    await plane.add("glossary_term_domains", term_id=term, domain_id="sales")

    blocking = await plane.blocking(ObjectRef("domain", "sales"), ALL_DEPENDENTS)
    assert blocking == {
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
    assert await plane.going(ObjectRef("domain", "sales"), ALL_DEPENDENTS) == {}
    # With assignments ruled parts of their role, the assignment is reported as the role.
    assert ObjectRef("role_assignment", assignment) not in await plane.blocking(
        ObjectRef("domain", "sales"), ALL_PARTS
    )
    # The other domain has one referrer: the role that lists both.
    assert await plane.blocking(ObjectRef("domain", "finance"), ALL_DEPENDENTS) == {
        ObjectRef("role", "seller")
    }


async def test_a_domain_nothing_refers_to_has_no_dependents(plane):
    assert await plane.blocking(ObjectRef("domain", "finance")) == set()
    assert await plane.cycle(ObjectRef("domain", "finance")) == []


# --- a table: its parts, and what reads it -------------------------------------------------------


async def test_a_tables_parts_go_with_it_and_its_readers_block_it(plane):
    orders = await plane.table("orders")
    await plane.add("table_columns", table_id=orders.id, column_name="id", domain_id="sales")
    await plane.add("table_columns", table_id=orders.id, column_name="amount", domain_id="sales")
    await plane.add(
        "tag_assignments",
        tag_id="pii",
        base_tag_id="pii",
        object_type="table",
        object_key="orders",
        table_id=orders.id,
    )
    view = await plane.table("open_orders", view_sql="SELECT o.id FROM orders o WHERE o.open")
    unrelated = await plane.table("customers", view_sql=None)
    await plane.add("metrics", name="revenue", expression="SUM(orders.amount)")
    await plane.add(
        "materialized_views",
        id="mv-daily",
        source_tables=["orders"],
        target_catalog="c",
        target_schema="s",
        target_table="daily",
    )

    assert await plane.blocking(orders) == {
        view,
        ObjectRef("metric", "revenue"),
        ObjectRef("materialized_view", "mv-daily"),
    }
    assert await plane.going(orders) == {
        "table_columns.table_id": 2,
        "tag_assignments.table_id": 1,
    }
    assert await plane.blocking(unrelated) == set()


async def test_a_views_own_storage_row_is_its_part(plane):
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
    assert await plane.going(ObjectRef("materialized_view", "view-open_orders")) == {
        "mv_refresh_log.mv_id": 1
    }


# --- an open choice has no default ---------------------------------------------------------------


async def test_a_row_matching_an_open_choice_raises_until_it_is_ruled(plane):
    orders, customers = await plane.table("orders"), await plane.table("customers")
    rel = await plane.relationship("o_c", orders, customers)

    with pytest.raises(PendingChoice) as err:
        await plane.blocking(orders)
    assert err.value.choice == "table.outgoing_relationships"
    assert "not ruled" in str(err.value)
    # The other end is reported as the relationship or as its owner, so it is open too.
    with pytest.raises(PendingChoice):
        await plane.blocking(customers)

    one = {"table.outgoing_relationships": DEPENDENT}
    assert await plane.blocking(orders, one) == {rel}
    assert await plane.blocking(customers, one) == {rel}
    other = {"table.outgoing_relationships": PART}
    assert await plane.blocking(orders, other) == set()
    assert await plane.going(orders, other) == {"relationships.source_table_id": 1}
    assert await plane.blocking(customers, other) == {orders}


async def test_an_open_choice_no_row_matches_does_not_stop_the_answer(plane):
    orders = await plane.table("orders")
    assert await plane.blocking(orders) == set()
    assert await plane.blocking(ObjectRef("source", "pg"), {"source.tables": DEPENDENT}) == {orders}
    with pytest.raises(PendingChoice) as err:
        await plane.blocking(ObjectRef("source", "pg"))
    assert err.value.choice == "source.tables"


async def test_a_role_and_what_names_it_under_each_ruling(plane):
    orders = await plane.table("orders")
    column = await plane.add(
        "table_columns", table_id=orders.id, column_name="id", visible_to=["seller", "analyst"]
    )
    await plane.add("roles", id="seller", capabilities=[], domain_access=["sales"])
    await plane.add(
        "roles", id="junior", capabilities=[], domain_access=[], parent_role_id="seller"
    )
    assignment = await plane.add(
        "user_role_assignments", user_id="u1", role_id="seller", domain_id="*"
    )
    await plane.add("rls_rules", role_id="seller", table_id=orders.id, filter_expr=b"1=1")
    seller = ObjectRef("role", "seller")

    assert await plane.blocking(seller, ALL_DEPENDENTS) == {
        ObjectRef("role", "junior"),
        ObjectRef("role_assignment", assignment),
        ObjectRef("column", column),
    }
    assert await plane.going(seller, ALL_DEPENDENTS) == {"rls_rules.role_id": 1}
    assert await plane.blocking(seller, ALL_PARTS) == {ObjectRef("role", "junior")}
    assert await plane.going(seller, ALL_PARTS) == {
        "rls_rules.role_id": 1,
        "table_columns.visible_to": 1,
        "user_role_assignments.role_id": 1,
    }


# --- cycles: every one the model permits today ---------------------------------------------------

OUT = "table.outgoing_relationships"


@pytest.mark.parametrize("ruling", [PART, DEPENDENT])
async def test_a_table_related_to_itself_is_never_blocked_by_that(plane, ruling):
    employees = await plane.table("employees")
    await plane.relationship("manager", employees, employees)
    assert await plane.blocking(employees, {OUT: ruling}) == set()
    # The one row is the table's part; it is counted under each column it refers through.
    assert await plane.going(employees, {OUT: ruling}) == {
        "relationships.source_table_id": 1,
        "relationships.target_table_id": 1,
    }
    assert await plane.cycle(employees, {OUT: ruling}) == []


async def test_two_tables_each_related_to_the_other_are_a_cycle_when_a_relationship_is_its_tables_part(
    plane,
):
    a, b = await plane.table("a"), await plane.table("b")
    await plane.relationship("a_b", a, b)
    await plane.relationship("b_a", b, a)
    rulings = {OUT: PART}
    assert await plane.blocking(a, rulings) == {b}
    assert await plane.blocking(b, rulings) == {a}
    assert await plane.cycle(a, rulings) == [b]
    assert await plane.cycle(b, rulings) == [a]
    # Each table's own relationship goes with it: four objects in all.
    assert await plane.going(a, rulings) == {"relationships.source_table_id": 1}
    assert await plane.going(b, rulings) == {"relationships.source_table_id": 1}


async def test_the_same_two_tables_are_no_cycle_when_a_relationship_stands_alone(plane):
    """A relationship that is its own object refers to both tables and nothing refers to it, so
    it can always be deleted first: each table is blocked, and neither is in a cycle."""
    a, b = await plane.table("a"), await plane.table("b")
    a_b = await plane.relationship("a_b", a, b)
    b_a = await plane.relationship("b_a", b, a)
    rulings = {OUT: DEPENDENT}
    assert await plane.blocking(a, rulings) == {a_b, b_a}
    assert await plane.blocking(a_b, rulings) == set()
    assert await plane.cycle(a, rulings) == []


async def test_a_dependent_outside_the_cycle_is_reported_and_is_not_a_member(plane):
    a, b = await plane.table("a"), await plane.table("b")
    await plane.relationship("a_b", a, b)
    await plane.relationship("b_a", b, a)
    outside = await plane.table("reads_a", view_sql="SELECT * FROM a")
    rulings = {OUT: PART}
    assert await plane.blocking(a, rulings) == {b, outside}
    assert await plane.cycle(a, rulings) == [b]
    assert await plane.blocking(outside, rulings) == set()


async def test_a_relationship_to_a_view_that_reads_its_source_table(plane):
    a = await plane.table("a")
    view = await plane.table("a_summary", view_sql="SELECT id FROM a")
    rel = await plane.relationship("a_summary_rel", a, view)

    assert await plane.blocking(a, {OUT: PART}) == {view}
    assert await plane.blocking(view, {OUT: PART}) == {a}
    assert await plane.cycle(a, {OUT: PART}) == [view]

    assert await plane.blocking(a, {OUT: DEPENDENT}) == {view, rel}
    assert await plane.blocking(view, {OUT: DEPENDENT}) == {rel}
    assert await plane.cycle(a, {OUT: DEPENDENT}) == []


async def test_views_that_read_each_other_are_a_cycle(plane):
    v1 = await plane.table("v1", view_sql="SELECT * FROM v2")
    v2 = await plane.table("v2", view_sql="SELECT * FROM v3")
    v3 = await plane.table("v3", view_sql="SELECT * FROM v1")
    assert await plane.blocking(v1) == {v3}
    assert await plane.cycle(v1) == [v2, v3]
    assert await plane.cycle(v2) == [v1, v3]


async def test_a_view_that_reads_itself_is_not_blocked_by_that(plane):
    v = await plane.table("v", view_sql="SELECT * FROM v")
    assert await plane.blocking(v) == set() and await plane.cycle(v) == []


async def test_a_role_parent_loop_is_a_cycle(plane):
    """Saving refuses a parent loop; the rows are written directly, as a damaged plane holds them."""
    await plane.add("roles", id="r1", capabilities=[], domain_access=["*"], parent_role_id="r2")
    await plane.add("roles", id="r2", capabilities=[], domain_access=["*"], parent_role_id="r1")
    assert await plane.cycle(ObjectRef("role", "r1")) == [ObjectRef("role", "r2")]


@pytest.mark.parametrize(("ruling", "members"), [(DEPENDENT, 1), (PART, 0)])
async def test_two_glossary_terms_each_linked_to_the_other(plane, ruling, members):
    t1 = await plane.add("glossary_terms", name="Order")
    t2 = await plane.add("glossary_terms", name="Sale")
    await plane.add("glossary_term_edges", from_term_id=t1, to_term_id=t2, rel_type="RELATED_TO")
    await plane.add("glossary_term_edges", from_term_id=t2, to_term_id=t1, rel_type="RELATED_TO")
    found = await plane.cycle(ObjectRef("glossary_term", t1), {"glossary_term.edges": ruling})
    assert found == [ObjectRef("glossary_term", t2)][:members]


async def test_a_domain_its_role_and_the_roles_assignment_are_not_a_cycle(plane):
    await plane.add("roles", id="seller", capabilities=[], domain_access=["sales"])
    await plane.add("user_role_assignments", user_id="u1", role_id="seller", domain_id="sales")
    for rulings in (ALL_PARTS, ALL_DEPENDENTS):
        assert await plane.cycle(ObjectRef("domain", "sales"), rulings) == []
        assert await plane.cycle(ObjectRef("role", "seller"), rulings) == []


@pytest.mark.parametrize("rulings", [ALL_PARTS, ALL_DEPENDENTS], ids=["parts", "dependents"])
async def test_every_object_can_be_removed_one_object_or_one_cycle_at_a_time(plane, rulings):
    """The invariant: there is always something that can go. Repeatedly take an object that
    nothing outside its own cycle blocks, and remove it with its cycle; nothing is left."""
    a, b = await plane.table("a"), await plane.table("b")
    await plane.relationship("a_b", a, b)
    await plane.relationship("b_a", b, a)
    await plane.relationship("a_a", a, a)
    v1 = await plane.table("v1", view_sql="SELECT * FROM v2 JOIN a ON true")
    v2 = await plane.table("v2", view_sql="SELECT * FROM v1")
    relationships = {ObjectRef("relationship", r) for r in ("a_b", "b_a")}
    remaining = {a, b, v1, v2} | (relationships if rulings is ALL_DEPENDENTS else set())
    removed_in_steps: list[set[ObjectRef]] = []
    while remaining:
        for candidate in sorted(remaining, key=lambda o: (o.kind, str(o.id))):
            members = {candidate, *await plane.cycle(candidate, rulings)}
            blocking = set()
            for member in members:
                blocking |= await plane.blocking(member, rulings)
            if not (blocking & remaining) - members:
                remaining -= members
                removed_in_steps.append(members)
                await _remove(plane, members)
                break
        else:
            pytest.fail(f"nothing can be removed; left: {remaining}")
    assert all(len(step) <= 2 for step in removed_in_steps)


async def _remove(plane: Plane, members: set[ObjectRef]) -> None:
    """Take the members' rows off the plane (and a table's own relationships with it)."""
    async with plane.db.acquire() as conn:
        rel, tbl = metadata.tables["relationships"], metadata.tables["registered_tables"]
        for member in members:
            if member.kind == "relationship":
                await conn.execute_core(rel.delete().where(rel.c.id == member.id))
            else:
                await conn.execute_core(rel.delete().where(rel.c.source_table_id == member.id))
                await conn.execute_core(tbl.delete().where(tbl.c.id == member.id))
        left = (await conn.execute_core(select(rel.c.id, rel.c.target_table_id))).fetchall()
        ids = {r[0] for r in (await conn.execute_core(select(tbl.c.id))).fetchall()}
        assert all(r[1] in ids for r in left), "a relationship is left pointing at a removed table"
