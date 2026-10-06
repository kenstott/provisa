# Copyright (c) 2026 Kenneth Stott
# Canary: c2958b5b-4924-4d67-9f1b-9517a0db38c5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""No model kind records where it came from (REQ-1919, amended 2026-10-06).

A configuration seeds the model store once, and from then on the store alone owns the model:
objects carry no mark of having been seeded. Sources, domains, roles and tables, and every other
kind a config can declare — relationships, metrics, commands, webhooks, data products, tags, tag
assignments, row filters and glossary terms — are written by one writer that takes no origin, the
same for a seed, an explicit apply and an admin's edit. A second write is an update of the same
object, whoever makes it. Runs on a SQLite control plane.
"""

# Requirements: REQ-1919

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from sqlalchemy import insert, select

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import _init_schema_portable
from provisa.core.models import (
    Cardinality,
    DataProduct,
    Function,
    Metric,
    Relationship,
    RLSRule,
    Tag,
    TagAssignment,
    Webhook,
)
from provisa.core.repositories import data_product as data_product_repo
from provisa.core.repositories import function as function_repo
from provisa.core.repositories import glossary as glossary_repo
from provisa.core.repositories import metric as metric_repo
from provisa.core.repositories import relationship as relationship_repo
from provisa.core.repositories import rls as rls_repo
from provisa.core.repositories import tag as tag_repo
from provisa.core.schema_org import (
    data_products,
    domains,
    glossary_terms,
    metrics,
    registered_tables,
    relationships,
    rls_rules,
    roles,
    sources,
    tag_assignments,
    tags,
    tracked_functions,
    tracked_webhooks,
)


@pytest.fixture
async def plane() -> Database:
    db = Database(create_engine_from_url("sqlite+pysqlite:///:memory:"), name="origin-kinds")
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql"))
        await conn.execute_core(insert(domains).values(id="sales"))
        await conn.execute_core(
            insert(roles).values(id="seller", capabilities=[], domain_access=["*"])
        )
        for table_id, name in ((1, "orders"), (2, "customers")):
            await conn.execute_core(
                insert(registered_tables).values(
                    id=table_id,
                    source_id="pg",
                    domain_id="sales",
                    schema_name="public",
                    table_name=name,
                )
            )
    return db


Write = Callable[[Any], Awaitable[Any]]

#: For each kind: how to write it, and where its row is.
KINDS: dict[str, tuple[Write, Any, Any]] = {
    "relationship": (
        lambda conn: relationship_repo.upsert(
            conn,
            Relationship(
                id="orders-to-customers",
                source_table_id="orders",
                target_table_id="customers",
                source_column="customer_id",
                target_column="id",
                cardinality=Cardinality.many_to_one,
            ),
        ),
        relationships,
        relationships.c.id == "orders-to-customers",
    ),
    "metric": (
        lambda conn: metric_repo.upsert(
            conn, Metric(name="revenue", expression="SUM(orders.amount)")
        ),
        metrics,
        metrics.c.name == "revenue",
    ),
    "command": (
        lambda conn: function_repo.upsert_function(
            conn,
            Function(
                name="refund", source_id="pg", function_name="refund", returns="", domain_id="sales"
            ),
        ),
        tracked_functions,
        tracked_functions.c.name == "refund",
    ),
    "webhook": (
        lambda conn: function_repo.upsert_webhook(
            conn, Webhook(name="notify", url="http://x", domain_id="sales")
        ),
        tracked_webhooks,
        tracked_webhooks.c.name == "notify",
    ),
    "data product": (
        lambda conn: data_product_repo.upsert(
            conn, DataProduct(id="sales-core", domain_id="sales", name="Sales")
        ),
        data_products,
        data_products.c.id == "sales-core",
    ),
    "tag": (
        lambda conn: tag_repo.upsert(conn, Tag(id="finance")),
        tags,
        tags.c.id == "finance",
    ),
    "tag assignment": (
        lambda conn: tag_repo.assign(
            conn,
            TagAssignment(tag_id="pii", object_type="column", table_id=1, column_name="email"),
        ),
        tag_assignments,
        tag_assignments.c.base_tag_id == "pii",
    ),
    "row filter": (
        lambda conn: rls_repo.upsert(
            conn,
            RLSRule(table_id="orders", role_id="seller", filter="region = 'eu'"),
        ),
        rls_rules,
        rls_rules.c.role_id == "seller",
    ),
    "glossary term": (
        lambda conn: glossary_repo.upsert_declared_term(
            conn, "Revenue", definition="money in", domains=set()
        ),
        glossary_terms,
        glossary_terms.c.name == "revenue",
    ),
}


async def _rows(plane: Database, table: Any, where: Any) -> list[dict]:
    async with plane.acquire() as conn:
        return [dict(r._mapping) for r in (await conn.execute_core(select(table).where(where)))]


MODEL_TABLES = (
    sources,
    domains,
    roles,
    registered_tables,
    relationships,
    metrics,
    tracked_functions,
    tracked_webhooks,
    data_products,
    tags,
    tag_assignments,
    rls_rules,
    glossary_terms,
)


@pytest.mark.parametrize("table", MODEL_TABLES, ids=lambda t: t.name)
def test_no_model_table_has_an_origin(table):
    assert "origin" not in table.c


@pytest.mark.parametrize("kind", sorted(KINDS))
async def test_a_second_write_updates_the_one_object_and_records_no_origin(plane, kind):
    write, table, where = KINDS[kind]
    async with plane.acquire() as conn:
        await write(conn)  # the seed, or an apply
    async with plane.acquire() as conn:
        await write(conn)  # an admin's edit, or a later apply
    rows = await _rows(plane, table, where)
    assert len(rows) == 1
    assert "origin" not in rows[0]


def test_no_writer_takes_an_origin():
    import inspect

    from provisa.core.repositories import (
        data_product,
        domain,
        function,
        glossary,
        metric,
        relationship,
        rls,
        role,
        source,
        table,
        tag,
    )

    writers = (
        source.upsert,
        domain.upsert,
        role.upsert,
        table.upsert,
        relationship.upsert,
        metric.upsert,
        function.upsert_function,
        function.upsert_webhook,
        data_product.upsert,
        tag.upsert,
        tag.assign,
        rls.upsert,
        glossary.upsert_declared_term,
    )
    for writer in writers:
        assert "origin" not in inspect.signature(writer).parameters, writer.__qualname__


async def test_a_term_derived_from_a_column_is_created_once(plane):
    """Nobody declared it: it exists because a column refers to it, and records no origin."""
    async with plane.acquire() as conn:
        first = await glossary_repo._find_or_create_term(conn, "email address")
        again = await glossary_repo._find_or_create_term(conn, "email address")
    assert first == again
    (row,) = await _rows(plane, glossary_terms, glossary_terms.c.name == "email address")
    assert "origin" not in row
