# Copyright (c) 2026 Kenneth Stott
# Canary: c2958b5b-4924-4d67-9f1b-9517a0db38c5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every model kind a config can declare records where it came from (REQ-1919).

Beyond sources, domains, roles and tables: relationships, metrics, commands, webhooks, data
products, tags, tag assignments, row filters and glossary terms. Each writer takes the origin as
a required keyword, writes it when the object is CREATED, leaves it alone on a later write — an
admin edit of a config's object leaves it the config's — and a config write takes over an object
made through the admin. Runs on a SQLite control plane; the scenarios that load whole configs run
on PostgreSQL too.
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
        await conn.execute_core(insert(sources).values(id="pg", type="postgresql", origin="admin"))
        await conn.execute_core(insert(domains).values(id="sales", origin="admin"))
        await conn.execute_core(
            insert(roles).values(id="seller", capabilities=[], domain_access=["*"], origin="admin")
        )
        for table_id, name in ((1, "orders"), (2, "customers")):
            await conn.execute_core(
                insert(registered_tables).values(
                    id=table_id,
                    source_id="pg",
                    domain_id="sales",
                    schema_name="public",
                    table_name=name,
                    origin="admin",
                )
            )
    return db


Write = Callable[[Any, str], Awaitable[Any]]

#: For each kind: how to write it (with an origin), and where to read its origin back.
KINDS: dict[str, tuple[Write, Any, Any]] = {
    "relationship": (
        lambda conn, origin: relationship_repo.upsert(
            conn,
            Relationship(
                id="orders-to-customers",
                source_table_id="orders",
                target_table_id="customers",
                source_column="customer_id",
                target_column="id",
                cardinality=Cardinality.many_to_one,
            ),
            origin=origin,
        ),
        relationships,
        relationships.c.id == "orders-to-customers",
    ),
    "metric": (
        lambda conn, origin: metric_repo.upsert(
            conn, Metric(name="revenue", expression="SUM(orders.amount)"), origin=origin
        ),
        metrics,
        metrics.c.name == "revenue",
    ),
    "command": (
        lambda conn, origin: function_repo.upsert_function(
            conn,
            Function(
                name="refund", source_id="pg", function_name="refund", returns="", domain_id="sales"
            ),
            origin=origin,
        ),
        tracked_functions,
        tracked_functions.c.name == "refund",
    ),
    "webhook": (
        lambda conn, origin: function_repo.upsert_webhook(
            conn, Webhook(name="notify", url="http://x", domain_id="sales"), origin=origin
        ),
        tracked_webhooks,
        tracked_webhooks.c.name == "notify",
    ),
    "data product": (
        lambda conn, origin: data_product_repo.upsert(
            conn, DataProduct(id="sales-core", domain_id="sales", name="Sales"), origin=origin
        ),
        data_products,
        data_products.c.id == "sales-core",
    ),
    "tag": (
        lambda conn, origin: tag_repo.upsert(conn, Tag(id="finance"), origin=origin),
        tags,
        tags.c.id == "finance",
    ),
    "tag assignment": (
        lambda conn, origin: tag_repo.assign(
            conn,
            TagAssignment(tag_id="pii", object_type="column", table_id=1, column_name="email"),
            origin=origin,
        ),
        tag_assignments,
        tag_assignments.c.base_tag_id == "pii",
    ),
    "row filter": (
        lambda conn, origin: rls_repo.upsert(
            conn,
            RLSRule(table_id="orders", role_id="seller", filter="region = 'eu'"),
            origin=origin,
        ),
        rls_rules,
        rls_rules.c.role_id == "seller",
    ),
    "glossary term": (
        lambda conn, origin: glossary_repo.upsert_declared_term(
            conn, "Revenue", definition="money in", domains=set(), origin=origin
        ),
        glossary_terms,
        glossary_terms.c.name == "revenue",
    ),
}


async def _origin(plane: Database, table: Any, where: Any) -> str:
    async with plane.acquire() as conn:
        return (await conn.execute_core(select(table.c.origin).where(where))).scalar_one()


@pytest.mark.parametrize("kind", sorted(KINDS))
async def test_the_origin_is_written_at_create_and_kept_by_an_admin_edit(plane, kind):
    write, table, where = KINDS[kind]
    async with plane.acquire() as conn:
        await write(conn, "config")
    assert await _origin(plane, table, where) == "config"
    async with plane.acquire() as conn:
        await write(conn, "admin")  # an edit through the admin of the config's object
    assert await _origin(plane, table, where) == "config"


@pytest.mark.parametrize("kind", sorted(KINDS))
async def test_a_config_write_takes_over_what_the_admin_made_and_says_so(plane, kind, caplog):
    write, table, where = KINDS[kind]
    async with plane.acquire() as conn:
        await write(conn, "admin")
    assert await _origin(plane, table, where) == "admin"
    with caplog.at_level("INFO", logger="provisa.core.repositories.origin"):
        async with plane.acquire() as conn:
            await write(conn, "config")
    assert await _origin(plane, table, where) == "config"
    # Only this logger's records: caplog also collects any other logger's WARNING (e.g. a pooled
    # HTTP client's "Resetting dropped connection" from a thread an earlier test left running).
    origin_records = [r for r in caplog.records if r.name == "provisa.core.repositories.origin"]
    assert [r.getMessage().split(" '")[0] for r in origin_records] == [
        f"config load takes over {kind}"
    ]


@pytest.mark.parametrize("kind", sorted(KINDS))
async def test_an_origin_outside_the_three_is_refused(plane, kind):
    write, _table, _where = KINDS[kind]
    async with plane.acquire() as conn:
        with pytest.raises(ValueError, match="origin must be one of"):
            await write(conn, "imported")


async def test_a_term_derived_from_a_column_is_the_systems_own(plane):
    """Nobody declared it: it exists because a column refers to it."""
    async with plane.acquire() as conn:
        await glossary_repo._find_or_create_term(conn, "email address")
    assert await _origin(plane, glossary_terms, glossary_terms.c.name == "email address") == "seed"
