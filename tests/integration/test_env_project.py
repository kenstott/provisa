# Copyright (c) 2026 Kenneth Stott
# Canary: e07d5b19-4a3f-4c86-9d21-b8f6a04c7e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One environment's schema projected into the file tree of REQ-1526, against a real schema.

The projection is only meaningful against PostgreSQL. What it has to get right is precisely what a
metadata double cannot show: that the serial keys the schema hands out do not reach a file, that a
reference to one becomes the target's PATH, and that the SAME model in two schemas -- which will
hold different serials, because each schema has its own sequences -- projects to byte-identical
files. So both environments here are real schemas built from the authoritative ``schema.sql``.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from provisa.core.env_files import dump
from provisa.core.env_project import project
from provisa.core.environments import org_schema
from provisa.core.schema_org import (
    domains,
    registered_tables,
    relationships,
    sources,
    table_columns,
    tag_assignments,
    tracked_functions,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ENV = "dev"


def _scoped(table, schema):
    from sqlalchemy import MetaData

    return table.to_metadata(MetaData(), schema=schema)


@pytest.fixture
async def org(docker_postgres):
    """An org with two schemas — ``prod`` and ``dev`` — and a helper that inserts into either."""
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import init_schema

    org_id = f"envproj{uuid.uuid4().hex[:8]}"
    url = (
        f"postgresql+asyncpg://provisa:{os.environ.get('PG_PASSWORD', 'provisa')}@"
        f"{docker_postgres['host']}:{docker_postgres['port']}/provisa"
    )
    engine = create_engine_from_url(url, pool_size=2)
    db = Database(engine, name="org", search_path=org_schema(org_id))
    schema_sql = (Path(__file__).parents[2] / "provisa" / "core" / "schema.sql").read_text()
    await init_schema(db, schema_sql, org_id=org_id)
    await init_schema(db, schema_sql, org_id=org_id, env=ENV)

    async def insert(table, env=None, **values):
        scoped = _scoped(table, org_schema(org_id, env))
        async with db.acquire() as conn:
            result = await conn.execute_core(
                scoped.insert().values(**values).returning(*scoped.primary_key.columns)
            )
            row = result.fetchone()
        return row[0] if row is not None else None

    async def tree(env=None):
        async with db.acquire() as conn:
            return await project(conn, org_schema(org_id, env))

    yield type(
        "Org",
        (),
        {
            "id": org_id,
            "db": db,
            "insert": staticmethod(insert),
            "tree": staticmethod(tree),
        },
    )
    await engine.dispose()


async def _seed(org, env=None, *, burn_serials: int = 0):
    """The same small model in whichever schema — optionally after burning some serials.

    ``burn_serials`` exists to make the two schemas disagree about every integer key: a projection
    that leaked one would then differ between them, which is the failure the requirement is about.
    """
    await org.insert(sources, env, id="warehouse", type="postgres", host="db.internal")
    await org.insert(domains, env, id="sales", description="revenue")
    for i in range(burn_serials):
        await org.insert(
            registered_tables,
            env,
            source_id="warehouse",
            domain_id="sales",
            schema_name="scratch",
            table_name=f"burn{i}",
        )
    customer = await org.insert(
        registered_tables,
        env,
        source_id="warehouse",
        domain_id="sales",
        schema_name="public",
        table_name="customers",
        alias="Customer",
    )
    order = await org.insert(
        registered_tables,
        env,
        source_id="warehouse",
        domain_id="sales",
        schema_name="public",
        table_name="orders",
        alias="Order",
    )
    await org.insert(table_columns, env, table_id=order, column_name="total")
    await org.insert(table_columns, env, table_id=order, column_name="customer_id")
    await org.insert(
        relationships,
        env,
        id="orders_customer",
        source_table_id=order,
        target_table_id=customer,
        source_column="customer_id",
        target_column="id",
        cardinality="many-to-one",
        alias="customer",
    )
    return {"customer": customer, "order": order}


class TestThePathIsTheAddress:
    async def test_a_table_is_addressed_by_its_domain_and_business_name(self, org):
        await _seed(org)
        tree = await org.tree()
        assert "sales/tables/Order.yaml" in tree
        assert "sales/tables/Customer.yaml" in tree

    async def test_a_domain_and_a_source_get_their_own_files(self, org):
        await _seed(org)
        tree = await org.tree()
        assert "sales/domain.yaml" in tree
        assert "sources/warehouse.yaml" in tree

    async def test_the_physical_coordinates_stay_off_the_path(self, org):
        await _seed(org)
        assert "sales/tables/orders.yaml" not in await org.tree()


class TestNoSurrogateReachesAFile:
    async def test_a_table_file_holds_no_serial(self, org):
        await _seed(org)
        body = (await org.tree())["sales/tables/Order.yaml"]
        assert "id" not in body
        assert all("table_id" not in column for column in body["columns"])

    async def test_a_relationship_names_its_target_by_path(self, org):
        await _seed(org)
        edge = (await org.tree())["sales/tables/Order.yaml"]["relationships"][0]
        assert edge["target"] == "sales/tables/Customer.yaml"
        assert edge["id"] == "orders_customer"
        assert "target_table_id" not in edge
        assert "source_table_id" not in edge

    async def test_the_tenancy_a_row_is_stored_under_is_not_part_of_the_model(self, org):
        await _seed(org)
        assert "tenant_id" not in (await org.tree())["sales/tables/Order.yaml"]


class TestABindingNeverReachesTheTree:
    async def test_where_a_source_points_is_absent(self, org):
        await _seed(org)
        body = (await org.tree())["sources/warehouse.yaml"]
        assert body["type"] == "postgres"
        assert "host" not in body
        assert "port" not in body
        assert "username" not in body


class TestDeterminism:
    async def test_the_same_model_in_two_schemas_projects_identically(self, org):
        await _seed(org)
        # dev's serials are deliberately out of step with prod's.
        await _seed(org, ENV, burn_serials=3)
        prod = await org.tree()
        dev = await org.tree(ENV)
        # The burned tables exist only in dev; the shared model must render byte-identically.
        shared = {p for p in prod if "burn" not in p}
        assert dump({p: prod[p] for p in shared}) == dump({p: dev[p] for p in shared})

    async def test_projecting_twice_gives_the_same_bytes(self, org):
        await _seed(org)
        assert dump(await org.tree()) == dump(await org.tree())

    async def test_columns_are_ordered_by_name_and_not_by_insertion(self, org):
        await _seed(org)
        columns = (await org.tree())["sales/tables/Order.yaml"]["columns"]
        assert [c["column_name"] for c in columns] == ["customer_id", "total"]


class TestATagFindsTheFileItsObjectIsIn:
    """A tag is stored against an object; the projection has to put it in that object's file.

    A command tag names neither a table nor a source, so neither of those nestings claims it. Left
    that way it belongs to no file and disappears from the tree — and a deploy of that tree would
    then delete it from the target.
    """

    async def _command_tag(self, org, env=None):
        await _seed(org, env)
        await org.insert(tracked_functions, env, name="refund_order", source_id="warehouse")
        await org.insert(
            tag_assignments,
            env,
            tag_id="deprecated",
            base_tag_id="deprecated",
            object_type="command",
            command_name="refund_order",
            object_key="command:refund_order",
            reason="superseded by refund_line",
        )

    async def test_a_command_tag_reaches_the_command_file(self, org):
        await self._command_tag(org)
        body = (await org.tree())["commands/refund_order.yaml"]
        assert [t["tag_id"] for t in body["tags"]] == ["deprecated"]
        assert body["tags"][0]["reason"] == "superseded by refund_line"

    async def test_a_command_tag_carries_no_routing_key_into_the_file(self, org):
        await self._command_tag(org)
        (tag,) = (await org.tree())["commands/refund_order.yaml"]["tags"]
        assert not {"owner_command_name", "owner_table_id", "owner_source_id", "at", "on"} & set(
            tag
        )

    async def test_a_command_tag_lands_in_no_other_file(self, org):
        await self._command_tag(org)
        tree = await org.tree()
        elsewhere = [
            p for p, body in tree.items() if body.get("tags") and p != "commands/refund_order.yaml"
        ]
        assert elsewhere == []
