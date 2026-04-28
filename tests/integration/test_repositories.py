# Copyright (c) 2026 Kenneth Stott
# Canary: 07b85bcd-e288-461b-a008-6237883989ba
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for CRUD repositories against real PG."""

from pathlib import Path

import pytest
import pytest_asyncio

from provisa.core.db import init_schema
from provisa.core.models import (
    Cardinality,
    Column,
    Domain,
    GovernanceLevel,
    Relationship,
    RLSRule,
    Role,
    Source,
    Table,
)
from provisa.core.repositories import (
    domain as domain_repo,
    relationship as rel_repo,
    rls as rls_repo,
    role as role_repo,
    source as source_repo,
    table as table_repo,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def _init_schema(pg_pool):
    await init_schema(pg_pool, SCHEMA_SQL)


@pytest_asyncio.fixture(autouse=True)
async def _clean(pg_pool, _init_schema):
    async with pg_pool.acquire() as conn:
        await conn.execute("""
            TRUNCATE rls_rules, relationships, table_columns,
                     registered_tables, naming_rules, roles, domains, sources
            CASCADE
        """)


def _make_source(**kwargs) -> Source:
    defaults = dict(
        id="test-pg", type="postgresql", host="localhost",
        port=5432, database="testdb", username="u", password="p",
    )
    defaults.update(kwargs)
    return Source(**defaults)


def _make_domain(**kwargs) -> Domain:
    return Domain(**({"id": "test-domain", "description": "Test"} | kwargs))


def _make_role(**kwargs) -> Role:
    return Role(**({"id": "tester", "capabilities": ["query_development"], "domain_access": ["test-domain"]} | kwargs))


def _make_table(**kwargs) -> Table:
    defaults = {
        "source_id": "test-pg",
        "domain_id": "test-domain",
        "schema": "public",
        "table": "test_table",
        "governance": "pre-approved",
        "columns": [{"name": "id", "visible_to": ["tester"]}],
    }
    defaults.update(kwargs)
    return Table.model_validate(defaults)


async def _setup_source_domain(conn):
    await source_repo.upsert(conn, _make_source())
    await domain_repo.upsert(conn, _make_domain())


async def _setup_source_domain_tables(conn):
    await _setup_source_domain(conn)
    await table_repo.upsert(conn, _make_table(**{"table": "orders"}))
    await table_repo.upsert(conn, _make_table(**{"table": "customers"}))


class TestSourceRepo:
    async def test_upsert_and_get(self, pg_pool):
        async with pg_pool.acquire() as conn:
            src = _make_source()
            await source_repo.upsert(conn, src)
            result = await source_repo.get(conn, "test-pg")
            assert result is not None
            assert result["type"] == "postgresql"
            assert result["dialect"] == "postgres"

    async def test_upsert_updates_existing(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await source_repo.upsert(conn, _make_source(host="host1"))
            await source_repo.upsert(conn, _make_source(host="host2"))
            result = await source_repo.get(conn, "test-pg")
            assert result["host"] == "host2"

    async def test_list_all(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await source_repo.upsert(conn, _make_source(id="a"))
            await source_repo.upsert(conn, _make_source(id="b"))
            result = await source_repo.list_all(conn)
            assert len(result) == 2

    async def test_delete(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await source_repo.upsert(conn, _make_source())
            deleted = await source_repo.delete(conn, "test-pg")
            assert deleted is True
            assert await source_repo.get(conn, "test-pg") is None

    async def test_get_nonexistent(self, pg_pool):
        async with pg_pool.acquire() as conn:
            assert await source_repo.get(conn, "nope") is None


class TestDomainRepo:
    async def test_upsert_and_get(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await domain_repo.upsert(conn, _make_domain())
            result = await domain_repo.get(conn, "test-domain")
            assert result["description"] == "Test"

    async def test_delete(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await domain_repo.upsert(conn, _make_domain())
            assert await domain_repo.delete(conn, "test-domain") is True
            assert await domain_repo.get(conn, "test-domain") is None


class TestRoleRepo:
    async def test_upsert_and_get(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await role_repo.upsert(conn, _make_role())
            result = await role_repo.get(conn, "tester")
            assert result["capabilities"] == ["query_development"]


class TestTableRepo:
    async def test_upsert_and_get(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await _setup_source_domain(conn)
            tbl = _make_table()
            table_id = await table_repo.upsert(conn, tbl)
            result = await table_repo.get(conn, table_id)
            assert result["table_name"] == "test_table"
            assert result["governance"] == "pre-approved"
            assert len(result["columns"]) == 1
            assert result["columns"][0]["column_name"] == "id"

    async def test_upsert_replaces_columns(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await _setup_source_domain(conn)
            tbl1 = _make_table(columns=[{"name": "a", "visible_to": ["tester"]}])
            await table_repo.upsert(conn, tbl1)
            tbl2 = _make_table(columns=[
                {"name": "b", "visible_to": ["tester"]},
                {"name": "c", "visible_to": ["tester"]},
            ])
            table_id = await table_repo.upsert(conn, tbl2)
            result = await table_repo.get(conn, table_id)
            assert len(result["columns"]) == 2
            names = {c["column_name"] for c in result["columns"]}
            assert names == {"b", "c"}

    async def test_get_by_name(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await _setup_source_domain(conn)
            await table_repo.upsert(conn, _make_table())
            result = await table_repo.get_by_name(conn, "test-pg", "public", "test_table")
            assert result is not None
            assert result["table_name"] == "test_table"

    async def test_find_by_table_name(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await _setup_source_domain(conn)
            await table_repo.upsert(conn, _make_table())
            result = await table_repo.find_by_table_name(conn, "test_table")
            assert result is not None

    async def test_cascade_delete_on_source(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await _setup_source_domain(conn)
            await table_repo.upsert(conn, _make_table())
            await source_repo.delete(conn, "test-pg")
            tables = await table_repo.list_all(conn)
            assert len(tables) == 0


class TestRelationshipRepo:
    async def test_upsert_and_get(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await _setup_source_domain_tables(conn)
            rel = Relationship(
                id="o2c", source_table_id="orders",
                target_table_id="customers",
                source_column="customer_id", target_column="id",
                cardinality="many-to-one",
            )
            await rel_repo.upsert(conn, rel)
            result = await rel_repo.get(conn, "o2c")
            assert result is not None
            assert result["cardinality"] == "many-to-one"

    async def test_upsert_rejects_unregistered_table(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await _setup_source_domain_tables(conn)
            rel = Relationship(
                id="bad", source_table_id="nonexistent",
                target_table_id="customers",
                source_column="x", target_column="id",
                cardinality="many-to-one",
            )
            with pytest.raises(ValueError, match="not registered"):
                await rel_repo.upsert(conn, rel)


class TestRLSRepo:
    async def test_upsert_and_get(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await _setup_source_domain(conn)
            await role_repo.upsert(conn, _make_role())
            await table_repo.upsert(conn, _make_table(**{"table": "orders"}))
            rule = RLSRule(table_id="orders", role_id="tester", filter="region = 'us'")
            await rls_repo.upsert(conn, rule)
            tbl = await table_repo.find_by_table_name(conn, "orders")
            result = await rls_repo.get_for_table_role(conn, tbl["id"], "tester")
            assert result is not None
            assert result["filter_expr"] == "region = 'us'"

    async def test_upsert_rejects_unregistered_table(self, pg_pool):
        async with pg_pool.acquire() as conn:
            await _setup_source_domain(conn)
            await role_repo.upsert(conn, _make_role())
            rule = RLSRule(table_id="ghost", role_id="tester", filter="1=1")
            with pytest.raises(ValueError, match="not registered"):
                await rls_repo.upsert(conn, rule)
