# Copyright (c) 2025 Kenneth Stott
# Canary: d364f0a1-a241-499d-8165-a467ed3b77fb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for config loader — YAML → PG round-trip."""

from pathlib import Path

import pytest
import pytest_asyncio

from provisa.core.config_loader import load_config, parse_config
from provisa.core.db import init_schema
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
FIXTURE_CONFIG = Path(__file__).parent.parent / "fixtures" / "sample_config.yaml"


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def _init_schema(pg_pool):
    """Create config schema tables once per module."""
    async with pg_pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


@pytest_asyncio.fixture(autouse=True)
async def _clean_tables(pg_pool, _init_schema):
    """Truncate config tables before each test."""
    async with pg_pool.acquire() as conn:
        await conn.execute("""
            TRUNCATE rls_rules, relationships, table_columns,
                     registered_tables, naming_rules, roles, domains, sources
            CASCADE
        """)


class TestConfigLoader:
    def test_parse_config_validates(self):
        config = parse_config(FIXTURE_CONFIG)
        assert len(config.sources) == 1
        assert config.sources[0].id == "sales-pg"

    async def test_load_config_creates_sources(self, pg_pool):
        config = parse_config(FIXTURE_CONFIG)
        async with pg_pool.acquire() as conn:
            await load_config(config, conn)
            sources = await source_repo.list_all(conn)
        assert len(sources) == 1
        assert sources[0]["id"] == "sales-pg"
        assert sources[0]["dialect"] == "postgres"

    async def test_load_config_creates_domains(self, pg_pool):
        config = parse_config(FIXTURE_CONFIG)
        async with pg_pool.acquire() as conn:
            await load_config(config, conn)
            domains = await domain_repo.list_all(conn)
        assert len(domains) == 2
        ids = {d["id"] for d in domains}
        assert ids == {"sales-analytics", "product-catalog"}

    async def test_load_config_creates_tables_with_columns(self, pg_pool):
        config = parse_config(FIXTURE_CONFIG)
        async with pg_pool.acquire() as conn:
            await load_config(config, conn)
            tables = await table_repo.list_all(conn)
        assert len(tables) == 3
        orders = next(t for t in tables if t["table_name"] == "orders")
        assert orders["governance"] == "pre-approved"
        assert len(orders["columns"]) == 6

    async def test_load_config_creates_relationships(self, pg_pool):
        config = parse_config(FIXTURE_CONFIG)
        async with pg_pool.acquire() as conn:
            await load_config(config, conn)
            rels = await rel_repo.list_all(conn)
        assert len(rels) == 1
        assert rels[0]["id"] == "orders-to-customers"
        assert rels[0]["cardinality"] == "many-to-one"

    async def test_load_config_creates_roles(self, pg_pool):
        config = parse_config(FIXTURE_CONFIG)
        async with pg_pool.acquire() as conn:
            await load_config(config, conn)
            roles = await role_repo.list_all(conn)
        assert len(roles) == 2
        admin = next(r for r in roles if r["id"] == "admin")
        assert "admin" in admin["capabilities"]
        assert admin["domain_access"] == ["*"]

    async def test_load_config_creates_rls_rules(self, pg_pool):
        config = parse_config(FIXTURE_CONFIG)
        async with pg_pool.acquire() as conn:
            await load_config(config, conn)
            rules = await rls_repo.list_all(conn)
        assert len(rules) == 1
        assert rules[0]["role_id"] == "analyst"

    async def test_load_config_idempotent(self, pg_pool):
        """Loading the same config twice produces the same state."""
        config = parse_config(FIXTURE_CONFIG)
        async with pg_pool.acquire() as conn:
            await load_config(config, conn)
            await load_config(config, conn)
            sources = await source_repo.list_all(conn)
            tables = await table_repo.list_all(conn)
        assert len(sources) == 1
        assert len(tables) == 3

    async def test_naming_rules_persisted(self, pg_pool):
        config = parse_config(FIXTURE_CONFIG)
        async with pg_pool.acquire() as conn:
            await load_config(config, conn)
            rules = await conn.fetch("SELECT * FROM naming_rules")
        assert len(rules) == 1
        assert rules[0]["pattern"] == "^prod_pg_"
