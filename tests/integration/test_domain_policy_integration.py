# Copyright (c) 2026 Kenneth Stott
# Canary: f02e6b86-b8a8-44a6-9752-02b03e7b5aa4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for the tri-state `naming.use_domains` feature — YAML/dict → PG.

Validates the three modes against a real PG metadata DB:
  * legacy (use_domains absent) — no impact on stored domain_id.
  * single-domain (use_domains=false) — everything stored under default_domain.
  * namespaced (use_domains=true) — domain_id stored as declared.
Plus the reload validation sweep that rejects pre-existing foreign domains.
"""

import os
from pathlib import Path

import pytest
import pytest_asyncio

from provisa.core import domain_policy
from provisa.core.config_loader import (
    _validate_existing_domains,
    load_config,
    load_config_from_yaml,
    parse_config_dict,
)
from provisa.core.models import Table, Column
from provisa.core.repositories import (
    domain as domain_repo,
    table as table_repo,
)

pytestmark = [pytest.mark.integration]

SCHEMA_SQL = (Path(__file__).parent.parent.parent / "provisa" / "core" / "schema.sql").read_text()
MAIN_CONFIG = Path(
    os.environ.get(
        "PROVISA_CONFIG", str(Path(__file__).parent.parent.parent / "config" / "provisa.yaml")
    )
)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def _init_schema(tenant_db):
    async with tenant_db.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


@pytest_asyncio.fixture(scope="module", loop_scope="session", autouse=True)
async def _restore_config_after_module(tenant_db, _init_schema):
    yield
    domain_policy.reset()
    async with tenant_db.acquire() as conn:
        await load_config_from_yaml(MAIN_CONFIG, conn, replace=True)


@pytest_asyncio.fixture(autouse=True)
async def _clean_and_reset(tenant_db, _init_schema):
    """Truncate config tables and reset the global policy before each test."""
    domain_policy.reset()
    async with tenant_db.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE rls_rules, relationships, relationship_candidates, table_columns,
                     registered_tables, naming_rules, roles, domains, sources CASCADE
            """
        )
    yield
    domain_policy.reset()


def _config(naming: dict, domains: list, table_domain: str) -> dict:
    return {
        "sources": [
            {
                "id": "pg1",
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "d",
                "username": "u",
                "password": "p",
            }
        ],
        "domains": domains,
        "naming": naming,
        "tables": [
            {
                "source_id": "pg1",
                "domain_id": table_domain,
                "schema": "public",
                "table": "orders",
                "columns": [{"name": "id", "visible_to": ["admin"]}],
            }
        ],
        "roles": [{"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}],
    }


async def _stored_domain(conn, table_name: str = "orders") -> str:
    return await conn.fetchval(
        "SELECT domain_id FROM registered_tables WHERE table_name = $1", table_name
    )


class TestSingleDomainMode:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_empty_table_domain_coerced_to_default(self, tenant_db):
        cfg = parse_config_dict(_config({"use_domains": False, "default_domain": "global"}, [], ""))
        async with tenant_db.acquire() as conn:
            await load_config(cfg, conn)
            assert await _stored_domain(conn) == "global"

    @pytest.mark.asyncio(loop_scope="session")
    async def test_default_domain_seeded(self, tenant_db):
        cfg = parse_config_dict(
            _config({"use_domains": False, "default_domain": "global"}, [], "global")
        )
        async with tenant_db.acquire() as conn:
            await load_config(cfg, conn)
            domains = {d["id"] for d in await domain_repo.list_all(conn)}
        assert "global" in domains

    @pytest.mark.asyncio(loop_scope="session")
    async def test_repo_upsert_rejects_foreign_domain(self, tenant_db):
        cfg = parse_config_dict(
            _config({"use_domains": False, "default_domain": "global"}, [], "global")
        )
        async with tenant_db.acquire() as conn:
            await load_config(cfg, conn)
            # Policy is now single-domain "global"; a foreign domain is a hard error.
            bad = Table(
                source_id="pg1",
                domain_id="sales",
                schema_name="public",
                table_name="widgets",
                columns=[Column(name="id", visible_to=["admin"])],
            )
            with pytest.raises(ValueError, match="cannot register domain"):
                await table_repo.upsert(conn, bad)


class TestLegacyMode:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_no_impact_on_stored_domain(self, tenant_db):
        # use_domains absent: declared domain_id stored verbatim, domains list allowed.
        cfg = parse_config_dict(_config({}, [{"id": "sales"}], "sales"))
        async with tenant_db.acquire() as conn:
            await load_config(cfg, conn)
            assert await _stored_domain(conn) == "sales"
            assert domain_policy.use_domains() is None


class TestNamespacedMode:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_declared_domain_stored(self, tenant_db):
        cfg = parse_config_dict(_config({"use_domains": True}, [{"id": "sales"}], "sales"))
        async with tenant_db.acquire() as conn:
            await load_config(cfg, conn)
            assert await _stored_domain(conn) == "sales"


class TestReloadValidationSweep:
    @pytest.mark.asyncio(loop_scope="session")
    async def test_sweep_rejects_preexisting_foreign_domain(self, tenant_db):
        # Simulate a dynamically-registered table left behind with a foreign domain,
        # then switch to single-domain mode and run the sweep.
        async with tenant_db.acquire() as conn:
            await conn.execute(
                "INSERT INTO sources (id, type, dialect) VALUES ('pg1', 'postgresql', 'postgres') "
                "ON CONFLICT (id) DO NOTHING"
            )
            await conn.execute(
                "INSERT INTO domains (id) VALUES ('sales') ON CONFLICT (id) DO NOTHING"
            )
            await conn.execute(
                "INSERT INTO registered_tables (source_id, domain_id, schema_name, table_name) "
                "VALUES ('pg1', 'sales', 'public', 'legacy_tbl')"
            )
            with pytest.raises(RuntimeError, match="re-register"):
                await _validate_existing_domains(conn, "global")

    @pytest.mark.asyncio(loop_scope="session")
    async def test_sweep_passes_when_all_default(self, tenant_db):
        async with tenant_db.acquire() as conn:
            await conn.execute(
                "INSERT INTO sources (id, type, dialect) VALUES ('pg1', 'postgresql', 'postgres') "
                "ON CONFLICT (id) DO NOTHING"
            )
            await conn.execute(
                "INSERT INTO domains (id) VALUES ('global') ON CONFLICT (id) DO NOTHING"
            )
            await conn.execute(
                "INSERT INTO registered_tables (source_id, domain_id, schema_name, table_name) "
                "VALUES ('pg1', 'global', 'public', 'ok_tbl')"
            )
            # Should not raise — all tables are in the default domain.
            await _validate_existing_domains(conn, "global")
            # Verify the table is actually present with the expected domain_id.
            domain_id = await conn.fetchval(
                "SELECT domain_id FROM registered_tables WHERE table_name = 'ok_tbl'"
            )
            assert domain_id == "global"
