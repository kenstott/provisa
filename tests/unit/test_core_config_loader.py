# Copyright (c) 2026 Kenneth Stott
# Canary: d2e3f4a5-b6c7-8901-bcde-f12345678901
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for config_loader: parse_config, parse_config_dict, and load_config orchestration."""

import textwrap
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
import yaml

from provisa.core.config_loader import (
    _load_config_in_txn,
    load_config,
    load_config_from_yaml,
    parse_config,
    parse_config_dict,
)
from provisa.core.models import (
    Cardinality,
    Column,
    Domain,
    GovernanceLevel,
    NamingConfig,
    NamingRule,
    ProvisaConfig,
    RLSRule,
    Relationship,
    Role,
    Source,
    SourceType,
    Table,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _minimal_config_dict():
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
        "domains": [{"id": "sales", "description": "Sales data"}],
        "tables": [
            {
                "source_id": "pg1",
                "domain_id": "sales",
                "schema": "public",
                "table": "orders",
                "governance": "pre-approved",
                "columns": [{"name": "id", "visible_to": ["admin"]}],
            }
        ],
        "roles": [{"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}],
    }


def _full_config_dict():
    """Config with sources, domains, naming, roles, tables, relationships, and RLS rules."""
    d = _minimal_config_dict()
    d["naming"] = {"convention": "snake_case", "rules": [{"pattern": "^prod_", "replace": ""}]}
    d["tables"].append(
        {
            "source_id": "pg1",
            "domain_id": "sales",
            "schema": "public",
            "table": "customers",
            "governance": "pre-approved",
            "columns": [{"name": "id", "visible_to": ["admin"]}],
        }
    )
    d["relationships"] = [
        {
            "id": "orders-customers",
            "source_table_id": "orders",
            "target_table_id": "customers",
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        }
    ]
    d["rls_rules"] = [
        {"table_id": "orders", "role_id": "admin", "filter": "1=1"}
    ]
    return d


# ---------------------------------------------------------------------------
# parse_config_dict
# ---------------------------------------------------------------------------


class TestParseConfigDict:
    def test_returns_provisa_config(self):
        cfg = parse_config_dict(_minimal_config_dict())
        assert isinstance(cfg, ProvisaConfig)

    def test_sources_parsed(self):
        cfg = parse_config_dict(_minimal_config_dict())
        assert len(cfg.sources) == 1
        assert cfg.sources[0].id == "pg1"
        assert cfg.sources[0].type == SourceType.postgresql

    def test_domains_parsed(self):
        cfg = parse_config_dict(_minimal_config_dict())
        assert len(cfg.domains) == 1
        assert cfg.domains[0].id == "sales"
        assert cfg.domains[0].description == "Sales data"

    def test_tables_parsed(self):
        cfg = parse_config_dict(_minimal_config_dict())
        assert len(cfg.tables) == 1
        t = cfg.tables[0]
        assert t.schema_name == "public"
        assert t.table_name == "orders"
        assert t.governance == GovernanceLevel.pre_approved

    def test_roles_parsed(self):
        cfg = parse_config_dict(_minimal_config_dict())
        assert len(cfg.roles) == 1
        assert cfg.roles[0].id == "admin"

    def test_defaults_applied(self):
        cfg = parse_config_dict(_minimal_config_dict())
        assert cfg.relationships == []
        assert cfg.rls_rules == []
        assert cfg.naming.convention == "snake_case"

    def test_full_config_relationships(self):
        cfg = parse_config_dict(_full_config_dict())
        assert len(cfg.relationships) == 1
        assert cfg.relationships[0].id == "orders-customers"
        assert cfg.relationships[0].cardinality == Cardinality.many_to_one

    def test_full_config_rls_rules(self):
        cfg = parse_config_dict(_full_config_dict())
        assert len(cfg.rls_rules) == 1
        assert cfg.rls_rules[0].filter == "1=1"

    def test_naming_rules_parsed(self):
        cfg = parse_config_dict(_full_config_dict())
        assert len(cfg.naming.rules) == 1
        assert cfg.naming.rules[0].pattern == "^prod_"

    def test_invalid_source_type_raises(self):
        data = _minimal_config_dict()
        data["sources"][0]["type"] = "not_a_real_type"
        with pytest.raises(Exception):
            parse_config_dict(data)

    def test_invalid_governance_raises(self):
        data = _minimal_config_dict()
        data["tables"][0]["governance"] = "open-access"
        with pytest.raises(Exception):
            parse_config_dict(data)

    def test_missing_sources_raises(self):
        data = _minimal_config_dict()
        del data["sources"]
        with pytest.raises(Exception):
            parse_config_dict(data)

    def test_secret_ref_passthrough(self):
        """Secret references should be stored as-is; not resolved during parse."""
        data = _minimal_config_dict()
        data["sources"][0]["password"] = "${env:MY_SECRET}"
        cfg = parse_config_dict(data)
        assert cfg.sources[0].password == "${env:MY_SECRET}"


# ---------------------------------------------------------------------------
# parse_config (file-based)
# ---------------------------------------------------------------------------


class TestParseConfig:
    def test_parses_yaml_file(self, tmp_path):
        config_path = tmp_path / "provisa.yaml"
        config_path.write_text(yaml.dump(_minimal_config_dict()), encoding="utf-8")
        cfg = parse_config(config_path)
        assert isinstance(cfg, ProvisaConfig)
        assert cfg.sources[0].id == "pg1"

    def test_parses_yaml_file_str_path(self, tmp_path):
        config_path = tmp_path / "provisa.yaml"
        config_path.write_text(yaml.dump(_minimal_config_dict()), encoding="utf-8")
        cfg = parse_config(str(config_path))
        assert isinstance(cfg, ProvisaConfig)

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            parse_config(tmp_path / "nonexistent.yaml")

    def test_full_yaml_config(self, tmp_path):
        config_path = tmp_path / "full.yaml"
        config_path.write_text(yaml.dump(_full_config_dict()), encoding="utf-8")
        cfg = parse_config(config_path)
        assert len(cfg.relationships) == 1
        assert len(cfg.rls_rules) == 1

    def test_sample_fixture_file(self):
        """Validate the shared sample_config.yaml fixture parses correctly."""
        fixture_path = Path(__file__).parent.parent / "fixtures" / "sample_config.yaml"
        cfg = parse_config(fixture_path)
        assert len(cfg.sources) >= 1
        assert len(cfg.tables) >= 1


# ---------------------------------------------------------------------------
# _load_config_in_txn (orchestration, mocked asyncpg)
# ---------------------------------------------------------------------------


class TestLoadConfigInTxn:
    def _make_conn(self):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="OK")
        conn.fetchval = AsyncMock(return_value=1)
        conn.fetchrow = AsyncMock(return_value=None)
        conn.fetch = AsyncMock(return_value=[])
        return conn

    @pytest.mark.asyncio
    async def test_sources_upserted(self):
        cfg = parse_config_dict(_minimal_config_dict())
        conn = self._make_conn()
        with patch("provisa.core.repositories.source.upsert", new_callable=AsyncMock) as mock_upsert, \
             patch("provisa.core.repositories.domain.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.role.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.table.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.relationship.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.rls.upsert", new_callable=AsyncMock):
            await _load_config_in_txn(cfg, conn, trino_conn=None)
        mock_upsert.assert_awaited_once()
        call_source = mock_upsert.call_args[0][1]
        assert call_source.id == "pg1"

    @pytest.mark.asyncio
    async def test_domains_upserted(self):
        cfg = parse_config_dict(_minimal_config_dict())
        conn = self._make_conn()
        with patch("provisa.core.repositories.source.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.domain.upsert", new_callable=AsyncMock) as mock_dom, \
             patch("provisa.core.repositories.role.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.table.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.relationship.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.rls.upsert", new_callable=AsyncMock):
            await _load_config_in_txn(cfg, conn, trino_conn=None)
        mock_dom.assert_awaited_once()
        call_domain = mock_dom.call_args[0][1]
        assert call_domain.id == "sales"

    @pytest.mark.asyncio
    async def test_naming_rules_deleted_then_inserted(self):
        cfg = parse_config_dict(_full_config_dict())
        conn = self._make_conn()
        with patch("provisa.core.repositories.source.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.domain.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.role.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.table.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.relationship.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.rls.upsert", new_callable=AsyncMock):
            await _load_config_in_txn(cfg, conn, trino_conn=None)

        execute_calls = [str(c) for c in conn.execute.call_args_list]
        delete_call = next((c for c in execute_calls if "DELETE FROM naming_rules" in c), None)
        insert_call = next((c for c in execute_calls if "INSERT INTO naming_rules" in c), None)
        assert delete_call is not None, "Expected DELETE FROM naming_rules call"
        assert insert_call is not None, "Expected INSERT INTO naming_rules call"

    @pytest.mark.asyncio
    async def test_roles_upserted(self):
        cfg = parse_config_dict(_minimal_config_dict())
        conn = self._make_conn()
        with patch("provisa.core.repositories.source.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.domain.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.role.upsert", new_callable=AsyncMock) as mock_role, \
             patch("provisa.core.repositories.table.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.relationship.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.rls.upsert", new_callable=AsyncMock):
            await _load_config_in_txn(cfg, conn, trino_conn=None)
        mock_role.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_tables_upserted(self):
        cfg = parse_config_dict(_minimal_config_dict())
        conn = self._make_conn()
        with patch("provisa.core.repositories.source.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.domain.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.role.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.table.upsert", new_callable=AsyncMock) as mock_tbl, \
             patch("provisa.core.repositories.relationship.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.rls.upsert", new_callable=AsyncMock):
            await _load_config_in_txn(cfg, conn, trino_conn=None)
        mock_tbl.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_relationships_upserted(self):
        cfg = parse_config_dict(_full_config_dict())
        conn = self._make_conn()
        with patch("provisa.core.repositories.source.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.domain.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.role.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.table.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.relationship.upsert", new_callable=AsyncMock) as mock_rel, \
             patch("provisa.core.repositories.rls.upsert", new_callable=AsyncMock):
            await _load_config_in_txn(cfg, conn, trino_conn=None)
        mock_rel.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rls_rules_upserted(self):
        cfg = parse_config_dict(_full_config_dict())
        conn = self._make_conn()
        with patch("provisa.core.repositories.source.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.domain.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.role.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.table.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.relationship.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.rls.upsert", new_callable=AsyncMock) as mock_rls:
            await _load_config_in_txn(cfg, conn, trino_conn=None)
        mock_rls.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_trino_catalog_created_when_conn_provided(self):
        cfg = parse_config_dict(_minimal_config_dict())
        conn = self._make_conn()
        trino_conn = MagicMock()
        with patch("provisa.core.repositories.source.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.domain.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.role.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.table.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.relationship.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.rls.upsert", new_callable=AsyncMock), \
             patch("provisa.core.catalog.create_catalog") as mock_create_catalog, \
             patch("provisa.core.catalog.analyze_source_tables"), \
             patch("provisa.core.secrets.resolve_secrets", return_value="resolved_pw"):
            await _load_config_in_txn(cfg, conn, trino_conn=trino_conn)
        mock_create_catalog.assert_called_once()

    @pytest.mark.asyncio
    async def test_trino_catalog_not_created_when_conn_none(self):
        cfg = parse_config_dict(_minimal_config_dict())
        conn = self._make_conn()
        with patch("provisa.core.repositories.source.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.domain.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.role.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.table.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.relationship.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.rls.upsert", new_callable=AsyncMock), \
             patch("provisa.core.catalog.create_catalog") as mock_create_catalog:
            await _load_config_in_txn(cfg, conn, trino_conn=None)
        mock_create_catalog.assert_not_called()

    @pytest.mark.asyncio
    async def test_trino_catalog_failure_does_not_raise(self):
        """Catalog creation failure is swallowed — config loading must continue."""
        cfg = parse_config_dict(_minimal_config_dict())
        conn = self._make_conn()
        trino_conn = MagicMock()
        with patch("provisa.core.repositories.source.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.domain.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.role.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.table.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.relationship.upsert", new_callable=AsyncMock), \
             patch("provisa.core.repositories.rls.upsert", new_callable=AsyncMock), \
             patch("provisa.core.catalog.create_catalog", side_effect=Exception("catalog error")), \
             patch("provisa.core.catalog.analyze_source_tables"), \
             patch("provisa.core.secrets.resolve_secrets", return_value="pw"):
            # Should not raise despite catalog failure
            await _load_config_in_txn(cfg, conn, trino_conn=trino_conn)


# ---------------------------------------------------------------------------
# load_config (transaction wrapper)
# ---------------------------------------------------------------------------


class TestLoadConfig:
    @pytest.mark.asyncio
    async def test_runs_in_transaction(self):
        cfg = parse_config_dict(_minimal_config_dict())

        # Simulate asyncpg connection with transaction context manager
        conn = AsyncMock()
        txn = AsyncMock()
        txn.__aenter__ = AsyncMock(return_value=txn)
        txn.__aexit__ = AsyncMock(return_value=False)
        conn.transaction = MagicMock(return_value=txn)

        with patch(
            "provisa.core.config_loader._load_config_in_txn", new_callable=AsyncMock
        ) as mock_inner:
            await load_config(cfg, conn, trino_conn=None)

        mock_inner.assert_awaited_once_with(cfg, conn, None)
        conn.transaction.assert_called_once()


# ---------------------------------------------------------------------------
# load_config_from_yaml
# ---------------------------------------------------------------------------


class TestLoadConfigFromYaml:
    @pytest.mark.asyncio
    async def test_returns_provisa_config(self, tmp_path):
        config_path = tmp_path / "provisa.yaml"
        config_path.write_text(yaml.dump(_minimal_config_dict()), encoding="utf-8")

        conn = AsyncMock()
        txn = AsyncMock()
        txn.__aenter__ = AsyncMock(return_value=txn)
        txn.__aexit__ = AsyncMock(return_value=False)
        conn.transaction = MagicMock(return_value=txn)

        with patch(
            "provisa.core.config_loader._load_config_in_txn", new_callable=AsyncMock
        ):
            result = await load_config_from_yaml(config_path, conn, trino_conn=None)

        assert isinstance(result, ProvisaConfig)
        assert result.sources[0].id == "pg1"

    @pytest.mark.asyncio
    async def test_raises_on_missing_file(self, tmp_path):
        conn = AsyncMock()
        with pytest.raises(FileNotFoundError):
            await load_config_from_yaml(tmp_path / "missing.yaml", conn)
