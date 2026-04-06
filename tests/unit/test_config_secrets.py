# Copyright (c) 2026 Kenneth Stott
# Canary: f1e7d23a-804c-4b19-9e50-c3d0a5b7e648
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for config loading and secrets resolution (REQ-164–168)."""

from __future__ import annotations

import textwrap
import tempfile
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from provisa.core.config_loader import parse_config, parse_config_dict
from provisa.core.models import ProvisaConfig
from provisa.core.secrets import (
    SecretsProvider,
    register_provider,
    resolve_secrets,
    resolve_secrets_in_dict,
)


# ---------------------------------------------------------------------------
# Minimal valid config dict used across tests
# ---------------------------------------------------------------------------

_VALID_CONFIG = {
    "sources": [
        {
            "id": "test-pg",
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "secret",
        }
    ],
    "domains": [{"id": "analytics", "description": "Analytics domain"}],
    "tables": [
        {
            "source_id": "test-pg",
            "domain_id": "analytics",
            "schema": "public",
            "table": "events",
            "governance": "pre-approved",
            "columns": [
                {"name": "id", "visible_to": ["admin"]},
                {"name": "event_type", "visible_to": ["admin"]},
            ],
        }
    ],
    "roles": [
        {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}
    ],
}


# ---------------------------------------------------------------------------
# TestResolveSecrets
# ---------------------------------------------------------------------------


class TestResolveSecrets:
    def test_env_pattern_resolved(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "resolved_value")
        result = resolve_secrets("${env:MY_VAR}")
        assert result == "resolved_value"

    def test_missing_env_raises_key_error(self):
        # Use an implausible name to avoid collision with real env vars
        os.environ.pop("PROVISA_TOTALLY_MISSING_8472", None)
        with pytest.raises(KeyError):
            resolve_secrets("${env:PROVISA_TOTALLY_MISSING_8472}")

    def test_string_without_pattern_returned_unchanged(self):
        assert resolve_secrets("just-a-plain-string") == "just-a-plain-string"

    def test_multiple_patterns_in_one_string_all_resolved(self, monkeypatch):
        monkeypatch.setenv("DB_HOST", "db.example.com")
        monkeypatch.setenv("DB_PORT", "5432")
        result = resolve_secrets("host=${env:DB_HOST} port=${env:DB_PORT}")
        assert result == "host=db.example.com port=5432"

    def test_unknown_provider_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown secrets provider"):
            resolve_secrets("${vault:secret/my-path}")


# ---------------------------------------------------------------------------
# TestResolveSecretsInDict
# ---------------------------------------------------------------------------


class TestResolveSecretsInDict:
    def test_flat_dict_string_values_resolved(self, monkeypatch):
        monkeypatch.setenv("API_KEY", "abc123")
        data = {"key": "${env:API_KEY}", "label": "static"}
        result = resolve_secrets_in_dict(data)
        assert result["key"] == "abc123"
        assert result["label"] == "static"

    def test_nested_dict_recursively_resolved(self, monkeypatch):
        monkeypatch.setenv("INNER_PW", "pw999")
        data = {"outer": {"inner": {"password": "${env:INNER_PW}"}}}
        result = resolve_secrets_in_dict(data)
        assert result["outer"]["inner"]["password"] == "pw999"

    def test_list_of_strings_each_resolved(self, monkeypatch):
        monkeypatch.setenv("TOKEN_A", "aaa")
        monkeypatch.setenv("TOKEN_B", "bbb")
        data = {"tokens": ["${env:TOKEN_A}", "${env:TOKEN_B}", "literal"]}
        result = resolve_secrets_in_dict(data)
        assert result["tokens"] == ["aaa", "bbb", "literal"]

    def test_list_of_dicts_recursively_resolved(self, monkeypatch):
        monkeypatch.setenv("SRC_PW", "pg_pw")
        data = {
            "sources": [
                {"host": "localhost", "password": "${env:SRC_PW}"},
            ]
        }
        result = resolve_secrets_in_dict(data)
        assert result["sources"][0]["password"] == "pg_pw"
        assert result["sources"][0]["host"] == "localhost"

    def test_non_string_values_passed_through_unchanged(self, monkeypatch):
        data = {"port": 5432, "enabled": True, "nothing": None, "count": 0}
        result = resolve_secrets_in_dict(data)
        assert result["port"] == 5432
        assert result["enabled"] is True
        assert result["nothing"] is None
        assert result["count"] == 0


# ---------------------------------------------------------------------------
# TestRegisterProvider
# ---------------------------------------------------------------------------


class TestRegisterProvider:
    def test_register_provider_adds_new_provider(self):
        mock_provider = MagicMock(spec=SecretsProvider)
        mock_provider.resolve.return_value = "from-mock"
        register_provider("mock", mock_provider)
        result = resolve_secrets("${mock:some-ref}")
        assert result == "from-mock"
        mock_provider.resolve.assert_called_once_with("some-ref")

    def test_custom_provider_resolve_called_with_reference(self):
        class UpperProvider(SecretsProvider):
            def resolve(self, reference: str) -> str:
                return reference.upper()

        register_provider("upper", UpperProvider())
        result = resolve_secrets("${upper:hello-world}")
        assert result == "HELLO-WORLD"


# ---------------------------------------------------------------------------
# TestParseConfig (parse_config_dict)
# ---------------------------------------------------------------------------


class TestParseConfig:
    def test_valid_config_dict_returns_provisa_config(self):
        config = parse_config_dict(_VALID_CONFIG)
        assert isinstance(config, ProvisaConfig)

    def test_sources_parsed(self):
        config = parse_config_dict(_VALID_CONFIG)
        assert len(config.sources) == 1
        assert config.sources[0].id == "test-pg"

    def test_roles_parsed(self):
        config = parse_config_dict(_VALID_CONFIG)
        assert len(config.roles) == 1
        assert config.roles[0].id == "admin"

    def test_tables_parsed(self):
        config = parse_config_dict(_VALID_CONFIG)
        assert len(config.tables) == 1
        assert config.tables[0].table_name == "events"

    def test_invalid_config_raises_validation_error(self):
        bad = {
            # Missing required fields: sources, domains, tables, roles
            "naming": {"convention": "snake_case"},
        }
        with pytest.raises(ValidationError):
            parse_config_dict(bad)

    def test_invalid_source_type_raises_validation_error(self):
        bad = dict(_VALID_CONFIG)
        bad["sources"] = [
            {
                "id": "bad-src",
                "type": "not_a_real_db",  # invalid SourceType
                "host": "localhost",
                "port": 5432,
                "database": "db",
                "username": "u",
                "password": "p",
            }
        ]
        with pytest.raises(ValidationError):
            parse_config_dict(bad)


# ---------------------------------------------------------------------------
# TestParseConfigFile
# ---------------------------------------------------------------------------


class TestParseConfigFile:
    def _write_yaml(self, content: str) -> Path:
        f = tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, encoding="utf-8"
        )
        f.write(textwrap.dedent(content))
        f.close()
        return Path(f.name)

    def test_valid_yaml_returns_provisa_config(self):
        path = self._write_yaml("""
            sources:
              - id: yaml-pg
                type: postgresql
                host: localhost
                port: 5432
                database: mydb
                username: admin
                password: secret
            domains:
              - id: ops
                description: Operations
            tables:
              - source_id: yaml-pg
                domain_id: ops
                schema: public
                table: shipments
                governance: pre-approved
                columns:
                  - name: id
                    visible_to: [admin]
            roles:
              - id: admin
                capabilities: [admin]
                domain_access: ["*"]
        """)
        try:
            config = parse_config(path)
            assert isinstance(config, ProvisaConfig)
            assert config.sources[0].id == "yaml-pg"
        finally:
            path.unlink(missing_ok=True)

    def test_config_with_sources_roles_tables_parsed_correctly(self):
        path = self._write_yaml("""
            sources:
              - id: analytics-pg
                type: postgresql
                host: db.internal
                port: 5432
                database: warehouse
                username: reader
                password: "${env:DB_PASS}"
            domains:
              - id: sales
                description: Sales Domain
              - id: ops
                description: Operations Domain
            tables:
              - source_id: analytics-pg
                domain_id: sales
                schema: public
                table: orders
                governance: pre-approved
                columns:
                  - name: order_id
                    visible_to: [admin, analyst]
                  - name: amount
                    visible_to: [admin]
              - source_id: analytics-pg
                domain_id: ops
                schema: public
                table: shipments
                governance: registry-required
                columns:
                  - name: id
                    visible_to: [admin]
            roles:
              - id: admin
                capabilities: [admin]
                domain_access: ["*"]
              - id: analyst
                capabilities: [query_development]
                domain_access: [sales]
        """)
        try:
            config = parse_config(path)
            assert len(config.sources) == 1
            assert len(config.domains) == 2
            assert len(config.tables) == 2
            assert len(config.roles) == 2
            assert config.tables[0].table_name == "orders"
            assert config.tables[1].table_name == "shipments"
        finally:
            path.unlink(missing_ok=True)

    def test_missing_required_fields_raises_validation_error(self):
        path = self._write_yaml("""
            # Config with no sources, domains, tables, or roles
            naming:
              convention: snake_case
        """)
        try:
            with pytest.raises(ValidationError):
                parse_config(path)
        finally:
            path.unlink(missing_ok=True)
