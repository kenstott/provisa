# Copyright (c) 2026 Kenneth Stott
# Canary: d4e5f6a7-b8c9-0123-def0-123456789bcd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for config-parsing edge cases.

Tests Pydantic validation and parse_config_dict logic for corner-cases NOT
already covered by tests/unit/test_models.py or tests/integration/test_config_loader.py.

All tests are synchronous, database-free, and require only Pydantic + models.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from provisa.core.config_loader import parse_config_dict
from provisa.core.models import (
    ColumnPreset,
    ProvisaConfig,
    RLSRule,
    Role,
    Source,
    flatten_roles,
)


# ---------------------------------------------------------------------------
# Minimal valid building blocks
# ---------------------------------------------------------------------------

def _minimal_source(source_id: str = "pg-src") -> dict:
    return {
        "id": source_id,
        "type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "username": "user",
        "password": "secret",
    }


def _minimal_domain(domain_id: str = "d1") -> dict:
    return {"id": domain_id}


def _minimal_table(source_id: str = "pg-src", domain_id: str = "d1") -> dict:
    return {
        "source_id": source_id,
        "domain_id": domain_id,
        "schema": "public",
        "table": "orders",
        "governance": "pre-approved",
        "columns": [{"name": "id", "visible_to": ["admin"]}],
    }


def _minimal_role(role_id: str = "analyst") -> dict:
    return {
        "id": role_id,
        "capabilities": ["read"],
        "domain_access": ["d1"],
    }


def _minimal_config(**overrides) -> dict:
    base = {
        "sources": [_minimal_source()],
        "domains": [_minimal_domain()],
        "tables": [_minimal_table()],
        "roles": [_minimal_role()],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# TestMissingRequiredTopLevelFields
# ---------------------------------------------------------------------------

class TestMissingRequiredTopLevelFields:
    """parse_config_dict must raise when any of the four required list fields
    are absent.  test_models.py covers the case where sources and domains are
    present but empty ({sources:[], domains:[]}); here we verify each individual
    required field in isolation and the fully empty dict case.
    """

    def test_empty_dict_raises(self):
        with pytest.raises(ValidationError):
            parse_config_dict({})

    def test_missing_sources_raises(self):
        data = {
            "domains": [_minimal_domain()],
            "tables": [_minimal_table()],
            "roles": [_minimal_role()],
        }
        with pytest.raises(ValidationError):
            parse_config_dict(data)

    def test_missing_domains_raises(self):
        data = {
            "sources": [_minimal_source()],
            "tables": [_minimal_table()],
            "roles": [_minimal_role()],
        }
        with pytest.raises(ValidationError):
            parse_config_dict(data)

    def test_missing_tables_raises(self):
        data = {
            "sources": [_minimal_source()],
            "domains": [_minimal_domain()],
            "roles": [_minimal_role()],
        }
        with pytest.raises(ValidationError):
            parse_config_dict(data)

    def test_missing_roles_raises(self):
        data = {
            "sources": [_minimal_source()],
            "domains": [_minimal_domain()],
            "tables": [_minimal_table()],
        }
        with pytest.raises(ValidationError):
            parse_config_dict(data)


# ---------------------------------------------------------------------------
# TestInvalidSourceType
# ---------------------------------------------------------------------------

class TestInvalidSourceType:
    """Source.type must be a valid SourceType enum member.
    test_models.py covers Source directly; here we test via parse_config_dict
    so the full config-loader path is exercised.
    """

    def test_unknown_connector_type_in_config_dict_raises(self):
        data = _minimal_config(sources=[{
            "id": "bad-src",
            "type": "fakedb",
            "host": "localhost",
            "port": 5432,
            "database": "d",
            "username": "u",
            "password": "p",
        }])
        with pytest.raises(ValidationError):
            parse_config_dict(data)

    def test_numeric_type_value_raises(self):
        data = _minimal_config(sources=[{
            "id": "numeric-src",
            "type": 42,
            "host": "localhost",
            "port": 5432,
            "database": "d",
            "username": "u",
            "password": "p",
        }])
        with pytest.raises(ValidationError):
            parse_config_dict(data)

    def test_empty_string_type_raises(self):
        data = _minimal_config(sources=[{
            "id": "empty-type-src",
            "type": "",
            "host": "localhost",
            "port": 5432,
            "database": "d",
            "username": "u",
            "password": "p",
        }])
        with pytest.raises(ValidationError):
            parse_config_dict(data)


# ---------------------------------------------------------------------------
# TestSourceIdValidation
# ---------------------------------------------------------------------------

class TestSourceIdValidation:
    """Source.id is restricted to ^[a-zA-Z][a-zA-Z0-9_-]*$ by a field_validator.
    Not tested via parse_config_dict in any existing file.
    """

    def test_source_id_starting_with_digit_raises(self):
        data = _minimal_config(sources=[{
            "id": "1invalid",
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "d",
            "username": "u",
            "password": "p",
        }])
        with pytest.raises(ValidationError, match="Source id"):
            parse_config_dict(data)

    def test_source_id_with_space_raises(self):
        data = _minimal_config(sources=[{
            "id": "has space",
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "d",
            "username": "u",
            "password": "p",
        }])
        with pytest.raises(ValidationError, match="Source id"):
            parse_config_dict(data)

    def test_source_id_with_dot_raises(self):
        data = _minimal_config(sources=[{
            "id": "src.name",
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "d",
            "username": "u",
            "password": "p",
        }])
        with pytest.raises(ValidationError, match="Source id"):
            parse_config_dict(data)

    def test_valid_source_id_with_hyphens_and_underscores_parses(self):
        data = _minimal_config(sources=[{
            "id": "my-pg_source",
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "d",
            "username": "u",
            "password": "p",
        }])
        config = parse_config_dict(data)
        assert config.sources[0].id == "my-pg_source"


# ---------------------------------------------------------------------------
# TestRLSFilterField
# ---------------------------------------------------------------------------

class TestRLSFilterField:
    """RLSRule.filter is a plain str with no Pydantic validator.

    Valid SQL filter expressions must parse without error.
    Edge cases (empty string, whitespace) are accepted by Pydantic because no
    validator restricts the field — we document that behaviour here.
    """

    def test_valid_sql_filter_expression_parses(self):
        rule = RLSRule(
            table_id="orders",
            role_id="analyst",
            filter="region = 'us-east' AND status != 'deleted'",
        )
        assert rule.filter == "region = 'us-east' AND status != 'deleted'"

    def test_filter_with_subquery_parses(self):
        rule = RLSRule(
            table_id="orders",
            role_id="analyst",
            filter="customer_id IN (SELECT id FROM customers WHERE tier = 'gold')",
        )
        assert "SELECT" in rule.filter

    def test_filter_empty_string_is_accepted_by_pydantic(self):
        """No validator exists for the filter field; empty string silently parses."""
        rule = RLSRule(table_id="orders", role_id="analyst", filter="")
        assert rule.filter == ""

    def test_rls_rule_filter_via_parse_config_dict(self):
        data = _minimal_config(rls_rules=[{
            "table_id": "orders",
            "role_id": "analyst",
            "filter": "user_id = current_user_id()",
        }])
        config = parse_config_dict(data)
        assert len(config.rls_rules) == 1
        assert config.rls_rules[0].filter == "user_id = current_user_id()"

    def test_filter_none_raises_validation_error(self):
        """filter is a required str field; None should raise."""
        with pytest.raises((ValidationError, TypeError)):
            RLSRule(table_id="orders", role_id="analyst", filter=None)


# ---------------------------------------------------------------------------
# TestColumnPresetFields
# ---------------------------------------------------------------------------

class TestColumnPresetFields:
    """ColumnPreset.source is a plain str (no enum validator).

    Source values "now", "header", "literal" are documented conventions but not
    enforced by Pydantic — we document that valid values parse cleanly and that
    the optional name/value fields behave as expected.
    """

    def test_source_now_parses(self):
        preset = ColumnPreset(column="created_at", source="now")
        assert preset.source == "now"
        assert preset.name is None
        assert preset.value is None

    def test_source_header_with_name_parses(self):
        preset = ColumnPreset(column="tenant_id", source="header", name="X-Tenant-ID")
        assert preset.source == "header"
        assert preset.name == "X-Tenant-ID"

    def test_source_header_without_name_parses_with_none(self):
        """No Pydantic validator enforces that header requires name; name defaults to None."""
        preset = ColumnPreset(column="user_id", source="header")
        assert preset.name is None

    def test_source_literal_with_value_parses(self):
        preset = ColumnPreset(column="status", source="literal", value="active")
        assert preset.source == "literal"
        assert preset.value == "active"

    def test_source_literal_without_value_parses_with_none(self):
        """No Pydantic validator enforces that literal requires value; value defaults to None."""
        preset = ColumnPreset(column="status", source="literal")
        assert preset.value is None

    def test_unrecognised_source_string_is_accepted_by_pydantic(self):
        """The source field is a plain str — Pydantic does not reject unknown values."""
        preset = ColumnPreset(column="col", source="unknown_source_type")
        assert preset.source == "unknown_source_type"

    def test_column_preset_via_parse_config_dict(self):
        table = _minimal_table()
        table["column_presets"] = [
            {"column": "created_at", "source": "now"},
            {"column": "tenant_id", "source": "header", "name": "X-Tenant-ID"},
        ]
        data = _minimal_config(tables=[table])
        config = parse_config_dict(data)
        presets = config.tables[0].column_presets
        assert len(presets) == 2
        assert presets[0].source == "now"
        assert presets[1].name == "X-Tenant-ID"


# ---------------------------------------------------------------------------
# TestRoleSelfReference
# ---------------------------------------------------------------------------

class TestRoleSelfReference:
    """Role.parent_role_id is str | None with no self-reference guard in the model.

    Pydantic will parse a role whose parent_role_id equals its own id without
    error.  The flatten_roles() helper, however, will recurse infinitely on
    such a config — we verify the parse succeeds and document the runtime
    behaviour of flatten_roles via sys.setrecursionlimit protection.
    """

    def test_self_referencing_parent_role_parses_without_error(self):
        """Pydantic accepts parent_role_id == id (no circular-ref validator)."""
        role = Role(
            id="admin",
            capabilities=["admin"],
            domain_access=["*"],
            parent_role_id="admin",  # points to itself
        )
        assert role.parent_role_id == role.id

    def test_self_referencing_role_via_parse_config_dict_parses(self):
        data = _minimal_config(roles=[{
            "id": "loop-role",
            "capabilities": ["read"],
            "domain_access": ["d1"],
            "parent_role_id": "loop-role",
        }])
        config = parse_config_dict(data)
        assert config.roles[0].parent_role_id == "loop-role"

    def test_flatten_roles_with_self_reference_raises_recursion_error(self):
        """flatten_roles() has no cycle guard; a self-referencing role causes
        infinite recursion.  RecursionError (or a stack overflow) is expected.
        """
        import sys
        role = Role(
            id="loop",
            capabilities=["read"],
            domain_access=["d1"],
            parent_role_id="loop",
        )
        old_limit = sys.getrecursionlimit()
        sys.setrecursionlimit(200)  # keep the test fast
        try:
            with pytest.raises(RecursionError):
                flatten_roles([role])
        finally:
            sys.setrecursionlimit(old_limit)


# ---------------------------------------------------------------------------
# TestDuplicateSourceIds
# ---------------------------------------------------------------------------

class TestDuplicateSourceIds:
    """Two Source entries with the same id are NOT rejected by Pydantic.

    ProvisaConfig has no uniqueness validator on the sources list; both entries
    will be present in config.sources.  This test documents that behaviour so
    that a future uniqueness validator does not regress silently.
    """

    def test_duplicate_source_ids_both_parse_into_sources_list(self):
        data = _minimal_config(sources=[
            _minimal_source("dup-src"),
            {
                "id": "dup-src",
                "type": "mysql",
                "host": "other-host",
                "port": 3306,
                "database": "d2",
                "username": "u2",
                "password": "p2",
            },
        ])
        config = parse_config_dict(data)
        assert len(config.sources) == 2
        assert all(s.id == "dup-src" for s in config.sources)

    def test_duplicate_source_ids_preserves_insertion_order(self):
        data = _minimal_config(sources=[
            _minimal_source("first"),
            _minimal_source("second"),
            _minimal_source("first"),  # duplicate
        ])
        config = parse_config_dict(data)
        ids = [s.id for s in config.sources]
        assert ids == ["first", "second", "first"]


# ---------------------------------------------------------------------------
# TestValidMinimalConfig
# ---------------------------------------------------------------------------

class TestValidMinimalConfig:
    """A config with the bare minimum fields must parse completely without error."""

    def test_minimal_config_parses_successfully(self):
        config = parse_config_dict(_minimal_config())
        assert isinstance(config, ProvisaConfig)
        assert len(config.sources) == 1
        assert len(config.domains) == 1
        assert len(config.tables) == 1
        assert len(config.roles) == 1

    def test_minimal_config_optional_lists_default_to_empty(self):
        config = parse_config_dict(_minimal_config())
        assert config.relationships == []
        assert config.rls_rules == []
        assert config.event_triggers == []
        assert config.scheduled_triggers == []
        assert config.functions == []
        assert config.webhooks == []

    def test_minimal_config_default_naming_convention_is_snake_case(self):
        config = parse_config_dict(_minimal_config())
        assert config.naming.convention == "snake_case"

    def test_minimal_config_default_server_hostname_is_localhost(self):
        config = parse_config_dict(_minimal_config())
        assert config.server.hostname == "localhost"
        assert config.server.port == 8000

    def test_minimal_config_all_valid_source_types_parse(self):
        """Spot-check a variety of valid SourceType values through parse_config_dict."""
        for src_type in ("mysql", "snowflake", "bigquery", "clickhouse", "mongodb"):
            data = _minimal_config(sources=[{
                "id": "src-a",
                "type": src_type,
                "host": "host",
                "port": 5432,
                "database": "db",
                "username": "u",
                "password": "p",
            }])
            config = parse_config_dict(data)
            assert config.sources[0].type.value == src_type
