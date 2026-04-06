# Copyright (c) 2026 Kenneth Stott
# Canary: 9f3a1d72-e4b8-4c21-bf90-7d2e5c8a0f16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Comprehensive unit tests for Hasura v2 migration tooling.

Covers models, parser, mapper, filter conversion, and warning collection
end-to-end across the full conversion pipeline.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pytest
import yaml

from provisa.core.models import GovernanceLevel, ProvisaConfig, SourceType
from provisa.hasura_v2.mapper import (
    _collect_roles,
    _extract_connection_info,
    _map_source,
    _source_type_from_kind,
    convert_metadata,
)
from provisa.hasura_v2.models import (
    HasuraAction,
    HasuraActionDefinition,
    HasuraComputedField,
    HasuraCronTrigger,
    HasuraEventTrigger,
    HasuraFunction,
    HasuraInheritedRole,
    HasuraMetadata,
    HasuraPermission,
    HasuraRelationship,
    HasuraRemoteSchema,
    HasuraSource,
    HasuraTable,
)
from provisa.hasura_v2.parser import (
    _parse_action,
    _parse_computed_field,
    _parse_cron_trigger,
    _parse_event_trigger,
    _parse_inherited_role,
    _parse_permission,
    _parse_relationship,
    _parse_table,
    parse_metadata_dir,
)
from provisa.import_shared.filters import bool_expr_to_sql
from provisa.import_shared.warnings import ImportWarning, WarningCollector


# ---------------------------------------------------------------------------
# TestHasuraModels
# ---------------------------------------------------------------------------


class TestHasuraModels:
    """Verify that the intermediate data models initialise with correct defaults."""

    def test_permission_columns_default_empty(self):
        perm = HasuraPermission(role="analyst")
        assert perm.columns == []

    def test_permission_stores_columns(self):
        perm = HasuraPermission(role="analyst", columns=["id", "name", "email"])
        assert perm.columns == ["id", "name", "email"]

    def test_permission_filter_default_empty(self):
        perm = HasuraPermission(role="user")
        assert perm.filter == {}

    def test_permission_stores_filter_dict(self):
        f = {"user_id": {"_eq": "X-Hasura-User-Id"}}
        perm = HasuraPermission(role="user", filter=f)
        assert perm.filter == f

    def test_permission_allow_aggregations_default_false(self):
        perm = HasuraPermission(role="analyst")
        assert perm.allow_aggregations is False

    def test_permission_allow_aggregations_set_true(self):
        perm = HasuraPermission(role="analyst", allow_aggregations=True)
        assert perm.allow_aggregations is True

    def test_permission_check_default_empty(self):
        perm = HasuraPermission(role="admin")
        assert perm.check == {}

    def test_permission_check_stored(self):
        check = {"status": {"_eq": "pending"}}
        perm = HasuraPermission(role="admin", check=check)
        assert perm.check == check

    def test_relationship_object_type(self):
        rel = HasuraRelationship(
            name="customer",
            rel_type="object",
            remote_table="customers",
            remote_schema="public",
            column_mapping={"customer_id": "id"},
        )
        assert rel.rel_type == "object"
        assert rel.remote_table == "customers"
        assert rel.remote_schema == "public"
        assert rel.column_mapping == {"customer_id": "id"}

    def test_relationship_array_type(self):
        rel = HasuraRelationship(
            name="order_items",
            rel_type="array",
            remote_table="order_items",
            remote_schema="public",
            column_mapping={"id": "order_id"},
        )
        assert rel.rel_type == "array"
        assert rel.column_mapping == {"id": "order_id"}

    def test_relationship_column_mapping_default_empty(self):
        rel = HasuraRelationship(
            name="profile",
            rel_type="object",
            remote_table="profiles",
            remote_schema="public",
        )
        assert rel.column_mapping == {}

    def test_table_defaults(self):
        tbl = HasuraTable(name="orders")
        assert tbl.schema_name == "public"
        assert tbl.custom_name is None
        assert tbl.select_permissions == []
        assert tbl.insert_permissions == []
        assert tbl.update_permissions == []
        assert tbl.delete_permissions == []
        assert tbl.object_relationships == []
        assert tbl.array_relationships == []
        assert tbl.computed_fields == []
        assert tbl.event_triggers == []
        assert tbl.is_enum is False

    def test_table_schema_name_set(self):
        tbl = HasuraTable(name="orders", schema_name="sales")
        assert tbl.schema_name == "sales"

    def test_table_custom_name(self):
        tbl = HasuraTable(name="orders", custom_name="Order")
        assert tbl.custom_name == "Order"

    def test_action_defaults(self):
        action = HasuraAction(name="sendEmail")
        assert action.name == "sendEmail"
        assert action.permissions == []
        assert action.definition.kind == "synchronous"

    def test_action_definition_handler(self):
        defn = HasuraActionDefinition(
            handler="https://api.example.com/send",
            action_type="mutation",
            arguments=[{"name": "to", "type": "String"}],
            output_type="SendResult",
        )
        action = HasuraAction(name="sendEmail", definition=defn)
        assert action.definition.handler == "https://api.example.com/send"
        assert action.definition.output_type == "SendResult"
        assert len(action.definition.arguments) == 1

    def test_action_permissions_list(self):
        action = HasuraAction(
            name="placeOrder",
            permissions=[{"role": "analyst"}, {"role": "manager"}],
        )
        assert len(action.permissions) == 2

    def test_metadata_version_default(self):
        md = HasuraMetadata()
        assert md.version == 3

    def test_metadata_sources_default_empty(self):
        md = HasuraMetadata()
        assert md.sources == []

    def test_metadata_actions_default_empty(self):
        md = HasuraMetadata()
        assert md.actions == []

    def test_metadata_cron_triggers_default_empty(self):
        md = HasuraMetadata()
        assert md.cron_triggers == []

    def test_metadata_inherited_roles_default_empty(self):
        md = HasuraMetadata()
        assert md.inherited_roles == []

    def test_metadata_version_stored(self):
        md = HasuraMetadata(version=2)
        assert md.version == 2


# ---------------------------------------------------------------------------
# TestParsePermission
# ---------------------------------------------------------------------------


class TestParsePermission:
    """Unit tests for the _parse_permission parser function."""

    def test_select_permission_role(self):
        raw = {
            "role": "analyst",
            "permission": {"columns": ["id", "amount"], "filter": {}},
        }
        perm = _parse_permission(raw)
        assert perm.role == "analyst"

    def test_select_permission_columns(self):
        raw = {
            "role": "analyst",
            "permission": {"columns": ["id", "amount", "region"], "filter": {}},
        }
        perm = _parse_permission(raw)
        assert perm.columns == ["id", "amount", "region"]

    def test_select_permission_filter_dict(self):
        raw = {
            "role": "user",
            "permission": {
                "columns": ["id", "name"],
                "filter": {"user_id": {"_eq": "X-Hasura-User-Id"}},
            },
        }
        perm = _parse_permission(raw)
        assert perm.filter == {"user_id": {"_eq": "X-Hasura-User-Id"}}

    def test_insert_permission_check_dict(self):
        raw = {
            "role": "admin",
            "permission": {
                "columns": ["amount", "customer_id"],
                "check": {"status": {"_eq": "pending"}},
            },
        }
        perm = _parse_permission(raw)
        assert perm.check == {"status": {"_eq": "pending"}}

    def test_permission_no_filter_defaults_empty(self):
        raw = {"role": "admin", "permission": {"columns": ["id"]}}
        perm = _parse_permission(raw)
        assert perm.filter == {}

    def test_permission_allow_aggregations_true(self):
        raw = {
            "role": "analyst",
            "permission": {
                "columns": ["id", "amount"],
                "filter": {},
                "allow_aggregations": True,
            },
        }
        perm = _parse_permission(raw)
        assert perm.allow_aggregations is True

    def test_permission_allow_aggregations_default_false(self):
        raw = {
            "role": "analyst",
            "permission": {"columns": ["id"], "filter": {}},
        }
        perm = _parse_permission(raw)
        assert perm.allow_aggregations is False

    def test_permission_empty_columns(self):
        raw = {"role": "readonly", "permission": {"filter": {}}}
        perm = _parse_permission(raw)
        assert perm.columns == []

    def test_permission_wildcard_columns(self):
        raw = {"role": "admin", "permission": {"columns": "*", "filter": {}}}
        perm = _parse_permission(raw)
        assert perm.columns == "*"


# ---------------------------------------------------------------------------
# TestParseRelationship
# ---------------------------------------------------------------------------


class TestParseRelationship:
    """Unit tests for the _parse_relationship parser function."""

    def test_object_relationship_fk_string(self):
        raw = {
            "name": "customer",
            "using": {"foreign_key_constraint_on": "customer_id"},
        }
        rel = _parse_relationship(raw, "object")
        assert rel.name == "customer"
        assert rel.rel_type == "object"
        assert rel.column_mapping == {"customer_id": "id"}

    def test_object_relationship_fk_dict_with_table(self):
        raw = {
            "name": "customer",
            "using": {
                "foreign_key_constraint_on": {
                    "table": {"name": "customers", "schema": "public"},
                    "column": "customer_id",
                },
            },
        }
        rel = _parse_relationship(raw, "object")
        assert rel.remote_table == "customers"
        assert rel.remote_schema == "public"
        assert rel.column_mapping == {"customer_id": "id"}

    def test_array_relationship_fk_dict(self):
        raw = {
            "name": "order_items",
            "using": {
                "foreign_key_constraint_on": {
                    "table": {"name": "order_items", "schema": "public"},
                    "column": "order_id",
                },
            },
        }
        rel = _parse_relationship(raw, "array")
        assert rel.rel_type == "array"
        assert rel.remote_table == "order_items"
        assert rel.column_mapping == {"id": "order_id"}

    def test_manual_configuration_with_column_mapping(self):
        raw = {
            "name": "profile",
            "using": {
                "manual_configuration": {
                    "remote_table": {"name": "profiles", "schema": "public"},
                    "column_mapping": {"id": "user_id"},
                },
            },
        }
        rel = _parse_relationship(raw, "object")
        assert rel.name == "profile"
        assert rel.rel_type == "object"
        assert rel.remote_table == "profiles"
        assert rel.remote_schema == "public"
        assert rel.column_mapping == {"id": "user_id"}

    def test_schema_qualified_remote_table(self):
        raw = {
            "name": "audit_records",
            "using": {
                "foreign_key_constraint_on": {
                    "table": {"name": "audit_log", "schema": "audit"},
                    "column": "entity_id",
                },
            },
        }
        rel = _parse_relationship(raw, "array")
        assert rel.remote_schema == "audit"
        assert rel.remote_table == "audit_log"

    def test_manual_config_string_remote_table(self):
        raw = {
            "name": "company",
            "using": {
                "manual_configuration": {
                    "remote_table": "companies",
                    "column_mapping": {"company_id": "id"},
                },
            },
        }
        rel = _parse_relationship(raw, "array")
        assert rel.name == "company"
        assert rel.rel_type == "array"
        assert rel.remote_table == "companies"
        assert rel.column_mapping == {"company_id": "id"}

    def test_unknown_using_returns_empty_relationship(self):
        raw = {"name": "mystery", "using": {}}
        rel = _parse_relationship(raw, "object")
        assert rel.name == "mystery"
        assert rel.remote_table == ""

    def test_relationship_name_preserved(self):
        raw = {
            "name": "assigned_agent",
            "using": {"foreign_key_constraint_on": "agent_id"},
        }
        rel = _parse_relationship(raw, "object")
        assert rel.name == "assigned_agent"


# ---------------------------------------------------------------------------
# TestParseTable
# ---------------------------------------------------------------------------


class TestParseTable:
    """Unit tests for the _parse_table parser function using inline dicts."""

    def _raw_orders_table(self) -> dict[str, Any]:
        return {
            "table": {"name": "orders", "schema": "public"},
            "select_permissions": [
                {
                    "role": "analyst",
                    "permission": {
                        "columns": ["id", "amount"],
                        "filter": {},
                    },
                }
            ],
            "object_relationships": [
                {
                    "name": "customer",
                    "using": {"foreign_key_constraint_on": "customer_id"},
                }
            ],
            "array_relationships": [
                {
                    "name": "items",
                    "using": {
                        "foreign_key_constraint_on": {
                            "table": {"name": "order_items", "schema": "public"},
                            "column": "order_id",
                        }
                    },
                }
            ],
            "event_triggers": [
                {
                    "name": "on_insert",
                    "definition": {
                        "enable_manual": False,
                        "insert": {"columns": "*"},
                    },
                    "webhook": "http://hook.example.com/notify",
                }
            ],
        }

    def test_table_name_parsed(self):
        tbl = _parse_table(self._raw_orders_table())
        assert tbl.name == "orders"

    def test_table_schema_parsed(self):
        tbl = _parse_table(self._raw_orders_table())
        assert tbl.schema_name == "public"

    def test_select_permissions_parsed(self):
        tbl = _parse_table(self._raw_orders_table())
        assert len(tbl.select_permissions) == 1
        assert tbl.select_permissions[0].role == "analyst"

    def test_object_relationships_parsed(self):
        tbl = _parse_table(self._raw_orders_table())
        assert len(tbl.object_relationships) == 1
        assert tbl.object_relationships[0].name == "customer"

    def test_array_relationships_parsed(self):
        tbl = _parse_table(self._raw_orders_table())
        assert len(tbl.array_relationships) == 1
        assert tbl.array_relationships[0].name == "items"

    def test_event_triggers_parsed(self):
        tbl = _parse_table(self._raw_orders_table())
        assert len(tbl.event_triggers) == 1
        assert tbl.event_triggers[0].name == "on_insert"
        assert tbl.event_triggers[0].webhook == "http://hook.example.com/notify"

    def test_event_trigger_operations_insert(self):
        tbl = _parse_table(self._raw_orders_table())
        assert "insert" in tbl.event_triggers[0].operations

    def test_custom_name_from_configuration(self):
        raw = {
            "table": {"name": "orders", "schema": "public"},
            "configuration": {"custom_name": "Order"},
        }
        tbl = _parse_table(raw)
        assert tbl.custom_name == "Order"

    def test_custom_root_fields_from_configuration(self):
        raw = {
            "table": {"name": "orders", "schema": "public"},
            "configuration": {
                "custom_root_fields": {"select": "allOrders", "select_by_pk": "orderById"},
            },
        }
        tbl = _parse_table(raw)
        assert tbl.custom_root_fields["select"] == "allOrders"

    def test_table_is_enum_flag(self):
        raw = {
            "table": {"name": "status_types", "schema": "public"},
            "is_enum": True,
        }
        tbl = _parse_table(raw)
        assert tbl.is_enum is True

    def test_multiple_permissions_parsed(self):
        raw = {
            "table": {"name": "orders", "schema": "public"},
            "select_permissions": [
                {"role": "analyst", "permission": {"columns": ["id"], "filter": {}}},
                {"role": "admin", "permission": {"columns": ["id", "amount"], "filter": {}}},
            ],
            "insert_permissions": [
                {"role": "admin", "permission": {"columns": ["amount"], "check": {}}},
            ],
        }
        tbl = _parse_table(raw)
        assert len(tbl.select_permissions) == 2
        assert len(tbl.insert_permissions) == 1

    def test_computed_fields_parsed(self):
        raw = {
            "table": {"name": "users", "schema": "public"},
            "computed_fields": [
                {
                    "name": "full_name",
                    "definition": {
                        "function": {"name": "compute_full_name", "schema": "public"},
                    },
                }
            ],
        }
        tbl = _parse_table(raw)
        assert len(tbl.computed_fields) == 1
        assert tbl.computed_fields[0].name == "full_name"
        assert tbl.computed_fields[0].function_name == "compute_full_name"

    def test_table_string_ref_defaults_to_public(self):
        raw = {"table": "legacy_table"}
        tbl = _parse_table(raw)
        assert tbl.name == "legacy_table"
        assert tbl.schema_name == "public"

    def test_delete_permissions_parsed(self):
        raw = {
            "table": {"name": "orders", "schema": "public"},
            "delete_permissions": [
                {"role": "admin", "permission": {}},
            ],
        }
        tbl = _parse_table(raw)
        assert len(tbl.delete_permissions) == 1
        assert tbl.delete_permissions[0].role == "admin"

    def test_update_permissions_parsed(self):
        raw = {
            "table": {"name": "orders", "schema": "public"},
            "update_permissions": [
                {
                    "role": "editor",
                    "permission": {"columns": ["status"], "filter": {}},
                },
            ],
        }
        tbl = _parse_table(raw)
        assert len(tbl.update_permissions) == 1
        assert tbl.update_permissions[0].role == "editor"

    def test_event_trigger_with_multiple_ops(self):
        raw = {
            "table": {"name": "orders", "schema": "public"},
            "event_triggers": [
                {
                    "name": "any_change",
                    "definition": {
                        "insert": {"columns": "*"},
                        "update": {"columns": ["status"]},
                        "delete": {"columns": "*"},
                    },
                    "webhook": "http://hooks.example.com/any",
                }
            ],
        }
        tbl = _parse_table(raw)
        ops = tbl.event_triggers[0].operations
        assert "insert" in ops
        assert "update" in ops
        assert "delete" in ops

    def test_event_trigger_enable_manual(self):
        raw = {
            "table": {"name": "tasks", "schema": "public"},
            "event_triggers": [
                {
                    "name": "manual_trigger",
                    "definition": {"enable_manual": True},
                    "webhook": "http://hooks.example.com/manual",
                }
            ],
        }
        tbl = _parse_table(raw)
        assert "manual" in tbl.event_triggers[0].operations


# ---------------------------------------------------------------------------
# TestParseMetadataDir
# ---------------------------------------------------------------------------


class TestParseMetadataDir:
    """Tests for parse_metadata_dir using tmp_path-based YAML files."""

    def test_minimal_flat_metadata_parses(self, tmp_path: Path):
        metadata_dict = {
            "version": 3,
            "sources": [
                {
                    "name": "default",
                    "kind": "postgres",
                    "tables": [{"table": {"name": "orders", "schema": "public"}}],
                    "configuration": {
                        "connection_info": {
                            "database_url": "postgres://user:pass@db:5432/app"
                        }
                    },
                }
            ],
        }
        tables_yaml = [{"table": {"name": "orders", "schema": "public"}}]
        (tmp_path / "tables.yaml").write_text(yaml.dump(tables_yaml))

        md = parse_metadata_dir(tmp_path)
        assert len(md.sources) == 1
        assert md.sources[0].name == "default"

    def test_tables_from_flat_source_are_parsed(self, tmp_path: Path):
        tables_yaml = [
            {"table": {"name": "orders", "schema": "public"}},
            {"table": {"name": "customers", "schema": "public"}},
        ]
        (tmp_path / "tables.yaml").write_text(yaml.dump(tables_yaml))

        md = parse_metadata_dir(tmp_path)
        table_names = [t.name for t in md.sources[0].tables]
        assert "orders" in table_names
        assert "customers" in table_names

    def test_missing_optional_files_handled_gracefully(self, tmp_path: Path):
        # Only tables.yaml exists; cron, actions, inherited_roles are absent
        (tmp_path / "tables.yaml").write_text("[]")

        collector = WarningCollector()
        md = parse_metadata_dir(tmp_path, collector)
        assert md.actions == []
        assert md.cron_triggers == []
        assert md.inherited_roles == []

    def test_actions_yaml_parsed_as_dict(self, tmp_path: Path):
        actions_data = {
            "actions": [
                {
                    "name": "notify_payment",
                    "definition": {
                        "kind": "synchronous",
                        "handler": "https://payments.example.com/notify",
                        "type": "mutation",
                        "arguments": [],
                        "output_type": "NotifyResult",
                    },
                    "permissions": [{"role": "admin"}],
                }
            ]
        }
        (tmp_path / "tables.yaml").write_text("[]")
        (tmp_path / "actions.yaml").write_text(yaml.dump(actions_data))

        md = parse_metadata_dir(tmp_path)
        assert len(md.actions) == 1
        assert md.actions[0].name == "notify_payment"

    def test_cron_triggers_parsed(self, tmp_path: Path):
        cron_data = [
            {
                "name": "midnight_cleanup",
                "webhook": "https://api.example.com/cleanup",
                "schedule": "0 0 * * *",
                "include_in_metadata": True,
            }
        ]
        (tmp_path / "tables.yaml").write_text("[]")
        (tmp_path / "cron_triggers.yaml").write_text(yaml.dump(cron_data))

        md = parse_metadata_dir(tmp_path)
        assert len(md.cron_triggers) == 1
        assert md.cron_triggers[0].schedule == "0 0 * * *"

    def test_databases_dir_layout_parsed(self, tmp_path: Path):
        db_dir = tmp_path / "databases" / "analytics"
        db_dir.mkdir(parents=True)
        tables_yaml = [{"table": {"name": "events", "schema": "public"}}]
        (db_dir / "tables.yaml").write_text(yaml.dump(tables_yaml))

        md = parse_metadata_dir(tmp_path)
        assert any(s.name == "analytics" for s in md.sources)

    def test_databases_dir_tables_loaded(self, tmp_path: Path):
        db_dir = tmp_path / "databases" / "main"
        db_dir.mkdir(parents=True)
        tables_yaml = [
            {"table": {"name": "products", "schema": "public"}},
            {"table": {"name": "categories", "schema": "public"}},
        ]
        (db_dir / "tables.yaml").write_text(yaml.dump(tables_yaml))

        md = parse_metadata_dir(tmp_path)
        main_src = next(s for s in md.sources if s.name == "main")
        assert len(main_src.tables) == 2

    def test_inherited_roles_parsed(self, tmp_path: Path):
        ir_data = [
            {"role_name": "superadmin", "role_set": ["admin", "analyst"]},
        ]
        (tmp_path / "tables.yaml").write_text("[]")
        (tmp_path / "inherited_roles.yaml").write_text(yaml.dump(ir_data))

        md = parse_metadata_dir(tmp_path)
        assert len(md.inherited_roles) == 1
        assert md.inherited_roles[0].role_name == "superadmin"
        assert "admin" in md.inherited_roles[0].role_set

    def test_remote_schemas_emit_warnings(self, tmp_path: Path):
        rs_data = [
            {"name": "payments_gql", "definition": {"url": "https://pay.example.com/graphql"}},
        ]
        (tmp_path / "tables.yaml").write_text("[]")
        (tmp_path / "remote_schemas.yaml").write_text(yaml.dump(rs_data))

        collector = WarningCollector()
        md = parse_metadata_dir(tmp_path, collector)
        assert len(md.remote_schemas) == 1
        assert collector.has_warnings()
        assert any(w.category == "remote_schemas" for w in collector.warnings)

    def test_tables_with_permissions_in_flat_layout(self, tmp_path: Path):
        tables_yaml = [
            {
                "table": {"name": "orders", "schema": "public"},
                "select_permissions": [
                    {"role": "analyst", "permission": {"columns": ["id", "amount"], "filter": {}}}
                ],
            }
        ]
        (tmp_path / "tables.yaml").write_text(yaml.dump(tables_yaml))

        md = parse_metadata_dir(tmp_path)
        orders = md.sources[0].tables[0]
        assert len(orders.select_permissions) == 1
        assert orders.select_permissions[0].role == "analyst"


# ---------------------------------------------------------------------------
# TestExtractConnectionInfo
# ---------------------------------------------------------------------------


class TestExtractConnectionInfo:
    """Unit tests for _extract_connection_info from the mapper."""

    def test_full_postgres_url(self):
        conn = {"database_url": "postgres://admin:s3cr3t@db.example.com:5432/myapp"}
        result = _extract_connection_info(conn)
        assert result["host"] == "db.example.com"
        assert result["port"] == 5432
        assert result["database"] == "myapp"
        assert result["username"] == "admin"
        assert result["password"] == "s3cr3t"

    def test_url_without_credentials(self):
        conn = {"database_url": "postgres://db.internal:5432/analytics"}
        result = _extract_connection_info(conn)
        assert result["host"] == "db.internal"
        assert result["port"] == 5432
        assert result["database"] == "analytics"
        # Defaults preserved for missing creds
        assert result["username"] == "postgres"

    def test_url_from_env_var_reference(self):
        conn = {"database_url": {"from_env": "DATABASE_URL"}}
        result = _extract_connection_info(conn)
        # Falls back to defaults since env var string is not a parseable URL
        assert result["host"] == "localhost"
        assert result["database"] == "default"

    def test_malformed_url_falls_back_to_defaults(self):
        conn = {"database_url": "not-a-valid-url-at-all"}
        result = _extract_connection_info(conn)
        assert result["host"] == "localhost"
        assert result["port"] == 5432
        assert result["database"] == "default"
        assert result["username"] == "postgres"

    def test_missing_database_url_uses_defaults(self):
        result = _extract_connection_info({})
        assert result["host"] == "localhost"
        assert result["port"] == 5432
        assert result["database"] == "default"

    def test_pool_settings_extracted(self):
        conn = {
            "database_url": "postgres://user:pw@db:5432/app",
            "pool_settings": {"min_connections": 2, "max_connections": 20},
        }
        result = _extract_connection_info(conn)
        assert result["pool_min"] == 2
        assert result["pool_max"] == 20

    def test_pool_settings_defaults(self):
        conn = {"database_url": "postgres://user:pw@db:5432/app"}
        result = _extract_connection_info(conn)
        assert result["pool_min"] == 1
        assert result["pool_max"] == 5

    def test_url_host_only_no_port(self):
        conn = {"database_url": "postgres://user:pw@db.example.com/mydb"}
        result = _extract_connection_info(conn)
        assert result["host"] == "db.example.com"
        assert result["database"] == "mydb"

    def test_password_with_special_chars(self):
        conn = {"database_url": "postgres://svc:p%40ssw0rd@host:5432/db"}
        result = _extract_connection_info(conn)
        # Parser splits on first colon in credentials
        assert result["username"] == "svc"


# ---------------------------------------------------------------------------
# TestMapperSourceType
# ---------------------------------------------------------------------------


class TestMapperSourceType:
    """Unit tests for _source_type_from_kind."""

    def test_postgres_kind(self):
        assert _source_type_from_kind("postgres") == SourceType("postgresql")

    def test_pg_kind_alias(self):
        assert _source_type_from_kind("pg") == SourceType("postgresql")

    def test_mysql_kind(self):
        assert _source_type_from_kind("mysql") == SourceType("mysql")

    def test_mssql_kind(self):
        assert _source_type_from_kind("mssql") == SourceType("sqlserver")

    def test_bigquery_kind(self):
        assert _source_type_from_kind("bigquery") == SourceType("bigquery")

    def test_citus_kind(self):
        assert _source_type_from_kind("citus") == SourceType("postgresql")

    def test_unknown_kind_falls_back_to_postgresql(self):
        assert _source_type_from_kind("exotic_db") == SourceType("postgresql")

    def test_case_insensitive_postgres(self):
        assert _source_type_from_kind("POSTGRES") == SourceType("postgresql")

    def test_case_insensitive_mysql(self):
        assert _source_type_from_kind("MySQL") == SourceType("mysql")


# ---------------------------------------------------------------------------
# TestConvertMetadata
# ---------------------------------------------------------------------------


def _build_full_metadata() -> HasuraMetadata:
    """Build a comprehensive HasuraMetadata fixture for mapper tests."""
    orders_table = HasuraTable(
        name="orders",
        schema_name="public",
        custom_root_fields={"select": "allOrders"},
        select_permissions=[
            HasuraPermission(
                role="analyst",
                columns=["id", "amount", "status", "customer_id"],
                filter={},
            ),
            HasuraPermission(
                role="admin",
                columns=["id", "amount", "status", "customer_id", "internal_notes"],
                filter={},
            ),
            HasuraPermission(
                role="customer",
                columns=["id", "amount", "status"],
                filter={"customer_id": {"_eq": "X-Hasura-User-Id"}},
            ),
        ],
        insert_permissions=[
            HasuraPermission(role="admin", columns=["amount", "status", "customer_id"]),
        ],
        object_relationships=[
            HasuraRelationship(
                name="customer",
                rel_type="object",
                remote_table="customers",
                remote_schema="public",
                column_mapping={"customer_id": "id"},
            ),
        ],
        array_relationships=[
            HasuraRelationship(
                name="order_items",
                rel_type="array",
                remote_table="order_items",
                remote_schema="public",
                column_mapping={"id": "order_id"},
            ),
        ],
        event_triggers=[
            HasuraEventTrigger(
                name="on_order_insert",
                table_name="orders",
                table_schema="public",
                webhook="https://events.example.com/order-created",
                operations=["insert"],
                retry_conf={"num_retries": 5, "interval_sec": 10},
            ),
        ],
    )

    customers_table = HasuraTable(
        name="customers",
        schema_name="public",
        select_permissions=[
            HasuraPermission(
                role="admin",
                columns=["id", "name", "email", "phone"],
                filter={},
            ),
            HasuraPermission(
                role="analyst",
                columns=["id", "name"],
                filter={},
            ),
        ],
    )

    source = HasuraSource(
        name="default",
        kind="postgres",
        connection_info={
            "database_url": "postgres://appuser:secret@pg.internal:5432/commerce",
            "pool_settings": {"min_connections": 2, "max_connections": 15},
        },
        tables=[orders_table, customers_table],
    )

    return HasuraMetadata(
        version=3,
        sources=[source],
        actions=[
            HasuraAction(
                name="notify_payment",
                definition=HasuraActionDefinition(
                    kind="synchronous",
                    handler="https://payments.example.com/notify",
                    action_type="mutation",
                    arguments=[
                        {"name": "order_id", "type": "Int"},
                        {"name": "amount", "type": "Float"},
                    ],
                    output_type="PaymentResult",
                ),
                permissions=[{"role": "admin"}, {"role": "analyst"}],
            ),
        ],
        cron_triggers=[
            HasuraCronTrigger(
                name="daily_report",
                webhook="https://reports.example.com/generate",
                schedule="0 0 * * *",
                include_in_metadata=True,
                enabled=True,
            ),
        ],
        inherited_roles=[
            HasuraInheritedRole(role_name="superadmin", role_set=["admin", "analyst"]),
        ],
    )


class TestConvertMetadata:
    """Core mapper tests using the full metadata fixture."""

    def test_convert_metadata_returns_provisa_config(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert isinstance(config, ProvisaConfig)

    def test_sources_count(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert len(config.sources) == 1

    def test_source_id(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert config.sources[0].id == "default"

    def test_source_type_is_postgresql(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert config.sources[0].type == SourceType.postgresql

    def test_source_host_extracted(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert config.sources[0].host == "pg.internal"

    def test_source_port_extracted(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert config.sources[0].port == 5432

    def test_source_database_extracted(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert config.sources[0].database == "commerce"

    def test_tables_count(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert len(config.tables) == 2

    def test_table_names_present(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        names = {t.table_name for t in config.tables}
        assert "orders" in names
        assert "customers" in names

    def test_roles_collected_from_permissions(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        role_ids = {r.id for r in config.roles}
        assert "analyst" in role_ids
        assert "admin" in role_ids
        assert "customer" in role_ids

    def test_superadmin_inherited_role_present(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        role_ids = {r.id for r in config.roles}
        assert "superadmin" in role_ids

    def test_superadmin_has_parent_role(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        superadmin = next(r for r in config.roles if r.id == "superadmin")
        assert superadmin.parent_role_id is not None

    def test_admin_has_write_capability(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        admin = next(r for r in config.roles if r.id == "admin")
        assert "write" in admin.capabilities

    def test_analyst_has_read_capability(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        analyst = next(r for r in config.roles if r.id == "analyst")
        assert "read" in analyst.capabilities

    def test_rls_rules_generated_for_filtered_permissions(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        customer_rls = [r for r in config.rls_rules if r.role_id == "customer"]
        assert len(customer_rls) >= 1

    def test_rls_filter_references_session_var(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        customer_rls = next(r for r in config.rls_rules if r.role_id == "customer")
        assert "customer_id" in customer_rls.filter

    def test_no_rls_rule_for_empty_filter(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        # analyst and admin have empty filters — no RLS rules for them on orders
        analyst_rls = [
            r for r in config.rls_rules
            if r.role_id == "analyst" and "orders" in r.table_id
        ]
        assert len(analyst_rls) == 0

    def test_object_relationship_converted(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        customer_rel = next(
            (r for r in config.relationships if "customer" in r.id), None
        )
        assert customer_rel is not None
        assert customer_rel.cardinality.value == "many-to-one"

    def test_array_relationship_converted(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        items_rel = next(
            (r for r in config.relationships if "order_items" in r.id), None
        )
        assert items_rel is not None
        assert items_rel.cardinality.value == "one-to-many"

    def test_relationship_source_and_target_table_ids(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        customer_rel = next(r for r in config.relationships if "customer" in r.id)
        assert "orders" in customer_rel.source_table_id
        assert "customers" in customer_rel.target_table_id

    def test_webhook_action_converted(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert len(config.webhooks) == 1
        wh = config.webhooks[0]
        assert wh.name == "notify_payment"
        assert wh.url == "https://payments.example.com/notify"

    def test_webhook_visible_to_roles(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        wh = config.webhooks[0]
        assert "admin" in wh.visible_to
        assert "analyst" in wh.visible_to

    def test_webhook_method_is_post(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert config.webhooks[0].method == "POST"

    def test_webhook_arguments_mapped(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        wh = config.webhooks[0]
        arg_names = {a.name for a in wh.arguments}
        assert "order_id" in arg_names
        assert "amount" in arg_names

    def test_cron_trigger_converted(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert len(config.scheduled_triggers) == 1

    def test_cron_trigger_id(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        st = config.scheduled_triggers[0]
        assert st.id == "daily_report"

    def test_cron_trigger_schedule(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert config.scheduled_triggers[0].cron == "0 0 * * *"

    def test_cron_trigger_url(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert config.scheduled_triggers[0].url == "https://reports.example.com/generate"

    def test_cron_trigger_enabled(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert config.scheduled_triggers[0].enabled is True

    def test_event_triggers_mapped(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert len(config.event_triggers) >= 1

    def test_event_trigger_webhook_url(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        et = config.event_triggers[0]
        assert et.webhook_url == "https://events.example.com/order-created"

    def test_event_trigger_retry_conf(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        et = config.event_triggers[0]
        assert et.retry_max == 5

    def test_event_trigger_emits_warning(self):
        md = _build_full_metadata()
        collector = WarningCollector()
        convert_metadata(md, collector=collector)
        assert any(w.category == "event_triggers" for w in collector.warnings)

    def test_domain_map_applied(self):
        md = _build_full_metadata()
        config = convert_metadata(md, domain_map={"public": "commerce"})
        for tbl in config.tables:
            assert tbl.domain_id == "commerce"

    def test_governance_default_applied(self):
        md = _build_full_metadata()
        config = convert_metadata(
            md, governance_default=GovernanceLevel.registry_required
        )
        for tbl in config.tables:
            assert tbl.governance == GovernanceLevel.registry_required

    def test_source_overrides_applied(self):
        md = _build_full_metadata()
        config = convert_metadata(
            md, source_overrides={"default": {"host": "override.db.internal"}}
        )
        assert config.sources[0].host == "override.db.internal"

    def test_auth_env_keycloak(self):
        md = _build_full_metadata()
        config = convert_metadata(
            md,
            auth_env={
                "AUTH_PROVIDER": "keycloak",
                "KEYCLOAK_URL": "https://auth.example.com",
                "KEYCLOAK_REALM": "commerce",
            },
        )
        assert config.auth.provider == "keycloak"
        assert config.auth.keycloak is not None
        assert config.auth.keycloak["realm"] == "commerce"

    def test_columns_visible_to_correct_roles(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        orders = next(t for t in config.tables if t.table_name == "orders")
        id_col = next(c for c in orders.columns if c.name == "id")
        assert "analyst" in id_col.visible_to
        assert "admin" in id_col.visible_to

    def test_internal_notes_only_visible_to_admin(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        orders = next(t for t in config.tables if t.table_name == "orders")
        notes_col = next(
            (c for c in orders.columns if c.name == "internal_notes"), None
        )
        assert notes_col is not None
        assert "admin" in notes_col.visible_to
        assert "analyst" not in notes_col.visible_to

    def test_table_alias_from_custom_root_fields(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        orders = next(t for t in config.tables if t.table_name == "orders")
        assert orders.alias == "allOrders"

    def test_config_validates_via_pydantic(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        dumped = config.model_dump(by_alias=True)
        validated = ProvisaConfig.model_validate(dumped)
        assert len(validated.tables) == 2
        assert len(validated.sources) == 1

    def test_no_collector_uses_internal_one(self):
        md = _build_full_metadata()
        # Should not raise even without passing a collector
        config = convert_metadata(md)
        assert config is not None

    def test_relay_pagination_false_by_default(self):
        md = _build_full_metadata()
        config = convert_metadata(md)
        assert config.naming.relay_pagination is False

    def test_relay_pagination_enabled(self):
        md = _build_full_metadata()
        md.graphql_engine = {"enable_relay": True}
        config = convert_metadata(md)
        assert config.naming.relay_pagination is True


# ---------------------------------------------------------------------------
# TestBoolExprToSql
# ---------------------------------------------------------------------------


class TestBoolExprToSql:
    """Unit tests for bool_expr_to_sql in import_shared/filters.py."""

    def test_empty_filter_returns_true(self):
        assert bool_expr_to_sql({}) == "TRUE"

    def test_simple_equality(self):
        result = bool_expr_to_sql({"user_id": {"_eq": "X-Hasura-User-Id"}})
        assert "user_id" in result
        assert "=" in result

    def test_equality_with_string_literal(self):
        result = bool_expr_to_sql({"status": {"_eq": "active"}})
        assert result == "status = 'active'"

    def test_equality_with_integer(self):
        result = bool_expr_to_sql({"age": {"_gte": 18}})
        assert "age" in result
        assert "18" in result
        assert ">=" in result

    def test_neq_operator(self):
        result = bool_expr_to_sql({"status": {"_neq": "deleted"}})
        assert "!=" in result
        assert "status" in result

    def test_in_operator(self):
        result = bool_expr_to_sql({"region": {"_in": ["us-east", "us-west"]}})
        assert "IN" in result
        assert "'us-east'" in result
        assert "'us-west'" in result

    def test_not_in_operator(self):
        result = bool_expr_to_sql({"status": {"_nin": ["deleted", "archived"]}})
        assert "NOT IN" in result

    def test_and_combination(self):
        expr = {
            "_and": [
                {"status": {"_eq": "active"}},
                {"deleted_at": {"_is_null": True}},
            ]
        }
        result = bool_expr_to_sql(expr)
        assert "AND" in result
        assert "status = 'active'" in result
        assert "deleted_at IS NULL" in result

    def test_or_combination(self):
        expr = {
            "_or": [
                {"role": {"_eq": "admin"}},
                {"role": {"_eq": "superuser"}},
            ]
        }
        result = bool_expr_to_sql(expr)
        assert "OR" in result
        assert "'admin'" in result
        assert "'superuser'" in result

    def test_empty_filter_returns_true_sentinel(self):
        result = bool_expr_to_sql({})
        assert result == "TRUE"

    def test_nested_boolean_expression(self):
        expr = {
            "_and": [
                {"_or": [
                    {"department": {"_eq": "sales"}},
                    {"department": {"_eq": "marketing"}},
                ]},
                {"active": {"_eq": True}},
            ]
        }
        result = bool_expr_to_sql(expr)
        assert "OR" in result
        assert "AND" in result

    def test_is_null_true(self):
        result = bool_expr_to_sql({"deleted_at": {"_is_null": True}})
        assert result == "deleted_at IS NULL"

    def test_is_null_false(self):
        result = bool_expr_to_sql({"deleted_at": {"_is_null": False}})
        assert result == "deleted_at IS NOT NULL"

    def test_like_operator(self):
        result = bool_expr_to_sql({"name": {"_like": "%john%"}})
        assert "LIKE" in result
        assert "'%john%'" in result

    def test_ilike_operator(self):
        result = bool_expr_to_sql({"email": {"_ilike": "%@example.com"}})
        assert "ILIKE" in result

    def test_table_alias_prefix(self):
        result = bool_expr_to_sql({"id": {"_eq": 42}}, table_alias="o")
        assert result.startswith("o.id")

    def test_hasura_session_var_x_hasura_user_id(self):
        result = bool_expr_to_sql({"user_id": {"_eq": "X-Hasura-User-Id"}})
        # Session variable should be rendered as placeholder, not quoted string
        assert "'" not in result or "${" in result

    def test_gt_operator(self):
        result = bool_expr_to_sql({"amount": {"_gt": 100}})
        assert ">" in result
        assert "100" in result

    def test_lte_operator(self):
        result = bool_expr_to_sql({"score": {"_lte": 50}})
        assert "<=" in result

    def test_exists_expression(self):
        expr = {
            "_exists": {
                "_table": {"schema": "public", "name": "memberships"},
                "_where": {"user_id": {"_eq": 1}},
            }
        }
        result = bool_expr_to_sql(expr)
        assert "EXISTS" in result
        assert "public.memberships" in result

    def test_not_expression(self):
        result = bool_expr_to_sql({"_not": {"status": {"_eq": "banned"}}})
        assert "NOT" in result
        assert "status = 'banned'" in result

    def test_multiple_columns_in_same_filter(self):
        expr = {"status": {"_eq": "active"}, "verified": {"_eq": True}}
        result = bool_expr_to_sql(expr)
        assert "status" in result
        assert "verified" in result

    def test_boolean_true_value(self):
        result = bool_expr_to_sql({"active": {"_eq": True}})
        assert "TRUE" in result

    def test_boolean_false_value(self):
        result = bool_expr_to_sql({"active": {"_eq": False}})
        assert "FALSE" in result


# ---------------------------------------------------------------------------
# TestWarningCollector
# ---------------------------------------------------------------------------


class TestWarningCollector:
    """Unit tests for WarningCollector in import_shared/warnings.py."""

    def test_add_warning_stores_it(self):
        c = WarningCollector()
        c.warn("remote_schemas", "Skipped schema X")
        assert len(c.warnings) == 1

    def test_warning_category_correct(self):
        c = WarningCollector()
        c.warn("event_triggers", "Trigger Y skipped")
        assert c.warnings[0].category == "event_triggers"

    def test_warning_message_correct(self):
        c = WarningCollector()
        c.warn("actions", "Action Z mapped as placeholder")
        assert c.warnings[0].message == "Action Z mapped as placeholder"

    def test_warning_source_path_stored(self):
        c = WarningCollector()
        c.warn("remote_schemas", "Skipped", source_path="remote_schemas.yaml")
        assert c.warnings[0].source_path == "remote_schemas.yaml"

    def test_has_warnings_returns_false_when_empty(self):
        c = WarningCollector()
        assert c.has_warnings() is False

    def test_has_warnings_returns_true_after_add(self):
        c = WarningCollector()
        c.warn("test", "something")
        assert c.has_warnings() is True

    def test_multiple_warnings_accumulated(self):
        c = WarningCollector()
        c.warn("a", "msg1")
        c.warn("b", "msg2")
        c.warn("c", "msg3")
        assert len(c.warnings) == 3

    def test_summary_no_warnings(self):
        c = WarningCollector()
        assert c.summary() == "No warnings."

    def test_summary_with_warnings_includes_count(self):
        c = WarningCollector()
        c.warn("remote_schemas", "Skipped X")
        c.warn("event_triggers", "Skipped Y")
        summary = c.summary()
        assert "2" in summary

    def test_summary_includes_category(self):
        c = WarningCollector()
        c.warn("remote_schemas", "Skipped schema X", "remote_schemas.yaml")
        summary = c.summary()
        assert "remote_schemas" in summary

    def test_summary_includes_message(self):
        c = WarningCollector()
        c.warn("event_triggers", "Trigger on_insert skipped")
        assert "Trigger on_insert skipped" in c.summary()

    def test_summary_includes_source_path(self):
        c = WarningCollector()
        c.warn("remote_schemas", "Skipped", source_path="remote_schemas.yaml")
        assert "remote_schemas.yaml" in c.summary()

    def test_import_warning_dataclass(self):
        w = ImportWarning(category="actions", message="Test msg", source_path="actions.yaml")
        assert w.category == "actions"
        assert w.message == "Test msg"
        assert w.source_path == "actions.yaml"

    def test_import_warning_source_path_default_empty(self):
        w = ImportWarning(category="actions", message="Test msg")
        assert w.source_path == ""

    def test_warnings_independent_across_collectors(self):
        c1 = WarningCollector()
        c2 = WarningCollector()
        c1.warn("a", "only in c1")
        assert len(c2.warnings) == 0

    def test_all_warning_categories_retrievable(self):
        c = WarningCollector()
        categories = ["remote_schemas", "event_triggers", "actions", "functions"]
        for cat in categories:
            c.warn(cat, f"msg for {cat}")
        found_categories = {w.category for w in c.warnings}
        assert found_categories == set(categories)


# ---------------------------------------------------------------------------
# TestParseActionAndCronHelpers
# ---------------------------------------------------------------------------


class TestParseActionAndCronHelpers:
    """Unit tests for _parse_action, _parse_cron_trigger, _parse_inherited_role."""

    def test_parse_action_name(self):
        raw = {
            "name": "createUser",
            "definition": {
                "kind": "synchronous",
                "handler": "https://api.example.com/users",
                "type": "mutation",
            },
            "permissions": [],
        }
        action = _parse_action(raw)
        assert action.name == "createUser"

    def test_parse_action_handler(self):
        raw = {
            "name": "createUser",
            "definition": {"handler": "https://api.example.com/create"},
            "permissions": [],
        }
        action = _parse_action(raw)
        assert action.definition.handler == "https://api.example.com/create"

    def test_parse_action_kind_default(self):
        raw = {"name": "myAction", "definition": {}, "permissions": []}
        action = _parse_action(raw)
        assert action.definition.kind == "synchronous"

    def test_parse_action_permissions(self):
        raw = {
            "name": "myAction",
            "definition": {},
            "permissions": [{"role": "admin"}, {"role": "manager"}],
        }
        action = _parse_action(raw)
        assert len(action.permissions) == 2

    def test_parse_action_arguments(self):
        raw = {
            "name": "submitOrder",
            "definition": {
                "arguments": [
                    {"name": "product_id", "type": "Int"},
                    {"name": "qty", "type": "Int"},
                ]
            },
            "permissions": [],
        }
        action = _parse_action(raw)
        assert len(action.definition.arguments) == 2

    def test_parse_cron_trigger_name(self):
        raw = {
            "name": "weekly_report",
            "webhook": "https://reports.example.com/weekly",
            "schedule": "0 9 * * 1",
        }
        ct = _parse_cron_trigger(raw)
        assert ct.name == "weekly_report"

    def test_parse_cron_trigger_schedule(self):
        raw = {
            "name": "hourly",
            "webhook": "https://api.example.com/hourly",
            "schedule": "0 * * * *",
        }
        ct = _parse_cron_trigger(raw)
        assert ct.schedule == "0 * * * *"

    def test_parse_cron_trigger_include_in_metadata(self):
        raw = {
            "name": "test",
            "webhook": "http://example.com",
            "schedule": "* * * * *",
            "include_in_metadata": False,
        }
        ct = _parse_cron_trigger(raw)
        assert ct.include_in_metadata is False

    def test_parse_inherited_role_name(self):
        raw = {"role_name": "manager", "role_set": ["editor", "viewer"]}
        ir = _parse_inherited_role(raw)
        assert ir.role_name == "manager"

    def test_parse_inherited_role_set(self):
        raw = {"role_name": "superadmin", "role_set": ["admin", "analyst"]}
        ir = _parse_inherited_role(raw)
        assert "admin" in ir.role_set
        assert "analyst" in ir.role_set

    def test_parse_computed_field_name(self):
        raw = {
            "name": "full_name",
            "definition": {
                "function": {"name": "compute_full_name", "schema": "public"}
            },
        }
        cf = _parse_computed_field(raw)
        assert cf.name == "full_name"
        assert cf.function_name == "compute_full_name"
        assert cf.function_schema == "public"

    def test_parse_computed_field_function_string(self):
        raw = {
            "name": "label",
            "definition": {"function": "generate_label"},
        }
        cf = _parse_computed_field(raw)
        assert cf.function_name == "generate_label"
        assert cf.function_schema == "public"

    def test_parse_event_trigger_name(self):
        et = _parse_event_trigger(
            {
                "name": "on_create",
                "definition": {"insert": {"columns": "*"}},
                "webhook": "http://hooks.example.com",
            },
            table_name="orders",
            table_schema="public",
        )
        assert et.name == "on_create"
        assert et.table_name == "orders"
        assert et.table_schema == "public"

    def test_parse_event_trigger_retry_conf(self):
        et = _parse_event_trigger(
            {
                "name": "retry_test",
                "definition": {"insert": {"columns": "*"}},
                "webhook": "http://hooks.example.com",
                "retry_conf": {"num_retries": 7, "interval_sec": 30},
            },
            table_name="orders",
            table_schema="public",
        )
        assert et.retry_conf == {"num_retries": 7, "interval_sec": 30}


# ---------------------------------------------------------------------------
# TestCollectRoles
# ---------------------------------------------------------------------------


class TestCollectRoles:
    """Unit tests for _collect_roles in the mapper."""

    def test_roles_from_select_permissions(self):
        tbl = HasuraTable(
            name="orders",
            schema_name="public",
            select_permissions=[
                HasuraPermission(role="analyst", columns=["id"]),
                HasuraPermission(role="admin", columns=["id"]),
            ],
        )
        md = HasuraMetadata(
            sources=[HasuraSource(name="default", kind="postgres", tables=[tbl])]
        )
        roles = _collect_roles(md)
        assert "analyst" in roles
        assert "admin" in roles

    def test_roles_from_action_permissions(self):
        action = HasuraAction(
            name="doThing",
            permissions=[{"role": "service_account"}],
        )
        md = HasuraMetadata(
            sources=[HasuraSource(name="default", kind="postgres")],
            actions=[action],
        )
        roles = _collect_roles(md)
        assert "service_account" in roles

    def test_inherited_roles_included(self):
        md = HasuraMetadata(
            sources=[HasuraSource(name="default", kind="postgres")],
            inherited_roles=[
                HasuraInheritedRole(role_name="superadmin", role_set=["admin", "analyst"]),
            ],
        )
        roles = _collect_roles(md)
        assert "superadmin" in roles

    def test_inherited_role_parent_set(self):
        md = HasuraMetadata(
            sources=[HasuraSource(name="default", kind="postgres")],
            inherited_roles=[
                HasuraInheritedRole(role_name="superadmin", role_set=["admin"]),
            ],
        )
        roles = _collect_roles(md)
        assert roles["superadmin"].parent_role_id == "admin"

    def test_write_capability_added_for_insert_permissions(self):
        tbl = HasuraTable(
            name="orders",
            schema_name="public",
            select_permissions=[HasuraPermission(role="editor", columns=["id"])],
            insert_permissions=[HasuraPermission(role="editor", columns=["amount"])],
        )
        md = HasuraMetadata(
            sources=[HasuraSource(name="default", kind="postgres", tables=[tbl])]
        )
        roles = _collect_roles(md)
        assert "write" in roles["editor"].capabilities

    def test_no_duplicate_roles(self):
        tbl = HasuraTable(
            name="orders",
            schema_name="public",
            select_permissions=[HasuraPermission(role="analyst", columns=["id"])],
        )
        tbl2 = HasuraTable(
            name="customers",
            schema_name="public",
            select_permissions=[HasuraPermission(role="analyst", columns=["name"])],
        )
        md = HasuraMetadata(
            sources=[HasuraSource(name="default", kind="postgres", tables=[tbl, tbl2])]
        )
        roles = _collect_roles(md)
        # Should have exactly one entry for analyst regardless of how many tables reference it
        assert list(roles.keys()).count("analyst") == 1
