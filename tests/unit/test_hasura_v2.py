# Copyright (c) 2026 Kenneth Stott
# Canary: b0725c4b-d6ce-4fd3-9b36-39e1e795a030
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Hasura v2 metadata converter."""

import textwrap
from pathlib import Path

import pytest
import yaml

from provisa.core.models import ProvisaConfig
from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.models import (
    HasuraAction,
    HasuraActionDefinition,
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
from provisa.hasura_v2.parser import parse_metadata_dir
from provisa.import_shared.filters import bool_expr_to_sql
from provisa.import_shared.warnings import WarningCollector


# ---------------------------------------------------------------------------
# Filter conversion tests
# ---------------------------------------------------------------------------


class TestBoolExprToSQL:
    def test_empty_expr(self):
        assert bool_expr_to_sql({}) == "TRUE"

    def test_simple_eq(self):
        result = bool_expr_to_sql({"status": {"_eq": "active"}})
        assert result == "status = 'active'"

    def test_numeric_comparison(self):
        result = bool_expr_to_sql({"age": {"_gte": 18}})
        assert result == "age >= 18"

    def test_and_expression(self):
        expr = {"_and": [
            {"status": {"_eq": "active"}},
            {"age": {"_gte": 18}},
        ]}
        result = bool_expr_to_sql(expr)
        assert "status = 'active'" in result
        assert "age >= 18" in result
        assert "AND" in result

    def test_or_expression(self):
        expr = {"_or": [
            {"role": {"_eq": "admin"}},
            {"role": {"_eq": "editor"}},
        ]}
        result = bool_expr_to_sql(expr)
        assert "role = 'admin'" in result
        assert "role = 'editor'" in result
        assert "OR" in result

    def test_not_expression(self):
        expr = {"_not": {"status": {"_eq": "deleted"}}}
        result = bool_expr_to_sql(expr)
        assert "NOT" in result
        assert "status = 'deleted'" in result

    def test_is_null(self):
        result = bool_expr_to_sql({"deleted_at": {"_is_null": True}})
        assert result == "deleted_at IS NULL"

    def test_is_not_null(self):
        result = bool_expr_to_sql({"deleted_at": {"_is_null": False}})
        assert result == "deleted_at IS NOT NULL"

    def test_in_operator(self):
        result = bool_expr_to_sql({"status": {"_in": ["active", "pending"]}})
        assert "IN" in result
        assert "'active'" in result
        assert "'pending'" in result

    def test_like_operator(self):
        result = bool_expr_to_sql({"name": {"_like": "%test%"}})
        assert result == "name LIKE '%test%'"

    def test_table_alias(self):
        result = bool_expr_to_sql({"id": {"_eq": 1}}, table_alias="t")
        assert result == "t.id = 1"

    def test_nested_and_or(self):
        expr = {"_and": [
            {"_or": [
                {"role": {"_eq": "admin"}},
                {"role": {"_eq": "editor"}},
            ]},
            {"active": {"_eq": True}},
        ]}
        result = bool_expr_to_sql(expr)
        assert "OR" in result
        assert "AND" in result

    def test_exists_expression(self):
        expr = {"_exists": {
            "_table": {"schema": "public", "name": "users"},
            "_where": {"id": {"_eq": 1}},
        }}
        result = bool_expr_to_sql(expr)
        assert "EXISTS" in result
        assert "public.users" in result


# ---------------------------------------------------------------------------
# Warning collector tests
# ---------------------------------------------------------------------------


class TestWarningCollector:
    def test_empty(self):
        c = WarningCollector()
        assert not c.has_warnings()
        assert c.summary() == "No warnings."

    def test_add_warning(self):
        c = WarningCollector()
        c.warn("remote_schemas", "Skipped schema X", "remote_schemas.yaml")
        assert c.has_warnings()
        assert len(c.warnings) == 1
        assert "remote_schemas" in c.summary()
        assert "Skipped schema X" in c.summary()

    def test_multiple_warnings(self):
        c = WarningCollector()
        c.warn("a", "msg1")
        c.warn("b", "msg2")
        assert len(c.warnings) == 2
        assert "2" in c.summary()


# ---------------------------------------------------------------------------
# Parser tests (filesystem-based)
# ---------------------------------------------------------------------------


class TestParser:
    def test_parse_flat_layout(self, tmp_path: Path):
        tables_yaml = [
            {
                "table": {"name": "users", "schema": "public"},
                "select_permissions": [
                    {
                        "role": "user",
                        "permission": {
                            "columns": ["id", "name", "email"],
                            "filter": {"id": {"_eq": "x-hasura-user-id"}},
                        },
                    },
                ],
                "object_relationships": [
                    {
                        "name": "profile",
                        "using": {
                            "manual_configuration": {
                                "remote_table": {"name": "profiles", "schema": "public"},
                                "column_mapping": {"id": "user_id"},
                            },
                        },
                    },
                ],
                "array_relationships": [
                    {
                        "name": "orders",
                        "using": {
                            "foreign_key_constraint_on": {
                                "table": {"name": "orders", "schema": "public"},
                                "column": "user_id",
                            },
                        },
                    },
                ],
            },
            {
                "table": {"name": "orders", "schema": "public"},
                "select_permissions": [
                    {
                        "role": "user",
                        "permission": {
                            "columns": ["id", "amount", "user_id"],
                            "filter": {},
                        },
                    },
                ],
            },
        ]
        (tmp_path / "tables.yaml").write_text(yaml.dump(tables_yaml))

        collector = WarningCollector()
        metadata = parse_metadata_dir(tmp_path, collector)

        assert len(metadata.sources) == 1
        assert metadata.sources[0].name == "default"
        assert len(metadata.sources[0].tables) == 2

        users = metadata.sources[0].tables[0]
        assert users.name == "users"
        assert len(users.select_permissions) == 1
        assert users.select_permissions[0].role == "user"
        assert len(users.object_relationships) == 1
        assert len(users.array_relationships) == 1

    def test_parse_databases_layout(self, tmp_path: Path):
        db_dir = tmp_path / "databases" / "mydb"
        db_dir.mkdir(parents=True)

        tables_yaml = [
            {"table": {"name": "products", "schema": "public"}},
        ]
        (db_dir / "tables.yaml").write_text(yaml.dump(tables_yaml))

        metadata = parse_metadata_dir(tmp_path)
        assert len(metadata.sources) == 1
        assert metadata.sources[0].name == "mydb"
        assert len(metadata.sources[0].tables) == 1

    def test_parse_actions(self, tmp_path: Path):
        actions_yaml = {
            "actions": [
                {
                    "name": "createUser",
                    "definition": {
                        "kind": "synchronous",
                        "handler": "https://api.example.com/create-user",
                        "type": "mutation",
                        "arguments": [
                            {"name": "name", "type": "String"},
                        ],
                        "output_type": "UserOutput",
                    },
                    "permissions": [{"role": "admin"}],
                },
            ],
        }
        (tmp_path / "actions.yaml").write_text(yaml.dump(actions_yaml))
        (tmp_path / "tables.yaml").write_text("[]")

        metadata = parse_metadata_dir(tmp_path)
        assert len(metadata.actions) == 1
        assert metadata.actions[0].name == "createUser"
        assert metadata.actions[0].definition.handler == "https://api.example.com/create-user"

    def test_parse_cron_triggers(self, tmp_path: Path):
        cron_yaml = [
            {
                "name": "daily_cleanup",
                "webhook": "https://api.example.com/cleanup",
                "schedule": "0 0 * * *",
            },
        ]
        (tmp_path / "cron_triggers.yaml").write_text(yaml.dump(cron_yaml))
        (tmp_path / "tables.yaml").write_text("[]")

        metadata = parse_metadata_dir(tmp_path)
        assert len(metadata.cron_triggers) == 1
        assert metadata.cron_triggers[0].schedule == "0 0 * * *"

    def test_parse_remote_schemas_emits_warning(self, tmp_path: Path):
        rs_yaml = [
            {"name": "my_remote", "definition": {"url": "https://remote.example.com"}},
        ]
        (tmp_path / "remote_schemas.yaml").write_text(yaml.dump(rs_yaml))
        (tmp_path / "tables.yaml").write_text("[]")

        collector = WarningCollector()
        metadata = parse_metadata_dir(tmp_path, collector)
        assert len(metadata.remote_schemas) == 1
        assert collector.has_warnings()
        assert any(w.category == "remote_schemas" for w in collector.warnings)

    def test_parse_inherited_roles(self, tmp_path: Path):
        ir_yaml = [
            {"role_name": "manager", "role_set": ["user", "editor"]},
        ]
        (tmp_path / "inherited_roles.yaml").write_text(yaml.dump(ir_yaml))
        (tmp_path / "tables.yaml").write_text("[]")

        metadata = parse_metadata_dir(tmp_path)
        assert len(metadata.inherited_roles) == 1
        assert metadata.inherited_roles[0].role_name == "manager"
        assert "user" in metadata.inherited_roles[0].role_set

    def test_parse_functions(self, tmp_path: Path):
        functions_yaml = [
            {"function": {"name": "search_products", "schema": "public"}},
        ]
        (tmp_path / "functions.yaml").write_text(yaml.dump(functions_yaml))
        (tmp_path / "tables.yaml").write_text("[]")

        metadata = parse_metadata_dir(tmp_path)
        assert len(metadata.sources[0].functions) == 1
        assert metadata.sources[0].functions[0].name == "search_products"

    def test_parse_event_triggers(self, tmp_path: Path):
        tables_yaml = [
            {
                "table": {"name": "orders", "schema": "public"},
                "event_triggers": [
                    {
                        "name": "order_created",
                        "definition": {"insert": {"columns": "*"}},
                        "webhook": "https://hooks.example.com/order",
                        "retry_conf": {"num_retries": 5, "interval_sec": 10},
                    },
                ],
            },
        ]
        (tmp_path / "tables.yaml").write_text(yaml.dump(tables_yaml))

        collector = WarningCollector()
        metadata = parse_metadata_dir(tmp_path, collector)
        assert len(metadata.sources[0].tables[0].event_triggers) == 1
        et = metadata.sources[0].tables[0].event_triggers[0]
        assert et.name == "order_created"
        assert "insert" in et.operations

    def test_parse_computed_fields(self, tmp_path: Path):
        tables_yaml = [
            {
                "table": {"name": "products", "schema": "public"},
                "computed_fields": [
                    {
                        "name": "full_name",
                        "definition": {
                            "function": {"name": "compute_full_name", "schema": "public"},
                        },
                    },
                ],
            },
        ]
        (tmp_path / "tables.yaml").write_text(yaml.dump(tables_yaml))

        metadata = parse_metadata_dir(tmp_path)
        assert len(metadata.sources[0].tables[0].computed_fields) == 1
        assert metadata.sources[0].tables[0].computed_fields[0].function_name == "compute_full_name"


# ---------------------------------------------------------------------------
# Mapper tests
# ---------------------------------------------------------------------------


class TestMapper:
    def _build_metadata(self) -> HasuraMetadata:
        """Build a representative Hasura metadata for testing."""
        users_table = HasuraTable(
            name="users",
            schema_name="public",
            custom_column_names={"first_name": "firstName"},
            custom_root_fields={"select": "allUsers"},
            select_permissions=[
                HasuraPermission(
                    role="user",
                    columns=["id", "first_name", "email"],
                    filter={"id": {"_eq": "x-hasura-user-id"}},
                    allow_aggregations=True,
                ),
                HasuraPermission(
                    role="admin",
                    columns=["id", "first_name", "email", "role"],
                    filter={},
                ),
            ],
            insert_permissions=[
                HasuraPermission(role="admin", columns=["first_name", "email", "role"]),
            ],
            update_permissions=[
                HasuraPermission(role="user", columns=["first_name", "email"]),
            ],
            object_relationships=[
                HasuraRelationship(
                    name="profile",
                    rel_type="object",
                    remote_table="profiles",
                    remote_schema="public",
                    column_mapping={"id": "user_id"},
                ),
            ],
            array_relationships=[
                HasuraRelationship(
                    name="orders",
                    rel_type="array",
                    remote_table="orders",
                    remote_schema="public",
                    column_mapping={"id": "user_id"},
                ),
            ],
        )

        orders_table = HasuraTable(
            name="orders",
            schema_name="public",
            select_permissions=[
                HasuraPermission(
                    role="user",
                    columns=["id", "amount", "user_id"],
                    filter={"user_id": {"_eq": "x-hasura-user-id"}},
                ),
            ],
            event_triggers=[
                HasuraEventTrigger(
                    name="order_notify",
                    table_name="orders",
                    table_schema="public",
                    webhook="https://hooks.example.com/order",
                    operations=["insert"],
                    retry_conf={"num_retries": 3, "interval_sec": 5},
                ),
            ],
        )

        source = HasuraSource(
            name="default",
            kind="postgres",
            connection_info={
                "database_url": "postgres://admin:secret@db.example.com:5432/myapp",
            },
            tables=[users_table, orders_table],
            functions=[
                HasuraFunction(name="search_users", schema_name="public"),
            ],
        )

        return HasuraMetadata(
            version=3,
            sources=[source],
            actions=[
                HasuraAction(
                    name="sendEmail",
                    definition=HasuraActionDefinition(
                        handler="https://api.example.com/send-email",
                        arguments=[{"name": "to", "type": "String"}],
                    ),
                    permissions=[{"role": "admin"}],
                ),
            ],
            cron_triggers=[
                HasuraCronTrigger(
                    name="hourly_sync",
                    webhook="https://api.example.com/sync",
                    schedule="0 * * * *",
                ),
            ],
            inherited_roles=[
                HasuraInheritedRole(role_name="manager", role_set=["user", "admin"]),
            ],
            remote_schemas=[
                HasuraRemoteSchema(name="payments", definition={"url": "https://pay.example.com"}),
            ],
        )

    def test_convert_produces_valid_config(self):
        metadata = self._build_metadata()
        collector = WarningCollector()
        config = convert_metadata(metadata, collector=collector)

        # Validate via Pydantic
        dumped = config.model_dump(by_alias=True)
        validated = ProvisaConfig.model_validate(dumped)
        assert len(validated.sources) == 1
        assert len(validated.tables) == 2

    def test_source_mapping(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        src = config.sources[0]
        assert src.id == "default"
        assert src.type.value == "postgresql"
        assert src.host == "db.example.com"
        assert src.port == 5432
        assert src.database == "myapp"
        assert src.username == "admin"

    def test_roles_collected(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        role_ids = {r.id for r in config.roles}
        assert "user" in role_ids
        assert "admin" in role_ids
        assert "manager" in role_ids

    def test_inherited_role_has_parent(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        manager = next(r for r in config.roles if r.id == "manager")
        assert manager.parent_role_id is not None

    def test_write_capabilities_upgraded(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        admin = next(r for r in config.roles if r.id == "admin")
        assert "write" in admin.capabilities

    def test_column_visible_to(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        users = next(t for t in config.tables if t.table_name == "users")
        id_col = next(c for c in users.columns if c.name == "id")
        assert "user" in id_col.visible_to
        assert "admin" in id_col.visible_to

    def test_column_writable_by(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        users = next(t for t in config.tables if t.table_name == "users")
        email_col = next(c for c in users.columns if c.name == "email")
        assert "admin" in email_col.writable_by  # insert permission
        assert "user" in email_col.writable_by  # update permission

    def test_custom_column_alias(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        users = next(t for t in config.tables if t.table_name == "users")
        fn_col = next(c for c in users.columns if c.name == "first_name")
        assert fn_col.alias == "firstName"

    def test_table_alias_from_custom_root_fields(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        users = next(t for t in config.tables if t.table_name == "users")
        assert users.alias == "allUsers"

    def test_rls_rules_generated(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        user_rls = [r for r in config.rls_rules if r.role_id == "user"]
        assert len(user_rls) >= 1
        # Users table has filter for user role
        users_rls = [r for r in user_rls if "users" in r.table_id]
        assert len(users_rls) == 1

    def test_object_relationship_many_to_one(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        profile_rel = next(
            (r for r in config.relationships if "profile" in r.id), None
        )
        assert profile_rel is not None
        assert profile_rel.cardinality.value == "many-to-one"

    def test_array_relationship_one_to_many(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        orders_rel = next(
            (r for r in config.relationships if "orders" in r.id), None
        )
        assert orders_rel is not None
        assert orders_rel.cardinality.value == "one-to-many"

    def test_tracked_functions_mapped(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        fn_names = {f.name for f in config.functions}
        assert "search_users" in fn_names

    def test_webhook_action_mapped(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        assert len(config.webhooks) == 1
        assert config.webhooks[0].name == "sendEmail"
        assert config.webhooks[0].url == "https://api.example.com/send-email"
        assert "admin" in config.webhooks[0].visible_to

    def test_cron_triggers_mapped(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        assert len(config.scheduled_triggers) == 1
        assert config.scheduled_triggers[0].id == "hourly_sync"
        assert config.scheduled_triggers[0].cron == "0 * * * *"

    def test_event_triggers_mapped_with_warning(self):
        metadata = self._build_metadata()
        collector = WarningCollector()
        config = convert_metadata(metadata, collector=collector)
        assert len(config.event_triggers) == 1
        et = config.event_triggers[0]
        assert et.webhook_url == "https://hooks.example.com/order"
        assert et.retry_max == 3
        assert any(w.category == "event_triggers" for w in collector.warnings)

    def test_domain_map_applied(self):
        metadata = self._build_metadata()
        config = convert_metadata(
            metadata, domain_map={"public": "core"},
        )
        for t in config.tables:
            assert t.domain_id == "core"

    def test_governance_default(self):
        from provisa.core.models import GovernanceLevel
        metadata = self._build_metadata()
        config = convert_metadata(
            metadata, governance_default=GovernanceLevel.registry_required,
        )
        for t in config.tables:
            assert t.governance == GovernanceLevel.registry_required

    def test_auth_env_firebase(self):
        metadata = self._build_metadata()
        config = convert_metadata(
            metadata,
            auth_env={
                "AUTH_PROVIDER": "firebase",
                "FIREBASE_PROJECT_ID": "my-project",
            },
        )
        assert config.auth.provider == "firebase"
        assert config.auth.firebase is not None
        assert config.auth.firebase["project_id"] == "my-project"

    def test_relay_pagination_defaults_false(self):
        metadata = self._build_metadata()
        config = convert_metadata(metadata)
        assert config.naming.relay_pagination is False

    def test_relay_pagination_enabled_via_graphql_engine(self):
        metadata = self._build_metadata()
        metadata.graphql_engine = {"enable_relay": True}
        config = convert_metadata(metadata)
        assert config.naming.relay_pagination is True

    def test_relay_pagination_disabled_when_graphql_engine_false(self):
        metadata = self._build_metadata()
        metadata.graphql_engine = {"enable_relay": False}
        config = convert_metadata(metadata)
        assert config.naming.relay_pagination is False

    def test_relay_pagination_absent_graphql_engine_key_defaults_false(self):
        metadata = self._build_metadata()
        metadata.graphql_engine = {"some_other_key": "value"}
        config = convert_metadata(metadata)
        assert config.naming.relay_pagination is False


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------


class TestCLI:
    def test_dry_run(self, tmp_path: Path):
        tables_yaml = [
            {
                "table": {"name": "items", "schema": "public"},
                "select_permissions": [
                    {
                        "role": "viewer",
                        "permission": {"columns": ["id", "name"], "filter": {}},
                    },
                ],
            },
        ]
        (tmp_path / "tables.yaml").write_text(yaml.dump(tables_yaml))

        from provisa.hasura_v2.cli import main
        ret = main([str(tmp_path), "--dry-run"])
        assert ret == 0

    def test_output_file(self, tmp_path: Path):
        tables_yaml = [
            {
                "table": {"name": "items", "schema": "public"},
                "select_permissions": [
                    {
                        "role": "viewer",
                        "permission": {"columns": ["id", "name"], "filter": {}},
                    },
                ],
            },
        ]
        (tmp_path / "tables.yaml").write_text(yaml.dump(tables_yaml))

        out_file = tmp_path / "output.yaml"
        from provisa.hasura_v2.cli import main
        ret = main([str(tmp_path), "-o", str(out_file)])
        assert ret == 0
        assert out_file.exists()

        # Validate output is valid YAML and valid ProvisaConfig
        data = yaml.safe_load(out_file.read_text())
        assert "sources" in data
        assert "tables" in data

    def test_missing_dir_returns_error(self, tmp_path: Path):
        from provisa.hasura_v2.cli import main
        ret = main([str(tmp_path / "nonexistent")])
        assert ret == 1
