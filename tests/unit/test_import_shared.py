# Copyright (c) 2026 Kenneth Stott
# Canary: ef5662e5-933a-4c96-8d58-d6fd70c6da1b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for import_shared — shared converter utilities used by Hasura v2
and DDN import pipelines."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.import_shared.filters import (
    _OPERATORS,
    _format_value,
    bool_expr_to_sql,
)
from provisa.import_shared.warnings import (
    ImportWarning,
    WarningCollector,
)


# ===========================================================================
# TestImportWarning
# ===========================================================================


class TestImportWarning:
    def test_fields_stored(self):
        w = ImportWarning(
            category="remote_schemas",
            message="Skipped schema X",
            source_path="remote_schemas.yaml",
        )
        assert w.category == "remote_schemas"
        assert w.message == "Skipped schema X"
        assert w.source_path == "remote_schemas.yaml"

    def test_default_source_path_empty(self):
        w = ImportWarning(category="actions", message="Unsupported action")
        assert w.source_path == ""

    def test_multiple_warnings_independent(self):
        w1 = ImportWarning(category="a", message="msg1")
        w2 = ImportWarning(category="b", message="msg2")
        assert w1.category != w2.category
        assert w1.message != w2.message


# ===========================================================================
# TestWarningCollector
# ===========================================================================


class TestWarningCollector:
    def test_starts_empty(self):
        c = WarningCollector()
        assert not c.has_warnings()
        assert c.warnings == []

    def test_summary_empty(self):
        c = WarningCollector()
        assert c.summary() == "No warnings."

    def test_add_single_warning(self):
        c = WarningCollector()
        c.warn("remote_schemas", "Schema 'payments' skipped", "remote_schemas.yaml")
        assert c.has_warnings()
        assert len(c.warnings) == 1
        w = c.warnings[0]
        assert w.category == "remote_schemas"
        assert "payments" in w.message
        assert w.source_path == "remote_schemas.yaml"

    def test_summary_includes_category_and_message(self):
        c = WarningCollector()
        c.warn("event_triggers", "Trigger 'on_insert' has limited fidelity")
        summary = c.summary()
        assert "event_triggers" in summary
        assert "on_insert" in summary

    def test_summary_includes_count(self):
        c = WarningCollector()
        c.warn("a", "msg1")
        c.warn("b", "msg2")
        c.warn("c", "msg3")
        summary = c.summary()
        assert "3" in summary

    def test_summary_includes_source_path_when_present(self):
        c = WarningCollector()
        c.warn("remote_schemas", "skipped", source_path="remote_schemas.yaml")
        assert "remote_schemas.yaml" in c.summary()

    def test_summary_no_source_path_no_brackets(self):
        c = WarningCollector()
        c.warn("actions", "some message")
        # Without source_path the bracketed path should not appear
        assert "[]" not in c.summary()

    def test_multiple_warnings_accumulated(self):
        c = WarningCollector()
        for i in range(5):
            c.warn(f"category_{i}", f"message_{i}")
        assert len(c.warnings) == 5

    def test_has_warnings_false_when_empty(self):
        c = WarningCollector()
        assert c.has_warnings() is False

    def test_has_warnings_true_after_warn(self):
        c = WarningCollector()
        c.warn("x", "y")
        assert c.has_warnings() is True

    def test_warn_without_source_path(self):
        c = WarningCollector()
        c.warn("category", "message")
        assert c.warnings[0].source_path == ""

    def test_warnings_are_import_warning_instances(self):
        c = WarningCollector()
        c.warn("cat", "msg", "path.yaml")
        assert isinstance(c.warnings[0], ImportWarning)

    def test_independent_collector_instances(self):
        c1 = WarningCollector()
        c2 = WarningCollector()
        c1.warn("a", "msg1")
        assert len(c1.warnings) == 1
        assert len(c2.warnings) == 0


# ===========================================================================
# TestFormatValue
# ===========================================================================


class TestFormatValue:
    def test_string_literal(self):
        assert _format_value("hello") == "'hello'"

    def test_integer(self):
        assert _format_value(42) == "42"

    def test_float(self):
        result = _format_value(3.14)
        assert "3.14" in result

    def test_boolean_true(self):
        assert _format_value(True) == "TRUE"

    def test_boolean_false(self):
        assert _format_value(False) == "FALSE"

    def test_none(self):
        assert _format_value(None) == "NULL"

    def test_list_of_strings(self):
        result = _format_value(["a", "b"])
        assert "'a'" in result
        assert "'b'" in result
        assert result.startswith("(")
        assert result.endswith(")")

    def test_list_of_integers(self):
        result = _format_value([1, 2, 3])
        assert "1" in result
        assert "2" in result
        assert "3" in result

    def test_hasura_session_var_lowercase(self):
        result = _format_value("x-hasura-user-id")
        assert result == "${x-hasura-user-id}"

    def test_hasura_session_var_uppercase_prefix(self):
        result = _format_value("X-Hasura-Role")
        assert result.startswith("${")
        assert "Hasura" in result

    def test_zero(self):
        assert _format_value(0) == "0"

    def test_empty_list(self):
        result = _format_value([])
        assert result == "()"


# ===========================================================================
# TestOperatorMap
# ===========================================================================


class TestOperatorMap:
    def test_eq_operator(self):
        assert _OPERATORS["_eq"] == "="

    def test_neq_operator(self):
        assert _OPERATORS["_neq"] == "!="

    def test_gt_operator(self):
        assert _OPERATORS["_gt"] == ">"

    def test_lt_operator(self):
        assert _OPERATORS["_lt"] == "<"

    def test_gte_operator(self):
        assert _OPERATORS["_gte"] == ">="

    def test_lte_operator(self):
        assert _OPERATORS["_lte"] == "<="

    def test_like_operator(self):
        assert _OPERATORS["_like"] == "LIKE"

    def test_nlike_operator(self):
        assert _OPERATORS["_nlike"] == "NOT LIKE"

    def test_ilike_operator(self):
        assert _OPERATORS["_ilike"] == "ILIKE"

    def test_nilike_operator(self):
        assert _OPERATORS["_nilike"] == "NOT ILIKE"

    def test_in_operator(self):
        assert _OPERATORS["_in"] == "IN"

    def test_nin_operator(self):
        assert _OPERATORS["_nin"] == "NOT IN"

    def test_is_null_operator(self):
        assert _OPERATORS["_is_null"] == "IS NULL"


# ===========================================================================
# TestBoolExprToSQL — comprehensive filter conversion
# ===========================================================================


class TestBoolExprToSQLEmpty:
    def test_empty_dict_returns_true(self):
        assert bool_expr_to_sql({}) == "TRUE"

    def test_none_coerced_is_true(self):
        # Empty filter is the common "no restriction" case
        assert bool_expr_to_sql({}) == "TRUE"


class TestBoolExprToSQLSimpleOperators:
    def test_eq_string(self):
        result = bool_expr_to_sql({"status": {"_eq": "active"}})
        assert result == "status = 'active'"

    def test_eq_integer(self):
        result = bool_expr_to_sql({"age": {"_eq": 30}})
        assert result == "age = 30"

    def test_neq(self):
        result = bool_expr_to_sql({"role": {"_neq": "guest"}})
        assert "!=" in result
        assert "'guest'" in result

    def test_gt(self):
        result = bool_expr_to_sql({"price": {"_gt": 100}})
        assert ">" in result
        assert "100" in result

    def test_lt(self):
        result = bool_expr_to_sql({"discount": {"_lt": 0.5}})
        assert "<" in result

    def test_gte(self):
        result = bool_expr_to_sql({"quantity": {"_gte": 1}})
        assert ">=" in result

    def test_lte(self):
        result = bool_expr_to_sql({"score": {"_lte": 100}})
        assert "<=" in result

    def test_like(self):
        result = bool_expr_to_sql({"name": {"_like": "%Smith%"}})
        assert "LIKE" in result
        assert "'%Smith%'" in result

    def test_nlike(self):
        result = bool_expr_to_sql({"name": {"_nlike": "%test%"}})
        assert "NOT LIKE" in result

    def test_ilike(self):
        result = bool_expr_to_sql({"email": {"_ilike": "%@example.com"}})
        assert "ILIKE" in result

    def test_nilike(self):
        result = bool_expr_to_sql({"email": {"_nilike": "%spam%"}})
        assert "NOT ILIKE" in result

    def test_in_list(self):
        result = bool_expr_to_sql({"status": {"_in": ["active", "pending"]}})
        assert "IN" in result
        assert "'active'" in result
        assert "'pending'" in result

    def test_nin_list(self):
        result = bool_expr_to_sql({"status": {"_nin": ["deleted", "archived"]}})
        assert "NOT IN" in result

    def test_is_null_true(self):
        result = bool_expr_to_sql({"deleted_at": {"_is_null": True}})
        assert result == "deleted_at IS NULL"

    def test_is_null_false(self):
        result = bool_expr_to_sql({"deleted_at": {"_is_null": False}})
        assert result == "deleted_at IS NOT NULL"


class TestBoolExprToSQLLogicalOperators:
    def test_and_two_conditions(self):
        expr = {"_and": [
            {"status": {"_eq": "active"}},
            {"age": {"_gte": 18}},
        ]}
        result = bool_expr_to_sql(expr)
        assert "AND" in result
        assert "status = 'active'" in result
        assert "age >= 18" in result

    def test_or_two_conditions(self):
        expr = {"_or": [
            {"role": {"_eq": "admin"}},
            {"role": {"_eq": "editor"}},
        ]}
        result = bool_expr_to_sql(expr)
        assert "OR" in result
        assert "role = 'admin'" in result
        assert "role = 'editor'" in result

    def test_not_condition(self):
        expr = {"_not": {"status": {"_eq": "deleted"}}}
        result = bool_expr_to_sql(expr)
        assert "NOT" in result
        assert "status = 'deleted'" in result

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

    def test_and_single_item(self):
        expr = {"_and": [{"status": {"_eq": "ok"}}]}
        result = bool_expr_to_sql(expr)
        assert "status = 'ok'" in result

    def test_or_single_item(self):
        expr = {"_or": [{"flag": {"_eq": True}}]}
        result = bool_expr_to_sql(expr)
        assert "flag = TRUE" in result

    def test_deeply_nested_not_and(self):
        expr = {
            "_not": {
                "_and": [
                    {"a": {"_eq": 1}},
                    {"b": {"_eq": 2}},
                ],
            },
        }
        result = bool_expr_to_sql(expr)
        assert "NOT" in result
        assert "AND" in result

    def test_multiple_top_level_columns_joined_with_and(self):
        """Two column conditions at the same level should be ANDed."""
        expr = {
            "status": {"_eq": "active"},
            "verified": {"_eq": True},
        }
        result = bool_expr_to_sql(expr)
        assert "status = 'active'" in result
        assert "verified = TRUE" in result


class TestBoolExprToSQLTableAlias:
    def test_column_prefixed_with_alias(self):
        result = bool_expr_to_sql({"id": {"_eq": 1}}, table_alias="u")
        assert result == "u.id = 1"

    def test_no_alias_no_prefix(self):
        result = bool_expr_to_sql({"id": {"_eq": 1}}, table_alias="")
        assert result == "id = 1"

    def test_alias_applied_in_and(self):
        expr = {"_and": [
            {"user_id": {"_eq": 5}},
            {"deleted": {"_is_null": True}},
        ]}
        result = bool_expr_to_sql(expr, table_alias="t")
        assert "t.user_id" in result
        assert "t.deleted" in result


class TestBoolExprToSQLSessionVariables:
    def test_hasura_user_id_session_var(self):
        """x-hasura-user-id in filter should become a ${} reference."""
        expr = {"user_id": {"_eq": "x-hasura-user-id"}}
        result = bool_expr_to_sql(expr)
        assert "${x-hasura-user-id}" in result

    def test_hasura_uppercase_session_var(self):
        expr = {"user_id": {"_eq": "X-Hasura-User-Id"}}
        result = bool_expr_to_sql(expr)
        assert "${" in result

    def test_session_var_as_dict_operand(self):
        """Hasura passes session vars as {x-hasura-user-id: x-hasura-user-id}."""
        expr = {"user_id": {"_eq": {"x-hasura-user-id": "x-hasura-user-id"}}}
        result = bool_expr_to_sql(expr)
        assert "${x-hasura-user-id}" in result

    def test_non_session_string_quoted(self):
        expr = {"role": {"_eq": "analyst"}}
        result = bool_expr_to_sql(expr)
        assert "'analyst'" in result
        assert "${" not in result


class TestBoolExprToSQLExistsSubquery:
    def test_exists_basic(self):
        expr = {"_exists": {
            "_table": {"schema": "public", "name": "users"},
            "_where": {"id": {"_eq": 1}},
        }}
        result = bool_expr_to_sql(expr)
        assert "EXISTS" in result
        assert "public.users" in result

    def test_exists_with_where_condition(self):
        expr = {"_exists": {
            "_table": {"schema": "auth", "name": "memberships"},
            "_where": {"org_id": {"_eq": 42}},
        }}
        result = bool_expr_to_sql(expr)
        assert "auth.memberships" in result
        assert "42" in result
        assert "SELECT 1 FROM" in result


class TestBoolExprToSQLUnsupportedOperator:
    def test_unsupported_op_produces_comment(self):
        expr = {"col": {"_custom_op": "value"}}
        result = bool_expr_to_sql(expr)
        assert "unsupported op" in result or "custom_op" in result

    def test_unknown_top_level_key_skipped(self):
        """Keys starting with _ that are not known operators are skipped."""
        expr = {"_unknown_thing": "value"}
        result = bool_expr_to_sql(expr)
        # Should still return something without crashing
        assert isinstance(result, str)


# ===========================================================================
# TestHasuraV2ConverterIntegration
# ===========================================================================


class TestHasuraV2ConverterIntegration:
    """Integration tests exercising import_shared utilities through the
    Hasura v2 mapper pipeline."""

    def _make_metadata_dir(self, tmp_path: Path, tables_data: list) -> Path:
        (tmp_path / "tables.yaml").write_text(yaml.dump(tables_data))
        return tmp_path

    def test_bool_expr_used_in_rls_rule(self, tmp_path: Path):
        """Filter expressions from select_permissions become RLS rules."""
        from provisa.hasura_v2.mapper import convert_metadata
        from provisa.hasura_v2.models import (
            HasuraMetadata,
            HasuraPermission,
            HasuraSource,
            HasuraTable,
        )

        table = HasuraTable(
            name="documents",
            schema_name="public",
            select_permissions=[
                HasuraPermission(
                    role="user",
                    columns=["id", "title"],
                    filter={"owner_id": {"_eq": "x-hasura-user-id"}},
                ),
            ],
        )
        source = HasuraSource(name="default", kind="postgres", tables=[table])
        metadata = HasuraMetadata(sources=[source])

        config = convert_metadata(metadata)
        user_rls = [r for r in config.rls_rules if r.role_id == "user"]
        assert len(user_rls) == 1
        assert "${x-hasura-user-id}" in user_rls[0].filter or "x-hasura-user-id" in user_rls[0].filter

    def test_empty_filter_generates_no_rls_rule(self, tmp_path: Path):
        """An empty filter {} means no row restriction — no RLS rule generated."""
        from provisa.hasura_v2.mapper import convert_metadata
        from provisa.hasura_v2.models import (
            HasuraMetadata,
            HasuraPermission,
            HasuraSource,
            HasuraTable,
        )

        table = HasuraTable(
            name="public_docs",
            schema_name="public",
            select_permissions=[
                HasuraPermission(role="viewer", columns=["id", "body"], filter={}),
            ],
        )
        source = HasuraSource(name="default", kind="postgres", tables=[table])
        metadata = HasuraMetadata(sources=[source])

        config = convert_metadata(metadata)
        viewer_rls = [r for r in config.rls_rules if r.role_id == "viewer"]
        assert len(viewer_rls) == 0

    def test_warning_collector_receives_remote_schema_warnings(self, tmp_path: Path):
        """Remote schemas generate warnings via parse_metadata_dir."""
        from provisa.hasura_v2.parser import parse_metadata_dir

        rs_yaml = [{"name": "payments_api", "definition": {"url": "https://pay.example.com"}}]
        (tmp_path / "remote_schemas.yaml").write_text(yaml.dump(rs_yaml))
        (tmp_path / "tables.yaml").write_text("[]")

        collector = WarningCollector()
        parse_metadata_dir(tmp_path, collector)

        assert collector.has_warnings()
        categories = {w.category for w in collector.warnings}
        assert "remote_schemas" in categories

    def test_warning_collector_receives_event_trigger_warnings(self):
        """Event triggers generate warnings during mapper convert."""
        from provisa.hasura_v2.mapper import convert_metadata
        from provisa.hasura_v2.models import (
            HasuraEventTrigger,
            HasuraMetadata,
            HasuraSource,
            HasuraTable,
        )

        table = HasuraTable(
            name="orders",
            schema_name="public",
            event_triggers=[
                HasuraEventTrigger(
                    name="order_notify",
                    table_name="orders",
                    table_schema="public",
                    webhook="https://hooks.example.com/order",
                    operations=["insert"],
                ),
            ],
        )
        source = HasuraSource(name="default", kind="postgres", tables=[table])
        metadata = HasuraMetadata(sources=[source])

        collector = WarningCollector()
        convert_metadata(metadata, collector=collector)

        assert collector.has_warnings()
        categories = {w.category for w in collector.warnings}
        assert "event_triggers" in categories


# ===========================================================================
# TestDDNConverterIntegration
# ===========================================================================


class TestDDNConverterIntegration:
    """Integration tests exercising import_shared utilities through the DDN
    mapper pipeline."""

    def test_warning_collector_used_for_unknown_skipped_kinds(self):
        from provisa.ddn.mapper import convert_hml
        from provisa.ddn.models import DDNMetadata

        md = DDNMetadata()
        md.skipped_kinds["UnknownKind"] = 3

        collector = WarningCollector()
        convert_hml(md, collector=collector)

        assert collector.has_warnings()
        assert any("UnknownKind" in w.message for w in collector.warnings)

    def test_warning_collector_empty_for_clean_metadata(self):
        from provisa.ddn.mapper import convert_hml
        from provisa.ddn.models import DDNMetadata

        md = DDNMetadata()
        collector = WarningCollector()
        convert_hml(md, collector=collector)

        assert not collector.has_warnings()

    def test_model_permission_filter_converted_to_rls(self):
        """DDN model permissions with filters should produce RLS rules."""
        from provisa.ddn.mapper import convert_hml
        from provisa.ddn.models import (
            DDNConnector,
            DDNFieldMapping,
            DDNMetadata,
            DDNModel,
            DDNModelPermission,
            DDNObjectType,
            DDNTypeMapping,
            DDNTypePermission,
        )

        conn = DDNConnector(name="pg", subgraph="app", url="http://localhost/postgres")
        obj_type = DDNObjectType(
            name="Order",
            subgraph="app",
            fields={"id": "Int", "user_id": "Int"},
            type_mappings=[DDNTypeMapping(
                connector_name="pg",
                source_type="orders",
                field_mappings=[
                    DDNFieldMapping(graphql_field="id", column="id"),
                    DDNFieldMapping(graphql_field="user_id", column="user_id"),
                ],
            )],
        )
        model = DDNModel(
            name="Order",
            subgraph="app",
            object_type="Order",
            connector_name="pg",
            collection="orders",
        )
        perm = DDNModelPermission(
            model_name="Order",
            role="customer",
            filter={"user_id": {"_eq": "x-hasura-user-id"}},
        )
        tp = DDNTypePermission(
            type_name="Order",
            role="customer",
            allowed_fields=["id", "user_id"],
        )

        md = DDNMetadata(
            connectors=[conn],
            object_types=[obj_type],
            models=[model],
            type_permissions=[tp],
            model_permissions=[perm],
            subgraphs={"app"},
        )

        config = convert_hml(md)
        rls = [r for r in config.rls_rules if r.role_id == "customer"]
        assert len(rls) == 1
        assert "user_id" in rls[0].filter


# ===========================================================================
# TestBoolExprEdgeCases
# ===========================================================================


class TestBoolExprEdgeCases:
    def test_numeric_zero_not_falsy_in_format(self):
        """Zero should not be treated as falsy (producing NULL)."""
        result = bool_expr_to_sql({"count": {"_eq": 0}})
        assert "0" in result
        assert "NULL" not in result

    def test_boolean_false_in_filter(self):
        result = bool_expr_to_sql({"active": {"_eq": False}})
        assert "FALSE" in result

    def test_string_with_single_quote_formatted(self):
        """Values with single quotes are wrapped."""
        result = bool_expr_to_sql({"name": {"_eq": "O'Brien"}})
        assert "O'Brien" in result

    def test_large_in_list(self):
        values = list(range(20))
        result = bool_expr_to_sql({"id": {"_in": values}})
        assert "IN" in result
        assert "19" in result

    def test_multiple_operators_on_single_column(self):
        """Hasura allows multiple operators on a single column."""
        expr = {"age": {"_gte": 18, "_lte": 65}}
        result = bool_expr_to_sql(expr)
        assert "18" in result
        assert "65" in result

    def test_deeply_nested_and(self):
        expr = {
            "_and": [
                {"_and": [
                    {"a": {"_eq": 1}},
                    {"b": {"_eq": 2}},
                ]},
                {"c": {"_eq": 3}},
            ],
        }
        result = bool_expr_to_sql(expr)
        assert "AND" in result
        assert "a = 1" in result or "1" in result

    def test_returns_string_always(self):
        expressions = [
            {},
            {"x": {"_eq": 1}},
            {"_and": [{"y": {"_eq": 2}}]},
            {"_or": []},
        ]
        for expr in expressions:
            result = bool_expr_to_sql(expr)
            assert isinstance(result, str)
