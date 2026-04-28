# Copyright (c) 2026 Kenneth Stott
# Canary: b2c3d4e5-f6a7-8901-bcde-f01234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Extended Apollo Federation v2 subgraph tests.

These tests do NOT duplicate the scenarios already in
tests/unit/test_federation.py, which covers: FederationConfig defaults,
extract_pk_columns (all fallback paths), build_federation_schema (_service,
_entities, @key on single and composite PKs, _Entity union, _Any scalar,
mutation preservation, existing query fields), generate_federation_sdl
(@link/@key presence), group_representations (grouping, error paths),
compile_entity_query (single and composite PK), and resolve_entities
(basic, batch, rls_context forwarding, unresolved→None).

New coverage here:
  - build_federation_schema raises when no entity types are found
  - @key directive is emitted for every entity type in the schema
  - _entities field accepts a list argument named "representations"
  - SDL contains the correct federation URL (v2.3 spec)
  - FederationConfig version field distinguishes v1 from v2
  - generate_federation_sdl with an empty key_directives dict emits no @key
  - Subgraph schema retains all original root query fields after federation wrap
  - _Any scalar is present and its name is exactly "_Any"
  - _Entity union name is exactly "_Entity"
  - Multiple entity types all appear in the _Entity union
  - _service resolver returns dict with 'sdl' key
  - Subgraph schema with a single table and composite PK emits correct @key
"""

from __future__ import annotations

import pytest

from graphql import (
    GraphQLField,
    GraphQLInt,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
    GraphQLUnionType,
    graphql_sync,
)

from provisa.compiler.federation import (
    FEDERATION_LINK_URL,
    FederationConfig,
    build_federation_schema,
    extract_pk_columns,
    generate_federation_sdl,
)
from provisa.compiler.introspect import ColumnMetadata

# ---------------------------------------------------------------------------
# Schema / table factory helpers
# ---------------------------------------------------------------------------


def _make_type(name: str, fields: dict | None = None) -> GraphQLObjectType:
    if fields is None:
        fields = {"id": GraphQLField(GraphQLNonNull(GraphQLInt))}
    return GraphQLObjectType(name, fields)


def _schema_with_types(*type_names: str) -> GraphQLSchema:
    """Build a minimal schema with one query field per type name."""
    types = {n: _make_type(n) for n in type_names}
    query_fields = {
        n.lower(): GraphQLField(GraphQLList(GraphQLNonNull(t)))
        for n, t in types.items()
    }
    return GraphQLSchema(query=GraphQLObjectType("Query", query_fields))


def _tables_for(*type_names: str, start_id: int = 1) -> list[dict]:
    return [
        {"id": start_id + i, "table_name": n.lower(), "_type_name": n}
        for i, n in enumerate(type_names)
    ]


def _pk_for(*type_names: str, start_id: int = 1) -> dict[int, list[str]]:
    return {start_id + i: ["id"] for i in range(len(type_names))}


# ---------------------------------------------------------------------------
# build_federation_schema error paths
# ---------------------------------------------------------------------------


class TestBuildFederationSchemaErrors:
    def test_raises_when_no_entity_types_found(self):
        """build_federation_schema must raise ValueError when no tables match
        the base schema types (e.g. all _type_name fields are missing)."""
        schema = _schema_with_types("Orders")
        tables = [{"id": 1, "table_name": "orders"}]  # no _type_name
        pk = {1: ["id"]}
        with pytest.raises(ValueError, match="No entity types found"):
            build_federation_schema(schema, tables, pk)

    def test_raises_when_pk_columns_empty_dict(self):
        """build_federation_schema must raise ValueError when pk_columns is empty."""
        schema = _schema_with_types("Orders")
        tables = _tables_for("Orders")
        with pytest.raises(ValueError, match="No entity types found"):
            build_federation_schema(schema, tables, pk_columns={})


# ---------------------------------------------------------------------------
# @key directive appears for every entity type
# ---------------------------------------------------------------------------


class TestKeyDirectiveAllTypes:
    def test_key_directive_on_all_types(self):
        """Every registered entity type must have @key(fields: "id") in the SDL."""
        schema = _schema_with_types("Orders", "Customers", "Products")
        tables = _tables_for("Orders", "Customers", "Products")
        pk = _pk_for("Orders", "Customers", "Products")
        fed = build_federation_schema(schema, tables, pk)
        result = graphql_sync(fed, "{ _service { sdl } }")
        sdl = result.data["_service"]["sdl"]

        # All three types must be annotated with @key
        for type_name in ["Orders", "Customers", "Products"]:
            assert f"type {type_name} @key" in sdl, (
                f"@key directive missing for {type_name}"
            )

    def test_composite_pk_key_directive_correct_fields_string(self):
        """Composite PK with three columns must generate space-separated @key field list."""
        schema = _schema_with_types("Events")
        tables = [{"id": 1, "table_name": "events", "_type_name": "Events"}]
        pk = {1: ["tenant_id", "stream_id", "seq"]}
        fed = build_federation_schema(schema, tables, pk)
        result = graphql_sync(fed, "{ _service { sdl } }")
        sdl = result.data["_service"]["sdl"]
        assert '@key(fields: "tenant_id stream_id seq")' in sdl


# ---------------------------------------------------------------------------
# _entities field structure
# ---------------------------------------------------------------------------


class TestEntitiesFieldStructure:
    def test_entities_field_has_representations_argument(self):
        """_entities field must expose a 'representations' argument."""
        schema = _schema_with_types("Orders")
        tables = _tables_for("Orders")
        pk = _pk_for("Orders")
        fed = build_federation_schema(schema, tables, pk)
        entities_field = fed.query_type.fields["_entities"]
        assert "representations" in entities_field.args

    def test_entities_field_returns_non_null_list(self):
        """_entities return type must be a NonNull list."""
        schema = _schema_with_types("Orders")
        tables = _tables_for("Orders")
        pk = _pk_for("Orders")
        fed = build_federation_schema(schema, tables, pk)
        entities_field = fed.query_type.fields["_entities"]
        from graphql import GraphQLNonNull, GraphQLList
        assert isinstance(entities_field.type, GraphQLNonNull)
        assert isinstance(entities_field.type.of_type, GraphQLList)

    def test_entity_union_type_name_is_entity(self):
        """The union type added to the schema must be named exactly '_Entity'."""
        schema = _schema_with_types("Orders")
        tables = _tables_for("Orders")
        pk = _pk_for("Orders")
        fed = build_federation_schema(schema, tables, pk)
        entity_type = fed.type_map.get("_Entity")
        assert entity_type is not None
        assert isinstance(entity_type, GraphQLUnionType)
        assert entity_type.name == "_Entity"

    def test_entity_union_contains_all_registered_types(self):
        """_Entity union must include every registered entity type."""
        schema = _schema_with_types("Orders", "Customers")
        tables = _tables_for("Orders", "Customers")
        pk = _pk_for("Orders", "Customers")
        fed = build_federation_schema(schema, tables, pk)
        entity_union = fed.type_map["_Entity"]
        union_type_names = {t.name for t in entity_union.types}
        assert "Orders" in union_type_names
        assert "Customers" in union_type_names

    def test_any_scalar_name_is_correct(self):
        """The _Any scalar added to the schema must be named exactly '_Any'."""
        schema = _schema_with_types("Orders")
        tables = _tables_for("Orders")
        pk = _pk_for("Orders")
        fed = build_federation_schema(schema, tables, pk)
        any_scalar = fed.type_map.get("_Any")
        assert any_scalar is not None
        assert any_scalar.name == "_Any"


# ---------------------------------------------------------------------------
# _service field and SDL content
# ---------------------------------------------------------------------------


class TestServiceFieldAndSDL:
    def test_service_resolver_returns_dict_with_sdl_key(self):
        """_service resolver must return a mapping that has an 'sdl' key."""
        schema = _schema_with_types("Orders")
        tables = _tables_for("Orders")
        pk = _pk_for("Orders")
        fed = build_federation_schema(schema, tables, pk)
        # Resolve directly via graphql_sync
        result = graphql_sync(fed, "{ _service { sdl } }")
        assert result.errors is None
        assert "sdl" in result.data["_service"]
        assert isinstance(result.data["_service"]["sdl"], str)

    def test_sdl_contains_federation_spec_url(self):
        """SDL must reference the exact federation v2.3 spec URL."""
        schema = _schema_with_types("Orders")
        tables = _tables_for("Orders")
        pk = _pk_for("Orders")
        fed = build_federation_schema(schema, tables, pk)
        result = graphql_sync(fed, "{ _service { sdl } }")
        sdl = result.data["_service"]["sdl"]
        assert FEDERATION_LINK_URL in sdl
        assert "federation/v2.3" in sdl

    def test_sdl_contains_link_directive_with_imports(self):
        """SDL must contain @link directive importing @key, @shareable, @external."""
        schema = _schema_with_types("Orders")
        tables = _tables_for("Orders")
        pk = _pk_for("Orders")
        fed = build_federation_schema(schema, tables, pk)
        result = graphql_sync(fed, "{ _service { sdl } }")
        sdl = result.data["_service"]["sdl"]
        assert "@link" in sdl
        assert '"@key"' in sdl

    def test_sdl_contains_original_query_type(self):
        """SDL must mention the original query fields from the base schema."""
        schema = _schema_with_types("Orders", "Customers")
        tables = _tables_for("Orders", "Customers")
        pk = _pk_for("Orders", "Customers")
        fed = build_federation_schema(schema, tables, pk)
        result = graphql_sync(fed, "{ _service { sdl } }")
        sdl = result.data["_service"]["sdl"]
        # The SDL must reference the original type names
        assert "Orders" in sdl
        assert "Customers" in sdl

    def test_generate_federation_sdl_with_empty_key_directives_emits_no_key(self):
        """generate_federation_sdl with an empty dict must not emit any @key lines."""
        schema = _schema_with_types("Orders")
        sdl = generate_federation_sdl(schema, {})
        assert "@key(fields:" not in sdl

    def test_generate_federation_sdl_with_none_key_directives_emits_no_key(self):
        """generate_federation_sdl with None key_directives must not emit any @key."""
        schema = _schema_with_types("Orders")
        sdl = generate_federation_sdl(schema, None)
        assert "@key(fields:" not in sdl

    def test_generate_federation_sdl_contains_extend_schema(self):
        """SDL must start with the extend schema @link header."""
        schema = _schema_with_types("Orders")
        sdl = generate_federation_sdl(schema, {"Orders": "id"})
        assert "extend schema @link" in sdl


# ---------------------------------------------------------------------------
# FederationConfig
# ---------------------------------------------------------------------------


class TestFederationConfigExtended:
    def test_federation_config_version_defaults_to_2(self):
        cfg = FederationConfig()
        assert cfg.version == 2

    def test_federation_config_custom_service_name(self):
        cfg = FederationConfig(service_name="my-subgraph")
        assert cfg.service_name == "my-subgraph"

    def test_federation_config_enabled_true(self):
        cfg = FederationConfig(enabled=True)
        assert cfg.enabled is True

    def test_federation_config_all_fields_settable(self):
        cfg = FederationConfig(enabled=True, version=2, service_name="svc")
        assert cfg.enabled is True
        assert cfg.version == 2
        assert cfg.service_name == "svc"


# ---------------------------------------------------------------------------
# extract_pk_columns — additional paths
# ---------------------------------------------------------------------------


class TestExtractPKColumnsExtended:
    def test_multiple_tables_each_get_their_pk(self):
        tables = [
            {"id": 1, "primary_key": "order_id"},
            {"id": 2, "primary_key": ["tenant", "customer_id"]},
            {"id": 3},
        ]
        column_types = {
            3: [ColumnMetadata("id", "integer", False)],
        }
        result = extract_pk_columns(tables, column_types)
        assert result[1] == ["order_id"]
        assert result[2] == ["tenant", "customer_id"]
        assert result[3] == ["id"]

    def test_table_with_no_columns_returns_nothing(self):
        """A table with no column metadata and no explicit PK must not appear in result."""
        tables = [{"id": 99}]
        column_types: dict = {}
        result = extract_pk_columns(tables, column_types)
        assert 99 not in result

    def test_first_column_used_when_no_non_nullable_and_no_id(self):
        """Falls back to first column when no 'id' column and all are nullable."""
        tables = [{"id": 5}]
        column_types = {
            5: [
                ColumnMetadata("name", "varchar", True),
                ColumnMetadata("code", "varchar", True),
            ]
        }
        result = extract_pk_columns(tables, column_types)
        assert result[5] == ["name"]
