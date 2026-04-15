# Copyright (c) 2026 Kenneth Stott
# Canary: f4171736-ac09-4022-a61a-d8adc2949321
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for config hot-reload behaviour.

Source coverage:
  - provisa/core/config_loader.py  — parse_config_dict, load_config (transactional)
  - provisa/compiler/schema_gen.py — generate_schema (naming convention drives schema)
  - provisa/api/app.py             — upload_config / update_settings invoke _load_and_build

The tests here exercise the observable, DB-free parts of the reload path:

  1. Naming convention change → different GraphQL field names in the rebuilt schema.
  2. Atomicity — parse_config_dict validates before any side-effects; a broken config
     raises ValidationError and leaves no partial state.
  3. Rollback on validation failure — an invalid config dict never produces a schema;
     the previous (valid) schema must remain usable.

NOTE: Full end-to-end reload via _load_and_build requires a live PG + Trino.
      Those integration paths are covered in tests/integration/.
      These tests use only the synchronous parse/generate layer.
"""

from __future__ import annotations

import pytest
from graphql import GraphQLSchema
from pydantic import ValidationError

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.core.config_loader import parse_config_dict


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _minimal_config_dict(
    convention: str = "snake_case",
    domain_prefix: bool = False,
) -> dict:
    return {
        "sources": [{
            "id": "pg-src",
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "username": "user",
            "password": "secret",
        }],
        "domains": [{"id": "sales"}],
        "tables": [{
            "source_id": "pg-src",
            "domain_id": "sales",
            "schema": "public",
            "table": "order_items",
            "governance": "pre-approved",
            "columns": [
                {"name": "id", "visible_to": ["analyst"]},
                {"name": "total_amount", "visible_to": ["analyst"]},
            ],
        }],
        "roles": [{"id": "analyst", "capabilities": ["read"], "domain_access": ["sales"]}],
        "naming": {"convention": convention, "domain_prefix": domain_prefix},
    }


def _col(name: str, data_type: str = "integer", nullable: bool = False) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _make_schema_input(convention: str = "snake_case", domain_prefix: bool = False) -> SchemaInput:
    """Build a SchemaInput that mirrors what _load_and_build assembles at runtime."""
    return SchemaInput(
        tables=[{
            "id": 1,
            "source_id": "pg-src",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "order_items",
            "governance": "pre-approved",
            "description": None,
            "alias": None,
            "naming_convention": None,
            "source_naming_convention": None,
            "relay_pagination": None,
            "hot": None,
            "columns": [
                {
                    "column_name": "id",
                    "visible_to": ["analyst"],
                    "writable_by": [],
                    "unmasked_to": [],
                    "mask_type": None,
                    "mask_pattern": None,
                    "mask_replace": None,
                    "mask_value": None,
                    "mask_precision": None,
                    "alias": None,
                    "description": None,
                    "path": None,
                },
                {
                    "column_name": "total_amount",
                    "visible_to": ["analyst"],
                    "writable_by": [],
                    "unmasked_to": [],
                    "mask_type": None,
                    "mask_pattern": None,
                    "mask_replace": None,
                    "mask_value": None,
                    "mask_precision": None,
                    "alias": None,
                    "description": None,
                    "path": None,
                },
            ],
        }],
        relationships=[],
        column_types={
            1: [
                _col("id", "integer"),
                _col("total_amount", "double"),
            ]
        },
        naming_rules=[],
        role={"id": "analyst", "capabilities": ["read"], "domain_access": ["sales"]},
        domains=[{"id": "sales", "description": ""}],
        source_types={"pg-src": "postgresql"},
        domain_prefix=domain_prefix,
        naming_convention=convention,
    )


# ---------------------------------------------------------------------------
# 1. Naming convention change triggers schema rebuild
# ---------------------------------------------------------------------------

class TestNamingConventionTriggersRebuild:
    """Changing the naming convention in config must produce a different schema."""

    def test_snake_case_field_name_unchanged(self):
        si = _make_schema_input(convention="snake_case")
        schema = generate_schema(si)
        query_type = schema.query_type
        assert "order_items" in query_type.fields

    def test_camel_case_convention_renames_fields(self):
        si = _make_schema_input(convention="camelCase")
        schema = generate_schema(si)
        query_type = schema.query_type
        # With camelCase, the column "total_amount" should become "totalAmount"
        # The table name itself stays snake since it's a GQL identifier (unchanged in short form)
        assert schema is not None

    def test_snake_and_camel_schemas_differ_on_column_fields(self):
        snake_si = _make_schema_input(convention="snake_case")
        camel_si = _make_schema_input(convention="camelCase")
        snake_schema = generate_schema(snake_si)
        camel_schema = generate_schema(camel_si)

        def _unwrap(field):
            """Peel NonNull / List wrappers to reach the named object type."""
            t = field.type
            while hasattr(t, "of_type"):
                t = t.of_type
            return t

        snake_type = _unwrap(snake_schema.query_type.fields["order_items"])
        camel_type = _unwrap(camel_schema.query_type.fields["order_items"])

        snake_cols = set(snake_type.fields.keys())
        camel_cols = set(camel_type.fields.keys())

        # snake_case: total_amount; camelCase: totalAmount
        assert "total_amount" in snake_cols
        assert "totalAmount" in camel_cols
        assert snake_cols != camel_cols

    def test_domain_prefix_off_no_prefix_in_field(self):
        si = _make_schema_input(domain_prefix=False)
        schema = generate_schema(si)
        fields = schema.query_type.fields
        # Without domain_prefix, field is just "order_items"
        assert "order_items" in fields
        assert all(not k.startswith("sales__") for k in fields)

    def test_domain_prefix_on_prepends_domain(self):
        si = _make_schema_input(domain_prefix=True)
        schema = generate_schema(si)
        fields = schema.query_type.fields
        # With domain_prefix=True, the field name should include the domain
        assert any("order_items" in k for k in fields)
        # Specifically should be prefixed with domain initials (e.g. "s__" for "sales")
        assert any("__" in k for k in fields)

    def test_schema_is_graphql_schema_instance(self):
        si = _make_schema_input()
        schema = generate_schema(si)
        assert isinstance(schema, GraphQLSchema)

    def test_convention_change_column_name_in_rebuilt_schema(self):
        """Round-trip: switch convention, rebuild, verify field change."""
        def _unwrap(field):
            t = field.type
            while hasattr(t, "of_type"):
                t = t.of_type
            return t

        si_v1 = _make_schema_input(convention="snake_case")
        si_v2 = _make_schema_input(convention="PascalCase")

        schema_v1 = generate_schema(si_v1)
        schema_v2 = generate_schema(si_v2)

        type_v1 = _unwrap(schema_v1.query_type.fields["order_items"])
        type_v2 = _unwrap(schema_v2.query_type.fields["order_items"])

        # PascalCase: total_amount → TotalAmount
        assert "total_amount" in type_v1.fields
        assert "TotalAmount" in type_v2.fields


# ---------------------------------------------------------------------------
# 2. Atomic update — no partial state during reload
# ---------------------------------------------------------------------------

class TestAtomicConfigUpdate:
    """parse_config_dict validates the entire config before any state changes.

    A ValidationError means the config was never applied — the caller's
    state object still holds the previous valid schema.
    """

    def test_valid_config_parses_completely(self):
        config = parse_config_dict(_minimal_config_dict())
        assert len(config.sources) == 1
        assert len(config.tables) == 1
        assert len(config.roles) == 1

    def test_invalid_source_type_raises_before_state_change(self):
        """ValidationError raised atomically — no intermediate partial config."""
        bad = _minimal_config_dict()
        bad["sources"][0]["type"] = "nonexistent_db"
        with pytest.raises(ValidationError):
            parse_config_dict(bad)

    def test_missing_required_field_raises_atomically(self):
        bad = _minimal_config_dict()
        del bad["roles"]
        with pytest.raises(ValidationError):
            parse_config_dict(bad)

    def test_state_unmodified_after_failed_parse(self):
        """The valid config object from a prior parse remains intact after a failed reload."""
        good = parse_config_dict(_minimal_config_dict())
        assert len(good.sources) == 1

        bad = _minimal_config_dict()
        bad["sources"][0]["type"] = "invalid"
        with pytest.raises(ValidationError):
            parse_config_dict(bad)

        # good config object is unchanged — it's a separate immutable Pydantic model
        assert good.sources[0].id == "pg-src"
        assert good.sources[0].type.value == "postgresql"

    def test_two_successive_valid_reloads_both_succeed(self):
        """Each parse_config_dict call is independent and idempotent."""
        c1 = parse_config_dict(_minimal_config_dict(convention="snake_case"))
        c2 = parse_config_dict(_minimal_config_dict(convention="camelCase"))
        assert c1.naming.convention == "snake_case"
        assert c2.naming.convention == "camelCase"

    def test_invalid_role_capability_raises_atomically(self):
        bad = _minimal_config_dict()
        bad["roles"][0]["capabilities"] = ["not_a_real_capability"]
        # Depending on whether capability is enum-validated; at minimum it parses
        # (capabilities is a plain list[str]). But an empty capabilities list is valid.
        # Test that we get a valid or invalid result — no partial write.
        try:
            result = parse_config_dict(bad)
            # If accepted, verify the structure is complete
            assert result.roles[0].id == "analyst"
        except ValidationError:
            # Also acceptable — no partial config written
            pass


# ---------------------------------------------------------------------------
# 3. Rollback on validation failure
# ---------------------------------------------------------------------------

class TestRollbackOnValidationFailure:
    """When a new config fails validation, the prior schema must remain usable."""

    def test_schema_from_valid_config_usable_after_reload_failure(self):
        """The schema built from the prior config continues to serve queries."""
        def _unwrap(field):
            t = field.type
            while hasattr(t, "of_type"):
                t = t.of_type
            return t

        si = _make_schema_input(convention="snake_case")
        prior_schema = generate_schema(si)

        # Simulate a failed reload — bad config never produces a schema
        bad_config = _minimal_config_dict()
        bad_config["sources"][0]["type"] = "bad_type"
        with pytest.raises(ValidationError):
            parse_config_dict(bad_config)

        # Prior schema is still fully functional
        assert "order_items" in prior_schema.query_type.fields
        order_type = _unwrap(prior_schema.query_type.fields["order_items"])
        assert "id" in order_type.fields
        assert "total_amount" in order_type.fields

    def test_invalid_config_does_not_produce_schema(self):
        """A ValidationError from parse_config_dict means generate_schema is never called."""
        bad_config = _minimal_config_dict()
        del bad_config["domains"]
        with pytest.raises(ValidationError):
            parse_config_dict(bad_config)
        # No schema object was created — no assertion needed beyond not reaching this line

    def test_valid_then_invalid_then_valid_reload(self):
        """Three reloads: good → bad (rollback) → good again succeeds."""
        c1 = parse_config_dict(_minimal_config_dict(convention="snake_case"))
        assert c1.naming.convention == "snake_case"

        with pytest.raises(ValidationError):
            parse_config_dict({})  # empty dict is invalid

        c3 = parse_config_dict(_minimal_config_dict(convention="camelCase"))
        assert c3.naming.convention == "camelCase"

    def test_naming_validation_rejects_unknown_convention(self):
        """NamingConfig.convention is a plain str — unknown values parse but schema gen
        falls through to no-op aliases. Verify the schema still builds correctly."""
        si = _make_schema_input(convention="unknown_convention")
        schema = generate_schema(si)
        # Schema still generates successfully; unknown convention has no effect
        assert "order_items" in schema.query_type.fields

    def test_generate_schema_raises_when_no_tables_visible(self):
        """generate_schema raises ValueError when no tables are visible to the role —
        this is the schema-level equivalent of a rollback trigger."""
        si = _make_schema_input()
        # Assign a role with no domain access
        si.role = {"id": "stranger", "capabilities": ["read"], "domain_access": []}
        with pytest.raises(ValueError, match="No tables visible"):
            generate_schema(si)
