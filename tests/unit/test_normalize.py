# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-049 — normalized (relational) result decomposition."""

import pytest
from graphql import parse

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.context import build_context
from provisa.compiler.normalize import (
    NormalizeError,
    check_normalizable,
    compile_normalized,
    discover_entity_paths,
)


def _col(name, data_type="varchar(100)", nullable=False):
    return ColumnMetadata(column_name=name, data_type=data_type, is_nullable=nullable)


def _ctx():
    tables = [
        {
            "id": 1,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "orders",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "customer_id", "visible_to": ["admin"]},
                {"column_name": "amount", "visible_to": ["admin"]},
            ],
        },
        {
            "id": 2,
            "source_id": "sales-pg",
            "domain_id": "sales",
            "schema_name": "public",
            "table_name": "customers",
            "governance": "pre-approved",
            "columns": [
                {"column_name": "id", "visible_to": ["admin"]},
                {"column_name": "name", "visible_to": ["admin"]},
                {"column_name": "email", "visible_to": ["admin"]},
            ],
        },
    ]
    relationships = [
        {
            "id": "ord-cust",
            "source_table_id": 1,
            "target_table_id": 2,
            "source_column": "customer_id",
            "target_column": "id",
            "cardinality": "many-to-one",
        },
    ]
    column_types = {
        1: [_col("id", "integer"), _col("customer_id", "integer"), _col("amount", "decimal(10,2)")],
        2: [_col("id", "integer"), _col("name", "varchar(100)"), _col("email", "varchar(200)")],
    }
    si = SchemaInput(
        tables=tables,
        relationships=relationships,
        column_types=column_types,
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )
    generate_schema(si)
    return build_context(si)


_QUERY = "{ orders { id amount customer { name } } }"


class TestDiscoverPaths:
    def test_root_and_nested_entity(self):
        ctx = _ctx()
        paths = discover_entity_paths(parse(_QUERY), ctx)
        names = {p.table_name for p in paths}
        assert names == {"orders", "customers"}

    def test_paths_track_chain(self):
        ctx = _ctx()
        paths = {p.table_name: p.path for p in discover_entity_paths(parse(_QUERY), ctx)}
        assert paths["orders"] == ("orders",)
        assert paths["customers"] == ("orders", "customer")


class TestNormalize:
    def test_one_table_per_entity(self):
        tables = compile_normalized(parse(_QUERY), _ctx())
        assert {t.table_name for t in tables} == {"orders", "customers"}

    def test_each_table_is_distinct(self):
        for t in compile_normalized(parse(_QUERY), _ctx()):
            assert t.compiled.sql.lstrip().upper().startswith("SELECT DISTINCT"), t.compiled.sql

    def test_fk_column_auto_included_on_child_side(self):
        # the FK (orders.customer_id) must be projected so the export self-joins
        tables = {t.table_name: t for t in compile_normalized(parse(_QUERY), _ctx())}
        assert "customer_id" in tables["orders"].compiled.sql

    def test_referenced_key_included_on_parent(self):
        # customers.id (the join target) must be present so children can reference it
        tables = {t.table_name: t for t in compile_normalized(parse(_QUERY), _ctx())}
        assert (
            '"id"' in tables["customers"].compiled.sql or " id" in tables["customers"].compiled.sql
        )


class TestPrecondition:
    def test_real_column_join_passes(self):
        result = check_normalizable(parse(_QUERY), _ctx())
        assert result is None

    def test_computed_join_rejected(self):
        import dataclasses

        ctx = _ctx()
        # turn the customer edge into a computed (source_expr) join
        for key, jm in list(ctx.joins.items()):
            if key[1] == "customer":
                ctx.joins[key] = dataclasses.replace(jm, source_expr="CONCAT({alias}.x)")
                break
        with pytest.raises(NormalizeError, match="computed"):
            check_normalizable(parse(_QUERY), ctx)
