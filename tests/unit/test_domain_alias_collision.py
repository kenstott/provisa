# Copyright (c) 2026 Kenneth Stott
# Canary: 4b18a06a-3f27-46da-b1d5-c8370947102e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Two domains whose computed aliases would be the same (ontology and ops both "o") are each named
apart in every surface: GraphQL's root fields and types, gRPC's messages, the compilation context's
names. Neither is named over the other, so each is shown and grouped as its own domain."""

# Requirements: REQ-471, REQ-2163

from __future__ import annotations

import pytest

from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.naming import domain_alias_map

pytestmark = pytest.mark.unit

TABLES = [("ontology", "_terms"), ("ontology", "_axioms"), ("ops", "queries"), ("ops", "traces")]


def _schema_input(domain_access: list[str]):
    from provisa.compiler.schema_gen import SchemaInput

    return SchemaInput(
        tables=[
            {
                "id": i,
                "source_id": "provisa-admin",
                "domain_id": domain,
                "schema_name": "public",
                "table_name": name,
                "columns": [{"column_name": "id", "visible_to": ["admin"]}],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
            for i, (domain, name) in enumerate(TABLES, 1)
        ],
        relationships=[],
        column_types={
            i: [ColumnMetadata(column_name="id", data_type="varchar", is_nullable=True)]
            for i in range(1, len(TABLES) + 1)
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": domain_access},
        domains=[
            {"id": "ontology", "description": "Ontology"},
            {"id": "ops", "description": "Ops"},
        ],
        domain_prefix=True,
    )


def test_aliases_computed_alike_are_each_the_domains_own_name():
    assert domain_alias_map(
        [
            {"id": "ontology"},
            {"id": "ops"},
            {"id": "sales_analytics"},
            {"id": "hr", "graphql_alias": "HR"},
        ]
    ) == {"ontology": "ontology", "ops": "ops", "sales_analytics": "sa", "hr": "hr"}


@pytest.mark.parametrize("access", [["*"], ["ontology", "ops"]])
def test_a_role_holding_ontology_and_ops_is_served_both_apart(access):
    from provisa.compiler.schema_gen import generate_schema

    fields = set(generate_schema(_schema_input(access)).query_type.fields)
    assert {"ontology__terms", "ops__queries", "ops__traces"} <= fields
    # The SQL-only table stays SQL's alone.
    assert not any("axioms" in f for f in fields)


def test_grpc_and_the_compilation_context_name_them_apart():
    from provisa.compiler.context import build_context
    from provisa.grpc.proto_gen import generate_proto

    si = _schema_input(["*"])
    names = {m.field_name for m in build_context(si).tables.values()}
    assert {"ontology__terms", "ops__queries"} <= names
    from provisa.grpc.proto_gen import _to_proto_field_name

    proto = generate_proto(si)
    for field in ("ontology__terms", "ops__queries"):
        assert f" {_to_proto_field_name(field)} = " in proto
