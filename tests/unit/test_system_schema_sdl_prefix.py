# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9b41-0d5a-4f86-a3b1-e64f2d8c9a15
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""In the GraphQL SDL every root field and type the ontology schema gives carries on_, and every
one the ops schema gives carries op_, whether or not domains prefix their fields (REQ-2163,
amended 2026-10-27): neither shares a name with a model table's, a model table that would is
refused by name, and the surfaces that find a field by name (meta's resolve, gRPC, MCP, the Cypher
label map, semantic SQL) still find it. Bare labels and SQL relations are unprefixed."""

# Requirements: REQ-2163, REQ-471, REQ-1365

from __future__ import annotations

import pytest

from provisa.compiler.introspect import ColumnMetadata

pytestmark = pytest.mark.unit

#: (domain, table, alias, the table's gql_naming_convention)
_TABLES = [
    ("ontology", "_terms", None, None),
    ("ops", "queries", None, None),
    ("meta", "registered_tables", None, None),
    ("sales", "terms", None, None),
    ("sales", "queries", None, None),
]


def _schema_input(domain_prefix: bool, tables=_TABLES):
    from provisa.compiler.schema_gen import SchemaInput

    return SchemaInput(
        tables=[
            {
                "id": i,
                "source_id": "provisa-admin",
                "domain_id": domain,
                "schema_name": "public",
                "table_name": name,
                "alias": alias,
                "gql_naming_convention": convention,
                "columns": [{"column_name": "id", "visible_to": ["admin"]}],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
            for i, (domain, name, alias, convention) in enumerate(tables, 1)
        ],
        relationships=[],
        column_types={
            i: [ColumnMetadata(column_name="id", data_type="varchar", is_nullable=True)]
            for i in range(1, len(tables) + 1)
        },
        naming_rules=[],
        role={"id": "admin", "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": d} for d in ("ontology", "ops", "meta", "sales")],
        domain_prefix=domain_prefix,
    )


@pytest.mark.parametrize("domain_prefix", [False, True])
def test_the_sdl_prefixes_ontology_on_and_ops_op(domain_prefix):
    from provisa.compiler.schema_gen import generate_schema

    schema = generate_schema(_schema_input(domain_prefix))
    assert schema.query_type is not None
    fields = schema.query_type.fields
    assert fields["on_terms"].type.of_type.of_type.name == "ON_Terms"
    assert fields["op_queries"].type.of_type.of_type.name == "OP_Queries"
    assert {"ON_TermsWhere", "OP_QueriesWhere", "ON_TermsOrderBy"} <= set(schema.type_map)
    # The model tables of the same names keep theirs, prefixed as their domain is or not at all.
    model = {"s__terms", "s__queries"} if domain_prefix else {"terms", "queries"}
    assert model <= set(fields)


@pytest.mark.parametrize("domain_prefix", [False, True])
def test_no_system_schema_name_is_a_model_tables(domain_prefix):
    from provisa.compiler.schema_gen import generate_schema

    schema = generate_schema(_schema_input(domain_prefix))
    assert schema.query_type is not None
    on_op = {
        n
        for n in [*schema.query_type.fields, *schema.type_map]
        if n[:3] in ("on_", "op_", "ON_", "OP_")
    }
    model_types = {"Terms", "Queries", "S__Terms", "S__Queries"}
    assert on_op and not on_op & model_types


@pytest.mark.parametrize("convention", ["snake", "hasura_graphql"])
def test_a_model_table_named_as_a_system_schemas_table_is_refused(convention):
    from provisa.compiler.schema_gen import generate_schema

    tables = [*_TABLES, ("sales", "orders", "on_terms", convention)]
    with pytest.raises(ValueError, match=r"'on_terms', which is the ontology schema's"):
        generate_schema(_schema_input(False, tables))


@pytest.mark.parametrize("domain_prefix", [False, True])
def test_meta_resolve_is_still_found(domain_prefix):
    from provisa.compiler.schema_gen import generate_schema, resolve_field

    schema = generate_schema(_schema_input(domain_prefix))
    assert resolve_field(schema) == ("m__resolve" if domain_prefix else "resolve")


@pytest.mark.parametrize("domain_prefix", [False, True])
def test_grpc_and_mcp_name_the_types_the_schema_does(domain_prefix):
    from provisa.compiler.context import build_context
    from provisa.compiler.naming import table_type_name
    from provisa.grpc.proto_gen import _to_proto_field_name, _to_proto_type_name, generate_proto

    si = _schema_input(domain_prefix)
    proto = generate_proto(si)
    for meta in build_context(si).tables.values():
        if meta.domain_id not in ("ontology", "ops"):
            continue
        # MCP's describe names the gRPC method from the field by the same rule the schema types it.
        assert table_type_name(meta.field_name, meta.domain_id) == meta.type_name
        assert f" {_to_proto_field_name(meta.field_name)} = " in proto
        assert f"Query{_to_proto_type_name(meta.type_name)}" in proto


@pytest.mark.parametrize("domain_prefix", [False, True])
def test_the_semantic_sql_name_and_the_bare_label_are_unprefixed(domain_prefix, monkeypatch):
    from provisa.compiler.context import build_context
    from provisa.compiler.sql_rewrite import semantic_table_name
    from provisa.core import domain_policy
    from provisa.gql.label_map import CypherLabelMap

    monkeypatch.setattr(domain_policy, "single_domain", lambda: False)
    ctx = build_context(_schema_input(domain_prefix))
    by_field = {m.field_name: m for m in ctx.tables.values()}
    assert semantic_table_name(by_field["on_terms"]) == "terms"
    assert semantic_table_name(by_field["op_queries"]) == "queries"
    lm = CypherLabelMap.from_schema(ctx, domain_access=["*"])
    labels = {nm.type_name: nm.label for nm in lm.nodes.values()}
    assert labels["OP_Queries"] == "Ops:Queries"


def test_a_name_without_its_system_prefix_is_no_system_tables():
    from provisa.compiler.naming import bare_field_name, unprefixed_field_name

    assert bare_field_name("on_terms", "ontology") == "terms"
    assert bare_field_name("sa__on_terms", "sales") == "on_terms"
    assert unprefixed_field_name("on_hold", "sales") == "on_hold"
    with pytest.raises(ValueError, match="lacks its prefix op_"):
        unprefixed_field_name("queries", "ops")
