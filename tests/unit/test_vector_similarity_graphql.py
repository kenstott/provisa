# Copyright (c) 2026 Kenneth Stott
# Canary: cb1b6774-5e7e-472e-a464-113159da31ea
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GraphQL's ``similar: {column, text | vector}`` and ``_similarity`` (REQ-430, REQ-423).

A table with an embedding column the role is served takes ``similar``; the field compiles to the
same ``cosine_similarity`` call SQL writes -- the query a bound value -- ordering the rows most
like the query first, and ``_similarity`` reads its score. Governance completes the call as it
does any statement's.
"""

# Requirements: REQ-423, REQ-430

from __future__ import annotations

import pytest

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.stage2 import GovernanceContext, apply_governance


def _schema(*, analyst_sees_vec: bool = True, source_type: str = ""):
    vec_roles = ["admin", "analyst"] if analyst_sees_vec else ["admin"]
    tables = [
        {
            "id": 1,
            "source_id": "pg",
            "domain_id": "shop",
            "schema_name": "public",
            "table_name": "docs",
            "columns": [
                {"column_name": "id", "visible_to": ["admin", "analyst"]},
                {"column_name": "title", "visible_to": ["admin", "analyst"]},
                {
                    "column_name": "vec",
                    "visible_to": vec_roles,
                    "embedding": True,
                    "embedding_model": "mini",
                },
            ],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        },
        {
            "id": 2,
            "source_id": "pg",
            "domain_id": "shop",
            "schema_name": "public",
            "table_name": "tags",
            "columns": [{"column_name": "id", "visible_to": ["admin", "analyst"]}],
            "write_ops": [],
            "write_returns_rows": False,
            "write_refused_forms": [],
        },
    ]
    types = {
        1: [
            ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
            ColumnMetadata(column_name="title", data_type="varchar", is_nullable=True),
            ColumnMetadata(column_name="vec", data_type="array(float)", is_nullable=True),
        ],
        2: [ColumnMetadata(column_name="id", data_type="integer", is_nullable=False)],
    }

    def build(role_id: str):
        si = SchemaInput(
            tables=tables,
            relationships=[],
            column_types=types,
            naming_rules=[],
            role={"id": role_id, "capabilities": ["query_development"], "domain_access": ["*"]},
            domains=[{"id": "shop", "description": "Shop"}],
            source_types={"pg": source_type} if source_type else {},
        )
        return generate_schema(si), build_context(si)

    return build


def _compiled(query: str, role_id: str = "admin", **kw):
    schema, ctx = _schema(**kw)(role_id)
    (compiled,) = compile_query(parse_query(schema, query, ctx=ctx), ctx)
    return compiled


def test_a_table_with_an_embedding_column_takes_similar_and_reads_its_score():
    schema, _ = _schema()("admin")
    assert schema.query_type is not None
    docs = schema.query_type.fields["docs"]
    similar = docs.args["similar"].type
    assert {f for f in similar.fields} == {"column", "text", "vector"}  # pyright: ignore[reportAttributeAccessIssue]
    assert "_similarity" in docs.type.of_type.of_type.fields  # pyright: ignore[reportAttributeAccessIssue]
    tags = schema.query_type.fields["tags"]
    assert "similar" not in tags.args


def test_a_table_a_subscription_also_serves_takes_one_similar_type():  # REQ-258
    # A PostgreSQL table is served as a subscription too, with the same arguments as its query;
    # both take the one input type, which a schema may name only once.
    schema, _ = _schema(source_type="postgresql")("admin")
    assert schema.query_type is not None and schema.subscription_type is not None
    queried = schema.query_type.fields["docs"].args["similar"].type
    assert schema.subscription_type.fields["docs"].args["similar"].type is queried


def test_a_role_not_served_the_column_is_offered_no_search():  # REQ-426
    schema, _ = _schema(analyst_sees_vec=False)("analyst")
    assert schema.query_type is not None
    assert "similar" not in schema.query_type.fields["docs"].args


def test_a_text_search_ranks_the_rows_by_its_score():
    compiled = _compiled(
        '{ docs(similar: {column: vec, text: "red shoes"}, limit: 3) { id _similarity } }'
    )
    assert 'cosine_similarity("vec", $1) AS "_similarity"' in compiled.sql
    assert 'ORDER BY cosine_similarity("vec", $1) DESC' in compiled.sql
    assert compiled.params[0] == "red shoes"
    assert compiled.api_args == {}


def test_a_vector_search_binds_the_vector_and_keeps_the_fields_own_order():
    compiled = _compiled(
        "{ docs(similar: {column: vec, vector: [1, 0, 0]}, order_by: [{id: asc}]) { id } }"
    )
    assert compiled.params[0] == [1.0, 0.0, 0.0]
    assert 'ORDER BY cosine_similarity("vec", $1) DESC, "id" ASC' in compiled.sql


def test_a_search_needs_a_text_or_a_vector_exactly():
    with pytest.raises(ValueError, match="exactly one"):
        _compiled("{ docs(similar: {column: vec}) { id } }")
    with pytest.raises(ValueError, match="exactly one"):
        _compiled('{ docs(similar: {column: vec, text: "a", vector: [1, 0, 0]}) { id } }')


def test_the_score_without_a_search_is_refused():
    with pytest.raises(ValueError, match="give docs a similar argument"):
        _compiled("{ docs { id _similarity } }")


def test_the_compiled_search_is_governed_as_sql_is():
    compiled = _compiled('{ docs(similar: {column: vec, text: "x"}) { id } }')
    gov = GovernanceContext()
    gov.role_id = "admin"
    gov.table_map = {"docs": 1, "shop.docs": 1, "public.docs": 1}
    gov.all_columns = {1: [("id", "integer"), ("title", "varchar"), ("vec", "array(float)")]}
    gov.visible_columns = {1: None}
    gov.embedding_columns = {1: {"vec": "mini"}}
    gov.embedding_dims = {"mini": 3}
    governed = apply_governance(compiled.sql, gov)
    assert "COSINE_SIMILARITY(\"vec\", $1, 'mini', 3)" in governed


def test_an_embedding_column_is_a_list_of_its_components_not_json():
    """REQ-424: an embedding is exempt from REQ-1965's JSON rule: its field is a list of its
    element type, as it was before native arrays became JSON columns."""
    schema, _ = _schema()("admin")
    assert schema.query_type is not None
    row = schema.query_type.fields["docs"].type.of_type.of_type  # pyright: ignore[reportAttributeAccessIssue]
    assert str(row.fields["vec"].type) == "[Float!]"
