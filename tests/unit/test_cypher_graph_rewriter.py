# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2f5a9c-3b4e-4a8f-9c2d-1e5b3f7a9d2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/cypher/graph_rewriter.py."""

import sqlglot
import sqlglot.expressions as exp

from provisa.cypher.graph_rewriter import apply_graph_rewrites
from provisa.cypher.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.cypher.translator import GraphVarKind


def _make_domain_label_map() -> CypherLabelMap:
    """Label map with two types in a 'PetStore' domain."""
    inquiries = NodeMapping(
        label="PetStore:Inquiries",
        type_name="PetStore_Inquiries",
        domain_label="PetStore",
        table_label="Inquiries",
        table_id=1,
        source_id="sqlite-petstore",
        id_column="inquiry_id",
        pk_columns=[],
        catalog_name="sqlite",
        schema_name="petstore",
        table_name="inquiries",
        properties={"inquiry_id": "inquiry_id", "name": "name", "email": "email"},
    )
    products = NodeMapping(
        label="PetStore:Products",
        type_name="PetStore_Products",
        domain_label="PetStore",
        table_label="Products",
        table_id=2,
        source_id="sqlite-petstore",
        id_column="product_id",
        pk_columns=[],
        catalog_name="sqlite",
        schema_name="petstore",
        table_name="products",
        properties={"product_id": "product_id", "name": "name"},
    )
    return CypherLabelMap(
        nodes={"PetStore_Inquiries": inquiries, "PetStore_Products": products},
        relationships={},
        domains={"PetStore": ["PetStore_Inquiries", "PetStore_Products"]},
        nodes_by_table={"Inquiries": ["PetStore_Inquiries"], "Products": ["PetStore_Products"]},
    )


def _make_label_map() -> CypherLabelMap:
    person_meta = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label=None,
        table_label="Person",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    return CypherLabelMap(nodes={"Person": person_meta}, relationships={})


def _parse_sql(sql: str) -> exp.Select:
    return sqlglot.parse_one(sql, dialect="trino")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_scalar_column_untouched():
    sql = 'SELECT n."name" FROM "public"."persons" AS n'
    ast = _parse_sql(sql)
    lm = _make_label_map()
    result = apply_graph_rewrites(ast, {}, lm)
    result_sql = result.sql(dialect="trino")
    assert "name" in result_sql
    # No ROW wrapping
    assert "ROW" not in result_sql.upper()


def test_node_variable_wrapped():
    sql = 'SELECT n AS n FROM "postgresql"."public"."persons" AS n'
    ast = _parse_sql(sql)
    lm = _make_label_map()
    graph_vars = {"n": GraphVarKind.NODE}
    result = apply_graph_rewrites(ast, graph_vars, lm)
    result_sql = result.sql(dialect="trino")
    # Should contain CAST and ROW or JSON wrapping
    assert "CAST" in result_sql.upper() or "ROW" in result_sql.upper() or "JSON" in result_sql.upper()


def test_mixed_scalar_and_node():
    sql = 'SELECT n."age" AS age, n AS n FROM "postgresql"."public"."persons" AS n'
    ast = _parse_sql(sql)
    lm = _make_label_map()
    graph_vars = {"n": GraphVarKind.NODE}
    result = apply_graph_rewrites(ast, graph_vars, lm)
    result_sql = result.sql(dialect="trino")
    # age column should remain untouched (no ROW/CAST around just age)
    assert "age" in result_sql


def test_no_graph_vars_no_change():
    sql = 'SELECT n."name", n."age" FROM "postgresql"."public"."persons" AS n'
    ast = _parse_sql(sql)
    lm = _make_label_map()
    result = apply_graph_rewrites(ast, {}, lm)
    result_sql = result.sql(dialect="trino")
    assert "ROW" not in result_sql.upper()
    assert "CAST" not in result_sql.upper()


def test_no_duplicate_id_key_when_column_named_id():
    """id_column != 'id' but table has a column named 'id' — must not emit duplicate 'id' JSON key."""
    inquiry_meta = NodeMapping(
        label="PetStore:Inquiries",
        type_name="PetStore_Inquiries",
        domain_label="PetStore",
        table_label="Inquiries",
        table_id=2,
        source_id="sqlite-petstore",
        id_column="inquiry_id",
        pk_columns=[],
        catalog_name="sqlite",
        schema_name="petstore",
        table_name="inquiries",
        properties={"inquiry_id": "inquiry_id", "id": "id", "name": "name"},
    )
    lm = CypherLabelMap(
        nodes={"PetStore_Inquiries": inquiry_meta},
        relationships={},
        domains={"PetStore": ["PetStore_Inquiries"]},
        nodes_by_table={"Inquiries": ["PetStore_Inquiries"]},
    )
    sql = 'SELECT n AS n FROM "sqlite"."petstore"."inquiries" AS n'
    ast = _parse_sql(sql)
    graph_vars = {"n": GraphVarKind.NODE}
    result = apply_graph_rewrites(ast, graph_vars, lm)
    result_sql = result.sql(dialect="trino")
    # Count occurrences of 'id' as a JSON key — should appear exactly once
    import re
    id_key_count = len(re.findall(r"'id'", result_sql))
    assert id_key_count == 1, f"Duplicate 'id' JSON key found in: {result_sql}"


def test_domain_node_props_in_json():
    """Domain-only MATCH (n:PetStore) RETURN n must include domain props in JSON output."""
    from provisa.cypher.parser import parse_cypher
    from provisa.cypher.translator import cypher_to_sql

    lm = _make_domain_label_map()
    ast = parse_cypher("MATCH (n:PetStore) RETURN n LIMIT 5")
    sql_ast, _, graph_vars = cypher_to_sql(ast, lm, {})
    result = apply_graph_rewrites(sql_ast, graph_vars, lm)
    result_sql = result.sql(dialect="trino")

    # Should contain property keys beyond just 'id' and 'label'
    assert "'name'" in result_sql, f"Missing 'name' property in domain JSON: {result_sql}"
    assert "JSON_OBJECT" in result_sql.upper(), f"Expected JSON_OBJECT in: {result_sql}"
