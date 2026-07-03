# Copyright (c) 2026 Kenneth Stott
# Canary: b3c4d5e6-f7a8-4b9c-0d1e-2f3a4b5c6d7e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for REQ-798: Cypher mutation transpilation + RLS injection.

Pure logic only — no I/O, no network, no DB, no docker.
Tests WriteTranslator (CREATE/DELETE/UPDATE), inject_rls_into_mutation,
MutationResult wrapping, and dialect-agnostic SQL output.
"""

from __future__ import annotations

import pytest

from provisa.cypher.label_map import CypherLabelMap, NodeMapping
from provisa.cypher.write_translator import (
    CypherWriteParseError,
    WriteTranslator,
    parse_cypher_write,
)
from provisa.compiler.mutation_gen import MutationResult, inject_rls_into_mutation


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

TABLE_ID_PERSON = 1
TABLE_ID_ORDER = 2


def _node(
    label: str,
    *,
    table_id: int = 1,
    schema: str = "public",
    table: str | None = None,
    catalog: str = "mycat",
    props: dict[str, str] | None = None,
) -> NodeMapping:
    tbl = table or label.lower() + "s"
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=table_id,
        source_id="test-pg",
        id_column="id",
        pk_columns=[],
        catalog_name=catalog,
        schema_name=schema,
        table_name=tbl,
        properties=props or {},
        physical_properties={},
    )


def _label_map(*nodes: NodeMapping) -> CypherLabelMap:
    return CypherLabelMap(
        nodes={n.label: n for n in nodes},
        relationships={},
    )


def _person_map() -> CypherLabelMap:
    return _label_map(
        _node(
            "Person",
            table_id=TABLE_ID_PERSON,
            schema="public",
            table="persons",
            catalog="mycat",
            props={"name": "name", "age": "age", "email": "email"},
        )
    )


def _order_map() -> CypherLabelMap:
    return _label_map(
        _node(
            "Order",
            table_id=TABLE_ID_ORDER,
            schema="sales",
            table="orders",
            catalog="mycat",
            props={"orderId": "order_id", "amount": "amount", "status": "status"},
        )
    )


def _make_delete_mutation(where_sql: str = '"id" = $1') -> MutationResult:
    return MutationResult(
        sql=f'DELETE FROM "public"."persons" WHERE {where_sql} RETURNING *',
        params=[42],
        mutation_type="delete",
        table_name="persons",
        source_id="test-pg",
        returning_columns=[],
    )


def _make_update_mutation(where_sql: str = '"id" = $1') -> MutationResult:
    return MutationResult(
        sql=f'UPDATE "public"."persons" SET "name" = $2 WHERE {where_sql} RETURNING "name"',
        params=[42, "Alice"],
        mutation_type="update",
        table_name="persons",
        source_id="test-pg",
        returning_columns=["name"],
    )


def _make_insert_mutation() -> MutationResult:
    return MutationResult(
        sql='INSERT INTO "public"."persons" ("name") VALUES ($1) RETURNING "name"',
        params=["Bob"],
        mutation_type="insert",
        table_name="persons",
        source_id="test-pg",
        returning_columns=["name"],
    )


# ---------------------------------------------------------------------------
# parse_cypher_write — CREATE
# ---------------------------------------------------------------------------


def test_parse_create_returns_create_kind():
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice', age: 30})")
    assert ast.kind == "create"


def test_parse_create_extracts_label():
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice'})")
    assert ast.label == "Person"


def test_parse_create_extracts_variable():
    ast = parse_cypher_write("CREATE (p:Person {name: 'Alice'})")
    assert ast.variable == "p"


def test_parse_create_string_prop():
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice'})")
    assert ast.props["name"] == "Alice"


def test_parse_create_int_prop():
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice', age: 30})")
    assert ast.props["age"] == 30


def test_parse_create_bool_prop():
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice', active: true})")
    assert ast.props["active"] is True


def test_parse_create_null_prop():
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice', email: null})")
    assert ast.props["email"] is None


# ---------------------------------------------------------------------------
# parse_cypher_write — DELETE
# ---------------------------------------------------------------------------


def test_parse_delete_returns_delete_kind():
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 1 DELETE n")
    assert ast.kind == "delete"


def test_parse_delete_extracts_where():
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 42 DELETE n")
    assert "42" in ast.where_expr


def test_parse_delete_variable_mismatch_raises():
    with pytest.raises(CypherWriteParseError, match="does not match"):
        parse_cypher_write("MATCH (n:Person) WHERE n.id = 1 DELETE m")


# ---------------------------------------------------------------------------
# parse_cypher_write — UPDATE
# ---------------------------------------------------------------------------


def test_parse_update_returns_update_kind():
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 1 SET n.name = 'Bob'")
    assert ast.kind == "update"


def test_parse_update_set_assignments():
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 1 SET n.name = 'Bob', n.age = 25")
    assert len(ast.set_assignments) == 2
    props = dict(ast.set_assignments)
    assert props["name"] == "Bob"
    assert props["age"] == 25


def test_parse_update_where_captured():
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.email = 'x@y.com' SET n.name = 'Bob'")
    assert "x@y.com" in ast.where_expr


def test_parse_unknown_raises():
    with pytest.raises(CypherWriteParseError):
        parse_cypher_write("MERGE (n:Person {id: 1})")


# ---------------------------------------------------------------------------
# parse_cypher_write — relationship writes rejected (REQ-665)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "query",
    [
        "CREATE (a:Person)-[r:KNOWS]->(b:Person)",
        "CREATE (a)-->(b)",
        "MATCH (a:Person)-[r:KNOWS]->(b) DELETE r",
        "MATCH (a:Person)-[r]-(b) SET r.weight = 5",
    ],
)
def test_parse_relationship_write_rejected(query):
    """Relationships are FK-derived, not stored edges — writing one is a hard error."""
    with pytest.raises(CypherWriteParseError, match="relationship"):
        parse_cypher_write(query)


@pytest.mark.parametrize(
    "query",
    [
        "CREATE (n:Person {name: 'a->b'})",  # arrow inside a string value
        "MATCH (n:Person) WHERE n.age = 3 SET n.note = 'x - y'",  # minus in a string
    ],
)
def test_parse_arrow_in_string_not_treated_as_relationship(query):
    """A `->` or `-` inside a scalar value must not be mistaken for an edge."""
    ast = parse_cypher_write(query)
    assert ast.kind in ("create", "update")


# ---------------------------------------------------------------------------
# writable_by column ACL on Cypher writes (REQ-663)
# ---------------------------------------------------------------------------


class _FakeTableMeta:
    def __init__(self, columns):
        self.columns = columns


def test_writable_by_denies_role_without_write_access():
    from provisa.cypher.write_translator import write_acl_error

    mapping = _person_map().nodes["Person"]
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice'})")
    table_meta = _FakeTableMeta([{"column_name": "name", "writable_by": ["admin"]}])
    err = write_acl_error(table_meta, ast, mapping, "analyst")
    assert err is not None and err[0] == 403


def test_writable_by_allows_permitted_role():
    from provisa.cypher.write_translator import write_acl_error

    mapping = _person_map().nodes["Person"]
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 1 SET n.name = 'Bob'")
    table_meta = _FakeTableMeta([{"column_name": "name", "writable_by": ["admin", "analyst"]}])
    assert write_acl_error(table_meta, ast, mapping, "analyst") is None


def test_writable_by_delete_is_not_gated():
    from provisa.cypher.write_translator import write_acl_error

    mapping = _person_map().nodes["Person"]
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 1 DELETE n")
    table_meta = _FakeTableMeta([{"column_name": "name", "writable_by": ["admin"]}])
    # DELETE carries no column writes — consistent with the GraphQL path (REQ-663).
    assert write_acl_error(table_meta, ast, mapping, "analyst") is None


# ---------------------------------------------------------------------------
# WriteTranslator — CREATE → INSERT
# ---------------------------------------------------------------------------


def test_translate_create_produces_insert():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice', age: 30})")
    sql = translator.translate(ast)
    assert sql.upper().startswith("INSERT INTO")


def test_translate_create_qualified_table_name():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice'})")
    sql = translator.translate(ast)
    # Qualified: catalog.schema.table
    assert '"mycat"."public"."persons"' in sql


def test_translate_create_column_in_cols():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice'})")
    sql = translator.translate(ast)
    assert '"name"' in sql


def test_translate_create_string_literal_quoted():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice'})")
    sql = translator.translate(ast)
    assert "'Alice'" in sql


def test_translate_create_int_literal_unquoted():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice', age: 30})")
    sql = translator.translate(ast)
    # Integer 30 must appear as bare number, not quoted
    assert " 30" in sql or "(30" in sql or ",30" in sql or "30)" in sql


def test_translate_create_null_literal():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("CREATE (n:Person {name: 'Alice', email: null})")
    sql = translator.translate(ast)
    assert "NULL" in sql.upper()


def test_translate_create_prop_mapping_applied():
    """orderId Cypher prop → order_id SQL column via mapping."""
    translator = WriteTranslator(_order_map())
    ast = parse_cypher_write("CREATE (o:Order {orderId: 99, amount: 199})")
    sql = translator.translate(ast)
    assert '"order_id"' in sql
    assert "orderId" not in sql


def test_translate_create_no_props_raises():
    translator = WriteTranslator(_person_map())
    # Build AST manually to bypass regex (empty props map)
    from provisa.cypher.write_translator import WriteAST

    ast = WriteAST(kind="create", label="Person", variable="n", props={})
    with pytest.raises(CypherWriteParseError, match="no properties"):
        translator.translate(ast)


def test_translate_create_unknown_label_raises():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("CREATE (n:Ghost {name: 'X'})")
    with pytest.raises(CypherWriteParseError, match="not registered"):
        translator.translate(ast)


# ---------------------------------------------------------------------------
# WriteTranslator — DELETE → DELETE FROM
# ---------------------------------------------------------------------------


def test_translate_delete_produces_delete_from():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 1 DELETE n")
    sql = translator.translate(ast)
    assert sql.upper().startswith("DELETE FROM")


def test_translate_delete_qualified_table():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 1 DELETE n")
    sql = translator.translate(ast)
    assert '"mycat"."public"."persons"' in sql


def test_translate_delete_where_clause_present():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 42 DELETE n")
    sql = translator.translate(ast)
    assert "WHERE" in sql.upper()
    assert "42" in sql


def test_translate_delete_prop_rewritten_to_column():
    """n.age → "age" in the WHERE clause."""
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.age > 18 DELETE n")
    sql = translator.translate(ast)
    assert '"age"' in sql
    # Cypher notation must be gone
    assert "n.age" not in sql


# ---------------------------------------------------------------------------
# WriteTranslator — UPDATE → UPDATE SET … WHERE
# ---------------------------------------------------------------------------


def test_translate_update_produces_update():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 1 SET n.name = 'Bob'")
    sql = translator.translate(ast)
    assert sql.upper().startswith("UPDATE")


def test_translate_update_set_clause():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 1 SET n.name = 'Bob'")
    sql = translator.translate(ast)
    assert "SET" in sql.upper()
    assert '"name"' in sql
    assert "'Bob'" in sql


def test_translate_update_where_clause():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 7 SET n.name = 'Bob'")
    sql = translator.translate(ast)
    assert "WHERE" in sql.upper()
    assert "7" in sql


def test_translate_update_where_prop_rewritten():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.email = 'x@y' SET n.name = 'Bob'")
    sql = translator.translate(ast)
    assert '"email"' in sql
    assert "n.email" not in sql


def test_translate_update_multiple_set_assignments():
    translator = WriteTranslator(_person_map())
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.id = 1 SET n.name = 'Bob', n.age = 25")
    sql = translator.translate(ast)
    assert '"name"' in sql
    assert '"age"' in sql
    assert "'Bob'" in sql
    assert "25" in sql


def test_translate_update_no_assignments_raises():
    from provisa.cypher.write_translator import WriteAST

    translator = WriteTranslator(_person_map())
    ast = WriteAST(
        kind="update", label="Person", variable="n", where_expr="n.id = 1", set_assignments=[]
    )
    with pytest.raises(CypherWriteParseError, match="no SET"):
        translator.translate(ast)


# ---------------------------------------------------------------------------
# MutationResult — wrapping
# ---------------------------------------------------------------------------


def test_mutation_result_fields_preserved():
    m = _make_delete_mutation()
    assert m.mutation_type == "delete"
    assert m.table_name == "persons"
    assert m.source_id == "test-pg"
    assert m.params == [42]
    assert m.returning_columns == []


def test_mutation_result_insert_type():
    m = _make_insert_mutation()
    assert m.mutation_type == "insert"
    assert "INSERT" in m.sql.upper()


def test_mutation_result_update_returning_columns():
    m = _make_update_mutation()
    assert "name" in m.returning_columns


# ---------------------------------------------------------------------------
# inject_rls_into_mutation — RLS predicate injection
# ---------------------------------------------------------------------------


def test_inject_rls_delete_prepends_filter():
    original = _make_delete_mutation()
    rls = {TABLE_ID_PERSON: "tenant_id = 'acme'"}
    result = inject_rls_into_mutation(original, TABLE_ID_PERSON, rls)
    assert "tenant_id = 'acme'" in result.sql


def test_inject_rls_delete_ands_to_existing_where():
    original = _make_delete_mutation('"id" = $1')
    rls = {TABLE_ID_PERSON: "tenant_id = 'acme'"}
    result = inject_rls_into_mutation(original, TABLE_ID_PERSON, rls)
    # Both the original predicate and the RLS filter must be present
    assert '"id" = $1' in result.sql
    assert "tenant_id = 'acme'" in result.sql
    # Must be joined with AND
    upper = result.sql.upper()
    assert "AND" in upper


def test_inject_rls_update_prepends_filter():
    original = _make_update_mutation()
    rls = {TABLE_ID_PERSON: "region = 'EU'"}
    result = inject_rls_into_mutation(original, TABLE_ID_PERSON, rls)
    assert "region = 'EU'" in result.sql


def test_inject_rls_update_sql_changes():
    original = _make_update_mutation()
    rls = {TABLE_ID_PERSON: "region = 'EU'"}
    result = inject_rls_into_mutation(original, TABLE_ID_PERSON, rls)
    assert result.sql != original.sql


def test_inject_rls_insert_is_noop():
    """INSERT mutations must not be modified — they have no WHERE clause."""
    original = _make_insert_mutation()
    rls = {TABLE_ID_PERSON: "tenant_id = 'acme'"}
    result = inject_rls_into_mutation(original, TABLE_ID_PERSON, rls)
    assert result.sql == original.sql


def test_inject_rls_no_matching_table_is_noop():
    original = _make_delete_mutation()
    rls = {TABLE_ID_ORDER: "tenant_id = 'acme'"}  # different table_id
    result = inject_rls_into_mutation(original, TABLE_ID_PERSON, rls)
    assert result.sql == original.sql


def test_inject_rls_preserves_params():
    original = _make_delete_mutation()
    rls = {TABLE_ID_PERSON: "tenant_id = 'acme'"}
    result = inject_rls_into_mutation(original, TABLE_ID_PERSON, rls)
    assert result.params == original.params


def test_inject_rls_preserves_metadata():
    original = _make_delete_mutation()
    rls = {TABLE_ID_PERSON: "tenant_id = 'acme'"}
    result = inject_rls_into_mutation(original, TABLE_ID_PERSON, rls)
    assert result.mutation_type == original.mutation_type
    assert result.table_name == original.table_name
    assert result.source_id == original.source_id
    assert result.returning_columns == original.returning_columns


def test_inject_rls_filter_wrapped_in_parens():
    original = _make_delete_mutation()
    rls = {TABLE_ID_PERSON: "tenant_id = 'acme' OR tenant_id = 'beta'"}
    result = inject_rls_into_mutation(original, TABLE_ID_PERSON, rls)
    # The RLS filter must be parenthesised so OR doesn't escape
    assert "(tenant_id = 'acme' OR tenant_id = 'beta')" in result.sql


# ---------------------------------------------------------------------------
# End-to-end: parse → translate → inject RLS
# ---------------------------------------------------------------------------


def test_e2e_create_no_rls_injection():
    """Full pipeline: CREATE Cypher → SQL INSERT; RLS injection is a no-op for INSERT."""
    label_map = _person_map()
    translator = WriteTranslator(label_map)
    ast = parse_cypher_write("CREATE (n:Person {name: 'Carol', age: 28})")
    sql = translator.translate(ast)

    mutation = MutationResult(
        sql=sql,
        params=[],
        mutation_type="insert",
        table_name="persons",
        source_id="test-pg",
        returning_columns=["name", "age"],
    )
    rls = {TABLE_ID_PERSON: "tenant_id = 'x'"}
    result = inject_rls_into_mutation(mutation, TABLE_ID_PERSON, rls)

    assert result.sql == sql  # INSERT unchanged
    assert "INSERT INTO" in sql.upper()
    assert "'Carol'" in sql
    assert "28" in sql


def test_e2e_delete_with_rls():
    """Full pipeline: MATCH-DELETE Cypher → SQL DELETE → RLS injected."""
    label_map = _person_map()
    translator = WriteTranslator(label_map)
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.age > 60 DELETE n")
    sql = translator.translate(ast)

    mutation = MutationResult(
        sql=sql,
        params=[],
        mutation_type="delete",
        table_name="persons",
        source_id="test-pg",
        returning_columns=[],
    )
    rls = {TABLE_ID_PERSON: "tenant_id = 'acme'"}
    result = inject_rls_into_mutation(mutation, TABLE_ID_PERSON, rls)

    assert "DELETE FROM" in result.sql.upper()
    assert '"age"' in result.sql
    assert "60" in result.sql
    assert "tenant_id = 'acme'" in result.sql
    assert "AND" in result.sql.upper()


def test_e2e_update_with_rls():
    """Full pipeline: MATCH-SET Cypher → SQL UPDATE → RLS injected."""
    label_map = _person_map()
    translator = WriteTranslator(label_map)
    ast = parse_cypher_write("MATCH (n:Person) WHERE n.email = 'old@x.com' SET n.name = 'New'")
    sql = translator.translate(ast)

    mutation = MutationResult(
        sql=sql,
        params=[],
        mutation_type="update",
        table_name="persons",
        source_id="test-pg",
        returning_columns=["name"],
    )
    rls = {TABLE_ID_PERSON: "region = 'EU'"}
    result = inject_rls_into_mutation(mutation, TABLE_ID_PERSON, rls)

    assert "UPDATE" in result.sql.upper()
    assert '"name"' in result.sql
    assert "'New'" in result.sql
    assert '"email"' in result.sql
    assert "region = 'EU'" in result.sql
    assert "AND" in result.sql.upper()
