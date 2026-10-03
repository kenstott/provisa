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
Tests WriteTranslator (CREATE/DELETE/UPDATE), the one write admission the translated statement
passes (tests/write_governance.py: its columns' writable_by and the role's row filter, applied
by the governance stage every surface's write goes through), MutationResult wrapping, and
dialect-agnostic SQL output.
"""

from __future__ import annotations

import pytest

from provisa.cypher.label_map import CypherLabelMap, NodeMapping
from provisa.cypher.write_translator import (
    CypherWriteParseError,
    WriteTranslator,
    parse_cypher_write,
)
from provisa.compiler.mutation_gen import MutationResult
from provisa.compiler.write_admission import WriteNotAdmitted
from provisa.security.mutation_authz import ColumnNotWritable
from tests.write_governance import admitted, write_governance


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


_PERSON_COLUMNS = ["id", "name", "age", "email", "tenant_id", "region"]


def _person_gov(*, rls: dict[int, str] | None = None, writable: list[str] | None = None):
    """Governance over the persons table, both as the translator addresses it and as the
    compiled helpers below do."""
    return write_governance(
        {
            "public.persons": (TABLE_ID_PERSON, _PERSON_COLUMNS),
            "mycat.public.persons": (TABLE_ID_PERSON, _PERSON_COLUMNS),
        },
        rls=rls,
        writable=None if writable is None else {TABLE_ID_PERSON: writable},
    )


def _translated(cypher: str) -> str:
    return WriteTranslator(_person_map()).translate(parse_cypher_write(cypher))


def test_writable_by_denies_role_without_write_access():
    with pytest.raises(ColumnNotWritable, match="column 'name'"):
        admitted(_translated("CREATE (n:Person {name: 'Alice'})"), _person_gov(writable=["id"]))


def test_writable_by_allows_permitted_role():
    sql = _translated("MATCH (n:Person) WHERE n.id = 1 SET n.name = 'Bob'")
    assert admitted(sql, _person_gov(writable=["name"])) == sql


def test_a_delete_removes_whole_rows_and_needs_every_column():
    """A DELETE removes every column of the rows it matches, so the role must be named on every
    column (REQ-663, as ruled 2026-10-02) — on Cypher as on every surface."""
    sql = _translated("MATCH (n:Person) WHERE n.id = 1 DELETE n")
    with pytest.raises(ColumnNotWritable):
        admitted(sql, _person_gov(writable=["name"]))
    assert admitted(sql, _person_gov()) == sql


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
# The role's row filter, applied by the admission
# ---------------------------------------------------------------------------

_ACME = {TABLE_ID_PERSON: "tenant_id = 'acme'"}


def test_a_filtered_delete_carries_the_filter():
    original = _make_delete_mutation()
    governed = admitted(original.sql, _person_gov(rls=_ACME), original.params)
    assert '"persons"."tenant_id" = \'acme\'' in governed


def test_the_filter_is_anded_to_the_existing_where():
    original = _make_delete_mutation('"id" = $1')
    governed = admitted(original.sql, _person_gov(rls=_ACME), original.params)
    assert '"id" = $1' in governed
    assert "tenant_id\" = 'acme'" in governed
    assert " AND " in governed.upper()


def test_a_filtered_update_carries_the_filter():
    original = _make_update_mutation()
    governed = admitted(
        original.sql, _person_gov(rls={TABLE_ID_PERSON: "region = 'EU'"}), original.params
    )
    assert "\"region\" = 'EU'" in governed
    assert governed != original.sql


def test_an_insert_carries_no_where_and_is_checked_against_the_filter():
    """An INSERT has no rows to narrow: its new row is checked against the filter instead —
    inside it is written as given, outside it is refused, undecidable it is refused."""
    original = _make_insert_mutation()
    with pytest.raises(WriteNotAdmitted, match="cannot be decided before writing"):
        # the filter reads tenant_id, which this INSERT does not supply
        admitted(original.sql, _person_gov(rls=_ACME), original.params)
    sql = 'INSERT INTO "public"."persons" ("name", "tenant_id") VALUES ($1, $2)'
    assert admitted(sql, _person_gov(rls=_ACME), ["Bob", "acme"]) == sql
    with pytest.raises(WriteNotAdmitted, match="outside role"):
        admitted(sql, _person_gov(rls=_ACME), ["Bob", "beta"])


def test_a_rule_on_another_table_leaves_the_statement_as_given():
    original = _make_delete_mutation()
    gov = _person_gov(rls={TABLE_ID_ORDER: "tenant_id = 'acme'"})
    assert admitted(original.sql, gov, original.params) == original.sql


def test_the_filter_is_parenthesised_so_an_or_does_not_escape():
    original = _make_delete_mutation()
    gov = _person_gov(rls={TABLE_ID_PERSON: "tenant_id = 'acme' OR tenant_id = 'beta'"})
    governed = admitted(original.sql, gov, original.params)
    assert (
        'AND ("persons"."tenant_id" = \'acme\' OR "persons"."tenant_id" = \'beta\')' in governed
    ), governed


# ---------------------------------------------------------------------------
# End-to-end: parse → translate → admit
# ---------------------------------------------------------------------------


def test_e2e_create_inside_the_filter_is_written_as_translated():
    sql = _translated("CREATE (n:Person {name: 'Carol', age: 28, tenant_id: 'x'})")
    assert admitted(sql, _person_gov(rls={TABLE_ID_PERSON: "tenant_id = 'x'"})) == sql
    assert "INSERT INTO" in sql.upper()
    assert "'Carol'" in sql
    assert "28" in sql


def test_e2e_delete_with_the_filter():
    sql = _translated("MATCH (n:Person) WHERE n.age > 60 DELETE n")
    governed = admitted(sql, _person_gov(rls=_ACME))
    assert "DELETE FROM" in governed.upper()
    assert '"age"' in governed
    assert "60" in governed
    assert "tenant_id\" = 'acme'" in governed
    assert "AND" in governed.upper()


def test_e2e_update_with_the_filter():
    sql = _translated("MATCH (n:Person) WHERE n.email = 'old@x.com' SET n.name = 'New'")
    governed = admitted(sql, _person_gov(rls={TABLE_ID_PERSON: "region = 'EU'"}))
    assert "UPDATE" in governed.upper()
    assert '"name"' in governed
    assert "'New'" in governed
    assert '"email"' in governed
    assert "region\" = 'EU'" in governed
    assert "AND" in governed.upper()


# ---------------------------------------------------------------------------
# Parameters: an unquoted $name is a value the request supplies, never text
# ---------------------------------------------------------------------------


def test_a_parameter_in_a_create_is_bound_not_written_as_its_name():
    from provisa.cypher.write_translator import bind_write_params

    sql = _translated("CREATE (n:Person {name: $name, age: $age, email: 'pay $x'})")
    bound, values = bind_write_params(sql, {"name": "Ann", "age": 3})
    assert bound == (
        'INSERT INTO "mycat"."public"."persons" ("name", "age", "email") '
        "VALUES ($1, $2, 'pay $x')"  # a quoted '$x' is text and stays so
    )
    assert values == ["Ann", 3]


def test_parameters_in_set_and_where_are_bound_in_order_and_a_repeat_is_one_value():
    from provisa.cypher.write_translator import bind_write_params

    sql = _translated("MATCH (n:Person) WHERE n.id = $id AND n.name <> $name SET n.name = $name")
    bound, values = bind_write_params(sql, {"id": 1, "name": "B", "unused": 9})
    assert bound == (
        'UPDATE "mycat"."public"."persons" SET "name" = $1 WHERE "id" = $2 AND "name" <> $1'
    )
    assert values == ["B", 1]


def test_a_parameter_the_request_does_not_supply_is_refused_by_name():
    from provisa.cypher.params import CypherParamError
    from provisa.cypher.write_translator import bind_write_params

    sql = _translated("MATCH (n:Person) WHERE n.id = $id DELETE n")
    with pytest.raises(CypherParamError, match=r"\['id'\]"):
        bind_write_params(sql, {})


def test_a_bound_value_is_checked_against_the_row_filter_like_a_literal():
    from provisa.cypher.write_translator import bind_write_params

    sql, values = bind_write_params(
        _translated("CREATE (n:Person {name: $name, tenant_id: $tenant})"),
        {"name": "Bob", "tenant": "beta"},
    )
    with pytest.raises(WriteNotAdmitted, match="outside role"):
        admitted(sql, _person_gov(rls=_ACME), values)
    sql, values = bind_write_params(
        _translated("CREATE (n:Person {name: $name, tenant_id: $tenant})"),
        {"name": "Bob", "tenant": "acme"},
    )
    assert admitted(sql, _person_gov(rls=_ACME), values) == sql
