# Copyright (c) 2026 Kenneth Stott
# Canary: 5c2a8e4f-9b7d-4f3a-8c1e-2d5b7f9a3c6e
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for Cypher Mutations.

REQ-666 — `CREATE (n:Label {props})` is translated to
`INSERT INTO catalog.schema.table (columns) VALUES (values)` with
property-to-column mapping and scalar type coercion.

REQ-667 — `MATCH (n:Label) WHERE ... DELETE n` is translated to
`DELETE FROM catalog.schema.table WHERE ...`, reusing the WHERE clause
translation from the read (MATCH) path.

REQ-668 — `MATCH (n:Label) WHERE ... SET n.prop = val` is translated to
`UPDATE catalog.schema.table SET column = value WHERE ...`. Property-to-column
mapping applies domain-prefix stripping; multiple SET clauses compose as
comma-separated column updates.

REQ-670 — Cypher write endpoints return the number of rows affected (rows
inserted for CREATE, rows updated for SET, rows deleted for DELETE) via an
`affected_rows` field in the JSON response body.
"""

from __future__ import annotations

import asyncio
import os

import httpx
import pytest
from pytest_bdd import given, when, then, scenarios, parsers

from provisa.cypher.parser import parse_cypher
from provisa.cypher.label_map import (
    CypherLabelMap,
    NodeMapping,
    RelationshipMapping,
)

# The WriteTranslator turns Cypher write clauses (CREATE/MERGE/SET/DELETE) into DML SQL.
from provisa.cypher.write_translator import WriteTranslator


scenarios("req_666.feature")
scenarios("req_667.feature")
scenarios("req_668.feature")
scenarios("req_670.feature")


def _make_write_label_map() -> CypherLabelMap:
    """Build a CypherLabelMap with a single registered Person label.

    Properties map graph-idiomatic names to physical columns; ``age`` is an
    integer column so the translator must coerce the Cypher scalar ``30`` to a
    numeric literal rather than a quoted string.
    """
    person_meta = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label=None,
        table_label="Person",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=["id"],
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"name": "name", "age": "age"},
    )
    company_meta = NodeMapping(
        label="Company",
        type_name="Company",
        domain_label=None,
        table_label="Company",
        table_id=2,
        source_id="pg-main",
        id_column="id",
        pk_columns=["id"],
        catalog_name="postgresql",
        schema_name="public",
        table_name="companies",
        properties={"name": "name"},
    )
    works_at_rel = RelationshipMapping(
        rel_type="WORKS_AT",
        source_label="Person",
        target_label="Company",
        join_source_column="company_id",
        join_target_column="id",
        field_name="works_at",
    )
    return CypherLabelMap(
        nodes={"Person": person_meta, "Company": company_meta},
        relationships={"WORKS_AT": works_at_rel},
    )


def _make_set_label_map() -> CypherLabelMap:
    """Build a CypherLabelMap whose Cypher property names differ from physical columns.

    The graph client uses domain-idiomatic property names (``fullName``,
    ``ageYears``) which must be mapped to the physical column names
    (``full_name``, ``age_years``) on the UPDATE path. This exercises
    property-to-column mapping with domain-prefix stripping.
    """
    person_meta = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label=None,
        table_label="Person",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=["id"],
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"fullName": "full_name", "ageYears": "age_years", "id": "id"},
    )
    return CypherLabelMap(
        nodes={"Person": person_meta},
        relationships={},
    )


def _coerce_to_sql(result) -> tuple[str, object]:
    """Normalize a WriteTranslator return value to (sql_text, params)."""
    sql_obj = result
    params = None
    if isinstance(result, tuple):
        sql_obj = result[0]
        params = result[1] if len(result) > 1 else None
    if hasattr(sql_obj, "sql"):
        # SQLGlot expression or wrapper object.
        sql_text = sql_obj.sql(dialect="trino") if callable(sql_obj.sql) else str(sql_obj.sql)
    else:
        sql_text = str(sql_obj)
    return sql_text, params


@given("a Cypher CREATE statement with a registered label and scalar properties")
def given_cypher_create_statement(shared_data):
    label_map = _make_write_label_map()
    cypher = "CREATE (n:Person {name: 'Alice', age: 30})"
    ast = parse_cypher(cypher)

    assert "Person" in label_map.nodes, "Person label must be registered in the label map"
    assert ast is not None, "parse_cypher must return a CypherAST for the CREATE statement"

    shared_data["label_map"] = label_map
    shared_data["cypher"] = cypher
    shared_data["ast"] = ast


@when("the WriteTranslator processes the statement")
def when_write_translator_processes(shared_data):
    translator = WriteTranslator(shared_data["label_map"])
    result = translator.translate(shared_data["ast"])
    sql_text, params = _coerce_to_sql(result)

    assert sql_text, "WriteTranslator must produce non-empty SQL output"

    shared_data["sql"] = sql_text
    shared_data["params"] = params


@then("the output is an INSERT INTO SQL statement with correct column-value pairs")
def then_insert_into_with_columns(shared_data):
    sql = shared_data["sql"]
    upper = sql.upper()

    # Statement form.
    assert "INSERT INTO" in upper, f"expected INSERT INTO, got: {sql}"
    assert "VALUES" in upper, f"expected VALUES clause, got: {sql}"

    # Fully-qualified target table catalog.schema.table.
    assert "PERSONS" in upper, f"target table 'persons' missing: {sql}"
    assert "PUBLIC" in upper, f"schema 'public' missing: {sql}"
    assert "POSTGRESQL" in upper, f"catalog 'postgresql' missing: {sql}"

    # Property names mapped to physical columns.
    assert "NAME" in upper, f"column 'name' missing: {sql}"
    assert "AGE" in upper, f"column 'age' missing: {sql}"

    # Scalar values present.
    assert "Alice" in sql, f"value 'Alice' missing: {sql}"
    assert "30" in sql, f"value 30 missing: {sql}"


@then("type coercion is applied to align Cypher scalar types with column types")
def then_type_coercion_applied(shared_data):
    sql = shared_data["sql"]

    # The integer 'age' value must be emitted as a numeric literal, never quoted.
    assert "'30'" not in sql, f"integer value 30 must not be quoted: {sql}"
    assert "30" in sql, f"numeric literal 30 missing: {sql}"

    # The string 'name' value must be emitted as a quoted string literal.
    assert "'Alice'" in sql, f"string value 'Alice' must be quoted: {sql}"


# ---------------------------------------------------------------------------
# REQ-667 — MATCH ... WHERE ... DELETE n  →  DELETE FROM ... WHERE ...
# ---------------------------------------------------------------------------


@given("a Cypher MATCH-DELETE statement targeting a registered label")
def given_cypher_match_delete_statement(shared_data):
    label_map = _make_write_label_map()
    # WHERE predicate compiled from the MATCH pattern must be reused on the
    # DELETE path: n.age > 21 must compile to a standard SQL WHERE predicate.
    cypher = "MATCH (n:Person) WHERE n.age > 21 DELETE n"
    ast = parse_cypher(cypher)

    assert "Person" in label_map.nodes, "Person label must be registered in the label map"
    assert ast is not None, "parse_cypher must return a CypherAST for the MATCH-DELETE statement"

    shared_data["label_map"] = label_map
    shared_data["cypher"] = cypher
    shared_data["ast"] = ast


@then(
    "the output is a DELETE FROM SQL statement with the WHERE clause from the MATCH pattern"
)
def then_delete_from_with_where(shared_data):
    sql = shared_data["sql"]
    upper = sql.upper()

    # Statement form.
    assert "DELETE FROM" in upper, f"expected DELETE FROM, got: {sql}"

    # Fully-qualified target table catalog.schema.table.
    assert "PERSONS" in upper, f"target table 'persons' missing: {sql}"
    assert "PUBLIC" in upper, f"schema 'public' missing: {sql}"
    assert "POSTGRESQL" in upper, f"catalog 'postgresql' missing: {sql}"

    # WHERE clause reused from the MATCH translator: predicate must compile to
    # a standard SQL WHERE predicate before deletion.
    assert "WHERE" in upper, f"expected WHERE clause, got: {sql}"
    assert "AGE" in upper, f"WHERE predicate column 'age' missing: {sql}"
    assert ">" in sql, f"comparison operator '>' missing from predicate: {sql}"
    assert "21" in sql, f"predicate literal 21 missing: {sql}"

    # A DELETE must never carry an INSERT/VALUES payload — it operates on the
    # filtered rows only.
    assert "INSERT INTO" not in upper, f"DELETE statement must not contain INSERT: {sql}"
    assert "VALUES" not in upper, f"DELETE statement must not contain VALUES: {sql}"


# ---------------------------------------------------------------------------
# REQ-668 — MATCH ... WHERE ... SET n.prop = val
#           → UPDATE catalog.schema.table SET column = value WHERE ...
# ---------------------------------------------------------------------------


@given("a Cypher MATCH-SET statement with multiple property assignments")
def given_cypher_match_set_statement(shared_data):
    label_map = _make_set_label_map()
    # Two property assignments must compose into comma-separated SET clauses.
    # The Cypher-idiomatic property names (fullName, ageYears) must be mapped to
    # their physical column names (full_name, age_years) via domain-prefix
    # stripping. The WHERE predicate (n.id = 1) is reused from the MATCH path.
    cypher = "MATCH (n:Person) WHERE n.id = 1 SET n.fullName = 'Bob', n.ageYears = 40"
    ast = parse_cypher(cypher)

    assert "Person" in label_map.nodes, "Person label must be registered in the label map"
    assert ast is not None, "parse_cypher must return a CypherAST for the MATCH-SET statement"

    shared_data["label_map"] = label_map
    shared_data["cypher"] = cypher
    shared_data["ast"] = ast


@then("the output is an UPDATE SQL statement with comma-separated SET clauses")
def then_update_with_comma_separated_set(shared_data):
    sql = shared_data["sql"]
    upper = sql.upper()

    # Statement form.
    assert "UPDATE" in upper, f"expected UPDATE statement, got: {sql}"
    assert "SET" in upper, f"expected SET clause, got: {sql}"

    # Fully-qualified target table catalog.schema.table.
    assert "PERSONS" in upper, f"target table 'persons' missing: {sql}"
    assert "PUBLIC" in upper, f"schema 'public' missing: {sql}"
    assert "POSTGRESQL" in upper, f"catalog 'postgresql' missing: {sql}"

    # WHERE clause reused from the MATCH pattern predicate.
    assert "WHERE" in upper, f"expected WHERE clause, got: {sql}"
    assert "1" in sql, f"WHERE predicate literal 1 missing: {sql}"

    # Multiple assignments compose as comma-separated column updates: the SET
    # segment between SET and WHERE must contain a comma separating the two
    # column assignments.
    set_idx = upper.index("SET")
    where_idx = upper.index("WHERE", set_idx)
    set_segment = sql[set_idx + len("SET"):where_idx]
    assert "," in set_segment, f"SET clauses must be comma-separated: {set_segment!r}"
    assert set_segment.count("=") >= 2, f"expected two assignments in SET: {set_segment!r}"

    # Assignment values must be valid SQL expressions/literals.
    assert "'Bob'" in sql, f"string value 'Bob' missing: {sql}"
    assert "40" in sql, f"numeric value 40 missing: {sql}"

    # An UPDATE must not carry an INSERT/VALUES or DELETE payload.
    assert "INSERT INTO" not in upper, f"UPDATE statement must not contain INSERT: {sql}"
    assert "DELETE FROM" not in upper, f"UPDATE statement must not contain DELETE: {sql}"


@then("domain-prefix stripping maps Cypher property names to physical column names")
def then_domain_prefix_stripping_maps_columns(shared_data):
    sql = shared_data["sql"]
    upper = sql.upper()

    # Physical column names from the NodeMapping.properties values must appear.
    assert "FULL_NAME" in upper, f"physical column 'full_name' missing: {sql}"
    assert "AGE_YEARS" in upper, f"physical column 'age_years' missing: {sql}"

    # The Cypher-idiomatic (camelCase) property names must NOT leak into the SQL:
    # they must have been mapped to physical column names.
    assert "FULLNAME" not in upper, f"Cypher property 'fullName' must be mapped, not emitted: {sql}"
    assert "AGEYEARS" not in upper, f"Cypher property 'ageYears' must be mapped, not emitted: {sql}"

    # The physical columns must be the targets of assignments within the SET segment.
    set_idx = upper.index("SET")
    where_idx = upper.index("WHERE", set_idx)
    set_segment = upper[set_idx + len("SET"):where_idx]
    assert "FULL_NAME" in set_segment, f"'full_name' must be assigned in SET: {set_segment!r}"
    assert "AGE_YEARS" in set_segment, f"'age_years' must be assigned in SET: {set_segment!r}"


# ---------------------------------------------------------------------------
# REQ-670 — Cypher write endpoints return affected_rows in the JSON response.
#
# Exercising the write endpoint end-to-end requires the live federation stack
# (Trino + backing catalogs) to actually perform the INSERT and report the
# committed row count, so this scenario is integration-only.
# ---------------------------------------------------------------------------


@pytest.mark.integration
@given("a successful Cypher CREATE statement executed via the write endpoint")
def given_successful_create_via_endpoint(shared_data):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    base_url = os.getenv("PROVISA_BASE_URL", "http://localhost:8000")
    token = os.getenv("PROVISA_TOKEN", "")
    cypher = "CREATE (n:Person {name: 'Carol', age: 28})"

    async def _run() -> httpx.Response:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
            return await client.post(
                "/query/cypher",
                json={"query": cypher, "params": {}},
                headers=headers,
            )

    resp = asyncio.run(_run())
    assert resp.status_code == 200, (
        f"write endpoint CREATE must succeed, got {resp.status_code}: {resp.text}"
    )

    shared_data["response"] = resp
    shared_data["cypher"] = cypher


@pytest.mark.integration
@when("the response is returned to the client")
def when_response_returned_to_client(shared_data):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    resp = shared_data["response"]
    body = resp.json()
    assert isinstance(body, dict), f"response body must be a JSON object: {body!r}"
    shared_data["body"] = body


@pytest.mark.integration
@then("the JSON body includes an affected_rows field with the count of inserted rows")
def then_affected_rows_count_inserted(shared_data):
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    body = shared_data["body"]
    assert "affected_rows" in body, f"affected_rows field missing from response: {body!r}"

    affected = body["affected_rows"]
    assert isinstance(affected, int), f"affected_rows must be an integer, got {affected!r}"
    # A single CREATE inserts exactly one row, so the reported count must be >= 1.
    assert affected >= 1, f"expected at least one inserted row, got affected_rows={affected}"
