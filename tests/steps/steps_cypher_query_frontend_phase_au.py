# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Step definitions for Cypher Query Frontend (Phase AU).

REQ-345: Provisa exposes POST /query/cypher accepting a Cypher SELECT query and
optional named parameters ($param). The query is compiled to SQL and executed via
Trino. All existing governance (RLS, column masking, domain visibility, row
ceiling) applies via Stage 2 identically to GraphQL-compiled queries.

REQ-347: Cypher clauses map to SQL as follows: MATCH -> JOIN; OPTIONAL MATCH ->
LEFT JOIN; WHERE -> WHERE; RETURN -> SELECT; ORDER BY -> ORDER BY; SKIP/LIMIT ->
OFFSET/LIMIT; WITH (pipeline) -> CTE or subquery. Node label :Label and
relationship type :TYPE are resolved via the steward-declared label mapping
(REQ-351) to physical table and join references.

REQ-348: Path queries — shortestPath(...), allShortestPaths(...), and
variable-length relationship patterns [*1..n] — translate to Trino recursive CTEs
(WITH RECURSIVE) against the adjacency relation defined in the label mapping.
Maximum hop depth is enforced at compile time; unbounded [*] is rejected.

REQ-349: When a RETURN clause references a whole node variable, relationship
variable, or path variable (not a scalar property), a Stage 3 SQLGlot rewrite
pass wraps the projected columns for that variable into a single JSON object
column using CAST(ROW(...) AS JSON). This rewrite runs after Stage 2 governance
and before execution. It does not modify scalar property projections.

REQ-352: Cypher named parameters ($param) are translated to Trino positional
parameters at compile time. Parameter types are inferred from the label mapping
schema. Missing parameters with no default are rejected at compile time.

REQ-353: WITHDRAWN (2026-06-19). Cross-source Cypher queries are allowed — Trino
joins across catalogs natively, so a query whose labels resolve to tables on
different sources translates and executes normally. No cross-source restriction
is enforced. (Supersedes REQ-481.)

REQ-572: Provisa handles CALL db.labels(), CALL db.relationshipTypes(), and
CALL db.propertyKeys() as introspection procedures that return data from the
in-memory semantic layer (CypherLabelMap) without generating or executing any
SQL.

REQ-573: Correlated CALL subqueries of the form
CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias } are translated to
CROSS JOIN LATERAL expressions. The outer-scope variable must appear in WITH;
multiple imported variables are supported. Non-correlated top-level CALL blocks
(without WITH) are handled by cypher_calls_to_sql_list.

REQ-575: Bidirectional traversal syntax (a)-[]-(b) is rewritten at compile time
to a UNION ALL of all matching directed forward and backward relationship joins
derived from the semantic layer. Every relationship is directional; the
bidirectional form is syntactic sugar that expands to both directions.

REQ-576: When shortestPath endpoints have different node types and no
self-referential relationship exists in the schema, the translator emits a flat
JOIN chain (structurally shortest schema path) rather than a recursive CTE with
ORDER BY hops. Hops are not tracked in this code path.

REQ-577: When multiple schema paths of equal hop count connect the same start
and end node types, all matching paths are emitted as UNION ALL branches.
Row-level deduplication across branches is not performed.
"""

from __future__ import annotations

import json

import pytest
import sqlglot
from pytest_bdd import given, when, then, scenarios

from provisa.cypher.parser import parse_cypher
from provisa.cypher.label_map import (
    CypherLabelMap,
    NodeMapping,
    RelationshipMapping,
)
from provisa.cypher.translator import cypher_to_sql
import provisa.cypher.translator as _translator_mod
from provisa.api.rest.cypher_router import _detect_procedure, _handle_procedure


scenarios("../features/req_353_cypher_cross_source.feature")
scenarios("../features/req_572_cypher_query_frontend_phase_au.feature")
scenarios("../features/req_573_cypher_query_frontend_phase_au.feature")
scenarios("../features/req_575_cypher_query_frontend_phase_au.feature")
scenarios("../features/req_576_cypher_query_frontend_phase_au.feature")
scenarios("../features/req_577_cypher_query_frontend_phase_au.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    """Plain dict to pass state between Given/When/Then steps."""
    return {}


def _make_cross_catalog_label_map() -> CypherLabelMap:
    """Label map where Person and Company live in *different* Trino catalogs.

    Person -> postgresql.public.persons
    Company -> mysql.public.companies
    A WORKS_AT relationship joins them across catalogs.
    """
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
        properties={"name": "name", "age": "age", "company_id": "company_id"},
    )
    company_meta = NodeMapping(
        label="Company",
        type_name="Company",
        domain_label=None,
        table_label="Company",
        table_id=2,
        source_id="mysql-secondary",
        id_column="id",
        pk_columns=[],
        catalog_name="mysql",
        schema_name="public",
        table_name="companies",
        properties={"name": "name", "founded": "founded"},
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


def _translate(query: str, label_map: CypherLabelMap) -> str:
    """Translate a Cypher query to SQL, tolerating string- or AST-based APIs."""
    try:
        return cypher_to_sql(query, label_map)
    except TypeError:
        return cypher_to_sql(parse_cypher(query), label_map)


# ---------------------------------------------------------------------------
# REQ-353 — cross-source Cypher queries are allowed (no restriction enforced)
# ---------------------------------------------------------------------------


@given("a Cypher query whose node labels resolve to tables on different Trino catalogs")
def given_cross_catalog_query(shared_data: dict) -> None:
    label_map = _make_cross_catalog_label_map()

    # Sanity check: the two labels genuinely resolve to distinct catalogs/sources.
    person = label_map.nodes["Person"]
    company = label_map.nodes["Company"]
    assert person.catalog_name != company.catalog_name, (
        "test setup must place the two labels on different catalogs"
    )
    assert person.source_id != company.source_id

    shared_data["label_map"] = label_map
    shared_data["query"] = (
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "RETURN p.name AS person, c.name AS company"
    )


@when("the translator processes it")
def when_translator_processes(shared_data: dict) -> None:
    error: Exception | None = None
    sql: str | None = None
    try:
        sql = _translate(shared_data["query"], shared_data["label_map"])
    except Exception as exc:  # noqa: BLE001 - we assert no error in the Then step
        error = exc
    shared_data["sql"] = sql
    shared_data["error"] = error


@then("it generates a cross-catalog JOIN and executes normally without error")
def then_cross_catalog_join(shared_data: dict) -> None:
    # No cross-source restriction must be raised at translation time.
    assert shared_data["error"] is None, (
        f"cross-source Cypher must not be rejected, got: {shared_data['error']!r}"
    )

    sql = shared_data["sql"]
    assert sql, "translator must produce SQL"

    # The SQL must reference both catalogs by name (cross-catalog references).
    lowered = sql.lower()
    assert "postgresql" in lowered, f"expected postgresql catalog in SQL:\n{sql}"
    assert "mysql" in lowered, f"expected mysql catalog in SQL:\n{sql}"

    # It must be a valid, parseable SQL statement containing a JOIN that the
    # Trino engine can execute (Trino joins across catalogs natively).
    parsed = sqlglot.parse_one(sql, read="trino")
    assert parsed is not None, "generated SQL must parse as Trino SQL"

    joins = list(parsed.find_all(sqlglot.exp.Join))
    assert joins, f"expected a cross-catalog JOIN in generated SQL:\n{sql}"

    # Confirm the join genuinely spans the two distinct physical tables.
    table_catalogs = {
        t.catalog.lower()
        for t in parsed.find_all(sqlglot.exp.Table)
        if t.catalog
    }
    assert {"postgresql", "mysql"}.issubset(table_catalogs), (
        f"join must span both catalogs, found catalogs={table_catalogs}\n{sql}"
    )


# ---------------------------------------------------------------------------
# REQ-572 — introspection procedures return semantic-layer data, no SQL
# ---------------------------------------------------------------------------


def _make_introspection_label_map() -> CypherLabelMap:
    """Label map with domain + table labels, properties, and relationship types
    used to verify CALL db.* introspection procedures."""
    person_meta = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label="SalesPerson",
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
    company_meta = NodeMapping(
        label="Company",
        type_name="Company",
        domain_label="SalesCompany",
        table_label="Company",
        table_id=2,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="companies",
        properties={"name": "name", "founded": "founded"},
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


@given("a client issuing CALL db.labels()")
def given_call_db_labels(shared_data: dict) -> None:
    shared_data["label_map"] = _make_introspection_label_map()
    shared_data["query"] = "CALL db.labels()"


@when("the cypher router handles it")
def when_router_handles_procedure(shared_data: dict) -> None:
    query = shared_data["query"]
    label_map = shared_data["label_map"]

    # Spy on the SQL translator: a true introspection path must never invoke it.
    sql_calls: list[int] = []
    original = _translator_mod.cypher_to_sql

    def _spy(*args, **kwargs):
        sql_calls.append(1)
        return original(*args, **kwargs)

    _translator_mod.cypher_to_sql = _spy
    try:
        proc = _detect_procedure(query)
        shared_data["proc"] = proc
        assert proc is not None, f"{query!r} must be detected as an introspection procedure"
        response = _handle_procedure(proc, label_map)
    finally:
        _translator_mod.cypher_to_sql = original

    shared_data["sql_calls"] = len(sql_calls)
    shared_data["response"] = response
    shared_data["payload"] = json.loads(response.body)


@then("it returns label data from CypherLabelMap without generating or executing SQL")
def then_returns_labels_no_sql(shared_data: dict) -> None:
    # Procedure must have been recognised and routed to the introspection handler.
    assert shared_data["proc"] == "db.labels"

    # No SQL translation was attempted — the data came from the semantic layer.
    assert shared_data["sql_calls"] == 0, (
        "introspection procedures must not invoke the SQL translator"
    )

    payload = shared_data["payload"]
    assert payload["columns"] == ["label"], f"unexpected columns: {payload['columns']}"

    returned = {row["label"]
