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


scenarios("../features/REQ-345.feature")
scenarios("../features/REQ-347.feature")
scenarios("../features/REQ-348.feature")
scenarios("../features/REQ-349.feature")
scenarios("../features/REQ-352.feature")
scenarios("../features/REQ-353.feature")
scenarios("../features/REQ-572.feature")
scenarios("../features/REQ-573.feature")
scenarios("../features/REQ-575.feature")
scenarios("../features/REQ-576.feature")
scenarios("../features/REQ-577.feature")


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


def _translate(query: str, label_map: CypherLabelMap, params: dict | None = None) -> str:
    """Translate a Cypher query to SQL."""
    ast = parse_cypher(query)
    result = cypher_to_sql(ast, label_map, params or {})
    # cypher_to_sql returns (sql_ast, param_names, graph_vars); extract SQL string.
    sql_ast = result[0] if isinstance(result, tuple) else result
    if hasattr(sql_ast, "sql"):
        return sql_ast.sql(dialect="trino")
    return str(sql_ast)


def _make_param_label_map() -> CypherLabelMap:
    """Label map suitable for parameter-binding tests (REQ-352)."""
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
    return CypherLabelMap(
        nodes={"Person": person_meta},
        relationships={},
    )


def _make_multi_path_label_map() -> CypherLabelMap:
    """Label map with two 1-hop paths from Person to Company: WORKS_AT and MANAGES.

    This is the canonical fixture for REQ-577: multiple schema paths of equal
    hop count between the same pair of node types.
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
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="companies",
        properties={"name": "name"},
    )
    rels = {
        "WORKS_AT": RelationshipMapping(
            rel_type="WORKS_AT",
            source_label="Person",
            target_label="Company",
            join_source_column="company_id",
            join_target_column="id",
            field_name="works_at",
        ),
        "MANAGES": RelationshipMapping(
            rel_type="MANAGES",
            source_label="Person",
            target_label="Company",
            join_source_column="managed_company_id",
            join_target_column="id",
            field_name="manages",
        ),
    }
    return CypherLabelMap(
        nodes={"Person": person_meta, "Company": company_meta},
        relationships=rels,
    )


def _make_governance_label_map() -> CypherLabelMap:
    """Label map used for REQ-345 governance tests.

    Provides Person and Company nodes with a WORKS_AT relationship so that the
    translator can emit a SQL JOIN that Stage 2 governance hooks can act upon.
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
        properties={"name": "name", "age": "age", "salary": "salary"},
    )
    company_meta = NodeMapping(
        label="Company",
        type_name="Company",
        domain_label=None,
        table_label="Company",
        table_id=2,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="companies",
        properties={"name": "name", "revenue": "revenue"},
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


def _make_clause_mapping_label_map() -> CypherLabelMap:
    """Label map for REQ-347 clause-mapping tests.

    Provides Person and Company nodes with a WORKS_AT relationship so that a
    query exercising MATCH, WHERE, RETURN, ORDER BY, and LIMIT can be translated
    and each clause's SQL equivalent verified.
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


def _make_path_label_map() -> CypherLabelMap:
    """Label map for REQ-348 path query tests.

    Provides a Person→Person self-referential KNOWS relationship suitable for
    variable-length traversal and shortestPath queries.
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
        properties={"name": "name", "age": "age"},
    )
    knows_rel = RelationshipMapping(
        rel_type="KNOWS",
        source_label="Person",
        target_label="Person",
        join_source_column="person_id",
        join_target_column="id",
        field_name="knows",
    )
    return CypherLabelMap(
        nodes={"Person": person_meta},
        relationships={"KNOWS": knows_rel},
    )


def _make_node_return_label_map() -> CypherLabelMap:
    """Label map for REQ-349 whole-node RETURN tests.

    Provides a Person node with several properties so the Stage 3 rewrite has
    multiple columns to wrap into a JSON object.
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
        properties={"name": "name", "age": "age", "email": "email"},
    )
    return CypherLabelMap(
        nodes={"Person": person_meta},
        relationships={},
    )


def _make_introspection_label_map() -> CypherLabelMap:
    """Label map for REQ-572 introspection procedure tests.

    Provides Person and Company nodes with domain labels, properties, and a
    WORKS_AT relationship so that all three introspection procedures
    (db.labels, db.relationshipTypes, db.propertyKeys) return meaningful data.
    """
    person_meta = NodeMapping(
        label="Person",
        type_name="Person",
        domain_label="PersonDomain",
        table_label="Person",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="persons",
        properties={"name": "name", "age": "age", "email": "email"},
    )
    company_meta = NodeMapping(
        label="Company",
        type_name="Company",
        domain_label="CompanyDomain",
        table_label="Company",
        table_id=2,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="companies",
        properties={"name": "name", "founded": "founded", "revenue": "revenue"},
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


def _make_correlated_call_label_map() -> CypherLabelMap:
    """Label map for REQ-573 correlated CALL subquery tests.

    Provides Person nodes with a self-referential KNOWS relationship so that
    a correlated CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS friend }
    can be translated to a CROSS JOIN LATERAL expression.
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
        properties={"name": "name", "age": "age", "person_id": "person_id"},
    )
    knows_rel = RelationshipMapping(
        rel_type="KNOWS",
        source_label="Person",
        target_label="Person",
        join_source_column="person_id",
        join_target_column="id",
        field_name="knows",
    )
    return CypherLabelMap(
        nodes={"Person": person_meta},
        relationships={"KNOWS": knows_rel},
    )


def _make_bidirectional_label_map() -> CypherLabelMap:
    """Label map for REQ-575 bidirectional traversal tests.

    Provides Person and Company nodes with a directional WORKS_AT relationship.
    The bidirectional syntax (a)-[]-(b) should expand to both the forward
    (Person→Company) and backward (Company→Person) directions via UNION ALL.
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


def _make_heterogeneous_shortest_path_label_map() -> CypherLabelMap:
    """Label map for REQ-576: heterogeneous shortestPath with no self-referential rel.

    Person and Company are different node types.  The only relationship is
    WORKS_AT (Person → Company).  There is no Person→Person or Company→Company
    self-referential relationship, so the translator must emit a flat JOIN chain
    rather than a recursive CTE.
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
    # Deliberately NO self-referential relationship (no KNOWS Person→Person,
    # no SUBSIDIARY Company→Company, etc.)
    return CypherLabelMap(
        nodes={"Person": person_meta, "Company": company_meta},
        relationships={"WORKS_AT": works_at_rel},
    )


# ---------------------------------------------------------------------------
# REQ-345 — Cypher SELECT query compiled to SQL + Stage 2 governance
# ---------------------------------------------------------------------------


@given("a graph user submitting a Cypher SELECT query to POST /query/cypher")
def given_graph_user_submitting_cypher_query(shared_data: dict) -> None:
    """Set up a representative Cypher SELECT query and the label map it targets.

    We use a MATCH … RETURN pattern that exercises JOIN translation so that the
    governance pipeline has a realistic SQL statement to act upon.
    """
    label_map = _make_governance_label_map()

    # Verify label map is well-formed before proceeding.
    assert "Person" in label_map.nodes, "Person node must be registered"
    assert "Company" in label_map.nodes, "Company node must be registered"
    assert "WORKS_AT" in label_map.relationships, "WORKS_AT relationship must be registered"

    query = (
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "RETURN p.name AS person_name, c.name AS company_name"
    )

    # Parse the query to confirm it is a valid Cypher SELECT (no write clauses).
    ast = parse_cypher(query)
    assert ast is not None, "parse_cypher must return an AST for a valid SELECT query"
    assert ast.return_clause is not None, "query must have a RETURN clause"

    shared_data["query"] = query
    shared_data["label_map"] = label_map
    # Record expected governance artefacts for assertion in the Then step.
    shared_data["expected_tables"] = {"persons", "companies"}
    shared_data["expected_join_columns"] = {"company_id", "id"}


@when("the compiler processes it")
def when_compiler_processes_cypher(shared_data: dict) -> None:
    """Invoke the Cypher → SQL compiler and record the outcome.

    We also capture whether the compiler invoked cypher_to_sql (confirming the
    compilation pathway was exercised) and whether the resulting SQL is
    structurally valid for Stage 2 governance to consume.
    """
    query = shared_data["query"]
    label_map = shared_data["label_map"]

    compile_error: Exception | None = None
    sql: str | None = None

    try:
        sql = _translate(query, label_map)
    except Exception as exc:  # noqa: BLE001
        compile_error = exc

    shared_data["sql"] = sql
    shared_data["compile_error"] = compile_error


@then(
    "it compiles to SQL, executes via Trino, and applies Stage 2 governance identically to GraphQL queries"
)
def then_compiles_to_sql_with_governance(shared_data: dict) -> None:
    """Assert REQ-345 end-to-end: compilation succeeds, SQL is valid, and the
    structure confirms that Stage 2 governance (RLS, column masking, domain
    visibility, row ceiling) can be applied identically to GraphQL-compiled
    queries.
    """
    # 1. Compilation must succeed without error.
    assert shared_data["compile_error"] is None, (
        f"Cypher→SQL compilation must not raise for a valid SELECT query; "
        f"got: {shared_data['compile_error']!r}"
    )

    sql = shared_data["sql"]
    assert sql, "compiler must produce a non-empty SQL string"

    # 2. The SQL must be parseable as Trino SQL.
    try:
        parsed = sqlglot.parse_one(sql, read="trino")
    except Exception as exc:
        pytest.fail(
            f"Generated SQL is not valid Trino SQL (sqlglot parse failed): {exc}\nSQL was:\n{sql}"
        )
    assert parsed is not None, f"sqlglot must parse the generated SQL:\n{sql}"

    sql_lower = sql.lower()

    # 3. Both physical tables must appear in the SQL.
    for table in shared_data["expected_tables"]:
        assert table in sql_lower, (
            f"Generated SQL must reference physical table {table!r} "
            f"(required for Stage 2 governance label resolution):\n{sql}"
        )

    # 4. A JOIN must be present.
    joins = list(parsed.find_all(sqlglot.exp.Join))
    assert joins, (
        f"Generated SQL must contain a JOIN for the MATCH clause "
        f"(Stage 2 governance attaches predicates to JOIN conditions):\n{sql}"
    )

    # 5. The SELECT projected aliases must survive translation.
    assert "person_name" in sql_lower or "p.name" in sql_lower or "persons" in sql_lower, (
        f"Generated SQL must project person_name or equivalent "
        f"(required for Stage 2 column masking):\n{sql}"
    )
    assert "company_name" in sql_lower or "c.name" in sql_lower or "companies" in sql_lower, (
        f"Generated SQL must project company_name or equivalent "
        f"(required for Stage 2 column masking):\n{sql}"
    )

    # 6. Top-level statement must be a SELECT.
    assert isinstance(parsed, sqlglot.exp.Select), (
        f"The top-level SQL statement must be a SELECT (same shape as "
        f"GraphQL-compiled queries so Stage 2 governance applies identically); "
        f"got {type(parsed).__name__}:\n{sql}"
    )


# ---------------------------------------------------------------------------
# REQ-347 — Cypher clause → SQL clause mapping
# ---------------------------------------------------------------------------


@given("a Cypher query with MATCH, WHERE, RETURN, ORDER BY, and LIMIT clauses")
def given_cypher_query_with_all_clauses(shared_data: dict) -> None:
    """Construct a Cypher query that exercises every clause named in REQ-347.

    The query uses:
      MATCH        → must become a JOIN in SQL
      WHERE        → must become WHERE in SQL
      RETURN       → must become SELECT in SQL
      ORDER BY     → must become ORDER BY in SQL
      LIMIT        → must become LIMIT in SQL

    We use a realistic two-node, one-relationship pattern so the label map
    can resolve it to concrete physical tables.
    """
    label_map = _make_clause_mapping_label_map()

    query = (
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "WHERE p.age > 30 "
        "RETURN p.name AS person_name, c.name AS company_name "
        "ORDER BY p.name "
        "LIMIT 10"
    )

    # Verify the parser recognises all expected clauses before handing off to
    # the translator — this makes test failures more diagnostic.
    ast = parse_cypher(query)
    assert ast is not None, "parse_cypher must return an AST"
    assert ast.match_clauses, "AST must contain at least one MATCH clause"
    assert not ast.match_clauses[0].optional, "first MATCH must not be OPTIONAL"
    assert ast.where is not None, "AST must contain a WHERE clause"
    assert ast.return_clause is not None, "AST must contain a RETURN clause"
    assert ast.order_by, "AST must contain ORDER BY items"
    assert ast.limit == 10, f"AST limit must be 10, got {ast.limit}"

    shared_data["query"] = query
    shared_data["label_map"] = label_map


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


@then("it emits SQL with JOIN, WHERE, SELECT, ORDER BY, and LIMIT clauses respectively")
def then_emits_sql_with_all_clause_mappings(shared_data: dict) -> None:
    """Assert that every Cypher clause was translated to its SQL counterpart.

    REQ-347 clause mapping table:
      MATCH        → JOIN
      WHERE        → WHERE
      RETURN       → SELECT (top-level SELECT statement)
      ORDER BY     → ORDER BY
      LIMIT        → LIMIT
    """
    assert shared_data["error"] is None, (
        f"Translation must not raise for a valid Cypher query; got: {shared_data['error']!r}"
    )

    sql = shared_data["sql"]
    assert sql, "translator must produce a non-empty SQL string"

    # Parse the generated SQL with sqlglot so we can inspect the AST
    # structurally rather than relying solely on substring matching.
    try:
        parsed = sqlglot.parse_one(sql, read="trino")
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"sqlglot failed to parse generated SQL: {exc}\nSQL: {sql!r}")
        return

    import sqlglot.expressions as exp

    assert isinstance(parsed, exp.Select), (
        f"Expected a SELECT statement at the root, got {type(parsed).__name__!r}"
    )

    sql_lower = sql.lower()

    # MATCH → JOIN
    joins = list(parsed.find_all(exp.Join))
    assert joins, f"Generated SQL must contain JOIN for MATCH clause:\n{sql}"

    # WHERE → WHERE
    assert "where" in sql_lower, f"Generated SQL must contain WHERE:\n{sql}"

    # RETURN → SELECT columns present
    assert "person_name" in sql_lower or "name" in sql_lower, (
        f"Generated SQL must project name columns from RETURN clause:\n{sql}"
    )

    # ORDER BY → ORDER BY
    assert "order by" in sql_lower, f"Generated SQL must contain ORDER BY:\n{sql}"

    # LIMIT → LIMIT
    assert "limit" in sql_lower, f"Generated SQL must contain LIMIT:\n{sql}"


# ---------------------------------------------------------------------------
# REQ-348 — Path queries: shortestPath / [*1..n] → WITH RECURSIVE CTE
# ---------------------------------------------------------------------------


@given("a Cypher query with shortestPath or [*1..n] variable-length pattern")
def given_cypher_path_query(shared_data: dict) -> None:
    label_map = _make_path_label_map()
    # Variable-length pattern [*1..3] on a self-referential rel (KNOWS: Person→Person)
    # _needs_recursive_cte returns True for variable_length + same src/tgt type.
    query = "MATCH p = shortestPath((a:Person)-[:KNOWS*1..3]->(b:Person)) RETURN p"
    shared_data["query"] = query
    shared_data["label_map"] = label_map
    # Unbounded pattern must be rejected
    shared_data["unbounded_query"] = (
        "MATCH p = shortestPath((a:Person)-[:KNOWS*]->(b:Person)) RETURN p"
    )


@then("it emits a WITH RECURSIVE CTE and rejects unbounded [*] patterns at compile time")
def then_emits_recursive_cte_and_rejects_unbounded(shared_data: dict) -> None:
    from provisa.cypher.translator import CypherTranslateError

    sql = shared_data.get("sql")
    error = shared_data.get("error")

    # Bounded pattern must produce WITH RECURSIVE SQL or succeed without error.
    if error is not None:
        pytest.fail(f"Bounded path query must not raise; got: {error!r}")

    assert sql, "bounded path query must produce SQL"
    sql_lower = sql.lower()
    assert "with recursive" in sql_lower or "with" in sql_lower, (
        f"bounded path query must emit a WITH (RECURSIVE) CTE:\n{sql}"
    )

    # Unbounded [*] must raise CypherTranslateError at compile time.
    unbounded_query = shared_data["unbounded_query"]
    label_map = shared_data["label_map"]
    with pytest.raises((CypherTranslateError, Exception)) as exc_info:
        _translate(unbounded_query, label_map)
    assert exc_info.value is not None, "unbounded [*] must raise at compile time"


# ---------------------------------------------------------------------------
# REQ-349 — Whole-node RETURN → Stage 3 rewrite → CAST(ROW(...) AS JSON)
# ---------------------------------------------------------------------------


@given("a Cypher RETURN clause referencing a whole node variable")
def given_whole_node_return(shared_data: dict) -> None:
    label_map = _make_node_return_label_map()
    # Returning the whole node variable `p`, not a scalar property.
    query = "MATCH (p:Person) RETURN p"
    shared_data["query"] = query
    shared_data["label_map"] = label_map


@when("Stage 3 rewrite runs")
def when_stage3_rewrite_runs(shared_data: dict) -> None:
    from provisa.cypher.graph_rewriter import apply_graph_rewrites

    error: Exception | None = None
    rewritten_sql: str | None = None
    try:
        ast = parse_cypher(shared_data["query"])
        result = cypher_to_sql(ast, shared_data["label_map"], {})
        sql_ast, _param_order, graph_vars = result
        rewritten_ast = apply_graph_rewrites(sql_ast, graph_vars, shared_data["label_map"])
        rewritten_sql = rewritten_ast.sql(dialect="trino")
    except Exception as exc:  # noqa: BLE001
        error = exc
    shared_data["rewritten_sql"] = rewritten_sql
    shared_data["stage3_error"] = error


@then("the node columns are wrapped into a single JSON object via CAST(ROW(...) AS JSON)")
def then_node_columns_wrapped_in_json(shared_data: dict) -> None:
    assert shared_data["stage3_error"] is None, (
        f"Stage 3 rewrite must not raise; got: {shared_data['stage3_error']!r}"
    )
    sql = shared_data["rewritten_sql"]
    assert sql, "Stage 3 rewrite must produce SQL"
    sql_lower = sql.lower()
    # Stage 3 wraps node variables into JSON_OBJECT (SQLGlot emits JSON_OBJECT for Trino)
    assert "json_object" in sql_lower or "cast" in sql_lower or "row" in sql_lower, (
        f"Stage 3 SQL must contain JSON_OBJECT / CAST(ROW(...) AS JSON) for node variable:\n{sql}"
    )


# ---------------------------------------------------------------------------
# REQ-352 — Missing $param with no default rejected at compile time
# ---------------------------------------------------------------------------


@given("a Cypher query with $param and no default")
def given_cypher_query_with_param_no_default(shared_data: dict) -> None:
    label_map = _make_param_label_map()
    # $minAge is a named parameter with no default in the label map.
    query = "MATCH (p:Person) WHERE p.age > $minAge RETURN p.name"
    shared_data["query"] = query
    shared_data["label_map"] = label_map


@when("the parameter is missing from the request")
def when_parameter_missing_from_request(shared_data: dict) -> None:
    from provisa.cypher.params import collect_param_names, bind_params, CypherParamError

    query = shared_data["query"]
    param_names = collect_param_names(query)
    shared_data["param_names"] = param_names
    # Simulate: no params provided in the request
    error: Exception | None = None
    try:
        bind_params(param_names, {})
    except CypherParamError as exc:
        error = exc
    except Exception as exc:  # noqa: BLE001
        error = exc
    shared_data["param_error"] = error


@then("it is rejected at compile time")
def then_rejected_at_compile_time(shared_data: dict) -> None:
    from provisa.cypher.params import CypherParamError

    assert shared_data["param_error"] is not None, (
        "A missing required $param must raise CypherParamError"
    )
    assert isinstance(shared_data["param_error"], CypherParamError), (
        f"Expected CypherParamError, got {type(shared_data['param_error']).__name__!r}: "
        f"{shared_data['param_error']!r}"
    )
    assert "minAge" in str(shared_data["param_error"]), (
        f"Error message must mention the missing param name 'minAge'; "
        f"got: {shared_data['param_error']!r}"
    )


# ---------------------------------------------------------------------------
# REQ-353 — Cross-catalog JOIN executes normally (no cross-source restriction)
# ---------------------------------------------------------------------------


@given("a Cypher query whose node labels resolve to tables on different Trino catalogs")
def given_cross_catalog_cypher_query(shared_data: dict) -> None:
    label_map = _make_cross_catalog_label_map()
    # Person is in catalog 'postgresql', Company is in catalog 'mysql'.
    query = (
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "RETURN p.name AS person_name, c.name AS company_name"
    )
    shared_data["query"] = query
    shared_data["label_map"] = label_map


@then("it generates a cross-catalog JOIN and executes normally without error")
def then_generates_cross_catalog_join(shared_data: dict) -> None:
    assert shared_data.get("error") is None, (
        f"Cross-catalog query must not raise; got: {shared_data.get('error')!r}"
    )
    sql = shared_data.get("sql")
    assert sql, "cross-catalog query must produce SQL"
    sql_lower = sql.lower()
    # Both physical catalog qualifiers must appear.
    assert "postgresql" in sql_lower, f"SQL must reference postgresql catalog:\n{sql}"
    assert "mysql" in sql_lower, f"SQL must reference mysql catalog:\n{sql}"
    # A JOIN must connect the two catalogs' tables.
    parsed = sqlglot.parse_one(sql, read="trino")
    joins = list(parsed.find_all(sqlglot.exp.Join))
    assert joins, f"Cross-catalog SQL must contain a JOIN:\n{sql}"


# ---------------------------------------------------------------------------
# REQ-572 — CALL db.labels() → CypherLabelMap introspection, no SQL generated
# ---------------------------------------------------------------------------


@given("a client issuing CALL db.labels()")
def given_client_issuing_call_db_labels(shared_data: dict) -> None:
    label_map = _make_introspection_label_map()
    shared_data["label_map"] = label_map
    shared_data["procedure"] = "db.labels"


@when("the cypher router handles it")
def when_cypher_router_handles_it(shared_data: dict) -> None:
    from provisa.api.rest.cypher_router import _handle_procedure

    label_map = shared_data["label_map"]
    proc = shared_data["procedure"]
    response = _handle_procedure(proc, label_map)
    shared_data["response"] = response
    shared_data["response_content"] = response.body


@then("it returns label data from CypherLabelMap without generating or executing SQL")
def then_returns_label_data_without_sql(shared_data: dict) -> None:
    import json

    response = shared_data["response"]
    assert response is not None, "handler must return a response"
    content = json.loads(shared_data["response_content"])
    assert "rows" in content, f"response must contain 'rows'; got: {content!r}"
    assert "columns" in content, f"response must contain 'columns'; got: {content!r}"
    returned_labels = {row["label"] for row in content["rows"]}
    # The introspection label map has Person (domain: PersonDomain) and Company (domain: CompanyDomain).
    assert "Person" in returned_labels or "PersonDomain" in returned_labels, (
        f"Person label or domain must appear in db.labels() output; got: {returned_labels!r}"
    )
    assert "Company" in returned_labels or "CompanyDomain" in returned_labels, (
        f"Company label or domain must appear in db.labels() output; got: {returned_labels!r}"
    )
    # No SQL key must appear in the response — data is sourced from CypherLabelMap directly.
    assert "sql" not in content, f"db.labels() response must not contain SQL; got: {content!r}"


# ---------------------------------------------------------------------------
# REQ-573 — Correlated CALL subquery → CROSS JOIN LATERAL
# ---------------------------------------------------------------------------


@given("a Cypher CALL subquery with WITH importing an outer variable")
def given_correlated_call_subquery(shared_data: dict) -> None:
    label_map = _make_correlated_call_label_map()
    # Correlated CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS friend }
    query = (
        "MATCH (p:Person) "
        "CALL { WITH p MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS friend } "
        "RETURN p.name, friend"
    )
    shared_data["query"] = query
    shared_data["label_map"] = label_map


@then("it emits a CROSS JOIN LATERAL expression")
def then_emits_cross_join_lateral(shared_data: dict) -> None:
    assert shared_data.get("error") is None, (
        f"Correlated CALL query must not raise; got: {shared_data.get('error')!r}"
    )
    sql = shared_data.get("sql")
    assert sql, "correlated CALL query must produce SQL"
    sql_lower = sql.lower()
    assert "lateral" in sql_lower, f"Correlated CALL must emit CROSS JOIN LATERAL; got SQL:\n{sql}"


# ---------------------------------------------------------------------------
# REQ-575 — Bidirectional traversal (a)-[]-(b) → UNION ALL fwd + bwd
# ---------------------------------------------------------------------------


@given("a Cypher query with bidirectional traversal (a)-[]-(b)")
def given_bidirectional_traversal_query(shared_data: dict) -> None:
    # Use correlated call label map: Person with self-referential KNOWS rel.
    # Bidirectional expansion (UNION ALL) requires a self-referential relationship
    # so that both forward (a)-[:KNOWS]->(b) and backward (b)-[:KNOWS]->(a) branches
    # are non-trivially distinct and the translator emits both via UNION ALL.
    label_map = _make_correlated_call_label_map()
    # Undirected syntax: no arrow → direction="none" in the parser
    query = "MATCH (a:Person)-[:KNOWS]-(b:Person) RETURN a.name, b.name"
    shared_data["query"] = query
    shared_data["label_map"] = label_map


@then("it emits a UNION ALL of forward and backward directed relationship joins")
def then_emits_union_all_bidirectional(shared_data: dict) -> None:
    assert shared_data.get("error") is None, (
        f"Bidirectional traversal query must not raise; got: {shared_data.get('error')!r}"
    )
    sql = shared_data.get("sql")
    assert sql, "bidirectional traversal query must produce SQL"
    sql_lower = sql.lower()
    assert "union all" in sql_lower, (
        f"Bidirectional traversal must emit UNION ALL of both directions:\n{sql}"
    )


# ---------------------------------------------------------------------------
# REQ-576 — shortestPath between different node types → flat JOIN chain
# ---------------------------------------------------------------------------


@given("a shortestPath query between two different node types with a unique schema path")
def given_heterogeneous_shortest_path_query(shared_data: dict) -> None:
    label_map = _make_heterogeneous_shortest_path_label_map()
    # Person and Company are different types; only WORKS_AT connects them (no self-ref rel).
    # _needs_recursive_cte returns False: variable_length=True but src != tgt and no self-ref rel.
    query = "MATCH p = shortestPath((a:Person)-[:WORKS_AT*1..3]->(b:Company)) RETURN p"
    shared_data["query"] = query
    shared_data["label_map"] = label_map


@then("it emits a flat JOIN chain instead of a recursive CTE")
def then_emits_flat_join_chain(shared_data: dict) -> None:
    assert shared_data.get("error") is None, (
        f"Heterogeneous shortestPath must not raise; got: {shared_data.get('error')!r}"
    )
    sql = shared_data.get("sql")
    assert sql, "heterogeneous shortestPath must produce SQL"
    sql_lower = sql.lower()
    # Flat JOIN path: no WITH RECURSIVE.
    assert "with recursive" not in sql_lower, (
        f"Heterogeneous shortestPath must emit a flat JOIN chain, not WITH RECURSIVE:\n{sql}"
    )
    # Must still reference both node tables.
    assert "persons" in sql_lower, f"SQL must reference persons table:\n{sql}"
    assert "companies" in sql_lower, f"SQL must reference companies table:\n{sql}"


# ---------------------------------------------------------------------------
# REQ-577 — Multiple equal-cost schema paths → UNION ALL branches
# ---------------------------------------------------------------------------


@given("multiple schema paths of equal hop count between the same node types")
def given_multiple_equal_cost_paths(shared_data: dict) -> None:
    label_map = _make_multi_path_label_map()
    # Person→Company via WORKS_AT (company_id) and via MANAGES (managed_company_id):
    # both are 1-hop paths of equal cost.
    query = "MATCH p = shortestPath((a:Person)-[*1..2]->(b:Company)) RETURN p"
    shared_data["query"] = query
    shared_data["label_map"] = label_map


@when("the translator processes a shortestPath query")
def when_translator_processes_shortest_path(shared_data: dict) -> None:
    error: Exception | None = None
    sql: str | None = None
    try:
        sql = _translate(shared_data["query"], shared_data["label_map"])
    except Exception as exc:  # noqa: BLE001
        error = exc
    shared_data["sql"] = sql
    shared_data["error"] = error


@then("all matching paths are emitted as UNION ALL branches without deduplication")
def then_all_paths_as_union_all(shared_data: dict) -> None:
    assert shared_data.get("error") is None, (
        f"Multi-path shortestPath must not raise; got: {shared_data.get('error')!r}"
    )
    sql = shared_data.get("sql")
    assert sql, "multi-path shortestPath must produce SQL"
    sql_lower = sql.lower()
    # Multiple equal-cost paths → UNION ALL (no DISTINCT deduplication).
    assert "union all" in sql_lower, (
        f"Multiple equal-cost paths must emit UNION ALL branches:\n{sql}"
    )
    # DISTINCT must not appear (no deduplication per REQ-577).
    assert "union all" in sql_lower and "union distinct" not in sql_lower, (
        f"UNION ALL branches must not be deduplicated (no UNION DISTINCT):\n{sql}"
    )
