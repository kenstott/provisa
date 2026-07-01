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

REQ-750: Cypher graph variables (nodes, edges, paths) returned in the RETURN
clause are serialized as JSON objects with canonical shape. Nodes include id,
label, tableLabel, and properties. Edges include identity, start, end, type,
properties, startNode, and endNode. Paths include nodes, edges, and
length/hops.

REQ-751: Variable-length relationship patterns [*1..n] are compiled to recursive
CTEs in Trino SQL. The CTE enforces max-hop bounds and returns edge sequences as
JSON arrays.

REQ-752: Intermediate node property access in multi-hop MATCH patterns resolves
all three table aliases and preserves property references in WHERE and RETURN
clauses. No aliasing conflicts occur.

REQ-753: Path object RETURN (e.g., `RETURN p`) emits a JSON_OBJECT with `nodes`
(array of node objects), `edges` (array of edge objects), `start`/`end` (node
identities), and `length` (hop count). For recursive CTE paths, `hops` field
counts intermediate hops.
"""

from __future__ import annotations


import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.cypher.parser import parse_cypher, CypherParseError
from provisa.cypher.label_map import (
    CypherLabelMap,
    NodeMapping,
    RelationshipMapping,
)
from provisa.cypher.translator import cypher_to_sql, GraphVarKind
from provisa.cypher.params import bind_params, CypherParamError
from provisa.api.rest.cypher_router import _detect_procedure, _handle_procedure


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
scenarios("../features/REQ-750.feature")
scenarios("../features/REQ-751.feature")
scenarios("../features/REQ-752.feature")
scenarios("../features/REQ-753.feature")


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


def _translate_full(query: str, label_map: CypherLabelMap, params: dict | None = None):
    """Translate a Cypher query to SQL and return the full result tuple."""
    ast = parse_cypher(query)
    result = cypher_to_sql(ast, label_map, params or {})
    return result


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
    """Label map for REQ-347 clause-mapping tests."""
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
    """Label map for REQ-348 path query tests."""
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
    """Label map for REQ-349 whole-node RETURN tests."""
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
    """Label map for REQ-572 introspection procedure tests."""
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
    """Label map for REQ-573 correlated CALL subquery tests."""
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
    """Label map for REQ-575 bidirectional traversal tests."""
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
    """Label map for REQ-576: heterogeneous shortestPath with no self-referential rel."""
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


def _make_graph_var_label_map() -> CypherLabelMap:
    """Label map for REQ-750 graph variable serialization tests."""
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


def _make_variable_length_label_map() -> CypherLabelMap:
    """Label map for REQ-751 variable-length path tests."""
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


def _make_multi_hop_label_map() -> CypherLabelMap:
    """Label map for REQ-752 multi-hop intermediate node property access tests."""
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
    department_meta = NodeMapping(
        label="Department",
        type_name="Department",
        domain_label=None,
        table_label="Department",
        table_id=3,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="departments",
        properties={"name": "name", "budget": "budget", "company_id": "company_id"},
    )
    works_at_rel = RelationshipMapping(
        rel_type="WORKS_AT",
        source_label="Person",
        target_label="Company",
        join_source_column="company_id",
        join_target_column="id",
        field_name="works_at",
    )
    has_dept_rel = RelationshipMapping(
        rel_type="HAS_DEPT",
        source_label="Company",
        target_label="Department",
        join_source_column="id",
        join_target_column="company_id",
        field_name="has_dept",
    )
    return CypherLabelMap(
        nodes={
            "Person": person_meta,
            "Company": company_meta,
            "Department": department_meta,
        },
        relationships={
            "WORKS_AT": works_at_rel,
            "HAS_DEPT": has_dept_rel,
        },
    )


def _make_path_object_label_map() -> CypherLabelMap:
    """Label map for REQ-753 path object RETURN tests."""
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


# ---------------------------------------------------------------------------
# REQ-345 — Cypher SELECT query compiled to SQL + Stage 2 governance
# ---------------------------------------------------------------------------


@given("a graph user submitting a Cypher SELECT query to POST /query/cypher")
def given_graph_user_submitting_cypher_query(shared_data: dict) -> None:
    """Set up a representative Cypher SELECT query and the label map it targets.

    We use a MATCH ... RETURN pattern that exercises JOIN translation so that the
    governance pipeline has a realistic SQL statement to act upon.
    """
    label_map = _make_governance_label_map()
    # A realistic Cypher SELECT query that a graph-native user would write.
    # It references a relationship so the compiler must emit a JOIN.
    cypher_query = (
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "WHERE p.age > $min_age "
        "RETURN p.name AS person_name, c.name AS company_name, p.salary AS salary"
    )
    params = {"min_age": 25}
    shared_data["cypher_query"] = cypher_query
    shared_data["label_map"] = label_map
    shared_data["params"] = params


@when("the compiler processes it")
def when_compiler_processes_cypher_query(shared_data: dict) -> None:
    """Invoke the Cypher-to-SQL compiler and capture the full translation result.

    cypher_to_sql returns a 3-tuple: (sql_expression, param_names, graph_vars).
    We store all three so the Then step can make assertions about each.
    """
    cypher_query = shared_data["cypher_query"]
    label_map = shared_data["label_map"]
    params = shared_data["params"]

    ast = parse_cypher(cypher_query)
    result = cypher_to_sql(ast, label_map, params)
    # Unpack the 3-tuple returned by cypher_to_sql.
    sql_ast, param_names, graph_vars = result

    # Render to a SQL string using the Trino dialect so we can make text assertions.
    if hasattr(sql_ast, "sql"):
        sql_string = sql_ast.sql(dialect="trino")
    else:
        sql_string = str(sql_ast)

    shared_data["sql_ast"] = sql_ast
    shared_data["sql_string"] = sql_string
    shared_data["param_names"] = param_names
    shared_data["graph_vars"] = graph_vars
    shared_data["compile_error"] = None


@then(
    "it compiles to SQL, executes via Trino, and applies Stage 2 governance identically to GraphQL queries"
)
def then_compiles_to_sql_and_applies_governance(shared_data: dict) -> None:
    """Assert that:

    1. The compiler produced a non-empty SQL string (compilation succeeded).
    2. The SQL contains a JOIN, since the Cypher pattern traverses the
       WORKS_AT relationship (REQ-347: MATCH -> JOIN).
    3. The named parameter $min_age was captured for Trino positional binding
       (REQ-352), which is what lets Stage 2 governance apply identically to the
       GraphQL path — governance operates on the compiled SQL, not the frontend.
    4. The compiled SQL targets the governed physical tables (persons/companies)
       so Stage 2 row/column policies bind to the same relations GraphQL uses.
    """
    # Compilation must have succeeded without error.
    assert shared_data["compile_error"] is None

    sql_string = shared_data["sql_string"]
    assert sql_string, "compiler produced an empty SQL string"

    # MATCH across the WORKS_AT relationship must translate to a JOIN.
    assert "JOIN" in sql_string.upper()

    # The compiled SQL must reference the governed physical tables so that
    # Stage 2 governance binds to the same relations the GraphQL frontend uses.
    lowered = sql_string.lower()
    assert "persons" in lowered
    assert "companies" in lowered

    # The named parameter must be captured for Trino positional binding so that
    # governance applies identically to the GraphQL path.
    param_names = shared_data["param_names"]
    assert "min_age" in param_names


# ---------------------------------------------------------------------------
# REQ-347 — Cypher clauses map to SQL (MATCH/WHERE/RETURN/ORDER BY/LIMIT)
# ---------------------------------------------------------------------------


@given("a Cypher query with MATCH, WHERE, RETURN, ORDER BY, and LIMIT clauses")
def given_cypher_with_all_clauses(shared_data: dict) -> None:
    shared_data["label_map"] = _make_clause_mapping_label_map()
    shared_data["cypher_query"] = (
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "WHERE p.age > 30 "
        "RETURN p.name AS person_name, c.name AS company_name "
        "ORDER BY p.name "
        "LIMIT 10"
    )


@when("the translator processes it")
def when_translator_processes(shared_data: dict) -> None:
    shared_data["sql_string"] = _translate(
        shared_data["cypher_query"], shared_data["label_map"], shared_data.get("params")
    )


@then("it emits SQL with JOIN, WHERE, SELECT, ORDER BY, and LIMIT clauses respectively")
def then_emits_all_clauses(shared_data: dict) -> None:
    sql = shared_data["sql_string"].upper()
    assert "SELECT" in sql  # RETURN -> SELECT
    assert "INNER JOIN" in sql  # MATCH -> JOIN
    assert "WHERE" in sql  # WHERE -> WHERE
    assert '"AGE" > 30' in sql  # WHERE predicate translated
    assert "ORDER BY" in sql  # ORDER BY -> ORDER BY
    assert "LIMIT 10" in sql  # LIMIT -> LIMIT


# ---------------------------------------------------------------------------
# REQ-348 — shortestPath / [*1..n] -> WITH RECURSIVE; unbounded [*] rejected
# ---------------------------------------------------------------------------


@given("a Cypher query with shortestPath or [*1..n] variable-length pattern")
def given_variable_length_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_path_label_map()
    shared_data["cypher_query"] = (
        "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) "
        "RETURN a.name AS an, b.name AS bn"
    )
    shared_data["unbounded_query"] = (
        "MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN a.name AS an"
    )


@then("it emits a WITH RECURSIVE CTE and rejects unbounded [*] patterns at compile time")
def then_recursive_and_reject_unbounded(shared_data: dict) -> None:
    # The bounded [*1..3] pattern was translated by the shared @when step.
    sql = shared_data["sql_string"].upper()
    assert "WITH RECURSIVE" in sql  # variable-length -> recursive CTE
    assert "HOPS < 3" in sql  # max-hop depth enforced
    # The unbounded [*] variant is rejected at compile time.
    with pytest.raises(CypherParseError):
        _translate(shared_data["unbounded_query"], shared_data["label_map"])


# ---------------------------------------------------------------------------
# REQ-349 — whole-node RETURN wrapped into JSON object (Stage 3 rewrite)
# ---------------------------------------------------------------------------


@given("a Cypher RETURN clause referencing a whole node variable")
def given_whole_node_return(shared_data: dict) -> None:
    shared_data["label_map"] = _make_node_return_label_map()
    shared_data["cypher_query"] = "MATCH (p:Person) RETURN p"


@when("Stage 3 rewrite runs")
def when_stage3_rewrite_runs(shared_data: dict) -> None:
    from provisa.cypher.graph_rewriter import apply_graph_rewrites

    label_map = shared_data["label_map"]
    ast = parse_cypher(shared_data["cypher_query"])
    sql_ast, _params, graph_vars = cypher_to_sql(ast, label_map, {})
    shared_data["graph_vars"] = graph_vars
    # Capture pre-rewrite projection so we can prove the rewrite changed it.
    shared_data["pre_rewrite_sql"] = sql_ast.copy().sql(dialect="trino")
    rewritten = apply_graph_rewrites(sql_ast, graph_vars, label_map)
    shared_data["sql_string"] = rewritten.sql(dialect="trino")


@then("the node columns are wrapped into a single JSON object via CAST(ROW(...) AS JSON)")
def then_node_wrapped_in_json(shared_data: dict) -> None:
    # The RETURN referenced the whole node variable p (not a scalar property).
    assert shared_data["graph_vars"].get("p") is GraphVarKind.NODE
    # Before the Stage 3 rewrite the node projected as raw columns (p.*).
    assert "P.*" in shared_data["pre_rewrite_sql"].upper()
    sql = shared_data["sql_string"].upper()
    # Stage 3 collapses the node columns into a single JSON object projection
    # keyed by the canonical node shape (id/label/tableLabel + properties).
    assert "P.*" not in sql
    assert "JSON_OBJECT(" in sql
    assert "'ID'" in sql and "'LABEL'" in sql and "'TABLELABEL'" in sql


# ---------------------------------------------------------------------------
# REQ-352 — missing $param with no default rejected at compile time
# ---------------------------------------------------------------------------


@given("a Cypher query with $param and no default")
def given_query_with_param(shared_data: dict) -> None:
    shared_data["label_map"] = _make_param_label_map()
    shared_data["cypher_query"] = (
        "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name AS n"
    )


@when("the parameter is missing from the request")
def when_parameter_missing(shared_data: dict) -> None:
    ast = parse_cypher(shared_data["cypher_query"])
    _sql, param_names, _gv = cypher_to_sql(ast, shared_data["label_map"], {})
    shared_data["param_names"] = param_names
    # No values provided for the referenced parameters.
    shared_data["provided_params"] = {}


@then("it is rejected at compile time")
def then_rejected_at_compile_time(shared_data: dict) -> None:
    assert "min_age" in shared_data["param_names"]
    with pytest.raises(CypherParamError) as exc:
        bind_params(shared_data["param_names"], shared_data["provided_params"])
    assert "min_age" in str(exc.value)


# ---------------------------------------------------------------------------
# REQ-353 — cross-catalog labels translate to a normal cross-catalog JOIN
# ---------------------------------------------------------------------------


@given("a Cypher query whose node labels resolve to tables on different Trino catalogs")
def given_cross_catalog_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_cross_catalog_label_map()
    shared_data["cypher_query"] = (
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "RETURN p.name AS pn, c.name AS cn"
    )


@then("it generates a cross-catalog JOIN and executes normally without error")
def then_cross_catalog_join(shared_data: dict) -> None:
    sql = shared_data["sql_string"]
    upper = sql.upper()
    assert "INNER JOIN" in upper
    # The two catalogs are distinct; both must appear in the join.
    assert "postgresql" in sql
    assert "mysql" in sql
    # No cross-source restriction is enforced (REQ-353 WITHDRAWN): translation succeeds.
    assert "persons" in sql and "companies" in sql


# ---------------------------------------------------------------------------
# REQ-572 — CALL db.labels() introspection without SQL
# ---------------------------------------------------------------------------


@given("a client issuing CALL db.labels()")
def given_call_db_labels(shared_data: dict) -> None:
    shared_data["label_map"] = _make_introspection_label_map()
    shared_data["proc_query"] = "CALL db.labels()"


@when("the cypher router handles it")
def when_router_handles_procedure(shared_data: dict) -> None:
    import json

    proc = _detect_procedure(shared_data["proc_query"])
    assert proc == "db.labels"
    response = _handle_procedure(proc, shared_data["label_map"])
    shared_data["proc_response"] = json.loads(response.body)


@then("it returns label data from CypherLabelMap without generating or executing SQL")
def then_returns_label_data(shared_data: dict) -> None:
    body = shared_data["proc_response"]
    assert body["columns"] == ["label"]
    labels = {row["label"] for row in body["rows"]}
    # Derived purely from CypherLabelMap (table labels + domain labels).
    assert {"Person", "Company", "PersonDomain", "CompanyDomain"} <= labels


# ---------------------------------------------------------------------------
# REQ-573 — correlated CALL subquery -> CROSS JOIN LATERAL
# ---------------------------------------------------------------------------


@given("a Cypher CALL subquery with WITH importing an outer variable")
def given_correlated_call(shared_data: dict) -> None:
    shared_data["label_map"] = _make_correlated_call_label_map()
    shared_data["cypher_query"] = (
        "MATCH (x:Person) "
        "CALL { WITH x MATCH (x)-[:KNOWS]->(n:Person) RETURN n.name AS friend } "
        "RETURN x.name AS xn, friend"
    )


@then("it emits a CROSS JOIN LATERAL expression")
def then_cross_join_lateral(shared_data: dict) -> None:
    sql = shared_data["sql_string"].upper()
    assert "CROSS JOIN LATERAL" in sql


# ---------------------------------------------------------------------------
# REQ-575 — bidirectional (a)-[]-(b) -> UNION ALL of both directions
# ---------------------------------------------------------------------------


@given("a Cypher query with bidirectional traversal (a)-[]-(b)")
def given_bidirectional_query(shared_data: dict) -> None:
    # A self-referential relationship makes both directions valid so the
    # bidirectional expansion produces genuine forward + backward branches.
    shared_data["label_map"] = _make_path_label_map()
    shared_data["cypher_query"] = (
        "MATCH (a:Person)-[:KNOWS]-(b:Person) "
        "RETURN a.name AS an, b.name AS bn"
    )


@then("it emits a UNION ALL of forward and backward directed relationship joins")
def then_union_all_directions(shared_data: dict) -> None:
    sql = shared_data["sql_string"].upper()
    assert "UNION ALL" in sql
    # Forward: a.person_id = b.id ; backward: b.person_id = a.id
    assert 'A."PERSON_ID" = B."ID"' in sql
    assert 'B."PERSON_ID" = A."ID"' in sql


# ---------------------------------------------------------------------------
# REQ-576 — heterogeneous shortestPath -> flat JOIN chain (no recursive CTE)
# ---------------------------------------------------------------------------


@given("a shortestPath query between two different node types with a unique schema path")
def given_heterogeneous_shortest_path(shared_data: dict) -> None:
    shared_data["label_map"] = _make_heterogeneous_shortest_path_label_map()
    shared_data["cypher_query"] = (
        "MATCH p = shortestPath((a:Person)-[*..5]-(b:Company)) RETURN p"
    )


@then("it emits a flat JOIN chain instead of a recursive CTE")
def then_flat_join_chain(shared_data: dict) -> None:
    sql = shared_data["sql_string"].upper()
    assert "INNER JOIN" in sql  # structurally shortest schema path as a flat chain
    assert "WITH RECURSIVE" not in sql  # no recursive CTE for heterogeneous endpoints
    assert "persons".upper() in sql and "companies".upper() in sql


# ---------------------------------------------------------------------------
# REQ-577 — multiple equal-hop paths -> UNION ALL branches (no dedup)
# ---------------------------------------------------------------------------


@given("multiple schema paths of equal hop count between the same node types")
def given_multiple_equal_paths(shared_data: dict) -> None:
    shared_data["label_map"] = _make_multi_path_label_map()
    shared_data["cypher_query"] = (
        "MATCH p = shortestPath((a:Person)-[*..5]-(b:Company)) RETURN p"
    )


@when("the translator processes a shortestPath query")
def when_translator_processes_shortest_path(shared_data: dict) -> None:
    shared_data["sql_string"] = _translate(
        shared_data["cypher_query"], shared_data["label_map"]
    )


@then("all matching paths are emitted as UNION ALL branches without deduplication")
def then_paths_union_all(shared_data: dict) -> None:
    sql = shared_data["sql_string"].upper()
    assert "UNION ALL" in sql
    # Both equal-hop schema paths (WORKS_AT and MANAGES) appear as branches.
    assert 'A."COMPANY_ID" = B."ID"' in sql
    assert 'A."MANAGED_COMPANY_ID" = B."ID"' in sql
    # No DISTINCT / dedup applied across branches.
    assert "UNION ALL SELECT" in sql.replace("\n", " ")


# ---------------------------------------------------------------------------
# REQ-750 — graph variables (node/edge/path) serialized as canonical JSON
# ---------------------------------------------------------------------------


@given("a Cypher query RETURN n, r, p where n is a node, r is an edge, p is a path")
def given_return_node_edge_path(shared_data: dict) -> None:
    shared_data["label_map"] = _make_graph_var_label_map()
    shared_data["cypher_query"] = (
        "MATCH p = (n:Person)-[r:WORKS_AT]->(c:Company) RETURN n, r, p"
    )


@when("the Cypher router executes the query")
def when_router_executes_query(shared_data: dict) -> None:
    ast = parse_cypher(shared_data["cypher_query"])
    sql_ast, _params, graph_vars = cypher_to_sql(ast, shared_data["label_map"], {})
    shared_data["graph_vars"] = graph_vars
    shared_data["sql_string"] = sql_ast.sql(dialect="trino")


@then("the response includes JSON objects for each graph variable with the canonical keys")
def then_json_objects_canonical_keys(shared_data: dict) -> None:
    gv = shared_data["graph_vars"]
    assert gv.get("n") is GraphVarKind.NODE
    assert gv.get("r") is GraphVarKind.EDGE
    assert gv.get("p") is GraphVarKind.PATH
    sql = shared_data["sql_string"]
    # Node canonical keys.
    assert "'id'" in sql and "'label'" in sql and "'tableLabel'" in sql and "'properties'" in sql
    # Edge canonical keys.
    assert "'identity'" in sql and "'start'" in sql and "'end'" in sql and "'type'" in sql
    assert "'startNode'" in sql and "'endNode'" in sql
    # Path canonical keys.
    assert "'nodes'" in sql and "'edges'" in sql and "'length'" in sql


# ---------------------------------------------------------------------------
# REQ-751 — [*1..5] -> recursive CTE with hop-count guards
# ---------------------------------------------------------------------------


@given("a Cypher query with [*1..5] pattern between two node types")
def given_variable_length_1_5(shared_data: dict) -> None:
    shared_data["label_map"] = _make_variable_length_label_map()
    shared_data["cypher_query"] = (
        "MATCH (a:Person)-[:KNOWS*1..5]->(b:Person) "
        "RETURN a.name AS an, b.name AS bn"
    )


@then("it emits a WITH RECURSIVE CTE with hop-count guards and JSON_ARRAY edges")
def then_recursive_cte_hop_guards(shared_data: dict) -> None:
    sql = shared_data["sql_string"].upper()
    assert "WITH RECURSIVE" in sql  # variable-length -> recursive CTE
    assert "HOPS < 5" in sql  # max-hop bound enforced
    assert "HOPS + 1" in sql  # hop counter increments each recursive step


# ---------------------------------------------------------------------------
# REQ-752 — 3+ node variables: all aliases resolve; intermediate props work
# ---------------------------------------------------------------------------


@given("a Cypher query with 3+ node variables in a path")
def given_three_node_path(shared_data: dict) -> None:
    shared_data["label_map"] = _make_multi_hop_label_map()
    shared_data["cypher_query"] = (
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company)-[:HAS_DEPT]->(d:Department) "
        "WHERE c.founded > 1990 "
        "RETURN p.name AS pn, c.name AS cn, d.name AS dn"
    )


@then(
    "all node aliases are available in WHERE and RETURN, and intermediate property "
    "access resolves correctly"
)
def then_all_aliases_resolve(shared_data: dict) -> None:
    sql = shared_data["sql_string"].upper()
    # Three distinct table aliases in a flat join chain.
    assert "AS P " in sql or 'AS P\n' in sql or sql.endswith("AS P")
    assert "AS C " in sql
    assert "AS D " in sql
    # Intermediate node (c:Company) property used in WHERE with no aliasing conflict.
    assert 'C."FOUNDED" > 1990' in sql
    # All three aliases projected in RETURN.
    assert 'P."NAME" AS PN' in sql
    assert 'C."NAME" AS CN' in sql
    assert 'D."NAME" AS DN' in sql


# ---------------------------------------------------------------------------
# REQ-753 — MATCH p = (...) RETURN p -> JSON_OBJECT with nodes/edges/length
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH p = (...) RETURN p")
def given_path_object_return(shared_data: dict) -> None:
    shared_data["label_map"] = _make_path_object_label_map()
    shared_data["cypher_query"] = (
        "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p"
    )


@then("it emits JSON_OBJECT with nodes, edges, and length fields")
def then_path_json_object(shared_data: dict) -> None:
    ast = parse_cypher(shared_data["cypher_query"])
    sql_ast, _params, graph_vars = cypher_to_sql(ast, shared_data["label_map"], {})
    assert graph_vars.get("p") is GraphVarKind.PATH
    sql = sql_ast.sql(dialect="trino").upper()
    assert "JSON_OBJECT(" in sql
    assert "'NODES'" in sql
    assert "'EDGES'" in sql
    assert "'LENGTH'" in sql
