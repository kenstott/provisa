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
    "it compiles to SQL, executes via Trino, and applies Stage 2 governance "
    "identically to GraphQL queries"
)
def then_compiles_to_sql_and_applies_governance(shared_data: dict) -> None:
    """Assert that:

    1. The compiler produced a non-empty SQL string (compilation succeeded).
    2. The SQL contains a JOIN, since the Cypher pattern traverses the
       WORKS_AT relationship (REQ-347: MATCH -> JOIN).
    3. The named parameter $min_age was captured for Trino positional binding
       (REQ-352), which is what lets Stage 2 governance apply identically to the
