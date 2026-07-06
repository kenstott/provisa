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

REQ-754: Correlated CALL subqueries (e.g., `CALL { WITH x MATCH (x)-[:REL]->(y)
RETURN y }`) translate to CROSS JOIN LATERAL subqueries in Trino, preserving the
outer variable binding.

REQ-755: Node label alternation (e.g., `(n:Label1|Label2)`) translates to
UNION ALL branches in SQL, one per label candidate. Each branch includes only
the relevant node type and relationships that originate from it.
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
from provisa.cypher.graph_rewriter import apply_graph_rewrites
from provisa.cypher.assembler import (
    Node,
    Edge,
    Path,
    assemble_rows,
    to_serializable,
)
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
scenarios("../features/REQ-754.feature")
scenarios("../features/REQ-755.feature")
scenarios("../features/REQ-756.feature")
scenarios("../features/REQ-757.feature")
scenarios("../features/REQ-758.feature")
scenarios("../features/REQ-759.feature")
scenarios("../features/REQ-760.feature")
scenarios("../features/REQ-761.feature")
scenarios("../features/REQ-762.feature")
scenarios("../features/REQ-763.feature")
scenarios("../features/REQ-764.feature")
scenarios("../features/REQ-765.feature")
scenarios("../features/REQ-766.feature")
scenarios("../features/REQ-767.feature")
scenarios("../features/REQ-768.feature")
scenarios("../features/REQ-769.feature")
scenarios("../features/REQ-770.feature")
scenarios("../features/REQ-771.feature")
scenarios("../features/REQ-772.feature")
scenarios("../features/REQ-773.feature")
scenarios("../features/REQ-774.feature")
scenarios("../features/REQ-775.feature")
scenarios("../features/REQ-776.feature")
scenarios("../features/REQ-777.feature")
scenarios("../features/REQ-778.feature")


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
    """Label map for REQ-573 / REQ-754 correlated CALL subquery tests."""
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


def _make_label_alternation_label_map() -> CypherLabelMap:
    """Label map for REQ-755 label alternation tests.

    Provides TypeA and TypeB nodes so that MATCH (n:TypeA|TypeB) can resolve
    both label candidates to separate physical tables.
    """
    type_a_meta = NodeMapping(
        label="TypeA",
        type_name="TypeA",
        domain_label=None,
        table_label="TypeA",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="type_a_nodes",
        properties={"id": "id", "name": "name"},
    )
    type_b_meta = NodeMapping(
        label="TypeB",
        type_name="TypeB",
        domain_label=None,
        table_label="TypeB",
        table_id=2,
        source_id="pg-main",
        id_column="id",
        pk_columns=[],
        catalog_name="postgresql",
        schema_name="public",
        table_name="type_b_nodes",
        properties={"id": "id", "name": "name"},
    )
    return CypherLabelMap(
        nodes={"TypeA": type_a_meta, "TypeB": type_b_meta},
        relationships={},
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
    cypher_query = (
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
        "WHERE p.age > $min_age "
        "RETURN p.name AS person_name, c.name AS company_name, p.salary AS salary"
    )
    shared_data["label_map"] = label_map
    shared_data["cypher_query"] = cypher_query


@when("the compiler processes it")
def when_compiler_processes_it(shared_data: dict) -> None:
    """Compile the Cypher query to SQL exercising the real translator.

    We stop at the compiled-SQL boundary (Stage 1) — the point governance
    (Stage 2) acts upon — rather than hitting a live Trino, which is not
    available in the unit test environment. The translator is pure, so the
    emitted SQL is fully checkable.
    """
    sql = _translate(shared_data["cypher_query"], shared_data["label_map"], {"min_age": 18})
    shared_data["sql"] = sql


@then(
    "it compiles to SQL, executes via Trino, and applies Stage 2 governance "
    "identically to GraphQL queries"
)
def then_compiles_executes_governance(shared_data: dict) -> None:
    sql = shared_data["sql"]
    upper = sql.upper()
    # Compiled to SQL: a SELECT over the mapped physical tables.
    assert upper.startswith("SELECT")
    assert "persons" in sql and "companies" in sql
    # MATCH -> JOIN, WHERE -> WHERE preserved so Stage 2 governance
    # (RLS / masking / ceiling) applies identically to GraphQL-compiled SQL.
    assert "JOIN" in upper
    assert "WHERE" in upper


# ---------------------------------------------------------------------------
# Additional fixtures for REQ-758/773/774
# ---------------------------------------------------------------------------


def _make_employs_bidirectional_label_map() -> CypherLabelMap:
    """Person→Company (WORKS_AT) and Company→Person (EMPLOYS): true both-direction rels."""
    person = NodeMapping(
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
    company = NodeMapping(
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
        "EMPLOYS": RelationshipMapping(
            rel_type="EMPLOYS",
            source_label="Company",
            target_label="Person",
            join_source_column="employee_id",
            join_target_column="id",
            field_name="employs",
        ),
    }
    return CypherLabelMap(nodes={"Person": person, "Company": company}, relationships=rels)


def _make_domain_label_map() -> CypherLabelMap:
    """Domain 'Sales' groups Person + Company (REQ-773)."""
    person = NodeMapping(
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
    company = NodeMapping(
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
    return CypherLabelMap(
        nodes={"Person": person, "Company": company},
        relationships={},
        domains={"Sales": ["Person", "Company"]},
    )


def _make_id_collision_label_map() -> CypherLabelMap:
    """id_column='inquiry_id' but table also has a column literally named 'id' (REQ-774)."""
    inquiry = NodeMapping(
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
    return CypherLabelMap(
        nodes={"PetStore_Inquiries": inquiry},
        relationships={},
        domains={"PetStore": ["PetStore_Inquiries"]},
        nodes_by_table={"Inquiries": ["PetStore_Inquiries"]},
    )


# ---------------------------------------------------------------------------
# REQ-347 — clause mapping MATCH/WHERE/RETURN/ORDER BY/LIMIT
# ---------------------------------------------------------------------------


@given("a Cypher query with MATCH, WHERE, RETURN, ORDER BY, and LIMIT clauses")
def given_full_clause_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_clause_mapping_label_map()
    shared_data["cypher_query"] = (
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company) WHERE p.age > 30 "
        "RETURN p.name AS person_name ORDER BY p.name LIMIT 10"
    )


@when("the translator processes it")
def when_translator_processes(shared_data: dict) -> None:
    """Shared When step: translate the stored query with the stored label map.

    Reused by the majority of translator scenarios. Errors are captured so
    rejection scenarios can assert on them.
    """
    try:
        shared_data["result"] = _translate_full(
            shared_data["cypher_query"],
            shared_data["label_map"],
            shared_data.get("params"),
        )
        sql_ast = shared_data["result"][0]
        shared_data["graph_vars"] = shared_data["result"][2]
        shared_data["sql"] = sql_ast.sql(dialect="trino")
        shared_data["error"] = None
    except Exception as exc:  # noqa: BLE001 — rejection paths assert on this
        shared_data["error"] = exc
        shared_data["sql"] = None


@then("it emits SQL with JOIN, WHERE, SELECT, ORDER BY, and LIMIT clauses respectively")
def then_emits_all_clauses(shared_data: dict) -> None:
    assert shared_data["error"] is None
    upper = shared_data["sql"].upper()
    assert "SELECT" in upper
    assert "JOIN" in upper
    assert "WHERE" in upper
    assert "ORDER BY" in upper
    assert "LIMIT" in upper


# ---------------------------------------------------------------------------
# REQ-348 — path queries -> recursive CTE; unbounded [*] rejected
# ---------------------------------------------------------------------------


@given("a Cypher query with shortestPath or [*1..n] variable-length pattern")
def given_path_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_path_label_map()
    shared_data["cypher_query"] = "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b.name"


@then("it emits a WITH RECURSIVE CTE and rejects unbounded [*] patterns at compile time")
def then_recursive_cte_and_rejects_unbounded(shared_data: dict) -> None:
    assert shared_data["error"] is None
    assert "WITH RECURSIVE" in shared_data["sql"].upper()
    # Unbounded [*] must be rejected at compile time (parser level).
    with pytest.raises(CypherParseError):
        parse_cypher("MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN b.name")


# ---------------------------------------------------------------------------
# REQ-349 — whole-node RETURN -> JSON object wrap
# ---------------------------------------------------------------------------


@given("a Cypher RETURN clause referencing a whole node variable")
def given_whole_node_return(shared_data: dict) -> None:
    shared_data["label_map"] = _make_node_return_label_map()
    shared_data["cypher_query"] = "MATCH (p:Person) RETURN p"


@when("Stage 3 rewrite runs")
def when_stage3_rewrite_runs(shared_data: dict) -> None:
    ast = parse_cypher(shared_data["cypher_query"])
    sql_ast, _, graph_vars = cypher_to_sql(ast, shared_data["label_map"], {})
    # Stage 3: the SQLGlot graph rewrite wraps whole-node columns into JSON.
    rewritten = apply_graph_rewrites(sql_ast, graph_vars, shared_data["label_map"])
    shared_data["graph_vars"] = graph_vars
    shared_data["sql"] = rewritten.sql(dialect="trino")


@then("the node columns are wrapped into a single JSON object via CAST(ROW(...) AS JSON)")
def then_node_wrapped_json(shared_data: dict) -> None:
    # The whole-node variable is classified as a NODE graph variable and its
    # columns are collapsed into one JSON object column.
    assert shared_data["graph_vars"].get("p") == GraphVarKind.NODE
    upper = shared_data["sql"].upper()
    assert "JSON_OBJECT" in upper or "CAST(ROW" in upper
    # Node identity + properties are present in the single wrapped column.
    assert "'label'" in shared_data["sql"]
    assert "'name'" in shared_data["sql"]


# ---------------------------------------------------------------------------
# REQ-352 — missing $param rejected at compile time
# ---------------------------------------------------------------------------


@given("a Cypher query with $param and no default")
def given_param_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_param_label_map()
    shared_data["cypher_query"] = "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name"


@when("the parameter is missing from the request")
def when_param_missing(shared_data: dict) -> None:
    ast = parse_cypher(shared_data["cypher_query"])
    _, param_names, _ = cypher_to_sql(ast, shared_data["label_map"], {})
    shared_data["param_names"] = param_names


@then("it is rejected at compile time")
def then_param_rejected(shared_data: dict) -> None:
    assert "min_age" in shared_data["param_names"]
    # bind_params with an empty request must raise for the unbound name.
    with pytest.raises(CypherParamError):
        bind_params(shared_data["param_names"], {})


# ---------------------------------------------------------------------------
# REQ-353 — cross-catalog JOIN (withdrawn restriction) executes normally
# ---------------------------------------------------------------------------


@given("a Cypher query whose node labels resolve to tables on different Trino catalogs")
def given_cross_catalog_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_cross_catalog_label_map()
    shared_data["cypher_query"] = "MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name"


@then("it generates a cross-catalog JOIN and executes normally without error")
def then_cross_catalog_join(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    # No cross-source restriction: both catalogs appear in a single JOIN.
    assert '"postgresql"' in sql
    assert '"mysql"' in sql
    assert "JOIN" in sql.upper()


# ---------------------------------------------------------------------------
# REQ-572 — introspection procedures (no SQL)
# ---------------------------------------------------------------------------


@given("a client issuing CALL db.labels()")
def given_db_labels_call(shared_data: dict) -> None:
    shared_data["label_map"] = _make_introspection_label_map()
    shared_data["proc_query"] = "CALL db.labels()"


@when("the cypher router handles it")
def when_router_handles_proc(shared_data: dict) -> None:
    proc = _detect_procedure(shared_data["proc_query"])
    assert proc is not None
    shared_data["proc"] = proc
    shared_data["proc_response"] = _handle_procedure(proc, shared_data["label_map"])


@then("it returns label data from CypherLabelMap without generating or executing SQL")
def then_returns_label_data(shared_data: dict) -> None:
    import json as _json

    assert shared_data["proc"] == "db.labels"
    body = _json.loads(bytes(shared_data["proc_response"].body).decode())
    assert body["columns"] == ["label"]
    labels = {r["label"] for r in body["rows"]}
    # Table labels from the introspection label map, resolved in-memory.
    assert "Person" in labels
    assert "Company" in labels


# ---------------------------------------------------------------------------
# REQ-573 / REQ-754 — correlated CALL subquery -> CROSS JOIN LATERAL
# ---------------------------------------------------------------------------


@given("a Cypher CALL subquery with WITH importing an outer variable")
def given_correlated_call(shared_data: dict) -> None:
    shared_data["label_map"] = _make_correlated_call_label_map()
    shared_data["cypher_query"] = (
        "MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(n:Person) "
        "RETURN n.name AS fname } RETURN p.name, fname"
    )


@then("it emits a CROSS JOIN LATERAL expression")
def then_cross_join_lateral(shared_data: dict) -> None:
    assert shared_data["error"] is None
    assert "CROSS JOIN LATERAL" in shared_data["sql"].upper()


@given("a Cypher query with CALL { WITH x ... } correlated subquery")
def given_correlated_call_754(shared_data: dict) -> None:
    shared_data["label_map"] = _make_correlated_call_label_map()
    shared_data["cypher_query"] = (
        "MATCH (p:Person) CALL { WITH p MATCH (p)-[:KNOWS]->(y:Person) "
        "RETURN y.name AS yn } RETURN p.name, yn"
    )


@then("it emits CROSS JOIN LATERAL with the outer variable bound in the join condition")
def then_cross_join_lateral_bound(shared_data: dict) -> None:
    assert shared_data["error"] is None
    upper = shared_data["sql"].upper()
    assert "CROSS JOIN LATERAL" in upper
    # The correlated subquery references the outer table's alias (p) inside.
    assert "_call0" in shared_data["sql"]


# ---------------------------------------------------------------------------
# REQ-575 — bidirectional traversal expands to UNION ALL of both directions
# ---------------------------------------------------------------------------


@given("a Cypher query with bidirectional traversal (a)-[]-(b)")
def given_bidirectional_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_employs_bidirectional_label_map()
    shared_data["cypher_query"] = "MATCH (a:Person)-[]-(b:Company) RETURN a.name, b.name"


@then("it emits a UNION ALL of forward and backward directed relationship joins")
def then_bidirectional_union_all(shared_data: dict) -> None:
    assert shared_data["error"] is None
    upper = shared_data["sql"].upper()
    assert "UNION ALL" in upper
    # Both branches reference both physical tables.
    assert shared_data["sql"].lower().count("persons") >= 2
    assert shared_data["sql"].lower().count("companies") >= 2


# ---------------------------------------------------------------------------
# REQ-576 — heterogeneous shortestPath -> flat JOIN chain (no recursive CTE)
# ---------------------------------------------------------------------------


@given("a shortestPath query between two different node types with a unique schema path")
def given_heterogeneous_shortest_path(shared_data: dict) -> None:
    shared_data["label_map"] = _make_heterogeneous_shortest_path_label_map()
    shared_data["cypher_query"] = (
        "MATCH p = shortestPath((a:Person)-[:WORKS_AT*..5]->(b:Company)) RETURN p"
    )


@then("it emits a flat JOIN chain instead of a recursive CTE")
def then_flat_join_chain(shared_data: dict) -> None:
    assert shared_data["error"] is None
    upper = shared_data["sql"].upper()
    assert "JOIN" in upper
    # Heterogeneous endpoints with no self-referential rel -> no recursion.
    assert "WITH RECURSIVE" not in upper
    assert "persons" in shared_data["sql"]
    assert "companies" in shared_data["sql"]


# ---------------------------------------------------------------------------
# REQ-577 — multiple equal-hop schema paths -> UNION ALL branches
# ---------------------------------------------------------------------------


@given("multiple schema paths of equal hop count between the same node types")
def given_multi_path(shared_data: dict) -> None:
    shared_data["label_map"] = _make_multi_path_label_map()
    shared_data["cypher_query"] = "MATCH p = shortestPath((a:Person)-[*..3]->(b:Company)) RETURN p"


@when("the translator processes a shortestPath query")
def when_translator_processes_shortest_path(shared_data: dict) -> None:
    when_translator_processes(shared_data)


@then("all matching paths are emitted as UNION ALL branches without deduplication")
def then_all_paths_union_all(shared_data: dict) -> None:
    assert shared_data["error"] is None
    upper = shared_data["sql"].upper()
    assert "UNION ALL" in upper
    # Both relationship join columns appear -> both schema paths emitted.
    assert "company_id" in shared_data["sql"]
    assert "managed_company_id" in shared_data["sql"]
    # No DISTINCT / dedup across branches.
    assert "DISTINCT" not in upper


# ---------------------------------------------------------------------------
# REQ-750 — graph variables serialized as canonical JSON objects
# ---------------------------------------------------------------------------


@given("a Cypher query RETURN n, r, p where n is a node, r is an edge, p is a path")
def given_node_edge_path_return(shared_data: dict) -> None:
    shared_data["label_map"] = _make_graph_var_label_map()
    shared_data["cypher_query"] = "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) RETURN p, r, c"


@when("the Cypher router executes the query")
def when_router_executes_query(shared_data: dict) -> None:
    """Translate, then assemble mock Trino result rows through the real assembler.

    Live Trino is unavailable, so we feed the canonical JSON the translator's
    SQL would produce into the real assemble_rows + to_serializable path — the
    router's post-execution serialization boundary.
    """
    import json as _json

    ast = parse_cypher(shared_data["cypher_query"])
    sql_ast, _, graph_vars = cypher_to_sql(ast, shared_data["label_map"], {})
    shared_data["graph_vars"] = graph_vars
    shared_data["sql"] = sql_ast.sql(dialect="trino")

    node_json = _json.dumps(
        {"id": "Person|1", "label": "Person", "tableLabel": "Person", "name": "Alice"}
    )
    company_json = _json.dumps(
        {"id": "Company|2", "label": "Company", "tableLabel": "Company", "name": "Acme"}
    )
    edge_json = _json.dumps(
        {
            "identity": "WORKS_AT:1-2",
            "start": "1",
            "end": "2",
            "type": "WORKS_AT",
            "properties": {},
            "startNode": {"id": "1", "label": "Person"},
            "endNode": {"id": "2", "label": "Company"},
        }
    )
    raw_rows = [{"p": node_json, "r": edge_json, "c": company_json}]
    assembled = assemble_rows(raw_rows, graph_vars)
    shared_data["assembled"] = assembled
    shared_data["serialized"] = [to_serializable(r) for r in assembled]


@then("the response includes JSON objects for each graph variable with the canonical keys")
def then_canonical_json_objects(shared_data: dict) -> None:
    row = shared_data["assembled"][0]
    assert isinstance(row["p"], Node)
    assert isinstance(row["r"], Edge)
    assert isinstance(row["c"], Node)
    ser = shared_data["serialized"][0]
    # Node canonical keys.
    assert set(["id", "label", "tableLabel", "properties"]).issubset(ser["p"].keys())
    # Edge canonical keys.
    assert set(["identity", "start", "end", "type", "properties", "startNode", "endNode"]).issubset(
        ser["r"].keys()
    )


# ---------------------------------------------------------------------------
# REQ-751 — variable-length pattern -> recursive CTE with hop guards
# ---------------------------------------------------------------------------


@given("a Cypher query with [*1..5] pattern between two node types")
def given_variable_length_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_variable_length_label_map()
    shared_data["cypher_query"] = "MATCH (a:Person)-[:KNOWS*1..5]->(b:Person) RETURN b.name"


@then("it emits a WITH RECURSIVE CTE with hop-count guards and JSON_ARRAY edges")
def then_recursive_cte_hop_guards(shared_data: dict) -> None:
    assert shared_data["error"] is None
    upper = shared_data["sql"].upper()
    assert "WITH RECURSIVE" in upper
    # Hop-count guard on the recursive arm.
    assert "HOPS" in upper
    assert "< 5" in shared_data["sql"]


# ---------------------------------------------------------------------------
# REQ-752 — intermediate node property access in multi-hop patterns
# ---------------------------------------------------------------------------


@given("a Cypher query with 3+ node variables in a path")
def given_multi_hop_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_multi_hop_label_map()
    shared_data["cypher_query"] = (
        "MATCH (p:Person)-[:WORKS_AT]->(c:Company)-[:HAS_DEPT]->(d:Department) "
        "WHERE c.founded > 2000 RETURN p.name, c.name, d.name"
    )


@then(
    "all node aliases are available in WHERE and RETURN, and intermediate property "
    "access resolves correctly"
)
def then_multi_hop_aliases(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    # All three aliases resolve to their physical tables.
    assert " AS p" in sql
    assert " AS c" in sql
    assert " AS d" in sql
    # Intermediate node property in WHERE resolves to the correct alias.
    assert 'c."founded"' in sql
    # Terminal + intermediate properties in RETURN resolve.
    assert 'p."name"' in sql
    assert 'd."name"' in sql


# ---------------------------------------------------------------------------
# REQ-753 — path object RETURN -> JSON_OBJECT with nodes/edges/length
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH p = (...) RETURN p")
def given_path_object_return(shared_data: dict) -> None:
    shared_data["label_map"] = _make_path_object_label_map()
    shared_data["cypher_query"] = "MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p"


@then("it emits JSON_OBJECT with nodes, edges, and length fields")
def then_path_json_object(shared_data: dict) -> None:
    assert shared_data["error"] is None
    assert shared_data["graph_vars"].get("p") == GraphVarKind.PATH
    sql = shared_data["sql"]
    assert "JSON_OBJECT" in sql.upper()
    assert "'nodes'" in sql
    assert "'edges'" in sql
    assert "'length'" in sql


# ---------------------------------------------------------------------------
# REQ-755 — node label alternation -> UNION ALL branches
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH (n:TypeA|TypeB) RETURN n")
def given_label_alternation(shared_data: dict) -> None:
    shared_data["label_map"] = _make_label_alternation_label_map()
    shared_data["cypher_query"] = "MATCH (n:TypeA|TypeB) RETURN n.name"


@then("it emits UNION ALL with one branch per type, each selecting from the appropriate table")
def then_label_alternation_union(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    assert "UNION ALL" in sql.upper()
    assert "type_a_nodes" in sql
    assert "type_b_nodes" in sql


# ---------------------------------------------------------------------------
# REQ-756 — EXISTS { ... } subquery predicate
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH (n) WHERE EXISTS { ... } RETURN n")
def given_exists_subquery(shared_data: dict) -> None:
    shared_data["label_map"] = _make_correlated_call_label_map()
    shared_data["cypher_query"] = (
        "MATCH (n:Person) WHERE EXISTS { MATCH (n)-[:KNOWS]->(m:Person) } RETURN n.name"
    )


@then("it emits a correlated EXISTS subquery in the WHERE clause")
def then_exists_subquery(shared_data: dict) -> None:
    assert shared_data["error"] is None
    upper = shared_data["sql"].upper()
    assert "EXISTS" in upper
    assert "WHERE" in upper
    # Correlated: subquery contains its own SELECT.
    assert upper.count("SELECT") >= 2


# ---------------------------------------------------------------------------
# REQ-757 — map projection -> MAP(ARRAY[...], ARRAY[...])
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH (n:Person) RETURN n { .name, .age }")
def given_map_projection(shared_data: dict) -> None:
    shared_data["label_map"] = _make_param_label_map()
    shared_data["cypher_query"] = "MATCH (n:Person) RETURN n { .name, .age }"


@then("it emits MAP(ARRAY['name','age'], ARRAY[n.\"name\",n.\"age\"])")
def then_map_projection(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    assert "MAP" in sql.upper()
    assert "'name'" in sql and "'age'" in sql
    assert 'n."name"' in sql and 'n."age"' in sql


# ---------------------------------------------------------------------------
# REQ-758 — bidirectional expands to UNION ALL when both directions exist
# ---------------------------------------------------------------------------


@given(
    "a Cypher query MATCH (a:Person)-[]-(b:Company) where forward and backward relationships exist"
)
def given_bidirectional_both(shared_data: dict) -> None:
    shared_data["label_map"] = _make_employs_bidirectional_label_map()
    shared_data["cypher_query"] = "MATCH (a:Person)-[]-(b:Company) RETURN a.name, b.name"


@then("it emits UNION ALL with one branch per direction")
def then_union_per_direction(shared_data: dict) -> None:
    assert shared_data["error"] is None
    assert "UNION ALL" in shared_data["sql"].upper()
    assert shared_data["sql"].lower().count("persons") >= 2
    assert shared_data["sql"].lower().count("companies") >= 2


# ---------------------------------------------------------------------------
# REQ-759 — backward traversal inverts the join condition
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH (c:Company)<-[:WORKS_AT]-(p:Person)")
def given_backward_traversal(shared_data: dict) -> None:
    shared_data["label_map"] = _make_clause_mapping_label_map()
    shared_data["cypher_query"] = "MATCH (c:Company)<-[:WORKS_AT]-(p:Person) RETURN p.name, c.name"


@then("it emits a JOIN with swapped ON condition: p.company_id = c.id")
def then_swapped_join(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    assert "JOIN" in sql.upper()
    # Backward edge: source column stays on Person, joined onto Company.id.
    assert 'p."company_id" = c."id"' in sql
    # Company (target of the arrow) is the driving table (FROM companies).
    assert 'companies" AS c' in sql


# ---------------------------------------------------------------------------
# REQ-760 — string/list/numeric functions map to Trino equivalents
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH (n) RETURN left(n.name, 3), size(collect(n.age))")
def given_functions_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_param_label_map()
    shared_data["cypher_query"] = (
        "MATCH (n:Person) RETURN left(n.name, 3) AS pfx, size(collect(n.age)) AS cnt"
    )


@then("it emits Trino functions: left(...), cardinality(array_agg(...))")
def then_trino_functions(shared_data: dict) -> None:
    assert shared_data["error"] is None
    lower = shared_data["sql"].lower()
    assert "left(" in lower
    # size(collect(...)) -> cardinality over an aggregate.
    assert "cardinality(" in lower


# ---------------------------------------------------------------------------
# REQ-761 — implicit GROUP BY inferred from non-aggregated columns
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH (n) RETURN n.type, count(*) AS cnt")
def given_group_by_query(shared_data: dict) -> None:
    lm = _make_param_label_map()
    lm.nodes["Person"].properties["type"] = "type"
    shared_data["label_map"] = lm
    shared_data["cypher_query"] = "MATCH (n:Person) RETURN n.type, count(*) AS cnt"


@then("it emits GROUP BY n.type inferred from non-aggregated columns")
def then_group_by_inferred(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    assert "GROUP BY" in sql.upper()
    assert 'n."type"' in sql
    assert "COUNT(*)" in sql.upper()


# ---------------------------------------------------------------------------
# REQ-762 — CASE expressions
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH (n) RETURN CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END")
def given_case_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_param_label_map()
    shared_data["cypher_query"] = (
        "MATCH (n:Person) RETURN CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END AS cat"
    )


@then("it emits Trino CASE...WHEN...THEN...ELSE...END syntax")
def then_case_syntax(shared_data: dict) -> None:
    assert shared_data["error"] is None
    upper = shared_data["sql"].upper()
    assert "CASE" in upper and "WHEN" in upper and "THEN" in upper
    assert "ELSE" in upper and "END" in upper


# ---------------------------------------------------------------------------
# REQ-763 — UNWIND -> UNNEST with CROSS JOIN
# ---------------------------------------------------------------------------


@given("a Cypher query UNWIND [1, 2, 3] AS x RETURN x")
def given_unwind_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_param_label_map()
    shared_data["cypher_query"] = "UNWIND [1, 2, 3] AS x RETURN x"


@then("it emits CROSS JOIN (SELECT ... FROM UNNEST(ARRAY[...]))")
def then_unwind_unnest(shared_data: dict) -> None:
    assert shared_data["error"] is None
    upper = shared_data["sql"].upper()
    assert "UNNEST" in upper
    assert "ARRAY[1, 2, 3]" in shared_data["sql"]


# ---------------------------------------------------------------------------
# REQ-764 — IN list predicate
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH (n) WHERE n.age IN [25, 30, 35] RETURN n.name")
def given_in_list_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_param_label_map()
    shared_data["cypher_query"] = "MATCH (n:Person) WHERE n.age IN [25, 30, 35] RETURN n.name"


@then("it emits SQL IN (25, 30, 35) in the WHERE clause")
def then_in_list_sql(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    assert "WHERE" in sql.upper()
    assert "IN (25, 30, 35)" in sql


# ---------------------------------------------------------------------------
# REQ-765 — pattern comprehension -> ARRAY subquery
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH (p) RETURN [(p)-[:WORKS_AT]->(c:Company) | c.name]")
def given_pattern_comprehension(shared_data: dict) -> None:
    shared_data["label_map"] = _make_clause_mapping_label_map()
    shared_data["cypher_query"] = (
        "MATCH (p:Person) RETURN [(p)-[:WORKS_AT]->(c:Company) | c.name] AS companies"
    )


@then('it emits ARRAY(SELECT c."name" FROM ... WHERE ...)')
def then_pattern_comprehension_array(shared_data: dict) -> None:
    assert shared_data["error"] is None
    # The comprehension is preserved as a graph expression in the RETURN.
    ast = parse_cypher(shared_data["cypher_query"])
    assert ast.return_clause is not None
    item = ast.return_clause.items[0]
    assert item.alias == "companies"
    assert "WORKS_AT" in item.expression
    upper = shared_data["sql"].upper()
    assert "ARRAY" in upper
    assert "COMPANIES" in upper


# ---------------------------------------------------------------------------
# REQ-766 — length(p) on a path variable
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH p = (...) RETURN length(p)")
def given_length_path_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_path_object_label_map()
    shared_data["cypher_query"] = (
        "MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person)) RETURN length(p)"
    )


@then("it extracts the `hops` field from the path object or returns 1 for single-hop paths")
def then_length_hops(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    # Recursive CTE path -> length(p) resolves to the hops column.
    assert "WITH RECURSIVE" in sql.upper()
    assert "hops" in sql.lower()


# ---------------------------------------------------------------------------
# REQ-767 — recursive shortestPath -> ORDER BY hops LIMIT 1
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))")
def given_recursive_shortest_path(shared_data: dict) -> None:
    shared_data["label_map"] = _make_path_object_label_map()
    shared_data["cypher_query"] = (
        "MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person)) RETURN a.name, b.name"
    )


@then("it emits a WITH RECURSIVE CTE with ORDER BY hops LIMIT 1")
def then_recursive_order_by_hops(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    upper = sql.upper()
    assert "WITH RECURSIVE" in upper
    assert "ORDER BY" in upper
    assert "hops" in sql.lower()
    assert "LIMIT 1" in upper


# ---------------------------------------------------------------------------
# REQ-768 — Node deserialization from SQL result JSON
# ---------------------------------------------------------------------------


@given("a SQL result with a JSON column marked as NODE")
def given_node_result_rows(shared_data: dict) -> None:
    import json as _json

    node = {"id": "1", "label": "Person", "tableLabel": "Person", "name": "Alice", "age": 30}
    shared_data["raw_rows"] = [{"n": _json.dumps(node), "extra": 7}]
    shared_data["graph_vars"] = {"n": GraphVarKind.NODE}


@when("the assembler processes the rows")
def when_assembler_processes_rows(shared_data: dict) -> None:
    shared_data["assembled"] = assemble_rows(shared_data["raw_rows"], shared_data["graph_vars"])


@then("it deserializes the JSON into typed Node objects")
def then_typed_nodes(shared_data: dict) -> None:
    row = shared_data["assembled"][0]
    assert isinstance(row["n"], Node)
    assert row["n"].id == "1"
    assert row["n"].label == "Person"
    assert row["n"].properties.get("name") == "Alice"
    # Scalar column passed through untouched.
    assert row["extra"] == 7


# ---------------------------------------------------------------------------
# REQ-769 — Edge deserialization with start/end node objects
# ---------------------------------------------------------------------------


@given("a SQL result with a JSON column marked as EDGE")
def given_edge_result_rows(shared_data: dict) -> None:
    import json as _json

    edge = {
        "id": "e1",
        "type": "WORKS_AT",
        "startNode": {"id": "1", "label": "Person"},
        "endNode": {"id": "2", "label": "Company"},
        "since": 2020,
    }
    shared_data["raw_rows"] = [{"rel": _json.dumps(edge)}]
    shared_data["graph_vars"] = {"rel": GraphVarKind.EDGE}


@then("it deserializes the JSON into typed Edge objects with start/end node objects")
def then_typed_edges(shared_data: dict) -> None:
    edge = shared_data["assembled"][0]["rel"]
    assert isinstance(edge, Edge)
    assert edge.type == "WORKS_AT"
    assert isinstance(edge.start_node, Node)
    assert edge.start_node.id == "1"
    assert isinstance(edge.end_node, Node)
    assert edge.end_node.id == "2"


# ---------------------------------------------------------------------------
# REQ-770 — Path row collapse on _path_id / _depth
# ---------------------------------------------------------------------------


@given("SQL result rows with _path_id and _depth columns marking path hops")
def given_path_result_rows(shared_data: dict) -> None:
    import json as _json

    hop1 = {"nodes": [{"id": "1", "label": "Person"}], "edges": []}
    hop2 = {
        "nodes": [{"id": "2", "label": "Company"}],
        "edges": [
            {
                "id": "e1",
                "type": "WORKS_AT",
                "startNode": {"id": "1", "label": "Person"},
                "endNode": {"id": "2", "label": "Company"},
            }
        ],
    }
    shared_data["raw_rows"] = [
        {"_path_id": "p1", "_depth": 1, "path": _json.dumps(hop1)},
        {"_path_id": "p1", "_depth": 2, "path": _json.dumps(hop2)},
    ]
    shared_data["graph_vars"] = {"path": GraphVarKind.PATH}


@then("rows with matching _path_id are collapsed into a single Path object")
def then_path_collapsed(shared_data: dict) -> None:
    assembled = shared_data["assembled"]
    # Two hop rows sharing _path_id collapse to one output row.
    assert len(assembled) == 1
    path = assembled[0]["path"]
    assert isinstance(path, Path)
    # Both hop nodes merged into the single path.
    assert len(path.nodes) >= 2
    assert len(path.edges) >= 1


# ---------------------------------------------------------------------------
# REQ-771 — variable-length edge column -> list of Edge objects
# ---------------------------------------------------------------------------


@given("a SQL result with a JSON_ARRAY column containing edge objects from [*..n] pattern")
def given_edge_array_rows(shared_data: dict) -> None:
    import json as _json

    edges = [
        {
            "id": "e1",
            "type": "KNOWS",
            "startNode": {"id": "1", "label": "Person"},
            "endNode": {"id": "2", "label": "Person"},
        },
        {
            "id": "e2",
            "type": "KNOWS",
            "startNode": {"id": "2", "label": "Person"},
            "endNode": {"id": "3", "label": "Person"},
        },
    ]
    shared_data["raw_rows"] = [{"c": _json.dumps(edges)}]
    shared_data["graph_vars"] = {"c": GraphVarKind.EDGE}


@then("it deserializes the array into a list of Edge objects")
def then_edge_list(shared_data: dict) -> None:
    val = shared_data["assembled"][0]["c"]
    assert isinstance(val, list)
    assert len(val) == 2
    assert all(isinstance(e, Edge) for e in val)
    assert val[0].type == "KNOWS"
    assert val[1].start_node.id == "2"


# ---------------------------------------------------------------------------
# REQ-772 — graph rewriter wraps graph vars, leaves scalars unchanged
# ---------------------------------------------------------------------------


@given("an SQL query with both scalar and graph variable columns")
def given_scalar_and_graph_sql(shared_data: dict) -> None:
    from sqlglot import parse_one

    shared_data["label_map"] = _make_node_return_label_map()
    # A literal scalar column (cnt) that is not a graph variable, plus the
    # whole-node graph variable n.
    shared_data["sql_ast"] = parse_one(
        'SELECT 42 AS cnt, n AS n FROM "postgresql"."public"."persons" AS n',
        dialect="trino",
    )
    shared_data["graph_vars"] = {"n": GraphVarKind.NODE}


@when("the graph rewriter processes it")
def when_graph_rewriter_processes(shared_data: dict) -> None:
    result = apply_graph_rewrites(
        shared_data["sql_ast"], shared_data["graph_vars"], shared_data["label_map"]
    )
    shared_data["sql"] = result.sql(dialect="trino")


@then("scalar columns are left unchanged, graph variables are wrapped in JSON_OBJECT")
def then_scalar_unchanged_graph_wrapped(shared_data: dict) -> None:
    sql = shared_data["sql"]
    # Scalar literal column untouched (no JSON wrapping applied to it).
    assert "42 AS cnt" in sql
    # Graph var n wrapped into a JSON object.
    assert "JSON_OBJECT" in sql.upper()
    assert "'label'" in sql
    assert "END AS n" in sql


# ---------------------------------------------------------------------------
# REQ-773 — domain-scoped node projection -> UNION ALL over node types
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH (n:DomainLabel) RETURN n where DomainLabel groups multiple node types")
def given_domain_projection(shared_data: dict) -> None:
    shared_data["label_map"] = _make_domain_label_map()
    shared_data["cypher_query"] = "MATCH (n:Sales) RETURN n"


@then("it emits UNION ALL with one branch per node type, each projecting all domain properties")
def then_domain_union_all(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    assert "UNION ALL" in sql.upper()
    assert shared_data["graph_vars"].get("n") == GraphVarKind.NODE
    # One branch per node type in the domain.
    assert "persons" in sql and "companies" in sql


# ---------------------------------------------------------------------------
# REQ-774 — duplicate JSON id key prevention
# ---------------------------------------------------------------------------


@given("a table with id_column='inquiry_id' but also a column named 'id'")
def given_id_collision_table(shared_data: dict) -> None:
    from sqlglot import parse_one

    shared_data["label_map"] = _make_id_collision_label_map()
    shared_data["sql_ast"] = parse_one(
        'SELECT n AS n FROM "sqlite"."petstore"."inquiries" AS n', dialect="trino"
    )
    shared_data["graph_vars"] = {"n": GraphVarKind.NODE}


@when("the graph rewriter projects the node")
def when_graph_rewriter_projects_node(shared_data: dict) -> None:
    when_graph_rewriter_processes(shared_data)


@then("the JSON_OBJECT contains only one 'id' key corresponding to the id_column")
def then_single_id_key(shared_data: dict) -> None:
    import re as _re

    sql = shared_data["sql"]
    id_key_count = len(_re.findall(r"'id'", sql))
    assert id_key_count == 1, f"Duplicate 'id' JSON key: {sql}"


# ---------------------------------------------------------------------------
# REQ-775 — anonymous all-rels path pattern
# ---------------------------------------------------------------------------


@given("a Cypher query MATCH p=()-->() RETURN p LIMIT 25")
def given_anonymous_path(shared_data: dict) -> None:
    shared_data["label_map"] = _make_graph_var_label_map()
    shared_data["cypher_query"] = "MATCH p=()-->() RETURN p LIMIT 25"


@then("it emits a valid _all_rels subquery with JSON_OBJECT path serialization")
def then_all_rels_subquery(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    assert "_all_rels" in sql
    assert "JSON_OBJECT" in sql.upper()
    assert shared_data["graph_vars"].get("p") == GraphVarKind.PATH
    assert "LIMIT 25" in sql.upper()


# ---------------------------------------------------------------------------
# REQ-776 — OPTIONAL MATCH chain -> sequential LEFT JOINs
# ---------------------------------------------------------------------------


@given("a Cypher query with a chain of OPTIONAL MATCH clauses")
def given_optional_match_chain(shared_data: dict) -> None:
    shared_data["label_map"] = _make_multi_hop_label_map()
    shared_data["cypher_query"] = (
        "MATCH (p:Person) "
        "OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company) "
        "OPTIONAL MATCH (c)-[:HAS_DEPT]->(d:Department) "
        "RETURN p.name, c.name, d.name"
    )


@then("it emits sequential LEFT JOINs with null-aware WHERE conditions")
def then_sequential_left_joins(shared_data: dict) -> None:
    assert shared_data["error"] is None
    upper = shared_data["sql"].upper()
    # Two OPTIONAL MATCH clauses -> two LEFT JOINs.
    assert upper.count("LEFT JOIN") >= 2
    assert "companies" in shared_data["sql"]
    assert "departments" in shared_data["sql"]


# ---------------------------------------------------------------------------
# REQ-777 — UNION ALL query with distinct aliases + unified WHERE
# ---------------------------------------------------------------------------


@given("a Cypher UNION ALL query with property filters on both node and edge properties")
def given_union_all_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_clause_mapping_label_map()
    shared_data["cypher_query"] = (
        "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS label "
        "UNION ALL "
        "MATCH (c:Company) WHERE c.founded > 2000 RETURN c.name AS label"
    )


@then("it emits a valid SQL UNION ALL with matching column aliases and unified WHERE conditions")
def then_union_all_aliases(shared_data: dict) -> None:
    assert shared_data["error"] is None
    sql = shared_data["sql"]
    upper = sql.upper()
    assert "UNION ALL" in upper
    # Both branches share the same output alias.
    assert upper.count(" AS LABEL") == 2
    # Both branch filters preserved.
    assert 'p."age" > 30' in sql
    assert 'c."founded" > 2000' in sql


# ---------------------------------------------------------------------------
# REQ-778 — /data/cypher typed response shape (columns, rows, error)
# ---------------------------------------------------------------------------


@given("a Cypher query submitted to POST /data/cypher")
def given_data_cypher_query(shared_data: dict) -> None:
    shared_data["label_map"] = _make_graph_var_label_map()
    shared_data["cypher_query"] = "MATCH (p:Person) RETURN p.name AS name, p AS p"


@when("the endpoint processes and executes it")
def when_endpoint_processes(shared_data: dict) -> None:
    """Compile + assemble mock rows, then build the router's response content.

    Exercises the real translator, assembler, and to_serializable used by the
    /data/cypher handler to shape its typed response, without live Trino.
    """
    import json as _json

    ast = parse_cypher(shared_data["cypher_query"])
    _, _, graph_vars = cypher_to_sql(ast, shared_data["label_map"], {})
    node = {"id": "Person|1", "label": "Person", "tableLabel": "Person", "name": "Alice"}
    raw_rows = [{"name": "Alice", "p": _json.dumps(node)}]
    assembled = assemble_rows(raw_rows, graph_vars)
    columns = list(raw_rows[0].keys())
    rows = [to_serializable(r) for r in assembled]
    shared_data["response"] = {"columns": columns, "rows": rows, "error": None}


@then("the response includes columns, rows, and error fields with the correct shape")
def then_typed_response_shape(shared_data: dict) -> None:
    resp = shared_data["response"]
    assert set(["columns", "rows", "error"]).issubset(resp.keys())
    assert resp["columns"] == ["name", "p"]
    assert isinstance(resp["rows"], list) and len(resp["rows"]) == 1
    row = resp["rows"][0]
    # Scalar column preserved; graph var serialized to canonical node dict.
    assert row["name"] == "Alice"
    assert row["p"]["label"] == "Person"
    assert set(["id", "label", "tableLabel", "properties"]).issubset(row["p"].keys())
    assert resp["error"] is None
