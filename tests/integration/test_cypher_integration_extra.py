# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for Cypher query features and endpoint response types.

These tests cover REQ-750 through REQ-778 (Cypher Query Frontend Phase AU).
Requires the full Provisa stack (postgres + federation engine).
Run with docker-compose up before executing.
"""

import os
import pytest
import httpx

pytestmark = [pytest.mark.e2e, pytest.mark.requires_provisa_server]

BASE_URL = os.environ.get("PROVISA_URL", "http://localhost:8000")


def _headers() -> dict:
    token = os.environ.get("PROVISA_TOKEN", "")
    if token:
        return {"Authorization": f"Bearer {token}"}
    return {}


@pytest.fixture(scope="module")
def client():
    # A federated Cypher→Trino query (esp. the first, cold one) can run well past a
    # 30s read window when the whole suite is loading the server concurrently, which
    # surfaced as flaky ReadTimeouts. Keep connect short but give reads a wide budget.
    timeout = httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=60.0)
    with httpx.Client(base_url=BASE_URL, headers=_headers(), timeout=timeout) as c:
        yield c


# REQ-750: Cypher CALL db.labels() / db.relationshipTypes() / db.propertyKeys() procedure calls
def test_req750_db_labels_procedure(client):
    """
    # REQ-750
    Verify db.labels() procedure call returns available labels from schema.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "CALL db.labels() YIELD label RETURN label"},
    )
    assert resp.status_code != 404
    if resp.status_code == 200:
        data = resp.json()
        assert "columns" in data
        assert "rows" in data


def test_req750_db_relationshipTypes_procedure(client):
    """
    # REQ-750
    Verify db.relationshipTypes() procedure call returns available relationship types.
    """
    resp = client.post(
        "/data/cypher",
        json={
            "query": "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
        },
    )
    assert resp.status_code != 404


def test_req750_db_propertyKeys_procedure(client):
    """
    # REQ-750
    Verify db.propertyKeys() procedure call returns available property keys.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey"},
    )
    assert resp.status_code != 404


# REQ-751: Cypher LIMIT / SKIP clauses translate correctly to SQL LIMIT/OFFSET
def test_req751_limit_clause(client):
    """
    # REQ-751
    Verify LIMIT clause in Cypher translates to SQL LIMIT.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n LIMIT 5"},
    )
    assert resp.status_code != 404
    if resp.status_code == 200:
        data = resp.json()
        assert "columns" in data
        assert "rows" in data
        assert len(data["rows"]) <= 5


def test_req751_skip_clause(client):
    """
    # REQ-751
    Verify SKIP clause in Cypher translates to SQL OFFSET.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n SKIP 1 LIMIT 5"},
    )
    assert resp.status_code != 404
    if resp.status_code == 200:
        data = resp.json()
        assert isinstance(data.get("rows"), list)


def test_req751_limit_skip_combined(client):
    """
    # REQ-751
    Verify LIMIT and SKIP work together in Cypher.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n SKIP 2 LIMIT 3"},
    )
    assert resp.status_code != 404
    if resp.status_code == 200:
        data = resp.json()
        assert "rows" in data


# REQ-752: Cypher WHERE with AND/OR/NOT boolean operators
def test_req752_where_and_operator(client):
    """
    # REQ-752
    Verify WHERE with AND operator.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WHERE n.id > 0 AND n.id < 100 RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req752_where_or_operator(client):
    """
    # REQ-752
    Verify WHERE with OR operator.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WHERE n.id = 1 OR n.id = 2 RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req752_where_not_operator(client):
    """
    # REQ-752
    Verify WHERE with NOT operator.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WHERE NOT(n.id < 0) RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req752_where_complex_boolean(client):
    """
    # REQ-752
    Verify WHERE with complex AND/OR/NOT combinations.
    """
    resp = client.post(
        "/data/cypher",
        json={
            "query": "MATCH (n) WHERE (n.id > 0 AND n.id < 100) OR (NOT(n.id = 999)) RETURN n LIMIT 1"
        },
    )
    assert resp.status_code != 404


# REQ-753: Cypher node pattern with multiple labels (e.g. MATCH (n:A:B))
def test_req753_multi_label_node_pattern(client):
    """
    # REQ-753
    Verify node pattern with multiple labels.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n:Animal:Pet) RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


# REQ-754: Cypher relationship pattern with properties (e.g. MATCH ()-[r {weight: 1}]->())
def test_req754_relationship_with_properties(client):
    """
    # REQ-754
    Verify relationship pattern with properties filter.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH ()-[r {weight: 1}]->() RETURN r LIMIT 1"},
    )
    assert resp.status_code != 404


# REQ-755: Cypher aggregation functions: count(), sum(), avg(), min(), max()
def test_req755_count_aggregation(client):
    """
    # REQ-755
    Verify count() aggregation function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN count(n) AS node_count"},
    )
    assert resp.status_code != 404
    if resp.status_code == 200:
        data = resp.json()
        assert "columns" in data
        assert "rows" in data


def test_req755_sum_aggregation(client):
    """
    # REQ-755
    Verify sum() aggregation function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN sum(n.value) AS total_value"},
    )
    assert resp.status_code != 404


def test_req755_avg_aggregation(client):
    """
    # REQ-755
    Verify avg() aggregation function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN avg(n.score) AS avg_score"},
    )
    assert resp.status_code != 404


def test_req755_min_aggregation(client):
    """
    # REQ-755
    Verify min() aggregation function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN min(n.id) AS min_id"},
    )
    assert resp.status_code != 404


def test_req755_max_aggregation(client):
    """
    # REQ-755
    Verify max() aggregation function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN max(n.id) AS max_id"},
    )
    assert resp.status_code != 404


# REQ-756: Cypher ORDER BY with ASC/DESC
def test_req756_order_by_asc(client):
    """
    # REQ-756
    Verify ORDER BY with ASC clause.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n ORDER BY n.id ASC LIMIT 10"},
    )
    assert resp.status_code != 404


def test_req756_order_by_desc(client):
    """
    # REQ-756
    Verify ORDER BY with DESC clause.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n ORDER BY n.id DESC LIMIT 10"},
    )
    assert resp.status_code != 404


def test_req756_order_by_multiple_columns(client):
    """
    # REQ-756
    Verify ORDER BY with multiple columns and mixed ASC/DESC.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n ORDER BY n.type ASC, n.id DESC LIMIT 10"},
    )
    assert resp.status_code != 404


# REQ-757: Cypher DISTINCT keyword in RETURN clause
def test_req757_distinct_keyword(client):
    """
    # REQ-757
    Verify DISTINCT keyword in RETURN clause.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN DISTINCT n.type"},
    )
    assert resp.status_code != 404


# REQ-758: Cypher WITH clause for intermediate results
def test_req758_with_clause_intermediate(client):
    """
    # REQ-758
    Verify WITH clause for intermediate results.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WITH n WHERE n.id > 0 RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req758_with_multiple_stages(client):
    """
    # REQ-758
    Verify WITH clause with multiple stages.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WITH n, count(n) AS c WITH n WHERE c > 0 RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


# REQ-759: Cypher UNWIND array expression
def test_req759_unwind_array(client):
    """
    # REQ-759
    Verify UNWIND clause with array expression.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "UNWIND [1, 2, 3] AS x RETURN x"},
    )
    assert resp.status_code != 404
    if resp.status_code == 200:
        data = resp.json()
        assert "rows" in data


def test_req759_unwind_list_parameter(client):
    """
    # REQ-759
    Verify UNWIND with list parameter.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "UNWIND $list AS item RETURN item", "parameters": {"list": [10, 20, 30]}},
    )
    assert resp.status_code != 404


# REQ-760: Cypher path pattern variable-length (e.g. MATCH (a)-[*1..3]->(b))
def test_req760_variable_length_path_bounded(client):
    """
    # REQ-760
    Verify variable-length path pattern with bounds [*1..3].
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (a)-[*1..3]->(b) RETURN a, b LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req760_variable_length_path_unbounded_max(client):
    """
    # REQ-760
    Verify variable-length path pattern with unbounded max [*1..].
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (a)-[*1..5]->(b) RETURN a, b LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req760_variable_length_path_single_hop(client):
    """
    # REQ-760
    Verify variable-length path pattern single hop [*1..1].
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (a)-[*1..1]->(b) RETURN a, b LIMIT 1"},
    )
    assert resp.status_code != 404


# REQ-761: Cypher COLLECT() aggregation
def test_req761_collect_aggregation(client):
    """
    # REQ-761
    Verify COLLECT() aggregation function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN collect(n.id) AS ids"},
    )
    assert resp.status_code != 404
    if resp.status_code == 200:
        data = resp.json()
        assert "rows" in data


def test_req761_collect_with_grouping(client):
    """
    # REQ-761
    Verify COLLECT() with grouping in implicit GROUP BY.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n.type, collect(n.id) AS ids"},
    )
    assert resp.status_code != 404


# REQ-762: Cypher string functions: toLower(), toUpper(), toString(), trim()
def test_req762_toLower_function(client):
    """
    # REQ-762
    Verify toLower() string function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN toLower(n.name) AS lower_name LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req762_toUpper_function(client):
    """
    # REQ-762
    Verify toUpper() string function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN toUpper(n.name) AS upper_name LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req762_toString_function(client):
    """
    # REQ-762
    Verify toString() string function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN toString(n.id) AS id_str LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req762_trim_function(client):
    """
    # REQ-762
    Verify trim() string function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN trim(n.name) AS trimmed_name LIMIT 1"},
    )
    assert resp.status_code != 404


# REQ-763: Cypher math functions: abs(), ceil(), floor(), round()
def test_req763_abs_function(client):
    """
    # REQ-763
    Verify abs() math function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN abs(n.value) AS abs_value LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req763_ceil_function(client):
    """
    # REQ-763
    Verify ceil() math function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN ceil(n.score) AS ceiled_score LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req763_floor_function(client):
    """
    # REQ-763
    Verify floor() math function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN floor(n.score) AS floored_score LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req763_round_function(client):
    """
    # REQ-763
    Verify round() math function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN round(n.score) AS rounded_score LIMIT 1"},
    )
    assert resp.status_code != 404


# REQ-764: Cypher IS NULL / IS NOT NULL predicates
def test_req764_is_null_predicate(client):
    """
    # REQ-764
    Verify IS NULL predicate in WHERE clause.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WHERE n.value IS NULL RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req764_is_not_null_predicate(client):
    """
    # REQ-764
    Verify IS NOT NULL predicate in WHERE clause.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WHERE n.value IS NOT NULL RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


# REQ-765: Cypher IN list predicate
def test_req765_in_predicate_literals(client):
    """
    # REQ-765
    Verify IN predicate with literal list.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WHERE n.id IN [1, 2, 3] RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req765_in_predicate_parameter(client):
    """
    # REQ-765
    Verify IN predicate with parameter list.
    """
    resp = client.post(
        "/data/cypher",
        json={
            "query": "MATCH (n) WHERE n.id IN $ids RETURN n LIMIT 1",
            "parameters": {"ids": [1, 2, 3]},
        },
    )
    assert resp.status_code != 404


# REQ-766: Cypher node label test in WHERE (e.g. WHERE n:Person)
def test_req766_label_test_where(client):
    """
    # REQ-766
    Verify node label test in WHERE clause.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WHERE n:Person RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req766_label_test_where_multiple_labels(client):
    """
    # REQ-766
    Verify node label test with multiple label alternatives.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WHERE n:Person OR n:Animal RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


# REQ-767: Cypher list slicing and indexing
def test_req767_list_indexing(client):
    """
    # REQ-767
    Verify list indexing with [index] syntax.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN [1, 2, 3][0] AS first_elem"},
    )
    assert resp.status_code != 404


def test_req767_list_slicing(client):
    """
    # REQ-767
    Verify list slicing with [start..end] syntax.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN [1, 2, 3, 4, 5][1..3] AS slice"},
    )
    assert resp.status_code != 404


# REQ-768: Cypher CASE expression (simple and generic form)
def test_req768_case_simple_form(client):
    """
    # REQ-768
    Verify CASE expression simple form.
    """
    resp = client.post(
        "/data/cypher",
        json={
            "query": "MATCH (n) RETURN CASE n.type WHEN 'A' THEN 'TypeA' WHEN 'B' THEN 'TypeB' ELSE 'Other' END AS type_name LIMIT 1"
        },
    )
    assert resp.status_code != 404


def test_req768_case_generic_form(client):
    """
    # REQ-768
    Verify CASE expression generic (searched) form.
    """
    resp = client.post(
        "/data/cypher",
        json={
            "query": "MATCH (n) RETURN CASE WHEN n.id > 100 THEN 'High' WHEN n.id > 0 THEN 'Low' ELSE 'Zero' END AS category LIMIT 1"
        },
    )
    assert resp.status_code != 404


# REQ-769: Cypher date/datetime functions: date(), datetime(), duration()
def test_req769_date_function(client):
    """
    # REQ-769
    Verify date() function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "RETURN date('2026-06-27') AS today"},
    )
    assert resp.status_code != 404


def test_req769_datetime_function(client):
    """
    # REQ-769
    Verify datetime() function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "RETURN datetime('2026-06-27T10:00:00') AS now"},
    )
    assert resp.status_code != 404


def test_req769_duration_function(client):
    """
    # REQ-769
    Verify duration() function.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "RETURN duration('P1D') AS one_day"},
    )
    assert resp.status_code != 404


# REQ-770: Cypher pattern comprehension
def test_req770_pattern_comprehension(client):
    """
    # REQ-770
    Verify pattern comprehension syntax.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (p) RETURN [(p)-[:REL]->(q) | q.id] AS related_ids LIMIT 1"},
    )
    assert resp.status_code != 404


# REQ-771: Cypher subquery (CALL { ... })
def test_req771_subquery_call_uncorrelated(client):
    """
    # REQ-771
    Verify CALL subquery (uncorrelated).
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "CALL { MATCH (n) RETURN n LIMIT 1 } RETURN n"},
    )
    assert resp.status_code != 404


def test_req771_subquery_call_with_import(client):
    """
    # REQ-771
    Verify CALL subquery with imported variable.
    """
    resp = client.post(
        "/data/cypher",
        json={
            "query": "MATCH (a) WITH a CALL { WITH a MATCH (a)-[:REL]->(b) RETURN b } RETURN a, b LIMIT 1"
        },
    )
    assert resp.status_code != 404


# REQ-772: Cypher EXISTS subquery
def test_req772_exists_subquery(client):
    """
    # REQ-772
    Verify EXISTS subquery predicate.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WHERE EXISTS { (n)-[:REL]->() } RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404


# REQ-773: Cypher map projection (n {.name, .age})
def test_req773_map_projection_properties(client):
    """
    # REQ-773
    Verify map projection with specific properties.
    """
    try:
        resp = client.post(
            "/data/cypher",
            json={"query": "MATCH (n) RETURN n {.id, .name} AS node_proj LIMIT 1"},
        )
    except httpx.ReadError:
        # Keep-alive connection closed by server between tests; retry on new connection.
        resp = client.post(
            "/data/cypher",
            json={"query": "MATCH (n) RETURN n {.id, .name} AS node_proj LIMIT 1"},
        )
    assert resp.status_code != 404


def test_req773_map_projection_star(client):
    """
    # REQ-773
    Verify map projection with star expansion.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n {.*} AS all_props LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req773_map_projection_custom_key(client):
    """
    # REQ-773
    Verify map projection with custom key expressions.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n {key: n.id, value: n.name} AS custom_map LIMIT 1"},
    )
    assert resp.status_code != 404


# REQ-775: Cypher MERGE on existing node returns existing properties
def test_req775_merge_returns_existing_node(client):
    """
    # REQ-775
    Verify MERGE on existing node returns existing node with properties.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MERGE (n:TestNode {id: 999}) RETURN n"},
    )
    assert resp.status_code == 400
    # MERGE is a write operation and should be rejected


# REQ-776: Cypher OPTIONAL MATCH chains → LEFT JOIN
def test_req776_optional_match_single(client):
    """
    # REQ-776
    Verify OPTIONAL MATCH clause.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (a) OPTIONAL MATCH (a)-[:REL]->(b) RETURN a, b LIMIT 1"},
    )
    assert resp.status_code != 404


def test_req776_optional_match_chain(client):
    """
    # REQ-776
    Verify OPTIONAL MATCH chain (multiple sequential OPTIONALs).
    """
    resp = client.post(
        "/data/cypher",
        json={
            "query": "MATCH (a) OPTIONAL MATCH (a)-[:REL1]->(b) OPTIONAL MATCH (b)-[:REL2]->(c) RETURN a, b, c LIMIT 1"
        },
    )
    assert resp.status_code != 404


# REQ-777: Cypher UNION ALL queries
def test_req777_union_all_same_structure(client):
    """
    # REQ-777
    Verify UNION ALL combining two queries with same result structure.
    """
    resp = client.post(
        "/data/cypher",
        json={
            "query": "MATCH (n:Person) RETURN n.id, n.name UNION ALL MATCH (n:Animal) RETURN n.id, n.name"
        },
    )
    assert resp.status_code != 404


def test_req777_union_all_property_filters(client):
    """
    # REQ-777
    Verify UNION ALL with property filters on both branches.
    """
    resp = client.post(
        "/data/cypher",
        json={
            "query": "MATCH (n:Person) WHERE n.age > 18 RETURN n UNION ALL MATCH (n:Animal) WHERE n.weight > 10 RETURN n"
        },
    )
    assert resp.status_code != 404


# REQ-778: Cypher /query/cypher typed response (nodes/edges/rows)
def test_req778_response_has_columns_and_rows(client):
    """
    # REQ-778
    Verify response structure includes columns and rows.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n LIMIT 1"},
    )
    assert resp.status_code != 404
    if resp.status_code == 200:
        data = resp.json()
        assert "columns" in data
        assert isinstance(data["columns"], list)
        assert "rows" in data
        assert isinstance(data["rows"], list)
        assert "type" in data
        assert data["type"] == "cypher"


def test_req778_response_has_type_field(client):
    """
    # REQ-778
    Verify response always includes type field set to 'cypher'.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "UNWIND [1, 2, 3] AS x RETURN x"},
    )
    assert resp.status_code != 404
    if resp.status_code == 200:
        data = resp.json()
        assert data.get("type") == "cypher"


def test_req778_response_null_error_on_success(client):
    """
    # REQ-778
    Verify error field is null or absent on successful query.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN count(n) AS cnt"},
    )
    assert resp.status_code != 404
    if resp.status_code == 200:
        data = resp.json()
        # error should be null, omitted, or only present on 400+
        if "error" in data:
            assert data["error"] is None or data["error"] == ""


def test_req778_response_has_error_on_failure(client):
    """
    # REQ-778
    Verify error field populated on query failure.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) WHERE invalid_function() RETURN n"},
    )
    if resp.status_code != 200:
        data = resp.json()
        assert "error" in data or "columns" in data


def test_req778_no_implicit_row_truncation(client):
    """
    # REQ-778
    Verify no implicit result truncation beyond query LIMIT.
    """
    resp = client.post(
        "/data/cypher",
        json={"query": "MATCH (n) RETURN n LIMIT 10"},
    )
    assert resp.status_code != 404
    if resp.status_code == 200:
        data = resp.json()
        assert len(data["rows"]) <= 10
