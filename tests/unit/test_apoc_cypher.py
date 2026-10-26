# Copyright (c) 2026 Kenneth Stott
# Canary: 8a4f2d61-7b39-4c0e-9e52-1d6b3a8c7f04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2168: the APOC set over Cypher. A Cypher tool learns the graph's schema with
apoc.meta.data -- composed in a statement exactly as neo4j-graphrag (LangChain's Neo4jGraph)
writes it -- and is answered from the acting role's label map; an APOC function is its SQL
template, pushed down with the statement; an APOC name no function is bound to is refused."""

# Requirements: REQ-2168

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest
import sqlglot

from provisa.api.data.cypher_commands import expand_functions, put_command_rows
from provisa.api.errors import ApiError
from provisa.core.builtin_commands import declared
from provisa.cypher.parser import CypherParseError, parse_cypher
from provisa.gql.ir import WithClause
from provisa.gql.label_map import CypherLabelMap, NodeMapping, RelationshipMapping
from provisa.gql.translator import cypher_to_sql

pytestmark = [pytest.mark.unit, pytest.mark.asyncio(loop_scope="session")]

# neo4j-graphrag 1.22 (langchain-neo4j 0.11's schema reader), verbatim.
NODE_PROPERTIES_QUERY = (
    "CALL apoc.meta.data({sample: $SAMPLE}) "
    "YIELD label, other, elementType, type, property "
    "WHERE NOT type = 'RELATIONSHIP' AND elementType = 'node' "
    "AND NOT label IN $EXCLUDED_LABELS "
    "WITH label AS nodeLabel, collect({property:property, type:type}) AS properties "
    "RETURN {label: nodeLabel, properties: properties} AS output"
)
REL_PROPERTIES_QUERY = (
    "CALL apoc.meta.data({sample: $SAMPLE}) "
    "YIELD label, other, elementType, type, property "
    "WHERE NOT type = 'RELATIONSHIP' AND elementType = 'relationship' "
    "AND NOT label in $EXCLUDED_LABELS "
    "WITH label AS relType, collect({property:property, type:type}) AS properties "
    "RETURN {type: relType, properties: properties} AS output"
)
REL_QUERY = (
    "CALL apoc.meta.data({sample: $SAMPLE}) "
    "YIELD label, other, elementType, type, property "
    "WHERE type = 'RELATIONSHIP' AND elementType = 'node' "
    "UNWIND other AS other_node "
    "WITH * WHERE NOT label IN $EXCLUDED_LABELS "
    "AND NOT other_node IN $EXCLUDED_LABELS "
    "RETURN {start: label, type: property, end: toString(other_node)} AS output"
)
_EXCLUDED = ["_Bloom_Perspective_", "_Bloom_Scene_"]
_PARAMS = {"SAMPLE": 1000, "EXCLUDED_LABELS": _EXCLUDED}


def _node(label: str, table_id: int, types: dict[str, str]) -> NodeMapping:
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=table_id,
        source_id="pg",
        id_column="id",
        pk_columns=[],
        catalog_name="pg",
        schema_name="public",
        table_name=label.lower(),
        properties={p: p for p in types},
        property_types=tuple(types.items()),
    )


def _graph() -> CypherLabelMap:
    """What a role that reads Employees and Departments, but not Payroll, is served: its label
    map holds no Payroll."""
    employees = _node("Employees", 1, {"id": "integer", "name": "text", "dept_id": "integer"})
    departments = _node("Departments", 2, {"id": "integer", "title": "varchar(64)"})
    works_in = RelationshipMapping(
        rel_type="WORKS_IN",
        source_label="Employees",
        target_label="Departments",
        join_source_column="dept_id",
        join_target_column="id",
        field_name="dept",
    )
    return CypherLabelMap(
        nodes={"Employees": employees, "Departments": departments},
        relationships={"WORKS_IN": works_in},
        aliases={"WORKS_IN": [works_in]},
    )


@pytest.fixture
def state(monkeypatch):
    from provisa.security.inheritance import expand_grants

    graph = _graph()
    monkeypatch.setattr("provisa.bolt.apoc_meta._graph", lambda _state, _role: graph)
    shipped = declared()
    # The runtime build widens each grant to the roles inheriting it: reader inherits viewer, to
    # which the APOC set is granted; outsider inherits nothing.
    expand_grants(shipped, {"reader": ["reader", "viewer"], "outsider": ["outsider"]})
    return SimpleNamespace(
        roles={
            "reader": {"id": "reader", "capabilities": [], "domain_access": ["shelter"]},
            "outsider": {"id": "outsider", "capabilities": [], "domain_access": ["shelter"]},
        },
        tracked_functions={f["name"]: f for f in shipped},
        tracked_webhooks={},
        undefined_commands={},
        graph=graph,
    )


async def _rows(query: str, state, role: str = "reader") -> list:
    """The statement's rows: translated, bound for the role, its command rows put in place, and
    run on DuckDB with its list parameter written in."""
    sql_ast, ordered, _ = cypher_to_sql(parse_cypher(query), state.graph, _PARAMS)
    sql_ast = expand_functions(sql_ast, role, state)
    sql = await put_command_rows(sql_ast.sql(dialect="postgres"), role, state)
    assert ordered == ["EXCLUDED_LABELS"]
    engine_sql = sqlglot.transpile(sql, read="postgres", write="duckdb")[0]
    excluded = ", ".join(f"'{label}'" for label in _EXCLUDED)
    return duckdb.sql(engine_sql.replace("$1", f"({excluded})")).fetchall()


async def test_graphrag_reads_each_label_and_its_properties(state):
    rows = await _rows(NODE_PROPERTIES_QUERY, state)
    assert {r[0]["label"] for r in rows} == {'"Employees"', '"Departments"'}
    employees = next(r[0] for r in rows if r[0]["label"] == '"Employees"')
    assert employees["properties"] == (
        '[{"property":"id","type":"INTEGER"},{"property":"name","type":"STRING"},'
        '{"property":"dept_id","type":"INTEGER"}]'
    )


async def test_graphrag_reads_each_relationship_and_its_ends(state):
    rows = await _rows(REL_QUERY, state)
    assert rows == [({"start": '"Employees"', "type": '"WORKS_IN"', "end": '"Departments"'},)]


async def test_graphrag_reads_no_relationship_properties_where_none_are_declared(state):
    assert await _rows(REL_PROPERTIES_QUERY, state) == []


async def test_what_the_role_does_not_read_is_absent(state):
    rows = await _rows(NODE_PROPERTIES_QUERY, state) + await _rows(REL_QUERY, state)
    assert "Payroll" not in repr(rows)


async def test_a_role_reaching_only_its_own_domain_calls_the_apoc_set_by_its_grant(state):
    # reader lists only shelter: every role reaches the meta domain the APOC set sits in, and
    # reader inherits viewer, to which it is granted.
    assert await _rows(REL_QUERY, state)


async def test_a_role_without_the_grant_cannot_call_it(state):
    with pytest.raises(ApiError) as refused:
        await _rows(NODE_PROPERTIES_QUERY, state, role="outsider")
    assert refused.value.code == "functions.unknown_command"


async def test_an_apoc_function_is_its_template_pushed_down_with_the_statement(state):
    sql_ast, _, _ = cypher_to_sql(
        parse_cypher("MATCH (e:Employees) RETURN apoc.text.join([e.name, 'x'], ', ') AS j"),
        state.graph,
        {},
    )
    sql = expand_functions(sql_ast, "reader", state).sql(dialect="postgres")
    assert "ARRAY_TO_STRING(ARRAY[" in sql
    assert "apoc" not in sql.lower()


async def test_an_apoc_name_no_function_is_bound_to_is_refused_by_name(state):
    sql_ast, _, _ = cypher_to_sql(
        parse_cypher("MATCH (e:Employees) RETURN apoc.util.sleep(100) AS s"), state.graph, {}
    )
    with pytest.raises(ApiError) as refused:
        expand_functions(sql_ast, "reader", state)
    assert refused.value.code == "cypher.unknown_function"
    assert "apoc.util.sleep" in str(refused.value)


async def test_a_yield_naming_a_column_the_command_does_not_return_is_refused(state):
    query = "CALL apoc.meta.data({}) YIELD nope WHERE nope = 1 RETURN nope"
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), state.graph, {})
    with pytest.raises(ApiError, match="'nope' is not a column it returns"):
        await put_command_rows(sql_ast.sql(dialect="postgres"), "reader", state)


async def test_a_composed_call_names_what_it_yields():
    with pytest.raises(CypherParseError, match="name the columns it returns with YIELD"):
        parse_cypher("CALL apoc.meta.data({}) RETURN 1")


async def test_with_star_carries_every_variable_in_scope():
    ast = parse_cypher("UNWIND [1, 2] AS x UNWIND [3] AS y WITH * RETURN x, y")
    (with_clause,) = [s for s in ast.pipeline if isinstance(s, WithClause)]
    assert [i.name for i in with_clause.items] == ["x", "y"]


async def test_apoc_meta_schema_is_one_map_of_the_role_s_graph(state):
    from provisa.bolt.apoc_meta import meta_schema

    ((row,),) = [list(r.values()) for r in await meta_schema({}, state, "reader")]
    assert set(row) == {"Employees", "Departments", "WORKS_IN"}
    assert row["WORKS_IN"]["type"] == "relationship"
    assert row["Employees"]["relationships"]["WORKS_IN"]["labels"] == ["Departments"]
    assert row["Departments"]["properties"]["title"]["type"] == "STRING"


# --- the path expanders, compiled to the variable-length pattern they walk ---------------------


def _people() -> CypherLabelMap:
    people = _node("Person", 3, {"id": "integer", "name": "text", "friend_id": "integer"})
    knows = RelationshipMapping(
        rel_type="KNOWS",
        source_label="Person",
        target_label="Person",
        join_source_column="friend_id",
        join_target_column="id",
        field_name="friend",
    )
    return CypherLabelMap(
        nodes={"Person": people}, relationships={"KNOWS": knows}, aliases={"KNOWS": [knows]}
    )


def _sql(query: str) -> str:
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), _people(), {})
    return sql_ast.sql(dialect="postgres")


async def test_subgraph_nodes_is_the_recursive_sql_of_its_variable_length_pattern():
    walked = _sql(
        "MATCH (n:Person {name: 'a'}) CALL apoc.path.subgraphNodes(n, {relationshipFilter: "
        "'KNOWS>', labelFilter: '+Person', minLevel: 1, maxLevel: 2}) YIELD node "
        "RETURN DISTINCT node.name AS name"
    )
    written = _sql(
        "MATCH (n:Person {name: 'a'}) MATCH (n:Person)-[:KNOWS*1..2]->(node:Person) "
        "RETURN DISTINCT node.name AS name"
    )
    assert walked == written
    assert "WITH RECURSIVE" in walked


async def test_expand_binds_the_path_it_yields():
    walked = _sql(
        "MATCH (n:Person {name: 'a'}) CALL apoc.path.expand(n, '<KNOWS', '+Person', 1, 3) "
        "YIELD path "
        "RETURN count(path) AS paths"
    )
    assert "WITH RECURSIVE" in walked


@pytest.mark.parametrize(
    ("call", "refusal"),
    [
        ("apoc.path.expand(n, 'KNOWS', '+Person', 1, 2) YIELD path", "walks each type one way"),
        ("apoc.path.expand(n, 'KNOWS>', '', 1, 2) YIELD path", "the label the walk ends at"),
        ("apoc.path.expand(n, 'KNOWS>|<KNOWS', '+Person', 1, 2) YIELD path", "different"),
        ("apoc.path.expand(n, 'KNOWS>', '-Person', 1, 2) YIELD path", "blacklists"),
        ("apoc.path.expand(n, 'KNOWS>', '+Person', 1, -1) YIELD path", "unbounded"),
        (
            "apoc.path.subgraphNodes(n, {relationshipFilter: 'KNOWS>', labelFilter: '+Person'}) "
            "YIELD node",
            "unbounded",
        ),
        ("apoc.path.expand(n, 'KNOWS>', '+Person', 1, 2) YIELD node", "yields path"),
        ("apoc.path.expand(m, 'KNOWS>', '+Person', 1, 2) YIELD path", "matches with a label"),
    ],
)
async def test_a_walk_a_pattern_cannot_express_is_refused_naming_why(call, refusal):
    with pytest.raises(CypherParseError, match=refusal):
        parse_cypher(f"MATCH (n:Person) MATCH (m) CALL {call} RETURN 1 AS one")


async def test_apoc_meta_data_without_its_config_takes_the_default(state):
    # Older LangChain (langchain_community) calls apoc.meta.data() with no argument.
    query = (
        "CALL apoc.meta.data() YIELD label, other, elementType, type, property "
        "WHERE type = 'RELATIONSHIP' AND elementType = 'node' "
        "RETURN label, property"
    )
    sql_ast, _, _ = cypher_to_sql(parse_cypher(query), state.graph, {})
    sql = await put_command_rows(sql_ast.sql(dialect="postgres"), "reader", state)
    engine_sql = sqlglot.transpile(sql, read="postgres", write="duckdb")[0]
    assert duckdb.sql(engine_sql).fetchall() == [("Employees", "WORKS_IN")]
