# Copyright (c) 2026 Kenneth Stott
# Canary: 480dadfb-6893-49ee-a357-80986a314c94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A CREATE answers its RETURN by a read of the node it created, and a relationship a registered
junction backs is written as a row of that junction between two matched nodes (REQ-665, amended
2026-10-27). Pure: the surface's read and write are stand-ins recording what they are given."""

# Requirements: REQ-665, REQ-670

from __future__ import annotations

import asyncio

import pytest

from provisa.cypher.write_translator import (
    CypherParam,
    CypherWriteParseError,
    WriteTranslator,
    parse_cypher_write,
    written,
)
from provisa.gql.label_map import (
    CypherLabelMap,
    JunctionMapping,
    NodeMapping,
    RelationshipMapping,
)


def _node(label: str, table: str, props: dict[str, str]) -> NodeMapping:
    return NodeMapping(
        label=label,
        type_name=label,
        domain_label=None,
        table_label=label,
        table_id=1,
        source_id="pg",
        id_column="id",
        pk_columns=["id"],
        catalog_name="",
        schema_name="conformance",
        table_name=table,
        properties=props,
    )


def _junction(rel_type: str, source: str, target: str, table: str) -> RelationshipMapping:
    return RelationshipMapping(
        rel_type=rel_type,
        source_label=source,
        target_label=target,
        join_source_column="id",
        join_target_column="id",
        field_name=table,
        via=JunctionMapping(
            catalog_name="",
            schema_name="conformance",
            table_name=table,
            type_name=table,
            source_columns=("from_id",),
            target_columns=("to_id",),
            attributes={"since": "since"},
        ),
    )


def _map() -> CypherLabelMap:
    knows = _junction("KNOWS", "Person", "Person", "knows")
    works = _junction("WORKS_FOR", "Person", "Company", "works_for")
    fk = RelationshipMapping(
        rel_type="BASED_IN",
        source_label="Company",
        target_label="Person",
        join_source_column="id",
        join_target_column="company_id",
        field_name="based_in",
    )
    return CypherLabelMap(
        nodes={
            "Person": _node("Person", "person", {"id": "id", "name": "name"}),
            "Company": _node("Company", "company", {"id": "id", "name": "name"}),
            "Greeting": _node("Greeting", "greeting", {"id": "id", "message": "message"}),
        },
        relationships={"KNOWS": knows, "WORKS_FOR": works, "BASED_IN": fk},
        aliases={"KNOWS": [knows], "WORKS_FOR": [works], "BASED_IN": [fk]},
    )


class _Surface:
    """The surface's read and write, answering what each test gives them."""

    def __init__(
        self, reads: list[tuple[list[str], list[list]]], wrote: tuple[list[str], list[list]]
    ):
        self.reads, self.wrote = list(reads), wrote
        self.read_calls: list[tuple[str, dict]] = []
        self.write_calls: list[tuple[str, list]] = []

    async def read(self, cypher: str, params: dict):
        self.read_calls.append((cypher, params))
        return self.reads.pop(0)

    async def write(self, sql: str, bound: list):
        self.write_calls.append((sql, bound))
        return (*self.wrote, None)


def _run(query: str, params: dict, surface: _Surface):
    ast = parse_cypher_write(query)
    return asyncio.run(written(ast, WriteTranslator(_map()), params, surface.read, surface.write))


def test_a_create_answers_its_return_by_reading_the_node_it_created():
    surface = _Surface(reads=[(["id(a)"], [[41]])], wrote=(["_id"], [[7]]))
    result = _run("CREATE (a:Person {name: $name}) RETURN id(a)", {"name": "Alice"}, surface)
    assert surface.write_calls == [
        (
            'INSERT INTO "conformance"."person" ("name") VALUES ($1) RETURNING "id" AS "_id"',
            ["Alice"],
        )
    ]
    assert surface.read_calls == [
        (
            "MATCH (a:Person {_id: $provisa_write_key}) RETURN id(a)",
            {"name": "Alice", "provisa_write_key": 7},
        )
    ]
    assert result == (["id(a)"], [[41]])


def test_a_set_on_the_node_created_is_its_property():
    ast = parse_cypher_write(
        "CREATE (a:Greeting) SET a.message = $message RETURN a.message + ', from node ' + id(a)"
    )
    assert ast.props == {"message": CypherParam("message")}
    assert ast.returns == "a.message + ', from node ' + id(a)"


def test_a_create_without_return_returns_nothing_read():
    surface = _Surface(reads=[], wrote=([], []))
    assert _run("CREATE (a:Person {name: 'Bob'})", {}, surface) == ([], [])
    assert surface.write_calls == [
        ('INSERT INTO "conformance"."person" ("name") VALUES (\'Bob\')', [])
    ]
    assert surface.read_calls == []


def test_a_relationship_is_a_junction_row_between_the_nodes_matched():
    surface = _Surface(reads=[(["a.id"], [[1]]), (["b.id"], [[2], [3]])], wrote=([], []))
    _run(
        "MATCH (a:Person {name: $a}) MATCH (b:Person {name: $b}) "
        "CREATE (a)-[:KNOWS {since: 2020}]->(b)",
        {"a": "Alice", "b": "Bob"},
        surface,
    )
    # Each node read apart: unrelated patterns are every combination of their nodes.
    assert [q for q, _ in surface.read_calls] == [
        "MATCH (a:Person {name: $provisa_write_0_name}) RETURN a.id",
        "MATCH (b:Person {name: $provisa_write_1_name}) RETURN b.id",
    ]
    assert surface.read_calls[1][1]["provisa_write_1_name"] == "Bob"
    assert surface.write_calls == [
        (
            'INSERT INTO "conformance"."knows" ("from_id", "to_id", "since") '
            "VALUES (1, 2, 2020), (1, 3, 2020)",
            [],
        )
    ]


def test_an_inbound_relationship_starts_at_its_far_node():
    surface = _Surface(reads=[(["c.id"], [[9]]), (["p.id"], [[3]])], wrote=([], []))
    _run(
        "MATCH (c:Company {name: 'Acme'}), (p:Person {name: 'Alice'}) CREATE (c)<-[:WORKS_FOR]-(p)",
        {},
        surface,
    )
    assert surface.write_calls == [
        ('INSERT INTO "conformance"."works_for" ("from_id", "to_id") VALUES (3, 9)', [])
    ]


def test_a_node_related_to_itself_gives_both_keys():
    surface = _Surface(reads=[(["a.id", "a.id"], [[4, 4]])], wrote=([], []))
    _run("MATCH (a:Person {name: 'x'}) CREATE (a)-[:KNOWS]->(a)", {}, surface)
    assert (
        surface.read_calls[0][0]
        == "MATCH (a:Person {name: $provisa_write_0_name}) RETURN a.id, a.id"
    )
    assert surface.write_calls == [
        ('INSERT INTO "conformance"."knows" ("from_id", "to_id") VALUES (4, 4)', [])
    ]


def test_no_matched_nodes_write_no_row():
    surface = _Surface(reads=[(["a.id"], []), (["b.id"], [[2]])], wrote=([], []))
    result = _run(
        "MATCH (a:Person {name: 'x'}), (b:Person {name: 'y'}) CREATE (a)-[:KNOWS]->(b)", {}, surface
    )
    assert result == ([], [])
    assert surface.write_calls == []


@pytest.mark.parametrize(
    ("query", "refusal"),
    [
        (
            "MATCH (a:Person {name: 'x'}), (b:Person {name: 'y'}) CREATE (a)-[:LIKES]->(b)",
            "LIKES is not registered",
        ),
        (
            "MATCH (a:Company {name: 'x'}), (b:Person {name: 'y'}) CREATE (a)-[:KNOWS]->(b)",
            "KNOWS is not registered from Company to Person",
        ),
        (
            "MATCH (a:Company {name: 'x'}), (b:Person {name: 'y'}) CREATE (a)-[:BASED_IN]->(b)",
            "derived from a foreign key",
        ),
        (
            "MATCH (a:Person {name: 'x'}), (b:Person {name: 'y'}) CREATE (a)-[:KNOWS {weight: 1}]->(b)",
            "KNOWS has no property weight",
        ),
    ],
)
def test_a_relationship_the_model_does_not_store_in_a_junction_is_refused_by_name(query, refusal):
    with pytest.raises(CypherWriteParseError, match=refusal):
        _run(query, {}, _Surface(reads=[(["a.id"], [[1]]), (["b.id"], [[2]])], wrote=([], [])))


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (a:Person {name: 'x'}) CREATE (a)-[:KNOWS]->(b)",  # b is not matched: never created
        "MATCH (a:Person {name: 'x'}) WHERE a.id > 1 MATCH (b:Person) CREATE (a)-[:KNOWS]->(b)",
        "MATCH (a:Person), (b:Person) CREATE (a)-[:KNOWS]-(b)",  # no direction
    ],
)
def test_a_relationship_create_outside_its_form_is_refused(query):
    with pytest.raises(CypherWriteParseError):
        parse_cypher_write(query)


def test_a_node_created_without_a_variable_is_written():
    surface = _Surface(reads=[], wrote=([], []))
    _run("CREATE (:Person {name: $name})", {"name": "Alice"}, surface)
    assert surface.write_calls == [
        ('INSERT INTO "conformance"."person" ("name") VALUES ($1)', ["Alice"])
    ]


# --- REQ-665 (amended 2026-10-28): a junction CREATE ending on rows the same statement creates ---


class _Keys(_Surface):
    """A surface whose writes answer in turn: each new row's generated key, then the junction's."""

    def __init__(self, reads: list[tuple[list[str], list[list]]], wrote: list[tuple[list, list]]):
        super().__init__(reads, ([], []))
        self.answers = list(wrote)

    async def write(self, sql: str, bound: list):
        self.write_calls.append((sql, bound))
        return (*self.answers.pop(0), None)


_NEW_PERSON = 'INSERT INTO "conformance"."person" ("name") VALUES ($1) RETURNING "id" AS "_id"'


def _new_row(key: int) -> tuple[list[str], list[list]]:
    return ["_id", "provisa_write_id"], [[key, key]]


def test_the_driver_introduction_example_writes_both_rows_then_the_junction_row():
    surface = _Keys(
        reads=[(["p1", "p2", "k"], [["n1", "n2", "r1"]])],
        wrote=[_new_row(10), _new_row(11), ([], [])],
    )
    result = _run(
        "CREATE (p1:Person {name:$a}) CREATE (p2:Person {name:$b}) "
        "CREATE (p1)-[k:KNOWS {since:$c}]->(p2) RETURN p1, p2, k",
        {"a": "Alice", "b": "Bob", "c": "2020"},
        surface,
    )
    assert surface.write_calls == [
        (f'{_NEW_PERSON}, "id" AS "provisa_write_id"', ["Alice"]),
        (f'{_NEW_PERSON}, "id" AS "provisa_write_id"', ["Bob"]),
        (
            'INSERT INTO "conformance"."knows" ("from_id", "to_id", "since") VALUES (10, 11, $1)',
            ["2020"],
        ),
    ]
    # The RETURN is read as every read is: the rows written, matched by the keys they were given.
    assert surface.read_calls == [
        (
            "MATCH (p1:Person {_id: $provisa_write_key0})-[k:KNOWS]->"
            "(p2:Person {_id: $provisa_write_key1}) RETURN p1, p2, k",
            {
                "a": "Alice",
                "b": "Bob",
                "c": "2020",
                "provisa_write_key0": 10,
                "provisa_write_key1": 11,
            },
        )
    ]
    assert result == (["p1", "p2", "k"], [["n1", "n2", "r1"]])


def test_a_generated_key_is_the_junction_rows_key_and_the_rows_identity():
    # The junction takes the key column it joins on, returned apart from the node's _id: here a
    # compound-free key whose generated value the CREATE never names.
    surface = _Keys(
        reads=[(["id(a)"], [[77]])],
        wrote=[(["_id", "provisa_write_id"], [[501, 501]]), _new_row(502), ([], [])],
    )
    _run("CREATE (a:Person {name: 'x'})-[:KNOWS]->(b:Person {name: 'y'}) RETURN id(a)", {}, surface)
    assert surface.write_calls[2] == (
        'INSERT INTO "conformance"."knows" ("from_id", "to_id") VALUES (501, 502)',
        [],
    )
    assert surface.read_calls[0][1]["provisa_write_key0"] == 501


def test_an_inbound_relationship_between_new_rows_starts_at_its_far_row():
    surface = _Keys(reads=[], wrote=[_new_row(9), _new_row(3), ([], [])])
    result = _run(
        "CREATE (c:Company {name: 'Acme'}), (p:Person {name: 'Alice'}), (c)<-[:WORKS_FOR]-(p)",
        {},
        surface,
    )
    assert surface.write_calls[2] == (
        'INSERT INTO "conformance"."works_for" ("from_id", "to_id") VALUES (3, 9)',
        [],
    )
    # No RETURN: nothing is read.
    assert result == ([], [])
    assert surface.read_calls == []


@pytest.mark.parametrize(
    ("returns", "read"),
    [
        (
            "RETURN p1.name AS first, k.since ORDER BY first LIMIT 1",
            "MATCH (p1:Person {_id: $provisa_write_key0})-[k:KNOWS]->"
            "(p2:Person {_id: $provisa_write_key1}) RETURN p1.name AS first, k.since "
            "ORDER BY first LIMIT 1",
        ),
        (
            # A relationship the RETURN cannot name is not read; each named row is.
            "RETURN p2",
            "MATCH (p1:Person {_id: $provisa_write_key0}), (p2:Person {_id: $provisa_write_key1}) "
            "RETURN p2",
        ),
    ],
)
def test_the_return_after_a_graph_create_is_answered_as_written(returns, read):
    rel = "[k:KNOWS]" if "k." in returns else "[:KNOWS]"
    surface = _Keys(reads=[(["x"], [[1]])], wrote=[_new_row(1), _new_row(2), ([], [])])
    _run(
        f"CREATE (p1:Person {{name: 'a'}}) CREATE (p2:Person {{name: 'b'}}) "
        f"CREATE (p1)-{rel}->(p2) {returns}",
        {},
        surface,
    )
    assert [q for q, _ in surface.read_calls] == [read]


def test_a_return_naming_no_row_reads_the_rows_written():
    surface = _Keys(reads=[(["1"], [[1]])], wrote=[_new_row(1), _new_row(2), ([], [])])
    _run("CREATE (:Person {name: 'a'})-[:KNOWS]->(:Person {name: 'b'}) RETURN 1", {}, surface)
    assert surface.read_calls[0][0] == (
        "MATCH (provisa_write_n0:Person {_id: $provisa_write_key0}), "
        "(provisa_write_n1:Person {_id: $provisa_write_key1}) RETURN 1"
    )


@pytest.mark.parametrize(
    ("query", "refusal"),
    [
        (
            "CREATE (c:Company {name: 'x'}) CREATE (p:Person {name: 'y'}) CREATE (c)-[:KNOWS]->(p)",
            "KNOWS is not registered from Company to Person",
        ),
        (
            "CREATE (r:Robot {name: 'x'}) CREATE (p:Person {name: 'y'}) CREATE (r)-[:KNOWS]->(p)",
            "Label 'Robot' is not registered",
        ),
        (
            "CREATE (a:Company {name: 'x'}), (b:Person {name: 'y'}), (a)-[:BASED_IN]->(b)",
            "derived from a foreign key",
        ),
        (
            "CREATE (a:Person {name: 'x'}), (b:Person {name: 'y'}), (a)-[:LIKES]->(b)",
            "LIKES is not registered",
        ),
        (
            "CREATE (a:Person {name: 'x'}), (b:Person {name: 'y'}), (a)-[:KNOWS {weight: 1}]->(b)",
            "KNOWS has no property weight",
        ),
    ],
)
def test_an_end_that_is_no_governed_row_of_the_junctions_end_table_is_refused_before_any_write(
    query, refusal
):
    surface = _Keys(reads=[], wrote=[])
    with pytest.raises(CypherWriteParseError, match=refusal):
        _run(query, {}, surface)
    assert surface.write_calls == []


@pytest.mark.parametrize(
    ("query", "refusal"),
    [
        # b is neither matched nor created.
        ("CREATE (a:Person {name: 'x'}) CREATE (a)-[:KNOWS]->(b)", "'b' is neither"),
        ("CREATE (a:Person {name: 'x'}) CREATE (a:Person {name: 'y'})", "creates 'a' once"),
        ("CREATE (a:Person {name: 'x'}), (b:Person), (a)-[:KNOWS]->(b)", r"\(b:Person\) is not"),
        ("CREATE (a:Person {name: 'x'}), (b:Person {name: 'y'}), (a)-[:KNOWS]-(b)", "direction"),
    ],
)
def test_a_graph_create_outside_its_form_is_refused_by_name(query, refusal):
    with pytest.raises(CypherWriteParseError, match=refusal):
        parse_cypher_write(query)


def test_a_relationship_pattern_in_no_write_form_is_refused_in_the_current_rule():
    with pytest.raises(CypherWriteParseError, match="row of the junction table registered"):
        parse_cypher_write("MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.id = 1 SET r.since = 1")
