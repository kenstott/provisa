# Copyright (c) 2026 Kenneth Stott
# Canary: 6c1f8a3e-2b94-4d07-9e5a-d18b7c4f2a65
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Governed MERGE of model nodes and registered junction relationships (REQ-2176, amended
2026-10-28): a MERGE matches on the properties it states (a node on its label and property map, a
relationship on its two ends and its type) and creates only when nothing matches, each part
through the governed write path; ON CREATE SET and ON MATCH SET apply to the part they name. A
foreign-key relationship is refused by name; a session MERGE is unchanged.

The model sits in one DuckDB (tests/unit/ephemeral_harness.py). A statement runs as Bolt runs it:
the session's seam first, then, when the seam hands it on, the governed write path (written)."""

# Requirements: REQ-2176, REQ-665

from __future__ import annotations

import asyncio

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.compiler.write_admission import WriteNotAdmitted
from provisa.cypher.write_translator import CypherWriteParseError, parse_cypher_write
from provisa.ephemeral import seam, store
from provisa.gql.label_map import JunctionMapping, RelationshipMapping
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_PERSON = {"id": "integer", "name": "text", "age": "integer"}
_COMPANY = {"id": "integer", "name": "text"}
_MODEL = {
    "Person": (_PERSON, "(1, 'Ann', 30), (2, 'Bo', 40), (3, 'Cy', 50)"),
    "Company": (_COMPANY, "(10, 'Acme')"),
    "Greeting": ({"id": "integer", "message": "text"}, "(20, 'hello')"),
}

#: The Neo4j Python driver README's add_friend.
ADD_FRIEND = (
    "MERGE (a:Person {name: $name}) MERGE (friend:Person {name: $friend}) "
    "MERGE (a)-[:KNOWS]->(friend)"
)
#: The README's read-back of it.
PRINT_FRIENDS = (
    "MATCH (a:Person)-[:KNOWS]->(friend) WHERE a.name = $name "
    "RETURN friend.name ORDER BY friend.name"
)


def _junction(
    rel_type: str, source: str, target: str, table: str, attributes: dict[str, str]
) -> RelationshipMapping:
    return RelationshipMapping(
        rel_type=rel_type,
        source_label=source,
        target_label=target,
        join_source_column="id",
        join_target_column="id",
        field_name=table,
        via=JunctionMapping(
            catalog_name="memory",
            schema_name="s",
            table_name=table,
            type_name=table,
            source_columns=("from_id",),
            target_columns=("to_id",),
            attributes=attributes,
        ),
    )


def _session(monkeypatch) -> Session:
    relationships = [
        _junction("KNOWS", "Person", "Person", "knows", {"since": "since"}),
        _junction("WORKS_FOR", "Person", "Company", "works_for", {}),
        RelationshipMapping(
            rel_type="GREETS",
            source_label="Greeting",
            target_label="Company",
            join_source_column="id",
            join_target_column="id",
            field_name="greets",
        ),
    ]
    s = Session(monkeypatch, _MODEL, relationships=relationships)
    # A person's key is generated, as the Bolt job's serial key is.
    s.duck.execute("CREATE SEQUENCE person_ids START 100")
    s.duck.execute("ALTER TABLE s.person ALTER COLUMN id SET DEFAULT nextval('person_ids')")
    s.duck.execute("CREATE TABLE s.knows (from_id integer, to_id integer, since integer)")
    s.duck.execute("INSERT INTO s.knows VALUES (1, 2, 2020)")
    s.duck.execute("CREATE TABLE s.works_for (from_id integer, to_id integer)")
    return s


def _rows(s: Session, table: str) -> list[tuple]:
    return s.duck.execute(f"SELECT * FROM s.{table} ORDER BY ALL").fetchall()


def _run(s: Session, statement: str, **params) -> None:
    """As Bolt runs a write: the seam first, then the governed write path."""
    made = asyncio.run(seam.write(statement, params, s.model, s.session, s.state, s.read, s.write))
    if made is None:
        asyncio.run(s.write(statement, params))


def test_a_node_merge_that_matches_writes_nothing(monkeypatch):
    s = _session(monkeypatch)
    _run(s, "MERGE (a:Person {name: 'Ann'}) MERGE (b:Person {name: 'Bo'})")
    assert s.sent == []
    assert len(_rows(s, "person")) == 3


def test_a_node_merge_that_matches_nothing_inserts_its_row(monkeypatch):
    s = _session(monkeypatch)
    _run(s, "MERGE (a:Person {name: 'Di'}) MERGE (b:Person {name: 'Ann'})")
    assert _rows(s, "person")[-1] == (100, "Di", None)
    assert len(_rows(s, "person")) == 4
    assert s.sent == [
        'INSERT INTO "memory"."s"."person" ("name") VALUES (\'Di\') '
        'RETURNING "id" AS "_id", "id" AS "id"'
    ]


def test_a_relationship_merge_between_matched_ends(monkeypatch):
    s = _session(monkeypatch)
    statement = "MATCH (a:Person {name: 'Ann'}), (c:Person {name: 'Cy'}) MERGE (a)-[:KNOWS]->(c)"
    _run(s, statement)
    _run(s, statement)
    assert _rows(s, "knows") == [(1, 2, 2020), (1, 3, None)]
    assert s.sent == ['INSERT INTO "memory"."s"."knows" ("from_id", "to_id") VALUES (1, 3)']


def test_a_relationship_merge_it_matches_writes_nothing(monkeypatch):
    s = _session(monkeypatch)
    _run(s, "MATCH (a:Person {name: 'Ann'}), (b:Person {name: 'Bo'}) MERGE (a)-[:KNOWS]->(b)")
    assert s.sent == []
    assert _rows(s, "knows") == [(1, 2, 2020)]


def test_the_readme_add_friend_creates_the_ends_and_the_relationship_and_reads_back(monkeypatch):
    s = _session(monkeypatch)
    for friend in ("Guinevere", "Lancelot", "Merlin"):
        _run(s, ADD_FRIEND, name="Arthur", friend=friend)
    _run(s, ADD_FRIEND, name="Arthur", friend="Merlin")  # matched: nothing more
    people = {name: pid for pid, name, _age in _rows(s, "person")}
    assert sorted(people) == ["Ann", "Arthur", "Bo", "Cy", "Guinevere", "Lancelot", "Merlin"]
    arthur = people["Arthur"]
    assert sorted(t for f, t, _ in _rows(s, "knows") if f == arthur) == sorted(
        people[n] for n in ("Guinevere", "Lancelot", "Merlin")
    )
    rows = s.run(PRINT_FRIENDS, name="Arthur")
    assert [r["friend.name"] for r in rows] == ["Guinevere", "Lancelot", "Merlin"]


def test_a_merge_of_a_whole_pattern_creates_its_ends_and_relationship(monkeypatch):
    s = _session(monkeypatch)
    statement = "MERGE (a:Person {name: 'Ed'})-[:KNOWS {since: 2024}]->(b:Person {name: 'Flo'})"
    _run(s, statement)
    _run(s, statement)
    people = {name: pid for pid, name, _age in _rows(s, "person")}
    assert (people["Ed"], people["Flo"], 2024) in _rows(s, "knows")
    assert len(_rows(s, "person")) == 5 and len(_rows(s, "knows")) == 2


def test_on_create_set_and_on_match_set_apply_to_the_part_they_name(monkeypatch):
    s = _session(monkeypatch)
    merge = "MERGE (p:Person {name: $name}) ON CREATE SET p.age = 1 ON MATCH SET p.age = 2"
    _run(s, merge, name="Ann")
    _run(s, merge, name="Gus")
    ages = {name: age for _pid, name, age in _rows(s, "person")}
    assert ages["Ann"] == 2 and ages["Gus"] == 1 and ages["Bo"] == 40
    rel = (
        "MATCH (a:Person {name: 'Ann'}), (b:Person {name: $to}) "
        "MERGE (a)-[k:KNOWS]->(b) ON CREATE SET k.since = 2000 ON MATCH SET k.since = 1999"
    )
    _run(s, rel, to="Bo")
    _run(s, rel, to="Cy")
    assert _rows(s, "knows") == [(1, 2, 1999), (1, 3, 2000)]


def test_a_foreign_key_relationship_merge_is_refused_by_name(monkeypatch):
    s = _session(monkeypatch)
    with pytest.raises(CypherWriteParseError, match="GREETS is derived from a foreign key"):
        _run(
            s,
            "MERGE (g:Greeting {message: 'hi'}) MERGE (c:Company {name: 'Acme'}) "
            "MERGE (g)-[:GREETS]->(c)",
        )
    assert s.sent == []
    assert _rows(s, "greeting") == [(20, "hello")]


def test_a_merge_of_model_data_reads_through_the_graph_ir():
    ast = parse_cypher_write(ADD_FRIEND)
    assert ast.kind == "merge"
    assert [[n.label for n in part.nodes] for part in ast.merges] == [
        ["Person"],
        ["Person"],
        ["", ""],
    ]
    assert ast.merges[2].links[0].rel_type == "KNOWS"


def test_a_merge_value_that_is_no_literal_or_parameter_is_refused_by_name():
    with pytest.raises(CypherWriteParseError, match="a literal or a parameter"):
        parse_cypher_write("MATCH (a:Person) MERGE (b:Person {name: a.name})")


def _gov(*operations: str) -> GovernanceContext:
    """A role reading the model's tables, holding the write right and a grant per operation."""
    tables = {"person": 1, "knows": 2}
    gov = GovernanceContext()
    gov.role_id = "reader"
    gov.can_write = True
    gov.table_map = {
        name: tid
        for table, tid in tables.items()
        for name in (table, f"s.{table}", f"memory.s.{table}")
    }
    gov.all_columns = {
        1: [("id", "integer"), ("name", "varchar"), ("age", "integer")],
        2: [("from_id", "integer"), ("to_id", "integer"), ("since", "integer")],
    }
    gov.write_grants = {
        tid: {
            op: {
                "role": "reader",
                "operation": op,
                "rows": "TRUE",
                "columns": [c for c, _ in gov.all_columns[tid]],
            }
            for op in operations
        }
        for tid in tables.values()
    }
    gov.write_ops = {tid: frozenset({"insert", "update", "delete"}) for tid in tables.values()}
    gov.write_returns_rows = {tid: True for tid in tables.values()}
    gov.write_refused_forms = {tid: frozenset() for tid in tables.values()}
    return gov


def test_each_write_of_a_merge_is_admitted_as_any_write(monkeypatch):
    s = _session(monkeypatch)
    _run(s, ADD_FRIEND, name="Arthur", friend="Merlin")
    _run(s, "MERGE (p:Person {name: 'Ann'}) ON MATCH SET p.age = 3")
    assert len(s.sent) == 4
    for sql in s.sent:
        apply_governance(sql, _gov("insert", "update"), session_vars={})


def test_a_role_without_write_grants_is_refused(monkeypatch):
    s = _session(monkeypatch)
    _run(s, ADD_FRIEND, name="Arthur", friend="Merlin")
    _run(s, "MERGE (p:Person {name: 'Ann'}) ON MATCH SET p.age = 3")
    for sql in s.sent:
        with pytest.raises(WriteNotAdmitted, match="holds no (insert|update) grant"):
            apply_governance(sql, _gov("delete"), session_vars={})


def test_a_session_merge_is_unchanged(monkeypatch):
    s = _session(monkeypatch)
    s.run("MERGE (n:Temp {k: 1})")
    s.run("MERGE (n:Temp {k: 1})")
    s.run("MERGE (p:Person {name: 'Ann'}) ON MATCH SET p:Vip")
    assert s.sent == []
    assert sorted(r["n.k"] for r in s.run("MATCH (n:Temp) RETURN n.k")) == [1]
    assert [r["n.name"] for r in s.run("MATCH (n:Vip) RETURN n.name")] == ["Ann"]
    assert "Temp" in store.graph(s.session).labels
