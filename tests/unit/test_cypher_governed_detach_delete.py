# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6f4e2a-9c31-4d7e-a8b5-3f2d1c6e7a90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Governed DELETE of a junction row and governed DETACH DELETE of a model node (REQ-2176,
REQ-665): DETACH DELETE is governed when every part has an exact target, the node's row and each
junction row it is an end of; a relationship a foreign key derives cannot be detached and is
refused by name before anything is written. Each DELETE the statement makes is admitted as any
write: a role holding no delete grant is refused.

The model sits in one DuckDB (tests/unit/ephemeral_harness.py) and each statement runs through
the governed write path a surface runs (written): its read, then its DELETEs."""

# Requirements: REQ-2176, REQ-665

from __future__ import annotations

import asyncio

import pytest

from provisa.compiler.stage2 import GovernanceContext, apply_governance
from provisa.compiler.write_admission import WriteNotAdmitted
from provisa.cypher.parser import CypherParseError
from provisa.cypher.write_translator import CypherWriteParseError, parse_cypher_write
from provisa.gql.label_map import JunctionMapping, RelationshipMapping
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_COLUMNS = {"id": "integer", "name": "text"}
_MODEL = {
    "Person": (_COLUMNS, "(1, 'Ann'), (2, 'Bo'), (3, 'Cy')"),
    "Company": (_COLUMNS, "(10, 'Acme'), (11, 'Initech')"),
    "Greeting": ({"id": "integer", "message": "text"}, "(20, 'hello')"),
}


def _junction(rel_type: str, source: str, target: str, table: str) -> RelationshipMapping:
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
        ),
    )


def _session(monkeypatch, *, foreign_key: bool = False) -> Session:
    relationships = [
        _junction("KNOWS", "Person", "Person", "knows"),
        _junction("WORKS_FOR", "Person", "Company", "works_for"),
    ]
    if foreign_key:
        relationships.append(
            RelationshipMapping(
                rel_type="GREETS",
                source_label="Greeting",
                target_label="Company",
                join_source_column="id",
                join_target_column="id",
                field_name="greets",
            )
        )
    s = Session(monkeypatch, _MODEL, relationships=relationships)
    s.duck.execute("CREATE TABLE s.knows (from_id integer, to_id integer)")
    s.duck.execute("INSERT INTO s.knows VALUES (1, 2), (2, 3), (3, 1)")
    s.duck.execute("CREATE TABLE s.works_for (from_id integer, to_id integer)")
    s.duck.execute("INSERT INTO s.works_for VALUES (1, 10), (2, 11)")
    return s


def _rows(s: Session, table: str) -> list[tuple]:
    return s.duck.execute(f"SELECT * FROM s.{table} ORDER BY ALL").fetchall()


def _write(s: Session, statement: str, **params) -> None:
    asyncio.run(s.write(statement, params))


def test_detach_delete_removes_the_junction_rows_then_the_row(monkeypatch):
    s = _session(monkeypatch)
    _write(s, "MATCH (p:Person {name: 'Bo'}) DETACH DELETE p")
    assert _rows(s, "person") == [(1, "Ann"), (3, "Cy")]
    # Bo's KNOWS rows, as either end, and Bo's WORKS_FOR row are gone; the others stay.
    assert _rows(s, "knows") == [(3, 1)]
    assert _rows(s, "works_for") == [(1, 10)]
    assert _rows(s, "company") == [(10, "Acme"), (11, "Initech")]
    assert s.sent == [
        'DELETE FROM "memory"."s"."knows" WHERE ("from_id" = 2) OR ("to_id" = 2)',
        'DELETE FROM "memory"."s"."works_for" WHERE ("from_id" = 2)',
        'DELETE FROM "memory"."s"."person" WHERE ("id" = 2)',
    ]


def test_detach_delete_of_a_node_with_no_relationships_deletes_its_row(monkeypatch):
    s = _session(monkeypatch)
    _write(s, "MATCH (g:Greeting) DETACH DELETE g")
    assert _rows(s, "greeting") == []
    assert s.sent == ['DELETE FROM "memory"."s"."greeting" WHERE ("id" = 20)']


def test_detach_delete_by_a_where_and_a_parameter(monkeypatch):
    s = _session(monkeypatch)
    _write(s, "MATCH (p:Person) WHERE p.name = $name DETACH DELETE p", name="Ann")
    assert _rows(s, "person") == [(2, "Bo"), (3, "Cy")]
    assert _rows(s, "knows") == [(2, 3)]
    assert _rows(s, "works_for") == [(2, 11)]


def test_a_node_a_foreign_key_relates_is_refused_by_name_and_nothing_is_written(monkeypatch):
    s = _session(monkeypatch, foreign_key=True)
    with pytest.raises(
        CypherWriteParseError, match="(Company|Greeting) is related by GREETS.*foreign key"
    ):
        _write(s, "MATCH (n) DETACH DELETE n")
    assert s.sent == []
    assert len(_rows(s, "person")) == 3 and len(_rows(s, "company")) == 2
    assert _rows(s, "greeting") == [(20, "hello")]


def test_a_relationship_delete_is_its_junction_row(monkeypatch):
    s = _session(monkeypatch)
    _write(s, "MATCH (a:Person {name: 'Ann'})-[r:KNOWS]->(b:Person) DELETE r")
    assert _rows(s, "knows") == [(2, 3), (3, 1)]
    assert len(_rows(s, "person")) == 3
    assert s.sent == ['DELETE FROM "memory"."s"."knows" WHERE ("from_id" = 1 AND "to_id" = 2)']


def test_a_relationship_read_from_its_far_end_deletes_the_same_row(monkeypatch):
    s = _session(monkeypatch)
    _write(s, "MATCH (c:Company {name: 'Acme'})<-[w:WORKS_FOR]-(p:Person) DELETE w")
    assert _rows(s, "works_for") == [(2, 11)]


def test_a_foreign_key_relationship_delete_is_refused_by_name(monkeypatch):
    s = _session(monkeypatch, foreign_key=True)
    s.duck.execute("UPDATE s.greeting SET id = 10")
    with pytest.raises(CypherWriteParseError, match="GREETS is derived from a foreign key"):
        _write(s, "MATCH (g:Greeting)-[r:GREETS]->(c:Company) DELETE r")
    assert s.sent == []


def test_match_every_node_detach_delete_empties_every_label(monkeypatch):
    # The Neo4j driver README's cleanup, MATCH (a) DETACH DELETE a.
    s = _session(monkeypatch)
    _write(s, "MATCH (a) DETACH DELETE a")
    for table in ("person", "company", "greeting", "knows", "works_for"):
        assert _rows(s, table) == [], table


def test_detach_delete_of_nothing_matched_writes_nothing(monkeypatch):
    s = _session(monkeypatch)
    _write(s, "MATCH (p:Person {name: 'Nobody'}) DETACH DELETE p")
    assert s.sent == []
    assert len(_rows(s, "person")) == 3


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (a:Person) DETACH DELETE a",
        "MATCH (a:Person)-[r:KNOWS]->(b) DELETE r",
        "MATCH (a:Person), (c:Company) DELETE a, c",
    ],
)
def test_each_delete_outside_the_one_node_form_is_read_through_the_graph_ir(query):
    ast = parse_cypher_write(query)
    assert ast.kind == "erase"


def test_the_one_node_delete_keeps_its_form():
    assert parse_cypher_write("MATCH (n:Person {name: 'Ann'}) DELETE n").kind == "delete"


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (a:Person) DETACH DELETE a RETURN a",
        "MATCH (a:Person) DETACH DELETE a.name",
    ],
)
def test_a_delete_outside_its_form_is_refused_by_name(query):
    with pytest.raises(CypherWriteParseError):
        parse_cypher_write(query)


def test_remove_is_refused_in_the_current_rule():
    with pytest.raises(CypherWriteParseError, match="Cypher write pattern 'REMOVE'") as err:
        parse_cypher_write("MATCH (a:Person) REMOVE a.name")
    assert "one exact target" in str(err.value)
    assert "REQ-" not in str(err.value)


def test_a_merge_of_model_data_is_a_governed_write():
    # REQ-2176 (amended 2026-10-28): tests/unit/test_cypher_governed_merge.py runs it.
    ast = parse_cypher_write("MERGE (a:Person {name: 'Ann'})-[:KNOWS]->(b:Person {name: 'Bo'})")
    assert ast.kind == "merge"


def _gov(*operations: str) -> GovernanceContext:
    """A role reading the model's tables, holding the write right and a grant per operation."""
    tables = {"person": 1, "knows": 2, "works_for": 3, "company": 4}
    gov = GovernanceContext()
    gov.role_id = "reader"
    gov.can_write = True
    gov.table_map = {
        name: tid
        for table, tid in tables.items()
        for name in (table, f"s.{table}", f"memory.s.{table}")
    }
    gov.all_columns = {
        tid: [("from_id", "integer"), ("to_id", "integer")]
        if table in ("knows", "works_for")
        else [("id", "integer"), ("name", "varchar")]
        for table, tid in tables.items()
    }
    grants = {op: {"role": "reader", "operation": op, "rows": "TRUE"} for op in operations}
    gov.write_grants = {tid: dict(grants) for tid in tables.values()}
    gov.write_ops = {tid: frozenset({"insert", "update", "delete"}) for tid in tables.values()}
    gov.write_returns_rows = {tid: True for tid in tables.values()}
    gov.write_refused_forms = {tid: frozenset() for tid in tables.values()}
    return gov


def test_each_delete_of_a_detach_is_admitted_as_any_write(monkeypatch):
    s = _session(monkeypatch)
    _write(s, "MATCH (p:Person {name: 'Bo'}) DETACH DELETE p")
    for sql in s.sent:
        apply_governance(sql, _gov("delete"), session_vars={})


def test_a_role_without_a_delete_grant_is_refused(monkeypatch):
    s = _session(monkeypatch)
    _write(s, "MATCH (p:Person {name: 'Bo'}) DETACH DELETE p")
    for sql in s.sent:
        with pytest.raises(WriteNotAdmitted, match="holds no delete grant"):
            apply_governance(sql, _gov("insert", "update"), session_vars={})


def test_with_session_objects_a_model_node_detach_is_the_governed_detach(monkeypatch):
    # The session holds a node, so the statement meets the ephemeral layer first; the model node
    # is the governed DETACH DELETE, its junction rows detached.
    s = _session(monkeypatch)
    s.run("CREATE (:Scratch {k: 1})")
    s.run("MATCH (p:Person) WHERE p.name = 'Bo' DETACH DELETE p")
    assert _rows(s, "person") == [(1, "Ann"), (3, "Cy")]
    assert _rows(s, "knows") == [(3, 1)]
    assert _rows(s, "works_for") == [(1, 10)]


def test_with_session_objects_a_foreign_key_detach_is_refused_before_any_write(monkeypatch):
    s = _session(monkeypatch, foreign_key=True)
    s.run("CREATE (:Scratch {k: 1})")
    with pytest.raises(CypherParseError, match="Greeting is related by GREETS"):
        s.run("MATCH (n:Person), (g:Greeting) DETACH DELETE n, g")
    assert s.sent == []
    assert len(_rows(s, "person")) == 3
