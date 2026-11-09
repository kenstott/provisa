# Copyright (c) 2026 Kenneth Stott
# Canary: e08f7c4a-9b91-4561-9df0-9f759384e5b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A traversal over the session's graph starts from its more selective end (REQ-2176, "SESSION
GRAPHS ARE INDEXED AND COUNTED"): the compiler's one entry (cypher_to_sql), which every client's
read passes, reads the session's counts from the overlaid label map and reads the path from the
end fewer nodes hold, written backward, which binds every variable as it was. A read with no
session, or one whose end is a model label, is translated as written."""

# Requirements: REQ-2176

from __future__ import annotations

import pytest
import sqlglot

from provisa.cypher.parser import parse_cypher
from provisa.ephemeral import seam
from provisa.gql.traversal_order import reordered
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_PEOPLE = {"Person": ({"id": "integer", "name": "text"}, "(1, 'Ann'), (2, 'Bo')")}


def _values(rows) -> list[tuple]:
    return sorted((tuple(r.values()) for r in rows), key=repr)


def _first_table(sql: str) -> str:
    tree = sqlglot.parse_one(sql, read="postgres")
    return tree.args["from_"].this.name


@pytest.fixture
def session(monkeypatch) -> Session:
    s = Session(monkeypatch, _PEOPLE)
    s.run("UNWIND range(1, 40) AS i CREATE (:Big {k: i})")
    s.run("CREATE (:Small {k: 1}), (:Small {k: 2})")
    s.run("MATCH (b:Big), (s:Small) WHERE b.k = s.k CREATE (b)-[:R]->(s)")
    return s


def test_a_path_starts_from_its_more_selective_end(session):
    query = "MATCH (a:Big)-[:R]->(b:Small) RETURN a.k, b.k"
    assert _first_table(session.sql(query)).startswith("__session_node__Small")
    assert _values(session.run(query)) == [(1, 1), (2, 2)]


def test_a_path_already_starting_there_is_as_written(session):
    query = "MATCH (b:Small)<-[:R]-(a:Big) RETURN a.k, b.k"
    assert _first_table(session.sql(query)).startswith("__session_node__Small")
    assert _values(session.run(query)) == [(1, 1), (2, 2)]


def test_a_path_ending_on_a_model_label_is_as_written(session):
    lm = seam.label_map(session.model, session.session)
    ast = parse_cypher("MATCH (a:Big)-[:R]->(p:Person) RETURN a.k", lm)
    assert reordered(ast, lm) is ast


def test_with_no_session_a_read_is_as_written(session):
    ast = parse_cypher("MATCH (p:Person) RETURN p.name", session.model)
    assert reordered(ast, session.model) is ast


def test_an_optional_or_bound_path_is_as_written(session):
    lm = seam.label_map(session.model, session.session)
    for query in (
        "MATCH (x:Small) OPTIONAL MATCH (a:Big)-[:R]->(b:Small) RETURN a.k",
        "MATCH p = (a:Big)-[:R]->(b:Small) RETURN p",
    ):
        ast = parse_cypher(query, lm)
        assert reordered(ast, lm) is ast, query
