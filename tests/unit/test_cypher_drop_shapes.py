# Copyright (c) 2026 Kenneth Stott
# Canary: 2d7e4b91-8a35-4c06-b1f2-6e9c0a3d7f58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
# ruff: noqa: F811 -- a fixture imported here is redefined by each test that takes it

"""What a drop onto a Cypher result with unlabelled nodes writes, and what Show children asks
(REQ-2135, provisa-ui's graph-drop.ts and cypherEndpoints.ts), runs: the endpoints labelled from the
schema, one MATCH per label pair, UNION ALL, the dropped label joined with OPTIONAL MATCH. Each query
is the one provisa-ui/src/__tests__/GraphDropUnlabelled.test.ts asserts the UI writes."""

# Requirements: REQ-2135

from provisa.cypher.parser import parse_cypher
from provisa.gql.assembler import Node, assemble_rows, take_cut
from provisa.gql.graph_rewriter import apply_graph_rewrites
from provisa.gql.translator import cypher_to_sql
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.unit.test_sparql_compiler import (  # noqa: F401 -- the fixtures
    _model,
    con,
    public_url,
)

#: A drop of Company onto MATCH ()-[r]->() RETURN r LIMIT 25.
LABELLED_DROP = "\n".join(
    [
        "MATCH (n1:Person)-[r:KNOWS]->(n2:Person)",
        "OPTIONAL MATCH (n1)-[rCompany:WORKS_AT]-(mCompany:Company)",
        "RETURN r, n1, n2, rCompany, mCompany",
        "LIMIT 25",
        "UNION ALL",
        "MATCH (n1:Person)-[r:WORKS_AT]->(n2:Company)",
        "OPTIONAL MATCH (n1)-[rCompany:WORKS_AT]-(mCompany:Company)",
        "RETURN r, n1, n2, rCompany, mCompany",
        "LIMIT 25",
        "UNION ALL",
        "MATCH (n1:Company)-[r:PARENT]->(n2:Company)",
        "OPTIONAL MATCH (n1)-[rCompany:PARENT]->(mCompany:Company)",
        "RETURN r, n1, n2, rCompany, mCompany",
        "LIMIT 25",
    ]
)

#: The shape of a branch no path within the selected domains joins: its columns are nulls.
PADDED_DROP = "\n".join(
    [
        "MATCH (n1:Person)-[r:KNOWS]->(n2:Person)",
        "OPTIONAL MATCH (n1)-[rCompany:WORKS_AT]-(mCompany:Company)",
        "RETURN r, n1, n2, rCompany, mCompany",
        "LIMIT 25",
        "UNION ALL",
        "MATCH (n1:Company)-[r:PARENT]->(n2:Company)",
        "RETURN r, n1, n2, null AS rCompany, null AS mCompany",
        "LIMIT 25",
    ]
)

#: Show children of Ann (Person 1).
CHILDREN = "\n".join(
    [
        "MATCH (n:Person)-[r:KNOWS]->(child:Person)",
        "WHERE id(n) IN [1]",
        "RETURN n, r, child",
        "UNION ALL",
        "MATCH (n:Person)-[r:WORKS_AT]->(child:Company)",
        "WHERE id(n) IN [1]",
        "RETURN n, r, child",
    ]
)


def _rows(con, query: str) -> list[dict]:
    lm = _model()
    sql, _, graph_vars = cypher_to_sql(parse_cypher(query), lm, {})
    sql = apply_graph_rewrites(sql, graph_vars, lm)
    cur = con.execute(transpile_to_duckdb(sql.sql(dialect="postgres"), generated=True))
    names = [d[0] for d in cur.description]
    return assemble_rows(take_cut([dict(zip(names, r)) for r in cur.fetchall()], []), graph_vars)


def _name(node: Node | None) -> str | None:
    return None if node is None else node.properties["name"]


def test_a_drop_onto_labelled_endpoints_joins_every_branch(con):
    """Ann knows Bo, Bo knows Cy; Ann and Bo work at Acme, Cy at Beta; Acme's parent is Beta,
    Beta's is Corp. Each branch's endpoints, and the company its path reaches from n1: a person's
    employer, a company's parent (PARENT is walked along its direction)."""
    rows = _rows(con, LABELLED_DROP)
    assert sorted((_name(r["n1"]), _name(r["n2"]), _name(r["mCompany"]) or "") for r in rows) == [
        ("Acme", "Beta", "Beta"),
        ("Ann", "Acme", "Acme"),
        ("Ann", "Bo", "Acme"),
        ("Beta", "Corp", "Corp"),
        ("Bo", "Acme", "Acme"),
        ("Bo", "Cy", "Acme"),
        ("Cy", "Beta", "Beta"),
    ]


def test_a_branch_no_path_joins_returns_nulls(con):
    rows = _rows(con, PADDED_DROP)
    parents = [r for r in rows if r["rCompany"] is None and r["mCompany"] is None]
    assert {(_name(r["n1"]), _name(r["n2"])) for r in parents} >= {
        ("Acme", "Beta"),
        ("Beta", "Corp"),
    }


def test_show_children_reads_every_label_pair(con):
    rows = _rows(con, CHILDREN)
    assert sorted(_name(r["child"]) or "" for r in rows) == ["Acme", "Bo"]
