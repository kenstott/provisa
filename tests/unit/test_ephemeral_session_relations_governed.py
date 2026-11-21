# Copyright (c) 2026 Kenneth Stott
# Canary: 3c1f6a0e-7b52-4d8e-9a41-5e2f0c8d7b19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A graph session's own tables pass governance as the session's (REQ-2176, REQ-615, REQ-039).

The read of a session label names the session's tables -- its anchor ``cypher__anchor__<Label>``,
``cypher__node_labels``, ``cypher__rels``, a type's ``cypher__rel__<type>`` -- by the exact names
the session holds them under. Governance judges a statement as written and as PostgreSQL folds it
(unquoted in lower case), so a session table named unquoted with an upper-case letter in it
(``cypher__anchor__Person``) was folded to a name the session does not hold and refused (V006),
and an engine that folds case would have read another table. Each is named quoted: both readings
name the table the session holds. A relation neither registered nor the session's is still
refused, however it is spelt."""

# Requirements: REQ-2176, REQ-615, REQ-039

from __future__ import annotations

import pytest
import sqlglot
import sqlglot.expressions as exp

from provisa.compiler import temp_tables
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_validator import READ, validate_sql
from provisa.compiler.stage2 import GovernanceContext
from provisa.compiler.temp_tables import TempSession, TempTable
from provisa.ephemeral.store import anchor_table
from tests.unit.ephemeral_harness import Session

pytestmark = pytest.mark.unit

_ROLE = {"id": "analyst", "capabilities": [], "domain_access": ["*"]}
_ORDERS = {"Orders": ({"id": "integer", "email": "text"}, "(1, 'a@x'), (2, 'b@x')")}


def _gov() -> GovernanceContext:
    """The model's Orders, registered as the harness holds it (schema ``s``)."""
    return GovernanceContext(
        rls_rules={},
        masking_rules={},
        visible_columns={1: frozenset({"id", "email"})},
        table_map={"s.orders": 1, "memory.s.orders": 1},
        all_columns={1: [("id", "integer"), ("email", "text")]},
        limit_ceiling=None,
        sample_size=None,
    )


def _refusals(sql: str) -> list[tuple[str, str]]:
    return [
        (v.code, v.message)
        for v in validate_sql(sql, CompilationContext(), _gov(), _ROLE, [])
        if v.code == "V006"
    ]


# -- the session's own tables, as the read names them --------------------------------------------


@pytest.mark.parametrize(
    ("writes", "reads"),
    [
        # test_cypher_endpoint: CREATE then MATCH of a label the model does not have
        ("CREATE (n:Person {name: 'Eve'})", "MATCH (n:Person) RETURN n.name AS name"),
        # test_bolt_ephemeral_session: the three refused reads
        (
            "CREATE (:Note {text: 'x'}), (:Note {text: 'y'})",
            "MATCH (n:Note) RETURN n.text AS text ORDER BY text",
        ),
        (
            "MATCH (o:Orders) CREATE (:Seen {id: o.id, email: o.email})",
            "MATCH (s:Seen) RETURN s.id AS id, s.email AS email ORDER BY id",
        ),
        (
            "MATCH (o:Orders) WHERE o.id = 1 CREATE (o)-[:FLAGGED]->(:Flag {why: 'late'})",
            "MATCH (o:Orders)-[:FLAGGED]->(f:Flag) RETURN o.id AS id, o.email AS email, f.why AS why",
        ),
    ],
)
def test_a_session_read_passes_governance(monkeypatch, writes, reads):
    s = Session(monkeypatch, _ORDERS)
    s.run(writes)
    sql = s.sql(reads)
    token = temp_tables.bind(s.session)
    try:
        assert _refusals(sql) == [], sql
    finally:
        temp_tables.unbind(token)


def test_each_session_table_is_named_as_the_session_holds_it(monkeypatch):
    """Quoted, so PostgreSQL's folding -- the engine's and governance's -- keeps its case."""
    s = Session(monkeypatch, _ORDERS)
    s.run("MATCH (o:Orders) WHERE o.id = 1 CREATE (o)-[:FLAGGED]->(:Flag {why: 'late'})")
    tree = sqlglot.parse_one(
        s.sql("MATCH (o:Orders)-[:FLAGGED]->(f:Flag) RETURN f.why AS why"), read=READ
    )
    named = [t for t in tree.find_all(exp.Table) if t.name in s.session.tables]
    assert {t.name for t in named} >= {anchor_table("Flag"), "cypher__rels"}
    assert all(isinstance(t.this, exp.Identifier) and t.this.quoted for t in named), [
        t.sql(dialect="postgres") for t in named
    ]


# -- a relation the client names: still refused --------------------------------------------------


def _session_holding(*names: str) -> TempSession:
    session = TempSession()
    for name in names:
        session.tables[name] = TempTable(name, [("node_id", "text")])
    return session


@pytest.mark.parametrize(
    "sql",
    [
        'SELECT "_a"."node_id" FROM "cypher__anchor__Person" AS "_a"',
        "SELECT node_id FROM cypher__anchor__person",
        "SELECT node_id FROM cypher__nodes",
        'SELECT 1 FROM "cypher__rels"',
        "SELECT 1 FROM pg_temp.cypher__anchor__Person",
    ],
)
def test_a_session_table_no_session_holds_is_refused(sql):
    assert _refusals(sql), sql


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT secret FROM unregistered_table",
        'SELECT * FROM "cypher__anchor__Other"',
        "SELECT * FROM meta.catalog_pg_class",
    ],
)
def test_a_relation_neither_registered_nor_the_sessions_is_refused(sql):
    token = temp_tables.bind(_session_holding("cypher__anchor__Person", "cypher__nodes"))
    try:
        assert _refusals(sql), sql
    finally:
        temp_tables.unbind(token)


def test_a_session_table_named_quoted_is_the_sessions():
    token = temp_tables.bind(_session_holding("cypher__anchor__Person"))
    try:
        assert _refusals('SELECT "_a"."node_id" FROM "cypher__anchor__Person" AS "_a"') == []
    finally:
        temp_tables.unbind(token)
