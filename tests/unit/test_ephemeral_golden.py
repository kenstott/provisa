# Copyright (c) 2026 Kenneth Stott
# Canary: cb7caa07-16a8-4dd7-bd61-7ef83c06bcf5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The governed path is unchanged by the ephemeral layer (REQ-2176): every Cypher statement of
the golden corpus (tests/fixtures/cypher_golden), compiled through the seam a surface uses,
gives exactly the SQL it gives without the layer -- with no session, with a session holding
nothing, and with a session whose ephemeral labels the statement does not name. Only a statement
that reads every label (a node with no label, a relationship with no type) reads the session's
too, and then the SQL differs by the session's tables alone; and a refusal of a type no one has
names the session's types among the near ones."""

# Requirements: REQ-2176

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from provisa.compiler.temp_tables import TempSession, TempTable
from provisa.ephemeral import seam, store

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "scripts"))

import cypher_golden  # noqa: E402

from tests.unit.test_cypher_golden_sql import _ENTRIES  # noqa: E402


def _holding_unnamed_labels() -> TempSession:
    """A session holding an ephemeral node and a relationship no corpus statement names."""
    session = TempSession()
    graph = store.graph(session)
    anchor = store.anchor_table("ZzScratch")
    graph.labels.add("ZzScratch")
    graph.anchors["ZzScratch"] = anchor
    graph.label_anchors["ZzScratch"] = {anchor}
    graph.types.add("ZZ_LINK")
    graph.rel_pairs.add(("ZZ_LINK", "ZzScratch", "ZzScratch"))
    session.tables[anchor] = TempTable(anchor, [("node_id", "text"), ("k", "bigint")])
    rels = store.rel_table("ZZ_LINK")
    session.tables[rels] = TempTable(rels, [("rel_id", "text")])
    return session


_SESSIONS = {
    "no session": lambda: None,
    "an empty session": TempSession,
    "a session of unnamed labels": _holding_unnamed_labels,
}


def _through_the_seam(entry: dict, session: TempSession | None) -> dict:
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.translator import cypher_to_sql

    model = cypher_golden.load_blob(entry["label_map"])
    params = cypher_golden.load_blob(entry["params"])
    try:
        ast = parse_cypher(entry["cypher"])
        if entry["call"] is not None:
            ast = ast.call_subqueries[entry["call"]].body
        compiled_over = seam.label_map(model, session)
        sql, ordered, graph_vars = cypher_to_sql(ast, compiled_over, params)
        return cypher_golden.rendered(
            (seam.expand(sql, compiled_over, model, session), ordered, graph_vars)
        )
    except Exception as exc:
        return cypher_golden.refused(exc)


def _reads_every_label(entry: dict) -> bool:
    """Whether the statement has a node with no label or a relationship with no type."""
    from provisa.cypher.parser import parse_cypher
    from provisa.gql.ir import PathPattern

    def walk(ast) -> bool:
        for m in ast.match_clauses:
            if isinstance(m.pattern, PathPattern) and (
                any(not n.labels for n in m.pattern.nodes)
                or any(not r.types for r in m.pattern.rels)
            ):
                return True
        parts = [cs.body for cs in ast.call_subqueries] + [p for p, _ in ast.union_parts]
        return any(walk(part) for part in parts)

    try:
        return walk(parse_cypher(entry["cypher"]))
    except Exception:  # allow-ble: a refused statement reads nothing
        return False


def _suggests_the_sessions_type(expected: dict, got: dict) -> bool:
    return (
        "error" in expected
        and got.get("error") == expected["error"]
        and ":ZZ_LINK" in got.get("message", "")
    )


@pytest.mark.parametrize("held", list(_SESSIONS))
def test_every_statement_compiles_through_the_seam_as_it_does_without_it(held):
    differs = []
    for entry in _ENTRIES:
        got = _through_the_seam(entry, _SESSIONS[held]())
        if got == entry["expected"]:
            continue
        sql = str(got.get("sql", ""))
        if held == "a session of unnamed labels":
            if _reads_every_label(entry) and "cypher__" in sql:
                continue  # it reads every label, the session's among them
            if _suggests_the_sessions_type(entry["expected"], got):
                continue  # an unknown type's refusal names the session's types as near ones
        differs.append(entry["origin"])
    assert differs == []
