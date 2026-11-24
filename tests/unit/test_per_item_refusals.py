# Copyright (c) 2026 Kenneth Stott
# Canary: 9b3e6f2a-41c7-4d8e-a5f0-7c2d9e1b8a34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-392: a loop that reads one statement per label or relationship lets no one item's refusal
fail the whole answer, and hides none: the refused item is its own entry, with its reason."""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from types import SimpleNamespace


def _node(label: str) -> SimpleNamespace:
    return SimpleNamespace(
        label=label,
        id_column="id",
        required_parameters=[],
        property_of=lambda col: col,
    )


def _label_map() -> SimpleNamespace:
    """Pet -[OWNS]-> Toy and Pet -[LIKES]-> Toy."""
    nodes = {"Pet": _node("Pet"), "Toy": _node("Toy")}
    rels = {
        "owns": SimpleNamespace(rel_type="OWNS", source_label="Pet", target_label="Toy"),
        "likes": SimpleNamespace(rel_type="LIKES", source_label="Pet", target_label="Toy"),
    }
    return SimpleNamespace(nodes=nodes, relationships=rels)


class _ModelDb:
    """The node-id registry: ids 1 and 2 are Pet|1 and Toy|7."""

    @asynccontextmanager
    async def acquire(self):
        rows = [
            {"id": 1, "label": "Pet", "composite_id": "Pet|1"},
            {"id": 2, "label": "Toy", "composite_id": "Toy|7"},
        ]

        class _Conn:
            async def execute_core(self, _stmt):
                return SimpleNamespace(fetchall=lambda: [SimpleNamespace(_mapping=r) for r in rows])

        yield _Conn()


REFUSAL = "Relation 'pet_store.likes_1' is not a registered table"


async def test_graph_counts_says_a_label_any_refusal_refuses_and_counts_the_others(monkeypatch):
    """A label whose read the operator's floor refuses (a PermissionError that is not a
    GovernanceRefused) is that label's entry, not a 403 for every label of the page."""
    from provisa.api.app import state
    from provisa.api.rest import cypher_router
    from provisa.compiler import sql_rewrite
    from provisa.core.operator_floor import OperatorFloorError
    from provisa.cypher import parser
    from provisa.executor.result import QueryResult
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(state, "contexts", {"analyst": object()}, raising=False)
    monkeypatch.setattr(state, "roles", {"analyst": {"domain_access": ["*"]}}, raising=False)
    monkeypatch.setattr(cypher_router, "_resolve_role_id", lambda request, st: "analyst")
    monkeypatch.setattr(cypher_router, "_build_label_map", lambda ctx, role, st: None)
    monkeypatch.setattr(
        cypher_router, "_countable_labels", lambda lm, d, r: (["Breed", "Floored"], [])
    )
    monkeypatch.setattr(
        cypher_router,
        "_build_sql_from_ast",
        lambda ast, *a: (
            "SELECT COUNT(*) AS cnt FROM " + ("floored" if ":Floored)" in ast else "breed"),
            None,
            None,
        ),
    )
    monkeypatch.setattr(sql_rewrite, "make_semantic_sql", lambda sql, ctx: sql)
    monkeypatch.setattr(parser, "parse_cypher", lambda cypher: cypher)

    floor = "Source 'floored' is load-protected: it is read only from its replicas"

    async def _govern(sql, role_id, **kw):
        if "floored" in sql:
            raise OperatorFloorError(floor)
        return SimpleNamespace(sql=sql)

    async def _run(plan, st):
        return QueryResult(rows=[(4,)], column_names=["cnt"])

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    monkeypatch.setattr(cypher_router, "_run_plan", _run)

    response = await cypher_router.graph_counts(SimpleNamespace(query_params={}))  # type: ignore[arg-type]
    assert response.status_code == 200
    body = json.loads(bytes(response.body))
    assert body["label_counts"]["Breed"] == {"count": 4}
    assert body["label_counts"]["Floored"]["count"] is None
    assert floor in body["label_counts"]["Floored"]["error"]
    assert body["node_count"] is None


async def test_bolt_impute_says_a_refused_relationship_and_returns_the_others(monkeypatch, caplog):
    """Neo4j Browser's edge imputation over Bolt: a relationship whose read is refused leaves only
    its edges out -- logged and sent as a warning notification with the reason -- and the other
    relationships' edges are still returned."""
    from provisa.bolt import session
    from provisa.compiler.sql_validator import GovernanceRefused, ValidationViolation
    from provisa.core.statement_warnings import collecting

    monkeypatch.setattr(session, "_bolt_label_map", lambda *a: _label_map())

    async def _execute(cypher, params, role_id, include_ops):
        if ":LIKES]" in cypher:
            raise GovernanceRefused([ValidationViolation("V006", REFUSAL)])
        return ["r"], [[{"identity": 11, "type": "OWNS"}]], None

    monkeypatch.setattr(session, "_execute_cypher", _execute)
    app_state = SimpleNamespace(model_db=_ModelDb())
    with caplog.at_level(logging.WARNING, logger="provisa.bolt.session"), collecting() as said:
        cols, edges = await session._impute_relationships(
            {"existingNodeIds": [1], "newNodeIds": [2]}, None, "analyst", False, app_state
        )
    assert cols == ["r"]
    assert edges == [[{"identity": 11, "type": "OWNS"}]]
    assert [w.code for w in said] == ["cypher.relationship_unread"]
    assert ":LIKES]" in said[0].params["statement"]
    assert REFUSAL in said[0].params["reason"]
    assert REFUSAL in caplog.text


async def test_rest_impute_says_a_refused_relationship_and_returns_the_others(monkeypatch):
    """/data/impute-relationships: a relationship whose read is refused is an ``unread`` entry with
    its reason; the other relationships' nodes and edges are still returned (200)."""
    from provisa.api.app import state
    from provisa.api.rest import graph_tools_router as gt
    from provisa.compiler.sql_validator import GovernanceRefused, ValidationViolation
    from provisa.cypher import parser
    from provisa.gql import assembler

    monkeypatch.setattr(state, "contexts", {"analyst": object()}, raising=False)
    monkeypatch.setattr(state, "model_db", _ModelDb(), raising=False)
    monkeypatch.setattr(gt, "_resolve_role_id", lambda request, st: "analyst")
    monkeypatch.setattr(gt, "_build_label_map", lambda ctx, role, st: _label_map())
    monkeypatch.setattr(parser, "parse_cypher", lambda cypher: cypher)

    async def _call(ast, label_map, params, st, ctx, role_id):
        if ":LIKES]" in ast:
            raise GovernanceRefused([ValidationViolation("V006", REFUSAL)])
        return [{"r": {"identity": 11, "type": "OWNS"}}], {}

    async def _register(rows, db):
        return None

    monkeypatch.setattr(gt, "_execute_call_body", _call)
    monkeypatch.setattr(assembler, "assemble_rows", lambda rows, graph_vars: rows)
    monkeypatch.setattr(assembler, "register_node_ids", _register)
    monkeypatch.setattr(assembler, "register_rel_ids", _register)

    body_in = gt.ImputeRequest(nodes=[{"label": "Pet", "id": 1}, {"label": "Toy", "id": 2}])
    response = await gt.impute_relationships(SimpleNamespace(), body_in)  # type: ignore[arg-type]
    assert response.status_code == 200
    body = json.loads(bytes(response.body))
    assert body["rows"] == [{"node": {"identity": 11, "type": "OWNS"}}]
    assert len(body["unread"]) == 1
    assert body["unread"][0]["type"] == "LIKES"
    assert REFUSAL in body["unread"][0]["error"]
