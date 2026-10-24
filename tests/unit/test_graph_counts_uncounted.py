# Copyright (c) 2026 Kenneth Stott
# Canary: becbfcf2-019d-477f-8cb5-8bcf222d65ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""/data/graph-counts (REQ-392): a label the engine could not count is said, with its reason, and
a total is a total only when every label in it was counted -- never a sum that left some out."""

from __future__ import annotations

import logging


from provisa.api.rest.cypher_router import _total, _uncounted, cypher_error


def test_an_uncounted_label_says_why_and_is_logged(caplog):
    refused = cypher_error(
        400,
        "cypher.execution_failed",
        "Catalog petstore is not attached",
        sql="SELECT",
        error="Catalog petstore is not attached",
    )
    with caplog.at_level(logging.WARNING, logger="provisa.api.rest.cypher_router"):
        entry = _uncounted("MATCH (n:PetStore:INVENTORY) RETURN count(n) AS cnt", refused)
    assert entry == {"count": None, "error": "Catalog petstore is not attached"}
    assert "PetStore:INVENTORY" in caplog.text
    assert "Catalog petstore is not attached" in caplog.text


def test_the_total_is_null_when_any_label_is_uncounted():
    assert _total({"A": {"count": 3}, "B": {"count": 4}}) == 7
    assert _total({"A": {"count": 3}, "B": {"count": None, "error": "not attached"}}) is None
    assert _total({}) == 0


async def test_bolt_totals_are_unknown_when_a_count_fails_and_say_why(monkeypatch, caplog):
    """The Neo4j Browser's node/relationship totals over Bolt: a count the engine could not make
    leaves the total unknown (null), logged and sent as a warning notification -- not 0."""
    from types import SimpleNamespace

    from provisa.bolt import session
    from provisa.core.statement_warnings import collecting

    nodes = {
        "Pet": SimpleNamespace(label="Pet", native_filter_columns=[]),
        "Inquiry": SimpleNamespace(label="Inquiry", native_filter_columns=[]),
    }
    monkeypatch.setattr(
        session,
        "_bolt_label_map",
        lambda *a: SimpleNamespace(nodes=nodes, relationships={}),
    )

    async def _execute(cypher, params, role_id, include_ops):
        if ":Inquiry" in cypher:
            raise RuntimeError("Catalog petstore is not attached")
        return ["cnt"], [[5]], None

    monkeypatch.setattr(session, "_execute_cypher", _execute)
    with caplog.at_level(logging.WARNING, logger="provisa.bolt.session"), collecting() as said:
        node_count, rel_count = await session._graph_counts(None, "analyst", False, None)
    assert node_count is None
    assert rel_count == 0
    assert [w.code for w in said] == ["cypher.count_unknown"]
    assert said[0].params["reason"] == "Catalog petstore is not attached"
    assert "Catalog petstore is not attached" in caplog.text


async def test_a_parameterized_table_is_uncounted_and_the_other_labels_are_counted(monkeypatch):
    """#297: a domain holding a table that needs a parameter answers 200 -- that label uncounted,
    with the reason its read was refused, the others counted -- not 500."""
    import json
    from types import SimpleNamespace

    from provisa.api.app import state
    from provisa.api.rest import cypher_router
    from provisa.compiler import sql_rewrite
    from provisa.compiler.nf_extractor import apply_dropped_tables
    from provisa.core.parameters import missing_reason
    from provisa.cypher import parser
    from provisa.executor.result import QueryResult
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(state, "contexts", {"analyst": object()}, raising=False)
    monkeypatch.setattr(state, "roles", {"analyst": {"domain_access": ["*"]}}, raising=False)
    monkeypatch.setattr(cypher_router, "_resolve_role_id", lambda request, st: "analyst")
    monkeypatch.setattr(cypher_router, "_build_label_map", lambda ctx, role, st: None)
    monkeypatch.setattr(
        cypher_router,
        "_countable_labels",
        lambda label_map, domains, reads: (["Breed", "AssignmentsByBreed"], []),
    )
    monkeypatch.setattr(
        cypher_router, "_build_sql_from_ast", lambda ast, *a: (ast_sql(ast), None, None)
    )
    monkeypatch.setattr(sql_rewrite, "make_semantic_sql", lambda sql, ctx: sql)
    # The Cypher is handed to the statement builder as it is (stubbed above).
    monkeypatch.setattr(parser, "parse_cypher", lambda cypher: cypher)

    def ast_sql(cypher: str) -> str:
        table = "assignments_by_breed" if ":AssignmentsByBreed)" in cypher else "breed"
        return f"SELECT COUNT(*) AS cnt FROM {table}"

    async def _govern(sql, role_id, **kw):
        # The pipeline's own refusal of a read that leaves a required parameter without a value.
        if "assignments_by_breed" in sql:
            apply_dropped_tables(sql, {"assignments_by_breed": missing_reason(["breed_name"])})
        return SimpleNamespace(sql=sql)

    async def _run(plan, st):
        return QueryResult(rows=[(4,)], column_names=["cnt"])

    monkeypatch.setattr(_pipeline, "_govern_and_route_compiled", _govern)
    monkeypatch.setattr(cypher_router, "_run_plan", _run)

    request = SimpleNamespace(query_params={})
    response = await cypher_router.graph_counts(request)  # type: ignore[arg-type]
    assert response.status_code == 200
    body = json.loads(bytes(response.body))
    assert body["label_counts"]["Breed"] == {"count": 4}
    uncounted = body["label_counts"]["AssignmentsByBreed"]
    assert uncounted["count"] is None
    assert "assignments_by_breed" in uncounted["error"]
    assert "breed_name" in uncounted["error"]
    assert body["node_count"] is None
