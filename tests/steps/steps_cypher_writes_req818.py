# Copyright (c) 2026 Kenneth Stott
# Canary: a0f58c0c-c02f-46b5-8a2a-c4f14010327a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD steps for REQ-818 — Cypher WRITES via /data/cypher.

A CREATE is posted to the real route. The route translates it and hands the translated write to
the one pipeline every surface's write goes through; there the one write admission applies the
role's row filter (driven here for real on exactly the statement the route sent), and the route
answers ``affected_rows`` from the pipeline's result. The steps after a write run at the
pipeline's terminal (``pgwire._pipeline.finalize_audit``), driven for real. A MERGE of a label
the model does not have is MATCH, then CREATE, in the principal's graph session (REQ-2176).
"""

from __future__ import annotations

import asyncio
import json
import types

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.auth.models import AuthIdentity
from provisa.compiler.write_admission import WriteNotAdmitted
from provisa.cypher.write_translator import CypherLabelMap, NodeMapping
from tests.unit.graph_session_double import empty_graph_sessions
from tests.write_governance import admitted, run_after_write, target_ref, write_governance

scenarios("../features/REQ-818.feature")

_USERS_TABLE_ID = 1
_ROW_FILTER = {_USERS_TABLE_ID: "id < 10"}


def _users_label_map() -> CypherLabelMap:
    users = NodeMapping(
        label="users",
        type_name="Users",
        domain_label=None,
        table_label="users",
        table_id=_USERS_TABLE_ID,
        source_id="pg-main",
        id_column="id",
        pk_columns=["id"],
        catalog_name="postgresql",
        schema_name="public",
        table_name="users",
        properties={"id": "id", "name": "name"},
    )
    return CypherLabelMap(nodes={"users": users}, relationships={})


def _governance(sql: str):
    """The writer role's governance over the table ``sql`` writes: the write right, every column
    writable, and a row filter that keeps ids under 10."""
    return write_governance({target_ref(sql): (_USERS_TABLE_ID, ["id", "name"])}, rls=_ROW_FILTER)


@given("a valid CREATE statement targeting a table with write rights", target_fixture="r818")
def _r818_given_create() -> dict:
    return {"create": "CREATE (u:users {id: 1, name: 'Ada'})"}


@when("executed via the /data/cypher endpoint")
def _r818_when_execute(r818: dict, monkeypatch) -> None:
    if "merge" in r818:
        _r818_merge_through_route(r818, monkeypatch)
        return
    import provisa.api.app as appmod
    import provisa.pgwire._pipeline as pipeline
    from provisa.api.rest import cypher_router

    sent: dict = {}

    async def _govern(sql, role_id, *, exec_params=None, state=None, cache_hint, sdl_joins):
        # The one pipeline's governance stage, as it receives the route's write: required
        # keywords as the real one declares them, so a call that drifts fails here by name.
        sent.update(sql=sql, role_id=role_id, params=exec_params, sdl_joins=sdl_joins)
        return types.SimpleNamespace(sql=sql)

    async def _execute(plan, _state):
        # A QueryResult's shape: an INSERT with no RETURNING names no columns.
        return types.SimpleNamespace(rowcount=1, rows=[], column_names=[])

    monkeypatch.setattr(pipeline, "_govern_and_route_compiled", _govern)
    monkeypatch.setattr(pipeline, "_execute_plan", _execute)
    monkeypatch.setattr(cypher_router, "_build_label_map", lambda *_a: _users_label_map())
    monkeypatch.setattr(appmod.state, "contexts", {"writer": object()}, raising=False)

    # REQ-2176: every statement runs in its principal's graph session; the stand-in state has no
    # control plane to hold one, so each is given an empty one for the principal below.
    empty_graph_sessions(monkeypatch)
    identity = AuthIdentity(user_id="ada", email=None, display_name=None, roles=["writer"])
    request = types.SimpleNamespace(state=types.SimpleNamespace(role="writer", identity=identity))
    body = cypher_router.CypherRequest(query=r818["create"], params={})
    response = asyncio.run(cypher_router.cypher_query(body, request, None, None))  # type: ignore[arg-type]
    r818["status"] = response.status_code
    r818["body"] = json.loads(response.body)
    r818["sent"] = sent


@then("the role's row filter is applied to the translated write by the one write admission")
def _r818_then_row_filter(r818: dict) -> None:
    sent = r818["sent"]
    assert sent["role_id"] == "writer"
    assert sent["sdl_joins"] is False  # a Cypher write: its joins are not the SDL's
    sql = sent["sql"]
    assert sql.upper().startswith("INSERT INTO") and "USERS" in sql.upper()
    # The new row (id 1) is inside the role's filter: the write goes on as translated.
    assert admitted(sql, _governance(sql), sent["params"]) == sql
    # The same write with a row outside it (id 42) is refused before anything reaches the source.
    outside = sql.replace("1", "42", 1)
    assert outside != sql
    with pytest.raises(WriteNotAdmitted, match="outside role"):
        admitted(outside, _governance(outside), sent["params"])


@then(
    "it executes as a direct table write, returns affected_rows, and runs the one after-write step"
)
def _r818_then_direct_write(r818: dict) -> None:
    # The route answers the pipeline's row count, and what the write did as Neo4j counts it
    # (REQ-670, amended 2026-10-28).
    assert (r818["status"], r818["body"]) == (
        200,
        {
            "affected_rows": 1,
            "type": "cypher",
            "counters": {
                "nodes-created": 1,
                "properties-set": 2,
                "labels-added": 1,
                "contains-updates": True,
            },
        },
    )
    # The one after-write step, at the pipeline's terminal, for the table written.
    calls = run_after_write(_USERS_TABLE_ID, "users", "pg-main")
    assert calls["invalidated"] == [_USERS_TABLE_ID]
    assert calls["stale"] == ["users"]
    assert calls["events"] == [("users", "pg-main")]
    assert [c[:2] for c in calls["changed"]] == [(_USERS_TABLE_ID, "pg-main")]


@given("a MERGE of a label the model does not have", target_fixture="r818")
def _r818_given_merge() -> dict:
    # Temp is no label of the model's (users is): REQ-2176 makes it the session's alone.
    return {"merge": "MERGE (n:Temp {k: 1})"}


def _r818_merge_through_route(r818: dict, monkeypatch) -> None:
    """The MERGE, posted to the real route twice, then a MERGE of other properties: each runs in
    the principal's graph session, held here over one DuckDB (tests.unit.ephemeral_harness)."""
    import provisa.api.app as appmod
    from provisa.api.rest import cypher_router
    from provisa.ephemeral import seam
    from tests.unit.ephemeral_harness import Session

    h = Session(monkeypatch, {"users": ({"id": "integer", "name": "text"}, "(1, 'Ada')")})
    monkeypatch.setattr(appmod.state, "contexts", {"writer": object()}, raising=False)
    monkeypatch.setattr(
        cypher_router, "role_surface", lambda _s, _r: types.SimpleNamespace(context=object())
    )
    monkeypatch.setattr(cypher_router, "_build_label_map", lambda *_a: h.model)
    real_write = seam.write

    async def write(statement, params, label_map, _session, _state, _read, _write):
        # One graph session across the statements: the principal's, as the store keeps it.
        return await real_write(statement, params, label_map, h.session, h.state, h.read, h.write)

    monkeypatch.setattr(seam, "write", write)
    empty_graph_sessions(monkeypatch)
    identity = AuthIdentity(user_id="ada", email=None, display_name=None, roles=["writer"])
    request = types.SimpleNamespace(state=types.SimpleNamespace(role="writer", identity=identity))
    answers = []
    for statement in (r818["merge"], r818["merge"], "MERGE (n:Temp {k: 2})"):
        body = cypher_router.CypherRequest(query=statement, params={})
        response = asyncio.run(cypher_router.cypher_query(body, request, None, None))  # type: ignore[arg-type]
        answers.append((response.status_code, json.loads(response.body)))
    r818["answers"] = answers
    r818["session_nodes"] = h.run("MATCH (n:Temp) RETURN n.k")
    r818["model_rows"] = h.duck.execute("SELECT id, name FROM s.users").fetchall()


@then("it matches the session node with those properties, or creates one ephemeral node (REQ-2176)")
def _r818_then_merged(r818: dict) -> None:
    (first, again, other) = r818["answers"]
    # The first MERGE matches nothing and creates one node; the same MERGE again matches it.
    assert first[0] == 200 and first[1]["counters"]["nodes"] == 1, first
    assert again[0] == 200 and again[1]["counters"]["nodes"] == 0, again
    # Other properties match no session node: one more is created.
    assert other[0] == 200 and other[1]["counters"]["nodes"] == 1, other
    assert sorted(tuple(r.values()) for r in r818["session_nodes"]) == [(1,), (2,)]
    # The model's table is not written: Temp is the session's alone.
    assert r818["model_rows"] == [(1, "Ada")]
