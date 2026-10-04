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
pipeline's terminal (``pgwire._pipeline.finalize_audit``), driven for real. MERGE and DETACH are
refused at parse time by the production parser.
"""

from __future__ import annotations

import asyncio
import json
import types

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.compiler.write_admission import WriteNotAdmitted
from provisa.cypher.write_translator import (
    CypherLabelMap,
    CypherWriteParseError,
    NodeMapping,
    parse_cypher_write,
)
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
    import provisa.api.app as appmod
    import provisa.pgwire._pipeline as pipeline
    from provisa.api.rest import cypher_router

    sent: dict = {}

    async def _govern(sql, role_id, *, exec_params=None, state=None, cache_hint=None):
        # The one pipeline's governance stage, as it receives the route's write.
        sent.update(sql=sql, role_id=role_id, params=exec_params)
        return types.SimpleNamespace(sql=sql)

    async def _execute(plan, _state):
        return types.SimpleNamespace(rowcount=1, rows=[])

    monkeypatch.setattr(pipeline, "_govern_and_route_compiled", _govern)
    monkeypatch.setattr(pipeline, "_execute_plan", _execute)
    monkeypatch.setattr(cypher_router, "_build_label_map", lambda *_a: _users_label_map())
    monkeypatch.setattr(appmod.state, "contexts", {"writer": object()}, raising=False)

    request = types.SimpleNamespace(state=types.SimpleNamespace(role="writer"))
    body = cypher_router.CypherRequest(query=r818["create"], params={})
    response = asyncio.run(cypher_router.cypher_query(body, request, None, None))  # type: ignore[arg-type]
    r818["status"] = response.status_code
    r818["body"] = json.loads(response.body)
    r818["sent"] = sent


@then("the role's row filter is applied to the translated write by the one write admission")
def _r818_then_row_filter(r818: dict) -> None:
    sent = r818["sent"]
    assert sent["role_id"] == "writer"
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
    # The route answers the pipeline's row count.
    assert (r818["status"], r818["body"]) == (200, {"affected_rows": 1, "type": "cypher"})
    # The one after-write step, at the pipeline's terminal, for the table written.
    calls = run_after_write(_USERS_TABLE_ID, "users", "pg-main")
    assert calls["invalidated"] == [_USERS_TABLE_ID]
    assert calls["stale"] == ["users"]
    assert calls["events"] == [("users", "pg-main")]
    assert calls["sinks"] == ["users"]


@given("a MERGE or DETACH statement", target_fixture="r818_bad")
def _r818_given_bad() -> dict:
    return {"stmts": ["MERGE (u:users {id: 1})", "MATCH (u:users) WHERE u.id = 1 DETACH DELETE u"]}


@when("parsed")
def _r818_when_parsed(r818_bad: dict) -> None:
    """Both parsers the route asks: the write parser refuses the statement as a write, and the
    read parser's refusal is the error the client is answered with."""
    from provisa.cypher.parser import CypherParseError, parse_cypher

    results = []
    for stmt in r818_bad["stmts"]:
        with pytest.raises(CypherWriteParseError):
            parse_cypher_write(stmt)
        try:
            parse_cypher(stmt)
            results.append((stmt, None))
        except CypherParseError as exc:
            results.append((stmt, str(exc)))
    r818_bad["results"] = results


@then("it is rejected at parse time with a precise error")
def _r818_then_rejected(r818_bad: dict) -> None:
    for stmt, error in r818_bad["results"]:
        assert error is not None, f"parsed without refusal: {stmt!r}"
        upper_err, upper_stmt = error.upper(), stmt.upper()
        # The error names the pattern it refuses.
        if "MERGE" in upper_stmt:
            assert "MERGE" in upper_err, error
        if "DETACH" in upper_stmt:
            assert "DETACH" in upper_err, error
        # Cypher is not read-only (REQ-818 supersedes REQ-346).
        assert "READ-ONLY" not in upper_err, error
