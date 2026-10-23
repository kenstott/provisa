# Copyright (c) 2026 Kenneth Stott
# Canary: 30079947-7d65-481c-b0eb-140d3d07421e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""/data/cypher with ``"results": "each"``: the statements a query holds, split as Neo4j Browser
splits them, each run on its own in order and answered by its own result or error."""

from __future__ import annotations

import json
from types import SimpleNamespace

from fastapi.responses import JSONResponse

from provisa.api.rest import cypher_router
from provisa.api.rest.cypher_router import CypherRequest
from provisa.cypher.parser import split_cypher_statements


def test_statements_split_on_semicolons_outside_strings_and_comments():
    assert split_cypher_statements(
        "MATCH (a:Pets) WHERE a.note = 'x;y' RETURN a; // one; two\n"
        'MATCH (b:Owners) WHERE b.n = "p;q" RETURN b;'
    ) == [
        "MATCH (a:Pets) WHERE a.note = 'x;y' RETURN a",
        '// one; two\nMATCH (b:Owners) WHERE b.n = "p;q" RETURN b',
    ]
    # A trailing ';', and a part holding only a comment, are no statements.
    assert split_cypher_statements("MATCH (n) RETURN n;") == ["MATCH (n) RETURN n"]
    assert split_cypher_statements("MATCH (n) RETURN n; // done") == ["MATCH (n) RETURN n"]
    assert split_cypher_statements("  ") == []


def _request():
    def forbidden(_request, exc):
        return JSONResponse(status_code=403, content={"error": str(exc)})

    return SimpleNamespace(app=SimpleNamespace(exception_handlers={PermissionError: forbidden}))


async def test_each_statement_is_run_on_its_own_and_answered_in_order(monkeypatch):
    ran: list[CypherRequest] = []

    async def one(body, request, query_id, x_provisa_stats):
        ran.append(body)
        if "Secret" in body.query:
            raise PermissionError("Label Secret is not visible to role analyst")
        if "Broken" in body.query:
            return JSONResponse(status_code=400, content={"error": "Unknown label Broken"})
        return JSONResponse(content={"columns": ["n"], "rows": [[body.query]]})

    monkeypatch.setattr(cypher_router, "cypher_query", one)
    body = CypherRequest(
        query="MATCH (a:Pets) RETURN a; MATCH (s:Secret) RETURN s; "
        "MATCH (b:Broken) RETURN b; MATCH (o:Owners) WHERE o.id = $id RETURN o",
        params={"id": 7},
        results="each",
    )
    answered = await cypher_router._each_statement(body, _request(), "true")
    results = json.loads(bytes(answered.body))["results"]

    # Each statement alone, in order, with the shared parameters.
    assert [r.query for r in ran] == [r["statement"] for r in results]
    assert all(r.results == "last" and r.params == {"id": 7} for r in ran)
    assert [r["status"] for r in results] == [200, 403, 400, 200]
    assert results[0]["body"]["rows"] == [["MATCH (a:Pets) RETURN a"]]
    # A refusal is answered by the app's own handler for it; the statements after it still run.
    assert results[1]["body"] == {"error": "Label Secret is not visible to role analyst"}
    assert results[2]["body"] == {"error": "Unknown label Broken"}
    assert results[3]["body"]["rows"] == [["MATCH (o:Owners) WHERE o.id = $id RETURN o"]]
