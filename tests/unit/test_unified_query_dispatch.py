# Copyright (c) 2026 Kenneth Stott
# Canary: 03de2839-bbc6-452b-9c5a-592d40eb6268
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Issue #104: /data/query dispatches to the three language endpoints by calling them as plain
functions, so FastAPI resolves none of their parameters. Every header and query parameter the
callee declares must be passed explicitly — an unpassed ``Header(None)`` arrives as the marker
object, which is not a string (the 500 in _parse_accept) and, for ``Query(None)``, is truthy (the
unconditional 410 on the Cypher path).
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest
from fastapi import Header, Query

from provisa.api.data.endpoint import graphql_endpoint
from provisa.api.data.endpoint_dev import QueryRequest, unified_query_endpoint
from provisa.api.data.endpoint_dev import sql_endpoint
from provisa.api.rest.cypher_router import cypher_query


def _fastapi_params(fn) -> set[str]:
    """Parameter names whose default is a FastAPI marker — the ones the caller must supply."""
    return {
        name
        for name, p in inspect.signature(fn).parameters.items()
        if isinstance(p.default, type(Header(None))) or isinstance(p.default, type(Query(None)))
    }


@pytest.mark.parametrize("target", ["sql", "graphql", "cypher"])
@pytest.mark.asyncio
async def test_every_marker_parameter_of_the_callee_is_passed(monkeypatch, target):
    queries = {
        "sql": "SELECT 1",
        "graphql": "{ pets { id } }",
        "cypher": "MATCH (n) RETURN n",
    }
    callees = {
        "sql": (sql_endpoint, "provisa.api.data.endpoint_dev", "sql_endpoint"),
        "graphql": (graphql_endpoint, "provisa.api.data.endpoint", "graphql_endpoint"),
        "cypher": (cypher_query, "provisa.api.rest.cypher_router", "cypher_query"),
    }
    real, module, attr = callees[target]
    required = _fastapi_params(real)
    assert required, f"{attr} declares no FastAPI parameters — the check would be vacuous"
    seen: dict = {}

    async def _spy(*args, **kwargs):
        seen.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(f"{module}.{attr}", _spy)
    monkeypatch.setattr("provisa.api.data.endpoint_dev._resolve_role_id", lambda *a: "org_admin")

    raw_request = SimpleNamespace(state=SimpleNamespace())
    body = QueryRequest(query=queries[target])
    result = await unified_query_endpoint(
        raw_request,
        body,
        x_provisa_role="org_admin",
        accept="application/json",
        x_provisa_stats=None,
        x_provisa_as_of=None,
    )

    assert result == {"ok": True}
    missing = required - set(seen)
    assert not missing, f"{attr} would receive FastAPI marker defaults for: {sorted(missing)}"
    for name, value in seen.items():
        assert not isinstance(value, (type(Header(None)), type(Query(None)))), name
