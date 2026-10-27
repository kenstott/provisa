# Copyright (c) 2026 Kenneth Stott
# Canary: 44b705f2-7571-4c45-8e57-1c0ecd666a89
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1350, REQ-1915: a Cypher read refused by name is answered with that refusal's own status
and code, as every other surface answers it -- never as a 500. The bench's filtered read of a
row-level Neo4j table answered REST with the refusal (400) and Cypher with "Governance failed"
(500): the Cypher route turned every error the pipeline raised into its own 500."""

# Requirements: REQ-1350, REQ-1915

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from provisa.api.errors import ApiError, RowLevelFilterNotPushable, RowLevelFilterRequired
from provisa.api.rest import cypher_router
from provisa.core.read_refusal import ReadRefused


class _Unavailable(ReadRefused):
    code = "query.home_region_unavailable"

    def __init__(self) -> None:
        super().__init__("the region holding orders cannot be reached")
        self.params = {"table": "orders"}


_REFUSALS = [
    RowLevelFilterRequired("bench_order_node"),
    RowLevelFilterNotPushable("bench_order_node", "neo4j", "the source has no such predicate"),
    _Unavailable(),
]


def _govern(monkeypatch, error: BaseException):
    import provisa.pgwire._pipeline as pipeline

    async def _refuse(*_a, **_k):
        raise error

    monkeypatch.setattr(pipeline, "_govern_and_route_compiled", _refuse)
    return asyncio.run(cypher_router._governed_plan("SELECT 1", "analyst"))


@pytest.mark.parametrize("refusal", _REFUSALS, ids=lambda e: type(e).__name__)
def test_a_refusal_at_planning_is_answered_by_its_own_handler(monkeypatch, refusal):
    with pytest.raises(type(refusal)) as raised:
        _govern(monkeypatch, refusal)
    assert raised.value is refusal


def test_an_unexpected_planning_failure_is_still_a_500(monkeypatch):
    answered = _govern(monkeypatch, KeyError("boom"))
    assert answered.status_code == 500


@pytest.mark.parametrize("refusal", _REFUSALS, ids=lambda e: type(e).__name__)
def test_a_refusal_at_execution_is_answered_by_its_own_handler(monkeypatch, refusal):
    import provisa.pgwire._pipeline as pipeline

    async def _refuse(_plan, _state):
        raise refusal

    monkeypatch.setattr(pipeline, "_execute_plan", _refuse)
    monkeypatch.setattr(pipeline, "require_governed_plan", lambda _plan: None)
    plan = SimpleNamespace(physical_sql="SELECT 1", exec_sql=None)
    state = SimpleNamespace(federation_engine=SimpleNamespace())
    with pytest.raises(type(refusal)):
        asyncio.run(cypher_router._run_plan(plan, state))


def test_the_refusals_are_coded_api_errors_or_read_refusals():
    # What the route lets through is exactly what the app has a handler for.
    assert all(isinstance(e, (ApiError, ReadRefused)) for e in _REFUSALS)


@pytest.mark.parametrize("refusal", _REFUSALS, ids=lambda e: type(e).__name__)
def test_a_refusal_in_an_independent_call_is_answered_by_its_own_handler(monkeypatch, refusal):
    async def _refuse(*_a, **_k):
        raise refusal

    monkeypatch.setattr(cypher_router, "_execute_call_body", _refuse)
    body = cypher_router.CypherRequest(query="CALL { MATCH (o:Order) RETURN o } RETURN o")
    with pytest.raises(type(refusal)):
        asyncio.run(
            cypher_router._execute_multi_call(
                [SimpleNamespace(body=None)], None, body, None, "analyst", None, None, None
            )
        )


def test_a_write_lets_a_refusal_past_its_catch_all():
    """The write path answers a source's error of any type as "Write failed" (500); a refusal by
    name is not one of those."""
    from pathlib import Path

    source = Path(cypher_router.__file__).read_text(encoding="utf-8")
    write = source[source.index("await written(") :]
    write = write[: write.index('"cypher.write_failed"')]
    assert "except (ApiError, ReadRefused):" in write


def test_the_neo4j_endpoint_answers_a_coded_refusal_with_its_own_status_and_message():
    import json

    from provisa.api.rest.neo4j_compat_router import _refused

    response = _refused(RowLevelFilterRequired("bench_order_node"))
    assert response.status_code == 400
    (error,) = json.loads(response.body)["errors"]
    assert "bench_order_node" in error["message"] and "Failed" not in error["message"]
    assert error["code"] == "Neo.ClientError.Statement.ArgumentError"
    assert _refused(_Unavailable()).status_code == 503
