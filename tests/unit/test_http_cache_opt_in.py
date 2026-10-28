# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2e5a91-3c4b-4f68-a0d7-e1b95c62f803
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The response-cache opt-in in an HTTP request's headers (REQ-544, REQ-536).

REST, JSON:API, GraphQL over HTTP, Cypher over HTTP and the Neo4j Query API take the standard
``Cache-Control: max-age=N`` (or ``max-stale=N``) and gRPC's ``x-provisa-cache`` /
``x-provisa-cache-ttl`` as headers; each behaves as ``@cached(ttl: N)``. Where the request's text
states an opt-in too, two that disagree are refused by name. Each surface reports
``X-Provisa-Cache`` on its row responses.
"""

# Requirements: REQ-544, REQ-536

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.compiler.directives import (
    NO_CACHE_HINT,
    CacheHint,
    cache_hint_from_cache_control,
    request_cache_hint,
)
from provisa.executor.result import QueryResult
from tests.unit import test_rest_jsonapi_request_plan as _plan_tests


# One instant for the whole test: an entry is stored relative to it and its age is read at it, so
# the age a response reports is the one the entry was made with however long the test takes.
_NOW = 1_800_000_000.0


@pytest.fixture(autouse=True)
def _frozen_clock(monkeypatch):
    from types import SimpleNamespace

    from provisa.cache import store

    monkeypatch.setattr(store, "time", SimpleNamespace(time=lambda: _NOW))


def _stored(age_seconds: int):
    from provisa.cache.store import CachedResult

    return CachedResult(data=b"", cached_at=_NOW - age_seconds, ttl=300)


# -- the standard header ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "hint"),
    [
        ("max-age=60", CacheHint(opt_in=True, ttl=60)),
        ('max-age="60"', CacheHint(opt_in=True, ttl=60)),
        ("max-stale=30", CacheHint(opt_in=True, ttl=30)),
        ("max-stale", CacheHint(opt_in=True, ttl=None)),
        ("Max-Age=5, max-stale=5", CacheHint(opt_in=True, ttl=5)),
        ("no-cache", NO_CACHE_HINT),
        ("no-store, no-cache", NO_CACHE_HINT),
        ("max-age=0", CacheHint(opt_in=True, ttl=0)),
        ("max-age=60, no-transform", CacheHint(opt_in=True, ttl=60)),
    ],
)
def test_cache_control_states_the_opt_in_as_cached_does(value, hint):
    assert cache_hint_from_cache_control(value) == hint


@pytest.mark.parametrize("value", [None, "", "no-transform", "only-if-cached, min-fresh=5"])
def test_a_header_without_a_cache_directive_states_nothing(value):
    assert cache_hint_from_cache_control(value) is None


@pytest.mark.parametrize("value", ["max-age=soon", "max-age=-1", "max-age=", "max-stale=1.5"])
def test_a_malformed_value_fails_the_request(value):
    with pytest.raises(ValueError, match="takes seconds"):
        cache_hint_from_cache_control(value)


@pytest.mark.parametrize("value", ["no-cache, max-age=60", "max-age=5, max-stale=60"])
def test_directives_that_disagree_fail_the_request(value):
    with pytest.raises(ValueError, match="more than one cache opt-in"):
        cache_hint_from_cache_control(value)


# -- every form of one request ---------------------------------------------------------------------


def test_no_form_is_no_cache():
    assert request_cache_hint(NO_CACHE_HINT, {}) == NO_CACHE_HINT


@pytest.mark.parametrize(
    ("headers", "hint"),
    [
        ({"x-provisa-cache": "true"}, CacheHint(opt_in=True, ttl=None)),
        ({"x-provisa-cache-ttl": "45"}, CacheHint(opt_in=True, ttl=45)),
        ({"cache-control": "max-age=45"}, CacheHint(opt_in=True, ttl=45)),
        ({"cache-control": "no-cache"}, NO_CACHE_HINT),
        ({"x-provisa-cache-ttl": "45", "cache-control": "max-age=45"}, CacheHint(True, 45)),
    ],
)
def test_the_provisa_headers_and_cache_control_each_opt_in(headers, hint):
    assert request_cache_hint(NO_CACHE_HINT, headers) == hint


def test_a_header_agreeing_with_the_text_is_accepted():
    text = CacheHint(opt_in=True, ttl=60)
    assert request_cache_hint(text, {"cache-control": "max-age=60"}) == text
    assert request_cache_hint(text, {}) == text


@pytest.mark.parametrize(
    ("text", "headers", "names"),
    [
        (CacheHint(True, 60), {"cache-control": "max-age=30"}, ("text", "Cache-Control")),
        (CacheHint(True, None), {"cache-control": "max-age=30"}, ("text", "Cache-Control")),
        (CacheHint(True, 60), {"cache-control": "no-store"}, ("text", "Cache-Control")),
        (CacheHint(True, 60), {"x-provisa-cache": "true"}, ("text", "x-provisa-cache")),
        (
            NO_CACHE_HINT,
            {"x-provisa-cache-ttl": "10", "cache-control": "max-age=20"},
            ("x-provisa-cache", "Cache-Control"),
        ),
    ],
)
def test_two_forms_that_disagree_are_refused_by_name(text, headers, names):
    with pytest.raises(ValueError, match="more than once") as refused:
        request_cache_hint(text, headers)
    for name in names:
        assert name in str(refused.value)


def test_the_text_debug_trace_and_the_headers_are_kept():
    text = CacheHint(opt_in=False, ttl=None, debug_trace=True)
    assert request_cache_hint(text, {"cache-control": "max-age=9"}) == CacheHint(True, 9, True)


# -- REST and JSON:API -----------------------------------------------------------------------------


@pytest.fixture
def anyio_backend():
    return "asyncio"


surface = _plan_tests.surface  # the real REST and JSON:API routers over a recorded pipeline


class _Recorded:
    """What each statement a surface hands the pipeline was asked with; a hit answers from an entry."""

    def __init__(self, monkeypatch, surface, hit=None) -> None:
        self.kwargs: list[dict] = []
        govern, execute = surface.pipeline.govern, surface.pipeline.execute

        async def _govern(sql, role_id, **kw):
            self.kwargs.append(kw)
            return await govern(sql, role_id, **kw)

        async def _execute(plan, state):
            answered = await execute(plan, state)
            answered.cache_entry = hit
            return answered

        monkeypatch.setattr("provisa.pgwire._pipeline._govern_and_route_compiled", _govern)
        monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", _execute)


_PATHS = {
    "rest": ("/data/rest/sales/orders", {"X-Provisa-Role": "admin"}),
    "jsonapi": (
        "/data/jsonapi/sales/orders",
        {"accept": "application/vnd.api+json", "X-Provisa-Role": "admin"},
    ),
}


@pytest.mark.anyio
@pytest.mark.parametrize("kind", list(_PATHS))
@pytest.mark.parametrize(
    ("sent", "hint"),
    [
        ({}, NO_CACHE_HINT),
        ({"Cache-Control": "max-age=120"}, CacheHint(True, 120)),
        ({"x-provisa-cache": "true"}, CacheHint(True, None)),
    ],
)
async def test_rest_and_jsonapi_carry_the_headers_opt_in(surface, monkeypatch, kind, sent, hint):
    recorded = _Recorded(monkeypatch, surface)
    path, headers = _PATHS[kind]
    resp = await surface.client.get(path, params={"limit": "2"}, headers={**headers, **sent})
    assert resp.status_code == 200, resp.text
    assert [(k["cache_hint"], k["serve_cached"]) for k in recorded.kwargs] == [(hint, True)]
    assert resp.headers["X-Provisa-Cache"] == "MISS"


@pytest.mark.anyio
@pytest.mark.parametrize("kind", list(_PATHS))
async def test_rest_and_jsonapi_report_a_hit_and_its_age(surface, monkeypatch, kind):
    _Recorded(monkeypatch, surface, hit=_stored(11))
    path, headers = _PATHS[kind]
    resp = await surface.client.get(path, headers={**headers, "Cache-Control": "max-age=60"})
    assert resp.status_code == 200, resp.text
    assert resp.headers["X-Provisa-Cache"] == "HIT"
    assert resp.headers["X-Provisa-Cache-Age"] == "11"


@pytest.mark.anyio
@pytest.mark.parametrize("kind", list(_PATHS))
async def test_rest_and_jsonapi_refuse_forms_that_disagree(surface, monkeypatch, kind):
    recorded = _Recorded(monkeypatch, surface)
    path, headers = _PATHS[kind]
    sent = {"x-provisa-cache-ttl": "10", "Cache-Control": "max-age=20"}
    resp = await surface.client.get(path, headers={**headers, **sent})
    assert resp.status_code == 400
    assert "more than once" in resp.text and "Cache-Control" in resp.text
    assert recorded.kwargs == []


# -- GraphQL over HTTP -----------------------------------------------------------------------------


def test_graphql_takes_the_header_beside_cached(monkeypatch):
    from tests.unit.test_graphql_plan_cache import _endpoint_harness

    h = _endpoint_harness(monkeypatch)
    h.call(request_headers={"cache-control": "max-age=90"})
    h.call()
    assert h.seen_hints[0].opt_in and h.seen_hints[0].ttl == 90
    assert not h.seen_hints[1].opt_in


def test_graphql_refuses_a_header_that_disagrees_with_cached(monkeypatch):
    from provisa.api.data import endpoint
    from provisa.api.errors import ApiError
    from provisa.compiler.directives import QueryDirectives
    from tests.unit.test_graphql_plan_cache import _endpoint_harness

    h = _endpoint_harness(monkeypatch)
    cached = QueryDirectives(cache_opt_in=True, cache_ttl=30)
    monkeypatch.setattr(endpoint, "_build_directives_with_legacy", lambda *_a: cached)
    h.call(request_headers={"x-provisa-cache-ttl": "30"})
    assert h.seen_hints[-1] == CacheHint(True, 30)
    with pytest.raises(ApiError) as refused:
        h.call(request_headers={"cache-control": "max-age=5"})
    assert refused.value.status_code == 400
    assert refused.value.code == "data.invalid_cache_control"
    assert "Cache-Control" in str(refused.value.detail)


# -- Cypher over HTTP and the Neo4j Query API ------------------------------------------------------


def _cypher(monkeypatch):
    from tests.unit.test_cypher_kept_plan import _Pipeline, _state

    pipe = _Pipeline(monkeypatch, _state())
    monkeypatch.setattr("provisa.compiler.stage2.build_governance_context", MagicMock())
    monkeypatch.setattr("provisa.compiler.sql_validator.validate_sql", MagicMock(return_value=[]))
    return pipe


def _request(headers: dict) -> MagicMock:
    request = MagicMock()
    request.state.role = "analyst"
    request.headers = headers
    return request


async def _data_cypher(query: str, headers: dict):
    from provisa.api.rest.cypher_router import CypherRequest, cypher_query
    from tests.unit.test_cypher_kept_plan import _CYPHER

    body = CypherRequest(query=query or _CYPHER, params={"min": 1})
    return await cypher_query(body, _request(headers), query_id=None, x_provisa_stats=None)


async def _neo4j(query: str, headers: dict):
    from provisa.api.rest.neo4j_compat_router import QueryV2Request, neo4j_query_v2
    from tests.unit.test_cypher_kept_plan import _CYPHER

    body = QueryV2Request(statement=query or _CYPHER, parameters={"min": 1})
    return await neo4j_query_v2("neo4j", body, _request(headers))


_CYPHER_SURFACES = {"data_cypher": _data_cypher, "neo4j": _neo4j}


@pytest.mark.anyio
@pytest.mark.parametrize("surface_name", list(_CYPHER_SURFACES))
async def test_cypher_over_http_takes_the_header(monkeypatch, surface_name):
    pipe = _cypher(monkeypatch)
    send = _CYPHER_SURFACES[surface_name]
    miss = await send("", {"cache-control": "max-age=75"})
    assert miss.status_code == 200, miss.body
    assert pipe.governed_kw[-1]["cache_hint"] == CacheHint(True, 75)
    assert pipe.governed_kw[-1]["serve_cached"] is True
    assert miss.headers["X-Provisa-Cache"] == "MISS"

    served = QueryResult(rows=[(10.5,)], column_names=["amount"], cache_entry=_stored(4))
    monkeypatch.setattr("provisa.pgwire._pipeline._execute_plan", AsyncMock(return_value=served))
    hit = await send("", {"x-provisa-cache": "true"})
    assert pipe.governed_kw[-1]["cache_hint"] == CacheHint(True, None)
    assert hit.headers["X-Provisa-Cache"] == "HIT"
    assert hit.headers["X-Provisa-Cache-Age"] == "4"
    assert hit.body == miss.body


@pytest.mark.anyio
@pytest.mark.parametrize("surface_name", list(_CYPHER_SURFACES))
async def test_cypher_over_http_refuses_a_header_that_disagrees_with_its_hint(
    monkeypatch, surface_name
):
    from tests.unit.test_cypher_kept_plan import _CYPHER

    pipe = _cypher(monkeypatch)
    hinted = "// @provisa cache_ttl=60\n" + _CYPHER
    agreed = await _CYPHER_SURFACES[surface_name](hinted, {"cache-control": "max-age=60"})
    assert agreed.status_code == 200, agreed.body
    assert pipe.governed_kw[-1]["cache_hint"] == CacheHint(True, 60)
    refused = await _CYPHER_SURFACES[surface_name](hinted, {"cache-control": "max-age=5"})
    assert refused.status_code == 400
    assert b"more than once" in bytes(refused.body)
    assert len(pipe.governed_kw) == 1
