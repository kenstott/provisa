# Copyright (c) 2026 Kenneth Stott
# Canary: 6c2e8f17-3a9d-4b54-8e01-9d7a4c5b2f38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Native gRPC is answered from the response cache before routing (REQ-1897, REQ-544, REQ-1877).

The servicer runs over the real pipeline (the ``pipe`` stand-in state of
``test_cache_before_route``: a counting cache store, every route-stage function and every SQL
parse counted). An opted-in ``Query{Type}`` whose entry exists is served from the plan the
planner returns — no route work, no SQL parse, nothing at the source. The row limit is a bound
value like the filter values, so a request with another limit is the same compiled statement."""

# Requirements: REQ-1897, REQ-544, REQ-1877

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import grpc
import grpc.aio
import pytest

from provisa.grpc.server import ProvisaServicer
from tests.unit.test_cache_before_route import pipe  # noqa: F401  (the pipeline fixture)

pytestmark = pytest.mark.asyncio

_HINT = [("x-provisa-role", "analyst"), ("x-provisa-cache", "true")]
_NO_HINT = [("x-provisa-role", "analyst")]


class _Order:
    """Stands in for the generated ``Orders`` message class."""

    DESCRIPTOR = SimpleNamespace(
        fields=[SimpleNamespace(name="id")],
        fields_by_name={"id": None},  # no field descriptor: the row value is passed through
    )

    def __init__(self, **fields) -> None:
        self.fields = fields


def _servicer(p) -> ProvisaServicer:
    ctx = p.state.contexts["analyst"]
    ctx.aggregate_columns[1] = [("id", "integer")]
    p.state.source_pools.supports_stream = lambda _source_id: False  # the buffered DIRECT read
    p.state.multitenancy = False
    servicer = ProvisaServicer(p.state, SimpleNamespace(Orders=_Order), MagicMock())
    servicer._emit_trailing_metadata = MagicMock()
    servicer._meter_msg = lambda msg: msg
    return servicer


def _request(limit: int, order_id: int | None = None):
    set_fields = {} if order_id is None else {"id": order_id}
    filter_msg = SimpleNamespace(
        DESCRIPTOR=SimpleNamespace(fields=[SimpleNamespace(name="id")]),
        HasField=lambda name: name in set_fields,
        **set_fields,
    )
    return SimpleNamespace(
        limit=limit,
        read_mask=SimpleNamespace(paths=[]),
        filter=filter_msg,
        HasField=lambda name: name == "filter" and bool(set_fields),
    )


async def _query(servicer, request, metadata) -> list[dict]:
    context = AsyncMock(spec=grpc.aio.ServicerContext)
    context.invocation_metadata = MagicMock(return_value=metadata)
    rows = [m.fields async for m in servicer._handle_query(request, context, "Orders", "orders")]
    context.abort.assert_not_awaited()
    return rows


async def test_a_hinted_hit_does_no_route_work_and_reaches_no_source(pipe):  # noqa: F811
    servicer = _servicer(pipe)
    source = pipe.state.federation_engine

    first = await _query(servicer, _request(5, order_id=7), _HINT)
    assert first == [{"id": 7}]
    assert pipe.counts["route"] == 1 and len(source.statements) == 1
    before = dict(pipe.counts)

    for _ in range(3):
        assert await _query(servicer, _request(5, order_id=7), _HINT) == first
    assert pipe.counts == before, "a cache hit routed, looked up API tables or parsed SQL"
    assert len(source.statements) == 1, "a cache hit reached the source"
    assert [a["route"] for a in pipe.audits] == ["direct", "cache", "cache", "cache"]
    assert [a["row_count"] for a in pipe.audits] == [1, 1, 1, 1]


async def test_a_hit_is_one_cache_read(pipe):  # noqa: F811
    servicer = _servicer(pipe)
    await _query(servicer, _request(5, order_id=7), _HINT)  # MISS: one read, then the write
    gets = pipe.state.response_cache_store.gets
    assert gets == 1

    await _query(servicer, _request(5, order_id=7), _HINT)
    assert pipe.state.response_cache_store.gets == gets + 1


async def test_an_unhinted_request_reads_no_cache_and_runs_at_the_source(pipe):  # noqa: F811
    servicer = _servicer(pipe)
    for _ in range(2):
        assert await _query(servicer, _request(5, order_id=7), _NO_HINT) == [{"id": 7}]
    assert pipe.state.response_cache_store.gets == 0
    assert len(pipe.state.federation_engine.statements) == 2
    assert [a["route"] for a in pipe.audits] == ["direct", "direct"]


async def test_another_bound_value_is_a_miss_but_not_another_statement(pipe):  # noqa: F811
    """The filter value and the row limit are bound: other values miss the cache (its key holds
    them) and run at the source, but the statement was compiled once — no parse, no route."""
    servicer = _servicer(pipe)
    source = pipe.state.federation_engine
    await _query(servicer, _request(5, order_id=7), _HINT)
    before = dict(pipe.counts)

    assert await _query(servicer, _request(5, order_id=8), _HINT) == [{"id": 8}]
    assert await _query(servicer, _request(6, order_id=7), _HINT) == [{"id": 7}]

    # (The API-table lookup is the planner's per-request check on every routed request.)
    assert (pipe.counts["parses"], pipe.counts["route"]) == (before["parses"], before["route"]), (
        "another bound value parsed SQL or routed again"
    )
    assert len({sql for _source_id, sql, _params in source.statements}) == 1
    assert [params for _source_id, _sql, params in source.statements] == [[7, 5], [8, 5], [7, 6]]


# -- the HTTP gRPC proxy: every transport takes the pre-route hit ------------------------------


class _ProxyRequest:
    """Stands in for the Starlette request ``grpc_proxy`` reads: a JSON body, headers, and the
    request state the auth layer writes the acting role to (none here: no middleware mounted)."""

    def __init__(self, body: dict, headers: dict[str, str]) -> None:
        self._body = body
        self.headers = headers
        self.state = SimpleNamespace()

    async def json(self) -> dict:
        return self._body


async def _proxy(p, body: dict, *, hinted: bool) -> list[dict]:
    import json

    from provisa.api.data.endpoint_grpc_proxy import grpc_proxy

    p.state.contexts["analyst"].aggregate_columns[1] = [("id", "integer")]
    p.state.schemas = {"analyst": object()}  # the proxy only checks the role has a schema
    headers = {"x-provisa-role": "analyst"}
    if hinted:
        headers["x-provisa-cache"] = "true"
    response = await grpc_proxy("Orders", _ProxyRequest(body, headers))  # type: ignore[arg-type]
    return json.loads(response.body)


async def test_a_hinted_proxy_hit_does_no_route_work_and_reaches_no_source(pipe):  # noqa: F811
    source = pipe.state.federation_engine
    body = {"limit": 5, "filter": {"id": 7}}

    first = await _proxy(pipe, body, hinted=True)
    assert first == [{"id": 7}]
    assert pipe.counts["route"] == 1 and len(source.statements) == 1
    before = dict(pipe.counts)

    for _ in range(3):
        assert await _proxy(pipe, body, hinted=True) == first
    assert pipe.counts == before, "a proxy cache hit routed, looked up API tables or parsed SQL"
    assert len(source.statements) == 1, "a proxy cache hit reached the source"
    assert [a["route"] for a in pipe.audits] == ["direct", "cache", "cache", "cache"]


async def test_a_proxy_hit_is_one_cache_read_and_native_shares_the_entry(pipe):  # noqa: F811
    """One read per request, and one entry for one governed statement whichever surface sent it:
    the proxy's MISS writes the entry the native RPC's request is then answered from."""
    await _proxy(pipe, {"limit": 5, "filter": {"id": 7}}, hinted=True)
    gets = pipe.state.response_cache_store.gets
    assert gets == 1

    await _proxy(pipe, {"limit": 5, "filter": {"id": 7}}, hinted=True)
    assert pipe.state.response_cache_store.gets == gets + 1

    native = await _query(_servicer(pipe), _request(5, order_id=7), _HINT)
    assert native == [{"id": 7}]
    assert len(pipe.state.federation_engine.statements) == 1
    assert pipe.audits[-1]["route"] == "cache"


async def test_an_unhinted_proxy_request_reads_no_cache(pipe):  # noqa: F811
    for _ in range(2):
        assert await _proxy(pipe, {"limit": 5, "filter": {"id": 7}}, hinted=False) == [{"id": 7}]
    assert pipe.state.response_cache_store.gets == 0
    assert len(pipe.state.federation_engine.statements) == 2
