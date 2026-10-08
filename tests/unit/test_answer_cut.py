# Copyright (c) 2026 Kenneth Stott
# Canary: fc2e383a-030d-403d-88e4-411308b63a78
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1350: an API answer cut at the endpoint's max_pages says so, and is never cached as the
whole answer.

The answer carries an ``api.answer_cut`` warning (code, params, English text) collected per
statement and reported in each surface's own channel — here GraphQL's ``extensions.warnings``
and pgwire's NoticeResponse. A cut answer lands under a name of its own, so the next identical
request calls the remote again; a cut fill has no fetch time, so it is fetched again too."""

from __future__ import annotations

import io
import json
import struct
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
import respx

from provisa.api_source.caller import answer_rows, call_api
from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint
from provisa.core.paging import PaginationConfig
from provisa.core.statement_warnings import (
    NoWarningChannel,
    ServerWarning,
    collecting,
    header_value,
    warn,
)

BASE = "http://api.test"


def _endpoint(pagination=None, **kw) -> ApiEndpoint:
    base = dict(
        source_id="api",
        path="/pets",
        table_name="pets",
        columns=[
            ApiColumn(name="id", type=ApiColumnType.integer),
            ApiColumn(name="name", type=ApiColumnType.string),
        ],
        pagination=pagination,
    )
    base.update(kw)
    return ApiEndpoint(**base)


def _pets(n: int, start: int = 0) -> list[dict]:
    return [{"id": i, "name": f"n{i}"} for i in range(start, start + n)]


def _offset(max_pages=2, page_size=2) -> PaginationConfig:
    return PaginationConfig(type="offset", page_size=page_size, max_pages=max_pages)


# -- the carrier ---------------------------------------------------------------------------------


def test_a_warning_with_no_collector_open_fails_instead_of_vanishing():
    w = ServerWarning(code="api.answer_cut", message="cut", params={"table": "t"})
    with pytest.raises(NoWarningChannel, match="cut"):
        warn(w)
    with collecting() as found:
        warn(w)
        with collecting() as inner:  # one statement, one list
            warn(w)
            assert inner is found
    assert found == [w]


def test_the_header_value_is_ascii_whatever_the_message_holds():
    """A header channel admits latin-1 or ASCII only; a non-ASCII message must still give a
    valid header (today's release-critical defect was a non-ASCII notice in an HTTP header)."""
    w = ServerWarning(code="api.answer_cut", message="coupé — 東京", params={"table": "café"})
    value = header_value([w])
    assert value.isascii()
    value.encode("latin-1")  # what an HTTP server encodes a header value with
    assert json.loads(value) == [w.as_dict()]


# -- detection -----------------------------------------------------------------------------------


@respx.mock
async def test_an_answer_that_stops_at_max_pages_with_a_full_last_page_is_cut():
    respx.get(f"{BASE}/pets").mock(
        side_effect=[httpx.Response(200, json=_pets(2, 2 * i)) for i in range(3)]
    )
    rows, cut = answer_rows(_endpoint(_offset()), await call_api(_endpoint(_offset()), {}, BASE))
    assert len(rows) == 4 and (cut.max_pages, cut.rows) == (2, 4)


@respx.mock
async def test_an_answer_that_ends_within_max_pages_is_whole():
    respx.get(f"{BASE}/pets").mock(
        side_effect=[httpx.Response(200, json=_pets(2)), httpx.Response(200, json=_pets(1, 2))]
    )
    endpoint = _endpoint(_offset())
    assert answer_rows(endpoint, await call_api(endpoint, {}, BASE)) == (_pets(3), None)


@respx.mock
async def test_cursor_and_link_paging_say_from_the_answer_whether_there_is_more():
    respx.get(f"{BASE}/pets").mock(
        side_effect=[
            httpx.Response(200, json={"items": _pets(1), "next_cursor": "a"}),
            httpx.Response(200, json={"items": _pets(1, 1), "next_cursor": "b"}),
        ]
    )
    cursor = _endpoint(
        PaginationConfig(type="cursor", page_size=1, max_pages=2), response_root="items"
    )
    assert answer_rows(cursor, await call_api(cursor, {}, BASE))[1] is not None

    respx.get(f"{BASE}/pets").mock(
        side_effect=[
            httpx.Response(200, json=_pets(1), headers={"link": f'<{BASE}/pets?p=2>; rel="next"'}),
            httpx.Response(200, json=_pets(1, 1)),
        ]
    )
    link = _endpoint(PaginationConfig(type="link_header", page_size=1, max_pages=2))
    assert answer_rows(link, await call_api(link, {}, BASE))[1] is None  # no next link


# -- the request path: warned, never cached as the call's answer ---------------------------------


@respx.mock
async def test_a_cut_request_warns_and_lands_under_a_name_no_later_request_finds():
    from provisa.api_source import router_integration
    from provisa.api_source.engine_cache import CacheLocation, cache_table_name

    route = respx.get(f"{BASE}/pets").mock(
        side_effect=lambda request: httpx.Response(200, json=_pets(2))
    )
    landed: list[str] = []

    async def land(engine, loc, tbl, rows, columns):
        landed.append(tbl)

    @contextmanager
    def isolated_sync():
        yield None

    engine = SimpleNamespace(isolated_sync=isolated_sync)
    loc = CacheLocation("c", "s", "relational")
    source = SimpleNamespace(base_url=BASE, auth=None, headers={})
    endpoint = _endpoint(_offset(max_pages=1))
    with (
        patch.object(
            router_integration, "table_exists", lambda conn, loc, t, ttl=None: t in landed
        ),
        patch.object(router_integration, "land_api_cache", land),
        patch.object(router_integration, "schedule_drop", lambda *a, **k: None),
        patch.object(router_integration, "cache_table_name", cache_table_name),
        patch("provisa.api_source.engine_cache._scope", lambda: "scope"),
    ):
        with collecting() as found:
            first = await router_integration.handle_api_query(
                endpoint, {}, engine, source=source, loc=loc
            )
        assert first.cut is not None and first.from_cache is False
        assert [w.code for w in found] == ["api.answer_cut"]
        assert found[0].params == {"table": "pets", "max_pages": 1, "rows": 2}
        assert first.cache_table != cache_table_name("api", "pets", {})
        with collecting():
            second = await router_integration.handle_api_query(
                endpoint, {}, engine, source=source, loc=loc
            )
    assert second.from_cache is False and route.call_count == 2  # called again, not a hit
    assert len(set(landed)) == 2


# -- a cut fill is fetched again ------------------------------------------------------------------


@respx.mock
async def test_a_cut_fill_warns_has_no_fetch_time_and_is_fetched_again():
    import duckdb

    from provisa.api_source import engine_cache, fill_cache
    from provisa.executor.session import EngineSession

    fill_cache._mem_fresh.clear()
    fill_cache._shapes.clear()
    engine_cache._SCHEMA_EXISTS_CACHE.clear()
    con = duckdb.connect()

    @contextmanager
    def isolated_sync():
        yield EngineSession(con, dialect="duckdb", placeholder="?")

    state = SimpleNamespace(
        org_id="o1",
        federation_engine=SimpleNamespace(
            cache_catalog=lambda: "memory", isolated_sync=isolated_sync
        ),
        source_catalogs={},
    )
    route = respx.get(f"{BASE}/pets").mock(
        side_effect=lambda request: httpx.Response(200, json=_pets(2))
    )
    endpoint = _endpoint(_offset(max_pages=1))
    source = SimpleNamespace(base_url=BASE, auth=None, headers={})
    with collecting() as found:
        assert await fill_cache.fill(state, endpoint, source, [{}], ttl=300) == 2
    assert [w.code for w in found] == ["api.answer_cut"]
    table = fill_cache.fill_table(state, endpoint, source)
    assert con.execute(
        f'SELECT COUNT(*) FROM "{table.loc.schema}"."{table.name}" WHERE "_cached_at" IS NULL'
    ).fetchone() == (2,)
    with collecting():
        await fill_cache.fill(state, endpoint, source, [{}], ttl=300)
    assert route.call_count == 2  # never fresh: the next request calls again
    con.close()


# -- the surfaces --------------------------------------------------------------------------------


def _cut_warning() -> ServerWarning:
    return ServerWarning(
        code="api.answer_cut",
        message="the answer for pets was cut at max_pages=1 (2 rows)",
        params={"table": "pets", "max_pages": 1, "rows": 2},
    )


def test_graphql_puts_the_warnings_in_extensions_beside_the_serializers_own():
    from fastapi.responses import JSONResponse
    from starlette.responses import Response

    from provisa.api.data.endpoint_helpers import _inject_warnings_into_response

    body = {"data": {"pets": []}, "extensions": {"warnings": [{"message": "many-to-one"}]}}
    out = _inject_warnings_into_response(body, [_cut_warning()])
    assert out["extensions"]["warnings"] == [{"message": "many-to-one"}, _cut_warning().as_dict()]

    json_out = _inject_warnings_into_response(JSONResponse({"data": {}}), [_cut_warning()])
    assert json.loads(bytes(json_out.body))["extensions"]["warnings"] == [_cut_warning().as_dict()]

    # A file format carries them in the header every HTTP response gets (the middleware).
    other = Response(b"PAR1", media_type="x")
    assert _inject_warnings_into_response(other, [_cut_warning()]) is other
    assert "x-provisa-warnings" not in other.headers


def test_pgwire_sends_each_warning_as_a_notice_ahead_of_the_rows_once():
    from provisa.executor.result import QueryResult
    from provisa.pgwire.server import ProvisaHandler, ProvisaQueryResult

    handler = ProvisaHandler.__new__(ProvisaHandler)
    handler.wfile = io.BytesIO()
    plan = SimpleNamespace(warnings=[_cut_warning()], audit=None)
    with patch("provisa.pgwire._pipeline.audit_on_drain", lambda plan, it: it):
        result = ProvisaQueryResult(
            QueryResult(rows=[(1,)], column_names=["id"], column_types=["integer"]),
            "SELECT id FROM pets",
            plan=plan,
        )
    handler.send_data_rows(result)
    data = handler.wfile.getvalue()
    assert data[:1] == b"N"  # the notice first
    length = struct.unpack("!i", data[1:5])[0]
    notice, rows = data[: 1 + length], data[1 + length :]
    assert b"01000" in notice and b"was cut at max_pages=1" in notice
    assert b'"code": "api.answer_cut"' in notice
    assert rows[:1] == b"D"
    handler.wfile = io.BytesIO()
    handler.send_data_rows(result)  # a later Execute of the same portal
    assert b"01000" not in handler.wfile.getvalue()


def test_a_warned_plan_is_never_stored_in_the_response_cache():
    from provisa.pgwire._pipeline import _cache_tee

    plan = SimpleNamespace(warnings=[_cut_warning()])
    assert _cache_tee(plan, SimpleNamespace(), None, None) is None  # type: ignore[arg-type]


async def test_an_event_loop_land_that_is_cut_fails_by_name():
    from provisa.api_source.replica_read import PageLimitReached
    from provisa.events.source_loader import _whole

    cut = SimpleNamespace(max_pages=3, rows=300)
    with pytest.raises(PageLimitReached, match="max_pages=3"):
        _whole(SimpleNamespace(id="api"), SimpleNamespace(table_name="pets"), [], cut)
    assert _whole(SimpleNamespace(id="api"), SimpleNamespace(table_name="pets"), [1], None) == [1]
