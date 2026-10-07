# Copyright (c) 2026 Kenneth Stott
# Canary: cc45354e-a8fa-44ae-9e65-22ffe7b0d5b8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1350: what a statement's answer says about itself (an API answer cut at max_pages)
reaches the caller on every surface, in that surface's own channel:

- every HTTP response: the ``X-Provisa-Warnings`` header, ASCII-escaped JSON;
- Flight SQL and Airport: the ``app_metadata`` of a zero-row batch ahead of the rows (a Flight
  header is sent before do_get runs), present for an empty result too;
- gRPC: the ``x-provisa-warnings`` trailing metadata, in the one call that also carries the
  license notice;
- Bolt: the RUN SUCCESS ``notifications``;
- MCP: the tool result's ``warnings``.
(GraphQL and pgwire: tests/unit/test_answer_cut.py.)"""

from __future__ import annotations

import contextlib

import json
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.flight as fl
import pytest

from provisa.core.statement_warnings import ServerWarning, warn

# What a governed plan's statement carries to the engine (REQ-1760); these tests call the Cypher
# helpers below the pipeline, so they hand one in themselves.
from provisa.federation.execution_auth import system_auth  # noqa: E402

_AUTH = system_auth("test: a governed plan's statement")


def _cut(message: str = "the answer for pets was cut at max_pages=1 (2 rows)") -> ServerWarning:
    return ServerWarning(
        code="api.answer_cut", message=message, params={"table": "pets", "max_pages": 1, "rows": 2}
    )


# -- HTTP ------------------------------------------------------------------------------------------


@contextlib.contextmanager
def _bound_to_the_requests_org():
    """A Cypher statement runs inside a request, with its org bound (REQ-1266): here the
    deployment's own org, whose runtime the app state holds."""
    import provisa.api.app as appmod
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(appmod.state.org_id)
    try:
        yield
    finally:
        reset_current_org(token)


async def test_every_http_response_carries_the_requests_warnings_as_an_ascii_header():
    from provisa.api.middleware.response_headers import ResponseHeadersMiddleware

    async def app(scope, receive, send):
        warn(_cut("coupé — 東京"))  # a statement deep in the request finds something to say
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"{}"})

    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    middleware = ResponseHeadersMiddleware(app, state=SimpleNamespace(schema_version=1))
    with patch("provisa.licensing.emit.should_nag", lambda: False):
        await middleware({"type": "http", "path": "/data/sql"}, None, send)
    headers = dict(sent[0]["headers"])
    value = headers[b"x-provisa-warnings"]
    assert value.decode("ascii")  # ASCII, so the response it rides on is valid
    assert json.loads(value) == [_cut("coupé — 東京").as_dict()]


async def test_a_request_with_nothing_to_say_gets_no_warnings_header():
    from provisa.api.middleware.response_headers import ResponseHeadersMiddleware

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})

    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    with patch("provisa.licensing.emit.should_nag", lambda: False):
        await ResponseHeadersMiddleware(app, state=SimpleNamespace(schema_version=1))(
            {"type": "http", "path": "/data/sql"}, None, send
        )
    assert b"x-provisa-warnings" not in dict(sent[0]["headers"])


# -- Flight SQL and Airport ------------------------------------------------------------------------


def _serve(stream_for):
    class _Server(fl.FlightServerBase):
        def do_get(self, context, ticket):
            return stream_for(ticket.ticket)

    return _Server("grpc://127.0.0.1:0")


@pytest.mark.parametrize("rows", [[1, 2], []])
def test_a_flight_stream_carries_the_warnings_ahead_of_the_rows_even_when_empty(rows):
    from provisa.api.flight.compression import generator_stream, record_batch_stream

    table = pa.table({"id": pa.array(rows, pa.int64())})
    streams = {
        b"table": lambda: record_batch_stream(table, [_cut()]),
        b"gen": lambda: generator_stream(table.schema, table.to_batches(), [_cut()]),
    }
    server = _serve(lambda ticket: streams[ticket]())
    client = fl.connect(f"grpc://127.0.0.1:{server.port}")
    try:
        for ticket in streams:
            reader = client.do_get(fl.Ticket(ticket))
            first = reader.read_chunk()
            assert first.data.num_rows == 0
            assert json.loads(first.app_metadata.to_pybytes()) == {
                "provisa_warnings": [_cut().as_dict()]
            }
            assert reader.read_all().column("id").to_pylist() == rows
    finally:
        client.close()
        server.shutdown()


def test_a_flight_stream_with_nothing_to_say_is_the_plain_stream():
    from provisa.api.flight.compression import record_batch_stream

    assert isinstance(record_batch_stream(pa.table({"id": [1]})), fl.RecordBatchStream)


# -- gRPC ------------------------------------------------------------------------------------------


def test_grpc_sends_the_warnings_and_the_license_notice_in_one_trailing_metadata_call():
    from provisa.grpc.server import ProvisaServicer

    calls: list = []
    context = SimpleNamespace(peer=lambda: "ipv4:1.2.3.4:5", set_trailing_metadata=calls.append)
    servicer = object.__new__(ProvisaServicer)
    with patch("provisa.licensing.emit.nag_for_connection", lambda key: "trial expired"):
        servicer._emit_trailing_metadata(context, [_cut("coupé")])
    ((first, second),) = calls  # one call: a second would replace the first
    assert first == ("x-provisa-license-notice", "trial expired")
    assert second[0] == "x-provisa-warnings" and second[1].isascii()
    assert json.loads(second[1]) == [_cut("coupé").as_dict()]


# -- Bolt ------------------------------------------------------------------------------------------


async def test_bolt_sends_the_warnings_as_notifications_with_the_runs_success():
    from tests.unit.test_bolt_show_databases import _make_session

    session, _writer = _make_session(["analyst"])

    async def execute(*args, **kwargs):
        warn(_cut())
        return ["id"], [[1]], None

    successes: list[dict] = []
    with (
        patch.object(session, "_resolve_db", return_value=("analyst", False)),
        patch("provisa.bolt.session._execute_cypher", execute),
        patch.object(session, "send_success", lambda meta=None: successes.append(meta or {})),
    ):
        await session.handle_run(["MATCH (p:Pets) RETURN p.id AS id", {}, {}])
    session._end_request()  # the RUN's records are never pulled here; its request ends with the test
    (meta,) = successes
    (note,) = meta["notifications"]
    assert note["code"] == "Provisa.api.answer_cut" and note["severity"] == "WARNING"
    assert note["description"] == _cut().message


# -- MCP -------------------------------------------------------------------------------------------


def test_an_mcp_tool_result_carries_the_warnings():
    from provisa.api.mcp.server import _with_warnings

    assert _with_warnings({"rows": []}, []) == {"rows": []}
    assert _with_warnings({"rows": []}, [_cut()]) == {"rows": [], "warnings": [_cut().as_dict()]}
    assert _with_warnings([1], [_cut()]) == {"result": [1], "warnings": [_cut().as_dict()]}


# -- SQL over HTTP ---------------------------------------------------------------------------------


def test_the_sql_endpoints_json_body_carries_the_warnings_in_extensions():
    from provisa.api.data.endpoint_dev import _with_warnings
    from provisa.core.statement_warnings import collecting

    with collecting():
        assert _with_warnings({"data": {"sql": []}}) == {"data": {"sql": []}}
        warn(_cut())
        assert _with_warnings({"data": {"sql": []}}) == {
            "data": {"sql": []},
            "extensions": {"warnings": [_cut().as_dict()]},
        }


# -- Cypher over HTTP: the cut answer's own table ----------------------------------------------------


async def test_a_cypher_statement_reads_a_cut_answer_from_its_own_table_and_never_promotes_it():
    """handle_api_query lands a cut answer under a name of its own; the Cypher path must read
    that table (not the whole answer's name, which holds nothing) and never promote its rows."""
    from contextlib import contextmanager

    from provisa.api.rest import cypher_exec
    from provisa.api_source import engine_cache, router_integration
    from provisa.api_source.caller import AnswerCut
    from provisa.api_source.engine_cache import CacheLocation
    from provisa.api_source.models import ApiColumn, ApiColumnType, ApiEndpoint

    endpoint = ApiEndpoint(
        source_id="api",
        path="/pets",
        table_name="pets",
        columns=[ApiColumn(name="id", type=ApiColumnType.integer)],
    )
    read: list[str] = []
    promoted: list = []

    @contextmanager
    def isolated_sync():
        yield None

    async def execute_engine(sql, params, **kw):
        read.append(sql)
        return SimpleNamespace(column_names=["id"], rows=[(1,)])

    async def handle(**kw):
        return router_integration.QueryResult(
            rows=[{"id": 1}], from_cache=False, cache_table="pets_cut_ab12", cut=AnswerCut(1, 1)
        )

    async def promote(table, rows):
        promoted.append(table)

    state = SimpleNamespace(
        api_endpoints={"pets": endpoint},
        api_sources={},
        source_catalogs={},
        source_cache={},
        response_cache_default_ttl=60,
        # ``pets`` is a registered table the statement reads (id 5), so the cut alone keeps its
        # rows from being promoted: a fetch with no arguments is otherwise promotable.
        tables=[{"id": 5, "table_name": "pets"}],
        hot_manager=SimpleNamespace(entries_for=lambda _ids: {}, maybe_promote_dicts=promote),
        federation_engine=SimpleNamespace(
            isolated_sync=isolated_sync,
            transpile_physical=lambda sql: sql,
            execute_engine=execute_engine,
        ),
    )
    with (
        patch.object(router_integration, "handle_api_query", lambda **kw: handle(**kw)),
        patch.object(engine_cache, "ensure_cache_schema", lambda conn, loc: None),
        patch.object(engine_cache, "table_exists", lambda conn, loc, t, ttl=None: False),
        patch.object(engine_cache, "schedule_drop", lambda *a, **k: None),
        patch.object(engine_cache, "org_cache_schema", lambda state: "s"),
        patch.object(
            engine_cache, "cache_location", lambda *a, **k: CacheLocation("c", "s", "relational")
        ),
        patch.object(engine_cache, "cache_table_name", lambda *a: "pets_whole"),
        _bound_to_the_requests_org(),
    ):
        rows = await cypher_exec._execute_with_api(
            "SELECT id FROM pets", [], {}, state, table_ids=[5], authorization=_AUTH
        )
    assert rows == [{"id": 1}]
    (sql,) = read
    assert '"pets_cut_ab12"' in sql and "pets_whole" not in sql
    assert promoted == []


async def test_a_cypher_statement_lands_a_remote_graphql_answer_cut_at_max_rows_as_its_own():
    """A remote GraphQL answer cut at max_rows lands under a name of this statement's own: the
    statement reads it, and the next identical statement does not find it as the answer."""
    from contextlib import contextmanager

    from provisa.api.rest import cypher_exec
    from provisa.api_source import engine_cache
    from provisa.graphql_remote import executor
    from provisa.graphql_remote.executor import RemoteAnswer

    created: list[str] = []
    read: list[str] = []

    @contextmanager
    def isolated_sync():
        yield None

    async def execute_engine(sql, params, **kw):
        read.append(sql)
        return SimpleNamespace(column_names=["id"], rows=[(1,)])

    async def remote(**kw):
        return RemoteAnswer([{"id": 1}], cut=True)

    state = SimpleNamespace(
        graphql_remote_sources={
            "gh": {
                "source_id": "gh",
                "url": "https://gh.test/graphql",
                "tables": [
                    {
                        "sql_name": "issues",
                        "name": "issues",
                        "field_name": "issues",
                        "rows_path": ["nodes"],
                        "columns": [{"name": "id", "type": "integer"}],
                    }
                ],
            }
        },
        config=SimpleNamespace(graphql_remote=SimpleNamespace(max_rows=1)),
        federation_engine=SimpleNamespace(
            cache_catalog=lambda: "c",
            isolated_sync=isolated_sync,
            transpile_physical=lambda sql: sql,
            execute_engine=execute_engine,
        ),
    )
    with (
        patch.object(executor, "execute_remote", remote),
        patch.object(engine_cache, "ensure_cache_schema", lambda conn, loc: None),
        patch.object(engine_cache, "table_exists", lambda conn, loc, t: t in created),
        patch.object(
            engine_cache, "create_and_insert", lambda conn, loc, t, rows, cols: created.append(t)
        ),
        patch.object(engine_cache, "schedule_drop", lambda *a, **k: None),
        patch.object(engine_cache, "org_cache_schema", lambda state, suffix=None: "s"),
        patch.object(engine_cache, "analyze_cache_table", lambda *a: None),
        patch.object(cypher_exec, "spawn_background", lambda coro: None),
        _bound_to_the_requests_org(),
    ):
        for _ in range(2):
            await cypher_exec._execute_with_gql_remote(
                "SELECT id FROM issues", [], {}, state, authorization=_AUTH
            )
    assert len(set(created)) == 2  # each cut statement landed its own; neither found the other's
    assert [f'"{name}"' in sql for name, sql in zip(created, read)] == [True, True]
