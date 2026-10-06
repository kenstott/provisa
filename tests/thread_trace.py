# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1d4f73-9e2a-4c58-a7d1-3f5e8b9c0d42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Test-only thread tracer for a real server process (REQ-1882).

Installed by ``tests.integration.thread_trace_app`` BEFORE the app is imported. It changes no
behaviour: every wrapper calls straight through. It records, as JSON lines in
``$PROVISA_TEST_THREAD_TRACE``:

- ``entry``: a transport handed a connection / RPC / request to a thread (the request's own
  thread). The wrapper also turns on ``sys.setprofile`` for that thread.
- ``stage``: a pipeline function was called (or, for a coroutine/generator, resumed) on a profiled
  thread — governance, routing, engine/driver execution, result encoding and send.
- ``hop``: work was handed to ANOTHER thread or loop from a profiled thread —
  ``ThreadPoolExecutor.submit`` (a real pool, never the inline executor), ``Thread.start``,
  ``asyncio.run_coroutine_threadsafe``, ``anyio.to_thread.run_sync``. The receiving thread is
  profiled too and inherits the request tag, so stages run there are recorded against the request.

Every record carries the OS thread ident and name, and the request tag when one is known: a
``91NNNNN`` number the test embeds in its query, picked out of the stage's arguments.
"""

# Requirements: REQ-1882

from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
from typing import Any

_TAG = re.compile(r"91\d{5}")
_REPO_MARK = os.sep + "provisa" + os.sep

# (path suffix, function name) -> stage label.
_STAGES: dict[tuple[str, str], str] = {
    # transport entry / dispatch
    ("provisa/core/request_thread.py", "_request"): "transport:http.asgi-app",
    ("provisa/api/data/endpoint.py", "graphql_endpoint"): "transport:http.graphql",
    ("provisa/api/data/endpoint_dev.py", "sql_endpoint"): "transport:http.sql",
    ("provisa/api/rest/cypher_router.py", "cypher_query"): "transport:http.cypher",
    ("provisa/api/data/subscribe.py", "subscribe"): "transport:http.subscribe",
    ("provisa/api/data/subscribe.py", "wrapped_generator"): "send:sse.generator",
    ("provisa/api/data/subscribe.py", "_provider_sse_generator"): "send:sse.provider",
    ("provisa/api/data/subscribe.py", "_sse_generator"): "send:sse.pg",
    ("provisa/api/mcp/tools.py", "run_sql"): "transport:mcp.run_sql",
    ("provisa/pgwire/server.py", "_execute_sql_bound"): "transport:pgwire.execute",
    ("provisa/pgwire/server.py", "describe_sql"): "transport:pgwire.describe",
    ("provisa/bolt/server.py", "_dispatch"): "transport:bolt.dispatch",
    ("provisa/bolt/session.py", "handle_run"): "transport:bolt.run",
    ("provisa/bolt/session.py", "_execute_cypher"): "execute:bolt.cypher",
    ("provisa/api/flight/server.py", "_do_get_inner"): "transport:flight.do_get",
    ("provisa/api/flight/server.py", "_do_get_sql_governed"): "transport:flight.sql",
    ("provisa/api/flight/server.py", "_do_get_graphql"): "transport:flight.graphql",
    ("provisa/api/flight/server.py", "_do_get_cypher"): "transport:flight.cypher",
    ("provisa/grpc/server.py", "_handle_query_bound"): "transport:grpc.query",
    ("provisa/api/rest/generator.py", "rest_table_endpoint"): "transport:http.rest",
    ("provisa/api/jsonapi/generator.py", "_jsonapi_table_endpoint"): "transport:http.jsonapi",
    ("provisa/api/airport/query.py", "governed_table_scan_stream"): "transport:airport.scan",
    ("provisa/api/airport/query.py", "_plan_for_scan"): "govern:airport.plan",
    ("provisa/api/airport/query.py", "_typed_batches_from_rows"): "send:airport.batches",
    # governance
    ("provisa/pgwire/_pipeline.py", "_govern_and_route"): "govern:raw",
    ("provisa/pgwire/_pipeline.py", "_govern_and_route_planned"): "govern:raw.planned",
    ("provisa/pgwire/_pipeline.py", "_govern_and_route_compiled"): "govern:compiled",
    (
        "provisa/pgwire/_pipeline.py",
        "_govern_and_route_compiled_planned",
    ): "govern:compiled.planned",
    ("provisa/pgwire/_pipeline.py", "govern_pgwire_plan"): "govern:pgwire",
    # REQ-589: on the extended protocol a statement is governed once, in its Describe; the Execute
    # only routes the governed statement with the Bind's values.
    ("provisa/pgwire/_pipeline.py", "describe_pgwire_statement"): "govern:pgwire.describe",
    ("provisa/pgwire/_pipeline.py", "govern_statement"): "govern:statement",
    ("provisa/pgwire/_pipeline.py", "plan_pgwire_statement"): "route:pgwire.plan_statement",
    ("provisa/pgwire/_pipeline.py", "govern_batch_final_plan_with_fn"): "govern:batch",
    ("provisa/api/data/endpoint.py", "_handle_query"): "govern:graphql",
    ("provisa/api/data/endpoint.py", "_execute_one_field"): "govern:graphql.field",
    ("provisa/compiler/sql_validator.py", "validate_sql"): "govern:validate_sql",
    # routing
    ("provisa/transpiler/router.py", "decide_route"): "route:decide_route",
    ("provisa/pgwire/_pipeline.py", "_optimize_and_route"): "route:optimize_and_route",
    # execution
    ("provisa/pgwire/_pipeline.py", "_execute_plan_in_org"): "execute:plan",
    ("provisa/pgwire/_pipeline.py", "_run_plan_terminal"): "execute:terminal",
    ("provisa/federation/query_residency.py", "ensure_resident"): "execute:ensure_resident",
    ("provisa/federation/duckdb_runtime.py", "run"): "execute:duckdb.run",
    ("provisa/federation/duckdb_runtime.py", "_run"): "execute:duckdb._run",
    ("provisa/federation/duckdb_runtime.py", "run_sync"): "execute:duckdb.run_sync",
    ("provisa/federation/duckdb_runtime.py", "run_arrow"): "execute:duckdb.run_arrow",
    ("provisa/federation/duckdb_runtime.py", "run_arrow_stream"): "execute:duckdb.run_arrow_stream",
    ("provisa/federation/duckdb_runtime.py", "_open_cursor"): "execute:duckdb.cursor",
    ("provisa/federation/duckdb_runtime.py", "land_table"): "land:duckdb.land_table",
    ("provisa/federation/materialize_broker.py", "land"): "land:store_broker.land",
    ("provisa/federation/materialize_broker.py", "_with_store"): "land:store_broker.store_io",
    ("provisa/federation/runtime.py", "execute_engine"): "execute:engine",
    ("provisa/federation/runtime.py", "execute_engine_sync"): "execute:engine_sync",
    ("provisa/federation/runtime.py", "execute_engine_stream"): "execute:engine_stream",
    ("provisa/federation/runtime.py", "execute_native"): "execute:native",
    ("provisa/federation/runtime.py", "execute_native_stream"): "execute:native_stream",
    ("provisa/executor/drivers/postgresql.py", "execute"): "execute:pg_source.execute",
    ("provisa/executor/drivers/postgresql.py", "_open"): "execute:pg_source.stream_open",
    ("provisa/executor/drivers/postgresql.py", "fetch"): "execute:pg_source.fetch",
    ("provisa/pgwire/pg_passthrough.py", "open_passthrough"): "execute:pg_passthrough.open",
    ("provisa/pgwire/pg_passthrough.py", "fetch"): "send:pg_passthrough.fetch",
    ("provisa/executor/trino.py", "execute_trino"): "execute:trino",
    ("provisa/executor/trino_flight.py", "execute_trino_flight_stream"): "execute:trino.flight",
    ("provisa/executor/trino_flight.py", "execute_trino_flight_arrow"): "execute:trino.flight",
    # encode / send
    ("provisa/executor/serialize.py", "serialize_rows"): "encode:json.serialize_rows",
    ("provisa/pgwire/server.py", "rows"): "encode:pgwire.rows",
    ("buenavista/postgres.py", "send_row_description"): "send:pgwire.row_description",
    ("buenavista/postgres.py", "send_data_rows"): "send:pgwire.data_rows",
    ("provisa/api/flight/server.py", "_metered_batches"): "send:flight.batches",
    ("provisa/api/flight/server.py", "_report_table"): "send:flight.table",
    ("provisa/executor/formats/arrow.py", "rows_to_arrow_table"): "encode:flight.rows_to_arrow",
    ("provisa/api/flight/server.py", "_license_stream"): "send:flight.table_stream",
    ("provisa/core/rpc_loop.py", "__next__"): "send:flight.loop_held_batch",
    ("provisa/federation/duckdb_runtime.py", "_batches"): "send:flight.engine_batches",
    ("provisa/api/flight/server.py", "_license_stream_gen"): "send:flight.stream",
    ("provisa/grpc/server.py", "_dict_row_to_message"): "encode:grpc.message",
    ("provisa/grpc/server.py", "_meter_msg"): "send:grpc.message",
    ("provisa/bolt/session.py", "send_record"): "send:bolt.record",
    ("provisa/bolt/session.py", "handle_pull"): "send:bolt.pull",
    ("provisa/core/request_thread.py", "thread_send"): "send:http.asgi-send",
}

_out_fd: int | None = None
_stage_of: dict[Any, str | None] = {}
_tls = threading.local()


def _write(record: dict[str, Any]) -> None:
    assert _out_fd is not None
    record["t"] = time.time()
    record["ident"] = threading.get_ident()
    record["thread"] = threading.current_thread().name
    detached = getattr(_tls, "detached", None)
    if detached is not None:
        record["detached_from"] = detached
    os.write(_out_fd, (json.dumps(record) + "\n").encode())  # O_APPEND: one atomic line


def _tag_in(value: Any, depth: int = 0) -> str | None:
    if isinstance(value, str):
        m = _TAG.search(value)
        return m.group(0) if m else None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        m = _TAG.fullmatch(str(value))
        return m.group(0) if m else None
    if isinstance(value, bytes):
        m = _TAG.search(value[:4096].decode("latin-1"))
        return m.group(0) if m else None
    if depth >= 2:
        return None
    if isinstance(value, dict):
        for v in list(value.values())[:32]:
            found = _tag_in(v, depth + 1)
            if found:
                return found
        return None
    if isinstance(value, (list, tuple)):
        for v in value[:32]:
            found = _tag_in(v, depth + 1)
            if found:
                return found
        return None
    for attr in ("sql", "exec_params", "params", "query", "original_sql"):
        try:
            inner = value.__dict__.get(attr)
        except AttributeError:
            return None
        if inner is not None:
            found = _tag_in(inner, depth + 1)
            if found:
                return found
    return None


def _frame_tag(frame: Any) -> str | None:
    code = frame.f_code
    names = code.co_varnames[: code.co_argcount + code.co_kwonlyargcount + 2]
    local = frame.f_locals
    for name in names:
        if name in local:
            found = _tag_in(local[name])
            if found:
                return found
    return None


def _profile(frame: Any, event: str, arg: Any) -> None:
    del arg
    if event != "call":
        return
    code = frame.f_code
    try:
        stage = _stage_of[code]
    except KeyError:
        path = code.co_filename.replace(os.sep, "/")
        stage = None
        for (suffix, name), label in _STAGES.items():
            if code.co_name == name and path.endswith(suffix):
                stage = label
                break
        _stage_of[code] = stage
    if stage is None:
        return
    tag = _frame_tag(frame)
    if tag is not None:
        _tls.tag = tag
    _write({"kind": "stage", "stage": stage, "tag": tag or getattr(_tls, "tag", None)})


def _enter(transport: str) -> None:
    """Mark this thread as a request's own thread and profile it."""
    _tls.tag = None
    sys.setprofile(_profile)
    _write({"kind": "entry", "transport": transport})


def _caller() -> str:
    """The nearest repo frames above the hop, innermost first."""
    frame = sys._getframe(2)
    seen: list[str] = []
    while frame is not None and len(seen) < 4:
        path = frame.f_code.co_filename
        if (_REPO_MARK in path or "/vendor/" in path) and "/tests/" not in path:
            rel = path[path.rfind(_REPO_MARK) + 1 :] if _REPO_MARK in path else path
            seen.append(f"{rel}:{frame.f_lineno}:{frame.f_code.co_name}")
        frame = frame.f_back
    return " <- ".join(seen)


def _profiled() -> bool:
    return sys.getprofile() is _profile


def _carry(fn: Any, tag: str | None, detached_from: str | None = None) -> Any:
    """Run ``fn`` on the receiving thread with profiling on and the sender's tag -- or, for work
    the request detached from itself, with no tag and a note of the request it came from. The
    receiving thread's own tag is restored afterwards: a pooled thread outlives the work."""

    def _inner(*args: Any, **kwargs: Any) -> Any:
        previous = sys.getprofile()
        prev_tag = getattr(_tls, "tag", None)
        prev_detached = getattr(_tls, "detached", None)
        _tls.tag = tag
        _tls.detached = detached_from
        sys.setprofile(_profile)
        try:
            return fn(*args, **kwargs)
        finally:
            sys.setprofile(previous)
            _tls.tag = prev_tag
            _tls.detached = prev_detached

    return _inner


# The one hop that detaches work from the request that started it (REQ-1882): the background
# worker pool (``provisa.core.connection_loop.spawn_background``). What runs there outlives the
# request and is not part of it, so it does not carry the request's tag.
_DETACHING_SITE = ("provisa/core/connection_loop.py", "_submit")


def _detaches(at: str) -> bool:
    innermost = at.split(" <- ")[0]
    path, _line, func = innermost.rsplit(":", 2)
    return (path, func) == _DETACHING_SITE


def install(path: str) -> None:
    """Open the trace file and wrap the entry points and the thread-hop primitives."""
    global _out_fd
    _out_fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)

    import asyncio
    import concurrent.futures

    # -- hops ---------------------------------------------------------------------------------
    real_submit = concurrent.futures.ThreadPoolExecutor.submit

    def submit(self, fn, /, *args, **kwargs):  # type: ignore[no-untyped-def]
        if _profiled():
            tag = getattr(_tls, "tag", None)
            at = _caller()
            _write(
                {
                    "kind": "hop",
                    "via": "ThreadPoolExecutor.submit",
                    "pool": getattr(self, "_thread_name_prefix", ""),
                    "tag": tag,
                    "at": at,
                }
            )
            fn = _carry(fn, None, detached_from=tag) if _detaches(at) else _carry(fn, tag)
        return real_submit(self, fn, *args, **kwargs)

    concurrent.futures.ThreadPoolExecutor.submit = submit  # type: ignore[method-assign]

    real_start = threading.Thread.start

    def start(self) -> None:  # type: ignore[no-untyped-def]
        if _profiled():
            tag = getattr(_tls, "tag", None)
            _write(
                {
                    "kind": "hop",
                    "via": "Thread.start",
                    "pool": self.name,
                    "tag": tag,
                    "at": _caller(),
                }
            )
            self.run = _carry(self.run, tag)  # type: ignore[method-assign]
        real_start(self)

    threading.Thread.start = start  # type: ignore[method-assign]

    real_rcts = asyncio.run_coroutine_threadsafe

    def run_coroutine_threadsafe(coro, loop):  # type: ignore[no-untyped-def]
        if _profiled():
            _write(
                {
                    "kind": "hop",
                    "via": "run_coroutine_threadsafe",
                    "pool": getattr(coro, "__qualname__", ""),
                    "tag": getattr(_tls, "tag", None),
                    "at": _caller(),
                }
            )
        return real_rcts(coro, loop)

    asyncio.run_coroutine_threadsafe = run_coroutine_threadsafe  # type: ignore[assignment]

    import anyio.to_thread

    real_run_sync = anyio.to_thread.run_sync

    async def run_sync(func, *args, **kwargs):  # type: ignore[no-untyped-def]
        if _profiled():
            tag = getattr(_tls, "tag", None)
            _write(
                {
                    "kind": "hop",
                    "via": "anyio.to_thread.run_sync",
                    "pool": getattr(func, "__qualname__", ""),
                    "tag": tag,
                    "at": _caller(),
                }
            )
            func = _carry(func, tag)
        return await real_run_sync(func, *args, **kwargs)

    anyio.to_thread.run_sync = run_sync  # type: ignore[assignment]

    # -- entries ------------------------------------------------------------------------------
    from provisa.core import request_thread

    real_serve = request_thread._serve

    def _serve(make_coro, ctx):  # type: ignore[no-untyped-def]
        _enter("http")
        return real_serve(make_coro, ctx)

    request_thread._serve = _serve  # type: ignore[assignment]

    from provisa.pgwire import server as pgwire_server

    real_handle = pgwire_server.ProvisaHandler.handle

    def handle(self) -> None:  # type: ignore[no-untyped-def]
        _enter("pgwire")
        real_handle(self)

    pgwire_server.ProvisaHandler.handle = handle  # type: ignore[method-assign]

    from provisa.bolt import server as bolt_server

    real_bolt = bolt_server._serve_connection

    def _serve_connection(sock, ssl_ctx):  # type: ignore[no-untyped-def]
        _enter("bolt")
        real_bolt(sock, ssl_ctx)

    bolt_server._serve_connection = _serve_connection  # type: ignore[assignment]

    from provisa.api.flight import server as flight_server

    for method in ("do_get", "do_action", "get_flight_info", "get_schema", "list_flights"):
        real = getattr(flight_server.ProvisaFlightServer, method)

        def _flight(self, *args, _real=real, _name=method, **kwargs):  # type: ignore[no-untyped-def]
            _enter(f"flight.{_name}")
            return _real(self, *args, **kwargs)

        setattr(flight_server.ProvisaFlightServer, method, _flight)

    from provisa.api.airport import server as airport_server

    for method in ("do_get", "do_exchange", "do_action", "get_flight_info"):
        real = getattr(airport_server.ProvisaAirportServer, method)

        def _airport(self, *args, _real=real, _name=method, **kwargs):  # type: ignore[no-untyped-def]
            _enter(f"airport.{_name}")
            return _real(self, *args, **kwargs)

        setattr(airport_server.ProvisaAirportServer, method, _airport)

    # pyarrow drains a GeneratorStream AFTER do_get returns, from the gRPC handler thread, in a
    # fresh Python thread state (no profile, no thread-locals survive), so each pull is recorded
    # here — and re-profiled, with the tag the stream was built under.
    import pyarrow.flight as pa_flight

    real_generator_stream = pa_flight.GeneratorStream

    def _traced_batches(batches, tag):  # type: ignore[no-untyped-def]
        iterator = iter(batches)
        try:
            while True:
                _tls.tag = tag
                sys.setprofile(_profile)
                _write({"kind": "stage", "stage": "send:flight.stream_pull", "tag": tag})
                try:
                    item = next(iterator)
                except StopIteration:
                    return
                yield item
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()

    def generator_stream(schema, generator, *args, **kwargs):  # type: ignore[no-untyped-def]
        tag = getattr(_tls, "tag", None)
        return real_generator_stream(schema, _traced_batches(generator, tag), *args, **kwargs)

    pa_flight.GeneratorStream = generator_stream  # type: ignore[misc,assignment]

    from provisa.grpc import server as grpc_server

    for factory in ("_unary", "_streaming"):
        real_factory = getattr(grpc_server, factory)

        def _factory(body, _real=real_factory, _name=factory):  # type: ignore[no-untyped-def]
            handler = _real(body)

            def _handler(request, context):  # type: ignore[no-untyped-def]
                _enter(f"grpc.{_name}")
                return handler(request, context)

            return _handler

        setattr(grpc_server, factory, _factory)
