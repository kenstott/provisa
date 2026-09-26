# Copyright (c) 2026 Kenneth Stott
# Canary: 15df79b1-9951-42e3-85f9-ba05354e6784
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regression tests for a class of pgwire bugs a real client library found and mocked/sequential
protocol tests never caught:

  1. Describe(Portal)/Describe(Statement) without a following Execute leaked whatever source
     connection/cursor the eager describe-time execution opened (``close_portal`` never closed the
     cached result; ``describe_statement``'s result was never cached OR closed at all).
  2. asyncpg (a real, DIRECT-route PostgreSQL client) reports column type names lowercase
     (``"int4"``), but ``_sql_type_to_bvtype`` looked them up case-sensitively against
     upper-only dicts — silently mistyping every lowercase-reporting integer column as TEXT, which
     then crashed the BINARY-format encoder (``.encode()`` on a raw ``int``) the moment asyncpg
     requested binary for that column. psycopg2 (TEXT-format by default, ``str()``-tolerant) never
     exercised this path.
  3. ``DuckDBFederationRuntime.land_table``'s duckdb-native landing path ran a synchronous,
     potentially multi-second DB write directly on the asyncio event loop, freezing every OTHER
     concurrent query for the whole duration.
  4. The fix for (3) must not let concurrent lands for DIFFERENT tables pile onto the ONE shared
     DuckDB connection at once — a dedicated SINGLE-worker executor must serialize them.

(1) and (2) are driven over a REAL socket with real client protocol traffic (a hand-rolled raw
extended-query client for (1), asyncpg itself for (2)) — mocked/sequential protocol-handler tests
never exercised either path. (3)/(4) are narrower structural tests against
``DuckDBFederationRuntime`` directly, per the task's own guidance that a timing-based real-land
test would be brittle; an in-memory DuckDB materialize store is real (not mocked), only the slow
underlying write is stubbed to make interleaving observable.
"""

from __future__ import annotations

import asyncio
import socket
import struct
import threading
import time
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import asyncpg
import pytest

pytestmark = [pytest.mark.integration]

from provisa.executor.result import QueryResult as EngineResult  # noqa: E402
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime  # noqa: E402
from provisa.pgwire import _pipeline as _pl  # noqa: E402
from provisa.pgwire.server import ProvisaConnection, ProvisaServer  # noqa: E402
import provisa.pgwire.server as _srv  # noqa: E402


# ---------------------------------------------------------------------------
# Shared server plumbing (adapted from tests/integration/test_pgwire_integration.py
# and tests/integration/test_duckdb_attach_pgwire.py's fixture pattern)
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_mock_state(role: str = "admin", provider: str = "none") -> MagicMock:
    """Minimal AppState mock — a data structure populated from config, not a docker service."""
    from provisa.compiler.rls import RLSContext

    ctx = MagicMock()
    ctx.tables = {}
    ctx.joins = {}
    state = MagicMock()
    state.contexts = {role: ctx}
    state.rls_contexts = {role: RLSContext.empty()}
    state.roles = {role: {"id": role, "capabilities": [], "domain_access": ["*"]}}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = {"provider": provider, "default_role": role, "role_mapping": []}
    state.auth_middleware_active = provider != "none"
    state.multitenancy = False
    state.masking_rules = {}
    state.source_types = {}
    state.source_dialects = {}
    state.source_pools = MagicMock()
    state.server_limits = {}
    state.engine_conn = None
    return state


@contextmanager
def _running_server(state: MagicMock, govern_stub):
    """Start a real ProvisaServer on a free port with ``govern_pgwire_plan`` stubbed to
    ``govern_stub``, and the event loop wired the way ``_execute_sql_bound`` expects.

    Yields the port. A real socket accepts real client traffic; only the governance/plan seam is
    stubbed (as in test_pgwire_integration.py) since a live compiler/RLS/source stack is not
    available in unit-tier CI — the wire, auth, extended-query, and type-encoding paths under test
    are all real.
    """
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()

    port = _free_port()
    conn = ProvisaConnection()
    server = ProvisaServer(("127.0.0.1", port), conn)

    with (
        patch("provisa.api.app.state", state),
        patch.object(_srv, "state", state, create=True),
        patch.object(_pl, "govern_pgwire_plan", govern_stub),
    ):
        with _srv._loop_lock:
            previous_loop = _srv._loop
            _srv._loop = loop
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            raise RuntimeError(f"pgwire server did not accept connections on {port} within 30s")
        try:
            yield port
        finally:
            server.shutdown()
            with _srv._loop_lock:
                _srv._loop = previous_loop
            loop.call_soon_threadsafe(loop.stop)


# ---------------------------------------------------------------------------
# A minimal raw extended-query protocol client — needed because no existing client library
# (asyncpg included) issues a bare Describe(Portal)/Close without an Execute in between; this is
# exactly the "aborts early / fetch metadata only" client behavior the leak needs driven directly.
# ---------------------------------------------------------------------------


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("pgwire socket closed mid-message")
        buf += chunk
    return buf


def _read_message(sock: socket.socket) -> tuple[bytes, bytes]:
    mtype = _recv_exact(sock, 1)
    length = struct.unpack("!i", _recv_exact(sock, 4))[0]
    payload = _recv_exact(sock, length - 4) if length > 4 else b""
    return mtype, payload


def _connect_and_auth(port: int, user: str, password: str = "x", database: str = "provisa"):
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    sock.settimeout(10)
    body = f"user\x00{user}\x00database\x00{database}\x00\x00".encode()
    sock.sendall(struct.pack("!i", 4 + 4 + len(body)) + struct.pack("!i", 196608) + body)
    mtype, _ = _read_message(sock)
    assert mtype == b"R", f"expected AuthenticationRequest, got {mtype!r}"
    pw_body = password.encode() + b"\x00"
    sock.sendall(b"p" + struct.pack("!i", 4 + len(pw_body)) + pw_body)
    while True:
        mtype, payload = _read_message(sock)
        if mtype == b"E":
            raise AssertionError(f"pgwire auth error: {payload!r}")
        if mtype == b"Z":
            break
    return sock


def _send_parse(sock: socket.socket, stmt: str, sql: str) -> None:
    body = stmt.encode() + b"\x00" + sql.encode() + b"\x00" + struct.pack("!h", 0)
    sock.sendall(b"P" + struct.pack("!i", 4 + len(body)) + body)


def _send_bind(sock: socket.socket, portal: str, stmt: str) -> None:
    body = portal.encode() + b"\x00" + stmt.encode() + b"\x00" + struct.pack("!hhh", 0, 0, 0)
    sock.sendall(b"B" + struct.pack("!i", 4 + len(body)) + body)


def _send_describe(sock: socket.socket, kind: str, name: str) -> None:
    body = kind.encode() + name.encode() + b"\x00"
    sock.sendall(b"D" + struct.pack("!i", 4 + len(body)) + body)


def _send_close(sock: socket.socket, kind: str, name: str) -> None:
    body = kind.encode() + name.encode() + b"\x00"
    sock.sendall(b"C" + struct.pack("!i", 4 + len(body)) + body)


def _send_sync(sock: socket.socket) -> None:
    sock.sendall(b"S" + struct.pack("!i", 4))


def _drain_until_ready(sock: socket.socket) -> list[bytes]:
    types: list[bytes] = []
    while True:
        mtype, payload = _read_message(sock)
        types.append(mtype)
        if mtype == b"E":
            raise AssertionError(f"pgwire server error: {payload!r}")
        if mtype == b"Z":
            return types


# ---------------------------------------------------------------------------
# (1) Describe-without-Execute must not leak the eagerly-run result
# ---------------------------------------------------------------------------


class TestDescribeWithoutExecuteDoesNotLeak:
    def test_describe_portal_then_close_without_execute_closes_cached_result(self):
        """Parse + Bind + Describe(Portal) + Close(Portal) + Sync — Execute never sent.

        describe_portal() eagerly runs the query and caches the result under the portal name
        (so a later Execute can hand it straight back). A client that Describes for metadata
        only and then aborts — never Executing — must still have that cached result released
        when the portal is Closed; before the fix, close_portal() only dropped the ``portals``
        bookkeeping entry and left the cached QueryResult (and whatever source
        connection/cursor it opened) referenced forever.
        """
        state = _make_mock_state("admin", "none")
        close_calls: list[object] = []
        orig_close = _srv.ProvisaQueryResult.close

        def _tracking_close(self):
            close_calls.append(self)
            orig_close(self)

        async def _fake_govern(sql, role_id):
            del sql, role_id
            return EngineResult(rows=[(1,)], column_names=["v"])

        with (
            patch.object(_srv.ProvisaQueryResult, "close", _tracking_close),
            _running_server(state, _fake_govern) as port,
        ):
            sock = _connect_and_auth(port, "admin")
            try:
                _send_parse(sock, "s1", "SELECT 1 AS v")
                _send_bind(sock, "p1", "s1")
                _send_describe(sock, "P", "p1")
                _send_close(sock, "P", "p1")
                _send_sync(sock)
                _drain_until_ready(sock)
            finally:
                sock.close()

        assert close_calls, (
            "close_portal() must close a Describe'd-but-never-Executed portal's cached result "
            "(regression: the cached result/source connection was left open forever)"
        )

    def test_bare_describe_statement_with_no_bind_or_execute_closes_its_probe_result(self):
        """Parse + Describe(Statement) + Sync — no Bind, no Execute, no Close at all.

        This is asyncpg's own prepare() flow (Parse then Describe(Statement) to learn parameter
        types before Bind) and hits EVERY query over the DIRECT route — unlike the portal case,
        there is no Close message to hook at all: describe_statement() executes the query purely
        to inspect its shape and must close what it opened itself, synchronously, before ever
        returning to the client.
        """
        state = _make_mock_state("admin", "none")
        close_calls: list[object] = []
        orig_close = _srv.ProvisaQueryResult.close

        def _tracking_close(self):
            close_calls.append(self)
            orig_close(self)

        async def _fake_govern(sql, role_id):
            del sql, role_id
            return EngineResult(rows=[(2,)], column_names=["v"])

        with (
            patch.object(_srv.ProvisaQueryResult, "close", _tracking_close),
            _running_server(state, _fake_govern) as port,
        ):
            sock = _connect_and_auth(port, "admin")
            try:
                _send_parse(sock, "s1", "SELECT 2 AS v")
                _send_describe(sock, "S", "s1")
                _send_sync(sock)
                _drain_until_ready(sock)
            finally:
                sock.close()

        assert close_calls, (
            "handle_describe()'s Describe(Statement) branch must close the probe QueryResult it "
            "eagerly executed even though no Bind/Execute/Close ever followed (regression: "
            "asyncpg's prepare() flow leaked a source connection/cursor on EVERY query)"
        )


# ---------------------------------------------------------------------------
# (2) asyncpg binary-format round-trip on an integer column — the literal bug reproduction
# ---------------------------------------------------------------------------


class TestAsyncpgBinaryTypeRoundTrip:
    async def test_lowercase_int4_column_type_survives_asyncpg_binary_fetch(self):
        """asyncpg reports/consumes column types lowercase ("int4"); a case-sensitive lookup
        against the upper-only _TYPE_TO_BVTYPE/_INT_TYPES dicts mistyped this column as TEXT.
        asyncpg then requested BINARY wire format for it (its normal behavior for a type it
        recognizes), and the TEXT binary encoder crashed calling .encode("utf-8") on the raw int.
        psycopg2 never surfaced this — it defaults to TEXT format, whose converter is str().
        """
        state = _make_mock_state("admin", "none")

        async def _fake_govern(sql, role_id):
            del sql, role_id
            return EngineResult(rows=[(42,)], column_names=["n"], column_types=["int4"])

        with _running_server(state, _fake_govern) as port:
            conn = await asyncpg.connect(
                host="127.0.0.1", port=port, user="admin", password="x", database="provisa"
            )
            try:
                val = await conn.fetchval("SELECT n FROM widgets")
            finally:
                await conn.close()

        assert val == 42
        assert isinstance(val, int), f"expected a real int, got {type(val).__name__}: {val!r}"

    async def test_lowercase_int8_column_type_survives_asyncpg_binary_fetch(self):
        """Same class of bug, a second lowercase integer type name (bigint's wire name)."""
        state = _make_mock_state("admin", "none")

        async def _fake_govern(sql, role_id):
            del sql, role_id
            return EngineResult(rows=[(9_876_543_210,)], column_names=["n"], column_types=["int8"])

        with _running_server(state, _fake_govern) as port:
            conn = await asyncpg.connect(
                host="127.0.0.1", port=port, user="admin", password="x", database="provisa"
            )
            try:
                val = await conn.fetchval("SELECT n FROM widgets")
            finally:
                await conn.close()

        assert val == 9_876_543_210
        assert isinstance(val, int)


# ---------------------------------------------------------------------------
# (3) Background land dispatched off the event loop (structural — real in-memory DuckDB store,
#     only the slow underlying write call is stubbed to make dispatch observable).
# ---------------------------------------------------------------------------


class TestLandTableDispatchedOffEventLoop:
    async def test_duckdb_native_land_runs_on_a_worker_thread_not_the_event_loop(self):
        runtime = DuckDBFederationRuntime(materialize_dsn="duckdb:///:memory:")
        caller_thread = threading.current_thread().name
        seen_threads: list[str] = []

        def _fake_land(con, **kwargs):
            del con, kwargs
            seen_threads.append(threading.current_thread().name)
            return "mat_store.s.t1"

        with patch(
            "provisa.federation.store_connection.land_duckdb_native", side_effect=_fake_land
        ):
            await runtime.land_table(
                schema="s", table="t1", columns=[("id", "INTEGER")], rows=[{"id": 1}]
            )

        assert seen_threads, "land_duckdb_native was never called"
        assert seen_threads[0] != caller_thread, (
            "land_table() must dispatch the duckdb-native write to an executor, not call it "
            "inline on the event loop (regression: a multi-second write froze every other "
            "concurrent query on any table/transport for its whole duration)"
        )


# ---------------------------------------------------------------------------
# (4) Concurrent lands for DIFFERENT tables serialize on the dedicated single-worker executor
# ---------------------------------------------------------------------------


class TestConcurrentLandsSerialize:
    async def test_two_concurrent_lands_for_different_tables_do_not_overlap(self):
        runtime = DuckDBFederationRuntime(materialize_dsn="duckdb:///:memory:")
        intervals: dict[str, tuple[float, float]] = {}
        lock = threading.Lock()

        def _slow_land(con, *, table, **kwargs):
            del con, kwargs
            start = time.monotonic()
            time.sleep(0.2)
            end = time.monotonic()
            with lock:
                intervals[table] = (start, end)
            return f"mat_store.s.{table}"

        with patch(
            "provisa.federation.store_connection.land_duckdb_native", side_effect=_slow_land
        ):
            await asyncio.gather(
                runtime.land_table(
                    schema="s", table="t1", columns=[("id", "INTEGER")], rows=[{"id": 1}]
                ),
                runtime.land_table(
                    schema="s", table="t2", columns=[("id", "INTEGER")], rows=[{"id": 2}]
                ),
            )

        assert set(intervals) == {"t1", "t2"}
        (s1, e1), (s2, e2) = intervals["t1"], intervals["t2"]
        # Serialized (not piled onto the one shared connection at once): one interval must
        # finish before the other starts, in either order.
        assert e1 <= s2 or e2 <= s1, (
            f"concurrent lands for different tables overlapped: t1={s1:.3f}-{e1:.3f}, "
            f"t2={s2:.3f}-{e2:.3f} — the single-worker executor did not serialize them"
        )
