# Copyright (c) 2026 Kenneth Stott
# Canary: 5c1e9f27-3b84-4d60-a7c2-91d4e6b03f18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The extended protocol against a live server (REQ-2167): Parse + Sync, Describe(Statement) +
Sync, then Bind + Describe(Portal) + Execute + Sync, from libpq, on a real pgwire server with a real
DuckDB engine, for a statement with no parameters and one with $1, on a plain and a money column.

The server runs in this process, so the client must leave the GIL free while it waits, and it reads
every answer with a deadline of its own. libpq's blocking calls are not all safe for that:
psycopg's ``pgconn.describe_prepared`` keeps the GIL through PQdescribePrepared, where ``prepare``
and ``exec_prepared`` release it, so the server thread never runs and both sides wait for ever.
A client in another process is answered in milliseconds. Use the ``send_*`` calls here."""

# Requirements: REQ-2167

from __future__ import annotations

import faulthandler
import select
import sys
import time
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from psycopg import pq

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.pgwire.server import ProvisaSession
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.pgwire_describe_parity import RuntimeEngine, registered_parameters, registered_shape
from tests.role_state import with_surfaces
from tests.unit.pgwire.test_extended_protocol_portals import _engine_plan
from tests.unit.pgwire.test_wire_protocol import _free_port, _make_server

ROLE = "alice"
_ANSWER_SECONDS = 20.0
_INT4, _TEXT, _MONEY = 23, 25, 790
_REGISTRY = {"m": [("id", "integer"), ("label", "text"), ("price", "money")]}

#: statement, its $1 values, the described parameter and column OIDs, then the first row as text
#: and as binary -- money's binary form is its cents as an int8 (cash_send).
_FLOWS = {
    "plain": ("SELECT id FROM m ORDER BY id", [], [], _INT4, "1", "00000001"),
    "plain, $1": (
        "SELECT id FROM m WHERE label = $1::text",
        [b"two"],
        [_TEXT],
        _INT4,
        "2",
        "00000002",
    ),
    "money": ("SELECT price FROM m ORDER BY id", [], [], _MONEY, "$1,234.56", "000000000001e240"),
    "money, $1": (
        "SELECT price FROM m WHERE label = $1::text",
        [b"two"],
        [_TEXT],
        _MONEY,
        "-$5.00",
        "fffffffffffffe0c",
    ),
    # An uncast $1 takes its type from what it is compared with, as PostgreSQL resolves it.
    "uncast $1, a text column": (
        "SELECT id FROM m WHERE label = $1",
        [b"two"],
        [_TEXT],
        _INT4,
        "2",
        "00000002",
    ),
    "uncast $1, an integer column": (
        "SELECT label FROM m WHERE id = $1",
        [b"2"],
        [_INT4],
        _TEXT,
        "two",
        "74776f",
    ),
    "uncast $1, LIKE a text column": (
        "SELECT id FROM m WHERE label LIKE $1",
        [b"tw%"],
        [_TEXT],
        _INT4,
        "2",
        "00000002",
    ),
    "uncast $1, BETWEEN on a text column": (
        "SELECT id FROM m WHERE label BETWEEN $1 AND $2",
        [b"tt", b"tz"],
        [_TEXT, _TEXT],
        _INT4,
        "2",
        "00000002",
    ),
    "uncast $1, IN on a text column": (
        "SELECT id FROM m WHERE label IN ($1, $2)",
        [b"two", b"zzz"],
        [_TEXT, _TEXT],
        _INT4,
        "2",
        "00000002",
    ),
}


def _results(pgconn: Any) -> list[Any]:
    """Every result of the command just sent. The wait is the client's own, on the socket: a
    server that stops answering fails here, with every thread's stack."""
    results: list[Any] = []
    deadline = time.monotonic() + _ANSWER_SECONDS
    while True:
        while pgconn.is_busy():
            left = max(deadline - time.monotonic(), 0)
            if not select.select([pgconn.socket], [], [], left)[0]:
                faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
                pytest.fail(f"the server sent nothing for {_ANSWER_SECONDS}s")
            pgconn.consume_input()
        result = pgconn.get_result()
        if result is None:
            return results
        results.append(result)


def _answered(pgconn: Any, status: pq.ExecStatus) -> Any:
    results = _results(pgconn)
    assert len(results) == 1, [r.error_message for r in results]
    result = results[0]
    assert result.status == status, (result.error_message or b"").decode()
    return result


@pytest.fixture(scope="module")
def pgwire():
    port = _free_port()
    server = _make_server(port)
    yield port
    server.shutdown()


@pytest.fixture
def live(pgwire: int):
    from provisa.pgwire._pipeline import _Described

    runtime = DuckDBFederationRuntime()
    runtime._con.execute("CREATE TABLE m (id INTEGER, label VARCHAR, price VARCHAR)")
    runtime._con.execute("INSERT INTO m VALUES (1, 'one', '$1,234.56'), (2, 'two', '-$5.00')")
    ctx = MagicMock()
    ctx.tables = {}
    state = with_surfaces(MagicMock())
    state.contexts = {ROLE: ctx}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = None
    state.auth_middleware_active = False
    state.multitenancy = False
    state.federation_engine = RuntimeEngine(runtime, "duckdb")

    def _plan_of(sql: str, params: list | None) -> Any:
        plan = _engine_plan(transpile_to_duckdb(sql))
        plan.exec_params = params or []
        return plan

    async def _govern(text, role_id, params=None, wire_formats=None, *, deliver, as_of=None):
        return _plan_of(text, params)

    async def _describe(text, role_id, *, as_of=None):
        return _Described(
            registered_shape(text, _REGISTRY),
            ("governed", text),  # type: ignore[arg-type]
            registered_parameters(text, _REGISTRY),
        )

    async def _plan(held, params, wire_formats=None, *, deliver):
        return _plan_of(held[1], params)

    async def _no_cache(plan, st):
        return None

    async def _resident(st, plan):
        return None

    with (
        patch("provisa.api.app.state", state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", _govern),
        patch("provisa.pgwire._pipeline.describe_pgwire_statement", _describe),
        patch("provisa.pgwire._pipeline.plan_pgwire_statement", _plan),
        patch(
            "provisa.pgwire._pipeline.governed_statement_is_current",
            lambda held, st, as_of=None: True,
        ),
        patch("provisa.pgwire._pipeline.prepare_residency_and_check_cache", _no_cache),
        patch("provisa.federation.query_residency.prepare_engine_residency", _resident),
        patch.object(ProvisaSession, "_finalize_audit", lambda self, governed, status, **kw: None),
    ):
        c = psycopg.connect(
            host="127.0.0.1",
            port=pgwire,
            user=ROLE,
            password="x",
            dbname="provisa",
            autocommit=True,
        )
        try:
            yield c.pgconn
        finally:
            c.close()


@pytest.mark.parametrize("flow", list(_FLOWS))
def test_each_step_of_the_extended_protocol_is_answered(live: Any, flow: str):
    sql, values, param_oids, oid, text, binary = _FLOWS[flow]
    live.send_prepare(b"probe", sql.encode())
    _answered(live, pq.ExecStatus.COMMAND_OK)

    live.send_describe_prepared(b"probe")
    described = _answered(live, pq.ExecStatus.COMMAND_OK)
    assert [described.param_type(i) for i in range(described.nparams)] == param_oids
    assert described.ftype(0) == oid

    for result_format, first in ((0, text.encode()), (1, bytes.fromhex(binary))):
        live.send_query_prepared(b"probe", values or None, result_format=result_format)
        executed = _answered(live, pq.ExecStatus.TUPLES_OK)
        assert executed.ftype(0) == oid
        assert executed.get_value(0, 0) == first


def _error(result: Any) -> tuple[str, str]:
    return (
        result.error_field(pq.DiagnosticField.SQLSTATE).decode(),
        result.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode(),
    )


def _still_answers(pgconn: Any) -> None:
    pgconn.send_query_params(b"SELECT id FROM m ORDER BY id", None)
    assert _answered(pgconn, pq.ExecStatus.TUPLES_OK).get_value(0, 0) == b"1"


def test_describing_an_unknown_statement_answers_26000_and_the_connection_goes_on(live: Any):
    live.send_describe_prepared(b"never_prepared")
    answered = _answered(live, pq.ExecStatus.FATAL_ERROR)
    assert _error(answered) == ("26000", 'prepared statement "never_prepared" does not exist')
    _still_answers(live)


def test_describing_an_unknown_portal_answers_34000_and_the_connection_goes_on(live: Any):
    live.send_describe_portal(b"never_bound")
    answered = _answered(live, pq.ExecStatus.FATAL_ERROR)
    assert _error(answered) == ("34000", 'portal "never_bound" does not exist')
    _still_answers(live)


def test_binding_an_unknown_statement_answers_26000_and_the_connection_goes_on(live: Any):
    live.send_query_prepared(b"never_prepared", None)
    answered = _answered(live, pq.ExecStatus.FATAL_ERROR)
    assert _error(answered) == ("26000", 'prepared statement "never_prepared" does not exist')
    _still_answers(live)
