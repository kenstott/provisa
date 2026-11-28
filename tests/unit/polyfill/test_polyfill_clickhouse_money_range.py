# Copyright (c) 2026 Kenneth Stott
# Canary: 77d0e6db-60b4-4ed1-ad5a-53fb7e975c3c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Money at its range limits on ClickHouse, through pgwire (REQ-2236; REQ-2167, NEVER A WRONG ANSWER
AND NEVER A RAW ENGINE ERROR).

ClickHouse reads a number with a fraction as a Float64, so a money literal at money's limit
($92233720368547758.07) reached it as 92233720368547758.0814: a value past int8 cents, which the
encoder refuses as it writes the row, and the client's connection was closed. Its text is read now
(pg_money.exact_on_clickhouse), and every money operation that may leave the range raises
PostgreSQL's 22003 on ClickHouse (throwIf) before a row is written.

The answers are PostgreSQL's (the embedded PostgreSQL 16.11 and postgres:17.11 agree on each):
text with lc_monetary C, binary as int8 cents."""

# Requirements: REQ-2236, REQ-2167

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from psycopg import pq

from provisa.federation.clickhouse_runtime import ClickHouseFederationRuntime
from provisa.transpiler import pg_money
from provisa.transpiler.transpile import transpile
from tests.pgwire_describe_parity import RuntimeEngine
from tests.role_state import with_surfaces
from tests.unit.pgwire.test_wire_protocol import _free_port, _make_server

import sqlglot

_ROLE = "money_range"
_MONEY = 790
_LIMITS = (
    "(VALUES ('$92233720368547758.07'::money), ('-$92233720368547758.08'::money), (NULL::money))"
    " AS t(c)"
)
_HIGHEST = ("$92,233,720,368,547,758.07", "7fffffffffffffff")
_LOWEST = ("-$92,233,720,368,547,758.08", "8000000000000000")

ANSWERED = [
    ("SELECT '$92233720368547758.07'::money AS v", _HIGHEST),
    ("SELECT '-$92233720368547758.08'::money AS v", _LOWEST),
    (f"SELECT max(c) AS v FROM {_LIMITS}", _HIGHEST),
    (f"SELECT min(c) AS v FROM {_LIMITS}", _LOWEST),
    (f"SELECT pg_catalog.max(c) AS v FROM {_LIMITS}", _HIGHEST),
    ("SELECT max('$92233720368547758.07'::money) AS v", _HIGHEST),
    ("SELECT min('-$92233720368547758.08'::money) AS v", _LOWEST),
    ("SELECT max(NULL::money) AS v", (None, None)),
    *[  # an infinite divisor is a zero quotient: no error
        (
            f"SELECT pg_catalog.cash_div_flt{4 if kind == 'real' else 8}"
            f"('12.34'::money, '{special}'::{kind}) AS v",
            ("$0.00", "00"),
        )
        for special in ("Infinity", "-Infinity")
        for kind in ("real", "double precision")
    ],
    *[  # a NULL argument is a NULL answer, as PostgreSQL's strict functions answer
        (f"SELECT pg_catalog.{f} AS v", (None, None))
        for n in ("4", "8")
        for kind in (("real",) if n == "4" else ("double precision",))
        for money, number in (
            ("NULL::money", f"NULL::{kind}"),
            ("NULL::money", f"'2.5'::{kind}"),
            ("'12.34'::money", f"NULL::{kind}"),
        )
        for f in (
            f"cash_mul_flt{n}({money}, {number})",
            f"cash_div_flt{n}({money}, {number})",
            f"flt{n}_mul_cash({number}, {money})",
        )
    ],
    ("SELECT max(c) AS v FROM (VALUES ('$1.5'::money), ('$1.50'::money)) AS t(c)", ("$1.50", "96")),
    (
        "SELECT min(c) AS v FROM (VALUES ('$0.005'::money), ('$0.01'::money)) AS t(c)",
        ("$0.01", "01"),
    ),
    (
        "SELECT '$92233720368547758.07'::money - '$0.01'::money AS v",
        ("$92,233,720,368,547,758.06", "7ffffffffffffffe"),
    ),
    (f"SELECT sum(c) AS v FROM {_LIMITS}", ("-$0.01", "ffffffffffffffff")),
    ("SELECT sum(c) AS v FROM (VALUES (NULL::money)) AS t(c)", (None, None)),
]
RAISED = [
    ("SELECT '$92233720368547758.07'::money + '$0.01'::money AS v", "22003"),
    ("SELECT '-$92233720368547758.08'::money - '$0.01'::money AS v", "22003"),
    ("SELECT '$46116860184273879.04'::money * 2 AS v", "22003"),
    ("SELECT '$1'::money / 0 AS v", "22012"),
    *[
        (f"SELECT pg_catalog.{f}", "22003")
        for special in ("NaN", "Infinity", "-Infinity")
        for kind in ("real", "double precision")
        for f in (
            f"cash_mul_flt{4 if kind == 'real' else 8}('12.34'::money, '{special}'::{kind}) AS v",
            f"flt{4 if kind == 'real' else 8}_mul_cash('{special}'::{kind}, '12.34'::money) AS v",
        )
    ],
    *[
        (
            f"SELECT pg_catalog.cash_div_flt{4 if kind == 'real' else 8}('12.34'::money, 'NaN'::{kind}) AS v",
            "22003",
        )
        for kind in ("real", "double precision")
    ],
    (
        "SELECT sum(c) AS v FROM (VALUES ('$92233720368547758.07'::money), ('$1'::money)) AS t(c)",
        "22003",
    ),
    (
        "SELECT sum(c) OVER () AS v "
        "FROM (VALUES ('$92233720368547758.07'::money), ('$1'::money)) AS t(c)",
        "22003",
    ),
]


def _plan(sql: str) -> Any:
    from provisa.pgwire._pipeline import _mint_stamp, _Plan
    from provisa.transpiler.router import Route

    return _Plan(
        route=Route.ENGINE,
        sql=sql,
        source_id="s",
        dialect="clickhouse",
        physical_sql=sql,
        exec_params=[],
        stamp=_mint_stamp(),
    )


@pytest.fixture(scope="module")
def pgwire() -> Iterator[int]:
    port = _free_port()
    server = _make_server(port)
    yield port
    server.shutdown()


@pytest.fixture
def conn(pgwire: int, runtime: ClickHouseFederationRuntime) -> Iterator[psycopg.Connection]:
    """A pgwire session served by the embedded ClickHouse, each statement transpiled for it."""
    from provisa.pgwire.server import ProvisaSession

    ctx = MagicMock()
    ctx.tables = {}
    state = with_surfaces(MagicMock())
    state.contexts = {_ROLE: ctx}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = None
    state.auth_middleware_active = False
    state.multitenancy = False
    state.federation_engine = RuntimeEngine(runtime, "clickhouse")

    async def _govern(text, _role_id, _params=None, _wire_formats=None, *, deliver, as_of=None):
        del deliver, as_of
        return _plan(transpile(text, "clickhouse", generated=False))

    async def _no_cache(_plan_, _state):
        return None

    with (
        patch("provisa.api.app.state", state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", _govern),
        patch("provisa.pgwire._pipeline.prepare_residency_and_check_cache", _no_cache),
        patch.object(ProvisaSession, "_finalize_audit", lambda *_a, **_k: None),
    ):
        c = psycopg.connect(
            host="127.0.0.1",
            port=pgwire,
            user=_ROLE,
            password="x",
            dbname="provisa",
            autocommit=True,
        )
        try:
            yield c
        finally:
            c.close()


def _still_open(c: psycopg.Connection) -> None:
    after = c.pgconn.exec_params(b"SELECT 1 AS v", [], result_format=0)
    assert after.status == pq.ExecStatus.TUPLES_OK, (after.error_message or b"").decode()
    assert c.pgconn.status == pq.ConnStatus.OK


@pytest.mark.parametrize(("sql", "want"), ANSWERED)
def test_money_at_its_limits_answers_postgresql_s_value(conn, sql, want):
    text, binary = want
    for fmt, expected in ((0, text), (1, binary)):
        res = conn.pgconn.exec_params(sql.encode(), [], result_format=fmt)
        assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
        assert res.ftype(0) == _MONEY
        value = res.get_value(0, 0)
        got = None if value is None else (value.decode() if fmt == 0 else value.hex())
        if fmt == 1 and got is not None:
            got = got.lstrip("0") or "0"
            expected = expected.lstrip("0") or "0"
        assert got == expected, (fmt, got, expected)
    _still_open(conn)


@pytest.mark.parametrize(("sql", "state"), RAISED)
def test_money_past_its_range_raises_postgresql_s_error_and_keeps_the_connection(conn, sql, state):
    res = conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert res.status == pq.ExecStatus.FATAL_ERROR
    said = res.error_field(pq.DiagnosticField.SQLSTATE)
    assert said is not None and said.decode() == state, (res.error_message or b"").decode()
    _still_open(conn)


def test_a_money_literal_reaches_clickhouse_as_its_text():
    written = transpile("SELECT '$92233720368547758.07'::money AS v", "clickhouse", generated=False)
    assert "'92233720368547758.07'" in written


def test_another_dialect_keeps_its_money_literal():
    written = pg_money.lowered("SELECT '$1.50'::money AS v", "duckdb")
    assert sqlglot.parse_one(written).find(sqlglot.exp.Literal).is_string is False  # type: ignore[union-attr]
