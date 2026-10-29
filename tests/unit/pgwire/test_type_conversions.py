# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7f0c52-9d1e-4a86-8e47-c5a2f6d10b93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PGWIRE CATALOG AND TYPES (REQ-2167, amended 2026-10-26), part 3: the casts tools issue, sent
as PostgreSQL 17 sends them -- the result column's type OID and typmod, and its text and binary
encodings.

Each statement runs as the conformance probe runs it (scripts/conformance/pgwire_catalog_types.py):
one ``exec_params`` over the extended protocol, once with text results and once with binary,
through a real pgwire server whose engine is a real DuckDB runtime (the statement's physical SQL is
the DuckDB transpile every engine read takes) and whose catalog is the real one. The expected
values are PostgreSQL 17.11's, taken from the PostgreSQL column of the Conformance (pgwire) run
40692101045 where it recorded them, and otherwise PostgreSQL's documented output for the type
(its ``*out`` and ``*send`` functions)."""

# Requirements: REQ-2167

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import psycopg
import pytest
from psycopg import pq

from provisa.federation.duckdb_runtime import DuckDBFederationRuntime
from provisa.transpiler.transpile import transpile_to_duckdb
from tests.pgwire_describe_parity import RuntimeEngine
from tests.role_state import with_surfaces
from tests.unit.pgwire.test_extended_protocol_portals import _engine_plan
from tests.unit.pgwire.test_wire_protocol import _free_port, _make_server

ROLE = "alice"

#: statement -> (type OID, typmod, text, binary hex), as PostgreSQL 17.11 sends them.
POSTGRES: dict[str, tuple[int, int, str | None, str | None]] = {
    "SELECT 1.5::float4 AS v": (700, -1, "1.5", "3fc00000"),
    "SELECT '12345.6789'::numeric AS v": (1700, -1, "12345.6789", "0003000100000004000109291a85"),
    # numeric(10,2): typmod ((10 << 16) | 2) + 4; 12345.68 is base-10000 digits 1, 2345, 6800.
    "SELECT '12345.6789'::numeric(10,2) AS v": (
        1700,
        655366,
        "12345.68",
        "000300010000000200010929" + "1a90",
    ),
    "SELECT 'x'::varchar AS v": (1043, -1, "x", "78"),
    "SELECT 'x'::varchar(10) AS v": (1043, 14, "x", "78"),
    "SELECT 'x'::char(3) AS v": (1042, 7, "x  ", "782020"),
    "SELECT 'x'::bpchar AS v": (1042, -1, "x", "78"),
    "SELECT 1::varchar AS v": (1043, -1, "1", "31"),
    "SELECT 'x'::name AS v": (19, -1, "x", "78"),
    "SELECT '2024-06-15 12:30:45+00'::timestamptz AS v": (
        1184,
        -1,
        "2024-06-15 12:30:45+00",
        "0002bdebab75a740",
    ),
    "SELECT '{\"a\": 1}'::jsonb AS v": (3802, -1, '{"a": 1}', "017b2261223a20317d"),
    "SELECT '\\xdeadbeef'::bytea AS v": (17, -1, "\\xdeadbeef", "deadbeef"),
    # array_send: ndim, has-nulls, element type, then per dimension its length and lower bound,
    # then each element's length and its own send form.
    "SELECT '{1,2}'::int4[] AS v": (
        1007,
        -1,
        "{1,2}",
        "000000010000000000000017000000020000000100000004000000010000000400000002",
    ),
    "SELECT '{a,b}'::text[] AS v": (
        1009,
        -1,
        "{a,b}",
        "000000010000000000000019000000020000000100000001610000000162",
    ),
    "SELECT 1::oid AS v": (26, -1, "1", "00000001"),
    "SELECT 23::regtype AS v": (2206, -1, "integer", "00000017"),
    "SELECT 'int4'::regtype AS v": (2206, -1, "integer", "00000017"),
    "SELECT 23::regtype::text AS v": (25, -1, "integer", "integer".encode().hex()),
    "SELECT 'pg_catalog.pg_class'::regclass AS v": (2205, -1, "pg_class", "000004eb"),
    "SELECT 'pg_catalog.pg_class'::regclass::oid AS v": (26, -1, "1259", "000004eb"),
    "SELECT 'pg_catalog'::regnamespace AS v": (4089, -1, "pg_catalog", "0000000b"),
    # An unknown-typed output column is text (PostgreSQL 10 and later).
    "SELECT NULL AS v": (25, -1, None, None),
    # inet_send: family (2, PGSQL_AF_INET), bits, is_cidr, address length, the address.
    "SELECT '192.168.0.1'::inet AS v": (869, -1, "192.168.0.1", "02200004c0a80001"),
    "SELECT pg_catalog.format_type(23, NULL) AS v": (25, -1, "integer", "integer".encode().hex()),
    "SELECT pg_catalog.format_type(1043, 24) AS v": (
        25,
        -1,
        "character varying(20)",
        "character varying(20)".encode().hex(),
    ),
    # No relation has oid 0: the function answers NULL.
    "SELECT pg_catalog.pg_table_is_visible(0) AS v": (16, -1, None, None),
    "SELECT pg_catalog.quote_ident('x') AS v": (25, -1, "x", "78"),
    # session_user names the role the session acts as (REQ-588), as a name.
    "SELECT session_user AS v": (19, -1, ROLE, ROLE.encode().hex()),
}


def _run(conn: psycopg.Connection, sql: str, fmt: int) -> tuple[int, int, str | None, str | None]:
    res = conn.pgconn.exec_params(sql.encode(), [], result_format=fmt)
    assert res.status == pq.ExecStatus.TUPLES_OK, (res.error_message or b"").decode()
    assert res.nfields == 1 and res.ntuples == 1
    assert res.fformat(0) == fmt
    value = res.get_value(0, 0)
    if fmt == 0:
        shown = None if value is None else value.decode()
    else:
        shown = None if value is None else value.hex()
    return res.ftype(0), res.fmod(0), shown, shown


@pytest.fixture(scope="module")
def pgwire():
    port = _free_port()
    server = _make_server(port)
    yield port
    server.shutdown()


@pytest.fixture
def conn(pgwire):
    from provisa.pgwire.server import ProvisaSession

    engine = RuntimeEngine(DuckDBFederationRuntime(), "duckdb")
    ctx = MagicMock()
    ctx.tables = {}
    state = with_surfaces(MagicMock())
    state.contexts = {ROLE: ctx}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = None
    state.auth_middleware_active = False
    state.multitenancy = False
    state.federation_engine = engine

    async def _govern(text, role_id, params=None, wire_formats=None, *, deliver, as_of=None):
        return _engine_plan(transpile_to_duckdb(text))

    async def _no_cache(plan, st):
        return None

    with (
        patch("provisa.api.app.state", state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", _govern),
        patch("provisa.pgwire._pipeline.prepare_residency_and_check_cache", _no_cache),
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
            c.execute("SET TimeZone TO 'UTC'")
            yield SimpleNamespace(conn=c, engine=engine)
        finally:
            c.close()


@pytest.mark.parametrize("sql", list(POSTGRES))
def test_a_cast_is_sent_as_postgresql_sends_it(conn: Any, sql: str):
    oid, typmod, text, binary = POSTGRES[sql]
    t_oid, t_mod, t_text, _ = _run(conn.conn, sql, 0)
    b_oid, b_mod, _, b_bin = _run(conn.conn, sql, 1)
    assert (t_oid, t_mod, t_text) == (oid, typmod, text)
    assert (b_oid, b_mod, b_bin) == (oid, typmod, binary)
