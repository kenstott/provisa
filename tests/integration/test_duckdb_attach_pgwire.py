# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""End-to-end DuckDB `ATTACH ... TYPE postgres` against the Provisa pgwire server (REQ-883).

This is the live-client verification REQ-883 lists as REMAINING: a real DuckDB
engine speaks libpq to the running ProvisaServer and reads a governed table. It
exercises, in one flow, the capability set DuckDB's `postgres` extension drives:

  (1) libpq startup + auth handshake (trust mode here)
  (2) catalog introspection over pg_namespace/pg_class/pg_attribute/pg_type/... —
      answered by the real catalog intercept from a real compilation context
  (3) the bulk read path: DuckDB wraps the scan as
      `COPY (SELECT ...) TO STDOUT (FORMAT binary)` and decodes the PG binary
      COPY stream using the OIDs the catalog reported — so a type/OID mismatch
      surfaces as a DuckDB decode error, not a silent wrong value
  (4) BEGIN/COMMIT/SET TRANSACTION accepted as no-ops (no real isolation)
  (5) SINGLE-STREAM scan only — ctid parallelism is a non-goal (PGW-023): the
      catalog reports relpages/reltuples ~ 0 so DuckDB never elects it.

The catalog is REAL (built from SchemaInput). Only the two data-execution seams
(`plan_pgwire_sql` → DIRECT, `_execute_plan` → rows) are stubbed, because a live
Trino/RDBMS source is not available in unit-tier CI; the wire, catalog, and
binary-COPY encoder under test are all real.
"""

from __future__ import annotations

import socket
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

pytestmark = [pytest.mark.integration]

duckdb = pytest.importorskip("duckdb", reason="duckdb required for ATTACH e2e")

from provisa.compiler.introspect import ColumnMetadata  # noqa: E402  # imports follow the duckdb importorskip guard
from provisa.compiler import naming as _naming  # noqa: E402  # imports follow the duckdb importorskip guard
from provisa.compiler.schema_gen import SchemaInput  # noqa: E402  # imports follow the duckdb importorskip guard
from provisa.compiler.context import build_context  # noqa: E402  # imports follow the duckdb importorskip guard
from provisa.executor.result import QueryResult  # noqa: E402  # imports follow the duckdb importorskip guard
from provisa.pgwire.server import ProvisaConnection, ProvisaServer  # noqa: E402  # imports follow the duckdb importorskip guard


# --- the governed table DuckDB will discover and scan --------------------------

_TABLES = [
    {
        "id": 1,
        "source_id": "sales-pg",
        "domain_id": "sales",
        "schema_name": "public",
        "table_name": "orders",
        "governance": "pre-approved",
        "columns": [
            {"column_name": "id", "visible_to": []},
            {"column_name": "amount", "visible_to": []},
            {"column_name": "region", "visible_to": []},
        ],
    }
]

_COL_TYPES: dict[int, list[ColumnMetadata]] = {
    1: [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
        ColumnMetadata(column_name="amount", data_type="double", is_nullable=True),
        ColumnMetadata(column_name="region", data_type="varchar", is_nullable=True),
    ]
}

# The rows the stubbed scan returns — DuckDB must decode these from binary COPY.
_ROWS = [(1, 10.5, "west"), (2, 20.0, "east"), (3, 30.25, None)]
_COL_NAMES = ["id", "amount", "region"]
_COL_WIRE_TYPES = ["INTEGER", "DOUBLE", "VARCHAR"]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _build_state():
    _naming.configure(gql="snake")
    si = SchemaInput(
        tables=_TABLES,
        relationships=[],
        column_types=_COL_TYPES,
        naming_rules=[],
        role={"id": "admin", "domain_access": ["*"], "capabilities": ["ddl"]},
        domains=[{"id": "sales", "graphql_alias": None}],
    )
    ctx = build_context(si)

    state = MagicMock()
    state.contexts = {"admin": ctx}
    state.rls_contexts = {}
    state.roles = {"admin": {"id": "admin", "capabilities": ["ddl"], "domain_access": ["*"]}}
    state.schema_build_cache = {"column_types": _COL_TYPES, "tables": [], "domains": []}
    # Trust mode: password ignored, username -> role_id.
    state.auth_config = {"provider": "none"}
    state.auth_middleware_active = False
    state.masking_rules = {}
    state.source_types = {"sales-pg": "postgresql"}
    state.source_dialects = {"sales-pg": "postgres"}
    state.source_pools = MagicMock()
    state.source_pools.has.return_value = False
    state.source_pools.source_ids = ["sales-pg"]
    state.server_limits = {}
    state.engine_conn = None
    return state


@pytest.fixture()
def pgwire_port():
    """Live ProvisaServer with a real catalog + stubbed data-execution seams."""
    import provisa.pgwire.server as _srv
    from provisa.pgwire import _pipeline as _pl
    from provisa.transpiler.router import Route

    state = _build_state()

    async def _fake_plan(sql: str, role_id: str):
        return _pl._Plan(route=Route.DIRECT, sql=sql, source_id="sales-pg", dialect="postgres")

    async def _fake_execute_plan(plan, _state=None):
        return QueryResult(rows=_ROWS, column_names=_COL_NAMES, column_types=_COL_WIRE_TYPES)

    async def _fake_execute_sql(sql: str, role_id: str):
        return QueryResult(rows=_ROWS, column_names=_COL_NAMES, column_types=_COL_WIRE_TYPES)

    port = _free_port()
    conn = ProvisaConnection()
    server = ProvisaServer(("127.0.0.1", port), conn)

    # The server thread has no asyncio loop; the COPY/pipeline seams need one.
    import asyncio

    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()

    with (
        patch("provisa.api.app.state", state),
        patch.object(_srv, "state", state, create=True),
        patch.object(_pl, "plan_pgwire_sql", _fake_plan),
        patch.object(_pl, "_execute_plan", _fake_execute_plan),
        patch.object(_pl, "execute_pgwire_sql", _fake_execute_sql),
    ):
        with _srv._loop_lock:
            _srv._loop = loop
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        # Poll until the server thread actually accepts a connection. A fixed sleep under-provisions
        # under full-suite load (the in-process server thread starves for the GIL), so the DuckDB
        # client would attach before the catalog/COPY path is serving — surfacing as
        # "table does not exist" or a truncated binary-COPY "out of buffer" decode.
        _deadline = time.time() + 30
        while time.time() < _deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            raise RuntimeError(f"pgwire server did not accept connections on {port} within 30s")
        try:
            yield port, state
        finally:
            server.shutdown()
            with _srv._loop_lock:
                _srv._loop = None
            loop.call_soon_threadsafe(loop.stop)


def _attach(port: int):
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    dsn = f"host=127.0.0.1 port={port} dbname=provisa user=admin password=x"
    con.execute(f"ATTACH '{dsn}' AS prov (TYPE postgres, READ_ONLY)")
    return con


# --- capability (1)+(2): ATTACH completes, DuckDB discovers the table ----------


def test_attach_and_discover_table(pgwire_port):
    port, _ = pgwire_port
    con = _attach(port)
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'prov' AND table_name = 'orders'"
    ).fetchall()
    con.close()
    assert ("orders",) in tables


# --- capability (2)+(3): scan reads rows and decodes binary COPY correctly ------


def test_scan_reads_rows(pgwire_port):
    port, _ = pgwire_port
    con = _attach(port)
    rows = con.execute("SELECT id, amount, region FROM prov.sales.orders ORDER BY id").fetchall()
    con.close()
    assert rows == _ROWS


def test_scan_types_survive_binary_copy(pgwire_port):
    """int4/float8/varchar + a NULL must round-trip through the PG binary COPY encoder.

    Selects all columns in stub order so the binary-COPY field count matches; the
    point under test is the per-type encoding, not projection pushdown.
    """
    port, _ = pgwire_port
    con = _attach(port)
    rows = con.execute("SELECT id, amount, region FROM prov.sales.orders ORDER BY id").fetchall()
    con.close()
    assert [r[0] for r in rows] == [1, 2, 3]  # int4 survived
    assert sum(r[1] for r in rows) == pytest.approx(60.75)  # float8 survived
    assert rows[2][2] is None  # the region NULL survived as NULL, not ""


# --- capability (4): transaction control accepted as no-ops ---------------------


def test_transaction_control_accepted_as_noops(pgwire_port):
    """BEGIN ... REPEATABLE READ READ ONLY / COMMIT / ROLLBACK are accepted, no isolation.

    DuckDB's postgres extension opens a read transaction around its scans; the
    server must accept these verbs without error even though it provides no real
    snapshot isolation (REQ-883 cap 4). Driven straight to the pgwire side via
    postgres_execute so the assertion is on the server, not DuckDB's own state.
    """
    port, _ = pgwire_port
    con = _attach(port)
    for stmt in (
        "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "COMMIT",
        "BEGIN",
        "ROLLBACK",
    ):
        con.execute(f"CALL postgres_execute('prov', '{stmt}')")  # raises if the server errors
    con.close()


# --- capability (5): single-stream — DuckDB must not see ctid parallelism -------


def test_catalog_reports_zero_pages_for_single_stream(pgwire_port):
    """PGW-023 non-goal: relpages/reltuples ~ 0 so DuckDB never elects ctid parallel scan.

    Asserted against the catalog the server serves (not via a DuckDB COPY, which
    would re-project pg_class through the row stub); this is the exact pg_class
    row DuckDB's postgres extension reads to decide parallelism.
    """
    from provisa.pgwire.catalog_populate import _build_catalog_db

    _, state = pgwire_port
    db = _build_catalog_db("admin", state)
    rows = db.execute("SELECT relname, relpages FROM pg_class").fetchall()
    db.close()
    assert rows, "catalog served pg_class"
    # The catalog advertises no physical page counts for ANY relation, so DuckDB's
    # postgres extension never elects ctid page-range parallelism — single stream.
    assert all((r[1] or 0) == 0 for r in rows)
