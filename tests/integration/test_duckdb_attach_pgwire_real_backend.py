# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""TRUE end-to-end: DuckDB `ATTACH ... TYPE postgres` → Provisa pgwire → a REAL source (REQ-883).

Companion to ``test_duckdb_attach_pgwire.py``, which stubs the data-execution seams
(``plan_pgwire_sql``/``_execute_plan``/``execute_pgwire_sql``) because a live RDBMS is
not guaranteed in that tier. THIS file stubs NONE of them: the rows DuckDB reads are
fetched from a real SQLite database (``demo/files/orders.sqlite``) through the actual
Provisa pipeline —

    DuckDB `postgres` extension
      → libpq/COPY to ProvisaServer
        → plan_pgwire_sql  (real govern + route → Route.DIRECT, dialect sqlite)
          → _execute_plan → federation_engine.execute_native
            → execute_direct → SQLite driver → SELECT on the real .sqlite file
        → PG binary-COPY encode of the fetched rows
      → DuckDB binary-COPY decode → returned to the DuckDB client

So a wrong route, a governance drop, a transpile error, an OID/type mismatch, or a
COPY-encode bug all surface as a failed assertion on the DuckDB-side rows. The only
non-real thing is the network being loopback.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytestmark = [pytest.mark.integration]

duckdb = pytest.importorskip("duckdb", reason="duckdb required for ATTACH e2e")
pytest.importorskip("sqlalchemy", reason="sqlite source driver requires sqlalchemy")

from provisa.compiler import naming as _naming  # noqa: E402
from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.schema_gen import SchemaInput  # noqa: E402
from provisa.compiler.context import build_context  # noqa: E402
from provisa.executor.pool import SourcePool  # noqa: E402
from provisa.federation.runtime import EngineRuntime  # noqa: E402
from provisa.federation.engine import build_engine  # noqa: E402
from provisa.pgwire.server import ProvisaConnection, ProvisaServer  # noqa: E402

_ORDERS_SQLITE = str(Path(__file__).parent.parent.parent / "demo" / "files" / "orders.sqlite")

# The real orders.sqlite lives in the default (``main``) schema — DuckDB will discover
# and scan ``prov.main.orders``. Columns picked to cover int / double / varchar decode.
_TABLES = [
    {
        "id": 1,
        "source_id": "sales-sqlite",
        "domain_id": "sales",
        "schema_name": "main",
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


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _build_state(pool: SourcePool):
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
    state.auth_config = {"provider": "none"}  # trust mode: username -> role_id
    state.auth_middleware_active = False
    state.masking_rules = {}
    state.source_types = {"sales-sqlite": "sqlite"}
    state.source_dialects = {"sales-sqlite": "sqlite"}
    state.source_pools = pool
    state.server_limits = {}
    state.engine_conn = None
    # Real federation engine — DIRECT execution delegates through execute_native.
    state.federation_engine = EngineRuntime(build_engine("duckdb"), state)
    return state


@pytest.fixture()
def pgwire_port():
    """Live ProvisaServer with a REAL catalog AND a REAL SQLite source — nothing stubbed."""
    import provisa.pgwire.server as _srv

    port = _free_port()
    conn = ProvisaConnection()
    server = ProvisaServer(("127.0.0.1", port), conn)

    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()

    # Build the real SQLite source pool on the server's loop.
    pool = SourcePool()
    fut = asyncio.run_coroutine_threadsafe(
        pool.add(
            source_id="sales-sqlite",
            source_type="sqlite",
            host="",
            port=0,
            database=_ORDERS_SQLITE,
            user="",
            password="",
        ),
        loop,
    )
    fut.result(timeout=30)

    state = _build_state(pool)

    with (
        patch("provisa.api.app.state", state),
        patch.object(_srv, "state", state, create=True),
    ):
        with _srv._loop_lock:
            _srv._loop = loop
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
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
            asyncio.run_coroutine_threadsafe(pool.close_all(), loop).result(timeout=10)
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


def test_attach_and_discover_table(pgwire_port):
    port, _ = pgwire_port
    con = _attach(port)
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = 'prov' AND table_name = 'orders'"
    ).fetchall()
    con.close()
    assert ("orders",) in tables


def test_scan_returns_real_sqlite_rows(pgwire_port):
    """DuckDB reads the 30 real rows out of the .sqlite file through the whole pipeline."""
    port, _ = pgwire_port
    con = _attach(port)
    n = con.execute("SELECT count(*) FROM prov.main.orders").fetchone()[0]
    total = con.execute("SELECT sum(amount) FROM prov.main.orders").fetchone()[0]
    con.close()
    assert n == 30  # every real row survived govern → route → transpile → COPY → decode
    assert total == pytest.approx(2471.98, rel=1e-6)


def test_scan_row_values_match_sqlite(pgwire_port):
    """Exact per-row values must round-trip int/double/varchar from SQLite to DuckDB."""
    port, _ = pgwire_port
    con = _attach(port)
    rows = con.execute(
        "SELECT id, amount, region FROM prov.main.orders ORDER BY id LIMIT 3"
    ).fetchall()
    con.close()
    assert rows == [
        (1, 19.98, "us-east"),
        (2, 49.99, "us-west"),
        (3, 199.99, "eu-west"),
    ]
