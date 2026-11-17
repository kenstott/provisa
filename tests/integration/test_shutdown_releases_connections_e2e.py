# Copyright (c) 2026 Kenneth Stott
# Canary: 8b3e1f72-4c9d-4a56-b0e7-2d6f9c1a5e84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A server that shuts down leaves no PostgreSQL backend connected (REQ-1316, REQ-258).

Every connection the app opens during its life is closed by its shutdown, including when the
config was re-applied while it ran (the settings and setup routers re-run ``_load_and_build``).
Three holders used to outlive it: the native engine's runtime (its DuckDB-attached sources keep a
postgres-extension pool) was never closed; a build pass still running, or one a finishing build
chained into, rebuilt that runtime after the engine closed; and each re-apply built new
control-plane engines, leaving every handle over the old ones with a pool nothing closed. A
process holding connections after its shutdown starves the server's ``max_connections`` for
whatever runs next (it failed the multi-worker suites that followed it).

Lands on the TEST instance's PostgreSQL only; the app is the in-process one the session config
describes."""

# Requirements: REQ-1316, REQ-258

from __future__ import annotations

import os
import sys

import pytest
import sqlalchemy as sa

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio(loop_scope="session")]


def _server() -> sa.Engine:
    url = "postgresql+psycopg://provisa:provisa@{h}:{p}/postgres".format(
        h=os.environ.get("PG_HOST", "localhost"), p=os.environ.get("PG_PORT", "5432")
    )
    return sa.create_engine(url, poolclass=sa.pool.NullPool)


def _opened_since(server: sa.Engine, started) -> list:
    """The client backends connected now that were opened at or after ``started``."""
    with server.connect() as conn:
        return conn.execute(
            sa.text(
                "SELECT pid, datname, left(query, 60) FROM pg_stat_activity "
                "WHERE backend_type = 'client backend' AND pid <> pg_backend_pid() "
                "AND backend_start >= :started"
            ),
            {"started": started},
        ).fetchall()


async def test_a_lifespan_with_a_reapplied_config_leaves_no_backend_open():
    os.environ.setdefault("PG_PASSWORD", "provisa")
    from provisa.api.app import _load_and_build, create_app
    from provisa.core import model_change

    # DIAGNOSTIC (R510-U2, reverted before landing).
    import gc
    import threading
    import time as _time
    import traceback

    from sqlalchemy import event
    from sqlalchemy.pool import Pool

    from provisa.federation import duckdb_runtime as _dr

    opened: dict = {}

    def _pid(dbapi_conn):
        info = getattr(dbapi_conn, "info", None)
        if getattr(info, "backend_pid", None) is not None:
            return info.backend_pid
        if hasattr(dbapi_conn, "get_backend_pid"):
            return dbapi_conn.get_backend_pid()
        return ("unknown", type(dbapi_conn).__module__, type(dbapi_conn).__qualname__)

    def _on_connect(dbapi_conn, rec):
        opened[_pid(dbapi_conn)] = (
            threading.current_thread().name,
            "".join(traceback.format_stack(limit=40)),
        )

    runtimes: list = []
    _orig_init, _orig_close = (
        _dr.DuckDBFederationRuntime.__init__,
        _dr.DuckDBFederationRuntime.close,
    )

    def _init(self, *a, **k):
        runtimes.append(
            (
                _time.monotonic(),
                "BUILT",
                id(self),
                threading.current_thread().name,
                "".join(traceback.format_stack(limit=30)),
            )
        )
        _orig_init(self, *a, **k)

    def _close(self):
        runtimes.append(
            (
                _time.monotonic(),
                "CLOSED",
                id(self),
                threading.current_thread().name,
                "".join(traceback.format_stack(limit=14)),
            )
        )
        _orig_close(self)

    _dr.DuckDBFederationRuntime.__init__ = _init
    _dr.DuckDBFederationRuntime.close = _close
    event.listen(Pool, "connect", _on_connect)
    server = _server()
    with server.connect() as conn:
        started = conn.execute(sa.text("SELECT now()")).scalar()
    # The deliberate leak the diagnostic must name: a pooled SQLAlchemy connection left open.
    probe = sa.create_engine(server.url.set(database="provisa"))
    probe_conn = probe.connect()
    probe_pid = probe_conn.connection.dbapi_connection.info.backend_pid
    app = create_app()
    async with app.router.lifespan_context(app):
        assert _opened_since(server, started), "the app opened no connection to observe"
        # A re-applied config, as the settings router runs it: inside the request's model-change
        # scope (ModelChangeMiddleware), since loading the config writes the model.
        async with model_change.scope("PUT /admin/config", actor=lambda: "test"):
            await _load_and_build()
    shut = _time.monotonic()
    with server.connect() as conn:
        shut_at = conn.execute(sa.text("SELECT now()")).scalar()
        detail = conn.execute(
            sa.text(
                "SELECT pid, application_name, client_port, backend_start, xact_start, "
                "state_change, state, left(query, 80) FROM pg_stat_activity "
                "WHERE backend_type = 'client backend' AND pid <> pg_backend_pid() "
                "AND backend_start >= :started"
            ),
            {"started": started},
        ).fetchall()
    left = [r for r in _opened_since(server, started) if r[0] != probe_pid]
    print(
        f"=== DIAG probe pid {probe_pid}: in pg_stat_activity="
        f"{any(r[0] == probe_pid for r in detail)} recorded by listener={probe_pid in opened}"
    )
    assert probe_pid in opened, "the diagnostic's pid lookup missed its own deliberate leak"
    probe_conn.close()
    probe.dispose()
    event.remove(Pool, "connect", _on_connect)
    _dr.DuckDBFederationRuntime.__init__, _dr.DuckDBFederationRuntime.close = (
        _orig_init,
        _orig_close,
    )
    if left:
        print(f"=== DIAG shutdown done at pg now()={shut_at}")
        for row in detail:
            print("=== DIAG backend", tuple(row))
        for pid, *_ in left:
            print(
                f"=== DIAG leaked pid {pid}: opened by {opened.get(pid, ('not SQLAlchemy', ''))[0]}"
            )
            print(opened.get(pid, ("", ""))[1])
        print("=== DIAG non-int listener keys:", [k for k in opened if not isinstance(k, int)])
        for t, what, rid, thread, stack in runtimes:
            print(f"=== DIAG runtime {what} {rid} at shutdown{t - shut:+.2f}s on {thread}")
            print(stack)
        print("=== DIAG threads alive:", sorted(t.name for t in threading.enumerate()))
        for th in threading.enumerate():
            frame = sys._current_frames().get(th.ident)
            if frame is not None and th.name != "MainThread":
                print(f"=== DIAG thread {th.name} stack:")
                print("".join(traceback.format_stack(frame, limit=25)))
        import duckdb

        for obj in gc.get_objects():
            if isinstance(obj, _dr.DuckDBFederationRuntime):
                holders = [type(r).__qualname__ for r in gc.get_referrers(obj)][:10]
                print("=== DIAG live runtime", id(obj), "referrers", holders)
            elif isinstance(obj, duckdb.DuckDBPyConnection):
                try:
                    dbs = obj.execute(
                        "SELECT database_name, type FROM duckdb_databases() WHERE NOT internal"
                    ).fetchall()
                except Exception as exc:  # diagnostic only
                    dbs = f"closed: {exc}"
                print("=== DIAG duckdb connection", id(obj), dbs)
            elif type(obj).__name__ in ("RecordBatchReader", "DuckDBPyRelation"):
                print(
                    "=== DIAG live",
                    type(obj).__module__,
                    type(obj).__qualname__,
                    id(obj),
                    [type(r).__qualname__ for r in gc.get_referrers(obj)][:6],
                )
    server.dispose()
    assert left == [], f"backends still connected after shutdown: {left}"
