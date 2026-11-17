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

    # DIAGNOSTIC (R510-U, reverted before landing): the stack that opened, and last checked out,
    # each pooled connection, by backend pid.
    import threading
    import traceback

    from sqlalchemy import event
    from sqlalchemy.pool import Pool

    opened: dict = {}
    checked: dict = {}

    def _pid(dbapi_conn):
        info = getattr(dbapi_conn, "info", None)
        return getattr(info, "backend_pid", None)

    def _on_connect(dbapi_conn, rec):
        opened[_pid(dbapi_conn)] = (
            threading.current_thread().name,
            "".join(traceback.format_stack(limit=40)),
        )

    def _on_checkout(dbapi_conn, rec, proxy):
        checked[_pid(dbapi_conn)] = (
            threading.current_thread().name,
            "".join(traceback.format_stack(limit=40)),
        )

    event.listen(Pool, "connect", _on_connect)
    event.listen(Pool, "checkout", _on_checkout)
    server = _server()
    with server.connect() as conn:
        started = conn.execute(sa.text("SELECT now()")).scalar()
    app = create_app()
    async with app.router.lifespan_context(app):
        assert _opened_since(server, started), "the app opened no connection to observe"
        # A re-applied config, as the settings router runs it: inside the request's model-change
        # scope (ModelChangeMiddleware), since loading the config writes the model.
        async with model_change.scope("PUT /admin/config", actor=lambda: "test"):
            await _load_and_build()
    left = _opened_since(server, started)
    for pid, *_ in left:
        print(f"=== DIAG leaked pid {pid}: opened by {opened.get(pid, ('not SQLAlchemy', ''))[0]}")
        print(opened.get(pid, ("", ""))[1])
        print(f"=== DIAG leaked pid {pid}: last checkout by {checked.get(pid, ('none', ''))[0]}")
        print(checked.get(pid, ("", ""))[1])
    if left:
        print("=== DIAG threads alive:", sorted(t.name for t in threading.enumerate()))
        print(
            "=== DIAG pids SQLAlchemy opened:",
            sorted(k for k in opened if k is not None)[:50],
            len(opened),
        )
        import gc

        import duckdb

        def _owner_chain(obj, depth=0, seen=None):
            seen = seen if seen is not None else set()
            if depth > 3:
                return
            for ref in gc.get_referrers(obj):
                if id(ref) in seen or ref is opened or ref is checked:
                    continue
                seen.add(id(ref))
                if isinstance(ref, dict):
                    for owner in gc.get_referrers(ref):
                        if getattr(owner, "__dict__", None) is ref:
                            keys = [k for k, v in ref.items() if v is obj]
                            print(
                                "  " * depth,
                                "held by",
                                type(owner).__module__,
                                type(owner).__qualname__,
                                keys,
                            )
                            _owner_chain(owner, depth + 1, seen)
                elif type(ref).__name__ not in ("frame", "list", "tuple", "cell", "function"):
                    print("  " * depth, "ref", type(ref).__module__, type(ref).__qualname__)

        for obj in gc.get_objects():
            if isinstance(obj, duckdb.DuckDBPyConnection):
                try:
                    dbs = obj.execute(
                        "SELECT database_name, type, path FROM duckdb_databases() WHERE NOT internal"
                    ).fetchall()
                except Exception as exc:  # diagnostic only
                    dbs = f"closed? {exc}"
                print("=== DIAG duckdb connection", id(obj), dbs)
                _owner_chain(obj)
    server.dispose()
    assert left == [], f"backends still connected after shutdown: {left}"
