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
    server.dispose()
    assert left == [], f"backends still connected after shutdown: {left}"
