# Copyright (c) 2026 Kenneth Stott
# Canary: a7b8c9d0-e1f2-3456-0123-678901234567
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Phase 1 wire protocol integration tests.

Spins up a ProvisaServer on a random port (no TLS, no full app stack),
connects with asyncpg, and verifies the PG wire protocol handshake,
SELECT 1, catalog queries, and auth rejection.

The auth provider is stubbed so no real bcrypt/DB is needed.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time
from unittest.mock import MagicMock, patch

import asyncpg
import pytest
import pytest_asyncio

from provisa.executor.result import QueryResult as EngineResult
from provisa.pgwire.server import ProvisaConnection, ProvisaServer


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_server(port: int) -> ProvisaServer:
    conn = ProvisaConnection()
    server = ProvisaServer(("127.0.0.1", port), conn, ssl_ctx=None)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.1)
    return server


def _stub_auth_provider(valid_user: str, valid_password: str):
    provider = MagicMock()

    def _login(username, password):
        if username == valid_user and password == valid_password:
            return username
        raise ValueError("Invalid credentials")

    provider.login.side_effect = _login
    return provider


@pytest_asyncio.fixture(scope="module")
async def pgwire_server():
    import provisa.pgwire.server as _srv

    loop = asyncio.get_running_loop()
    with _srv._loop_lock:
        _srv._loop = loop
    port = _free_port()
    server = _make_server(port)
    yield port
    server.shutdown()
    with _srv._loop_lock:
        _srv._loop = None


@pytest.fixture(scope="module")
def mock_state():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.contexts = {"alice": ctx}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = {"provider": "simple"}
    state.auth_middleware_active = True
    return state


@pytest.mark.asyncio
async def test_select_1(pgwire_server, mock_state):
    port = pgwire_server
    provider = _stub_auth_provider("alice", "secret")

    async def _stub_pipeline(sql, role_id):
        return EngineResult(rows=[(1,)], column_names=["?column?"])

    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", _stub_pipeline),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=port,
            user="alice",
            password="secret",
            database="provisa",
        )
        result = await conn.fetchval("SELECT 1")
        await conn.close()
    assert result == 1


@pytest.mark.asyncio
async def test_wrong_password_raises(pgwire_server, mock_state):
    port = pgwire_server
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state),
    ):
        with pytest.raises(asyncpg.InvalidPasswordError):
            await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="alice",
                password="wrong",
                database="provisa",
            )


@pytest.mark.asyncio
async def test_none_provider_trust_mode(pgwire_server):
    """provider=none: any username accepted, password ignored, username becomes role_id."""
    port = pgwire_server
    trust_state = MagicMock()
    ctx = MagicMock()
    ctx.tables = {}
    trust_state.contexts = {"analyst": ctx}
    trust_state.schema_build_cache = {"column_types": {}}
    trust_state.auth_config = {"provider": "none"}
    trust_state.auth_middleware_active = False

    async def _stub_pipeline(sql, role_id):
        from provisa.executor.result import QueryResult as EngineResult

        return EngineResult(rows=[(role_id,)], column_names=["role"])

    with (
        patch("provisa.api.app.state", trust_state),
        patch("provisa.pgwire._pipeline.govern_pgwire_plan", _stub_pipeline),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=port,
            user="analyst",
            password="ignored",
            database="provisa",
        )
        row = await conn.fetchrow("SELECT 1")
        await conn.close()
    assert row is not None


@pytest.mark.asyncio
async def test_show_server_version(pgwire_server, mock_state):
    port = pgwire_server
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=port,
            user="alice",
            password="secret",
            database="provisa",
        )
        row = await conn.fetchrow("SHOW server_version")
        await conn.close()
    assert row is not None
    assert "provisa" in str(row[0])


@pytest.mark.asyncio
async def test_catalog_pg_namespace(pgwire_server, mock_state):
    port = pgwire_server
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=port,
            user="alice",
            password="secret",
            database="provisa",
        )
        rows = await conn.fetch("SELECT nspname FROM pg_catalog.pg_namespace")
        await conn.close()
    ns_names = [r["nspname"] for r in rows]
    assert "public" in ns_names
    assert "pg_catalog" in ns_names


@pytest.mark.asyncio
async def test_set_does_not_raise(pgwire_server, mock_state):
    port = pgwire_server
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=port,
            user="alice",
            password="secret",
            database="provisa",
        )
        status = await conn.execute("SET search_path TO public")
        await conn.close()
    assert status == "SET"


@pytest.mark.asyncio
async def test_multi_statement(pgwire_server, mock_state):
    port = pgwire_server
    provider = _stub_auth_provider("alice", "secret")
    with (
        patch("provisa.auth.providers.simple._provider_instance", provider),
        patch("provisa.api.app.state", mock_state),
    ):
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=port,
            user="alice",
            password="secret",
            database="provisa",
        )
        status = await conn.execute("BEGIN; COMMIT")
        await conn.close()
    assert status == "COMMIT"
