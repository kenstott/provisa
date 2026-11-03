# Copyright (c) 2026 Kenneth Stott
# Canary: e9e32df5-6fad-4fe6-a6b6-af06d4b240ca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PGWIRE'S CATALOG FUNCTIONS (REQ-2167, amended 2026-10-27), part 3: the server functions.

pg_backend_pid is the connection's own process id; pg_cancel_backend and pg_terminate_backend act
on a pgwire session the caller owns, or any session of the caller's org for a caller with the
user_management right; the advisory locks are PostgreSQL's, on this server's sessions, and refused
by name in a statement that does more than call one. The other server functions answer as
PostgreSQL does (test_server_answers)."""

from __future__ import annotations

import asyncio
import base64
import socket
import threading
import time
from unittest.mock import MagicMock, patch

import asyncpg
import pytest
import pytest_asyncio

from provisa.pgwire.server import ProvisaConnection, ProvisaServer

_USERS = {"alice": "analyst", "bob": "analyst", "root": "org_admin"}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _provider():
    from provisa.auth.models import AuthIdentity

    class _Stub:
        auth_scheme = "basic"

        @property
        def token_validators(self):
            return {"basic": self._validate}

        async def _validate(self, token: str) -> AuthIdentity:
            username, password = base64.b64decode(token).decode().split(":", 1)
            if username not in _USERS or password != "secret":
                raise ValueError("Invalid credentials")
            return AuthIdentity(
                user_id=username,
                email=None,
                display_name=username,
                roles=[_USERS[username]],
                raw_claims={"sub": username},
            )

    return _Stub()


@pytest_asyncio.fixture(scope="module")
async def port():
    port = _free_port()
    server = ProvisaServer(("127.0.0.1", port), ProvisaConnection(), ssl_ctx=None)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    time.sleep(0.1)
    yield port
    server.shutdown()


@pytest.fixture
def state():
    ctx = MagicMock()
    ctx.tables = {}
    state = MagicMock()
    state.contexts = {"analyst": ctx, "org_admin": ctx}
    state.roles = {
        "analyst": {"id": "analyst", "capabilities": ["query_development"]},
        "org_admin": {"id": "org_admin", "capabilities": ["user_management"]},
    }
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = {"provider": "simple", "default_role": "analyst", "role_mapping": []}
    state.auth_middleware_active = True
    state.multitenancy = False
    state.admin_db = None
    return state


async def _connect(port: int, user: str):
    return await asyncpg.connect(
        host="127.0.0.1", port=port, user=user, password="secret", database="provisa"
    )


@pytest.fixture
def served(state):
    with (
        patch("provisa.auth.wiring.build_auth_provider", return_value=_provider()),
        patch("provisa.api.app.state", state),
    ):
        yield


@pytest.mark.asyncio
async def test_pg_backend_pid_is_the_connections_own(port, served):
    a, b = await _connect(port, "alice"), await _connect(port, "alice")
    try:
        pid_a, pid_b = (
            await a.fetchval("SELECT pg_backend_pid()"),
            await b.fetchval("SELECT pg_catalog.pg_backend_pid()"),
        )
        assert 0 < pid_a < 2**31 and 0 < pid_b < 2**31 and pid_a != pid_b
        assert await a.fetchval("SELECT pg_backend_pid()") == pid_a
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_a_session_of_ones_own_is_terminated(port, served):
    a, b = await _connect(port, "alice"), await _connect(port, "alice")
    pid_b = await b.fetchval("SELECT pg_backend_pid()")
    try:
        assert await a.fetchval(f"SELECT pg_terminate_backend({pid_b})") is True
        with pytest.raises(
            (asyncpg.InterfaceError, asyncpg.ConnectionDoesNotExistError, ConnectionError)
        ):
            await asyncio.wait_for(b.fetchval("SELECT pg_backend_pid()"), 5)
        assert await a.fetchval("SELECT pg_terminate_backend(2147483000)") is False
    finally:
        await a.close()
        b.terminate()


@pytest.mark.asyncio
async def test_another_users_session_is_refused_by_name_and_open_to_user_management(port, served):
    a, bob, root = (
        await _connect(port, "alice"),
        await _connect(port, "bob"),
        await _connect(port, "root"),
    )
    pid_a = await a.fetchval("SELECT pg_backend_pid()")
    try:
        with pytest.raises(asyncpg.InsufficientPrivilegeError, match="pg_cancel_backend"):
            await bob.fetchval(f"SELECT pg_cancel_backend({pid_a})")
        with pytest.raises(asyncpg.InsufficientPrivilegeError, match="pg_terminate_backend"):
            await bob.fetchval(f"SELECT pg_terminate_backend({pid_a})")
        # Nothing is running on alice's connection: a cancel ends nothing and is answered true.
        assert await root.fetchval(f"SELECT pg_cancel_backend({pid_a})") is True
        assert await a.fetchval("SELECT pg_backend_pid()") == pid_a
    finally:
        for c in (a, bob, root):
            await c.close()


@pytest.mark.asyncio
async def test_a_lock_whose_key_is_no_constant_is_refused_by_name(port, served):
    a = await _connect(port, "alice")
    try:
        for sql in (
            # A key is evaluated outside governance: nothing may be read for one.
            "SELECT pg_advisory_lock((SELECT count(*) FROM read_text('/etc/hosts')))",
            "SELECT pg_advisory_unlock(length(version()))",
        ):
            with pytest.raises(asyncpg.FeatureNotSupportedError, match="pg_"):
                await a.fetchval(sql)
    finally:
        await a.close()


_LOCKS_HELD = (
    "SELECT classid, objid, objsubid, mode, granted FROM pg_catalog.pg_locks "
    "WHERE locktype = 'advisory' AND pid = pg_catalog.pg_backend_pid() "
    "ORDER BY objsubid, classid, objid"
)


@pytest.mark.asyncio
async def test_a_lock_in_a_larger_statement_is_taken_as_postgresql_takes_it(port, served):
    a, b = await _connect(port, "alice"), await _connect(port, "alice")
    held = (
        "(SELECT count(*) FROM pg_catalog.pg_locks WHERE locktype = 'advisory' "
        "AND pid = pg_catalog.pg_backend_pid())"
    )
    try:
        both = f"SELECT pg_advisory_lock(7101) AS a, pg_advisory_lock(0, 7102) AS b, {held} AS n"
        assert tuple(await a.fetchrow(both)) == ("", "", 2)
        tried = f"SELECT pg_try_advisory_lock(7103) AS l, {held} AS n"
        assert tuple(await a.fetchrow(tried)) == (True, 3)
        assert await b.fetchval("SELECT pg_try_advisory_lock(7101)") is False
        assert await a.fetchval("SELECT pg_advisory_unlock_all()") is None
        assert await b.fetchval("SELECT pg_try_advisory_lock(7101)") is True
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_pg_locks_lists_the_sessions_advisory_locks_as_postgresql_does(port, served):
    a = await _connect(port, "alice")
    try:
        # A bigint key is split into its high and low halves (objsubid 1); two integers are
        # listed as they are (objsubid 2). Each mode a session holds is one row.
        await a.execute("SELECT pg_advisory_lock(4242::bigint)")
        await a.execute("SELECT pg_advisory_lock(4242::bigint)")
        await a.execute("SELECT pg_advisory_lock_shared(42, 42)")
        await a.execute("SELECT pg_advisory_lock(8589934593)")
        assert [tuple(r) for r in await a.fetch(_LOCKS_HELD)] == [
            (0, 4242, 1, "ExclusiveLock", True),
            (2, 1, 1, "ExclusiveLock", True),
            (42, 42, 2, "ShareLock", True),
        ]
        await a.execute("SELECT pg_advisory_unlock_all()")
        assert await a.fetch(_LOCKS_HELD) == []
    finally:
        await a.close()


@pytest.mark.asyncio
async def test_a_transaction_lock_is_held_for_the_rest_of_its_statement(port, served):
    a = await _connect(port, "alice")
    try:
        for call in (
            "pg_advisory_xact_lock(4242::bigint)",
            "pg_try_advisory_xact_lock_shared(42, 42)",
        ):
            counted = (
                "SELECT (SELECT count(*) FROM pg_catalog.pg_locks WHERE locktype = 'advisory' "
                "AND pid = pg_catalog.pg_backend_pid()) AS v "
                f"FROM (SELECT pg_catalog.{call} OFFSET 0) s"
            )
            assert await a.fetchval(counted) == 1
            # The statement was its transaction: the lock ended with it.
            assert await a.fetch(_LOCKS_HELD) == []
    finally:
        await a.close()


@pytest.mark.parametrize(
    "call",
    [
        "pg_sleep((SELECT count(*) FROM read_text('/etc/hosts')))",
        "pg_sleep_for((SELECT '1 second'::interval FROM read_text('/etc/hosts')))",
    ],
)
@pytest.mark.asyncio
async def test_a_sleep_reads_nothing_for_its_duration(port, served, call):
    a = await _connect(port, "alice")
    try:
        with pytest.raises(asyncpg.FeatureNotSupportedError, match="not a constant expression"):
            await a.fetchval(f"SELECT {call}")
    finally:
        await a.close()


@pytest.mark.asyncio
async def test_an_advisory_lock_excludes_another_session_and_ends_with_its_own(port, served):
    a, b = await _connect(port, "alice"), await _connect(port, "alice")
    try:
        assert await a.fetchval("SELECT pg_try_advisory_lock(7001)") is True
        assert await b.fetchval("SELECT pg_try_advisory_lock(7001)") is False
        # Its two-argument key is another lock; a shared lock conflicts with the exclusive one.
        assert await b.fetchval("SELECT pg_try_advisory_lock(0, 7001)") is True
        assert await b.fetchval("SELECT pg_try_advisory_lock_shared(7001)") is False
        # Re-entrant: held twice, released twice.
        assert await a.fetchval("SELECT pg_try_advisory_lock(7001)") is True
        assert await a.fetchval("SELECT pg_advisory_unlock(7001)") is True
        assert await b.fetchval("SELECT pg_try_advisory_lock(7001)") is False
        assert await a.fetchval("SELECT pg_advisory_unlock(7001)") is True
        assert await b.fetchval("SELECT pg_try_advisory_lock(7001)") is True
        # Nothing held by a: false, with PostgreSQL's warning.
        assert await a.fetchval("SELECT pg_advisory_unlock(7001)") is False
    finally:
        await a.close()
        await b.close()
    c = await _connect(port, "alice")
    try:
        assert await c.fetchval("SELECT pg_try_advisory_lock(7001)") is True  # b's ended with it
        assert await c.fetchval("SELECT pg_advisory_unlock_all()") is None
        assert await c.fetchval("SELECT pg_advisory_unlock(7001)") is False
    finally:
        await c.close()


@pytest.mark.asyncio
async def test_shared_advisory_locks_share(port, served):
    a, b = await _connect(port, "alice"), await _connect(port, "alice")
    try:
        assert await a.fetchval("SELECT pg_try_advisory_lock_shared(7002)") is True
        assert await b.fetchval("SELECT pg_try_advisory_lock_shared(7002)") is True
        assert await b.fetchval("SELECT pg_try_advisory_lock(7002)") is False
        assert await a.fetchval("SELECT pg_advisory_unlock_shared(7002)") is True
        assert await b.fetchval("SELECT pg_try_advisory_lock(7002)") is True
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_a_transaction_lock_ends_with_its_transaction(port, served):
    a, b = await _connect(port, "alice"), await _connect(port, "alice")
    try:
        await a.execute("BEGIN")
        assert await a.fetchval("SELECT pg_try_advisory_xact_lock(7003)") is True
        assert await b.fetchval("SELECT pg_try_advisory_lock(7003)") is False
        await a.execute("COMMIT")
        assert await b.fetchval("SELECT pg_try_advisory_lock(7003)") is True
        # No block: the statement was its transaction, and the lock ended with it.
        assert await b.fetchval("SELECT pg_advisory_unlock(7003)") is True
        assert await a.fetchval("SELECT pg_try_advisory_xact_lock(7003)") is True
        assert await b.fetchval("SELECT pg_try_advisory_lock(7003)") is True
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_a_blocking_lock_waits_for_the_holder(port, served):
    a, b = await _connect(port, "alice"), await _connect(port, "alice")
    try:
        assert await a.fetchval("SELECT pg_try_advisory_lock(7004)") is True
        waiting = asyncio.create_task(b.fetchval("SELECT pg_advisory_lock(7004)"))
        await asyncio.sleep(0.2)
        assert not waiting.done()
        assert await a.fetchval("SELECT pg_advisory_unlock(7004)") is True
        await asyncio.wait_for(waiting, 5)
        assert await a.fetchval("SELECT pg_try_advisory_lock(7004)") is False
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_a_lock_is_taken_once_with_the_binds_key_and_a_null_key_takes_none(port, served):
    a, b = await _connect(port, "alice"), await _connect(port, "alice")
    try:
        # A prepared statement: Describe's example value takes no lock, the Bind's does.
        assert await a.fetchval("SELECT pg_try_advisory_lock($1)", 7005) is True
        assert await b.fetchval("SELECT pg_try_advisory_lock(7005)") is False
        assert await a.fetchval("SELECT pg_advisory_unlock($1)", 7005) is True
        assert await a.fetchval("SELECT pg_advisory_unlock(7005)") is False
        assert await a.fetchval("SELECT pg_try_advisory_lock(NULL::bigint)") is None
        assert await a.fetchval("SELECT pg_advisory_lock(NULL::bigint)") is None
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_a_sessions_own_setting_is_kept_as_postgresql_keeps_it(port, served):
    a, b = await _connect(port, "alice"), await _connect(port, "alice")
    try:
        assert (
            await a.fetchval("SELECT pg_catalog.set_config('conformance.probe', 'on', false)")
            == "on"
        )
        assert await a.fetchval("SELECT pg_catalog.current_setting('conformance.probe')") == "on"
        assert (
            await b.fetchval("SELECT pg_catalog.current_setting('conformance.probe', true)") is None
        )
        # Local to a transaction it was not in: it ends with the statement.
        await a.fetchval("SELECT pg_catalog.set_config('conformance.other', 'x', true)")
        assert (
            await a.fetchval("SELECT pg_catalog.current_setting('conformance.other', true)") is None
        )
    finally:
        await a.close()
        await b.close()


@pytest.mark.asyncio
async def test_an_unknown_setting_is_null_where_missing_ok(port, served):
    a = await _connect(port, "alice")
    try:
        sql = "SELECT pg_catalog.\"current_setting\"('no_such.setting'::text, true::boolean) AS v"
        assert await a.fetchval(sql) is None
    finally:
        await a.close()


@pytest.mark.parametrize(
    "call",
    [
        "pg_catalog.pg_sleep_until(pg_catalog.clock_timestamp() + '0.2 seconds'::interval)",
        "pg_sleep_until(statement_timestamp() + '0.2 seconds'::interval)",
        "pg_sleep_for(transaction_timestamp() - now() + '0.2 seconds'::interval)",
    ],
)
@pytest.mark.asyncio
async def test_a_sleep_until_a_time_read_from_the_clock_sleeps_until_then(port, served, call):
    a = await _connect(port, "alice")
    try:
        started = time.monotonic()
        # void, which asyncpg reads as None from PostgreSQL too.
        assert await a.fetchval(f"SELECT {call} AS v") is None
        assert time.monotonic() - started >= 0.2 - 0.01  # the client's clock holds the sleep
    finally:
        await a.close()
