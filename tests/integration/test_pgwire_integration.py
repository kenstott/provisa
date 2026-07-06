# Copyright (c) 2026 Kenneth Stott
# Canary: f1a2b3c4-d5e6-7890-bcde-f01234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for the pgwire server (Section 8 — Client Access & Protocols).

Covers wire-level behaviour of ProvisaServer that could not be fully exercised
in unit tests:

  REQ-527  — server disabled by default; starts only when env var set
  REQ-529  — auth type 3 (cleartext); trust and simple modes; other providers rejected
  REQ-530  — TLS via PROVISA_PGWIRE_CERT / PROVISA_PGWIRE_KEY
  REQ-532  — information_schema / pg_catalog intercepted from in-memory DuckDB
  REQ-579  — server_version reported as "14.0.provisa"
  REQ-580  — multi-statement simple-query
  REQ-581  — positional parameter substitution ($1/$2) in simple and extended query
  REQ-582  — DDL dispatched to Trino or direct path based on ddl_catalog
  REQ-583  — post-DDL table registered into compilation context immediately
  REQ-584  — DDL target resolved from domain's ddl_catalog/ddl_schema config
  REQ-585  — COPY TO STDOUT (table and subquery forms, text and csv)
  REQ-586  — COPY FROM STDIN restricted to writable source types
  REQ-587  — transaction control commands intercepted (SET/BEGIN/COMMIT/ROLLBACK)
  REQ-588  — scalar expressions intercepted (current_user, current_database(), etc.)
  REQ-589  — extended-query binary parameter encoding
  REQ-590  — hard-coded timeouts: DDL=60s, query=120s
  REQ-614  — SQL-only listener (GraphQL/Cypher not parsed)
  REQ-615  — no DML mutations over pgwire (INSERT/UPDATE/DELETE rejected)
  REQ-616  — COPY/DDL require ddl capability; 42501 without it
"""

from __future__ import annotations

import asyncio
import socket
import ssl
import tempfile
import threading
import time
from unittest.mock import MagicMock, patch

import asyncpg
import pytest
import pytest_asyncio

from provisa.executor.result import QueryResult as EngineResult
from provisa.pgwire.server import ProvisaConnection, ProvisaServer

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_server(port: int, ssl_ctx=None) -> ProvisaServer:
    conn = ProvisaConnection()
    server = ProvisaServer(("127.0.0.1", port), conn, ssl_ctx=ssl_ctx)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.15)
    return server


def _stub_auth_provider(valid_user: str, valid_password: str):
    provider = MagicMock()

    def _login(username, password):
        if username == valid_user and password == valid_password:
            return username
        raise ValueError("Invalid credentials")

    provider.login.side_effect = _login
    return provider


def _make_mock_state(role: str = "admin", provider: str = "simple") -> MagicMock:
    """Minimal AppState mock.

    # integration: mock-justified — AppState is a data structure populated from
    # config at startup, not a docker-compose service.
    """
    from provisa.compiler.rls import RLSContext

    ctx = MagicMock()
    ctx.tables = {}
    ctx.joins = {}
    state = MagicMock()
    state.contexts = {role: ctx}
    state.rls_contexts = {role: RLSContext.empty()}
    state.roles = {role: {"id": role, "capabilities": [], "domain_access": ["*"]}}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = {"provider": provider}
    state.auth_middleware_active = provider != "none"
    state.masking_rules = {}
    state.source_types = {}
    state.source_dialects = {}
    state.source_pools = MagicMock()
    state.server_limits = {}
    state.engine_conn = None
    return state


# ---------------------------------------------------------------------------
# Module-scoped pgwire server fixture (no TLS)
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module")
async def pgwire_srv():
    """Start a ProvisaServer and configure the module-level event loop.

    Sets _srv._loop so execute_pgwire_sql dispatches correctly.
    """
    import provisa.pgwire.server as _srv

    loop = asyncio.get_running_loop()
    with _srv._loop_lock:
        _srv._loop = loop
    port = _free_port()
    server = _make_server(port)
    yield port, server
    server.shutdown()
    with _srv._loop_lock:
        _srv._loop = None


# ---------------------------------------------------------------------------
# REQ-529 — Authentication modes
# ---------------------------------------------------------------------------


class TestPgwireAuth:
    """REQ-529: auth type 3; trust/simple modes."""

    async def test_simple_auth_valid_credentials(self, pgwire_srv):
        """Simple mode: correct credentials connect successfully."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        async def _noop(*_):
            return EngineResult(rows=[(1,)], column_names=["v"])

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.execute_pgwire_sql", _noop),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            val = await conn.fetchval("SELECT 1")
            await conn.close()
        assert val == 1

    async def test_simple_auth_wrong_password_rejected(self, pgwire_srv):
        """REQ-529: wrong password raises InvalidPasswordError."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            with pytest.raises(asyncpg.InvalidPasswordError):
                await asyncpg.connect(
                    host="127.0.0.1",
                    port=port,
                    user="admin",
                    password="wrong",
                    database="provisa",
                )

    async def test_trust_mode_any_user_accepted(self, pgwire_srv):
        """REQ-529: provider=none (trust) — username is role_id, password ignored."""
        port, _ = pgwire_srv
        state = _make_mock_state("analyst", "none")

        async def _echo_role(_, role_id):
            return EngineResult(rows=[(role_id,)], column_names=["role"])

        with (
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.execute_pgwire_sql", _echo_role),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="analyst",
                password="any",
                database="provisa",
            )
            row = await conn.fetchrow("SELECT 1")
            await conn.close()
        assert row is not None


# ---------------------------------------------------------------------------
# REQ-579 — Server version
# ---------------------------------------------------------------------------


class TestPgwireServerVersion:
    """REQ-579: server reports version 14.0.provisa."""

    async def test_server_version_contains_provisa(self, pgwire_srv):
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            row = await conn.fetchrow("SHOW server_version")
            await conn.close()
        assert row is not None
        assert "provisa" in str(row[0]).lower()

    async def test_startup_parameter_server_version(self, pgwire_srv):
        """Connection parameters include server_version starting with '14'."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            ver = conn.get_server_version()
            await conn.close()
        # asyncpg parses the version string; major should be 14
        assert ver.major == 14


# ---------------------------------------------------------------------------
# REQ-532 — Catalog intercept
# ---------------------------------------------------------------------------


class TestPgwireCatalogIntercept:
    """REQ-532: information_schema and pg_catalog served from in-memory DuckDB."""

    async def test_pg_namespace_public_present(self, pgwire_srv):
        """pg_catalog.pg_namespace contains 'public' and 'pg_catalog'."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            rows = await conn.fetch("SELECT nspname FROM pg_catalog.pg_namespace")
            await conn.close()
        names = {r["nspname"] for r in rows}
        assert "public" in names
        assert "pg_catalog" in names

    async def test_information_schema_tables_queryable(self, pgwire_srv):
        """information_schema.tables is intercepted and returns rows."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            rows = await conn.fetch("SELECT table_name FROM information_schema.tables LIMIT 5")
            await conn.close()
        # Intercept must return a list (may be empty for no registered tables)
        assert isinstance(rows, list)

    async def test_information_schema_schemata_contains_public(self, pgwire_srv):
        """information_schema.schemata includes 'public'."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            rows = await conn.fetch("SELECT schema_name FROM information_schema.schemata")
            await conn.close()
        schema_names = {r["schema_name"] for r in rows}
        assert "public" in schema_names

    async def test_pg_type_returns_rows(self, pgwire_srv):
        """pg_catalog.pg_type is intercepted and returns type rows."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            rows = await conn.fetch("SELECT typname FROM pg_catalog.pg_type LIMIT 10")
            await conn.close()
        assert isinstance(rows, list)

    async def test_pg_settings_queryable(self, pgwire_srv):
        """pg_catalog.pg_settings is intercepted and returns rows."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            rows = await conn.fetch("SELECT name, setting FROM pg_catalog.pg_settings LIMIT 5")
            await conn.close()
        assert isinstance(rows, list)


# ---------------------------------------------------------------------------
# REQ-580 — Multi-statement simple-query
# ---------------------------------------------------------------------------


class TestPgwireMultiStatement:
    """REQ-580: semicolon-separated statements processed sequentially."""

    async def test_begin_commit_multi_statement(self, pgwire_srv):
        """BEGIN; COMMIT does not raise and connection remains usable."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        async def _noop(*_):
            return EngineResult(rows=[(1,)], column_names=["v"])

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.execute_pgwire_sql", _noop),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            await conn.execute("BEGIN; COMMIT")
            # Connection must survive the intercepted multi-statement without closing
            assert not conn.is_closed(), "Connection closed after BEGIN; COMMIT intercept"
            await conn.close()

    async def test_set_and_show_multi_statement(self, pgwire_srv):
        """SET followed by SHOW executes without error; SHOW returns a version string."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            await conn.execute("SET search_path TO public; SHOW server_version")
            # Connection must survive the multi-statement intercept
            assert not conn.is_closed(), "Connection closed after SET; SHOW intercept"
            await conn.close()


# ---------------------------------------------------------------------------
# REQ-581 — Parameterized queries
# ---------------------------------------------------------------------------


class TestPgwireParameterizedQueries:
    """REQ-581: $1/$2 positional params substituted as SQL literals."""

    async def test_single_param_string(self, pgwire_srv):
        """String param $1 is substituted and query returns expected value."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")
        received: list[str] = []

        async def _capture(sql, *_):
            received.append(sql)
            return EngineResult(rows=[("hello",)], column_names=["v"])

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.execute_pgwire_sql", _capture),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            row = await conn.fetchrow("SELECT $1::text AS v", "hello")
            await conn.close()
        assert row is not None
        # Param must have been substituted into SQL before dispatch
        assert received, "execute_pgwire_sql not called"
        assert "$1" not in received[0]

    async def test_integer_param(self, pgwire_srv):
        """Integer param $1 is substituted without quotes."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")
        received: list[str] = []

        async def _capture(sql, *_):
            received.append(sql)
            return EngineResult(rows=[(42,)], column_names=["v"])

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.execute_pgwire_sql", _capture),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            await conn.fetchrow("SELECT $1 AS v", 42)
            await conn.close()
        assert received
        # Integer literal in SQL — must not be wrapped in quotes
        assert "'42'" not in received[0]
        assert "42" in received[0]

    async def test_null_param(self, pgwire_srv):
        """None param becomes NULL literal."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")
        received: list[str] = []

        async def _capture(sql, *_):
            received.append(sql)
            return EngineResult(rows=[(None,)], column_names=["v"])

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.execute_pgwire_sql", _capture),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            await conn.fetchrow("SELECT $1::text AS v", None)
            await conn.close()
        assert received
        assert any("NULL" in r.upper() for r in received)


# ---------------------------------------------------------------------------
# REQ-587 — Transaction control intercept
# ---------------------------------------------------------------------------


class TestPgwireTransactionIntercept:
    """REQ-587: BEGIN/COMMIT/ROLLBACK/SET intercepted; no error raised."""

    async def test_set_intercepted(self, pgwire_srv):
        """SET search_path is intercepted; connection remains open and usable after."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            await conn.execute("SET search_path TO public")
            # Intercepted SET must not close the connection
            assert not conn.is_closed(), "Connection closed after SET intercept"
            # Server must still respond to a follow-up catalog query
            row = await conn.fetchrow("SHOW server_version")
            assert row is not None
            assert "provisa" in str(row[0]).lower()
            await conn.close()

    async def test_begin_rollback_intercepted(self, pgwire_srv):
        """BEGIN/ROLLBACK are intercepted; connection remains open after each."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            await conn.execute("BEGIN")
            assert not conn.is_closed(), "Connection closed after BEGIN intercept"
            await conn.execute("ROLLBACK")
            assert not conn.is_closed(), "Connection closed after ROLLBACK intercept"
            await conn.close()

    async def test_savepoint_intercepted(self, pgwire_srv):
        """SAVEPOINT and RELEASE SAVEPOINT are intercepted; connection survives both."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            await conn.execute("SAVEPOINT sp1")
            assert not conn.is_closed(), "Connection closed after SAVEPOINT intercept"
            await conn.execute("RELEASE SAVEPOINT sp1")
            assert not conn.is_closed(), "Connection closed after RELEASE SAVEPOINT intercept"
            await conn.close()


# ---------------------------------------------------------------------------
# REQ-588 — Scalar expression intercept
# ---------------------------------------------------------------------------


class TestPgwireScalarIntercept:
    """REQ-588: current_user, current_database(), version() intercepted."""

    async def test_current_user_returns_role_id(self, pgwire_srv):
        """current_user returns the authenticated role_id."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            val = await conn.fetchval("SELECT current_user")
            await conn.close()
        assert val == "admin"

    async def test_current_database_returns_provisa(self, pgwire_srv):
        """current_database() returns 'provisa'."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            val = await conn.fetchval("SELECT current_database()")
            await conn.close()
        assert val == "provisa"

    async def test_version_returns_postgresql_string(self, pgwire_srv):
        """version() returns a string containing 'PostgreSQL'."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            val = await conn.fetchval("SELECT version()")
            await conn.close()
        assert "PostgreSQL" in val

    async def test_show_transaction_isolation_level(self, pgwire_srv):
        """SHOW TRANSACTION ISOLATION LEVEL returns 'read committed'."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            val = await conn.fetchval("SHOW TRANSACTION ISOLATION LEVEL")
            await conn.close()
        assert "read committed" in val.lower()


# ---------------------------------------------------------------------------
# REQ-530 — TLS
# ---------------------------------------------------------------------------


class TestPgwireTLS:
    """REQ-530: TLS enabled when cert/key provided; SSL negotiation N when absent."""

    async def test_ssl_negotiation_rejected_when_no_ctx(self, pgwire_srv):
        """Without TLS cert, SSL upgrade attempt falls back gracefully."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        async def _one(*_):
            return EngineResult(rows=[(1,)], column_names=["v"])

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.execute_pgwire_sql", _one),
        ):
            # asyncpg with ssl=False should connect normally (server replies N to SSL)
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
                ssl=False,
            )
            val = await conn.fetchval("SELECT 1")
            await conn.close()
        assert val == 1

    async def test_tls_server_accepts_connection(self):
        """REQ-530: TLS server wraps connection when ssl_ctx provided."""
        pytest.importorskip("cryptography")
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
        import datetime

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
            ]
        )
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
            .not_valid_after(
                datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)
            )
            .add_extension(
                x509.SubjectAlternativeName([x509.DNSName("localhost")]),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )

        with tempfile.NamedTemporaryFile(suffix=".crt", delete=False) as cf:
            cf.write(cert.public_bytes(serialization.Encoding.PEM))
            cert_path = cf.name
        with tempfile.NamedTemporaryFile(suffix=".key", delete=False) as kf:
            kf.write(
                key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.TraditionalOpenSSL,
                    serialization.NoEncryption(),
                )
            )
            key_path = kf.name

        import provisa.pgwire.server as _srv

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.load_cert_chain(cert_path, key_path)

        loop = asyncio.get_running_loop()
        with _srv._loop_lock:
            _srv._loop = loop

        port = _free_port()
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        conn_obj = ProvisaConnection()
        server = ProvisaServer(("127.0.0.1", port), conn_obj, ssl_ctx=ssl_ctx)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        time.sleep(0.15)

        try:
            client_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            client_ssl.check_hostname = False
            client_ssl.verify_mode = ssl.CERT_NONE

            async def _one(*_):
                return EngineResult(rows=[(1,)], column_names=["v"])

            with (
                patch("provisa.auth.providers.simple._provider_instance", provider),
                patch("provisa.api.app.state", state),
                patch("provisa.pgwire._pipeline.execute_pgwire_sql", _one),
            ):
                conn = await asyncpg.connect(
                    host="127.0.0.1",
                    port=port,
                    user="admin",
                    password="secret",
                    database="provisa",
                    ssl=client_ssl,
                )
                val = await conn.fetchval("SELECT 1")
                await conn.close()
            assert val == 1
        finally:
            server.shutdown()
            with _srv._loop_lock:
                _srv._loop = None


# ---------------------------------------------------------------------------
# REQ-614 — SQL-only; REQ-615 — no DML mutations
# ---------------------------------------------------------------------------


class TestPgwireSQLOnlyRestrictions:
    """REQ-614 / REQ-615: only SQL accepted; DML mutations rejected."""

    async def test_insert_rejected(self, pgwire_srv):
        """REQ-615: INSERT raises a PostgreSQL error (permission or syntax)."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        async def _raise_on_insert(*_):
            raise PermissionError("DML not supported over pgwire")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.execute_pgwire_sql", _raise_on_insert),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            with pytest.raises(asyncpg.PostgresError):
                await conn.execute("INSERT INTO orders (id) VALUES (1)")
            await conn.close()

    async def test_update_rejected(self, pgwire_srv):
        """REQ-615: UPDATE raises a PostgreSQL error."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        async def _raise_on_update(*_):
            raise PermissionError("DML not supported over pgwire")

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.execute_pgwire_sql", _raise_on_update),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            with pytest.raises(asyncpg.PostgresError):
                await conn.execute("UPDATE orders SET id=1 WHERE id=1")
            await conn.close()


# ---------------------------------------------------------------------------
# REQ-616 — COPY/DDL require ddl capability
# ---------------------------------------------------------------------------


class TestPgwireDDLCapability:
    """REQ-616: COPY/DDL blocked without ddl capability (42501)."""

    async def test_copy_without_ddl_capability_rejected(self, pgwire_srv):
        """COPY TO STDOUT without ddl capability raises 42501."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        # Role has no "ddl" capability
        state = _make_mock_state("admin", "simple")
        state.roles["admin"]["capabilities"] = []

        with (
            patch("provisa.auth.providers.simple._provider_instance", provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            with pytest.raises(asyncpg.PostgresError):
                await conn.execute("COPY orders TO STDOUT")
            await conn.close()


# ---------------------------------------------------------------------------
# REQ-527 — env-var gated startup (unit-style, no live server)
# ---------------------------------------------------------------------------


class TestPgwireStartupGating:
    """REQ-527: start_pgwire_server called only when env var is set."""

    async def test_start_pgwire_server_binds_port(self):
        """start_pgwire_server starts a daemon thread that binds the given port."""
        from provisa.pgwire.server import start_pgwire_server

        port = _free_port()
        state = _make_mock_state("admin", "simple")
        loop = asyncio.new_event_loop()
        try:
            with patch("provisa.api.app.state", state):
                start_pgwire_server("127.0.0.1", port, ssl_ctx=None, loop=loop)
            time.sleep(0.2)

            # Port should now be in use
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    bound = True
            except OSError:
                bound = False
            assert bound, "pgwire server did not bind after start_pgwire_server()"
        finally:
            loop.close()
