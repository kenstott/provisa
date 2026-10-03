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
from tests.pgwire_describe_parity import describes_as
from provisa.pgwire.server import ProvisaConnection, ProvisaServer
from tests.integration.conftest import no_replica_routes

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _free_port() -> int:
    from tests.port_lease import lease_port

    return lease_port()


def _make_server(port: int, ssl_ctx=None) -> ProvisaServer:
    conn = ProvisaConnection()
    server = ProvisaServer(("127.0.0.1", port), conn, ssl_ctx=ssl_ctx)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.15)
    return server


def _stub_auth_provider(valid_user: str, valid_password: str):
    """A provider accepting one username/password pair through the ``basic`` presentation.

    pgwire hands the startup credential to the provider's scheme validator, so a stub has to
    offer one — the username and password arrive base64-encoded as ``user:password``.
    """
    import base64

    from provisa.auth.models import AuthIdentity

    class _Stub:
        auth_scheme = "basic"

        @property
        def token_validators(self):
            return {"basic": self._validate_basic}

        async def _validate_basic(self, token: str) -> AuthIdentity:
            username, password = base64.b64decode(token).decode().split(":", 1)
            if username != valid_user or password != valid_password:
                raise ValueError("Invalid credentials")
            return AuthIdentity(
                user_id=username,
                email=None,
                display_name=username,
                roles=[username],
                raw_claims={"sub": username},
            )

    return _Stub()


def _make_mock_state(role: str = "admin", provider: str = "simple") -> MagicMock:
    """Minimal AppState mock.

    # integration: mock-justified — AppState is a data structure populated from
    # config at startup, not a docker-compose service.
    """
    from provisa.compiler.rls import RLSContext

    ctx = MagicMock()
    ctx.tables = {}
    ctx.joins = {}
    state = no_replica_routes(MagicMock())
    state.contexts = {role: ctx}
    state.rls_contexts = {role: RLSContext.empty()}
    state.roles = {role: {"id": role, "capabilities": [], "domain_access": ["*"]}}
    state.schema_build_cache = {"column_types": {}}
    state.auth_config = {"provider": provider, "default_role": role, "role_mapping": []}
    state.auth_middleware_active = provider != "none"
    state.multitenancy = False
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
    """Start a ProvisaServer.

    Each connection runs on its own thread with a loop that thread owns (REQ-1882); there is no
    shared loop to configure here.
    """
    port = _free_port()
    server = _make_server(port)
    yield port, server
    server.shutdown()


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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.govern_pgwire_plan", _noop),
            patch("provisa.pgwire._pipeline.describe_pgwire_statement", describes_as(("v", "INT"))),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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

        async def _echo_role(_, role_id, params=None, wire_formats=None):
            return EngineResult(rows=[(role_id,)], column_names=["role"])

        with (
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.govern_pgwire_plan", _echo_role),
            patch(
                "provisa.pgwire._pipeline.describe_pgwire_statement",
                describes_as(("role", "VARCHAR")),
            ),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
    """REQ-581 / REQ-589 (amended 2026-09-30): $1/$2 positional params reach the pipeline as
    placeholders with the values BOUND alongside — never spliced into the SQL text."""

    async def test_single_param_string(self, pgwire_srv):
        """String param $1 is substituted and query returns expected value."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")
        received: list[str] = []

        async def _capture(sql, role_id, params=None, wire_formats=None):
            received.append((sql, params))
            return EngineResult(rows=[("hello",)], column_names=["v"])

        with (
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.govern_pgwire_plan", _capture),
            patch(
                "provisa.pgwire._pipeline.describe_pgwire_statement", describes_as(("v", "VARCHAR"))
            ),
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
        assert received, "govern_pgwire_plan not called"
        assert received[-1] == ("SELECT $1::text AS v", ["hello"])

    async def test_integer_param(self, pgwire_srv):
        """Integer param $1 is substituted without quotes."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")
        received: list[str] = []

        async def _capture(sql, role_id, params=None, wire_formats=None):
            received.append((sql, params))
            return EngineResult(rows=[(42,)], column_names=["v"])

        with (
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.govern_pgwire_plan", _capture),
            patch("provisa.pgwire._pipeline.describe_pgwire_statement", describes_as(("v", "INT"))),
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
        # The integer stays a bound int, typed by its declared parameter type — never spliced.
        assert received[-1] == ("SELECT $1 AS v", [42])

    async def test_null_param(self, pgwire_srv):
        """None param becomes NULL literal."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")
        received: list[str] = []

        async def _capture(sql, role_id, params=None, wire_formats=None):
            received.append((sql, params))
            return EngineResult(rows=[(None,)], column_names=["v"])

        with (
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.govern_pgwire_plan", _capture),
            patch(
                "provisa.pgwire._pipeline.describe_pgwire_statement", describes_as(("v", "VARCHAR"))
            ),
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
        assert received[-1] == ("SELECT $1::text AS v", [None])

    async def test_multi_param_extended(self, pgwire_srv):
        """Two params ($1, $2) bound in a single extended-protocol query."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")
        received: list[str] = []

        async def _capture(sql, role_id, params=None, wire_formats=None):
            received.append((sql, params))
            return EngineResult(rows=[("a", 7)], column_names=["s", "n"])

        with (
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.govern_pgwire_plan", _capture),
            patch(
                "provisa.pgwire._pipeline.describe_pgwire_statement",
                describes_as(("s", "VARCHAR"), ("n", "INT")),
            ),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            await conn.fetchrow("SELECT $1::text AS s, $2::int AS n", "a", 7)
            await conn.close()
        assert received
        # received[-1] is the Execute (received[0] is the Describe, bound with example values).
        assert received[-1] == ("SELECT $1::text AS s, $2::int AS n", ["a", 7])

    async def test_explicit_prepared_statement(self, pgwire_srv):
        """conn.prepare() drives a discrete Parse/Describe/Bind/Execute cycle.

        asyncpg's prepare() issues Parse + Describe(statement) before any Bind,
        exercising the extended-protocol path end to end (REQ-581/REQ-589).
        """
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")
        received: list[str] = []

        async def _capture(sql, role_id, params=None, wire_formats=None):
            received.append((sql, params))
            return EngineResult(rows=[(99,)], column_names=["v"])

        with (
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.govern_pgwire_plan", _capture),
            patch("provisa.pgwire._pipeline.describe_pgwire_statement", describes_as(("v", "INT"))),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            stmt = await conn.prepare("SELECT $1::int AS v")
            row = await stmt.fetchrow(99)
            await conn.close()
        assert row is not None
        assert received
        assert received[-1] == ("SELECT $1::int AS v", [99])


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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.govern_pgwire_plan", _one),
            patch("provisa.pgwire._pipeline.describe_pgwire_statement", describes_as(("v", "INT"))),
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

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.load_cert_chain(cert_path, key_path)

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
                patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
                patch("provisa.api.app.state", state),
                patch("provisa.pgwire._pipeline.govern_pgwire_plan", _one),
                patch(
                    "provisa.pgwire._pipeline.describe_pgwire_statement", describes_as(("v", "INT"))
                ),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
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


class TestPgwireCopyFrom:
    """REQ-615: COPY FROM STDIN is a write, and pgwire takes no writes."""

    async def test_copy_from_stdin_is_refused(self, pgwire_srv):
        """COPY FROM STDIN is refused with 0A000 before the client is asked for data."""
        port, _ = pgwire_srv
        provider = _stub_auth_provider("admin", "secret")
        state = _make_mock_state("admin", "simple")

        with (
            patch("provisa.auth.wiring.build_auth_provider", return_value=provider),
            patch("provisa.api.app.state", state),
        ):
            conn = await asyncpg.connect(
                host="127.0.0.1",
                port=port,
                user="admin",
                password="secret",
                database="provisa",
            )
            with pytest.raises(asyncpg.FeatureNotSupportedError):
                await conn.execute("COPY orders FROM STDIN")
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
        with patch("provisa.api.app.state", state):
            start_pgwire_server("127.0.0.1", port, ssl_ctx=None)
        time.sleep(0.2)

        # Port should now be in use
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                bound = True
        except OSError:
            bound = False
        assert bound, "pgwire server did not bind after start_pgwire_server()"


# ---------------------------------------------------------------------------
# REQ-1882 — governance does not serialize concurrent connections
# ---------------------------------------------------------------------------


class TestPgwireConcurrentGovernanceIsolation:
    """REQ-1882: one connection's slow governed statement must not delay a concurrently
    submitted, unrelated statement on a SEPARATE connection — live-verified original symptom: a
    full-table GROUP BY took a concurrent single-row PK lookup from ~120ms to ~12.1s.

    Uses an injectable delay hook (a patched `govern_pgwire_plan` that offloads a synchronous
    sleep via `_off_loop`, exactly like a slow real governance pass would) rather than a real
    large dataset — deterministic and immune to drift as the demo dataset changes.
    """

    async def test_slow_connection_does_not_delay_concurrent_cheap_connection(self, pgwire_srv):
        port, _ = pgwire_srv
        state = _make_mock_state("trust_role", "none")

        from provisa.pgwire import _pipeline

        async def _fake_govern(sql, role_id, params=None, wire_formats=None):
            return EngineResult(rows=[(role_id,)], column_names=["role"])

        async def _fake_describe(sql, role_id):
            # asyncpg's extended protocol governs the statement at its Describe (REQ-589).
            from provisa.pgwire._pipeline import _Described

            if role_id == "slow":
                await _pipeline._off_loop(time.sleep, 0.5)
            return _Described([("role", "VARCHAR")], None)

        async def _connect_and_time(role: str) -> float:
            conn = await asyncpg.connect(
                host="127.0.0.1", port=port, user=role, password="any", database="provisa"
            )
            t0 = time.monotonic()
            await conn.fetchrow("SELECT 1")
            elapsed = time.monotonic() - t0
            await conn.close()
            return elapsed

        with (
            patch("provisa.api.app.state", state),
            patch("provisa.pgwire._pipeline.govern_pgwire_plan", _fake_govern),
            patch("provisa.pgwire._pipeline.describe_pgwire_statement", _fake_describe),
        ):
            baseline = await _connect_and_time("cheap")

            slow_task = asyncio.ensure_future(_connect_and_time("slow"))
            await asyncio.sleep(0.05)  # let the slow statement start governing first
            cheap_elapsed = await _connect_and_time("cheap")
            slow_elapsed = await slow_task

        assert slow_elapsed >= 0.5
        # The old shared-loop-serialization bug would have stretched the cheap request out to
        # track the slow one's duration; isolated, it stays close to its solo baseline.
        assert cheap_elapsed < max(baseline * 4, 0.3)

    async def test_governance_runs_on_the_connection_thread_in_parallel(self, pgwire_srv):
        """REQ-1882 (amended 2026-09-29): the entire request runs on its connection thread.

        Each connection's governance coroutine must execute on that connection's own handler
        thread (threading.get_ident() == the handler thread's ident) — never on a shared loop's
        thread, never via run_coroutine_threadsafe. And two connections must govern in parallel:
        each governance coroutine BLOCKS its thread on a two-party barrier, so the barrier only
        opens if both are inside governance at the same moment. On one shared loop the first
        blocked coroutine would stall the loop, the second could never arrive, and the barrier
        would break."""
        import threading

        from provisa.pgwire import _pipeline
        from provisa.pgwire.server import ProvisaHandler

        port, _ = pgwire_srv
        state = _make_mock_state("trust_role", "none")

        handler_idents: set[int] = set()
        govern_idents: list[int] = []
        both_governing = threading.Barrier(2)
        real_handle = ProvisaHandler.handle

        def _recording_handle(self) -> None:
            handler_idents.add(threading.get_ident())
            real_handle(self)

        async def _blocking_describe(sql, role_id):
            # The statement's one governance pass runs at its Describe (REQ-589).
            from provisa.pgwire._pipeline import _Described

            del sql
            govern_idents.append(threading.get_ident())
            both_governing.wait(timeout=10)  # blocks this connection's thread AND its loop
            return _Described([("role", "VARCHAR")], None)

        async def _execute(sql, role_id, params=None, wire_formats=None):
            del sql, params, wire_formats
            govern_idents.append(threading.get_ident())
            return EngineResult(rows=[(role_id,)], column_names=["role"])

        def _no_cross_thread_hop(*_args, **_kwargs):
            raise AssertionError("pgwire must not dispatch to another loop (REQ-1882)")

        async def _query(role: str) -> str:
            conn = await asyncpg.connect(
                host="127.0.0.1", port=port, user=role, password="any", database="provisa"
            )
            try:
                row = await conn.fetchrow("SELECT 1")
            finally:
                await conn.close()
            assert row is not None
            return row[0]

        with (
            patch("provisa.api.app.state", state),
            patch.object(ProvisaHandler, "handle", _recording_handle),
            patch.object(_pipeline, "describe_pgwire_statement", _blocking_describe),
            patch.object(_pipeline, "govern_pgwire_plan", _execute),
            patch("asyncio.run_coroutine_threadsafe", _no_cross_thread_hop),
        ):
            results = await asyncio.wait_for(
                asyncio.gather(_query("conn_a"), _query("conn_b")), timeout=30
            )

        assert sorted(results) == ["conn_a", "conn_b"]
        assert both_governing.broken is False
        # Each connection runs its Describe (the governance pass) and its Execute — always on its
        # own thread.
        assert len(govern_idents) == 4
        assert len(set(govern_idents)) == 2, "the two connections governed on one thread"
        assert set(govern_idents) <= handler_idents, (
            "governance ran on a thread that is not a connection handler thread"
        )
        assert threading.get_ident() not in govern_idents
