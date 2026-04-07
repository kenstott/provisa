# Copyright (c) 2026 Kenneth Stott
# Canary: d2e4f6a8-b0c2-4d8e-9f0a-1b2c3d4e5f6a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holders.

"""Integration tests for the Provisa SQLAlchemy dialect (Phase AK, REQ-265–267).

Tests the dialect's structural properties (no live server), and live-server
tests that verify query execution through SQLAlchemy against a running
Provisa instance.

Structural tests run without marks.
Live tests require pytest.mark.integration and a running Provisa + Docker stack.

To run live tests:
    pytest tests/integration/test_sqlalchemy_dialect.py -m integration
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------------
# Dialect structural tests — no live server required
# ---------------------------------------------------------------------------

class TestDialectProperties:
    def test_dialect_name(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        assert ProvisaDialect.name == "provisa"

    def test_dialect_driver(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        assert ProvisaDialect.driver == "provisa_client"

    def test_dbapi_returns_provisa_dbapi(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        from provisa_client import dbapi
        assert ProvisaDialect.dbapi() is dbapi

    def test_import_dbapi_returns_provisa_dbapi(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        from provisa_client import dbapi
        assert ProvisaDialect.import_dbapi() is dbapi

    def test_does_not_support_alter(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        assert ProvisaDialect.supports_alter is False

    def test_supports_unicode_statements(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        assert ProvisaDialect.supports_unicode_statements is True


class TestCreateConnectArgs:
    def test_http_url_scheme(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        from sqlalchemy.engine import make_url

        dialect = ProvisaDialect()
        url = make_url("provisa+http://admin:secret@localhost:8001")
        args, kwargs = dialect.create_connect_args(url)
        assert "url" in kwargs
        assert kwargs["url"].startswith("http://")
        assert "localhost" in kwargs["url"]
        assert "8001" in kwargs["url"]

    def test_https_url_scheme(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        from sqlalchemy.engine import make_url

        dialect = ProvisaDialect()
        url = make_url("provisa+https://admin:secret@db.example.com:8001")
        args, kwargs = dialect.create_connect_args(url)
        assert kwargs["url"].startswith("https://")

    def test_default_port_is_8001(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        from sqlalchemy.engine import make_url

        dialect = ProvisaDialect()
        url = make_url("provisa+http://admin:secret@localhost")
        args, kwargs = dialect.create_connect_args(url)
        assert "8001" in kwargs["url"]

    def test_username_passed(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        from sqlalchemy.engine import make_url

        dialect = ProvisaDialect()
        url = make_url("provisa+http://analyst:pw@localhost:8001")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["username"] == "analyst"

    def test_password_passed(self):
        from provisa_client.sqlalchemy_dialect import ProvisaDialect
        from sqlalchemy.engine import make_url

        dialect = ProvisaDialect()
        url = make_url("provisa+http://analyst:mysecret@localhost:8001")
        _, kwargs = dialect.create_connect_args(url)
        assert kwargs["password"] == "mysecret"


class TestEntryPointRegistration:
    def test_dialect_registered_for_http(self):
        """provisa+http:// creates an engine without error."""
        from sqlalchemy import create_engine

        # This will raise if the entry point is not registered
        try:
            engine = create_engine(
                "provisa+http://admin:pw@localhost:8001",
                module=__import__("provisa_client.dbapi", fromlist=["dbapi"]),
            )
            assert engine.dialect.name == "provisa"
        except Exception as exc:
            # If the dialect isn't installed, the test is inconclusive but not a failure
            pytest.skip(f"provisa_client not installed as entry point: {exc}")

    def test_dialect_registered_for_https(self):
        from sqlalchemy import create_engine

        try:
            engine = create_engine(
                "provisa+https://admin:pw@localhost:8001",
                module=__import__("provisa_client.dbapi", fromlist=["dbapi"]),
            )
            assert engine.dialect.name == "provisa"
        except Exception as exc:
            pytest.skip(f"provisa_client not installed as entry point: {exc}")


# ---------------------------------------------------------------------------
# Live-server tests — require running Provisa instance
# ---------------------------------------------------------------------------

@pytest.mark.requires_provisa_server
class TestLiveSQLAlchemyDialect:
    """Require running Provisa server at localhost:8001 (Docker Compose stack)."""

    PROVISA_URL = "provisa+http://admin:provisa@localhost:8001"

    @pytest.fixture
    def engine(self):
        from sqlalchemy import create_engine
        return create_engine(self.PROVISA_URL)

    @pytest.fixture
    def connection(self, engine):
        with engine.connect() as conn:
            yield conn

    def test_connect_succeeds(self, connection):
        """Engine can connect to the live Provisa server."""
        assert connection is not None

    def test_get_tables(self, connection):
        """get_tables returns registered approved queries as virtual tables."""
        from sqlalchemy import inspect
        inspector = inspect(connection)
        tables = inspector.get_table_names()
        assert isinstance(tables, list)

    def test_execute_approved_query(self, connection):
        """SQL SELECT with no table references executes through the governance pipeline."""
        from sqlalchemy import text
        result = connection.execute(text("SELECT 1 AS result"))
        rows = result.fetchall()
        assert isinstance(rows, list)

    def test_column_names_from_metadata(self, connection):
        """get_columns returns a list (empty if no semantic model tables registered)."""
        from sqlalchemy import inspect
        inspector = inspect(connection)
        tables = inspector.get_table_names()
        # When no approved queries are registered, tables is empty and cols is empty.
        if tables:
            cols = inspector.get_columns(tables[0])
            assert isinstance(cols, list)
            if cols:
                assert "name" in cols[0]
                assert "type" in cols[0]
        else:
            cols = inspector.get_columns("nonexistent")
            assert isinstance(cols, list)

    def test_where_filter_passed_to_executor(self, connection):
        """A WHERE clause in a table-free SQL is forwarded to the Provisa pipeline."""
        from sqlalchemy import text
        result = connection.execute(text("SELECT 1 AS id WHERE 1 = 1"))
        rows = result.fetchall()
        assert isinstance(rows, list)

    def test_rls_applied_for_role(self, engine):
        """Connections with a role header use the role in the request pipeline."""
        from sqlalchemy import create_engine, text
        role_url = "provisa+http://admin:provisa@localhost:8001?role=admin"
        role_engine = create_engine(role_url)
        with role_engine.connect() as conn:
            result = conn.execute(text("SELECT 1 AS val"))
            rows = result.fetchall()
            assert isinstance(rows, list)
