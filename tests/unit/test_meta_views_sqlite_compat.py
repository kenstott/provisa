# Copyright (c) 2026 Kenneth Stott
# Canary: 8a3e7c12-1f4b-4d9e-b6a0-2c5f9e0d1b3a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Unit tests: meta-view DDL must be SQLite-compatible (no CREATE OR REPLACE VIEW).

SQLite rejects ``CREATE OR REPLACE VIEW`` (syntax error near "OR").  When the
control plane is SQLite (e.g. the duckdb_sqlite_control_plane_e2e fixture), the
server startup fails during ``_seed_meta_domain`` / ``_seed_ops_domain``.

The fix: ``startup_seed`` must adapt the DDL per-dialect before calling
``conn.execute()``.
"""

from __future__ import annotations

import re
from unittest.mock import AsyncMock, MagicMock

import pytest


class TestMetaViewDdlSqliteCompat:
    """_seed_meta_domain and _seed_ops_domain must not send CREATE OR REPLACE VIEW
    to a SQLite control-plane connection."""

    @pytest.mark.asyncio
    async def test_seed_meta_domain_does_not_use_create_or_replace_view_on_sqlite(
        self,
    ) -> None:
        """On a SQLite control plane, _seed_meta_domain must not execute
        CREATE OR REPLACE VIEW (SQLite syntax error)."""
        from provisa.api.startup_seed import _seed_meta_domain

        capabilities = MagicMock()
        capabilities.dialect = "sqlite"

        mock_conn = AsyncMock()
        mock_conn.capabilities = capabilities
        mock_conn.reflect_columns = AsyncMock(return_value=[])
        mock_conn.upsert_returning = AsyncMock(return_value=1)
        result = MagicMock()
        result.scalar.return_value = 0
        result.fetchone.return_value = None
        mock_conn.execute_core = AsyncMock(return_value=result)
        mock_conn.execute = AsyncMock(return_value="")

        await _seed_meta_domain(mock_conn, org_id="default")

        # Every DDL string sent to conn.execute must NOT start with
        # "CREATE OR REPLACE VIEW" — that syntax is PostgreSQL-only.
        for call in mock_conn.execute.await_args_list:
            ddl = call.args[0] if call.args else ""
            assert not re.search(
                r"CREATE\s+OR\s+REPLACE\s+VIEW", ddl, re.IGNORECASE
            ), f"SQLite-incompatible DDL sent to conn.execute: {ddl[:120]!r}"

    @pytest.mark.asyncio
    async def test_seed_ops_domain_does_not_use_create_or_replace_view_on_sqlite(
        self,
    ) -> None:
        """On a SQLite control plane, _seed_ops_domain must not execute
        CREATE OR REPLACE VIEW."""
        from provisa.api.startup_seed import _seed_ops_domain

        capabilities = MagicMock()
        capabilities.dialect = "sqlite"

        mock_conn = AsyncMock()
        mock_conn.capabilities = capabilities
        mock_conn.reflect_columns = AsyncMock(return_value=[])
        mock_conn.upsert_returning = AsyncMock(return_value=1)
        result = MagicMock()
        result.scalar.return_value = 0
        result.fetchone.return_value = None
        mock_conn.execute_core = AsyncMock(return_value=result)
        mock_conn.execute = AsyncMock(return_value="")

        await _seed_ops_domain(mock_conn, org_id="default")

        for call in mock_conn.execute.await_args_list:
            ddl = call.args[0] if call.args else ""
            assert not re.search(
                r"CREATE\s+OR\s+REPLACE\s+VIEW", ddl, re.IGNORECASE
            ), f"SQLite-incompatible DDL sent to conn.execute: {ddl[:120]!r}"

    @pytest.mark.asyncio
    async def test_seed_meta_domain_uses_drop_then_create_on_sqlite(
        self,
    ) -> None:
        """On SQLite, the view DDL must use DROP VIEW IF EXISTS + CREATE VIEW
        (the canonical idempotent SQLite pattern)."""
        from provisa.api.startup_seed import _seed_meta_domain

        capabilities = MagicMock()
        capabilities.dialect = "sqlite"

        mock_conn = AsyncMock()
        mock_conn.capabilities = capabilities
        mock_conn.reflect_columns = AsyncMock(return_value=[])
        mock_conn.upsert_returning = AsyncMock(return_value=1)
        result = MagicMock()
        result.scalar.return_value = 0
        result.fetchone.return_value = None
        mock_conn.execute_core = AsyncMock(return_value=result)
        mock_conn.execute = AsyncMock(return_value="")

        await _seed_meta_domain(mock_conn, org_id="default")

        view_ddls = [
            call.args[0]
            for call in mock_conn.execute.await_args_list
            if call.args and re.search(r"VIEW", call.args[0], re.IGNORECASE)
        ]
        assert view_ddls, "No view DDL executed at all"
        for ddl in view_ddls:
            assert re.search(r"DROP\s+VIEW\s+IF\s+EXISTS", ddl, re.IGNORECASE), (
                f"SQLite view DDL missing DROP VIEW IF EXISTS: {ddl[:120]!r}"
            )
            assert re.search(r"\bCREATE\s+VIEW\b", ddl, re.IGNORECASE), (
                f"SQLite view DDL missing CREATE VIEW: {ddl[:120]!r}"
            )
