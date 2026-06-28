# Copyright (c) 2026 Kenneth Stott
# Canary: 3b7c1d2e-4f5a-6b7c-8d9e-0f1a2b3c4d5e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa.compiler.introspect — REQ-636."""

from __future__ import annotations

from unittest.mock import MagicMock

import trino.exceptions

from provisa.compiler.introspect import introspect_column_types


def _conn_raising(exc: Exception) -> MagicMock:
    """Return a mock Trino connection whose cursor().execute() raises exc."""
    cur = MagicMock()
    cur.execute.side_effect = exc
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


class TestIntrospectColumnTypes:
    def test_returns_columns_on_success(self):
        cur = MagicMock()
        cur.fetchall.return_value = [("id", "integer"), ("name", "varchar")]
        conn = MagicMock()
        conn.cursor.return_value = cur

        result = introspect_column_types(conn, "mycat", "myschema", "mytable")

        assert result == {"id": "integer", "name": "varchar"}

    def test_returns_empty_on_trino_user_error(self):
        """TrinoUserError (catalog not found etc.) returns {} without raising."""
        exc = trino.exceptions.TrinoUserError(
            {"errorName": "CATALOG_NOT_FOUND", "errorType": "USER_ERROR", "message": "no catalog"},
            query_id="q1",
        )
        conn = _conn_raising(exc)

        result = introspect_column_types(conn, "nocat", "s", "t")

        assert result == {}

    def test_returns_empty_on_server_starting_up(self):
        """SERVER_STARTING_UP (INTERNAL_ERROR TrinoQueryError) returns {} without raising.

        Regression: introspect_column_types caught only TrinoUserError; a plain
        TrinoQueryError with error_name=SERVER_STARTING_UP propagated to the lifespan
        context and aborted app startup.
        """
        exc = trino.exceptions.TrinoQueryError(
            {
                "errorName": "SERVER_STARTING_UP",
                "errorType": "INTERNAL_ERROR",
                "message": "Trino server is still initializing",
            },
            query_id="q2",
        )
        conn = _conn_raising(exc)

        result = introspect_column_types(conn, "mycat", "s", "t")

        assert result == {}
