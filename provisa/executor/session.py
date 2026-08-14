# Copyright (c) 2026 Kenneth Stott
# Canary: b9beeee4-8177-4486-b5ae-5df347c78c9d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The isolated_sync() session contract (Fix 1, abstraction-leak remediation).

``EngineRuntime.isolated_sync()`` promises callers a fresh, thread-isolated execution surface
without ever handing them a concrete physical-driver connection (``duckdb.DuckDBPyConnection``,
a SQLAlchemy connection, the raw ``trino.dbapi`` connection, etc). :class:`EngineSession` is that
surface: it owns the one place a dbapi cursor is opened, confining ``.cursor()``/execute/fetchall
driver calls to this module so every ``isolated_sync()`` backend can yield the same shape.
"""

from __future__ import annotations

from typing import Any


class EngineSession:
    """Thin cursor-execution wrapper over a raw dbapi connection.

    ``execute`` opens a fresh cursor per call (matching the dbapi cursor-per-statement pattern
    the ``isolated_sync()`` callers already used directly) and keeps it for the following
    ``fetchall()``. ``close()`` closes the underlying connection — only meaningful for backends
    that hand out a dedicated, disposable connection (e.g. Trino); native-engine backends share
    the runtime's own persistent connection and never call it.
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self._cursor: Any = None

    def execute(self, sql: str, params: list | None = None) -> "EngineSession":
        self._cursor = self._conn.cursor()
        if params is None:
            self._cursor.execute(sql)
        else:
            self._cursor.execute(sql, params)
        return self

    def fetchall(self) -> list:
        return self._cursor.fetchall()

    def close(self) -> None:
        self._conn.close()
