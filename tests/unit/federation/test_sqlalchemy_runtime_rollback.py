"""A failing statement on the SQLAlchemy runtime does not poison the statements after it.

PostgreSQL-family stores (psycopg2: cockroachdb, greenplum, opengauss, yugabytedb) abort the open
transaction on any statement error and refuse every later statement on that connection with
"current transaction is aborted, commands ignored until end of transaction block" until it is
rolled back. ``run()`` executes on the runtime's one held DBAPI connection, so it must roll that
transaction back when a statement fails (REQ-1912, REQ-2236).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from provisa.federation.sqlalchemy_runtime import SqlAlchemyFederationRuntime


class _AbortedTxn(Exception):
    pass


class _BadStatement(Exception):
    pass


class _PgLikeCursor:
    def __init__(self, conn: _PgLikeConnection) -> None:
        self._conn = conn
        self.description: list[tuple[str]] | None = None
        self._rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        if self._conn.aborted:
            raise _AbortedTxn(
                "current transaction is aborted, commands ignored until end of transaction block"
            )
        if sql == "SELECT bad":
            self._conn.aborted = True
            raise _BadStatement('column "bad" does not exist')
        self.description = [("one",)]
        self._rows = [(1,)]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def close(self) -> None:
        pass


class _PgLikeConnection:
    """PostgreSQL transaction semantics: an error aborts the transaction until rollback."""

    def __init__(self) -> None:
        self.aborted = False
        self.dbapi_connection = self  # driver module is this test module: no cancel hook

    def cursor(self) -> _PgLikeCursor:
        return _PgLikeCursor(self)

    def commit(self) -> None:
        if self.aborted:
            raise _AbortedTxn("commit on an aborted transaction")

    def rollback(self) -> None:
        self.aborted = False

    def close(self) -> None:
        pass


def test_a_failed_statement_does_not_abort_the_next_one(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = SqlAlchemyFederationRuntime(url="sqlite://")
    monkeypatch.setattr(runtime, "_con", _PgLikeConnection())
    try:
        with pytest.raises(_BadStatement):
            asyncio.run(runtime.run("SELECT bad"))
        result = asyncio.run(runtime.run("SELECT 1"))
        assert result.column_names == ["one"]
        assert result.rows == [(1,)]
    finally:
        monkeypatch.undo()
        runtime.close()
