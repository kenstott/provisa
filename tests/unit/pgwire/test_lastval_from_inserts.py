# Copyright (c) 2026 Kenneth Stott
# Canary: 3f6c2a1e-8d4b-4e7a-9c15-2b7e0d9a4f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2167 (amended 2026-11-02, lastval from the session's own inserts): an INSERT through a
pgwire session that makes its source generate an identity or serial value is written to report
it, the session keeps the last one, and lastval() answers it."""

# Requirements: REQ-2167

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlglot

from provisa.executor.drivers.base import GeneratedColumn
from provisa.executor.result import QueryResult
from provisa.pgwire.generated_keys import (
    LAST_INSERT_ID,
    RETURNING,
    GeneratedKey,
    answered_lastval,
    inserted_table,
    returning_rewrite,
)

ID = (GeneratedColumn("id", 1),)


def _returning(sql: str) -> list[str]:
    tree = sqlglot.parse_one(sql, read="postgres")
    return [e.sql("postgres") for e in tree.args["returning"].expressions]


# --- the rewrite --------------------------------------------------------------------------------


def test_an_insert_that_leaves_the_identity_out_returns_it():
    sql, key = returning_rewrite('INSERT INTO "s"."t" ("x") VALUES ($1)', ID)
    assert _returning(sql) == ['"id" AS "provisa_generated_1"']
    assert key == GeneratedKey(RETURNING, added=1, client_returns=False, rows=(0,))


def test_the_clients_own_returning_comes_first_and_is_kept():
    sql, key = returning_rewrite('INSERT INTO t ("x") VALUES (1) RETURNING "x", "y" AS z', ID)
    assert _returning(sql) == ['"x"', '"y" AS z', '"id" AS "provisa_generated_1"']
    assert key.client_returns and key.added == 1


def test_default_in_the_identity_place_generates_and_a_value_there_does_not():
    _, key = returning_rewrite('INSERT INTO t ("id", "x") VALUES (DEFAULT, 1), (9, 2)', ID)
    assert key.rows == (0, None)


def test_an_insert_that_gives_every_row_its_identity_is_not_rewritten():
    assert returning_rewrite('INSERT INTO t ("id", "x") VALUES (9, 1)', ID) is None
    assert returning_rewrite('INSERT INTO t ("id", "x") SELECT 9, 1', ID) is None


def test_without_a_column_list_the_place_is_the_columns_own():
    columns = (GeneratedColumn("id", 2),)
    assert returning_rewrite("INSERT INTO t VALUES ('a', 5)", columns) is None
    _, key = returning_rewrite("INSERT INTO t VALUES ('a', DEFAULT), ('b')", columns)
    assert key.rows == (0, 0)


def test_a_select_that_leaves_the_identity_out_generates_on_every_row():
    _, key = returning_rewrite('INSERT INTO t ("x") SELECT "x" FROM u', ID)
    assert key.rows is None and key.every == 0
    _, key = returning_rewrite("INSERT INTO t SELECT 'a'", (GeneratedColumn("id", 2),))
    assert key.every == 0


def test_default_values_generates():
    _, key = returning_rewrite("INSERT INTO t DEFAULT VALUES", ID)
    assert key.every == 0


def test_of_two_generated_columns_the_later_is_the_last_value():
    columns = (GeneratedColumn("id", 1), GeneratedColumn("n", 3))
    _, key = returning_rewrite('INSERT INTO t ("x") VALUES (1)', columns)
    assert key.rows == (1,) and key.added == 2


def test_quoted_names_are_matched_as_written():
    columns = (GeneratedColumn("Id", 1),)
    assert returning_rewrite('INSERT INTO t ("Id", "x") VALUES (3, 1)', columns) is None
    assert returning_rewrite('INSERT INTO t (id, "x") VALUES (3, 1)', columns) is not None


def test_the_target_table_is_read_from_the_statement():
    assert inserted_table('INSERT INTO "s"."T" ("x") VALUES (1)', "postgres") == ("s", "T")
    assert inserted_table("INSERT INTO Orders (x) VALUES (1)", "postgres") == (None, "orders")
    assert inserted_table("INSERT INTO `db`.`t` (x) VALUES (1)", "mysql") == ("db", "t")
    assert inserted_table("UPDATE t SET x = 1", "postgres") is None


# --- what the client is answered ----------------------------------------------------------------


def test_the_added_column_is_stripped_and_the_count_kept():
    key = GeneratedKey(RETURNING, added=1, client_returns=False, rows=(0, 0))
    taken = key.taken(QueryResult(rows=[(1,), (2,)], column_names=["provisa_generated_1"]))
    assert taken.rows == [] and taken.column_names == [] and taken.rowcount == 2
    assert taken.generated_key == 2


def test_the_clients_rows_are_exactly_what_it_asked_for():
    key = GeneratedKey(RETURNING, added=1, client_returns=True, rows=(0, None))
    taken = key.taken(
        QueryResult(
            rows=[("a", 7), ("b", 9)],
            column_names=["x", "provisa_generated_1"],
            column_types=["text", "int8"],
        )
    )
    assert taken.rows == [("a",), ("b",)]
    assert taken.column_names == ["x"] and taken.column_types == ["text"]
    # The last row gave its own identity: the last value generated is the first row's.
    assert taken.generated_key == 7


def test_a_statement_that_inserted_nothing_reports_nothing():
    key = GeneratedKey(RETURNING, added=1, client_returns=False, every=0)
    assert (
        key.taken(QueryResult(rows=[], column_names=["provisa_generated_1"])).generated_key is None
    )


def test_the_connections_own_report_is_passed_on():
    reported = QueryResult(rows=[], column_names=[], rowcount=1, generated_key=41)
    assert GeneratedKey(LAST_INSERT_ID).taken(reported) is reported


# --- lastval ------------------------------------------------------------------------------------


def test_lastval_is_answered_with_the_value_named_as_postgresql_names_it():
    answered = answered_lastval('SELECT pg_catalog."lastval"()', 42)
    assert answered == 'SELECT CAST(42 AS BIGINT) AS "lastval"'
    assert answered_lastval("SELECT lastval() + 1 AS n", 42) == "SELECT CAST(42 AS BIGINT) + 1 AS n"


def test_a_statement_without_lastval_is_left_alone():
    assert answered_lastval("SELECT 'lastval()'", 42) is None
    assert answered_lastval("SELECT 1", 42) is None


# --- the plan -----------------------------------------------------------------------------------


def _write_plan(sql: str, dialect: str = "postgres"):
    from provisa.pgwire._pipeline import _Plan
    from provisa.transpiler.router import Route

    return _Plan(route=Route.DIRECT, sql=sql, source_id="src", dialect=dialect, writes_tables=True)


def _state(driver) -> SimpleNamespace:
    pools = MagicMock()
    pools.has.return_value = True
    pools.get.return_value = driver
    return SimpleNamespace(source_pools=pools)


@pytest.mark.asyncio
async def test_a_postgresql_insert_is_written_to_return_what_the_catalog_says_it_generates():
    from provisa.pgwire._pipeline import reporting_generated

    driver = SimpleNamespace(generated_keys=RETURNING, generated_columns=AsyncMock(return_value=ID))
    plan = await reporting_generated(
        _write_plan('INSERT INTO "s"."t" ("x") VALUES ($1)'), _state(driver)
    )
    driver.generated_columns.assert_awaited_once_with("s", "t")
    assert plan.sql.endswith('RETURNING "id" AS "provisa_generated_1"')
    assert plan.generated_key == GeneratedKey(RETURNING, added=1, rows=(0,))


@pytest.mark.asyncio
async def test_a_mysql_insert_is_reported_by_its_connection():
    from provisa.pgwire._pipeline import reporting_generated

    driver = SimpleNamespace(
        generated_keys=LAST_INSERT_ID, generated_columns=AsyncMock(return_value=ID)
    )
    sql = "INSERT INTO `t` (`x`) VALUES (@1)"
    plan = await reporting_generated(_write_plan(sql, "mysql"), _state(driver))
    assert plan.sql == sql
    assert plan.generated_key == GeneratedKey(LAST_INSERT_ID)


@pytest.mark.asyncio
async def test_a_source_with_no_generated_key_mechanism_is_not_asked():
    from provisa.pgwire._pipeline import reporting_generated

    driver = SimpleNamespace(generated_keys=None, generated_columns=AsyncMock())
    plan = await reporting_generated(_write_plan("INSERT INTO t (x) VALUES (1)"), _state(driver))
    driver.generated_columns.assert_not_awaited()
    assert plan.generated_key is None


@pytest.mark.asyncio
async def test_a_table_with_no_generated_column_is_not_rewritten():
    from provisa.pgwire._pipeline import reporting_generated

    driver = SimpleNamespace(generated_keys=RETURNING, generated_columns=AsyncMock(return_value=()))
    sql = "INSERT INTO t (x) VALUES (1)"
    plan = await reporting_generated(_write_plan(sql), _state(driver))
    assert plan.sql == sql and plan.generated_key is None


@pytest.mark.asyncio
async def test_a_read_or_an_update_is_not_asked():
    from provisa.pgwire._pipeline import _Plan, reporting_generated
    from provisa.transpiler.router import Route

    driver = SimpleNamespace(generated_keys=RETURNING, generated_columns=AsyncMock(return_value=ID))
    await reporting_generated(_write_plan("UPDATE t SET x = 1"), _state(driver))
    await reporting_generated(
        _Plan(route=Route.DIRECT, sql="SELECT 1", source_id="src", dialect="postgres"),
        _state(driver),
    )
    driver.generated_columns.assert_not_awaited()


# --- the session --------------------------------------------------------------------------------


def _session_running(monkeypatch, executed: QueryResult) -> tuple[Any, list[str]]:
    from provisa.cache.store import NoopCacheStore
    from provisa.pgwire import _pipeline
    from provisa.pgwire._pipeline import _Plan, _mint_stamp
    from provisa.pgwire.server import ProvisaSession
    from provisa.transpiler.router import Route

    governed: list[str] = []

    async def _govern(sql, role_id, params=None, wire_formats=None, *, deliver, as_of=None):
        governed.append(sql)
        writes = sql.upper().startswith("INSERT")
        return _Plan(
            route=Route.DIRECT,
            sql=sql,
            source_id="src",
            dialect="postgres",
            writes_tables=writes,
            stamp=_mint_stamp(),
        )

    async def _execute(plan, state=None):
        return executed if plan.writes_tables else QueryResult(rows=[(1,)], column_names=["v"])

    monkeypatch.setattr(_pipeline, "govern_pgwire_plan", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    monkeypatch.setattr(
        "provisa.pgwire.server.ProvisaSession._finalize_audit", lambda *a, **k: None
    )
    state = MagicMock()
    state.multitenancy = False
    state.response_cache_store = NoopCacheStore()
    state.source_pools.has.return_value = False
    monkeypatch.setattr("provisa.api.app.state", state)
    session = ProvisaSession()
    session.role_id = session.user_id = "alice"
    return session, governed


def test_before_any_insert_lastval_goes_to_postgresqls_own_error(monkeypatch):
    from provisa.core.connection_loop import connection_loop

    with connection_loop():
        session, governed = _session_running(monkeypatch, QueryResult(rows=[], column_names=[]))
        session.execute_sql("SELECT lastval()")
        # Not answered by the session: the statement keeps its call, which raises 55000
        # (pg_forms_system; test_pg_enum_forms holds that error).
        assert governed == ["SELECT lastval()"]
        session.close()


def test_the_session_keeps_the_last_value_and_lastval_answers_it(monkeypatch):
    from provisa.core.connection_loop import connection_loop

    inserted = QueryResult(rows=[], column_names=[], rowcount=1, generated_key=17)
    with connection_loop():
        session, governed = _session_running(monkeypatch, inserted)
        tagged = session.execute_sql('INSERT INTO "t" ("x") VALUES (1)')
        assert tagged.status() == "INSERT 0 1"
        session.execute_sql("SELECT lastval()")
        assert governed[-1] == 'SELECT CAST(17 AS BIGINT) AS "lastval"'
        session.close()


def test_an_insert_that_generated_nothing_keeps_the_previous_value(monkeypatch):
    from provisa.core.connection_loop import connection_loop

    with connection_loop():
        session, governed = _session_running(
            monkeypatch, QueryResult(rows=[], column_names=[], rowcount=1, generated_key=5)
        )
        session.execute_sql('INSERT INTO "t" ("x") VALUES (1)')
        from provisa.pgwire import _pipeline

        async def _nothing(plan, state=None):
            return QueryResult(rows=[], column_names=[], rowcount=1)

        monkeypatch.setattr(_pipeline, "_execute_plan", _nothing)
        session.execute_sql('INSERT INTO "t" ("id", "x") VALUES (9, 1)')
        session.execute_sql("SELECT lastval()")
        assert governed[-1] == 'SELECT CAST(5 AS BIGINT) AS "lastval"'
        session.close()


def test_a_described_lastval_is_answered_from_the_value_at_execute(monkeypatch):
    """A statement held from its Describe was governed with the call in it; at Execute it is
    governed again, with the value the session holds then."""
    from provisa.core.connection_loop import connection_loop

    inserted = QueryResult(rows=[], column_names=[], rowcount=1, generated_key=8)
    with connection_loop():
        session, governed = _session_running(monkeypatch, inserted)
        session.execute_sql('INSERT INTO "t" ("x") VALUES (1)')
        held = SimpleNamespace(sql="SELECT lastval()")
        session.execute_sql("SELECT lastval()", prepared=held)
        assert governed[-1] == 'SELECT CAST(8 AS BIGINT) AS "lastval"'
        session.close()


def test_sequence_functions_stay_refused():
    from provisa.transpiler.function_macros import FunctionRefused
    from provisa.transpiler.transpile import transpile

    for call in ("nextval('s')", "currval('s')", "setval('s', 1)"):
        with pytest.raises(FunctionRefused):
            transpile(f"SELECT {call} AS v", "postgres")


# --- the drivers --------------------------------------------------------------------------------


class _MySQLCursor:
    """PyMySQL's cursor over a connection whose LAST_INSERT_ID() a statement sets."""

    def __init__(self, conn: "_MySQLConn") -> None:
        self.conn = conn
        self.description = None
        self.rowcount = -1
        self._rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query: str, args=None) -> None:
        sql = query % tuple(repr(a) for a in args) if args is not None else query
        self.conn.ran.append(sql)
        if sql == "SELECT LAST_INSERT_ID(0)":
            self.conn.last_insert_id = 0
            self._rows, self.description = [(0,)], [("LAST_INSERT_ID(0)",)]
        elif sql == "SELECT LAST_INSERT_ID()":
            self._rows, self.description = [(self.conn.last_insert_id,)], [("LAST_INSERT_ID()",)]
        else:  # the write
            self.conn.last_insert_id = self.conn.generates
            self._rows, self.description, self.rowcount = [], None, 2

    def fetchone(self):
        return self._rows[0]

    def fetchmany(self, _n):
        rows, self._rows = self._rows, []
        return rows


class _MySQLConn:
    def __init__(self, generates: int) -> None:
        self.generates = generates
        self.last_insert_id = 99  # a previous statement's, on this pooled connection
        self.ran: list[str] = []

    def thread_id(self) -> int:
        return 1

    def cursor(self) -> _MySQLCursor:
        return _MySQLCursor(self)


def _mysql(generates: int):
    from contextlib import contextmanager

    from provisa.executor.drivers.mysql import MySQLDriver

    conn = _MySQLConn(generates)

    class _Pool:
        @contextmanager
        def connection(self, is_broken=None):
            yield conn

    driver = MySQLDriver()
    driver._pool = _Pool()  # type: ignore[assignment]
    return driver, conn


@pytest.mark.asyncio
async def test_mysql_reports_last_insert_id_from_the_writes_own_connection():
    driver, conn = _mysql(generates=31)
    result = await driver.execute_reporting_generated("INSERT INTO t (x) VALUES ($1)", ["a"])
    assert conn.ran == [
        "SELECT LAST_INSERT_ID(0)",
        "INSERT INTO t (x) VALUES ('a')",
        "SELECT LAST_INSERT_ID()",
    ]
    assert result.generated_key == 31 and result.rowcount == 2 and result.rows == []


@pytest.mark.asyncio
async def test_a_mysql_write_that_generated_nothing_reports_nothing_not_an_earlier_value():
    driver, _ = _mysql(generates=0)
    result = await driver.execute_reporting_generated("INSERT INTO t (id) VALUES (5)")
    assert result.generated_key is None


@pytest.mark.asyncio
async def test_a_mysql_write_reports_how_many_rows_it_changed():
    driver, _ = _mysql(generates=0)
    result = await driver.execute("UPDATE t SET x = 1")
    assert result.rowcount == 2


@pytest.mark.asyncio
async def test_mysql_reads_auto_increment_from_its_catalog():
    driver, _ = _mysql(generates=0)
    driver.execute = AsyncMock(return_value=QueryResult(rows=[("id", 1)], column_names=["c", "p"]))
    assert await driver.generated_columns(None, "t") == (GeneratedColumn("id", 1),)
    sql, params = driver.execute.await_args.args
    assert "auto_increment" in sql and params == [None, "t"]


@pytest.mark.asyncio
async def test_postgresql_reads_identity_serial_and_nextval_defaults_from_its_catalog():
    from provisa.executor.drivers.postgresql import PostgreSQLDriver

    driver = PostgreSQLDriver()
    driver.execute = AsyncMock(return_value=QueryResult(rows=[("id", 1)], column_names=["c", "p"]))
    assert await driver.generated_columns("s", "t") == (GeneratedColumn("id", 1),)
    sql, params = driver.execute.await_args.args
    for read in ("attidentity", "pg_get_serial_sequence", "nextval("):
        assert read in sql
    assert params == ["s", "t"]
    assert driver.generated_keys == RETURNING


def test_a_source_written_through_its_own_pgwire_server_reports_no_generated_key():
    from provisa.executor.drivers.pgwire_server import PgwireServerDriver

    assert PgwireServerDriver().generated_keys is None
