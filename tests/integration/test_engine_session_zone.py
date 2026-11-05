# Copyright (c) 2026 Kenneth Stott
# Canary: c47e1d90-2a6b-4f38-b5c3-97e0d2a816f4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement in a session on a zone, on the engines tests/unit/pgwire/test_engine_session_zone.py
does not reach (REQ-2171). PostgreSQL 17 proves session_zone_answers.py is its table, and Trino and
ClickHouse are each held to it case by case, as Provisa writes the statement and tells the engine
the zone. The instants on each engine are the kind Provisa reads: Trino's carry UTC, as its PostgreSQL
connector's do, and ClickHouse's are DateTime64(6, 'UTC'), Provisa's own type for an instant. An
instant an engine returns is held to the table as pgwire writes it."""

# Requirements: REQ-2171

from __future__ import annotations

import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from provisa.core.pg_error import PostgresError
from provisa.core.session_time_zone import bind_zone, reset_zone
from provisa.executor.trino import execute_trino
from provisa.federation.clickhouse_runtime import _ServerBackend
from provisa.pgwire.session_zone import SessionZone
from provisa.transpiler.transpile import transpile, transpile_to_trino
from tests.unit.pgwire.session_zone_answers import ANSWERS, CASES, INSTANTS, sent, statement

pytestmark = [pytest.mark.integration]

_IMAGE = "postgres:17.11"
_READY_S = 120
#: The zone each statement of the table puts a session on.
_SESSIONS = {
    "SET TimeZone = 'America/New_York'": "America/New_York",
    "SET TimeZone = 'Asia/Kolkata'": "Asia/Kolkata",
    "SET TimeZone = 'UTC+5'": "UTC+5",
    "SET TIME ZONE -5": "-5",
    "SET TimeZone = 'UTC'": "UTC",
}


@pytest.fixture(scope="module")
def postgres_17() -> Iterator[psycopg.Connection]:
    name = f"provisa-zone-{uuid.uuid4().hex[:8]}"
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", name, "-e", "POSTGRES_PASSWORD=p"]
        + ["-p", "127.0.0.1::5432", _IMAGE],
        check=True,
        capture_output=True,
    )
    try:
        mapped = subprocess.run(
            ["docker", "port", name, "5432/tcp"], check=True, capture_output=True, text=True
        ).stdout.split(":")[-1]
        deadline = time.monotonic() + _READY_S
        while True:
            try:
                connection = psycopg.connect(
                    f"host=127.0.0.1 port={int(mapped)} user=postgres password=p dbname=postgres",
                    autocommit=True,
                )
                break
            except psycopg.OperationalError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.5)
        connection.execute("CREATE TABLE t (i int, c timestamptz)")
        for i, instant in enumerate(INSTANTS):
            connection.execute("INSERT INTO t VALUES (%s, %s)", (i, instant))
        yield connection
        connection.close()
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True)


@pytest.mark.parametrize("set_zone, name, want", CASES)
def test_the_table_is_what_postgresql_17_answers(postgres_17, set_zone, name, want):
    postgres_17.execute(set_zone)
    result = postgres_17.pgconn.exec_(statement(name).encode())
    assert tuple(result.get_value(i, 0).decode() for i in range(result.ntuples)) == want


#: to_json of an instant the statement says is one, as the compiler says a registered column is.
_STATED_JSON = "SELECT to_json(CAST(c AS timestamptz)) AS v FROM t ORDER BY i"


@pytest.mark.parametrize("set_zone", list(_SESSIONS))
def test_saying_an_instant_is_one_changes_no_answer_of_postgresql_17(postgres_17, set_zone):
    postgres_17.execute(set_zone)
    result = postgres_17.pgconn.exec_(_STATED_JSON.encode())
    got = tuple(result.get_value(i, 0).decode() for i in range(result.ntuples))
    assert got == ANSWERS[set_zone]["json"]


def test_postgresql_17_is_the_table_for_every_zone_in_it(postgres_17):
    assert set(ANSWERS) == set(_SESSIONS)


def _in_zone(set_zone: str, run: Any) -> Any:
    token = bind_zone(SessionZone(_SESSIONS[set_zone]).statement_zone())
    try:
        return run()
    finally:
        reset_zone(token)


def _sent(set_zone: str, value: Any) -> str:
    """``value`` as pgwire sends it: an instant as timestamptz_out writes it in the session's zone."""
    return sent(_SESSIONS[set_zone], value)


_TRINO_ROWS = ", ".join(
    f"({i}, TIMESTAMP '{instant.replace('+00', '')} UTC')" for i, instant in enumerate(INSTANTS)
)


def _trino_kwargs() -> dict[str, Any]:
    return {
        "host": os.environ.get("TRINO_HOST", "localhost"),
        "port": int(os.environ.get("TRINO_PORT", "8080")),
        "user": "test",
        "catalog": "sales_pg",
        "schema": "public",
    }


def _on_trino(trino_conn: Any, name: str) -> Any:
    return _run_on_trino(trino_conn, statement(name))


def _run_on_trino(trino_conn: Any, pg_sql: str) -> Any:
    sql = transpile_to_trino(pg_sql).replace(
        "FROM t ORDER", f"FROM (VALUES {_TRINO_ROWS}) AS t(i, c) ORDER"
    )
    # A statement bound to no zone would run on trino_conn; a zone takes conn_kwargs' connection.
    return execute_trino(trino_conn, sql, conn_kwargs=_trino_kwargs())


@pytest.mark.requires_trino
@pytest.mark.parametrize(
    "set_zone, name, want", [case for case in CASES if case[1] != "json"]
)  # to_json of a value not said to be an instant: refused by name, below
def test_trino_answers_as_postgresql_17_answered(trino_conn, set_zone, name, want):
    result = _in_zone(set_zone, lambda: _on_trino(trino_conn, name))
    assert tuple(_sent(set_zone, row[0]) for row in result.rows) == want


@pytest.mark.requires_trino
@pytest.mark.parametrize("set_zone", list(_SESSIONS))
def test_trino_answers_to_json_of_a_stated_instant_as_postgresql_17(trino_conn, set_zone):
    result = _in_zone(set_zone, lambda: _run_on_trino(trino_conn, _STATED_JSON))
    assert tuple(str(row[0]) for row in result.rows) == ANSWERS[set_zone]["json"]


@pytest.mark.requires_trino
@pytest.mark.parametrize("set_zone", list(_SESSIONS))
def test_trino_refuses_to_json_of_an_instant_by_name(trino_conn, set_zone):
    with pytest.raises(PostgresError, match="to_json of a timestamptz") as refused:
        _in_zone(set_zone, lambda: _on_trino(trino_conn, "json"))
    assert refused.value.sqlstate == "0A000"


@pytest.mark.requires_trino
def test_a_trino_client_after_another_is_on_its_own_zone(trino_conn):
    def current() -> str:
        result = execute_trino(trino_conn, "SELECT current_timezone()", conn_kwargs=_trino_kwargs())
        return result.rows[0][0]

    assert _in_zone("SET TimeZone = 'Asia/Kolkata'", current) == "Asia/Kolkata"
    assert _in_zone("SET TimeZone = 'America/New_York'", current) == "America/New_York"
    cursor = trino_conn.cursor()
    cursor.execute("SELECT current_timezone()")
    assert cursor.fetchall()[0][0] != "Asia/Kolkata"


@pytest.fixture  # each test's: requires_clickhouse removes the container after every test
def clickhouse() -> Iterator[_ServerBackend]:
    backend = _ServerBackend(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    backend.command("DROP TABLE IF EXISTS default.t")
    backend.command(
        "CREATE TABLE default.t (i Int32, c DateTime64(6, 'UTC')) ENGINE = MergeTree ORDER BY i"
    )
    for i, instant in enumerate(INSTANTS):
        wall = instant.replace("+00", "")
        backend.command(f"INSERT INTO default.t SELECT {i}, toDateTime64('{wall}', 6, 'UTC')")
    yield backend
    backend.command("DROP TABLE default.t")
    backend.close()


@pytest.mark.requires_clickhouse
@pytest.mark.parametrize(
    "set_zone, name, want", [case for case in CASES if case[1] != "json"]
)  # to_json has no form on ClickHouse: it is refused by name (pg_forms_json), in every zone
def test_clickhouse_answers_as_postgresql_17_answered(clickhouse, set_zone, name, want):
    def run() -> Any:
        return clickhouse.query_arrow(transpile(statement(name), "clickhouse"))

    rows = _in_zone(set_zone, run).column(0).to_pylist()
    assert tuple(_sent(set_zone, value) for value in rows) == want


@pytest.mark.requires_clickhouse
def test_a_clickhouse_client_after_another_is_on_its_own_zone(clickhouse):
    def current() -> str:
        return clickhouse.query_arrow("SELECT timezone()").column(0)[0].as_py()

    own = current()
    assert _in_zone("SET TimeZone = 'Asia/Kolkata'", current) == "Asia/Kolkata"
    assert _in_zone("SET TimeZone = 'America/New_York'", current) == "America/New_York"
    assert current() == own
