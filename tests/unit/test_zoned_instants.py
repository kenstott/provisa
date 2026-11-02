# Copyright (c) 2026 Kenneth Stott
# Canary: 94c2d7b1-0e5f-4a36-8b71-d3f6a2c19e58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The session's zone written into what Trino and ClickHouse read of an instant (REQ-2171). ClickHouse
is held to PostgreSQL 17's table on chDB; tests/integration/test_engine_session_zone.py holds both
engines to it on their servers. DuckDB is told its zone and has no such forms."""

# Requirements: REQ-2171

from __future__ import annotations

import datetime
import re
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
import trino

from provisa.core.pg_error import PostgresError
from provisa.core.session_time_zone import StatementZone, bind_zone, reset_zone, trino_spelling
from provisa.executor import trino as trino_exec
from provisa.pgwire._pipeline import _kept_engine_form
from provisa.pgwire.session_zone import SessionZone
from provisa.transpiler.transpile import transpile, transpile_to_duckdb, transpile_to_trino
from tests.unit.pgwire.session_zone_answers import ANSWERS, CASES, INSTANTS, statement

_SESSIONS = {
    "SET TimeZone = 'America/New_York'": "America/New_York",
    "SET TimeZone = 'Asia/Kolkata'": "Asia/Kolkata",
    "SET TimeZone = 'UTC+5'": "UTC+5",
    "SET TIME ZONE -5": "-5",
    "SET TimeZone = 'UTC'": "UTC",
}
_NEW_YORK = "SET TimeZone = 'America/New_York'"
_RULED = StatementZone("EST5EDT4,M3.2.0,M11.1.0", posix="EST5EDT4,M3.2.0,M11.1.0")
_TIMESTAMP_ZONE = "timestamp(%) with time zone"


def _in_zone(zone: StatementZone | None, run: Any) -> Any:
    token = bind_zone(zone)
    try:
        return run()
    finally:
        reset_zone(token)


def _in_session(set_zone: str, run: Any) -> Any:
    return _in_zone(SessionZone(_SESSIONS[set_zone]).statement_zone(), run)


@pytest.fixture(scope="module")
def chdb_session() -> Iterator[Any]:
    from chdb import session

    held = session.Session()
    held.query(
        "CREATE TABLE default.t (i Int32, c DateTime64(6, 'UTC'), plain DateTime64(3), s String) "
        "ENGINE = MergeTree ORDER BY i"
    )
    for i, instant in enumerate(INSTANTS):
        wall = instant.replace("+00", "")
        held.query(
            f"INSERT INTO default.t SELECT {i}, toDateTime64('{wall}', 6, 'UTC'), "
            f"toDateTime64('{wall}', 3, 'UTC'), 'text'"
        )
    yield held
    held.close()


def _rows(held: Any, sql: str) -> list[Any]:
    named = re.sub(r" FROM t\b", " FROM default.t", sql)
    raw = held.query(named, "ArrowStream").bytes()
    return pa.ipc.open_stream(raw).read_all().column(0).to_pylist()


def _sent(value: Any, set_zone: str) -> str:
    if not isinstance(value, datetime.datetime):
        return str(value)
    from buenavista.postgres import _timestamp_text

    return _timestamp_text(value.astimezone(SessionZone(_SESSIONS[set_zone]).written_in()))


@pytest.mark.parametrize("set_zone, name, want", [case for case in CASES if case[1] != "json"])
def test_clickhouse_answers_as_postgresql_17(chdb_session, set_zone, name, want):
    sql = _in_session(set_zone, lambda: transpile(statement(name), "clickhouse"))
    assert tuple(_sent(v, set_zone) for v in _rows(chdb_session, sql)) == want


def test_a_column_that_declares_no_zone_is_a_plain_timestamp_on_clickhouse(chdb_session):
    sql = _in_session(
        _NEW_YORK, lambda: transpile("SELECT plain::text FROM t ORDER BY i", "clickhouse")
    )
    assert "-04" not in _rows(chdb_session, sql)[0]


def test_clickhouse_text_of_a_text_is_that_text(chdb_session):
    sql = _in_session(_NEW_YORK, lambda: transpile("SELECT s::text FROM t", "clickhouse"))
    assert _rows(chdb_session, sql)[0] == "text"


@pytest.mark.parametrize("name", ["date", "hour", "trunc"])
def test_trino_writes_the_zone_into_each_read(name):
    sql = _in_session(_NEW_YORK, lambda: transpile_to_trino(statement(name)))
    assert "'America/New_York'" in sql
    assert _TIMESTAMP_ZONE in sql


def test_trino_text_is_postgresqls_on_utc_too():
    assert _TIMESTAMP_ZONE in transpile_to_trino(statement("text"))


@pytest.mark.parametrize(
    "name, dialect",
    [("date", "trino"), ("hour", "trino"), ("trunc", "trino"), ("hour", "clickhouse")],
)
def test_on_utc_what_already_answers_is_left_as_it_was(name, dialect):
    assert "toTimeZone" not in transpile(statement(name), dialect)
    assert "typeof" not in transpile(statement(name), dialect).lower()


@pytest.mark.parametrize(
    "name, typed", [("date", pa.types.is_date), ("trunc", pa.types.is_timestamp)]
)
def test_on_utc_clickhouse_answers_a_date_and_an_instant_not_a_count(chdb_session, name, typed):
    """Its own Date and DateTime reach Arrow as counts of days and seconds, which pgwire would send
    as numbers: on UTC too, the date is a Date32 and the start of a unit a DateTime64."""
    sql = transpile(statement(name), "clickhouse")
    raw = chdb_session.query(re.sub(r" FROM t\b", " FROM default.t", sql), "ArrowStream").bytes()
    assert typed(pa.ipc.open_stream(raw).schema.field(0).type)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'a'::text",
        "SELECT (a || b)::text FROM t",
        "SELECT a::int::text FROM t",
        "SELECT $1::text",
    ],
)
def test_what_the_statement_says_is_no_instant_is_not_asked_about(sql):
    assert "typeof" not in _in_session(_NEW_YORK, lambda: transpile(sql, "trino")).lower()


def test_an_epoch_is_the_same_in_every_zone():
    sql = _in_session(_NEW_YORK, lambda: transpile("SELECT extract(epoch from c) FROM t", "trino"))
    assert "America/New_York" not in sql


def test_a_zone_cast_to_varchar_of_a_length_keeps_the_length():
    sql = _in_session(_NEW_YORK, lambda: transpile("SELECT c::varchar(5) FROM t", "trino"))
    assert sql.count("VARCHAR(5)") == 2


@pytest.mark.parametrize("dialect, engine", [("trino", "Trino"), ("clickhouse", "ClickHouse")])
def test_a_zone_the_engine_cannot_spell_is_refused_by_name(dialect, engine):
    with pytest.raises(PostgresError, match=f"{engine} cannot run this statement") as refused:
        _in_zone(_RULED, lambda: transpile(statement("hour"), dialect))
    assert refused.value.sqlstate == "0A000"


def test_a_zone_no_engine_form_needs_is_not_refused():
    assert _in_zone(_RULED, lambda: transpile("SELECT 1 AS v FROM t", "trino"))


def test_duckdb_is_told_its_zone_and_has_no_form_of_it():
    plain = transpile_to_duckdb(statement("hour"))
    assert _in_session(_NEW_YORK, lambda: transpile_to_duckdb(statement("hour"))) == plain


def test_an_engine_form_is_derived_again_in_another_zone():
    forms = iter(["in new york", "in kolkata"])
    memo: dict[str, Any] = {}
    state = MagicMock()

    async def derive() -> str:
        return next(forms)

    def kept() -> Any:
        return _kept_engine_form(memo, "SELECT 1", state, derive)

    import asyncio

    assert _in_session(_NEW_YORK, lambda: asyncio.run(kept())) == "in new york"
    assert _in_session(_NEW_YORK, lambda: asyncio.run(kept())) == "in new york"
    in_kolkata = _in_session("SET TimeZone = 'Asia/Kolkata'", lambda: asyncio.run(kept()))
    assert in_kolkata == "in kolkata"


_STATED_JSON = "SELECT to_json(CAST(c AS timestamptz)) AS v FROM t ORDER BY i"


@pytest.mark.parametrize("set_zone", list(_SESSIONS))
def test_trino_writes_to_json_of_a_stated_instant_in_the_session_zone(set_zone):
    sql = _in_session(set_zone, lambda: transpile_to_trino(_STATED_JSON))
    assert "CAST(c AS JSON)" not in sql
    assert "JSON_PARSE(CONCAT('\"'" in sql
    zone = SessionZone(_SESSIONS[set_zone]).statement_zone()
    assert f"'{trino_spelling(zone) if zone is not None else 'UTC'}'" in sql


@pytest.mark.parametrize("call", ["to_json", "to_jsonb"])
@pytest.mark.parametrize("value", ["CAST(c AS timestamptz)", "now()", "(c::timestamptz)"])
def test_trino_writes_each_to_json_of_a_stated_instant(call, value):
    sql = _in_session(_NEW_YORK, lambda: transpile_to_trino(f"SELECT {call}({value}) FROM t"))
    assert "JSON_PARSE(" in sql
    assert "'America/New_York'" in sql


@pytest.mark.parametrize(
    "value", ["ARRAY[CAST(c AS timestamptz)]", "ROW(1, c::timestamptz)", "ARRAY[ARRAY[now()]]"]
)
def test_trino_refuses_to_json_of_a_stated_instant_in_an_array_or_a_row_by_name(value):
    with pytest.raises(PostgresError, match="to_json of a timestamptz in an array") as refused:
        transpile_to_trino(f"SELECT to_json({value}) FROM t")
    assert refused.value.sqlstate == "0A000"


def test_trino_leaves_to_json_of_a_value_not_said_to_be_an_instant_to_its_own_cast():
    assert "CAST(c AS JSON)" in _in_session(
        _NEW_YORK, lambda: transpile_to_trino("SELECT to_json(c) FROM t")
    )


def test_trino_json_text_of_an_instant_is_postgresqls():
    """The form's text, its JSON string marks aside, read as PostgreSQL's to_json writes it."""
    sql = _in_session(_NEW_YORK, lambda: transpile_to_trino(_STATED_JSON))
    assert "'T')" in sql  # the date and the time joined by a T
    assert "'+00:00'" in sql  # UTC's offset, which format_datetime writes Z


def test_to_json_of_an_instant_on_trino_is_refused_by_name():
    cursor = MagicMock()
    cursor.execute.side_effect = trino.exceptions.TrinoUserError(
        {
            "errorType": "USER_ERROR",
            "errorName": "TYPE_MISMATCH",
            "message": "line 1:8: Cannot cast timestamp(1) with time zone to json",
        }
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor
    with (
        patch.object(trino_exec, "_alive", return_value=True),
        pytest.raises(PostgresError, match="to_json of a timestamptz") as refused,
    ):
        trino_exec.execute_trino(conn, "SELECT CAST(c AS JSON) FROM t")
    assert refused.value.sqlstate == "0A000"


def test_the_table_covers_every_session_here():
    assert set(ANSWERS) == set(_SESSIONS)
