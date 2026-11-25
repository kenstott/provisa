# Copyright (c) 2026 Kenneth Stott
# Canary: e6d40b73-1c28-4f95-a07e-3b94c1d7f026
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The type operators family answers as PostgreSQL 17 does (REQ-2171): each statement of
type_io_corpus, written for DuckDB, against what postgres:17.11 answered. Trino and ClickHouse replay
the same statements in tests/integration/test_type_io_on_engines.py."""

# Requirements: REQ-2167, REQ-2171

# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import Any

import duckdb
import psycopg2
import pytest

from provisa.core.pg_error import PostgresError, from_engine
from provisa.pgwire.cast_types import result_casts
from provisa.pgwire.copy_handler import CopyHandler
from provisa.pgwire.server import ProvisaSession, _sqlstate_of, startup_time_zone
from provisa.pgwire.session_zone import UNKNOWN_ZONE, SessionZone, refuse_other_zone
from provisa.transpiler.function_macros import MACROS, outcome
from provisa.transpiler.pg_forms_type_io import _HELD, ROWS, TRINO, ZONED, readable, zoned_call
from provisa.transpiler.pg_type_literals import interval_fields, interval_text
from provisa.transpiler.transpile import transpile, transpile_to_duckdb
from tests.unit.pgwire.pg17_type_io_answers import RECORDED
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401
from tests.unit.pgwire.type_io_corpus import (
    NAME_OF_THE_REFUSAL,
    REFUSALS,
    REFUSED,
    by_function,
    column_statements,
    statements,
    verdict,
)

_FUNCTIONS = by_function(RECORDED)
_ENGINES = ("duckdb", "trino", "clickhouse")


def test_the_corpus_is_what_was_recorded():
    assert list(RECORDED) == statements()


@pytest.fixture(scope="module")
def connection() -> Any:
    connected = duckdb.connect()
    connected.execute("SET TimeZone='UTC'")
    return connected


def _difference(connection: Any, sql: str, recorded: str | None = None) -> str | None:
    """How ``sql`` differs from the answer recorded for ``recorded``, by default ``sql`` itself."""
    recorded = recorded or sql
    written = transpile_to_duckdb(sql, generated=False)
    try:
        if connection.sql(written).types == ["INTERVAL"]:
            got = _interval_read(connection, written)
        else:
            got = _as_sent(connection.execute(written).fetchone()[0])
    except duckdb.Error as raised:
        refusal = from_engine(raised)
        if refusal is None:
            return f"not raised as PostgreSQL's: {raised}"
        return verdict(recorded, RECORDED[recorded], None, f"{refusal.sqlstate}: {refusal}")
    return verdict(recorded, RECORDED[recorded], got, None)


#: DuckDB's text of a date or timestamp BC, which its Python driver hands over where no Python
#: value holds it: the date, its era, the rest.
_DUCKDB_BC = re.compile(r"(?P<day>[0-9]{4,}-[0-9]{2}-[0-9]{2}) \(BC\)(?P<rest>.*)")


def _as_sent(value: Any) -> Any:
    """A value as the wire writes it: a date or timestamp BC as PostgreSQL writes it, its era
    after the clock and offset (REQ-2236; the pgwire encoder writes it from DuckDB's Arrow)."""
    bc = _DUCKDB_BC.fullmatch(value) if isinstance(value, str) else None
    return value if bc is None else f"{bc['day']}{bc['rest']} BC"


def _interval_read(connection: Any, written: str) -> str | None:
    """An interval as the wire writes it (interval_out of its fields): the driver's timedelta
    carries its months into days."""
    _, fields = _fields(connection, written)
    return None if None in fields else interval_text(*(int(f) for f in fields))


@pytest.mark.parametrize("function", sorted(_FUNCTIONS))
def test_duckdb_answers_as_postgresql_17_answered(connection: Any, function: str):
    found = {sql: _difference(connection, sql) for sql in _FUNCTIONS[function]}
    assert {s: why for s, why in found.items() if why not in (None, REFUSED)} == {}
    assert sum(why == REFUSED for why in found.values()) <= REFUSALS.get(function, 0)


_COLUMNS = column_statements(RECORDED, "duckdb")


@pytest.mark.parametrize("sql", sorted(_COLUMNS))
def test_a_column_is_answered_as_its_literal_was(connection: Any, sql: str):
    """A literal's call is folded where the statement is read; a column's reaches the form."""
    assert _difference(connection, sql, _COLUMNS[sql]) is None


def test_a_hash_reads_a_timetz_zone_of_seconds():
    """12:30:45-11:15:30 is 40530 seconds west: hashint8 of its time xor hash_uint32 of 40530."""
    sql = "SELECT timetz_hash(t) AS v FROM (SELECT '12:30:45-11:15:30'::timetz AS t) AS r"
    time = "SELECT time_hash('12:30:45'::time) # hashint4(40530)"
    connection = duckdb.connect()
    assert (
        connection.execute(transpile_to_duckdb(sql, generated=False)).fetchone()[0]
        == connection.execute(transpile_to_duckdb(time, generated=False)).fetchone()[0]
    )


@pytest.mark.parametrize("name, arity", [(row[0], len(row[1])) for row in ROWS])
@pytest.mark.parametrize("dialect", _ENGINES)
def test_every_overload_has_a_form_on_the_engine(name: str, arity: int, dialect: str):
    assert outcome(name, arity, dialect) != "refused"
    assert MACROS[(name, arity)].forms


def test_a_function_is_read_row_by_row():
    connection = duckdb.connect()
    connection.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1), (-7), (NULL)) v(i)")
    rows = connection.execute(
        transpile_to_duckdb("SELECT int4send(i) AS v FROM t", generated=False)
    ).fetchall()
    assert [row[0] for row in rows] == [b"\x00\x00\x00\x01", b"\xff\xff\xff\xf9", None]


def test_a_statement_that_calls_none_of_them_is_not_parsed():
    assert readable("SELECT ¿ FROM t") == "SELECT ¿ FROM t"


def test_a_call_keeps_the_name_of_its_column():
    written = transpile_to_duckdb("SELECT pg_catalog.int4out(NULL::integer)", generated=False)
    assert duckdb.connect().execute(written).description[0][0] == "int4out"


@pytest.mark.parametrize(
    "sql", [sql for sql, (oid, _) in RECORDED.items() if oid is not None and "ROW(" not in sql]
)
def test_the_wire_says_the_type_postgresql_says(sql: str):
    casts = result_casts(sql)
    assert casts is not None and casts[0] is not None and casts[0].oid == RECORDED[sql][0]


#: interval_in of a literal the engine's interval holds: each recorded call of no typmod, and the
#: function check's, which names its type by regtype.
_INTERVALS = {
    **{
        sql: sql
        for sql, (oid, text) in RECORDED.items()
        if sql.startswith('SELECT pg_catalog."interval_in"(\'')
        and "(-1)::integer" in sql
        and oid == 1186
        and text not in (None, "infinity", "-infinity")
    },
    "SELECT pg_catalog.\"interval_in\"('1 day 02:03:04'::cstring, 'interval'::regtype::oid, "
    "(-1)::integer) AS v": "SELECT pg_catalog.\"interval_in\"('1 day 02:03:04'::cstring, 1186::oid, "
    "(-1)::integer) AS v",
}


def _fields(connection: Any, written: str) -> tuple[str, tuple[int, int, int]]:
    """The type DuckDB answers ``written`` in, and its interval's months, days and microseconds."""
    return connection.execute(
        "SELECT typeof(v), (date_part('year', v) * 12 + date_part('month', v), date_part('day', v), "
        "date_part('hour', v) * 3600000000 + date_part('minute', v) * 60000000 + "
        f"date_part('microseconds', v)) FROM ({written}) AS r"
    ).fetchone()


@pytest.mark.parametrize("sql", sorted(_INTERVALS))
def test_interval_in_of_a_literal_is_an_interval_on_duckdb(connection: Any, sql: str):
    """REQ-2167: the wire says interval only of a value the engine holds as one (_describe_casts);
    a literal's interval_in folded to its text was sent as text."""
    kind, fields = _fields(connection, transpile_to_duckdb(sql, generated=False))
    assert (kind, tuple(fields)) == ("INTERVAL", interval_fields(RECORDED[_INTERVALS[sql]][1]))


@pytest.mark.parametrize("sql", sorted(_INTERVALS))
def test_interval_in_of_a_literal_reaches_the_wire_as_an_interval(conn: Any, sql: str):
    """The function check's finding (run 41836627668): the wire said text (25) of interval_in."""
    res = conn.conn.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert (res.ftype(0), res.get_value(0, 0).decode()) == RECORDED[_INTERVALS[sql]]


@pytest.fixture(scope="module")
def postgres(embedded_postgres: Any) -> Any:
    connected = psycopg2.connect(embedded_postgres.dsn())
    connected.autocommit = True
    # An interval as interval_out writes it, not the driver's timedelta.
    as_text = psycopg2.extensions.new_type((1186,), "INTERVAL_TEXT", lambda value, _: value)
    psycopg2.extensions.register_type(as_text, connected)
    yield connected
    connected.close()


@pytest.mark.parametrize("sql", sorted(_INTERVALS))
def test_interval_in_of_a_literal_is_an_interval_on_postgresql(postgres: Any, sql: str):
    cursor = postgres.cursor()
    try:
        cursor.execute(transpile(sql, "postgres", generated=False))
        answer = (cursor.description[0].type_code, cursor.fetchone()[0])
    finally:
        cursor.close()
    assert answer == RECORDED[_INTERVALS[sql]]


_ZONED_CALLS = {
    "timestamptz_in": "SELECT timestamptz_in('2024-06-15 12:30:45+00', 1184, -1)",
    "timestamptz_out": "SELECT pg_catalog.\"timestamptz_out\"('2024-06-15 12:30:45+00'::timestamptz)",
    "timestamptz_send": "SELECT timestamptz_send(t) FROM events",
}


@pytest.mark.parametrize("function", sorted(ZONED))
@pytest.mark.parametrize("zone", ["America/New_York", "Asia/Kolkata", "-5", UNKNOWN_ZONE])
def test_a_form_bound_to_utc_is_refused_by_name_in_another_zone(function: str, zone: str):
    session = ProvisaSession()
    session.role_id = "analyst"
    session.zone = SessionZone(zone)
    with pytest.raises(PostgresError) as raised:
        session._execute_sql_bound(_ZONED_CALLS[function])
    assert _sqlstate_of(raised.value) == "0A000"
    assert str(raised.value).startswith(f"{function} does not read this value")
    assert zone in str(raised.value)


@pytest.mark.parametrize("function", sorted(ZONED))
@pytest.mark.parametrize("zone", [None, "UTC", "utc", "Etc/UTC", "GMT"])
def test_a_session_on_utc_is_answered(function: str, zone: str | None):
    refuse_other_zone(_ZONED_CALLS[function], zone)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "SELECT 'timestamptz_out(' AS said",
        "SELECT timestamp_out('2024-06-15 12:30:45'::timestamp)",
    ],
)
def test_a_statement_that_calls_no_such_form_is_not_refused(sql: str):
    assert zoned_call(sql) is None
    refuse_other_zone(sql, "America/New_York")


def test_a_call_in_a_text_sqlglot_cannot_read_is_taken_for_a_call():
    assert zoned_call("SELECT ¿ timestamptz_out(x) FROM (") == "timestamptz_out"


def _zone_after(*statements: str, started: str | None = None) -> str | None:
    zone = SessionZone(started)
    in_block = False
    for sql in statements:
        word = sql.split()[0].upper()
        opens = word == "BEGIN" or (in_block and word not in ("COMMIT", "ROLLBACK", "ABORT"))
        to_savepoint = sql.upper().startswith("ROLLBACK TO")
        zone.note(sql, in_block, opens or to_savepoint)
        in_block = opens or to_savepoint
    return zone.current


@pytest.mark.parametrize(
    "statements, want",
    [
        (["SET TimeZone TO 'Asia/Tokyo'"], "Asia/Tokyo"),
        (["SET timezone = 'Asia/Tokyo';"], "Asia/Tokyo"),
        (["SET TIME ZONE 'Asia/Tokyo'"], "Asia/Tokyo"),
        (["SET SESSION TIME ZONE 'Asia/Tokyo'"], "Asia/Tokyo"),
        (["SET TIME ZONE -5"], "-5"),
        (["SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE"], "INTERVAL '-08:00' HOUR TO MINUTE"),
        (["SET TimeZone TO 'Asia/Tokyo'", "SET TimeZone TO DEFAULT"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "SET TIME ZONE LOCAL"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "RESET TimeZone"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "RESET ALL"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "DISCARD ALL"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "SET TimeZone TO ''"], "Asia/Tokyo"),
        (["SET LOCAL TimeZone TO 'Asia/Tokyo'"], None),
        (["BEGIN", "SET LOCAL TimeZone TO 'Asia/Tokyo'"], "Asia/Tokyo"),
        (["BEGIN", "SET LOCAL TimeZone TO 'Asia/Tokyo'", "COMMIT"], None),
        (["BEGIN", "SET TimeZone TO 'Asia/Tokyo'", "COMMIT"], "Asia/Tokyo"),
        (["BEGIN", "SET TimeZone TO 'Asia/Tokyo'", "ROLLBACK"], None),
        (["SET TimeZone TO 'Asia/Tokyo'", "BEGIN", "SET TimeZone TO 'UTC'", "ABORT"], "Asia/Tokyo"),
        (["BEGIN", "SET TimeZone TO 'Asia/Tokyo'", "ROLLBACK TO s"], UNKNOWN_ZONE),
        (["SELECT set_config('TimeZone', 'Asia/Tokyo', false)"], "Asia/Tokyo"),
        (["SELECT pg_catalog.set_config('timezone', 'Asia/Tokyo'::text, false)"], "Asia/Tokyo"),
        (["SELECT set_config('TimeZone', 'Asia/Tokyo', true)"], None),
        (["SELECT set_config('search_path', 'a', false)"], None),
        (["SELECT set_config($1, $2, false)"], UNKNOWN_ZONE),
        (["SELECT set_config('TimeZone', $1, false)"], UNKNOWN_ZONE),
    ],
)
def test_the_session_knows_its_zone(statements: list[str], want: str | None):
    assert _zone_after(*statements) == want


@pytest.mark.parametrize(
    "sql, params, want",
    [
        ("SELECT set_config('TimeZone', $1, false)", ["Asia/Tokyo"], "Asia/Tokyo"),
        ("SELECT set_config($1, $2, false)", ["TimeZone", "Asia/Tokyo"], "Asia/Tokyo"),
        ("SELECT set_config($1, $2, false)", ["timezone", "Asia/Tokyo"], "Asia/Tokyo"),
        ("SELECT set_config($1, $2, false)", ["search_path", "a"], None),
        ("SELECT set_config('TimeZone', $1::text, $2)", ["Asia/Tokyo", False], "Asia/Tokyo"),
        ("SELECT set_config('TimeZone', $1, $2::bool)", ["Asia/Tokyo", "off"], "Asia/Tokyo"),
        ("SELECT set_config('TimeZone', $1, $2)", ["Asia/Tokyo", True], None),
        ("SELECT set_config('TimeZone', $1, false)", [None], UNKNOWN_ZONE),
        ("SELECT set_config($1, 'Asia/Tokyo', false)", [None], UNKNOWN_ZONE),
        ("SELECT set_config('TimeZone', $2, false)", ["Asia/Tokyo"], UNKNOWN_ZONE),
        ("SELECT set_config('TimeZone', 'Asia/Tokyo', $1)", None, UNKNOWN_ZONE),
        ("SELECT set_config('TimeZone', 'Asia/Tokyo', $1)", [None], "Asia/Tokyo"),
    ],
)
def test_a_bound_value_of_set_config_is_read_as_the_literal_would_be(sql, params, want):
    zone = SessionZone()
    zone.note(sql, False, False, params)
    assert zone.current == want


def test_set_local_of_a_bound_zone_ends_with_the_block():
    zone = SessionZone()
    zone.note("BEGIN", False, True)
    zone.note("SELECT set_config('TimeZone', $1, $2)", True, True, ["Asia/Tokyo", True])
    assert zone.current == "Asia/Tokyo"
    zone.note("COMMIT", True, False)
    assert zone.current is None


def test_a_bound_zone_postgresql_does_not_know_is_refused():
    with pytest.raises(PostgresError, match='invalid value for parameter "TimeZone"'):
        SessionZone().note("SELECT set_config('TimeZone', $1, false)", False, False, ["Bad/One"])


def test_copy_of_such_a_form_is_refused_in_another_zone():
    session = SimpleNamespace(role_id="analyst", zone=SessionZone("Asia/Tokyo"))
    sql = "COPY (SELECT timestamptz_out(t) FROM events) TO STDOUT"
    with pytest.raises(PostgresError, match="^timestamptz_out does not read this value"):
        CopyHandler(SimpleNamespace()).handle(SimpleNamespace(session=session), sql)


@pytest.mark.parametrize("set_zone", ["SET TimeZone TO 'Asia/Tokyo'", "SET TIME ZONE 'Asia/Tokyo'"])
def test_a_set_of_the_session_reaches_the_refusal(set_zone: str):
    session = ProvisaSession()
    session.role_id = "analyst"
    session.user_id = "analyst"
    session._execute_sql_bound(set_zone)
    assert session.zone.current == "Asia/Tokyo"
    with pytest.raises(PostgresError, match="^timestamptz_out does not read this value"):
        session._execute_sql_bound(_ZONED_CALLS["timestamptz_out"])


def test_reset_returns_to_the_zone_the_connection_started_on():
    assert (
        _zone_after("SET TimeZone TO 'UTC'", "RESET TimeZone", started="Asia/Tokyo") == "Asia/Tokyo"
    )
    assert _zone_after("SET TimeZone TO DEFAULT", started="Asia/Tokyo") == "Asia/Tokyo"


@pytest.mark.parametrize(
    "params, want",
    [
        ({}, None),
        ({"TimeZone": "America/New_York"}, "America/New_York"),  # pgjdbc
        ({"timezone": "America/New_York"}, "America/New_York"),  # libpq, for PGTZ
        ({"options": "-c TimeZone=America/New_York"}, "America/New_York"),
        ({"options": "--timezone=America/New_York"}, "America/New_York"),
        ({"TimeZone": "Asia/Tokyo", "options": "-c TimeZone=UTC"}, "Asia/Tokyo"),
        ({"options": "-c TimeZone=UTC -c TimeZone=Asia/Tokyo"}, UNKNOWN_ZONE),
    ],
)
def test_a_connection_can_start_on_a_zone(params: dict[str, str], want: str | None):
    assert startup_time_zone(params) == want


@pytest.mark.parametrize("function", ["timestamptz_out", "timestamptz_send"])
def test_the_engines_own_zone_does_not_change_an_answer(function: str):
    connection = duckdb.connect()
    connection.execute("SET TimeZone='America/New_York'")
    found = {sql: _difference(connection, sql) for sql in _FUNCTIONS[function]}
    assert {s: why for s, why in found.items() if why not in (None, REFUSED)} == {}
    assert sum(why == REFUSED for why in found.values()) <= REFUSALS.get(function, 0)


@pytest.mark.parametrize(("name", "form"), [(r[0], r[2][TRINO.dialects]) for r in ROWS])
def test_trino_is_handed_nothing_it_refuses_to_analyze(name: str, form: str):
    assert re.search(r"AS INTEGER\) AS (DATE|TIME|VARBINARY|UUID)", form) is None
    assert "[[]" not in form and "[]]" not in form


@pytest.mark.parametrize(
    "sql",
    [
        """SELECT pg_catalog."float4out"('1.4e-45'::real) AS v""",
        """SELECT pg_catalog."float8out"('5e-324'::double precision) AS v""",
    ],
)
def test_trino_writes_the_one_digit_java_does_not(sql: str):
    assert "'%.0e'" in transpile(sql, "trino", generated=False)


def test_trino_does_not_take_a_float4s_digits_from_its_cast():
    """Trino's cast keeps seven digits: 3.402823E38, where PostgreSQL's nine read back."""
    sql = """SELECT pg_catalog."float4out"('3.4028235e38'::real) AS v"""
    written = transpile(sql, "trino", generated=False)
    assert "'%.8e'" in written and "CAST(_s AS REAL)" in written


def test_trino_reads_an_instant_into_utc():
    """Trino keeps a text's offset, which PostgreSQL does not: 23:59:59+02 is 21:59:59+00."""
    sql = """SELECT pg_catalog."timestamptz_in"('1999-12-31 23:59:59+02'::cstring, 1184::oid, (-1)::integer) AS v"""
    assert "TIME ZONE), 'UTC')" in transpile(sql, "trino", generated=False)


def test_a_null_time_is_text_on_clickhouse():
    sql = """SELECT pg_catalog."time_in"(NULL::cstring, NULL::oid, NULL::integer) AS v"""
    assert "toTime64" not in transpile(sql, "clickhouse", generated=False)
    assert "TIME" in transpile(sql, "trino", generated=False)


@pytest.mark.parametrize(
    ("value", "refuses"),
    [
        ("'(1,2)'::point", False),
        ("'1,2'::point", True),
        ("'[(0,0),(3,4)]'::lseg", False),
        ("'((0,0),(1,1))'::lseg", True),
        ("'[(0,0),(1,1)]'::path", False),
        ("'((0,0),(1,1))'::polygon", False),
        ("'(0,0),(1,1)'::polygon", True),
        ("'<(1,2),3>'::circle", False),
    ],
)
def test_a_canonical_geometric_literal_cannot_reach_a_refusal(value: str, refuses: bool):
    stem = value.split("::")[1].replace("polygon", "poly")
    sql = f'SELECT pg_catalog."{stem}_send"({value}) AS v'
    assert (NAME_OF_THE_REFUSAL in transpile(sql, "clickhouse", generated=False)) is refuses


@pytest.mark.parametrize(
    ("value", "said"),
    [
        ("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", None),
        ("x", 'invalid input syntax for type uuid: "x"'),
        (
            "a0eebc99-9c0b-4ef8-bb6d",
            'invalid input syntax for type uuid: "a0eebc99-9c0b-4ef8-bb6d"',
        ),
    ],
)
def test_uuid_in_on_clickhouse_answers_or_raises_by_name_never_an_engine_error(
    value: str, said: str | None
):
    """ClickHouse's NULL of a uuid is a Nullable(UUID): a CAST(NULL AS UUID) is its own error,
    raised in place of the refusal a value it does not read gets."""
    import chdb

    written = transpile(
        f"SELECT pg_catalog.uuid_in('{value}'::cstring) AS v", "clickhouse", generated=False
    )
    if said is None:
        assert chdb.query(written, "TabSeparated").bytes().decode().strip() == value
        return
    with pytest.raises(Exception) as raised:  # noqa: PT011 -- chdb raises its own
        chdb.query(written, "TabSeparated")
    refusal = from_engine(raised.value)
    assert refusal is not None, str(raised.value)[:300]
    assert str(refusal) in (said, f"uuid_in {NAME_OF_THE_REFUSAL}")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT pg_catalog.box_send('(1,1),(0,0)'::box) AS v",
        "SELECT pg_catalog.point_send('(1,2)'::point) AS v",
        "SELECT pg_catalog.circle_send('<(0,0),1>'::circle) AS v",
        "SELECT pg_catalog.line_send('{1,2,3}'::line) AS v",
        "SELECT pg_catalog.lseg_send('[(0,0),(1,1)]'::lseg) AS v",
        "SELECT pg_catalog.path_send('[(0,0),(1,1)]'::path) AS v",
        "SELECT pg_catalog.poly_send('((0,0),(1,1),(2,0))'::polygon) AS v",
    ],
)
def test_a_geometric_send_on_clickhouse_reads_its_numbers_from_a_text_that_is_not_nullable(
    sql: str,
):
    """ClickHouse 24.3 holds no Nullable(Array): extractAll of a Nullable(String) is the engine's
    error 'Nested type Array(String) cannot be inside Nullable type', not an answer. A NULL is
    answered before the numbers are read."""
    written = transpile(sql, "clickhouse", generated=False)
    assert "extractAll(" in written
    assert re.search(r"extractAll\((?!ifNull\()", written) is None, written


#: Edges of a clock and of an era each engine's type does not hold, and PostgreSQL 17's answer
#: (REQ-2236): time 24:00:00, a timetz offset to TZDISP_LIMIT's 15:59, and 4713 BC, the first year
#: a date holds. A send's bytes are in hexadecimal.
_EDGES = [
    ('SELECT pg_catalog."time_out"(\'24:00:00\'::time without time zone) AS v', "24:00:00"),
    ('SELECT pg_catalog."time_send"(\'24:00:00\'::time without time zone) AS v', "000000141DD76000"),
    ('SELECT pg_catalog."timetz_out"(\'00:00:00+15:59\'::time with time zone) AS v', "00:00:00+15:59"),
    ('SELECT pg_catalog."timetz_out"(\'24:00:00-15:59\'::time with time zone) AS v', "24:00:00-15:59"),
    (
        'SELECT pg_catalog."timetz_send"(\'00:00:00+15:59\'::time with time zone) AS v',
        "0000000000000000FFFF1F3C",
    ),
    (
        'SELECT pg_catalog."timetz_send"(\'24:00:00-15:59\'::time with time zone) AS v',
        "000000141DD760000000E0C4",
    ),
    ('SELECT pg_catalog."date_out"(\'4713-01-01 BC\'::date) AS v', "4713-01-01 BC"),
    ('SELECT pg_catalog."date_send"(\'4713-01-01 BC\'::date) AS v', "FFDA97CD"),
    (
        'SELECT pg_catalog."timestamp_out"(\'4713-01-01 00:00:00 BC\'::timestamp without time zone) AS v',
        "4713-01-01 00:00:00 BC",
    ),
    (
        'SELECT pg_catalog."timestamp_send"(\'4713-01-01 00:00:00 BC\'::timestamp without time zone) AS v',
        "FD0F7FBDAF17E000",
    ),
    (
        'SELECT pg_catalog."timestamptz_out"(\'4713-01-01 00:00:00+00 BC\'::timestamp with time zone) AS v',
        "4713-01-01 00:00:00+00 BC",
    ),
    (
        'SELECT pg_catalog."timestamptz_send"(\'4713-01-01 00:00:00+00 BC\'::timestamp with time zone) AS v',
        "FD0F7FBDAF17E000",
    ),
    ('SELECT pg_catalog."date_out"(\'5874897-12-31\'::date) AS v', "5874897-12-31"),
    ('SELECT pg_catalog."date_send"(\'5874897-12-31\'::date) AS v', "7FDA970C"),
    ('SELECT pg_catalog."date_out"(\'-infinity\'::date) AS v', "-infinity"),
    ('SELECT pg_catalog."date_send"(\'infinity\'::date) AS v', "7FFFFFFF"),
    (
        'SELECT pg_catalog."timestamp_out"(\'294276-12-31 23:59:59.999999\'::timestamp without time zone) AS v',
        "294276-12-31 23:59:59.999999",
    ),
    (
        'SELECT pg_catalog."timestamptz_send"(\'294276-12-31 23:59:59.999999+00\'::timestamp with time zone) AS v',
        "7FFFFF5BB3B29FFF",
    ),
    ('SELECT pg_catalog."timestamp_send"(\'-infinity\'::timestamp without time zone) AS v', "8000000000000000"),
]  # fmt: skip
#: A date or timestamp past its type: PostgreSQL's 22008, raised as the statement is read.
_PAST = [
    ('SELECT pg_catalog."date_send"(\'5874898-01-01\'::date) AS v', 'date out of range: "5874898-01-01"'),
    ('SELECT pg_catalog."date_out"(\'4714-01-01 BC\'::date) AS v', 'date out of range: "4714-01-01 BC"'),
    (
        'SELECT pg_catalog."timestamp_hash"(\'294277-01-01 00:00:00\'::timestamp without time zone) AS v',
        'timestamp out of range: "294277-01-01 00:00:00"',
    ),
    (
        'SELECT pg_catalog."timestamptz_out"(\'294277-01-01 00:00:00+00\'::timestamp with time zone) AS v',
        'timestamp out of range: "294277-01-01 00:00:00+00"',
    ),
]  # fmt: skip
#: The same edges' hashes, each with what PostgreSQL computes it of: a time's and a timestamp's is
#: hashint8 of its microseconds, a timetz's its time's xor its zone's seconds west (date.c).
_EDGE_HASHES = [
    (
        'SELECT pg_catalog."time_hash"(\'24:00:00\'::time without time zone) AS v',
        "SELECT hashint8(86400000000::bigint) AS v",
    ),
    (
        'SELECT pg_catalog."time_hash_extended"(\'24:00:00\'::time without time zone, 3::bigint) AS v',
        "SELECT hashint8extended(86400000000::bigint, 3::bigint) AS v",
    ),
    (
        'SELECT pg_catalog."timetz_hash"(\'00:00:00+15:59\'::time with time zone) AS v',
        "SELECT hashint8(0::bigint) # hashint4(-57540) AS v",
    ),
    (
        'SELECT pg_catalog."timetz_hash_extended"(\'24:00:00-15:59\'::time with time zone, 3::bigint) AS v',
        "SELECT hashint8extended(86400000000::bigint, 3::bigint) # hashint4extended(57540, 3::bigint) AS v",
    ),
    (
        'SELECT pg_catalog."timestamp_hash"(\'4713-01-01 00:00:00 BC\'::timestamp without time zone) AS v',
        "SELECT hashint8((-211810204800000000)::bigint) AS v",
    ),
    (
        'SELECT pg_catalog."timestamp_hash_extended"(\'4713-01-01 00:00:00 BC\'::timestamp without time zone, '
        "3::bigint) AS v",
        "SELECT hashint8extended((-211810204800000000)::bigint, 3::bigint) AS v",
    ),
]  # fmt: skip


def _hex_or_text(sql: str, written: str) -> str:
    """``written`` read as the wire writes its answer: a send's bytes in hexadecimal."""
    return f"SELECT upper(hex(v)) AS v FROM ({written}) AS r" if "_send" in sql else written


def _on_chdb(written: str) -> str:
    import chdb

    return chdb.query(written, "TabSeparated").bytes().decode().strip()


#: DuckDB's timestamp ends at 294247-01-10 04:00:54.775806, past which its row refuses a literal
#: with that bound stated (polyfill.duckdb_literals), before any form reads it.
_DUCKDB_EDGES = [(sql, want) for sql, want in _EDGES if "294276-" not in sql]


@pytest.mark.parametrize(("sql", "want"), _DUCKDB_EDGES)
def test_an_edge_postgresql_holds_answers_on_duckdb(connection: Any, sql: str, want: str):
    written = _hex_or_text(sql, transpile_to_duckdb(sql, generated=False))
    assert _as_sent(connection.execute(written).fetchone()[0]) == want


@pytest.mark.parametrize(("sql", "want"), _EDGES)
def test_an_edge_postgresql_holds_answers_on_clickhouse(sql: str, want: str):
    assert _on_chdb(_hex_or_text(sql, transpile(sql, "clickhouse", generated=False))) == want


@pytest.mark.parametrize(("sql", "want"), _EDGES + _EDGE_HASHES)
def test_an_edge_postgresql_holds_is_no_refusal_on_trino(sql: str, want: str):
    """Trino's TIME holds no 24:00:00 and its TIMESTAMP no year BC: such a literal is answered as
    the statement is read, so no input of it reaches the engine. A timetz is held as its text."""
    written = transpile(sql, "trino", generated=False)
    for function in ("time_in", "date_in", "timestamp_in", "timestamptz_in"):
        assert f"{function} {NAME_OF_THE_REFUSAL}" not in written
    if "timetz" in sql:
        (text,) = re.findall(r"'([^']+)'::time with time zone", sql)
        (pattern,) = [held[2] for held in _HELD if held[0] == "timetz_in"]
        assert re.fullmatch(pattern, text)


@pytest.mark.parametrize(("sql", "same"), _EDGE_HASHES)
def test_an_edges_hash_is_postgresql_s_on_duckdb_and_clickhouse(
    connection: Any, sql: str, same: str
):
    want = connection.execute(transpile_to_duckdb(same, generated=False)).fetchone()[0]
    assert connection.execute(transpile_to_duckdb(sql, generated=False)).fetchone()[0] == want
    assert int(_on_chdb(transpile(sql, "clickhouse", generated=False))) == want


#: The recorded statements of a date or timestamp BC: ClickHouse's Date32 and DateTime64 hold no
#: year BC, so each is answered as the statement is read.
_RECORDED_BC = [
    sql
    for sql, (oid, _) in RECORDED.items()
    if " BC'::" in sql and oid is not None and re.search(r'"(date|timestamp)(tz)?_(out|send)"', sql)
]


@pytest.mark.parametrize("sql", _RECORDED_BC)
def test_a_recorded_date_bc_answers_on_clickhouse(sql: str):
    written = transpile(sql, "clickhouse", generated=False)
    got = _on_chdb(_hex_or_text(sql, written))
    want = RECORDED[sql][1]
    assert (("\\x" + got.lower()) if "_send" in sql else got) == want


@pytest.mark.parametrize(("sql", "said"), _PAST)
@pytest.mark.parametrize("dialect", _ENGINES)
def test_a_date_or_timestamp_past_its_type_raises_postgresql_s_22008(
    sql: str, said: str, dialect: str
):
    written = transpile(sql, dialect, generated=False)
    if dialect == "trino":
        assert "22008" in written and said in written, written[:400]
        return
    with pytest.raises(Exception) as raised:  # noqa: PT011 -- each engine raises its own
        if dialect == "duckdb":
            duckdb.connect().execute(written).fetchall()
        else:
            _on_chdb(written)
    refusal = from_engine(raised.value)
    assert refusal is not None and (refusal.sqlstate, str(refusal)) == ("22008", said)
