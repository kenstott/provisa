# Copyright (c) 2026 Kenneth Stott
# Canary: 9b7e2f41-6c03-4d58-a1e9-3f80c5d27b6a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2194: the Oracle driver reads a result as Arrow through python-oracledb's data-frame fetch
(``fetch_df_all`` / ``fetch_df_batches``), never its Python rows, with the types the statement
declares; a type that fetch has no Arrow form for is refused by name before the statement runs.

The connection is a fake standing in for python-oracledb's: its data-frame fetch hands back the
pyarrow table python-oracledb builds for the described columns (the PyCapsule stream pa.table reads
is pyarrow's own here)."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, NamedTuple

import oracledb  # pyright: ignore[reportMissingImports]
import pyarrow as pa
import pytest

from provisa.core.ir_types import interval_value
from provisa.executor.drivers.oracle import OracleDriver
from provisa.executor.drivers.oracle_arrow import OracleArrowUnsupported
from provisa.executor.result import ArrowResult


class _Col(NamedTuple):
    name: str
    type_code: Any


class _Cursor:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn
        self.description: Any = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def parse(self, sql: str) -> None:
        self._conn.parsed.append(sql)
        self.description = self._conn.described

    def execute(self, sql, params, **kwargs):
        self._conn.executed.append((sql, params, kwargs))

    def close(self) -> None:
        pass


class _Conn:
    def __init__(self, described: list[_Col] | None, frames: list[pa.Table]) -> None:
        self.described = described
        self._frames = frames
        self.parsed: list[str] = []
        self.executed: list[tuple] = []
        self.df_calls: list[tuple] = []
        self.commits = 0

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def fetch_df_all(self, statement, parameters, arraysize=None, *, fetch_decimals=None):
        self.df_calls.append(("all", statement, parameters, fetch_decimals))
        return pa.concat_tables(self._frames)

    def fetch_df_batches(self, statement, parameters, size=None, *, fetch_decimals=None):
        self.df_calls.append(("batches", statement, parameters, fetch_decimals))
        yield from self._frames

    def cancel(self) -> None:
        pass

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        pass


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self.conn = conn
        self.returned = 0
        self.discarded = 0

    @contextmanager
    def connection(self, is_broken):
        yield self.conn

    def getconn(self) -> _Conn:
        return self.conn

    def putconn(self, _conn) -> None:
        self.returned += 1

    def discard(self, _conn) -> None:
        self.discarded += 1


def _driver(conn: _Conn) -> tuple[OracleDriver, _Pool]:
    driver = OracleDriver()
    pool = _Pool(conn)
    driver._pool = pool  # pyright: ignore[reportAttributeAccessIssue]
    return driver, pool


_TYPED = [
    _Col("AMOUNT", oracledb.DB_TYPE_NUMBER),
    _Col("BORN", oracledb.DB_TYPE_DATE),
    _Col("SEEN", oracledb.DB_TYPE_TIMESTAMP),
    _Col("NOTE", oracledb.DB_TYPE_CLOB),
    _Col("PHOTO", oracledb.DB_TYPE_BLOB),
]


def _typed_frame() -> pa.Table:
    # What python-oracledb builds for NUMBER(10,2), DATE, TIMESTAMP(9), CLOB and BLOB with
    # fetch_decimals: its own Arrow types for them.
    return pa.table(
        {
            "AMOUNT": pa.array([Decimal("12.50"), None], pa.decimal128(10, 2)),
            "BORN": pa.array([datetime(2026, 1, 2, 3, 4, 5), None], pa.timestamp("s")),
            "SEEN": pa.array([1_767_322_800_123_456_789, None], pa.timestamp("ns")),
            "NOTE": pa.array(["a long text", None], pa.large_string()),
            "PHOTO": pa.array([b"\x89PNG", None], pa.large_binary()),
        }
    )


async def test_a_result_is_read_through_the_arrow_fetch_with_its_declared_types():
    conn = _Conn(_TYPED, [_typed_frame()])
    driver, _ = _driver(conn)
    result = await driver.execute("SELECT * FROM t WHERE id = $1", [7])

    assert isinstance(result, ArrowResult)
    # The statement is read as Arrow, its NUMBER(p,s) columns as decimals, never run as rows.
    assert conn.df_calls == [("all", "SELECT * FROM t WHERE id = :1", [7], True)]
    assert conn.executed == []
    assert conn.commits == 1
    assert result.column_names == ["amount", "born", "seen", "note", "photo"]
    schema = result.table.schema
    assert schema.field("amount").type == pa.decimal128(10, 2)
    assert schema.field("born").type == pa.timestamp("s")
    assert schema.field("seen").type == pa.timestamp("ns")  # the batch keeps its nanoseconds
    assert schema.field("note").type == pa.large_string()  # a CLOB as its text
    assert schema.field("photo").type == pa.large_binary()  # a BLOB as its bytes
    assert result.rows == [
        (
            Decimal("12.50"),
            datetime(2026, 1, 2, 3, 4, 5),
            datetime(2026, 1, 2, 3, 0, 0, 123456),  # a row holds microseconds, truncated
            "a long text",
            b"\x89PNG",
        ),
        (None, None, None, None, None),
    ]


async def test_a_local_time_zone_column_is_read_as_its_utc_instant():
    conn = _Conn(
        [_Col("AT", oracledb.DB_TYPE_TIMESTAMP_LTZ)],
        [pa.table({"AT": pa.array([datetime(2026, 3, 1, 12, 0)], pa.timestamp("us"))})],
    )
    driver, _ = _driver(conn)
    result = await driver.execute("SELECT at FROM t")
    assert isinstance(result, ArrowResult)
    assert result.table.schema.field("at").type == pa.timestamp("us", tz="UTC")
    assert result.rows == [(datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),)]


async def test_a_zoned_column_is_cast_in_the_database_and_read_as_its_utc_instant():
    conn = _Conn(
        [_Col("AT", oracledb.DB_TYPE_TIMESTAMP_TZ)],
        [pa.table({"C0": pa.array([datetime(2026, 3, 1, 12, 0)], pa.timestamp("us"))})],
    )
    driver, _ = _driver(conn)
    result = await driver.execute("SELECT at FROM t")
    (_, statement, _, _) = conn.df_calls[0]
    assert statement.startswith("WITH provisa_read (c0) AS (SELECT at FROM t)")
    assert "CAST(c0 AS TIMESTAMP WITH LOCAL TIME ZONE)" in statement
    assert isinstance(result, ArrowResult)
    assert result.column_names == ["at"]
    assert result.table.schema.field("at").type == pa.timestamp("us", tz="UTC")


async def test_an_interval_is_read_as_its_ir_interval():
    month_day_nano = pa.array(
        [pa.MonthDayNano([14, 0, 0]), pa.MonthDayNano([0, 3, 4_000_000_000])],
        pa.month_day_nano_interval(),
    )
    conn = _Conn([_Col("SPAN", oracledb.DB_TYPE_INTERVAL_YM)], [pa.table({"SPAN": month_day_nano})])
    driver, _ = _driver(conn)
    result = await driver.execute("SELECT span FROM t")
    assert result.rows == [(interval_value(14, 0, 0),), (interval_value(0, 3, 4_000_000),)]


@pytest.mark.parametrize(
    ("described", "cast"),
    [
        (oracledb.DB_TYPE_ROWID, "CAST(c1 AS VARCHAR2(4000)) AS c1"),
        (oracledb.DB_TYPE_UROWID, "CAST(c1 AS VARCHAR2(4000)) AS c1"),
        (oracledb.DB_TYPE_XMLTYPE, "XMLSERIALIZE(CONTENT c1 AS CLOB) AS c1"),
    ],
)
async def test_a_type_without_an_arrow_form_is_cast_to_its_text_in_the_database(described, cast):
    conn = _Conn(
        [_Col("ID", oracledb.DB_TYPE_NUMBER), _Col("R", described)],
        [
            pa.table(
                {
                    "C0": pa.array([1.0], pa.float64()),
                    "C1": pa.array(["AAAR3sAAEAAAACXAAA"], pa.large_string()),
                }
            )
        ],
    )
    driver, _ = _driver(conn)
    result = await driver.execute("SELECT id, r FROM t")
    (_, statement, _, _) = conn.df_calls[0]
    assert statement == (
        f"WITH provisa_read (c0, c1) AS (SELECT id, r FROM t) SELECT c0, {cast} FROM provisa_read"
    )
    assert isinstance(result, ArrowResult)
    assert result.column_names == ["id", "r"]
    assert result.table.schema.field("r").type == pa.large_string()
    assert result.rows == [(1.0, "AAAR3sAAEAAAACXAAA")]


async def test_a_json_column_is_read_as_its_text_typed_as_json():
    conn = _Conn(
        [_Col("DOC", oracledb.DB_TYPE_JSON)],
        [pa.table({"C0": pa.array(['{"a":[1,2]}', None], pa.large_string())})],
    )
    driver, _ = _driver(conn)
    result = await driver.execute("SELECT doc FROM t")
    (_, statement, _, _) = conn.df_calls[0]
    assert statement == (
        "WITH provisa_read (c0) AS (SELECT doc FROM t) "
        "SELECT JSON_SERIALIZE(c0 RETURNING CLOB) AS c0 FROM provisa_read"
    )
    assert isinstance(result, ArrowResult)
    assert result.table.schema.field("doc").type == pa.json_(pa.large_string())
    assert result.rows == [('{"a":[1,2]}',), (None,)]


async def test_a_streamed_json_column_is_typed_as_json():
    from provisa.executor.drivers.oracle import _ArrowCursor

    conn = _Conn(
        [_Col("DOC", oracledb.DB_TYPE_JSON)],
        [pa.table({"C0": pa.array(['{"a":1}'], pa.large_string())})],
    )
    cursor = _ArrowCursor(conn)
    cursor.execute("SELECT doc FROM t", [])
    (_, statement, _, _) = conn.df_calls[0]
    assert "JSON_SERIALIZE(c0 RETURNING CLOB) AS c0" in statement
    batch = cursor.fetch_batch()
    assert batch.schema.field("doc").type == pa.json_(pa.large_string())
    assert batch.column(0).to_pylist() == ['{"a":1}']


@pytest.mark.parametrize(
    "unreadable", [oracledb.DB_TYPE_OBJECT, oracledb.DB_TYPE_BFILE, oracledb.DB_TYPE_CURSOR]
)
async def test_a_type_without_an_arrow_form_is_refused_by_name_before_it_runs(unreadable):
    conn = _Conn([_Col("ID", oracledb.DB_TYPE_NUMBER), _Col("R", unreadable)], [])
    driver, _ = _driver(conn)
    with pytest.raises(OracleArrowUnsupported) as refused:
        await driver.execute("SELECT id, r FROM t")
    assert refused.value.column == "r"
    assert refused.value.type_name == unreadable.name
    # Never retried as Python rows, nor run at all.
    assert conn.df_calls == []
    assert conn.executed == []


async def test_a_statement_without_rows_is_run_on_a_cursor():
    conn = _Conn(None, [])
    driver, _ = _driver(conn)
    result = await driver.execute("UPDATE t SET n = $1", [1])
    assert conn.executed == [("UPDATE t SET n = :1", [1], {})]
    assert conn.df_calls == []
    assert result.rows == [] and result.column_names == []
    assert conn.commits == 1


async def test_a_stream_reads_arrow_batches_and_builds_only_the_rows_asked_for():
    frames = [
        pa.table({"N": pa.array([Decimal(i) for i in range(s, s + 3)], pa.decimal128(5, 0))})
        for s in (0, 3)
    ]
    conn = _Conn([_Col("N", oracledb.DB_TYPE_NUMBER)], frames)
    driver, pool = _driver(conn)
    stream = await driver.open_stream("SELECT n FROM t")
    assert conn.df_calls == [("batches", "SELECT n FROM t", [], True)]
    assert stream.column_names == ["n"]

    fetched = []
    while rows := await stream.fetch(2):
        assert len(rows) <= 2
        fetched.append(rows)
    assert fetched == [
        [(Decimal(0),), (Decimal(1),)],
        [(Decimal(2),), (Decimal(3),)],
        [(Decimal(4),), (Decimal(5),)],
    ]
    # Read to its end, the connection is committed and given back.
    assert conn.commits == 1 and pool.returned == 1 and pool.discarded == 0


async def test_a_stream_hands_on_the_record_batches_as_fetched():
    from provisa.executor.drivers.oracle import _ArrowCursor

    frames = [pa.table({"N": pa.array([1.5, 2.5], pa.float64())})]
    conn = _Conn([_Col("N", oracledb.DB_TYPE_NUMBER)], frames)
    cursor = _ArrowCursor(conn)
    cursor.execute("SELECT n FROM t", [])
    batch = cursor.fetch_batch()
    assert isinstance(batch, pa.RecordBatch)
    assert batch.schema.names == ["n"] and batch.column(0).to_pylist() == [1.5, 2.5]
    assert cursor.fetch_batch() is None
