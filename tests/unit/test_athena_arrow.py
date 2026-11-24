# Copyright (c) 2026 Kenneth Stott
# Canary: 5f0c1f9e-2a51-4b7e-9d0e-6c3a8e1b4d27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2194: an Athena DIRECT result is read as Arrow through PyAthena's Arrow cursor, which
reads the result file from the query output location. The executor gets the Arrow table, no
Python row is built at the fetch, and each value is the one PyAthena's row cursor gave. A source
whose credentials cannot read the output location is refused when it connects, naming it."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pytest
from pyarrow import fs
from pyathena.arrow.cursor import ArrowCursor
from pyathena.arrow.result_set import AthenaArrowResultSet
from pyathena.converter import DefaultTypeConverter
from pyathena.error import OperationalError

from provisa.executor.drivers import athena as driver_mod
from provisa.executor.result import ArrowResult, is_arrow

#: The result file Athena writes for a SELECT: a quoted header and values, a null unquoted empty.
_CSV = (
    '"id","price","day","at","t","raw","zoned","tags"\n'
    '"1","1.50","2024-01-02","2024-01-02 03:04:05.123","03:04:05.123","61 62",'
    '"2024-01-02 03:04:05.123 UTC","[a, b]"\n'
    ",,,,,,,\n"
)

#: Its columns as Athena's result metadata describes them (name, type, .., precision, scale, ..).
_DESCRIPTION = [
    ("id", "bigint", None, None, 19, 0, "UNKNOWN"),
    ("price", "decimal", None, None, 10, 2, "UNKNOWN"),
    ("day", "date", None, None, 0, 0, "UNKNOWN"),
    ("at", "timestamp", None, None, 3, 0, "UNKNOWN"),
    ("t", "time", None, None, 3, 0, "UNKNOWN"),
    ("raw", "varbinary", None, None, 0, 0, "UNKNOWN"),
    ("zoned", "timestamp with time zone", None, None, 3, 0, "UNKNOWN"),
    ("tags", "array", None, None, 0, 0, "UNKNOWN"),
]


class _ResultFile(AthenaArrowResultSet):
    """PyAthena's own Arrow result set reading ``path`` as the query output location, with no
    AWS call: the file system is the local one, and the metadata is ``_DESCRIPTION``."""

    def __init__(self, root: Path, converter) -> None:  # noqa: ANN001
        self._converter = converter
        self._block_size = self.DEFAULT_BLOCK_SIZE
        self._fs = fs.SubTreeFileSystem(str(root), fs.LocalFileSystem())

    @property
    def output_location(self) -> str | None:
        return "s3://results/q.csv"

    @property
    def substatement_type(self) -> str | None:
        return "SELECT"

    @property
    def description(self) -> list[tuple[str, str, None, None, int, int, str]] | None:
        return _DESCRIPTION

    def _get_content_length(self) -> int:
        return len(_CSV)


class _Cursor:
    """An Arrow cursor over the result file; a row fetch fails the test."""

    def __init__(self, root: Path, converter) -> None:  # noqa: ANN001
        self._file = _ResultFile(root, converter)
        self.description = _DESCRIPTION
        self.query_id = "q"

    def execute(self, statement, bind=None) -> None:  # noqa: ANN001
        self.statement = statement

    def as_arrow(self) -> pa.Table:
        return self._file._read_csv()

    def fetchall(self):
        raise AssertionError("rows fetched from Athena, which has an Arrow cursor")

    fetchone = fetchmany = fetchall

    def close(self) -> None:
        pass


@pytest.fixture
def result_file(tmp_path: Path) -> Path:
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "q.csv").write_text(_CSV)
    return tmp_path


def _driver(conn) -> driver_mod.AthenaDriver:  # noqa: ANN001
    d = driver_mod.AthenaDriver()
    d._conn = conn
    return d


def _old_rows() -> list[tuple]:
    """The rows PyAthena's row cursor gave: each value of the file converted by its type."""
    convert = DefaultTypeConverter()
    values = [
        ["1", "1.50", "2024-01-02", "2024-01-02 03:04:05.123", "03:04:05.123", "61 62"],
        [None] * 6,
    ]
    types = [d[1] for d in _DESCRIPTION[:6]]
    return [tuple(convert.convert(t, v) for t, v in zip(types, row)) for row in values]


def test_athena_hands_the_executor_arrow_with_the_values_its_rows_had(result_file):
    cursors: list = []

    def cursor(cls, **kwargs):  # noqa: ANN001
        assert cls is ArrowCursor
        cursors.append(_Cursor(result_file, kwargs["converter"]))
        return cursors[-1]

    result = asyncio.run(_driver(SimpleNamespace(cursor=cursor)).execute("SELECT * FROM t"))

    assert is_arrow(result)
    assert isinstance(result, ArrowResult)
    assert result._rows is None  # no Python row built at the fetch
    assert result.stats.row_count == 2
    schema = result.table.schema
    assert schema.field("price").type == pa.decimal128(10, 2)
    assert schema.field("day").type == pa.date32()
    assert schema.field("at").type == pa.timestamp("us")
    assert schema.field("t").type == pa.time64("us")
    assert schema.field("raw").type == pa.binary()
    assert schema.field("tags").type == pa.string()  # a composite is text, as the IR types it

    rows = result.rows
    assert [r[:6] for r in rows] == _old_rows()
    first = rows[0]
    assert first[1] == Decimal("1.50") and type(first[1]) is Decimal
    assert first[2] == date(2024, 1, 2) and type(first[2]) is date
    assert first[3] == datetime(2024, 1, 2, 3, 4, 5, 123000) and first[3].tzinfo is None
    assert first[4] == time(3, 4, 5, 123000)
    assert first[5] == b"ab"
    assert first[6] == datetime(2024, 1, 2, 3, 4, 5, 123000, tzinfo=timezone.utc)
    assert first[7] == "[a, b]"
    assert rows[1][:7] == (None,) * 7


def test_a_statement_with_no_result_set_reports_its_count():
    cur = SimpleNamespace(
        execute=lambda s, b=None: None, description=None, rowcount=3, close=lambda: None
    )
    result = asyncio.run(_driver(SimpleNamespace(cursor=lambda cls, **k: cur)).execute("INSERT"))
    assert result.rows == [] and result.rowcount == 3


def test_an_empty_listing_keeps_its_columns():
    cur = SimpleNamespace(
        execute=lambda s, b=None: None,
        description=[("tab_name", "string", None, None, 0, 0, "UNKNOWN")],
        as_arrow=lambda: pa.Table.from_pydict({}),
        close=lambda: None,
    )
    result = asyncio.run(_driver(SimpleNamespace(cursor=lambda cls, **k: cur)).execute("SHOW"))
    assert result.rows == [] and result.column_names == ["tab_name"]


# -- the output location, checked when the source connects ------------------------------------


class _Probe:
    """A cursor whose statement succeeds in Athena but whose result file cannot be read."""

    def __init__(self, *, state: str, unreadable: bool) -> None:
        self.query_id = None
        self._state = state
        self._unreadable = unreadable
        self.closed = False

    def execute(self, statement) -> None:  # noqa: ANN001
        self.query_id = "q-1"
        if self._unreadable:
            raise OperationalError("AWS Error ACCESS_DENIED during GetObject operation")

    def close(self) -> None:
        self.closed = True


def _connection(probe: _Probe) -> SimpleNamespace:
    executions = {
        "q-1": {
            "QueryExecution": {
                "Status": {"State": probe._state},
                "ResultConfiguration": {"OutputLocation": "s3://athena-out/results/q-1.csv"},
            }
        }
    }
    conn = SimpleNamespace(
        cursor=lambda cls, **kwargs: probe,
        client=SimpleNamespace(
            get_query_execution=lambda QueryExecutionId: executions[QueryExecutionId]
        ),
        closed=False,
    )
    conn.close = lambda: setattr(conn, "closed", True)
    return conn


async def _connect(monkeypatch, conn) -> driver_mod.AthenaDriver:  # noqa: ANN001
    monkeypatch.setattr(driver_mod.pyathena, "connect", lambda **kwargs: conn)
    d = driver_mod.AthenaDriver()
    d.configure({"region": "us-east-1"})
    await d.connect("", 0, "sales", "", "")
    return d


async def test_a_source_that_cannot_read_its_output_location_is_refused_naming_it(monkeypatch):
    probe = _Probe(state="SUCCEEDED", unreadable=True)
    conn = _connection(probe)
    with pytest.raises(PermissionError, match="s3://athena-out/results/q-1.csv"):
        await _connect(monkeypatch, conn)
    assert probe.closed and conn.closed


async def test_a_failed_statement_is_not_called_an_output_location_refusal(monkeypatch):
    probe = _Probe(state="FAILED", unreadable=True)
    with pytest.raises(OperationalError):
        await _connect(monkeypatch, _connection(probe))


async def test_a_source_that_reads_its_output_location_connects(monkeypatch):
    probe = _Probe(state="SUCCEEDED", unreadable=False)
    d = await _connect(monkeypatch, _connection(probe))
    assert d.is_connected and probe.closed
