# Copyright (c) 2026 Kenneth Stott
# Canary: 5d2a9c4e-81f3-4b6a-9e07-c3f1a8d26b54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The reader that lands a Google Sheets source's replicas through the Google Sheets API, where
the engine has no gsheets extension (REQ-1998, MOTHERDUCK LANDS THROUGH ITS POSTGRES ENDPOINT:
"A Google Sheets source likewise lands through the Google Sheets API").

A registered table is one sheet of the source's spreadsheet (``source.database``): the sheet
titled as the table, or, for the table named after the source itself (the one table the Register
Table picker offers, read by the DuckDB engine's ``read_gsheet`` view as the spreadsheet's first
sheet), the first sheet. Its first row is the header naming the columns; every later row that
holds a value is a row of the table.

A landing makes two Sheets API read requests: one ``spreadsheets.get`` for the sheets' titles and
grid sizes, and one ``values.batchGet`` (``valueRenderOption=UNFORMATTED_VALUE``) whose single
range is the sheet's whole grid, header row and every row under it: nothing is exported and
nothing is capped. A replica build's batch size divides the rows read; it never makes another
request. A read the API refuses with HTTP 429 (the per-minute read quota) is retried with the
truncated exponential backoff Google documents for it.
Unformatted, a date, a time or a timestamp is the spreadsheet's serial number (days since
1899-12-30), so each cell is converted by its registered column's type before the rows become
Arrow record batches of the registered column types, which the landing path writes as the
replica.

The source signs in with its own service account key, as entered in the form
(``mapping.credentials_json``: a path, or a reference to a secret holding the key or its path),
reading as the service account itself: the spreadsheet is shared with it.
"""

# Requirements: REQ-1998
from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable, Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from provisa.core.ir_types import to_ir

#: What a Sheets read asks for: reading, nothing more.
SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
API = "https://sheets.googleapis.com/v4/spreadsheets"
#: The rows of a whole load's batches; a load still reads its sheet in one request.
PAGE_ROWS = 5_000
#: Google stops processing one Sheets API request at 180 seconds
#: (https://developers.google.com/sheets/api/limits, "Behavior and limitations").
_TIMEOUT = 180.0
#: The HTTP status of a Sheets API time-based quota error ("Read requests per minute per user").
_QUOTA_EXCEEDED = 429
#: Truncated exponential backoff, as https://developers.google.com/sheets/api/limits ("Resolve
#: time-based quota errors") documents it: retry n waits min(2**n + random_number_milliseconds,
#: maximum_backoff), random_number_milliseconds at most 1,000 and recalculated per retry,
#: maximum_backoff "typically 32 or 64 seconds", then retries at maximum_backoff up to a maximum
#: number of retries. Seven retries wait 1, 2, 4, 8, 16, 32 and 32 seconds (95 or more in all),
#: past the minute after which the page says the quota is refilled.
MAXIMUM_BACKOFF = 32.0
MAXIMUM_RETRIES = 7
#: Day 0 of a spreadsheet's serial dates.
_SERIAL_EPOCH = datetime(1899, 12, 30)
_MS_PER_DAY = 86_400_000
_INTEGERS = frozenset({"smallint", "integer", "bigint"})
#: The IR type of a column whose numbers carry a Sheets date, time or date-time number format.
_MOMENT_TYPES = {"DATE": "date", "TIME": "time", "DATE_TIME": "timestamp"}
_FLOATS = frozenset({"float", "double"})


class SheetsReadError(Exception):
    """The Sheets API refused a read, or a sheet does not hold what its table declares."""


def service_account_key(source: Any) -> str:
    """The source's service account key, as JSON: ``mapping.credentials_json`` as entered,
    resolved -- the key itself where the reference holds it, else the file it names."""
    from provisa.core.org_files import resolve_path
    from provisa.core.secrets import resolve_secrets

    entered = (getattr(source, "mapping", None) or {}).get("credentials_json")
    if not entered:
        raise ValueError(
            f"Source {source.id!r}: mapping 'credentials_json' (the service account key, a path "
            "or a reference to one) is required to read a Google Sheets source"
        )
    resolved = resolve_secrets(entered)
    if resolved.lstrip().startswith("{"):
        return resolved
    return Path(resolve_path(resolved)).read_text()


def _spreadsheet(source: Any) -> str:
    from provisa.core.secrets import resolve_secrets

    if not source.database:
        raise ValueError(
            f"Source {source.id!r}: 'database' (the spreadsheet id) is required to read a Google "
            "Sheets source"
        )
    return resolve_secrets(source.database)


def column_letters(number: int) -> str:
    """The A1 letters of the 1-based column ``number`` (1 -> A, 27 -> AA)."""
    letters = ""
    while number:
        number, rest = divmod(number - 1, 26)
        letters = chr(ord("A") + rest) + letters
    return letters


def _a1(title: str, first_row: int, last_row: int, columns: int) -> str:
    quoted = title.replace("'", "''")
    return f"'{quoted}'!A{first_row}:{column_letters(columns)}{last_row}"


class SheetsApi:
    """One spreadsheet, read through the Sheets API with a token ``token()`` gives."""

    def __init__(
        self,
        spreadsheet: str,
        token: Callable[[], str],
        client: httpx.Client,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = lambda: random.randint(0, 1000) / 1000,
    ) -> None:
        self._spreadsheet = spreadsheet
        self._token = token
        self._client = client
        self._sleep = sleep
        self._jitter = jitter

    def _get(self, path: str, params: dict[str, str | list[str]]) -> dict:
        retry = 0
        while True:
            answer = self._client.get(
                f"{API}/{self._spreadsheet}{path}",
                params=params,
                headers={"Authorization": f"Bearer {self._token()}"},
                timeout=_TIMEOUT,
            )
            if answer.status_code != _QUOTA_EXCEEDED or retry == MAXIMUM_RETRIES:
                break
            # The documented backoff (MAXIMUM_BACKOFF): 1 + jitter, 2 + jitter, ... capped.
            self._sleep(min(2**retry + self._jitter(), MAXIMUM_BACKOFF))
            retry += 1
        if answer.status_code != 200:
            raise SheetsReadError(
                f"The Google Sheets API refused a read of spreadsheet {self._spreadsheet}: "
                f"{_reason(answer)} (HTTP {answer.status_code})"
            )
        return answer.json()

    def sheets(self) -> list[dict]:
        """Each sheet's properties: title, index and grid size."""
        body = self._get(
            "",
            {"fields": "sheets.properties(title,index,gridProperties(rowCount,columnCount))"},
        )
        return [sheet["properties"] for sheet in body.get("sheets", [])]

    def ranges(self, ranges: list[str]) -> list[list[list[Any]]]:
        """The unformatted values of each of ``ranges``, by row; a range's trailing empty rows
        and cells are left out, as the API leaves them."""
        params: dict[str, str | list[str]] = {
            "ranges": ranges,
            "valueRenderOption": "UNFORMATTED_VALUE",
            "dateTimeRenderOption": "SERIAL_NUMBER",
            "majorDimension": "ROWS",
        }
        body = self._get("/values:batchGet", params)
        return [value_range.get("values", []) for value_range in body.get("valueRanges", [])]

    def grid(self, a1: str) -> list[list[dict]]:
        """Each cell of the range ``a1``, by row: its effective value and its number format's type.
        A row's trailing empty cells, and the range's trailing empty rows, are left out."""
        body = self._get(
            "",
            {
                "ranges": a1,
                "includeGridData": "true",
                "fields": (
                    "sheets.data.rowData.values(effectiveValue,effectiveFormat.numberFormat.type)"
                ),
            },
        )
        return [
            row.get("values", [])
            for sheet in body.get("sheets", [])
            for data in sheet.get("data", [])
            for row in data.get("rowData", [])
        ]


def _reason(answer: httpx.Response) -> str:
    try:
        error = answer.json().get("error", {})
    except ValueError:
        return answer.reason_phrase
    return str(error.get("message") or answer.reason_phrase)


def sheet_of(sheets: list[dict], source: Any, table: Any) -> dict:
    """The sheet ``table`` reads: the one titled as the table, or the first sheet for the table
    named after the source. Any other table names a sheet the spreadsheet does not have."""
    return _sheet_named(sheets, source, table.table_name)


def _sheet_named(sheets: list[dict], source: Any, table_name: str) -> dict:
    for sheet in sheets:
        if sheet["title"] == table_name:
            return sheet
    if table_name == source.id and sheets:
        return min(sheets, key=lambda sheet: sheet.get("index", 0))
    titles = ", ".join(repr(sheet["title"]) for sheet in sheets)
    raise SheetsReadError(
        f"Table {source.id}.{table_name}: spreadsheet {source.database} has no sheet named "
        f"{table_name!r} (its sheets: {titles})"
    )


def _serial(value: Any) -> datetime:
    # A spreadsheet keeps a moment to the millisecond; the float's last digits are no part of it.
    return _SERIAL_EPOCH + timedelta(milliseconds=round(float(value) * _MS_PER_DAY))


def cell(value: Any, ir_type: str, where: str) -> Any:
    """A cell's unformatted value as its column's registered type takes it: a serial number as
    the date, time or timestamp it is; a number in an integer column as that integer; a value in
    a text column as its text. An empty cell is NULL."""
    if value is None or value == "":
        return None
    canonical = to_ir(ir_type)
    number = isinstance(value, (int, float)) and not isinstance(value, bool)
    if canonical == "date":
        return _serial(value).date() if number else date.fromisoformat(str(value))
    if canonical == "timestamp":
        return _serial(value) if number else value
    if canonical == "timestamptz":
        return _serial(value).replace(tzinfo=timezone.utc) if number else value
    if canonical == "time":
        if not number:
            return str(value)
        return _serial(float(value) % 1).time().isoformat()
    if canonical in _INTEGERS:
        if number and float(value).is_integer():
            return int(value)
        if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
            return int(value)
        raise SheetsReadError(f"{where}: {value!r} is not an integer, as its column declares")
    if canonical in _FLOATS:
        return float(value)
    if canonical in ("text", "uuid", "json"):
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)
    return value


def _positions(header: list[Any], columns: list[tuple[str, str]], where: str) -> list[int]:
    names = [str(name) for name in header]
    missing = [name for name, _ in columns if name not in names]
    if missing:
        raise SheetsReadError(
            f"{where}: the sheet's header row has no column {', '.join(map(repr, missing))} "
            f"(its header: {', '.join(map(repr, names))})"
        )
    return [names.index(name) for name, _ in columns]


def row_batches(
    api: SheetsApi, source: Any, table: Any, columns: list[tuple[str, str]], batch_rows: int
) -> Iterator[list[dict]]:
    """``table``'s rows as dicts of its registered ``columns``, typed by them, at most
    ``batch_rows`` to a batch. The sheet is read in one ``values.batchGet`` whose one range is
    its whole grid, the header row with every row under it."""
    where = f"Table {source.id}.{table.table_name}"
    sheet = sheet_of(api.sheets(), source, table)
    grid = sheet.get("gridProperties", {})
    last_row = int(grid.get("rowCount", 0))
    width = int(grid.get("columnCount", 0))
    if last_row == 0 or width == 0:
        raise SheetsReadError(f"{where}: sheet {sheet['title']!r} is empty, with no header row")
    (values,) = api.ranges([_a1(sheet["title"], 1, last_row, width)])
    if not values or not values[0]:
        raise SheetsReadError(f"{where}: sheet {sheet['title']!r} has no header row")
    positions = _positions(values[0], columns, where)
    size = max(1, batch_rows)
    rows: list[dict] = []
    for offset, values_row in enumerate(values[1:]):
        if all(v is None or v == "" for v in values_row):
            continue  # a blank sheet row holds no row of the table
        at = f"{where}, sheet row {offset + 2}"
        rows.append(
            {
                name: cell(values_row[i] if i < len(values_row) else None, ir, f"{at} {name!r}")
                for (name, ir), i in zip(columns, positions)
            }
        )
        if len(rows) == size:
            yield rows
            rows = []
    if rows:
        yield rows


def _cell_type(grid_cell: dict) -> str | None:
    """The IR type one cell's value is, or None for an empty cell."""
    value = grid_cell.get("effectiveValue") or {}
    if "boolValue" in value:
        return "boolean"
    if "numberValue" in value:
        number_format = (grid_cell.get("effectiveFormat") or {}).get("numberFormat") or {}
        moment = _MOMENT_TYPES.get(number_format.get("type", ""))
        if moment is not None:
            return moment
        return "bigint" if float(value["numberValue"]).is_integer() else "double"
    return "text" if value else None  # a string, or an error the cell shows as text


def _column_type(types: set[str]) -> str:
    """The IR type that holds every value of a column whose cells are of ``types``: integers with
    fractions are doubles, dates with date-times are timestamps, and any other mix is text."""
    if len(types) == 1:
        return next(iter(types))
    if types == {"bigint", "double"}:
        return "double"
    if types == {"date", "timestamp"}:
        return "timestamp"
    return "text"  # no value at all, or values no one type holds


def _header_name(grid_cell: dict) -> str:
    value = grid_cell.get("effectiveValue") or {}
    if "numberValue" in value:
        number = float(value["numberValue"])
        return str(int(number)) if number.is_integer() else str(number)
    if "boolValue" in value:
        return "TRUE" if value["boolValue"] else "FALSE"
    return str(value.get("stringValue", ""))


def sheet_columns(api: SheetsApi, source: Any, table_name: str) -> list[tuple[str, str]]:
    """The columns of the sheet ``table_name`` reads, as (name, IR type): each named by its header
    cell and typed by every value under it, so a landing reads each cell as its column holds it
    (``cell``). A number formatted as a date, a time or a date-time is that moment. A column with
    no header cell is no column of the table. The whole grid is read in one request."""
    where = f"Table {source.id}.{table_name}"
    sheet = _sheet_named(api.sheets(), source, table_name)
    grid = sheet.get("gridProperties", {})
    last_row = int(grid.get("rowCount", 0))
    width = int(grid.get("columnCount", 0))
    if last_row == 0 or width == 0:
        raise SheetsReadError(f"{where}: sheet {sheet['title']!r} is empty, with no header row")
    rows = api.grid(_a1(sheet["title"], 1, last_row, width))
    header = [_header_name(c) for c in rows[0]] if rows else []
    if not any(header):
        raise SheetsReadError(f"{where}: sheet {sheet['title']!r} has no header row")
    columns: list[tuple[str, str]] = []
    for i, name in enumerate(header):
        if not name:
            continue
        types = {t for row in rows[1:] if i < len(row) and (t := _cell_type(row[i])) is not None}
        columns.append((name, _column_type(types)))
    return columns


def read_sheet_columns(source: Any, table_name: str) -> list[tuple[str, str]]:
    """``sheet_columns`` of ``source``'s spreadsheet, signed in with its service account key."""
    from provisa.api_source.oauth_grants import service_account_token

    key = service_account_key(source)
    with httpx.Client() as client:
        api = SheetsApi(_spreadsheet(source), lambda: service_account_token(key, [SCOPE]), client)
        return sheet_columns(api, source, table_name)


def _registered_columns(table: Any) -> list[tuple[str, str]]:
    """The table's registered data columns as (name, IR type); a column registered with no type
    is text, as the IR names a type it does not know."""
    return [
        (c.name, to_ir(c.data_type) if c.data_type else "text")
        for c in table.columns
        if getattr(c, "native_filter_type", None) is None
    ]


def make_google_sheets_loader(
    client_factory: Callable[[], httpx.Client] = httpx.Client,
) -> Any:
    """The Google Sheets row-fetch (REQ-1998): whole for :meth:`SourceRowLoader.load`, and a
    range of rows at a time for a replica build (``replica_source``)."""
    from provisa.api_source.oauth_grants import service_account_token

    def _batches(source: Any, table: Any, columns: list[tuple[str, str]], batch_rows: int):
        key = service_account_key(source)
        with client_factory() as client:
            api = SheetsApi(
                _spreadsheet(source), lambda: service_account_token(key, [SCOPE]), client
            )
            yield from row_batches(api, source, table, columns, batch_rows)

    async def _load(source: Any, table: Any) -> list[dict]:
        columns = _registered_columns(table)
        if not columns:
            return []

        def _all() -> list[dict]:
            return [row for rows in _batches(source, table, columns, PAGE_ROWS) for row in rows]

        return await asyncio.to_thread(_all)

    def _replica_source(source: Any, table: Any, columns: list[tuple[str, str]]) -> Any:
        from provisa.federation.replica_source import BlockingCursorSource

        return BlockingCursorSource(
            lambda batch_rows: _batches(source, table, columns, batch_rows), columns
        )

    _load.replica_source = _replica_source  # type: ignore[attr-defined]
    return _load
