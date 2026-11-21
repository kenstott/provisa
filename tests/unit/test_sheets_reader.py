# Copyright (c) 2026 Kenneth Stott
# Canary: 2c71f8a3-94e5-4d1b-a6c0-5b8e3d9f1a27
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1998 (MOTHERDUCK LANDS THROUGH ITS POSTGRES ENDPOINT: "A Google Sheets source likewise
lands through the Google Sheets API where the engine has no gsheets extension"): a registered
sheet is read with one values.batchGet of its whole grid, unformatted -- a landing makes two
Sheets API requests, however many rows it has and whatever its batch size -- a read refused for
the per-minute quota (HTTP 429) is retried with Google's documented truncated exponential
backoff, its serial dates converted by the registered column types, and built into Arrow of those
types for the landing path. Signed in with the source's own service account key, reading as the
service account itself."""

# Requirements: REQ-1998

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import httpx
import jwt
import pyarrow as pa
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from provisa.events import sheets_reader
from provisa.events.sheets_reader import (
    SheetsApi,
    SheetsReadError,
    cell,
    column_letters,
    make_google_sheets_loader,
    row_batches,
    service_account_key,
    sheet_of,
)

SHEET_ID = "sheet-123"


def _source(**kw):
    base = dict(
        id="gs",
        type=SimpleNamespace(value="google_sheets"),
        database=SHEET_ID,
        mapping={"credentials_json": '{"client_email": "sa@x"}'},
    )
    return SimpleNamespace(**{**base, **kw})


def _column(name, data_type, **kw):
    return SimpleNamespace(name=name, data_type=data_type, native_filter_type=None, **kw)


def _table(name="Orders", columns=None):
    return SimpleNamespace(
        table_name=name,
        schema_name="main",
        columns=columns
        or [_column("id", "bigint"), _column("placed", "date"), _column("note", "varchar")],
        landing_filter=None,
    )


class _Spreadsheet:
    """A spreadsheet the Sheets API answers for: its sheets' rows, and every request seen."""

    def __init__(self, sheets: dict[str, list[list]], row_count: int, column_count: int) -> None:
        self.sheets = sheets
        self.row_count = row_count
        self.column_count = column_count
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        assert request.headers["Authorization"] == "Bearer tok"
        if path.endswith("/values:batchGet"):
            query = parse_qs(urlparse(str(request.url)).query)
            assert query["valueRenderOption"] == ["UNFORMATTED_VALUE"]
            assert query["dateTimeRenderOption"] == ["SERIAL_NUMBER"]
            return httpx.Response(
                200,
                json={"valueRanges": [{"values": self._range(r)} for r in query["ranges"]]},
            )
        assert path.endswith(f"/{SHEET_ID}")
        return httpx.Response(
            200,
            json={
                "sheets": [
                    {
                        "properties": {
                            "title": title,
                            "index": index,
                            "gridProperties": {
                                "rowCount": self.row_count,
                                "columnCount": self.column_count,
                            },
                        }
                    }
                    for index, title in enumerate(self.sheets)
                ]
            },
        )

    def _range(self, a1: str) -> list[list]:
        title, cells = a1.rsplit("!", 1)
        title = title.strip("'").replace("''", "'")
        start, end = cells.split(":")
        first = int("".join(c for c in start if c.isdigit()))
        last = int("".join(c for c in end if c.isdigit()))
        rows = self.sheets[title][first - 1 : last]
        while rows and not rows[-1]:
            rows = rows[:-1]  # the API leaves a range's trailing empty rows out
        return rows

    def ranges_asked(self) -> list[list[str]]:
        """Each values.batchGet request's ranges."""
        return [
            parse_qs(urlparse(str(r.url)).query)["ranges"]
            for r in self.requests
            if r.url.path.endswith("/values:batchGet")
        ]


def _api(spreadsheet: _Spreadsheet) -> SheetsApi:
    return SheetsApi(
        SHEET_ID, lambda: "tok", httpx.Client(transport=httpx.MockTransport(spreadsheet))
    )


_ORDERS = [
    ["id", "note", "placed"],
    [1, "first", 45000],
    [2, "", 45001],
    [],  # a blank sheet row
    [3, "third"],  # trailing cells left out by the API
    [4, 5, 45003],
]


# --- cells --------------------------------------------------------------------------------


def test_serial_dates_times_and_timestamps_become_what_their_columns_declare():
    assert cell(45000, "date", "c") == date(2023, 3, 15)
    assert cell(45000.5, "timestamp", "c") == datetime(2023, 3, 15, 12, 0)
    assert cell(45000.25, "timestamptz", "c") == datetime(2023, 3, 15, 6, 0, tzinfo=timezone.utc)
    assert cell(0.75, "time", "c") == "18:00:00"
    # A spreadsheet keeps a moment to the millisecond: 12:30 is not 12:29:59.999999.
    assert cell(45000 + 12.5 / 24, "timestamp", "c") == datetime(2023, 3, 15, 12, 30)


def test_a_date_written_as_text_is_read_as_that_date():
    assert cell("2024-02-29", "date", "c") == date(2024, 2, 29)


def test_numbers_and_text_are_their_columns_types():
    assert cell(3.0, "bigint", "c") == 3 and isinstance(cell(3.0, "bigint", "c"), int)
    assert cell("7", "integer", "c") == 7
    assert cell(2, "double", "c") == 2.0
    assert cell(1.0, "varchar", "c") == "1"
    assert cell(1.5, "text", "c") == "1.5"
    assert cell(True, "text", "c") == "TRUE"
    assert cell(True, "boolean", "c") is True


def test_an_empty_cell_is_null():
    assert cell("", "bigint", "c") is None
    assert cell(None, "date", "c") is None


def test_a_fraction_in_an_integer_column_is_refused_naming_the_cell():
    with pytest.raises(SheetsReadError, match=r"row 4 'id'.*1\.5 is not an integer"):
        cell(1.5, "bigint", "row 4 'id'")


def test_column_letters():
    assert [column_letters(n) for n in (1, 26, 27, 52, 703)] == ["A", "Z", "AA", "AZ", "AAA"]


# --- which sheet --------------------------------------------------------------------------


def test_a_table_reads_the_sheet_of_its_name_or_the_first_for_the_source_table():
    sheets = [{"title": "Second", "index": 1}, {"title": "First", "index": 0}]
    assert sheet_of(sheets, _source(), _table("Second"))["title"] == "Second"
    assert sheet_of(sheets, _source(), _table("gs"))["title"] == "First"


def test_a_table_naming_no_sheet_is_refused_naming_the_sheets():
    with pytest.raises(SheetsReadError, match=r"no sheet named 'Nope'.*'First'"):
        sheet_of([{"title": "First", "index": 0}], _source(), _table("Nope"))


# --- reading by range ----------------------------------------------------------------------


def test_the_sheet_is_read_in_one_batchget_of_its_whole_grid():
    spreadsheet = _Spreadsheet({"Orders": _ORDERS}, row_count=7, column_count=3)
    columns = [("id", "bigint"), ("placed", "date"), ("note", "text")]
    batches = list(row_batches(_api(spreadsheet), _source(), _table(), columns, 3))
    # One range: the header row and every row under it, to the sheet's last row.
    assert spreadsheet.ranges_asked() == [["'Orders'!A1:C7"]]
    assert len(spreadsheet.requests) == 2  # the sheets' properties, then the one batchGet
    assert batches == [
        [
            {"id": 1, "placed": date(2023, 3, 15), "note": "first"},
            {"id": 2, "placed": date(2023, 3, 16), "note": None},
            {"id": 3, "placed": None, "note": "third"},
        ],
        [{"id": 4, "placed": date(2023, 3, 18), "note": "5"}],
    ]


def test_the_batch_size_divides_the_rows_and_never_adds_a_request():
    rows = [["id"]] + [[n] for n in range(1, 12_001)]
    spreadsheet = _Spreadsheet({"Orders": rows}, row_count=len(rows), column_count=1)
    batches = list(row_batches(_api(spreadsheet), _source(), _table(), [("id", "bigint")], 2))
    assert len(spreadsheet.requests) == 2
    assert spreadsheet.ranges_asked() == [["'Orders'!A1:A12001"]]
    assert len(batches) == 6_000 and all(len(batch) == 2 for batch in batches)
    assert [row["id"] for batch in batches for row in batch] == list(range(1, 12_001))


def test_a_registered_column_missing_from_the_header_is_refused_by_name():
    spreadsheet = _Spreadsheet({"Orders": _ORDERS}, row_count=6, column_count=3)
    with pytest.raises(SheetsReadError, match=r"no column 'total'.*'id', 'note', 'placed'"):
        list(row_batches(_api(spreadsheet), _source(), _table(), [("total", "double")], 10))


def test_a_sheet_title_with_a_quote_is_quoted_in_its_range():
    spreadsheet = _Spreadsheet({"Bob's": [["id"], [1]]}, row_count=2, column_count=1)
    rows = list(row_batches(_api(spreadsheet), _source(), _table("Bob's"), [("id", "bigint")], 10))
    assert rows == [[{"id": 1}]]
    assert spreadsheet.ranges_asked() == [["'Bob''s'!A1:A2"]]


def test_a_refusal_by_the_api_names_the_spreadsheet_and_googles_reason():
    def refuse(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403, json={"error": {"message": "The caller does not have permission"}}
        )

    api = SheetsApi(SHEET_ID, lambda: "tok", httpx.Client(transport=httpx.MockTransport(refuse)))
    with pytest.raises(SheetsReadError, match=r"sheet-123: The caller does not have.*HTTP 403"):
        api.sheets()


# --- the per-minute quota: truncated exponential backoff ------------------------------------

_QUOTA = {"error": {"message": "Quota exceeded for quota metric 'Read requests'"}}


def _quota_then(spreadsheet: _Spreadsheet, refusals: int):
    answered: list[int] = []

    def answer(request: httpx.Request) -> httpx.Response:
        answered.append(1)
        if len(answered) <= refusals:
            return httpx.Response(429, json=_QUOTA)
        return spreadsheet(request)

    return answer


def test_a_read_refused_for_the_quota_waits_two_to_the_n_plus_jitter_and_is_retried():
    spreadsheet = _Spreadsheet({"Orders": _ORDERS}, row_count=7, column_count=3)
    waits: list[float] = []
    api = SheetsApi(
        SHEET_ID,
        lambda: "tok",
        httpx.Client(transport=httpx.MockTransport(_quota_then(spreadsheet, 3))),
        sleep=waits.append,
        jitter=lambda: 0.25,
    )
    assert [sheet["title"] for sheet in api.sheets()] == ["Orders"]
    # https://developers.google.com/sheets/api/limits: 1 + jitter, 2 + jitter, 4 + jitter, ...
    assert waits == pytest.approx([1.25, 2.25, 4.25], rel=1e-14)
    assert len(spreadsheet.requests) == 1


def test_the_backoff_is_truncated_at_the_maximum_and_gives_up_after_the_last_retry():
    waits: list[float] = []
    asked: list[int] = []

    def refuse(request: httpx.Request) -> httpx.Response:
        asked.append(1)
        return httpx.Response(429, json=_QUOTA)

    api = SheetsApi(
        SHEET_ID,
        lambda: "tok",
        httpx.Client(transport=httpx.MockTransport(refuse)),
        sleep=waits.append,
        jitter=lambda: 1.0,
    )
    with pytest.raises(SheetsReadError, match=r"sheet-123: Quota exceeded.*HTTP 429"):
        api.sheets()
    assert sheets_reader.MAXIMUM_BACKOFF == pytest.approx(32.0, rel=1e-14)
    assert sheets_reader.MAXIMUM_RETRIES == 7
    assert waits == pytest.approx([2.0, 3.0, 5.0, 9.0, 17.0, 32.0, 32.0], rel=1e-14)
    assert len(asked) == sheets_reader.MAXIMUM_RETRIES + 1


def test_the_jitter_is_at_most_a_second():
    api = SheetsApi(SHEET_ID, lambda: "tok", httpx.Client())
    assert all(0.0 <= api._jitter() <= 1.0 for _ in range(1_000))


def test_a_refusal_other_than_the_quota_is_not_retried():
    asked: list[int] = []
    waits: list[float] = []

    def refuse(request: httpx.Request) -> httpx.Response:
        asked.append(1)
        return httpx.Response(403, json={"error": {"message": "no"}})

    api = SheetsApi(
        SHEET_ID,
        lambda: "tok",
        httpx.Client(transport=httpx.MockTransport(refuse)),
        sleep=waits.append,
    )
    with pytest.raises(SheetsReadError, match="HTTP 403"):
        api.sheets()
    assert len(asked) == 1 and waits == []


# --- the source's own service account -------------------------------------------------------


def test_the_key_is_the_one_entered_as_a_reference_or_a_path(tmp_path, monkeypatch):
    key = '{"client_email": "sa@x"}'
    monkeypatch.setenv("J866_SHEETS_KEY", key)
    assert (
        service_account_key(_source(mapping={"credentials_json": "${env:J866_SHEETS_KEY}"})) == key
    )
    path = tmp_path / "key.json"
    path.write_text(key)
    assert service_account_key(_source(mapping={"credentials_json": str(path)})) == key


def test_a_source_with_no_key_is_refused_by_name():
    with pytest.raises(ValueError, match="'gs'.*credentials_json"):
        service_account_key(_source(mapping={}))


def test_the_service_account_reads_as_itself(monkeypatch):
    # No "sub": a sheet shared with the service account is read as the account, not as a
    # delegated user of a domain.
    from provisa.api_source import oauth_grants

    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    key = json.dumps(
        {"client_email": "sa@p.iam", "private_key": pem, "token_uri": "https://oauth2/token"}
    )
    sent: list[dict] = []

    def exchange(token_url, form, credential):
        sent.append(jwt.decode(form["assertion"], options={"verify_signature": False}))
        assert "reading as itself" in credential
        return {"access_token": "at", "expires_in": 3600}

    monkeypatch.setattr(oauth_grants, "_exchange", exchange)
    monkeypatch.setattr(oauth_grants, "_tokens", {})
    assert oauth_grants.service_account_token(key, [sheets_reader.SCOPE]) == "at"
    assert oauth_grants.service_account_token(key, [sheets_reader.SCOPE]) == "at"  # held
    assert len(sent) == 1
    assert "sub" not in sent[0]
    assert sent[0]["iss"] == "sa@p.iam" and sent[0]["scope"] == sheets_reader.SCOPE


# --- the loader ------------------------------------------------------------------------------


def _loader(monkeypatch, spreadsheet):
    from provisa.api_source import oauth_grants

    monkeypatch.setattr(oauth_grants, "service_account_token", lambda key, scopes: "tok")
    return make_google_sheets_loader(
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(spreadsheet))
    )


@pytest.mark.asyncio
async def test_a_replica_build_reads_arrow_of_the_registered_types(monkeypatch):
    spreadsheet = _Spreadsheet({"Orders": _ORDERS}, row_count=7, column_count=3)
    loader = _loader(monkeypatch, spreadsheet)
    columns = [("id", "bigint"), ("placed", "date"), ("note", "text")]
    source = loader.replica_source(_source(), _table(), columns)
    batches = [b async for b in source.batches(1000)]
    table = pa.Table.from_batches(batches)
    assert table.schema.types == [pa.int64(), pa.date32(), pa.string()]
    assert table.column("id").to_pylist() == [1, 2, 3, 4]
    assert table.column("placed").to_pylist()[0] == date(2023, 3, 15)


@pytest.mark.asyncio
async def test_one_landing_makes_two_sheets_api_requests(monkeypatch):
    # The live warehouse-lane test lands the fixture sheet at batch size 2 and then loads it
    # whole: four requests in all, where paging by the batch size made hundreds and met HTTP 429.
    rows = [["id", "name"]] + [[f"w{n}", f"Widget {n}"] for n in range(1, 1_000)]
    spreadsheet = _Spreadsheet({"gs": rows}, row_count=1_000, column_count=26)
    loader = _loader(monkeypatch, spreadsheet)
    table = _table("gs", [_column("id", "varchar"), _column("name", "varchar")])
    columns = [("id", "text"), ("name", "text")]
    replica = loader.replica_source(_source(), table, columns)
    landed = pa.Table.from_batches([b async for b in replica.batches(2)])
    assert landed.num_rows == 999
    assert len(spreadsheet.requests) == 2
    assert spreadsheet.ranges_asked() == [["'gs'!A1:Z1000"]]
    assert len(await loader(_source(), table)) == 999
    assert len(spreadsheet.requests) == 4


@pytest.mark.asyncio
async def test_a_whole_load_reads_every_row_by_the_registered_types(monkeypatch):
    spreadsheet = _Spreadsheet({"Orders": _ORDERS}, row_count=7, column_count=3)
    rows = await _loader(monkeypatch, spreadsheet)(_source(), _table())
    assert [r["id"] for r in rows] == [1, 2, 3, 4]
    assert rows[0]["placed"] == date(2023, 3, 15)


def test_the_loader_is_wired_where_the_engine_has_no_live_sheets_reach():
    from provisa.events.app_wiring import build_adapter_loaders
    from provisa.federation.connector import Mechanism

    state = SimpleNamespace(config=SimpleNamespace(sources=[]))
    landed = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={
                "google_sheets": SimpleNamespace(mechanism=Mechanism.FETCH, reads_in_place=False)
            }
        )
    )
    assert "google_sheets" in build_adapter_loaders(state, landed)
    trino = SimpleNamespace(
        engine=SimpleNamespace(
            connectors={
                "google_sheets": SimpleNamespace(mechanism=Mechanism.ATTACH_R, reads_in_place=True)
            }
        )
    )
    assert "google_sheets" not in build_adapter_loaders(state, trino)
