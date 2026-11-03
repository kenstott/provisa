# Copyright (c) 2026 Kenneth Stott
# Canary: f4c27a91-8e03-4b6d-a5f2-3b90d1e7c648
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""QuickBooks Online as a named source (REQ-2197): the entity catalog's columns, a page's rows,
a statement's conditions lowered into QuickBooks' query language, the reader's paging by
STARTPOSITION/MAXRESULTS, and the refresh that keeps the token Intuit replaces."""

# Requirements: REQ-2197
from __future__ import annotations

import asyncio
import base64
import datetime as dt
import json
import sys
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
import respx

from provisa.api_source.caller import answer_rows, call_api
from provisa.api_source.models import ApiColumnType
from provisa.api_source.oauth_grants import CredentialRefused
from provisa.quickbooks import entities, query
from provisa.quickbooks import read as qb_read
from provisa.quickbooks import token as qb_token
from provisa.quickbooks.entities import columns_of, entity, snake
from provisa.quickbooks.persist import QuickBooksTableRefused, build_endpoint, table_paging
from provisa.quickbooks.pushdown import pushed_arguments
from provisa.quickbooks.rows import page_rows
from provisa.quickbooks.settings import (
    TOKEN_URL,
    InvalidQuickBooksSource,
    parse,
)

BASE = "https://sandbox-quickbooks.api.intuit.com/v3/company/9341453456789012"
QUERY_URL = f"{BASE}/query"


# -- the entity catalog --------------------------------------------------------------------------


def test_the_named_entities_are_all_offered():
    for name in (
        "Customer",
        "Vendor",
        "Invoice",
        "Bill",
        "Payment",
        "Account",
        "Item",
        "Employee",
        "JournalEntry",
        "Estimate",
    ):
        assert name in entities.ENTITIES
    assert len(entities.ENTITIES) >= 25


def test_field_names_are_written_in_snake_case():
    assert snake("TotalAmt") == "total_amt"
    assert snake("PrimaryEmailAddr") == "primary_email_addr"
    assert snake("APAccountRef") == "ap_account_ref"
    assert snake("AllowOnlineACHPayment") == "allow_online_ach_payment"
    assert snake("domain") == "domain"
    assert snake("Id") == "id"


def test_a_reference_is_two_columns_and_line_items_one_json_column():
    by_name = {c.name: c for c in columns_of(entity("Invoice"))}
    assert by_name["customer_ref_value"].path == "CustomerRef"
    assert by_name["customer_ref_value"].filterable
    assert by_name["customer_ref_name"].type == ApiColumnType.string
    assert not by_name["customer_ref_name"].filterable  # QuickBooks filters a ref by its id
    assert by_name["line"].type == ApiColumnType.jsonb
    assert by_name["balance"].type == ApiColumnType.number
    assert by_name["txn_date"].type == ApiColumnType.date
    assert by_name["meta_last_updated_time"].path == "MetaData.LastUpdatedTime"


def test_no_entity_names_a_column_twice():
    for e in entities.ENTITIES.values():
        names = [c.name for c in columns_of(e)]
        assert len(names) == len(set(names)), e.name


def test_an_entity_is_found_by_its_table_name_and_an_unknown_one_is_refused_by_name():
    assert entity("journal_entry").name == "JournalEntry"
    assert entity("Invoice") is entity("invoice")
    with pytest.raises(KeyError, match="no entity 'Ledger'"):
        entity("Ledger")


# -- a page's rows -------------------------------------------------------------------------------

_INVOICE = {
    "Id": "130",
    "SyncToken": "0",
    "DocNumber": "1037",
    "TxnDate": "2026-10-05",
    "TotalAmt": 362.07,
    "Balance": 362.07,
    "CustomerRef": {"value": "24", "name": "Sonnenschein Family Store"},
    "Line": [{"Id": "1", "Amount": 362.07, "DetailType": "SalesItemLineDetail"}],
    "BillAddr": {"Line1": "Russ Sonnenschein", "City": "Middlefield"},
    "CustomerMemo": {"value": "Thank you for your business"},
    "MetaData": {
        "CreateTime": "2026-10-05T13:12:44-07:00",
        "LastUpdatedTime": "2026-10-06T09:00:00-07:00",
    },
}


def _page(entity_name: str, records: list[dict], start: int = 1) -> dict:
    return {
        "QueryResponse": {entity_name: records, "startPosition": start, "maxResults": len(records)},
        "time": "2026-11-03T08:00:00.000-08:00",
    }


def test_a_record_reads_as_one_row_with_its_reference_split_and_its_times():
    (row,) = page_rows(_page("Invoice", [_INVOICE]))
    assert row["id"] == "130"
    assert row["total_amt"] == 362.07
    assert row["customer_ref_value"] == "24"
    assert row["customer_ref_name"] == "Sonnenschein Family Store"
    assert row["line"] == _INVOICE["Line"]
    assert row["customer_memo"] == {"value": "Thank you for your business"}  # no Ref: JSON
    assert row["meta_create_time"] == "2026-10-05T13:12:44-07:00"


def test_a_query_that_matches_nothing_reads_as_no_rows():
    assert page_rows({"QueryResponse": {}, "time": "x"}) == []


def test_an_answer_without_a_query_response_is_refused():
    with pytest.raises(ValueError, match="no QueryResponse"):
        page_rows({"Fault": {}})


# -- the query language --------------------------------------------------------------------------


def test_an_entity_with_an_active_flag_is_read_with_its_inactive_records():
    assert query.entity_query(entity("Invoice")) == "SELECT * FROM Invoice"
    assert (
        query.entity_query(entity("Customer"))
        == "SELECT * FROM Customer WHERE Active IN (true, false)"
    )
    assert (
        query.entity_query(entity("Customer"), ["Active = false"])
        == "SELECT * FROM Customer WHERE Active = false"
    )


def test_a_page_is_asked_for_by_its_start_and_size():
    assert (
        query.page_query("SELECT * FROM Bill", 1001, 1000)
        == "SELECT * FROM Bill STARTPOSITION 1001 MAXRESULTS 1000"
    )
    with pytest.raises(ValueError):
        query.page_query("SELECT * FROM Bill", 1, 1001)
    with pytest.raises(ValueError):
        query.page_query("SELECT * FROM Bill", 0, 10)


def _col(entity_name: str, name: str) -> entities.Column:
    return {c.name: c for c in columns_of(entity(entity_name))}[name]


@pytest.mark.parametrize(
    ("column", "op", "value", "lowered"),
    [
        ("balance", "gt", 0, "Balance > '0'"),
        ("balance", "lte", 100.5, "Balance <= '100.5'"),
        ("total_amt", "in", [1, 2], "TotalAmt IN ('1', '2')"),
        ("txn_date", "gte", "2026-01-01", "TxnDate >= '2026-01-01'"),
        ("txn_date", "lt", dt.date(2026, 2, 1), "TxnDate < '2026-02-01'"),
        ("doc_number", "eq", "1037", "DocNumber = '1037'"),
        ("doc_number", "in", ["1", "2"], "DocNumber IN ('1', '2')"),
        ("customer_ref_value", "eq", "24", "CustomerRef = '24'"),
        ("id", "eq", "O'Brien", "Id = 'O\\'Brien'"),
        (
            "meta_last_updated_time",
            "gt",
            "2026-10-01T00:00:00",
            "MetaData.LastUpdatedTime > '2026-10-01T00:00:00'",
        ),
    ],
)
def test_a_condition_quickbooks_takes_is_lowered(column, op, value, lowered):
    assert query.condition(_col("Invoice", column), op, value) == lowered


@pytest.mark.parametrize(
    ("entity_name", "column", "op", "value"),
    [
        ("Invoice", "balance", "ne", 0),  # QuickBooks has no !=
        ("Invoice", "doc_number", "gt", "1000"),  # text is compared by equality only
        ("Invoice", "private_note", "eq", "x"),  # not a field QuickBooks filters
        ("Invoice", "customer_ref_name", "eq", "x"),  # a reference is filtered by its id
        ("Invoice", "balance", "eq", "0"),  # text against a number
        ("Invoice", "txn_date", "eq", "October"),  # not a date
        ("Invoice", "doc_number", "like", "10%"),  # LIKE only on the fields QuickBooks matches
        ("Customer", "display_name", "like", "A_y%"),  # _ is not a QuickBooks wildcard
        ("Customer", "active", "in", [True]),
        ("Customer", "active", "gt", True),
    ],
)
def test_a_condition_quickbooks_cannot_take_is_left_to_the_engine(entity_name, column, op, value):
    assert query.condition(_col(entity_name, column), op, value) is None


def test_like_and_boolean_are_lowered_where_quickbooks_takes_them():
    assert query.condition(_col("Customer", "display_name"), "like", "Amy%") == (
        "DisplayName LIKE 'Amy%'"
    )
    assert query.condition(_col("Customer", "active"), "eq", True) == "Active = true"


def _endpoint(table: str = "invoice", columns: list[str] | None = None, **kw):
    return build_endpoint("qb", table, columns or [], ttl=300, **kw)


def test_a_statements_conditions_are_pushed_and_the_rest_left_to_the_engine():
    ep = _endpoint()
    args = pushed_arguments(
        ep,
        "SELECT id FROM invoice WHERE balance > 0 AND txn_date >= '2026-01-01' "
        "AND private_note = 'x' LIMIT 5",
        "invoice",
    )
    # The private note is not filterable, so not every condition went: the LIMIT stays.
    assert args == {
        "query": "SELECT * FROM Invoice WHERE Balance > '0' AND TxnDate >= '2026-01-01'"
    }


def test_the_limit_is_pushed_when_every_condition_was():
    args = pushed_arguments(
        _endpoint(), "SELECT id FROM invoice WHERE balance > 0 LIMIT 7", "invoice"
    )
    assert args == {"query": "SELECT * FROM Invoice WHERE Balance > '0'", "max_rows": 7}
    assert pushed_arguments(_endpoint(), "SELECT id FROM invoice LIMIT 3", "invoice") == {
        "max_rows": 3
    }


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM invoice ORDER BY balance LIMIT 7",
        "SELECT DISTINCT doc_number FROM invoice LIMIT 7",
        "SELECT count(*) FROM invoice LIMIT 7",
        "SELECT id FROM invoice LIMIT 7 OFFSET 2",
    ],
)
def test_a_limit_that_needs_more_rows_than_it_names_is_not_pushed(sql):
    assert pushed_arguments(_endpoint(), sql, "invoice") == {}


def test_an_or_and_a_join_are_not_pushed():
    assert (
        pushed_arguments(
            _endpoint(), "SELECT id FROM invoice WHERE balance > 0 OR balance < -5", "invoice"
        )
        == {}
    )
    assert (
        pushed_arguments(
            _endpoint(),
            "SELECT i.id FROM invoice i JOIN customer c ON c.id = i.customer_ref_value "
            "WHERE i.balance > 0",
            "invoice",
        )
        == {}
    )


def test_a_customer_condition_on_active_replaces_the_read_of_both():
    args = pushed_arguments(
        _endpoint("customer"), "SELECT id FROM customer WHERE active = true", "customer"
    )
    assert args == {"query": "SELECT * FROM Customer WHERE Active = true"}


def test_only_a_registered_column_is_pushed():
    ep = _endpoint(columns=["id", "doc_number"])
    assert pushed_arguments(ep, "SELECT id FROM invoice WHERE doc_number = '1'", "invoice") == {
        "query": "SELECT * FROM Invoice WHERE DocNumber = '1'"
    }


# -- registration --------------------------------------------------------------------------------


def test_a_table_is_registered_with_the_entitys_query_typed_columns_and_paging():
    ep = _endpoint(columns=["id", "balance", "customer_ref_value"])
    assert ep.path == "/query" and ep.method == "GET"
    assert ep.query_template == "SELECT * FROM Invoice"
    assert ep.response_normalizer == "quickbooks_query"
    assert [(c.name, c.type) for c in ep.columns] == [
        ("id", ApiColumnType.string),
        ("balance", ApiColumnType.number),
        ("customer_ref_value", ApiColumnType.string),
    ]
    assert ep.pagination is not None
    assert (ep.pagination.page_size, ep.pagination.max_pages) == (1000, 100)


def test_an_unknown_column_or_entity_or_an_oversized_page_is_refused_by_name():
    with pytest.raises(QuickBooksTableRefused, match="no column nope") as refused:
        _endpoint(columns=["id", "nope"])
    assert refused.value.code == "quickbooks.unknown_column"
    with pytest.raises(QuickBooksTableRefused) as refused:
        _endpoint("ledger")
    assert refused.value.code == "quickbooks.unknown_entity"
    from provisa.core.paging import PaginationConfig, PaginationType

    with pytest.raises(QuickBooksTableRefused):
        table_paging(PaginationConfig(type=PaginationType.offset, page_size=5000))


def test_the_settings_name_each_missing_field_and_never_print_their_credentials():
    settings = parse(
        "qb",
        username="client-id",
        password="client-secret",
        database="9341453456789012",
        mapping={"environment": "production", "refresh_token": "rt-entered"},
    )
    assert settings.base_url == "https://quickbooks.api.intuit.com/v3/company/9341453456789012"
    assert "client-secret" not in repr(settings) and "rt-entered" not in repr(settings)
    assert settings.auth().token_url == TOKEN_URL
    sandbox = parse("qb", username="a", password="b", database="123", mapping={})
    assert sandbox.base_url.startswith("https://sandbox-quickbooks.api.intuit.com/")
    for missing, kw in (
        ("client id", {"username": ""}),
        ("client secret", {"password": ""}),
        ("realm id", {"database": ""}),
    ):
        with pytest.raises(InvalidQuickBooksSource, match=missing):
            parse("qb", **{"username": "a", "password": "b", "database": "1", "mapping": {}, **kw})
    with pytest.raises(InvalidQuickBooksSource, match="company's number"):
        parse("qb", username="a", password="b", database="acme", mapping={})
    with pytest.raises(InvalidQuickBooksSource, match="sandbox or production"):
        parse("qb", username="a", password="b", database="1", mapping={"environment": "dev"})


# -- the reader's paging -------------------------------------------------------------------------


def _records(n: int, first: int = 1) -> list[dict]:
    return [{**_INVOICE, "Id": str(i)} for i in range(first, first + n)]


class _Company:
    """A company of ``total`` invoices, answering each query's page, recording what it was
    asked."""

    def __init__(self, total: int) -> None:
        self.total = total
        self.queries: list[str] = []
        self.tokens: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        asked = parse_qs(urlsplit(str(request.url)).query)
        assert asked["minorversion"] == [qb_read.MINOR_VERSION]
        text = asked["query"][0]
        self.queries.append(text)
        self.tokens.append(request.headers["Authorization"])
        start = int(text.split("STARTPOSITION ")[1].split()[0])
        size = int(text.split("MAXRESULTS ")[1])
        n = max(0, min(size, self.total - start + 1))
        return httpx.Response(200, json=_page("Invoice", _records(n, start), start))


async def _token() -> str:
    return "at-1"


def _read(ep, params: dict | None = None, read: qb_read.Read | None = None) -> list[dict]:
    async def run() -> list[dict]:
        return [
            p
            async for p in qb_read.pages(
                ep, params or {}, BASE, _token, lambda: None, timeout=10, read=read
            )
        ]

    return asyncio.run(run())


def test_a_read_follows_startposition_until_a_short_page():
    from provisa.core.paging import PaginationConfig, PaginationType

    ep = _endpoint(pagination=PaginationConfig(type=PaginationType.offset, page_size=4))
    company = _Company(10)
    with respx.mock(assert_all_called=True) as mock:
        mock.get(QUERY_URL).mock(side_effect=company)
        pages = _read(ep)
    assert company.queries == [
        "SELECT * FROM Invoice STARTPOSITION 1 MAXRESULTS 4",
        "SELECT * FROM Invoice STARTPOSITION 5 MAXRESULTS 4",
        "SELECT * FROM Invoice STARTPOSITION 9 MAXRESULTS 4",
    ]
    ids = [r["id"] for p in pages for r in page_rows(p)]
    assert ids == [str(i) for i in range(1, 11)]
    assert set(company.tokens) == {"Bearer at-1"}


def test_a_full_last_page_is_followed_by_one_that_comes_back_empty():
    from provisa.core.paging import PaginationConfig, PaginationType

    ep = _endpoint(pagination=PaginationConfig(type=PaginationType.offset, page_size=5))
    company = _Company(10)
    with respx.mock() as mock:
        mock.get(QUERY_URL).mock(side_effect=company)
        pages = _read(ep)
    assert len(company.queries) == 3 and len(pages) == 3
    assert page_rows(pages[-1]) == []


def test_a_pushed_query_and_limit_shape_every_page():
    from provisa.core.paging import PaginationConfig, PaginationType

    ep = _endpoint(pagination=PaginationConfig(type=PaginationType.offset, page_size=4))
    company = _Company(100)
    read = qb_read.Read()
    with respx.mock() as mock:
        mock.get(QUERY_URL).mock(side_effect=company)
        pages = _read(
            ep, {"query": "SELECT * FROM Invoice WHERE Balance > '0'", "max_rows": 6}, read
        )
    assert company.queries == [
        "SELECT * FROM Invoice WHERE Balance > '0' STARTPOSITION 1 MAXRESULTS 4",
        "SELECT * FROM Invoice WHERE Balance > '0' STARTPOSITION 5 MAXRESULTS 2",
    ]
    assert sum(len(page_rows(p)) for p in pages) == 6
    assert read.more is False


def test_a_read_stops_at_max_pages_and_its_answer_says_it_was_cut(monkeypatch):
    from provisa.core.paging import PaginationConfig, PaginationType

    ep = _endpoint(
        columns=["id"],
        pagination=PaginationConfig(type=PaginationType.offset, page_size=3, max_pages=2),
    )
    company = _Company(50)

    async def token(source_id: str, realm_id: str, auth) -> str:
        assert (source_id, realm_id) == ("qb", "9341453456789012")
        return "at-2"

    monkeypatch.setattr(qb_token, "access_token", token)
    auth = parse("qb", username="a", password="b", database="9341453456789012", mapping={}).auth()
    with respx.mock() as mock:
        mock.get(QUERY_URL).mock(side_effect=company)
        answer = asyncio.run(call_api(ep, {}, base_url=BASE, auth=auth))
    rows, cut = answer_rows(ep, answer)
    assert [r["id"] for r in rows] == ["1", "2", "3", "4", "5", "6"]
    assert cut is not None and cut.max_pages == 2
    assert set(company.tokens) == {"Bearer at-2"}


def test_a_refused_token_is_dropped_and_the_page_asked_for_again(monkeypatch):
    ep = _endpoint()
    tokens = iter(["stale", "fresh"])
    dropped: list[bool] = []

    async def token() -> str:
        return next(tokens)

    def answer(request: httpx.Request) -> httpx.Response:
        if request.headers["Authorization"] == "Bearer stale":
            return httpx.Response(
                401, json={"Fault": {"Error": [{"Message": "AuthenticationFailed"}]}}
            )
        return httpx.Response(200, json=_page("Invoice", _records(2)))

    async def run() -> list[dict]:
        return [
            p
            async for p in qb_read.pages(
                ep, {}, BASE, token, lambda: dropped.append(True), timeout=10
            )
        ]

    with respx.mock() as mock:
        mock.get(QUERY_URL).mock(side_effect=answer)
        pages = asyncio.run(run())
    assert dropped == [True]
    assert len(page_rows(pages[0])) == 2


def test_a_refused_query_fails_with_quickbooks_own_reason():
    ep = _endpoint()
    fault = {
        "Fault": {
            "Error": [
                {
                    "Message": "Error parsing query",
                    "Detail": 'QueryParserError: Encountered "Nope"',
                    "code": "4000",
                }
            ],
            "type": "ValidationFault",
        }
    }
    with respx.mock() as mock:
        mock.get(QUERY_URL).mock(return_value=httpx.Response(400, json=fault))
        with pytest.raises(
            qb_read.QuickBooksRefused, match="Error parsing query: QueryParserError"
        ):
            _read(ep)


# -- the token Intuit replaces -------------------------------------------------------------------


def _auth():
    return parse(
        "qb", username="client-id", password="client-secret", database="1", mapping={}
    ).auth()


def test_a_refresh_sends_the_apps_credentials_as_basic_auth_and_returns_the_replacement():
    with respx.mock() as mock:
        route = mock.post(TOKEN_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "access_token": "at-new",
                    "expires_in": 3600,
                    "refresh_token": "rt-new",
                    "x_refresh_token_expires_in": 8640000,
                    "token_type": "bearer",
                },
            )
        )
        grant = qb_token.exchange(_auth(), "rt-old")
    sent = route.calls.last.request
    expected = base64.b64encode(b"client-id:client-secret").decode()
    assert sent.headers["Authorization"] == f"Basic {expected}"
    assert parse_qs(sent.content.decode()) == {
        "grant_type": ["refresh_token"],
        "refresh_token": ["rt-old"],
    }
    assert (grant.access_token, grant.expires_in, grant.refresh_token) == (
        "at-new",
        3600.0,
        "rt-new",
    )
    assert "rt-new" not in repr(grant)


def test_a_refused_refresh_names_intuits_reason_and_never_the_token():
    with respx.mock() as mock:
        mock.post(TOKEN_URL).mock(return_value=httpx.Response(400, json={"error": "invalid_grant"}))
        with pytest.raises(CredentialRefused) as refused:
            qb_token.exchange(_auth(), "rt-secret-value")
    assert "invalid_grant" in str(refused.value)
    assert "rt-secret-value" not in str(refused.value)
    assert "client-secret" not in str(refused.value)


@pytest.fixture
def _isolated_store(tmp_path, monkeypatch):
    from provisa.api_source import oauth_store
    from provisa.encryption.runtime import reset_encryption

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("PROVISA_ENCRYPTION_KEY", raising=False)
    monkeypatch.setitem(sys.modules, "keyring", None)
    reset_encryption()
    oauth_store._held.clear()
    yield
    oauth_store._held.clear()
    reset_encryption()


def test_each_refresh_keeps_the_token_intuit_replaced_it_with(_isolated_store, control_plane):
    """The entered approval is sent once and kept; every later refresh sends the replacement the
    last one kept, never the entered one again."""
    from provisa.api_source import oauth_store
    from provisa.federation import mail_sync_state

    db, url = control_plane.org(), control_plane.url
    sent: list[str] = []

    def intuit(request: httpx.Request) -> httpx.Response:
        refresh_token = parse_qs(request.content.decode())["refresh_token"][0]
        sent.append(refresh_token)
        n = len(sent)
        return httpx.Response(
            200,
            json={"access_token": f"at-{n}", "expires_in": 3600, "refresh_token": f"rt-{n}"},
        )

    async def entered() -> str | None:
        return "rt-entered"

    def token():
        return oauth_store.stored_access_token(
            db,
            url,
            "acme",
            source_id="qb",
            account="1",
            exchange=lambda rt: qb_token.exchange(_auth(), rt),
            replaces=True,
            earlier=entered,
        )

    async def kept() -> str | None:
        async with db.acquire() as conn:
            return await mail_sync_state.grant(conn, "qb", "1")

    async def run():
        assert await token() == "at-1"
        assert await kept() == "rt-1"
        assert await token() == "at-1"  # held: Intuit is not asked again
        oauth_store.forget("acme", "qb", "1")  # QuickBooks refused the access token
        assert await token() == "at-2"
        assert await kept() == "rt-2"

    with respx.mock() as mock:
        mock.post(TOKEN_URL).mock(side_effect=intuit)
        asyncio.run(run())
    assert sent == ["rt-entered", "rt-1"]


# -- a refresh token entered on the form of a saved source --------------------------------------


def _intuit_answering(sent: list[str]):
    def intuit(request: httpx.Request) -> httpx.Response:
        sent.append(parse_qs(request.content.decode())["refresh_token"][0])
        n = len(sent)
        return httpx.Response(
            200,
            json={"access_token": f"at-{n}", "expires_in": 3600, "refresh_token": f"rt-{n}"},
        )

    return intuit


def _read_token(db, url, entered: str = "rt-entered"):
    from provisa.api_source import oauth_store

    async def earlier() -> str | None:
        return entered

    return oauth_store.stored_access_token(
        db,
        url,
        "acme",
        source_id="qb",
        account="1",
        exchange=lambda rt: qb_token.exchange(_auth(), rt),
        replaces=True,
        earlier=earlier,
    )


def test_a_changed_refresh_token_on_save_replaces_the_kept_one(_isolated_store, control_plane):
    """The owner enters a new approval on an existing source: it is the one sent next, not the
    replacement Intuit answered the last refresh with, and the access token held is dropped."""
    from provisa.federation import mail_sync_state

    db, url = control_plane.org(), control_plane.url
    sent: list[str] = []

    async def run():
        assert await _read_token(db, url) == "at-1"  # rt-entered sent, rt-1 kept
        kept = await qb_token.keep_entered(
            db, url, "acme", source_id="qb", realm_id="1", entered="rt-new", before="rt-entered"
        )
        assert kept is True
        async with db.acquire() as conn:
            assert await mail_sync_state.grant(conn, "qb", "1") == "rt-new"
            assert await mail_sync_state.granted(conn, "qb", "1") == {
                "com.intuit.quickbooks.accounting"
            }
        assert await _read_token(db, url, entered="rt-new") == "at-2"

    with respx.mock() as mock:
        mock.post(TOKEN_URL).mock(side_effect=_intuit_answering(sent))
        asyncio.run(run())
    assert sent == ["rt-entered", "rt-new"]


@pytest.mark.parametrize("entered", ["rt-entered", None, ""])
def test_a_save_that_does_not_change_the_refresh_token_leaves_the_kept_one(
    _isolated_store, control_plane, entered
):
    from provisa.federation import mail_sync_state

    db, url = control_plane.org(), control_plane.url
    sent: list[str] = []

    async def run():
        assert await _read_token(db, url) == "at-1"
        kept = await qb_token.keep_entered(
            db, url, "acme", source_id="qb", realm_id="1", entered=entered, before="rt-entered"
        )
        assert kept is False
        async with db.acquire() as conn:
            assert await mail_sync_state.grant(conn, "qb", "1") == "rt-1"
        assert await _read_token(db, url) == "at-1"  # still held: Intuit is not asked again

    with respx.mock() as mock:
        mock.post(TOKEN_URL).mock(side_effect=_intuit_answering(sent))
        asyncio.run(run())
    assert sent == ["rt-entered"]


def test_a_saved_source_hands_its_entered_token_and_resolved_company_to_the_store(monkeypatch):
    """What a save of a QuickBooks source compares (the tokens as entered) and keeps (the token
    and the realm id resolved in the organisation's vault)."""
    kept: list[dict] = []

    async def keep_entered(state_db, platform_url, org_id, **given):
        kept.append({"state_db": state_db, "platform_url": platform_url, "org_id": org_id, **given})
        return True

    async def resolved(value: str) -> str:
        return {"${secret:qb_rt}": "rt-from-vault", "${secret:realm}": "42"}.get(value, value)

    monkeypatch.setattr(qb_token, "keep_entered", keep_entered)
    monkeypatch.setattr(qb_token, "_resolved", resolved)
    monkeypatch.setattr(qb_token, "_store", lambda: ("state-store", "platform-url", "acme"))
    asyncio.run(
        qb_token.entered_on_save(
            "qb",
            database="${secret:realm}",
            before_mapping={"environment": "sandbox", "refresh_token": "rt-old"},
            mapping={"environment": "sandbox", "refresh_token": "${secret:qb_rt}"},
        )
    )
    asyncio.run(
        qb_token.entered_on_save(
            "qb",
            database="7",
            before_mapping={"refresh_token": "${secret:qb_rt}"},
            mapping={"refresh_token": "${secret:qb_rt}"},
        )
    )
    assert kept == [
        {
            "state_db": "state-store",
            "platform_url": "platform-url",
            "org_id": "acme",
            "source_id": "qb",
            "realm_id": "42",
            "entered": "rt-from-vault",
            "before": "rt-old",
        }
    ]


def test_the_normalizer_is_registered_for_the_api_source_flattener():
    from provisa.api_source.flattener import flatten_response

    ep = _endpoint(columns=["id", "balance", "customer_ref_value", "line", "meta_create_time"])
    (row,) = flatten_response(
        _page("Invoice", [_INVOICE]), None, ep.columns, ep.response_normalizer
    )
    assert row["balance"] == 362.07
    assert json.loads(row["line"]) == _INVOICE["Line"]
    assert row["meta_create_time"] == dt.datetime(2026, 10, 5, 20, 12, 44)
