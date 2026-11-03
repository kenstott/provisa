# Copyright (c) 2026 Kenneth Stott
# Canary: 9a3e61c8-f05b-4d27-b4a9-7c2d18e5f063
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live QuickBooks Online source (REQ-2197): a real sandbox company added, its invoices and
customers registered and read through the admin API the UI uses, on an in-process app.

What it proves that the mocked company of tests/unit/test_quickbooks_source.py cannot: Intuit
exchanges the refresh token for an access token under the app's own credentials; QuickBooks
answers the entity queries the source sends; a condition the statement puts on a filterable
field (``balance > 0``) is sent in QuickBooks' query and the company answers only rows that meet
it; and a read follows STARTPOSITION across more than one page.

The app runs in this process on the local platform (its own Postgres); the integration compose
stack is not started. Nothing is written to the company.

Credentials
-----------
There is no QuickBooks emulator: a sandbox company and an Intuit app are required. Read from the
environment (``.env`` through tests/env_creds.py), and handed to the source only as ``${env:…}``
references, so no value is in a mutation, an answer or a log:

  QUICKBOOKS_CLIENT_ID      the Intuit app's client id
  QUICKBOOKS_SECRET         its client secret
  QUICKBOOKS_REALM_ID       the sandbox company's realm id
  QUICKBOOKS_REFRESH_TOKEN  an approval of the app for that company (Accounting scope)

Intuit answers each refresh with the refresh token to send next, and the one it replaces stops
working. The source keeps the newest in the organisation's state store of this app's platform.

Like the live Shopify test it is in no CI lane (the company is the maintainer's own; CI holds no
QuickBooks secrets), and a credential that is not set fails it by name.
"""

# Requirements: REQ-2197
from __future__ import annotations

import json
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.env_creds import unset
from tests.helpers import registered_id, release_mutation

pytestmark = [
    pytest.mark.integration,
    pytest.mark.local_platform,
    pytest.mark.requires_warehouse,
    pytest.mark.requires_quickbooks,
    pytest.mark.asyncio(loop_scope="session"),
]

_ENV = (
    "QUICKBOOKS_CLIENT_ID",
    "QUICKBOOKS_SECRET",
    "QUICKBOOKS_REALM_ID",
    "QUICKBOOKS_REFRESH_TOKEN",
)

_SOURCE_ID = "quickbooks-live"
_DOMAIN = "quickbooks_live"
_ROLE = "org_admin"
_VISIBLE_TO = f'["{_ROLE}"]'
# The page a read asks for: small, so a sandbox company's few dozen customers are read across
# pages. A table is registered at QuickBooks' largest page, 1,000.
_PAGE_SIZE = 5


@pytest.fixture(scope="module")
def creds() -> None:
    missing = unset(*_ENV)
    if missing:
        pytest.fail(f"not set: {missing}")


@pytest_asyncio.fixture(loop_scope="session", scope="module")
async def client(creds):
    """An in-process app."""
    import provisa.api.app as app_mod

    del creds
    app = app_mod.create_app()
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test", timeout=300) as c:
            yield c


async def _mutation(client: AsyncClient, query: str, field: str) -> dict[str, Any]:
    """An admin GraphQL mutation's result, success or not."""
    resp = await client.post("/admin/graphql", json={"query": query})
    assert resp.status_code == 200, f"{field}: HTTP {resp.status_code}: {resp.text}"
    body = resp.json()
    assert "errors" not in body, f"{field}: GraphQL errors: {resp.text}"
    return body["data"][field]


async def _registered(client: AsyncClient, table: str, alias: str, columns: list[str]) -> None:
    """``table`` registered with ``columns`` and released, as a steward does."""
    cols = ", ".join(f'{{ name: "{c}", visibleTo: {_VISIBLE_TO} }}' for c in columns)
    result = await _mutation(
        client,
        f"""
        mutation {{
            registerTable(input: {{
                sourceId: "{_SOURCE_ID}", domainId: "{_DOMAIN}", schemaName: "default",
                tableName: "{table}", alias: "{alias}", columns: [{cols}]
            }}) {{ success message code }}
        }}
        """,
        "registerTable",
    )
    assert result["success"], result
    released = await _mutation(
        client, release_mutation(registered_id(result["message"])), "setTableDraft"
    )
    assert released["success"], released


async def _sql(client: AsyncClient, sql: str) -> list[dict[str, Any]]:
    resp = await client.post("/data/sql", json={"sql": sql, "role": _ROLE})
    assert resp.status_code == 200, f"{sql!r}: HTTP {resp.status_code}: {resp.text}"
    return resp.json()["data"]["sql"]


@pytest_asyncio.fixture(loop_scope="session", scope="module")
async def company(client) -> None:
    """The sandbox company added as a QuickBooks source, its credentials as references."""
    domain = await _mutation(
        client,
        f'mutation {{ createDomain(input: {{ id: "{_DOMAIN}", description: "QuickBooks live" }}) '
        "{ success message } }",
        "createDomain",
    )
    assert domain["success"], domain
    mapping = {"environment": "sandbox", "refresh_token": "${env:QUICKBOOKS_REFRESH_TOKEN}"}
    created = await _mutation(
        client,
        f"""
        mutation {{
            createSource(input: {{
                id: "{_SOURCE_ID}",
                type: "quickbooks",
                host: "",
                port: 0,
                database: "${{env:QUICKBOOKS_REALM_ID}}",
                username: "${{env:QUICKBOOKS_CLIENT_ID}}",
                password: "${{env:QUICKBOOKS_SECRET}}",
                mappingJson: {json.dumps(json.dumps(mapping))},
                cacheTtl: 300
            }}) {{ success message }}
        }}
        """,
        "createSource",
    )
    assert created["success"], created


@pytest.fixture
def sent(monkeypatch) -> list[str]:
    """Every query the source sends QuickBooks, in order, as its reader sends it (one entry per
    request: a statement's own read and the table's replica build each send theirs)."""
    from provisa.quickbooks import read as qb_read

    queries: list[str] = []
    page = qb_read._page

    async def recorded(client, url, query, token, forget, timeout):
        queries.append(query)
        return await page(client, url, query, token, forget, timeout)

    monkeypatch.setattr(qb_read, "_page", recorded)
    return queries


def _starts(queries: list[str]) -> list[int]:
    return [int(q.split("STARTPOSITION ")[1].split()[0]) for q in queries]


async def test_customers_are_read_a_page_at_a_time_following_startposition(
    client, company, sent, monkeypatch
):
    from provisa.quickbooks import persist

    del company
    # The table is registered with a page of _PAGE_SIZE: every reader of it pages by that size.
    monkeypatch.setattr(persist, "MAX_PAGE", _PAGE_SIZE)
    await _registered(client, "customer", "customers", ["id", "display_name", "balance", "active"])
    rows = await _sql(
        client, f'SELECT "id", "display_name", "balance" FROM "{_DOMAIN}"."customers"'
    )
    print(f"\ncustomer: {len(rows)} rows; {len(sent)} requests of {_PAGE_SIZE} rows")
    for q in sent:
        print(f"  sent: {q}")
    assert rows, "the sandbox company holds no customers"
    assert all(r["id"] and r["display_name"] for r in rows)
    assert len(rows) == len({r["id"] for r in rows})
    assert len(rows) > _PAGE_SIZE, f"the company holds {len(rows)} customers: one page"
    assert all(
        q.startswith("SELECT * FROM Customer WHERE Active IN (true, false) STARTPOSITION ")
        and q.endswith(f"MAXRESULTS {_PAGE_SIZE}")
        for q in sent
    ), sent
    # Each page starts after the last: 1, 6, 11, ... up to the page that came back short.
    assert set(range(1, len(rows) + 1, _PAGE_SIZE)) <= set(_starts(sent)), sent


async def test_an_invoice_balance_condition_is_sent_in_quickbooks_query(client, company, sent):
    del company
    await _registered(
        client,
        "invoice",
        "invoices",
        ["id", "doc_number", "txn_date", "total_amt", "balance", "customer_ref_value"],
    )
    # The first statement on the table is read from QuickBooks with its condition pushed; the
    # table's replica, built beside it, serves the statements after it.
    open_ones = await _sql(
        client,
        f'SELECT "id", "balance", "customer_ref_value" FROM "{_DOMAIN}"."invoices" '
        'WHERE "balance" > 0',
    )
    every = await _sql(client, f'SELECT "id", "balance" FROM "{_DOMAIN}"."invoices"')
    print(f"\ninvoice: {len(every)} rows; balance > 0: {len(open_ones)} rows")
    for q in sent:
        print(f"  sent: {q}")
    assert every, "the sandbox company holds no invoices"
    assert open_ones, "the sandbox company holds no invoice with a balance"
    assert all(float(r["balance"]) > 0 for r in open_ones)
    assert all(r["customer_ref_value"] for r in open_ones)
    assert {r["id"] for r in open_ones} == {r["id"] for r in every if float(r["balance"]) > 0}
    assert "SELECT * FROM Invoice WHERE Balance > '0' STARTPOSITION 1 MAXRESULTS 1000" in sent
